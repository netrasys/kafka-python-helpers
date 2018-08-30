from collections import defaultdict, namedtuple

import six
from kafka import TopicPartition
from sortedcontainers import SortedList

__logger = None


def _get_logger():
    global __logger
    if __logger is None:
        import logging
        __logger = logging.getLogger(__name__)

    return __logger


class KafkaProcessedMessageTracker(object):
    """
    Track messages by their ID (as returned by 'get_message_id').
    WARNING: You should not use the same instance for multiple topics, as 'reset' is global
             and it is called per topic from KafkaCommitOffsetManager.
    """

    def get_message_id(self, message):
        """
        Returns the ID of the message (such as 'request_id').
        """
        raise NotImplementedError

    def get_processed_message_ids(self):
        """
        Returns a list of message IDs previously returned by 'get_message_id' that are considered "processed"
        and whose consumer offsets can be committed.
        The implementation must discard its list of IDs after each call, as they are only required once.
        """
        raise NotImplementedError

    def mark_message_ids_processed(self, message_ids):
        """
        Manually mark some message IDs as "processed".
        They must be returned by the next call to 'get_processed_message_ids'.
        """
        raise NotImplementedError

    def remove_message_ids(self, message_ids):
        """
        Removes message IDs from tracking.
        """
        raise NotImplementedError


class KafkaBasicProcessedMessageTracker(KafkaProcessedMessageTracker):
    def __init__(self, lock_class):
        self._lock = lock_class()
        self._processed_ids = set()

    def get_message_id(self, message):
        raise NotImplementedError

    def get_processed_message_ids(self):
        with self._lock:
            ids = self._processed_ids
            self._processed_ids = set()
            return list(ids)

    def mark_message_ids_processed(self, message_ids):
        with self._lock:
            self._processed_ids.update(message_ids)

    def remove_message_ids(self, message_ids):
        self._processed_ids -= set(message_ids)


class KafkaMessageOffsetTracker(object):
    """
    Keep a list of queued (id, offset).
    Mark id as "processed".
    Get highest contiguous "processed" offset (i.e. all offsets before it belong to
    "processed" ids).
    """

    def __init__(self, initial_offset=-1):
        # The commit offset is actually the offset of the *next* message to be consumed
        # (i.e. the offset of the last consumed message plus one)
        self._commit_offset = self._old_commit_offset = initial_offset
        self._ids_to_offsets = {}
        self._done_offsets = SortedList()

    def push_id_and_offset(self, id, offset, as_done=False):
        # Skip duplicate id (i.e. mark its offset "done")
        if as_done or id in self._ids_to_offsets:
            self._done_offsets.add(offset)
        else:
            self._ids_to_offsets[id] = offset

        # Detect last committed offset, if not known
        if self._commit_offset < 0 or self._commit_offset > offset:
            self._commit_offset = offset
            self._old_commit_offset = self._commit_offset  # not dirty since it was already committed

    def pop_id(self, id):
        offset = self._ids_to_offsets.pop(id, None)
        assert (offset is not None)

        self._done_offsets.add(offset)

        # Update last processed offset
        if self._done_offsets:
            try:
                while True:
                    self._done_offsets.remove(self._commit_offset)
                    self._commit_offset += 1
            except ValueError:
                pass

    def filter_popped_ids(self, ids):
        """
        Return a set of ids in 'ids' that are no longer tracked.
        :param ids: iterable of message IDs
        :return: set
        """
        return {id for id in ids if id not in self._ids_to_offsets}

    def __repr__(self):
        return "KafkaMessageOffsetTracker(commit_offset=%d, dirty=%s, done_offsets=%s)" % \
               (self._commit_offset, self.dirty(), self._done_offsets)

    def dirty(self):
        return self._commit_offset != self._old_commit_offset

    def get_offset_to_commit_and_reset_dirty(self):
        self._old_commit_offset = self._commit_offset
        return self._commit_offset


class KafkaCommitOffsetManager(object):
    """
    Singleton object that tracks processed offsets in all topics.
    Not thread/process safe!
    """
    _TopicData = namedtuple('_TopicData', 'processed_message_tracker partition_offset_trackers')

    def __init__(self, topic_processed_message_trackers):
        """
        :param topic_processed_message_trackers: dict of topic names to KafkaProcessedMessageTracker instances
        """
        self._topic_data = {}
        self._message_topic_partitions = {}
        self._topic_partition_messages = defaultdict(set)
        self._processed_message_trackers = set()

        _get_logger().debug("KafkaCommitOffsetManager: Tracking %s" % list(topic_processed_message_trackers.keys()))

        for topic, tracker in six.iteritems(topic_processed_message_trackers):
            if not isinstance(tracker, KafkaProcessedMessageTracker):
                raise TypeError("Topic tracker must be a KafkaProcessedMessageTracker")

            self._processed_message_trackers.add(tracker)
            self._topic_data[topic] = self._TopicData(processed_message_tracker=tracker,
                                                      partition_offset_trackers=defaultdict(KafkaMessageOffsetTracker))

        self._immediate_commit_offsets = {}

    def _topic_has_immediate_commit(self, topic):
        return topic not in self._topic_data

    def _processed_message_tracker(self, topic):
        return self._topic_data[topic].processed_message_tracker

    def _partition_offset_tracker(self, msg):
        return self._topic_data[msg.topic].partition_offset_trackers[msg.partition]

    def _get_message_id(self, msg):
        processed_message_tracker = self._processed_message_tracker(msg.topic)
        return processed_message_tracker.get_message_id(msg.value)

    def _track_message_offset(self, msg):
        msg_id = self._get_message_id(msg)
        tracker = self._partition_offset_tracker(msg)

        _get_logger().debug("KafkaCommitOffsetManager: Tracking message ID '%s'" % msg_id)

        if msg_id in self._message_topic_partitions:
            tracker.push_id_and_offset(msg_id, msg.offset, as_done=True)
            _get_logger().warn("KafkaCommitOffsetManager: Duplicate message ID '%s', discarding" % msg_id)
            return False

        tracker.push_id_and_offset(msg_id, msg.offset)

        topic_partition = TopicPartition(msg.topic, msg.partition)
        self._message_topic_partitions[msg_id] = topic_partition
        self._topic_partition_messages[topic_partition].add(msg_id)

        return True

    def _get_all_processed_message_ids(self):
        all_processed_ids = []

        _get_logger().debug('KafkaCommitOffsetManager: Getting processed IDs from all trackers')
        for processed_message_tracker in self._processed_message_trackers:
            processed_ids = processed_message_tracker.get_processed_message_ids()
            _get_logger().debug("KafkaCommitOffsetManager: Got processed IDs: %s" % processed_ids)
            all_processed_ids.extend(processed_ids)
        _get_logger().debug('KafkaCommitOffsetManager: Got all processed IDs')

        return all_processed_ids

    def reset_topic_partition(self, topic, partition):
        """
        Clear a topic partition's state.
        :param topic: topic name
        :param partition: partition index
        """
        _get_logger().debug("KafkaCommitOffsetManager: Resetting offsets for topic '%s' partition %d" %
                            (topic, partition))

        topic_partition = TopicPartition(topic, partition)

        if self._topic_has_immediate_commit(topic):
            self._immediate_commit_offsets.pop(topic_partition, None)
        else:
            topic_data = self._topic_data.get(topic)

            msg_ids = self._topic_partition_messages.pop(topic_partition, set())
            topic_data.processed_message_tracker.remove_message_ids(list(msg_ids))

            topic_data.partition_offset_trackers.pop(partition, None)

    def watch_message(self, msg):
        """
        Watch a message for "done-ness".
        When it is done, its consumer offset can be committed.
        :type msg: KafkaMessage
        :rtype: bool
        :returns True if message was added, False if it was discarded
        """
        _get_logger().debug("KafkaCommitOffsetManager: Watching message: %s" % repr(msg))

        topic_partition = TopicPartition(msg.topic, msg.partition)
        if self._topic_has_immediate_commit(msg.topic):
            offset = self._immediate_commit_offsets.pop(topic_partition, None)

            # The commit offset is actually the offset of the *next* message to be consumed
            # (i.e. the offset of the last consumed message plus one)
            if offset is None or offset < msg.offset + 1:
                self._immediate_commit_offsets[topic_partition] = msg.offset + 1
                return True
        else:
            return self._track_message_offset(msg)

    def mark_message_ids_done(self, topic, msg_ids):
        """
        Convenience method to mark a topic's messages as "done" through the manager instead of through the assigned
        processed message tracker.
        :param topic: topic name
        :param msg_ids: list of message IDs belonging to the topic
        """
        if self._topic_has_immediate_commit(topic):
            return

        _get_logger().debug("KafkaCommitOffsetManager: Marking message IDs %s done" % msg_ids)

        tracker = self._processed_message_tracker(topic)
        tracker.mark_message_ids_processed(msg_ids)

    def _update_message_states(self):
        """
        Update "done" offsets for all monitored topics and partitions.
        :return:
        """
        processed_ids = self._get_all_processed_message_ids()
        for processed_id in processed_ids:
            topic_partition = self._message_topic_partitions.pop(processed_id)
            topic_data = self._topic_data[topic_partition.topic]
            offset_tracker = topic_data.partition_offset_trackers[topic_partition.partition]
            offset_tracker.pop_id(processed_id)

        _get_logger().debug("KafkaCommitOffsetManager: Updated message states; partition offset trackers: %s" %
                            {topic: dict(topic_data.partition_offset_trackers)
                             for topic, topic_data in six.iteritems(self._topic_data)})

    def pop_offsets_to_commit(self):
        # type: () -> dict[TopicPartition, int]
        """
        Get current list of offsets to commit for all monitored topics and partitions.
        The internal list of offsets is cleared upon return.
        :return: list of OffsetAndMetadata to commit
        """
        offsets = {}

        self._update_message_states()

        for topic, topic_data in six.iteritems(self._topic_data):
            for partition, tracker in six.iteritems(topic_data.partition_offset_trackers):
                if tracker.dirty():
                    topic_partition = TopicPartition(topic, partition)
                    offsets[topic_partition] = tracker.get_offset_to_commit_and_reset_dirty()

                    # Remove committed or committable messages
                    partition_msg_ids = self._topic_partition_messages[topic_partition]
                    partition_msg_ids -= tracker.filter_popped_ids(partition_msg_ids)

        offsets.update(self._immediate_commit_offsets)
        self._immediate_commit_offsets = {}

        _get_logger().debug("KafkaCommitOffsetManager: Offsets to commit: %s" % offsets)
        return offsets
