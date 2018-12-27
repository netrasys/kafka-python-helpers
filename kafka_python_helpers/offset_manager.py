from collections import defaultdict, namedtuple
from threading import Lock

import six
from kafka import TopicPartition

from kafka_python_helpers.utils import compact_int_list

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
    """

    def get_message_id(self, message):
        """
        Returns the ID of the message (such as 'request_id').
        """
        raise NotImplementedError

    def get_and_clear_processed_message_ids(self):
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

    def get_and_clear_processed_message_ids(self):
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

    def __init__(self, last_committed_offset=0):
        # The commit offset is actually the offset of the *next* message to be consumed
        # (i.e. the offset of the last consumed message plus one)
        self._last_committed_offset = last_committed_offset
        self._offset_to_commit = last_committed_offset
        self._dirty = False
        self._ids_to_offsets = {}
        self._done_offsets = set()
        self._done_ids = set()

    def _update_offset_to_commit(self):
        self._offset_to_commit = self._last_committed_offset
        while self._offset_to_commit in self._done_offsets:
            self._offset_to_commit += 1

        self._dirty = self._offset_to_commit != self._last_committed_offset

    def _add_done_id_and_offset(self, id, offset):
        assert self._last_committed_offset >= 0
        assert offset >= self._last_committed_offset

        self._done_ids.add(id)
        self._done_offsets.add(offset)

        self._update_offset_to_commit()

    def push_id_and_offset(self, id, offset, as_done=False):
        # Detect last committed offset, if not known
        if self._last_committed_offset <= 0 or self._last_committed_offset > offset:
            self._last_committed_offset = offset

        # Skip duplicate id (i.e. mark its offset "done")
        if as_done or id in self._ids_to_offsets:
            self._add_done_id_and_offset(id, offset)
        else:
            self._ids_to_offsets[id] = offset

    def pop_id(self, id):
        offset = self._ids_to_offsets.pop(id, None)
        assert (offset is not None)

        self._add_done_id_and_offset(id, offset)

    def __repr__(self):
        return "KafkaMessageOffsetTracker(last_committed_offset=%d, offset_to_commit=%d, dirty=%s, done_offsets=%s)" % \
               (self._last_committed_offset, self._offset_to_commit, self.dirty(),
                compact_int_list(sorted(self._done_offsets)))

    def dirty(self):
        return self._dirty

    def commit_done_ids(self):
        # All offsets in the range must be in '_done_offsets', so 'remove' should never fail
        for offset in range(self._last_committed_offset, self._offset_to_commit):
            self._done_offsets.remove(offset)

        self._last_committed_offset = self._offset_to_commit
        self._done_ids = set()
        self._dirty = False

    def get_offset_to_commit(self):
        return self._offset_to_commit

    def get_all_ids(self):
        return self._done_ids | set(self._ids_to_offsets.keys())

    def get_done_ids(self):
        return self._done_ids


class KafkaCommitOffsetManager(object):
    """
    Thread-safe object that tracks processed offsets in all topics. Designed to be used as a singleton.
    """
    _TopicData = namedtuple('_TopicData', 'processed_message_tracker partition_offset_trackers')

    def __init__(self, topic_processed_message_trackers):
        """
        :param topic_processed_message_trackers: dict of topic names to KafkaProcessedMessageTracker instances
        """
        self._topic_datas = {}
        self._message_topic_partitions = {}
        self._processed_message_trackers = set()
        self._lock = Lock()

        _get_logger().debug("KafkaCommitOffsetManager: Tracking %s" % list(topic_processed_message_trackers.keys()))

        for topic, tracker in six.iteritems(topic_processed_message_trackers):
            if not isinstance(tracker, KafkaProcessedMessageTracker):
                raise TypeError("Topic tracker must be a KafkaProcessedMessageTracker")

            self._processed_message_trackers.add(tracker)
            self._topic_datas[topic] = self._TopicData(processed_message_tracker=tracker,
                                                       partition_offset_trackers=defaultdict(KafkaMessageOffsetTracker))

    def _processed_message_tracker(self, topic):
        # Processed message trackers are immutable, no need for locking
        return self._topic_datas[topic].processed_message_tracker

    def _partition_offset_tracker(self, topic, partition, delete=False):
        if delete:
            return self._topic_datas[topic].partition_offset_trackers.pop(partition, None)
        else:
            return self._topic_datas[topic].partition_offset_trackers[partition]

    def _get_message_id(self, msg):
        # Processed message trackers are immutable, no need for locking
        processed_message_tracker = self._processed_message_tracker(msg.topic)
        return processed_message_tracker.get_message_id(msg.value)

    def _get_all_processed_message_ids(self):
        all_processed_ids = []

        for processed_message_tracker in self._processed_message_trackers:
            processed_ids = processed_message_tracker.get_and_clear_processed_message_ids()
            if processed_ids:
                all_processed_ids.extend(processed_ids)
                _get_logger().debug("KafkaCommitOffsetManager: Got processed IDs: %s" % processed_ids)

        return all_processed_ids

    def _mark_message_offset_done(self, msg_id):
        topic_partition = self._message_topic_partitions.pop(msg_id, None)
        if topic_partition is None:
            _get_logger().error("KafkaCommitOffsetManager: Wanted to mark untracked message ID '%s' done; "
                                "maybe unhandled duplicate?" %
                                msg_id)
            return

        _get_logger().debug("KafkaCommitOffsetManager: Message ID '%s' done on %s" %
                            (msg_id, topic_partition))

        self._partition_offset_tracker(topic_partition.topic, topic_partition.partition).pop_id(msg_id)

    def _update_done_offsets(self):
        """
        Update "done" offsets for all monitored topics and partitions.
        :return:
        """
        processed_ids = self._get_all_processed_message_ids()
        for processed_id in processed_ids:
            self._mark_message_offset_done(processed_id)

        if processed_ids:
            _get_logger().debug("KafkaCommitOffsetManager: Updated message states; partition offset trackers: %s" %
                                {topic: dict(topic_data.partition_offset_trackers)
                                 for topic, topic_data in six.iteritems(self._topic_datas)})

    def remove_topic_partition(self, topic, partition):
        """
        Remove a topic partition and un-track its messages.
        :param topic: topic name
        :param partition: partition index
        """
        with self._lock:
            _get_logger().debug("KafkaCommitOffsetManager: Removing topic '%s' partition %d" %
                                (topic, partition))

            offset_tracker = self._partition_offset_tracker(topic, partition, delete=True)
            if offset_tracker is not None:
                msg_ids = offset_tracker.get_all_ids()
                self._processed_message_tracker(topic).remove_message_ids(list(msg_ids))

    def watch_message(self, msg):
        """
        Watch a message for "done-ness".
        When it is done, its consumer offset can be committed.
        :type msg: KafkaMessage
        :rtype: bool
        :returns True if message was added, False if it was a duplicate (therefore discarded)
        """
        msg_id = self._get_message_id(msg)
        _get_logger().debug("KafkaCommitOffsetManager: Tracking message ID '%s'" % msg_id)

        with self._lock:
            offset_tracker = self._partition_offset_tracker(msg.topic, msg.partition)

            if msg_id in self._message_topic_partitions:
                _get_logger().warning("KafkaCommitOffsetManager: Duplicate message ID '%s', discarding" % msg_id)
                is_duplicate = True
            else:
                is_duplicate = False
                self._message_topic_partitions[msg_id] = TopicPartition(msg.topic, msg.partition)

            offset_tracker.push_id_and_offset(msg_id, msg.offset, as_done=is_duplicate)

            return not is_duplicate

    def mark_message_ids_done(self, topic, msg_ids):
        """
        Convenience method to mark a topic's messages as "done" through the manager instead of through the assigned
        processed message tracker.
        :param topic: topic name
        :param msg_ids: list of message IDs belonging to the topic
        """
        assert isinstance(msg_ids, (list, tuple))
        _get_logger().debug("KafkaCommitOffsetManager: Marking message IDs %s done" % msg_ids)

        with self._lock:
            self._processed_message_tracker(topic).mark_message_ids_processed(msg_ids)

    def pop_offsets_to_commit(self):
        # type: () -> dict[TopicPartition, int]
        """
        Get current list of offsets to commit for all monitored topics and partitions.
        The internal list of offsets is cleared upon return.
        :return: list of OffsetAndMetadata to commit
        """
        offsets = {}

        with self._lock:
            self._update_done_offsets()

            for topic, topic_data in six.iteritems(self._topic_datas):
                for partition, tracker in six.iteritems(topic_data.partition_offset_trackers):
                    if tracker.dirty():
                        topic_partition = TopicPartition(topic, partition)
                        offsets[topic_partition] = tracker.get_offset_to_commit()
                        tracker.commit_done_ids()

        if offsets:
            _get_logger().debug("KafkaCommitOffsetManager: Offsets to commit: %s" % offsets)
        return offsets
