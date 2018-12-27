import pytest
from kafka import TopicPartition
from kafka.structs import KafkaMessage

from kafka_python_helpers.offset_manager import KafkaMessageOffsetTracker, KafkaProcessedMessageTracker, \
    KafkaCommitOffsetManager


class TestKafkaMessageOffsetTracker(object):
    def test_pop_id_requires_existing_id(self):
        tracker = KafkaMessageOffsetTracker(10)
        with pytest.raises(AssertionError):
            tracker.pop_id(11)
        tracker.push_id_and_offset(11, 100)
        tracker.pop_id(11)  # should not raise

    def test_pop_id_updates_last_offset_with_min_offset_if_not_passed_through_constructor(self):
        tracker = KafkaMessageOffsetTracker()

        tracker.push_id_and_offset(4, 14)
        tracker.push_id_and_offset(1, 11)
        tracker.push_id_and_offset(2, 12)
        tracker.push_id_and_offset(3, 13)

        # last offset was not known, so we assume it's 11 (equal to lowest message offset)
        tracker.pop_id(2)
        assert tracker.get_offset_to_commit() == 11

    def test_pop_id_updates_last_offset(self):
        tracker = KafkaMessageOffsetTracker(10)
        # random order
        tracker.push_id_and_offset(4, 13)
        tracker.push_id_and_offset(1, 10)
        tracker.push_id_and_offset(2, 11)
        tracker.push_id_and_offset(3, 12)

        tracker.pop_id(2)
        assert tracker.get_offset_to_commit() == 10
        tracker.pop_id(1)
        assert tracker.get_offset_to_commit() == 12
        tracker.pop_id(4)
        assert tracker.get_offset_to_commit() == 12
        tracker.pop_id(3)
        assert tracker.get_offset_to_commit() == 14

    def test_get_done_ids_returns_popped_ids(self):
        tracker = KafkaMessageOffsetTracker()

        tracker.push_id_and_offset(1, 11)
        tracker.push_id_and_offset(2, 12)
        tracker.push_id_and_offset(3, 13)
        tracker.push_id_and_offset(4, 14)

        tracker.pop_id(2)
        tracker.pop_id(4)

        assert tracker.get_done_ids() == {2, 4}

    def test_get_all_ids_returns_all_ids(self):
        tracker = KafkaMessageOffsetTracker()

        tracker.push_id_and_offset(1, 11)
        tracker.push_id_and_offset(2, 12)
        tracker.push_id_and_offset(3, 13)
        tracker.push_id_and_offset(4, 14)

        tracker.pop_id(2)
        tracker.pop_id(4)

        assert tracker.get_all_ids() == {1, 2, 3, 4}

    def test_commit_done_ids_resets_done_ids(self):
        tracker = KafkaMessageOffsetTracker()

        tracker.push_id_and_offset(1, 11)
        tracker.push_id_and_offset(2, 12)
        tracker.push_id_and_offset(3, 13)
        tracker.push_id_and_offset(4, 14)

        tracker.pop_id(2)
        tracker.pop_id(4)

        assert tracker.get_done_ids() == {2, 4}

        tracker.commit_done_ids()
        assert tracker.get_done_ids() == set()


class TestKafkaCommitOffsetManager(object):
    class _DummyDoneIdsTracker(KafkaProcessedMessageTracker):
        def __init__(self):
            self.done_ids = []

        def get_message_id(self, message):
            return message['id']

        def get_and_clear_processed_message_ids(self):
            ids = self.done_ids
            self.done_ids = []
            return ids

        def mark_message_ids_processed(self, message_ids):
            self.done_ids.extend(message_ids)

        def remove_message_ids(self, message_ids):
            for msg_id in message_ids:
                self.done_ids.remove(msg_id)

    @staticmethod
    def _build_offsets(*tuples):
        offsets = {}
        for t in tuples:
            topic_partition = TopicPartition(t[0], t[1])
            offset = t[2]
            offsets[topic_partition] = offset
        return offsets

    def test_watch_message_delays_waitable_offsets(self):
        done_tracker = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker
        })

        # Messages added but not yet 'done'
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000)))
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=10, offset=1, key='', value=dict(id=1001)))

        assert offset_manager.pop_offsets_to_commit() == {}

        # Mark message #2 done
        done_tracker.mark_message_ids_processed([1001])
        assert offset_manager.pop_offsets_to_commit() == self._build_offsets(('foo', 10, 2))

        # Mark message #1 done
        done_tracker.mark_message_ids_processed([1000])
        assert offset_manager.pop_offsets_to_commit() == self._build_offsets(('foo', 0, 101))

    def test_pop_offsets_to_commit_gets_immediate_commit_offsets_and_done_offsets_then_clears(self):
        done_tracker = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker
        })

        # Messages added but not yet 'done'
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000)))

        # Message not yet done, nothing to commit
        assert offset_manager.pop_offsets_to_commit() == {}

        # Message done, offset can be committed
        done_tracker.mark_message_ids_processed([1000])
        assert offset_manager.pop_offsets_to_commit() == self._build_offsets(('foo', 0, 101))

        # Stored offsets should be have been cleared
        assert offset_manager.pop_offsets_to_commit() == {}

    def test_pop_offsets_to_commit_clears_done_stored_partition_message_ids(self):
        done_tracker = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker
        })

        # Done messages
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000)))
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=101, key='', value=dict(id=1001)))

        # Messages added but not yet 'done'
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=102, key='', value=dict(id=1002)))

        done_tracker.mark_message_ids_processed([1000, 1001])

        assert offset_manager.pop_offsets_to_commit() == {TopicPartition(topic='foo', partition=0): 102}

        # Should be empty the second time around
        assert offset_manager.pop_offsets_to_commit() == {}

    def test_mark_message_done_marks_message_done(self):
        done_tracker = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker
        })

        # Messages added but not yet 'done'
        msg_foo_0 = KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000))
        msg_foo_10 = KafkaMessage(topic='foo', partition=10, offset=1, key='', value=dict(id=1001))
        offset_manager.watch_message(msg_foo_0)
        offset_manager.watch_message(msg_foo_10)

        assert offset_manager.pop_offsets_to_commit() == {}

        # Mark message #2 done
        offset_manager.mark_message_ids_done('foo', [1001])
        assert offset_manager.pop_offsets_to_commit() == self._build_offsets(('foo', 10, 2))

        # Mark message #1 done
        offset_manager.mark_message_ids_done('foo', [1000])
        assert offset_manager.pop_offsets_to_commit() == self._build_offsets(('foo', 0, 101))

    def test_reset_topic_partition_resets_single_topic_partition(self):
        done_tracker_foo = self._DummyDoneIdsTracker()
        done_tracker_bar = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker_foo,
            'bar': done_tracker_bar
        })

        # Messages added but not yet 'done'
        msg_foo_0 = KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000))
        msg_foo_1 = KafkaMessage(topic='foo', partition=1, offset=10, key='', value=dict(id=1001))
        msg_bar = KafkaMessage(topic='bar', partition=10, offset=1, key='', value=dict(id=2000))
        offset_manager.watch_message(msg_foo_0)
        offset_manager.watch_message(msg_foo_1)
        offset_manager.watch_message(msg_bar)
        offset_manager.mark_message_ids_done('foo', [1000])
        offset_manager.mark_message_ids_done('foo', [1001])
        offset_manager.mark_message_ids_done('bar', [2000])

        assert done_tracker_foo.done_ids == [1000, 1001]
        assert done_tracker_bar.done_ids == [2000]

        offset_manager.reset_topic_partition('foo', 0)

        assert done_tracker_foo.done_ids == [1001]
        assert done_tracker_bar.done_ids == [2000]

        assert offset_manager.pop_offsets_to_commit() == self._build_offsets(('foo', 1, 11), ('bar', 10, 2))

