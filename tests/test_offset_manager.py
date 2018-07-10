import pytest
from kafka import TopicPartition, OffsetAndMetadata
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
        tracker = KafkaMessageOffsetTracker(-1)

        tracker.push_id_and_offset(4, 14)
        tracker.push_id_and_offset(1, 11)
        tracker.push_id_and_offset(2, 12)
        tracker.push_id_and_offset(3, 13)

        # last offset was not known, so we assume it's 10 (before smallest message offset, which is 11)
        tracker.pop_id(2)
        assert tracker.last_done_offset() == 10

    def test_pop_id_updates_last_offset(self):
        tracker = KafkaMessageOffsetTracker(10)
        # random order
        tracker.push_id_and_offset(4, 14)
        tracker.push_id_and_offset(1, 11)
        tracker.push_id_and_offset(2, 12)
        tracker.push_id_and_offset(3, 13)

        tracker.pop_id(2)
        assert tracker.last_done_offset() == 10
        tracker.pop_id(1)
        assert tracker.last_done_offset() == 12
        tracker.pop_id(4)
        assert tracker.last_done_offset() == 12
        tracker.pop_id(3)
        assert tracker.last_done_offset() == 14


class TestKafkaCommitOffsetManager(object):
    class _DummyDoneIdsTracker(KafkaProcessedMessageTracker):
        def __init__(self):
            self.done_ids = []

        def get_message_id(self, message):
            return message['id']

        def get_processed_message_ids(self):
            ids = self.done_ids
            self.done_ids = []
            return ids

        def mark_message_ids_processed(self, message_ids):
            self.done_ids.extend(message_ids)

    @staticmethod
    def _build_offsets(*tuples):
        offsets = {}
        for t in tuples:
            topic_partition = TopicPartition(t[0], t[1])
            offset = OffsetAndMetadata(t[2], '')
            offsets[topic_partition] = offset
        return offsets

    def test_watch_message_immediately_updates_increasing_immediate_commit_offsets(self):
        # Only immediate commit topics (no get_message_id and get_processed_message_ids callables)
        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={})

        # Normally messages shouldn't arrive out of order, but we must not fail anyway
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=100, key='', value=''))
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=99, key='', value=''))
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=101, key='', value=''))

        offset_manager.watch_message(KafkaMessage(topic='foo', partition=10, offset=1, key='', value=''))

        offset_manager.update_message_states()

        assert offset_manager.get_offsets_to_commit() == self._build_offsets(('foo', 0, 101), ('foo', 10, 1))

    def test_watch_message_delays_waitable_offsets(self):
        done_tracker = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker
        })

        # Messages added but not yet 'done'
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000)))
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=10, offset=1, key='', value=dict(id=1001)))

        offset_manager.update_message_states()
        assert offset_manager.get_offsets_to_commit() == {}

        # Mark message #2 done
        done_tracker.mark_message_ids_processed([1001])
        offset_manager.update_message_states()
        assert offset_manager.get_offsets_to_commit() == self._build_offsets(('foo', 10, 1))

        # Mark message #1 done
        done_tracker.mark_message_ids_processed([1000])
        offset_manager.update_message_states()
        assert offset_manager.get_offsets_to_commit() == self._build_offsets(('foo', 0, 100))

    def test_get_offsets_to_commit_gets_immediate_commit_offsets_and_done_waitable_offsets_then_clears(self):
        done_tracker = self._DummyDoneIdsTracker()

        offset_manager = KafkaCommitOffsetManager(topic_processed_message_trackers={
            'foo': done_tracker
        })

        # Messages added but not yet 'done'
        offset_manager.watch_message(KafkaMessage(topic='foo', partition=0, offset=100, key='', value=dict(id=1000)))

        # Immediate commit messages (topic 'bar' not in 'topic_tasks')
        offset_manager.watch_message(KafkaMessage(topic='bar', partition=10, offset=1, key='', value=''))

        done_tracker.mark_message_ids_processed([1000])
        offset_manager.update_message_states()
        assert offset_manager.get_offsets_to_commit() == self._build_offsets(('foo', 0, 100), ('bar', 10, 1))

        # Second time the stored offsets should be empty
        assert offset_manager.get_offsets_to_commit() == {}
