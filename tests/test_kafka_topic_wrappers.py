from kafka import TopicPartition, OffsetAndMetadata

from kafka_python_helpers.kafka_topic_wrappers import convert_partition_offsets


def test_convert_partition_offsets_translates_partition_offsets_to_committable_topic_offsets():
    offsets = convert_partition_offsets('foo', {0: 100, 1: 200})
    assert offsets == {
        TopicPartition(topic='foo', partition=0): OffsetAndMetadata(offset=100, metadata=''),
        TopicPartition(topic='foo', partition=1): OffsetAndMetadata(offset=200, metadata='')
    }
