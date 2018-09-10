import six
from kafka import ConsumerRebalanceListener, TopicPartition, OffsetAndMetadata

from kafka_python_helpers.kafka_client import new_kafka_json_consumer, new_kafka_json_producer
from kafka_python_helpers.offset_manager import KafkaCommitOffsetManager

__logger = None


def _get_logger():
    global __logger
    if __logger is None:
        import logging
        __logger = logging.getLogger(__name__)

    return __logger


def convert_partition_offsets(topic, partition_offsets):
    # type: (str, dict[int, int]) -> dict[TopicPartition, OffsetAndMetadata]
    return {TopicPartition(topic, partition): OffsetAndMetadata(offset, '')
            for partition, offset in six.iteritems(partition_offsets)}


class KafkaTopicConsumer(ConsumerRebalanceListener):
    def __init__(self, bootstrap_servers, ssl_path_prefix,
                 topic,
                 consumer_name, consumer_group_id,
                 rebalance_listener=None,
                 **consumer_extra_args):
        self._paused = False
        self._topic = topic
        self._rebalance_listener = rebalance_listener

        self._kafka_consumer = new_kafka_json_consumer(consumer_name=consumer_name,
                                                       bootstrap_servers=bootstrap_servers,
                                                       consumer_group_id=consumer_group_id,
                                                       ssl_path_prefix=ssl_path_prefix,
                                                       **consumer_extra_args)
        self._kafka_consumer.subscribe(topics=[topic],
                                       listener=self)

        _get_logger().info("KafkaTopicConsumer: subscribed to Kafka topic '%s'" % topic)

    def set_rebalance_listener(self, rebalance_listener):
        self._rebalance_listener = rebalance_listener

    def consume(self, timeout_ms=None):
        return self._kafka_consumer.poll(timeout_ms)

    def commit_offsets(self, partition_offsets=None):
        """
        :type partition_offsets: dict[int, int]
        """
        if partition_offsets is None:
            _get_logger().debug("Commit all '%s' offsets" % self._topic)
            self._kafka_consumer.commit()
        else:
            offsets = convert_partition_offsets(self._topic, partition_offsets)
            if offsets:
                _get_logger().debug("Commit offsets: %s" % repr(offsets))
                self._kafka_consumer.commit(offsets)

    def pause_all_partitions(self):
        partitions = self._kafka_consumer.assignment()
        self._kafka_consumer.pause(*partitions)
        self._paused = True

    def resume_all_partitions(self):
        partitions = self._kafka_consumer.paused()
        self._kafka_consumer.resume(*list(partitions))
        self._paused = False

    def is_paused(self):
        return self._paused

    def on_partitions_assigned(self, assigned):
        _get_logger().info("Kafka rebalance, partitions assigned: %s" % assigned)
        if self._rebalance_listener is not None:
            partitions = [tp.partition for tp in assigned]
            self._rebalance_listener.on_partitions_assigned(partitions)

    def on_partitions_revoked(self, revoked):
        _get_logger().info("Kafka rebalance, partitions revoked: %s" % revoked)
        if self._rebalance_listener is not None:
            partitions = [tp.partition for tp in revoked]
            self._rebalance_listener.on_partitions_revoked(partitions)


class KafkaTopicExchange(object):
    def __init__(self, bootstrap_servers, ssl_path_prefix,
                 topic,
                 consumer_name, consumer_group_id,
                 consumer_extra_args=None,
                 producer=None,
                 producer_extra_args=None,
                 rebalance_listener=None):
        self._topic = topic

        consumer_args = dict(enable_auto_commit=False,
                             auto_offset_reset='earliest')
        if consumer_extra_args is not None:
            consumer_args.update(consumer_extra_args)

        self._topic_consumer = KafkaTopicConsumer(bootstrap_servers=bootstrap_servers,
                                                  ssl_path_prefix=ssl_path_prefix,
                                                  topic=topic,
                                                  consumer_name=consumer_name,
                                                  consumer_group_id=consumer_group_id,
                                                  rebalance_listener=rebalance_listener,
                                                  **consumer_args)

        if producer is not None:
            self._kafka_producer = producer
        else:
            producer_args = {}
            if producer_extra_args is not None:
                producer_args.update(producer_extra_args)

            self._kafka_producer = new_kafka_json_producer(bootstrap_servers=bootstrap_servers,
                                                           ssl_path_prefix=ssl_path_prefix,
                                                           **producer_args)

    def set_consumer_rebalance_listener(self, rebalance_listener):
        self._topic_consumer.set_rebalance_listener(rebalance_listener)

    def consume(self, timeout_ms=None):
        return self._topic_consumer.consume(timeout_ms)

    def commit_offsets(self, partition_offsets):
        """
        :type partition_offsets: dict[int, int]
        """
        self._topic_consumer.commit_offsets(partition_offsets)

    def send(self, msg, partition):
        return self._kafka_producer.send(self._topic, partition=partition, value=msg)


class KafkaTopicCommitOffsetManager(object):
    def __init__(self, topic, processed_message_tracker):
        self._topic = topic
        self._commit_offset_manager = KafkaCommitOffsetManager({topic: processed_message_tracker})

    def mark_message_ids_done(self, msg_ids):
        self._commit_offset_manager.mark_message_ids_done(topic=self._topic, msg_ids=msg_ids)

    def get_offsets_to_commit(self):
        # type: () -> dict[int, int]
        return {tp.partition: offset
                for tp, offset in six.iteritems(self._commit_offset_manager.pop_offsets_to_commit())}

    def reset_partition(self, partition):
        self._commit_offset_manager.reset_topic_partition(self._topic, partition)

    def __getattr__(self, item):
        return self._commit_offset_manager.__getattribute__(item)
