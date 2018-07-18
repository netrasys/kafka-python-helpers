#!/usr/bin/env python
from __future__ import print_function

import argparse

from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.errors import CommitFailedError
from kafka.structs import OffsetAndMetadata

from kafka_python_helpers.kafka_client import new_kafka_json_consumer


class MyConsumerRebalanceListener(ConsumerRebalanceListener):
    def on_partitions_assigned(self, partitions):
        # print "on_partitions_assigned(%s)" % partitions
        pass

    def on_partitions_revoked(self, partitions):
        # print "on_partitions_revoked(%s)" % partitions
        pass


def flush_topic(servers, certs_path_prefix, topic, group):
    print("Connecting to Kafka servers %s and subscribing to '%s' in consumer group '%s' - this might take a while..." %
          (servers, topic, group))

    consumer = new_kafka_json_consumer(consumer_name='flush_kafka_topic',
                                       bootstrap_servers=servers,
                                       consumer_group_id=group,
                                       ssl_path_prefix=certs_path_prefix,
                                       enable_auto_commit=False)

    consumer.subscribe(topic, listener=MyConsumerRebalanceListener())
    consumer.poll()	 # will trigger a rebalance operation with partition assignment

    topic_partitions = consumer.partitions_for_topic(topic)
    assigned_partitions = consumer.assignment()

    # No other consumers must be part of the group while we seek
    if topic_partitions != set([p.partition for p in assigned_partitions]):
        raise RuntimeError("This script needs to be the only consumer in the '%s' group,"
                           "but it has only been assigned %d partitions out of %d. "
                           "Please stop all other consumers and retry." %
                           (group, len(assigned_partitions), len(topic_partitions)))

    consumer.seek_to_end()
    end_positions = {
        partition: OffsetAndMetadata(consumer.position(partition), u'')
        for partition in assigned_partitions
    }

    print("Seeking to the end of '%s'; resulting partition offsets:" % topic)
    for partition in sorted(end_positions.keys()):
        position = end_positions[partition]
        print("  Partition #%d: %d" % (partition.partition, position.offset))

    retry = True
    while retry:
        try:
            consumer._coordinator.ensure_active_group()
            consumer._coordinator.commit_offsets_sync(end_positions)
            retry = False
        except CommitFailedError:
            pass

    print('All done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--bootstrap-servers', help='Kafka bootstrap servers (comma separated)', required=True)
    parser.add_argument('-g', '--group', help='Kafka consumer group name', default='iris-daemons')
    parser.add_argument('--certs-path-prefix', help='Kafka certificate files path prefix',
                        default=None)
    parser.add_argument('topic', help='Kafka topic to flush')
    args = parser.parse_args()

    flush_topic(servers=args.bootstrap_servers.split(','),
                certs_path_prefix=args.certs_path_prefix,
                topic=args.topic,
                group=args.group)
