#!/usr/bin/env python
from __future__ import print_function

import argparse
import json
import sys

from kafka.consumer.subscription_state import ConsumerRebalanceListener

from kafka_python_helpers.kafka_client import new_kafka_json_producer


class MyConsumerRebalanceListener(ConsumerRebalanceListener):
    def on_partitions_assigned(self, partitions):
        print("on_partitions_assigned: %s" % partitions)

    def on_partitions_revoked(self, partitions):
        print("on_partitions_revoked: %s" % partitions)


def produce(servers, certs_path_prefix, topic, partition, data):
    if partition is None:
        print("Connecting to Kafka servers %s and sending to topic '%s' on random partition" %
              (servers, topic))
    else:
        print("Connecting to Kafka servers %s and sending to topic '%s' on partition %d" %
              (servers, topic, partition))

    consumer = new_kafka_json_producer(bootstrap_servers=servers,
                                       ssl_path_prefix=certs_path_prefix)

    consumer.send(topic, value=data, partition=partition).get(10000)
    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--bootstrap-servers', help='Kafka bootstrap servers (comma separated)', required=True)
    parser.add_argument('-g', '--group', help='Kafka consumer group name', default='iris-daemons')
    parser.add_argument('-C', '--certs-path-prefix', help='Kafka certificate files path prefix',
                        default=None)
    parser.add_argument('topic_partition', help='Kafka topic/partition to send to, like "topic[:partition]"')
    args = parser.parse_args()

    try:
        topic, partition = args.topic_partition.split(':')
        partition = int(partition)
    except ValueError:
        topic = args.topic_partition
        partition = None

    data = json.load(sys.stdin)

    produce(servers=args.bootstrap_servers.split(','),
            certs_path_prefix=args.certs_path_prefix,
            topic=topic, partition=partition,
            data=data)
