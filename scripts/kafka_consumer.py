#!/usr/bin/env python


import argparse
import json
from collections import defaultdict

import colorama
import six
from future.moves import itertools
from kafka.consumer.subscription_state import ConsumerRebalanceListener

from kafka_python_helpers.kafka_client import new_kafka_json_consumer

__topic_colors = {}
__available_colors = (colorama.Fore.GREEN, colorama.Fore.YELLOW, colorama.Fore.CYAN,
                      colorama.Fore.RED, colorama.Fore.MAGENTA, colorama.Fore.WHITE,
                      colorama.Fore.BLUE)
__next_color = itertools.cycle(__available_colors)


class MyConsumerRebalanceListener(ConsumerRebalanceListener):
    @staticmethod
    def _pretty_topic_partitions(partitions):
        topic_partitions = defaultdict(list)
        for tp in partitions:
            topic_partitions[tp.topic].append(tp.partition)

        return {topic: sorted(partitions) for topic, partitions in six.iteritems(topic_partitions)}

    def on_partitions_assigned(self, partitions):
        print("on_partitions_assigned: %s" % self._pretty_topic_partitions(partitions))

    def on_partitions_revoked(self, partitions):
        print("on_partitions_revoked: %s" % self._pretty_topic_partitions(partitions))


def topic_print_colorama(topic, txt):
    if topic in __topic_colors:
        color = __topic_colors[topic]
    else:
        color = next(__next_color)
        __topic_colors[topic] = color

    print(color + txt)


def topic_print_default(_topic, txt):
    print(txt)


def consume_topics(servers, certs_path_prefix, topics, group, enable_auto_commit, auto_offset_reset,
                   print_raw, print_func):
    print("Connecting to Kafka servers %s and subscribing to %s in consumer group '%s' - this might take a while..." %
          (servers, topics, group))

    consumer = new_kafka_json_consumer(consumer_name='flush_kafka_topic',
                                       bootstrap_servers=servers,
                                       consumer_group_id=group,
                                       ssl_path_prefix=certs_path_prefix,
                                       enable_auto_commit=enable_auto_commit,
                                       auto_offset_reset=auto_offset_reset)

    consumer.subscribe(topics, listener=MyConsumerRebalanceListener())

    for msg in consumer:
        if print_raw:
            print_func(msg.topic, msg)
        else:
            topic_par_txt = "[%s: %d]" % (msg.topic, msg.partition)
            empty_txt = " " * len(topic_par_txt)
            prefix = topic_par_txt
            for l in json.dumps(msg.value, indent=2).splitlines():
                print_func(msg.topic, "%s %s" % (prefix, l))
                prefix = empty_txt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--bootstrap-servers', help='Kafka bootstrap servers (comma separated)', required=True)
    parser.add_argument('-g', '--group', help='Kafka consumer group name', default='iris-daemons')
    parser.add_argument('-C', '--certs-path-prefix', help='Kafka certificate files path prefix',
                        default=None)
    parser.add_argument('--no-offset-commit', help="Don't commit consumer offsets (i.e. don't advance)",
                        action='store_true')
    parser.add_argument('--since-offset', choices=('earliest', 'latest'),
                        help="Start from earliest or latest offset",
                        default='latest')
    parser.add_argument('-r', '--raw', help='Print raw messages (as received from Kafka)', action='store_true')
    parser.add_argument('topic', help='Kafka topic(s) to consume from', nargs='+')
    args = parser.parse_args()

    if len(args.topic) > 1:
        colorama.init(autoreset=True, strip=False)
        print_func = topic_print_colorama
    else:
        print_func = topic_print_default

    consume_topics(servers=args.bootstrap_servers.split(','),
                   certs_path_prefix=args.certs_path_prefix,
                   topics=args.topic,
                   group=args.group,
                   enable_auto_commit=not args.no_offset_commit,
                   auto_offset_reset=args.since_offset,
                   print_raw=args.raw,
                   print_func=print_func)
