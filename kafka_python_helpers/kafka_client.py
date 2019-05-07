import json
import sys

from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, RoundRobinPartitioner

from kafka_python_helpers.decorators import kafka_retriable

__logger = None
__old_match_hostname = None


def _get_logger():
    global __logger
    if __logger is None:
        import logging
        __logger = logging.getLogger(__name__)

    return __logger


class _DateTimeJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


def _patch_ssl_IP_SAN_check():
    """
    Replace :func:`match_hostname` in :mod:`ssl` with a version that allows IP addresses in SAN entries.
    """
    if tuple(sys.version_info[:2]) >= (3, 5):
        return  # Python 3.5+ has support for IP checks in certificate's subjectAltName

    global __old_match_hostname

    if __old_match_hostname is not None:
        return

    def new_match_hostname(cert, hostname):
        """
        Convert 'IP' entries in certificate to 'DNS' entries because IP checks are not supported by Python.
        """
        if cert:
            san = cert.get('subjectAltName')
            if san:
                new_san = [('DNS' if kv[0] == 'IP Address' else kv[0], kv[1]) for kv in san]
                cert['subjectAltName'] = new_san

        return __old_match_hostname(cert, hostname)

    ssl_module = __import__('ssl')
    __old_match_hostname = getattr(ssl_module, 'match_hostname')
    setattr(ssl_module, 'match_hostname', new_match_hostname)

    return ssl_module


def _security_config(ssl_path_prefix):
    if ssl_path_prefix is not None:
        _get_logger().debug("Kafka uses TLS, getting certificates from '%s'" % ssl_path_prefix)

        _patch_ssl_IP_SAN_check()

        ssl_cafile = "%sca-cert.pem" % ssl_path_prefix
        ssl_certfile = "%sclient-cert-chain.pem" % ssl_path_prefix
        ssl_keyfile = "%sclient.key" % ssl_path_prefix
        security_protocol = 'SSL'
    else:
        ssl_cafile = None
        ssl_certfile = None
        ssl_keyfile = None
        security_protocol = 'PLAINTEXT'

    return dict(security_protocol=security_protocol,
                ssl_cafile=ssl_cafile,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile)


def new_kafka_json_consumer(consumer_name, bootstrap_servers, consumer_group_id, ssl_path_prefix=None,
                            **kafka_consumer_args):
    @kafka_retriable
    def init_consumer():
        _get_logger().debug("Connecting to Kafka on %s, using group ID '%s'" % (bootstrap_servers, consumer_group_id))

        security_cfg = _security_config(ssl_path_prefix)

        extra_args = dict(max_poll_records=20,
                          request_timeout_ms=70000,
                          session_timeout_ms=60000)
        extra_args.update(kafka_consumer_args)

        return KafkaConsumer(bootstrap_servers=bootstrap_servers,
                             security_protocol=security_cfg['security_protocol'],
                             ssl_cafile=security_cfg['ssl_cafile'],
                             ssl_certfile=security_cfg['ssl_certfile'],
                             ssl_keyfile=security_cfg['ssl_keyfile'],
                             client_id=consumer_name,
                             group_id=consumer_group_id,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             **extra_args)

    return init_consumer()


def _serialize_json(msg):
    return json.dumps(msg, cls=_DateTimeJsonEncoder, default=str).encode('utf-8')


def new_kafka_json_producer(bootstrap_servers,
                            ssl_path_prefix=None,
                            partitioner=None,
                            value_serializer=None,
                            **kafka_producer_args):
    if partitioner is None:
        partitioner = RoundRobinPartitioner()

    @kafka_retriable
    def init_producer():
        _get_logger().debug("Connecting to Kafka on %s as producer" % bootstrap_servers)

        security_cfg = _security_config(ssl_path_prefix)

        extra_args = dict(retries=5,
                          max_block_ms=10000,
                          partitioner=partitioner)
        extra_args.update(kafka_producer_args)

        serializer = _serialize_json
        if value_serializer is not None:
            serializer = value_serializer

        return KafkaProducer(bootstrap_servers=bootstrap_servers,
                             security_protocol=security_cfg['security_protocol'],
                             ssl_cafile=security_cfg['ssl_cafile'],
                             ssl_certfile=security_cfg['ssl_certfile'],
                             ssl_keyfile=security_cfg['ssl_keyfile'],
                             value_serializer=serializer,
                             **extra_args)

    return init_producer()
