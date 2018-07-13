import json
import sys
import time
from datetime import datetime
from functools import wraps

from kafka import KafkaConsumer, KafkaProducer, RoundRobinPartitioner
from kafka.errors import KafkaError, CommitFailedError

__logger = None
__old_match_hostname = None


def _get_logger():
    global __logger
    if __logger is None:
        import logging
        __logger = logging.getLogger('kafka_python_helpers')

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
                new_san = map(lambda kv: ('DNS' if kv[0] == 'IP Address' else kv[0], kv[1]), san)
                cert['subjectAltName'] = new_san

        return __old_match_hostname(cert, hostname)

    ssl_module = __import__('ssl')
    __old_match_hostname = getattr(ssl_module, 'match_hostname')
    setattr(ssl_module, 'match_hostname', new_match_hostname)

    return ssl_module


def kafka_retriable(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        delay = 1
        while True:
            try:
                return f(*args, **kwargs)
            except KafkaError as e:
                if e.retriable:
                    if delay > 30:
                        _get_logger().error("Got Kafka retriable exception too many times, giving up: %s" % repr(e))
                        raise
                    else:
                        _get_logger().debug("Got Kafka retriable exception, will retry in %ds: %s" % (delay, repr(e)))
                        time.sleep(delay)
                        delay += delay
                elif isinstance(e, CommitFailedError):
                    _get_logger().warn('Got Kafka commit error, ignoring')
                    return None

    return wrapper


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
                            enable_auto_commit=True):
    @kafka_retriable
    def init_consumer():
        _get_logger().debug("Connecting to Kafka on %s, using group ID '%s'" % (bootstrap_servers, consumer_group_id))

        security_cfg = _security_config(ssl_path_prefix)

        return KafkaConsumer(bootstrap_servers=bootstrap_servers,
                             security_protocol=security_cfg['security_protocol'],
                             ssl_cafile=security_cfg['ssl_cafile'],
                             ssl_certfile=security_cfg['ssl_certfile'],
                             ssl_keyfile=security_cfg['ssl_keyfile'],
                             client_id=consumer_name,
                             group_id=consumer_group_id,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             enable_auto_commit=enable_auto_commit,
                             max_poll_records=20,
                             request_timeout_ms=70000,
                             session_timeout_ms=60000)

    return init_consumer()


def new_kafka_json_producer(bootstrap_servers, ssl_path_prefix=None, partitioner=None):
    if partitioner is None:
        partitioner = RoundRobinPartitioner()

    @kafka_retriable
    def init_producer():
        _get_logger().debug("Connecting to Kafka on %s as producer" % bootstrap_servers)

        security_cfg = _security_config(ssl_path_prefix)

        return KafkaProducer(bootstrap_servers=bootstrap_servers,
                             security_protocol=security_cfg['security_protocol'],
                             ssl_cafile=security_cfg['ssl_cafile'],
                             ssl_certfile=security_cfg['ssl_certfile'],
                             ssl_keyfile=security_cfg['ssl_keyfile'],
                             retries=5,
                             max_block_ms=10000,
                             value_serializer=lambda m: json.dumps(m, cls=_DateTimeJsonEncoder,
                                                                   default=str).encode('utf-8'),
                             partitioner=partitioner)

    return init_producer()
