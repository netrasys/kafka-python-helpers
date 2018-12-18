import time
from functools import wraps

from kafka.errors import KafkaError, CommitFailedError

__logger = None


def _get_logger():
    global __logger
    if __logger is None:
        import logging
        __logger = logging.getLogger(__name__)

    return __logger


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
                    _get_logger().warning('Got Kafka commit error, ignoring')
                    return None

    return wrapper


def kafka_retriable_future(callback, errback=None):
    def outer_wrapper(f):
        @wraps(f)
        def inner_wrapper(*args, **kwargs):
            state = {'delay': 1}

            def on_future_error(e):
                if isinstance(e, KafkaError) and e.retriable and state['delay'] <= 30:
                    _get_logger().debug("Got Kafka retriable error in future, will retry in %ds: %s" %
                                        (state['delay'], repr(e)))
                    time.sleep(state['delay'])
                    state['delay'] *= 2

                    future = f(*args, **kwargs)
                    future.add_callback(callback)
                    future.add_errback(on_future_error)
                else:
                    if errback:
                        _get_logger().error("Got Kafka fatal error or too many retries in future, passing along: %r" %
                                            e)
                        errback(e)
                    else:
                        import inspect
                        import traceback
                        from six.moves import _thread

                        frame = inspect.currentframe()
                        stack_trace = traceback.format_stack(frame)
                        _get_logger().error("Got Kafka fatal error or too many retries in future, exiting: %r" % e)
                        _get_logger().error(''.join(stack_trace[:-2]))
                        # os._exit(1)     # FIXME: This is very ugly (exit without cleanup)
                        _thread.interrupt_main()

            future = f(*args, **kwargs)
            if callback is not None:
                future.add_callback(callback)
            future.add_errback(on_future_error)
            return future

        return inner_wrapper
    return outer_wrapper
