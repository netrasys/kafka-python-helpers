import sys

import pytest
from kafka.errors import NodeNotReadyError
from kafka.future import Future
from mock import patch

from kafka_python_helpers.decorators import kafka_retriable, kafka_retriable_future


@patch('time.sleep', return_value=None)
def test_kafka_retriable_retries_retriable_exception(patched_sleep):
    state = {'tries': 0}

    @kafka_retriable
    def foo():
        state['tries'] += 1
        if state['tries'] < 2:
            raise NodeNotReadyError()   # which is retriable

    foo()
    assert state['tries'] == 2


@patch('time.sleep', return_value=None)
def test_kafka_retriable_tries_at_most_6_times(patched_sleep):
    state = {'tries': 0}

    @kafka_retriable
    def foo():
        state['tries'] += 1
        raise NodeNotReadyError()   # which is retriable

    with pytest.raises(NodeNotReadyError):
        foo()
    assert state['tries'] == 6


@patch('time.sleep', return_value=None)
def test_kafka_retriable_future_calls_success_callback_on_success(patched_sleep):
    state = {}

    def on_success(value):
        state['value'] = value

    def on_error(e):
        state['error'] = repr(e)

    @kafka_retriable_future(on_success, on_error)
    def foo():
        return Future()

    f = foo()
    assert state == {}
    f.success(10)
    assert state == {'value': 10}


@patch('time.sleep', return_value=None)
def test_kafka_retriable_future_retries_retriable_exception(patched_sleep):
    state = {'tries': 0}

    def on_success(value):
        state['value'] = value

    def on_error(e):
        state['error'] = repr(e)

    @kafka_retriable_future(on_success, on_error)
    def foo():
        state['tries'] += 1
        if state['tries'] < 2:
            return Future().failure(NodeNotReadyError())    # which is retriable
        else:
            return Future().success(11)

    foo()
    assert state == {'tries': 2, 'value': 11}


@patch('time.sleep', return_value=None)
def test_kafka_retriable_future_tries_at_most_6_times(patched_sleep):
    state = {'tries': 0}

    def on_success(value):
        state['value'] = value

    def on_error(e):
        state['error'] = e

    @kafka_retriable_future(on_success, on_error)
    def foo():
        state['tries'] += 1
        return Future().failure(NodeNotReadyError())    # which is retriable

    foo()
    assert state['tries'] == 6
    assert isinstance(state['error'], NodeNotReadyError)


@patch('time.sleep', return_value=None)
@patch('six.moves._thread.interrupt_main', side_effect=sys.exit)
def test_kafka_retriable_future_exits_process_on_fatal_error_if_no_errback(patched_sleep, patched_interrupt_main):
    state = {}

    def on_success(value):
        state['value'] = value

    @kafka_retriable_future(on_success)
    def foo():
        return Future().failure(RuntimeError())

    with pytest.raises(SystemExit):
        foo()
    assert state == {}
