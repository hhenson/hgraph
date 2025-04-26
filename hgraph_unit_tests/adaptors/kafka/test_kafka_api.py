from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from hgraph import (
    graph,
    TS,
    debug_print,
    const,
    GlobalState,
    evaluate_graph,
    GraphConfiguration,
    EvaluationMode,
    sample,
    if_true,
    TSB,
    combine,
)

from hgraph.adaptors.kafka import register_kafka_adaptor, message_subscriber, message_publisher
from hgraph.test import eval_node


@pytest.fixture
def mock_kafka_producer():
    # Create a mock producer instance
    producer = MagicMock()
    # Mock the asynchronous 'send' method to return a mock Future
    mock_future = MagicMock()
    mock_future.add_callback.return_value = None
    mock_future.add_errback.return_value = None
    producer.send.return_value = mock_future
    producer.flush.return_value = None
    producer.close.return_value = None
    return producer


@pytest.fixture
def mock_kafka_state(mock_kafka_producer):
    # Mock the KafkaClientsState instance
    with GlobalState() as gs:
        from hgraph.adaptors.kafka._impl import KafkaMessageState

        state_instance = KafkaMessageState().instance()
        # Just for the value of the _kafka_producer and up the count to make sure
        # We don't try and kill it for now.
        state_instance._kafka_producer = mock_kafka_producer
        state_instance._kafka_producer_count += 1
        yield state_instance


@pytest.mark.skip(reason="Not patched yet")
def test_subscriber():
    @message_subscriber
    def my_subscriber(msg: TS[bytes]):
        debug_print("test_subs1", msg)

    @message_subscriber(topic="test")
    def my_other_subscriber(msg: TS[bytes], recovered: TS[bool]):
        debug_print("test_subs2:msg", msg)
        debug_print("test_subs2:recovered", recovered)

    @message_publisher(topic="test")
    def my_publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        return sample(if_true(recovered), const(b"recovered"))

    @graph
    def g():
        register_kafka_adaptor({})
        my_subscriber(topic="test")
        my_other_subscriber()
        my_publisher()

    evaluate_graph(
        g,
        GraphConfiguration(
            run_mode=EvaluationMode.REAL_TIME,
            start_time=(st := datetime.utcnow()) - timedelta(hours=12),
            end_time=st + timedelta(seconds=4),
            trace=False,
        ),
    )
    # assert eval_node(g) == None


def test_publisher(mock_kafka_state, mock_kafka_producer):
    @message_publisher(topic="test")
    def my_publisher() -> TS[bytes]:
        return const(b"my publisher")

    @graph
    def g():
        register_kafka_adaptor({})
        my_publisher()

    assert eval_node(g) == None
    assert mock_kafka_producer.send.call_count == 1
    assert mock_kafka_producer.send.call_args[0][0] == "test"
    assert mock_kafka_producer.send.call_args[0][1] == b"my publisher"


def test_publisher_without_predefined_topic(mock_kafka_state, mock_kafka_producer):
    @message_publisher
    def my_publisher() -> TS[bytes]:
        return const(b"my publisher")

    @graph
    def g():
        register_kafka_adaptor({})
        my_publisher(topic="test")

    assert eval_node(g) == None
    assert mock_kafka_producer.send.call_count == 1
    assert mock_kafka_producer.send.call_args[0][0] == "test"
    assert mock_kafka_producer.send.call_args[0][1] == b"my publisher"


def test_publisher_with_tsb_out(mock_kafka_state, mock_kafka_producer):
    @message_publisher
    def my_publisher() -> TSB["msg" : TS[bytes], "out" : TS[bool]]:
        return combine(msg=const(b"my publisher"), out=const(True))

    @graph
    def g() -> TS[bool]:
        register_kafka_adaptor({})
        return my_publisher(topic="test")

    assert eval_node(g) == [True]
    assert mock_kafka_producer.send.call_count == 1
    assert mock_kafka_producer.send.call_args[0][0] == "test"
    assert mock_kafka_producer.send.call_args[0][1] == b"my publisher"
