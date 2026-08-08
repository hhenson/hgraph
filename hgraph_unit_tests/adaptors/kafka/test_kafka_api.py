import pytest
from datetime import timedelta
from frozendict import frozendict
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
    utc_now, record, stop_engine, get_recorded_value,
)
from hgraph.adaptors.kafka import register_kafka_adaptor, message_subscriber, message_publisher, KafkaMessage
from hgraph.test import eval_node
from kafka import KafkaConsumer
from types import SimpleNamespace
from unittest.mock import MagicMock


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
            start_time=(st := utc_now()) - timedelta(hours=12),
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


def test_publisher_with_kafka_message(mock_kafka_state, mock_kafka_producer):
    @message_publisher(topic="test")
    def my_publisher() -> TS[KafkaMessage]:
        return const(
            KafkaMessage(
                payload=b"my publisher",
                key=b"my key",
                content_type="application/json",
                headers=frozendict({"h1": b"v1"}),
            )
        )

    @graph
    def g():
        register_kafka_adaptor({})
        my_publisher()

    assert eval_node(g) == None
    assert mock_kafka_producer.send.call_count == 1
    assert mock_kafka_producer.send.call_args[0][0] == "test"
    assert mock_kafka_producer.send.call_args[0][1] == b"my publisher"
    assert mock_kafka_producer.send.call_args[1]["key"] == b"my key"
    assert mock_kafka_producer.send.call_args[1]["headers"] == [("h1", b"v1"), ("content-type", b"application/json")]


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

def test_realtime_subscriber(monkeypatch):
    class Consumer(KafkaConsumer):
        sent = False

        def __init__(self, *args, **kwargs): ...
        def partitions_for_topic(self, topic: str): return {0}
        def assign(self, partitions): ...
        def close(self): ...
        def poll(self, **kwargs):
            if self.sent:
                return {}
            self.sent = True
            return {object(): [SimpleNamespace(value=b'ready', key=None, headers=())]}

    from hgraph.adaptors.kafka import _impl as kafka_impl
    monkeypatch.setattr(kafka_impl, "KafkaConsumer", Consumer)

    @message_subscriber(topic="test")
    def subscriber(msg: TS[bytes]) -> TS[bytes]:
        return msg

    @graph
    def g():
        register_kafka_adaptor({})
        msg = subscriber()
        record(msg)
        stop_engine(msg == b"ready")

    with GlobalState():
        evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))
        assert [value for _, value in get_recorded_value()] == [b"ready"]

class _RecordingConsumer(KafkaConsumer):
    """Enough of a KafkaConsumer for the wiring and the historical replay to run."""

    def __init__(self, *args, **kwargs):
        self._polls = 0

    def partitions_for_topic(self, topic: str):
        return {0}

    def assign(self, partitions): ...

    def close(self): ...

    def offsets_for_times(self, timestamps):
        return {tp: None for tp in timestamps}

    def seek(self, tp, offset): ...

    def poll(self, **kwargs):
        self._polls += 1
        if self._polls == 1:
            return {
                "tp": [
                    SimpleNamespace(value=b"history", key=None, headers=(), timestamp=1, topic="t", offset=0)
                ]
            }
        return {}


@pytest.fixture
def fake_kafka(monkeypatch):
    from hgraph.adaptors.kafka import _impl as kafka_impl

    monkeypatch.setattr(kafka_impl, "KafkaConsumer", _RecordingConsumer)
    monkeypatch.setattr(kafka_impl, "KafkaProducer", MagicMock)


def _run(g):
    with GlobalState():
        evaluate_graph(
            g,
            GraphConfiguration(
                run_mode=EvaluationMode.REAL_TIME,
                start_time=(st := utc_now()) - timedelta(minutes=1),
                end_time=st + timedelta(milliseconds=200),
            ),
        )


# The four combinations of publisher and subscriber recovery. Three of these used to fail at
# wiring: a subscriber declaring 'recovered' produced a cycle between the subscriber service impl
# and the adaptor supplying its real-time output, and a subscriber without 'recovered' sharing a
# topic with a recovering publisher left the real-time adaptor with no implementation.


def test_subscriber_with_recovery(fake_kafka):
    @message_subscriber(topic="r1")
    def subscriber(msg: TS[bytes], recovered: TS[bool]):
        debug_print("msg", msg)

    @graph
    def g():
        register_kafka_adaptor({})
        subscriber()

    _run(g)


def test_publisher_with_recovery(fake_kafka):
    @message_publisher(topic="r2")
    def publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        return sample(if_true(recovered), const(b"recovered"))

    @graph
    def g():
        register_kafka_adaptor({})
        publisher()

    _run(g)


def test_publisher_and_subscriber_both_with_recovery(fake_kafka):
    @message_subscriber(topic="r3")
    def subscriber(msg: TS[bytes], recovered: TS[bool]):
        debug_print("msg", msg)

    @message_publisher(topic="r3")
    def publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        return sample(if_true(recovered), const(b"recovered"))

    @graph
    def g():
        register_kafka_adaptor({})
        subscriber()
        publisher()

    _run(g)


def test_subscriber_without_recovery_sharing_a_topic_with_a_recovering_publisher(fake_kafka):
    @message_subscriber(topic="r4")
    def subscriber(msg: TS[bytes]):
        debug_print("msg", msg)

    @message_publisher(topic="r4")
    def publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        return sample(if_true(recovered), const(b"recovered"))

    @graph
    def g():
        register_kafka_adaptor({})
        subscriber()
        publisher()

    _run(g)


class _HandoverConsumer(KafkaConsumer):
    """
    Replays one historical message, then serves one live message.

    The empty second poll ends the replay loop, so the third poll is necessarily the consumer
    thread's: that is what makes this able to tell apart what arrived before and after recovery.
    """

    def __init__(self, *args, **kwargs):
        self._polls = 0

    def partitions_for_topic(self, topic: str):
        return {0}

    def assign(self, partitions): ...

    def close(self): ...

    def offsets_for_times(self, timestamps):
        return {tp: None for tp in timestamps}

    def seek(self, tp, offset): ...

    def _record(self, value: bytes, offset: int):
        rec = SimpleNamespace(value=value, key=None, headers=(), timestamp=offset + 1, topic="h", offset=offset)
        return {"tp": [rec]}

    def poll(self, **kwargs):
        self._polls += 1
        if self._polls == 1:
            return self._record(b"history", 0)
        if self._polls == 3:
            return self._record(b"live", 1)
        return {}


@pytest.fixture
def handover_kafka(monkeypatch):
    from hgraph.adaptors.kafka import _impl as kafka_impl

    monkeypatch.setattr(kafka_impl, "KafkaConsumer", _HandoverConsumer)
    monkeypatch.setattr(kafka_impl, "KafkaProducer", MagicMock)


def _run_recording(g, *keys: str) -> dict[str, list]:
    """Run `g` and return the values recorded under each key. Reading has to happen inside the
    GlobalState the graph ran in, which is why the assertions cannot pull it out themselves."""
    with GlobalState():
        evaluate_graph(
            g,
            GraphConfiguration(
                run_mode=EvaluationMode.REAL_TIME,
                start_time=(st := utc_now()) - timedelta(minutes=1),
                end_time=st + timedelta(milliseconds=500),
            ),
        )
        return {k: [value for _, value in get_recorded_value(key=k)] for k in keys}


# Replay and live delivery share one stream, so each client selects the part it asked for. These
# pin down that selection: getting it wrong is silent, and shows up only as a publisher republishing
# live traffic or a subscriber replaying history it never requested.


def test_a_replaying_publisher_is_not_fed_live_messages(handover_kafka):
    """A publisher replays to rebuild state; feeding it live messages would loop its own output back."""

    @message_subscriber(topic="h1")
    def subscriber(msg: TS[bytes], recovered: TS[bool]):
        record(msg, key="sub")

    @message_publisher(topic="h1")
    def publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        record(msg, key="pub")
        return sample(if_true(recovered), const(b"done"))

    @graph
    def g():
        register_kafka_adaptor({})
        subscriber()
        publisher()

    seen = _run_recording(g, "sub", "pub")
    assert seen["sub"] == [b"history", b"live"]
    assert seen["pub"] == [b"history"]


def test_a_subscriber_without_recovery_sees_only_live_messages(handover_kafka):
    """The publisher's replay must not leak history to a subscriber that never asked for it."""

    @message_subscriber(topic="h2")
    def subscriber(msg: TS[bytes]):
        record(msg, key="sub")

    @message_publisher(topic="h2")
    def publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        return sample(if_true(recovered), const(b"done"))

    @graph
    def g():
        register_kafka_adaptor({})
        subscriber()
        publisher()

    assert _run_recording(g, "sub")["sub"] == [b"live"]


def test_a_subscriber_without_recovery_is_not_given_the_last_replayed_message(fake_kafka):
    """
    Opening the gate at recovery must not hand over the message that arrived before it.

    `fake_kafka` replays one message and then goes quiet, so anything the subscriber receives here
    can only have come from the recovery transition itself. This is the case a gate built on
    `filter_` gets wrong: it copies its input's latest value when the condition turns True.
    """

    @message_subscriber(topic="h3")
    def subscriber(msg: TS[bytes]):
        record(msg, key="sub")

    @message_publisher(topic="h3")
    def publisher(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
        return sample(if_true(recovered), const(b"done"))

    @graph
    def g():
        register_kafka_adaptor({})
        subscriber()
        publisher()

    assert _run_recording(g, "sub")["sub"] == []


def test_kafka_timestamps_are_not_double_counted():
    """
    Kafka timestamps are epoch milliseconds. The conversion used to add the sub-second part twice,
    so a message at .999 replayed a whole second late.
    """
    from datetime import datetime
    from hgraph.adaptors.kafka._impl import _timestamp_to_datetime

    assert _timestamp_to_datetime(0) == datetime(1970, 1, 1)
    assert _timestamp_to_datetime(1) == datetime(1970, 1, 1, 0, 0, 0, 1000)
    assert _timestamp_to_datetime(999) == datetime(1970, 1, 1, 0, 0, 0, 999000)
    assert _timestamp_to_datetime(1_700_000_000_123) == datetime(2023, 11, 14, 22, 13, 20, 123000)


def test_consumer_failure_is_reported_rather_than_swallowed():
    """A broker failure used to be logged and the thread left to exit, so the graph carried on
    against a feed that would never tick again."""
    from hgraph.adaptors.kafka._impl import KafkaConsumerThread

    reported = []

    class Failing:
        def poll(self, **kwargs):
            raise RuntimeError("broker gone")

        def close(self):
            reported.append("closed")

    thread = KafkaConsumerThread("t", Failing(), lambda m: None, on_error=reported.append)
    thread.run()

    assert any(isinstance(r, RuntimeError) for r in reported), f"failure was not reported: {reported}"
    assert "closed" in reported, "the consumer was not closed"
