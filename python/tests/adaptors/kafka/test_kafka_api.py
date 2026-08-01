import inspect
import threading
import time
from datetime import UTC, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.kafka import (
    KafkaMessage,
    MessageState,
    message_publisher,
    message_subscriber,
    register_kafka_adaptor,
)
from hgraph.adaptors.kafka._impl import KafkaMessageState


class _Consumer:
    def __init__(self, batches, partitions=(0, 1)):
        self._batches = list(batches)
        self._partitions = set(partitions)
        self._lock = threading.Lock()
        self.assigned = None
        self.offset_requests = None
        self.seeked = []
        self.close_count = 0

    def partitions_for_topic(self, topic):
        self.topic = topic
        return self._partitions

    def assign(self, partitions):
        self.assigned = tuple(partitions)

    def offsets_for_times(self, timestamps):
        self.offset_requests = dict(timestamps)
        return {
            partition: SimpleNamespace(offset=10 + partition.partition)
            for partition in timestamps
        }

    def seek(self, partition, offset):
        self.seeked.append((partition, offset))

    def poll(self, **kwargs):
        del kwargs
        with self._lock:
            if self._batches:
                return self._batches.pop(0)
        time.sleep(0.005)
        return {}

    def close(self):
        with self._lock:
            self.close_count += 1


def _record(payload, timestamp, *, partition=0, offset=0, headers=()):
    return SimpleNamespace(
        value=payload,
        key=f"key-{offset}".encode(),
        headers=headers,
        timestamp=timestamp,
        topic="test",
        partition=partition,
        offset=offset,
    )


def _state_with_producer(producer):
    state = KafkaMessageState.instance()
    state.configure({"producer": producer})
    return state


def test_public_surface_and_message_state_signatures_match_upstream():
    import hgraph.adaptors.kafka as kafka
    from hgraph.adaptors.kafka import _api, _impl

    assert kafka.__all__ == (
        "message_publisher",
        "message_subscriber",
        "MessageState",
        "KafkaMessage",
        "register_kafka_adaptor",
    )
    assert _api.__all__ == (
        "message_publisher",
        "message_subscriber",
        "MessageState",
        "KafkaMessage",
    )
    assert _impl.__all__ == ("register_kafka_adaptor",)
    assert str(inspect.signature(MessageState.add_subscriber)) == (
        "(self, topic: str, replay: bool = False)")
    assert str(inspect.signature(MessageState.add_historical_subscriber)) == (
        "(self, topic: str)")


def test_kafka_message_compound_scalar_dictionary_conversion():
    message = KafkaMessage(
        payload=b"payload",
        key=b"key",
        content_type="application/json",
        headers=frozendict({"trace": b"1"}),
    )
    assert message.to_dict() == {
        "payload": b"payload",
        "key": b"key",
        "content_type": "application/json",
        "headers": frozendict({"trace": b"1"}),
    }
    assert KafkaMessage(payload=b"payload").to_dict() == {
        "payload": b"payload",
        "headers": frozendict(),
    }
    assert KafkaMessage.from_dict({
        **message.to_dict(), "ignored": "upstream ignores unknown fields"
    }) == message


def test_wrapped_graph_user_signatures_match_upstream_keyword_interface():
    @message_publisher(topic="published")
    def publisher(
            msg: hg.TS[bytes], recovered: hg.TS[bool],
            scale: hg.TS[int] = None) -> hg.TS[bytes]:
        return msg

    @message_subscriber
    def subscriber(
            msg: hg.TS[bytes], recovered: hg.TS[bool],
            scale: hg.TS[int] = None) -> hg.TS[bytes]:
        return msg

    publisher_signature = inspect.signature(publisher)
    assert tuple(publisher_signature.parameters) == ("scale", "topic")
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in publisher_signature.parameters.values())
    assert publisher_signature.parameters["topic"].default == "published"
    assert publisher_signature.return_annotation is None

    subscriber_signature = inspect.signature(subscriber)
    assert tuple(subscriber_signature.parameters) == ("scale", "topic")
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in subscriber_signature.parameters.values())
    assert subscriber_signature.parameters["topic"].default is inspect.Parameter.empty
    assert subscriber_signature.return_annotation == hg.TS[bytes]


def test_message_publisher_uses_the_configured_producer_via_eval_node():
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher() -> hg.TS[bytes]:
        return hg.const(b"payload", tp=hg.TS[bytes])

    @hg.graph
    def app():
        register_kafka_adaptor({"producer": producer})
        publisher()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    producer.send.assert_called_once_with("test", b"payload")
    # One timer-driven flush plus the lifecycle flush on stop.
    assert producer.flush.call_count == 2


def test_structured_message_maps_headers_without_a_second_protocol():
    producer = MagicMock()
    message = KafkaMessage(
        payload=b"payload",
        key=b"key",
        content_type="application/json",
        headers=frozendict({"trace": b"1"}),
    )

    @message_publisher
    def publisher() -> hg.TS[KafkaMessage]:
        return hg.const(message, tp=hg.TS[KafkaMessage])

    @hg.graph
    def app():
        register_kafka_adaptor({"producer": producer})
        publisher(topic="test")

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    producer.send.assert_called_once_with(
        "test",
        b"payload",
        key=b"key",
        headers=[("trace", b"1"), ("content-type", b"application/json")],
    )


def test_bundle_publisher_returns_its_non_message_output():
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher() -> hg.TSB["msg" : hg.TS[bytes], "out" : hg.TS[bool]]:
        return hg.combine(
            msg=hg.const(b"payload", tp=hg.TS[bytes]),
            out=hg.const(True, tp=hg.TS[bool]),
        )

    @hg.graph
    def app() -> hg.TS[bool]:
        register_kafka_adaptor({"producer": producer})
        return publisher()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) == [True]
    producer.send.assert_called_once()


def test_owned_producer_is_closed_on_stop():
    """Regression: an auto-created (owned) producer must be closed when the
    publisher stops. Previously KafkaMessageState.close() was never called and
    the publisher's stop only flushed, leaking the producer connection."""
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher() -> hg.TS[bytes]:
        return hg.const(b"payload", tp=hg.TS[bytes])

    @hg.graph
    def app():
        # producer_factory -> the state OWNS the producer (vs injecting one).
        register_kafka_adaptor({"producer_factory": lambda **opts: producer})
        publisher()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    producer.send.assert_called_once_with("test", b"payload")
    producer.close.assert_called_once()   # the leak is fixed


def test_injected_producer_is_not_closed():
    """An injected producer is owned by the caller and must be left open."""
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher() -> hg.TS[bytes]:
        return hg.const(b"payload", tp=hg.TS[bytes])

    @hg.graph
    def app():
        register_kafka_adaptor({"producer": producer})
        publisher()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    producer.close.assert_not_called()


def test_producer_is_reference_counted_across_publishers():
    """Two publishers share one owned producer: created once, closed once
    when the last publisher stops."""
    created = []

    def factory(**opts):
        producer = MagicMock()
        created.append(producer)
        return producer

    @message_publisher(topic="a")
    def publisher_a() -> hg.TS[bytes]:
        return hg.const(b"a", tp=hg.TS[bytes])

    @message_publisher(topic="b")
    def publisher_b() -> hg.TS[bytes]:
        return hg.const(b"b", tp=hg.TS[bytes])

    @hg.graph
    def app():
        register_kafka_adaptor({"producer_factory": factory})
        publisher_a()
        publisher_b()

    with hg.GlobalContext(hg.GlobalState()):
        assert hg.eval_node(app) is None

    assert len(created) == 1                     # one shared producer
    created[0].close.assert_called_once()        # closed exactly once


def test_live_subscribers_share_one_consumer_and_preserve_structured_message():
    now_ms = int(hg.utc_now().replace(tzinfo=UTC).timestamp() * 1000)
    consumer = _Consumer([
        {0: [_record(
            b"ready", now_ms, offset=1,
            headers=(("content-type", b"application/json"), ("trace", b"7")),
        )]},
    ], partitions=(0,))
    seen_a = []
    seen_b = []

    @hg.sink_node
    def capture_a(msg: hg.TS[KafkaMessage]):
        seen_a.append(msg.value)

    @hg.sink_node
    def capture_b(msg: hg.TS[KafkaMessage]):
        seen_b.append(msg.value)

    @message_subscriber(topic="test")
    def subscriber_a(msg: hg.TS[KafkaMessage]):
        capture_a(msg)

    @message_subscriber(topic="test")
    def subscriber_b(msg: hg.TS[KafkaMessage]):
        capture_b(msg)

    @hg.graph
    def app():
        register_kafka_adaptor({"consumer_factory": lambda **opts: consumer})
        subscriber_a()
        subscriber_b()

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=hg.utc_now() + timedelta(seconds=0.2),
        )

    expected = KafkaMessage(
        payload=b"ready",
        key=b"key-1",
        content_type="application/json",
        headers=frozendict({"trace": b"7"}),
    )
    assert seen_a == [expected]
    assert seen_b == [expected]
    assert consumer.topic == "test"
    assert len(consumer.assigned) == 1
    assert consumer.close_count == 1


def test_replay_seeks_from_graph_start_orders_history_and_hands_off_to_live():
    start_time = hg.utc_now() - timedelta(milliseconds=100)
    start_ms = int(start_time.replace(tzinfo=UTC).timestamp() * 1000)
    later = _record(b"history-2", start_ms + 20, partition=1, offset=2)
    earlier = _record(b"history-1", start_ms + 10, partition=0, offset=1)
    live = _record(b"live", start_ms + 120, partition=0, offset=3)
    consumer = _Consumer([
        {1: [later], 0: [earlier]},
        {},
        {0: [live]},
    ])
    messages = []
    recovered_values = []

    @hg.sink_node
    def capture_message(msg: hg.TS[bytes]):
        messages.append(msg.value)

    @hg.sink_node
    def capture_recovered(recovered: hg.TS[bool]):
        recovered_values.append(recovered.value)

    @message_subscriber(topic="test")
    def subscriber(msg: hg.TS[bytes], recovered: hg.TS[bool]):
        capture_message(msg)
        capture_recovered(recovered)

    @hg.graph
    def app():
        register_kafka_adaptor({"consumer_factory": lambda **opts: consumer})
        subscriber()

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            start_time=start_time,
            end_time=hg.utc_now() + timedelta(seconds=0.25),
        )

    assert messages == [b"history-1", b"history-2", b"live"]
    assert recovered_values == [False, True]
    assert set(consumer.offset_requests.values()) == {start_ms}
    assert [offset for _, offset in consumer.seeked] == [10, 11]
    assert consumer.close_count == 1


def test_replay_aware_publisher_republishes_history_and_closes_history_consumer():
    start_time = hg.utc_now() - timedelta(milliseconds=50)
    start_ms = int(start_time.replace(tzinfo=UTC).timestamp() * 1000)
    consumer = _Consumer([
        {0: [_record(b"history", start_ms + 10, offset=1)]},
        {},
    ], partitions=(0,))
    producer = MagicMock()

    @message_publisher(topic="test")
    def publisher(msg: hg.TS[bytes], recovered: hg.TS[bool]) -> hg.TS[bytes]:
        return msg

    @hg.graph
    def app():
        register_kafka_adaptor({
            "consumer_factory": lambda **opts: consumer,
            "producer": producer,
        })
        publisher()

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            start_time=start_time,
            end_time=hg.utc_now() + timedelta(seconds=0.1),
        )

    producer.send.assert_called_once_with("test", b"history")
    assert consumer.close_count == 1


def test_replay_parameters_are_required_together_and_typed():
    def missing_recovered(msg: hg.TS[bytes]) -> hg.TS[bytes]:
        return msg

    try:
        message_publisher(missing_recovered, topic="test")
    except TypeError as error:
        assert "both msg and recovered" in str(error)
    else:
        raise AssertionError("missing recovered input was accepted")

    def wrong_recovered(msg: hg.TS[bytes], recovered: hg.TS[int]) -> hg.TS[bytes]:
        return msg

    try:
        message_subscriber(wrong_recovered, topic="test")
    except TypeError as error:
        assert "recovered input must be TS[bool]" in str(error)
    else:
        raise AssertionError("wrong recovered type was accepted")


def test_subscriber_fails_clearly_when_topic_has_no_partitions():
    consumer = _Consumer([], partitions=())

    @message_subscriber(topic="missing")
    def subscriber(msg: hg.TS[bytes]):
        pass

    @hg.graph
    def app():
        register_kafka_adaptor({"consumer_factory": lambda **opts: consumer})
        subscriber()

    with hg.GlobalContext(hg.GlobalState()):
        try:
            hg.run_graph(
                app,
                run_mode=hg.EvaluationMode.REAL_TIME,
                end_time=hg.utc_now() + timedelta(seconds=0.05),
            )
        except RuntimeError as error:
            assert "No partitions found for topic 'missing'" in str(error)
        else:
            raise AssertionError("missing topic partitions were accepted")
    assert consumer.close_count == 1
