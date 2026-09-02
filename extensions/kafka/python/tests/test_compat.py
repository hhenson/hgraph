from __future__ import annotations

import importlib
import inspect
import pickle

import _hgraph
import hgraph as hg
import pytest
from frozendict import frozendict
from hgraph.test import use_wiring

from hgraph_kafka.compat import (
    KafkaMessage,
    MessageState,
    get_message_state,
    message_publisher,
    message_subscriber,
    register_kafka_adaptor,
)
from hgraph_kafka.compat import _legacy_config, _subscription_key


def test_released_surface_and_signatures_are_preserved() -> None:
    legacy = importlib.import_module("hgraph.adaptors.kafka")
    assert legacy.KafkaMessage is KafkaMessage
    assert legacy.MessageState is MessageState
    assert KafkaMessage.__module__ == "hgraph.adaptors.kafka._api"
    assert KafkaMessage.__compound_namespace__ == "hgraph.adaptors.kafka._api"

    assert str(inspect.signature(MessageState.add_subscriber)) == (
        "(self, topic: 'str', replay: 'bool' = False)"
    )
    assert str(inspect.signature(MessageState.add_historical_subscriber)) == (
        "(self, topic: 'str')"
    )
    assert tuple(inspect.signature(KafkaMessage.from_dict).parameters) == ("d",)

    @message_publisher(topic="published")
    def publisher(
        msg: hg.TS[bytes],
        recovered: hg.TS[bool],
        scale: hg.TS[int] = None,
    ) -> hg.TS[bytes]:
        return msg

    @message_subscriber
    def subscriber(
        msg: hg.TS[bytes],
        recovered: hg.TS[bool],
        scale: hg.TS[int] = None,
    ) -> hg.TS[bytes]:
        return msg

    publisher_signature = inspect.signature(publisher)
    assert tuple(publisher_signature.parameters) == ("scale", "topic")
    assert publisher_signature.parameters["topic"].default == "published"
    assert publisher_signature.return_annotation is None

    subscriber_signature = inspect.signature(subscriber)
    assert tuple(subscriber_signature.parameters) == ("scale", "topic")
    assert subscriber_signature.parameters["topic"].default is inspect.Parameter.empty
    assert subscriber_signature.return_annotation == hg.TS[bytes]


def test_released_message_dictionary_projection_is_preserved() -> None:
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
    assert KafkaMessage.from_dict({**message.to_dict(), "ignored": True}) == message
    assert pickle.loads(pickle.dumps(message)) == message


def test_legacy_decorators_lower_to_one_native_service() -> None:
    @message_publisher(topic="orders-out")
    def publisher(value: hg.TS[bytes]) -> hg.TS[bytes]:
        return value

    @message_subscriber(topic="orders-live")
    def live_subscriber(msg: hg.TS[KafkaMessage]):
        del msg

    @message_subscriber(topic="orders-history")
    def replay_subscriber(msg: hg.TS[bytes], recovered: hg.TS[bool]):
        del msg, recovered

    wiring = _hgraph.Wiring()
    with hg.GlobalContext(hg.GlobalState()), use_wiring(wiring):
        register_kafka_adaptor(
            {"bootstrap_servers": ["localhost:9092"], "client_id": "legacy-test"}
        )
        publisher(value=hg.const(b"payload", tp=hg.TS[bytes]))
        live_subscriber()
        replay_subscriber()
    wiring.build_services()


def test_legacy_one_publisher_per_topic_error_is_preserved() -> None:
    @message_publisher(topic="orders")
    def first() -> hg.TS[bytes]:
        return hg.const(b"first", tp=hg.TS[bytes])

    @message_publisher(topic="orders")
    def second() -> hg.TS[bytes]:
        return hg.const(b"second", tp=hg.TS[bytes])

    wiring = _hgraph.Wiring()
    with hg.GlobalContext(hg.GlobalState()), use_wiring(wiring):
        register_kafka_adaptor({"bootstrap_servers": "localhost:9092"})
        first()
        with pytest.raises(ValueError, match="already has a publisher"):
            second()


def test_production_compatibility_config_rejects_injected_objects() -> None:
    wiring = _hgraph.Wiring()
    with hg.GlobalContext(hg.GlobalState()), use_wiring(wiring):
        with pytest.raises(ValueError, match="cannot inject native objects"):
            register_kafka_adaptor(
                {"bootstrap_servers": "localhost:9092", "producer": object()}
            )


def test_replay_uses_graph_start_with_released_end_offset_fallback() -> None:
    wiring = _hgraph.Wiring()
    with hg.GlobalContext(hg.GlobalState()), use_wiring(wiring):
        register_kafka_adaptor({"bootstrap_servers": "localhost:9092"})
        key = _subscription_key("orders", history=True, continue_live=True)

    assert key.start_position.kind.name == "GRAPH_START_TIME"
    assert key.start_position.fallback.name == "LATEST"


def test_released_subscription_uses_native_graph_lifetime_policy() -> None:
    wiring = _hgraph.Wiring()
    with hg.GlobalContext(hg.GlobalState()), use_wiring(wiring):
        register_kafka_adaptor({"bootstrap_servers": "localhost:9092"})
        key = _subscription_key("orders", history=True, continue_live=True)

    assert key.stop_position.kind.name == "GRAPH_LIFETIME"


def test_released_compatibility_state_is_owned_by_each_wiring() -> None:
    first = _hgraph.Wiring()
    second = _hgraph.Wiring()

    with hg.GlobalContext(hg.GlobalState()), use_wiring(first):
        register_kafka_adaptor(
            {"bootstrap_servers": "localhost:9092", "client_id": "first"}
        )
        first_state = get_message_state()
        assert get_message_state() is first_state
        first_key = _subscription_key("orders", history=False, continue_live=True)

    with hg.GlobalContext(hg.GlobalState()), use_wiring(second):
        register_kafka_adaptor(
            {"bootstrap_servers": "localhost:9092", "client_id": "second"}
        )
        second_state = get_message_state()
        second_key = _subscription_key("orders", history=False, continue_live=True)

    assert first_state is not second_state
    assert first_key.group_id == "first-legacy-live-orders"
    assert second_key.group_id == "second-legacy-live-orders"


def test_released_producer_batching_defaults_map_to_typed_native_options() -> None:
    config = _legacy_config(
        {
            "bootstrap_servers": "localhost:9092",
            "acks": "all",
            "retries": 7,
            "linger_ms": 25,
        }
    )

    assert config.producer.idempotent is False
    assert config.producer.acknowledgements == "all"
    assert config.producer.retries == 7
    assert config.producer.linger_ms == 25
    assert config.producer.batch_record_limit == 1_000
    assert not hasattr(config.consumer_defaults, "failure_policy")


def _consume(headers, payload=b'{"a": 1}'):
    """Run the ingest projection over one record's headers."""
    from hgraph_kafka import KafkaHeader, KafkaRecord, KafkaTimestampType
    from hgraph_kafka.compat import _to_legacy_message

    record = KafkaRecord(
        topic="t", partition=0, offset=0, timestamp=None,
        timestamp_type=KafkaTimestampType.NOT_AVAILABLE,
        key=None, value=payload,
        headers=tuple(KafkaHeader(name, value) for name, value in headers),
    )
    return hg.eval_node(_to_legacy_message, [record])[-1]


def _produce(message):
    """Run the publish projection over one message."""
    from hgraph_kafka.compat import _message_to_record

    return hg.eval_node(_message_to_record, [message])[-1]


def test_content_type_is_recognised_whatever_its_casing() -> None:
    """Kafka header names are arbitrary byte strings, and producers outside
    hgraph commonly send ``Content-Type``. Matching exact-case left the value
    in ``headers`` where no consumer looks for it."""
    for name in ("content-type", "Content-Type", "CONTENT-TYPE"):
        message = _consume([(name, b"application/json"), ("trace", b"7")])
        assert message.content_type == "application/json", name
        # ...and it is NOT also left in the generic header bag.
        assert dict(message.headers) == {"trace": b"7"}, name


def test_a_json_payload_without_a_content_type_header_is_untouched() -> None:
    """Nothing in the Kafka path inspects the content type: the payload is
    opaque bytes either way, so a JSON body with no header still arrives whole.
    Decoding is the consumer's business."""
    message = _consume([("trace", b"7")])
    assert message.content_type is None
    assert message.payload == b'{"a": 1}'


def test_content_type_round_trips_under_its_canonical_name() -> None:
    """Consume then re-produce: the header comes back once, lower-cased."""
    record = _produce(_consume([("Content-Type", b"application/json")]))
    assert [(h.name, h.value) for h in record.headers] == [
        ("content-type", b"application/json")
    ]


def test_a_content_type_header_and_field_do_not_both_publish() -> None:
    """The field is the canonical carrier. Emitting both would put two
    content-type headers on the record, with values free to disagree."""
    message = KafkaMessage(
        payload=b"{}",
        content_type="application/json",
        headers=frozendict({"Content-Type": b"text/csv", "trace": b"7"}),
    )
    record = _produce(message)
    names = [h.name for h in record.headers]
    assert names.count("content-type") == 1
    assert "Content-Type" not in names
    assert dict((h.name, h.value) for h in record.headers) == {
        "trace": b"7",
        "content-type": b"application/json",
    }
