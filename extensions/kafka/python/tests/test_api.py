from __future__ import annotations

from datetime import datetime

import _hgraph
import hgraph as hg
import pytest
from hgraph.test import use_wiring

import hgraph_kafka as kafka


def test_time_series_collections_use_the_native_named_tsb_schemas() -> None:
    assert issubclass(kafka.KafkaSubscriptionOutput, hg.TimeSeriesSchema)
    assert issubclass(kafka.KafkaPublishRequest, hg.TimeSeriesSchema)
    assert kafka.KafkaSubscriptionOutput.__time_series_namespace__ == "hgraph.kafka"
    assert kafka.KafkaPublishRequest.__time_series_namespace__ == "hgraph.kafka"

    assert hg.TSB[kafka.KafkaSubscriptionOutput].handle == _hgraph.tsb(
        "hgraph.kafka::KafkaSubscriptionOutput",
        [
            ("record", hg.TS[hg.Shared[kafka.KafkaRecord]].handle),
            ("cursor", hg.TS[kafka.KafkaCursor].handle),
            ("state", hg.TS[kafka.KafkaSubscriptionState].handle),
        ],
    )
    assert hg.TSB[kafka.KafkaPublishRequest].handle == _hgraph.tsb(
        "hgraph.kafka::KafkaPublishRequest",
        [
            ("topic", hg.TS[str].handle),
            ("record", hg.TS[kafka.KafkaProduceRecord].handle),
        ],
    )


def test_compound_scalar_api_preserves_nullable_and_ordered_values() -> None:
    record = kafka.KafkaProduceRecord(
        value=None,
        key=b"",
        headers=(
            kafka.KafkaHeader("trace", None),
            kafka.KafkaHeader("trace", b""),
            kafka.KafkaHeader("trace", b"2"),
        ),
        timestamp=datetime(2026, 8, 7, 12, 30),
        partition=0,
        user_token="publish-7",
    )

    assert record.value is None
    assert record.key == b""
    assert tuple(header.name for header in record.headers) == (
        "trace",
        "trace",
        "trace",
    )
    assert tuple(header.value for header in record.headers) == (None, b"", b"2")


@pytest.mark.parametrize(
    "factory",
    [
        lambda: kafka.KafkaHeader(""),
        lambda: kafka.KafkaOption("", "value"),
        lambda: kafka.KafkaTopicPartition("", 0),
        lambda: kafka.KafkaTopicPartition("orders", -1),
        lambda: kafka.KafkaPartitionOffset("orders", 0, -1),
        lambda: kafka.KafkaStartPosition(kafka.KafkaStartPositionKind.TIMESTAMP),
        lambda: kafka.KafkaStartPosition(
            kafka.KafkaStartPositionKind.EARLIEST,
            timestamp=datetime(2026, 8, 7),
        ),
        lambda: kafka.KafkaStopPosition(kafka.KafkaStopPositionKind.OFFSETS),
        lambda: kafka.KafkaSubscriptionKey(group_id="risk"),
        lambda: kafka.KafkaSubscriptionKey(
            group_id="risk", topics=("orders",), topic_pattern="orders-.*"
        ),
        lambda: kafka.KafkaSubscriptionKey(
            group_id="risk",
            topic_pattern="orders-.*",
            assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
        ),
        lambda: kafka.KafkaProduceRecord(b"value", partition=-1),
        lambda: kafka.KafkaRecord(
            "", 0, 0, None, kafka.KafkaTimestampType.NOT_AVAILABLE, None, None
        ),
        lambda: kafka.KafkaCursor("", 0, "", -1, -1),
        lambda: kafka.KafkaConsumerDefaults(ingress_record_limit=0),
        lambda: kafka.KafkaConsumerDefaults(
            inbound_overflow=kafka.KafkaOverflowAction.STAGE
        ),
        lambda: kafka.KafkaProducerOptions(
            stage_overflow=kafka.KafkaOverflowAction.STAGE
        ),
        lambda: kafka.KafkaProducerOptions(acknowledgements="1"),
        lambda: kafka.KafkaProducerOptions(retries=-1),
    ],
)
def test_invalid_public_values_are_rejected_at_construction(factory) -> None:
    with pytest.raises(ValueError):
        factory()


def test_graph_lifetime_stop_policy_is_exposed_to_python() -> None:
    stop = kafka.KafkaStopPosition.graph_lifetime()

    assert stop.kind == kafka.KafkaStopPositionKind.GRAPH_LIFETIME
    assert stop.timestamp is None
    assert stop.offsets == ()


def test_all_service_interfaces_and_topic_forms_share_one_binding() -> None:
    wiring = _hgraph.Wiring()
    config = kafka.KafkaServiceConfig.from_bootstrap_servers(
        ["localhost:9092"], client_id="python-wiring"
    )
    key_value = kafka.KafkaSubscriptionKey(
        topics=("orders",),
        group_id="risk",
        start_position=kafka.KafkaStartPosition.earliest(),
        stop_position=kafka.KafkaStopPosition.snapshot(),
        commit_mode=kafka.KafkaCommitMode.EXPLICIT,
        sharing_identity="orders-risk",
    )
    cursor_value = kafka.KafkaCursor("orders-risk", 1, "orders", 0, 1)
    record_value = kafka.KafkaProduceRecord(b"payload", user_token="python-1")

    with use_wiring(wiring):
        kafka.register_kafka_service(config, path="primary")
        key = hg.const(key_value, tp=hg.TS[kafka.KafkaSubscriptionKey])
        subscription = kafka.kafka_subscribe(key, path="primary")

        record = hg.const(record_value, tp=hg.TS[kafka.KafkaProduceRecord])
        static_delivery = kafka.kafka_publish("orders", record, path="primary")
        dynamic_topic = hg.const("orders", tp=hg.TS[str])
        dynamic_delivery = kafka.kafka_publish(
            dynamic_topic, record, path="primary"
        )

        cursor = hg.const(cursor_value, tp=hg.TS[kafka.KafkaCursor])
        kafka.kafka_commit(cursor, path="primary")
        event = kafka.kafka_events(path="primary")

    assert subscription is not None
    assert static_delivery is not None
    assert dynamic_delivery is not None
    assert event is not None
    wiring.build_services()


def test_subscription_selector_kind_cannot_contradict_selector() -> None:
    with pytest.raises(ValueError, match="selector_kind contradicts"):
        kafka.KafkaSubscriptionKey(
            group_id="risk",
            topics=("orders",),
            selector_kind=kafka.KafkaSelectorKind.PATTERN,
        )
