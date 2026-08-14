from __future__ import annotations

from datetime import UTC, datetime, timedelta
import threading
import time

import hgraph as hg
import pytest

import hgraph_kafka as kafka
import hgraph_kafka.compat as kafka_compat
from hgraph_kafka.compat import (
    KafkaMessage,
    message_publisher,
    message_subscriber,
    register_kafka_adaptor,
)
from hgraph_kafka.testing import MockCluster, MockConsumeError, MockProduceError


def _use_mock_cluster_history_start(monkeypatch) -> None:
    """Bypass ListOffsets-by-timestamp, which librdkafka's mock lacks."""

    monkeypatch.setattr(
        kafka_compat.KafkaStartPosition,
        "graph_start_time",
        staticmethod(lambda fallback=None: kafka.KafkaStartPosition.earliest()),
    )


def test_python_authoring_uses_native_record_time_simulation() -> None:
    cluster = MockCluster()
    cluster.create_topic("python-simulation")
    graph_start = datetime(2026, 8, 7, 12, 0)
    record_time = graph_start + timedelta(seconds=1)
    cluster.seed_record(
        "python-simulation",
        b"payload",
        key=b"",
        headers=(("duplicate", b"one"), ("duplicate", None)),
        timestamp_ms=int(record_time.replace(tzinfo=UTC).timestamp() * 1000),
    )
    records = []
    evaluation_times = []

    @hg.sink_node
    def capture(
        record: hg.TS[kafka.KafkaRecord],
        _clock: hg.CLOCK = None,
    ):
        records.append(record.value)
        evaluation_times.append(_clock.evaluation_time)

    @hg.graph
    def app():
        kafka.register_kafka_service(
            kafka.KafkaServiceConfig.from_bootstrap_servers(
                [cluster.bootstrap_servers()], client_id="python-simulation"
            ),
            path="simulation",
        )
        key = kafka.KafkaSubscriptionKey(
            topics=("python-simulation",),
            group_id="python-simulation",
            assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
            start_position=kafka.KafkaStartPosition.earliest(),
            stop_position=kafka.KafkaStopPosition.snapshot(),
            recovery_clock=kafka.KafkaRecoveryClock.RECORD_TIMESTAMP,
            merge_policy=kafka.KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET,
            sharing_identity="python-simulation",
        )
        subscription = kafka.kafka_subscribe(
            hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
            path="simulation",
        )
        capture(subscription["record"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.SIMULATION,
        start_time=graph_start,
        end_time=record_time + timedelta(seconds=1),
    )

    assert len(records) == 1
    assert records[0].value == b"payload"
    assert records[0].key == b""
    assert tuple((item.name, item.value) for item in records[0].headers) == (
        ("duplicate", b"one"),
        ("duplicate", None),
    )
    assert evaluation_times == [record_time]


def test_multiple_python_replays_follow_record_timestamps() -> None:
    cluster = MockCluster()
    cluster.create_topic("python-replay-a")
    cluster.create_topic("python-replay-b")
    graph_start = datetime(2026, 8, 7, 12, 30)
    first_time = graph_start + timedelta(seconds=1)
    second_time = graph_start + timedelta(seconds=2)
    expected = {
        b"a-first": first_time,
        b"a-second": second_time,
        b"b-first": first_time,
        b"b-second": second_time,
    }
    for topic, prefix in (
        ("python-replay-a", b"a"),
        ("python-replay-b", b"b"),
    ):
        cluster.seed_record(
            topic,
            prefix + b"-first",
            timestamp_ms=int(first_time.replace(tzinfo=UTC).timestamp() * 1000),
        )
        cluster.seed_record(
            topic,
            prefix + b"-second",
            timestamp_ms=int(second_time.replace(tzinfo=UTC).timestamp() * 1000),
        )

    observed = []

    @hg.sink_node
    def capture(
        record: hg.TS[kafka.KafkaRecord],
        _clock: hg.CLOCK = None,
    ):
        observed.append((record.value.value, _clock.evaluation_time))

    @hg.graph
    def app():
        kafka.register_kafka_service(
            kafka.KafkaServiceConfig.from_bootstrap_servers(
                [cluster.bootstrap_servers()], client_id="python-multi-replay"
            ),
            path="simulation",
        )
        for topic in ("python-replay-a", "python-replay-b"):
            key = kafka.KafkaSubscriptionKey(
                topics=(topic,),
                group_id=topic,
                assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
                start_position=kafka.KafkaStartPosition.earliest(),
                stop_position=kafka.KafkaStopPosition.snapshot(),
                recovery_clock=kafka.KafkaRecoveryClock.RECORD_TIMESTAMP,
                merge_policy=(
                    kafka.KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET
                ),
                sharing_identity=topic,
            )
            subscription = kafka.kafka_subscribe(
                hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
                path="simulation",
            )
            capture(subscription["record"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.SIMULATION,
        start_time=graph_start,
        end_time=second_time + timedelta(seconds=1),
    )

    assert dict(observed) == expected


def test_python_record_time_recovery_hands_off_before_live_records() -> None:
    cluster = MockCluster()
    cluster.create_topic("python-record-time-live")
    history_time = hg.utc_now().replace(microsecond=0) + timedelta(seconds=3)
    cluster.seed_record(
        "python-record-time-live",
        b"history",
        timestamp_ms=int(history_time.replace(tzinfo=UTC).timestamp() * 1000),
    )
    recovery_started = threading.Event()
    live_seeded = threading.Event()
    observed = []

    def seed_live() -> None:
        if recovery_started.wait(5.0):
            cluster.seed_record("python-record-time-live", b"live")
            live_seeded.set()

    @hg.sink_node
    def capture_state(state: hg.TS[kafka.KafkaSubscriptionState]):
        if state.value == kafka.KafkaSubscriptionState.RECOVERING:
            recovery_started.set()

    @hg.sink_node
    def capture_record(
        record: hg.TS[kafka.KafkaRecord],
        _clock: hg.CLOCK = None,
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append((record.value.value, _clock.evaluation_time))
        if len(observed) == 2:
            _api.request_engine_stop()

    @hg.graph
    def app():
        kafka.register_kafka_service(
            kafka.KafkaServiceConfig.from_bootstrap_servers(
                [cluster.bootstrap_servers()],
                client_id="python-record-time-live",
            ),
            path="record-time-live",
        )
        key = kafka.KafkaSubscriptionKey(
            topics=("python-record-time-live",),
            group_id="python-record-time-live",
            assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
            start_position=kafka.KafkaStartPosition.earliest(),
            stop_position=kafka.KafkaStopPosition.unbounded(),
            commit_mode=kafka.KafkaCommitMode.NONE,
            recovery_clock=kafka.KafkaRecoveryClock.RECORD_TIMESTAMP,
            merge_policy=kafka.KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET,
            sharing_identity="python-record-time-live",
        )
        subscription = kafka.kafka_subscribe(
            hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
            path="record-time-live",
        )
        capture_state(subscription["state"])
        capture_record(subscription["record"])

    producer = threading.Thread(target=seed_live)
    producer.start()
    try:
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=history_time + timedelta(seconds=5),
        )
    finally:
        producer.join(timeout=6.0)

    assert not producer.is_alive()
    assert live_seeded.is_set()
    assert [value for value, _ in observed] == [b"history", b"live"]
    assert observed[0][1] == history_time
    assert observed[1][1] > observed[0][1]


def test_legacy_replay_surface_uses_the_same_native_history_session(monkeypatch) -> None:
    _use_mock_cluster_history_start(monkeypatch)
    cluster = MockCluster()
    cluster.create_topic("legacy-replay")
    graph_start = datetime(2026, 8, 7, 13, 0)
    record_time = graph_start + timedelta(seconds=1)
    cluster.seed_record(
        "legacy-replay",
        b"legacy-payload",
        key=b"legacy-key",
        headers=(("content-type", b"application/json"), ("trace", b"7")),
        timestamp_ms=int(record_time.replace(tzinfo=UTC).timestamp() * 1000),
    )
    messages = []
    recovery = []

    @hg.sink_node
    def capture_message(msg: hg.TS[KafkaMessage]):
        if msg.modified:
            messages.append(msg.value)

    @hg.sink_node
    def capture_recovery(recovered: hg.TS[bool]):
        if recovered.modified:
            recovery.append(recovered.value)

    @message_subscriber(topic="legacy-replay")
    def subscriber(msg: hg.TS[KafkaMessage], recovered: hg.TS[bool]):
        capture_message(msg)
        capture_recovery(recovered)

    @hg.graph
    def app():
        register_kafka_adaptor(
            {
                "bootstrap_servers": cluster.bootstrap_servers(),
                "client_id": "legacy-replay",
            }
        )
        subscriber()

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.SIMULATION,
        start_time=graph_start,
        end_time=record_time + timedelta(seconds=1),
    )

    assert messages == [
        KafkaMessage(
            payload=b"legacy-payload",
            key=b"legacy-key",
            content_type="application/json",
            headers={"trace": b"7"},
        )
    ]
    assert recovery == [False, True]


def test_legacy_publisher_uses_native_producer_and_header_projection() -> None:
    cluster = MockCluster()
    cluster.create_topic("legacy-publish")
    observed = []

    @message_publisher(topic="legacy-publish")
    def publisher() -> hg.TS[KafkaMessage]:
        return hg.const(
            KafkaMessage(
                payload=b"published",
                key=b"key",
                content_type="application/json",
                headers={"trace": b"9"},
            ),
            tp=hg.TS[KafkaMessage],
        )

    @hg.sink_node
    def capture(
        record: hg.TS[kafka.KafkaRecord],
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append(record.value)
        _api.request_engine_stop()

    @hg.graph
    def app():
        register_kafka_adaptor(
            {
                "bootstrap_servers": cluster.bootstrap_servers(),
                "client_id": "legacy-publish",
            }
        )
        publisher()
        key = kafka.KafkaSubscriptionKey(
            topics=("legacy-publish",),
            group_id="legacy-publish-observer",
            assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
            start_position=kafka.KafkaStartPosition.earliest(),
            stop_position=kafka.KafkaStopPosition.unbounded(),
            commit_mode=kafka.KafkaCommitMode.NONE,
            sharing_identity="legacy-publish-observer",
        )
        subscription = kafka.kafka_subscribe(
            hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
            path="legacy",
        )
        capture(subscription["record"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=5),
    )

    assert len(observed) == 1
    assert observed[0].value == b"published"
    assert observed[0].key == b"key"
    assert tuple((item.name, item.value) for item in observed[0].headers) == (
        ("trace", b"9"),
        ("content-type", b"application/json"),
    )


@pytest.mark.parametrize(
    ("injected", "expected_status", "expected_retriable"),
    [
        (
            MockProduceError.RETRIABLE,
            kafka.KafkaDeliveryStatus.RETRIABLE_FAILURE,
            True,
        ),
        (
            MockProduceError.PERMANENT,
            kafka.KafkaDeliveryStatus.PERMANENT_FAILURE,
            False,
        ),
    ],
)
def test_python_delivery_failures_are_typed(
    injected, expected_status, expected_retriable
) -> None:
    cluster = MockCluster()
    cluster.create_topic("python-delivery-failure")
    cluster.fail_next_produce(injected)
    observed = []

    @hg.sink_node
    def capture(
        report: hg.TS[kafka.KafkaDeliveryReport],
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append(report.value)
        _api.request_engine_stop()

    @hg.graph
    def app():
        config = kafka.KafkaServiceConfig(
            kafka.KafkaConnectionConfig(
                (cluster.bootstrap_servers(),), "python-delivery-failure"
            ),
            producer=kafka.KafkaProducerOptions(
                idempotent=False,
                retries=0,
                options=(kafka.KafkaOption("message.timeout.ms", "1000"),),
            ),
        )
        kafka.register_kafka_service(config, path="python-delivery-failure")
        record = hg.const(
            kafka.KafkaProduceRecord(b"failure", user_token="python-failure"),
            tp=hg.TS[kafka.KafkaProduceRecord],
        )
        capture(
            kafka.kafka_publish(
                "python-delivery-failure",
                record,
                path="python-delivery-failure",
            )
        )

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=5),
    )

    assert len(observed) == 1
    assert observed[0].status == expected_status
    assert observed[0].retriable is expected_retriable
    assert observed[0].error_code != 0


def test_legacy_live_only_subscriber_does_not_receive_history() -> None:
    cluster = MockCluster()
    cluster.create_topic("legacy-live-only")
    cluster.seed_record("legacy-live-only", b"history")
    observed = []

    @hg.sink_node
    def capture(
        msg: hg.TS[bytes],
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append(msg.value)
        _api.request_engine_stop()

    @message_subscriber(topic="legacy-live-only")
    def subscriber(msg: hg.TS[bytes]):
        capture(msg)

    @hg.graph
    def app():
        register_kafka_adaptor(
            {
                "bootstrap_servers": cluster.bootstrap_servers(),
                "client_id": "legacy-live-only",
            }
        )
        subscriber()

    def publish_live():
        time.sleep(0.5)
        cluster.seed_record("legacy-live-only", b"live")

    publisher = threading.Thread(target=publish_live)
    publisher.start()
    try:
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=hg.utc_now() + timedelta(seconds=5),
        )
    finally:
        publisher.join()

    assert observed == [b"live"]


def test_legacy_permanent_consumer_failure_stops_the_graph() -> None:
    cluster = MockCluster()
    cluster.create_topic("legacy-consumer-failure")
    cluster.fail_next_fetch(MockConsumeError.PERMANENT, 10)

    @message_subscriber(topic="legacy-consumer-failure")
    def subscriber(msg: hg.TS[bytes]):
        del msg

    @hg.graph
    def app():
        register_kafka_adaptor(
            {
                "bootstrap_servers": cluster.bootstrap_servers(),
                "client_id": "legacy-consumer-failure",
            }
        )
        subscriber()
        kafka.kafka_events(path="legacy")

    started = time.monotonic()
    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=5),
    )

    assert time.monotonic() - started < 2.0


def test_legacy_replaying_publisher_is_bounded_to_its_start_snapshot(monkeypatch) -> None:
    _use_mock_cluster_history_start(monkeypatch)
    cluster = MockCluster()
    cluster.create_topic("legacy-republish")
    now = hg.utc_now()
    cluster.seed_record(
        "legacy-republish",
        b"history",
        timestamp_ms=int((now + timedelta(milliseconds=100)).replace(tzinfo=UTC).timestamp() * 1000),
    )
    observed = []

    @message_publisher(topic="legacy-republish")
    def republish(msg: hg.TS[bytes], recovered: hg.TS[bool]) -> hg.TS[bytes]:
        del recovered
        return msg

    @hg.sink_node
    def capture(
        record: hg.TS[kafka.KafkaRecord],
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append(record.value.value)
        if len(observed) == 2:
            _api.request_engine_stop()

    @hg.graph
    def app():
        register_kafka_adaptor(
            {
                "bootstrap_servers": cluster.bootstrap_servers(),
                "client_id": "legacy-republish",
            }
        )
        republish()
        key = kafka.KafkaSubscriptionKey(
            topics=("legacy-republish",),
            group_id="legacy-republish-observer",
            assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
            start_position=kafka.KafkaStartPosition.earliest(),
            stop_position=kafka.KafkaStopPosition.unbounded(),
            commit_mode=kafka.KafkaCommitMode.NONE,
            sharing_identity="legacy-republish-observer",
        )
        subscription = kafka.kafka_subscribe(
            hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
            path="legacy",
        )
        capture(subscription["record"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        start_time=now,
        end_time=now + timedelta(seconds=5),
    )

    assert observed == [b"history", b"history"]


def test_simulation_rejects_publish_work() -> None:
    cluster = MockCluster()
    cluster.create_topic("simulation-publish")

    @hg.graph
    def app():
        kafka.register_kafka_service(
            kafka.KafkaServiceConfig.from_bootstrap_servers(
                [cluster.bootstrap_servers()], client_id="simulation-publish"
            ),
            path="simulation-publish",
        )
        record = hg.const(
            kafka.KafkaProduceRecord(b"payload"),
            tp=hg.TS[kafka.KafkaProduceRecord],
        )
        kafka.kafka_publish("simulation-publish", record, path="simulation-publish")

    with pytest.raises(Exception, match="publishing is not supported"):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.SIMULATION,
            start_time=datetime(2026, 8, 7, 14, 0),
            end_time=datetime(2026, 8, 7, 14, 0, 1),
        )


def test_simulation_rejects_commit_work() -> None:
    cluster = MockCluster()

    @hg.graph
    def app():
        kafka.register_kafka_service(
            kafka.KafkaServiceConfig.from_bootstrap_servers(
                [cluster.bootstrap_servers()], client_id="simulation-commit"
            ),
            path="simulation-commit",
        )
        cursor = hg.const(
            kafka.KafkaCursor("simulation-commit", 1, "topic", 0, 1),
            tp=hg.TS[kafka.KafkaCursor],
        )
        kafka.kafka_commit(cursor, path="simulation-commit")

    with pytest.raises(Exception, match="commits are not supported"):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.SIMULATION,
            start_time=datetime(2026, 8, 7, 14, 0),
            end_time=datetime(2026, 8, 7, 14, 0, 1),
        )


@pytest.mark.parametrize(
    ("stop_position", "merge_policy", "error"),
    [
        (
            kafka.KafkaStopPosition.unbounded(),
            kafka.KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET,
            "bounded stop position",
        ),
        (
            kafka.KafkaStopPosition.snapshot(),
            kafka.KafkaMergePolicy.PARTITION,
            "TimestampTopicPartitionOffset recovery ordering",
        ),
    ],
)
def test_simulation_rejects_asynchronous_or_nondeterministic_subscriptions(
    stop_position, merge_policy, error
) -> None:
    cluster = MockCluster()

    @hg.graph
    def app():
        kafka.register_kafka_service(
            kafka.KafkaServiceConfig.from_bootstrap_servers(
                [cluster.bootstrap_servers()], client_id="simulation-contract"
            ),
            path="simulation-contract",
        )
        key = kafka.KafkaSubscriptionKey(
            topics=("simulation-contract",),
            group_id="simulation-contract",
            assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
            start_position=kafka.KafkaStartPosition.earliest(),
            stop_position=stop_position,
            recovery_clock=kafka.KafkaRecoveryClock.RECORD_TIMESTAMP,
            merge_policy=merge_policy,
            sharing_identity="simulation-contract",
        )
        kafka.kafka_subscribe(
            hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]),
            path="simulation-contract",
        )

    with pytest.raises(Exception, match=error):
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.SIMULATION,
            start_time=datetime(2026, 8, 7, 14, 0),
            end_time=datetime(2026, 8, 7, 14, 0, 1),
        )
