"""C++-first Kafka services for hgraph."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from hgraph import (
    CompoundScalar,
    TS,
    TSB,
    TimeSeriesSchema,
    operator_function,
    reference_service,
    request_reply_service,
    subscription_service,
)

from ._hgraph_kafka import (
    KafkaCommitMode,
    KafkaAssignmentMode,
    KafkaDeliveryStatus,
    KafkaMergePolicy,
    KafkaOffsetFallback,
    KafkaOverflowAction,
    KafkaFailurePolicy,
    KafkaRecoveryClock,
    KafkaSelectorKind,
    KafkaSeverity,
    KafkaStartPositionKind,
    KafkaStopPositionKind,
    KafkaSubscriptionState,
    KafkaTimestampType,
)


_NAMESPACE = "hgraph.kafka"


@dataclass(frozen=True)
class KafkaHeader(CompoundScalar, namespace=_NAMESPACE):
    name: str
    value: Optional[bytes] = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Kafka header names cannot be empty")


@dataclass(frozen=True)
class KafkaOption(CompoundScalar, namespace=_NAMESPACE):
    name: str
    value: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Kafka option names cannot be empty")


@dataclass(frozen=True)
class KafkaTopicPartition(CompoundScalar, namespace=_NAMESPACE):
    topic: str
    partition: int

    def __post_init__(self) -> None:
        if not self.topic or self.partition < 0:
            raise ValueError("Kafka topic partitions require a topic and non-negative partition")


@dataclass(frozen=True)
class KafkaPartitionOffset(CompoundScalar, namespace=_NAMESPACE):
    topic: str
    partition: int
    offset: int

    def __post_init__(self) -> None:
        if not self.topic or self.partition < 0 or self.offset < 0:
            raise ValueError("Kafka partition offsets must be non-negative and name a topic")


@dataclass(frozen=True)
class KafkaStartPosition(CompoundScalar, namespace=_NAMESPACE):
    kind: KafkaStartPositionKind
    fallback: KafkaOffsetFallback = KafkaOffsetFallback.EARLIEST
    timestamp: Optional[datetime] = None
    offsets: tuple[KafkaPartitionOffset, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == KafkaStartPositionKind.TIMESTAMP and self.timestamp is None:
            raise ValueError("Kafka timestamp start requires a timestamp")
        if self.kind == KafkaStartPositionKind.OFFSETS and not self.offsets:
            raise ValueError("Kafka offset start requires partition offsets")
        if self.kind != KafkaStartPositionKind.TIMESTAMP and self.timestamp is not None:
            raise ValueError("Kafka start timestamp is only valid for Timestamp")
        if self.kind != KafkaStartPositionKind.OFFSETS and self.offsets:
            raise ValueError("Kafka start offsets are only valid for Offsets")

    @staticmethod
    def earliest() -> "KafkaStartPosition":
        return KafkaStartPosition(KafkaStartPositionKind.EARLIEST)

    @staticmethod
    def latest() -> "KafkaStartPosition":
        return KafkaStartPosition(KafkaStartPositionKind.LATEST)

    @staticmethod
    def committed(
        fallback: KafkaOffsetFallback = KafkaOffsetFallback.EARLIEST,
    ) -> "KafkaStartPosition":
        return KafkaStartPosition(KafkaStartPositionKind.COMMITTED, fallback)

    @staticmethod
    def at_timestamp(
        timestamp: datetime,
        fallback: KafkaOffsetFallback = KafkaOffsetFallback.EARLIEST,
    ) -> "KafkaStartPosition":
        return KafkaStartPosition(
            KafkaStartPositionKind.TIMESTAMP, fallback, timestamp=timestamp
        )

    @staticmethod
    def at_offsets(
        offsets: tuple[KafkaPartitionOffset, ...],
    ) -> "KafkaStartPosition":
        if not offsets:
            raise ValueError("Kafka offset start requires partition offsets")
        return KafkaStartPosition(
            KafkaStartPositionKind.OFFSETS,
            KafkaOffsetFallback.FAIL,
            offsets=tuple(offsets),
        )

    @staticmethod
    def graph_start_time(
        fallback: KafkaOffsetFallback = KafkaOffsetFallback.EARLIEST,
    ) -> "KafkaStartPosition":
        return KafkaStartPosition(KafkaStartPositionKind.GRAPH_START_TIME, fallback)


@dataclass(frozen=True)
class KafkaStopPosition(CompoundScalar, namespace=_NAMESPACE):
    kind: KafkaStopPositionKind
    timestamp: Optional[datetime] = None
    offsets: tuple[KafkaPartitionOffset, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == KafkaStopPositionKind.TIMESTAMP and self.timestamp is None:
            raise ValueError("Kafka timestamp stop requires a timestamp")
        if self.kind == KafkaStopPositionKind.OFFSETS and not self.offsets:
            raise ValueError("Kafka offset stop requires partition offsets")
        if self.kind != KafkaStopPositionKind.TIMESTAMP and self.timestamp is not None:
            raise ValueError("Kafka stop timestamp is only valid for Timestamp")
        if self.kind != KafkaStopPositionKind.OFFSETS and self.offsets:
            raise ValueError("Kafka stop offsets are only valid for Offsets")

    @staticmethod
    def unbounded() -> "KafkaStopPosition":
        return KafkaStopPosition(KafkaStopPositionKind.UNBOUNDED)

    @staticmethod
    def snapshot() -> "KafkaStopPosition":
        return KafkaStopPosition(KafkaStopPositionKind.SNAPSHOT)

    @staticmethod
    def at_timestamp(timestamp: datetime) -> "KafkaStopPosition":
        return KafkaStopPosition(KafkaStopPositionKind.TIMESTAMP, timestamp=timestamp)

    @staticmethod
    def at_offsets(
        offsets: tuple[KafkaPartitionOffset, ...],
    ) -> "KafkaStopPosition":
        if not offsets:
            raise ValueError("Kafka offset stop requires partition offsets")
        return KafkaStopPosition(KafkaStopPositionKind.OFFSETS, offsets=tuple(offsets))


@dataclass(frozen=True)
class KafkaConnectionConfig(CompoundScalar, namespace=_NAMESPACE):
    bootstrap_servers: tuple[str, ...]
    client_id: str = ""
    options: tuple[KafkaOption, ...] = ()

    def __post_init__(self) -> None:
        if not self.bootstrap_servers or any(not server for server in self.bootstrap_servers):
            raise ValueError("Kafka service config requires a bootstrap server")


@dataclass(frozen=True)
class KafkaConsumerDefaults(CompoundScalar, namespace=_NAMESPACE):
    ingress_record_limit: int = 10_000
    ingress_byte_limit: int = 64 * 1024 * 1024
    inbound_overflow: KafkaOverflowAction = KafkaOverflowAction.FAIL
    failure_policy: KafkaFailurePolicy = KafkaFailurePolicy.REPORT
    options: tuple[KafkaOption, ...] = ()

    def __post_init__(self) -> None:
        if self.ingress_record_limit <= 0 or self.ingress_byte_limit <= 0:
            raise ValueError("Kafka ingress limits must be positive")
        if self.inbound_overflow == KafkaOverflowAction.STAGE:
            raise ValueError("Kafka inbound overflow cannot use Stage")


@dataclass(frozen=True)
class KafkaProducerOptions(CompoundScalar, namespace=_NAMESPACE):
    idempotent: bool = True
    acknowledgements: str = "all"
    retries: int = 2_147_483_647
    linger_ms: int = 5
    batch_record_limit: int = 1_000
    outbound_record_limit: int = 10_000
    outbound_byte_limit: int = 64 * 1024 * 1024
    overflow: KafkaOverflowAction = KafkaOverflowAction.STAGE
    stage_overflow: KafkaOverflowAction = KafkaOverflowAction.FAIL
    shutdown_drain_timeout_ms: int = 5_000
    failure_policy: KafkaFailurePolicy = KafkaFailurePolicy.REPORT
    options: tuple[KafkaOption, ...] = ()

    def __post_init__(self) -> None:
        if self.outbound_record_limit <= 0 or self.outbound_byte_limit <= 0:
            raise ValueError("Kafka outbound limits must be positive")
        if self.acknowledgements not in ("0", "1", "all", "-1"):
            raise ValueError("Kafka acknowledgements must be 0, 1, all, or -1")
        if self.idempotent and self.acknowledgements not in ("all", "-1"):
            raise ValueError("Kafka idempotence requires all acknowledgements")
        if self.retries < 0 or self.linger_ms < 0 or self.batch_record_limit <= 0:
            raise ValueError("Kafka producer retry/batch settings are out of range")
        if self.stage_overflow == KafkaOverflowAction.STAGE:
            raise ValueError("Kafka stage overflow must be Fail or Drop")
        if self.shutdown_drain_timeout_ms < 0:
            raise ValueError("Kafka shutdown drain timeout must be non-negative")


@dataclass(frozen=True)
class KafkaServiceConfig(CompoundScalar, namespace=_NAMESPACE):
    connection: KafkaConnectionConfig
    consumer_defaults: KafkaConsumerDefaults = field(default_factory=KafkaConsumerDefaults)
    producer: KafkaProducerOptions = field(default_factory=KafkaProducerOptions)

    @staticmethod
    def from_bootstrap_servers(
        bootstrap_servers: tuple[str, ...] | list[str],
        *,
        client_id: str = "",
    ) -> "KafkaServiceConfig":
        servers = tuple(bootstrap_servers)
        if not servers:
            raise ValueError("Kafka service config requires a bootstrap server")
        return KafkaServiceConfig(KafkaConnectionConfig(servers, client_id))


@dataclass(frozen=True, init=False)
class KafkaSubscriptionKey(CompoundScalar, namespace=_NAMESPACE):
    selector_kind: KafkaSelectorKind
    topics: tuple[str, ...]
    topic_pattern: Optional[str]
    partitions: tuple[KafkaTopicPartition, ...]
    group_id: str
    assignment_mode: KafkaAssignmentMode
    start_position: KafkaStartPosition
    stop_position: KafkaStopPosition
    isolation_level: str
    commit_mode: KafkaCommitMode
    recovery_clock: KafkaRecoveryClock
    merge_policy: KafkaMergePolicy
    key_filter: Optional[bytes]
    sharing_identity: str

    def __init__(
        self,
        *,
        group_id: str,
        assignment_mode: KafkaAssignmentMode = KafkaAssignmentMode.GROUP,
        topics: tuple[str, ...] | list[str] = (),
        topic_pattern: Optional[str] = None,
        partitions: tuple[KafkaTopicPartition, ...] | list[KafkaTopicPartition] = (),
        start_position: Optional[KafkaStartPosition] = None,
        stop_position: Optional[KafkaStopPosition] = None,
        isolation_level: str = "read_uncommitted",
        commit_mode: KafkaCommitMode = KafkaCommitMode.EXPLICIT,
        recovery_clock: KafkaRecoveryClock = KafkaRecoveryClock.ARRIVAL,
        merge_policy: KafkaMergePolicy = KafkaMergePolicy.PARTITION,
        key_filter: Optional[bytes] = None,
        sharing_identity: str = "",
        selector_kind: Optional[KafkaSelectorKind] = None,
    ) -> None:
        topics = tuple(topics)
        partitions = tuple(partitions)
        selected = int(bool(topics)) + int(bool(topic_pattern)) + int(bool(partitions))
        if selected != 1:
            raise ValueError("Kafka subscription requires exactly one topic selector")
        inferred = (
            KafkaSelectorKind.TOPICS
            if topics
            else KafkaSelectorKind.PATTERN
            if topic_pattern
            else KafkaSelectorKind.PARTITIONS
        )
        if selector_kind is not None and selector_kind != inferred:
            raise ValueError("selector_kind contradicts the supplied Kafka selector")
        if not group_id:
            raise ValueError("Kafka subscription requires a group id")
        if topics and any(not topic for topic in topics):
            raise ValueError("Kafka subscription topics cannot be empty")
        if assignment_mode == KafkaAssignmentMode.INDEPENDENT and topic_pattern:
            raise ValueError(
                "Independent Kafka assignment requires explicit topics or partitions"
            )
        if isolation_level not in ("read_uncommitted", "read_committed"):
            raise ValueError(
                "Kafka isolation level must be read_uncommitted or read_committed"
            )
        object.__setattr__(self, "selector_kind", inferred)
        object.__setattr__(self, "topics", topics)
        object.__setattr__(self, "topic_pattern", topic_pattern)
        object.__setattr__(self, "partitions", partitions)
        object.__setattr__(self, "group_id", group_id)
        object.__setattr__(self, "assignment_mode", assignment_mode)
        object.__setattr__(
            self,
            "start_position",
            start_position or KafkaStartPosition.committed(),
        )
        object.__setattr__(
            self,
            "stop_position",
            stop_position or KafkaStopPosition.unbounded(),
        )
        object.__setattr__(self, "isolation_level", isolation_level)
        object.__setattr__(self, "commit_mode", commit_mode)
        object.__setattr__(self, "recovery_clock", recovery_clock)
        object.__setattr__(self, "merge_policy", merge_policy)
        object.__setattr__(self, "key_filter", key_filter)
        object.__setattr__(self, "sharing_identity", sharing_identity)


@dataclass(frozen=True)
class KafkaRecord(CompoundScalar, namespace=_NAMESPACE):
    topic: str
    partition: int
    offset: int
    timestamp: Optional[datetime]
    timestamp_type: KafkaTimestampType
    key: Optional[bytes]
    value: Optional[bytes]
    headers: tuple[KafkaHeader, ...] = ()

    def __post_init__(self) -> None:
        if not self.topic or self.partition < 0 or self.offset < 0:
            raise ValueError(
                "Kafka records require a topic and non-negative partition/offset"
            )


@dataclass(frozen=True)
class KafkaProduceRecord(CompoundScalar, namespace=_NAMESPACE):
    value: Optional[bytes]
    key: Optional[bytes] = None
    headers: tuple[KafkaHeader, ...] = ()
    timestamp: Optional[datetime] = None
    partition: Optional[int] = None
    user_token: str = ""

    def __post_init__(self) -> None:
        if self.partition is not None and self.partition < 0:
            raise ValueError("Kafka partition must be non-negative")


@dataclass(frozen=True)
class KafkaCursor(CompoundScalar, namespace=_NAMESPACE):
    subscription_identity: str
    assignment_generation: int
    topic: str
    partition: int
    next_offset: int

    def __post_init__(self) -> None:
        if (
            not self.subscription_identity
            or self.assignment_generation <= 0
            or not self.topic
            or self.partition < 0
            or self.next_offset < 0
        ):
            raise ValueError(
                "Kafka cursors require live identity, generation, topic, partition, and offset"
            )


@dataclass(frozen=True)
class KafkaDeliveryReport(CompoundScalar, namespace=_NAMESPACE):
    user_token: str
    sequence: int
    topic: str
    partition: Optional[int]
    offset: Optional[int]
    status: KafkaDeliveryStatus
    error_code: int
    retriable: bool
    fatal: bool
    message: str


@dataclass(frozen=True)
class KafkaEvent(CompoundScalar, namespace=_NAMESPACE):
    severity: KafkaSeverity
    component: str
    category: str
    error_code: int
    retriable: bool
    fatal: bool
    service_path: str
    subscription_identity: str
    publisher_identity: str
    message: str


class KafkaSubscriptionOutput(TimeSeriesSchema, namespace=_NAMESPACE):
    record: TS[KafkaRecord]
    cursor: TS[KafkaCursor]
    state: TS[KafkaSubscriptionState]


class KafkaPublishRequest(TimeSeriesSchema, namespace=_NAMESPACE):
    topic: TS[str]
    record: TS[KafkaProduceRecord]


# Configuration crosses a wiring-time scalar boundary rather than a TS
# annotation. Materialise its registered Bundle schema now so erased operator
# dispatch does not infer an arbitrary Python object (``Any``) at first use.
_KAFKA_SERVICE_CONFIG_TS = TS[KafkaServiceConfig]


def _subscription_service(
    key: TS[KafkaSubscriptionKey], path: str = ""
) -> TSB[KafkaSubscriptionOutput]: ...


_subscription_service.__name__ = "kafka_subscription"
_kafka_subscription_service = subscription_service(_subscription_service)


def _publish_service(
    request: TSB[KafkaPublishRequest], path: str = ""
) -> TS[KafkaDeliveryReport]: ...


_publish_service.__name__ = "kafka_publish"
_kafka_publish_service = request_reply_service(_publish_service)


def kafka_commit(cursor: TS[KafkaCursor], path: str = "") -> None: ...


kafka_commit = request_reply_service(kafka_commit)


def kafka_events(path: str = "") -> TS[KafkaEvent]: ...


kafka_events = reference_service(kafka_events)


_register_service = operator_function("hgraph.kafka.register_service")


def register_kafka_service(config: KafkaServiceConfig, path: str = "") -> None:
    """Register one lazy native Kafka service implementation at ``path``."""

    _register_service(config, path)


def kafka_subscribe(
    key: TS[KafkaSubscriptionKey], path: str = ""
) -> TSB[KafkaSubscriptionOutput]:
    return _kafka_subscription_service(key, path=path)


def kafka_publish(
    topic: str | TS[str] | TSB[KafkaPublishRequest],
    record: Optional[TS[KafkaProduceRecord]] = None,
    path: str = "",
) -> TS[KafkaDeliveryReport]:
    if record is None:
        request = topic
    else:
        request = TSB[KafkaPublishRequest].from_ts(topic=topic, record=record)
    return _kafka_publish_service(request, path=path)


__all__ = [name for name in globals() if name.startswith("Kafka")]
__all__ += [
    "kafka_commit",
    "kafka_events",
    "kafka_publish",
    "kafka_subscribe",
    "register_kafka_service",
]
