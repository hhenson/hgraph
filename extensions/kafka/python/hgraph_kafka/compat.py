"""Released ``hgraph.adaptors.kafka`` surface over the native Kafka service.

This module preserves the decorator/signature contract.  Broker ownership,
consumer sessions, publishing, recovery, and teardown remain in the C++
service implementation.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields, field
from typing import Callable

from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    TS,
    compute_node,
    drop_dups,
    graph,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap
from hgraph._wiring._core import _current_wiring
from hgraph._wiring._markers import _INJECTABLE_MARKERS, _StateExpr

from . import (
    KafkaAssignmentMode,
    KafkaCommitMode,
    KafkaConnectionConfig,
    KafkaConsumerDefaults,
    KafkaHeader,
    KafkaMergePolicy,
    KafkaOffsetFallback,
    KafkaOption,
    KafkaProduceRecord,
    KafkaProducerOptions,
    KafkaRecord,
    KafkaRecoveryClock,
    KafkaServiceConfig,
    KafkaStartPosition,
    KafkaStopPosition,
    KafkaSubscriptionKey,
    KafkaSubscriptionState,
    kafka_publish,
    kafka_subscribe,
    register_kafka_service,
)

__all__ = (
    "message_publisher",
    "message_subscriber",
    "MessageState",
    "KafkaMessage",
    "register_kafka_adaptor",
)

_LEGACY_PATH = "legacy"
_CONTENT_TYPE_HEADER = "content-type"
_COMPATIBILITY_STATE_KEY = "hgraph-kafka.compatibility-state"


@dataclass(frozen=True)
class KafkaMessage(CompoundScalar, namespace="hgraph.adaptors.kafka._api"):
    """The released structured-message shape."""

    payload: bytes
    key: bytes | None = None
    content_type: str | None = None
    headers: frozendict[str, bytes] = frozendict()

    def to_dict(self):
        return {
            item.name: value
            for item in fields(self)
            if (value := getattr(self, item.name, None)) is not None
        }

    @classmethod
    def from_dict(cls, d: dict) -> "KafkaMessage":
        names = {item.name for item in fields(cls)}
        return cls(**{name: value for name, value in d.items() if name in names})


class MessageState(ABC):
    @abstractmethod
    def add_publisher(self, topic: str): ...

    @abstractmethod
    def add_subscriber(self, topic: str, replay: bool = False): ...

    @abstractmethod
    def add_historical_subscriber(self, topic: str): ...


@dataclass
class _CompatibilityState(MessageState):
    publishers: set[str] = field(default_factory=set)
    subscribers: set[str] = field(default_factory=set)
    historical_subscribers: set[str] = field(default_factory=set)
    service_config: KafkaServiceConfig | None = None

    def add_publisher(self, topic: str):
        if topic in self.publishers:
            raise ValueError(f"topic {topic!r} already has a publisher")
        self.publishers.add(topic)

    def add_subscriber(self, topic: str, replay: bool = False):
        del replay
        self.subscribers.add(topic)

    def add_historical_subscriber(self, topic: str):
        self.historical_subscribers.add(topic)


def get_message_state() -> MessageState:
    wiring = _current_wiring()
    return wiring._acquire_extension_state(
        _COMPATIBILITY_STATE_KEY, _CompatibilityState
    )


_COMMON_OPTIONS = {
    "security_protocol": "security.protocol",
    "sasl_mechanism": "sasl.mechanism",
    "sasl_plain_username": "sasl.username",
    "sasl_plain_password": "sasl.password",
    "ssl_cafile": "ssl.ca.location",
    "ssl_certfile": "ssl.certificate.location",
    "ssl_keyfile": "ssl.key.location",
    "ssl_password": "ssl.key.password",
}
_CONSUMER_OPTIONS = {
    "fetch_max_bytes": "fetch.message.max.bytes",
    "max_partition_fetch_bytes": "max.partition.fetch.bytes",
    "session_timeout_ms": "session.timeout.ms",
    "heartbeat_interval_ms": "heartbeat.interval.ms",
}
_PRODUCER_OPTIONS = {
    "acks": "acks",
    "retries": "retries",
    "compression_type": "compression.codec",
    "batch_size": "batch.size",
    "linger_ms": "linger.ms",
}
_IGNORED_LEGACY_OPTIONS = {
    "api_version",
    "api_version_auto_timeout_ms",
    "consumer_timeout_ms",
    "group_id",
    "key_deserializer",
    "key_serializer",
    "value_deserializer",
    "value_serializer",
}


def _option(name: str, value) -> KafkaOption:
    if isinstance(value, bool):
        rendered = "true" if value else "false"
    elif isinstance(value, (tuple, list)):
        rendered = ",".join(str(item) for item in value)
    else:
        rendered = str(value)
    return KafkaOption(name, rendered)


def _legacy_config(value: dict | KafkaServiceConfig) -> KafkaServiceConfig:
    if isinstance(value, KafkaServiceConfig):
        return value
    if not isinstance(value, dict):
        raise TypeError("register_kafka_adaptor requires a dict or KafkaServiceConfig")
    unsupported = {"producer", "producer_factory", "consumer_factory"} & value.keys()
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(
            f"production Kafka configuration cannot inject native objects ({names}); "
            "use hgraph_kafka.testing for broker-backed tests"
        )

    options = dict(value)
    servers = options.pop("bootstrap_servers", options.pop("bootstrap.servers", None))
    if isinstance(servers, str):
        servers = (servers,)
    else:
        servers = tuple(servers or ())
    if not servers:
        raise ValueError("Kafka compatibility configuration requires bootstrap_servers")
    client_id = str(options.pop("client_id", options.pop("client.id", "")))
    acknowledgements = str(options.pop("acks", "1"))
    retries = int(options.pop("retries", 0))
    linger_ms = int(options.pop("linger_ms", options.pop("linger.ms", 100)))
    batch_record_limit = int(options.pop("batch.num.messages", 1_000))

    common: list[KafkaOption] = []
    consumer: list[KafkaOption] = []
    producer: list[KafkaOption] = []
    for name, item in options.items():
        if name in _IGNORED_LEGACY_OPTIONS:
            continue
        if name in _COMMON_OPTIONS:
            common.append(_option(_COMMON_OPTIONS[name], item))
        elif name in _CONSUMER_OPTIONS:
            consumer.append(_option(_CONSUMER_OPTIONS[name], item))
        elif name in _PRODUCER_OPTIONS:
            producer.append(_option(_PRODUCER_OPTIONS[name], item))
        elif "." in name:
            common.append(_option(name, item))
        else:
            raise ValueError(
                f"Kafka compatibility option {name!r} has no safe librdkafka mapping"
            )

    return KafkaServiceConfig(
        KafkaConnectionConfig(servers, client_id, tuple(common)),
        KafkaConsumerDefaults(
            options=tuple(consumer),
        ),
        KafkaProducerOptions(
            idempotent=False,
            acknowledgements=acknowledgements,
            retries=retries,
            linger_ms=linger_ms,
            batch_record_limit=batch_record_limit,
            options=tuple(producer),
        ),
    )


def register_kafka_adaptor(config: dict | KafkaServiceConfig):
    """Register the released adaptor surface on the native service path."""

    service_config = _legacy_config(config)
    state = get_message_state()
    assert isinstance(state, _CompatibilityState)
    state.service_config = service_config
    register_kafka_service(service_config, path=_LEGACY_PATH)


def _configured_state() -> _CompatibilityState:
    state = get_message_state()
    if not isinstance(state, _CompatibilityState) or state.service_config is None:
        raise RuntimeError("register_kafka_adaptor must be called before Kafka wiring")
    return state


def _subscription_key(topic: str, *, history: bool, continue_live: bool):
    state = _configured_state()
    client_id = state.service_config.connection.client_id or "hgraph"
    phase = "replay" if history else "live"
    return KafkaSubscriptionKey(
        topics=(topic,),
        group_id=f"{client_id}-legacy-{phase}-{topic}",
        assignment_mode=KafkaAssignmentMode.INDEPENDENT,
        start_position=(
            KafkaStartPosition.graph_start_time(KafkaOffsetFallback.LATEST)
            if history
            else KafkaStartPosition.latest()
        ),
        stop_position=(
            KafkaStopPosition.graph_lifetime()
            if continue_live
            else KafkaStopPosition.snapshot()
        ),
        commit_mode=KafkaCommitMode.NONE,
        recovery_clock=(
            KafkaRecoveryClock.RECORD_TIMESTAMP
            if history
            else KafkaRecoveryClock.ARRIVAL
        ),
        merge_policy=(
            KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET
            if history
            else KafkaMergePolicy.PARTITION
        ),
        sharing_identity=(
            f"legacy:{phase}:{topic}:{'live' if continue_live else 'history'}"
        ),
    )


@compute_node
def _to_legacy_message(record: TS[KafkaRecord]) -> TS[KafkaMessage]:
    headers = {}
    content_type = None
    for header in record.value.headers:
        # Matched case-INSENSITIVELY. Kafka header names are arbitrary byte
        # strings, and producers outside hgraph commonly send ``Content-Type``.
        # An exact-case match left such a message with content_type=None while
        # quietly depositing the header in ``headers``, so the information
        # survived where no consumer looks for it - and re-producing then
        # emitted the original casing rather than the canonical one, making the
        # round trip lossy in a way nothing reported.
        if header.name.lower() == _CONTENT_TYPE_HEADER:
            content_type = (
                header.value.decode("utf-8") if header.value is not None else None
            )
        else:
            headers[header.name] = header.value
    return KafkaMessage(
        payload=record.value.value,
        key=record.value.key,
        content_type=content_type,
        headers=frozendict(headers),
    )


@compute_node
def _payload(message: TS[KafkaMessage]) -> TS[bytes]:
    return message.value.payload


@compute_node
def _bytes_to_record(message: TS[bytes]) -> TS[KafkaProduceRecord]:
    return KafkaProduceRecord(value=message.value)


@compute_node
def _message_to_record(message: TS[KafkaMessage]) -> TS[KafkaProduceRecord]:
    # ``content_type`` is the canonical carrier. A content-type entry that also
    # sits in ``headers`` - under any casing - must not be emitted alongside it,
    # or the record leaves with two content-type headers whose values may
    # disagree. The field wins when set; otherwise a header-carried value passes
    # through untouched.
    headers = [
        KafkaHeader(name, value)
        for name, value in message.value.headers.items()
        if message.value.content_type is None or name.lower() != _CONTENT_TYPE_HEADER
    ]
    if message.value.content_type is not None:
        headers.append(
            KafkaHeader(
                _CONTENT_TYPE_HEADER,
                message.value.content_type.encode("utf-8"),
            )
        )
    return KafkaProduceRecord(
        value=message.value.payload,
        key=message.value.key,
        headers=tuple(headers),
    )


@compute_node
def _is_recovered(state: TS[KafkaSubscriptionState]) -> TS[bool]:
    return state.value in (
        KafkaSubscriptionState.LIVE,
        KafkaSubscriptionState.BOUNDED_COMPLETE,
    )


def _subscribe(topic: str, *, history: bool, continue_live: bool):
    import hgraph as hg

    key = _subscription_key(topic, history=history, continue_live=continue_live)
    key_ts = hg.const(key, tp=TS[KafkaSubscriptionKey])
    return kafka_subscribe(key_ts, path=_LEGACY_PATH)


def _decorator_signature(fn, excluded, topic):
    target = getattr(fn, "fn", fn)
    signature = inspect.signature(target, eval_str=True)
    parameters = []
    for parameter in signature.parameters.values():
        if parameter.name in excluded:
            continue
        if parameter.annotation in _INJECTABLE_MARKERS or isinstance(
            parameter.annotation, _StateExpr
        ):
            continue
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(
                "Kafka publisher/subscriber graphs do not support variadic positional inputs"
            )
        if parameter.kind is not inspect.Parameter.VAR_KEYWORD:
            parameter = parameter.replace(kind=inspect.Parameter.KEYWORD_ONLY)
        parameters.append(parameter)
    topic_parameter = inspect.Parameter(
        "topic",
        inspect.Parameter.KEYWORD_ONLY,
        default=topic if topic is not None else inspect.Parameter.empty,
        annotation=str,
    )
    var_keyword = next(
        (
            index
            for index, parameter in enumerate(parameters)
            if parameter.kind is inspect.Parameter.VAR_KEYWORD
        ),
        len(parameters),
    )
    parameters.insert(var_keyword, topic_parameter)
    return signature, signature.replace(parameters=parameters)


def message_publisher(fn: Callable = None, *, topic: str = None):
    if fn is None:
        return lambda value: message_publisher(value, topic=topic)
    if not hasattr(fn, "signature"):
        fn = graph(fn)

    signature, public_signature = _decorator_signature(
        fn, {"msg", "recovered", "topic"}, topic
    )
    replay_parameters = {
        name: signature.parameters.get(name) for name in ("msg", "recovered")
    }
    replay_history = any(item is not None for item in replay_parameters.values())
    replay_msg_is_bytes = True
    if replay_history:
        if any(item is None for item in replay_parameters.values()):
            raise TypeError("message_publisher replay requires both msg and recovered inputs")
        message_annotation = replay_parameters["msg"].annotation
        if message_annotation not in (TS[bytes], TS[KafkaMessage]):
            raise TypeError(
                "message_publisher replay requires msg: TS[bytes] or msg: TS[KafkaMessage]"
            )
        if replay_parameters["recovered"].annotation != TS[bool]:
            raise TypeError("message_publisher replay requires recovered: TS[bool]")
        replay_msg_is_bytes = message_annotation == TS[bytes]

    output_type = signature.return_annotation
    if not isinstance(output_type, _TsExpr):
        raise TypeError("message_publisher requires a time-series output")
    is_bundle = output_type.handle.is_tsb
    fields_ = (
        tuple(__import__("_hgraph").ts_field_types(output_type.handle))
        if is_bundle
        else ()
    )
    message_type = dict(fields_).get("msg") if is_bundle else output_type.handle
    if is_bundle and message_type is None:
        raise TypeError("message_publisher TSB output must contain a 'msg' field")
    if message_type not in (TS[bytes].handle, TS[KafkaMessage].handle):
        raise TypeError(
            "message_publisher output must be TS[bytes], TS[KafkaMessage], "
            "or a TSB with a matching 'msg' field"
        )
    return_type = output_type
    if len(fields_) == 2 and "out" in dict(fields_):
        out_handle = dict(fields_)["out"]
        return_type = _TsExpr(out_handle, repr(out_handle))

    def publisher(*args, **kwargs):
        bound = public_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        selected_topic = bound.arguments.pop("topic")
        if selected_topic is None:
            raise ValueError(f"topic must be provided to {fn.__name__}")
        state = _configured_state()
        state.add_publisher(selected_topic)
        if replay_history:
            state.add_historical_subscriber(selected_topic)
            history = _subscribe(
                selected_topic, history=True, continue_live=False
            )
            replay_message = _to_legacy_message(history["record"])
            bound.arguments["msg"] = (
                _payload(replay_message) if replay_msg_is_bytes else replay_message
            )
            bound.arguments["recovered"] = drop_dups(_is_recovered(history["state"]))
        out = fn(**bound.arguments)
        message = out["msg"] if is_bundle else out
        record = (
            _bytes_to_record(message)
            if message_type == TS[bytes].handle
            else _message_to_record(message)
        )
        kafka_publish(selected_topic, record, path=_LEGACY_PATH)
        if not is_bundle:
            return None
        if len(fields_) == 2 and "out" in dict(fields_):
            return out["out"]
        return out

    publisher.__name__ = fn.__name__
    publisher.__signature__ = public_signature.replace(
        return_annotation=None if not is_bundle else return_type
    )
    return graph(publisher)


def message_subscriber(fn: Callable = None, *, topic: str = None):
    if fn is None:
        return lambda value: message_subscriber(value, topic=topic)
    if not hasattr(fn, "signature"):
        fn = graph(fn)

    signature, public_signature = _decorator_signature(
        fn, {"msg", "recovered", "topic"}, topic
    )
    message_parameter = signature.parameters.get("msg")
    if message_parameter is None or message_parameter.annotation not in (
        TS[bytes],
        TS[KafkaMessage],
    ):
        raise TypeError("message_subscriber requires msg: TS[bytes] or msg: TS[KafkaMessage]")
    has_recovered = "recovered" in signature.parameters
    if has_recovered and signature.parameters["recovered"].annotation != TS[bool]:
        raise TypeError("message_subscriber recovered input must be TS[bool]")

    def subscriber(*args, **kwargs):
        bound = public_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        selected_topic = bound.arguments.pop("topic")
        if selected_topic is None:
            raise ValueError(f"topic must be provided to {fn.__name__}")
        state = _configured_state()
        state.add_subscriber(selected_topic, replay=has_recovered)
        if has_recovered:
            state.add_historical_subscriber(selected_topic)
        subscription = _subscribe(
            selected_topic, history=has_recovered, continue_live=True
        )
        message = _to_legacy_message(subscription["record"])
        bound.arguments["msg"] = (
            _payload(message) if message_parameter.annotation == TS[bytes] else message
        )
        if has_recovered:
            bound.arguments["recovered"] = drop_dups(
                _is_recovered(subscription["state"])
            )
        return fn(**bound.arguments)

    subscriber.__name__ = fn.__name__
    subscriber.__signature__ = public_signature.replace(
        return_annotation=signature.return_annotation
    )
    return graph(subscriber)


# The compatibility wheel owns the legacy import path.  Set the original
# qualified names after the hgraph scalar metadata has been materialised in
# this module, preserving pickles and user-facing introspection as well as the
# explicit CompoundScalar namespace above.
KafkaMessage.__module__ = "hgraph.adaptors.kafka._api"
MessageState.__module__ = "hgraph.adaptors.kafka._api"
