import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from datetime import timedelta
from typing import Callable

from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    EvaluationEngineApi,
    EvaluationMode,
    SCHEDULER,
    STATE,
    TS,
    TSB,
    compute_node,
    default,
    graph,
    if_then_else,
    reference_service,
    sink_node,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap
from hgraph._wiring._markers import _INJECTABLE_MARKERS, _StateExpr

__all__ = (
    "message_publisher",
    "message_subscriber",
    "MessageState",
    "KafkaMessage",
)

_FLUSH_INTERVAL = timedelta(milliseconds=100)
_FLUSH_MESSAGE_COUNT = 1000


@dataclass
class _PublisherFlushState:
    pending_count: int = 0


@dataclass(frozen=True)
class KafkaMessage(CompoundScalar):
    """A structured Kafka message.

    ``content_type`` is transported in the Kafka ``content-type`` header;
    the remaining headers are preserved in ``headers``.
    """

    payload: bytes
    key: bytes = None
    content_type: str = None
    headers: frozendict[str, bytes] = frozendict()

    def to_dict(self):
        """Return the upstream ``CompoundScalar`` dictionary shape."""
        return {
            field.name: value
            for field in fields(self)
            if (value := getattr(self, field.name, None)) is not None
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CompoundScalar":
        """Construct from known schema fields, ignoring unknown fields."""
        names = {field.name for field in fields(cls)}
        return cls(**{name: value for name, value in d.items() if name in names})


class MessageState(ABC):
    @abstractmethod
    def add_publisher(self, topic: str):
        """Register the graph's single publisher for ``topic``."""
        ...

    @abstractmethod
    def add_subscriber(self, topic: str, replay: bool = False):
        """Register a live subscriber, retaining replay handoff when requested."""
        ...

    @abstractmethod
    def add_historical_subscriber(self, topic: str):
        """Register a historical subscriber for ``topic``."""
        ...


def get_message_state():
    from ._impl import KafkaMessageState

    return KafkaMessageState.instance()


def _publish_and_flush(msg, topic, message_state, scheduler, api, state):
    flush_due = scheduler.is_scheduled_now
    if msg.modified:
        message_state.publish(topic, msg.value)
        state.pending_count += 1

    if flush_due or state.pending_count >= _FLUSH_MESSAGE_COUNT:
        message_state.flush()
        state.pending_count = 0
        scheduler.reset()
    elif msg.modified and not scheduler.is_scheduled:
        scheduler.schedule(
            _FLUSH_INTERVAL,
            tag="flush_timer",
            on_wall_clock=api.evaluation_mode == EvaluationMode.REAL_TIME,
        )


@sink_node
def _publish_bytes(
        msg: TS[bytes], topic: str, message_state: object,
        _scheduler: SCHEDULER = None,
        _api: EvaluationEngineApi = None,
        _state: STATE[_PublisherFlushState] = None):
    _publish_and_flush(
        msg, topic, message_state, _scheduler, _api, _state)


@_publish_bytes.start
def _start_publish_bytes(message_state: object):
    message_state.acquire_producer()


@_publish_bytes.stop
def _stop_publish_bytes(message_state: object):
    message_state.flush()
    message_state.close_producer()


@sink_node
def _publish_message(
        msg: TS[KafkaMessage], topic: str, message_state: object,
        _scheduler: SCHEDULER = None,
        _api: EvaluationEngineApi = None,
        _state: STATE[_PublisherFlushState] = None):
    _publish_and_flush(
        msg, topic, message_state, _scheduler, _api, _state)


@_publish_message.start
def _start_publish_message(message_state: object):
    message_state.acquire_producer()


@_publish_message.stop
def _stop_publish_message(message_state: object):
    message_state.flush()
    message_state.close_producer()


def message_publisher_operator(msg, topic: str):
    state = get_message_state()
    if _unwrap(msg).ts_type == TS[bytes].handle:
        _publish_bytes(msg, topic=topic, message_state=state)
    elif _unwrap(msg).ts_type == TS[KafkaMessage].handle:
        _publish_message(msg, topic=topic, message_state=state)
    else:
        raise TypeError("message publisher output must be TS[bytes] or TS[KafkaMessage]")


def _decorator_signature(fn, excluded, topic):
    target = getattr(fn, "fn", fn)
    signature = inspect.signature(target, eval_str=True)
    parameters = []
    for parameter in signature.parameters.values():
        if parameter.name in excluded:
            continue
        if (parameter.annotation in _INJECTABLE_MARKERS
                or isinstance(parameter.annotation, _StateExpr)):
            continue
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(
                "Kafka publisher/subscriber graphs do not support variadic "
                "positional inputs")
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
        (index for index, parameter in enumerate(parameters)
         if parameter.kind is inspect.Parameter.VAR_KEYWORD),
        len(parameters),
    )
    parameters.insert(var_keyword, topic_parameter)
    return signature, signature.replace(parameters=parameters)


def message_publisher(fn: Callable = None, *, topic: str = None):
    """Publish the wrapped graph's message output to a Kafka topic.

    The output may be ``TS[bytes]``, ``TS[KafkaMessage]``, or a ``TSB`` with
    a ``msg`` field of either type. ``topic`` can be supplied on the
    decorator or when the wrapped graph is called. A graph declaring both
    ``msg`` and ``recovered`` inputs receives historical topic messages from
    the graph start time before recovery is signalled.
    """
    if fn is None:
        return lambda value: message_publisher(value, topic=topic)
    if not hasattr(fn, "signature"):
        fn = graph(fn)

    signature, public_signature = _decorator_signature(
        fn, {"msg", "recovered", "topic"}, topic)
    replay_parameters = {
        name: signature.parameters.get(name) for name in ("msg", "recovered")
    }
    replay_history = any(parameter is not None for parameter in replay_parameters.values())
    replay_msg_is_bytes = True
    if replay_history:
        if any(parameter is None for parameter in replay_parameters.values()):
            raise TypeError(
                "message_publisher replay requires both msg and recovered inputs")
        msg_annotation = replay_parameters["msg"].annotation
        if msg_annotation not in (TS[bytes], TS[KafkaMessage]):
            raise TypeError(
                "message_publisher replay requires msg: TS[bytes] or "
                "msg: TS[KafkaMessage]")
        if replay_parameters["recovered"].annotation != TS[bool]:
            raise TypeError(
                "message_publisher replay requires recovered: TS[bool]")
        replay_msg_is_bytes = msg_annotation == TS[bytes]

    output_type = signature.return_annotation
    if not isinstance(output_type, _TsExpr):
        raise TypeError("message_publisher requires a time-series output")
    is_bundle = output_type.handle.is_tsb

    fields = tuple(__import__("_hgraph").ts_field_types(output_type.handle)) if is_bundle else ()
    message_type = dict(fields).get("msg") if is_bundle else output_type.handle
    if is_bundle and message_type is None:
        raise TypeError("message_publisher TSB output must contain a 'msg' field")
    if message_type not in (TS[bytes].handle, TS[KafkaMessage].handle):
        raise TypeError(
            "message_publisher output must be TS[bytes], TS[KafkaMessage], "
            "or a TSB with a matching 'msg' field")
    return_type = output_type
    if len(fields) == 2 and "out" in dict(fields):
        out_handle = dict(fields)["out"]
        return_type = _TsExpr(out_handle, repr(out_handle))

    def publisher(*args, **kwargs):
        bound = public_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        selected_topic = bound.arguments.pop("topic")
        if selected_topic is None:
            raise ValueError(f"topic must be provided to {fn.__name__}")
        state = get_message_state()
        state.add_publisher(selected_topic)
        if replay_history:
            state.add_historical_subscriber(selected_topic)
            history = message_history_subscriber_service(path=selected_topic)
            replay_message = history["msg"]
            bound.arguments["msg"] = (
                _payload(replay_message) if replay_msg_is_bytes else replay_message)
            bound.arguments["recovered"] = history["recovered"]
        out = fn(**bound.arguments)
        message = out["msg"] if is_bundle else out
        message_publisher_operator(message, selected_topic)
        if not is_bundle:
            return None
        if len(fields) == 2 and "out" in dict(fields):
            return out["out"]
        return out

    publisher.__name__ = fn.__name__
    publisher.__signature__ = public_signature.replace(
        return_annotation=None if not is_bundle else return_type
    )
    return graph(publisher)


@compute_node
def _payload(message: TS[KafkaMessage]) -> TS[bytes]:
    return message.value.payload


def message_subscriber(fn: Callable = None, *, topic: str = None):
    """Supply Kafka topic messages to the wrapped graph's ``msg`` input.

    ``msg`` must be ``TS[bytes]`` or ``TS[KafkaMessage]``. When the wrapped
    graph also declares ``recovered: TS[bool]``, history is replayed from the
    graph start time, ``recovered`` changes from false to true, and the same
    Kafka consumer continues with live messages.
    """
    if fn is None:
        return lambda value: message_subscriber(value, topic=topic)
    if not hasattr(fn, "signature"):
        fn = graph(fn)

    signature, public_signature = _decorator_signature(
        fn, {"msg", "recovered", "topic"}, topic)
    message_annotation = signature.parameters.get("msg")
    if message_annotation is None or message_annotation.annotation not in (TS[bytes], TS[KafkaMessage]):
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
        state = get_message_state()
        state.add_subscriber(selected_topic, replay=has_recovered)
        message = message_subscriber_service(path=selected_topic)
        if has_recovered:
            state.add_historical_subscriber(selected_topic)
            history = message_history_subscriber_service(path=selected_topic)
            recovered = history["recovered"]
            bound.arguments["recovered"] = recovered
            message = if_then_else(
                default(recovered, False), message, history["msg"])
        bound.arguments["msg"] = _payload(message) if message_annotation.annotation == TS[bytes] else message
        return fn(**bound.arguments)

    publisher_return = signature.return_annotation
    subscriber.__name__ = fn.__name__
    subscriber.__signature__ = public_signature.replace(return_annotation=publisher_return)
    return graph(subscriber)


@reference_service
def message_history_subscriber_service(
        path: str) -> TSB["msg" : TS[KafkaMessage], "recovered" : TS[bool]]:
    """Shared historical topic stream and recovery signal."""


@reference_service
def message_subscriber_service(path: str) -> TS[KafkaMessage]:
    """Shared real-time topic stream."""
