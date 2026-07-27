import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass

from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    TS,
    TSB,
    compute_node,
    const,
    graph,
    sink_node,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _unwrap

__all__ = (
    "KafkaMessage",
    "MessageState",
    "get_message_state",
    "message_publisher",
    "message_subscriber",
    "message_publisher_operator",
)


@dataclass(frozen=True)
class KafkaMessage(CompoundScalar):
    payload: bytes
    key: bytes = None
    content_type: str = None
    headers: frozendict[str, bytes] = frozendict()


class MessageState(ABC):
    @abstractmethod
    def add_publisher(self, topic: str):
        ...

    @abstractmethod
    def add_subscriber(self, topic: str):
        ...


def get_message_state():
    from ._impl import KafkaMessageState

    return KafkaMessageState.instance()


@sink_node
def _publish_bytes(msg: TS[bytes], topic: str, message_state: object):
    message_state.publish(topic, msg.value)


@_publish_bytes.start
def _start_publish_bytes(message_state: object):
    message_state.acquire_producer()


@_publish_bytes.stop
def _stop_publish_bytes(message_state: object):
    message_state.flush()
    message_state.close_producer()


@sink_node
def _publish_message(msg: TS[KafkaMessage], topic: str, message_state: object):
    message_state.publish(topic, msg.value)


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
    parameters = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.name not in excluded
    ]
    parameters.append(
        inspect.Parameter(
            "topic",
            inspect.Parameter.KEYWORD_ONLY,
            default=topic if topic is not None else inspect.Parameter.empty,
            annotation=str,
        )
    )
    return signature, signature.replace(parameters=parameters)


def message_publisher(fn=None, *, topic: str = None):
    if fn is None:
        return lambda value: message_publisher(value, topic=topic)
    if not hasattr(fn, "signature"):
        fn = graph(fn)

    signature, public_signature = _decorator_signature(fn, {"msg", "recovered"}, topic)
    if "msg" in signature.parameters or "recovered" in signature.parameters:
        raise NotImplementedError(
            "Kafka historical replay is not supported by the C++-first adaptor"
        )
    output_type = signature.return_annotation
    if not isinstance(output_type, _TsExpr):
        raise TypeError("message_publisher requires a time-series output")
    is_bundle = output_type.handle.is_tsb

    fields = tuple(__import__("_hgraph").ts_field_types(output_type.handle)) if is_bundle else ()
    return_type = output_type
    if tuple(name for name, _ in fields) == ("msg", "out"):
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
        out = fn(**bound.arguments)
        message = out["msg"] if is_bundle else out
        message_publisher_operator(message, selected_topic)
        if not is_bundle:
            return None
        if tuple(name for name, _ in fields) == ("msg", "out"):
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


def message_subscriber(fn=None, *, topic: str = None):
    if fn is None:
        return lambda value: message_subscriber(value, topic=topic)
    if not hasattr(fn, "signature"):
        fn = graph(fn)

    signature, public_signature = _decorator_signature(fn, {"msg", "recovered"}, topic)
    message_annotation = signature.parameters.get("msg")
    if message_annotation is None or message_annotation.annotation not in (TS[bytes], TS[KafkaMessage]):
        raise TypeError("message_subscriber requires msg: TS[bytes] or msg: TS[KafkaMessage]")
    has_recovered = "recovered" in signature.parameters
    if has_recovered:
        raise NotImplementedError(
            "Kafka historical replay is not supported by the C++-first adaptor"
        )

    def subscriber(*args, **kwargs):
        from ._impl import message_source

        bound = public_signature.bind(*args, **kwargs)
        bound.apply_defaults()
        selected_topic = bound.arguments.pop("topic")
        if selected_topic is None:
            raise ValueError(f"topic must be provided to {fn.__name__}")
        state = get_message_state()
        state.add_subscriber(selected_topic)
        message = message_source(selected_topic, state)
        bound.arguments["msg"] = _payload(message) if message_annotation.annotation == TS[bytes] else message
        if has_recovered:
            bound.arguments["recovered"] = const(True, tp=TS[bool])
        return fn(**bound.arguments)

    publisher_return = signature.return_annotation
    subscriber.__name__ = fn.__name__
    subscriber.__signature__ = public_signature.replace(return_annotation=publisher_return)
    return graph(subscriber)
