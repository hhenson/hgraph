from abc import ABC, abstractmethod
from typing import Callable

from hgraph import (
    adaptor,
    TS,
    graph,
    HgTSBTypeMetaData,
    TSB,
    with_signature,
    null_sink,
    reference_service,
    debug_print,
    default,
    if_then_else,
    operator,
    HgAtomicType,
)

__all__ = ("message_publisher", "message_subscriber", "MessageState")


def message_publisher(fn: Callable = None, *, topic: str = None):
    """
    Wraps a publisher function as a publisher for a messaging topic.
    The function should return a ``TS[bytes]`` that will be published to the topic.
    The default signature looks like this:

    ::

        @message_publisher(topic="my_topic")
        def my_fn() -> TS[bytes]:
            ...

    If ``msg`` and ``recovered`` inputs are present, then the publisher will replay the history of the topic before
    processing any new data.
    The data will be started from ``start_time`` provided to the graph engine. This should be set to the first time
    the data needs to be processed. To support replay capabilities, the function must accept a parameter
    ``msg: TS[bytes]`` and ``recovered: TS[bool]``, on which the history will be replayed.

    ::

        @message_publisher(topic="my_topic"))
        def my_fn(msg: TS[bytes], recovered: TS[bool]) -> TS[bytes]:
            ...

    The function can also take additional arguments. If the function has to return additional values, then there
    the output type should be a TSB with one of the outputs being ``msg: TS[bytes]``. For example:

    ::

        @message_publisher(topic="my_topic")
        def my_fn(arg1: TS[int], arg2: TS[str]) -> TSB[{"msg": TS[bytes], "out_1": TS[int], "out_2": TS[str]}]:
            ...

    In this form, the adaptor will capture the msg output, but it will also be returned as part of the bundle.

    To ensure we only wire in logic intended for use, even if the implementation only has message inputs/outputs,
    the function must still be called in the main wiring graph to ensure it gets used.
    """
    if fn is None:
        return lambda fn: message_publisher(fn, topic=topic)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    if "msg" in fn.signature.time_series_args or "recovered" in fn.signature.time_series_args:
        assert (
            "msg" in fn.signature.time_series_inputs.keys()
        ), "kafka_publisher graph must have an input named 'msg' when defining replay args"
        assert fn.signature.time_series_inputs["msg"].matches_type(
            TS[bytes]
        ), f"Graph must have an input named 'msg' of type TS[bytes] got {fn.signature.time_series_inputs['msg']}"
        assert (
            "recovered" in fn.signature.time_series_inputs.keys()
        ), "kafka_publisher graph must have an input named 'recovered' when defining replay args"
        assert fn.signature.time_series_inputs["recovered"].matches_type(TS[bool]), (
            "Graph input named 'recovered' must be of of type TS[bool] got"
            f" {fn.signature.time_series_inputs['recovered']}"
        )
        replay_history = True
    else:
        replay_history = False

    output_type = fn.signature.output_type
    is_tsb = False
    if isinstance(output_type, HgTSBTypeMetaData):
        is_tsb = True
        assert "msg" in (schema := output_type.bundle_schema_tp.meta_data_schema), "TSB must have a 'msg' output"
        output_type = output_type["msg"]

    assert output_type.matches_type(TS[bytes]), "Graph must have a message output of type TS[bytes]"

    final_output_type = None
    if is_tsb:
        if len(schema) == 2 and "out" in schema:
            final_output_type = schema["out"]
        else:
            final_output_type = fn.signature.output_type

    @graph
    @with_signature(
        kwargs=(
            {
                k: v
                for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items()
                if k not in ("msg", "recovered")
            }
            | {"topic": HgAtomicType(str)}
        ),
        return_annotation=final_output_type,
        defaults=fn.signature.defaults | {"topic": topic} if topic is not None else {},
    )
    def message_publisher_graph(**kwargs):
        topic_ = kwargs.pop("topic", None)
        if topic_ is None:
            raise ValueError(f"topic must be provided to {fn.signature.name}")
        get_message_state().add_publisher(topic_)
        if replay_history:
            get_message_state().add_historical_subscriber(topic_)
            msg_history = message_history_subscriber_service(path=topic_)
            kwargs["msg"] = msg_history["msg"]  # Connect replay
            kwargs["recovered"] = msg_history["recovered"]  # Connect replay

        out = fn(**kwargs)
        out_msg = out["msg"] if is_tsb else out
        # Connect output to the message bus
        message_publisher_operator(out_msg, topic=topic_)
        if is_tsb:
            keys = tuple(out.keys())
            if len(keys) == 2 and "out" in keys:
                return out["out"]
            else:
                return out
        return None

    return message_publisher_graph


def message_subscriber(fn: Callable = None, *, topic: str = None):
    """
    Subscribe to a kafka topic, the path binds to the topic. The values are provided to ``msg``. This is an example:

    ::

        @message_subscriber(topic="my_topic")
        def my_fn(msg: TS[bytes]):
            ...

    If the ``recovered`` argument is present, the subscriber will replay
    the history of the topic and then continue to process new data. The ``recovered: TS[bool]``
    will tick True when the subscriber has recovered the history data.

    ::

        @message_subscriber(topic="my_topic")
        def my_fn(msg: TS[bytes], recovered: TS[bool]):
            ...

    It is also possible to supply additional arguments and return additional values.

    To ensure we only wire in logic intended for use, even if the implementation only has message inputs,
    the function must still be called in the main wiring graph to ensure it gets used.
    """
    if fn is None:
        return lambda fn: message_subscriber(fn, topic=topic)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "msg" in fn.signature.time_series_inputs.keys(), "message_subscriber graph must have an input named 'msg'"
    assert fn.signature.time_series_inputs["msg"].matches_type(
        TS[bytes]
    ), f"The input named 'msg' must be of type TS[bytes] got {fn.signature.time_series_inputs['msg']}"
    has_recovered = "recovered" in fn.signature.time_series_inputs.keys()
    assert not has_recovered or fn.signature.time_series_inputs["recovered"].matches_type(
        TS[bool]
    ), f"The input named 'recovered' must be of type TS[bool] got {fn.signature.time_series_inputs['recovered']}"

    output_type = fn.signature.output_type

    @graph
    @with_signature(
        kwargs={
            k: v
            for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items()
            if k not in ("msg", "recovered")
        }
        | {"topic": HgAtomicType(str)},
        return_annotation=output_type,
        defaults=fn.signature.defaults | {"topic": topic} if topic is not None else {},
    )
    def message_subscriber_graph(**kwargs):
        topic_ = kwargs.pop("topic", None)
        if topic_ is None:
            raise ValueError(f"topic must be provided to {fn.signature.name}")
        get_message_state().add_subscriber(topic_)
        msg_input = message_subscriber_service(path=topic_)
        if has_recovered:
            get_message_state().add_historical_subscriber(topic_)
            msg_history = message_history_subscriber_service(path=topic_)
            kwargs["recovered"] = (recovered := msg_history["recovered"])  # Connect recovered signal
            msg_input = if_then_else(default(recovered, False), msg_input, msg_history["msg"])
        kwargs["msg"] = msg_input
        out = fn(**kwargs)
        return out

    return message_subscriber_graph


class MessageState(ABC):

    @abstractmethod
    def add_publisher(self, topic: str):
        """Adds a publisher to the message state"""

    @abstractmethod
    def add_subscriber(self, topic: str):
        """Adds a subscriber to the message state"""

    @abstractmethod
    def add_historical_subscriber(self, topic: str):
        """Adds a historical subscriber to the message state"""


def get_message_state() -> MessageState:
    from hgraph.adaptors.kafka._impl import KafkaMessageState

    return KafkaMessageState.instance()


@operator
def message_publisher_operator(msg: TS[bytes], topic: str):
    """Publishes the msg to the topic provided."""


@reference_service
def message_history_subscriber_service(path: str) -> TSB["msg" : TS[bytes], "recovered" : TS[bool]]:
    """Only retrieve history, after which the topic can be unsubscribed."""


@reference_service
def message_subscriber_service(path: str) -> TS[bytes]:
    """
    Subscriber for kafka, output contains the msg and a recovered flag.
    """
