from dataclasses import dataclass, field
from typing import Callable

from hgraph import adaptor, TS, graph, HgTSBTypeMetaData, TSB, ts_schema, with_signature, null_sink, GlobalState


def message_publisher(fn: Callable = None, *, topic: str, replay_history: bool = False):
    """
    Wraps a publisher function as a publisher for a messaging topic.
    The function should return a ``TS[bytes]`` that will be published to the topic.
    The default signature looks like this:

    ::

        @message_publisher(topic="my_topic")
        def my_fn() -> TS[bytes]:
            ...

    If ``replay_history`` is True, the publisher will replay the history of the topic before processing any new data.
    The data will be started from ``start_time`` provided to the graph engine. This should be set to the first time
    the data needs to be processed. To support replay capabilities, the function must accept a parameter
    ``msg: TS[bytes]``, on which the history will be replayed.

    ::

        @message_publisher(topic="my_topic", replay_history=True))
        def my_fn(msg: TS[bytes]) -> TS[bytes]:
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
        return lambda fn: message_publisher(fn, topic=topic, replay_history=replay_history)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    if replay_history:
        assert (
                "msg" in fn.signature.time_series_inputs.keys()
        ), "kafka_publisher graph must have an input named 'msg' when replay is set to True"
        assert fn.signature.time_series_inputs["msg"].matches_type(
            TS[bytes]
        ), f"Graph must have an input named 'msg' of type TS[bytes]"

    output_type = fn.signature.output_type
    is_tsb = False
    if isinstance(output_type, HgTSBTypeMetaData):
        is_tsb = True
        assert "msg" in output_type, "TSB must have a 'msg' output"
        output_type = output_type["msg"]

    assert (output_type.matches_type(TS[bytes])), "Graph must have a message output of type TS[bytes]"

    final_output_type = fn.signature.output_type if is_tsb else None

    @graph
    @with_signature(
        kwargs={k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k != "msg"},
        return_annotation=final_output_type,
    )
    def message_publisher_graph(**kwargs):
        MessagingState.instance().add_publisher(topic, replay_history)
        msg_input = message_publisher_adaptor.to_graph(path=topic, __no_ts_inputs__=True)
        if replay_history:
            kwargs["msg"] = msg_input  # Connect replay
        else:
            null_sink(msg_input)
        out = fn(**kwargs)
        out_msg = out["msg"] if is_tsb else out
        # Connect output to kafka
        message_publisher_adaptor.from_graph(out_msg, path=topic)
        return out if is_tsb else None

    return message_publisher_graph


@adaptor
def message_subscriber(fn: Callable = None, *, topic: str, replay_history: bool = False):
    """
    Subscribe to a kafka topic, the path binds to the topic. The values are provided to ``msg``. This is an example:

    ::

        @message_subscriber(topic="my_topic")
        def my_fn(msg: TS[bytes]):
            ...

    If ``replay_history`` is True, the subscriber will replay
    the history of the topic and then continue to process new data. If the function has a parameter called
    ``recovered: TS[bool]`` it will tick True when the subscriber has recovered the history data.

    ::

        @message_subscriber(topic="my_topic")
        def my_fn(msg: TS[bytes], recovered: TS[bool]):
            ...

    It is also possible to supply additional arguments and return additional values.

    To ensure we only wire in logic intended for use, even if the implementation only has message inputs,
    the function must still be called in the main wiring graph to ensure it gets used.
    """
    if fn is None:
        return lambda fn: message_subscriber(fn, topic=topic, replay_history=replay_history)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "msg" in fn.signature.time_series_inputs.keys(), "message_subscriber graph must have an input named 'msg'"
    has_recovered = "recovered" in fn.signature.time_series_inputs.keys() if replay_history else False

    output_type = fn.signature.output_type

    @graph
    @with_signature(
        kwargs={
            k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k not in ("msg", "recovered")
        },
        return_annotation=output_type,
    )
    def message_subscriber_graph(**kwargs):
        MessagingState.instance().add_publisher(topic, replay_history)
        msg_input = message_subscriber_adaptor(path=topic)
        kwargs["msg"] = msg_input["msg"]
        if has_recovered:
            kwargs["recovered"] = msg_input["recorded"]  # Connect recovered signal
        else:
            null_sink(msg_input["recorded"])
        out = fn(**kwargs)
        return out

    return message_subscriber_graph


MESSAGE_ADAPTOR_PATH = "service.messaging"


@dataclass
class MessagingState:
    """Tracks the registered topics and their replay state."""
    subscribers: dict[str, tuple[str, bool]] = field(default_factory=dict)
    publishers: dict[str, tuple[str, bool]] = field(default_factory=dict)

    @classmethod
    def instance(cls) -> "MessagingState":
        if "service.messaging.state" not in (gs := GlobalState.instance()):
            gs["service.messaging.state"] = cls()
        return gs["service.messaging.state"]

    def add_subscriber(self, topic: str, replay_history: bool):
        self.subscribers[topic] = (topic, replay_history)

    def add_publisher(self, topic: str, replay_history: bool):
        self.publishers[topic] = (topic, replay_history)


@adaptor
def message_publisher_adaptor(msg: TS[bytes], path: str = MESSAGE_ADAPTOR_PATH) -> TS[bytes]:
    """Publisher adaptor for kafka, The input is what needs to be published, the output is to be
    used when we are in recovery."""


@adaptor
def message_subscriber_adaptor(path: str = MESSAGE_ADAPTOR_PATH) -> TSB[{"msg": TS[bytes], "recovered": TS[bool]}]:
    """
    Subscriber adaptor for kafka, output contains the msg and a recovered flag.
    """
