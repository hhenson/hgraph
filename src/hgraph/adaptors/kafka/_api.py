from dataclasses import dataclass, field
from typing import Callable

from hgraph import adaptor, TS, graph, HgTSBTypeMetaData, TSB, with_signature, null_sink, GlobalState, \
    reference_service, register_adaptor, adaptor_impl, debug_print, const, convert, combine, service_impl, \
    register_service, MIN_TD, switch_, default, if_then_else

__all__ = ("message_publisher", "message_subscriber",)


def message_publisher(fn: Callable = None, *, topic: str):
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

    if "msg" in fn.signature.time_series_args or 'recovered' in fn.signature.time_series_args:
        assert (
                "msg" in fn.signature.time_series_inputs.keys()
        ), "kafka_publisher graph must have an input named 'msg' when defining replay args"
        assert fn.signature.time_series_inputs["msg"].matches_type(
            TS[bytes]
        ), f"Graph must have an input named 'msg' of type TS[bytes] got {fn.signature.time_series_inputs['msg']}"
        assert ('recovered' in fn.signature.time_series_inputs.keys()), \
            "kafka_publisher graph must have an input named 'recovered' when defining replay args"
        assert fn.signature.time_series_inputs['recovered'].matches_type(TS[bool]), \
            f"Graph input named 'recovered' must be of of type TS[bool] got {fn.signature.time_series_inputs['recovered']}"
        replay_history = True

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
            msg_history = message_history_subscriber_service(path=topic, topic=topic)
            kwargs["msg"] = msg_history["msg"]  # Connect replay
            kwargs["recovered"] = msg_history["recovered"]  # Connect replay
        else:
            null_sink(msg_input)
        out = fn(**kwargs)
        out_msg = out["msg"] if is_tsb else out
        # Connect output to kafka
        message_publisher_adaptor.from_graph(out_msg, path=topic)
        return out if is_tsb else None

    return message_publisher_graph


# TODO: It may be better to move the replay_history into the method of the function?
# Then we could deal with differences in requirements for subscribers desire for history.
# Another option would be to only re-play history in real-time mode if the input signature
# includes the recovered input. Then we don't need this flag at all.
# In simulation mode all messages are only replayed by default.
def message_subscriber(fn: Callable = None, *, topic: str):
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
    assert fn.signature.time_series_inputs["msg"].matches_type(TS[bytes]), \
        f"The input named 'msg' must be of type TS[bytes] got {fn.signature.time_series_inputs['msg']}"
    has_recovered = "recovered" in fn.signature.time_series_inputs.keys()
    assert not has_recovered or fn.signature.time_series_inputs["recovered"].matches_type(TS[bool]), \
        f"The input named 'recovered' must be of type TS[bool] got {fn.signature.time_series_inputs['recovered']}"

    output_type = fn.signature.output_type

    @graph
    @with_signature(
        kwargs={
            k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if
            k not in ("msg", "recovered")
        },
        return_annotation=output_type,
    )
    def message_subscriber_graph(**kwargs):
        MessagingState.instance().add_subscriber(topic, has_recovered)
        msg_input = message_subscriber_service(path=topic)
        if has_recovered:
            msg_history = message_history_subscriber_service(path=topic)
            debug_print(f"msg_history", msg_history)
            kwargs["recovered"] = (recovered := msg_history["recovered"])  # Connect recovered signal
            msg_input = if_then_else(
                default(recovered, False),
                msg_input,
                msg_history["msg"]
            )
        kwargs["msg"] = msg_input
        out = fn(**kwargs)
        return out

    return message_subscriber_graph


@dataclass
class MessagingState:
    """Tracks the registered topics and their replay state."""
    subscribers: set[str] = field(default_factory=set)
    history_subscribers: set[str] = field(default_factory=set)
    publishers: set[str] = field(default_factory=set)

    @classmethod
    def instance(cls) -> "MessagingState":
        if "service.messaging.state" not in (gs := GlobalState.instance()):
            gs["service.messaging.state"] = cls()
        return gs["service.messaging.state"]

    def add_subscriber(self, topic: str, replay_history: bool):
        if topic not in self.subscribers:
            self.subscribers.add(topic)
            register_service(topic, _message_subscriber_aggregator, topic=topic)
        if replay_history and topic not in self.history_subscribers:
            self.history_subscribers.add(topic)
            register_service(topic, _message_subscriber_history_aggregator, topic=topic)

    def add_publisher(self, topic: str, replay_history: bool):
        if topic in self.publishers:
            # There can only be one publisher per topic.
            raise ValueError(f"Topic {topic} already has a publisher")
        self.publishers.add(topic)
        register_adaptor(topic, _message_publisher_aggregator, topic=topic)
        if replay_history and topic not in self.history_subscribers:
            self.history_subscribers.add(topic)
            register_service(topic, _message_subscriber_history_aggregator, topic=topic)


@adaptor
def message_publisher_adaptor(msg: TS[bytes], path: str) -> TS[bytes]:
    """Publisher adaptor for kafka, The input is what needs to be published, the output is to be
    used when we are in recovery."""


@reference_service
def message_history_subscriber_service(path: str) -> TSB["msg": TS[bytes], "recovered": TS[bool]]:
    """Only retrieve history, after which the topic can be unsubscribed."""


@reference_service
def message_subscriber_service(path: str) -> TS[bytes]:
    """
    Subscriber for kafka, output contains the msg and a recovered flag.
    """


@adaptor_impl(interfaces=(message_publisher_adaptor,))
def _message_publisher_aggregator(path: str, msg: TS[bytes], topic: str) -> TS[bytes]:
    debug_print(f"publish topic: {topic}", msg)
    topic_b = b'pub: ' + topic.encode("utf-8")
    return const(topic_b)


@service_impl(interfaces=(message_subscriber_service,))
def _message_subscriber_aggregator(path: str, topic: str) -> TS[bytes]:
    print(f"subscribe topic: {topic}")
    topic_b = b'sub: ' + topic.encode("utf-8")
    return const(topic_b, delay=MIN_TD*2)


@service_impl(interfaces=(message_history_subscriber_service,))
def _message_subscriber_history_aggregator(path: str, topic: str) -> TSB["msg": TS[bytes], "recovered": TS[bool]]:
    """Recovered must tick after the last message has been delivered."""
    print(f"subscribe topic: {topic}")
    topic_b = b'sub_h: ' + topic.encode("utf-8")
    return combine[TSB](msg=const(topic_b), recovered=const(True, delay=MIN_TD))
