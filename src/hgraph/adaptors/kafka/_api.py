from typing import Callable

from hgraph import adaptor, TS, graph, HgTSBTypeMetaData, TSB, ts_schema, with_signature, null_sink


def kafka_publisher(fn: Callable = None, *, topic: str, replay_history: bool = False):
    """
    Wraps a publisher function as a publisher for a kafka topic.
    The function should return a ``TS[bytes]`` that will be published to the topic.
    The default signature looks like this:

    ::

        @kafka_publisher(topic="my_topic")
        def my_fn() -> TS[bytes]:
            ...

    If ``replay_history`` is True, the publisher will replay the history of the topic before processing any new data.
    The data will be started from ``start_time`` provided to the graph engine. This should be set to the first time
    the data needs to be processed. To support replay capabilities, the function must accept a parameter
    ``msg: TS[bytes]``, on which the history will be replayed.

    ::

        @kafka_publisher(topic="my_topic", replay_history=True))
        def my_fn(msg: TS[bytes]) -> TS[bytes]:
            ...

    The function can also take additional arguments. If the function has to return additional values, then there
    the output type should be a TSB with one of the outputs being ``msg: TS[bytes]``. For example:

    ::

        @kafka_publisher(topic="my_topic")
        def my_fn(arg1: TS[int], arg2: TS[str]) -> TSB[{"msg": TS[bytes], "out_1": TS[int], "out_2": TS[str]}]:
            ...

    In this form, the adaptor will capture the msg output, but it will also be returned as part of the bundle.
    """
    if fn is None:
        return lambda fn: kafka_publisher(fn, topic=topic, replay_history=replay_history)

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

    assert (single_value := output_type.matches_type(TS[bytes])), "Graph must have a message output of type TS[bytes]"

    final_output_type = fn.signature.output_type if is_tsb else None

    # If inputs or outputs are not standard, then we use the graph, otherwise we can wire up?

    @graph
    @with_signature(
        kwargs={k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k != "msg"},
        return_annotation=final_output_type,
    )
    def kafka_publisher_graph(**kwargs):
        msg_input = kafka_publisher_adaptor.to_graph(path=topic, __no_ts_inputs__=True)
        if replay_history:
            kwargs["msg"] = msg_input  # Connect replay
        else:
            null_sink(msg_input)
        out = fn(**kwargs)
        out_msg = out["msg"] if is_tsb else out
        # Connect output to kafka
        kafka_publisher_adaptor.from_graph(out_msg, path=topic)
        return out if is_tsb else None

    return kafka_publisher_graph


@adaptor
def kafka_subscriber(fn: Callable = None, *, topic: str, replay_history: bool = False):
    """
    Subscribe to a kafka topic, the path binds to the topic. The values are provided to ``msg``. This is an example:

    ::

        @kafka_subscriber(topic="my_topic")
        def my_fn(msg: TS[bytes]):
            ...

    If ``replay_history`` is True, the subscriber will replay
    the history of the topic and then continue to process new data. If the function has a parameter called
    ``recovered: TS[bool]`` it will tick True when the subscriber has recovered the history data.

    ::

        @kafka_subscriber(topic="my_topic")
        def my_fn(msg: TS[bytes], recovered: TS[bool]):
            ...

    It is also possible to supply additional arguments and return additional values.
    """
    if fn is None:
        return lambda fn: kafka_subscriber(fn, topic=topic, replay_history=replay_history)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "msg" in fn.signature.time_series_inputs.keys(), "kafka_subscriber graph must have an input named 'msg'"
    has_recovered = "recovered" in fn.signature.time_series_inputs.keys() if replay_history else False

    output_type = fn.signature.output_type

    # If inputs or outputs are not standard, then we use the graph, otherwise we can wire up?

    @graph
    @with_signature(
        kwargs={
            k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k in ("msg", "recovered")
        },
        return_annotation=output_type,
    )
    def kafka_subscriber_graph(**kwargs):
        msg_input = kafka_subscriber_adaptor(path=topic)
        kwargs["msg"] = msg_input["msg"]
        if has_recovered:
            kwargs["recovered"] = msg_input["recorded"]  # Connect recovered signal
        else:
            null_sink(msg_input["recorded"])
        out = fn(**kwargs)
        return out

    return kafka_subscriber_graph


KAFKA_ADAPTOR_PATH = "service.kafka"


@adaptor
def kafka_publisher_adaptor(msg: TS[bytes], path: str = KAFKA_ADAPTOR_PATH) -> TS[bytes]:
    """Publisher adaptor for kafka, The input is what needs to be published, the output is to be
    used when we are in recovery."""


@adaptor
def kafka_subscriber_adaptor(path: str = KAFKA_ADAPTOR_PATH) -> TSB[{"msg": TS[bytes], "recovered": TS[bool]}]:
    """
    Subscriber adaptor for kafka, output contains the msg and a recovered flag.
    """
