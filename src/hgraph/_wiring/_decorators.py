from inspect import signature, isfunction
from typing import TypeVar, Callable, Type, Sequence, TYPE_CHECKING, Mapping, Any

from frozendict import frozendict

from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import extract_injectable_inputs

if TYPE_CHECKING:
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

__all__ = (
    "operator",
    "compute_node",
    "pull_source_node",
    "push_source_node",
    "sink_node",
    "graph",
    "generator",
    "reference_service",
    "request_reply_service",
    "subscription_service",
    "default_path",
    "service_impl",
    "service_adaptor",
    "register_service",
    "push_queue",
)

SOURCE_NODE_SIGNATURE = TypeVar("SOURCE_NODE_SIGNATURE", bound=Callable)
COMPUTE_NODE_SIGNATURE = TypeVar("COMPUTE_NODE_SIGNATURE", bound=Callable)
SINK_NODE_SIGNATURE = TypeVar("SINK_NODE_SIGNATURE", bound=Callable)
GRAPH_SIGNATURE = TypeVar("GRAPH_SIGNATURE", bound=Callable)


def operator(fn: GRAPH_SIGNATURE, deprecated: bool | str = False) -> GRAPH_SIGNATURE:
    """
    Used to define a name and partial signature of a graph operator. A graph operator is a function that
    operates on more or more time-series values, typically producing a time-series value.

    An operator cannot have an implementation and requires an override by a relevant graph node instance.

    For example:
    ::

        @operator
        def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
            ...

        @compute_node(overloads=add_)
        def add_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
            ...

    In this case, we define a generic `add_` operator; this is then overloaded to implement the operator over
    two TS values.

    Note that with an operator, the exact signature is not enforced and is provided to better support type
    hinting, but as with C++ templates, the implementation can overwrite the exact signature as required.

    Thus, it is possible to perform the following override:
    ::
        @compute_node(overloads=add_)
        def add_ts_date(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]:
            ...

    The overload mechanism attempts to match the provided inputs to the implementation that is the closest fit.

    All overloads need to be imported to work, thus when overloading an operator, it is important to make sure
    the overload would have been imported prior to using. This can be done by making sure the overloads are included
    in a __init__ (or chain thereof) where importing the top-level package ensures all the children are imported.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.OPERATOR,
        fn,
        None,
        None,
        None,
        None,
        overloads=None,
        resolvers=None,
        requires=None,
        deprecated=deprecated,
    )


def compute_node(
    fn: COMPUTE_NODE_SIGNATURE = None,
    /,
    node_impl=None,
    active: Sequence[str] | Callable = None,
    valid: Sequence[str] | Callable = None,
    all_valid: Sequence[str] | Callable = None,
    overloads: "WiringNodeClass" | COMPUTE_NODE_SIGNATURE = None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> COMPUTE_NODE_SIGNATURE:
    """
    Marks a function as being a compute node in the graph, a compute node accepts one or more time-series inputs and
    returns a time-series value. The compute node performs work whenever an input (marked as active) is modified.

    Example:
    ::

        @compute_node
        def add_ts(lhs: TS[NUMBER], rhs: TS[NUMBER]) -> TS[NUMBER]:
            return lhs.value + rhs.value

    In this example, the code accepts two time-series inputs and returns the sum of the two input values.

    A compute node can also define some useful parameters to describe the initial state, perform input validations,
    automatically determine type-signatures, etc.

    The `active`, `valid`, and `all_valid` parameters' control how the compute node being defined will be marked for
    evaluation.

    The `active` parameter lists the input names that should be marked as active, if no list is provided then all inputs
    are marked as being active.

    For example:
    ::
        @compute_node(active('trade_request',))
        def accept_trade_request(trade_request: TS[Trade], market_data: TS[float]) -> TS[bool]:
            return trade_request.value.price == market_data.value

    In the above example, we only accept trade requests if `price` on the request is exactly the same as the market_data.
    In this scenario, we only care to respond to trade requests and not market data. By marking the `trade_request` as
    being active, it implies the market data is passive, or in other words, it will not activate the logic when the
    market data changes. Marking an input passive does not mean the value will be out of date, the value is always
    up-to-date; it merely ensures the function is not activated when the input is modified.

    'valid' works in a similar way to active, in that if it is not set, all inputs are required to be valid before the
    function will be called. If set, then only the nodes that are listed are included in the guard. When using this the
    function is required to test if an input is valid when not explicitly listed.

    `all_valid`, when not specified, is defaulted to not requiring the inputs to be all valid. For collection time-series
    values such as: TSB, TSD, and TSL; if any of the elements are valid, then the collection is marked as valid.
    However, there are times when you want to ensure all inputs of a collection are valid, in this case mark the inputs
    in the all_valid clause. This will ensure the function is only evaluated when this state is true.

    The `overload` argument allows the node to be marked as implementing an `operator`, see help on the :func:`operator`
    decorator for more information.

    Resolvers allow the user to provide additional logic to determine a resolution of a `TypeVar`. The `resolver`
    argument is set as a dictionary mapping the type var to be resolved and a function that will be able to resolve
    the type.
    For example:
    ::
        def _resolve_type(mapping: dict[TypeVar, type], scalars: dict[str, Any]) -> type:
            schema = mapping[TS_SCHEMA]  # resolved as it is an input in to the node
            attr = scalars['attr']
            return schema.__meta_data_schema__[attr].value_type.py_type

        @compute_node(resolvers={SCALAR: _resolve_type})
        def get_attr_tsb(ts: TSB[TS_SCHEMA], attr: str) -> TS[SCALAR]:
            ...

    In the above example, the `_resolve_type` function gets the resolved schema, extracts out the type of the attr
    and returns the type, which is the resolution of the SCALAR type var.

    The `requires` argument is similar to the `resolver` argument, but only takes a single function whose responsibility
    is to determine if the provided inputs meet with the requirements of the node.
    For example:
    ::
        def _requires_enum_values(_resolve_type(mapping: dict[TypeVar, type], scalars: dict[str, Any]):
            if scalars['rw_flags'] not in ('r', 'w', 'rw'):
                raise ValueError("rw_flags must be one of 'r', 'w' or 'rw'")

        @compute_node(requires=_requires_enum_values)
        def some_func(ts: TS[float], rw_flags: str) -> TS[float]:
            ...

    In the above example, the `requires` function ensures the provided input strings match a valid list.

    :param fn: The function to wrap
    :param node_impl: The node implementation to use (this makes fn a signature only method)
    :param active: Which inputs to mark as being active (by default all are active)
    :param valid: Which inputs to require to be valid (by default all are valid)
    :param all_valid: Which inputs are required to be ``all_valid`` (by default none are all_valid)
    :param overloads: If this node overloads an operator, this is the operator it is designed to overload.
    :param resolvers: A resolver method to assist with resolving types when they can be inferred but not deduced
                      directly.
    :param requires: Callable which accepts the mapping and scalars as parameters and validates the inputs meet with
                     the requirements defined in the function.
    :param deprecated: Marks the node as no longer supported and likely to be removed shortly
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.COMPUTE_NODE,
        fn,
        node_impl,
        active,
        valid,
        all_valid,
        overloads=overloads,
        resolvers=resolvers,
        requires=requires,
        deprecated=deprecated,
    )


def pull_source_node(
    fn: SOURCE_NODE_SIGNATURE = None,
    /,
    node_impl=None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> SOURCE_NODE_SIGNATURE:
    """
    Used to indicate the signature for a source node. For Python source nodes use either the
    generator or source_adapter annotations.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.PULL_SOURCE_NODE, fn, node_impl, resolvers=resolvers, requires=requires, deprecated=deprecated
    )


def push_source_node(
    fn: SOURCE_NODE_SIGNATURE = None,
    /,
    node_impl=None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> SOURCE_NODE_SIGNATURE:
    """
    Used to indicate the signature for a push source node.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.PUSH_SOURCE_NODE, fn, node_impl, resolvers=resolvers, requires=requires, deprecated=deprecated
    )


def sink_node(
    fn: SINK_NODE_SIGNATURE = None,
    /,
    node_impl=None,
    active: Sequence[str] | Callable = None,
    valid: Sequence[str] | Callable = None,
    all_valid: Sequence[str] | Callable = None,
    overloads: "WiringNodeClass" | SINK_NODE_SIGNATURE = None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> SINK_NODE_SIGNATURE:
    """
    A sink node is a node in the graph that accepts one or more time-series inputs and produces no output.
    These nodes are leaf nodes of the graph and generally the only nodes in the graph that we expect to have side
    effects. Examples of sink nodes include: writing to the output stream, network, database, etc.
    ::
        @sink_node
        def print_(format_str: str, value: TS[SCALAR]):
            print(format_str.format(value.value))


    The remaining arguments are the same as described in the :func:`compute_node` decorator.

    :param fn: The function to wrap
    :param node_impl: The node implementation to use (this makes fn a signature only method)
    :param active: Which inputs to mark as being active (by default all are active)
    :param valid: Which inputs to require to be valid (by default all are valid)
    :param all_valid: Which inputs are required to be ``all_valid`` (by default none are all_valid)
    :param overloads: If this node overloads an operator, this is the operator it is designed to overload.
    :param resolvers: A resolver method to assist with resolving types when they can be inferred but not deduced
                      directly.
    :param requires: Callable which accepts the mapping and scalars as parameters and validates the inputs meet with
                     the requirements defined in the function.
    :param deprecated: Marks the node as no longer supported and likely to be removed shortly
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.SINK_NODE,
        fn,
        node_impl,
        active,
        valid,
        all_valid,
        overloads=overloads,
        resolvers=resolvers,
        requires=requires,
        deprecated=deprecated,
    )


def graph(
    fn: GRAPH_SIGNATURE = None,
    overloads: "WiringNodeClass" | GRAPH_SIGNATURE = None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> GRAPH_SIGNATURE:
    """
    The `graph` decorator represents a function that contains wiring logic. Wiring logic is only evaluated once
    when the graph is created and is used to indicate which nodes to construct and how to connect the input and outputs
    of the nodes (or the edges of the graph). It is important to note that the logic of the function does not do any
    work and merely describes the shape of the runtime graph. A graph can take time-series inputs and can return
    a time-series value, but this is not a requirement. Typcially the main graph will only take scalar value inputs
    for configuration (or take no imputs at all).
    ::

        @graph
        def my_graph():
            c = const(1)
            debug_print('c', c)

    This is the smallest meaningful graph.

    The graph signature, whilst being more flexible takes the same form as for a source, compute or sink node.
    A common design principle would be to describe behaviour with graph's initially and then convert to the appropriate
    nodes once the logic is decomposed sufficiently. It is also possible to re-code a node as a graph, for example:
    ::

        @compute_node
        def a_plus_b_plus_c(a: TS[float], b: TS[float]) -> TS[float]:
            return a.value + b.value + c.value

    Can be reworked to be:
    ::

        @graph
        def a_plus_b_plus_c(a: TS[float], b: TS[float]) -> TS[float]:
            return a+b+c

    Or visa-versa. The trade-off is, typically, fewer compute nodes can be faster to evaluate, but `graph`s are far
    better at re-use of existing components. The preference should always be to use graph logic before constructing
    node functions.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.GRAPH, fn, overloads=overloads, resolvers=resolvers, requires=requires, deprecated=deprecated
    )


def generator(
    fn: SOURCE_NODE_SIGNATURE = None,
    overloads: "WiringNodeClass" | GRAPH_SIGNATURE = None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> SOURCE_NODE_SIGNATURE:
    """
    Creates a pull source node that supports generating a sequence of ticks that will be fed into the
    graph. The generator wraps a function that is implemented as a python generator which returns a tuple of
    time (or timedelta) and value.

    For example:
    ```Python

    @generator
    def signal() -> TS[bool]:
        while True:
            yield (timedelta(milliseconds=1), True)

    ```

    This will cause an infinite sequence of ticks (with value of True) that will tick one a millisecond.

    The generator will fetch the first tick during the start life-cycle of the node. If no tick is returned, the
    generator WILL do NOTHING.

    """
    from hgraph._wiring._wiring_node_class._python_wiring_node_classes import PythonGeneratorWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.PULL_SOURCE_NODE,
        fn,
        overloads=overloads,
        node_class=PythonGeneratorWiringNodeClass,
        resolvers=resolvers,
        requires=requires,
        deprecated=deprecated,
    )


def push_queue(
    tp: type[TIME_SERIES_TYPE],
    overloads: "WiringNodeClass" | SOURCE_NODE_SIGNATURE = None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
):
    """
    Creates a push source node that supports injecting values into the graph asynchronously.
    The function that is wrapped by this decorator will be called as a start lifecycle method.
    The function must take as its first parameter the sender callable.
    It is possible to take additional scalar values that will be provided as kwargs.

    For example,

    ```Python
        @push_queue(TS[bool])
        def my_message_sender(sender: Callable[[SCALAR], None):
            ...
    ```
    """
    from hgraph._wiring._wiring_node_class._python_wiring_node_classes import PythonPushQueueWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    def _(fn):
        sig = signature(fn)
        sender_arg = next(iter(sig.parameters.keys()))
        annotations = {k: v.annotation for k, v in sig.parameters.items() if k != sender_arg}
        defaults = {k: v.default for k, v in sig.parameters.items() if k != sender_arg}

        node = _create_node(
            _create_node_signature(
                fn.__name__,
                annotations,
                tp,
                defaults=defaults,
                node_type=WiringNodeType.PUSH_SOURCE_NODE,
                deprecated=deprecated,
                requires=requires,
            ),
            impl_fn=fn,
            node_type=WiringNodeType.PUSH_SOURCE_NODE,
            node_class=PythonPushQueueWiringNodeClass,
        )

        if resolvers is not None:
            node = node[tuple(slice(k, v) for k, v in resolvers.items())]

        if overloads is not None:
            overloads.overload(node)
            return node
        else:
            return node

    return _


SERVICE_DEFINITION = TypeVar("SERVICE_DEFINITION", bound=Callable)

default_path = None


def subscription_service(
    fn: SERVICE_DEFINITION = None, resolvers: Mapping[TypeVar, Callable] = None
) -> SERVICE_DEFINITION:
    """
    A subscription service is a service where the input receives a subscription key and then
    streams back results. This looks like:

        default=None

        @subscription_service
        def my_subscription_svc(path: str | None, ts1: TS[str]) -> TS[float]:
            ...

        @service_impl(interface=my_subscription_svc)
        def my_subscription_svc_impl(ts1: TSD[RequesterId, TS[str]]) -> TSD[RequesterId, TS[float]]:
            ...

        @graph
        def my_code():
            register_service(default, my_subscription_svc, my_subscription_svc_impl)
            ...
            out = my_subscription_svc(default, ts1="mkt_data.mcu_3m")
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(WiringNodeType.SUBS_SVC, fn, resolvers=resolvers)


def reference_service(
    fn: SERVICE_DEFINITION = None, resolvers: Mapping[TypeVar, Callable] = None
) -> SERVICE_DEFINITION:
    """
    A reference service is a service that only produces a value that does not vary by request.
    The pattern for a reference services is the same as a source node.

    for example:

        @reference_service
        def my_reference_service(path: str | None) -> OUT_TIME_SERIES:
            ...

    if path is not provided or defined in the configuration it is assumed there will only be one bound instance
    and that bound instance will be to the path 'ref_svc://<module>.<svc_name>' for example:
    'ref_svc://a.b.c.my_reference_service'

    The implementation needs to be registered by the outer wiring node, if not registered, it will look for a remote
    instance of the service to bind to.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(WiringNodeType.REF_SVC, fn, resolvers=resolvers)


def request_reply_service(
    fn: SERVICE_DEFINITION = None, resolvers: Mapping[TypeVar, Callable] = None
) -> SERVICE_DEFINITION:
    """
    A request-reply service takes a request and returns a response, error or time-out.
    for example:

        class RequestReplyService(Generic[TIME_SERIES_TYPE_1]):
            result: TIME_SERIES_TYPE_1
            time_out: TS[bool]
            error: TS[str]

        @request_reply_service
        def my_request_reply(path: str | None, request: TIME_SERIES_TYPE) -> TSB[ReqRepResponse[TIME_SERIES_TYPE_1]]:
            ...

    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(WiringNodeType.REQ_REP_SVC, fn, resolvers=resolvers)


def service_impl(
    *,
    interfaces: Sequence[SERVICE_DEFINITION] | SERVICE_DEFINITION = None,
    resolvers: Mapping[TypeVar, Callable] = None,
    deprecated: bool | str = False,
):
    """
    Wraps a service implementation. The service is defined to implement the declared interface.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return _node_decorator(
        WiringNodeType.SVC_IMPL, None, interfaces=interfaces, resolvers=resolvers, deprecated=deprecated
    )


def register_service(path: str, implementation, resolution_dict=None, **kwargs):
    """
    Binds the implementation of the interface to the path provided. The additional kwargs
    are passed to the implementation. These should be defined on the implementation and are independent of the
    attributes defined in the service.
    :param path:
    :param implementation:
    :param resolution_dict:
    :param kwargs:
    :return:
    """
    from hgraph._wiring._wiring_node_class._wiring_node_class import PreResolvedWiringNodeWrapper
    from hgraph._wiring._wiring_node_class._service_impl_node_class import ServiceImplNodeClass

    if isinstance(implementation, PreResolvedWiringNodeWrapper):
        implementation = implementation.underlying_node
        resolution_dict = implementation.resolved_types or {}
    else:
        resolution_dict = implementation._convert_item(resolution_dict) if resolution_dict else {}

    if not isinstance(implementation, ServiceImplNodeClass):
        raise CustomMessageWiringError("The provided implementation is not a 'service_impl' wrapped function.")

    from hgraph import WiringGraphContext

    for i in implementation.interfaces:
        WiringGraphContext.instance().register_service_impl(
            i, i.full_path(path), implementation, kwargs, resolution_dict
        )


def service_adaptor(interface):
    """
    @service
    def my_interface(ts1: TIME_SERIES, ...) -> OUT_TIME_SERIES:
        pass

    @service_adapter(my_interface)
    class MyAdapter:

        def __init__(self, sender: Sender, ...):
            ''' The sender has a method called send on it that takes a Python object that will enqueue into the out
                shape, use this to send a message'''

        def on_data(ts1: TIME_SERIES, ...):
            ''' Is called each time one of the inputs ticks '''

    Use the register_service method to with the class as the impl value.
    """


def _node_decorator(
    node_type: "WiringNodeType",
    impl_fn,
    node_impl=None,
    active: Sequence[str] | Callable = None,
    valid: Sequence[str] | Callable = None,
    all_valid: Sequence[str] | Callable = None,
    node_class: Type["WiringNodeClass"] = None,
    overloads: "WiringNodeClass" = None,
    interfaces=None,
    resolvers: Mapping[TypeVar, Callable] = None,
    requires: Callable[[..., ...], bool] = None,
    deprecated: bool | str = False,
) -> Callable:
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
    from hgraph._wiring._wiring_node_class._node_impl_wiring_node_class import NodeImplWiringNodeClass
    from hgraph._wiring._wiring_node_class._graph_wiring_node_class import GraphWiringNodeClass
    from hgraph._wiring._wiring_node_class._python_wiring_node_classes import PythonWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    kwargs = dict(
        node_type=node_type,
        node_class=PythonWiringNodeClass if node_class is None else node_class,
        active=active,
        valid=valid,
        all_valid=all_valid,
        interfaces=interfaces,
        deprecated=deprecated,
        requires=requires,
    )
    if node_impl is not None:
        if isinstance(node_impl, type) and issubclass(node_impl, WiringNodeClass):
            kwargs["node_class"] = node_impl
        else:
            kwargs["node_class"] = NodeImplWiringNodeClass
            kwargs["impl_fn"] = node_impl

    interfaces = kwargs.pop("interfaces")
    match node_type:
        case WiringNodeType.OPERATOR:
            from hgraph._wiring._wiring_node_class._operator_wiring_node import OperatorWiringNodeClass

            kwargs["node_class"] = OperatorWiringNodeClass
            _assert_no_node_configs("Operators", kwargs)
        case WiringNodeType.GRAPH:
            kwargs["node_class"] = GraphWiringNodeClass
            _assert_no_node_configs("Graphs", kwargs)
        case WiringNodeType.REF_SVC:
            from hgraph._wiring._wiring_node_class._reference_service_node_class import ReferenceServiceNodeClass

            kwargs["node_class"] = ReferenceServiceNodeClass
            _assert_no_node_configs("Reference Services", kwargs)
        case WiringNodeType.SUBS_SVC:
            from hgraph._wiring._wiring_node_class._subscription_service_node_service import (
                SubscriptionServiceNodeClass,
            )

            kwargs["node_class"] = SubscriptionServiceNodeClass
            _assert_no_node_configs("Subscription Services", kwargs)
        case WiringNodeType.REQ_REP_SVC:
            from hgraph._wiring._wiring_node_class._req_repl_service_node_service import RequestReplyServiceNodeClass

            kwargs["node_class"] = RequestReplyServiceNodeClass
            _assert_no_node_configs("Request Reply Services", kwargs)
        case WiringNodeType.SVC_IMPL:
            from hgraph._wiring._wiring_node_class._service_impl_node_class import ServiceImplNodeClass

            kwargs["node_class"] = ServiceImplNodeClass
            kwargs["interfaces"] = interfaces
            _assert_no_node_configs("Service Impl", kwargs)

    if overloads is not None and impl_fn is None:
        kwargs["overloads"] = overloads

    if impl_fn is None:
        if "impl_fn" in kwargs:
            return lambda fn: _create_node(fn, **kwargs)
        else:
            return lambda fn: _node_decorator(impl_fn=fn, **kwargs, resolvers=resolvers)
    elif overloads is not None:
        overload = _create_node(impl_fn, **kwargs)
        if resolvers is not None:
            overload = overload[tuple(slice(k, v) for k, v in resolvers.items())]
        overloads.overload(overload)
        return overload
    else:
        node = _create_node(impl_fn, **kwargs)
        if resolvers is not None:
            node = node[tuple(slice(k, v) for k, v in resolvers.items())]
        return node


def _assert_no_node_configs(label: str, kwargs):
    if kwargs.get("active") is not None:
        raise ValueError(f"{label} do not support ticked")
    if kwargs.get("valid") is not None:
        raise ValueError(f"{label} do not support valid")
    if kwargs.get("all_valid") is not None:
        raise ValueError(f"{label} do not support all_valid")


def _set_or_lambda(value):
    if value is None:
        return None
    elif isfunction(value) and value.__name__ == "<lambda>":
        return value
    elif isinstance(value, str):
        return frozenset({value})
    else:
        return frozenset(value)


def _create_node(
    signature_fn,
    impl_fn=None,
    node_type: "WiringNodeType" = None,
    node_class: Type["WiringNodeClass"] = None,
    active: Sequence[str] | Callable = None,
    valid: Sequence[str] | Callable = None,
    all_valid: Sequence[str] | Callable = None,
    interfaces=None,
    deprecated: bool | str = False,
    requires: Callable[[..., ...], bool] = None,
) -> "WiringNodeClass":
    """
    Create the wiring node using the supplied node_type and impl_fn, for non-cpp types the impl_fn is assumed to be
    the signature fn as well.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature, extract_signature

    if impl_fn is None:
        impl_fn = signature_fn

    active_inputs = _set_or_lambda(active)
    valid_inputs = _set_or_lambda(valid)
    all_valid_inputs = _set_or_lambda(all_valid)

    signature = (
        signature_fn
        if isinstance(signature_fn, WiringNodeSignature)
        else extract_signature(
            signature_fn,
            node_type,
            active_inputs=active_inputs,
            valid_inputs=valid_inputs,
            all_valid_inputs=all_valid_inputs,
            deprecated=deprecated,
            requires=requires,
        )
    )
    if interfaces is None:
        return node_class(signature, impl_fn)
    else:
        return node_class(signature, impl_fn, interfaces=interfaces)


def _create_node_signature(
    name: str,
    kwargs: dict[str, Type],
    ret_type: Type,
    node_type: "WiringNodeType",
    active_inputs: frozenset[str] = None,
    valid_inputs: frozenset[str] = None,
    all_valid_inputs: frozenset[str] = None,
    defaults: dict[str, Any] = None,
    deprecated: bool | str = False,
    requires: Callable[[..., ...], bool] | None = None,
) -> Callable:
    """
    Create a function that takes the kwargs and returns the kwargs. This is used to create a function that
    can be used to create a signature.
    """
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeSignature
    from hgraph import HgScalarTypeMetaData, HgTimeSeriesTypeMetaData

    from hgraph import SourceCodeDetails
    from pathlib import Path

    signature = WiringNodeSignature(
        node_type=node_type,
        name=name,
        args=tuple(kwargs.keys()),
        defaults=frozendict() if defaults is None else frozendict(defaults),
        input_types=frozendict({k: HgScalarTypeMetaData.parse_type(v) for k, v in kwargs.items()}),
        output_type=HgTimeSeriesTypeMetaData.parse_type(ret_type) if ret_type is not None else None,
        src_location=SourceCodeDetails(Path(), 0),  # TODO: make this point to a real place in code.
        active_inputs=active_inputs,
        valid_inputs=valid_inputs,
        all_valid_inputs=all_valid_inputs,
        context_inputs=None,
        unresolved_args=frozenset(),
        time_series_args=frozenset(),
        injectable_inputs=extract_injectable_inputs(**kwargs),
        deprecated=deprecated,
        requires=requires,
    )
    return signature


CALLABLE = TypeVar("CALLABLE", bound=Callable)
