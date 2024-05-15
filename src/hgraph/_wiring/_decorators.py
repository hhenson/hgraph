from inspect import signature
from typing import TypeVar, Callable, Type, Sequence, TYPE_CHECKING, Mapping, Any

from frozendict import frozendict

from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_signature import extract_injectable_inputs

if TYPE_CHECKING:
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

__all__ = (
    "compute_node", "pull_source_node", "push_source_node", "sink_node", "graph", "generator", "reference_service",
    "request_reply_service", "subscription_service", "default_path",
    "service_impl", "service_adaptor", "register_service", "push_queue")

SOURCE_NODE_SIGNATURE = TypeVar("SOURCE_NODE_SIGNATURE", bound=Callable)
COMPUTE_NODE_SIGNATURE = TypeVar("COMPUTE_NODE_SIGNATURE", bound=Callable)
SINK_NODE_SIGNATURE = TypeVar("SINK_NODE_SIGNATURE", bound=Callable)
GRAPH_SIGNATURE = TypeVar("GRAPH_SIGNATURE", bound=Callable)


def compute_node(fn: COMPUTE_NODE_SIGNATURE = None, /,
                 node_impl=None,
                 active: Sequence[str] = None,
                 valid: Sequence[str] = None,
                 all_valid: Sequence[str] = None,
                 overloads: "WiringNodeClass" | COMPUTE_NODE_SIGNATURE = None,
                 resolvers: Mapping[TypeVar, Callable] = None,
                 requires: Callable[[..., ...], bool] = None,
                 deprecated: bool | str = False) -> COMPUTE_NODE_SIGNATURE:
    """
    Used to define a python function to be a compute-node. A compute-node is the worker unit in the graph and
    will be called each time of the inputs to the compute node ticks.
    A compute-node requires inputs and outputs.

    :param fn: The function to wrap
    :param node_impl: The node implementation to use (this makes fn a signature only method)
    :param active: Which inputs to mark as being active (by default all are active)
    :param valid: Which inputs to require to be valid (by default all are valid)
    :param all_valid: Which inputs are required to be ``all_valid`` (by default none are all_valid)
    :param overloads: If this node overloads an operator, this is the operator it is designed to overload.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.COMPUTE_NODE, fn, node_impl, active, valid, all_valid, overloads=overloads,
                           resolvers=resolvers, requires=requires, deprecated=deprecated)


def pull_source_node(fn: SOURCE_NODE_SIGNATURE = None, /, node_impl=None,
                     resolvers: Mapping[TypeVar, Callable] = None,
                     requires: Callable[[..., ...], bool] = None,
                     deprecated: bool | str = False) -> SOURCE_NODE_SIGNATURE:
    """
    Used to indicate the signature for a source node. For Python source nodes use either the
    generator or source_adapter annotations.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.PULL_SOURCE_NODE, fn, node_impl, resolvers=resolvers, requires=requires,
                           deprecated=deprecated)


def push_source_node(fn: SOURCE_NODE_SIGNATURE = None, /, node_impl=None,
                     resolvers: Mapping[TypeVar, Callable] = None,
                     requires: Callable[[..., ...], bool] = None,
                     deprecated: bool | str = False) -> SOURCE_NODE_SIGNATURE:
    """
    Used to indicate the signature for a push source node.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.PUSH_SOURCE_NODE, fn, node_impl, resolvers=resolvers, requires=requires,
                           deprecated=deprecated)


def sink_node(fn: SINK_NODE_SIGNATURE = None, /,
              node_impl=None,
              active: Sequence[str] = None,
              valid: Sequence[str] = None,
              all_valid: Sequence[str] = None,
              overloads: "WiringNodeClass" | SINK_NODE_SIGNATURE = None,
              resolvers: Mapping[TypeVar, Callable] = None,
              requires: Callable[[..., ...], bool] = None,
              deprecated: bool | str = False) -> SINK_NODE_SIGNATURE:
    """
    Indicates the function definition represents a sink node. This type of node has no return type.
    Other than that it behaves in much the same way as compute node.

    :param fn: The function to wrap
    :param node_impl: The node implementation to use (this makes fn a signature only method)
    :param active: Which inputs to mark as being active (by default all are active)
    :param valid: Which inputs to require to be valid (by default all are valid)
    :param all_valid: Which inputs are required to be ``all_valid`` (by default none are all_valid)
    :param overloads: If this node overloads an operator, this is the operator it is designed to overload.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.SINK_NODE, fn, node_impl, active, valid, all_valid, overloads=overloads,
                           resolvers=resolvers, requires=requires, deprecated=deprecated)


def graph(fn: GRAPH_SIGNATURE = None,
          overloads: "WiringNodeClass" | GRAPH_SIGNATURE = None,
          resolvers: Mapping[TypeVar, Callable] = None,
          requires: Callable[[..., ...], bool] = None,
          ) -> GRAPH_SIGNATURE:
    """
    Wraps a wiring function. The function can take the form of a function that looks like a compute_node,
    sink_node, souce_node, or a graph with no inputs or outputs. There is generally at least one graph in
    any application. The main graph.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.GRAPH, fn, overloads=overloads, resolvers=resolvers, requires=requires)


def generator(fn: SOURCE_NODE_SIGNATURE = None,
              overloads: "WiringNodeClass" | GRAPH_SIGNATURE = None,
              resolvers: Mapping[TypeVar, Callable] = None,
              requires: Callable[[..., ...], bool] = None,
              deprecated: bool | str = False) -> SOURCE_NODE_SIGNATURE:
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
    return _node_decorator(WiringNodeType.PULL_SOURCE_NODE, fn, overloads=overloads,
                           node_class=PythonGeneratorWiringNodeClass, resolvers=resolvers, requires=requires,
                           deprecated=deprecated)


def push_queue(tp: type[TIME_SERIES_TYPE],
               overloads: "WiringNodeClass" | SOURCE_NODE_SIGNATURE = None,
               resolvers: Mapping[TypeVar, Callable] = None,
               requires: Callable[[..., ...], bool] = None,
               deprecated: bool | str = False
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

        node = _create_node(_create_node_signature(fn.__name__,
                                                   annotations, tp, defaults=defaults,
                                                   node_type=WiringNodeType.PUSH_SOURCE_NODE,
                                                   deprecated=deprecated,
                                                   requires=requires),
                            impl_fn=fn, node_type=WiringNodeType.PUSH_SOURCE_NODE,
                            node_class=PythonPushQueueWiringNodeClass)

        if resolvers is not None:
            node = node[tuple(slice(k, v) for k, v in resolvers.items())]

        if overloads is not None:
            overloads.overload(node)
            return node
        else:
            return node

    return _


SERVICE_DEFINITION = TypeVar('SERVICE_DEFINITION', bound=Callable)

default_path = None


def subscription_service(fn: SERVICE_DEFINITION = None,
                         resolvers: Mapping[TypeVar, Callable] = None) -> SERVICE_DEFINITION:
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


def reference_service(fn: SERVICE_DEFINITION = None,
                      resolvers: Mapping[TypeVar, Callable] = None) -> SERVICE_DEFINITION:
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


def request_reply_service(fn: SERVICE_DEFINITION = None,
                          resolvers: Mapping[TypeVar, Callable] = None) -> SERVICE_DEFINITION:
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


def service_impl(*, interfaces: Sequence[SERVICE_DEFINITION] | SERVICE_DEFINITION = None,
                 resolvers: Mapping[TypeVar, Callable] = None,
                 deprecated: bool | str = False):
    """
    Wraps a service implementation. The service is defined to implement the declared interface.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.SVC_IMPL, None, interfaces=interfaces, resolvers=resolvers,
                           deprecated=deprecated)


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
    from hgraph._wiring._wiring_node_class._wiring_node_class import PreResolvedWiringNodeWrapper, WiringNodeClass
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
            i, i.full_path(path), implementation, kwargs,
            resolution_dict)


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


def _node_decorator(node_type: "WiringNodeType", impl_fn, node_impl=None, active: Sequence[str] = None,
                    valid: Sequence[str] = None, all_valid: Sequence[str] = None,
                    node_class: Type["WiringNodeClass"] = None,
                    overloads: "WiringNodeClass" = None, interfaces=None,
                    resolvers: Mapping[TypeVar, Callable] = None,
                    requires: Callable[[..., ...], bool] = None,
                    deprecated: bool | str = False) -> Callable:
    from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
    from hgraph._wiring._wiring_node_class._node_impl_wiring_node_class import NodeImplWiringNodeClass
    from hgraph._wiring._wiring_node_class._graph_wiring_node_class import GraphWiringNodeClass
    from hgraph._wiring._wiring_node_class._python_wiring_node_classes import PythonWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    kwargs = dict(node_type=node_type,
                  node_class=PythonWiringNodeClass if node_class is None else node_class,
                  active=active,
                  valid=valid,
                  all_valid=all_valid,
                  interfaces=interfaces,
                  deprecated=deprecated,
                  requires=requires
                  )
    if node_impl is not None:
        if isinstance(node_impl, type) and issubclass(node_impl, WiringNodeClass):
            kwargs["node_class"] = node_impl
        else:
            kwargs['node_class'] = NodeImplWiringNodeClass
            kwargs['impl_fn'] = node_impl

    interfaces = kwargs.pop('interfaces')
    match node_type:
        case WiringNodeType.GRAPH:
            kwargs['node_class'] = GraphWiringNodeClass
            _assert_no_node_configs("Graphs", kwargs)
        case WiringNodeType.REF_SVC:
            from hgraph._wiring._wiring_node_class._reference_service_node_class import ReferenceServiceNodeClass
            kwargs['node_class'] = ReferenceServiceNodeClass
            _assert_no_node_configs("Reference Services", kwargs)
        case WiringNodeType.SUBS_SVC:
            from hgraph._wiring._wiring_node_class._subscription_service_node_service import \
                SubscriptionServiceNodeClass
            kwargs['node_class'] = SubscriptionServiceNodeClass
            _assert_no_node_configs("Subscription Services", kwargs)
        case WiringNodeType.REQ_REP_SVC:
            from hgraph._wiring._wiring_node_class._req_repl_service_node_service import RequestReplyServiceNodeClass
            kwargs['node_class'] = RequestReplyServiceNodeClass
            _assert_no_node_configs("Request Reply Services", kwargs)
        case WiringNodeType.SVC_IMPL:
            from hgraph._wiring._wiring_node_class._service_impl_node_class import ServiceImplNodeClass
            kwargs['node_class'] = ServiceImplNodeClass
            kwargs['interfaces'] = interfaces
            _assert_no_node_configs("Service Impl", kwargs)

    if overloads is not None and impl_fn is None:
        kwargs['overloads'] = overloads

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


def _create_node(signature_fn, impl_fn=None,
                 node_type: "WiringNodeType" = None,
                 node_class: Type["WiringNodeClass"] = None,
                 active: Sequence[str] = None,
                 valid: Sequence[str] = None,
                 all_valid: Sequence[str] = None,
                 interfaces=None,
                 deprecated: bool | str = False,
                 requires: Callable[[..., ...], bool] = None
                 ) -> "WiringNodeClass":
    """
    Create the wiring node using the supplied node_type and impl_fn, for non-cpp types the impl_fn is assumed to be
    the signature fn as well.
    """
    from hgraph._wiring._wiring_node_signature import extract_signature
    if impl_fn is None:
        impl_fn = signature_fn
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature
    active_inputs = frozenset(active) if active is not None else None
    valid_inputs = frozenset(valid) if valid is not None else None
    all_valid_inputs = frozenset(all_valid) if all_valid is not None else None
    signature = signature_fn if isinstance(signature_fn, WiringNodeSignature) else \
        extract_signature(signature_fn, node_type, active_inputs=active_inputs, valid_inputs=valid_inputs,
                          all_valid_inputs=all_valid_inputs, deprecated=deprecated, requires=requires)
    if interfaces is None:
        return node_class(signature, impl_fn)
    else:
        return node_class(signature, impl_fn, interfaces=interfaces)


def _create_node_signature(name: str, kwargs: dict[str, Type], ret_type: Type, node_type: "WiringNodeType",
                           active_inputs: frozenset[str] = None, valid_inputs: frozenset[str] = None,
                           all_valid_inputs: frozenset[str] = None, defaults: dict[str, Any] = None,
                           deprecated: bool | str = False, requires: Callable[[..., ...], bool] | None = None) -> Callable:
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
        requires=requires
    )
    return signature


CALLABLE = TypeVar("CALLABLE", bound=Callable)
