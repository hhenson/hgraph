import inspect
from functools import partial
from typing import TypeVar, Callable, Type, Sequence, TYPE_CHECKING, TypeVarTuple

from frozendict import frozendict

from hgraph._types._scalar_type_meta_data import HgSchedulerType
from hgraph._types._time_series_types import TIME_SERIES_TYPE

if TYPE_CHECKING:
    from hgraph._wiring._wiring import WiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

__all__ = (
    "compute_node", "pull_source_node", "push_source_node", "sink_node", "graph", "generator", "service",
    "service_impl", "service_adaptor", "register_service", "push_queue")

SOURCE_NODE_SIGNATURE = TypeVar("SOURCE_NODE_SIGNATURE", bound=Callable)
COMPUTE_NODE_SIGNATURE = TypeVar("COMPUTE_NODE_SIGNATURE", bound=Callable)
SINK_NODE_SIGNATURE = TypeVar("SINK_NODE_SIGNATURE", bound=Callable)
GRAPH_SIGNATURE = TypeVar("GRAPH_SIGNATURE", bound=Callable)


def compute_node(fn: COMPUTE_NODE_SIGNATURE = None, /,
                 cpp_impl=None,
                 active: Sequence[str] = None,
                 valid: Sequence[str] = None,
                 all_valid: Sequence[str] = None,
                 overloads: "WiringNodeClass" | COMPUTE_NODE_SIGNATURE = None) -> COMPUTE_NODE_SIGNATURE:
    """
    Used to define a python function to be a compute-node. A compute-node is the worker unit in the graph and
    will be called each time of the inputs to the compute node ticks.
    A compute-node requires inputs and outputs.

    :param fn: The function to wrap
    :param cpp_impl: The C++ implementation to use (this makes fn a signature only method)
    :param active: Which inputs to mark as being active (by default all are active)
    :param valid: Which inputs to require to be valid (by default all are valid)
    :param all_valid: Which inputs are required to be ``all_valid`` (by default none are all_valid)
    :param overloads: If this node overloads an operator, this is the operator it is designed to overload.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.COMPUTE_NODE, fn, cpp_impl, active, valid, all_valid, overloads=overloads)


def pull_source_node(fn: SOURCE_NODE_SIGNATURE = None, /, cpp_impl=None) -> SOURCE_NODE_SIGNATURE:
    """
    Used to indicate the signature for a C++ source node. For Python source nodes use either the
    generator or source_adapter annotations.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.PULL_SOURCE_NODE, fn, cpp_impl)


def push_source_node(fn: SOURCE_NODE_SIGNATURE = None, /, cpp_impl=None) -> SOURCE_NODE_SIGNATURE:
    """
    Used to indicate the signature for a C++ push source node.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.PUSH_SOURCE_NODE, fn, cpp_impl)


def sink_node(fn: SINK_NODE_SIGNATURE=None, /,
              cpp_impl=None,
              active: Sequence[str] = None,
              valid: Sequence[str] = None,
              all_valid: Sequence[str] = None,
              overloads: "WiringNodeClass" | SINK_NODE_SIGNATURE = None) -> SINK_NODE_SIGNATURE:
    """
    Indicates the function definition represents a sink node. This type of node has no return type.
    Other than that it behaves in much the same way as compute node.

    :param fn: The function to wrap
    :param cpp_impl: The C++ implementation to use (this makes fn a signature only method)
    :param active: Which inputs to mark as being active (by default all are active)
    :param valid: Which inputs to require to be valid (by default all are valid)
    :param all_valid: Which inputs are required to be ``all_valid`` (by default none are all_valid)
    :param overloads: If this node overloads an operator, this is the operator it is designed to overload.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.SINK_NODE, fn, cpp_impl, active, valid, all_valid, overloads=overloads)


def graph(fn: GRAPH_SIGNATURE=None, overloads: "WiringNodeClass" = None) -> GRAPH_SIGNATURE:
    """
    Wraps a wiring function. The function can take the form of a function that looks like a compute_node,
    sink_node, souce_node, or a graph with no inputs or outputs. There is generally at least one graph in
    any application. The main graph.
    """
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.GRAPH, fn, overloads=overloads)


def generator(fn: SOURCE_NODE_SIGNATURE = None) -> SOURCE_NODE_SIGNATURE:
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
    from hgraph._wiring._wiring import PythonGeneratorWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType
    return _node_decorator(WiringNodeType.PULL_SOURCE_NODE, fn, node_class=PythonGeneratorWiringNodeClass)


def push_queue(tp: type[TIME_SERIES_TYPE]):
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
    from hgraph._wiring._wiring import PythonPushQueueWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    return lambda fn: _create_node(_create_node_signature(
        fn.__name__,
        {k: v for k, v in fn.__annotations__.items() if k != fn.__code__.co_varnames[0]}, tp,
        WiringNodeType.PUSH_SOURCE_NODE), impl_fn=fn,
        node_type=WiringNodeType.PUSH_SOURCE_NODE, node_class=PythonPushQueueWiringNodeClass)


def service(fn):
    """
    Decorates a function that describes the service signature. A service requires an implementation
    to describe its behavior.
    A service implementation must be registered by the graph.

    A service definition cannot take any scalar values except for the path.

    for example:

        @service
        def my_service(path: str, ts1: TIME_SERIES, ...) -> OUT_TIME_SERIES:
            pass

    """


SERVICE_DEFINITION = TypeVar('SERVICE_DEFINITION', bound=Callable)


def service_impl(fn=None, /, interface: SERVICE_DEFINITION = None):
    """
    Wraps a service implementation. The service is defined to implement the declared interface.
    """


def register_service(path: str, interface, implementation, **kwargs):
    """
    Binds the implementation of the interface to the path provided. The additional kwargs
    are passed to the implementation. These should be defined on the implementation and are independent of the
    attributes defined in the service.
    :param path:
    :param interface:
    :param implementation:
    :param kwargs:
    :return:
    """


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


def _node_decorator(node_type: "WiringNodeType", impl_fn, cpp_impl=None, active: Sequence[str] = None,
                    valid: Sequence[str] = None, all_valid: Sequence[str] = None,
                    node_class: Type["WiringNodeClass"] = None,
                    overloads: "WiringNodeClass" = None):
    from hgraph._wiring._wiring import CppWiringNodeClass, GraphWiringNodeClass, PythonWiringNodeClass
    from hgraph._wiring._wiring_node_signature import WiringNodeType

    kwargs = dict(node_type=node_type,
                  node_class=PythonWiringNodeClass if node_class is None else node_class,
                  active=active,
                  valid=valid,
                  all_valid=all_valid)
    if cpp_impl is not None:
        kwargs['node_class'] = CppWiringNodeClass
        kwargs['impl_fn'] = cpp_impl

    if node_type is WiringNodeType.GRAPH:
        kwargs['node_class'] = GraphWiringNodeClass
        if active is not None:
            raise ValueError("Graphs do not support ticked")
        if valid is not None:
            raise ValueError("Graphs do not support valid")
        if all_valid is not None:
            raise ValueError("Graph do not support all_valid")

    if overloads is not None and impl_fn is None:
        kwargs['overloads'] = overloads

    if impl_fn is None:
        return lambda fn: _node_decorator(impl_fn=fn, **kwargs)
    elif overloads is not None:
        overload = _create_node(impl_fn, **kwargs)
        overloads.overload(overload)
        return overload
    else:
        return _create_node(impl_fn, **kwargs)


def _create_node(signature_fn, impl_fn=None, node_type: "WiringNodeType" = None,
                 node_class: Type["WiringNodeClass"] = None, active: Sequence[str] = None, valid: Sequence[str] = None,
                 all_valid: Sequence[str] = None) -> "WiringNodeClass":
    """
    Create the wiring node using the supplied node_type and impl_fn, for non-cpp types the impl_fn is assumed to be
    the signature fn as well.
    """
    from hgraph._wiring._wiring_node_signature import extract_signature
    if impl_fn is None:
        impl_fn = signature_fn
    from hgraph._wiring._wiring import WiringNodeSignature
    active_inputs = frozenset(active) if active is not None else None
    valid_inputs = frozenset(valid) if valid is not None else None
    all_valid_inputs = frozenset(all_valid) if all_valid is not None else None
    signature = signature_fn if isinstance(signature_fn, WiringNodeSignature) else \
        extract_signature(signature_fn, node_type, active_inputs=active_inputs, valid_inputs=valid_inputs,
                          all_valid_inputs=all_valid_inputs)
    return node_class(signature, impl_fn)


def _create_node_signature(name: str, kwargs: dict[str, Type], ret_type: Type, node_type: "WiringNodeType",
                           active_inputs: frozenset[str] = None, valid_inputs: frozenset[str] = None,
                           all_valid_inputs: frozenset[str] = None) -> Callable:
    """
    Create a function that takes the kwargs and returns the kwargs. This is used to create a function that
    can be used to create a signature.
    """
    from hgraph._wiring._wiring import WiringNodeSignature
    from hgraph import HgScalarTypeMetaData, HgTimeSeriesTypeMetaData

    from hgraph import SourceCodeDetails
    from pathlib import Path
    signature = WiringNodeSignature(
        node_type=node_type,
        name=name,
        args=tuple(kwargs.keys()),
        defaults=frozendict(),
        input_types=frozendict({k: HgScalarTypeMetaData.parse(v) for k, v in kwargs.items()}),
        output_type=HgTimeSeriesTypeMetaData.parse(ret_type) if ret_type is not None else None,
        src_location=SourceCodeDetails(Path(), 0),  # TODO: make this point to a real place in code.
        active_inputs=active_inputs,
        valid_inputs=valid_inputs,
        all_valid_inputs=all_valid_inputs,
        unresolved_args=frozenset(),
        time_series_args=frozenset(),
        uses_scheduler=any(type(v) is HgSchedulerType for v in kwargs.values())
    )
    return signature
