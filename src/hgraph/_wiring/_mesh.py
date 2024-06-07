from dataclasses import dataclass
from itertools import chain
from typing import Callable

from frozendict import frozendict

from hgraph._types._scalar_types import STATE, SCALAR
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_meta_data import HgTSTypeMetaData, HgTimeSeriesTypeMetaData
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._context_wiring import TimeSeriesContextTracker
from hgraph._wiring._map import (
    _deduce_signature_from_lambda_and_args,
    _extract_map_fn_key_arg_and_type,
    _split_inputs,
    _MappingMarker,
    KEYS_ARG,
    _prepare_stub_inputs,
)
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class import extract_kwargs, WiringNodeClass
from hgraph._wiring._wiring_node_class._mesh_wiring_node import MeshWiringNodeClass, MeshWiringSignature
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_port import WiringPort, DelayedBindingWiringPort, TSDREFWiringPort
from hgraph._wiring._wiring_utils import as_reference, wire_nested_graph


def mesh_(func: Callable, *args, **kwargs):
    """
    Wrap the given graph into a calculation mesh - a structure that is akin to a map_ but allows instances of
    the graph to access outputs of other instances of the graph. New instances will also be created on demand from
    inner graphs as well as from the keys inputs.
    """

    if len(args) + len(kwargs) == 0:
        # calling mesh_ without any arguments is used to access it from the inner graphs
        return get_mesh(kwargs.get("__name__", func))

    from inspect import isfunction

    name = kwargs.pop("__name__", None)
    if isfunction(func) and func.__name__ == "<lambda>":
        graph = _deduce_signature_from_lambda_and_args(func, *args, **kwargs)
    elif isinstance(func, WiringNodeClass):
        graph = func
    else:
        raise RuntimeError(f"The supplied function is not a graph or node function or lambda: '{func.__name__}'")

    with WiringContext(current_signature=STATE(signature=f"mesh_('{graph.signature.signature}', ...)")):
        signature: WiringNodeSignature = graph.signature
        map_wiring_node, calling_kwargs = _build_mesh_wiring_node_and_inputs(
            graph, signature, *args, **kwargs, __name__=name
        )
        return map_wiring_node(**calling_kwargs).out[calling_kwargs[KEYS_ARG]]


def _build_mesh_wiring_node_and_inputs(
    fn: Callable, signature: WiringNodeSignature, *args, __keys__=None, __key_arg__=None, __name__=None, **kwargs
) -> tuple[WiringNodeClass, dict[str, WiringPort | SCALAR]]:
    """
    Build the mesh wiring signature. This works exactly like the map_ function but without TSL support and building
    a mesh node instead of a map node.
    """
    input_has_key_arg, input_key_name, input_key_tp = _extract_map_fn_key_arg_and_type(signature, __key_arg__)
    kwargs_ = extract_kwargs(
        signature, *args, _ensure_match=False, _args_offset=1 if input_has_key_arg else 0, **kwargs
    )
    multiplex_args, no_key_args, pass_through_args, _, map_type, key_tp_ = _split_inputs(signature, kwargs_, __keys__)
    if map_type == "TSL":
        raise CustomMessageWiringError("Mesh does not support TSL types")
    else:
        tp = key_tp_

    if input_has_key_arg and not input_key_tp.matches(tp):
        raise CustomMessageWiringError(f"The ndx argument '{signature.args[0]}: {input_key_tp}' does not match '{tp}'")
    input_key_tp = tp

    input_types = {
        k: v.output_type.dereference() if isinstance(v, (WiringPort, _MappingMarker)) else signature.input_types[k]
        for k, v in kwargs_.items()
    }

    # Create the wiring node
    if __keys__ is not None:
        kwargs_[KEYS_ARG] = __keys__
    else:
        if len(multiplex_args) > 1:
            from hgraph import union

            __keys__ = union(*tuple(kwargs_[k].key_set for k in multiplex_args if k not in no_key_args))
        else:
            __keys__ = kwargs_[next(iter(multiplex_args))].key_set
        kwargs_[KEYS_ARG] = __keys__
    input_types = input_types | {KEYS_ARG: __keys__.output_type.dereference()}
    mesh_wiring_node = _create_mesh_wiring_node(
        fn,
        kwargs_,
        input_types,
        multiplex_args,
        no_key_args,
        input_key_tp,
        input_key_name if input_has_key_arg else None,
        name=__name__,
    )

    # 7. Clean the inputs (eliminate the marker wrappers)
    for arg in chain(pass_through_args, no_key_args):
        kwargs_[arg] = kwargs_[arg].value  # Unwrap the marker inputs.

    return mesh_wiring_node, kwargs_


def _create_mesh_wiring_node(
    fn: WiringNodeClass,
    kwargs_: dict[str, WiringPort | SCALAR],
    input_types: dict[str, HgTypeMetaData],
    multiplex_args: frozenset[str],
    no_key_args: frozenset[str],
    input_key_tp: HgTSTypeMetaData,
    input_key_name: str | None,
    name: str = None,
) -> MeshWiringNodeClass:
    from hgraph._types._ref_meta_data import HgREFTypeMetaData

    # This again follows the pattern in map_ with the following differences: creates a different type of wiring
    # signature and also exposes mesh contexts while wiring the nested graph

    stub_inputs = _prepare_stub_inputs(kwargs_, input_types, multiplex_args, no_key_args, input_key_tp, input_key_name)
    resolved_signature = fn.resolve_signature(**stub_inputs)

    reference_inputs = frozendict(
        {
            k: as_reference(v, k in multiplex_args) if isinstance(v, HgTimeSeriesTypeMetaData) and k != KEYS_ARG else v
            for k, v in input_types.items()
        }
    )

    if resolved_signature.output_type is None:
        raise CustomMessageWiringError("The mesh function must have an output type")

    output_tsd_type = HgTSDTypeMetaData(
        input_key_tp.value_scalar_tp, HgREFTypeMetaData(resolved_signature.output_type.dereference())
    )

    # The mesh needs a reference output that references its real output, so fold it into an unnamed bundle
    from hgraph import UnNamedTimeSeriesSchema

    output_type = HgTSBTypeMetaData(
        HgTypeMetaData.parse_type(
            UnNamedTimeSeriesSchema.create(out=output_tsd_type, ref=HgREFTypeMetaData(output_tsd_type))
        )
    )

    provisional_signature = WiringNodeSignature(
        node_type=WiringNodeType.COMPUTE_NODE if resolved_signature.output_type else WiringNodeType.SINK_NODE,
        name="mesh",
        args=tuple(input_types.keys()),
        defaults=frozendict(),
        input_types=reference_inputs,
        output_type=output_type,
        src_location=resolved_signature.src_location,  # TODO: Figure out something better for this.
        active_inputs=None,
        valid_inputs=frozenset(
            {
                KEYS_ARG,
            }
        ),
        all_valid_inputs=None,
        context_inputs=None,
        unresolved_args=frozenset(),
        time_series_args=frozenset(k for k, v in input_types.items() if not v.is_scalar),
        label=f"mesh('{resolved_signature.signature}', {', '.join(input_types.keys())})",
    )

    try:
        name = f"mesh_{name}" if name else f"mesh_{fn.signature.name}"
        context_wiring_port = DelayedBindingWiringPort(output_type=output_tsd_type)
        path = TimeSeriesContextTracker.instance().enter_context(
            context_wiring_port, WiringNodeInstanceContext.instance(), STATE(f_locals={name: context_wiring_port})
        )

        builder, ss, cc = wire_nested_graph(fn,
                          resolved_signature.input_types,
                          {k: kwargs_[k] for k, v in resolved_signature.input_types.items()
                           if not isinstance(v, HgTimeSeriesTypeMetaData) and k != KEYS_ARG},
                          provisional_signature,
                          input_key_name,
                          depth=2)

        mesh_signature = MeshWiringSignature(
            **provisional_signature.as_dict(),
            map_fn_signature=resolved_signature,
            key_tp=input_key_tp.value_scalar_tp,
            key_arg=input_key_name,
            multiplexed_args=multiplex_args,
            inner_graph=builder,
            context_path=path
        )

    finally:
        TimeSeriesContextTracker.instance().exit_context(context_wiring_port, capture=False)

    wiring_node = MeshWiringNodeClass(mesh_signature, fn)
    return wiring_node


@dataclass(frozen=True)
class MeshWiringPort(TSDREFWiringPort):
    """
    A wiring port that represents a lattice wiring node
    """

    def __init__(self, context_wiring_port: WiringPort):
        super().__init__(context_wiring_port.node_instance, context_wiring_port.path)

    def __getitem__(self, item):
        from hgraph.nodes._mesh_util import mesh_subscribe_node

        return mesh_subscribe_node(self, item)[item]


def get_mesh(fn_or_name: str | WiringNodeClass) -> MeshWiringPort | None:
    """
    Get the mesh wiring node from the inner graph function or the name
    """
    if type(fn_or_name) is not str:
        fn_or_name = fn_or_name.signature.name

    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
    from hgraph import TimeSeriesContextTracker

    context = TimeSeriesContextTracker.instance().get_context(
        HgTypeMetaData.parse_type(TIME_SERIES_TYPE), WiringNodeInstanceContext.instance(), f"mesh_{fn_or_name}"
    )

    return MeshWiringPort(context) if context else None
