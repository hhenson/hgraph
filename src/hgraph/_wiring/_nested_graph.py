from pathlib import Path
from typing import Callable

from frozendict import frozendict

from hgraph._types._scalar_types import STATE
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass, extract_kwargs
from hgraph._wiring._wiring_node_signature import WiringNodeSignature, WiringNodeType
from hgraph._wiring._wiring_utils import as_reference, wire_nested_graph

__all__ = ("nested_graph",)


def nested_graph(
    func: Callable[..., TIME_SERIES_TYPE],
    *args,
    **kwargs,
) -> TIME_SERIES_TYPE:
    """
    Wrap a graph nested graph.

    :param func: The graph to wrap
    :param args: The arguments to pass to the wrapped graph
    :param kwargs: The kwargs to pass to the wrapped graph.
    :return: Reference to the wrapped graph
    """
    with WiringContext(current_signature=STATE(signature=f"nested_graph('{func.signature.signature}', ...)")):
        func: WiringNodeClass
        signature: WiringNodeSignature = func.signature
        kwargs_ = extract_kwargs(signature, *args, **kwargs)
        resolved_signature = func.resolve_signature(**kwargs_)
        output_type = as_reference(resolved_signature.output_type) if resolved_signature.output_type else None
        input_types = {
            k: as_reference(v) if isinstance(v, HgTimeSeriesTypeMetaData) else v
            for k, v in resolved_signature.input_types.items()
        }

        time_series_args = resolved_signature.time_series_args
        has_ts_inputs = bool(time_series_args)
        node_type = (
            WiringNodeType.COMPUTE_NODE if has_ts_inputs else WiringNodeType.PULL_SOURCE_NODE
        )

        resolved_signature_outer = WiringNodeSignature(
            node_type=node_type,
            name="nested_graph",
            args=resolved_signature.args,
            defaults=frozendict(),  # Defaults would have already been applied.
            input_types=frozendict(input_types),
            output_type=output_type,
            src_location=SourceCodeDetails(Path(__file__), 25),
            active_inputs=frozenset(),
            valid_inputs=frozenset(),
            all_valid_inputs=frozenset(),
            context_inputs=None,
            unresolved_args=frozenset(),
            time_series_args=time_series_args,
            label=f"nested_graph({resolved_signature.signature}, {', '.join(resolved_signature.args)})",
        )
        from hgraph import NestedGraphWiringNodeClass

        graph, reassignables = wire_nested_graph(
            func,
            resolved_signature.input_types,
            {
                k: kwargs_[k]
                for k, v in resolved_signature.input_types.items()
                if v != resolved_signature.defaults.get(k)
            },
            resolved_signature_outer,
            None,
            depth=1,
        )

        # noinspection PyTypeChecker
        port = NestedGraphWiringNodeClass(resolved_signature_outer, graph, resolved_signature)(**kwargs_)

        from hgraph import WiringGraphContext

        WiringGraphContext.instance().reassign_items(reassignables, port.node_instance)

        if port.output_type is not None:
            return port
        else:
            WiringGraphContext.instance().add_sink_node(port.node_instance)
