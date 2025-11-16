from inspect import isfunction
from pathlib import Path
from typing import Callable, Optional, cast

from frozendict import frozendict

from hgraph._types._scalar_types import SCALAR, STATE
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_meta_data import HgTSTypeMetaData
from hgraph._types._ts_type import TS
from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._wiring._source_code_details import SourceCodeDetails
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._switch_wiring_node import SwitchWiringSignature
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass, extract_kwargs
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_node_signature import WiringNodeType
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_utils import as_reference, wire_nested_graph

__all__ = ("switch_",)


def switch_(
    key: TS[SCALAR],
    cases: dict[SCALAR, Callable],
    *args,
    reload_on_ticked: bool = False,
    **kwargs,
) -> Optional[TIME_SERIES_TYPE]:
    """
    The ability to select and instantiate a graph based on the value of the key time-series input.
    By default, when the key changes, a new instance of graph is created and run.
    The graph will be evaluated when it is initially created and then as the values are ticked as per normal.
    If the code depends on inputs to have ticked, they will only be evaluated when the inputs next tick (unless
    they have ticked when the graph is wired in).

    The selector is part of the graph shaping operators. This allows for different shapes that operate on the same
    inputs and return the same output. An example of using this is when you have different order types, and then you
    dynamically choose which graph to evaluate based on the order type.

    This node has the overhead of instantiating and tearing down the subgraphs as the key changes. The use of switch
    should consider this overhead, the positive side is that once the graph is instantiated, the performance is the same
    as if it were wired in directly. This is great when the key changes infrequently.

    The mapped graphs / nodes can have a first argument which is of the same type as the key. In this case, the key
    will be mapped into this argument. If the first argument is not of the same type as the key, or the kwargs match
    the argument name of the first argument, the key will not be mapped into the argument.

    Example:

    ::

        key: TS[str] = ...
        ts1: TS[int] = ...
        ts2: TS[int] = ...
        out = switch(key, {'add': add_, "sub": sub_}, ts1, ts2)

    Which will perform the computation based on the key's value.

    A default option can be provided by using the DEFAULT marker. For example:

    ::

        out = switch_(key, {DEFAULT: add_}, ...)

    """
    # Create a nifty simplified signature for the switch node.
    with WiringContext(current_signature=STATE(signature=f"switch_({{{', '.join(f'{k}: ...' for k in cases)}}}, ...)")):
        from hgraph._wiring._wiring_node_class._wiring_node_class import WiringPort

        # Perform basic validations fo the inputs
        if cases is None or len(cases) == 0:
            raise CustomMessageWiringError("No components supplied to switch")
        if key is None:
            raise CustomMessageWiringError("No key supplied to switch")
        if not isinstance(key, WiringPort) or not isinstance(
            cast(WiringPort, key).output_type.dereference(), HgTSTypeMetaData
        ):
            raise CustomMessageWiringError(
                f"The key must be a time-series value of form TS[SCALAR], received {key.output_type.dereference()}"
            )

        cases = {
            k: v if isinstance(v, WiringNodeClass) else _deduce_signature_from_lambda_and_args(v, key, *args, **kwargs)
            for k, v in cases.items()
        }

        # Assume that the switch items are correctly typed to start with, then we can take the first signature and
        # use it to create a signature for the outer switch node.
        a_signature = cast(WiringNodeClass, next(iter(cases.values()))).signature

        input_has_key_arg = bool(
            a_signature.args
            and a_signature.args[0] == "key"
            and a_signature.input_types["key"] == cast(WiringPort, key).output_type
        )

        # We add the key to the inputs if the internal component has a key argument.
        kwargs_ = extract_kwargs(a_signature, *args, **(kwargs | dict(key=key) if input_has_key_arg else kwargs))

        # Now create a resolved signature for the inner graph, then for the outer switch node.
        resolved_signature_inner = _validate_signature(cases, **kwargs_)

        input_types = {
            k: as_reference(v) if k != "key" and isinstance(v, HgTimeSeriesTypeMetaData) else v.dereference()
            for k, v in resolved_signature_inner.input_types.items()
        }
        time_series_args = resolved_signature_inner.time_series_args
        if not input_has_key_arg:
            input_types["key"] = cast(WiringPort, key).output_type.dereference()
            time_series_args = time_series_args | {
                "key",
            }

        output_type = (
            as_reference(resolved_signature_inner.output_type) if resolved_signature_inner.output_type else None
        )

        resolved_signature_outer = WiringNodeSignature(
            # QUESTION: Can we support a switch on a SOURCE_NODE?
            node_type=WiringNodeType.COMPUTE_NODE if time_series_args and output_type else WiringNodeType.SINK_NODE,
            name="switch",
            # All actual inputs are encoded in the input_types, so we just need to add the keys if present.
            args=resolved_signature_inner.args if input_has_key_arg else ("key",) + resolved_signature_inner.args,
            defaults=frozendict(),  # Defaults would have already been applied.
            input_types=frozendict(input_types),
            output_type=output_type,
            src_location=SourceCodeDetails(Path(__file__), 25),
            active_inputs=frozenset({
                "key",
            }),
            valid_inputs=frozenset({
                "key",
            }),
            all_valid_inputs=None,
            context_inputs=None,
            # We have constructed the map so that the key are is always present.
            unresolved_args=frozenset(),
            time_series_args=time_series_args,
            # label=f"switch_({{{', '.join(f'{k}: ...' for k in cases)}}}, ...)",
            has_nested_graphs=True,
        )

        nested_graphs = {}
        reassignables = None
        for k, v in cases.items():
            graph, ri = wire_nested_graph(
                v,
                resolved_signature_inner.input_types,
                {
                    k: kwargs_[k]
                    for k, v in resolved_signature_inner.input_types.items()
                    if not isinstance(v, HgTimeSeriesTypeMetaData) and k != "key"
                },
                resolved_signature_outer,
                "key",
                depth=2,
            )
            nested_graphs[k] = graph
            if reassignables is None:
                reassignables = ri
            else:
                for i, r in enumerate(ri):
                    reassignables[i].extend(r)

        switch_signature = SwitchWiringSignature(
            **resolved_signature_outer.as_dict(), inner_graphs=frozendict(nested_graphs)
        )

        # Create the outer wiring node, and call it with the inputs
        from hgraph._wiring._wiring_node_class._switch_wiring_node import SwitchWiringNodeClass

        # noinspection PyTypeChecker
        port = SwitchWiringNodeClass(switch_signature, cases, resolved_signature_inner, reload_on_ticked)(
            **(kwargs_ | ({} if input_has_key_arg else dict(key=key))), __return_sink_wp__=True
        )

        from hgraph import WiringGraphContext

        WiringGraphContext.instance().reassign_items(reassignables, port.node_instance)

        if port.output_type is not None:
            return port
        else:
            WiringGraphContext.instance().add_sink_node(port.node_instance)


def _validate_signature(
    cases: dict[SCALAR, Callable[[...], Optional[TIME_SERIES_TYPE]]], **kwargs
) -> WiringNodeSignature:
    check_signature: WiringNodeSignature | None = None
    for k, v in cases.items():
        if check_signature is None:
            check_signature = cast(WiringNodeClass, v).resolve_signature(**kwargs)
        else:
            this_signature = cast(WiringNodeClass, v).resolve_signature(**kwargs)
            same_arg_no = this_signature.args == check_signature.args
            same_arg_types = same_arg_no and all(
                check_signature.input_types[arg].matches(this_signature.input_types[arg])
                for arg in check_signature.args
            )
            same_output = (
                check_signature.output_type.dereference().matches(this_signature.output_type.dereference())
                if check_signature.output_type is not None
                else this_signature.output_type is None
            )
            if not (same_arg_no and same_arg_types and same_output):
                # If the signatures do not match, then we cannot wire the switch.
                # We ensure the arguments and their types match, as well as the output type.
                raise CustomMessageWiringError(
                    "The signature of the cases nodes do not match: "
                    f"{check_signature.signature} != {k}: {this_signature.signature}"
                )
    return check_signature


def _deduce_signature_from_lambda_and_args(func, key, *args, __key_arg__="key", **kwargs) -> WiringNodeClass:
    """
    A lambda was provided for map_ so it will not have a signature to be used. This function will try to work out the
    signature from the names of the lambda arguments and the incoming arguments and their types. The logic here
    duplicates a little what is found in the _build_map_wiring_node_and_inputs and friends but it is essentially the
    inside out of it
    """
    from inspect import signature, Parameter

    if not isfunction(func) or func.__name__ != "<lambda>":
        raise CustomMessageWiringError("Only graphs, nodes or lambda functions are supported for switch_")

    sig = signature(func)

    input_has_key_arg = False
    input_key_name = __key_arg__

    # 1. First figure out what is the type of the key
    key_type = key.output_type.dereference().value_scalar_tp

    # 2. Put together annotations for the lambda from the parameter types
    annotations = {}
    values = {}
    i = 0
    for i, (n, p) in enumerate(sig.parameters.items()):
        if p.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            raise CustomMessageWiringError("lambdas with variable argument list are not supported for switch_()")

        if i == 0:
            if n == input_key_name:  # this is the key input
                input_has_key_arg = True
                input_key_tp = HgTSTypeMetaData(key_type)
                annotations[input_key_name] = input_key_tp
                continue

        if input_has_key_arg:
            i -= 1

        if i < len(args):  # provided as positional and not key
            if isinstance(args[i], WiringPort):
                tp = args[i].output_type.dereference()
                annotations[n] = tp
            else:
                annotations[n] = HgTypeMetaData.parse_type(SCALAR)

            values[n] = args[i]
            continue

        if n in kwargs:  # provided as keyword
            if isinstance(kwargs[n], WiringPort):
                tp = kwargs[n].output_type
                annotations[n] = tp
            else:
                annotations[n] = HgTypeMetaData.parse_type(SCALAR)

            values[n] = kwargs[n]
            continue

        raise CustomMessageWiringError(f"no input for the parameter {n} of the lambda passed into switch_")

    if (unused := kwargs.keys() - sig.parameters.keys()) != set():
        raise CustomMessageWiringError(f"keyword arguments {unused} are not used in the lambda signature")

    if i + 1 < len(args):
        raise CustomMessageWiringError(f"{len(args) - 2} of positional arguments not used in the lambda signature")

    # 3. now we have annotations for the parameters of the lambda the only way to figure out the output type is to
    # try to wire it
    inputs_ = {}
    for k, v in annotations.items():
        if v.is_scalar:
            inputs_[k] = values[k]
        else:
            from hgraph import create_input_stub

            inputs_[k] = create_input_stub(k, cast(HgTimeSeriesTypeMetaData, v), k == input_key_name)

    from hgraph import WiringGraphContext

    with WiringGraphContext(temporary=True):
        out = func(**inputs_)
        if out is not None:
            output_type = out.output_type
        else:
            output_type = None

    # 4. Now create a graph with the signature we worked out and return
    from hgraph import with_signature, graph

    return graph(with_signature(func, annotations=annotations, return_annotation=output_type))
