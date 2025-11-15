import inspect
from typing import Callable, cast

from hgraph._types._ts_meta_data import HgTSTypeMetaData
from hgraph._types._scalar_types import SIZE, ZERO, STATE
from hgraph._types._scalar_type_meta_data import HgTupleCollectionScalarType
from hgraph._types._time_series_types import TIME_SERIES_TYPE, TIME_SERIES_TYPE_1, K
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tsl_type import TSL
from hgraph._types._typing_utils import with_signature
from hgraph._wiring._decorators import compute_node, graph
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_errors import CustomMessageWiringError
from hgraph._wiring._wiring_node_class._reduce_wiring_node import (
    TsdReduceWiringNodeClass,
    ReduceWiringSignature,
    TsdNonAssociativeReduceWiringNodeClass,
)
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_utils import wire_nested_graph

__all__ = ("reduce",)


def reduce(
    func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE_1], TIME_SERIES_TYPE],
    ts: TSD[K, TIME_SERIES_TYPE_1] | TSL[TIME_SERIES_TYPE_1, SIZE],
    zero: TIME_SERIES_TYPE = ZERO,
    is_associative: bool = True,
) -> TIME_SERIES_TYPE:
    """
    Reduce the input time-series collection into a single time-series value.
    The zero must-be compatible with the TIME_SERIES_TYPE value and be constructable as const(zero, TIME_SERIES_TYPE).
    If the function is associative, then TIME_SERIES_TYPE must be the same as TIME_SERIES_TYPE_1.
    When the function is associative, the 'reduce' will perform a tree reduction, otherwise it will perform a linear
    reduction. The tree reduction is much faster on change.

    By definition, the reduce function over a TSD must be commutative and associative in the sense that the order of the
    inputs are not guaranteed. Only a TSL supports non-commutative reduce functions.

    Example [TSD]:

    ::

        tsd: TSD[str, TS[int]] = ...
        out = reduce(add_, tsd, 0)
        >> tsd <- {'a': [1], 'b': [4], 'c': [7]}
        >> out -> 12

    Example [TSL]:

    ::

        tsl: TSL[TS[int], SIZE] = ...
        out = reduce(add_, tsl, 0)
        >> tsl <- ([1], [2], [3], [4], [5])
        >> out -> 15

    Example [TS[tuple[SCALAR, ...]]:

    ::

        ts: TS[tuple[int, ...]] = ...
        initial_value: TS[str] = ...
        out = reduce(lambda x, y: format_("{}, {}", x, y), ts, initial_value)

    NOTE: TSD[int, TIME_SERIES_TYPE_1] with is_associative=False is the only TSD non-associative reduce supported.
          The expectation is that the integer values represent a uniform list from 0 to size-1. There cannot be holes
          in the sequence. Removals of keys must be of the form [0:n] where n is the last element in the new set.
          This allows for processing an input that is different from the output type, as for the tuple example.
          The ``zero`` element is used as the default result if no value is supplied, it is also used as the input
          for the chain of keys. The output of the n-1th element is used as the input to the nth element lhs.
          The values from the dict are used as the rhs input.
    """
    if isinstance(func, WiringNodeClass):
        signature = func.signature.signature
    elif isinstance(func, Callable) and func.__name__ == "<lambda>":
        signature = inspect.signature(func).__str__()
    else:
        raise RuntimeError(f"The supplied time-series is not a valid input: '{ts}'")

    _tp = ts.output_type.dereference()
    with WiringContext(current_signature=STATE(signature=f"reduce('{signature}', {_tp}, {zero})")):
        if type(_tp) is HgTSLTypeMetaData:
            return _reduce_tsl(func, ts, zero, is_associative)
        elif type(_tp) is HgTSDTypeMetaData:
            if not is_associative:
                if _tp.key_tp.py_type is not int:
                    raise CustomMessageWiringError(
                        "Non-associative operators are not supported using TSD inputs that are not integer keyed"
                    )
            return _reduce_tsd(func, ts, zero, is_associative)
        elif type(_tp) is HgTSTypeMetaData and type(_tp.value_scalar_tp) is HgTupleCollectionScalarType:
            if is_associative:
                raise CustomMessageWiringError("Associative operators are not supported using TS[tuple[...]] inputs")
            return _reduce_tuple(func, ts, zero)
        else:
            raise RuntimeError(f"Unexpected time-series type: {ts.output_type}")


def _reduce_tsl(func, ts, zero, is_associative):
    """For the moment, we only support fixed size TSLs. So we can lay out the reduction in the graph statically"""
    from hgraph import default

    tp_ = ts.output_type
    item_tp = tp_.value_tp.py_type

    if not isinstance(zero, WiringPort):
        if not is_associative:
            raise CustomMessageWiringError(
                "Non-associative operators require a time-series value for zero to be provided"
            )
        if zero is ZERO:
            import hgraph

            zero = hgraph._operators._operators.zero(item_tp, func)
        elif zero is None:
            from hgraph import nothing

            zero = nothing(item_tp)
        else:
            from hgraph import const

            zero = const(zero, item_tp)
    
    if (sz := tp_.size_tp.py_type.SIZE) == 0:
        return zero
    
    if not is_associative or sz < 4:
        out = default(ts[0], zero)
        for i in range(1, sz):
            out = func(out, default(ts[i], zero))
        return out
    else:
        outs = [func(default(ts[i], zero), default(ts[i + 1], zero)) for i in range(0, sz - sz % 2, 2)]
        over_run = None if sz % 2 == 0 else default(ts[-1], zero)
        while len(outs) > 1:
            l = len(outs)
            if l % 2 == 1:
                if over_run is not None:
                    outs.append(over_run)
                    l += 1
                else:
                    over_run = outs.pop()
                    l -= 1
            outs = [func(outs[i], outs[i + 1]) for i in range(0, l, 2)]
        if over_run is not None:
            out = func(outs[0], over_run)
        else:
            out = outs[0]
        return out


def _reduce_tsd(func, ts, zero, is_associative=True) -> TIME_SERIES_TYPE:
    from hgraph._types._ref_type import REF

    # We need to ensure that the reduction graph contains no push nodes. (We should be able to support pull nodes)

    @compute_node
    def _reduce_tsd_signature(
        ts: TSD[K, REF[TIME_SERIES_TYPE_1]], zero: REF[TIME_SERIES_TYPE]
    ) -> REF[TIME_SERIES_TYPE]:
        ...
        # Used to create a WiringNodeClass template

    tp = ts.output_type.dereference()
    item_tp_md = tp.value_tp
    item_tp = item_tp_md.py_type

    if not isinstance(zero, WiringPort):
        if not is_associative:
            raise CustomMessageWiringError(
                "Non-associative operators require a time-series value for zero to be provided"
            )
        if zero is ZERO:
            import hgraph

            zero = hgraph._operators._operators.zero(item_tp, func)
        elif zero is None:
            from hgraph import nothing

            zero = nothing(item_tp)
        else:
            from hgraph import const

            zero = const(zero, item_tp)

    wp = _reduce_tsd_signature(ts, zero)
    resolved_signature = cast(WiringPort, wp).node_instance.resolved_signature
    resolved_signature = WiringNodeSignature(
        node_type=resolved_signature.node_type,
        name="reduce",
        args=resolved_signature.args,
        defaults=resolved_signature.defaults,
        input_types=resolved_signature.input_types,
        output_type=resolved_signature.output_type,
        src_location=resolved_signature.src_location,
        active_inputs=resolved_signature.active_inputs,
        valid_inputs=resolved_signature.valid_inputs,
        all_valid_inputs=resolved_signature.all_valid_inputs,
        context_inputs=resolved_signature.context_inputs,
        unresolved_args=resolved_signature.unresolved_args,
        time_series_args=resolved_signature.time_series_args,
        injectables=resolved_signature.injectables,
        # label=resolved_signature.label,
        has_nested_graphs=True,
    )

    if not isinstance(func, WiringNodeClass):
        if is_associative:
            func = graph(
                with_signature(
                    func,
                    annotations={k: item_tp for k in inspect.signature(func).parameters},
                    return_annotation=item_tp,
                )
            )
        else:
            zero_tp = zero.output_type.dereference()
            parameters = list(inspect.signature(func).parameters)
            if len(parameters) != 2:
                raise CustomMessageWiringError(
                    f"The function must have exactly two arguments, but has {len(parameters)}"
                )
            annotations = {parameters[0]: zero_tp, parameters[1]: item_tp}
            func = graph(
                with_signature(
                    func,
                    annotations=annotations,
                    return_annotation=zero_tp,
                )
            )
    if is_associative:
        input_types = {k: tp.value_tp for k in func.signature.input_types}
    else:
        input_types = {k: v for k, v in zip(func.signature.args, (zero.output_type, item_tp_md))}

    builder, ri = wire_nested_graph(func, input_types, {}, resolved_signature, None, depth=2)

    reduce_signature = ReduceWiringSignature(**resolved_signature.as_dict(), inner_graph=builder)
    wiring_node = (
        TsdReduceWiringNodeClass(reduce_signature, func)
        if is_associative
        else TsdNonAssociativeReduceWiringNodeClass(reduce_signature, func)
    )
    port = wiring_node(ts, zero)

    from hgraph import WiringGraphContext

    WiringGraphContext.instance().reassign_items(ri, port.node_instance)

    return port


def _reduce_tuple(func, ts, zero):
    from hgraph import convert, dedup

    tsd = dedup(convert[TSD](ts))
    return _reduce_tsd(func, tsd, zero, False)
