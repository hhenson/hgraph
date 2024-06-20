import inspect
from typing import Callable, cast

from hgraph._types._scalar_types import SIZE, ZERO
from hgraph._types._scalar_types import STATE
from hgraph._types._time_series_types import TIME_SERIES_TYPE, TIME_SERIES_TYPE_1, K
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tsl_type import TSL
from hgraph._types._typing_utils import with_signature
from hgraph._wiring._decorators import compute_node, graph
from hgraph._wiring._wiring_context import WiringContext
from hgraph._wiring._wiring_node_class._reduce_wiring_node import TsdReduceWiringNodeClass, ReduceWiringSignature
from hgraph._wiring._wiring_node_class._wiring_node_class import WiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_utils import wire_nested_graph

__all__ = ("reduce",)


def reduce(
    func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE_1], TIME_SERIES_TYPE],
    ts: TSD[K, TIME_SERIES_TYPE_1] | TSL[TIME_SERIES_TYPE_1, SIZE],
    zero: TIME_SERIES_TYPE = ZERO,
    is_associated: bool = True,
) -> TIME_SERIES_TYPE:
    """
    Reduce the input time-series collection into a single time-series value.
    The zero must be compatible with the TIME_SERIES_TYPE value and be constructable as const(zero, TIME_SERIES_TYPE).
    If the function is associative, then TIME_SERIES_TYPE must be the same as TIME_SERIES_TYPE_1.
    When the function is associative, the 'reduce' will perform a tree reduction, otherwise it will perform a linear
    reduction. The tree reduction is much faster on change.

    By definition, the reduce function over a TSD must be commutative and associative in the sense that the order of the
    inputs are not guaranteed. Only a TSL supports non-commutative reduce functions.

    Example [TSD]:
        tsd: TSD[str, TS[int]] = ...
        out = reduce(add_, tsd, 0)
        >> tsd <- {'a': [1], 'b': [4], 'c': [7]}
        >> out -> 12

    Example [TSL]:
        tsl: TSL[TS[int], SIZE] = ...
        out = reduce(add_, tsl, 0)
        >> tsl <- ([1], [2], [3], [4], [5])
        >> out -> 15
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
            return _reduce_tsl(func, ts, zero, is_associated)
        elif type(_tp) is HgTSDTypeMetaData:
            return _reduce_tsd(func, ts, zero)
        else:
            raise RuntimeError(f"Unexpected time-series type: {ts.output_type}")


def _reduce_tsl(func, ts, zero, is_associated):
    """For the moment, we only support fixed size TSLs. So we can lay out the reduction in the graph statically"""
    from hgraph import default

    tp_ = ts.output_type
    if (sz := tp_.size_tp.py_type.SIZE) == 0:
        return zero
    if not is_associated or sz < 4:
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


def _reduce_tsd(func, ts, zero):
    from hgraph._types._ref_type import REF

    # We need to ensure that the reduction graph contains no push nodes. (We should be able to support pull nodes)

    @compute_node
    def _reduce_tsd_signature(ts: TSD[K, REF[TIME_SERIES_TYPE]], zero: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
        ...
        # Used to create a WiringNodeClass template

    tp = ts.output_type.dereference()
    item_tp = tp.value_tp.py_type

    if not isinstance(zero, WiringPort):
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
        injectable_inputs=resolved_signature.injectable_inputs,
        label=resolved_signature.label,
    )

    if not isinstance(func, WiringNodeClass):
        func = graph(
            with_signature(
                func,
                annotations={k: item_tp for k in inspect.signature(func).parameters},
                return_annotation=TIME_SERIES_TYPE,
            )
        )

    builder, sc, cc = wire_nested_graph(func,
                                      {k: tp.value_tp for k in func.signature.input_types},
                                      {},
                                      resolved_signature,
                                      None,
                                      depth=2)

    reduce_signature = ReduceWiringSignature(
        **resolved_signature.as_dict(),
        inner_graph=builder
    )
    wiring_node = TsdReduceWiringNodeClass(reduce_signature, func)
    return wiring_node(ts, zero)
