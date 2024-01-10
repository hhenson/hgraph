from typing import Callable, cast

from hgraph._wiring._decorators import compute_node
from hgraph._types._scalar_types import SCALAR, STATE
from hgraph._types._time_series_types import TIME_SERIES_TYPE, TIME_SERIES_TYPE_1
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._scalar_types import SIZE
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_type import TSL
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._wiring._reduce_wiring_node import TsdReduceWiringNodeClass
from hgraph._wiring._wiring import WiringNodeClass
from hgraph._wiring._wiring_node_signature import WiringNodeSignature
from hgraph._wiring._wiring_port import WiringPort
from hgraph._wiring._wiring_context import WiringContext

__all__ = ("reduce",)


def reduce(func: Callable[[TIME_SERIES_TYPE, TIME_SERIES_TYPE_1], TIME_SERIES_TYPE],
           ts: TSD[SCALAR, TIME_SERIES_TYPE_1] | TSL[TIME_SERIES_TYPE_1, SIZE],
           zero: TIME_SERIES_TYPE, is_associated: bool = True) -> TIME_SERIES_TYPE:
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
    if not isinstance(func, WiringNodeClass):
        raise RuntimeError(f"The supplied function is not a graph or node function: '{func.__name__}'")
    if not isinstance(ts, WiringPort):
        raise RuntimeError(f"The supplied time-series is not a valid input: '{ts}'")
    with WiringContext(current_signature=STATE(signature=f"reduce('{func.signature.signature}', {ts.output_type}, {zero})")):
        if type(tp_:=ts.output_type) is HgTSLTypeMetaData:
            return _reduce_tsl(func, ts, zero, is_associated)
        elif type(tp_) is HgTSDTypeMetaData:
            return _reduce_tsd(func, ts, zero)
        else:
            raise RuntimeError(f"Unexpected time-series type: {ts.output_type}")


def _reduce_tsl(func, ts, zero, is_associated):
    """For the moment, we only support fixed size TSLs. So we can lay out the reduction in the graph statically"""
    from hgraph.nodes import default
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
    def _reduce_tsd_signature(ts: TSD[SCALAR, REF[TIME_SERIES_TYPE]], zero: REF[TIME_SERIES_TYPE]) \
            -> REF[TIME_SERIES_TYPE]:
        ...
        # Used to create a WiringNodeClass template

    wp = _reduce_tsd_signature(ts, zero)
    resolved_signature = cast(WiringPort, wp).node_instance.resolved_signature
    resolved_signature = WiringNodeSignature(
        node_type=resolved_signature.node_type,
        name='reduce',
        args=resolved_signature.args,
        defaults=resolved_signature.defaults,
        input_types=resolved_signature.input_types,
        output_type=resolved_signature.output_type,
        src_location=resolved_signature.src_location,
        active_inputs=resolved_signature.active_inputs,
        valid_inputs=resolved_signature.valid_inputs,
        all_valid_inputs=resolved_signature.all_valid_inputs,
        unresolved_args=resolved_signature.unresolved_args,
        time_series_args=resolved_signature.time_series_args,
        uses_scheduler=resolved_signature.uses_scheduler,
        label=resolved_signature.label,
    )
    wiring_node = TsdReduceWiringNodeClass(resolved_signature, func)
    return wiring_node(ts, zero)
