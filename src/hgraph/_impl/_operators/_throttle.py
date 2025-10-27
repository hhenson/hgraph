from typing import Any, Callable

from multimethod import multimethod

from hgraph import (
    HgTypeMetaData,
    HgTSTypeMetaData,
    HgTSLTypeMetaData,
    HgTSBTypeMetaData,
    HgTSSTypeMetaData,
    HgTSDTypeMetaData,
    REMOVE_IF_EXISTS,
)

__all__ = ["collect_converter"]


@multimethod
def collect_converter(tp: HgTypeMetaData) -> Callable[[Any, Any], Any]:
    """
    Build a function that accumulates deltas for throttle between scheduled emits.

    Signature:
        fn(input_ts, out_tick) -> updated_out_tick

    The returned function should not mutate input_ts, but may read and combine with the provided
    out_tick (previously accumulated structure). It returns the new accumulated structure.
    """
    raise RuntimeError(f"Unsupported type for throttle collect builder: {tp}")


@collect_converter.register
def _(tp: HgTSTypeMetaData) -> Callable[[Any, Any], Any]:
    # Scalar TS: we just take the current value
    def _fn(input_ts, out_tick):
        return input_ts.value

    return _fn


@collect_converter.register
def _(tp: HgTSDTypeMetaData) -> Callable[[Any, Any], Any]:
    value_builder = collect_converter(tp.value_tp)

    def _fn(input_ts, out_tick: dict | None):
        out_tick = dict(out_tick) if out_tick else {}
        # Merge modified items (recursively accumulate per key)
        for k, v_in in input_ts.modified_items():
            v_out_prev = out_tick.get(k)
            v_new = value_builder(v_in, v_out_prev if v_out_prev is not None else {})
            if v_new is not None:
                out_tick[k] = v_new
        # Add removed keys
        for k in input_ts.removed_keys():
            out_tick[k] = REMOVE_IF_EXISTS
        return out_tick

    return _fn


@collect_converter.register
def _(tp: HgTSSTypeMetaData) -> Callable[[Any, Any], Any]:
    # Sets: accumulate added/removed across ticks
    def _fn(input_ts, out_tick):
        from hgraph import set_delta  # lazy import to avoid circulars

        if not out_tick:
            out_tick = set_delta(set(), set())
        new_added, new_removed = input_ts.added(), input_ts.removed()
        added = (out_tick.added - new_removed) | new_added
        removed = (out_tick.removed - new_added) | new_removed
        return set_delta(added, removed)

    return _fn


@collect_converter.register
def _(tp: HgTSLTypeMetaData) -> Callable[[Any, Any], Any]:
    el_builder = collect_converter(tp.value_tp)

    def _fn(input_ts, out_tick: dict | None):
        # For lists, mirror original collect_tick semantics: return only current modified indices
        new_tick: dict[int, Any] = {}
        for idx, v_in in input_ts.modified_items():
            v_new = el_builder(v_in, {})
            if v_new is not None:
                new_tick[idx] = v_new
        return new_tick

    return _fn


@collect_converter.register
def _(tp: HgTSBTypeMetaData) -> Callable[[Any, Any], Any]:
    field_builders: dict[str, Callable[[Any, Any], Any]] = {
        k: collect_converter(v) for k, v in tp.bundle_schema_tp.meta_data_schema.items()
    }

    def _fn(input_ts, out_tick: dict | None):
        out_tick = {} if out_tick is None else dict(out_tick)
        new_top: dict[str, Any] = {}
        for k, v_in in input_ts.modified_items():
            prev = out_tick.get(k)
            v_new = field_builders[k](v_in, prev if prev is not None else {})
            if v_new is not None:
                new_top[k] = v_new
        return new_top

    return _fn
