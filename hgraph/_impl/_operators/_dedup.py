from typing import Any, Callable

from multimethod import multimethod

from hgraph import (
    HgTypeMetaData,
    HgTSTypeMetaData,
    HgTSLTypeMetaData,
    HgTSBTypeMetaData,
    HgTSSTypeMetaData,
    HgTSDTypeMetaData,
    REMOVE,
)

__all__ = ["dedup_converter"]


@multimethod
def dedup_converter(tp: HgTypeMetaData) -> Callable[[Any, Any], Any]:
    """
    Build a function that, given an input time-series and its paired output, returns the minimal
    delta required to reflect only true changes (i.e., removes duplicates). The function signature is:

        fn(input_ts, output_ts) -> delta_or_none

    This mirrors the approach used by the JSON converter builders but for de-duplication.
    """
    raise RuntimeError(f"Unsupported type for dedup builder: {tp}")


@dedup_converter.register
def _(tp: HgTSTypeMetaData) -> Callable[[Any, Any], Any]:
    # Scalar time-series: emit only when changed vs output. Generic equality.
    def _fn(input_ts, output_ts):
        if not input_ts.modified:
            return None
        if not output_ts.valid:
            # First tick to populate output
            return input_ts.value
        # Only emit if different from the last output value
        if input_ts.value == output_ts.value:
            return None
        return input_ts.delta_value

    return _fn


@dedup_converter.register
def _(tp: HgTSDTypeMetaData) -> Callable[[Any, Any], Any]:
    v_builder = dedup_converter(tp.value_tp)

    def _fn(input_ts, output_ts):
        out = {}
        # Handle modified items
        for k, v_in in input_ts.modified_items():
            v_out = output_ts.get_or_create(k)
            if (d := v_builder(v_in, v_out)) is not None:
                out[k] = d
        # Handle removed keys
        for k in input_ts.removed_keys():
            out[k] = REMOVE
        return out or None

    return _fn


@dedup_converter.register
def _(tp: HgTSSTypeMetaData) -> Callable[[Any, Any], Any]:
    # Sets report added/removed as their delta; nothing to dedup beyond that.
    def _fn(input_ts, output_ts):
        return input_ts.delta_value

    return _fn


@dedup_converter.register
def _(tp: HgTSLTypeMetaData) -> Callable[[Any, Any], Any]:
    el_builder = dedup_converter(tp.value_tp)

    def _fn(input_ts, output_ts):
        out = {}
        for idx, v_in in input_ts.modified_items():
            v_out = output_ts[idx]
            if (d := el_builder(v_in, v_out)) is not None:
                out[idx] = d
        return out or None

    return _fn


@dedup_converter.register
def _(tp: HgTSBTypeMetaData) -> Callable[[Any, Any], Any]:
    field_builders: dict[str, Callable[[Any, Any], Any]] = {
        k: dedup_converter(v) for k, v in tp.bundle_schema_tp.meta_data_schema.items()
    }

    def _fn(input_ts, output_ts):
        out = {}
        for k, v_in in input_ts.items():
            if not v_in.modified:
                continue
            v_out = output_ts[k]
            if (d := field_builders[k](v_in, v_out)) is not None:
                out[k] = d
        return out or None

    return _fn
