"""to-frame ``convert`` targets (hgraph parity, ARROW-NATIVE).

Ports ext/main ``_impl/_operators/_conversion_operators/_to_data_frame_converters.py``
onto the Arrow substrate (ruling 2026-07-17: Arrow is the Frame
representation; polars interop is boundary conversion). Frames are built as
``pyarrow`` tables; consumers convert with ``pl.from_arrow`` when they want
polars.

Because frame-of-schema has no generic registry pattern, these converters
resolve at Python wiring through the ``convert`` target-handler extension
(:func:`hgraph._wiring._compose.register_convert_target_handler`) — the same
import-to-activate shape as upstream (``import hgraph.adaptors.data_frame``).
"""
from datetime import date, datetime

import pyarrow as pa

import _hgraph
from hgraph._wiring._compose import register_convert_target_handler
from hgraph._wiring._core import WiringPort, WiringError, _unwrap

__all__ = ("register_to_frame_converters",)

_BITEMPORAL = ("__date_time__", "__as_of__")

# column kind tags carried to the runtime bodies (wiring-time scalars)
_KIND_BY_PY = {int: "int", float: "float", bool: "bool", str: "str",
               datetime: "dt", date: "date"}
_PA_BY_KIND = {"int": pa.int64(), "float": pa.float64(), "bool": pa.bool_(),
               "str": pa.string(), "dt": pa.timestamp("us"), "date": pa.date32()}


def _pa_schema(cols):
    return pa.schema([(name, _PA_BY_KIND.get(kind, pa.string())) for name, kind in cols])


def _is_frame_ts(handle) -> bool:
    try:
        return _hgraph.ts_value_vt(handle).name.split("[")[0] == "frame"
    except Exception:
        return False


def _is_bare_frame(handle) -> bool:
    return _hgraph.ts_value_vt(handle).name == "frame"


def _frame_columns(target):
    """[(name, kind-tag)] for a parameterized TS[Frame[cs]] target."""
    from hgraph._table import table_schema, get_table_schema_date_key, get_table_schema_as_of_key

    schema = table_schema(target).value
    drop = set(_BITEMPORAL) | {get_table_schema_date_key(), get_table_schema_as_of_key()}
    return [(name, _KIND_BY_PY.get(tp, "str"))
            for name, tp in zip(schema.keys, schema.types) if name not in drop]


def _scalar_kind_of(port) -> str:
    name = _hgraph.ts_value_vt(_unwrap(port).ts_type).name
    return {"int": "int", "float": "float", "bool": "bool", "str": "str",
            "date_time": "dt", "date": "date"}.get(name, "str")


def _frame_target_for(cols):
    from hgraph import TS, Frame, compound_scalar

    py = {"int": int, "float": float, "bool": bool, "str": str,
          "dt": datetime, "date": date}
    return TS[Frame[compound_scalar(**{name: py[kind] for name, kind in cols})]]


def _ts_columns(cols, value_col, dt_col, dt_is_date, scalar_kind):
    """Upstream _ts_frame_cs_check_and_defaults: validate/derive the 1-2
    column shape for the scalar-TS conversion."""
    if cols is None:   # bare TS[Frame]: derive from kwargs
        if value_col is None:
            raise WiringError("to_frame: value_col cannot be None")
        cols = ([(dt_col, "date" if dt_is_date else "dt")] if dt_col else [])
        cols.append((value_col, scalar_kind))
        return cols, value_col, dt_col, dt_is_date
    if len(cols) > 2 or len(cols) < 1:
        raise WiringError(f"to_frame_ts requires 1-2 columns, got: {cols}")
    if len(cols) == 1 and dt_col is not None:
        raise WiringError(f"to_frame_ts cannot have only one column with dt_col('{dt_col}')")
    if dt_col is None and len(cols) == 2:
        dt_name, dt_kind = cols[0]
        if dt_kind not in ("dt", "date"):
            raise WiringError(
                f"to_frame_ts dt_col('{dt_name}') is {dt_kind}, not date or datetime")
        dt_col, dt_is_date = dt_name, dt_kind == "date"
    if value_col is None:
        value_col = cols[-1][0]
    return cols, value_col, dt_col, dt_is_date


def _tsb_columns(cols, field_kinds, dt_col, dt_is_date, map_):
    """Upstream _tsb_frame_cs_checker: match TSB fields onto the frame
    schema, deriving the dt column when the schema carries one extra."""
    renamed = {(map_ or {}).get(name, name): kind for name, kind in field_kinds}
    if cols is None:   # bare TS[Frame]: schema = [dt?] + renamed fields
        out = ([(dt_col, "date" if dt_is_date else "dt")] if dt_col else [])
        return out + list(renamed.items()), dt_col, dt_is_date
    frame = dict(cols)
    if len(field_kinds) + 1 == len(frame):
        if dt_col is None:
            dt_col = next(iter(frame))
        if frame.get(dt_col) not in ("dt", "date"):
            raise WiringError(f"to_frame dt_col('{dt_col}') is not date or datetime")
        dt_is_date = frame.pop(dt_col) == "date"
    if renamed.keys() != frame.keys():
        raise WiringError(
            f"to_frame unable to map from {list(renamed)} to {list(frame)}"
            f"{' using ' + str(dict(map_)) if map_ else ''}")
    return cols, dt_col, dt_is_date


def _convert_frame_target(target, inputs, kwargs):
    from hgraph._types import _TsExpr

    if not isinstance(target, _TsExpr) or not _is_frame_ts(target.handle):
        return None
    if len(inputs) != 1 or not isinstance(inputs[0], WiringPort):
        return None
    port = inputs[0]
    handle = _unwrap(port).ts_type

    value_col = kwargs.pop("value_col", None)
    dt_col = kwargs.pop("dt_col", None)
    dt_is_date = bool(kwargs.pop("dt_is_date", False))
    map_ = kwargs.pop("map_", None)
    mapping = kwargs.pop("mapping", None)
    bare = _is_bare_frame(target.handle)
    cols = None if bare else _frame_columns(target)

    if _is_frame_ts(handle):
        # frame -> frame: identity / column rename (mapping=)
        out_target = target if not bare else _frame_target_for(_frame_columns(
            _TsExpr(handle, "frame-in")))
        return _to_frame_from_frame[_TS_VAR: out_target](
            port, mapping=tuple((mapping or {}).items()))

    if getattr(handle, "is_tsb", False):
        fields = [(name, _hgraph.ts_value_vt(field_ts).name)
                  for name, field_ts in _hgraph.ts_field_types(handle)]
        field_kinds = [(name, {"int": "int", "float": "float", "bool": "bool",
                               "str": "str", "date_time": "dt", "date": "date"
                               }.get(vt, "str")) for name, vt in fields]
        cols, dt_col, dt_is_date = _tsb_columns(cols, field_kinds, dt_col, dt_is_date, map_)
        out_target = target if not bare else _frame_target_for(cols)
        return _to_frame_from_tsb[_TS_VAR: out_target](
            port,
            fields=tuple(name for name, _ in field_kinds),
            out_cols=tuple((map_ or {}).get(name, name) for name, _ in field_kinds),
            schema_cols=tuple(cols),
            dt_col=dt_col or "",
            dt_is_date=dt_is_date)

    # scalar TS
    cols, value_col, dt_col, dt_is_date = _ts_columns(
        cols, value_col, dt_col, dt_is_date, _scalar_kind_of(port))
    out_target = target if not bare else _frame_target_for(cols)
    return _to_frame_from_ts[_TS_VAR: out_target](
        port,
        value_col=value_col,
        schema_cols=tuple(cols),
        dt_col=dt_col or "",
        dt_is_date=dt_is_date)


def _define_nodes():
    global _to_frame_from_ts, _to_frame_from_tsb, _to_frame_from_frame, _TS_VAR
    from hgraph import TIME_SERIES_TYPE, TS, SCALAR, compute_node
    from hgraph._types import TSB, TS_SCHEMA

    _TS_VAR = TIME_SERIES_TYPE

    @compute_node
    def _to_frame_from_ts(ts: TS[SCALAR], value_col: str, schema_cols: tuple,
                          dt_col: str = "", dt_is_date: bool = False) -> TIME_SERIES_TYPE:
        row = {}
        if dt_col:
            when = ts.last_modified_time
            row[dt_col] = [when.date() if dt_is_date else when]
        row[value_col] = [ts.value]
        return pa.table(row, schema=_pa_schema(schema_cols))

    @compute_node
    def _to_frame_from_tsb(ts: TSB[TS_SCHEMA], fields: tuple, out_cols: tuple,
                           schema_cols: tuple, dt_col: str = "",
                           dt_is_date: bool = False) -> TIME_SERIES_TYPE:
        row = {}
        if dt_col:
            when = ts.last_modified_time
            row[dt_col] = [when.date() if dt_is_date else when]
        for name, out_name in zip(fields, out_cols):
            child = ts[name]
            row[out_name] = [child.value if child.valid else None]
        return pa.table(row, schema=_pa_schema(schema_cols))

    @compute_node
    def _to_frame_from_frame(ts: TIME_SERIES_TYPE, mapping: tuple = ()) -> TIME_SERIES_TYPE:
        table = ts.value
        if not mapping:
            return table
        renames = dict(mapping)
        keep = [name for name in table.schema.names if renames.get(name, name) is not None]
        return table.select(keep).rename_columns(
            [renames.get(name, name) for name in keep])


_defined = False


def register_to_frame_converters():
    """Idempotently register the frame convert targets (import-to-activate)."""
    global _defined
    if not _defined:
        _define_nodes()
        _defined = True
    register_convert_target_handler(_convert_frame_target)


register_to_frame_converters()
