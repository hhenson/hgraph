from collections import defaultdict

from hgraph import graph, TIME_SERIES_TYPE, TS, Frame, COMPOUND_SCALAR, SCALAR, compute_node, AUTO_RESOLVE, TSD, \
    SCALAR_1, TSB
import polars as pl

from hgraph._types._scalar_types import COMPOUND_SCALAR_1


__all__ = ("to_frame",)


@graph
def to_frame(ts: TIME_SERIES_TYPE) -> TS[Frame[COMPOUND_SCALAR]]:
    raise RuntimeError("Not implemented yet")


@compute_node(overloads=to_frame)
def to_frame_ts_scalar(ts: TS[SCALAR], _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
                       _ts_tp: type[SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
    return pl.DataFrame({next(iter(_tp.__meta_data_schema__)): ts.value})


@to_frame_ts_scalar.start
def to_frame_ts_scalar_start(_tp: type[COMPOUND_SCALAR], _ts_tp: type[SCALAR]):
    schema = _tp.__meta_data_schema__
    if len(schema) != 1:
        raise RuntimeError(f"The schema should only have one value, got: {schema}")
    if next(iter(schema.values())) != _ts_tp:
        raise RuntimeError(f"The schema type does not match that of the input TS[{_ts_tp}], got: {schema}")


@compute_node(overloads=to_frame)
def to_frame_tsd_ts(ts: TSD[SCALAR, TS[SCALAR]],
                    _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
    keys, values = zip(*ts.delta_value.items())
    return pl.DataFrame({k: v for k, v in zip(_tp.__meta_data_schema__.keys(), (keys, values))})


@compute_node(overloads=to_frame)
def to_frame_tsd_tsd_ts(ts: TSD[SCALAR, TSD[SCALAR_1, TS[SCALAR]]],
                        _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
    keys_1 = []
    keys_2 = []
    values = []
    for k1, k_v in ts.delta_value.items():
        for k2, v in k_v.items():
            keys_1.append(k1)
            keys_2.append(k2)
            values.append(v)
    return pl.DataFrame({k: v for k, v in zip(_tp.__meta_data_schema__.keys(), (keys_1, keys_2, values))})


# @compute_node(overloads=to_frame)
# def to_frame_tsb(ts: TSB[COMPOUND_SCALAR], _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
#     dv = ts.delta_value
#     return pl.DataFrame({k: [dv.get(k, None)] for k in _tp.__meta_data_schema__.keys()})


# @compute_node(overloads=to_frame)
# def to_frame_tsb_tsb(ts: TSB[SCALAR, TSB[COMPOUND_SCALAR_1]],
#                      _tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
#     schema_keys = [k for k in ts.__meta_data_schema__.keys()]
#     values = defaultdict(list)
#     for k, v in ts.delta_value.items():
#         values[schema_keys[0]].append(k)
#         for k_ in schema_keys[1:]:
#             values[k_].append(v.get(k_, None))
#     return pl.DataFrame(values)
