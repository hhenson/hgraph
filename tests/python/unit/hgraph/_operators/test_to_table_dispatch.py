from dataclasses import dataclass

from frozendict import frozendict as fd

from hgraph import TS, CompoundScalar, TimeSeriesSchema, TSB, compute_node, TIME_SERIES_TYPE, TABLE, DEFAULT, TSD
from hgraph._impl._operators._to_table_dispatch_impl import extract_table_schema_raw_type, PartialSchema
from hgraph.test import eval_node


@compute_node(resolvers={TABLE: lambda m, s: tuple[*s["schema"].types]})
def to_table_test_fn(ts: DEFAULT[TIME_SERIES_TYPE], schema: PartialSchema) -> TS[TABLE]:
    return schema.to_table(ts)


def _compare(tp, value, table, keys, types, partition_keys=tuple(), remove_partition_keys=tuple()):
    schema = extract_table_schema_raw_type(tp)
    assert schema.keys == keys
    assert schema.types == types
    assert schema.partition_keys == partition_keys
    assert schema.remove_partition_keys == remove_partition_keys
    assert schema.from_table(iter(table)) == value
    assert eval_node(to_table_test_fn[tp], [value], schema) == [table]


def test_to_table_dispatch_ts_int():
    _compare(TS[int], 1, (1,), ("value",), (int,))


@dataclass
class MyCS1(CompoundScalar):
    a: str
    b: float


@dataclass
class MyCS2(CompoundScalar):
    p1: int
    p2: MyCS1


def test_to_table_dispatch_ts_complex():
    _compare(TS[MyCS2], MyCS2(p1=1, p2=MyCS1(a="a", b=2.0)), (1, "a", 2.0), ("p1", "p2.a", "p2.b"), (int, str, float))


@dataclass
class MyBundle1(TimeSeriesSchema):
    a: TS[str]
    b: TS[float]


@dataclass
class MyBundle2(TimeSeriesSchema):
    p1: TS[int]
    p2: TSB[MyBundle1]


def test_to_table_dispatch_tsb():
    _compare(TSB[MyBundle2], fd(p1=1, p2=fd(a="a", b=2.0)), (1, "a", 2.0), ("p1", "p2.a", "p2.b"), (int, str, float))


def test_to_table_dispatch_tsd():
    _compare(
        TSD[str, TS[int]],
        fd(p1=1, p2=2),
        ((False, "p1", 1), (False, "p2", 2)),
        ("__key_1_removed__", "__key_1__", "value"),
        (bool, str, int),
        ("__key_1__",),
        ("__key_1_removed__",),
    )
