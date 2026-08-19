"""The data_frame adaptor exposes the upstream hgraph module-level surface.

Upstream user code imports these names both from the package root and from
the private module paths (upstream's own ``_lift`` does the latter), so both
import styles are pinned here alongside light behavioural checks of the
parity delegates.
"""

from dataclasses import dataclass
import inspect

import pyarrow as pa
import pytest
from frozendict import frozendict as fd

from hgraph import TS, TSD, CompoundScalar, Frame, graph
from hgraph.test import eval_node

# Exact curated package contract from upstream hgraph after hgraph#368, plus
# hg_cpp's documented Arrow-native source extension.
PUBLIC_SURFACE = (
    "join",
    "filter_frame",
    "filter_cs",
    "filter_exp",
    "filter_exp_seq",
    "group_by",
    "ungroup",
    "sorted_",
    "concat",
    "with_columns",
    "DATA_FRAME_RECORD_REPLAY",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "replay_data_frame",
    "WriteMode",
    "DataFrameStorage",
    "BaseDataFrameStorage",
    "FileBasedDataFrameStorage",
    "MemoryDataFrameStorage",
    "DataFrameSource",
    "DataStore",
    "DATA_FRAME_SOURCE",
    "DataConnectionStore",
    "SqlDataFrameSource",
    "PolarsDataFrameSource",
    "ArrowDataFrameSource",
    "tsb_from_data_source",
    "tsd_k_a_from_data_source",
    "ts_of_matrix_from_data_source",
    "tsd_k_v_from_data_source",
    "tsd_k_b_from_data_source",
    "tsd_k_tsd_from_data_source",
    "ts_of_array_from_data_source",
    "tsl_from_data_source",
    "schema_from_frame",
)


def test_package_root_has_exact_curated_surface():
    import hgraph.adaptors.data_frame as df

    assert tuple(df.__all__) == PUBLIC_SURFACE
    assert not hasattr(df, "group_by_single")
    assert not hasattr(df, "record_to_data_frame")
    assert not hasattr(df, "SIZE_1")


def test_private_module_paths_match_upstream_import_sites():
    # Upstream's _lift imports from the private module path; user code copies
    # it. The backend sentinel is core (it selects the backend); the durable
    # recording state moved to hgraph-persistence at RFC 0025 checkpoint 5 and
    # resolves through the same private path when it is installed.
    from hgraph.adaptors.data_frame._data_frame_record_replay import (  # noqa: F401
        DATA_FRAME_RECORD_REPLAY,
    )

    assert DATA_FRAME_RECORD_REPLAY == ":data_frame:__data_frame_record_replay__"

    pytest.importorskip("hgraph_persistence")
    from hgraph.adaptors.data_frame._data_frame_record_replay import (  # noqa: F401
        DATA_FRAME_RECORD_OVERRIDES,
        DATA_FRAME_RECORD_REPLAY_PATH,
        MemoryDataFrameStorage,
        get_data_frame_record_overrides,
        set_data_frame_overrides,
        set_data_frame_record_path,
    )

    assert DATA_FRAME_RECORD_REPLAY_PATH == ":data_frame:__path__"
    assert DATA_FRAME_RECORD_OVERRIDES == ":data_frame:__overrides__"


def test_native_operator_callables_expose_user_signatures():
    from hgraph.adaptors import data_frame as df

    expected = {
        "join": "(lhs, rhs, on, how='inner', suffix='_right')",
        "group_by": "(ts, by)",
        "ungroup": "(ts)",
        "sorted_": "(ts, by, descending=False)",
        "concat": "(ts1, ts2)",
        "with_columns": "(ts, **columns)",
    }
    assert {
        name: str(inspect.signature(getattr(df, name))) for name in expected
    } == expected


@dataclass(frozen=True)
class Row(CompoundScalar):
    k: str
    v: int


def test_group_by_public_delegate():
    from hgraph.adaptors.data_frame import group_by

    @graph
    def g(ts: TS[Frame[Row]]) -> TSD[str, TS[Frame[Row]]]:
        return group_by(ts, "k")

    frame = pa.table({"k": ["a", "a", "b"], "v": [1, 2, 3]})
    (out,) = eval_node(g, ts=[frame])
    assert set(out.keys()) == {"a", "b"}
    assert out["a"].column("v").to_pylist() == [1, 2]


@dataclass(frozen=True)
class Value(CompoundScalar):
    v: int


def test_ungroup_with_key_public_dispatch():
    from hgraph.adaptors.data_frame import ungroup

    @graph
    def g(ts: TSD[str, TS[Frame[Value]]]) -> TS[Frame[Row]]:
        return ungroup[TS[Frame[Row]]](ts, "k")

    (out,) = eval_node(
        g,
        ts=[fd({"a": pa.table({"v": [1]}), "b": pa.table({"v": [2]})})],
    )
    assert sorted(zip(out.column("k").to_pylist(), out.column("v").to_pylist())) == [
        ("a", 1),
        ("b", 2),
    ]


def test_ungroup_from_items_public_dispatch():
    from hgraph.adaptors.data_frame import ungroup

    @graph
    def g(ts: TSD[str, TS[Row]]) -> TS[Frame[Row]]:
        return ungroup(ts)

    (out,) = eval_node(g, ts=[fd({"a": Row(k="a", v=1), "b": Row(k="b", v=2)})])
    assert sorted(out.column("v").to_pylist()) == [1, 2]


def test_concat_public_delegate():
    from hgraph.adaptors.data_frame import concat

    @graph
    def g(ts1: TS[Frame[Row]], ts2: TS[Frame[Row]]) -> TS[Frame[Row]]:
        return concat(ts1, ts2)

    a = pa.table({"k": ["a"], "v": [1]})
    b = pa.table({"k": ["b"], "v": [2]})
    (out,) = eval_node(g, ts1=[a], ts2=[b])
    assert out.column("v").to_pylist() == [1, 2]


def test_with_columns_public_delegate():
    from hgraph.adaptors.data_frame import with_columns

    @graph
    def g(ts: TS[Frame[Row]], v: TS[int]) -> TS[Frame[Row]]:
        return with_columns(ts, v=v)

    frame = pa.table({"k": ["a", "b"], "v": [1, 2]})
    (out,) = eval_node(g, ts=[frame], v=[9])
    assert out.column("v").to_pylist() == [9, 9]
