"""The data_frame adaptor exposes the upstream hgraph module-level surface.

Upstream user code imports these names both from the package root and from
the private module paths (upstream's own ``_lift`` does the latter), so both
import styles are pinned here alongside light behavioural checks of the
parity delegates.
"""

from dataclasses import dataclass

import pyarrow as pa
from frozendict import frozendict as fd

from hgraph import TS, TSD, CompoundScalar, Frame, graph
from hgraph.test import eval_node

# Every module-level name upstream exposes, importable from the package root.
UPSTREAM_SURFACE = (
    "DATA_FRAME_RECORD_REPLAY",
    "DATA_FRAME_RECORD_REPLAY_PATH",
    "DATA_FRAME_RECORD_OVERRIDES",
    "ON_TYPE",
    "SIZE_1",
    "tuple_resolver",
    "group_by",
    "group_by_single",
    "group_by_tuple",
    "ungroup",
    "ungroup_default",
    "ungroup_with_key",
    "ungroup_with_keys",
    "ungroup_from_items",
    "concat",
    "concat_frames",
    "with_columns",
    "with_columns_default",
    "with_columns_typed",
    "MemoryDataFrameStorage",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "replay_data_frame",
    "WriteMode",
)


def test_package_root_exposes_upstream_surface():
    import hgraph.adaptors.data_frame as df

    missing = [name for name in UPSTREAM_SURFACE if not hasattr(df, name)]
    assert not missing, missing


def test_private_module_paths_match_upstream_import_sites():
    # Upstream's _lift imports from the private module path; user code copies it.
    from hgraph.adaptors.data_frame._data_frame_record_replay import (  # noqa: F401
        DATA_FRAME_RECORD_REPLAY,
        DATA_FRAME_RECORD_REPLAY_PATH,
        DATA_FRAME_RECORD_OVERRIDES,
        MemoryDataFrameStorage,
    )

    assert DATA_FRAME_RECORD_REPLAY == ":data_frame:__data_frame_record_replay__"
    assert DATA_FRAME_RECORD_REPLAY_PATH == ":data_frame:__path__"
    assert DATA_FRAME_RECORD_OVERRIDES == ":data_frame:__overrides__"


@dataclass(frozen=True)
class Row(CompoundScalar):
    k: str
    v: int


def test_group_by_single_delegate():
    from hgraph.adaptors.data_frame import group_by_single

    @graph
    def g(ts: TS[Frame[Row]]) -> TSD[str, TS[Frame[Row]]]:
        return group_by_single(ts, "k")

    frame = pa.table({"k": ["a", "a", "b"], "v": [1, 2, 3]})
    (out,) = eval_node(g, ts=[frame])
    assert set(out.keys()) == {"a", "b"}
    assert out["a"].column("v").to_pylist() == [1, 2]


@dataclass(frozen=True)
class Value(CompoundScalar):
    v: int


def test_ungroup_with_key_delegate():
    from hgraph.adaptors.data_frame import ungroup_with_key

    @graph
    def g(ts: TSD[str, TS[Frame[Value]]]) -> TS[Frame[Row]]:
        return ungroup_with_key(ts, "k", _tp_out=Row)

    (out,) = eval_node(
        g,
        ts=[fd({"a": pa.table({"v": [1]}), "b": pa.table({"v": [2]})})],
    )
    assert sorted(zip(out.column("k").to_pylist(), out.column("v").to_pylist())) == [
        ("a", 1),
        ("b", 2),
    ]


def test_ungroup_from_items_delegate():
    from hgraph.adaptors.data_frame import ungroup_from_items

    @graph
    def g(ts: TSD[str, TS[Row]]) -> TS[Frame[Row]]:
        return ungroup_from_items(ts)

    (out,) = eval_node(g, ts=[fd({"a": Row(k="a", v=1), "b": Row(k="b", v=2)})])
    assert sorted(out.column("v").to_pylist()) == [1, 2]


def test_concat_frames_delegate():
    from hgraph.adaptors.data_frame import concat_frames

    @graph
    def g(ts1: TS[Frame[Row]], ts2: TS[Frame[Row]]) -> TS[Frame[Row]]:
        return concat_frames(ts1, ts2)

    a = pa.table({"k": ["a"], "v": [1]})
    b = pa.table({"k": ["b"], "v": [2]})
    (out,) = eval_node(g, ts1=[a], ts2=[b])
    assert out.column("v").to_pylist() == [1, 2]


def test_with_columns_default_delegate():
    from hgraph.adaptors.data_frame import with_columns_default

    @graph
    def g(ts: TS[Frame[Row]], v: TS[int]) -> TS[Frame[Row]]:
        return with_columns_default(ts, v=v)

    frame = pa.table({"k": ["a", "b"], "v": [1, 2]})
    (out,) = eval_node(g, ts=[frame], v=[9])
    assert out.column("v").to_pylist() == [9, 9]
