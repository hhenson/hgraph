from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import inspect
import json
import pytest
import threading
import time

import hgraph as hg
from hgraph.adaptors.perspective import (
    PerspectiveTablesManager,
    TableEdits,
    publish_multitable,
    publish_table,
    publish_table_editable,
    register_perspective_adaptors,
)


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


class _Table:
    def __init__(self, rows, index=None):
        self.definition = rows
        self.index = index
        self.updates = [] if isinstance(rows, dict) else [list(rows)]
        self.removals = []
        self.replacements = []
        self.num_views = 0
        self.deleted = False

    def update(self, rows):
        self.updates.append(list(rows))

    def remove(self, keys):
        self.removals.append(list(keys))

    def replace(self, rows):
        self.replacements.append(list(rows))

    def schema(self):
        return self.definition if isinstance(self.definition, dict) else {}

    def get_index(self):
        return self.index

    def get_num_views(self):
        return self.num_views

    def delete(self):
        self.deleted = True


class _Client:
    def __init__(self):
        self.tables = {}

    def table(self, rows, *, name, **kwargs):
        table = _Table(rows, kwargs.get("index"))
        self.tables[name] = (table, kwargs)
        return table


def test_publish_table_applies_tsd_add_modify_and_remove_deltas_with_eval_node():
    client = _Client()
    manager = PerspectiveTablesManager(client)

    @hg.graph
    def app(rows: hg.TSD[int, hg.TS[_Row]]):
        register_perspective_adaptors()
        publish_table("rows", rows, index_col_name="id")

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        assert hg.eval_node(
            app,
            [
                {1: _Row("a", 1)},
                {1: _Row("b", 2), 2: _Row("c", 3)},
                {1: hg.REMOVE},
            ],
        ) is None

    table, options = client.tables["rows"]
    assert options["index"] == "id"
    assert table.updates == [
        [{"id": 1, "name": "a", "value": 1}],
        [
            {"id": 1, "name": "b", "value": 2},
            {"id": 2, "name": "c", "value": 3},
        ],
    ]
    assert table.removals == [[1]]


def test_publish_table_operates_against_the_supported_perspective_client():
    pytest.importorskip("perspective")
    manager = PerspectiveTablesManager()

    @hg.graph
    def app(rows: hg.TSD[int, hg.TS[int]]):
        register_perspective_adaptors()
        publish_table("live_rows", rows, index_col_name="id")

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        hg.eval_node(app, [{1: 10}, {1: 11, 2: 20}, {1: hg.REMOVE}])

    view = manager.get_table("live_rows").view()
    try:
        assert view.to_records() == [{"id": 2, "value": 20}]
    finally:
        view.delete()
        manager.close()


def test_manager_edit_callbacks_do_not_require_perspective_to_be_installed():
    manager = PerspectiveTablesManager(_Client())
    updates = []
    token = manager.subscribe_table_updates(
        "rows", lambda rows, removals: updates.append((rows, removals))
    )
    manager.publish_edits("rows", [{"id": 1, "value": 2}], [3])
    manager.unsubscribe_table_updates("rows", token)
    manager.publish_edits("rows", [{"id": 2}], ())

    assert updates == [([{"id": 1, "value": 2}], [3])]


def test_manager_current_honours_an_empty_injected_global_state():
    state = hg.GlobalState()
    manager = PerspectiveTablesManager(_Client())

    PerspectiveTablesManager.set_current(manager, state)

    assert PerspectiveTablesManager.current(state) is manager


def test_perspective_public_surface_matches_the_python_adaptor():
    import hgraph.adaptors.perspective as perspective_api

    assert perspective_api.__all__ == [
        "perspective_web", "PerspectiveTablesManager", "TablePageHandler",
        "IndexPageHandler", "TableEdits", "defaultdbldict", "publish_table",
        "publish_table_editable", "publish_multitable", "publish_table_impl",
        "publish_table_editable_impl", "publish_multitable_impl",
        "register_perspective_adaptors",
    ]
    assert tuple(inspect.signature(publish_table).parameters) == (
        "path", "ts", "index_col_name", "history")
    assert tuple(inspect.signature(perspective_api.publish_table_editable).parameters) == (
        "path", "ts", "index_col_name", "history", "edit_role", "empty_row")
    assert tuple(inspect.signature(publish_multitable).parameters) == (
        "path", "key", "ts", "unique", "index_col_name", "history")
    assert not hasattr(perspective_api, "_publish_table")
    assert not hasattr(perspective_api, "_receive_table_edits")

    manager_methods = {
        "create_table": ("self", "args", "name", "editable", "user",
                         "edit_role", "temporary", "kwargs"),
        "add_table": ("self", "name", "table", "editable", "user",
                      "edit_role", "temporary"),
        "update_table": ("self", "name", "data", "removals"),
        "subscribe_table_updates": ("self", "name", "cb", "self_updates"),
    }
    for name, expected in manager_methods.items():
        assert tuple(inspect.signature(
            getattr(PerspectiveTablesManager, name)).parameters) == expected


def test_manager_configuration_stats_callbacks_and_temporary_cleanup(tmp_path):
    table_config = tmp_path / "tables.json"
    table_config.write_text(json.dumps({"from_file": {"plugin": "Datagrid"}}))
    view_config = tmp_path / "views.json"
    view_config.write_text(json.dumps({"summary": {
        "table": "rows", "group_by": ["name"],
    }}))
    manager = PerspectiveTablesManager(
        client=_Client(),
        table_config_file=table_config,
        table_configs={"inline": json.dumps({
            "from_inline": {"columns": ["value"]},
        })},
        view_config_file=view_config,
    )

    assert manager.read_table_config() == {
        "from_file": {"plugin": "Datagrid"},
        "from_inline": {"columns": ["value"]},
    }
    manager.add_table_configs({
        "second": json.dumps({"second": {"plugin": "X Bar"}}),
    })
    assert manager.read_table_config()["second"] == {"plugin": "X Bar"}
    assert manager.get_view_names() == ["summary"]
    assert manager.get_view("summary")["table"] == "rows"

    manager.start()
    assert {"index", "table_stats"} <= set(manager.get_table_names())
    manager.create_table(
        {"id": int, "value": int}, name="rows", index="id")
    manager.update_table("rows", [{"id": 1, "value": 2}])
    assert manager.get_stats()["updates"] == 1
    assert manager.get_stats()["rows"] == 1

    callbacks = []
    manager.set_loop_callback(
        lambda prefix, fn, *args, **kwargs:
        (callbacks.append(prefix), fn(*args, **kwargs))[1],
        "loop",
    )
    manager.update_table("rows", [{"id": 1, "value": 3}])
    assert callbacks == ["loop"]

    temporary = manager.create_table(
        {"id": int}, name="temporary", index="id", temporary=True)
    temporary.num_views = 1
    manager.cleanup_temporary_tables()
    temporary.num_views = 0
    manager.cleanup_temporary_tables()
    assert temporary.deleted
    assert "temporary" not in manager.get_table_names()


@dataclass(frozen=True)
class _Key(hg.CompoundScalar):
    desk: str
    book: int


class _Bundle(hg.TimeSeriesSchema):
    name: hg.TS[str]
    value: hg.TS[int]


def test_publish_table_supports_tuple_and_compound_keys_and_bundle_values():
    client = _Client()
    manager = PerspectiveTablesManager(client)

    @hg.graph
    def tuple_rows(rows: hg.TSD[tuple[int, str], hg.TSB[_Bundle]]):
        register_perspective_adaptors()
        publish_table("tuple_rows", rows, index_col_name="number,code")

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        hg.eval_node(tuple_rows, [{(1, "A"): {"name": "one", "value": 10}}])

    table, options = client.tables["tuple_rows"]
    assert options["index"] == "index"
    assert table.updates == [[{
        "index": "1,A", "number": 1, "code": "A",
        "name": "one", "value": 10,
    }]]

    client = _Client()
    manager = PerspectiveTablesManager(client)

    @hg.graph
    def compound_rows(rows: hg.TSD[_Key, hg.TS[int]]):
        register_perspective_adaptors()
        publish_table("compound_rows", rows, index_col_name="desk,book")

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        hg.eval_node(compound_rows, [{_Key("LDN", 7): 42}])

    table, options = client.tables["compound_rows"]
    assert options["index"] == "index"
    assert table.updates == [[{
        "index": "LDN,7", "desk": "LDN", "book": 7, "value": 42,
    }]]


def test_publish_table_supports_frame_rows_and_residual_index_columns():
    client = _Client()
    manager = PerspectiveTablesManager(client)

    @hg.graph
    def app(rows: hg.TSD[int, hg.TS[hg.Frame[_Row]]]):
        register_perspective_adaptors()
        publish_table("frame_rows", rows, index_col_name="id,name")

    frame = pa.Table.from_pylist([
        {"name": "a", "value": 1},
        {"name": "b", "value": 2},
    ])
    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        hg.eval_node(app, [{7: frame}, {7: pa.Table.from_pylist([
            {"name": "b", "value": 3},
        ])}])

    table, options = client.tables["frame_rows"]
    assert options["index"] == "index"
    assert table.updates[0] == [
        {"index": "7,a", "id": 7, "name": "a", "value": 1},
        {"index": "7,b", "id": 7, "name": "b", "value": 2},
    ]
    assert table.updates[1] == [
        {"index": "7,b", "id": 7, "name": "b", "value": 3},
    ]
    assert table.removals == [["7,a"]]


def test_publish_multitable_combines_multiple_clients_without_a_reply_channel():
    client = _Client()
    manager = PerspectiveTablesManager(client)

    @hg.graph
    def app(
        left_key: hg.TS[int], left: hg.TS[_Row],
        right_key: hg.TS[int], right: hg.TS[_Row],
    ) -> hg.TS[int]:
        register_perspective_adaptors()
        publish_multitable(
            "shared", left_key, left, unique=True, index_col_name="id")
        publish_multitable(
            "shared", right_key, right, unique=True, index_col_name="id")
        return left_key

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        assert hg.eval_node(
            app,
            [1, 1, 1], [_Row("a", 1), _Row("a", 2), _Row("a", 3)],
            [2, 2, 2], [_Row("b", 10), _Row("b", 20), _Row("b", 30)],
        ) == [1, 1, 1]

    table, options = client.tables["shared"]
    assert options["index"] == "id"
    assert table.updates == [
        [
            {"id": 1, "name": "a", "value": 1},
            {"id": 2, "name": "b", "value": 10},
        ],
        [
            {"id": 1, "name": "a", "value": 2},
            {"id": 2, "name": "b", "value": 20},
        ],
        [
            {"id": 1, "name": "a", "value": 3},
            {"id": 2, "name": "b", "value": 30},
        ],
    ]


def test_editable_table_emits_typed_feedback_and_unsubscribes_on_stop():
    client = _Client()
    manager = PerspectiveTablesManager(client)
    received = []
    threads = []

    @hg.push_queue(hg.TSD[int, hg.TS[_Row]])
    def rows(sender):
        def feed():
            deadline = time.monotonic() + 2.0
            while "editable" not in manager.get_table_names():
                if time.monotonic() >= deadline:
                    return
                time.sleep(0.01)
            time.sleep(0.05)
            sender({1: _Row("initial", 1)})
            manager.publish_edits(
                "editable",
                [{"id": 1, "name": "edited", "value": 2}],
                [3],
            )

        thread = threading.Thread(target=feed)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def capture(value: hg.TSB[TableEdits[int, hg.TS[_Row]]]):
        edits = {
            key: child.value
            for key, child in value.edits.modified_items()
        }
        removes = set(value.removes.added()) if value.removes.modified else set()
        if edits or removes:
            received.append((edits, removes))

    @hg.graph
    def app():
        register_perspective_adaptors()
        capture(publish_table_editable(
            "editable", rows(), index_col_name="id"))

    with hg.GlobalContext(hg.GlobalState()):
        PerspectiveTablesManager.set_current(manager)
        hg.run_graph(
            app,
            run_mode=hg.EvaluationMode.REAL_TIME,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=0.5),
        )
    for thread in threads:
        thread.join(timeout=1.0)

    assert received == [({1: _Row("edited", 2)}, {3})]
    assert not manager._subscribers["editable"]
