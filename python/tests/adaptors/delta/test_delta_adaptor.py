from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa

import hgraph as hg
from hgraph.adaptors.delta import (
    DeltaSchemaMode,
    DeltaWriteMode,
    delta_query_adaptor,
    delta_query_adaptor_impl,
    delta_read_adaptor,
    delta_read_adaptor_impl,
    delta_write_adaptor,
    delta_write_adaptor_impl,
    publish_tsd_to_delta_table,
)
from hgraph.adaptors.data_catalogue import DataEnvironment, DataEnvironmentEntry
from hgraph.adaptors.delta.delta_adaptor_raw import delta_table_maintenance
from hgraph.adaptors.delta.delta_tsd_publisher import tsd_to_frame_batched
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


def _end_time():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5)


def _environment(path):
    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("memory", f"{path}/"))
    return environment


def test_delta_read_is_arrow_native_and_typed(tmp_path):
    from deltalake import write_deltalake
    write_deltalake(str(tmp_path / "rows"), pa.table(
        {"name": ["b", "a"], "value": [2, 1]}))
    stream_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]
    values = []

    @hg.sink_node
    def capture(response: stream_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            values.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", delta_read_adaptor_impl)
        hg.register_adaptor(f"{tmp_path}/", __import__(
            "hgraph.adaptors.delta.delta_adaptor_raw", fromlist=["delta_read_adaptor_raw_impl"]
        ).delta_read_adaptor_raw_impl)
        capture(delta_read_adaptor[_Row](path="memory", table="rows", sort=(("value", True),)))

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(tmp_path):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert values[0].equals(pa.table({"name": ["a", "b"], "value": [1, 2]}))


def test_delta_query_uses_the_injected_backend(tmp_path):
    from deltalake import write_deltalake
    expected = pa.table({"name": ["b", "a"], "value": [2, 1]})
    write_deltalake(str(tmp_path / "rows"), expected)
    values = []
    stream_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]

    @hg.sink_node
    def capture(response: stream_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            values.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", delta_query_adaptor_impl)
        hg.register_adaptor(f"{tmp_path}/", __import__(
            "hgraph.adaptors.delta.delta_adaptor_raw", fromlist=["delta_query_adaptor_raw_impl"]
        ).delta_query_adaptor_raw_impl)
        capture(
            delta_query_adaptor[_Row](
                path="memory", tables=frozenset({"rows"}), query="select * from rows"
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(tmp_path):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert values[0].column_names == expected.column_names
    assert values[0].to_pylist() == expected.to_pylist()


def test_delta_write_preserves_modes_keys_and_partitions(tmp_path):
    from deltalake import DeltaTable, write_deltalake

    write_deltalake(str(tmp_path / "rows"), pa.table(
        {"name": ["a", "b"], "value": [0, 2]}), partition_by=["name"])
    frame = pa.table({"name": ["a"], "value": [1]})

    @hg.push_queue(hg.TS[hg.Frame[_Row]])
    def data(sender):
        sender(frame)

    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]

    @hg.sink_node
    def stop(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("memory", delta_write_adaptor_impl)
        hg.register_adaptor(f"{tmp_path}/", __import__(
            "hgraph.adaptors.delta.delta_adaptor_raw", fromlist=["delta_write_adaptor_raw_impl"]
        ).delta_write_adaptor_raw_impl)
        stop(
            delta_write_adaptor(
                path="memory",
                table="rows",
                data=data(),
                write_mode=DeltaWriteMode.OVERWRITE,
                schema_mode=DeltaSchemaMode.MERGE,
                keys=("name",),
                partition=("name",),
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(tmp_path):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    result = DeltaTable(str(tmp_path / "rows")).to_pyarrow_table()
    assert sorted(result.to_pylist(), key=lambda row: row["name"]) == [
        {"name": "a", "value": 1},
        {"name": "b", "value": 2},
    ]


def test_delta_table_maintenance_compacts_and_vacuums(monkeypatch):
    calls = []

    class _Optimize:
        def compact(self):
            calls.append(("compact",))

    class _DeltaTable:
        def __init__(self, path, storage_options=None):
            calls.append(("open", path, storage_options))
            self.optimize = _Optimize()

        def vacuum(self, **kwargs):
            calls.append(("vacuum", kwargs))

    monkeypatch.setattr(
        "hgraph.adaptors.delta.delta_adaptor_raw._require_delta_lake",
        lambda: (_DeltaTable, None, None),
    )

    @hg.graph
    def app() -> hg.TS[bool]:
        delta_table_maintenance(
            "memory",
            "rows",
            periodic=hg.MIN_TD,
            start=hg.MIN_ST,
        )
        return hg.const(True)

    with hg.GlobalContext(hg.GlobalState()):
        hg.eval_node(app, __end_time__=hg.MIN_ST + 2 * hg.MIN_TD)

    assert calls == [
        ("open", "memory/rows", None),
        ("compact",),
        ("vacuum", {
            "retention_hours": 1,
            "enforce_retention_duration": False,
            "dry_run": False,
        }),
    ]


def test_tsd_delta_publisher_uses_the_native_table_codec(tmp_path):
    from deltalake import DeltaTable

    @hg.push_queue(hg.TSD[int, hg.TS[_Row]])
    def rows(sender):
        sender({1: _Row("a", 1)})

    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]

    @hg.sink_node
    def stop(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor(f"{tmp_path}/", __import__(
            "hgraph.adaptors.delta.delta_adaptor_raw", fromlist=["delta_write_adaptor_raw_impl"]
        ).delta_write_adaptor_raw_impl)
        stop(publish_tsd_to_delta_table("history", rows(), max_rows=1))

    with hg.GlobalContext(hg.GlobalState()):
        environment = DataEnvironment()
        environment.add_entry(DataEnvironmentEntry(
            "table_history_path", f"{tmp_path}/"))
        with environment:
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    frame = DeltaTable(str(tmp_path / "history")).to_pyarrow_table()
    assert frame.column_names[-3:] == ["key", "name", "value"]
    assert frame.select(["key", "name", "value"]).to_pylist() == [
        {"key": 1, "name": "a", "value": 1}
    ]


def _expected_tsd_batch(rows, as_of):
    return pa.Table.from_pylist([
        {
            "__date__": timestamp.date(),
            "__timestamp__": timestamp,
            "__is_deleted__": False,
            "key": key,
            "name": name,
            "value": value,
        }
        for timestamp, key, name, value in rows
    ])


def test_tsd_to_frame_batches_scalar_values_by_row_count():
    @hg.graph
    def app(tsd: hg.TSD[str, hg.TS[_Row]]) -> hg.TS[object]:
        return tsd_to_frame_batched(tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = hg.MIN_ST + 10 * hg.MIN_TD
    with hg.GlobalState():
        hg.set_as_of(as_of)
        out = hg.eval_node(
            app,
            [{"a": _Row("a", 1)}, {"b": _Row("b", 2)}],
            __end_time__=hg.MIN_ST + timedelta(milliseconds=10),
            __elide__=True,
        )

    expected = _expected_tsd_batch([
        (hg.MIN_ST, "a", "a", 1),
        (hg.MIN_ST + hg.MIN_TD, "b", "b", 2),
    ], as_of)
    assert out == [expected]


def test_tsd_to_frame_batches_bundle_values_by_row_count():
    @hg.graph
    def app(tsd: hg.TSD[str, hg.TSB[_Row]]) -> hg.TS[object]:
        return tsd_to_frame_batched(tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = hg.MIN_ST + 10 * hg.MIN_TD
    with hg.GlobalState():
        hg.set_as_of(as_of)
        out = hg.eval_node(
            app,
            [{"a": _Row("a", 1)}, {"b": _Row("b", 2)}],
            __end_time__=hg.MIN_ST + timedelta(milliseconds=10),
            __elide__=True,
        )

    expected = _expected_tsd_batch([
        (hg.MIN_ST, "a", "a", 1),
        (hg.MIN_ST + hg.MIN_TD, "b", "b", 2),
    ], as_of)
    assert out == [expected]


def test_tsd_to_frame_emits_multiple_row_count_batches():
    @hg.graph
    def app(tsd: hg.TSD[str, hg.TS[_Row]]) -> hg.TS[object]:
        return tsd_to_frame_batched(tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = hg.MIN_ST + 10 * hg.MIN_TD
    with hg.GlobalState():
        hg.set_as_of(as_of)
        out = hg.eval_node(
            app,
            [
                {"a": _Row("a", 1)},
                {"b": _Row("b", 2)},
                {"a": _Row("a", 3)},
                {"c": _Row("c", 4)},
            ],
            __end_time__=hg.MIN_ST + timedelta(milliseconds=10),
            __elide__=True,
        )

    assert out == [
        _expected_tsd_batch([
            (hg.MIN_ST, "a", "a", 1),
            (hg.MIN_ST + hg.MIN_TD, "b", "b", 2),
        ], as_of),
        _expected_tsd_batch([
            (hg.MIN_ST + 2 * hg.MIN_TD, "a", "a", 3),
            (hg.MIN_ST + 3 * hg.MIN_TD, "c", "c", 4),
        ], as_of),
    ]
