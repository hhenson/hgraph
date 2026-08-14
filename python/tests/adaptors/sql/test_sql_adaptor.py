import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.data_catalogue import DataCatalogue, DataCatalogueEntry, DataEnvironment, DataEnvironmentEntry
from hgraph.adaptors.data_catalogue.publish import publish, publish_adaptor_impl
from hgraph.adaptors.sql import (
    SQLWriteMode,
    sql_execute_adaptor,
    sql_execute_adaptor_impl,
    sql_read_adaptor,
    sql_read_adaptor_impl,
    sql_write_adaptor,
    sql_write_adaptor_impl,
)
from hgraph.adaptors.sql.sql_publisher import SqlDataSink
from hgraph.adaptors.sql.sql_adaptor_raw import (
    sql_read_adaptor_raw,
    sql_execute_adaptor_raw_impl,
    sql_read_adaptor_raw_impl,
    sql_write_adaptor_raw_impl,
)
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


def _end_time(seconds=30):
    """Return a safety deadline; successful adaptor tests stop themselves."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=seconds)


def _environment(path):
    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("database", f"sqlite:///{path}"))
    return environment


def test_sql_read_adaptor_returns_a_typed_arrow_frame(tmp_path):
    database = tmp_path / "read.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("create table rows (name text, value integer)")
        connection.executemany("insert into rows values (?, ?)", [("a", 1), ("b", 2)])

    captured = []

    @hg.push_queue(hg.TS[str])
    def query(sender):
        sender("select name, value from rows order by value")

    @hg.sink_node
    def capture(response: hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]], engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            captured.append(response["values"].value)
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("database", sql_read_adaptor_impl)
        hg.register_adaptor(f"sqlite:///{database}", sql_read_adaptor_raw_impl)
        capture(sql_read_adaptor[_Row](query(), path="database"))

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert captured[0].to_pylist() == [
        {"name": "a", "value": 1},
        {"name": "b", "value": 2},
    ]


def test_sql_raw_adaptor_serves_repeated_requests_from_the_same_client(
    tmp_path, monkeypatch,
):
    import importlib
    import threading

    database = tmp_path / "repeated-read.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("create table rows (value integer)")
        connection.executemany("insert into rows values (?)", [(1,), (2,)])
    target = f"sqlite:///{database}"
    captured = []
    first_query_started = threading.Event()
    first_response_captured = threading.Event()
    release_first = threading.Event()
    feeder_errors = []
    feeder_threads = []
    sql_module = importlib.import_module("hgraph.adaptors.sql.sql_connection")
    connection_type = sql_module.SqlAdaptorConnectionSQLServer
    original_read = connection_type.read_database
    calls = 0

    def blocked_first_read(connection, statement):
        nonlocal calls
        calls += 1
        if calls == 1:
            first_query_started.set()
            if not release_first.wait(timeout=10.0):
                raise TimeoutError("the second request was not sent while the first SQL read was blocked")
        elif calls == 2 and not first_response_captured.wait(timeout=10.0):
            raise TimeoutError("the first SQL response was not captured before the second completed")
        return original_read(connection, statement)

    monkeypatch.setattr(connection_type, "read_database", blocked_first_read)

    @hg.push_queue(hg.TS[str])
    def query(sender):
        def feed():
            try:
                sender("select value from rows where value = 1")
                if not first_query_started.wait(timeout=10.0):
                    raise TimeoutError("the first SQL request did not start")
                sender("select value from rows where value = 2")
            except Exception as error:
                feeder_errors.append(error)
            finally:
                release_first.set()

        thread = threading.Thread(target=feed, daemon=True)
        feeder_threads.append(thread)
        thread.start()

    @hg.sink_node
    def capture(response: hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame]]],
                engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            captured.append(response["values"].value.column("value")[0].as_py())
            if captured == [1]:
                first_response_captured.set()
            if len(captured) == 2:
                engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor(target, sql_read_adaptor_raw_impl)
        capture(sql_read_adaptor_raw(query(), path=target))

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    for thread in feeder_threads:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    assert not feeder_errors

    assert captured == [1, 2]


def test_sql_write_adaptor_writes_arrow_rows(tmp_path):
    database = tmp_path / "write.sqlite"
    frame = pa.table({"name": ["a", "b"], "value": [1, 2]})

    @hg.push_queue(hg.TS[hg.Frame[_Row]])
    def data(sender):
        sender(frame)

    @hg.sink_node
    def stop_when_done(response: hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]], engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("database", sql_write_adaptor_impl)
        hg.register_adaptor(f"sqlite:///{database}", sql_write_adaptor_raw_impl)
        stop_when_done(
            sql_write_adaptor(
                path="database",
                table="rows",
                data=data(),
                mode=SQLWriteMode.OVERWRITE,
            )
        )

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    with sqlite3.connect(database) as connection:
        assert connection.execute("select name, value from rows order by value").fetchall() == [
            ("a", 1),
            ("b", 2),
        ]


def test_sql_execute_adaptor_appends_reference_timestamp_query(tmp_path, monkeypatch):
    import importlib

    database = tmp_path / "execute.sqlite"
    statements = []
    connection_module = importlib.import_module(
        "hgraph.adaptors.sql.sql_connection")

    def execute(connection, statement):
        statements.append(statement)
        with sqlite3.connect(database) as sqlite_connection:
            sqlite_connection.execute(statement.split(";", 1)[0])
        return pa.table({})

    monkeypatch.setattr(
        connection_module.SqlAdaptorConnectionSQLServer,
        "read_database", execute)

    @hg.push_queue(hg.TS[str])
    def query(sender):
        sender("create table executed (value integer)")

    @hg.sink_node
    def stop_when_done(response: hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]], engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("database", sql_execute_adaptor_impl)
        hg.register_adaptor(f"sqlite:///{database}", sql_execute_adaptor_raw_impl)
        stop_when_done(sql_execute_adaptor(query(), path="database"))

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "select name from sqlite_master where type='table' and name='executed'"
        ).fetchone() == ("executed",)
    assert statements == [
        "create table executed (value integer); select getutcdate()"
    ]


def test_catalogue_publish_routes_to_sql_write_adaptor(tmp_path):
    database = tmp_path / "catalogue.sqlite"
    frame = pa.table({"name": ["a"], "value": [1]})

    @hg.push_queue(hg.TS[hg.Frame[_Row]])
    def data(sender): sender(frame)

    @hg.sink_node
    def stop(response: hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]],
             engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("data-catalogue-publish", publish_adaptor_impl)
        hg.register_adaptor("database", sql_write_adaptor_impl)
        hg.register_adaptor(f"sqlite:///{database}", sql_write_adaptor_raw_impl)
        stop(publish[_Row]("rows", data()))

    with hg.GlobalContext(hg.GlobalState()):
        with DataCatalogue(), _environment(database):
            DataCatalogueEntry[SqlDataSink](
                _Row, "rows", frozendict(),
                SqlDataSink("database", "rows", mode=SQLWriteMode.OVERWRITE))
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    with sqlite3.connect(database) as connection:
        assert connection.execute("select name, value from rows").fetchall() == [("a", 1)]
