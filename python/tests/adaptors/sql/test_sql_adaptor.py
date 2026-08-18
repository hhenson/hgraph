import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.data_catalogue import (
    DataCatalogue, DataCatalogueEntry, DataEnvironment, DataEnvironmentEntry,
    Scope,
)
from hgraph.adaptors.data_catalogue.publish import publish, publish_adaptor_impl
from hgraph.adaptors.sql import (
    BatchSqlDataSource,
    SQLWriteMode,
    sql_adaptor_batch,
    sql_adaptor_batch_impl,
    sql_execute_adaptor,
    sql_execute_adaptor_impl,
    sql_read_adaptor,
    sql_read_adaptor_impl,
    sql_write_adaptor,
    sql_write_adaptor_impl,
)
from hgraph.adaptors.sql.sql_connection import parse_connection_params
from hgraph.adaptors.sql.sql_publisher import SqlDataSink
from hgraph.adaptors.sql.sql_subscriber import SqlDataSource
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


@dataclass(frozen=True)
class _BatchRow(hg.CompoundScalar):
    symbol: str
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


def test_sql_batch_adaptor_coalesces_and_filters_requests(tmp_path):
    database = tmp_path / "batch.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("create table rows (symbol text, value integer)")
        connection.executemany(
            "insert into rows values (?, ?)", [("A", 1), ("B", 2)])

    class _SymbolSequenceScope(Scope):
        def in_scope(self, value):
            return isinstance(value, str)

        def adjust(self, value):
            if isinstance(value, str):
                return value
            return ", ".join(f"'{item}'" for item in value)

    source = BatchSqlDataSource(
        source_path="database",
        name="rows",
        query="select symbol, value from rows where symbol in ({symbol})",
        filters={"symbol": "symbol = '{0}'"},
    )
    scope = frozendict({"symbol": _SymbolSequenceScope()})
    captured = {}

    @hg.sink_node
    def capture(
        name: str,
        response: hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_BatchRow]]]],
        engine: hg.EvaluationEngineApi = None,
    ):
        if response.status.value is StreamStatus.OK:
            captured[name] = response["values"].value.to_pylist()
            if len(captured) == 2:
                engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor(
            "database", sql_adaptor_batch_impl,
            batch_period=timedelta(milliseconds=10))
        hg.register_adaptor(
            f"sqlite:///{database}", sql_read_adaptor_raw_impl)
        capture("A", sql_adaptor_batch[_BatchRow](
            source, scope, {"symbol": "A"}, path="database"))
        capture("B", sql_adaptor_batch[_BatchRow](
            source, scope, {"symbol": "B"}, path="database"))

    with hg.GlobalContext(hg.GlobalState()):
        with _environment(database):
            hg.run_graph(
                app, run_mode=hg.EvaluationMode.REAL_TIME,
                end_time=_end_time())

    assert captured == {
        "A": [{"symbol": "A", "value": 1}],
        "B": [{"symbol": "B", "value": 2}],
    }


def test_sql_query_substitutions(monkeypatch):
    import hgraph.adaptors.sql.sql_connection as connection_module

    monkeypatch.setattr(
        connection_module, "get_secret",
        lambda name: {"password": "secret-value"})
    monkeypatch.setenv("SQL_TEST_VALUE", "environment-value")
    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("other", "catalogue-value"))
    source = SqlDataSource(
        source_path="database",
        query=(
            "select '{secret:database/password}', '{$SQL_TEST_VALUE}', "
            "'{dataenv:other}', '{symbol}'"
        ),
    )

    with environment:
        assert source.render(symbol="ABC") == (
            "select 'secret-value', 'environment-value', "
            "'catalogue-value', 'ABC'"
        )


def test_connection_substitution_preserves_literal_braces():
    scheme, path, options = parse_connection_params(
        "mssql+pyodbc://server/database?driver={ODBC Driver 18 for SQL Server}")

    assert scheme == "mssql+pyodbc"
    assert path == "mssql+pyodbc://server/database"
    assert options == {"driver": "{ODBC Driver 18 for SQL Server}"}
