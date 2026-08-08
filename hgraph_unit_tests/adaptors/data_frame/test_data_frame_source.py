from datetime import date, datetime

import polars as pl
import pytest

from hgraph import GraphConfiguration, evaluate_graph, graph, TSB, ts_schema, TS
from hgraph.adaptors.data_frame import (
    PolarsDataFrameSource,
    DataStore,
    DataConnectionStore,
    SqlDataFrameSource,
    tsb_from_data_source,
)
from hgraph.adaptors.data_frame._data_source_generators import ts_from_data_source


class MockDataSource(PolarsDataFrameSource):

    def __init__(self):
        df = pl.DataFrame({
            "date": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
            "name": ["John", "Alice", "Bob"],
            "age": [25, 30, 35],
        })
        super().__init__(df)


@pytest.mark.serial
@pytest.mark.xfail(raises=RuntimeError, reason="When run on the CI/CD this triggers when doing a release (xdist issue)")
def test_ts_data_source():
    @graph
    def main() -> TS[int]:
        return ts_from_data_source(MockDataSource, "date", "age")

    with DataStore() as store:
        config = GraphConfiguration()
        result = evaluate_graph(main, config)

    assert result == [(datetime(2020, 1, 1), 25), (datetime(2020, 1, 2), 30), (datetime(2020, 1, 3), 35)]


@pytest.mark.serial
@pytest.mark.xfail(raises=RuntimeError, reason="When run on the CI/CD this triggers when doing a release (xdist issue)")
def test_data_source():
    @graph
    def main() -> TSB[ts_schema(name=TS[str], age=TS[int])]:
        return tsb_from_data_source(MockDataSource, "date")

    with DataStore() as store:
        config = GraphConfiguration()
        result = evaluate_graph(main, config)

    assert result == [
        (datetime(2020, 1, 1), {"name": "John", "age": 25}),
        (datetime(2020, 1, 2), {"name": "Alice", "age": 30}),
        (datetime(2020, 1, 3), {"name": "Bob", "age": 35}),
    ]


CREATE_TBL_SQL = """
CREATE TABLE my_table (
    date DATE,
    name TEXT,
    age INTEGER,
    PRIMARY KEY (date, name)
);
"""

INSERT_TEST_DATA = [
    "INSERT INTO my_table (date, name, age) VALUES ('2020-01-01', 'John', 25);",
    "INSERT INTO my_table (date, name, age) VALUES ('2020-01-02', 'Alice', 30);",
    "INSERT INTO my_table (date, name, age) VALUES ('2020-01-03', 'Bob', 35);",
]


@pytest.fixture(scope="function")
def connection():
    # sqlite3 rather than a third-party engine: this exercises SqlDataFrameSource, which only
    # needs something polars.read_database accepts, and the standard library provides that
    # without adding a native dependency to every install.
    #
    # PARSE_DECLTYPES is what makes the DATE column arrive as a date rather than a string, so the
    # frame carries the same polars schema a richer engine would produce.
    import sqlite3

    conn = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def age_data(connection):
    connection.execute(CREATE_TBL_SQL)
    connection.commit()
    for ins in INSERT_TEST_DATA:
        connection.execute(ins)
    connection.commit()
    print("Data loaded")
    yield
    connection.execute("DROP TABLE IF EXISTS my_table")
    connection.commit()


@pytest.fixture(scope="function")
def data_store_connection(connection):
    # The store is a process-wide singleton whose register_instance() raises if one is already
    # registered, so leaking it does not fail the test that leaked it, it fails whichever test
    # runs next. The fixture therefore owns registration and release rather than leaving it to
    # each test body to remember; release_instance() is idempotent, so this is safe even if a
    # test enters the store as a context manager itself.
    dsc = DataConnectionStore()
    dsc.set_connection("sqlite", connection)
    with dsc:
        yield dsc


def test_db_source(age_data, data_store_connection):

    class AgeDataSource(SqlDataFrameSource):

        def __init__(self):
            super().__init__("SELECT date, name, age FROM my_table", "sqlite")

    @graph
    def main() -> TSB[ts_schema(name=TS[str], age=TS[int])]:
        return tsb_from_data_source(AgeDataSource, "date")

    with DataStore():
        config = GraphConfiguration()
        result = evaluate_graph(main, config)

    assert result == [
        (datetime(2020, 1, 1), {"name": "John", "age": 25}),
        (datetime(2020, 1, 2), {"name": "Alice", "age": 30}),
        (datetime(2020, 1, 3), {"name": "Bob", "age": 35}),
    ]
