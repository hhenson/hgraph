import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import tempfile
from typing import Mapping

from frozendict import frozendict

from hgraph.adaptors.sql.sql_adaptor import sql_read_adaptor, sql_read_adaptor_impl
from hgraph.adaptors.sql.sql_adaptor_raw import sql_read_adaptor_raw_impl
from hgraph.adaptors.data_catalogue.subscribe import (
    subscribe,
    subscribe_adaptor_impl,
    subscriber_impl_from_graph,
    subscriber_impl_to_graph,
)
from hgraph.stream.stream import Stream, Data, StreamStatus
from hgraph.adaptors.data_catalogue.catalogue import DataCatalogueEntry, DataSource, DataEnvironment, DataEnvironmentEntry
from hgraph.adaptors.data_catalogue.data_scopes import ExponentialBackoffRetryOptions, RetryOptions, RetryScope, Scope
from hgraph import (
    LOGGER,
    STATE,
    const,
    default,
    graph,
    TS,
    TSB,
    Frame,
    SCHEMA,
    CompoundScalar,
    register_adaptor,
    debug_print,
    if_true,
    valid,
    run_graph,
    EvaluationMode,
    compute_node,
    AUTO_RESOLVE,
    SCHEDULER,
    MIN_DT,
    EvaluationClock,
)
from hgraph import stop_engine
from hgraph.stream.stream import Stream, Data

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlDataSource(DataSource):
    query: str

    def render(self, **options) -> str:
        try:
            return self.query.format(**options)
        except:
            logger.exception(f"Error rendering query for {self.__class__.__name__} {self.source_path}")
            logger.error(f"- Query: {self.query}")
            logger.error(f"- Options: {options}")


@compute_node(valid=("ds", "scope"), active=("ds", "scope", "options"))
def render_query(
    ds: TS[SqlDataSource],
    scope: TS[Mapping[str, Scope]],
    options: TS[dict[str, object]],
    feedback: TS[StreamStatus] = None,
    _scheduler: SCHEDULER = None,
    _clock: EvaluationClock = None,
    _state: STATE = None,
    _logger: LOGGER = None,
) -> TS[str]:
    scope_v = scope.value
    if len(scope_v) == 0:
        options_v = {}
    elif not options.valid:
        return
    else:
        options_v: dict = options.value

    poll = scope_v.get("poll", None)
    if poll:
        poll = poll.default()

    if (interval := options_v.get("poll", poll)) is not None:
        use_wall_clock = True
        next = (
            1 + ((_clock.now if use_wall_clock else _clock.evaluation_time) - MIN_DT) // interval
        ) * interval + MIN_DT
        _scheduler.schedule(next, "_", on_wall_clock=use_wall_clock)

    retry = scope_v.get("retry", None)
    if retry:
        retry = retry.default()
        
    if (retry_opts := options_v.get("retry", retry)) is not None:
        assert poll is None, "Cannot have both 'poll' and 'retry' options set"
        assert isinstance(retry_opts, RetryOptions)
        
        if not feedback.active:
            feedback.make_active()
        
        if ds.modified or scope.modified or options.modified:
            # reset retry state on new query
            if hasattr(_state, "retry"):
                _scheduler.un_schedule("_")
                del _state.retry
        
        elif feedback.modified:
            if feedback.value == StreamStatus.ERROR:
                retry_state = getattr(_state, "retry", retry_opts.create())
                _state.retry = retry_state
                use_wall_clock = True
                now = _clock.now
                next = retry_state.next(now)
                if next is not None:
                    _logger.info(f"Will retry SQL query  in {next - now} at {next} after error")
                    _scheduler.schedule(next, "_", on_wall_clock=use_wall_clock)
                else:
                    _logger.info(f"Max retries reached for SQL query, not retrying further")
            else:
                if hasattr(_state, "retry"):
                    _scheduler.un_schedule("_")
                    del _state.retry
            
            return # not ticking on feedback

    return ds.value.render(
        **{k: v.adjust(options_v[k]) if k in options_v else v.default() for k, v in scope_v.items()}
    )


@subscriber_impl_from_graph
def subscribe_sql_from_graph(
    dce: DataCatalogueEntry,
    ds: TS[SqlDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    feedback: TS[StreamStatus] = None,
    _schema: type[SCHEMA] = AUTO_RESOLVE,
):
    return sql_read_adaptor[_schema].from_graph(
        path=dce.store.source_path, query=render_query(ds, dce.scope, options, feedback), __request_id__=request_id
    )


@subscriber_impl_to_graph
def subscribe_sql_to_graph(
    dce: DataCatalogueEntry,
    ds: TS[SqlDataSource],
    options: TS[dict[str, object]],
    request_id: TS[int],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    return sql_read_adaptor[_schema].to_graph(
        path=dce.store.source_path, __request_id__=request_id, __no_ts_inputs__=True
    )


if __name__ == "__main__":
    import sqlite3

    dir = tempfile.gettempdir()

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(dir + "/prices.db")
    cursor = conn.cursor()

    # Create table
    cursor.execute("DROP TABLE IF EXISTS reuters_close")
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS reuters_close (
        val REAL,
        business_date DATE,
        ticker TEXT
    );
    """)
    
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS counter (
        id INTEGER PRIMARY KEY,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        val INTEGER DEFAULT 0
    );
    """)
    
    cursor.execute(
        """    
    INSERT INTO counter (id, val)     
    SELECT 1, 0 
    WHERE NOT EXISTS (SELECT 1 FROM counter WHERE id = 1);
    """)

    cursor.execute(
        """    
    UPDATE counter SET  timestamp = datetime('now') WHERE id = 1;
    """)

    # Insert sample data
    data = [
        (100.0, "2024-01-02", ".SPX"),
        (1001.0, "2024-01-02", ".FTSE"),
        (101.0, "2024-01-03", ".SPX"),
        (1011.0, "2024-01-03", ".FTSE"),
        (102.0, "2024-01-04", ".SPX"),
        (1022.0, "2024-01-04", ".FTSE"),
        (103.0, "2024-01-05", ".SPX"),
        (103.0, "2024-01-05", ".FTSE"),
        (104.0, "2024-01-06", ".SPX"),
        (1014.0, "2024-01-06", ".FTSE"),
        (105.0, "2024-01-07", ".SPX"),
        (1025.0, "2024-01-07", ".FTSE"),
    ]

    cursor.executemany(
        """
    INSERT INTO reuters_close (val, business_date, ticker)
    VALUES (?, ?, ?)
    """,
        data,
    )

    # Commit and close connection
    conn.commit()
    conn.close()

    de = DataEnvironment()
    de.add_entry(DataEnvironmentEntry(source_path="close_prices", environment_path=f"sqlite:///{dir}/prices.db"))

    DataEnvironment.set_current(de)

    class HistoricalPrice(CompoundScalar):
        val: float
        timestamp: date

    class TickerScope(Scope):
        def in_scope(self, value: object) -> bool:
            return isinstance(value, str)

        def adjust(self, value: object) -> object:
            return value

    class DateTimeScope(Scope):
        def in_scope(self, value: object) -> bool:
            return isinstance(value, datetime)

    DataCatalogueEntry[DataSource](
        HistoricalPrice,
        "exchange",
        scope=frozendict({
            "ticker": TickerScope(), 
            "start": DateTimeScope(), 
            "end": DateTimeScope(),
            "retry": RetryScope(ExponentialBackoffRetryOptions(delay=timedelta(seconds=1), initial_delay=True, randomise=False)),
        }),
        store=SqlDataSource(
            source_path="close_prices",
            query=(
                "SELECT val, business_date as timestamp FROM reuters_close "
                "WHERE business_date > '{start:%Y-%m-%d}' AND business_date < '{end:%Y-%m-%d}' "
                "AND ticker = '{ticker}'"
                "AND ("
                "    SELECT CASE WHEN c2.timestamp > datetime('now', '-10 seconds') THEN 1/0 ELSE 1 END "
                "    FROM counter c2 "
                "    WHERE c2.id = 1 "
                ")"
            ),
        ),
    )

    @graph
    def g():
        register_adaptor("close_prices", sql_read_adaptor_impl)
        register_adaptor(None, sql_read_adaptor_raw_impl)
        register_adaptor(None, subscribe_adaptor_impl)

        res = subscribe[HistoricalPrice](
            "exchange", ticker=".FTSE", start=datetime(2024, 1, 1), end=datetime(2024, 1, 31)  # dataset
        )

        res1 = subscribe[HistoricalPrice](
            "exchange", ticker=".FTSE", start=datetime(2024, 1, 1), end=datetime(2024, 1, 31)  # dataset
        )

        debug_print("result", res)
        debug_print("result1", res1)
        stop_engine(if_true(valid(res) & (res.status == StreamStatus.OK)))

    run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=100))
