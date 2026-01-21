import os
from datetime import timedelta
import re
import sys
from urllib.parse import quote

import adbc_driver_snowflake.dbapi
import polars as pl
from polars import DataFrame
from sqlalchemy import create_engine, QueuePool, event

from hgraph import generator, STATE, TS


__all__ = ['SqlAdaptorConnection', 'SqlAdaptorConnectionSQLServer', 'SqlAdaptorConnectionSnowflake', 'start_sql_adaptor', 'get_secret']

class SqlAdaptorConnection:
    ...


class SqlAdaptorConnectionSQLServer(SqlAdaptorConnection):

    def __init__(self, path, connection_params: dict[str, object]):
        self.path = path
        self.connection = create_engine(path, **connection_params)
        
        if os.name != "nt" and "mssql" in path:
            @event.listens_for(self.connection, "reset")
            def _rollback_mssql(connection, connection_record, reset_state):
                if not reset_state.terminate_only:
                    connection.execute("{call sys.sp_reset_connection}")

                # so that the DBAPI itself knows that the connection has been reset
                connection.rollback()

    def read_database(self, query: str) -> DataFrame:
        return pl.read_database_uri(query=query, uri=self.path)


class SqlAdaptorConnectionSnowflake(SqlAdaptorConnection):

    def __init__(self, connection_params: dict[str, object]):
        self.lowercase_columns = connection_params.pop("hgraph.sql_adaptor.lowercase_columns", "false").lower() == "true"
        self.connection = adbc_driver_snowflake.dbapi.connect(db_kwargs=connection_params)

    def read_database(self, query: str) -> DataFrame:
        df = pl.read_database(connection=self.connection, query=query)
        if self.lowercase_columns:
            df = df.rename({col: col.lower() for col in df.columns})
        return df


get_secret = None

def process_substitution(v: str) -> str:
    if v.startswith("secret:"):
        split = v[7:].split("/", 1)
        
        if get_secret is not None:
            sec = get_secret(split[0])
            if split and len(split) == 2:
                sec = sec.get(split[1])
        
            if sec is None:
                raise ValueError(f"Secret {v[7:]} not set for connection parameter")
            return sec
        else:
            raise ValueError(f"get_secret is not defined, cannot get '{v[7:]}' not set for connection parameter")
    
    if v.startswith("$"):
        value = sys.env.get(v[1:])
        if value is None:
            raise ValueError(f"Environment variable {v[1:]} not set for connection parameter")
        return value

    return v


def parse_connection_params(path: str) -> dict[str, object]:
    from urllib.parse import urlparse, parse_qs, quote
    path = re.sub(r'(?<!\{)\{([^{}]*)\}(?!\})', lambda x: quote(process_substitution(x.group(1)), safe=''), path)
    url = urlparse(path)
    query_params = parse_qs(url.query)
    url_base = url._replace(query="").geturl()
    if url.netloc == '' and url.scheme != '':
        url_base = url_base.replace(':/', ':///')
    return url.scheme, url_base, {k: v[0] for k, v in query_params.items()}


@generator
def start_sql_adaptor(
    path: str,
    _state: STATE = None
) -> TS[SqlAdaptorConnection]:
    
    scheme, path, connection_params = parse_connection_params(path)
    
    match scheme:
        case "snowflake":
            connection = create_snowflake_connection(path, connection_params=dict(connection_params))
        case "mssql" | "sqlite":
            connection = create_sql_db_connection(path, connection_params=dict(connection_params), _state=_state)

    yield timedelta(), connection


@start_sql_adaptor.stop
def stop_sql_server_adaptor(_state: STATE):
    if (connection := getattr(_state, "connection", None)) is not None:
        connection.connection.dispose()
    _state.executor.shutdown()


def create_sql_db_connection(
    path: str,
    connection_params: dict[str, object],
    _state: STATE
) -> TS[SqlAdaptorConnection]:

    default_params = {
        "poolclass": QueuePool,
        "pool_size": 5,
        "max_overflow": 50,
        "pool_timeout": 600,
        "pool_recycle": 90,
        "execution_options": {"isolation_level": "AUTOCOMMIT"},
        "echo": True,
    }
    default_params.update({k: v for k,v in connection_params.items() if k in default_params})

    uri_params = {k: v for k, v in connection_params.items() if k not in default_params}
    kw_params = {k: v for k, v in connection_params.items() if k not in uri_params}
    path += "?" + "&".join(f"{k}={quote(str(v))}" for k, v in uri_params.items())

    if "mssql" in path:
        # https://github.com/sqlalchemy/sqlalchemy/issues/8177 (also causes invalid precision errors)
        kw_params["use_setinputsizes"] = False

    _state.connection = SqlAdaptorConnectionSQLServer(path, kw_params)

    return _state.connection


def create_snowflake_connection(path: str, connection_params: dict[str, object]) -> TS[SqlAdaptorConnection]:
    if not connection_params:
        raise ValueError("Snowflake adaptor requires connection parameters")

    from urllib.parse import urlparse, unquote
    url = urlparse(path)
    
    db_schema = url.path.lstrip("/").split("/") if url.path else None
    
    connection_details = {
        "adbc.snowflake.sql.account": url.hostname,
        "username": unquote(url.username),
        "password": unquote(url.password),
        "database": db_schema[0] if db_schema else None,
        "schema": db_schema[1] if len(db_schema) > 1 else None,
        **connection_params
    }

    connection_details = {k: v for k, v in connection_details.items() if v is not None}

    return SqlAdaptorConnectionSnowflake(connection_details)
