import os
import re
from datetime import timedelta
from urllib.parse import quote

import pyarrow as pa

from hgraph import STATE, TS, generator
from hgraph.adaptors.data_catalogue import DataEnvironment

__all__ = (
    "SqlAdaptorConnection",
    "SqlAdaptorConnectionSQLServer",
    "SqlAdaptorConnectionSnowflake",
    "start_sql_adaptor",
    "get_secret",
)


class SqlAdaptorConnection:
    ...


def _require_polars():
    try:
        import polars
    except ModuleNotFoundError as error:
        raise RuntimeError("SQL adaptors require the 'sql' extra") from error
    return polars


def _require_sqlalchemy():
    try:
        from sqlalchemy import QueuePool, create_engine, event
    except ModuleNotFoundError as error:
        raise RuntimeError("SQL connections require the 'sql' extra") from error
    return QueuePool, create_engine, event


def _require_snowflake():
    try:
        import adbc_driver_snowflake.dbapi
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Snowflake connections require the 'snowflake' extra"
        ) from error
    return adbc_driver_snowflake.dbapi


class SqlAdaptorConnectionSQLServer(SqlAdaptorConnection):
    def __init__(self, path, connection_params: dict[str, object]):
        _, create_engine, event = _require_sqlalchemy()
        self.path = path
        self.connection = create_engine(path, **connection_params)

        if os.name != "nt" and "mssql" in path:
            @event.listens_for(self.connection, "reset")
            def _rollback_mssql(connection, connection_record, reset_state):
                if not reset_state.terminate_only:
                    connection.execute("{call sys.sp_reset_connection}")
                connection.rollback()

    def read_database(self, query: str) -> pa.Table:
        pl = _require_polars()
        return pl.read_database_uri(query=query, uri=self.path).to_arrow()


class SqlAdaptorConnectionSnowflake(SqlAdaptorConnection):
    def __init__(self, connection_params: dict[str, object]):
        snowflake = _require_snowflake()
        self.lowercase_columns = (
            connection_params.pop(
                "hgraph.sql_adaptor.lowercase_columns", "false"
            ).lower() == "true"
        )
        self.connection = snowflake.connect(db_kwargs=connection_params)

    def read_database(self, query: str) -> pa.Table:
        pl = _require_polars()
        frame = pl.read_database(connection=self.connection, query=query)
        if self.lowercase_columns:
            frame = frame.rename({column: column.lower() for column in frame.columns})
        return frame.to_arrow()


get_secret = None


def process_substitution(value: str, scope: str = "connection") -> str:
    if value.startswith("secret:"):
        split = value[7:].split("/", 1)
        if get_secret is None:
            raise ValueError(
                f"get_secret is not defined, cannot get '{value[7:]}' "
                f"for {scope} parameter")
        secret = get_secret(split[0])
        if len(split) == 2:
            secret = secret.get(split[1])
        if secret is None:
            raise ValueError(
                f"Secret {value[7:]} not set for {scope} parameter")
        return secret

    if value.startswith("$"):
        result = os.environ.get(value[1:])
        if result is None:
            raise ValueError(
                f"Environment variable {value[1:]} not set for {scope} parameter")
        return result

    if value.startswith("dataenv:"):
        environment = DataEnvironment.current()
        source_path = value[8:]
        if environment is not None and environment.has_entry(source_path):
            result = environment.get_entry(source_path).environment_path
            if result is not None:
                return result
        raise ValueError(
            f"Data environment variable {source_path} not set for {scope} parameter")

    return f"{{{value}}}"


def parse_connection_params(path: str):
    from urllib.parse import parse_qs, urlparse

    path = re.sub(
        r"(?<!\{)\{([^{}]*)\}(?!\})",
        lambda match: quote(process_substitution(match.group(1)), safe=""),
        path,
    )
    url = urlparse(path)
    query_params = parse_qs(url.query)
    url_base = url._replace(query="").geturl()
    if url.netloc == "" and url.scheme != "":
        url_base = url_base.replace(":/", ":///")
    return url.scheme, url_base, {
        key: value[0] for key, value in query_params.items()
    }


@generator
def start_sql_adaptor(path: str, _state: STATE = None) -> TS[SqlAdaptorConnection]:
    scheme, path, connection_params = parse_connection_params(path)

    match scheme:
        case "snowflake":
            connection = create_snowflake_connection(
                path, connection_params=dict(connection_params))
        case "mssql" | "sqlite" | "postgresql":
            connection = create_sql_db_connection(
                path, connection_params=dict(connection_params), _state=_state)

    yield timedelta(), connection


@start_sql_adaptor.stop
def stop_sql_server_adaptor(_state: STATE = None):
    if (connection := getattr(_state, "connection", None)) is not None:
        connection.connection.dispose()


def create_sql_db_connection(
    path: str, connection_params: dict[str, object], _state: STATE,
) -> SqlAdaptorConnection:
    QueuePool, _, _ = _require_sqlalchemy()
    default_params = {
        "poolclass": QueuePool,
        "pool_size": 5,
        "max_overflow": 50,
        "pool_timeout": 600,
        "pool_recycle": 90,
        "execution_options": {"isolation_level": "AUTOCOMMIT"},
        "echo": True,
    }
    default_params.update({
        key: value for key, value in connection_params.items()
        if key in default_params
    })
    uri_params = {
        key: value for key, value in connection_params.items()
        if key not in default_params
    }
    keyword_params = {
        key: value for key, value in connection_params.items()
        if key not in uri_params
    }
    path += "?" + "&".join(
        f"{key}={quote(str(value))}" for key, value in uri_params.items())

    if "mssql" in path:
        keyword_params["use_setinputsizes"] = False

    _state.connection = SqlAdaptorConnectionSQLServer(path, keyword_params)
    return _state.connection


def create_snowflake_connection(
    path: str, connection_params: dict[str, object],
) -> SqlAdaptorConnection:
    if not connection_params:
        raise ValueError("Snowflake adaptor requires connection parameters")

    from urllib.parse import unquote, urlparse

    url = urlparse(path)
    db_schema = url.path.lstrip("/").split("/") if url.path else None
    connection_details = {
        "adbc.snowflake.sql.account": url.hostname,
        "username": unquote(url.username),
        "password": unquote(url.password),
        "database": db_schema[0] if db_schema else None,
        "schema": db_schema[1] if db_schema and len(db_schema) > 1 else None,
        **connection_params,
    }
    return SqlAdaptorConnectionSnowflake({
        key: value for key, value in connection_details.items()
        if value is not None
    })
