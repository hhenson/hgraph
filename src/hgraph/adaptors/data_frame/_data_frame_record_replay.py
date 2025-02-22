from abc import abstractmethod, ABC
from datetime import datetime
from enum import Enum, auto
from pathlib import Path

import polars as pl

from hgraph import (
    graph,
    record,
    TIME_SERIES_TYPE,
    to_table,
    table_schema,
    AUTO_RESOLVE,
    TableSchema,
    sink_node,
    TS,
    STATE,
    GlobalState,
    generator,
    OUT,
    get_table_schema_date_key,
    from_table,
    EvaluationEngineApi,
    get_as_of,
    LOGGER,
    const_fn,
    get_fq_recordable_id,
    Frame,
    DEFAULT,
    MAX_DT,
)
from hgraph._operators._record_replay import record_replay_model_restriction, replay, replay_const
from hgraph._operators._to_table import get_table_schema_as_of_key, from_table_const
from hgraph._runtime._traits import Traits

__all__ = (
    "DATA_FRAME_RECORD_REPLAY",
    "set_data_frame_record_path",
    "MemoryDataFrameStorage",
    "DataFrameStorage",
    "FileBasedDataFrameStorage",
    "BaseDataFrameStorage",
    "replay_data_frame",
)

DATA_FRAME_RECORD_REPLAY = ":data_frame:__data_frame_record_replay__"
DATA_FRAME_RECORD_REPLAY_PATH = ":data_frame:__path__"


def set_data_frame_record_path(path: Path):
    """Sets the location where the files are to be written"""
    GlobalState.instance()[DATA_FRAME_RECORD_REPLAY_PATH] = path


@graph(overloads=record, requires=record_replay_model_restriction(DATA_FRAME_RECORD_REPLAY))
def record_to_data_frame(
    ts: TIME_SERIES_TYPE,
    key: str,
    recordable_id: str = None,
    tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
):
    """
    converts the ts to a table and then records the table to memory, writing to a parquet file in the end.
    delta_value are ignored when working in the tabular space for now.
    """
    tbl = to_table(ts)
    schema = table_schema(tp)
    _record_to_data_frame(tbl, schema, key, recordable_id)


@sink_node
def _record_to_data_frame(
    ts: TIME_SERIES_TYPE,
    schema: TS[TableSchema],
    key: str,
    recordable_id: str = None,
    _state: STATE = None,
    _traits: Traits = None,
    _logger: LOGGER = None,
):
    _state.value.append(ts.value)


@_record_to_data_frame.start
def _record_to_data_frame_start(key: str, recordable_id: str, _state: STATE, _traits: Traits = None):
    _state.value = []
    recordable_id = get_fq_recordable_id(_traits, recordable_id)
    _state.recordable_id = f"{recordable_id}::{key}"


@_record_to_data_frame.stop
def _record_to_data_frame_stop(_state: STATE, schema: TS[TableSchema], _logger: LOGGER):
    schema: TableSchema = schema.value
    if schema.partition_keys:
        rows = list(i for row in _state.value for i in row)
    else:
        rows = _state.value
    df = pl.from_records(rows, schema=[(k, t) for k, t in zip(schema.keys, schema.types)], orient="row")
    DataFrameStorage.instance().write_frame(_state.recordable_id, df)


@graph(overloads=replay, requires=record_replay_model_restriction(DATA_FRAME_RECORD_REPLAY))
def replay_from_data_frame(key: str, tp: type[OUT] = AUTO_RESOLVE, recordable_id: str = None) -> OUT:
    values = _replay_from_data_frame(key, tp, recordable_id)
    return from_table[tp](values)


@graph
def replay_data_frame(
    data_frame: Frame, schema: TableSchema = None, as_of_time: datetime = None, tp: type[OUT] = AUTO_RESOLVE
) -> DEFAULT[OUT]:
    """
    Support replay of a raw polars data frame. Supports supplying a custom schema, if the schema is not supplied it
    will be extracted from the output type. If an as_of_time is supplied then use it, otherwise the as_of time will
    be set to either the globally set as_of time or the current time.
    """
    if schema is None:
        schema = table_schema(tp).value
    if as_of_time is None:
        as_of_time = MAX_DT
    return from_table[tp](_replay_from_data_frame_raw(data_frame, tp, schema, as_of_time))


def _get_df(key, recordable_id: str, traits: Traits) -> pl.DataFrame:
    recordable_id = get_fq_recordable_id(traits, recordable_id) if traits is not None else recordable_id
    return DataFrameStorage.instance().read_frame(f"{recordable_id}::{key}")


def _replay_from_data_frame_output_shape(m, s):
    tp = m[TIME_SERIES_TYPE].py_type
    schema: TableSchema = table_schema(tp).value
    if schema.partition_keys:
        return tuple[tuple[*schema.types], ...]
    else:
        return tuple[*schema.types]


@generator(resolvers={OUT: _replay_from_data_frame_output_shape})
def _replay_from_data_frame(
    key: str,
    tp: type[TIME_SERIES_TYPE],
    recordable_id: str = None,
    _traits: Traits = None,
    _api: EvaluationEngineApi = None,
) -> TS[OUT]:
    schema: TableSchema = table_schema(tp).value
    df_source = _get_df(key, recordable_id, _traits)
    start_time = _api.start_time
    as_of_time = get_as_of(_api.evaluation_clock)
    return _do_replay_from_data_frame(schema, df_source, start_time, as_of_time)


@generator(resolvers={OUT: _replay_from_data_frame_output_shape})
def _replay_from_data_frame_raw(
    data_frame: Frame,
    tp: type[TIME_SERIES_TYPE],
    schema: TableSchema,
    as_of_time: datetime,
    _traits: Traits = None,
    _api: EvaluationEngineApi = None,
) -> TS[OUT]:
    if as_of_time is MAX_DT:
        as_of_time = get_as_of(_api.evaluation_clock)
    start_time = _api.start_time
    return _do_replay_from_data_frame(schema, data_frame, start_time, as_of_time)


def _do_replay_from_data_frame(
    schema: TableSchema,
    df_source: pl.DataFrame,
    start_time: datetime,
    as_of_time: datetime,
):
    dt_col_str = get_table_schema_date_key()
    dt_col = pl.col(dt_col_str)
    as_of_str = get_table_schema_as_of_key()
    as_of_col = pl.col(as_of_str)
    partition_keys = [dt_col] + [pl.col(k) for k in schema.partition_keys]
    df_source: pl.DataFrame = (
        df_source.lazy()
        .filter(dt_col >= start_time, as_of_col <= as_of_time)
        .sort(dt_col, as_of_col, descending=[False, True])  # Get the most recent version for current as_of
        .with_columns(pl.cum_count(as_of_str).over(partition_keys).alias("__n__"))
        .filter(pl.col("__n__") == 1)
        .drop("__n__")
        .collect()
        .group_by(dt_col, maintain_order=True)
    )
    dt: datetime
    for (dt,), df in df_source:
        out = tuple(df.iter_rows())
        if out:
            if schema.partition_keys:
                yield dt, out
            else:
                yield dt, out[0]  # If there are no partition keys we should only return a single value


@const_fn(overloads=replay_const, requires=record_replay_model_restriction(DATA_FRAME_RECORD_REPLAY))
def replay_const_from_data_frame(
    key: str,
    tp: type[OUT] = AUTO_RESOLVE,
    recordable_id: str = None,
    tm: datetime = None,
    as_of: datetime = None,
    _traits: Traits = None,
    _api: EvaluationEngineApi = None,
) -> OUT:
    schema: TableSchema = table_schema(tp).value
    df_source = _get_df(key, recordable_id, _traits)
    dt_col_str = get_table_schema_date_key()
    dt_col = pl.col(dt_col_str)
    as_of_str = get_table_schema_as_of_key()
    as_of_col = pl.col(as_of_str)
    partition_keys = [dt_col] + [pl.col(k) for k in schema.partition_keys]
    start_time = _api.start_time if tm is None else tm
    as_of_time = get_as_of(_api.evaluation_clock) if as_of is None else as_of
    df = (
        df_source.lazy()
        .filter(dt_col >= start_time, as_of_col <= as_of_time)
        .sort(dt_col, as_of_col, descending=[False, True])  # Get the most recent version for current as_of
        .with_columns(pl.cum_count(as_of_str).over(partition_keys).alias("__n__"))
        .filter(pl.col("__n__") == 1)
        .drop("__n__")
        .collect()
        .group_by(dt_col, maintain_order=True)
    )
    # TODO: For schema's that hold buffered data, need to replay more than one row of data now
    dt, df_ = next(iter(df))
    results = tuple(df_.iter_rows())
    return from_table_const[tp](results if schema.partition_keys else results[0]).value


class WriteMode(Enum):
    EXTEND = auto()  # Extend the existing data frame (if it exists) with the new data (only adds new data)
    OVERWRITE = auto()  # Replaces the existing data frame (if it exists) with the new one
    MERGE = auto()  # Replaces the values that overlap with the values from the new data frame.


class DataFrameStorage(ABC):
    """
    Describes an abstract representation of a frame-store. Underlying implementations may not honour all the
    characteristics defined here. Make sure the implementation will be sufficient for the expected use-case.
    """

    _INSTANCE: "DataFrameStorage" = None

    def __init__(self):
        self._previous_instance: "DataFrameStorage" = None

    @staticmethod
    def instance() -> "DataFrameStorage":
        return DataFrameStorage._INSTANCE

    def set_as_instance(self):
        if DataFrameStorage._INSTANCE is not None:
            self._previous_instance = DataFrameStorage._INSTANCE
        DataFrameStorage._INSTANCE = self

    def release_as_instance(self):
        DataFrameStorage._INSTANCE = self._previous_instance
        self._previous_instance = None

    def __enter__(self) -> "DataFrameStorage":
        self.set_as_instance()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_as_instance()

    @abstractmethod
    def read_frame(
        self, path: str, start_time: datetime = None, end_time: datetime = None, as_of: datetime = None
    ) -> pl.DataFrame:
        """
        Read the data frame from the source. Given most of our use-case is for temporal data-frames.
        The read frame method will support time-range style queries as an optimisation. If the parameters
        are not supplied, then the data frame is treated as a single piece of data.

        If the data is stored with as_of information, then the return will contain the data including all relevant
        rows as updated as_of. If the as_of time is specified, then the as_of column is removed and the data represents
        a correct view as of the point in time.
        """

    @abstractmethod
    def write_frame(
        self, path: str, df: pl.DataFrame, mode: WriteMode = WriteMode.OVERWRITE, as_of: datetime = None
    ) -> pl.DataFrame:
        """
        Write the data frame to the source. If the data schema includes as_of information, then if this schema has the
        as_of field, it will make use of it. Alternatively, if the as_of argument is supplied, this is used for
        updating the information.

        If there is no schema information defined, the write_frame may attempt to heuristically determine if there
        is a date or as_of column based on common patterns.
        """

    @abstractmethod
    def set_schema_info(self, path: str, date_time_col: str = None, as_of_col: str = None):
        """
        Sets the relevant schema information for the data to be stored / retrieved.
        If as_of_col is supplied but no date_time_col is provided, it is assumed that full copies of the data frame
        are to be stored for each os_of epoc. If the schema information is not stored it is assumed that the data
        frame is not time-based.
        """


class BaseDataFrameStorage(DataFrameStorage, ABC):
    """Provide a common set of utilities for memory and file-based data frame storage"""

    @abstractmethod
    def _write(self, path: Path, df: pl.DataFrame):
        """Perform the physical act of writing the resource"""

    @abstractmethod
    def _read(self, path: Path) -> pl.DataFrame:
        """Read the raw resource"""

    @abstractmethod
    def _get_schema_info(self, path: str) -> tuple[str, str]:
        """Get the underlying schema data"""

    def read_frame(
        self, path: str, start_time: datetime = None, end_time: datetime = None, as_of: datetime = None
    ) -> pl.DataFrame:
        date_time_col, as_of_col = self._get_schema_info(path)
        df = self._read(path).lazy()
        if start_time is not None or end_time is not None:
            date_time_col = "date" if date_time_col is None else date_time_col
            if start_time is not None:
                df = df.filter(pl.col(date_time_col) >= start_time)
            if end_time is not None:
                df = df.filter(pl.col(date_time_col) <= end_time)
        if as_of is not None:
            df = df.filter(pl.col(as_of) <= as_of)
        return df.collect()

    def write_frame(
        self, path: str, df: pl.DataFrame, mode: WriteMode = WriteMode.OVERWRITE, as_of: datetime = None
    ) -> pl.DataFrame:
        self.set_schema_info(path, date_time_col=get_table_schema_date_key(), as_of_col=get_table_schema_as_of_key())
        if mode != WriteMode.OVERWRITE:
            raise RuntimeError(f"Currently mode: {mode.name} is not supported")
        self._write(path, df)


class FileBasedDataFrameStorage(BaseDataFrameStorage):
    """A simple file-system-based data frame store, useful for integration testing"""

    def __init__(self, path: Path):
        """Set the path for the entry-point for saving frames and related data"""
        super().__init__()
        self._path = path
        path.mkdir(parents=True, exist_ok=True)

    def _read(self, path: Path) -> pl.DataFrame:
        path = self._path / f"{path}.parquet"
        return pl.read_parquet(path)

    def _write(self, path, df):
        path = self._path / f"{path}.parquet"
        df.write_parquet(path)

    def set_schema_info(self, path: str, date_time_col: str = None, as_of_col: str = None):
        schema = self._path / f"{path}.schema"
        schema.write_text(
            f"date_time_col: {date_time_col}\nas_of_col: {as_of_col}",
        )

    def _get_schema_info(self, path: str) -> tuple[str, str]:
        schema = self._path / f"{path}.schema"
        txt = schema.read_text()
        row_1, row_2 = txt.split("\n")
        date_time_col = row_1.replace("date_time_col: ", "")
        as_of_col = row_2.replace("as_of_col: ", "")
        return None if date_time_col == "None" else date_time_col, None if as_of_col == "None" else as_of_col


class MemoryDataFrameStorage(BaseDataFrameStorage):
    """
    Provide an in memory data frame storage for unit testing.
    """

    def __init__(self):
        super().__init__()
        self._frames = {}
        self._schema = {}

    def _write(self, path: Path, df: pl.DataFrame):
        self._frames[str(path)] = df

    def _read(self, path: Path) -> pl.DataFrame:
        return self._frames.get(str(path), None)

    def _get_schema_info(self, path: str) -> tuple[str, str]:
        return self._schema.get(str(path), (None, None))

    def set_schema_info(self, path: str, date_time_col: str = None, as_of_col: str = None):
        self._schema[str(path)] = date_time_col, as_of_col
