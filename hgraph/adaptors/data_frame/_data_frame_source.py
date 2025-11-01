from abc import abstractmethod, ABC
from datetime import datetime
from functools import cached_property
from typing import Iterator, TypeVar, Optional, OrderedDict, Any

import polars as pl

__all__ = (
    "DataFrameSource",
    "DataStore",
    "DATA_FRAME_SOURCE",
    "DataConnectionStore",
    "SqlDataFrameSource",
    "PolarsDataFrameSource",
)


class DataFrameSource(ABC):
    """
    Provide an abstraction over retrieving a data-source.
    This provides the ability to test the retrieval of data independent of
    the graph. This can then be provided to the ``data_frame_source`` generator
    to feed into the graph.
    """

    @abstractmethod
    def data_frame(self, start_time: datetime = None, end_time: datetime = None) -> pl.DataFrame:
        """
        Returns a data-frame representing this data source.
        The start_time and end_time are the engine start and end times.
        """

    @property
    def schema(self) -> OrderedDict[str, pl.DataType]:
        """
        The schema describing this data source. By default, the code will get then data_frame and then
        extract the schema from that. If the data-source is large it is possible to provide the value
        directly. (Override the property)
        """
        df = self.data_frame()
        return df.schema

    def iter_frames(self, start_time: datetime = None, end_time: datetime = None) -> Iterator[pl.DataFrame]:
        """
        Return the data source as a sequence of dataframes.
        By default, this is just an iterator over the data_frame provided by this data source.
        When possible, this is useful when the data source can return results in batches.
        This may produce better memory consumption and possibly improve the performance of the
        data source when back-testing.
        The start_time and end_time are the engine start and end times.
        """
        return iter([self.data_frame(start_time, end_time)])


DATA_FRAME_SOURCE = TypeVar("DATA_FRAME_SOURCE", bound=DataFrameSource)


class DataStore:
    """A cache of DataFrameSource instances"""

    _instance: Optional["DataStore"] = None

    def __init__(self):
        self._data_frame_sources: dict[str, DATA_FRAME_SOURCE] = {}

    def register_instance(self):
        if DataStore._instance is None:
            DataStore._instance = self
        else:
            raise RuntimeError("Datastore already registered")

    @staticmethod
    def release_instance():
        DataStore._instance = None

    @staticmethod
    def instance() -> "DataStore":
        if DataStore._instance is None:
            DataStore().register_instance()
        return DataStore._instance

    def set_data_source(self, dfs: type[DATA_FRAME_SOURCE], dfs_instance: DATA_FRAME_SOURCE):
        """
        Allow for pre-setting the data source. Useful when the data source requires initialisation.
        """
        self._data_frame_sources[dfs] = dfs_instance

    def get_data_source(self, dfs: type[DATA_FRAME_SOURCE]) -> DATA_FRAME_SOURCE:
        """
        Returns an instance of the DataFrameSource, if one exists, otherwise it will instantiate the
        data source, cache it and then return it.
        """
        dfs_instance = self._data_frame_sources.get(dfs)
        if dfs_instance is None:
            dfs_instance = dfs()
            self._data_frame_sources[dfs] = dfs_instance
        return dfs_instance

    def __enter__(self):
        self.register_instance()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_instance()


class PolarsDataFrameSource(DataFrameSource):
    """
    A simple data frame source
    """

    def __init__(self, df: pl.DataFrame):
        self._df: pl.DataFrame = df

    def data_frame(self, start_time: datetime = None, end_time: datetime = None) -> pl.DataFrame:
        return self._df


class DataConnectionStore:
    _instance: Optional["DataConnectionStore"] = None

    def __init__(self):
        self._connections: dict[str, Any] = {}

    def register_instance(self):
        if DataConnectionStore._instance is None:
            DataConnectionStore._instance = self
        else:
            raise RuntimeError("DataConnectionStore already registered")

    @staticmethod
    def release_instance():
        DataConnectionStore._instance = None

    @staticmethod
    def instance() -> "DataConnectionStore":
        if DataConnectionStore._instance is None:
            DataConnectionStore().register_instance()
        return DataConnectionStore._instance

    def get_connection(self, name: str) -> Any:
        connection = self._connections.get(name)
        if connection is None:
            raise ValueError(f"No connection found with name '{name}'")
        return connection

    def set_connection(self, name: str, connection):
        self._connections[name] = connection

    def __enter__(self):
        self.register_instance()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_instance()


class SqlDataFrameSource(DataFrameSource):
    """
    See https://docs.pola.rs/py-polars/html/reference/api/polars.read_database.html for more info.
    This uses the query connection and batch_size properties. Any execute_options can be provided as kwargs.
    """

    def __init__(self, query: str, connection: str, batch_size: int = -1, **kwargs):
        self._query: str = query
        self._kwargs: dict = kwargs
        self._connection: str = connection
        self._batch_size: int = batch_size
        self._df: DATA_FRAME_SOURCE | None = None
        self._iter: Iterator[DATA_FRAME_SOURCE] | None = None

    @property
    def connection(self):
        return DataConnectionStore.instance().get_connection(self._connection)

    def data_frame(self, start_time: datetime = None, end_time: datetime = None) -> pl.DataFrame:
        if self._df is None:
            self._df = pl.read_database(self._query, self.connection, **self._kwargs)
        return self._df

    def iter_frames(self, start_time: datetime = None, end_time: datetime = None) -> Iterator[pl.DataFrame]:
        if self._df is None:
            if self._batch_size == -1:
                return iter([self.data_frame(start_time, end_time)])
            else:
                df = pl.read_database(
                    self._query,
                    self.connection,
                    iter_batches=True,
                    batch_size=self._batch_size,
                    execute_options=self._kwargs,
                )
                if isinstance(df, pl.DataFrame):
                    return iter([self.data_frame(start_time, end_time)])
                else:
                    return df
        else:
            # Since we already have the data loaded, just use the loaded data-frame
            return iter([self.data_frame(start_time, end_time)])

    @cached_property
    def schema(self) -> OrderedDict[str, pl.DataType]:
        df = pl.read_database(self._query + " LIMIT 1", self.connection)
        return df.schema
