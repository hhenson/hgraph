"""
Data catalogue describes source of data in terms of the schema and its mapping to the data source.
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import InitVar, dataclass
from typing import Type, TypeVar, Generic
from sys import version_info

from frozendict import frozendict

from hgraph.adaptors.data_catalogue.data_scopes import Scope

if version_info >= (3, 10):
    try:
        from hgraph import CompoundScalar
    except ImportError:
        @dataclass(frozen=True)
        class CompoundScalar:
            pass
else:
    @dataclass(frozen=True)
    class CompoundScalar:
        pass


__all__ = ['DataSource', 'DataSink', 'DATA_STORE', 'DataCatalogueEntry', 'DataCatalogue', 'DataEnvironmentEntry', 'DataEnvironment']


# Datasource is just a struct that contains the 'about' of the datasource, not the 'how'.
# It is not intended to encapsulate access methods along with data about the source.
# Instead it *is* intended that the type of the datasource will allow downstream dispatch
# to apply appropriate access methods.
@dataclass(frozen=True)
class DataSource(CompoundScalar):
    """
    Represents a data source, such as a database, a file, queue, service etc.
    """
    source_path: str


@dataclass(frozen=True)
class DataSink(CompoundScalar):
    """
    Represents a data sink, such as a database, a file, queue, service etc.
    """
    sink_path: str

DATA_STORE = TypeVar("DATA_STORE", DataSource, DataSink)


@dataclass(frozen=True)
class DataCatalogueEntry(CompoundScalar, Generic[DATA_STORE]):
    schema: Type[CompoundScalar]  # Price, Volumes, etc.
    dataset: str  # 'Exchange', 'PointConnect', etc.
    scope: frozendict[str, Scope]  # shards?
    store: DATA_STORE # DataSource or DataSink

    auto_register: InitVar[bool] = True

    def __post_init__(self, auto_register: bool):
        if auto_register:
            DataCatalogue.instance().add_entry(self)


class DataCatalogue:
    _instance: "DataCatalogue" = None

    def __init__(self):
        self.catalogue: dict[tuple[type, str], set[DataCatalogueEntry]] = defaultdict(set)
        self.dataset_map: dict[tuple[str, type], DataCatalogueEntry] = {}

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def __enter__(self):
        self._prev = DataCatalogue._instance
        DataCatalogue._instance = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        DataCatalogue._instance = self._prev

    def add_entry(self, entry: DataCatalogueEntry):
        dataset_map_key = (entry.dataset, entry.store.__class__)
        if dataset_map_key in self.dataset_map:
            raise ValueError(f"Duplicate data catalogue entries: {entry} has the same dataset and store class as "
                             f"{self.dataset_map[dataset_map_key]}")
        self.dataset_map[dataset_map_key] = entry

        self.catalogue[(entry.schema, entry.dataset)].add(entry)

    def get_entries(self,schema: type, dataset: str, store: type[DATA_STORE]) -> set[DataCatalogueEntry]:
        entries = self.catalogue.get((schema, dataset))
        if entries is None:
            raise ValueError(f"No catalogue entries found for {(schema, dataset)}")
        return {entry for entry in entries if isinstance(entry.store, store)}

    def get_entries_for_store_type(self, schema: type, store_type: type[DATA_STORE]) -> set[DataCatalogueEntry]:
        return {
            entry
            for entries in self.catalogue.values()
            for entry in entries
            if isinstance(entry.store, store_type) and entry.schema == schema
        }

    def get_registered_schemas(self) -> set[type]:
        return {schema for schema, _ in self.catalogue.keys()}


@dataclass
class DataEnvironmentEntry:
    source_path: str
    environment_path: str


class DataEnvironment:
    """
    Maps source paths from the data catalogue to the current environment's paths. For example data catalogue's entry
    will have 'my_database' as the source path and the current DataEnvironment will map it to the appropriate
    connection string, like 'mysql://localhost/dev_database?options=...'
    """

    _current: "DataEnvironment" = None  # the current data environment

    def __init__(self):
        self.environment: dict[str, DataEnvironmentEntry] = {}

    @classmethod
    def current(cls):
        return cls._current

    # Set the current environment to an instance of the class
    @classmethod
    def set_current(cls, environment: "DataEnvironment"):
        if cls._current is not None:
            raise ValueError("Current environment already set")

        cls._current = environment

    def add_entry(self, entry: DataEnvironmentEntry):
        self.environment[entry.source_path] = entry

    def get_entry(self, source_path: str) -> DataEnvironmentEntry:
        return self.environment[source_path]
