from collections import defaultdict
from dataclasses import dataclass
from typing import Generic, TypeVar

from frozendict import frozendict

from hgraph import CompoundScalar, GlobalState

from .data_scopes import Scope
from hgraph._wiring._state import (_active_global_state, _global_state_scope,
                                   _published_in_global_state)

__all__ = (
    "DataSource",
    "DataSink",
    "DATA_STORE",
    "DataCatalogueEntry",
    "DataCatalogue",
    "DataEnvironmentEntry",
    "DataEnvironment",
)


@dataclass(frozen=True)
class DataSource(CompoundScalar, abstract=True):
    source_path: str


@dataclass(frozen=True)
class DataSink(CompoundScalar, abstract=True):
    sink_path: str


DATA_STORE = TypeVar("DATA_STORE", DataSource, DataSink)


@dataclass(frozen=True)
class DataCatalogueEntry(CompoundScalar, Generic[DATA_STORE]):
    schema: object
    dataset: str
    scope: frozendict[str, Scope]
    store: DATA_STORE


class DataCatalogue(_published_in_global_state):
    _STATE_KEY = ":adaptors:data_catalogue:catalogue"
    _MISSING = object()

    def __init__(self):
        self.catalogue = defaultdict(set)
        self.dataset_map = {}
        self._previous = self._MISSING

    @classmethod
    def instance(cls, global_state=None):
        state = global_state if global_state is not None else _active_global_state()
        catalogue = state.get(cls._STATE_KEY)
        if catalogue is None:
            catalogue = cls()
            state[cls._STATE_KEY] = catalogue
        return catalogue

    def add_entry(self, entry: DataCatalogueEntry):
        dataset_key = (entry.dataset, type(entry.store))
        previous = self.dataset_map.get(dataset_key)
        if previous is not None:
            raise ValueError(
                f"duplicate data catalogue entries: {entry!r} and {previous!r} "
                "have the same dataset and store class"
            )
        self.dataset_map[dataset_key] = entry
        self.catalogue[(entry.schema, entry.dataset)].add(entry)

    def get_entries(self, schema, dataset: str, store: type[DATA_STORE]):
        entries = self.catalogue.get((schema, dataset))
        if entries is None:
            raise ValueError(f"no catalogue entries found for {(schema, dataset)!r}")
        return {entry for entry in entries if isinstance(entry.store, store)}

    def get_entries_for_store_type(self, schema, store_type: type[DATA_STORE]):
        return {
            entry
            for entries in self.catalogue.values()
            for entry in entries
            if entry.schema is schema and isinstance(entry.store, store_type)
        }

    def get_registered_schemas(self):
        return {schema for schema, _ in self.catalogue}

    def matching_entries(self, schema, dataset, store_type, options):
        entries = self.get_entries(schema, dataset, store_type)
        scope_checks = {}
        matches = []
        for entry in entries:
            if not all(name in entry.scope for name in options):
                continue
            resolved = {
                name: options.get(name, scope.default())
                for name, scope in entry.scope.items()
            }
            checks = {
                name: scope.in_scope(resolved[name])
                for name, scope in entry.scope.items()
            }
            if all(checks.values()):
                matches.append(
                    (
                        entry,
                        frozendict(
                            {
                                name: scope.adjust(resolved[name])
                                for name, scope in entry.scope.items()
                            }
                        ),
                    )
                )
            else:
                scope_checks[entry.store.source_path if isinstance(entry.store, DataSource) else entry.store.sink_path] = checks
        if matches:
            return matches
        raise ValueError(
            f"no data catalogue entry found for {schema!r}, dataset {dataset!r}, "
            f"options {dict(options)!r}; scope checks: {scope_checks!r}"
        )


@dataclass(frozen=True)
class DataEnvironmentEntry:
    source_path: str
    environment_path: str


class DataEnvironment(_published_in_global_state):
    _STATE_KEY = ":adaptors:data_catalogue:environment"
    _MISSING = object()

    def __init__(self):
        self.environment = {}
        self._previous = self._MISSING

    @classmethod
    def current(cls):
        return _active_global_state().get(cls._STATE_KEY)

    @classmethod
    def set_current(cls, environment):
        state = _active_global_state()
        if cls._STATE_KEY in state:
            raise ValueError("current data environment is already set")
        state[cls._STATE_KEY] = environment

    @classmethod
    def clear_current(cls):
        _active_global_state().pop(cls._STATE_KEY, None)

    def add_entry(self, entry: DataEnvironmentEntry):
        self.environment[entry.source_path] = entry

    def has_entry(self, source_path: str):
        return source_path in self.environment

    def get_entry(self, source_path: str):
        try:
            return self.environment[source_path]
        except KeyError as error:
            raise KeyError(f"no data environment entry for {source_path!r}") from error
