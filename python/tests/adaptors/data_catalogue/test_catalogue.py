from dataclasses import dataclass
from datetime import datetime, timedelta

from frozendict import frozendict

from hgraph import CompoundScalar, TS, compute_node, eval_node
from hgraph.adaptors.data_catalogue import (
    DataCatalogue,
    DataCatalogueEntry,
    DataEnvironment,
    DataEnvironmentEntry,
    DataSource,
    FixedDelayRetryOptions,
    IntegerScope,
)


@dataclass(frozen=True)
class _Row(CompoundScalar):
    value: int


@dataclass(frozen=True)
class _FileSource(DataSource):
    file_name: str


def test_catalogue_entry_selection_and_environment_mapping():
    with DataCatalogue() as catalogue:
        entry = DataCatalogueEntry[_FileSource](
            schema=_Row,
            dataset="prices",
            scope=frozendict({"rank": IntegerScope(default=1)}),
            store=_FileSource(source_path="primary", file_name="prices.arrow"),
        )
        assert catalogue.get_entries(_Row, "prices", DataSource) == {entry}
        assert catalogue.get_entries_for_store_type(_Row, _FileSource) == {entry}

    with DataEnvironment() as environment:
        environment.add_entry(DataEnvironmentEntry("primary", "/data/prices"))
        assert DataEnvironment.current() is environment
        assert environment.get_entry("primary").environment_path == "/data/prices"


def test_catalogue_entry_crosses_the_runtime_value_boundary():
    entry_type = DataCatalogueEntry[_FileSource]
    entry = entry_type(
        schema=_Row,
        dataset="prices",
        scope=frozendict(),
        store=_FileSource(source_path="primary", file_name="prices.arrow"),
        auto_register=False,
    )

    @compute_node
    def dataset(value: TS[entry_type]) -> TS[str]:
        return value.value.dataset

    assert eval_node(dataset, [entry]) == ["prices"]


def test_fixed_retry_scope_tracks_each_created_state_independently():
    options = FixedDelayRetryOptions(timedelta(seconds=2), max_retries=2)
    first = options.create()
    second = options.create()
    start = datetime(2026, 1, 1)

    assert first.next(start) == start + timedelta(seconds=2)
    assert first.next(start) == start + timedelta(seconds=2)
    assert first.next(start) is None
    assert second.next(start) == start + timedelta(seconds=2)
