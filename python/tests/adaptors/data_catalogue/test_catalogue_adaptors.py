from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pytest
from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.data_catalogue import (
    DataCatalogue,
    DataCatalogueEntry,
    DataSink,
    DataSource,
    publish,
    publish_adaptor_impl,
    subscriber_impl_from_graph,
    subscriber_impl_to_graph,
    subscribe,
    subscribe_adaptor_impl,
)
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


@dataclass(frozen=True)
class _Sink(DataSink):
    table: str


@dataclass(frozen=True)
class _Source(DataSource):
    table: str


_ROUTED_FRAME = pa.table({"name": ["a"], "value": [1]})


@subscriber_impl_from_graph
def subscribe_test_from_graph(
    dce: DataCatalogueEntry, ds: hg.TS[_Source],
    options: hg.TS[dict[str, object]], request_id: hg.TS[int],
    _schema: type[hg.SCHEMA] = hg.AUTO_RESOLVE,
):
    hg.null_sink(request_id)


@subscriber_impl_to_graph
def subscribe_test_to_graph(
    dce: DataCatalogueEntry, ds: hg.TS[_Source],
    options: hg.TS[dict[str, object]], request_id: hg.TS[int],
    _schema: type[hg.SCHEMA] = hg.AUTO_RESOLVE,
) -> hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[hg.SCHEMA]]]]:
    return hg.combine[
        hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_schema]]]]
    ](
        status=StreamStatus.OK,
        status_msg="",
        values=hg.const(_ROUTED_FRAME, tp=hg.TS[hg.Frame[_schema]]),
        timestamp=hg.MIN_DT,
    )


def test_catalogue_subscribe_routes_matching_source():
    responses = []
    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]

    @hg.sink_node
    def capture(response: response_type):
        if response.status.modified:
            responses.append((response.status.value, response.status_msg.value,
                              response["values"].value))

    @hg.graph
    def app():
        hg.register_adaptor("data-catalogue", subscribe_adaptor_impl)
        capture(subscribe[_Row]("rows"))

    catalogue = DataCatalogue()
    with hg.GlobalContext(hg.GlobalState()):
        with catalogue:
            DataCatalogueEntry[_Source](
                _Row,
                "rows",
                frozendict(),
                _Source(source_path="memory", table="rows"),
            )
            hg.run_graph(app)

    assert responses[0][0] is StreamStatus.OK, responses[0][1]
    assert responses[0][2].equals(_ROUTED_FRAME)


def test_subscriber_handler_requires_concrete_source_annotation():
    from hgraph.adaptors.data_catalogue.subscribe import subscriber_impl_to_graph

    with pytest.raises(TypeError, match=r"ds must be TS\[DataSource subclass\]"):
        @subscriber_impl_to_graph
        def untyped_source(dce, ds, options, request_id):
            pass


def test_catalogue_publish_routes_all_matching_sinks():
    writes = []

    from hgraph.adaptors.data_catalogue.publish import (
        publish_impl_from_graph, publish_impl_to_graph,
    )

    @hg.compute_node
    def capture_write(
        data: hg.TS[hg.Frame[_Row]], options: hg.TS[dict[str, object]],
    ) -> hg.TS[datetime]:
        writes.append((data.value, options.value))
        return hg.MIN_DT

    @publish_impl_from_graph
    def publish_test_from_graph(
        dce: DataCatalogueEntry, data_sink: hg.TS[_Sink],
        options: hg.TS[dict[str, object]], request_id: hg.TS[int],
        data: hg.TS[hg.Frame[hg.SCHEMA]],
        _schema: type[hg.SCHEMA] = hg.AUTO_RESOLVE,
    ):
        hg.null_sink(request_id)

    @publish_impl_to_graph
    def publish_test_to_graph(
        dce: DataCatalogueEntry, data_sink: hg.TS[_Sink],
        options: hg.TS[dict[str, object]], request_id: hg.TS[int],
        data: hg.TS[hg.Frame[hg.SCHEMA]],
        _schema: type[hg.SCHEMA] = hg.AUTO_RESOLVE,
    ) -> hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]:
        return hg.combine[hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]](
            status=StreamStatus.OK,
            status_msg="",
            values=capture_write(data, options),
            timestamp=hg.MIN_DT,
        )

    frame = pa.table({"name": ["a"], "value": [1]})

    @hg.graph
    def app():
        hg.register_adaptor("data-catalogue-publish", publish_adaptor_impl)
        data = hg.const(frame, tp=hg.TS[hg.Frame[_Row]])
        hg.null_sink(publish[_Row]("rows", data))

    catalogue = DataCatalogue()
    with hg.GlobalContext(hg.GlobalState()):
        with catalogue:
            DataCatalogueEntry[_Sink](
                _Row,
                "rows",
                frozendict(),
                _Sink(
                    sink_path="memory",
                    table="rows",
                ),
            )
            hg.run_graph(app)

    assert writes[0][0].equals(frame)
