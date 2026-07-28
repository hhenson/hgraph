from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from frozendict import frozendict

import hgraph as hg
from hgraph.adaptors.data_catalogue import (
    DataCatalogue,
    DataCatalogueEntry,
    DataEnvironment,
    DataEnvironmentEntry,
    DataSink,
    publish,
    publish_adaptor_impl,
    subscribe,
    subscribe_adaptor_impl,
)
from hgraph.adaptors.json import JsonDataSource
from hgraph.adaptors.json import json_adaptor_impl
from hgraph.stream import StreamStatus


@dataclass(frozen=True)
class _Row(hg.CompoundScalar):
    name: str
    value: int


@dataclass(frozen=True)
class _Sink(DataSink):
    table: str


def _end_time():
    # Safety net only: both real-time tests stop their own engine after the
    # asynchronous adaptor response arrives. Keep enough headroom for a cold
    # wheel or a stalled shared runner to finish the worker task before the
    # executor reaches end_time and rejects a late push.
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=15)


def test_catalogue_subscribe_routes_json_source(tmp_path):
    (tmp_path / "rows.json").write_text('[{"name":"a","value":1}]')
    responses = []
    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[hg.Frame[_Row]]]]

    @hg.sink_node
    def capture(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.modified:
            responses.append((response.status.value, response.status_msg.value,
                              response["values"].value))
        if response.status.value in (StreamStatus.OK, StreamStatus.ERROR):
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("data-catalogue", subscribe_adaptor_impl)
        hg.register_adaptor("json", json_adaptor_impl)
        capture(subscribe[_Row]("rows"))

    catalogue = DataCatalogue()
    environment = DataEnvironment()
    environment.add_entry(DataEnvironmentEntry("json", str(tmp_path)))
    with hg.GlobalContext(hg.GlobalState()):
        with catalogue, environment:
            DataCatalogueEntry[JsonDataSource](
                _Row,
                "rows",
                frozendict(),
                JsonDataSource(source_path="json", file="rows.json"),
            )
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert responses[0][0] is StreamStatus.OK, responses[0][1]
    assert responses[0][2].equals(pa.table({"name": ["a"], "value": [1]}))


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
        return datetime.now(timezone.utc).replace(tzinfo=None)

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
            timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
        )

    frame = pa.table({"name": ["a"], "value": [1]})

    @hg.push_queue(hg.TS[hg.Frame[_Row]])
    def data(sender):
        sender(frame)

    response_type = hg.TSB[hg.stream.Stream[hg.stream.Data[datetime]]]

    @hg.sink_node
    def stop(response: response_type, engine: hg.EvaluationEngineApi = None):
        if response.status.value is StreamStatus.OK:
            engine.request_engine_stop()

    @hg.graph
    def app():
        hg.register_adaptor("data-catalogue-publish", publish_adaptor_impl)
        stop(publish[_Row]("rows", data()))

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
            hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME, end_time=_end_time())

    assert writes[0][0].equals(frame)
