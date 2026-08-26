from dataclasses import dataclass
from datetime import datetime
import inspect

from frozendict import frozendict

from hgraph import (
    AUTO_RESOLVE, DEFAULT, MIN_DT, SCHEMA, TS_SCHEMA, CompoundScalar, Frame, GlobalState, TS, TSB, TSD, TSS,
    combine, compute_node, const, convert, dispatch, downcast_ref, emit, feedback, graph,
    len_, map_, max_, nothing, null_sink, operator, reduce, service_adaptor, service_adaptor_impl,
    switch_,
)
from hgraph.reflection import scalar_type
from hgraph.stream import Data, Stream, StreamStatus
from hgraph._wiring._core import _current_wiring
from hgraph._wiring._services import _materialize_pending_registrations

from .catalogue import DataCatalogue, DataCatalogueEntry, DataSink

__all__ = (
    "DataCatalogSinkResult",
    "find_data_catalogue_entries",
    "publish",
    "publish_adaptor",
    "publish_adaptor_impl",
    "publish_impl_from_graph",
    "publish_impl_to_graph",
)


@dataclass(frozen=True)
class DataCatalogSinkResult(CompoundScalar):
    dce: DataCatalogueEntry[DataSink]
    options: frozendict[str, object]


@compute_node
def find_data_catalogue_entries(
    dataset: TS[str], options: TS[object], schema: object,
    global_state: GlobalState = None,
) -> TSS[DataCatalogSinkResult]:
    catalogue = DataCatalogue.instance(global_state)
    if not catalogue.get_entries(schema, dataset.value, DataSink):
        return frozenset()
    return frozenset(
        DataCatalogSinkResult(entry, resolved)
        for entry, resolved in catalogue.matching_entries(
            schema, dataset.value, DataSink, options.value or {})
    )


@service_adaptor
def publish_adaptor(
    dce: TS[DataCatalogueEntry[DataSink]],
    options: TS[dict[str, object]],
    data: TS[Frame[SCHEMA]],
    path: str = "data-catalogue-publish",
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]:
    ...


@service_adaptor_impl(interfaces=publish_adaptor)
def publish_adaptor_impl(
    dce: TSD[int, TS[DataCatalogueEntry[DataSink]]],
    options: TSD[int, TS[dict[str, object]]],
    data: TSD[int, TS[Frame[SCHEMA]]],
    path: str,
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[datetime]]]]:
    @graph
    def route_from(
        key: TS[int], entry: TS[DataCatalogueEntry[DataSink]],
        opts: TS[dict[str, object]], frame: TS[Frame[_schema]],
    ):
        cases = {}
        for catalogue_entry in DataCatalogue.instance().get_entries_for_store_type(
            _schema, DataSink
        ):
            sink_type = type(catalogue_entry.store)

            def make_case(sink_type):
                @graph
                def call(
                    entry: TS[DataCatalogueEntry[DataSink]], opts: TS[dict[str, object]],
                    frame: TS[Frame[_schema]], request_id: TS[int],
                ):
                    publish_sink_from_graph[SCHEMA:_schema](
                        request_id,
                        downcast_ref(sink_type, entry.store),
                        opts,
                        frame,
                    )
                return call

            cases[catalogue_entry.store.sink_path] = make_case(sink_type)
        if not cases:
            null_sink(key)
            return
        return switch_(entry.store.sink_path, cases, entry, opts, frame, key)

    map_(route_from, entry=dce, opts=options, frame=data)

    @graph
    def extract_sink(entry: TS[DataCatalogueEntry[DataSink]]) -> TS[DataSink]:
        return entry.store

    feedback_sink = feedback(TSD[int, TS[DataSink]])
    feedback_sink(map_(extract_sink, entry=dce))
    feedback_options = feedback(TSD[int, TS[dict[str, object]]])
    feedback_options(options)
    feedback_data = feedback(TSD[int, TS[Frame[_schema]]])
    feedback_data(data)

    @graph
    def route_to(
        key: TS[int], sink: TS[DataSink],
        opts: TS[dict[str, object]], frame: TS[Frame[_schema]],
    ) -> TSB[Stream[Data[datetime]]]:
        cases = {}
        for catalogue_entry in DataCatalogue.instance().get_entries_for_store_type(
            _schema, DataSink
        ):
            sink_type = type(catalogue_entry.store)

            def make_case(sink_type):
                @graph
                def call(
                    sink: TS[DataSink], opts: TS[dict[str, object]],
                    frame: TS[Frame[_schema]], request_id: TS[int],
                ) -> TSB[Stream[Data[datetime]]]:
                    return publish_sink_to_graph[SCHEMA:_schema](
                        request_id,
                        downcast_ref(sink_type, sink),
                        opts,
                        frame,
                    )
                return call

            cases[catalogue_entry.store.sink_path] = make_case(sink_type)
        if not cases:
            return nothing[TSB[Stream[Data[datetime]]]]()
        return switch_(sink.sink_path, cases, sink, opts, frame, key)

    return map_(
        route_to,
        sink=feedback_sink(),
        opts=feedback_options(),
        frame=feedback_data(),
    )


@dispatch
@operator
def publish_sink_from_graph(
    request_id: TS[int], data_sink: TS[DataSink],
    options: TS[dict[str, object]], data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
): ...


@dispatch
@operator
def publish_sink_to_graph(
    request_id: TS[int], data_sink: TS[DataSink],
    options: TS[dict[str, object]], data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]: ...


@operator
def publish(
    dataset: TS[str], data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE, **options: TSB[TS_SCHEMA],
) -> TSB[Stream[Data[datetime]]]:
    ...


@graph(overloads=publish)
def _publish_frame(
    dataset: TS[str], data: TS[Frame[SCHEMA]], options: TS[dict[str, object]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[datetime]]]:
    selections = find_data_catalogue_entries(dataset, options, _schema)
    adaptor = publish_adaptor[SCHEMA:_schema]
    _materialize_pending_registrations(
        publish_adaptor, adaptor._resolution, _current_wiring())

    @graph
    def publish_mapped(
        key: TS[DataCatalogSinkResult], frame: TS[Frame[_schema]],
    ) -> TSB[Stream[Data[datetime]]]:
        path = "data-catalogue-publish"
        rid = adaptor._client_request_id(path, frame)
        adaptor.from_graph(
            key.dce, key.options, frame, path=path,
            __request_id__=rid)
        return adaptor.to_graph(
            path=path, __request_id__=rid, __no_ts_inputs__=True)

    @graph
    def publish_none(keys: TSS[DataCatalogSinkResult], frame: TS[Frame[_schema]]) \
            -> TSB[Stream[Data[datetime]]]:
        return combine[TSB[Stream[Data[datetime]]]](
            status=StreamStatus.OK,
            status_msg="",
            values=_publish_noop(frame),
            timestamp=MIN_DT,
        )

    @graph
    def publish_one(keys: TSS[DataCatalogSinkResult], frame: TS[Frame[_schema]]) \
            -> TSB[Stream[Data[datetime]]]:
        key = emit(keys)
        return adaptor(
            key.dce, key.options, frame, path="data-catalogue-publish")

    @graph
    def publish_many(keys: TSS[DataCatalogSinkResult], frame: TS[Frame[_schema]]) \
            -> TSB[Stream[Data[datetime]]]:
        responses = map_(publish_mapped, frame=frame, __keys__=keys)
        return combine[TSB[Stream[Data[datetime]]]](
            status=reduce(max_, responses.status),
            status_msg=reduce(lambda lhs, rhs: lhs + rhs, responses.status_msg),
            values=reduce(max_, responses.values),
            timestamp=reduce(max_, responses.timestamp),
        )

    return switch_(
        len_(selections), {0: publish_none, 1: publish_one, DEFAULT: publish_many},
        selections, data)


@graph(overloads=publish)
def _publish_frame_options(
    dataset: TS[str], data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = AUTO_RESOLVE, **options: TSB[TS_SCHEMA],
) -> TSB[Stream[Data[datetime]]]:
    return publish[SCHEMA:_schema](dataset, data, _pack_options(options))


@graph(overloads=publish)
def _publish_row_options(
    dataset: TS[str], data: TS[SCHEMA],
    _schema: type[SCHEMA] = AUTO_RESOLVE, **options: TSB[TS_SCHEMA],
) -> TSB[Stream[Data[datetime]]]:
    return publish[SCHEMA:_schema](
        dataset,
        convert[TS[Frame[_schema]]](data),
        _pack_options(options),
    )


def _pack_options(options):
    if not options:
        return const({}, tp=TS[dict[str, object]])
    boxed = {
        name: convert[TS[object]](value)
        for name, value in options.items()
    }
    return convert[TS[dict[str, object]]](combine(**boxed))


@compute_node
def _publish_noop(data: TS[Frame[SCHEMA]]) -> TS[datetime]:
    return MIN_DT


def _publisher_function(fn):
    fn = fn if hasattr(fn, "signature") else graph(fn)
    signature = inspect.signature(fn, eval_str=True)
    for name in ("dce", "data_sink", "options", "request_id", "data"):
        if name not in signature.parameters:
            raise TypeError(f"{fn.__name__} requires a '{name}' parameter")
    try:
        sink_type = scalar_type(signature.parameters["data_sink"].annotation)
    except TypeError as error:
        raise TypeError(f"{fn.__name__}.data_sink must be TS[DataSink subclass]") from error
    if not isinstance(sink_type, type) or not issubclass(sink_type, DataSink):
        raise TypeError(f"{fn.__name__}.data_sink must be TS[DataSink subclass]")
    return fn, sink_type


def publish_impl_from_graph(fn=None):
    if fn is None:
        return publish_impl_from_graph
    fn, sink_type = _publisher_function(fn)

    @graph(overloads=publish_sink_from_graph)
    def wrapper(
        request_id: TS[int], data_sink: TS[sink_type],
        options: TS[dict[str, object]], data: TS[Frame[SCHEMA]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ):
        entries = DataCatalogue.instance().get_entries_for_store_type(_schema, sink_type)
        cases = {}
        for entry in entries:
            def make_case(entry):
                @graph
                def call(ds, opts, frame, rid):
                    fn[SCHEMA:_schema](
                        dce=entry, data_sink=ds, options=opts,
                        request_id=rid, data=frame)
                return call
            cases[entry.store.sink_path] = make_case(entry)
        if not cases:
            null_sink(request_id)
            return
        return switch_(data_sink.sink_path, cases, data_sink, options, data, request_id)
    return fn


def publish_impl_to_graph(fn=None):
    if fn is None:
        return publish_impl_to_graph
    fn, sink_type = _publisher_function(fn)

    @graph(overloads=publish_sink_to_graph)
    def wrapper(
        request_id: TS[int], data_sink: TS[sink_type],
        options: TS[dict[str, object]], data: TS[Frame[SCHEMA]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ) -> TSB[Stream[Data[datetime]]]:
        entries = DataCatalogue.instance().get_entries_for_store_type(_schema, sink_type)
        cases = {}
        for entry in entries:
            def make_case(entry):
                @graph
                def call(ds, opts, frame, rid) -> TSB[Stream[Data[datetime]]]:
                    return fn[SCHEMA:_schema](
                        dce=entry, data_sink=ds, options=opts,
                        request_id=rid, data=frame)
                return call
            cases[entry.store.sink_path] = make_case(entry)
        if not cases:
            return nothing[TSB[Stream[Data[datetime]]]]()
        return switch_(data_sink.sink_path, cases, data_sink, options, data, request_id)
    return fn
