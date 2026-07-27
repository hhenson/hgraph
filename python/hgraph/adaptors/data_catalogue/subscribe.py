from dataclasses import dataclass
import inspect

from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    AUTO_RESOLVE,
    Frame,
    SCHEMA,
    TS,
    TSB,
    TSD,
    WiringPort,
    combine,
    compute_node,
    const,
    convert,
    delayed_binding,
    dispatch,
    downcast_ref,
    graph,
    map_,
    nothing,
    null_sink,
    operator,
    service_adaptor,
    service_adaptor_impl,
    switch_,
)
from hgraph.stream import Data, Stream, StreamStatus
from hgraph.reflection import scalar_type

from .catalogue import DataCatalogue, DataCatalogueEntry, DataSource

__all__ = (
    "FindDCEResult",
    "find_data_catalogue_entry",
    "subscribe",
    "subscribe_adaptor",
    "subscribe_adaptor_impl",
    "subscriber_impl_from_graph",
    "subscriber_impl_to_graph",
)


_RAW_STREAM = TSB[Stream[Data[Frame]]]


@dataclass(frozen=True)
class FindDCEResult(CompoundScalar):
    dce: DataCatalogueEntry[DataSource]
    options: frozendict[str, object]


@compute_node
def find_data_catalogue_entry(
    dataset: TS[str], options: TS[object], schema: object
) -> TS[FindDCEResult]:
    match = DataCatalogue.instance().matching_entries(
        schema, dataset.value, DataSource, options.value or {}
    )[0]
    return FindDCEResult(*match)


@dataclass(frozen=True)
class _SubscribeRequest(CompoundScalar):
    entry: DataCatalogueEntry[DataSource]
    options: frozendict[str, object]


@compute_node
def _make_request(selection: TS[FindDCEResult]) -> TS[_SubscribeRequest]:
    return _SubscribeRequest(selection.value.dce, selection.value.options)


@service_adaptor
def subscribe_adaptor(
    request: TS[_SubscribeRequest],
    path: str = "data-catalogue",
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=subscribe_adaptor)
def subscribe_adaptor_impl(
    requests: TSD[int, TS[_SubscribeRequest]],
    path: str,
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    replies = delayed_binding(TSD[int, TSB[Stream[Data[Frame[_schema]]]]])

    @graph
    def route_from(
        key: TS[int], request: TS[_SubscribeRequest],
        response: TSB[Stream[Data[Frame[_schema]]]],
    ) -> TS[bool]:
        cases = {}
        for entry in DataCatalogue.instance().get_entries_for_store_type(_schema, DataSource):
            source_type = type(entry.store)

            def make_case(source_type):
                @graph
                def call(request: TS[_SubscribeRequest],
                         response: TSB[Stream[Data[Frame[_schema]]]],
                         rid: TS[int]) -> TS[bool]:
                    d = request.entry
                    subscribe_source_from_graph[SCHEMA:_schema](
                        rid, d.dataset, downcast_ref(source_type, d.store),
                        request.options, response)
                    return const(True, tp=TS[bool])
                return call
            cases[entry.dataset] = make_case(source_type)
        return switch_(request.entry.dataset, cases, request, response, key)

    null_sink(map_(route_from, request=requests, response=replies()))

    @graph
    def route_to(
        key: TS[int], request: TS[_SubscribeRequest],
    ) -> TSB[Stream[Data[Frame[_schema]]]]:
        cases = {}
        for entry in DataCatalogue.instance().get_entries_for_store_type(_schema, DataSource):
            source_type = type(entry.store)

            def make_case(source_type):
                @graph
                def call(request: TS[_SubscribeRequest], rid: TS[int]) \
                        -> TSB[Stream[Data[Frame[_schema]]]]:
                    d = request.entry
                    return subscribe_source_to_graph[SCHEMA:_schema](
                        rid, d.dataset, downcast_ref(source_type, d.store),
                        request.options)
                return call
            cases[entry.dataset] = make_case(source_type)
        return switch_(request.entry.dataset, cases, request, key)

    result = map_(route_to, request=requests)
    replies(result)
    return result


@dispatch
@operator
def subscribe_source_from_graph(
    request_id: TS[int], dataset: TS[str], ds: TS[DataSource],
    options: TS[dict[str, object]],
    feedback: TSB[Stream[Data[Frame[SCHEMA]]]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
): ...


@dispatch
@operator
def subscribe_source_to_graph(
    request_id: TS[int], dataset: TS[str], ds: TS[DataSource],
    options: TS[dict[str, object]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]: ...


class _Subscribe:
    __name__ = "subscribe"

    def __init__(self, schema=None):
        self.schema = schema

    def __getitem__(self, schema):
        return _Subscribe(schema)

    def __call__(self, dataset, __options__=None, **options):
        if self.schema is None:
            raise TypeError("subscribe requires a schema, for example subscribe[Row](...)")
        options_port = _options_port(__options__, options)
        dataset_port = dataset if isinstance(dataset, WiringPort) else const(dataset, tp=TS[str])
        selection = find_data_catalogue_entry(dataset_port, options_port, self.schema)
        raw = subscribe_adaptor[SCHEMA:self.schema](
            _make_request(selection), path="data-catalogue")
        output_type = TSB[Stream[Data[Frame[self.schema]]]]
        return output_type.from_ts(
            status=raw.status,
            status_msg=raw.status_msg,
            values=convert[TS[Frame[self.schema]]](raw.values),
            timestamp=raw.timestamp,
        )


subscribe = _Subscribe()


def _subscriber_function(fn):
    fn = fn if hasattr(fn, "signature") else graph(fn)
    signature = inspect.signature(fn, eval_str=True)
    for name in ("dce", "ds", "options", "request_id"):
        if name not in signature.parameters:
            raise TypeError(f"{fn.__name__} requires a '{name}' parameter")
    try:
        source_type = scalar_type(signature.parameters["ds"].annotation)
    except TypeError as error:
        raise TypeError(f"{fn.__name__}.ds must be TS[DataSource subclass]") from error
    if not isinstance(source_type, type) or not issubclass(source_type, DataSource):
        raise TypeError(f"{fn.__name__}.ds must be TS[DataSource subclass]")
    return fn, signature, source_type


def subscriber_impl_from_graph(fn=None):
    if fn is None:
        return subscriber_impl_from_graph
    fn, signature, source_type = _subscriber_function(fn)
    feedback_parameter = signature.parameters.get("feedback")
    feedback_type = None if feedback_parameter is None else feedback_parameter.annotation
    status_only = feedback_type == TS[StreamStatus]

    @graph(overloads=subscribe_source_from_graph)
    def wrapper(
        request_id: TS[int], dataset: TS[str], ds: TS[source_type],
        options: TS[dict[str, object]],
        feedback: TSB[Stream[Data[Frame[SCHEMA]]]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ) -> TS[bool]:
        entries = DataCatalogue.instance().get_entries_for_store_type(
            _schema, source_type)
        cases = {}
        for entry in entries:
            def make_case(entry):
                @graph
                def call(
                    ds: TS[source_type], opts: TS[dict[str, object]],
                    rid: TS[int], response: TSB[Stream[Data[Frame[_schema]]]],
                ) -> TS[bool]:
                    kwargs = dict(dce=entry, ds=ds, options=opts, request_id=rid)
                    if feedback_type is not None:
                        kwargs["feedback"] = response.status if status_only else response
                    fn[SCHEMA:_schema](**kwargs)
                    return const(True, tp=TS[bool])
                return call
            cases[entry.dataset] = make_case(entry)
        if not cases:
            null_sink(request_id)
            return const(True, tp=TS[bool])
        return switch_(dataset, cases, ds, options, request_id, feedback)
    return fn


def subscriber_impl_to_graph(fn=None):
    if fn is None:
        return subscriber_impl_to_graph
    fn, signature, source_type = _subscriber_function(fn)

    @graph(overloads=subscribe_source_to_graph)
    def wrapper(
        request_id: TS[int], dataset: TS[str], ds: TS[source_type],
        options: TS[dict[str, object]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
        entries = DataCatalogue.instance().get_entries_for_store_type(
            _schema, source_type)
        cases = {}
        for entry in entries:
            def make_case(entry):
                @graph
                def call(
                    ds: TS[source_type], opts: TS[dict[str, object]], rid: TS[int],
                ) -> TSB[Stream[Data[Frame[_schema]]]]:
                    return fn[SCHEMA:_schema](
                        dce=entry, ds=ds, options=opts, request_id=rid)
                return call
            cases[entry.dataset] = make_case(entry)
        if not cases:
            return nothing[TSB[Stream[Data[Frame[_schema]]]]]()
        return switch_(dataset, cases, ds, options, request_id)
    return fn


def _options_port(explicit, options):
    if explicit is not None and options:
        raise TypeError("provide either __options__ or keyword options, not both")
    if explicit is not None:
        return explicit if isinstance(explicit, WiringPort) else const(explicit, tp=TS[object])
    if not options:
        return const(frozendict(), tp=TS[object])
    if any(isinstance(value, WiringPort) for value in options.values()):
        return convert[TS[dict[str, object]]](combine(**options))
    return const(frozendict(options), tp=TS[object])
