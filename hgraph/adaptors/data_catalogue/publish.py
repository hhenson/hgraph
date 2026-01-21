from dataclasses import dataclass
from datetime import datetime

from frozendict import frozendict

from hgraph.stream.stream import reduce_statuses, reduce_status_messages
from hgraph.adaptors.data_catalogue import DataCatalogue, DataCatalogueEntry, DataSink
from hgraph import (
    all_,
    emit,
    if_true,
    last_modified_time,
    len_,
    log_,
    operator,
    TS,
    Frame,
    SCHEMA,
    TSB,
    DEFAULT,
    graph,
    service_adaptor,
    service_adaptor_impl,
    TSD,
    AUTO_RESOLVE,
    dispatch,
    WiringNodeClass,
    switch_,
    null_sink,
    nothing,
    map_,
    feedback,
    compute_node,
    exception_time_series,
    if_then_else,
    valid,
    combine,
    TS_SCHEMA,
    convert,
    TSS,
    max_,
    CompoundScalar,
    filter_,
    sample,
)
from hgraph.stream.stream import Stream, Data, StreamStatus


__all__ = ['DataCatalogSinkResult', 'find_data_catalogue_entries', 'publish', 'publish_adaptor', 'publish_adaptor_impl', 'publish_impl_to_graph', 'publish_impl_from_graph']


@dataclass(frozen=True)
class DataCatalogSinkResult(CompoundScalar):
    dce: DataCatalogueEntry[DataSink]
    options: dict[str, object]


@compute_node(valid=("dataset",))
def find_data_catalogue_entries(
    tp: type[SCHEMA], dataset: TS[str], __options__: TS[dict[str, object]]
) -> TSS[DataCatalogSinkResult]:
    dataset = dataset.value
    dces = DataCatalogue.instance().get_entries(tp, dataset, DataSink)
    if not dces:
        raise ValueError(f"No data catalogue entry found for {tp} and {dataset}")

    opts = __options__.value if __options__.valid else {}
    valid_entries = set()
    scope_checks = {}

    for dce in dces:
        if all(k in dce.scope for k in opts):
            defaulted_opts = {k: opts.get(k, scope.default()) for k, scope in dce.scope.items()}
            checks = {k: scope.in_scope(defaulted_opts[k]) for k, scope in dce.scope.items()}
            if all(checks.values()):
                valid_entries.add(DataCatalogSinkResult(dce=dce, options=frozendict(opts)))
            else:
                scope_checks[dce.dataset] = checks

    if valid_entries:
        return valid_entries

    raise ValueError(
        f"No data catalogue entry found for {tp} and '{dataset}' with options {opts} and check results {scope_checks}"
    )


@operator
def publish(
    dataset: TS[str], data: TS[Frame[SCHEMA]], _tp: type[SCHEMA] = DEFAULT[SCHEMA], **__options__: TSB[TS_SCHEMA]
) -> TSB[Stream[Data[datetime]]]: ...


@graph(overloads=publish)
def publish_row(
    dataset: TS[str], data: TS[SCHEMA], _tp: type[SCHEMA] = DEFAULT[SCHEMA], **__options__: TSB[TS_SCHEMA]
) -> TSB[Stream[Data[datetime]]]:
    options = convert[TS[dict[str, object]]](__options__)
    return publish[_tp](dataset, convert[TS[Frame[_tp]]](data), options)


@graph(overloads=publish)
def publish_kwargs(
    dataset: TS[str], data: TS[Frame[SCHEMA]], _tp: type[SCHEMA] = DEFAULT[SCHEMA], **__options__: TSB[TS_SCHEMA]
) -> TSB[Stream[Data[datetime]]]:
    options = convert[TS[dict[str, object]]](__options__)
    return publish[_tp](dataset, data, options)


@graph(overloads=publish)
def publish_dict(
    dataset: TS[str], data: TS[Frame[SCHEMA]], __options__: TS[dict[str, object]], _tp: type[SCHEMA] = DEFAULT[SCHEMA]
) -> TSB[Stream[Data[datetime]]]:
    dces_and_options = find_data_catalogue_entries(_tp, dataset, __options__)
    error = exception_time_series(dces_and_options)

    _schema = _tp
    return if_then_else(
                    valid(error),
                    combine[TSB[Stream[Data[datetime]]]](status=StreamStatus.ERROR, status_msg=error.error_msg),
                    switch_(len_(dces_and_options), {
                        0: lambda dce, d: combine[TSB[Stream[Data[datetime]]]](status=StreamStatus.ERROR, status_msg="no sinks found in the data catalogue"),
                        1: lambda dce, d: _publish_one(emit(dce), d, _schema),
                        DEFAULT: lambda dce, d: _publish_many(dce, d, _schema),
                    }, dce=dces_and_options, d=data)
    )

@graph
def _publish_one(dce: TS[DataCatalogSinkResult], data: TS[Frame[SCHEMA]], _schema: type[SCHEMA]) -> TSB[Stream[Data[datetime]]]:
    return publish_adaptor[_schema](dce=dce.dce, options=dce.options, data=data)


@graph
def _publish_many(dces: TSS[DataCatalogSinkResult], data: TS[Frame[SCHEMA]], _schema: type[SCHEMA]) -> TSB[Stream[Data[datetime]]]:
    out = map_(
        lambda key, d: publish_adaptor[_schema](dce=key.dce, options=key.options, data=d),
        d=data,
        __keys__=dces,
    )
    
    done = all_(map_(lambda o, t: last_modified_time(o) > t, out, last_modified_time(data)))
    
    return sample(if_true(done), combine[TSB[Stream[Data[datetime]]]](
        status=reduce_statuses(out.status),
        status_msg=reduce_status_messages(out.status_msg),
        values=max_(out.values),
    ))
    
    
@service_adaptor
def publish_adaptor(
    dce: TS[DataCatalogueEntry],
    options: TS[dict[str, object]],
    data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = DEFAULT[SCHEMA],
) -> TSB[Stream[Data[datetime]]]: ...


@service_adaptor_impl(interfaces=publish_adaptor)
def publish_adaptor_impl(
    path: str,
    dce: TSD[int, TS[DataCatalogueEntry]],
    options: TSD[int, TS[dict[str, object]]],
    data: TSD[int, TS[Frame[SCHEMA]]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[datetime]]]]:
    map_(lambda key, dc, o, d: publish_sink_from_graph[SCHEMA:_schema](key, dc.store, o, d), dc=dce, o=options, d=data)

    fb_sink = feedback(TSD[int, TS[DataSink]])
    fb_sink(dce.store)
    fb_data = feedback(TSD[int, TS[Frame[_schema]]])
    fb_data(data)
    fb_options = feedback(TSD[int, TS[dict[str, object]]])
    fb_options(options)

    return map_(
        lambda key, ds, o, d: publish_sink_to_graph[SCHEMA:_schema](key, ds, o, d),
        ds=fb_sink(),
        o=fb_options(),
        d=fb_data(),
    )


@dispatch
@operator
def publish_sink_from_graph(
    request_id: TS[int], data_sink: TS[DataSink], options: TS[dict[str, object]], data: TS[Frame[SCHEMA]]
): ...


@dispatch
@operator
def publish_sink_to_graph(
    request_id: TS[int],
    data_sink: TS[DataSink],
    options: TS[dict[str, object]],
    data: TS[Frame[SCHEMA]],
    _schema: type[SCHEMA] = SCHEMA,
) -> TSB[Stream[Data[datetime]]]: ...


def publish_impl_from_graph(fn: callable = None):
    if fn is None:
        return lambda func: publish_impl_from_graph(func)

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    data_sink_type = fn.signature.input_types["data_sink"].value_scalar_tp.py_type

    @graph(overloads=publish_sink_from_graph)
    def publish_sink_from_graph_wrapper(
        request_id: TS[int],
        data_sink: TS[data_sink_type],
        options: TS[dict[str, object]],
        data: TS[Frame[SCHEMA]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ):
        if sources := DataCatalogue.instance().get_entries_for_store_type(_schema, data_sink_type):

            def make_switch(dce):
                return (
                    lambda ds, o, d, r: fn[SCHEMA:_schema](dce=dce, data_sink=ds, options=o, data=d, request_id=r)
                    and None
                )

            switch_(
                data_sink.sink_path,
                {dce.store.sink_path: make_switch(dce) for dce in sources},
                data_sink,
                options,
                data,
                request_id,
            )
        else:
            null_sink(request_id)


def publish_impl_to_graph(fn: callable = None):
    if fn is None:
        return lambda func: publish_impl_to_graph(func)

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    data_sink_type = fn.signature.input_types["data_sink"].value_scalar_tp.py_type

    @graph(overloads=publish_sink_to_graph)
    def publish_sink_to_graph_wrapper(
        request_id: TS[int],
        data_sink: TS[data_sink_type],
        options: TS[dict[str, object]],
        data: TS[Frame[SCHEMA]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ) -> TSB[Stream[Data[datetime]]]:
        if sources := DataCatalogue.instance().get_entries_for_store_type(_schema, data_sink_type):

            def make_switch(dce):
                return lambda ds, o, d, r: fn[SCHEMA:_schema](dce=dce, data_sink=ds, options=o, data=d, request_id=r)

            return switch_(
                data_sink.sink_path,
                {dce.store.sink_path: make_switch(dce) for dce in sources},
                data_sink,
                options,
                data,
                request_id,
            )
        else:
            return nothing[TSB[Stream[Data[datetime]]]]()
