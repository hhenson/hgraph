from dataclasses import dataclass

from frozendict import frozendict

from hgraph.adaptors.data_catalogue import DataCatalogue, DataCatalogueEntry, DataSource
from hgraph import (
    TS_SCHEMA,
    TSB,
    TS,
    SCHEMA,
    DEFAULT,
    Frame,
    GlobalState,
    delayed_binding,
    graph,
    compute_node,
    exception_time_series,
    if_then_else,
    combine,
    convert,
    service_adaptor,
    service_adaptor_impl,
    TSD,
    AUTO_RESOLVE,
    map_,
    dispatch,
    valid,
    operator,
    feedback,
    WiringNodeClass,
    switch_,
    null_sink,
    nothing,
    HgTypeMetaData,
    CompoundScalar,
    const,
)
from hgraph.stream.stream import Stream, StreamStatus, Data


__all__ = ['FindDCEResult', 'find_data_catalogue_entry', 'subscribe', 'subscribe_adaptor', 'subscribe_adaptor_impl', 'subscriber_impl_to_graph', 'subscriber_impl_from_graph']


@dataclass(frozen=True)
class FindDCEResult(CompoundScalar):
    dce: DataCatalogueEntry[DataSource]
    options: dict[str, object]


@compute_node
def find_data_catalogue_entry(
    tp: type[SCHEMA], dataset: TS[str], __options__: TS[dict[str, object]]
) -> TSB[FindDCEResult]:
    dataset = dataset.value
    dc = GlobalState.instance().get("data_catalogue", DataCatalogue.instance())
    dces = dc.get_entries(tp, dataset, DataSource)
    if not dces:
        raise ValueError(f"No data catalogue entry found for {tp} and {dataset}")

    opts = __options__.value if __options__.valid else {}
    scope_checks = {}
    for dce in dces:
        if all(k in dce.scope for k in opts):
            defaulted_opts = {k: opts.get(k, scope.default()) for k, scope in dce.scope.items()}
            checks = {k: scope.in_scope(defaulted_opts[k]) for k, scope in dce.scope.items()}
            if all(checks.values()):
                return {"dce": dce, "options": opts}
            else:
                scope_checks[dce.dataset] = checks

    raise ValueError(f"Given options ({[o for o in opts]}) do not match any of the {len(dces)} "
                     f"data catalogue entries for {tp.__name__}, dataset '{dataset}'")


@operator
def subscribe(
    dataset: TS[str], _tp: type[SCHEMA] = DEFAULT[SCHEMA], **__options__: TSB[TS_SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]: ...


@graph(overloads=subscribe)
def subscribe_kwargs(
    dataset: TS[str], _tp: type[SCHEMA] = DEFAULT[SCHEMA], **__options__: TSB[TS_SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    if len(__options__.as_dict()) > 0:
        options = convert[TS[dict[str, object]]](__options__, __strict__=True)
    else:
        options = const(frozendict(), TS[dict[str, object]])
    return subscribe[_tp](dataset, options)


@graph(overloads=subscribe)
def subscribe_dict(
    dataset: TS[str], __options__: TS[dict[str, object]], _tp: type[SCHEMA] = DEFAULT[SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    dce_and_options = find_data_catalogue_entry(_tp, dataset, __options__)
    error = exception_time_series(dce_and_options)
    dce = dce_and_options.dce
    options = dce_and_options.options
    data = subscribe_adaptor[_tp](dce, options)
    return if_then_else(
        valid(error),
        combine[TSB[Stream[Data[Frame[_tp]]]]](status=StreamStatus.ERROR, status_msg=error.error_msg),
        data,
    )


@service_adaptor
def subscribe_adaptor(
    dce: TS[DataCatalogueEntry], options: TS[dict[str, object]], _schema: type[SCHEMA] = DEFAULT[SCHEMA]
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


@service_adaptor_impl(interfaces=subscribe_adaptor, label="{_schema.__name__}")
def subscribe_adaptor_impl(
    path: str,
    dce: TSD[int, TS[DataCatalogueEntry]],
    options: TSD[int, TS[dict[str, object]]],
    _schema: type[SCHEMA] = AUTO_RESOLVE,
) -> TSD[int, TSB[Stream[Data[Frame[SCHEMA]]]]]:
    fb = delayed_binding(TSD[int, TSB[Stream[Data[Frame[_schema]]]]])
    map_(lambda key, d, o, f: subscribe_source_from_graph[SCHEMA:_schema](key, d.dataset, d.store, o, f), d=dce, o=options, f=fb())
    fb_store = feedback(TSD[int, TS[DataSource]])
    fb_store(dce.store)
    fb_options = feedback(TSD[int, TS[dict[str, object]]])
    fb_options(options)
    fb_dataset = feedback(TSD[int, TS[str]])
    fb_dataset(dce.dataset)
    results = map_(
        lambda key, dataset, store, o: subscribe_source_to_graph[SCHEMA:_schema](key, dataset, store, o),
        dataset=fb_dataset(),
        store=fb_store(),
        o=fb_options(),
    )
    fb(results)
    return results


@dispatch
@operator
def subscribe_source_from_graph(
    request_id: TS[int],
    dataset: TS[str],
    ds: TS[DataSource],
    options: TS[dict[str, object]], 
    feedback: TSB[Stream[Data[Frame[SCHEMA]]]] = None,
    _schema: type[SCHEMA] = SCHEMA,
):
    ...


@dispatch
@operator
def subscribe_source_to_graph(
    request_id: TS[int],
    dataset: TS[str],
    ds: TS[DataSource],
    options: TS[dict[str, object]],
    _schema: type[SCHEMA] = SCHEMA,
) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
    ...


def subscriber_impl_from_graph(fn: callable = None):
    if fn is None:
        return lambda fn: subscriber_impl_from_graph(fn)
    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)
    for n, t in {
        "dce": HgTypeMetaData.parse_type(DataCatalogueEntry),
        "ds": HgTypeMetaData.parse_type(TS[DataSource]),
        "options": HgTypeMetaData.parse_type(TS[dict[str, object]]),
        "request_id": HgTypeMetaData.parse_type(TS[int]),
    }.items():
        assert t.matches(
            fn.signature.input_types.get(n)
        ), f"Expected {n} to have type {t}, got {fn.signature.input_types.get(n)}"
        
    if fb_type := fn.signature.input_types.get("feedback"):
        if HgTypeMetaData.parse_type(TSB[Stream[Data[Frame[SCHEMA]]]]).matches(fb_type):
            fb_status_only = False
        elif HgTypeMetaData.parse_type(TS[StreamStatus]).matches(fb_type):
            fb_status_only = True
        
    data_source_type = fn.signature.input_types["ds"].value_scalar_tp.py_type

    @graph(overloads=subscribe_source_from_graph)
    def subscribe_source_from_graph_wrapper(
        request_id: TS[int],
        dataset: TS[str],
        ds: TS[data_source_type],
        options: TS[dict[str, object]],
        feedback: TSB[Stream[Data[Frame[SCHEMA]]]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ):
        sources = DataCatalogue.instance().get_entries_for_store_type(_schema, data_source_type)
        if sources:
            def make_switch(dce):
                if fb_type is None:
                    return lambda ds, o, r, f: fn[SCHEMA:_schema](dce=dce, ds=ds, options=o, request_id=r) and None
                elif fb_status_only:
                    return lambda ds, o, r, f: fn[SCHEMA:_schema](dce=dce, ds=ds, options=o, request_id=r, feedback=f.status) and None
                else:
                    return lambda ds, o, r, f: fn[SCHEMA:_schema](dce=dce, ds=ds, options=o, request_id=r, feedback=f) and None

            switch_(
                dataset,
                {dce.dataset: make_switch(dce) for dce in sources},
                ds,
                options,
                request_id, 
                feedback,
            )
        else:
            null_sink(request_id)


def subscriber_impl_to_graph(fn: callable = None):
    if fn is None:
        return lambda fn: subscriber_impl_to_graph(fn)
    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)
    for n, t in {
        "dce": HgTypeMetaData.parse_type(DataCatalogueEntry),
        "ds": HgTypeMetaData.parse_type(TS[DataSource]),
        "options": HgTypeMetaData.parse_type(TS[dict[str, object]]),
        "request_id": HgTypeMetaData.parse_type(TS[int]),
    }.items():
        assert t.matches(
            fn.signature.input_types.get(n)
        ), f"Expected {n} to have type {t}, got {fn.signature.input_types.get(n)}"
        
    data_source_type = fn.signature.input_types["ds"].value_scalar_tp.py_type

    @graph(overloads=subscribe_source_to_graph)
    def subscribe_source_to_graph_wrapper(
        request_id: TS[int],
        dataset: TS[str],
        ds: TS[data_source_type],
        options: TS[dict[str, object]],
        _schema: type[SCHEMA] = AUTO_RESOLVE,
    ) -> TSB[Stream[Data[Frame[SCHEMA]]]]:
        sources = DataCatalogue.instance().get_entries_for_store_type(_schema, data_source_type)
        if sources:
            def make_switch(dce):
                return lambda ds, o, r: fn[SCHEMA: _schema](dce=dce, ds=ds, options=o, request_id=r)

            return switch_(
                dataset,
                {dce.dataset: make_switch(dce) for dce in sources},
                ds,
                options,
                request_id
            )
        else:
            return nothing[TSB[Stream[Data[Frame[_schema]]]]]()
