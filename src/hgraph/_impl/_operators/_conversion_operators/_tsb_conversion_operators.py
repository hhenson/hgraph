from typing import Type

from hgraph import (
    TSB,
    TS_SCHEMA,
    TS,
    compute_node,
    TSD,
    TIME_SERIES_TYPE,
    AUTO_RESOLVE,
    graph,
    combine,
    convert,
    DEFAULT,
    OUT,
    TS_SCHEMA_1,
    HgTypeMetaData,
)

__all__ = ("convert_tsb_to_bool", "convert_tsb_to_tsd")


@graph(overloads=combine, requires=lambda m, s: (OUT not in m or m[OUT].py_type is TSB) and not s["__strict__"])
def combine_unnamed_tsb(__strict__: bool = False, **bundle: TSB[TS_SCHEMA]) -> TSB[TS_SCHEMA]:
    return bundle


@compute_node(
    overloads=combine,
    requires=lambda m, s: (OUT not in m or m[OUT].py_type is TSB) and s["__strict__"],
    all_valid=("bundle",),
)
def combine_unnamed_tsb_strict(*, __strict__: bool, _output: OUT = None, **bundle: TSB[TS_SCHEMA]) -> TSB[TS_SCHEMA]:
    return bundle.value if not _output.valid else bundle.delta_value


@graph(overloads=combine)
def combine_named_tsb(tp_: Type[TSB[TS_SCHEMA]] = DEFAULT[OUT], **bundle: TSB[TS_SCHEMA]) -> TSB[TS_SCHEMA]:
    return bundle


def _combine_tsb_partial_requirements(mapping, scalars):
    in_schema = mapping[TS_SCHEMA_1].meta_data_schema
    out_schema = mapping[TS_SCHEMA].meta_data_schema
    return set(in_schema.keys()).issubset(out_schema.keys()) and all(
        out_schema[k].matches(in_) for k, in_ in in_schema.items()
    )


@graph(overloads=combine, requires=_combine_tsb_partial_requirements)
def combine_named_tsb_partial(tp_: Type[TSB[TS_SCHEMA]] = DEFAULT[OUT], **bundle: TSB[TS_SCHEMA_1]) -> TSB[TS_SCHEMA]:
    return TSB.from_ts(__type__=tp_, **bundle.as_dict())


@graph(overloads=convert)
def convert_tsbs(ts: TSB[TS_SCHEMA], to: type[TSB[TS_SCHEMA_1]] = DEFAULT[OUT]) -> TSB[TS_SCHEMA_1]:
    """
    Converts a TSB to another TSB if the keys and types match.
    """
    return combine[to](**{
        k: v
        for k, v in ts.as_dict().items()
        if k in HgTypeMetaData.parse_type(to).bundle_schema_tp.meta_data_schema.keys()
    })


@compute_node(overloads=convert)
def convert_tsb_to_bool(ts: TSB[TS_SCHEMA], to: type[TS[bool]]) -> TS[bool]:
    """
    Returns True if the ts is valid or false otherwise.
    """
    return ts.valid  # AB: There is a 'valid' node for that, I would not see this as conversion


def _convert_tsb_to_tsd_requirements(mapping, scalars):
    schema = mapping[TS_SCHEMA].py_type.__meta_data_schema__
    keys = scalars["keys"]
    if keys is None:
        keys = schema.keys()
    value_types = set(schema[k] for k in keys)
    return (
        len(value_types) == 1
        or f"{mapping[TS_SCHEMA].py_type} cannot be converted to a TSD as it requires all values to be the same type"
    )


@compute_node(overloads=convert, requires=_convert_tsb_to_tsd_requirements)
def convert_tsb_to_tsd(
    ts: TSB[TS_SCHEMA],
    to: type[TSD[str, TIME_SERIES_TYPE]],
    keys: tuple[str, ...] = None,
    _schema_tp: type[TS_SCHEMA] = AUTO_RESOLVE,
) -> TSD[str, TIME_SERIES_TYPE]:
    """
    Converts a suitable TSB into a TSD of TIME_SERIES_TYPE values. This requires the
    items of the `TSB` all have the same type.
    If a tuple of keys are provided, only the keys listed are considered.
    """
    if keys is None:
        keys = _schema_tp.__meta_data_schema__.keys()
    out = {k: v.delta_value for k in keys if (v := ts[k]).modified}
    if out:
        return out
