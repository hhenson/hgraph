from hgraph import TSB, TS_SCHEMA, TS, compute_node, TSD, TIME_SERIES_TYPE, TimeSeriesSchema, AUTO_RESOLVE
from hgraph.nodes._conversion_operators._conversion_operator_templates import convert


__all__ = ("convert_tsb_to_bool", "convert_tsb_to_tsd")


@compute_node(overloads=convert)
def convert_tsb_to_bool(ts: TSB[TS_SCHEMA], to: type[TS[bool]]) -> TS[bool]:
    """
    Returns True if the ts is valid or false otherwise.
    """
    return ts.valid


def _convert_tsb_to_tsd_requirements(mapping, scalars):
    schema = mapping[TS_SCHEMA].py_type.__meta_data_schema__
    keys = scalars['keys']
    if keys is None:
        keys = schema.keys()
    value_types = set(schema[k] for k in keys)
    return len(value_types) == 1


@compute_node(overloads=convert, requires=_convert_tsb_to_tsd_requirements)
def convert_tsb_to_tsd(ts: TSB[TS_SCHEMA], to: type[TSD[str, TIME_SERIES_TYPE]],
                       keys: tuple[str, ...] = None,
                       _schema_tp: type[TS_SCHEMA] = AUTO_RESOLVE) -> TSD[str, TIME_SERIES_TYPE]:
    """
    Converts a suitable TSB into a TSD of TIME_SERIES_TYPE values. This requires the
    items of the `TSB` all have the same type.
    If a tuple of keys are provided, only the keys listed are considered.
    """
    if keys is None:
        keys = _schema_tp.__meta_data_schema__.keys()
    out = {
        k: v.delta_value for k in keys if (v := ts[k]).modified
    }
    if out:
        return out

