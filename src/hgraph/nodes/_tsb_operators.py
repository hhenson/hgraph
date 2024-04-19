from typing import Type

from hgraph import compute_node, REF, TSB, TS_SCHEMA, TIME_SERIES_TYPE, PythonTimeSeriesReference, AUTO_RESOLVE, SCALAR, \
    WiringError
from hgraph._types._ref_type import TimeSeriesReference


__all__ = ("tsb_get_item", "tsb_get_item_by_name", "tsb_get_item_by_index")


@compute_node()
def tsb_get_item(tsb: REF[TSB[TS_SCHEMA]], key: SCALAR, key_type: Type[SCALAR] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:
    raise WiringError(f"tsb_get_item can only pick items using string name or integer index, received {key_type}")


@compute_node(overloads=tsb_get_item, resolvers={TIME_SERIES_TYPE: lambda mapping, scalars: mapping[TS_SCHEMA][scalars['key']]})
def tsb_get_item_by_name(tsb: REF[TSB[TS_SCHEMA]], key: str, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:
    """
    Return a reference to an item in the TSB referenced, by its name
    """
    if tsb.value.valid:
        if tsb.value.has_peer:
            return PythonTimeSeriesReference(tsb.value.output[key])
        else:
            item = tsb.value.items[_schema._schema_index_of(key)]
            return item if isinstance(item, TimeSeriesReference) else PythonTimeSeriesReference(item)
    else:
        return PythonTimeSeriesReference()


@compute_node(overloads=tsb_get_item, resolvers={TIME_SERIES_TYPE: lambda mapping, scalars: mapping[TS_SCHEMA][scalars['key']]})
def tsb_get_item_by_index(tsb: REF[TSB[TS_SCHEMA]], key: int, _schema: Type[TS_SCHEMA] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:
    """
    Return a reference to an item in the TSB referenced, by its name
    """
    if tsb.value.valid:
        if tsb.value.has_peer:
            return PythonTimeSeriesReference(tsb.value.output[key])
        else:
            return PythonTimeSeriesReference(tsb.value[key])
    else:
        return PythonTimeSeriesReference()
