"""Arrow-native dataframe adaptors.

The public source API follows Python hgraph, but graph execution is delegated
to the native ``from_data_frame`` and conversion operators.  Polars is an
optional producer only: values are converted to Arrow at the boundary.

The durable record/replay surface (``DataFrameStorage`` et al.) is provided
by the optional ``hgraph-persistence`` distribution (RFC 0025): the released
import paths here stay valid, and the pointed install error fires when a
durable name is USED, never when this package is imported — consumers of the
data-frame sources and operators alone keep working without the extension
(the ``hgraph.adaptors.tornado`` precedent).
"""

import sys as _sys

from ._to_frame_converters import register_to_frame_converters  # noqa: F401 — import registers the frame convert targets

_EXPORTS = {
    "_data_frame_source": (
        "DataFrameSource",
        "ArrowDataFrameSource",
        "PolarsDataFrameSource",
        "SqlDataFrameSource",
        "DataStore",
        "DataConnectionStore",
        "DATA_FRAME_SOURCE",
    ),
    "_data_source_generators": (
        "schema_from_frame",
        "tsb_from_data_source",
        "tsd_k_v_from_data_source",
        "tsd_k_b_from_data_source",
        "tsd_k_tsd_from_data_source",
        "ts_of_array_from_data_source",
        "tsd_k_a_from_data_source",
        "tsl_from_data_source",
        "ts_of_matrix_from_data_source",
    ),
    "_data_frame_operators": (
        "join",
        "filter_frame",
        "filter_cs",
        "filter_exp",
        "filter_exp_seq",
        "group_by",
        "ungroup",
        "sorted_",
        "concat",
        "with_columns",
    ),
    "_data_frame_record_replay": (
        "DATA_FRAME_RECORD_REPLAY",
        "set_data_frame_record_path",
        "set_data_frame_overrides",
        "replay_data_frame",
        "WriteMode",
        "DataFrameStorage",
        "BaseDataFrameStorage",
        "FileBasedDataFrameStorage",
        "MemoryDataFrameStorage",
    ),
}

_NAME_TO_MODULE = {
    name: module for module, names in _EXPORTS.items() for name in names
}

# The released public inventory, unchanged (surface-parity pinned); the
# extra module-level names in _EXPORTS remain importable but unadvertised.
__all__ = [
    "join",
    "filter_frame",
    "filter_cs",
    "filter_exp",
    "filter_exp_seq",
    "group_by",
    "ungroup",
    "sorted_",
    "concat",
    "with_columns",
    "DATA_FRAME_RECORD_REPLAY",
    "set_data_frame_record_path",
    "set_data_frame_overrides",
    "replay_data_frame",
    "WriteMode",
    "DataFrameStorage",
    "BaseDataFrameStorage",
    "FileBasedDataFrameStorage",
    "MemoryDataFrameStorage",
    "DataFrameSource",
    "DataStore",
    "DATA_FRAME_SOURCE",
    "DataConnectionStore",
    "SqlDataFrameSource",
    "PolarsDataFrameSource",
    "ArrowDataFrameSource",
    "tsb_from_data_source",
    "tsd_k_a_from_data_source",
    "ts_of_matrix_from_data_source",
    "tsd_k_v_from_data_source",
    "tsd_k_b_from_data_source",
    "tsd_k_tsd_from_data_source",
    "ts_of_array_from_data_source",
    "tsl_from_data_source",
    "schema_from_frame",
]


def __getattr__(name: str):
    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    module = import_module(f".{module_name}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_NAME_TO_MODULE))
