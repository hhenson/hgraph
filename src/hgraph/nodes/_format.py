import re

from hgraph import TS, TS_SCHEMA, TSB, AUTO_RESOLVE, compute_node, STATE, ts_schema, WiringPort, HgTypeMetaData
from hgraph.nodes import const

__all__ = ("format_", "format_tsb")


def format_(format_str: TS[str] | str, *args, __sample__: int = -1, **kwargs) -> TS[str]:
    """
    Writes the contents of the time-series values provided (in args / kwargs) to a string using the format string
    provided. The kwargs will be used as named inputs to the format string and args as enumerated args.
    :param format_str: A standard python format string (using {}). When converted to C++ this will use the c++ fmt
                       specifications.
    :param __sample__: set this to a positive value > 1 to produce a sampled formatted string.
    :param args: Time series args
    :param kwargs: Time series kwargs
    :return:
    """
    schema_defn = {f"_{ndx}": value.output_type if isinstance(value, WiringPort) else HgTypeMetaData.parse_type(value) for
                   ndx, value in enumerate(args)}
    schema_defn |= {k: v.output_type if isinstance(v, WiringPort) else HgTypeMetaData.parse_type(v) for k, v in kwargs.items()}
    kwargs_ = {f"_{ndx}": value if isinstance(value, WiringPort) else const(value) for ndx, value in enumerate(args)}
    kwargs_ |= {k: v if isinstance(v, WiringPort) else const(v) for k, v in kwargs.items()}
    schema_ = ts_schema(**schema_defn)
    args_ = TSB[schema_].from_ts(**kwargs_)
    return format_tsb(format_str, args_, __sample__)


_ARG_PATTERN = re.compile(r"_([0-9]+)")


@compute_node
def format_tsb(format_str: TS[str], args: TSB[TS_SCHEMA], sample: int = -1, _schema: type[TS_SCHEMA] = AUTO_RESOLVE, _state: STATE = None) \
        -> TS[str]:
    """Format the supplied string pattern with the input args"""
    _state.count += 1
    if sample < 2 or _state.count % sample == 0:
        args_ = [args[k].value for k in _state.args]
        kwargs_ = {k: args[k].value for k in _state.kwargs}
        return format_str.value.format(*args_, **kwargs_)


@format_tsb.start
def format_tsb_start(_schema: type[TS_SCHEMA], _state: STATE):
    """Pre-parse structure to speed up processing"""
    _state.count = 0
    _state.args = tuple(k for k in _schema._schema_keys() if _ARG_PATTERN.search(k) is not None)
    _state.kwargs = tuple(k for k in _schema._schema_keys() if _ARG_PATTERN.search(k) is None)
