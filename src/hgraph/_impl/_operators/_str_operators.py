from dataclasses import dataclass
from typing import Tuple

from hgraph import (
    compute_node,
    TS,
    TSB,
    TIME_SERIES_TYPE,
    str_,
    Match,
    match_,
    replace,
    split,
    OUT,
    DEFAULT,
    HgTupleCollectionScalarType,
    join,
    TSL,
    SIZE,
    format_,
    TS_SCHEMA,
    TS_SCHEMA_1,
    add_,
    graph,
    sub_,
    WiringError,
    mul_,
    STATE,
)

__all__ = tuple()


@compute_node(overloads=add_)
def add_str(lhs: TS[str], rhs: TS[str]) -> TS[str]:
    """
    Concatenates two strings.
    """
    return lhs.value + rhs.value


@graph(overloads=sub_)
def sub_strs(lhs: TS[str], rhs: TS[str]) -> TS[str]:
    raise WiringError("Cannot subtract one string from another")


@compute_node(overloads=mul_)
def mul_strs(lhs: TS[str], rhs: TS[int]) -> TS[str]:
    return lhs.value * rhs.value


@graph(overloads=str_)
def str_str(ts: TS[str]) -> TS[str]:
    return ts


@compute_node(overloads=str_)
def str_default(ts: TIME_SERIES_TYPE) -> TS[str]:
    """
    Returns the string representation of the time-series value.
    """
    return str(ts.value)


@compute_node(overloads=match_)
def match_default(pattern: TS[str], s: TS[str]) -> TSB[Match]:
    """
    Matches the pattern in the string and returns the groups.
    """
    import re

    m = re.match(pattern.value, s.value)
    if m:
        return {"is_match": True, "groups": m.groups()}
    else:
        return {"is_match": False}


@compute_node(overloads=replace)
def replace_default(pattern: TS[str], repl: TS[str], s: TS[str]) -> TS[str]:
    """
    Replaces the pattern in the string with the replacement.
    """
    import re

    return re.sub(pattern.value, repl.value, s.value)


def _check_sizes(mapping, scalars):
    from hgraph import HgTSLTypeMetaData, HgTupleFixedScalarType, HgTSTypeMetaData

    if out := mapping.get(OUT):
        if isinstance(out, HgTSLTypeMetaData):
            if (maxsplit := scalars["maxsplit"]) != -1:
                if maxsplit != out.size.SIZE:
                    return "The maxsplit should be equal to the size of the output time-series list"
            else:
                scalars["maxsplit"] = out.size.SIZE - 1

            return True if out.value_tp.matches_type(TS[str]) else "Output type of split should be TSL[TS[str], Size]"
        elif isinstance(out, HgTSTypeMetaData):
            out_scalar = out.value_scalar_tp
            if isinstance(out_scalar, HgTupleFixedScalarType):
                if (maxsplit := scalars["maxsplit"]) != -1:
                    if maxsplit != len(out_scalar.element_types):
                        return "The maxsplit should be equal to the size of the output time-series list"
                else:
                    scalars["maxsplit"] = len(out_scalar.element_types) - 1
                return (
                    True
                    if (e.matches_type(str) for e in out_scalar.element_types)
                    else "Output type of split should be TSL[TS[str], Size]"
                )
            elif isinstance(out_scalar, HgTupleCollectionScalarType):
                return (
                    True
                    if out_scalar.element_type.matches_type(str)
                    else "Output type of split should be TS[Tuple[str, ...]]"
                )

    return f"Output type of {out} is not supported for split"


@compute_node(overloads=split, requires=_check_sizes, resolvers={OUT: lambda m, s: TS[Tuple[str, ...]]})
def split_default(s: TS[str], separator: str, maxsplit: int = -1) -> DEFAULT[OUT]:
    """
    Splits the string over the separator into one of the given types:
     - TS[Tuple[str, ...]],
     - TS[Tuple[str, str]],
     - TSL[TS[str], SIZE]
    """
    return tuple(s.value.split(separator, maxsplit))


@compute_node(overloads=join)
def join_tsl(*strings: TSL[TS[str], SIZE], separator: str) -> TS[str]:
    return separator.join(s.value for s in strings.valid_values())


@compute_node(overloads=join)
def join_tuple(strings: TS[Tuple[str, ...]], separator: str) -> TS[str]:
    return separator.join(s for s in strings.value)


@dataclass
class FormatState:
    count = 0


@compute_node(overloads=format_, valid=())
def format_(
    fmt: TS[str],
    *__pos_args__: TSB[TS_SCHEMA],
    __sample__: int = -1,
    _state: STATE[FormatState] = None,
    **__kw_args__: TSB[TS_SCHEMA_1],
) -> TS[str]:
    if __sample__ > 1:
        _state.count += 1
        if _state.count % __sample__ != 0:
            return None

    return fmt.value.format(*(a.value for a in __pos_args__), **{k: v.value for k, v in __kw_args__.items()})
