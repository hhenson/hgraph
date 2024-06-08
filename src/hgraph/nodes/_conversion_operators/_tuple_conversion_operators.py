from typing import Tuple, Type

from hgraph import compute_node, combine, TSL, TIME_SERIES_TYPE, SIZE, TS, SCALAR, DEFAULT, OUT, collect, TS_OUT, \
    SIGNAL, OUT_1, HgTypeMetaData

__all__ = ()


@compute_node(overloads=combine,
              requires=lambda m, s: m[OUT].py_type == TS[Tuple],
              all_valid=lambda m, s: ('tsl',) if s['__strict__'] else None)
def combine_tuple_generic(*tsl: TSL[TS[SCALAR], SIZE], __strict__: bool = True) -> TS[Tuple[SCALAR, ...]]:
    return tuple(v.value for v in tsl)


@compute_node(overloads=combine,
              requires=lambda m, s: HgTypeMetaData.parse_type(TS[Tuple[m[SCALAR], ...]]).matches(m[OUT]),
              all_valid=lambda m, s: ('tsl',) if s['__strict__'] else None)
def combine_tuple_specific(*tsl: TSL[TS[SCALAR], SIZE], __strict__: bool = True) -> OUT:
    return tuple(v.value for v in tsl)


@compute_node(overloads=collect,
              requires=lambda m, s: m[OUT].py_type == TS[Tuple] or m[OUT].matches_type(TS[Tuple[m[SCALAR], ...]]),
              valid=('ts',)
              )
def collect_tuple(ts: TS[SCALAR], *, reset: SIGNAL = None, tp_: Type[OUT] = DEFAULT[OUT],
                  _output: TS_OUT[Tuple[SCALAR, ...]] = None) -> TS[Tuple[SCALAR, ...]]:
    prev_value = _output.value if _output.valid and not reset.modified else ()
    new_value = (ts.value,) if ts.modified else ()
    return prev_value + new_value
