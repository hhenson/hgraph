from typing import Tuple

from hgraph import compute_node, OUT, TS, combine, TSL, SCALAR, SIZE, TIME_SERIES_TYPE, TSD, REF


@compute_node(overloads=combine,
              requires=lambda m, s: ((m[OUT].py_type == TSD or m[OUT].matches_type(TSD[m[SCALAR], m[TIME_SERIES_TYPE]]))
                                     and (len(s['keys']) == m[SIZE].py_type.SIZE or f"Length of keys ({len(s['keys'])}) and values ({m[SIZE].py_type.SIZE}) does not match")),
              all_valid=lambda m, s: ('tsl',) if s['__strict__'] else None)
def combine_tsd_from_tuple_and_tsl(keys: Tuple[SCALAR, ...], *tsl: TSL[REF[TIME_SERIES_TYPE], SIZE], __strict__: bool = True) \
        -> TSD[SCALAR, REF[TIME_SERIES_TYPE]]:
    return {k: v.value for k, v in zip(keys, tsl)}


