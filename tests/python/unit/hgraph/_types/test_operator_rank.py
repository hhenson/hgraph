from typing import Tuple, Dict

import pytest

from hgraph import SCALAR, HgTypeMetaData, TIME_SERIES_TYPE, TS, TSD, V, K, TSL, SIZE, Size, TSS


def test_rank_values():
    assert HgTypeMetaData.parse_type(int).operator_rank == 1e-10
    assert HgTypeMetaData.parse_type(str).operator_rank == 1e-10

    # In this case we expect a hard-coded 1.0 value, so == over float is fine.
    assert HgTypeMetaData.parse_type(SCALAR).operator_rank == 1.  # NOSONAR
    assert HgTypeMetaData.parse_type(TIME_SERIES_TYPE).operator_rank == 1.  # NOSONAR


@pytest.mark.parametrize(('t1', 't2'),(
        (TS[int], TS[SCALAR]),
        (TS[Tuple[SCALAR]], TS[SCALAR]),

        (Tuple[SCALAR, int], Tuple[SCALAR, SCALAR]),
        (Tuple[SCALAR, Tuple[SCALAR]], Tuple[SCALAR, SCALAR]),

        (TSL[TS[int], SIZE], TSL[TIME_SERIES_TYPE, Size[2]]),
        (TSL[TS[int], Size[2]], TSL[TS[int], SIZE]),
        (TSL[TS[SCALAR], Size[2]], TSL[TS[SCALAR], SIZE]),
        (TSL[TS[Dict[int, SCALAR]], SIZE], TSL[TS[SCALAR], Size[2]]),

        (TSD[int, V], TSD[K, V]),
        (TSD[int, TS[SCALAR]], TSD[int, V]),

        (TSS[int], TSS[K])
))
def test_rank_order(t1, t2):
    print(f"{HgTypeMetaData.parse_type(t1).operator_rank} < {HgTypeMetaData.parse_type(t2).operator_rank}")
    assert HgTypeMetaData.parse_type(t1).operator_rank < HgTypeMetaData.parse_type(t2).operator_rank
