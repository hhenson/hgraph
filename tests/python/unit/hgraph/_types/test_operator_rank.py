from typing import Tuple, Dict

import pytest

from hgraph import SCALAR, HgTypeMetaData, TIME_SERIES_TYPE, TS, TSL, SIZE, Size, TSD, V, K, TSS


def test_rank_values():
    assert HgTypeMetaData.parse(int).operator_rank == 0
    assert HgTypeMetaData.parse(str).operator_rank == 0

    # In this case we expect a hard-coded 1.0 value, so == over float is fine.
    assert HgTypeMetaData.parse(SCALAR).operator_rank == 1.  # NOSONAR
    assert HgTypeMetaData.parse(TIME_SERIES_TYPE).operator_rank == 1.  # NOSONAR


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

        (TSS[int], TSS[K]),
))
def test_rank_order(t1, t2):
    print(f"{HgTypeMetaData.parse(t1).operator_rank} < {HgTypeMetaData.parse(t2).operator_rank}")
    assert HgTypeMetaData.parse(t1).operator_rank < HgTypeMetaData.parse(t2).operator_rank