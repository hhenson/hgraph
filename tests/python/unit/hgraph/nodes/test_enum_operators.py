from datetime import timedelta, date, datetime
from enum import Enum
from typing import Tuple

import pytest
from frozendict import frozendict

from hgraph import WiringError, add_, sub_, mul_, lshift_, rshift_, bit_and, bit_or, bit_xor, eq_, neg_, pos_, invert_, \
    abs_, TS, len_, and_, or_, min_, max_, graph, str_
from hgraph._operators._operators import sum_
from hgraph.nodes import ENUM
from hgraph.test import eval_node


class TestEnum(Enum):
    A = 1
    B = 2


def test_eq_enums():
    @graph
    def app(lhs: TS[ENUM], rhs: TS[ENUM]) -> TS[bool]:
        return lhs == rhs

    assert eval_node(app, [TestEnum.A, None], [TestEnum.B, TestEnum.A]) == [False, True]
