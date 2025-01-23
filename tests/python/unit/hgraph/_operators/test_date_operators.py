from datetime import date

from hgraph import explode
from hgraph.test import eval_node


def test_explode():
    assert eval_node(
        explode,
          [
              date(2024, 1, 1),
              date(2024, 1, 2),
              date(2024, 2, 2),
              date(2025, 2, 2)
          ]
    ) == [{0: 2024, 1: 1, 2: 1}, {2: 2}, {1: 2}, {0: 2025}]
