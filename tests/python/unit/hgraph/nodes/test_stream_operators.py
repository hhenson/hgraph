from hgraph import TS
from hgraph.nodes import sample
from hgraph.test import eval_node


def test_sample():
    expected = [
        None,
        2,
        None,
        4,
        None
    ]

    assert eval_node(sample, [None, True, None, True], [1, 2, 3, 4, 5],
                     resolution_dict={'signal': TS[bool]}) == expected
