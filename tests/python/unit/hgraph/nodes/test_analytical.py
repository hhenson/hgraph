from hgraph.nodes._analytical import ewma, center_of_mass_to_alpha, span_top_aplha
from hgraph.test import eval_node


def test_ewma():
    assert eval_node(ewma,
                     [1.0, 2.0, 3.0, 4.0, 3.0, 2.0, 1.0],
                     0.5) == [1.0, 1.5, 2.25, 3.125, 3.0625, 2.53125, 1.765625]


def test_conversions():
    assert center_of_mass_to_alpha(1.0) == 0.5
    assert span_top_aplha(1.0) == 1.0
