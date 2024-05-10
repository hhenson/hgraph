from hgraph import TS, MIN_TD
from hgraph.nodes import sample, signal
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


def test_signal():
    # TODO - test the times that the signals tick at
    assert eval_node(signal, delay=MIN_TD, max_ticks=4, initial_delay=False) == [True, True, True, True]

    # The generator node seems to tick out None for the time every engine cycle where there is no generator value
    assert eval_node(signal, delay=MIN_TD, max_ticks=4, initial_delay=True) == [None, True, True, True, True]

    assert eval_node(signal, delay=MIN_TD, max_ticks=1, initial_delay=False) == [True]
