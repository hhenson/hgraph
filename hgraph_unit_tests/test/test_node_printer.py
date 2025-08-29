import re

from hgraph import not_
from hgraph.test import eval_node


def test_node_printer(capsys):
    eval_node(not_, [True])
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    expected = iter([
        "Starting Graph",
        "not_.+Started node",
        "Started Graph",
        "Eval Start",
        "not_.+True\\[IN\\]",
        "not_.+False\\[OUT\\]",
        "Eval Done",
        "Graph Stopping",
        "not_.+Stopped nodeGraph Stopped",
    ])
    expected_next = re.compile(next(expected))
    for line in expected:
        if expected_next.search(line) is not None:
            expected_next = next(expected)
    try:
        missing = next(expected)
        assert False, f"Last item not found: {missing}"
    except StopIteration:
        pass
