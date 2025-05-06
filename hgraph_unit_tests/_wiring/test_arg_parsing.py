import pytest

from hgraph import compute_node, TS, extract_kwargs


def test_extract_kwargs_extra_args():
    @compute_node
    def n(a: TS[int], b: TS[int]) -> TS[int]:
        return a + b

    with pytest.raises(SyntaxError):
        extract_kwargs(n.signature, "a", "b", "c")


def test_extract_kwargs_extra_kwargs():
    @compute_node
    def n(a: TS[int], b: TS[int]) -> TS[int]:
        return a + b

    with pytest.raises(SyntaxError):
        extract_kwargs(n.signature, a="a", b="b", c="c")
