from hgraph import graph, TS, convert, str_
from hgraph.test import eval_node




def test_convert_noop():
    @graph
    def g(i: TS[int]) -> TS[int]:
        j = convert[TS[int]](i)
        assert j is i
        return j

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


def test_str_of_a_float_reads_back_exactly():
    """``str_`` of a float must not lose precision.

    The value layer rendered doubles through a stream, whose default is six
    significant figures, so ``1/3`` became ``"0.333333"`` -- every string built
    from a double was silently truncated and could not be read back (issue
    #831). Each expected value below is released hgraph 0.5.41's own output.
    """

    @graph
    def g(ts: TS[float]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [1 / 3]) == ["0.3333333333333333"]
    assert eval_node(g, [0.1 + 0.2]) == ["0.30000000000000004"]
    assert eval_node(g, [2.5, 1e20, 1e-7]) == ["2.5", "1e+20", "1e-07"]

    # The property the issue is about: the text parses back to the same value.
    for value in (1 / 3, 0.1 + 0.2, 2.5, 1e20, 1e-7, 3.14159265358979):
        assert float(eval_node(g, [value])[0]) == value
