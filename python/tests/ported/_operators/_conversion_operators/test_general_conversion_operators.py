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


def test_str_of_an_integral_float_keeps_the_point():
    """A float whose value is integral must still look like a float.

    ``3`` says nothing about the type; ``3.0`` does, which is why Python
    writes the point. Measured across magnitudes from 5e-324 to 1.8e308, this
    was the only way the two spellings differed -- the shortest-round-trip
    form already agrees with Python on when to switch to exponent notation --
    so adding the point where it is missing makes the rendering match.
    """

    @graph
    def g(ts: TS[float]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [3.0, -7.0, 0.0, -0.0]) == ["3.0", "-7.0", "0.0", "-0.0"]
    assert eval_node(g, [1e15, 123456789.0]) == ["1000000000000000.0", "123456789.0"]

    # Exponent and non-finite spellings are untouched.
    assert eval_node(g, [1e16, 1e-7]) == ["1e+16", "1e-07"]
    assert eval_node(g, [float("inf"), float("-inf")]) == ["inf", "-inf"]
