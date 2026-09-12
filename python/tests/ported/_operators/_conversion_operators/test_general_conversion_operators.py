from hgraph import graph, TS, TSS, convert, str_
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


def test_str_of_a_bool_uses_the_python_spelling():
    """``str_`` is the user-facing spelling, so a Python reader gets Python's.

    The diagnostic rendering keeps the C++ ``true``/``false``, and JSON writes
    its own lowercase literals, so neither is disturbed (issue #819).
    """

    @graph
    def g(ts: TS[bool]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [True, False]) == ["True", "False"]


def test_a_string_is_quoted_inside_a_container_and_bare_on_its_own():
    """Python's rule, and the one worth having: ``str()`` at the top level and
    ``repr()`` inside a container.

    A bare element is ambiguous -- ``{a}`` could be a name or the text ``a`` --
    and quoting says which.
    """

    @graph
    def bare(ts: TS[str]) -> TS[str]:
        return str_(ts)

    @graph
    def in_a_set(ts: TSS[str]) -> TS[str]:
        return str_(ts)

    @graph
    def in_a_dict(ts: TS[dict[str, str]]) -> TS[str]:
        return str_(ts)

    assert eval_node(bare, ["a"]) == ["a"]
    assert eval_node(in_a_set, [{"a"}]) == ["{'a'}"]
    assert eval_node(in_a_dict, [{"a": "b"}]) == ["{'a': 'b'}"]

    # The quote follows Python's choice: single unless the text contains one
    # and no double, so an apostrophe stays readable.
    assert eval_node(in_a_set, [{"it's"}]) == ['{"it\'s"}']


def test_a_tuple_quotes_its_strings_but_keeps_our_brackets():
    """NOT a parity test for the brackets.

    The quoting matches released hgraph; the BRACKETS do not -- upstream
    writes ``('a', 'b')`` where this renders ``['a', 'b']``, and a one-element
    tuple gets no trailing comma. That difference is still open on issue #819
    and is deliberately not changed here, so this pins what we actually do
    rather than leaving it unasserted.
    """

    @graph
    def g(ts: TS[tuple[str, ...]]) -> TS[str]:
        return str_(ts)

    rendered = eval_node(g, [("a", "b")])[0]
    assert "'a'" in rendered and "'b'" in rendered   # the quoting: parity-true
    assert rendered == "['a', 'b']"                  # the brackets: ours
