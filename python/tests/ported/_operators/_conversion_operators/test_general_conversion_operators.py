from dataclasses import dataclass

from hgraph import CompoundScalar, graph, TS, TSS, convert, str_
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


def test_a_tuple_renders_with_round_brackets():
    """A tuple reads back as a tuple, so it has to look like one.

    Square brackets said "list", and a one-element tuple needs the trailing
    comma that tells ``(1,)`` apart from a parenthesised ``1``. Verified
    against released hgraph 0.5.41, which answers each of these exactly.

    Only the variadic instantiation changes: our list storage also backs a
    plain list and a shaped array, and those still read back as ``[...]``.
    """

    @graph
    def variadic(ts: TS[tuple[int, ...]]) -> TS[str]:
        return str_(ts)

    @graph
    def of_strings(ts: TS[tuple[str, ...]]) -> TS[str]:
        return str_(ts)

    @graph
    def fixed(ts: TS[tuple[int, str]]) -> TS[str]:
        return str_(ts)

    @graph
    def nested(ts: TS[tuple[tuple[int, ...], ...]]) -> TS[str]:
        return str_(ts)

    assert eval_node(variadic, [(1, 2)]) == ["(1, 2)"]
    assert eval_node(variadic, [(1,)]) == ["(1,)"]
    assert eval_node(variadic, [()]) == ["()"]
    assert eval_node(of_strings, [("a", "b")]) == ["('a', 'b')"]

    # A FIXED tuple goes through the composite formatter, which already had
    # the brackets but rendered its string field bare.
    assert eval_node(fixed, [(1, "a")]) == ["(1, 'a')"]

    assert eval_node(nested, [((1,), (2, 3))]) == ["((1,), (2, 3))"]


def test_sets_and_dicts_keep_their_own_brackets():
    """The guard for the above: only the tuple spelling moved."""

    @graph
    def a_set(ts: TS[frozenset[int]]) -> TS[str]:
        return str_(ts)

    @graph
    def a_dict(ts: TS[dict[str, int]]) -> TS[str]:
        return str_(ts)

    @graph
    def a_tuple_in_a_dict(ts: TS[dict[str, tuple[int, ...]]]) -> TS[str]:
        return str_(ts)

    assert eval_node(a_set, [frozenset({1, 2})]) == ["{1, 2}"]
    assert eval_node(a_dict, [{"a": 1}]) == ["{'a': 1}"]
    assert eval_node(a_tuple_in_a_dict, [{"a": (1,)}]) == ["{'a': (1,)}"]


@dataclass
class _Pair(CompoundScalar):
    a: int
    b: str


def test_a_named_compound_scalar_renders_with_its_short_name():
    """A named CompoundScalar has a type, and the type is worth saying.

    ``{a: 1, b: 'x'}`` only says the value has those fields; ``_Pair(a=1,
    b='x')`` says what it is. That is the same distinction the type system
    draws between a named bundle and the un-named schema it wraps
    (``ValueTypeMetaData::is_named_bundle``), so the rendering follows it: a
    short name, round brackets and ``=`` separators, matching released
    hgraph 0.5.41 exactly.

    Note the registry label is qualified (``__main__::_Pair``); the SHORT name
    is what a repr uses.
    """

    @graph
    def g(ts: TS[_Pair]) -> TS[str]:
        return str_(ts)

    assert eval_node(g, [_Pair(a=1, b="x")]) == ["_Pair(a=1, b='x')"]
