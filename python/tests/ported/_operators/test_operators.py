from dataclasses import dataclass

from hgraph import take, cast_, graph, CompoundScalar, TS, setattr_
from hgraph.test import eval_node





def test_cast():
    expected = [1.0, 2.0, 3.0]

    assert eval_node(cast_, float, [1, 2, 3]) == expected


def test_take():
    assert eval_node(take, [1, 2, 3, 4, 5], 3) == [1, 2, 3, None, None]


def test_setattr():

    @dataclass
    class Simple(CompoundScalar):
        a: int
        b: str

    @graph
    def g(ts: TS[Simple], p: TS[int]) -> TS[Simple]:
        return setattr_(ts, "a", p)

    assert eval_node(g, [Simple(1, "a")], [2]) == [Simple(2, "a")]

def test_setattr_on_a_frozen_compound_scalar_is_copy_on_write():
    """``setattr_`` produces a NEW value; it does not mutate its input.

    Released hgraph raises ``cannot assign to field`` for a frozen
    CompoundScalar. Copy-on-write is the right answer for an immutable type --
    the same thing ``dataclasses.replace`` does -- so this runtime differs
    deliberately (issue #810 item 5.3).

    The point of the test is the SEMANTICS, not the difference. Nothing else
    pins that the source is left alone, so an implementation that mutated in
    place would satisfy every other assertion in the suite while quietly
    changing a value other consumers still hold.
    """
    from hgraph import TSL, Size

    @dataclass(frozen=True)
    class Frozen(CompoundScalar):
        a: int
        b: str = "keep"

    @graph
    def g(ts: TS[Frozen], p: TS[int]) -> TSL[TS[Frozen], Size[2]]:
        return TSL.from_ts(setattr_(ts, "a", p), ts)

    original = Frozen(a=1)
    (out,) = eval_node(g, [original], [5])

    # The result carries the change and every untouched field.
    assert out[0] == Frozen(a=5, b="keep")
    # The source, read in the same graph, is unchanged.
    assert out[1] == Frozen(a=1, b="keep")
    # And so is the object handed in.
    assert original == Frozen(a=1, b="keep")
