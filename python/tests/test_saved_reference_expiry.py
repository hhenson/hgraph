"""A saved REF to a forwarding output in a switch_ branch (hgraph#1938).

Each switch_ branch returns a REF to its reduce output, which forwards to the
combiner tree. The saved REF is overwritten after switch_ has retired the
branch that owned the first target. The from-REF link must have dropped its
handle to that output when switch_ invalidated it, both when the branch is
only retired (0 -> 1) and when its slot is reused (0 -> 1 -> 0).
"""

import hgraph as hg
from hgraph.test import eval_node

Book = hg.TSD[int, hg.TS[int]]


@hg.compute_node(active=("choice",), valid=("choice",))
def _select_ref(choice: hg.TS[int], selected: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
    return selected.value


@hg.compute_node(active=("save",), valid=("save",))
def _save_ref(save: hg.TS[bool], selected: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
    if save.value:
        return selected.value


@hg.compute_node(active=("clock",), valid=("clock",))
def _saved_value(clock: hg.TS[int], saved: hg.TS[int]) -> hg.TS[int]:
    return saved.value if saved.valid else -1


@hg.graph
def _left(a: Book, b: Book) -> hg.REF[hg.TS[int]]:
    return _select_ref(hg.const(0), hg.reduce(hg.add_, a, 0))


@hg.graph
def _right(a: Book, b: Book) -> hg.REF[hg.TS[int]]:
    return _select_ref(hg.const(0), hg.reduce(hg.add_, b, 0))


@hg.graph
def _saved_reduce_ref(select: hg.TS[int], a: Book, b: Book, save: hg.TS[bool], clock: hg.TS[int]) -> hg.TS[int]:
    selected = hg.switch_(select, {0: _left, 1: _right}, a, b)
    return _saved_value(clock, _save_ref(save, selected))


def test_saved_ref_to_reduce_output_survives_branch_retirement_and_overwrite():
    # The stopped branch's sum stays readable until switch_ expires it.
    assert eval_node(
        _saved_reduce_ref,
        [0, 1, None, None, None],
        [{1: 10}],
        [{2: 20, 3: 30}],
        [True, False, False, True, False],
        [0, 1, 2, 3, 4],
    ) == [10, 10, -1, 50, 50]


def test_saved_ref_to_reduce_output_survives_branch_slot_reuse_and_overwrite():
    # Switching 0 -> 1 -> 0 rebuilds branch 0 in the slot the saved output
    # lived in; the overwrite at 3 names the rebuilt branch's output.
    assert eval_node(
        _saved_reduce_ref,
        [0, 1, 0, None, None],
        [{1: 10}, None, None, None, {4: 40}],
        [{2: 20, 3: 30}],
        [True, False, False, True, False],
        [0, 1, 2, 3, 4],
    ) == [10, 10, -1, 10, 50]
