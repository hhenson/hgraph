"""A switch_ / dispatch branch passes the identity of what it returns through.

hgraph's ``switch_`` output is always a reference to what the selected branch
returned. A branch that returns its input (or a bundle rebuilt from its input's
fields) therefore publishes a reference EQUAL to the input's own reference, and
a consumer re-pointed between the switch and that input sees the same series:
it is not re-bound and does not tick. Copying the branch result into a
switch-owned value gave every passed-through field a new identity, so the
re-point re-ticked a stale value (``[0, 1]`` where released hgraph gives
``[0]``). Expected values below are the released hgraph (0.5) results.
"""
from hgraph import (
    TS,
    TSB,
    CompoundScalar,
    combine,
    compute_node,
    const,
    default,
    dispatch,
    graph,
    if_then_else,
    modified,
    nothing,
    switch_,
)
from hgraph.test import eval_node


class Event(CompoundScalar):
    pass


class Reject(Event):
    pass


class Accept(Event):
    pass


State = TSB['done': TS[bool], 'pending': TS[int], 'id': TS[int]]


@dispatch(on=('event',))
def apply(state: State, event: TS[Event]) -> State:
    return state


@graph(overloads=apply)
def reject(state: State, event: TS[Reject]) -> State:
    return state.copy_with(done=True)


@graph(overloads=apply)
def accept(state: State, event: TS[Accept]) -> State:
    return state.copy_with(pending=state.pending + 10)


@compute_node
def clear(pending: TS[int], event: TS[Event]) -> TS[int]:
    return 0 if event.modified else pending.value


@graph
def reselect_after_dispatch(event: TS[Event]) -> TS[int]:
    state = const({'pending': 1}, State)
    state = state.copy_with(id=default(nothing(TS[int]), -1))
    state = if_then_else(modified(event), apply(state, event), state)
    return clear(state.pending, event)


@graph
def reselect_after_dispatch_peered(event: TS[Event]) -> TS[int]:
    state = const({'pending': 1}, State)
    state = if_then_else(modified(event), apply(state, event), state)
    return clear(state.pending, event)


def test_a_cleared_value_stays_cleared_after_a_rebuilt_bundle_branch():
    assert eval_node(reselect_after_dispatch, [Reject()]) == [0]
    assert eval_node(reselect_after_dispatch_peered, [Reject()]) == [0]


def test_a_cleared_value_stays_cleared_after_a_pass_through_branch():
    assert eval_node(reselect_after_dispatch, [Event()]) == [0]


def test_a_branch_that_computes_a_field_still_re_ticks_it():
    # The accept branch publishes a NEW pending series, so re-pointing back
    # to the input's own pending is a real change and ticks its value.
    assert eval_node(reselect_after_dispatch, [Accept()]) == [0, 1]
    assert eval_node(
        reselect_after_dispatch, [Event(), Reject(), None, Accept(), None]
    ) == [0, 0, None, 0, 1]


@graph
def dispatch_state(event: TS[Event], pending: TS[int]) -> State:
    state = combine[State](done=const(False), pending=pending, id=const(7))
    return apply(state, event)


def test_a_branch_change_ticks_only_the_fields_whose_series_changed():
    assert eval_node(
        dispatch_state,
        [Event(), Reject(), None, Accept(), None, Event()],
        [1, None, 2, None, 3, 4],
    ) == [
        {'done': False, 'pending': 1, 'id': 7},
        {'done': True},
        {'pending': 2},
        {'done': False, 'pending': 12},
        {'pending': 13},
        {'pending': 4},
    ]


@graph
def _identity(ts: TS[int]) -> TS[int]:
    return ts


@graph
def _double(ts: TS[int]) -> TS[int]:
    return ts * 2


@graph
def reselect_after_switch(key: TS[str], ts: TS[int], event: TS[Event]) -> TS[int]:
    switched = switch_(key, {'id': _identity, 'double': _double}, ts)
    return clear(if_then_else(modified(event), switched, ts), event)


def test_a_switch_pass_through_branch_publishes_its_input_reference():
    assert eval_node(
        reselect_after_switch,
        ['id', None, 'double', 'id', None, None],
        [3, None, None, None, None, 7],
        [Event(), None, Event(), Event(), None, None],
    ) == [0, None, 0, 0, None, 7]
