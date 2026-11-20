"""A REF reader observes designations. A value reader follows the selected route."""

import pytest

import hgraph as hg


@hg.compute_node(active=("command",), valid=("command",))
def select(command: hg.TS[str], a: hg.REF[hg.TS[int]], b: hg.REF[hg.TS[int]],
           _output: hg.TS_OUT[hg.REF[hg.TS[int]]] = None) -> hg.REF[hg.TS[int]]:
    if command.value == "clear":
        _output.invalidate()
    elif command.value == "empty":
        return hg.TimeSeriesReference.make()
    elif command.value == "write both":
        _output.value = a.value
        _output.value = b.value
    else:
        return a.value if command.value == "a" else b.value


@hg.compute_node(valid=())
def read(value: hg.TS[int]) -> hg.TS[str]:
    if value.valid:
        return str(value.value)
    assert not value.modified
    assert value.last_modified_time == hg.MIN_DT
    return "invalid"


@hg.graph
def follow(command: hg.TS[str], a: hg.TS[int], b: hg.TS[int]) -> hg.TS[str]:
    return read(select(command, a, b))


@pytest.mark.parametrize("withdraw", ["empty", "clear", "b"])
def test_losing_the_selected_value_wakes_once_and_detaches_the_old_target(withdraw):
    # b has never published. Once the input loses a, a's next tick stays silent.
    assert hg.eval_node(follow, ["a", withdraw, withdraw, None, "a"], [7, None, None, 8], []) == [
        "7", "invalid", None, None, "8",
    ]


def test_passive_follower_reads_withdrawal_only_when_the_clock_wakes_it():
    @hg.compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: hg.TS[int], value: hg.TS[int]) -> hg.TS[str]:
        if value.valid:
            return str(value.value)
        assert not value.modified
        assert value.last_modified_time == hg.MIN_DT
        return "invalid"

    @hg.graph
    def sampled(clock: hg.TS[int], command: hg.TS[str], a: hg.TS[int], b: hg.TS[int]) -> hg.TS[str]:
        return inspect(clock, select(command, a, b))

    # The first clear coincides with a probe. The second clear has no probe.
    assert hg.eval_node(sampled, [1, 1, 1, None, 1], ["a", "clear", "a", "clear"], [7], []) == [
        "7", "invalid", "7", None, "invalid",
    ]


def test_two_writes_leave_readers_on_the_final_target():
    assert hg.eval_node(follow, ["write both"], [1, 2, None], [10, None, 11]) == ["10", None, "11"]


Pair = hg.TSL[hg.TS[int], hg.Size[2]]
Positions = hg.TSL[hg.REF[hg.TS[int]], hg.Size[2]]


def test_withdrawing_the_last_child_clears_the_assembled_list_observations():
    @hg.compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: hg.TS[int], items: Pair) -> hg.TS[str]:
        if items.valid:
            return str(items[0].value)
        assert not items.modified
        assert items.last_modified_time == hg.MIN_DT
        assert items.delta_value is None
        return "invalid"

    @hg.graph
    def observe(clock: hg.TS[int], command: hg.TS[str], a: hg.TS[int], b: hg.TS[int]) -> hg.TS[str]:
        item = select(command, a, b)
        return inspect(clock, Positions.from_ts(item, item))

    assert hg.eval_node(observe, [1, 1, 1, 1], ["a", "clear", None, "a"], [7], []) == [
        "7", "invalid", "invalid", "7",
    ]


@hg.compute_node
def pair(value: hg.TS[int]) -> Pair:
    return value.value, -value.value


@hg.compute_node(active=("right",), valid=("right",))
def select_pair(right: hg.TS[bool], a: hg.REF[Pair], b: hg.REF[Pair]) -> hg.REF[Pair]:
    return b.value if right.value else a.value


@hg.compute_node(active=("index",), valid=("index",))
def select_position(index: hg.TS[int], items: Positions) -> hg.REF[hg.TS[int]]:
    return items[index.value].value


def test_saved_position_follows_the_list_after_the_selecting_node_goes_quiet():
    @hg.graph
    def routed(index: hg.TS[int], right: hg.TS[bool], a: hg.TS[int], b: hg.TS[int]) -> hg.TS[str]:
        return read(select_position(index, select_pair(right, pair(a), pair(b))))

    # Select position 1 once. The route follows the new list and ignores the old list.
    assert hg.eval_node(routed, [1], [True, None, False], [3, None, None, None, 4],
                        [10, None, None, 11]) == ["-10", None, "-3", None, "-4"]


@hg.compute_node
def count_designations(item: hg.REF[hg.TS[int]], _state: hg.STATE = None) -> hg.TS[int]:
    _state.count = getattr(_state, "count", 0) + 1
    return _state.count


@hg.graph
def count_first(items: Pair) -> hg.TS[int]:
    return count_designations(items[0])


@pytest.mark.parametrize("through_parameter", [False, True], ids=["direct", "graph-parameter"])
def test_position_reference_observes_repointing_and_ignores_value_ticks(through_parameter):
    @hg.graph
    def changes(right: hg.TS[bool], a: hg.TS[int], b: hg.TS[int]) -> hg.TS[int]:
        items = select_pair(right, pair(a), pair(b))
        return count_first(items) if through_parameter else count_designations(items[0])

    assert hg.eval_node(changes, [True, None, False, False], [3, 4, None, 5], [10, 11]) == [
        1, None, 2, None,
    ]
