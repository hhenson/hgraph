"""Public Python wiring regressions for fixed parity issues #69, #70, #72, #74,
#82, #148/#161/#162 (overlapping set deltas are rejected), and #149 (contains
seeds False).

Each test pins the released-hgraph trace the differential harness verified;
the corpus retains the minimized recipes as passing regressions.
"""

import pytest

import hgraph as hg
from hgraph import TS, REF, compute_node, graph
from hgraph.test import eval_node


def test_float_dedup_applies_default_tolerance():
    # Issue #69: upstream's float dedup overload defaults abs_tol=1e-15;
    # a sub-tolerance change from the last emitted value does not tick.
    @graph
    def dedup_product(lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return hg.dedup(rhs * lhs)

    out = eval_node(dedup_product, [0.0, 1.0], [9.395605309808467e-37, None])
    assert out == [0.0, None]

    # The explicit-tolerance arity is unchanged.
    @graph
    def dedup_tol(v: TS[float]) -> TS[float]:
        return hg.dedup(v, 0.5)

    assert eval_node(dedup_tol, [1.0, 1.4, 2.0]) == [1.0, None, 2.0]


def test_dedup_int_const_stays_int():
    # Issue #74: an int scalar auto-const selects the generic dedup overload;
    # it must not coerce into the float-tolerance overload (a regression from
    # the #69 fix making that overload single-arg callable).
    @graph
    def dedup_const(lhs: TS[int], rhs: TS[int]) -> TS[int]:
        return hg.dedup(2)

    out = eval_node(dedup_const, [0], [None])
    assert out == [2]
    assert all(type(v) is int for v in out)


def test_timedelta_and_datetime_accessors():
    # Issue #82: the upstream getattr_ tables for timedelta/datetime/time.
    from datetime import date, datetime, time, timedelta

    @graph
    def day_count(a: TS[date], b: TS[date]) -> TS[int]:
        return (a - b).days

    assert eval_node(day_count, [date(2026, 7, 27)], [date(2026, 7, 20)]) == [7]

    @graph
    def td_parts(td: TS[timedelta]) -> TS[int]:
        return td.days * 1_000_000_000_000 + td.seconds * 1_000_000 + td.microseconds

    # Python normalization: -1 day + 1s + 5us → days=-1, seconds=1, microseconds=5.
    negative = timedelta(days=-1, seconds=1, microseconds=5)
    assert eval_node(td_parts, [negative]) == [-1 * 1_000_000_000_000 + 1 * 1_000_000 + 5]

    @graph
    def td_total(td: TS[timedelta]) -> TS[float]:
        return td.total_seconds()

    assert eval_node(td_total, [timedelta(days=1, seconds=30)]) == [86430.0]

    @graph
    def dt_parts(dt: TS[datetime]) -> TS[int]:
        return (dt.year * 10_000 + dt.month * 100 + dt.day) * 1_000_000 + (
            dt.hour * 10_000 + dt.minute * 100 + dt.second)

    stamp = datetime(2026, 7, 27, 13, 5, 9, 123456)
    assert eval_node(dt_parts, [stamp]) == [20260727_130509]

    @graph
    def dt_micro_weekday(dt: TS[datetime]) -> TS[int]:
        return dt.microsecond + dt.weekday() * 10_000_000 + dt.isoweekday() * 100_000_000

    # 2026-07-27 is a Monday: weekday()=0, isoweekday()=1.
    assert eval_node(dt_micro_weekday, [stamp]) == [123456 + 0 + 100_000_000]

    @graph
    def time_parts(t: TS[time]) -> TS[int]:
        return t.hour * 10_000 + t.minute * 100 + t.second

    assert eval_node(time_parts, [time(13, 5, 9)]) == [130509]

    # timestamp(): fractional UTC epoch seconds (TS[float]) — the recorded
    # deviation from upstream's local-tz-dependent naive timestamp.
    from datetime import timezone

    @graph
    def dt_timestamp(dt: TS[datetime]) -> TS[float]:
        return dt.timestamp()

    half_second = datetime(1970, 1, 1, 0, 0, 0, 500000)
    assert eval_node(dt_timestamp, [half_second]) == [0.5]
    assert eval_node(dt_timestamp, [stamp]) == [
        stamp.replace(tzinfo=timezone.utc).timestamp()]


def _selection(choose_minimum, lhs, rhs):
    return hg.if_then_else(choose_minimum, hg.min_(lhs, rhs), hg.max_(lhs, rhs))


def test_format_renders_ref_arguments_dereferenced():
    # Issue #72: a REF-valued format argument renders its referenced VALUE,
    # never the reference object.
    @graph
    def formatted(lhs: TS[int], rhs: TS[int], choose_minimum: TS[bool]) -> TS[str]:
        return hg.format_("{}:{}", _selection(choose_minimum, lhs, rhs), lhs % rhs)

    assert eval_node(formatted, [8], [-6], [True]) == ["-6:-4"]


def test_valid_over_silent_ref_produces_no_tick():
    # Issue #70: valid over a REF-valued source that never ticks produces NO
    # output (upstream's valid_impl requires the REF input valid); a plain
    # statically-referenced source still ticks False at start.
    @graph
    def selection_valid(lhs: TS[int], rhs: TS[int], choose_minimum: TS[bool]) -> TS[bool]:
        return hg.valid(_selection(choose_minimum, lhs, rhs))

    assert eval_node(selection_valid, [None], [None], [None]) is None
    assert eval_node(selection_valid, [8], [-6], [True]) == [True]

    @compute_node
    def never_ref(i: TS[int]) -> REF[TS[int]]:
        return None

    @graph
    def never_valid(i: TS[int]) -> TS[bool]:
        return hg.valid(never_ref(i))

    assert eval_node(never_valid, [None]) is None

    # Plain (non-REF-valued) sources keep the start False tick.
    @graph
    def plain_valid(i: TS[int]) -> TS[bool]:
        return hg.valid(i)

    assert eval_node(plain_valid, [None, 1]) == [False, True]


def test_set_delta_added_removed_must_be_disjoint():
    # Ruling 2026-07-28 (issues #148/#161/#162): an element listed in BOTH
    # added and removed of one set delta is incorrect data — rejected at
    # construction, never resolved by convention. (Released hgraph happens
    # to tolerate the shape by filtering against prior membership; that is
    # an accepted deviation, roadmap.rst.)
    with pytest.raises(ValueError, match="add and remove the same element"):
        hg.set_delta(added={4, 0}, removed={0, -4}, tp=int)

    # The marker-set spelling reaches the same boundary.
    with pytest.raises(ValueError, match="add and remove the same element"):
        hg.set_delta(added={0}, removed={0}, tp=int)

    # Disjoint deltas construct and compose freely; composition preserves
    # disjointness (upstream's sequential-application formula NETS an
    # add-then-remove of the same element to no mention at all).
    first = hg.set_delta(added={1}, tp=int)
    second = hg.set_delta(added={2}, removed={1}, tp=int)
    combined = first + second
    assert combined.added == {2}
    assert combined.removed == frozenset()
    # A removal that predates the window survives composition.
    third = hg.set_delta(removed={9}, tp=int) + hg.set_delta(added={8}, tp=int)
    assert third.added == {8}
    assert third.removed == {9}


def test_contains_seeds_false_before_the_container_ticks():
    # Issue #149: upstream initializes the contains ref-output, so contains_
    # publishes False at start even when the container never becomes valid —
    # unlike len_, which stays silent (see test_len_sized_types).
    @graph
    def tsd_contains(ts: hg.TSD[str, TS[int]]) -> TS[bool]:
        return hg.contains_(ts, "a")

    assert eval_node(tsd_contains, [None, None]) == [False, None]

    @graph
    def tss_contains(ts: hg.TSS[int]) -> TS[bool]:
        return hg.contains_(ts, 1)

    assert eval_node(tss_contains, [None, None]) == [False, None]


def test_mesh_with_never_valid_keys_never_ticks():
    # Issues #128/#132/#151: a mesh_ whose __keys__ source never validates
    # must not touch-validate its owned output — upstream emits nothing.
    @graph
    def keyed(key: TS[str]) -> TS[int]:
        return hg.len_(key)

    @graph
    def g(keys: hg.TSS[str]) -> hg.TSD[str, TS[int]]:
        return hg.mesh_(keyed, __keys__=keys, __key_arg__="key")

    assert eval_node(g, [None, None]) is None


@pytest.mark.parametrize("use_mesh", [False, True], ids=["map", "mesh"])
def test_keyed_child_response_survives_new_key_in_delivery_cycle(use_mesh):
    # Issue #175: a keyed child scheduled by its own INTERNAL nodes (here a
    # request-reply response due for delivery) must evaluate even when an
    # outer input ticks in the same cycle. Map originally lost that wake-up;
    # mesh uses the same sparse child-schedule worklist.
    @hg.request_reply_service
    def adjust(path: str, request: TS[int]) -> TS[int]: ...

    @hg.service_impl(interfaces=adjust)
    def adjust_impl(request: hg.TSD[int, TS[int]]) -> hg.TSD[int, TS[int]]:
        return hg.map_(lambda value: value + 0, request)

    @graph
    def alpha_branch(value: TS[int]) -> TS[int]:
        return adjust("issue-175-svc", value)

    @graph
    def beta_branch(value: TS[int]) -> TS[int]:
        return value * 2

    @graph
    def per_key(key: TS[str], value: TS[int], selector: TS[str]) -> TS[int]:
        del key
        return hg.switch_(selector, {"alpha": alpha_branch, "beta": beta_branch}, value)

    @graph
    def g(values: hg.TSD[str, TS[int]], selector: TS[str]) -> hg.TSD[str, TS[int]]:
        hg.register_service("issue-175-svc", adjust_impl)
        keyed_operator = hg.mesh_ if use_mesh else hg.map_
        return keyed_operator(per_key, values, selector)

    # k2 arrives EXACTLY in k1's response-delivery cycle (t2).
    assert eval_node(g, [{"k1": 8}, None, {"k2": -2}], ["alpha", None, None]) == [
        {}, None, {"k1": 8}, None, {"k2": -2}]
