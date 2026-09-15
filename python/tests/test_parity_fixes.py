"""Public Python wiring regressions for fixed parity issues #69, #70, #72, #74,
#82, #148/#161/#162 (overlapping set deltas are rejected), #149 (contains
seeds False), #570-#604 (a CompoundScalar field projection ticks with its
parent), #909-#916/#928/#936 (a converted dictionary entry ticks when its
value re-sends), and #818 items 2.4 and 2.7 (take by duration, and convert
into a nested dictionary).

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

    # The direct response leg advances both deliveries by one cycle; the
    # higher-order worklist must still keep each due child.
    assert eval_node(g, [{"k1": 8}, None, {"k2": -2}], ["alpha", None, None]) == [
        {}, {"k1": 8}, {}, {"k2": -2}]


def test_compound_scalar_field_projection_ticks_with_its_parent():
    """Issues #570-#604: 35 minimized recipes, one defect.

    ``getattr_`` over a ``TS[CompoundScalar]`` suppressed the tick whenever the
    projected field repeated its previous value, so a parent update that only
    changed a *sibling* field published nothing. ``TS[CompoundScalar]`` is one
    value stream rather than a bundle of independently ticking fields, so the
    projection follows its parent. Both the polymorphic field (declared type is
    a base with descendants) and the plain control shape are pinned here; the
    differential harness reported the same trace for every one of the 35 cases.
    """
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Series(hg.CompoundScalar):
        symbol: str

    @dataclass(frozen=True)
    class DerivedSeries(Series):
        underlying: Series

    @dataclass(frozen=True)
    class PlainInner(hg.CompoundScalar):
        symbol: str

    @dataclass(frozen=True)
    class HasPlainField(hg.CompoundScalar):
        symbol: str
        inner: PlainInner

    @compute_node
    def read_base(series: TS[Series]) -> TS[str]:
        return series.value.symbol

    @compute_node
    def read_plain(series: TS[PlainInner]) -> TS[str]:
        return series.value.symbol

    @graph
    def nested_field(series: TS[DerivedSeries]) -> TS[str]:
        return read_base(series.underlying)

    @graph
    def plain_field(series: TS[HasPlainField]) -> TS[str]:
        return read_plain(series.inner)

    # The projected field repeats while a sibling changes: both ticks publish.
    assert eval_node(nested_field, [
        DerivedSeries(symbol="BACK", underlying=Series(symbol="BOM")),
        DerivedSeries(symbol="BOM", underlying=Series(symbol="BOM")),
    ]) == ["BOM", "BOM"]

    assert eval_node(plain_field, [
        HasPlainField(symbol="BACK", inner=PlainInner(symbol="BOM")),
        HasPlainField(symbol="BOM", inner=PlainInner(symbol="BOM")),
    ]) == ["BOM", "BOM"]

    # A changing field was never affected; it stays correct.
    assert eval_node(plain_field, [
        HasPlainField(symbol="A", inner=PlainInner(symbol="X")),
        HasPlainField(symbol="B", inner=PlainInner(symbol="Y")),
    ]) == ["X", "Y"]

    # A cycle where the parent does not tick still publishes nothing.
    assert eval_node(plain_field, [
        HasPlainField(symbol="A", inner=PlainInner(symbol="X")),
        None,
        HasPlainField(symbol="A", inner=PlainInner(symbol="X")),
    ]) == ["X", None, "X"]


def test_compound_scalar_field_projection_default_ticks_with_its_parent():
    """The ``default`` arity of the same operator carries the same rule.

    Not reported by the harness -- the generated recipes never take a default
    -- but it shared the suppression, so it would have produced the identical
    dropped tick for anyone who did.
    """
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Quote(hg.CompoundScalar):
        symbol: str
        venue: str

    @graph
    def venue_or_default(quote: TS[Quote]) -> TS[str]:
        return hg.getattr_(quote, "venue", "UNKNOWN")

    assert eval_node(venue_or_default, [
        Quote(symbol="A", venue="LSE"),
        Quote(symbol="B", venue="LSE"),
    ]) == ["LSE", "LSE"]


def test_converted_dictionary_entry_ticks_when_its_value_re_sends():
    """Issues #909-#916, #928, #936: ten minimized recipes, one defect.

    ``convert[TSD[K, TS[V]]](key, value)`` copies the value into the entry,
    and the copy was skipped whenever it equalled what the entry already held.
    Released hgraph holds a ``REF`` to the value in every entry instead, so an
    entry ticks exactly when the referenced output does -- a re-send of the
    same value included. Anything reading the dictionary therefore stalled on
    the second send.
    """

    @graph
    def convert_pair(key: TS[str], value: TS[int]) -> hg.TSD[str, TS[int]]:
        return hg.convert[hg.TSD[str, TS[int]]](key, value)

    # The value re-sends what it already carries; the key stands still.
    assert eval_node(convert_pair, ["b", None], [19, 19]) == [{"b": 19}, {"b": 19}]

    # A changed value was never affected.
    assert eval_node(convert_pair, ["b", None], [19, 20]) == [{"b": 19}, {"b": 20}]

    # The key re-sending the key it already had is not news: the node ran, but
    # no entry moved. Released hgraph re-sets the same reference and elides it
    # for the same reason.
    assert eval_node(convert_pair, ["b", "b"], [19, None]) == [{"b": 19}, None]

    # A new key still removes the old one and carries the standing value.
    assert eval_node(convert_pair, ["b", "c"], [19, None]) == [
        {"b": 19},
        {"c": 19, "b": hg.REMOVE},
    ]


def test_converted_dictionary_entry_ticks_through_map_and_switch():
    """The composed shapes the harness actually minimized to.

    ``element_or_whole`` reads the conversion through two ``map_`` layers;
    ``branch_shape_equivalence`` reaches it through a ``switch_`` branch that
    either builds the dictionary directly or projects it through a per-key
    child graph. All three stalled on the re-sent value.
    """

    @graph
    def wrap(v: TS[int], k: TS[str]) -> hg.TSD[str, TS[int]]:
        return hg.convert[hg.TSD[str, TS[int]]](k, v)

    @graph
    def child(value: TS[int], nested: hg.TSD[str, hg.TIME_SERIES_TYPE]) -> TS[int]:
        return value + hg.len_(nested)

    @graph
    def element_or_whole(value: TS[int], key: TS[str]) -> hg.TSD[str, TS[int]]:
        inner = hg.convert[hg.TSD[str, TS[int]]](key, value)
        return hg.map_(child, inner, hg.map_(wrap, inner, key))

    assert eval_node(element_or_whole, [19, 19], ["b", None]) == [
        {"b": 20},
        {"b": 20},
    ]

    @graph
    def identity(a: TS[int]) -> TS[int]:
        return a

    @graph
    def direct(a: TS[int], b: TS[str]) -> hg.TSD[str, TS[int]]:
        return hg.convert[hg.TSD[str, TS[int]]](b, a)

    @graph
    def projected(a: TS[int], b: TS[str]) -> hg.TSD[str, TS[int]]:
        return hg.map_(identity, hg.convert[hg.TSD[str, TS[int]]](b, a))

    @graph
    def branches(
        selector: TS[str], value: TS[int], key: TS[str]
    ) -> hg.TSD[str, TS[int]]:
        return hg.switch_(
            selector, {"direct": direct, "projected": projected}, value, key
        )

    assert eval_node(branches, [None, "direct", None], [-5, None, -5], [None, "c", None]) == [
        None,
        {"c": -5},
        {"c": -5},
    ]
    assert eval_node(branches, ["projected", None], [-3, -3], ["b", None]) == [
        {"b": -3},
        {"b": -3},
    ]


def test_take_accepts_a_duration_as_well_as_a_count():
    """Issue #818 item 2.4: ``take(ts, timedelta)``.

    The count form worked; the duration form was rejected at wiring. The
    window opens at the SOURCE'S FIRST TICK rather than at graph start, and
    the source passivates once it moves beyond the span -- upstream's
    ``take_by_time``.
    """
    from datetime import timedelta

    @graph
    def by_time(ts: TS[int], span: int) -> TS[int]:
        return hg.take(ts, timedelta(microseconds=span))

    @graph
    def by_count(ts: TS[int], count: int) -> TS[int]:
        return hg.take(ts, count)

    @graph
    def dict_by_time(ts: hg.TSD[str, TS[int]], span: int) -> hg.TSD[str, TS[int]]:
        return hg.take(ts, timedelta(microseconds=span))

    assert eval_node(by_time, [1, 2, 3, 4, 5], 2) == [1, 2, 3, None, None]
    assert eval_node(by_time, [1, 2, 3], 0) == [1, None, None]

    # Any other shape forwards the delta, as the count form does.
    assert eval_node(
        dict_by_time, [{"a": 1}, {"b": 2}, {"c": 3}], 1
    ) == [{"a": 1}, {"b": 2}, None]

    # The count spelling is untouched.
    assert eval_node(by_count, [1, 2, 3], 2) == [1, 2, None]


def test_convert_may_build_a_nested_dictionary():
    """Issue #818 item 2.7: ``convert[TSD[K, TSD[...]]](key, inner)``.

    Released hgraph declares the conversion's value as
    ``REF[TIME_SERIES_TYPE]``, so any time series may be the entry. Requiring
    a leaf rejected the nested spelling at wiring, and the fuzzer draw that
    found it had to route around through ``map_``.
    """

    @graph
    def nested(
        key: TS[str], inner: hg.TSD[str, TS[int]]
    ) -> hg.TSD[str, hg.TSD[str, TS[int]]]:
        return hg.convert[hg.TSD[str, hg.TSD[str, TS[int]]]](key, inner)

    assert eval_node(nested, ["k"], [{"a": 1}]) == [{"k": {"a": 1}}]

    # A new key carries the standing inner dictionary and drops the old one.
    assert eval_node(nested, ["k", "j"], [{"a": 1}, None]) == [
        {"k": {"a": 1}},
        {"j": {"a": 1}, "k": hg.REMOVE},
    ]


def test_cast_parses_a_string_into_a_number():
    """Issue #818 item 2.5: ``cast_(int, ts)`` and ``cast_(float, ts)``.

    ``cast_`` lowers to ``convert``, and released hgraph spells the body
    ``tp(ts.value)``, so the accepted text is Python's. Only the parsing
    overload was missing -- an unparseable string already raised on both
    sides, and still does.
    """
    import pytest

    @graph
    def to_int(ts: TS[str]) -> TS[int]:
        return hg.cast_(int, ts)

    @graph
    def to_float(ts: TS[str]) -> TS[float]:
        return hg.cast_(float, ts)

    @graph
    def to_bool(ts: TS[str]) -> TS[bool]:
        return hg.cast_(bool, ts)

    # Surrounding whitespace and a sign are allowed; underscores only between
    # digits, as Python has them.
    assert eval_node(to_int, ["12", " 12 ", "-3", "+3", "1_000"]) == [
        12, 12, -3, 3, 1000
    ]
    assert eval_node(to_float, ["1.5", "1e3", "-2.5", ".5"]) == [
        1.5, 1000.0, -2.5, 0.5
    ]
    assert eval_node(to_float, ["inf", "-inf"]) == [
        float("inf"), float("-inf")
    ]

    # ``bool`` of a string is emptiness, as Python has it.
    assert eval_node(to_bool, ["x", ""]) == [True, False]

    # Everything Python rejects is still rejected: a float literal for int, a
    # hex literal, an empty string, and an underscore outside the digits.
    for text in ("1.5", "x", "", "0x10", "_1", "1_"):
        with pytest.raises(Exception):
            eval_node(to_int, [text])
    for text in ("x", "", "0x10"):
        with pytest.raises(Exception):
            eval_node(to_float, [text])


def test_the_named_set_operators_work_over_dictionaries():
    """Issue #818 item 2.3: ``union`` and friends over two TSDs.

    Released hgraph registers the whole named family over dictionaries as well
    as sets. Only the BITWISE spellings reached the TSD binaries here, so
    ``union(a, b)`` was rejected at wiring while ``bit_or(a, b)`` evaluated.
    """
    import pytest

    D = hg.TSD[str, TS[int]]

    def pair(node):
        @graph
        def g(a: D, b: D) -> D:
            return node(a, b)

        return eval_node(g, [{"a": 1, "c": 3}], [{"b": 2, "c": 4}])

    assert pair(hg.union) == [{"a": 1, "b": 2, "c": 3}]
    assert pair(hg.intersection) == [{"c": 3}]
    assert pair(hg.difference) == [{"a": 1}]
    assert pair(hg.symmetric_difference) == [{"a": 1, "b": 2}]

    # The bitwise spellings answer the same, as they always did.
    assert pair(hg.bit_or) == [{"a": 1, "b": 2, "c": 3}]

    # The fold is pairwise and n-ary, which is what upstream's three-input
    # answers show -- for a dictionary, unlike a TSS, even for intersection
    # and symmetric_difference.
    @graph
    def three(a: D, b: D, c: D) -> D:
        return hg.union(a, b, c)

    assert eval_node(three, [{"a": 1}], [{"b": 2}], [{"c": 3}]) == [
        {"a": 1, "b": 2, "c": 3}
    ]

    # difference is binary only, the arity released hgraph supports.
    @graph
    def three_differences(a: D, b: D, c: D) -> D:
        return hg.difference(a, b, c)

    with pytest.raises(Exception):
        eval_node(three_differences, [{"a": 1}], [{"b": 2}], [{"c": 3}])


def test_an_ordering_comparison_has_no_mixed_numeric_form():
    """Issue #818 item 5.7: ``gt_(TS[float], 1)`` must be rejected.

    Released hgraph declares both operands as one ``TIME_SERIES_TYPE`` and
    resolves them together, so every mixed int/float ordering fails at wiring
    there -- with a raw scalar and with two time series alike. This runtime
    answered them, so a comparison released hgraph refuses evaluated silently.

    ``eq_``/``ne_`` are not the same case and keep their mixed form: upstream
    gives them a float-epsilon overload.
    """
    import pytest

    # 2.0 against 1.0: true for the two "greater" senses, false for the other.
    for node, matched_answer in (
        (hg.gt_, True), (hg.ge_, True), (hg.lt_, False), (hg.le_, False)
    ):

        @graph
        def against_a_scalar(ts: TS[float]) -> TS[bool]:
            return node(ts, 1)

        @graph
        def against_a_series(a: TS[float], b: TS[int]) -> TS[bool]:
            return node(a, b)

        @graph
        def matched(ts: TS[float]) -> TS[bool]:
            return node(ts, 1.0)

        with pytest.raises(Exception):
            eval_node(against_a_scalar, [2.0])
        with pytest.raises(Exception):
            eval_node(against_a_series, [2.0], [1])

        # The same-type spelling is untouched.
        assert eval_node(matched, [2.0]) == [matched_answer]

    @graph
    def equality(ts: TS[float]) -> TS[bool]:
        return hg.eq_(ts, 2)

    assert eval_node(equality, [2.0]) == [True]
