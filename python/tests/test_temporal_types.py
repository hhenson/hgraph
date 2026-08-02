import datetime as dt

import pytest

import hgraph as hg
from hgraph.test import eval_node


def test_temporal_values_are_immutable_hashable_native_scalars():
    civil = hg.CivilDateTime(dt.date(2026, 7, 23), dt.time(12, 34, 56, 789))
    period = hg.Period(years=1, months=2, days=-3)
    zone = hg.ZoneId("America/New_York")

    assert (civil.year, civil.month, civil.day) == (2026, 7, 23)
    assert (civil.hour, civil.minute, civil.second, civil.microsecond) == (
        12,
        34,
        56,
        789,
    )
    assert civil.weekday() == 3
    assert civil.isoweekday() == 4
    assert (period.total_months, period.years, period.months, period.days) == (
        14,
        1,
        2,
        -3,
    )
    assert str(zone) == "America/New_York"
    assert zone == hg.ZoneId("America/New_York")
    assert len({civil, civil}) == 1
    assert len({period, period}) == 1
    assert len({zone, hg.ZoneId("America/New_York")}) == 1

    with pytest.raises(AttributeError):
        civil.year = 2025

    @hg.compute_node
    def identity(value: hg.TS[hg.CivilDateTime]) -> hg.TS[hg.CivilDateTime]:
        return value.value

    assert eval_node(identity, [civil]) == [civil]


def test_temporal_value_arithmetic_matches_graph_operators_inside_compute_nodes():
    civil = hg.CivilDateTime(dt.date(2026, 7, 23), 12, 30)
    delta = dt.timedelta(hours=2, minutes=15)
    period = hg.Period(months=1)

    assert civil + delta == hg.temporal.checked_add(civil, delta)
    assert civil - delta == hg.temporal.checked_subtract(civil, delta)
    assert (civil + delta) - civil == delta
    assert period + period == hg.Period(months=2)
    assert -period == hg.Period(months=-1)
    assert 3 * period == hg.Period(months=3)
    assert dt.date(2025, 1, 15) + period == dt.date(2025, 2, 15)
    with pytest.raises(TypeError):
        dt.datetime(2025, 1, 15, 12) + period
    with pytest.raises(TypeError):
        hg.temporal.checked_add(
            dt.datetime(2025, 1, 15, 12),
            period,
        )
    assert hg.temporal.apply_period(
        dt.date(2025, 1, 31),
        period,
        month_end_policy=hg.MonthEndPolicy.CLAMP,
    ) == dt.date(2025, 2, 28)

    @hg.compute_node
    def add_values(
        value: hg.TS[hg.CivilDateTime],
        amount: hg.TS[dt.timedelta],
    ) -> hg.TS[hg.CivilDateTime]:
        return value.value + amount.value

    expected = eval_node(hg.add_, [civil], [delta])
    assert eval_node(add_values, [civil], [delta]) == expected

    @hg.compute_node
    def round_values(
        value: hg.TS[dt.timedelta],
        quantum: hg.TS[dt.timedelta],
    ) -> hg.TS[dt.timedelta]:
        return hg.temporal.round(value.value, quantum.value)

    value = dt.timedelta(microseconds=1_500_000)
    quantum = dt.timedelta(seconds=1)
    expected = eval_node(hg.temporal_round, [value], [quantum])
    assert eval_node(round_values, [value], [quantum]) == expected


def test_range_normalization_algebra_and_python_sequence_contract():
    t0 = dt.datetime(2026, 1, 1)
    t1 = t0 + dt.timedelta(hours=1)
    t2 = t1 + dt.timedelta(hours=1)
    left = hg.InstantRange(t0, t1)
    right = hg.InstantRange(t1, t2)

    assert left.contains(t0)
    assert not left.contains(t1)
    assert hg.InstantRange(t0, t2).contains(left)
    assert left.adjacent(right)
    assert left.mergeable(right)
    assert left.merge(right) == hg.InstantRange(t0, t2)
    assert left.intersection(right).is_empty

    pieces = hg.InstantRange(t0, t2).difference(
        hg.InstantRange(
            t0 + dt.timedelta(minutes=15),
            t1 + dt.timedelta(minutes=45),
        )
    )
    assert len(pieces) == 2
    assert list(pieces) == [
        hg.InstantRange(t0, t0 + dt.timedelta(minutes=15)),
        hg.InstantRange(t1 + dt.timedelta(minutes=45), t2),
    ]
    assert pieces == hg.InstantRangeSet(list(reversed(list(pieces))))
    assert hash(pieces) == hash(hg.InstantRangeSet(list(pieces)))


def test_temporal_range_value_functions_match_graph_operators():
    start = dt.datetime(2026, 1, 1)
    range_ = hg.InstantRange(start, start + dt.timedelta(hours=2))
    delta = dt.timedelta(minutes=30)

    assert hg.temporal.range_contains(range_, start)
    assert hg.temporal.range_shift(range_, delta) == hg.InstantRange(
        start + delta,
        start + dt.timedelta(hours=2, minutes=30),
    )
    assert hg.temporal.range_extent(range_) == dt.timedelta(hours=2)
    assert hg.temporal.bucket(
        start + dt.timedelta(minutes=75),
        dt.timedelta(hours=1),
    ) == hg.InstantRange(
        start + dt.timedelta(hours=1),
        start + dt.timedelta(hours=2),
    )

    assert eval_node(hg.range_shift, [range_], [delta]) == [
        hg.temporal.range_shift(range_, delta)
    ]
    assert eval_node(hg.range_extent, [range_]) == [
        hg.temporal.range_extent(range_)
    ]


def test_checked_graph_arithmetic_uses_static_policy_selection():
    jan_31 = dt.date(2025, 1, 31)
    one_month = hg.Period(months=1)

    assert eval_node(
        hg.add_,
        [jan_31],
        [one_month],
        month_end_policy=hg.MonthEndPolicy.CLAMP,
    ) == [dt.date(2025, 2, 28)]
    with pytest.raises(Exception, match="month|day"):
        eval_node(hg.add_, [jan_31], [one_month])

    half_seconds = [
        dt.timedelta(microseconds=500_000),
        dt.timedelta(microseconds=1_500_000),
        dt.timedelta(microseconds=-500_000),
        dt.timedelta(microseconds=-1_500_000),
    ]
    assert eval_node(
        hg.temporal_round,
        half_seconds,
        [dt.timedelta(seconds=1)] * len(half_seconds),
    ) == [
        dt.timedelta(0),
        dt.timedelta(seconds=2),
        dt.timedelta(0),
        dt.timedelta(seconds=-2),
    ]


def test_zone_resolution_uses_the_global_state_provider():
    zone = hg.ZoneId("America/New_York")
    instant = dt.datetime(2025, 1, 15, 12)

    with hg.GlobalContext(hg.GlobalState()):
        hg.set_time_zone_provider()
        zoned = eval_node(hg.at_zone, [instant], [zone])[0]
        assert zoned.instant == instant
        assert zoned.zone == zone
        assert zoned.offset_seconds == -5 * 60 * 60
        assert zoned.civil == hg.CivilDateTime(dt.date(2025, 1, 15), 7)
        with pytest.raises(TypeError):
            hg.ZonedDateTime(instant, zone, zoned.offset_seconds)

        fold = hg.CivilDateTime(dt.date(2025, 11, 2), 1, 30)
        earliest = eval_node(
            hg.resolve_civil,
            [fold],
            [zone],
            ambiguous=hg.AmbiguousTimePolicy.EARLIEST,
            nonexistent=hg.NonexistentTimePolicy.REJECT,
        )[0]
        latest = eval_node(
            hg.resolve_civil,
            [fold],
            [zone],
            ambiguous=hg.AmbiguousTimePolicy.LATEST,
            nonexistent=hg.NonexistentTimePolicy.REJECT,
        )[0]
        assert latest.instant - earliest.instant == dt.timedelta(hours=1)


def test_provider_backed_value_operations_match_graph_operators_inside_compute_nodes():
    zone = hg.ZoneId("America/New_York")
    other_zone = hg.ZoneId("Europe/London")
    instant = dt.datetime(2025, 1, 15, 12)
    delta = dt.timedelta(days=180)

    @hg.compute_node
    def at_zone_value(
        value: hg.TS[dt.datetime],
        zone_value: hg.TS[hg.ZoneId],
        global_state: hg.GlobalState = None,
    ) -> hg.TS[hg.ZonedDateTime]:
        return hg.temporal.at_zone(
            value.value, zone_value.value, global_state=global_state
        )

    @hg.compute_node
    def add_zoned_value(
        value: hg.TS[hg.ZonedDateTime],
        amount: hg.TS[dt.timedelta],
        global_state: hg.GlobalState = None,
    ) -> hg.TS[hg.ZonedDateTime]:
        return hg.temporal.checked_add(
            value.value, amount.value, global_state=global_state
        )

    with hg.GlobalContext(hg.GlobalState()):
        hg.set_time_zone_provider()
        direct = hg.temporal.at_zone(instant, zone)
        assert eval_node(at_zone_value, [instant], [zone]) == [direct]
        assert eval_node(hg.at_zone, [instant], [zone]) == [direct]

        converted = hg.temporal.convert_zone(direct, other_zone)
        assert converted.instant == direct.instant
        assert converted.zone == other_zone
        assert hg.temporal.to_instant(converted) == instant
        assert hg.temporal.to_civil(converted) == converted.civil

        expected = eval_node(hg.add_, [direct], [delta])
        assert eval_node(add_zoned_value, [direct], [delta]) == expected
        assert hg.temporal.checked_add(direct, delta) == expected[0]


def test_temporal_schema_identity_is_available_to_python_wiring():
    assert hg.TS[hg.CivilDateTime] != hg.TS[dt.datetime]
    assert hg.TS[hg.Period] != hg.TS[dt.timedelta]
    assert hg.TS[hg.InstantRange] != hg.TS[hg.CivilDateRange]
    assert hg.TSD[hg.ZoneId, hg.TS[hg.ZonedDateTime]]
