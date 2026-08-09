"""The Python bridge, slice 1: wire and run a graph purely by operator name.

Run directly (ctest registers it when HGRAPH_BUILD_PYTHON_BINDINGS=ON).
"""
import _hgraph as hg


def ts_int():
    # Looked up per test: type metadata pointers do not survive a registry
    # reset, so never cache them across resets.
    return hg.ts_type("TS[int]")


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_const_add_record():
    w = hg.Wiring()
    lhs = w.wire("const", (42,), {}, output_type=ts_int())
    rhs = w.wire("const", (8,), {}, output_type=ts_int())
    total = w.wire("add_", (lhs, rhs), {})
    w.wire("record", (total, "out"), {})
    run = w.run()
    check(run.recorded("out") == [50], f"unexpected: {run.recorded('out')}")


def test_replay_through_graph():
    w = hg.Wiring()
    src = w.wire("replay", ("in",), {}, output_type=ts_int())
    doubled = w.wire("add_", (src, src), {})
    w.wire("record", (doubled, "out"), {})
    w.set_replay("in", [1, None, 3])
    run = w.run()
    check(run.recorded("out") == [2, None, 6], f"unexpected: {run.recorded('out')}")


def test_keyword_arguments():
    w = hg.Wiring()
    src = w.wire("const", (), {"value": 7}, output_type=ts_int())
    total = w.wire("add_", (), {"rhs": src, "lhs": src})
    w.wire("record", (total, "kw"), {})
    run = w.run()
    check(run.recorded("kw") == [14], f"unexpected: {run.recorded('kw')}")


def test_json_round_trip():
    w = hg.Wiring()
    src = w.wire("replay", ("in",), {}, output_type=ts_int())
    text = w.wire("to_json", (src,), {})
    back = w.wire("from_json", (text,), {}, output_type=ts_int())
    w.wire("record", (back, "out"), {})
    w.set_replay("in", [5, -9])
    run = w.run()
    check(run.recorded("out") == [5, -9], f"unexpected: {run.recorded('out')}")


def test_value_conversion_round_trips():
    import datetime

    samples = [
        True,
        42,
        2**40,
        -(2**40),
        -1.5,
        "text",
        b"\x00raw",
        datetime.datetime(2024, 1, 2, 3, 4, 5, 123456),
        datetime.datetime(1, 1, 1),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
        datetime.date(2024, 1, 2),
        datetime.timedelta(seconds=90),
        datetime.time(3, 4, 5, 123456),
        frozenset({1, 2, 3}),
        {"a": 1, "b": 2},
        (1, 2, 3),
        [1.5, 2.5],
    ]
    for sample in samples:
        result = hg._roundtrip_value(sample)
        expected = sample
        if isinstance(sample, (set, frozenset)):
            expected = frozenset(sample)
        elif isinstance(sample, tuple):
            expected = sample  # tuple identity is preserved by variadic-tuple storage
        check(result == expected, f"round trip {sample!r} -> {result!r}")


def test_dynamic_attributes_do_not_masquerade_as_datetime_values():
    class DynamicAttributes:
        def __getattr__(self, name):
            return DynamicAttributes()

    sample = DynamicAttributes()
    result = hg._roundtrip_value(sample)
    check(result is sample, "schema-free conversion must preserve opaque Python objects")


def test_aware_datetime_is_normalized_to_naive_utc():
    import datetime

    source = datetime.datetime(
        2024, 1, 2, 12, 30, 0,
        tzinfo=datetime.timezone(datetime.timedelta(hours=4)),
    )
    result = hg._roundtrip_value(source)
    check(result == datetime.datetime(2024, 1, 2, 8, 30, 0), f"unexpected: {result!r}")
    check(result.tzinfo is None, f"native datetime must be timezone-free: {result!r}")


def test_aware_time_is_rejected_without_a_zoned_time_scalar():
    import datetime
    from zoneinfo import ZoneInfo

    for source in (
        datetime.time(12, 30, tzinfo=datetime.timezone.utc),
        datetime.time(12, 30, tzinfo=ZoneInfo("Africa/Johannesburg")),
    ):
        try:
            hg._roundtrip_value(source)
        except TypeError as error:
            check("timezone-aware time" in str(error), f"unexpected error: {error}")
        else:
            raise AssertionError(f"timezone-bearing time was accepted: {source!r}")


def test_datetime_scalars_through_a_graph():
    import datetime

    w = hg.Wiring()
    src = w.wire("const", (datetime.date(2024, 6, 30),), {}, output_type=hg.ts_type("TS[date]"))
    w.wire("record", (src, "d"), {})
    run = w.run()
    check(run.recorded("d") == [datetime.date(2024, 6, 30)], f"unexpected: {run.recorded('d')}")


def test_type_construction():
    ts_int = hg.ts(hg.value_type("int"))
    tss_str = hg.tss(hg.value_type("str"))
    tsd = hg.tsd(hg.value_type("str"), ts_int)
    tsl = hg.tsl(ts_int, 3)
    check(all(x is not None for x in (ts_int, tss_str, tsd, tsl)), "type construction failed")
    # Constructed TS[int] resolves identically to the registered name.
    w = hg.Wiring()
    p = w.wire("const", (1,), {}, output_type=ts_int)
    check(p is not None, "constructed type failed to wire")


def test_type_predicates_and_patterns():
    int_vt = hg.value_type("int")
    str_vt = hg.value_type("str")
    ts_int = hg.ts(int_vt)
    tsd = hg.tsd(str_vt, ts_int)
    tsl = hg.tsl(ts_int, 3)
    json_ts = hg.ts(hg.value_type("JSON"))
    mapping_ts = hg.ts(hg.map_vt(str_vt, int_vt))

    check(ts_int.is_ts and not ts_int.is_tsd, "TS predicate failed")
    check(tsd.is_tsd and not tsd.is_ts, "TSD predicate failed")
    check(tsl.is_tsl and tsl.is_fixed_tsl and tsl.fixed_size == 3, "TSL predicate failed")
    check(json_ts.is_ts_json, "JSON predicate failed")
    check(mapping_ts.is_ts_mapping, "mapping predicate failed")
    check(repr(hg.type_pattern_tsw(hg.scalar_pattern_var("T"))) == "TSW[~T, *]", "TSW pattern")
    check(repr(hg.type_pattern_signal()) == "SIGNAL", "SIGNAL pattern")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} bridge tests passed")


if __name__ == "__main__":
    main()
