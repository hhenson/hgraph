from typing import Optional

import pytest

import hgraph as hg


Book = hg.TSD[str, hg.TS[Optional[int]]]


@hg.compute_node
def write(step: hg.TS[int], _output: hg.TSD = None) -> Book:
    match step.value:
        case 0:
            _output.get_or_create("x")
        case 1 | 4:
            return {"x": step.value}
        case 2:
            _output["x"].invalidate()
        case 5:
            return {"x": hg.Null}
        case 6:
            del _output["x"]


@hg.compute_node(active=("clock",), valid=("clock",))
def observe(clock: hg.TS[int], book: Book) -> hg.TS[str]:
    child = book["x"] if "x" in book else None
    if child is None:
        value = "absent"
    elif child.valid:
        value = str(child.value)
    else:
        value = "invalid"
    return f"{value} {sorted(book.added_keys())} {sorted(book.removed_keys())}"


@hg.graph
def live(clock: hg.TS[int]) -> hg.TS[str]:
    book = write(clock)
    hg.record(book, "rows", recordable_id="dict")
    return observe(clock, book)


@hg.graph
def replay(clock: hg.TS[int]) -> hg.TS[str]:
    return observe(clock, hg.replay("rows", Book, recordable_id="dict"))


def test_sparse_recorded_dictionary_keeps_child_validity_and_real_removal():
    clock = list(range(8))
    expected = ["invalid ['x'] []", "1 [] []", "invalid [] []", "invalid [] []",
                "4 [] []", "None [] []", "absent [] ['x']", "absent [] []"]
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        assert hg.eval_node(live, clock) == expected
        assert hg.eval_node(replay, clock) == expected
        rows = hg.get_recorded_value("rows", "dict")
        assert [entry[1] for entry in rows] == [
            {"x": hg.INVALID_CHILD}, {"x": 1}, {"x": hg.INVALID_CHILD},
            {"x": 4}, {"x": hg.Null}, {"x": hg.REMOVE},
        ]
        for i, row in enumerate(rows):
            rows[i] = row
        assert hg.eval_node(replay, clock) == expected
        rows[3] = rows[3][0], {"x": hg.REMOVE}
        assert hg.eval_node(replay, clock)[3:7] == [
            "invalid [] []", "absent [] ['x']", "None ['x'] []", "absent [] ['x']",
        ]


def test_edit_legacy_sparse_rows_to_retain_an_invalid_key():
    clock = [1, 4, 6]
    with hg.GlobalState() as state:
        hg.set_record_replay_config(hg.IN_MEMORY)
        expected = ["1 ['x'] []", "4 [] []", "absent [] ['x']"]
        assert hg.eval_node(live, clock) == expected
        key = ":memory:dict.rows"
        assert all(len(row) == 2 for row in state._impl[key])
        assert hg.eval_node(replay, clock) == expected
        rows = hg.get_recorded_value("rows", "dict")
        rows[1] = rows[1][0], {"x": hg.INVALID_CHILD}
        assert hg.eval_node(replay, clock) == ["1 ['x'] []", "invalid [] []", "absent [] ['x']"]
        assert hg.get_recorded_value("rows", "dict") == rows


def test_append_an_unset_key_to_an_older_recording_preserves_its_history():
    with hg.GlobalState() as state:
        hg.set_record_replay_config(hg.IN_MEMORY)
        assert hg.eval_node(live, [1, 6]) == ["1 ['x'] []", "absent [] ['x']"]
        original = list(hg.get_recorded_value("rows", "dict"))
        assert all(len(row) == 2 for row in state._impl[":memory:dict.rows"])
        later = hg.MIN_ST + 3 * hg.MIN_TD
        assert hg.eval_node(live, [0, 6], __start_time__=later) == ["invalid ['x'] []", "absent [] ['x']"]
        assert list(hg.get_recorded_value("rows", "dict"))[:2] == original
        assert hg.eval_node(replay, list(range(5))) == [
            "1 ['x'] []", "absent [] ['x']", "absent [] []", "invalid ['x'] []", "absent [] ['x']",
        ]


class Price(hg.TimeSeriesSchema):
    price: hg.TS[int]


@pytest.mark.parametrize(("child_type", "publish", "read"), [
    (hg.TSB[Price], lambda n: {"price": n}, lambda x: x.price.value),
    (hg.TSS[int], lambda n: frozenset({n}), lambda x: tuple(sorted(x.value))),
    (hg.TSD[str, hg.TS[int]], lambda n: {"price": n}, lambda x: x["price"].value),
    (hg.TSL[hg.TS[int], hg.Size[1]], lambda n: (n,), lambda x: x[0].value),
    (hg.TSL[hg.TS[int], hg.Size[-1]], lambda n: (n,), lambda x: x[0].value),
], ids=["bundle", "set", "dictionary", "fixed-list", "growing-list"])
def test_whole_collection_child_withdrawal_keeps_the_key_across_replay(child_type, publish, read):
    book_type = hg.TSD[str, child_type]

    @hg.compute_node
    def write_book(step: hg.TS[int], _output: book_type = None) -> book_type:
        match step.value:
            case 0:
                return {"x": publish(7), "neighbor": publish(42)}
            case 1:
                _output["x"].invalidate()
            case 3:
                return {"x": publish(11)}
            case 4:
                del _output["x"]

    @hg.compute_node(active=("clock",), valid=("clock",))
    def inspect(clock: hg.TS[int], book: book_type) -> hg.TS[object]:
        return tuple(sorted((k, read(v) if v.valid else None) for k, v in book.items()))

    @hg.graph
    def record_book(clock: hg.TS[int]) -> hg.TS[object]:
        book = write_book(clock)
        hg.record(book, "rows", recordable_id="compound")
        return inspect(clock, book)

    @hg.graph
    def read_book(clock: hg.TS[int]) -> hg.TS[object]:
        return inspect(clock, hg.replay("rows", book_type, recordable_id="compound"))

    ticks = list(range(6))
    values = [(n,) if child_type == hg.TSS[int] else n for n in (7, 42, 11)]
    first, neighbor, recovered = values
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        expected = [(("neighbor", neighbor), ("x", x)) for x in (first, None, None, recovered)]
        expected.extend([(("neighbor", neighbor),)] * 2)
        assert hg.eval_node(record_book, ticks) == expected
        assert hg.eval_node(read_book, ticks) == expected
        changes = hg.get_recorded_value("rows", "compound")
        assert changes[1][1] == {"x": hg.INVALID_CHILD}
        assert changes[-1][1] == {"x": hg.REMOVE}


class RecordedPair(hg.TimeSeriesSchema):
    price: hg.TS[Optional[int]]
    keep: hg.TS[Optional[int]]


@pytest.mark.parametrize("keyed", [False, True])
def test_partial_child_withdrawal_survives_recording_edit_and_replay(keyed):
    child_type = hg.TSD[str, hg.TS[Optional[int]]] if keyed else hg.TSB[RecordedPair]
    book_type = hg.TSD[str, child_type]

    @hg.compute_node
    def write_pair(step: hg.TS[int], _output: book_type = None) -> book_type:
        match step.value:
            case 0:
                return {"x": {"price": 7, "keep": 13}, "z": {"keep": 42}}
            case 1:
                _output["x"]["price"].invalidate()
            case 3:
                return {"x": {"price": 11}}
            case 4:
                return {"x": {"price": hg.Null}}
            case 5:
                _output["x"]["price"].invalidate()
                return {"x": {"keep": 14}}
            case 6 if keyed:
                del _output["x"]["price"]
            case 7:
                return {"x": {"price": 15}}
            case 8:
                del _output["x"]
            case 9:
                return {"x": {"price": 19, "keep": 17}}
            case 10:
                _output["x"].invalidate()

    @hg.compute_node(active=("clock",), valid=("clock",))
    def read_pair(clock: hg.TS[int], book: book_type) -> hg.TS[object]:
        if "x" not in book:
            return "absent", None, book["z"]["keep"].value
        child = book["x"]
        if not child.valid:
            return "invalid", "invalid", book["z"]["keep"].value
        if keyed and "price" not in child:
            value = "absent"
        elif child["price"].valid:
            value = child["price"].value
        else:
            value = "invalid"
        return value, child["keep"].value if child.valid else "invalid", book["z"]["keep"].value

    @hg.graph
    def live_pair(clock: hg.TS[int]) -> hg.TS[object]:
        book = write_pair(clock)
        hg.record(book, "rows", recordable_id="nested")
        return read_pair(clock, book)

    @hg.graph
    def replay_pair(clock: hg.TS[int]) -> hg.TS[object]:
        return read_pair(clock, hg.replay("rows", book_type, recordable_id="nested"))

    clock = list(range(10))
    expected = [
        (7, 13, 42), ("invalid", 13, 42), ("invalid", 13, 42), (11, 13, 42),
        (None, 13, 42), ("invalid", 14, 42), ("absent" if keyed else "invalid", 14, 42),
        (15, 14, 42), ("absent", None, 42), (19, 17, 42),
    ]
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        assert hg.eval_node(live_pair, clock) == expected
        rows = hg.get_recorded_value("rows", "nested")
        changed = rows[1][1]["x"]
        assert (changed["modified"] if keyed else changed)["price"] is hg.INVALID_CHILD
        assert hg.eval_node(replay_pair, clock) == expected
        for i, row in enumerate(rows):
            rows[i] = row
        assert hg.eval_node(replay_pair, clock) == expected

        # A historical row could record the empty edit but had no evidence of
        # the withdrawn field. It must still replay its original observation.
        clear = rows[1]
        rows[1] = clear[0], {"x": {"removed": frozenset(), "modified": {}} if keyed else {}}
        assert hg.eval_node(replay_pair, clock)[1:3] == [(7, 13, 42)] * 2
        rows[1] = clear
        assert hg.eval_node(replay_pair, clock) == expected
        if not keyed:
            with pytest.raises(KeyError):
                rows[1] = clear[0], {"x": {"unknown": hg.INVALID_CHILD}}
            assert hg.get_recorded_value("rows", "nested")[1] == clear

    # Promote a recording that already has a whole-child withdrawal, then edit it.
    with hg.GlobalState() as state:
        hg.set_record_replay_config(hg.IN_MEMORY)
        hg.eval_node(live_pair, [0, 10, 0])
        previous = list(hg.get_recorded_value("rows", "nested"))
        assert previous[1][1] == {"x": hg.INVALID_CHILD}
        assert len(state._impl[":memory:nested.rows"][0]) == 3
        hg.eval_node(live_pair, [0, 1], __start_time__=hg.MIN_ST + 4 * hg.MIN_TD)
        promoted = hg.get_recorded_value("rows", "nested")
        assert list(promoted[:3]) == previous
        assert len(state._impl[":memory:nested.rows"][0]) == 4
        promoted[1] = previous[1]
        assert list(hg.get_recorded_value("rows", "nested")[:3]) == previous


class ObjectField(hg.TimeSeriesSchema):
    payload: hg.TS[object]


@pytest.mark.parametrize("shape", ["scalar", "dictionary-member", "bundle-field", "nested-key"])
def test_edit_opaque_payload_preserves_literal_marker(shape):
    scalar = hg.TS[object]
    wrapped = {
        "scalar": (scalar, lambda v: v, lambda v: v),
        "dictionary-member": (hg.TSD[str, scalar], lambda v: {"x": v}, lambda v: v["x"]),
        "bundle-field": (hg.TSD[str, hg.TSB[ObjectField]], lambda v: {"x": {"payload": v}},
                         lambda v: v["x"]["payload"]),
        "nested-key": (hg.TSD[str, hg.TSD[str, scalar]], lambda v: {"x": {"payload": v}},
                       lambda v: v["x"]["payload"]),
    }
    schema, wrap, unwrap = wrapped[shape]

    @hg.compute_node
    def publish(step: hg.TS[int]) -> schema:
        return wrap({"ordinary": step.value})

    @hg.graph
    def write_objects(clock: hg.TS[int]) -> schema:
        value = publish(clock)
        hg.record(value, "objects", recordable_id="marker")
        return value

    @hg.compute_node(active=("clock",), valid=("clock",))
    def read_object(clock: hg.TS[int], value: schema) -> scalar:
        return unwrap(value).value

    @hg.graph
    def replay_objects(clock: hg.TS[int]) -> scalar:
        return read_object(clock, hg.replay("objects", schema, recordable_id="marker"))

    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        hg.eval_node(write_objects, [1, 2])
        rows = hg.get_recorded_value("objects", "marker")
        literal = {"kind": {"item": hg.INVALID_CHILD}}
        changed = wrap(literal)
        if shape == "nested-key":
            changed = {"x": {"removed": frozenset(), "modified": {"payload": literal}}}
        rows[0] = rows[0][0], changed
        first, second = hg.eval_node(replay_objects, [1, 2])
        assert first["kind"]["item"] is hg.INVALID_CHILD
        assert second == {"ordinary": 2}
