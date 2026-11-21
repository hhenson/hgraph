from typing import Optional

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
