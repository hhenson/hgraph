"""Closed-file replay when a dictionary event has no complete leaf partition."""

import hgraph as hg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hgraph_persistence.compat import FileBasedDataFrameStorage


Book = hg.TSD[str, hg.TSD[str, hg.TSD[str, hg.TS[int]]]]


@pytest.mark.parametrize("mode", [hg.ToTableMode.Tick, hg.ToTableMode.Sample, hg.ToTableMode.Snap])
@pytest.mark.parametrize("segmented", [False, True])
@pytest.mark.parametrize("first", ["priced", "empty", "only_empty"])
def test_reopen_a_withdrawn_intermediate_dictionary(tmp_path, mode, segmented, first):
    @hg.compute_node
    def publish(step: hg.TS[int], _output: Book = None) -> Book:
        if step.value == 0:
            price = {"amount": 7} if first == "priced" else {}
            x = {"price": price}
            if first != "only_empty":
                x["keep"] = {"amount": 13}
            return {"x": x}
        if step.value == 1:
            _output["x"]["price"].invalidate()
        if step.value == 3:
            return {"x": {"price": {} if first == "only_empty" else {"amount": 11}}}

    @hg.compute_node(active=("clock",), valid=("clock",))
    def read(clock: hg.TS[int], book: Book) -> hg.TS[object]:
        if not book.valid or "x" not in book.keys():
            return "absent"
        x = book["x"]
        if "price" not in x.keys():
            return "absent"
        keep = x["keep"]["amount"].value if "keep" in x.keys() else "absent"
        price = x["price"]
        if not price.valid:
            return (keep, "invalid")
        keys = tuple(price.keys())
        return (keep, price["amount"].value if keys == ("amount",) else keys)

    @hg.graph
    def writer(clock: hg.TS[int]) -> hg.TS[object]:
        book = publish(clock)
        hg.record(book, "book", recordable_id="prefix", mode=mode, flush_rows=1 if segmented else 0, track_validity=True)
        return read(clock, book)

    @hg.graph
    def reader(clock: hg.TS[int]) -> hg.TS[object]:
        return read(clock, hg.replay("book", Book, recordable_id="prefix"))

    keep = "absent" if first == "only_empty" else 13
    prices = (7 if first == "priced" else (), "invalid", "invalid", () if first == "only_empty" else 11)
    for graph in (writer, reader):
        with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
            hg.set_record_replay_config("hgraph.persistence.frame")
            assert hg.eval_node(graph, list(range(4))) == [(keep, price) for price in prices]

    if first == "only_empty" and not segmented:
        assert pq.read_table(tmp_path / "prefix.book.parquet").num_rows == 3


def test_prefix_withdrawal_and_membership_after_sorting_a_recorded_table(tmp_path):
    from hgraph_persistence import RecordRemoves

    names = ("__key_2_valid__", "member", "field")

    @hg.compute_node
    def update(step: hg.TS[int], _output: Book = None) -> Book:
        if step.value == 0:
            return {"x": {"price": {"amount": 7}, "keep": {"amount": 13}}}
        if step.value == 1:
            _output["x"]["price"].invalidate()
            return {"x": {"keep": {"amount": 17}}}
        if step.value == 2:
            return {"x": {"price": hg.REMOVE}}
        if step.value == 3:
            return {"x": {"price": {}}}
        if step.value == 4:
            return {"x": {"price": {"amount": 11}}}

    @hg.compute_node(active=("clock",), valid=("clock",))
    def observe(clock: hg.TS[int], book: Book) -> hg.TS[object]:
        x = book["x"]
        keep = x["keep"]["amount"].value
        if "price" not in x.keys():
            return (keep, "absent")
        price = x["price"]
        if not price.valid:
            return (keep, "invalid")
        return (keep, price["amount"].value if "amount" in price.keys() else "empty")

    @hg.graph
    def record(clock: hg.TS[int]) -> hg.TS[object]:
        book = update(clock)
        hg.record(book, "data", recordable_id="prices", partition_names=names, date_key="when",
                  removes=RecordRemoves.TRACK, track_validity=True)
        return observe(clock, book)

    @hg.graph
    def replay(clock: hg.TS[int]) -> hg.TS[object]:
        book = hg.replay("data", Book, recordable_id="prices", partition_names=names, date_key="when")
        return observe(clock, book)

    expected = [(13, 7), (17, "invalid"), (17, "absent"), (17, "empty"), (17, 11)]

    def run(graph):
        with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
            hg.set_record_replay_config("hgraph.persistence.frame")
            return hg.eval_node(graph, list(range(5)))

    assert run(record) == expected
    file = tmp_path / "prices.data.parquet"
    table = pq.read_table(file).sort_by([("when", "descending")])
    flag = table.schema.metadata[b"hgraph.recording.dictionary_validity.v1.level.2"].decode()
    assert flag not in names
    assert sum(cell is False for cell in table[flag].to_pylist()) == 1
    assert sum(cell is True for cell in table[flag].to_pylist()) == 1
    pq.write_table(table, file, row_group_size=1)
    assert run(replay) == expected

    index = table.schema.get_field_index(flag)
    malformed = table.set_column(index, flag, pa.array(["unknown"] * table.num_rows))
    pq.write_table(malformed, file)
    with pytest.raises(RuntimeError, match="incompatible dictionary validity column"):
        run(replay)


@pytest.mark.parametrize(
    "steps, prices, price_events, parent_events",
    [
        (["seed", "clear", "idle", "recover"], [7, None, None, 11],
         [(0, 7), (1, None), (3, 11)], [0, 1, 3]),
        (["seed", "clear", "clear", "sibling", "idle", "recover", "clear", "clear", "idle"],
         [7, None, None, None, None, 11, None, None, None],
         [(0, 7), (1, None), (5, 11), (6, None)], [0, 1, 3, 5, 6]),
    ],
)
def test_reopened_tick_file_contains_each_price_transition_once(tmp_path, steps, prices, price_events, parent_events):
    @hg.compute_node
    def update(clock: hg.TS[int], _output: Book = None) -> Book:
        match steps[clock.value]:
            case "seed":
                return {"x": {"price": {"amount": 7}, "keep": {"amount": 13}}}
            case "clear":
                _output["x"]["price"].invalidate()
            case "sibling":
                return {"x": {"keep": {"amount": 17}}}
            case "recover":
                return {"x": {"price": {"amount": 11}}}

    @hg.compute_node(active=("clock",), valid=("clock",))
    def observe(clock: hg.TS[int], book: Book) -> hg.TS[object]:
        entry = book["x"]
        assert "price" in entry.keys()
        price = entry["price"]
        return (price["amount"].value if price.valid else None, entry["keep"]["amount"].value, book.modified)

    @hg.graph
    def writer(clock: hg.TS[int]) -> hg.TS[object]:
        book = update(clock)
        hg.record(book, "book", recordable_id="tick", mode=hg.ToTableMode.Tick, track_validity=True)
        return observe(clock, book)

    @hg.graph
    def reader(clock: hg.TS[int]) -> hg.TS[object]:
        return observe(clock, hg.replay("book", Book, recordable_id="tick"))

    keep = 13
    expected = []
    for index, (step, price) in enumerate(zip(steps, prices)):
        if step == "sibling":
            keep = 17
        expected.append((price, keep, index in parent_events))
    for graph in (writer, reader):
        with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
            hg.set_record_replay_config("hgraph.persistence.frame")
            assert hg.eval_node(graph, list(range(len(steps)))) == expected

    def graph_time(when):
        return (when.replace(tzinfo=None) - hg.MIN_ST) // hg.MIN_TD

    table = pq.read_table(tmp_path / "tick.book.parquet")
    events = [(graph_time(row["__date_time__"]), row["value"]) for row in table.to_pylist()
              if row["__key_2__"] == "price" and row["__key_3__"] == "amount"]
    flag = table.schema.metadata[b"hgraph.recording.dictionary_validity.v1.level.2"].decode()
    for row in table.to_pylist():
        if row[flag] is not None:
            assert (row["__key_1__"], row["__key_2__"], row["__key_3__"], row[flag]) == ("x", "price", None, False)
            events.append((graph_time(row["__date_time__"]), None))
    assert sorted(events) == price_events


@pytest.mark.parametrize("removals", [False, True])
@pytest.mark.parametrize("tracking", [None, False, True])
@pytest.mark.parametrize("mode", [hg.ToTableMode.Tick, hg.ToTableMode.Sample, hg.ToTableMode.Snap])
def test_validity_tracking_is_independent_of_the_existing_removal_policy(tmp_path, mode, tracking, removals):
    from hgraph_persistence import RecordRemoves

    @hg.compute_node
    def update(step: hg.TS[int], _output: Book = None) -> Book:
        if step.value == 0:
            return {"x": {"price": {"amount": 7}, "keep": {"amount": 13}}}
        if step.value == 1:
            _output["x"]["price"].invalidate()
        if step.value == 3:
            return {"x": {"price": {"amount": 11}}}

    @hg.compute_node(active=("clock",), valid=("clock",))
    def observe(clock: hg.TS[int], book: Book) -> hg.TS[object]:
        entry = book["x"]
        assert "price" in entry.keys() and entry["keep"]["amount"].value == 13
        price = entry["price"]
        return (price.valid, price["amount"].value if price.valid and "amount" in price.keys() else None)

    @hg.graph
    def write(clock: hg.TS[int]) -> hg.TS[object]:
        book = update(clock)
        options = {} if tracking is None else {"track_validity": tracking}
        hg.record(book, "book", recordable_id="option", mode=mode,
                  removes=RecordRemoves.TRACK if removals else RecordRemoves.OMIT, **options)
        return observe(clock, book)

    @hg.graph
    def read(clock: hg.TS[int]) -> hg.TS[object]:
        return observe(clock, hg.replay("book", Book, recordable_id="option"))

    def run(graph):
        with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
            hg.set_record_replay_config("hgraph.persistence.frame")
            return hg.eval_node(graph, [0, 1, 2, 3])

    invalid = (False, None)
    assert run(write) == [(True, 7), invalid, invalid, (True, 11)]
    missing = invalid if tracking else (True, None if removals and mode != hg.ToTableMode.Snap else 7)
    assert run(read) == [(True, 7), missing, missing, (True, 11)]

    table = pq.read_table(tmp_path / "option.book.parquet")
    extra = [field for field in table.column_names if field.endswith("_valid__")]
    if tracking:
        assert extra == ["__key_1_valid__", "__key_2_valid__"]
        states = [row for row in table.to_pylist() if row[extra[1]] is not None]
        assert len(states) == 1
        assert states[0]["__date_time__"] == hg.MIN_ST + hg.MIN_TD
        assert states[0][extra[1]] is False and states[0]["__key_3__"] is None
    else:
        assert extra == []
        assert not any(b"validity" in key for key in table.schema.metadata)
    assert ("__key_2_removed__" in table.column_names) == removals
