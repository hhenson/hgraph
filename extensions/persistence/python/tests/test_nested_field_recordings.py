"""TS-19 through closed and reopened partitioned Parquet recordings."""

import hgraph as hg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hgraph_persistence import RecordRemoves
from hgraph_persistence.compat import FileBasedDataFrameStorage


class Quote(hg.TimeSeriesSchema):
    price: hg.TS[int]
    keep: hg.TS[int]


@pytest.mark.parametrize("fields", [hg.TSB[Quote], hg.TSD[str, hg.TS[int]]])
@pytest.mark.parametrize("mode", [hg.ToTableMode.Tick, hg.ToTableMode.Sample, hg.ToTableMode.Snap])
@pytest.mark.parametrize("segmented", [False, True])
def test_reopen_partitioned_table_with_a_withdrawn_field(tmp_path, fields, mode, segmented):
    book_type = hg.TSD[str, fields]

    @hg.compute_node
    def publish(step: hg.TS[int], _output: book_type = None) -> book_type:
        if step.value == 0:
            return {"x": {"price": 7, "keep": 13}, "y": {"price": 8, "keep": 14}}
        if step.value == 1:
            _output["x"]["price"].invalidate()
            _output["y"]["keep"].invalidate()
        if step.value == 3:
            return {"x": {"price": 11}, "y": {"keep": 18}}

    @hg.compute_node(active=("step",), valid=("step",))
    def observe(step: hg.TS[int], book: book_type) -> hg.TS[object]:
        x, y = book["x"], book["y"]
        assert x.valid and y.valid
        if fields == hg.TSD[str, hg.TS[int]]:
            assert "price" in x.keys() and "keep" in y.keys()
        return tuple(child.value if child.valid else None for child in (
            x["price"], x["keep"], y["price"], y["keep"]
        ))

    @hg.graph
    def writer(step: hg.TS[int]) -> hg.TS[object]:
        book = publish(step)
        hg.record(book, "book", recordable_id="file", mode=mode, flush_rows=1 if segmented else 0)
        return observe(step, book)

    @hg.graph
    def reader(step: hg.TS[int]) -> hg.TS[object]:
        return observe(step, hg.replay("book", book_type, recordable_id="file"))

    steps = [0, 1, 2, 3]
    expected = [(7, 13, 8, 14), (None, 13, 8, None), (None, 13, 8, None), (11, 13, 8, 18)]
    with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
        hg.set_record_replay_config("hgraph.persistence.frame")
        assert hg.eval_node(writer, steps) == expected
    with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
        hg.set_record_replay_config("hgraph.persistence.frame")
        assert hg.eval_node(reader, steps) == expected

    if not segmented:
        # Older files contain only the projected value cells. Two null cells
        # cannot tell which field withdrew; the legacy reader keeps its value.
        path = tmp_path / "file.book.parquet"
        table = pq.read_table(path)
        metadata = dict(table.schema.metadata)
        marker = metadata.pop(b"hgraph.recording.withdrawals.v1.column").decode()
        legacy = table.drop([marker]).replace_schema_metadata(metadata)
        pq.write_table(legacy, path)
        with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
            hg.set_record_replay_config("hgraph.persistence.frame")
            assert hg.eval_node(reader, steps) == [expected[0], expected[0], expected[0], expected[3]]

def test_a_field_withdrawal_with_a_sibling_write_and_tracked_key_deletion(tmp_path):
    book_type = hg.TSD[str, hg.TSB[Quote]]
    name = "__hgraph_withdrawn_fields__"

    @hg.compute_node
    def publish(step: hg.TS[int], _output: book_type = None) -> book_type:
        if step.value == 0:
            return {"x": {"price": 7, "keep": 13}}
        if step.value == 1:
            _output["x"]["price"].invalidate()
            _output["x"]["keep"].value = 17
        if step.value == 2:
            return {"x": hg.REMOVE}
        if step.value == 3:
            return {"x": {"price": 23, "keep": 19}}

    @hg.compute_node(active=("step",), valid=("step",))
    def observe(step: hg.TS[int], book: book_type) -> hg.TS[object]:
        if "x" not in book.keys():
            return ("absent",)
        price = book["x"]["price"]
        return (price.value if price.valid else None, book["x"]["keep"].value)

    @hg.graph
    def writer(step: hg.TS[int]) -> hg.TS[object]:
        book = publish(step)
        hg.record(book, "book", recordable_id="file", partition_names=(name,), removes=RecordRemoves.TRACK)
        return observe(step, book)

    @hg.graph
    def reader(step: hg.TS[int]) -> hg.TS[object]:
        return observe(step, hg.replay("book", book_type, recordable_id="file", partition_names=(name,)))

    expected = [(7, 13), (None, 17), ("absent",), (23, 19)]
    for graph in (writer, reader):
        with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
            hg.set_record_replay_config("hgraph.persistence.frame")
            assert hg.eval_node(graph, list(range(4))) == expected

    path = tmp_path / "file.book.parquet"
    table = pq.read_table(path)
    column = table.schema.metadata[b"hgraph.recording.withdrawals.v1.column"].decode()
    assert column != name
    assert table[name].to_pylist() == ["x"] * 4

    # A malformed marker in an edited file cannot name a different field.
    table = table.set_column(table.schema.get_field_index(column), table.schema.field(column),
                             pa.array([None, [1, 999], None, None], type=pa.list_(pa.int32())))
    pq.write_table(table, path)
    with hg.GlobalState(), FileBasedDataFrameStorage(tmp_path):
        hg.set_record_replay_config("hgraph.persistence.frame")
        with pytest.raises(RuntimeError, match="unknown bundle field"):
            hg.eval_node(reader, list(range(4)))
