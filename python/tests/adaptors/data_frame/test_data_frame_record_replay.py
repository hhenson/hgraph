from datetime import datetime

import pyarrow as pa
import pytest

import hgraph as hg
from hgraph.adaptors.data_frame import (
    BaseDataFrameStorage,
    FileBasedDataFrameStorage,
    MemoryDataFrameStorage,
    WriteMode,
    replay_data_frame,
    set_data_frame_overrides,
)
from hgraph.adaptors.data_frame._data_frame_record_replay import (
    get_data_frame_record_overrides,
)


def test_memory_storage_and_configuration_live_in_global_state():
    first = pa.table({"time": [datetime(2026, 1, 1)], "value": [1]})
    second = pa.table({"time": [datetime(2026, 1, 2)], "value": [2]})

    with hg.GlobalContext(hg.GlobalState()):
        storage = MemoryDataFrameStorage()
        with storage:
            assert MemoryDataFrameStorage.instance() is storage
            storage.set_schema_info("values", "time", None)
            storage.write_frame("values", first)
            storage.write_frame("values", second, mode=WriteMode.EXTEND)
            assert storage.read_frame(
                "values", start_time=datetime(2026, 1, 2)
            ).equals(second)

        assert MemoryDataFrameStorage.instance() is None
        set_data_frame_overrides(key="values", track_removes=True)
        overrides = get_data_frame_record_overrides("values", "graph")
        assert overrides["track_as_of"] is True
        assert overrides["track_removes"] is True


def test_write_mode_values_and_unsupported_merge_are_explicit():
    assert [mode.value for mode in WriteMode] == [1, 2, 3]
    storage = MemoryDataFrameStorage()
    frame = pa.table({"value": [1]})
    storage.write_frame("values", df=frame)
    with pytest.raises(RuntimeError, match="MERGE"):
        storage.write_frame("values", df=frame, mode=WriteMode.MERGE)


def test_upstream_style_storage_subclass_hooks_are_supported():
    class Storage(BaseDataFrameStorage):
        def __init__(self):
            super().__init__()
            self.frames = {}
            self.schemas = {}

        def _write(self, path, df):
            self.frames[str(path)] = df

        def _read(self, path):
            return self.frames.get(str(path))

        def _get_schema_info(self, path):
            return self.schemas.get(str(path), (None, None))

        def set_schema_info(self, path, date_time_col=None, as_of_col=None):
            self.schemas[str(path)] = (date_time_col, as_of_col)

    storage = Storage()
    frame = pa.table({"value": [1]})
    storage.write_frame("values", df=frame)
    assert storage.frames["values"].equals(frame)
    assert storage.schemas["values"] == ("__date_time__", "__as_of__")


def test_storage_fallback_uses_configured_bitemporal_column_names():
    first_time = datetime(2026, 1, 1)
    second_time = datetime(2026, 1, 2)
    first_revision = datetime(2026, 1, 3)
    second_revision = datetime(2026, 1, 4)
    frame = pa.table(
        {
            "event_time": [first_time, second_time],
            "observed_at": [first_revision, second_revision],
            "value": [1, 2],
        }
    )

    with hg.GlobalContext(hg.GlobalState()):
        hg.set_table_schema_date_key("event_time")
        hg.set_table_schema_as_of_key("observed_at")
        storage = MemoryDataFrameStorage()
        storage.write_frame("values", frame)

        assert storage._get_schema_info("values") == ("event_time", "observed_at")
        assert storage.read_frame(
            "values", start_time=first_time, as_of=first_revision
        ).equals(frame.slice(0, 1))


def test_runtime_storage_write_uses_injected_schema_configuration():
    seen_keys = []

    @hg.sink_node
    def write_frame(value: hg.TS[int], global_state: hg.GlobalState = None):
        date_key = hg.get_table_schema_date_key(global_state)
        as_of_key = hg.get_table_schema_as_of_key(global_state)
        seen_keys.append((date_key, as_of_key))
        storage = MemoryDataFrameStorage.instance(global_state)
        storage.write_frame(
            "runtime",
            pa.table(
                {
                    date_key: [hg.MIN_ST],
                    as_of_key: [hg.MIN_ST],
                    "value": [value.value],
                }
            ),
            global_state=global_state,
        )

    with hg.GlobalState():
        hg.set_table_schema_date_key("event_time")
        hg.set_table_schema_as_of_key("observed_at")
        with MemoryDataFrameStorage() as storage:
            assert hg.eval_node(write_frame, [7]) is None
            assert seen_keys == [("event_time", "observed_at")]
            assert storage.read_frame("runtime", as_of=hg.MIN_ST).equals(
                pa.table(
                    {
                        "event_time": [hg.MIN_ST],
                        "observed_at": [hg.MIN_ST],
                        "value": [7],
                    }
                )
            )


def test_runtime_storage_write_preserves_default_names_without_state_lookup():
    storage = MemoryDataFrameStorage()

    @hg.sink_node
    def write_frame(value: hg.TS[int]):
        storage.write_frame(
            "runtime-default",
            pa.table(
                {
                    "__date_time__": [hg.MIN_ST],
                    "__as_of__": [hg.MIN_ST],
                    "value": [value.value],
                }
            ),
        )

    with hg.GlobalState():
        assert hg.eval_node(write_frame, [7]) is None
        assert storage.read_frame("runtime-default", as_of=hg.MIN_ST).equals(
            pa.table(
                {
                    "__date_time__": [hg.MIN_ST],
                    "__as_of__": [hg.MIN_ST],
                    "value": [7],
                }
            )
        )


def test_file_storage_schema_metadata_is_upstream_interoperable(tmp_path):
    storage = FileBasedDataFrameStorage(tmp_path)
    storage.set_schema_info("values", "when", "revision")

    metadata = (tmp_path / "values.schema").read_text()
    assert metadata == "date_time_col: when\nas_of_col: revision"
    assert storage._get_schema_info("values") == ("when", "revision")

    # Keep already-written hg_cpp artifacts readable during migration to the
    # upstream-labelled metadata format.
    (tmp_path / "values.schema").write_text("when\nrevision")
    assert storage._get_schema_info("values") == ("when", "revision")


def _raw_scalar_frame():
    return pa.table(
        {
            "__date_time__": [hg.MIN_ST, hg.MIN_ST, hg.MIN_ST + hg.MIN_TD,
                              hg.MIN_ST + hg.MIN_TD],
            "__as_of__": [hg.MIN_ST + 20 * hg.MIN_TD,
                           hg.MIN_ST + 10 * hg.MIN_TD,
                           hg.MIN_ST + 20 * hg.MIN_TD,
                           hg.MIN_ST + 10 * hg.MIN_TD],
            "value": [20, 10, 40, 30],
        }
    )


def test_raw_replay_selects_latest_revision_for_explicit_and_default_as_of():
    frame = _raw_scalar_frame()
    cutoff = hg.MIN_ST + 15 * hg.MIN_TD
    assert hg.eval_node(
        replay_data_frame[hg.TS[int]], frame, as_of_time=cutoff
    ) == [10, 30]

    with hg.GlobalState():
        hg.set_as_of(cutoff)
        assert hg.eval_node(replay_data_frame[hg.TS[int]], frame) == [10, 30]


def test_raw_replay_honours_custom_schema_and_empty_frames():
    schema = hg.make_table_schema(
        hg.TS[int], keys=("v",), types=(int,),
        date_key="when", as_of_key="revision"
    )
    frame = pa.table(
        {
            "when": [hg.MIN_ST],
            "revision": [hg.MIN_ST],
            "v": [7],
            "ignored": [99],
        }
    )
    assert hg.eval_node(
        replay_data_frame[hg.TS[int]], frame, schema=schema,
        as_of_time=hg.MIN_ST
    ) == [7]

    canonical_empty = _raw_scalar_frame().slice(0, 0)
    assert hg.eval_node(
        replay_data_frame[hg.TS[int]], canonical_empty,
        as_of_time=hg.MAX_DT
    ) is None


def test_raw_replay_applies_tsb_and_selects_each_tsd_partition():
    bundle_schema = hg.ts_schema(a=hg.TS[int], b=hg.TS[str])
    bundle_frame = pa.table(
        {
            "__date_time__": [hg.MIN_ST],
            "__as_of__": [hg.MIN_ST],
            "a": [1],
            "b": ["one"],
        }
    )
    assert hg.eval_node(
        replay_data_frame[hg.TSB[bundle_schema]], bundle_frame,
        as_of_time=hg.MIN_ST
    ) == [{"a": 1, "b": "one"}]

    dict_frame = pa.table(
        {
            "__date_time__": [hg.MIN_ST, hg.MIN_ST, hg.MIN_ST,
                              hg.MIN_ST + hg.MIN_TD],
            "__as_of__": [hg.MIN_ST + 20 * hg.MIN_TD,
                           hg.MIN_ST + 10 * hg.MIN_TD,
                           hg.MIN_ST + 10 * hg.MIN_TD,
                           hg.MIN_ST + 10 * hg.MIN_TD],
            "__key_1_removed__": [False, False, False, True],
            "__key_1__": ["a", "a", "b", "a"],
            "value": [2, 1, 3, None],
        }
    )
    assert hg.eval_node(
        replay_data_frame[hg.TSD[str, hg.TS[int]]], dict_frame,
        as_of_time=hg.MIN_ST + 15 * hg.MIN_TD
    ) == [{"a": 1, "b": 3}, {"a": hg.REMOVE}]


def _record_ints(storage, n):
    from hgraph.adaptors.data_frame import _data_frame_record_replay as impl

    with hg.GlobalState():
        hg.set_record_replay_model(impl.DATA_FRAME_RECORD_REPLAY)
        with storage:

            @hg.graph
            def g(ts: hg.TS[int]):
                hg.record(ts, key="out", recordable_id="batched")

            with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
                hg.eval_node(g, list(range(n)))
            return storage.read_frame("batched.out")


@pytest.mark.parametrize("batch_rows", [4, 1000])
def test_recording_spanning_several_batches_is_complete_and_ordered(monkeypatch, batch_rows):
    """Recording buffers rows and flushes in batches.

    The tail is flushed at stop, so a threshold that does not divide the row
    count must not truncate; and rows must stay in tick order across the flush
    boundaries. With the default threshold the second case never flushes
    mid-run, which keeps the stop-only path covered too.
    """
    from hgraph.adaptors.data_frame import _data_frame_record_replay as impl

    monkeypatch.setattr(impl, "RECORD_BATCH_ROWS", batch_rows)
    frame = _record_ints(MemoryDataFrameStorage(), 10)

    column = frame["value"]
    # read_frame yields pyarrow or polars depending on what is installed.
    values = column.to_pylist() if hasattr(column, "to_pylist") else column.to_list()
    assert values == list(range(10))


def test_recording_does_not_retain_a_chunk_per_tick():
    """The regression this batching exists for.

    Building a one-row table per tick and extending produced a chunk per tick
    per column, each carrying its own aligned data and validity buffers, so
    retained memory ran to multiples of the payload. Chunk count must track
    flushes, not ticks.
    """
    # Deliberately uses the DEFAULT batch size and stays under it, so this
    # asserts the shape of what gets stored rather than the batching knob -
    # it fails against a per-tick writer even if the knob does not exist.
    storage = MemoryDataFrameStorage()
    _record_ints(storage, 200)

    stored = storage._read("batched.out")
    table = stored if isinstance(stored, pa.Table) else pa.table(stored.to_arrow())
    assert table.num_rows == 200
    assert table.column("value").num_chunks == 1
