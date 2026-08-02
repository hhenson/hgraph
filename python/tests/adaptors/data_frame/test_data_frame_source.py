from datetime import date, datetime

import numpy as np
import pyarrow as pa
from frozendict import frozendict

from hgraph import MIN_ST, MIN_TD, eval_node
from hgraph.adaptors.data_frame import (
    ArrowDataFrameSource,
    DataConnectionStore,
    DataStore,
    SqlDataFrameSource,
    schema_from_frame,
    ts_of_array_from_data_source,
    ts_of_matrix_from_data_source,
    tsb_from_data_source,
    tsd_k_a_from_data_source,
    tsd_k_b_from_data_source,
    tsd_k_tsd_from_data_source,
    tsd_k_v_from_data_source,
    tsl_from_data_source,
)
from hgraph.adaptors.data_frame._data_source_generators import (
    ts_from_data_source,
    ts_of_frames_from_data_source,
)


class _Rows(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD],
                    "name": ["one", "two", "three"],
                    "value": [1, 2, 3],
                }
            )
        )


def test_arrow_source_scalar_and_bundle_use_native_replay():
    with DataStore():
        assert eval_node(ts_from_data_source, _Rows, "dt", "value") == [1, 2, 3]
        assert eval_node(tsb_from_data_source, _Rows, "dt") == [
            frozendict(name="one", value=1),
            frozendict(name="two", value=2),
            frozendict(name="three", value=3),
        ]


class _Keyed(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST, MIN_ST + MIN_TD],
                    "key": ["a", "b", "a"],
                    "value": [1, 2, 3],
                    "label": ["A", "B", "C"],
                }
            )
        )


class _KeyValue(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(_Keyed().data_frame().drop(["label"]))


def test_arrow_source_keyed_scalar_and_bundle():
    with DataStore():
        assert eval_node(tsd_k_v_from_data_source, _KeyValue, "dt", "key") == [
            frozendict(a=1, b=2),
            frozendict(a=3),
        ]
        assert eval_node(tsd_k_b_from_data_source, _Keyed, "dt", "key") == [
            frozendict(
                a=frozendict(value=1, label="A"),
                b=frozendict(value=2, label="B"),
            ),
            frozendict(a=frozendict(value=3, label="C")),
        ]


class _Arrays(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD],
                    "key": ["a", "b", "c"],
                    "x": [1, 2, 3],
                    "y": [4, 5, 6],
                }
            )
        )


class _UnkeyedArrays(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(_Arrays().data_frame().drop(["key"]))


def test_array_sources_are_packed_before_native_replay():
    with DataStore():
        arrays = eval_node(ts_of_array_from_data_source, _UnkeyedArrays, "dt")
        assert all(isinstance(value, np.ndarray) for value in arrays)
        assert [value.tolist() for value in arrays] == [[1, 4], [2, 5], [3, 6]]

        keyed = eval_node(tsd_k_a_from_data_source, _Arrays, "dt", "key")
        assert all(isinstance(next(iter(value.values())), np.ndarray) for value in keyed)
        assert [
            {key: value.tolist() for key, value in delta.items()}
            for delta in keyed
        ] == [{"a": [1, 4]}, {"b": [2, 5]}, {"c": [3, 6]}]


class _Grouped(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD],
                    "x": [1, 2, 3, 4],
                    "y": [5, 6, 7, 8],
                }
            )
        )


def test_matrix_and_frame_batch_sources():
    with DataStore():
        matrices = eval_node(ts_of_matrix_from_data_source, _Grouped, "dt")
        assert all(isinstance(value, np.ndarray) for value in matrices)
        assert [value.tolist() for value in matrices] == [
            [[1, 5], [2, 6]],
            [[3, 7], [4, 8]],
        ]
        frames = eval_node(ts_of_frames_from_data_source, _Grouped, "dt")
        assert frames[0].equals(pa.table({"x": [1, 2], "y": [5, 6]}))
        assert frames[1].equals(pa.table({"x": [3, 4], "y": [7, 8]}))


def test_source_receives_evaluation_window_once_and_empty_schema_is_valid():
    class _Windowed(ArrowDataFrameSource):
        calls = []

        def __init__(self):
            super().__init__(pa.table({"dt": pa.array([], type=pa.timestamp("us")),
                                       "value": pa.array([], type=pa.int64())}))

        def iter_frames(self, start_time=None, end_time=None):
            self.calls.append((start_time, end_time))
            return iter(())

    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 2)
    _Windowed.calls.clear()
    with DataStore():
        assert eval_node(
            ts_from_data_source,
            _Windowed,
            "dt",
            "value",
            __start_time__=start,
            __end_time__=end,
        ) is None
    assert _Windowed.calls == [(start, end)]


def test_batched_source_is_consumed_once_and_preserves_boundary_groups():
    schema = pa.schema(
        (("dt", pa.timestamp("us")), ("x", pa.int64()), ("y", pa.int64()))
    )

    class _Batched(ArrowDataFrameSource):
        def __init__(self):
            self.calls = 0

        @property
        def schema(self):
            return schema

        def data_frame(self, start_time=None, end_time=None):
            raise AssertionError("the batched path must not materialize data_frame()")

        def iter_frames(self, start_time=None, end_time=None):
            self.calls += 1
            yield pa.table(
                {
                    "dt": [MIN_ST, MIN_ST + MIN_TD],
                    "x": [1, 2],
                    "y": [10, 20],
                },
                schema=schema,
            )
            # The MIN_ST + MIN_TD group straddles the batch boundary. It must
            # still produce one time-series tick, not two events at one time.
            yield pa.table(
                {
                    "dt": [MIN_ST + MIN_TD, MIN_ST + 2 * MIN_TD],
                    "x": [3, 4],
                    "y": [30, 40],
                },
                schema=schema,
            )

    source = _Batched()
    with DataStore() as store:
        store.set_data_source(_Batched, source)
        values = eval_node(tsl_from_data_source, _Batched, "dt")

    assert source.calls == 1
    assert [list(value.values()) for value in values] == [
        [1, 10],
        [3, 30],
        [4, 40],
    ]


class _Pivot(ArrowDataFrameSource):
    def __init__(self):
        super().__init__(
            pa.table(
                {
                    "dt": [MIN_ST, MIN_ST, MIN_ST + MIN_TD],
                    "key": ["a", "a", "b"],
                    "pivot": [1, 2, 1],
                    "value": [10, 20, 30],
                }
            )
        )


def test_nested_dictionary_source_emits_additive_deltas():
    # upstream parity: each tick is an ADDITIVE TSD delta — keys absent from
    # a tick keep their prior state (no REMOVE). The old convert/map_
    # pipeline state-synced and emitted removes; the source now yields the
    # nested dicts as deltas directly.
    with DataStore():
        assert eval_node(tsd_k_tsd_from_data_source, _Pivot, "dt", "key", "pivot") == [
            frozendict(a=frozendict({1: 10, 2: 20})),
            frozendict(b=frozendict({1: 30})),
        ]


def test_date_columns_are_normalised_and_schema_is_arrow_derived():
    class _Dates(ArrowDataFrameSource):
        def __init__(self):
            super().__init__(
                pa.table(
                    {
                        "date": [date(2026, 1, 1), date(2026, 1, 2)],
                        "value": [1, 2],
                    }
                )
            )

    schema = schema_from_frame(_Dates().data_frame())
    assert schema.__annotations__ == {"date": date, "value": int}
    with DataStore():
        assert eval_node(
            ts_from_data_source,
            _Dates,
            "date",
            "value",
            __start_time__=datetime(2026, 1, 1),
            __elide__=True,
        ) == [1, 2]


def test_sql_source_accepts_a_dbapi_result_without_importing_a_database_driver():
    class _Result:
        description = (("date",), ("value",))

        def fetchall(self):
            return [(date(2026, 1, 1), 1)]

    class _Connection:
        def execute(self, query):
            assert query == "select date, value from data"
            return _Result()

    with DataConnectionStore() as connections:
        connections.set_connection("test", _Connection())
        source = SqlDataFrameSource("select date, value from data", "test")
        assert source.data_frame().to_pylist() == [
            {"date": date(2026, 1, 1), "value": 1}
        ]
