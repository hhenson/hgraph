from datetime import datetime, date

from hgraph import compound_scalar, MIN_TD
from hgraph.nodes.analytics._polars_recorder_api_impl import PolarsRecorderAPI


def test_polars_recorder_api_impl():
    polars_api = PolarsRecorderAPI()

    polars_api.create_or_update_table_definition("test_frame", compound_scalar(qid=str, value=float), )
    assert polars_api.has_table_definition("test_frame")
    assert not polars_api.has_table_definition("test_frame_two")

    polars_api.rename_table_definition("test_frame", "test_frame_two")
    assert polars_api.has_table_definition("test_frame_two")
    assert not polars_api.has_table_definition("test_frame")

    polars_api.drop_table("test_frame_two")
    assert not polars_api.has_table_definition("test_frame_two")


def test_polars_reader():
    polars_api = PolarsRecorderAPI()

    polars_api.create_or_update_table_definition("test_frame", compound_scalar(qid=str, value=float), )

    reader = polars_api.get_table_reader("test_frame")
    assert reader.first_time is None
    assert reader.last_time is None


def test_polars_writer():
    polars_api = PolarsRecorderAPI()

    polars_api.create_or_update_table_definition("test_frame", compound_scalar(qid=str, value=float), )

    writer = polars_api.get_table_writer("test_frame")

    writer.current_time = tm = datetime(1970, 1, 1) + MIN_TD

    writer.write_columns(qid='qid1', value=1.0)

    writer.flush()

    reader = polars_api.get_table_reader("test_frame")
    reader.current_time = tm

    assert reader.first_time == tm.date()
    assert reader.last_time == tm.date()
    assert len(reader.data_frame) == 1
    assert reader.data_frame['qid'][0] == 'qid1'
    assert reader.data_frame['value'][0] == 1.0


def test_polars_writer_data_frame():
    import polars as pl
    polars_api = PolarsRecorderAPI()

    polars_api.create_or_update_table_definition("test_frame", compound_scalar(qid=str, value=float), )

    writer = polars_api.get_table_writer("test_frame")

    writer.current_time = tm = datetime(1970, 1, 1) + MIN_TD

    writer.write_data_frame(pl.DataFrame(dict(qid=['qid1'], value=[1.0])))

    writer.flush()

    reader = polars_api.get_table_reader("test_frame")
    reader.current_time = tm

    assert reader.first_time == tm.date()
    assert reader.last_time == tm.date()
    assert len(reader.data_frame) == 1
    assert reader.data_frame['qid'][0] == 'qid1'
    assert reader.data_frame['value'][0] == 1.0