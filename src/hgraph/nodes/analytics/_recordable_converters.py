import datetime
from abc import abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Generic, Any, TypeVar, Union

from hgraph import TIME_SERIES_TYPE, Frame, COMPOUND_SCALAR, TS, SCALAR, compound_scalar, HgTSTypeMetaData, \
    CompoundScalar, sink_node, STATE, AUTO_RESOLVE, SCALAR_1, TSD, HgTimeSeriesTypeMetaData, HgTSSTypeMetaData, \
    HgTSLTypeMetaData, HgTSBTypeMetaData

from hgraph._types._tsd_meta_data import HgTSDTypeMetaData

from hgraph.nodes.analytics._recorder_api import get_recorder_api, TableWriterAPI, \
    get_recording_label

__all__ = ("get_converter_for", "register_converter")


_CONVERTERS = {}
RECORDABLE_CONVERTER = TypeVar("RECORDABLE_CONVERTER", bound="RecordableConverter")


def _converter_path(ts_meta: HgTimeSeriesTypeMetaData) -> str:
    if type(ts_meta) is HgTSTypeMetaData:
        return "TS"
    if type(ts_meta) is HgTSSTypeMetaData:
        return "TSS"
    if type(ts_meta) is HgTSLTypeMetaData:
        return f"TSL_{_converter_path(ts_meta.value_tp)}"
    if type(ts_meta) is HgTSDTypeMetaData:
        return f"TSD_{_converter_path(ts_meta.value_tp)}"
    if type(ts_meta) is HgTSBTypeMetaData:
        return f"TSB"  # It's too complicated to match TSB with anything more complicated than simple TS values.


def get_converter_for(ts_tp: type[TIME_SERIES_TYPE]) -> type[RECORDABLE_CONVERTER]:
    global _CONVERTERS
    ts_meta = HgTimeSeriesTypeMetaData.parse_type(ts_tp)
    key = _converter_path(ts_meta)
    converter = _CONVERTERS.get(key)
    return converter(ts_tp)


def register_converter(ts_tp: type[TIME_SERIES_TYPE], converter: type[RECORDABLE_CONVERTER]):
    global _CONVERTERS
    ts_meta = HgTimeSeriesTypeMetaData.parse_type(ts_tp)
    key = _converter_path(ts_meta)
    _CONVERTERS[key] = converter


@dataclass
class RecordTsState(CompoundScalar):
    converter: RECORDABLE_CONVERTER | None = None
    writer: TableWriterAPI | None = None


@sink_node
def record_to_table_api(table_id: str, ts: TIME_SERIES_TYPE, tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
              _state: STATE[RecordTsState] = None):
    """
    Records a single value. The value name will be the name of the column in the table (that is not the date columns)
    """
    writer: TableWriterAPI = _state.writer
    writer.current_time = ts.last_modified_time
    converter: RecordableConverter = _state.converter
    if converter.multi_row:
        writer.write_rows(converter.convert_from_ts(ts))
    else:
        writer.write_columns(**converter.convert_from_ts(ts))


@record_to_table_api.start
def record_to_table_api_start(table_id: str, tp: type[TIME_SERIES_TYPE], _state: STATE[RecordTsState]):
    _state.writer: TableWriterAPI = get_recorder_api().get_table_writer(table_id, get_recording_label())
    _state.converter = get_converter_for(tp)
    if _state.converter is None:
        raise ValueError(f"record_to_table_api({table_id}, {tp}) could not find a registered converter")


@record_to_table_api.stop
def record_to_table_api_stop(_state: STATE[RecordTsState]):
    _state.writer.flush()


class RecordableConverter(Generic[TIME_SERIES_TYPE, COMPOUND_SCALAR]):

    def __init__(
            self,
            time_series_tp: type[TIME_SERIES_TYPE],
            table_schema_tp: type[COMPOUND_SCALAR],
            date_column: tuple[str, type[Union[date, datetime]]] = ("date", date),
            multi_row: bool = False,
    ):
        self.time_series_tp = time_series_tp
        self.table_schema_tp = table_schema_tp
        self.date_column = date_column
        self.multi_row = multi_row

    def register_schema(self, table_id: str):
        """
        Registers the schema with the recorder API.
        """
        get_recorder_api().create_or_update_table_definition(table_id, self.table_schema_tp, self.date_column)

    @abstractmethod
    def convert_from_ts(self, ts: TIME_SERIES_TYPE) -> [list[dict] | dict]:
        """
        Converts the time-series into a dictionary suitable for writing to a recordable table.
        :param ts:
        :return:
        """

    @abstractmethod
    def convert_to_ts_value(self, df: Frame[COMPOUND_SCALAR]) -> Any:
        """
        Convert the value of the dataframe into a dictionary suitable for setting the value of the associated time-series
        type.
        """


TS_FRAME_SCHEMA = compound_scalar(value=SCALAR)


class TsRecordableConverter(RecordableConverter[TS[SCALAR], TS_FRAME_SCHEMA]):

    def __init__(self, time_series_tp: type[TIME_SERIES_TYPE], value_key: str = 'value',
                 date_column: tuple[str, type[Union[date, datetime]]] = ("date", date)):
        tp: HgTSTypeMetaData = HgTSTypeMetaData.parse_type(time_series_tp)
        super().__init__(time_series_tp, compound_scalar(**{value_key: tp.value_scalar_tp.py_type}), date_column)
        self._value_key = value_key

    def convert_from_ts(self, ts: TIME_SERIES_TYPE) -> dict:
        return {self._value_key: ts.value}

    def convert_to_ts_value(self, df: Frame[COMPOUND_SCALAR]) -> Any:
        if len(df) > 0:
            return df[self._value_key][0]


register_converter(TS[SCALAR], TsRecordableConverter)


class TsdTsRecordableConverter(RecordableConverter[TSD[SCALAR, TS[SCALAR_1]], TS_FRAME_SCHEMA]):

    def __init__(self,
                 time_series_tp: type[TIME_SERIES_TYPE], key_column: str = 'key', value_column: str = 'value',
                 date_column: tuple[str, type[Union[date, datetime]]] = ("date", date)):
        tp: HgTSDTypeMetaData = HgTSDTypeMetaData.parse_type(time_series_tp)
        super().__init__(time_series_tp,
                         compound_scalar(**{key_column: tp.key_tp.py_type, value_column: tp.value_tp.py_type}),
                         date_column, True)
        self.key_column = key_column
        self.value_column = value_column

    def convert_from_ts(self, ts: TIME_SERIES_TYPE) -> [list[dict] | dict]:
        return [{self.key_column: k, self.value_column: ts_.value} for k, ts_ in ts.modified_items()]

    def convert_to_ts_value(self, df: Frame[COMPOUND_SCALAR]) -> Any:
        return {k: v for k, v in zip(df[self.key_column], df[self.value_column])}


register_converter(TSD[SCALAR, TS[SCALAR_1]], TsdTsRecordableConverter)
