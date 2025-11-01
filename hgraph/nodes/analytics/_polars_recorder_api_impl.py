from collections import defaultdict
from datetime import date, datetime
from typing import Generic, Sequence, Any

import polars as pl
from polars import DataFrame

from hgraph import COMPOUND_SCALAR, Frame
from ._recorder_api import RecorderAPI, TableAPI, TableReaderAPI, TableWriterAPI

__all__ = ("PolarsRecorderAPI",)


class PolarsRecorderAPI(RecorderAPI):

    def __init__(self):
        self._table_definitions: dict[str, tuple[COMPOUND_SCALAR, tuple[str, type[date | datetime]]]] = {}
        self._tables: dict[str, dict[str, pl.DataFrame]] = defaultdict(dict)
        self._as_of = datetime.utcnow()

    def has_table_definition(self, table_name: str) -> bool:
        return table_name in self._table_definitions

    def create_or_update_table_definition(
        self,
        table_name: str,
        definition: type[COMPOUND_SCALAR],
        date_column: tuple[str, type[date | datetime]] = ("date", date),
        renamed_fields: dict[str, str] = None,
    ) -> None:
        """For now this just registers the definition."""
        self._table_definitions[table_name] = (definition, date_column)

    def rename_table_definition(self, table_name: str, new_table_name: str):
        self._table_definitions[new_table_name] = self._table_definitions[table_name]
        del self._table_definitions[table_name]
        if table_name in self._tables:
            self._tables[new_table_name] = self._tables[table_name]
            del self._tables[table_name]

    def drop_table(self, table_name: str, variant=None):
        variant = "__default__" if variant is None else variant
        self._table_definitions.pop(table_name, None)
        self._tables[table_name].pop(table_name, None)

    def reset_table(self, table_name: str, variant: str = None):
        variant = "__default__" if variant is None else variant
        self._tables[table_name][variant] = self._tables[table_name][variant].filter(pl.lit(False))

    def _get_or_create_table(self, table_name: str, variant: str = None) -> pl.DataFrame:
        variant = "__default__" if variant is None else variant
        if defn := self._table_definitions.get(table_name):
            if variant not in self._tables[table_name]:
                self._tables[table_name][variant] = pl.DataFrame(
                    [],
                    schema={defn[1][0]: defn[1][1]} | {k: v.py_type for k, v in defn[0].__meta_data_schema__.items()},
                )
        return self._tables[table_name][variant]

    def get_table_writer(self, table_name: str, variant: str = None):
        variant = "__default__" if variant is None else variant
        defn = self._table_definitions[table_name]
        tbl = self._get_or_create_table(table_name, variant)
        return PolarsTableWriterAPI(defn[0], defn[1], self._as_of, table_name, variant, self)

    def get_table_reader(self, table_name: str, variant: str = None):
        variant = "__default__" if variant is None else variant
        defn = self._table_definitions[table_name]
        tbl = self._get_or_create_table(table_name, variant)
        return PolarsTableReaderAPI(tbl, defn[0], defn[1], self._as_of)


class PolarsTableAPI(TableAPI[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):

    def __init__(self, schema: COMPOUND_SCALAR, date_column: tuple[str, type[date | datetime]], as_of: datetime):
        self._schema: COMPOUND_SCALAR = schema
        self._date_column = date_column
        self._as_of = as_of
        self._current_time: datetime | None = None

    @property
    def as_of_time(self) -> datetime:
        return self._as_of

    @property
    def current_time(self) -> datetime:
        return self._current_time

    @current_time.setter
    def current_time(self, value: datetime):
        self._current_time = value

    @property
    def date_column(self) -> tuple[str, type[date | datetime]]:
        return self._date_column

    @property
    def schema(self) -> COMPOUND_SCALAR:
        return self._schema


class PolarsTableWriterAPI(PolarsTableAPI[COMPOUND_SCALAR], TableWriterAPI[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):

    def __init__(
        self,
        schema: COMPOUND_SCALAR,
        date_column: tuple[str, type[date | datetime]],
        as_of: datetime,
        table_name: str,
        variant: str,
        recorder_api: PolarsRecorderAPI,
    ):
        super().__init__(schema, date_column, as_of)
        self._table_name = table_name
        self._variant = variant
        self._current_data: dict[str, list] = self._empty_struct()
        self._current_table: pl.DataFrame | None = None
        self._recorder_api: PolarsRecorderAPI = recorder_api

    def write_columns(self, **kwargs):
        self._current_data[self.date_column[0]].append(self.current_time.date())
        for k in self._schema.__meta_data_schema__:
            self._current_data[k].append(kwargs.get(k, None))

    def write_rows(self, rows: Sequence[tuple[Any, ...] | dict[str, Any]]):
        for row in rows:
            if isinstance(row, dict):
                self.write_columns(**row)
            else:
                self.write_columns(**{k: v for k, v in zip(self._schema.__meta_data_schema__, row)})

    def write_data_frame(self, frame: Frame[COMPOUND_SCALAR]):
        frame = frame.select(pl.lit(self.current_time.date()).alias(self.date_column[0]), pl.all())
        current_frame = self._data_as_frame()
        if len(current_frame) > 0:
            self._current_table = pl.concat([self._data_as_frame(), frame])
        else:
            self._current_table = frame

    def _empty_struct(self) -> dict[str, list]:
        return {self.date_column[0]: []} | {k: [] for k in self._schema.__meta_data_schema__}

    def _data_as_frame(self) -> DataFrame:
        if self._current_table is not None:
            if self._current_data[self.date_column[0]]:
                return pl.concat([self._current_table, self._current_table])
            else:
                return self._current_table
        else:
            self._current_table = pl.DataFrame(self._current_data)
            self._current_data = self._empty_struct()
            return self._current_table

    def flush(self):
        df = pl.concat([self._recorder_api._tables[self._table_name][self._variant], self._data_as_frame()])
        self._recorder_api._tables[self._table_name][self._variant] = df


class PolarsTableReaderAPI(PolarsTableAPI[COMPOUND_SCALAR], TableReaderAPI[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):

    def __init__(
        self,
        existing_table: pl.DataFrame,
        schema: COMPOUND_SCALAR,
        date_column: tuple[str, type[date | datetime]],
        as_of: datetime,
    ):
        super().__init__(schema, date_column, as_of)
        self._table = existing_table

    @property
    def raw_table(self) -> pl.DataFrame:
        return self._table

    @property
    def data_frame(self) -> Frame[COMPOUND_SCALAR]:
        if self.current_time is not None:
            return self._table.filter(pl.col(self.date_column[0]) == self.current_time.date()).drop(self.date_column[0])
        else:
            raise ValueError("No current time set")

    @property
    def next_available_time(self) -> datetime | None:
        tbl = self._table.filter(pl.col(self.date_column[0]) > self.current_time.date())
        if len(tbl) > 0:
            return tbl[self.date_column[0]].min()

    @property
    def previous_available_time(self) -> datetime | None:
        tbl = self._table.filter(pl.col(self.date_column[0]) < self.current_time.date())
        if len(tbl) > 0:
            v = tbl[self.date_column[0]].max()
            if type(v) is date:
                return datetime(v.year, v.month, v.day)
            return v

    @property
    def first_time(self) -> datetime:
        return self._table[self.date_column[0]].min()

    @property
    def last_time(self) -> datetime:
        return self._table[self.date_column[0]].max()
