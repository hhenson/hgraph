
"""
Write a graph by creating a file for storing values at a TS value.
This requires:
    * Naming convention for each time-series entry
    * Writer for fixed and dynamic sized properties.
"""
import os
from datetime import datetime
from pathlib import Path

from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
from hgraph._types._scalar_types import STATE


class GraphWriter:

    def __init__(self, path: Path, filename: str):
        self.path: Path = path
        self.filename: str = filename
        self.ndx = os.open(path.joinpath(f"{filename}.ndx"), os.O_RDWR | os.O_CREAT)
        self.meta = os.open(path.joinpath(f"{filename}.meta"), os.O_RDWR | os.O_CREAT)
        self.data = os.open(path.joinpath(f"{filename}.dat"), os.O_RDWR | os.O_CREAT)

    def register_time_series(self, tm: datetime,
                             ts_id: tuple[int | str, ...],
                             tp: HgScalarTypeMetaData) -> int:
        """
        Registers the time-series with the writer. This is when the time series
        became available, (for example if this is a dynamic time-series or
        part of a nested graph).
        :param tm: The time-series
        :param ts_id:
        :param tp:
        :return: An identifier to reference the time-series when later recording events.
        """

    def de_register_time_series(self, tm: datetime, id: int):
        """
        Indicates that the time-series associated with the given id is no longer valid.
        :param tm:
        :param id:
        :return:
        """

    def register_state(self, tm: datetime, node_id: tuple[int, ...], state: HgScalarTypeMetaData) -> int:
        """
        Registers the state to be recorded.
        :param tm: The time when the state is to be available (start time of the graph)
        :param node_id:
        :param state:
        :return: An identifier to use to store the state.
        """

    def de_register_state(self, tm: datetime, id_: int):
        """
        :param tm:
        :param id:
        :return:
        """

    def write_time_series(self, tm: datetime, id_: int, value):
        """
        Writes the value to the store.
        :param tm:
        :param id_:
        :param value:
        :return:
        """

    def write_state(self, tm: datetime, id_: int, value: STATE):
        """
        Writes teh state to the store
        :param tm:
        :param id_:
        :param value:
        :return:
        """

