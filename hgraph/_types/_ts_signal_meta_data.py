from typing import Type, TypeVar, Optional

__all__ = ("HgSignalMetaData",)

from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData


class HgSignalMetaData(HgTimeSeriesTypeMetaData):
    """Parses SIGNAL"""

    @property
    def is_resolved(self) -> bool:
        return True

    @property
    def py_type(self) -> Type:
        from hgraph._types._time_series_types import SIGNAL

        return SIGNAL

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return isinstance(tp, HgTimeSeriesTypeMetaData)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        return self

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        pass  # SIGNAL has no possible validation or resolution logic

    def build_resolution_dict_from_scalar(
        self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData", value: object
    ):
        """A signal has no meaningful scalar resolution"""

    def scalar_type(self) -> "HgScalarTypeMetaData":
        return HgScalarTypeMetaData.parse_type(bool)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._time_series_types import SIGNAL

        if value_tp is SIGNAL:
            return HgSignalMetaData()

    def __eq__(self, o: object) -> bool:
        return type(o) is HgSignalMetaData

    def __str__(self) -> str:
        return "SIGNAL"

    def __repr__(self) -> str:
        return "HgSignalMetaData()"

    def __hash__(self) -> int:
        from hgraph._types._time_series_types import SIGNAL

        return hash(SIGNAL)
