from typing import Type, TypeVar, Optional, _GenericAlias


__all__ = ("HgSignalMetaData",)

from hg._types._scalar_type_meta_data import HgScalarTypeMetaData
from hg._types._tsb_meta_data import HgTimeSeriesTypeMetaData


class HgSignalMetaData(HgTimeSeriesTypeMetaData):
    """Parses SIGNAL"""

    @property
    def is_resolved(self) -> bool:
        return True

    @property
    def py_type(self) -> Type:
        from hg._types._time_series_types import SIGNAL
        return SIGNAL

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"]) -> "HgTypeMetaData":
        return self

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        pass # SIGNAL has no possible validation or resolution logic

    @classmethod
    def parse(cls, value) -> Optional["HgTypeMetaData"]:
        from hg._types._time_series_types import SIGNAL
        if value is SIGNAL:
            return HgSignalMetaData()

    def __eq__(self, o: object) -> bool:
        return type(o) is HgSignalMetaData

    def __str__(self) -> str:
        return 'SIGNAL'

    def __repr__(self) -> str:
        return 'HgSignalMetaData()'

    def __hash__(self) -> int:
        from hg._types._time_series_types import SIGNAL
        return hash(SIGNAL)