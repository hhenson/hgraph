from dataclasses import dataclass
from typing import Type, TypeVar, Optional

__all__ = ("HgSignalMetaData",)

from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData


@dataclass
class HgSignalMetaData(HgTimeSeriesTypeMetaData):
    """Parses SIGNAL"""

    resolved: bool = False
    value_tp: "HgTimeSeriesTypeMetaData" = None

    @property
    def is_resolved(self) -> bool:
        return self.resolved

    @property
    def py_type(self) -> Type:
        from hgraph._types._time_series_types import SIGNAL

        return SIGNAL

    def matches(self, tp: "HgTypeMetaData") -> bool:
        return isinstance(tp, HgTimeSeriesTypeMetaData)

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        return self

    def build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        # SIGNAL has no possible validation or resolution logic if the input type has no references
        # otherwise we have to have a SIGNAL with dereferenced input inside
        self._tv = TypeVar(f"SIGNAL_{id(self)}")
        if wired_type.has_references:
            resolution_dict[self._tv] = HgSignalMetaData(resolved=True, value_tp=wired_type.dereference())
        else:
            resolution_dict[self._tv] = HgSignalMetaData(resolved=True)

    def build_resolution_dict_from_scalar(
        self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData", value: object
    ):
        """A signal has no meaningful scalar resolution"""

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if (tv := getattr(self, "_tv", None)) and tv in resolution_dict:
            return resolution_dict[tv]
        else:
            return self

    def scalar_type(self) -> "HgScalarTypeMetaData":
        return HgScalarTypeMetaData.parse_type(bool)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._time_series_types import SIGNAL

        if value_tp is SIGNAL:
            return HgSignalMetaData()

    def __eq__(self, o: object) -> bool:
        return type(o) is HgSignalMetaData and self.value_tp == o.value_tp

    def __str__(self) -> str:
        if self.value_tp:
            return f"SIGNAL[{self.value_tp}]"
        else:
            return "SIGNAL"

    def __repr__(self) -> str:
        if self.value_tp:
            return f"HgSignalMetaData(value_tp={self.value_tp})"
        else:
            return "HgSignalMetaData()"

    def __hash__(self) -> int:
        from hgraph._types._time_series_types import SIGNAL

        return hash(SIGNAL) ^ hash(self.value_tp)
