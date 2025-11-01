from abc import abstractmethod
from typing import Optional, TYPE_CHECKING, TypeVar

from hgraph._types._type_meta_data import HgTypeMetaData
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData

if TYPE_CHECKING:
    from hgraph._types._time_series_types import (
        TimeSeriesInput,
        TimeSeriesPushQueue,
        TimeSeriesPullQueue,
        TimeSeriesOutput,
    )


__all__ = ("HgTimeSeriesTypeMetaData",)


class HgTimeSeriesTypeMetaData(HgTypeMetaData):
    """
    Marker to indicate this represents a time-series type. This sets ``is_scalar`` to False and
    ``is_atomic`` to False.
    """

    is_scalar = False
    is_atomic = False

    # End of Node constructor helper methods

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTimeSeriesTypeMetaData"]:
        from hgraph._types._ts_meta_data import HgTSTypeMetaData, HgTSOutTypeMetaData
        from hgraph._types._tsw_meta_data import HgTSWTypeMetaData, HgTSWOutTypeMetaData
        from hgraph._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
        from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData, HgTSBTypeMetaData
        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData, HgTSDOutTypeMetaData
        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData, HgTSLOutTypeMetaData
        from hgraph._types._tss_meta_data import HgTSSTypeMetaData, HgTSSOutTypeMetaData
        from hgraph._types._ref_meta_data import HgREFTypeMetaData, HgREFOutTypeMetaData
        from hgraph._types._ts_signal_meta_data import HgSignalMetaData
        from hgraph._types._context_meta_data import HgCONTEXTTypeMetaData

        parsers = (
            HgTSTypeMetaData,
            HgTSOutTypeMetaData,
            HgTSWTypeMetaData,
            HgTSWOutTypeMetaData,
            HgTSLTypeMetaData,
            HgTSLOutTypeMetaData,
            HgTSSTypeMetaData,
            HgTSSOutTypeMetaData,
            HgTSDTypeMetaData,
            HgTSDOutTypeMetaData,
            HgTimeSeriesSchemaTypeMetaData,
            HgTSBTypeMetaData,
            HgTsTypeVarTypeMetaData,
            HgREFTypeMetaData,
            HgREFOutTypeMetaData,
            HgSignalMetaData,
            HgCONTEXTTypeMetaData,
        )

        if isinstance(value_tp, parsers):
            return value_tp

        for parser in parsers:
            if meta_data := parser.parse_type(value_tp):
                return meta_data

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        from hgraph._wiring._wiring_port import WiringPort

        if isinstance(value, WiringPort):
            return value.output_type

    @property
    def has_references(self) -> bool:
        return False

    def build_resolution_dict_from_scalar(
        self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData", value: object
    ):
        """
        To be overridden by derived classes
        """
        from hgraph._wiring._wiring_errors import IncorrectTypeBinding

        raise IncorrectTypeBinding(self, wired_type)

    @abstractmethod
    def scalar_type(self) -> "HgScalarTypeMetaData": ...
