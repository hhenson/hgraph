from typing import Optional, TYPE_CHECKING, TypeVar

from hgraph._types._type_meta_data import HgTypeMetaData
if TYPE_CHECKING:
    from hgraph._types._time_series_types import TimeSeriesInput, TimeSeriesPushQueue, TimeSeriesPullQueue, TimeSeriesOutput


__all__ = ("HgTimeSeriesTypeMetaData",)


class HgTimeSeriesTypeMetaData(HgTypeMetaData):
    is_scalar = False
    is_atomic = False

    # Begin Node constructor helper methods.

    def create_input(self) -> "TimeSeriesInput":
        raise NotImplementedError()

    def create_push_queue(self) -> "TimeSeriesPushQueue":
        raise NotImplementedError()

    def create_pull_queue(self) -> "TimeSeriesPullQueue":
        raise NotImplementedError()

    def create_output(self) -> "TimeSeriesOutput":
        raise NotImplementedError()

    # End of Node constructor helper methods

    @classmethod
    def parse(cls, value) -> Optional["HgTimeSeriesTypeMetaData"]:
        from hgraph._types._ts_meta_data import HgTSTypeMetaData, HgTSOutTypeMetaData
        from hgraph._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
        from hgraph._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData, HgTSBTypeMetaData
        from hgraph._types._tsd_meta_data import HgTSDTypeMetaData, HgTSDOutTypeMetaData
        from hgraph._types._tsl_meta_data import HgTSLTypeMetaData, HgTSLOutTypeMetaData
        from hgraph._types._tss_meta_data import HgTSSTypeMetaData, HgTSSOutTypeMetaData
        from hgraph._types._ref_meta_data import HgREFTypeMetaData, HgREFOutTypeMetaData
        from hgraph._types._ts_signal_meta_data import HgSignalMetaData

        parsers = (HgTSTypeMetaData, HgTSOutTypeMetaData, HgTSLTypeMetaData, HgTSLOutTypeMetaData, HgTSSTypeMetaData,
                  HgTSSOutTypeMetaData, HgTSDTypeMetaData, HgTSDOutTypeMetaData, HgTimeSeriesSchemaTypeMetaData,
                  HgTSBTypeMetaData, HgTsTypeVarTypeMetaData, HgREFTypeMetaData, HgREFOutTypeMetaData, HgSignalMetaData)

        if isinstance(value, parsers):
            return value

        for parser in parsers:
            if meta_data := parser.parse(value):
                return meta_data

    @property
    def has_references(self) -> bool:
        return False

    def dereference(self) -> "HgTimeSeriesTypeMetaData":
        return self

    def build_resolution_dict_from_scalar(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"],
                                          wired_type: "HgTypeMetaData", value: object):
        """
        To be override by derived classes
        """
        from hgraph._wiring._wiring_errors import IncorrectTypeBinding
        raise IncorrectTypeBinding(self, wired_type)
