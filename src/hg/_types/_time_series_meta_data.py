from typing import Optional, TYPE_CHECKING

from hg._types._type_meta_data import HgTypeMetaData
if TYPE_CHECKING:
    from hg._types._time_series_types import TimeSeriesInput, TimeSeriesPushQueue, TimeSeriesPullQueue, TimeSeriesOutput


__all__ = ("HgTimeSeriesTypeMetaData",)


class HgTimeSeriesTypeMetaData(HgTypeMetaData):
    is_scalar = False
    is_atomic = False

    # Begin Node constructor helper methods.

    def create_input(self) -> "TimeSeriesInput":
        raise NotImplemented()

    def create_push_queue(self) -> "TimeSeriesPushQueue":
        raise NotImplemented()

    def create_pull_queue(self) -> "TimeSeriesPullQueue":
        raise NotImplemented()

    def create_output(self) -> "TimeSeriesOutput":
        raise NotImplemented()

    # End of Node constructor helper methods

    @classmethod
    def parse(cls, value) -> Optional["HgTimeSeriesTypeMetaData"]:
        from hg._types._ts_meta_data import HgTSTypeMetaData, HgTSOutTypeMetaData
        from hg._types._ts_type_var_meta_data import HgTsTypeVarTypeMetaData
        from hg._types._tsb_meta_data import HgTimeSeriesSchemaTypeMetaData, HgTSBTypeMetaData
        from hg._types._tsd_meta_data import HgTSDTypeMetaData, HgTSDOutTypeMetaData
        from hg._types._tsl_meta_data import HgTSLTypeMetaData, HgTSLOutTypeMetaData
        from hg._types._tss_meta_data import HgTSSTypeMetaData, HgTSSOutTypeMetaData
        from hg._types._ref_meta_data import HgREFTypeMetaData, HgREFOutTypeMetaData
        from hg._types._ts_signal_meta_data import HgSignalMetaData

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
