from typing import Optional, TYPE_CHECKING, TypeVar, _GenericAlias

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
                if isinstance(value, _GenericAlias):
                    from hgraph._types._scalar_type_meta_data import FlagMeta
                    from hgraph._types._scalar_type_meta_data import HgTypeFlagsMetaData
                    flags = tuple(arg for arg in value.__args__
                                  if isinstance(arg, FlagMeta) or (
                                      isinstance(arg, TypeVar) and arg.__constraints__ and
                                      all(isinstance(a, FlagMeta) for a in arg.__constraints__)))
                    meta_data.flags = HgTypeFlagsMetaData.parse(flags)
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
