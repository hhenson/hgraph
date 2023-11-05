from abc import ABC
from datetime import datetime
from typing import Union, Any, Generic, Optional, get_origin, TypeVar, Type, TYPE_CHECKING, Mapping, KeysView, \
    ItemsView, ValuesView

from more_itertools import nth

from hg._types._schema_type import AbstractSchema
from hg._types._time_series_types import TimeSeriesInput, TimeSeriesOutput, DELTA_SCALAR, TimeSeriesDeltaValue, \
    TimeSeries
from hg._types._type_meta_data import ParseError

if TYPE_CHECKING:
    from hg._types._type_meta_data import HgTypeMetaData
    from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
    from hg import ScalarValue

__all__ = ("TimeSeriesSchema", "TSB", "TSB_OUT", "TS_SCHEMA", "is_bundle", "TimeSeriesBundle", "TimeSeriesBundleInput",
           "TimeSeriesBundleOutput", "UnNamedTimeSeriesSchema")


class TimeSeriesSchema(AbstractSchema):
    """
    Describes a time series schema, this is similar to a data class, and produces a data class to represent
    it's point-in-time value.
    """

    @classmethod
    def _parse_type(cls, tp: Type) -> "HgTypeMetaData":
        from hg._types._time_series_meta_data import HgTimeSeriesTypeMetaData
        return HgTimeSeriesTypeMetaData.parse(tp)


TS_SCHEMA = TypeVar("TS_SCHEMA", bound=TimeSeriesSchema)


class UnNamedTimeSeriesSchema(TimeSeriesSchema):
    """Use this class to create un-named bundle schemas"""

    @classmethod
    def create_resolved_schema(cls, schema: Mapping[str, "HgTimeSeriesTypeMetaData"]) \
            -> Type["UnNamedTimeSeriesSchema"]:
        """Creates a type instance with root class UnNamedTimeSeriesSchema using the schema provided"""
        return cls._create_resolved_class(schema)


class TimeSeriesBundle(TimeSeriesDeltaValue[Union[TS_SCHEMA, dict[str, Any]], Union[TS_SCHEMA, dict[str, Any]]], ABC,
                       Generic[TS_SCHEMA]):
    """
    Represents a non-homogenous collection of time-series values.
    We call this a time-series bundle.
    """

    # TODO: This only works if we are creating new sub-classes (i.e. class MyTSB(schema=...))
    @property
    def __schema__(self) -> TS_SCHEMA:
        return self.__orig_class__.__args__[0]

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        if item is not TS_SCHEMA:
            from hg._types._type_meta_data import HgTypeMetaData
            if HgTypeMetaData.parse(item).is_scalar:
                raise ParseError(
                    f"Type '{item}' must be a TimeSeriesSchema or a valid TypeVar (bound to to TimeSeriesSchema)")
        return super(TimeSeriesBundle, cls).__class_getitem__(item)

    def __init__(self, ts_value: TS_SCHEMA = None):
        """
        Create an instance of a bundle with the value provided. The value is an instance of the appropriate
        TS_SCHEMA type defining this Bundle definition.
        """
        self._ts_value = ts_value

    @property
    def ts_value(self) -> TS_SCHEMA:
        """
        The value of the bundle as it's collection of time-series properties.
        This is different to ``value`` which is instead the point in time representation of the values of the bundle.
        """
        return self._ts_value

    def __getattr__(self, item) -> TimeSeries:
        """
        The time-series value for the property associated to item in the schema
        :param item:
        :return:
        """
        if item in self.__schema__.__meta_data_schema__:
            return getattr(self._ts_value, item)
        else:
            raise ValueError(f"'{item}' is not a valid property of TSB")

    def __getitem__(self, item: Union[int, str]) -> "TimeSeries":
        """
        If item is of type int, will return the item defined by the sequence of the schema. If it is a str, then
        the item as named.
        """
        if type(item) is int:
            return getattr(self, nth(iter(self.__schema__.__meta_data_schema__), item))
        else:
            return getattr(self, item)

    def keys(self) -> KeysView[str]:
        """The keys of the schema defining the bundle"""
        return self._ts_value.keys()

    def items(self) -> ItemsView[str, TimeSeries]:
        """The values of the bundle"""
        return self._ts_value.items()

    def values(self) -> ValuesView[TimeSeries]:
        """The values of the bundle"""
        return self._ts_value.values()


class TimeSeriesBundleInput(TimeSeriesInput, TimeSeriesBundle[TS_SCHEMA], ABC, Generic[TS_SCHEMA]):
    """
    The input form of the bundle
    """


class TimeSeriesBundleOutput(TimeSeriesOutput, TimeSeriesBundle[TS_SCHEMA], ABC, Generic[TS_SCHEMA]):
    """
    The output form of the bundle
    """


TSB = TimeSeriesBundleInput
TSB_OUT = TimeSeriesBundleOutput


def is_bundle(bundle: Union[type, TimeSeriesBundle]) -> bool:
    """Is the value a TimeSeriesBundle type, or an instance of a TimeSeriesBundle"""
    return (origin := get_origin(bundle)) and issubclass(origin, TimeSeriesBundle) or isinstance(bundle,
                                                                                                 TimeSeriesBundle)
