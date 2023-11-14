from abc import abstractmethod, ABC
from typing import Generic, Iterable, Mapping, Union, Any, TYPE_CHECKING, Tuple

from frozendict import frozendict

from hg._types._time_series_types import TimeSeriesIterable, TimeSeriesInput, TimeSeriesOutput, K, V, \
    TimeSeriesDeltaValue

if TYPE_CHECKING:
    from hg import HgScalarTypeMetaData, HgTimeSeriesTypeMetaData

__all__ = ("TSD", "TSD_OUT", "TimeSeriesDict", "TimeSeriesDictInput", "TimeSeriesDictOutput", "REMOVE_KEY", "REMOVE_KEY_IF_EXISTS")


REMOVE_KEY = object()
REMOVE_KEY_IF_EXISTS = object()


class TimeSeriesDict(TimeSeriesIterable[K, V], TimeSeriesDeltaValue[frozendict, frozendict], Generic[K, V]):
    """
    A TSD is a collection of time-series values keyed off of a scalar key K.
    """

    def __init__(self, __key_tp__: "HgScalarTypeMetaData", __value_tp__: "HgTimeSeriesTypeMetaData"):
        Generic.__init__(self)
        TimeSeriesDeltaValue.__init__(self)
        TimeSeriesIterable.__init__(self)
        self.__key_tp__: "HgScalarTypeMetaData" = __key_tp__
        self.__value_tp__: "HgTimeSeriesTypeMetaData" = __value_tp__
        self._ts_values: dict[str, Union[TimeSeriesInput, TimeSeriesOutput]] = {}

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        out = super(TimeSeriesDict, cls).__class_getitem__(item)
        if type(item) is not tuple or len(item) != 2:
            return out
        if item != (K, V):
            from hg._types._type_meta_data import HgTypeMetaData
            __key_tp__ = HgTypeMetaData.parse(item[0])
            __value_tp__ = HgTypeMetaData.parse(item[1])
            if not __key_tp__.is_scalar:
                from hg import ParseError
                raise ParseError(
                    f"For TSD[{__key_tp__}][{__value_tp__}], '{__value_tp__}', '{__key_tp__}' must be a SCALAR type")
            if __value_tp__.is_scalar:
                from hg import ParseError
                raise ParseError(
                    f"For TSD[{__key_tp__}][{__value_tp__}], '{__value_tp__}' must be a TIME_SERIES_TYPE type")
            out.__key_tp__ = __key_tp__
            out.__value_tp__ = __value_tp__
            _init = out.__init__
            out.__init__ = lambda *args, **kwargs: _init(out.__key_tp__, out.__value_tp__, *args, **kwargs)
        return out

    def __getitem__(self, item: K) -> V:
        """
        Returns the time series at this index position
        :param item:
        :return:
        """
        return self._ts_values[item]

    def __iter__(self) -> Iterable[K]:
        """
        Iterator over the time-series values
        :return:
        """
        return iter(self._ts_values)

    def keys(self) -> Iterable[K]:
        return self._ts_values.keys()

    def values(self) -> Iterable[V]:
        return self._ts_values.values()

    def items(self) -> Iterable[Tuple[K, V]]:
        return self._ts_values.items()

    def modified_keys(self) -> Iterable[K]:
        return (k for k in self.keys() if self._ts_values[k].modified)

    def modified_values(self) -> Iterable[V]:
        return (v for v in self.values() if v.modified)

    def modified_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, v) for k, v in self.items() if v.modified)

    def valid_keys(self) -> Iterable[K]:
        return (k for k in self.keys() if self._ts_values[k].valid)

    def valid_values(self) -> Iterable[V]:
        return (v for v in self.values() if v.valid)

    def valid_items(self) -> Iterable[Tuple[K, V]]:
        return ((k, v) for k, v in self.items() if v.valid)

    @abstractmethod
    def added_keys(self) -> Iterable[K]:
        """
        Returns the keys that were added since the last tick.
        :return:
        """
        pass

    @abstractmethod
    def added_values(self) -> Iterable[V]:
        """
        Returns the values that were added since the last tick.
        :return:
        """
        pass

    @abstractmethod
    def added_items(self) -> Iterable[Tuple[K, V]]:
        """
        Returns the items that were added since the last tick.
        :return:
        """
        pass

    @abstractmethod
    def removed_keys(self) -> Iterable[K]:
        """
        Returns the keys that were removed since the last tick.
        :return:
        """
        pass

    @abstractmethod
    def removed_values(self) -> Iterable[V]:
        """
        Returns the values that were removed since the last tick.
        :return:
        """
        pass

    @abstractmethod
    def removed_items(self) -> Iterable[Tuple[K, V]]:
        """
        Returns the items that were removed since the last tick.
        :return:
        """
        pass


class TimeSeriesDictInput(TimeSeriesInput, TimeSeriesDict[K, V], ABC, Generic[K, V]):
    """
    The TSD input
    """

    def __init__(self, __key_tp__, __value_tp__):
        Generic.__init__(self)
        TimeSeriesDict.__init__(self, __key_tp__, __value_tp__)
        TimeSeriesInput.__init__(self)


class TimeSeriesDictOutput(TimeSeriesOutput, TimeSeriesDict[K, V], ABC, Generic[K, V]):
    """
    The TSD output
    """

    def __init__(self, __key_tp__, __value_tp__):
        Generic.__init__(self)
        TimeSeriesDict.__init__(self, __key_tp__, __value_tp__)
        TimeSeriesOutput.__init__(self)

    value: frozendict


TSD = TimeSeriesDictInput
TSD_OUT = TimeSeriesDictOutput
