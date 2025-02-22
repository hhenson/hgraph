from abc import abstractmethod, ABC
from typing import Generic, Iterable, Union, Any, TYPE_CHECKING, Tuple, Type

from frozendict import frozendict

from hgraph._types._time_series_types import (
    TimeSeriesIterable,
    TimeSeriesInput,
    TimeSeriesOutput,
    K,
    V,
    TimeSeriesDeltaValue,
)
from hgraph._types._typing_utils import Sentinel

if TYPE_CHECKING:
    from hgraph import (
        HgScalarTypeMetaData,
        HgTimeSeriesTypeMetaData,
        SCALAR,
        TimeSeriesSet,
        TimeSeriesReferenceOutput,
        K,
        V,
        ParseError,
    )

__all__ = (
    "TSD",
    "TSD_OUT",
    "TimeSeriesDict",
    "TimeSeriesDictInput",
    "TimeSeriesDictOutput",
    "REMOVE",
    "REMOVE_IF_EXISTS",
    "KEY_SET_ID",
)

REMOVE = Sentinel("REMOVE")
REMOVE_IF_EXISTS = Sentinel("REMOVE_IF_EXISTS")

KEY_SET_ID = "__key_set__"


class TimeSeriesDict(TimeSeriesIterable[K, V], TimeSeriesDeltaValue[frozendict, frozendict], Generic[K, V]):
    """
    A TSD is a dynamic collection of time-series values keyed off of a scalar key K. The dynamic nature of the TSD
    is very powerful, but comes with additional complexity and cost. The TSD is used to process collections where the
    structure is not known at wiring time. It supports the concept of a TSS key-set as well as methods to detect
    the addition and removal of time-series values. These tick over time as well as the elements themselves.
    """

    def __init__(
        self, __key_set__: "TimeSeriesSet", __key_tp__: "HgScalarTypeMetaData", __value_tp__: "HgTimeSeriesTypeMetaData"
    ):
        Generic.__init__(self)
        TimeSeriesDeltaValue.__init__(self)
        TimeSeriesIterable.__init__(self)
        self._key_set: "TimeSeriesSet" = __key_set__
        self.__key_tp__: "HgScalarTypeMetaData" = __key_tp__
        self.__value_tp__: "HgTimeSeriesTypeMetaData" = __value_tp__
        self._ts_values: dict[str, Union[TimeSeriesInput, TimeSeriesOutput]] = {}

    def __class_getitem__(cls, item) -> Any:
        # For now limit to validation of item
        out = super(TimeSeriesDict, cls).__class_getitem__(item)
        if type(item) is not tuple or len(item) != 2:
            return out
        if item != (K, V):
            from hgraph._types._type_meta_data import HgTypeMetaData

            __key_tp__ = HgTypeMetaData.parse_type(item[0])
            __value_tp__ = HgTypeMetaData.parse_type(item[1])
            if not __key_tp__.is_scalar:
                from hgraph import ParseError

                raise ParseError(
                    f"For TSD[{__key_tp__}][{__value_tp__}], '{__value_tp__}', '{__key_tp__}' must be a SCALAR type"
                )
            if __value_tp__.is_scalar:
                from hgraph import ParseError

                raise ParseError(
                    f"For TSD[{__key_tp__}, {__value_tp__}], '{__value_tp__}' must be a TIME_SERIES_TYPE type"
                )
            out.__key_tp__ = __key_tp__
            out.__value_tp__ = __value_tp__
            _init = out.__init__
            out.__init__ = lambda *args, **kwargs: _init(out.__key_tp__, out.__value_tp__, *args, **kwargs)
        return out

    def __len__(self):
        """
        Returns the number of time-series values
        :return:
        """
        return len(self._ts_values)

    def __getitem__(self, item: K) -> V:
        """
        Returns the time series at this index position
        :param item:
        :return:
        """
        if KEY_SET_ID is item:
            return self._key_set
        return self._ts_values[item]

    def get_or_create(self, key: K) -> V:
        """
        Returns the time series at this index position
        If the key does not yet exist, it will be created. In the case of an input, this will create a stub input
        that will only be bound when the corresponding output is created. In the case of an output, the
        output is constructed but will be in an invalid state until it is set with a value.
        """
        if key not in self._ts_values:
            self._create(key)
        return self._ts_values[key]

    def get(self, key: K) -> V | None:
        return self._ts_values.get(key)

    @abstractmethod
    def _create(self, key: K):
        """Implemented by subclasses to create a new time series at this index position"""

    def __iter__(self) -> Iterable[K]:
        """
        Iterator over the time-series values
        :return:
        """
        return iter(self._ts_values)

    def __contains__(self, item):
        return item in self._key_set

    @property
    def key_set(self) -> "TimeSeriesSet":
        return self._key_set

    def keys(self) -> Iterable[K]:
        return self.key_set.values()

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
        :return: The keys that were added since the last tick.
        """
        pass

    @abstractmethod
    def added_values(self) -> Iterable[V]:
        """
        :return: The values that were added since the last tick.
        """
        pass

    @abstractmethod
    def added_items(self) -> Iterable[Tuple[K, V]]:
        """
        :return: The items that were added since the last tick.
        """
        pass

    @abstractmethod
    def removed_keys(self) -> Iterable[K]:
        """
        :return: The keys that were removed since the last tick.
        """
        pass

    @abstractmethod
    def removed_values(self) -> Iterable[V]:
        """
        :return: The values that were removed since the last tick.
        """
        pass

    @abstractmethod
    def removed_items(self) -> Iterable[Tuple[K, V]]:
        """
        :return: The items that were removed since the last tick.
        """
        pass


class TimeSeriesDictInput(TimeSeriesInput, TimeSeriesDict[K, V], ABC, Generic[K, V]):
    """
    The TSD input
    """

    def __init__(self, __key_set__, __key_tp__, __value_tp__):
        Generic.__init__(self)
        TimeSeriesDict.__init__(self, __key_set__, __key_tp__, __value_tp__)
        TimeSeriesInput.__init__(self)

    def __getitem__(self, item):
        return self._ts_values[item]

    @staticmethod
    def from_ts(arg=None, /, **kwargs) -> "TimeSeriesDictInput[K, V]":
        """
        Create an instance of TSD from the kwargs provided.
        """
        tsd: Type[TSD[K, V]] = kwargs.pop("__type__", None)

        if arg is not None:
            if isinstance(arg, dict):
                kwargs.update(arg)
            else:
                raise ParseError(f"Expected a dictionary of values, got {arg}")

        from hgraph import combine

        return combine[tsd](tuple(kwargs.keys()), *kwargs.values())


class TimeSeriesDictOutput(TimeSeriesOutput, TimeSeriesDict[K, V], ABC, Generic[K, V]):
    """
    The TSD output
    """

    def __init__(self, __key_set__, __key_tp__, __value_tp__):
        Generic.__init__(self)
        TimeSeriesDict.__init__(self, __key_set__, __key_tp__, __value_tp__)
        TimeSeriesOutput.__init__(self)

    def __setitem__(self, key: K, value: "SCALAR"):
        self._ts_values[key].value = value

    def __delitem__(self, key: K):
        del self._ts_values[key]

    def pop(self, key: K) -> V:
        """Deletes the key (if it exists) and returns the value."""
        return self._ts_values.pop(key)

    def get_ref(self, key: K, requester: Any) -> "TimeSeriesReferenceOutput":
        """
        Returns a reference time-series output for the key supplied, this will not create the time-series.
        This is useful to subscribe to a time-series where the coming and going of the time-series can be tracked.
        The requester is provided to assist with tracking the reference count. Many requesters can request a key,
        and when they are no longer interested, they will release the reference. We need to ensure that we only remove
        the reference once all requesters have gone. We could just leave them dangling, but that could cause a memory
        leak. When requesting this from the input perspective, the reference is not provided.
        """

    def release_ref(self, key: K, requester: Any) -> None:
        """Releases the reference request"""

    value: frozendict


TSD = TimeSeriesDictInput
TSD_OUT = TimeSeriesDictOutput
