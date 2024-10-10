from abc import ABC
from typing import Any, Generic, Iterable, TYPE_CHECKING, Tuple, Generator

from frozendict import frozendict

from hgraph._types._scalar_type_meta_data import HgTypeMetaData
from hgraph._types._time_series_types import K, V
from hgraph._types._scalar_types import SIZE, Size, STATE
from hgraph._types._time_series_types import (
    TimeSeriesIterable,
    TimeSeriesInput,
    TimeSeriesOutput,
    TIME_SERIES_TYPE,
    TimeSeriesDeltaValue,
)

if TYPE_CHECKING:
    from hgraph._wiring._wiring_context import WiringContext

__all__ = ("TSL", "TSL_OUT", "TimeSeriesList", "TimeSeriesListInput", "TimeSeriesListOutput")


class TimeSeriesList(
    TimeSeriesIterable[int, TIME_SERIES_TYPE],
    TimeSeriesDeltaValue[tuple, dict[int, Any]],
    Generic[TIME_SERIES_TYPE, SIZE],
):
    """
    Represents a linear collection of time-series inputs.
    Think of this as a list of time-series values.
    """

    def __init__(self, __type__: TIME_SERIES_TYPE, __size__: SIZE):
        self.__type__: TIME_SERIES_TYPE = __type__
        self.__size__: SIZE = __size__
        self._ts_values: list[TIME_SERIES_TYPE] = [None] * __size__.SIZE if __size__.FIXED_SIZE else []

    def __class_getitem__(cls, item) -> Any:
        # For now, limit to validation of item
        is_tuple = type(item) is tuple
        if is_tuple and len(item) != 2:
            item = (item[0] if len(item) == 1 else TIME_SERIES_TYPE), SIZE
        out = super(TimeSeriesList, cls).__class_getitem__(item)
        if item != (TIME_SERIES_TYPE, SIZE):
            from hgraph._types._type_meta_data import HgTypeMetaData

            if HgTypeMetaData.parse_type(item[0]).is_scalar:
                from hgraph import ParseError

                raise ParseError(
                    f"Type '{item[0]}' must be a TimeSeriesSchema or a valid TypeVar (bound to TimeSeriesSchema)"
                )
        return out

    def __len__(self) -> int:
        return len(self._ts_values)

    def __getitem__(self, item: int) -> TIME_SERIES_TYPE:
        """
        Returns the time series at this index position
        """
        return self._ts_values[item]

    def __iter__(self) -> Iterable[TIME_SERIES_TYPE]:
        """
        Iterator over the time-series values
        """
        return iter(self._ts_values)

    def key_from_value(self, value: TIME_SERIES_TYPE) -> int:
        """
        They key for a given value.
        """
        return self._ts_values.index(value)

    def keys(self) -> Iterable[K]:
        """The list of indices (effectively ``range(len(self._ts_values)``)"""
        return range(len(self._ts_values))

    def values(self) -> Iterable[V]:
        return self._ts_values

    def items(self) -> Iterable[Tuple[K, V]]:
        return enumerate(self._ts_values)

    def modified_keys(self) -> Iterable[K]:
        """
        The indices that have been modified in this engine cycle.
        """
        return (i for i in self.keys() if self._ts_values[i].modified)

    def modified_values(self) -> Iterable[V]:
        """The values that have been modified in this engine cycle."""
        return (v for v in self.values() if v.modified)

    def modified_items(self) -> Iterable[Tuple[K, V]]:
        """The pair of index and value for the values that have been modified in this engine cycle."""
        return ((i, v) for i, v in self.items() if v.modified)

    def valid_keys(self) -> Iterable[K]:
        """The indices containing valid time-series values."""
        return (i for i in self.keys() if self._ts_values[i].valid)

    def valid_values(self) -> Iterable[V]:
        """The valid time-series values."""
        return (v for v in self.values() if v.valid)

    def valid_items(self) -> Iterable[Tuple[K, V]]:
        """The indices and value pairs containing valid time-series values."""
        return ((i, v) for i, v in self.items() if v.valid)


class TimeSeriesListInput(
    TimeSeriesList[TIME_SERIES_TYPE, SIZE], TimeSeriesInput, ABC, Generic[TIME_SERIES_TYPE, SIZE]
):
    """
    The input of a time series list.
    """

    @classmethod
    def from_ts(cls, *args, tp=TIME_SERIES_TYPE, size=SIZE, __type__=None) -> "TimeSeriesList[TIME_SERIES_TYPE, SIZE]":
        """To force a Type (to ensure input types are as expected, then provide __type__ and / or __size__"""
        if len(args) == 1 and isinstance(args[0], (Generator, list, tuple)):
            args = [arg for arg in args[0]]
        if __type__ is not None:  # remove tp and size args once `combine` is done and from_ts is not used directly
            tp = __type__.value_tp.py_type
            size = __type__.size_tp.py_type
        args, size_, tp_ = cls._validate_inputs(*args, tp_=tp, size_=size)
        fn_details = TimeSeriesListInput.from_ts.__code__
        from hgraph import (
            WiringNodeSignature,
            WiringNodeType,
            SourceCodeDetails,
            HgTSLTypeMetaData,
            HgTimeSeriesTypeMetaData,
            HgAtomicType,
        )
        from hgraph import WiringNodeInstance

        hg_tp_ = HgTimeSeriesTypeMetaData.parse_type(tp_)
        args_ = tuple(f"ts_{i}" for i in range(size_.SIZE))

        wiring_node_signature = WiringNodeSignature(
            node_type=WiringNodeType.STUB,
            name=f"TSL[{tp_}, {size_}].from_ts",
            args=args_,
            defaults=frozendict(),
            input_types=frozendict({k: hg_tp_ for k in args_}),
            output_type=HgTSLTypeMetaData(hg_tp_, HgAtomicType.parse_type(size_)),
            src_location=SourceCodeDetails(fn_details.co_filename, fn_details.co_firstlineno),
            active_inputs=None,
            valid_inputs=None,
            all_valid_inputs=None,
            context_inputs=None,
            unresolved_args=frozenset(),
            time_series_args=frozenset(args_),
        )
        from hgraph._wiring._wiring_node_class._stub_wiring_node_class import NonPeeredWiringNodeClass
        from hgraph._wiring._wiring_port import TSLWiringPort

        wiring_node = NonPeeredWiringNodeClass(wiring_node_signature, lambda *args, **kwargs: None)

        from hgraph._wiring._wiring_node_instance import create_wiring_node_instance
        from hgraph._wiring._context_wiring import TimeSeriesContextTracker
        from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext

        wiring_node_instance = create_wiring_node_instance(
            node=wiring_node,
            resolved_signature=wiring_node_signature,
            inputs=frozendict({k: v for k, v in zip(args_, args)}),
        )
        return TSLWiringPort(wiring_node_instance, tuple())

    @classmethod
    def _validate_inputs(cls, *args, tp_=None, size_=None):
        if not args:
            with _from_ts_wiring_context(tp_, size_):
                from hgraph._wiring._wiring_errors import NoTimeSeriesInputsError

                raise NoTimeSeriesInputsError()
        if size_ is SIZE:
            size_ = Size[len(args)]
        else:
            if size_.FIXED_SIZE and len(args) != size_.SIZE:
                from hgraph import ParseError

                with _from_ts_wiring_context(tp_, size_):
                    from hgraph._wiring._wiring_errors import CustomMessageWiringError

                    raise CustomMessageWiringError(
                        f"Incorrect number of inputs provided, declared as {size_} but received {len(args)} inputs"
                    )
        inputs = list(args)
        if tp_ is TIME_SERIES_TYPE:  # Check the types all match, if they do we have a resolved type!
            # Try resolve the input types
            tp_ = inputs[0].output_type.dereference()
        else:
            from hgraph import HgTimeSeriesTypeMetaData

            tp_ = HgTimeSeriesTypeMetaData.parse_type(tp_)

        for i, v in enumerate(inputs):
            from hgraph._wiring._wiring_port import WiringPort

            if isinstance(v, WiringPort):
                if not tp_.matches(v.output_type.dereference()):
                    with _from_ts_wiring_context(tp_, size_):
                        from hgraph._wiring._wiring_errors import CustomMessageWiringError

                        raise CustomMessageWiringError(
                            f"Input types must be the same type, expected: {tp_} but found"
                            f" [{ ', '.join(str(v.output_type) for v in args)}]"
                        )
            elif tp_.scalar_type().matches(HgTypeMetaData.parse_value(v)):
                from hgraph import const

                inputs[i] = const(v, tp=tp_.py_type)
            elif v is None:
                from hgraph.nodes import nothing

                inputs[i] = nothing(tp=tp_.py_type)
            else:
                from hgraph import IncorrectTypeBinding
                from hgraph import WiringContext
                from hgraph import STATE

                with WiringContext(current_arg=i, current_signature=STATE(signature=f"TSL[{tp_}].from_ts(...)")):
                    raise IncorrectTypeBinding(expected_type=tp_, actual_type=v)

        tp_ = tp_.py_type  # This would be in HgTypeMetaData format, need to put back into python type form
        return inputs, size_, tp_


def _from_ts_wiring_context(tp_, size_) -> "WiringContext":
    from hgraph._wiring._wiring_context import WiringContext

    return WiringContext(
        current_signature=STATE(signature=f"TSL[{tp_}, {size_}].from_ts(**kwargs) -> TSL[{tp_}, {size_}]")
    )


class TimeSeriesListOutput(
    TimeSeriesList[TIME_SERIES_TYPE, SIZE], TimeSeriesOutput, ABC, Generic[TIME_SERIES_TYPE, SIZE]
):
    """
    The output of the time series list
    """

    value: TimeSeriesOutput


TSL = TimeSeriesListInput
TSL_OUT = TimeSeriesListOutput
