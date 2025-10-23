from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from sys import exc_info
import sys
from typing import Mapping, cast

from frozendict import frozendict

from hgraph._builder._ts_builder import (
    TSOutputBuilder,
    TimeSeriesBuilderFactory,
    TSInputBuilder,
    TSBInputBuilder,
    TSSignalInputBuilder,
    TSBOutputBuilder,
    TSSOutputBuilder,
    TSSInputBuilder,
    TSLOutputBuilder,
    TSLInputBuilder,
    TSDOutputBuilder,
    TSDInputBuilder,
    REFOutputBuilder,
    REFInputBuilder,
    TSWInputBuilder,
    TSWOutputBuilder,
)
from hgraph._runtime._node import Node
from hgraph._types._time_series_types import TimeSeries
from hgraph._types._tsw_meta_data import HgTSWTypeMetaData
from hgraph._types._context_meta_data import HgCONTEXTTypeMetaData
from hgraph._types._ref_meta_data import HgREFTypeMetaData
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData
from hgraph._types._time_series_meta_data import HgTimeSeriesTypeMetaData
from hgraph._types._time_series_types import TimeSeriesOutput, TimeSeriesInput
from hgraph._types._ts_meta_data import HgTSTypeMetaData, HgTSOutTypeMetaData
from hgraph._types._ts_signal_meta_data import HgSignalMetaData
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData, HgTSDOutTypeMetaData
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tss_meta_data import HgTSSTypeMetaData, HgTSSOutTypeMetaData

__all__ = ("PythonTSOutputBuilder", "PythonTSInputBuilder", "PythonTimeSeriesBuilderFactory")


class PythonOutputBuilder:
    """Mixin base class for Python output builders that provides a standard release_instance implementation."""
    
    def release_instance(self, item: "PythonTimeSeriesOutput"):
        if not sys.exc_info():
            assert len(item._subscribers._subscribers) == 0, (
                f"Output instance still has subscribers when released, this is a bug. \n"
                f"output belongs to node {item.owning_node}\n"
                f"subscriber nodes are {[i.owning_node if isinstance(i, TimeSeries) else i for i in item._subscribers._subscribers]}\n\n"
                f"subscriber inputs are {[i for i in item._subscribers._subscribers if isinstance(i, TimeSeries)]}\n\n"
                f"{item}"
            )
            
            item._parent_or_node = None


class PythonInputBuilder:
    """Mixin base class for Python input builders that provides a standard release_instance implementation."""
    
    def release_instance(self, item):
        if not sys.exc_info():
            assert item._output is None, (
                f"Input instance still has an output reference when released, this is a bug. {item}"
            )

            item._parent_or_node = None


class PythonTSOutputBuilder(PythonOutputBuilder, TSOutputBuilder):

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph import PythonTimeSeriesValueOutput

        return PythonTimeSeriesValueOutput(
            _parent_or_node=owning_output if owning_output is not None else owning_node, _tp=self.value_tp.py_type
        )

    def release_instance(self, item):
        super().release_instance(item)
        item._value = None


class PythonTSInputBuilder(PythonInputBuilder, TSInputBuilder):

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph import PythonTimeSeriesValueInput

        return PythonTimeSeriesValueInput(_parent_or_node=owning_input if owning_input is not None else owning_node)

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonITSWOutputBuilder(PythonOutputBuilder, TSWOutputBuilder):

    _size: int
    _min_size: int

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph import PythonTimeSeriesFixedWindowOutput

        return PythonTimeSeriesFixedWindowOutput(
            _parent_or_node=owning_output if owning_output is not None else owning_node,
            _tp=self.value_tp.py_type,
            _size=self._size,
            _min_size=self._min_size,
        )

    def release_instance(self, item):
        super().release_instance(item)
        item._value = None


@dataclass(frozen=True)
class PythonTTSWOutputBuilder(PythonOutputBuilder, TSWOutputBuilder):

    _size: timedelta
    _min_size: timedelta

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph import PythonTimeSeriesTimeWindowOutput

        return PythonTimeSeriesTimeWindowOutput(
            _parent_or_node=owning_output if owning_output is not None else owning_node,
            _tp=self.value_tp.py_type,
            _size=self._size,
            _min_size=self._min_size,
        )

    def release_instance(self, item):
        super().release_instance(item)
        item._value = None


class PythonTSWInputBuilder(PythonInputBuilder, TSWInputBuilder):

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph import PythonTimeSeriesWindowInput

        return PythonTimeSeriesWindowInput(_parent_or_node=owning_input if owning_input is not None else owning_node)

    def release_instance(self, item):
        super().release_instance(item)


class PythonSignalInputBuilder(PythonInputBuilder, TSSignalInputBuilder):

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph import PythonTimeSeriesSignal

        return PythonTimeSeriesSignal(_parent_or_node=owning_input if owning_input is not None else owning_node)

    def release_instance(self, item):
        super().release_instance(item)
        if item._ts_values is not None:
            for i in item._ts_values:
                self.release_instance(i)


@dataclass(frozen=True)
class PythonTSBOutputBuilder(PythonOutputBuilder, TSBOutputBuilder):
    schema_builders: Mapping[str, TSOutputBuilder] | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(
            self,
            "schema_builders",
            frozendict({k: factory.make_output_builder(v) for k, v in self.schema._schema_items()}),
        )

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph import PythonTimeSeriesBundleOutput

        tsb = PythonTimeSeriesBundleOutput[self.schema](
            self.schema, _parent_or_node=owning_output if owning_output is not None else owning_node
        )
        tsb._ts_values = {k: v.make_instance(owning_output=tsb) for k, v in self.schema_builders.items()}
        return tsb

    def release_instance(self, item):
        super().release_instance(item)
        for k, v in self.schema_builders.items():
            v.release_instance(item._ts_values[k])


@dataclass(frozen=True)
class PythonTSBInputBuilder(PythonInputBuilder, TSBInputBuilder):
    schema_builders: Mapping[str, TSInputBuilder] | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(
            self,
            "schema_builders",
            frozendict({k: factory.make_input_builder(v) for k, v in self.schema._schema_items()}),
        )

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph import PythonTimeSeriesBundleInput

        tsb = PythonTimeSeriesBundleInput[self.schema](self.schema, _parent_or_node=owning_input if owning_input is not None else owning_node)
        tsb._ts_values = {k: v.make_instance(owning_input=tsb) for k, v in self.schema_builders.items()}
        return tsb

    def release_instance(self, item):
        super().release_instance(item)
        for k, v in self.schema_builders.items():
            v.release_instance(item._ts_values[k])


@dataclass(frozen=True)
class PythonTSLOutputBuilder(PythonOutputBuilder, TSLOutputBuilder):
    value_tp: HgTimeSeriesTypeMetaData
    size_tp: HgScalarTypeMetaData
    value_builder: TSOutputBuilder | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(self, "value_builder", factory.make_output_builder(self.value_tp))

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph import Size
        from hgraph._impl._types._tsl import PythonTimeSeriesListOutput

        tsl = PythonTimeSeriesListOutput[self.value_tp.py_type, self.size_tp.py_type](
            __type__=self.value_tp.py_type,
            __size__=self.size_tp.py_type,
            _parent_or_node=owning_output if owning_output is not None else owning_node,
        )
        tsl._ts_values = [
            self.value_builder.make_instance(owning_output=tsl) for _ in range(cast(Size, self.size_tp.py_type).SIZE)
        ]
        return tsl

    def release_instance(self, item):
        super().release_instance(item)
        for value in item._ts_values:
            self.value_builder.release_instance(value)


@dataclass(frozen=True)
class PythonTSLInputBuilder(PythonInputBuilder, TSLInputBuilder):
    value_tp: HgTimeSeriesTypeMetaData
    size_tp: HgScalarTypeMetaData
    value_builder: TSOutputBuilder | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(self, "value_builder", factory.make_input_builder(self.value_tp))

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph import PythonTimeSeriesListInput, Size

        tsl = PythonTimeSeriesListInput[self.value_tp.py_type, self.size_tp.py_type](
            __type__=self.value_tp.py_type,
            __size__=self.size_tp.py_type,
            _parent_or_node=owning_input if owning_input is not None else owning_node,
        )
        tsl._ts_values = [
            self.value_builder.make_instance(owning_input=tsl) for _ in range(cast(Size, self.size_tp.py_type).SIZE)
        ]
        return tsl

    def release_instance(self, item):
        super().release_instance(item)
        for value in item._ts_values:
            self.value_builder.release_instance(value)


@dataclass(frozen=True)
class PythonTSDOutputBuilder(PythonOutputBuilder, TSDOutputBuilder):
    key_tp: "HgScalarTypeMetaData"
    value_tp: "HgTimeSeriesTypeMetaData"
    value_builder: TSOutputBuilder | None = None
    value_reference_builder: TSOutputBuilder | None = None
    key_set_builder: TSOutputBuilder | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(self, "key_set_builder", factory.make_output_builder(HgTSSTypeMetaData(self.key_tp)))
        object.__setattr__(self, "value_builder", factory.make_output_builder(self.value_tp))
        object.__setattr__(self, "value_reference_builder", factory.make_output_builder(self.value_tp.as_reference()))

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._tsd import PythonTimeSeriesDictOutput

        tsd = PythonTimeSeriesDictOutput[self.key_tp.py_type, self.value_tp.py_type](
            __key_set__=self.key_set_builder.make_instance(),
            __key_tp__=self.key_tp,
            __value_tp__=self.value_tp,
            __value_output_builder__=self.value_builder,
            __value_reference_builder__=self.value_reference_builder,
            _parent_or_node=owning_output if owning_output is not None else owning_node,
        )
        return tsd

    def release_instance(self, item):
        item._dispose()
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSDInputBuilder(PythonInputBuilder, TSDInputBuilder):
    key_tp: "HgScalarTypeMetaData"
    value_tp: "HgTimeSeriesTypeMetaData"
    value_builder: TSOutputBuilder | None = None
    key_set_builder: TSOutputBuilder | None = None

    def __post_init__(self):
        factory = TimeSeriesBuilderFactory.instance()
        object.__setattr__(self, "key_set_builder", factory.make_input_builder(HgTSSTypeMetaData(self.key_tp)))
        object.__setattr__(self, "value_builder", factory.make_input_builder(self.value_tp))

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._tsd import PythonTimeSeriesDictInput

        tsd = PythonTimeSeriesDictInput[self.key_tp.py_type, self.value_tp.py_type](
            __key_set__=self.key_set_builder.make_instance(),
            __key_tp__=self.key_tp,
            __value_tp__=self.value_tp,
            _parent_or_node=owning_input if owning_input is not None else owning_node,
        )
        return tsd

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSSOutputBuilder(PythonOutputBuilder, TSSOutputBuilder):

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None) -> TimeSeriesOutput:
        from hgraph import PythonTimeSeriesSetOutput

        return PythonTimeSeriesSetOutput(
            _parent_or_node=owning_output if owning_output is not None else owning_node, _tp=self.value_tp.py_type
        )

    def release_instance(self, item: TimeSeriesOutput):
        super().release_instance(item)
        item._value = None


@dataclass(frozen=True)
class PythonTSSInputBuilder(PythonInputBuilder, TSSInputBuilder):

    def make_instance(self, owning_node: Node = None, owning_input: TimeSeriesInput = None) -> TimeSeriesInput:
        from hgraph import PythonTimeSeriesSetInput

        return PythonTimeSeriesSetInput(_parent_or_node=owning_input if owning_input is not None else owning_node)

    def release_instance(self, item: TimeSeriesInput):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesReferenceOutput

        ref = PythonTimeSeriesReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp, _parent_or_node=owning_output if owning_output is not None else owning_node
        )
        return ref

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    value_tp: "HgTimeSeriesTypeMetaData"
    value_builder: TSOutputBuilder | None = None

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesReferenceInput

        ref = PythonTimeSeriesReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node
        )
        return ref

    def release_instance(self, item):
        super().release_instance(item)


def _throw(value_tp):
    if type(value_tp) in (HgTSOutTypeMetaData, HgTSDOutTypeMetaData, HgTSSOutTypeMetaData):
        raise TypeError(
            f"An output type was detected in the wiring input signature ({value_tp})\n"
            "Check input name, consider using '_output' for the argument name."
        )
    raise TypeError(f"Got unexpected value type {type(value_tp)}: {value_tp}")


class PythonTimeSeriesBuilderFactory(TimeSeriesBuilderFactory):

    def make_input_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSInputBuilder:
        return {
            HgTSTypeMetaData: lambda: PythonTSInputBuilder(value_tp=cast(HgTSTypeMetaData, value_tp).value_scalar_tp),
            HgTSWTypeMetaData: lambda: PythonTSWInputBuilder(value_tp=value_tp.value_scalar_tp),
            HgTSBTypeMetaData: lambda: PythonTSBInputBuilder(
                schema=cast(HgTSBTypeMetaData, value_tp).bundle_schema_tp.py_type
            ),
            HgSignalMetaData: lambda: PythonSignalInputBuilder(),
            HgTSSTypeMetaData: lambda: PythonTSSInputBuilder(
                value_tp=cast(HgTSSTypeMetaData, value_tp).value_scalar_tp
            ),
            HgTSLTypeMetaData: lambda: PythonTSLInputBuilder(
                value_tp=cast(HgTSLTypeMetaData, value_tp).value_tp, size_tp=cast(HgTSLTypeMetaData, value_tp).size_tp
            ),
            HgTSDTypeMetaData: lambda: PythonTSDInputBuilder(
                key_tp=cast(HgTSDTypeMetaData, value_tp).key_tp, value_tp=cast(HgTSDTypeMetaData, value_tp).value_tp
            ),
            HgREFTypeMetaData: lambda: PythonREFInputBuilder(value_tp=cast(HgREFTypeMetaData, value_tp).value_tp),
            HgCONTEXTTypeMetaData: lambda: self.make_input_builder(value_tp.ts_type),
        }.get(type(value_tp), lambda: _throw(value_tp))()

    def make_output_builder(self, value_tp: HgTimeSeriesTypeMetaData) -> TSOutputBuilder:
        return {
            HgTSTypeMetaData: lambda: PythonTSOutputBuilder(value_tp=value_tp.value_scalar_tp),
            HgTSWTypeMetaData: lambda: _make_buff_output(value_tp),
            HgTSBTypeMetaData: lambda: PythonTSBOutputBuilder(schema=value_tp.bundle_schema_tp.py_type),
            HgTSSTypeMetaData: lambda: PythonTSSOutputBuilder(value_tp=value_tp.value_scalar_tp),
            HgTSLTypeMetaData: lambda: PythonTSLOutputBuilder(
                value_tp=cast(HgTSLTypeMetaData, value_tp).value_tp, size_tp=cast(HgTSLTypeMetaData, value_tp).size_tp
            ),
            HgTSDTypeMetaData: lambda: PythonTSDOutputBuilder(
                key_tp=cast(HgTSDTypeMetaData, value_tp).key_tp, value_tp=cast(HgTSDTypeMetaData, value_tp).value_tp
            ),
            HgREFTypeMetaData: lambda: PythonREFOutputBuilder(value_tp=cast(HgREFTypeMetaData, value_tp).value_tp),
        }.get(type(value_tp), lambda: _throw(value_tp))()


def _make_buff_output(meta_data: HgTSWTypeMetaData) -> TSOutputBuilder:
    return (
        PythonITSWOutputBuilder(
            value_tp=meta_data.value_scalar_tp,
            _size=meta_data.size_tp.py_type.SIZE,
            _min_size=meta_data.min_size_tp.py_type.SIZE,
        )
        if meta_data.size_tp.py_type.FIXED_SIZE
        else PythonTTSWOutputBuilder(
            value_tp=meta_data.value_scalar_tp,
            _size=meta_data.size_tp.py_type.TIME_RANGE,
            _min_size=meta_data.min_size_tp.py_type.TIME_RANGE,
        )
    )


def _make_buff_input(meta_data: HgTSWTypeMetaData) -> TSInputBuilder:
    return (
        PythonTSWInputBuilder(
            value_tp=meta_data.value_scalar_tp,
            _size=meta_data.size_tp.py_type.SIZE,
            _min_size=meta_data.min_size_tp.py_type.SIZE,
        )
        if meta_data.size_tp.py_type.FIXED_SIZE
        else PythonTSWInputBuilder(
            value_tp=meta_data.value_scalar_tp,
            _size=meta_data.size_tp.py_type.TIME_RANGE,
            _min_size=meta_data.min_size_tp.py_type.TIME_RANGE,
        )
    )
