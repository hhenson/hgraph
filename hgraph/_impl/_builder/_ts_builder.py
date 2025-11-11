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
        if sys.exc_info()[0] is None:
            assert len(item._subscribers) == 0, (
                f"Output instance still has subscribers when released, this is a bug. \n"
                f"output belongs to node {item.owning_node}\n"
                f"subscriber nodes are {[i.owning_node if isinstance(i, TimeSeries) else i for i in item._subscribers]}\n\n"
                f"subscriber inputs are {[i for i in item._subscribers if isinstance(i, TimeSeries)]}\n\n"
                f"{item}"
            )
            
            item._parent_or_node = None


class PythonInputBuilder:
    """Mixin base class for Python input builders that provides a standard release_instance implementation."""
    
    def release_instance(self, item):
        if sys.exc_info()[0] is None:
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


@dataclass(frozen=True)
class PythonTSREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TS[...]] - scalar/value reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesValueReferenceInput
        
        return PythonTimeSeriesValueReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSDREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSD[...]] - dict reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesDictReferenceInput
        
        return PythonTimeSeriesDictReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSSREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSS[...]] - set reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesSetReferenceInput
        
        return PythonTimeSeriesSetReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSWREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSW[...]] - window reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesWindowReferenceInput
        
        return PythonTimeSeriesWindowReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSLREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSL[...]] - stores size and child builder for on-demand creation"""
    value_tp: "HgTimeSeriesTypeMetaData"
    value_builder: TSInputBuilder  # Builder for child references
    size_tp: "HgScalarTypeMetaData"
    
    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesListReferenceInput
        from hgraph import Size
        
        size = cast(Size, self.size_tp.py_type).SIZE
        
        ref = PythonTimeSeriesListReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node,
            _size=size,
            _value_builder=self.value_builder
        )
        
        # Don't pre-create items!
        # If peered binding: _items stays None
        # If non-peered: __getitem__ called during wiring, creates all items
        
        return ref
    
    def release_instance(self, item):
        super().release_instance(item)
        # Don't release children - they may be shared or released elsewhere
        # Match generic PythonREFInputBuilder behavior


@dataclass(frozen=True)
class PythonTSBREFInputBuilder(PythonInputBuilder, REFInputBuilder):
    """Builder for REF[TSB[...]] - stores schema and field builders in ordinal order"""
    value_tp: "HgTimeSeriesTypeMetaData"
    schema: "TimeSeriesSchema"
    field_builders: tuple[TSInputBuilder, ...]  # Tuple of builders in schema order, heterogeneous
    
    def make_instance(self, owning_node=None, owning_input=None):
        from hgraph._impl._types._ref import PythonTimeSeriesBundleReferenceInput
        
        ref = PythonTimeSeriesBundleReferenceInput[self.value_tp.py_type](
            _parent_or_node=owning_input if owning_input is not None else owning_node,
            _size=len(self.field_builders),
            _field_builders=list(self.field_builders)
        )
        
        return ref
    
    def release_instance(self, item):
        super().release_instance(item)
        # Don't release children - they may be shared or released elsewhere
        # Match generic PythonREFInputBuilder behavior


@dataclass(frozen=True)
class PythonTSREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TS[...]] output - scalar/value reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesValueReferenceOutput
        
        return PythonTimeSeriesValueReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp,
            _parent_or_node=owning_output if owning_output is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSDREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSD[...]] output - dict reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesDictReferenceOutput
        
        return PythonTimeSeriesDictReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp,
            _parent_or_node=owning_output if owning_output is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSSREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSS[...]] output - set reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesSetReferenceOutput
        
        return PythonTimeSeriesSetReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp,
            _parent_or_node=owning_output if owning_output is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSWREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSW[...]] output - window reference types"""
    value_tp: "HgTimeSeriesTypeMetaData"

    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesWindowReferenceOutput
        
        return PythonTimeSeriesWindowReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp,
            _parent_or_node=owning_output if owning_output is not None else owning_node
        )

    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSLREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSL[...]] output"""
    value_tp: "HgTimeSeriesTypeMetaData"
    size_tp: "HgScalarTypeMetaData"
    
    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesListReferenceOutput
        from hgraph import Size
        
        size = cast(Size, self.size_tp.py_type).SIZE
        
        ref = PythonTimeSeriesListReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp,
            _size=size,
            _parent_or_node=owning_output if owning_output is not None else owning_node
        )
        return ref
    
    def release_instance(self, item):
        super().release_instance(item)


@dataclass(frozen=True)
class PythonTSBREFOutputBuilder(PythonOutputBuilder, REFOutputBuilder):
    """Builder for REF[TSB[...]] output"""
    value_tp: "HgTimeSeriesTypeMetaData"
    schema: "TimeSeriesSchema"
    
    def make_instance(self, owning_node: Node = None, owning_output: TimeSeriesOutput = None):
        from hgraph._impl._types._ref import PythonTimeSeriesBundleReferenceOutput
        
        # Get size from schema
        size = len(self.schema.__meta_data_schema__)
        
        ref = PythonTimeSeriesBundleReferenceOutput[self.value_tp.py_type](
            _tp=self.value_tp,
            _size=size,
            _parent_or_node=owning_output if owning_output is not None else owning_node
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
            HgREFTypeMetaData: lambda: self._make_ref_input_builder(cast(HgREFTypeMetaData, value_tp)),
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
            HgREFTypeMetaData: lambda: self._make_ref_output_builder(cast(HgREFTypeMetaData, value_tp)),
        }.get(type(value_tp), lambda: _throw(value_tp))()
    
    def _make_ref_input_builder(self, ref_tp: HgREFTypeMetaData) -> TSInputBuilder:
        """Create appropriate reference input builder based on what's being referenced"""
        referenced_tp = ref_tp.value_tp
        
        def _make_tsl_ref_builder():
            # REF[TSL[...]] - create specialized builder with child references
            # Don't create REF[REF[...]] - if already REF type, use it directly
            if type(referenced_tp.value_tp) is HgREFTypeMetaData:
                # Already a reference type, use its builder directly
                child_builder = self._make_ref_input_builder(referenced_tp.value_tp)
            else:
                # Wrap in REF
                child_ref_tp = HgREFTypeMetaData(referenced_tp.value_tp)
                child_builder = self._make_ref_input_builder(child_ref_tp)
            return PythonTSLREFInputBuilder(
                value_tp=referenced_tp,
                value_builder=child_builder,
                size_tp=referenced_tp.size_tp
            )
        
        def _make_tsb_ref_builder():
            # REF[TSB[...]] - create specialized builder with field references
            # Iterate schema in order to create list (not dict) - TSB accessed by index
            field_builders_list = []
            for key, field_tp in referenced_tp.bundle_schema_tp.meta_data_schema.items():
                # Don't create REF[REF[...]] - if already REF type, use it directly
                if type(field_tp) is HgREFTypeMetaData:
                    # Already a reference type, use its builder directly
                    field_builders_list.append(self._make_ref_input_builder(field_tp))
                else:
                    # Wrap in REF
                    field_ref_tp = HgREFTypeMetaData(field_tp)
                    field_builders_list.append(self._make_ref_input_builder(field_ref_tp))
            return PythonTSBREFInputBuilder(
                value_tp=referenced_tp,
                schema=referenced_tp.bundle_schema_tp.py_type,
                field_builders=tuple(field_builders_list)  # Tuple for immutability
            )
        
        def _make_ts_ref_builder():
            # REF[TS[...]] - create specialized builder for scalar/value types
            return PythonTSREFInputBuilder(value_tp=referenced_tp)
        
        def _make_tsd_ref_builder():
            # REF[TSD[...]] - create specialized builder for dict types
            return PythonTSDREFInputBuilder(value_tp=referenced_tp)
        
        def _make_tss_ref_builder():
            # REF[TSS[...]] - create specialized builder for set types
            return PythonTSSREFInputBuilder(value_tp=referenced_tp)
        
        def _make_tsw_ref_builder():
            # REF[TSW[...]] - create specialized builder for window types
            return PythonTSWREFInputBuilder(value_tp=referenced_tp)
        
        # Use dictionary lookup for faster dispatch
        return {
            HgTSTypeMetaData: _make_ts_ref_builder,
            HgTSLTypeMetaData: _make_tsl_ref_builder,
            HgTSBTypeMetaData: _make_tsb_ref_builder,
            HgTSDTypeMetaData: _make_tsd_ref_builder,
            HgTSSTypeMetaData: _make_tss_ref_builder,
            HgTSWTypeMetaData: _make_tsw_ref_builder,
        }.get(type(referenced_tp), lambda: PythonREFInputBuilder(value_tp=referenced_tp))()
    
    def _make_ref_output_builder(self, ref_tp: HgREFTypeMetaData) -> TSOutputBuilder:
        """Create appropriate reference output builder based on what's being referenced"""
        referenced_tp = ref_tp.value_tp
        
        # Use dictionary lookup for faster dispatch
        return {
            HgTSTypeMetaData: lambda: PythonTSREFOutputBuilder(value_tp=referenced_tp),
            HgTSLTypeMetaData: lambda: PythonTSLREFOutputBuilder(
                value_tp=referenced_tp,
                size_tp=referenced_tp.size_tp
            ),
            HgTSBTypeMetaData: lambda: PythonTSBREFOutputBuilder(
                value_tp=referenced_tp,
                schema=referenced_tp.bundle_schema_tp.py_type
            ),
            HgTSDTypeMetaData: lambda: PythonTSDREFOutputBuilder(value_tp=referenced_tp),
            HgTSSTypeMetaData: lambda: PythonTSSREFOutputBuilder(value_tp=referenced_tp),
            HgTSWTypeMetaData: lambda: PythonTSWREFOutputBuilder(value_tp=referenced_tp),
        }.get(type(referenced_tp), lambda: PythonREFOutputBuilder(value_tp=referenced_tp))()


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
