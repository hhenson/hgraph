import re
import sys
from abc import ABCMeta
from collections import defaultdict
from types import GenericAlias
from typing import Union, Sequence, Mapping, Set

import polars as pl
from frozendict import frozendict
from multimethod import multimethod

from hgraph import Node, Graph, PythonTimeSeriesValueInput, PythonTimeSeriesValueOutput, \
    PythonTimeSeriesReferenceOutput, PythonTimeSeriesReferenceInput, \
    TimeSeriesList, TimeSeriesDict, TimeSeriesBundle, TimeSeriesSet, TimeSeriesInput, TimeSeriesOutput, \
    PythonNestedNodeImpl, PythonTsdMapNodeImpl, PythonServiceNodeImpl, PythonReduceNodeImpl, PythonSwitchNodeImpl, \
    PythonTryExceptNodeImpl, HgTSBTypeMetaData, PythonPushQueueNodeImpl, CompoundScalar, HgTSLTypeMetaData, \
    HgTSDTypeMetaData, HgCompoundScalarType, MIN_DT, HgTypeMetaData, EvaluationEngine, \
    EvaluationLifeCycleObserver, TimeSeries, Builder, TimeSeriesReference
from hgraph._impl._runtime._component_node import PythonComponentNodeImpl
from hgraph._impl._runtime._mesh_node import PythonMeshNodeImpl

STR_ID_SYMBOLS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
STR_ID_NUMBERS = [STR_ID_SYMBOLS.find(chr(i)) for i in range(128)]


def base62(i: int) -> str:
    return f"{STR_ID_SYMBOLS[i // 3844]}{STR_ID_SYMBOLS[i // 62 % 62]}{STR_ID_SYMBOLS[i % 62]}"


def str_node_id(id: tuple[int, ...]) -> str:
    return ''.join(base62(i) for i in id)


def node_id_from_str(s: str) -> tuple:
    return tuple(
        STR_ID_NUMBERS[ord(s[i])] * 3844 + STR_ID_NUMBERS[ord(s[i + 1])] * 62 + STR_ID_NUMBERS[ord(s[i + 2])] for i in
        range(0, len(s), 3))


# These are visitors that are used to extract information from the graph objects to display in the inspector

@multimethod
def format_name(value, key="?"):
    return str(key)


@format_name.register
def format_name_graph(graph: "Graph", key=None) -> str:
    if key:
        return str(key)

    return graph.label or "_"


@format_name.register
def format_name_node(node: "Node", key=None) -> str:
    long_name = (
        f"{node.signature.wiring_path_name}.{node.signature.name}"
        f"{(':' + node.signature.label) if node.signature.label else ''}"
    )

    if node.graph.parent_node:
        return long_name.replace(node.graph.parent_node.signature.wiring_path_name + ".", "")
    else:
        return long_name


@multimethod
def format_value(value):
    if isinstance(value, str):
        s = value
    elif isinstance(value, (Mapping, Set, Sequence)):
        return f"{len(value)} items"
    else:
        s = repr(value)

    if len(s) > 256:
        s = s[:253] + "..."
    return s


@format_value.register
def format_value_graph(value: Graph):
    return format_name(value)


@format_value.register
def format_value_node(value: Node):
    if (out := value.output) is not None:
        return format_value(out)
    else:
        return "-"


@format_value.register
def format_value_python_push_queue_node_impl(value: PythonPushQueueNodeImpl):
    return f"{value.messages_in_queue} items in the queue"


@format_value.register
def format_value_python_time_series_value_output(value: Union[PythonTimeSeriesValueOutput]):
    if value.valid:
        return format_value(value.value)
    else:
        return "INVALID"


@format_value.register
def format_value_python_time_series_value_input(value: Union[PythonTimeSeriesValueInput]):
    if value.output is not None:
        return format_value(value.output)
    else:
        return "INVALID"


@format_value.register
def format_value_python_time_series_reference_output(value: Union[PythonTimeSeriesReferenceOutput, PythonTimeSeriesReferenceInput]):
    if value.valid:
        ref = value.value
        if ref.is_valid:
            if not ref.has_output and ref.items:
                return f"{len(ref.items)} items"
            else:
                return str(ref)
    else:
        return "INVALID"


@format_value.register
def format_value_python_time_series_reference(value: TimeSeriesReference):
    if value.is_valid:
        if not value.has_output and value.items:
            return f"{len(value.items)} items"
        else:
            return str(value)
    else:
        return "INVALID"


@format_value.register
def format_value_time_series_list(value: Union[TimeSeriesList, TimeSeriesSet, TimeSeriesBundle, TimeSeriesDict]):
    if value.valid:
        return f"{len(value)} items"
    else:
        return "INVALID"


@format_value.register
def format_value_polars_data_frame(value: pl.DataFrame):
    return f"Frame {value.width}x{value.height}"


@multimethod
def format_modified(value):
    return None


@format_modified.register
def format_timestamp_node(value: Node):
    return max(
        value.input.last_modified_time if value.input else MIN_DT,
        value.output.last_modified_time if value.output else MIN_DT,)


@format_modified.register
def format_timestamp_node(value: PythonNestedNodeImpl):
    return value._last_evaluation_time


@format_modified.register
def format_timestamp_graph(value: Graph):
    return value._last_evaluation_time


@format_modified.register
def format_timestamp_time_series_output(value: Union[TimeSeriesOutput, TimeSeriesInput]):
    if value.valid:
        return value.last_modified_time
    else:
        return None


@multimethod
def format_scheduled(value):
    return None


@format_scheduled.register
def format_scheduled_node(value: Node):
    return value.graph._schedule[value.node_ndx]


@format_scheduled.register
def format_scheduled_graph(value: Graph):
    if value.parent_node is not None:
        return format_scheduled(value.parent_node)


@multimethod
def enum_items(value):
    yield from ()


@enum_items.register
def enum_items_mapping(value: Mapping):
    yield from value.items()


@enum_items.register
def enum_items_set(value: Set):
    yield from ((k, k) for k in value)


@enum_items.register
def enum_items_sequence(value: Sequence):
    if not isinstance(value, str):
        yield from enumerate(value)
    else:
        yield from ()


@enum_items.register
def enum_items_compound_scalar(value: CompoundScalar):
    yield from ((k, v) for k, v in value.to_dict().items())


@enum_items.register
def enum_items_python_time_series_value_output(value: Union[PythonTimeSeriesValueOutput, PythonTimeSeriesValueInput]):
    if value.valid:
        yield from enum_items(value.value)
    yield from ()


@enum_items.register
def enum_items_python_time_series_reference_output(value: Union[PythonTimeSeriesReferenceOutput, PythonTimeSeriesReferenceInput]):
    if value.valid:
        ref = value.value
        if ref.is_valid:
            if not ref.has_output and ref.items:
                yield from enumerate(ref.items)
    yield from ()


@enum_items.register
def enum_items_time_series_collection(value: Union[TimeSeriesList, TimeSeriesBundle, TimeSeriesDict]):
    if value.valid:
        yield from value.items()
    yield from ()


@enum_items.register
def enum_items_time_series_set(value: TimeSeriesSet):
    if value.valid:
        yield from ((k, k) for k in value.values())
    yield from ()


@enum_items.register
def enum_items_python_nested_node_impl(value: PythonNestedNodeImpl):
    yield from value.nested_graphs().items()


@enum_items.register
def enum_items_graph(value: Graph):
    yield from enumerate(value.nodes)


@multimethod
def format_type(value):
    return type(value).__name__


@format_type.register
def format_type_node(value: Node):
    return value.signature.node_type.name.replace("_NODE", "")


@format_type.register
def format_type_python_nested_node_impl(value: PythonNestedNodeImpl):
    return {
        PythonTsdMapNodeImpl: "MAP",
        PythonMeshNodeImpl: "MESH",
        PythonServiceNodeImpl: "SERVICE",
        PythonReduceNodeImpl: "REDUCE",
        PythonSwitchNodeImpl: "SWITCH",
        PythonTryExceptNodeImpl: "TRY_EXCEPT",
        PythonComponentNodeImpl: "COMPONENT",
    }.get(type(value), "?")


@format_type.register
def format_type_python_time_series_reference(value: TimeSeriesReference):
    return "REF"


@format_type.register
def format_type_time_series_input(value: TimeSeriesInput):
    path = []
    while value.parent_input:
        if isinstance(value.parent_input, TimeSeriesBundle):
            path.append(next(k for i, (k, v) in enumerate(value.parent_input._ts_values.items()) if v is value))
        else:
            path.append(0)
        value = value.parent_input

    if not path:
        return "INPUTS"

    tp = value.owning_node.signature.time_series_inputs[path[-1]]
    for i in reversed(path[:-1]):
        if isinstance(tp, HgTSBTypeMetaData):
            tp = tp.bundle_schema_tp.meta_data_schema[i]
        else:
            tp = tp.value_tp

    return re.sub(r"<class '(?:\w+\.)+", r"", str(tp)).replace("'>", "")


@format_type.register
def format_type_time_series_output(value: TimeSeriesOutput):
    path = []
    while value.parent_output:
        if isinstance(value.parent_output, TimeSeriesBundle):
            path.append(next(k for i, (k, v) in enumerate(value.parent_output._ts_values.items()) if v is value))
        else:
            path.append(0)
        value = value.parent_output

    tp = value.owning_node.signature.time_series_output
    for i in reversed(path):
        if isinstance(tp, HgTSBTypeMetaData):
            tp = tp.bundle_schema_tp.meta_data_schema[i]
        else:
            tp = tp.value_tp

    return re.sub(r"<class '(?:\w+\.)+", r"", str(tp)).replace("'>", "")


@multimethod
def inspect_item(value, key):
    return value[key]


@inspect_item.register
def inspect_item_compound_scalar(value: CompoundScalar, key):
    return getattr(value, key)


@inspect_item.register
def inspect_item_python_nested_node_impl(value: PythonNestedNodeImpl, key):
    return value.nested_graphs().get(key)


@inspect_item.register
def inspect_item_python_time_series_reference(value: TimeSeriesReference, key):
    return value.items[key]


@inspect_item.register
def inspect_item_python_time_series_reference_output(value: Union[PythonTimeSeriesReferenceInput | PythonTimeSeriesReferenceOutput], key):
    return value.value.items[key]


@inspect_item.register
def inspect_item_python_time_series_value_output(value: Union[PythonTimeSeriesValueInput | PythonTimeSeriesValueOutput], key):
    if value.valid:
        return inspect_item(value.value, key)


@multimethod
def inspect_type(value, key):
    return inspect_item(value, key)


@inspect_type.register
def inspect_type_tsb(value: HgTSBTypeMetaData, key):
    return value.meta_data_schema[key]


@inspect_type.register
def inspect_type_tsl_d(value: Union[HgTSLTypeMetaData, HgTSDTypeMetaData], key):
    return value.value_tp


@inspect_type.register
def inspect_type_cs(value: HgCompoundScalarType, key):
    return value.meta_data_schema[key]


#-----------------------------------------------------------------------------------------------------------------------
# object size computation


def estimate_value_size(value):
    with SizeOpts(value_only=True):
        return estimate_size_dispatch(value)


def estimate_size(value):
    ignore = ()
    parent = None
    if isinstance(value, TimeSeriesInput):
        ignore = _ignore_out_types
        parent = value.parent_input
    elif isinstance(value, TimeSeriesOutput):
        ignore = _ignore_in_types
        parent = value.parent_output

    with SizeOpts(ignore):
        SizeOpts.seen(parent)
        size = estimate_size_dispatch(value)
        return size


@multimethod
def estimate_size_impl(value):
    return 0


def subclasses_of(cls):
    yield cls
    for sub in cls.__subclasses__():
        yield from subclasses_of(sub)

_ignore_general_types = {type, ABCMeta, *subclasses_of(GenericAlias), type(subclasses_of), type(max), property, staticmethod,
                         *subclasses_of(HgTypeMetaData), *subclasses_of(EvaluationLifeCycleObserver), *subclasses_of(Builder)}
_ignore_graph_types = {*_ignore_general_types, *subclasses_of(Graph), *subclasses_of(EvaluationEngine)}

_ignore_ts_types = {*_ignore_graph_types, *subclasses_of(TimeSeriesOutput), *subclasses_of(TimeSeriesInput)}
_ignore_out_types = {*_ignore_graph_types, *subclasses_of(Node), *subclasses_of(TimeSeriesOutput)}
_ignore_in_types = {*_ignore_graph_types, *subclasses_of(Node), *subclasses_of(TimeSeriesInput)}
_ignore_node_types = {*_ignore_graph_types, *subclasses_of(Node)}


class SizeOpts:
    _ignore_types = []
    _opts = None
    _stats_mode = 0
    _cache_size = False

    def __init__(self, ignore=(), value_only=False, stats_mode=0, cache_size=None):
        self.ignore = ignore
        self.value_only = value_only
        self.stats_mode = stats_mode
        self.cache_size = cache_size

    def __enter__(self):
        self._ignore_types.append(self.ignore)
        self.value_only = self.value_only or SizeOpts.is_value_only()
        self._prev = self._opts
        self.__class__._opts = self

        if self._prev is not None:
            self.seen = self._prev.seen
            self.stats = self._prev.stats
        else:
            self.seen = set()
            self.stats = defaultdict(lambda: [0, 0, 0, 0])

        if self.__class__._stats_mode < self.stats_mode:
            self._prev_stats_mode = self.__class__._stats_mode
            self.__class__._stats_mode = self.stats_mode

        if self.cache_size is not None and self.__class__._cache_size != self.cache_size:
            self._prev_cache_size = self.__class__._cache_size
            self.__class__._cache_size = self.cache_size

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ignore_types.pop()
        self.__class__._opts = self._prev
        self.__class__._stats_mode = getattr(self, '_prev_stats_mode', self.__class__._stats_mode)
        self.__class__._cache_size = getattr(self, '_prev_cache_size', self.__class__._cache_size)

    @classmethod
    def is_ignored(cls, tp):
        return any(tp in ignore for ignore in cls._ignore_types)

    @classmethod
    def is_value_only(cls):
        return cls._opts.value_only if cls._opts is not None else False

    @classmethod
    def cache_size(cls):
        return cls._opts.cache_size if cls._opts is not None else False

    @classmethod
    def seen(cls, value):
        if id(value) in cls._opts.seen:
            return True

        cls._opts.seen.add(id(value))
        return False

    @classmethod
    def add_stats(cls, v, s):
        if cls._stats_mode == 0: return
        if cls._stats_mode == 1:
            tp = type(v).__name__
            cls._opts.stats[tp][0] += 1
            cls._opts.stats[tp][1] += s
        else:
            tp = (type(v).__name__, id(v))
            cls._opts.stats[tp][0] += 1
            cls._opts.stats[tp][1] += sys.getrefcount(v)
            cls._opts.stats[tp][2] += s
            cls._opts.stats[tp][3] = v

    @classmethod
    def get_stats(cls):
        return cls._opts.stats


def estimate_size_dispatch(value):
    if SizeOpts.is_ignored(type(value)):
        return 0
    if SizeOpts.seen(value):
        return 0
    if sys.getrefcount(value) > 256:
        return 0  # heuristics: this is either an interned object or something widely shared so skip to avoid doublecounting

    try:
        size = estimate_size_impl(value)
        SizeOpts.add_stats(value, size)
        return size
    except Exception as e:
        print(f"failed to calc size of {type(value)}: {value}, e: {e}")
        return 0


@estimate_size_impl.register
def estimate_size_default(value):
    return 0


@estimate_size_impl.register
def estimate_size_none(value: type(None)):
    return 0


@estimate_size_impl.register
def estimate_size_none(value: type | GenericAlias):
    return 0


@estimate_size_impl.register
def estimate_size_builtin(value: int | str | float | bool):
    return sys.getsizeof(value)


@estimate_size_impl.register
def estimate_size_dict(value: dict | frozendict):
    return (
            sum(estimate_size_dispatch(k) + estimate_size_dispatch(v) for k, v in value.items())
            + len(value) * type(value).__itemsize__
            + sys.getsizeof(value)
    ) if value is not None else 0


@estimate_size_impl.register
def estimate_size_list(value: list | tuple | set | frozenset):
    return (
            sum(estimate_size_dispatch(v) for v in value)
            + len(value) * type(value).__itemsize__
            + sys.getsizeof(value)
    )


@estimate_size_impl.register
def estimate_size_object(value: object):
    if not SizeOpts.is_ignored(type(value)):
        return (estimate_size_dict(getattr(value, '__dict__', None))
                + estimate_size_dispatch(getattr(value, '__slots__', None))
                + sys.getsizeof(value)
                )
    else:
        return 0


@estimate_size_impl.register
def estimate_size_df(value: pl.DataFrame):
    return value.estimated_size()


def cached_ts_size(value: TimeSeries):
    if SizeOpts.seen(value.__dict__):
        return 0

    size = getattr(value, '__cached_size__', None) if SizeOpts.cache_size() else None
    if not value.modified and size is not None:
        return size

    size = estimate_size_dict(value.__dict__) + sys.getsizeof(value)
    if SizeOpts.cache_size():
        object.__setattr__(value, '__cached_size__', size)

    return size


@estimate_size_impl.register
def estimate_size_python_time_series_value_output(value: PythonTimeSeriesValueOutput):
    if SizeOpts.is_value_only():
        if value.valid:
            return estimate_size_dispatch(value.value)
        else:
            return 0
    else:
        return cached_ts_size(value)


@estimate_size_impl.register
def estimate_size_python_time_series_reference_output(value: PythonTimeSeriesReferenceOutput):
    if SizeOpts.is_value_only():
        if value.valid:
            v = value.value
            return estimate_size_dispatch(v)
        else:
            return 0
    else:
        with SizeOpts(_ignore_out_types):
            return cached_ts_size(value)


@estimate_size_impl.register
def estimate_size_python_time_series_reference(value: TimeSeriesReference):
    with SizeOpts(_ignore_out_types):
        return estimate_size_object(value)


@estimate_size_impl.register
def estimate_size_time_series_list(value: TimeSeriesList | TimeSeriesBundle):
    return sum(estimate_size_dispatch(v) for v in value.values()) \
        if SizeOpts.is_value_only() \
        else cached_ts_size(value)


@estimate_size_impl.register
def estimate_size_time_series_set(value: TimeSeriesSet):
    if SizeOpts.is_value_only():
        return estimate_size_dispatch(value.value) + estimate_size_dispatch(value.added) + estimate_size_dispatch(value.removed)
    else:
        return cached_ts_size(value)


@estimate_size_impl.register
def estimate_size_time_series_dict(value: TimeSeriesDict):
    if SizeOpts.is_value_only():
        return sum(estimate_size_dispatch(v) for v in value.values()) + estimate_size_dispatch(value.key_set)
    else:
        return cached_ts_size(value)


@estimate_size_impl.register
def estimate_size_node(value: Node):
    total = 0
    if not SizeOpts.is_value_only():
        if SizeOpts.cache_size() and (const_size := getattr(value, '__const_size__', None)) is not None:
            total += const_size
        else:
            with SizeOpts(_ignore_ts_types):
                const_size = estimate_size_dict(value.__dict__)
                if SizeOpts.cache_size():
                    object.__setattr__(value, '__const_size__', const_size)
                total += const_size
        with SizeOpts(_ignore_out_types):
            total += estimate_size_dispatch(value.input)
    with SizeOpts(_ignore_in_types):
        total += estimate_size_dispatch(value.output)

    return total


@estimate_size_impl.register
def estimate_size_graph(value: Graph):
    if SizeOpts.cache_size() and (const_size := getattr(value, '__const_size__', None)) is not None:
        return const_size
    else:
        with SizeOpts(_ignore_node_types):
            const_size = estimate_size_dict(value.__dict__)
            if SizeOpts.cache_size():
                object.__setattr__(value, '__const_size__', const_size)

            return const_size


@estimate_size_impl.register
def estimate_size_nested(value: PythonNestedNodeImpl):
    total = estimate_size_node(value)
    return total + sum(estimate_size_dispatch(v) for v in value.nested_graphs().values())
