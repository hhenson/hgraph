"""Injectable annotation markers and lazy type-kind caches.

``_INJECTABLE_MARKERS`` keys on CLASS IDENTITY (STATE/CLOCK/SCHEDULER/NODE/
EvaluationEngineApi/GlobalState) — import the classes, never redefine them. The ``global``
kind caches live beside their factory functions (``global`` binds to the
defining module)."""
import _hgraph

from .._types import _GenericTsExpr, _TsExpr
from ._state import GlobalState

class STATE:
    """Injectable per-node state. ``STATE`` supplies a mutable namespace;
    ``STATE[T]`` constructs and preserves one ``T`` instance."""

    def __class_getitem__(cls, item):
        return _StateExpr(item)


class _StateExpr:
    __slots__ = ("factory",)

    def __init__(self, factory):
        if not callable(factory):
            raise TypeError("STATE[T] requires a callable state type or factory")
        self.factory = factory

    def __repr__(self):
        return f"STATE[{self.factory!r}]"


class SCHEDULER:
    """Injectable: the node scheduler - .schedule(datetime) /
    .schedule_delta(timedelta). Annotate a parameter with SCHEDULER."""


class CLOCK:
    """Injectable: the evaluation clock - .evaluation_time. Annotate a
    parameter with CLOCK."""


class LOGGER:
    """Injectable: the Python logger configured for this graph run.
    Resolved at wiring time from the copied-in GlobalState."""


EvaluationEngineApi = _hgraph.EvaluationEngineApi
EvaluationClock = _hgraph.EvaluationClock   # hgraph's clock annotation (same injectable as CLOCK)
Node = _hgraph.Node
NODE = Node


_INJECTABLE_MARKERS = {
    STATE: "S",
    CLOCK: "c",
    EvaluationClock: "c",
    SCHEDULER: "d",
    EvaluationEngineApi: "e",
    GlobalState: "g",
    NODE: "n",
}


_MISSING = object()


def _is_object_vt(vt):
    try:
        return vt == _hgraph.value_type("object")
    except TypeError:
        return False


def _unbounded_tuple_kind():
    global _UNBOUNDED_TUPLE_KIND
    if _UNBOUNDED_TUPLE_KIND is None:
        _UNBOUNDED_TUPLE_KIND = _hgraph.vt_kind(_hgraph.tuple_vt(_hgraph.value_type("int")))
    return _UNBOUNDED_TUPLE_KIND


_UNBOUNDED_TUPLE_KIND = None


def _annotation_ts_kind(annotation):
    """The TS KIND an annotation describes, via the C++ pattern machinery
    (-1 / None when unconstrained). Never classify by rendered labels."""
    if isinstance(annotation, _TsExpr):
        return annotation.handle.kind
    if isinstance(annotation, _GenericTsExpr) and annotation.pattern is not None:
        return annotation.pattern.ts_kind
    return None


def _tsw_kind():
    global _TSW_KIND
    if _TSW_KIND is None:
        from .._types import TSW, WindowSize

        _TSW_KIND = TSW[int, WindowSize[1]].handle.kind
    return _TSW_KIND


_TSW_KIND = None

class _RecordableStateMarker:
    """RECORDABLE_STATE[Schema]: a C++ hidden output-backed node state."""

    def __getitem__(self, item):
        return _RecordableStateExpr(item)


class _RecordableStateExpr:
    __slots__ = ("schema",)

    def __init__(self, schema):
        self.schema = schema

    def __repr__(self):
        return f"RECORDABLE_STATE[{self.schema!r}]"


RECORDABLE_STATE = _RecordableStateMarker()


class _TsOutMarker:
    """TS_OUT[X]: output-backed state field, represented by TS[X]."""

    def __getitem__(self, item):
        from .._types import TS, _TsExpr, _GenericTsExpr

        return item if isinstance(item, (_TsExpr, _GenericTsExpr)) else TS[item]


TS_OUT = _TsOutMarker()


class _TswOutMarker:
    """TSW_OUT[...]: output-typed window annotation - represented by the
    matching TSW[...] input shape (hgraph compat)."""

    def __getitem__(self, item):
        from .._types import TSW

        return TSW[item]


TSW_OUT = _TswOutMarker()


class _TsdOutMarker:
    """TSD_OUT[K, V]: output-backed dictionary annotation sugar."""

    def __getitem__(self, item):
        from .._types import TSD

        return TSD[item]


TSD_OUT = _TsdOutMarker()


class _TssOutMarker:
    """TSS_OUT[T]: output-backed set annotation sugar."""

    def __getitem__(self, item):
        from .._types import TSS

        return TSS[item]


TSS_OUT = _TssOutMarker()

class TSB_OUT:
    """_output annotation sugar (TSB_OUT[Schema]); injection keys on the
    parameter NAME (_output), the subscript documents the shape."""

    def __class_getitem__(cls, item):
        return cls
