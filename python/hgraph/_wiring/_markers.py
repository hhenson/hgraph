"""Injectable annotation markers and lazy type-kind caches.

``_INJECTABLE_MARKERS`` keys on CLASS IDENTITY (STATE/CLOCK/SCHEDULER/NODE/
EvaluationEngineApi/GlobalState) — import the classes, never redefine them. The ``global``
kind caches live beside their factory functions (``global`` binds to the
defining module)."""
import _hgraph

from .._types import _GenericTsExpr, _TsExpr
from ._state import GlobalState

class STATE:
    """Injectable per-node state.

    Naked ``STATE`` stores values in an attribute dictionary, with attribute
    access mirrored through ``state[name]`` and ``keys`` / ``items`` /
    ``values`` views. ``STATE[T]`` constructs and preserves one ``T``
    instance instead.
    """

    def __init__(self, **kwargs):
        self.__dict__["__schema__"] = None
        self.__dict__["_updated"] = False
        self.__dict__["_value"] = dict(kwargs)

    def __class_getitem__(cls, item):
        return _StateExpr(item)

    @property
    def as_schema(self):
        return self.__dict__["_value"]

    def __getattr__(self, name):
        values = self.__dict__.get("_value")
        if values is not None and name in values:
            return values[name]
        raise AttributeError(name)

    def __getitem__(self, name):
        return getattr(self, name)

    def keys(self):
        return self.__dict__["_value"].keys()

    def items(self):
        return self.__dict__["_value"].items()

    def values(self):
        return self.__dict__["_value"].values()

    def __setattr__(self, name, value):
        if name in ("__schema__", "_updated", "_value"):
            self.__dict__[name] = value
            return
        self.__dict__["_updated"] = True
        self.__dict__["_value"][name] = value

    def __delattr__(self, name):
        if name in ("__schema__", "_updated", "_value"):
            return
        self.__dict__["_updated"] = True
        del self.__dict__["_value"][name]

    def reset_updated(self):
        self.__dict__["_updated"] = False

    def is_updated(self):
        return self.__dict__["_updated"]

    def __repr__(self):
        values = ", ".join(
            f"{name}={value!r}" for name, value in self.__dict__["_value"].items()
        )
        return f"SCALAR({values})"


class _StateExpr:
    __slots__ = ("factory",)

    def __init__(self, factory):
        if not callable(factory):
            raise TypeError("STATE[T] requires a callable state type or factory")
        self.factory = factory

    def __repr__(self):
        return f"STATE[{self.factory!r}]"


class SCHEDULER:
    """Annotation marker for injecting the current node's scheduler.

    The callback receives a scheduler whose ``schedule`` method accepts an
    absolute ``datetime`` or relative ``timedelta`` and an optional replacement
    tag. ``reset`` cancels every outstanding schedule for the node. The object
    is callback-scoped and must not be retained.
    """


class CLOCK:
    """Annotation marker for injecting the graph evaluation clock.

    The callback receives an ``EvaluationClock`` exposing logical evaluation
    time, mode-dependent current time, cycle duration, and the logical time of
    the immediately following possible evaluation cycle. The object is
    callback-scoped.
    """


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
    """Persistent output-backed node state selected as RECORDABLE_STATE[Schema].

    The callback receives a mutable view whose writes participate in the
    configured record/replay model. The view is callback-scoped and must not
    be retained.
    """

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
    """Annotate ``_output`` as TS_OUT[X] to inspect or mutate its native output.

    The callback receives a mutable, callback-scoped output view. Returning a
    value remains the usual publication path; the view is useful for checking
    the existing value, suppressing duplicate ticks, and mutating collection
    outputs in place.
    """

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
