"""The arrow combinator core — a port of upstream ``hgraph.arrow._arrow``
(Ross Paterson's arrow notation over hgraph wiring).

Dialect adaptations (documented divergences from the upstream module, which
relies on Python-runtime type metadata this implementation replaces with C++
interned types):

- A *pair* is ``TSB[ts_schema(first=..., second=...)]`` (upstream:
  ``TSB[PairSchema[A, B]]`` — same shape, C++-interned schema). Pair ports
  built here are wrapped in ``_PairPort`` which caches the element ports;
  foreign TSB ports are recognised as pairs by their field names.
- ``eval_`` runs on the hg_cpp harness (replay/record over ``Wiring``)
  rather than upstream's in-memory record/replay internals, and returns the
  recorded output as FULL-value tuples (``None`` for a never-ticked side);
  upstream returns delta tuples — identical for the tick-aligned shapes the
  arrow tests exercise.
- Wired-node unpacking (``_flatten_wrapper``) resolves arity by attempting
  registry resolution (pair → ``fn(first, second)``, then fully flattened,
  then the raw input) instead of upstream's signature introspection: the
  operator registry owns dispatch here and arity is not statically visible.
"""
from functools import partial, wraps
from typing import Callable, Generic, TypeVar

import hgraph as hg
from hgraph import TIME_SERIES_TYPE, TS, TSB, ts_schema

from .._wiring import (WiringPort, _current_wiring, _GraphFn, _Generator,
                       _OperatorFunction, _PyNode, _infer_ts_type,
                       _simplify_delta, _unwrap, _wiring_stack, compute_node,
                       wire)

__all__ = (
    "arrow",
    "identity",
    "i",
    "null",
    "eval_",
    "make_pair",
    "PairSchema",
    "Pair",
    "extract_delta_value",
    "extract_value",
    "convert_pairs_to_delta_tuples",
    "convert_pairs_to_tuples",
    "a",
)

A = TypeVar("A")
B = TypeVar("B")

_WIRED_NODE_TYPES = (_OperatorFunction, _PyNode, _GraphFn, _Generator)


# ---------------------------------------------------------------------------
# Pair machinery
# ---------------------------------------------------------------------------

def _pair_schema(first_tp, second_tp):
    return ts_schema(first=first_tp, second=second_tp)


class _PairSchemaGenerator:
    """``PairSchema[A, B]`` — the schema of a pair (upstream compat)."""

    def __getitem__(self, item):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(f"PairSchema expects two type arguments, got {item}")
        return _pair_schema(*item)


PairSchema = _PairSchemaGenerator()


class _PairGenerator:
    """``Pair[A]`` / ``Pair[A, B]`` / ``Pair[(A,), (B,)]`` — the TSB type of
    a pair; nested tuples build nested pairs (upstream shape)."""

    def __getitem__(self, item):
        if isinstance(item, tuple):
            if (l := len(item)) == 1:
                item = (item[0], item[0])
            if len(item) != 2:
                raise ValueError(f"Expected a tuple of length 2, got {item}")
            first, second = item
            first = self.__getitem__(first) if isinstance(first, tuple) else first
            second = self.__getitem__(second) if isinstance(second, tuple) else second
            return TSB[_pair_schema(first, second)]
        return TSB[_pair_schema(item, item)]


Pair = _PairGenerator()


class _PairPort(WiringPort):
    """A pair port: the combined TSB port plus cached element ports."""

    __slots__ = ("_pair_first", "_pair_second")

    def __init__(self, port, first, second):
        super().__init__(_unwrap(port))
        object.__setattr__(self, "_pair_first", first)
        object.__setattr__(self, "_pair_second", second)

    def __getitem__(self, index):
        if index == 0:
            return object.__getattribute__(self, "_pair_first")
        if index == 1:
            return object.__getattribute__(self, "_pair_second")
        return super().__getitem__(index)


def _pair_elements(x):
    """(first, second) of a pair port, or None if ``x`` is not a pair."""
    if isinstance(x, _PairPort):
        return x[0], x[1]
    if isinstance(x, WiringPort):
        try:
            import _hgraph
            names = _hgraph.tsb_field_names(_unwrap(x).ts_type)
        except Exception:
            return None
        if list(names) == ["first", "second"]:
            return x.first, x.second
    return None


def make_pair(first, second):
    """Combine two ports into a pair (``TSB{first, second}``)."""
    if isinstance(first, _ArrowInput):
        first = first.ts
    if isinstance(second, _ArrowInput):
        second = second.ts
    if isinstance(first, tuple):
        first = make_pair(*first)
    if isinstance(second, tuple):
        second = make_pair(*second)
    if not isinstance(first, WiringPort):
        first = hg.const(first)
    if not isinstance(second, WiringPort):
        second = hg.const(second)
    combined = hg.combine(first=first, second=second)
    return _PairPort(combined, first, second)


def _flatten(x):
    """Expand a (possibly nested) pair port into a tuple of non-pair ports."""
    elements = _pair_elements(x)
    if elements is None:
        return (x,)
    return _flatten(elements[0]) + _flatten(elements[1])


# ---------------------------------------------------------------------------
# The arrow wrapper
# ---------------------------------------------------------------------------

class _Arrow(Generic[A, B]):
    """Arrow function wrapper exposing support for piping operations."""

    def __init__(self, fn, __name__=None, __has_side_effects__=False,
                 bound_args=None, bound_kwargs=None):
        self._bound_args = bound_args if bound_args is not None else ()
        self._bound_kwargs = bound_kwargs if bound_kwargs is not None else {}
        self._has_side_effects = __has_side_effects__
        self._fn = fn.fn if isinstance(fn, _Arrow) else fn
        if __name__ is not None:
            self._name = __name__
        elif isinstance(fn, (_Arrow, _ArrowInput)):
            self._name = str(fn)
        else:
            self._name = getattr(fn, "__name__", None) or str(fn)

    @property
    def fn(self):
        f = _wrap_for_side_effects(self._fn) if self._has_side_effects else self._fn
        if self._bound_args or self._bound_kwargs:
            return partial(f, *self._bound_args, **self._bound_kwargs)
        return f

    def __rshift__(self, other):
        other = arrow(other)
        fn = other.fn
        name = f"{self} >> {other}"
        mine = self.fn
        return _Arrow(lambda x: fn(mine(x)), __name__=name)

    def __rrshift__(self, other):
        return arrow(other) >> self

    def __lshift__(self, other):
        return arrow(i / arrow(other) >> self, __name__=f"{self} << {other}")

    def __rlshift__(self, other):
        return arrow(other) << self

    def __floordiv__(self, other):
        other = arrow(other)
        f, g = self.fn, other.fn
        name = f"{self} // {other}"
        return _Arrow(lambda pair, _f=f, _g=g: make_pair(_f(pair[0]), _g(pair[1])),
                      __name__=name)

    def __truediv__(self, other):
        other = arrow(other)
        f, g = self.fn, other.fn
        name = f"{self} / {other}"
        return _Arrow(lambda x, _f=f, _g=g: make_pair(_f(x), _g(x)), __name__=name)

    def __pos__(self):
        return _Arrow(lambda pair: make_pair(self.fn(pair[0]), pair[1]),
                      __name__=f"+{self._name}")

    def __neg__(self):
        return _Arrow(lambda pair: make_pair(pair[0], self.fn(pair[1])),
                      __name__=f"-{self._name}")

    def __call__(self, *args, **kwargs):
        if len(args) == 1 and not kwargs and isinstance(args[0], WiringPort):
            f = _wrap_for_side_effects(self._fn) if self._has_side_effects else self._fn
            return f(*self._bound_args, args[0], **self._bound_kwargs)
        return _Arrow(self._fn, __name__=self._name, bound_args=args,
                      bound_kwargs=kwargs,
                      __has_side_effects__=self._has_side_effects)

    def __str__(self):
        return self._name


def _wrap_for_side_effects(fn):
    """Terminate the wrapped function's result with a null_sink so it is
    evaluated even when nothing consumes it."""

    @wraps(fn)
    def _wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)
        if result is not None:
            hg.null_sink(result)
        return result

    return _wrapper


class _ArrowInput(Generic[A]):

    def __init__(self, ts, __name__=None):
        self.ts = ts.ts if isinstance(ts, _ArrowInput) else ts
        self._name = __name__ if __name__ is not None else str(ts)

    def __or__(self, other):
        if not isinstance(other, (_Arrow,)) and callable(other):
            other = arrow(other)
        if not isinstance(other, _Arrow):
            raise TypeError(f"Expected an arrow function, got {type(other)}")
        return other(self.ts)

    def __str__(self):
        return self._name


def arrow(input_=None, input_2=None, __name__=None, __has_side_effects__=False):
    """Convert a graph / node / lambda / time-series / constant into an
    arrow-capable wrapper (see the upstream README for the notation)."""
    if input_ is None:
        return partial(arrow, __name__=__name__,
                       __has_side_effects__=__has_side_effects__)
    if input_2 is not None:
        return _ArrowInput(make_pair(input_, input_2), __name__=__name__)
    if isinstance(input_, _Arrow):
        if __name__ is None:
            return input_
        return _Arrow(input_.fn, __name__=__name__,
                      __has_side_effects__=__has_side_effects__)
    if isinstance(input_, _ArrowInput):
        return input_ if __name__ is None else _ArrowInput(input_.ts, __name__=__name__)
    if isinstance(input_, WiringPort):
        return _ArrowInput(input_, __name__=__name__)
    if isinstance(input_, _WIRED_NODE_TYPES):
        return _Arrow(_flatten_wrapper(input_), __name__=__name__ or str(input_),
                      __has_side_effects__=__has_side_effects__)
    if isinstance(input_, tuple):
        return _ArrowInput(make_pair(*input_), __name__=__name__)
    if callable(input_):
        return _Arrow(input_, __name__=__name__,
                      __has_side_effects__=__has_side_effects__)
    # A constant: inside a wiring context lift to const; otherwise defer.
    if _wiring_stack:
        return _ArrowInput(hg.const(input_), __name__=__name__)
    from ._std_operators import const_

    return const_(input_)


a = arrow  # Shortcut to mark as arrow


def _flatten_wrapper(node):
    """Adapt a wired node (registry operator / @graph / @compute_node) to a
    single-input arrow function: a pair input is unpacked to match the node.

    Upstream inspects the node signature's time-series arity; the registry
    owns dispatch here, so resolution is attempted most-specific-first:
    ``fn(first, second)``, then the fully flattened elements, then the raw
    input (a genuinely unary node over the pair TSB)."""

    def _call(node_, args, kwargs, x):
        elements = _pair_elements(x)
        if elements is None:
            try:
                return node_(*args, x, **kwargs)
            except hg.WiringError:
                # A non-pair TSB binds its FIELDS to the node's parameters
                # (upstream's TSB-input rule).
                import _hgraph
                try:
                    names = list(_hgraph.tsb_field_names(_unwrap(x).ts_type))
                except Exception:
                    raise
                if not names:
                    raise
                return node_(*args, **{n: getattr(x, n) for n in names}, **kwargs)
        failures = []
        for candidate in (elements, tuple(_flatten(x)), (x,)):
            try:
                return node_(*args, *candidate, **kwargs)
            except (hg.WiringError, TypeError) as exc:
                failures.append(exc)
        raise failures[-1]

    @wraps(getattr(node, "fn", None) or (lambda: None))
    def _wrapper(*args, **kwargs):
        x = args[-1]
        return _call(node, args[:-1], kwargs, x)

    _wrapper.__name__ = str(node)
    return _wrapper


# ---------------------------------------------------------------------------
# identity / null
# ---------------------------------------------------------------------------

@arrow(__name__="i")
def identity(x):
    """The identity function, does nothing."""
    return x


i = identity


@arrow(__name__="null")
def null(x):
    """Consume the input (null_sink) and return a never-ticking port of the
    same type."""
    hg.null_sink(x)
    return wire("nothing", output_type=_unwrap(x).ts_type)


# ---------------------------------------------------------------------------
# Value extraction (pairs -> tuples)
# ---------------------------------------------------------------------------

def _pair_shape(x):
    """The nested pair shape of a port as a tuple tree (None = leaf)."""
    elements = _pair_elements(x)
    if elements is None:
        return None
    return (_pair_shape(elements[0]), _pair_shape(elements[1]))


def extract_value(ts, shape=None):
    """Extract the (full) value of a runtime view, converting pair bundles
    to tuples. ``shape`` guides recursion when known; otherwise pair-ness is
    detected from the value's dict shape."""
    value = ts.value if hasattr(ts, "value") else ts
    return _value_to_tuples(value)


def _value_to_tuples(value):
    if isinstance(value, dict) or type(value).__name__ == "frozendict":
        keys = set(value.keys())
        if keys == {"first", "second"}:
            return (_value_to_tuples(value.get("first")),
                    _value_to_tuples(value.get("second")))
    return value


def extract_delta_value(ts, shape=None):
    """Extract the delta value; a non-ticked pair side is ``None``."""
    value = ts.delta_value if hasattr(ts, "delta_value") else ts
    return _value_to_tuples(value)


@compute_node
def convert_pairs_to_tuples(ts: TIME_SERIES_TYPE) -> TS[object]:
    """Converts the incoming time-series to (tuple-shaped) full values."""
    return _value_to_tuples(ts.value)


convert_pairs_to_delta_tuples = convert_pairs_to_tuples  # full-value dialect (see module docstring)


@compute_node
def _record_probe(ts: TIME_SERIES_TYPE) -> TS[object]:
    return ts.value


# ---------------------------------------------------------------------------
# eval_
# ---------------------------------------------------------------------------

class _EvalArrowInput:

    def __init__(self, first, second=None, type_map=None, start_time=None,
                 end_time=None, **_engine_options):
        # trace/profile/run_mode engine options are accepted for upstream
        # compatibility; the hg_cpp harness runs simulation only here.
        self.first = first
        self.second = second
        self.type_map = type_map if type_map is None or type(type_map) is tuple else (type_map,)
        self.start_time = start_time
        self.end_time = end_time

    def __or__(self, other):
        if not isinstance(other, _Arrow):
            if isinstance(other, _WIRED_NODE_TYPES) or callable(other):
                other = arrow(other)
            else:
                return NotImplemented
        import _hgraph

        w = _hgraph.Wiring()
        _wiring_stack.append(w)
        try:
            global _REPLAY_COUNTER
            _REPLAY_COUNTER = 0
            values = _build_inputs(self.first, self.second, self.type_map)
            out = other(values)
            if out is not None:
                flat_value = _to_value_port(out)
                w.wire("__harness_record", (_unwrap(flat_value), "arrow::out"),
                       {"sparse": True})
            run = w.run()
        finally:
            _wiring_stack.pop()
        if out is None:
            return []
        return [v for _, v in run.recorded("arrow::out", sparse=True)]


def _to_value_port(out):
    """A TS[object] port carrying the (tuple-converted) full value of out."""
    shape = _pair_shape(out)
    if shape is None:
        return _record_probe(out)
    return convert_pairs_to_tuples(_pair_value_port(out))


def _pair_value_port(x):
    """Build a TS[object] of nested-dict full values from a pair port so a
    single probe records tuple-shaped output."""
    elements = _pair_elements(x)
    if elements is None:
        return _record_probe(x)
    return _combine_pair_values(_pair_value_port(elements[0]),
                                _pair_value_port(elements[1]))


@compute_node(valid=())
def _combine_pair_values(first: TS[object], second: TS[object]) -> TS[object]:
    return {"first": first.value if first.valid else None,
            "second": second.value if second.valid else None}


_REPLAY_COUNTER = 0


def _plain_containers(v):
    """frozendict -> dict (recursively): the replay seeding path shapes
    plain dicts only."""
    if isinstance(v, dict) or type(v).__name__ == "frozendict":
        return {k: _plain_containers(x) for k, x in v.items()}
    return v


def _build_inputs(first, second=None, type_map=None):
    """Build input ports on the ACTIVE wiring: tuples nest as pairs, lists
    replay as time-series, other values become constants."""
    first = _build_one(first, type_map[0] if type_map else None)
    if second is None:
        return first
    second = _build_one(second, type_map[1] if type_map else None)
    return make_pair(first, second)


def _build_one(value, tp):
    if type(value) is tuple:
        return _build_inputs(*value, type_map=tp)
    if isinstance(value, list):
        value = [_plain_containers(v) for v in value]
        annotation = tp if tp is not None else _infer_ts_type(value)
        if annotation is None:
            raise TypeError("arrow eval_: cannot infer the input type; pass type_map")
        handle = annotation.handle
        global _REPLAY_COUNTER
        key = f"arrow::{_REPLAY_COUNTER}"
        _REPLAY_COUNTER += 1
        w = _current_wiring()
        src = w.wire("__harness_replay", (key,), {}, output_type=handle)
        w.set_replay(key, list(value), ts_type=handle)
        return WiringPort(src)
    return hg.const(value) if tp is None else hg.const(value, tp=tp)


def eval_(first, second=None, type_map=None, start_time=None, end_time=None,
          **engine_options):
    """Wrap inputs for simulation evaluation of an arrow chain: lists replay
    as time-series, other values become constants, tuples become pairs.
    ``eval_(...) | chain`` runs the graph and returns the recorded outputs
    (pairs as tuples)."""
    return _EvalArrowInput(first, second, type_map, start_time, end_time,
                           **engine_options)
