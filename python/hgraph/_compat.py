"""hgraph-parity names: enums, markers and thin aliases the ported test
suite imports. Real gaps raise at USE (never at import) with a "gap:"
message so ported tests can skip precisely."""
from dataclasses import dataclass
from enum import Enum


class CmpResult(Enum):
    LT = -1
    EQ = 0
    GT = 1


class DivideByZero(Enum):
    # Values match the C++ stdlib::DivideByZero scale.
    ERROR = 0
    NAN = 1
    INF = 2
    NONE = 3   # C++ NoTick: a zero divisor leaves the output un-ticked
    ZERO = 4
    ONE = 5


def exception_time_series(ts, trace_back_depth=1, capture_values=False):
    """Activate error capture on ``ts``'s producing node; returns the
    native error output. Ordinary nodes expose ``TS[NodeError]``; a TSD
    ``map_`` exposes ``TSD[K, TS[NodeError]]`` with one retained error series
    per live mapped child."""
    from ._wiring import WiringPort, _current_wiring, _unwrap

    return WiringPort(_current_wiring().exception_time_series(
        _unwrap(ts), trace_back_depth, capture_values))


# The type of a module-level operator function (hgraph exposes the wiring
# node class; ours is the generated operator_function).
from ._wiring import operator_function as _operator_function

OperatorWiringNodeClass = type(_operator_function("add_"))


class PartialSchema:
    """Upstream's per-type to_table builder bundle (an _impl internal). The
    C++ equivalent is the interned TS-table layout (TableConverter /
    ts_table_layout); this name exists so ported tests import, and raises
    at use."""

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "gap: PartialSchema is an upstream _impl internal (C++: ts_table_layout)")


def extract_table_schema_raw_type(tp):
    raise NotImplementedError(
        "gap: extract_table_schema_raw_type is an upstream _impl internal "
        "(use table_schema(tp) - the C++ layout introspection)")


# Node evaluation errors surface as RuntimeError through the C++ bridge;
# hgraph's NodeException is an alias until richer error info is bridged.
NodeException = RuntimeError


def accumulate(*ts, **kwargs):
    """hgraph parity (deprecated upstream): accumulate == running sum_."""
    from . import sum_

    return sum_(*ts, **kwargs)


def average(*ts, **kwargs):
    """hgraph parity (deprecated upstream): average == running mean."""
    from . import mean

    return mean(*ts, **kwargs)


def to_json_builder(tp):
    """hgraph's scalar-level JSON serializer factory: returns a callable
    rendering instances of ``tp`` as a JSON string (the C++ codec)."""
    import _hgraph

    from ._types import _value_type

    meta = _value_type(tp)

    def _write(value):
        return _hgraph.value_to_json(meta, value)

    return _write


def from_json_builder(tp):
    """The inverse: returns a callable parsing a JSON string (or an already
    json.loads'd tree, which re-dumps first) into a ``tp`` instance."""
    import json as _json

    import _hgraph

    from ._types import _value_type

    meta = _value_type(tp)

    def _read(payload):
        text = payload if isinstance(payload, str) else _json.dumps(payload)
        return _hgraph.value_from_json(meta, text)

    return _read


def register_json_datetime_format(fmt: str, *, time_only: bool = False) -> None:
    """Register an additional ``strptime``-style temporal JSON input format.

    ISO 8601 and the native compatibility formats remain preferred. The
    registration is process-wide and is consumed directly by the C++ JSON
    codec used by both builders and graph nodes.
    """
    import _hgraph

    _hgraph.register_json_datetime_format(fmt, time_only)


def _window_result():
    """hgraph's window() result schema {buffer, index} (generic over the
    element type; resolves when the window operator lands). Built lazily -
    the annotations subscript TS[...] which needs this module initialized."""
    import datetime
    from typing import Tuple

    from ._types import TS, SCALAR, TimeSeriesSchema

    class WindowResult(TimeSeriesSchema):
        buffer: TS[Tuple[SCALAR, ...]]
        index: TS[Tuple[datetime.datetime, ...]]

    return WindowResult


class BoolResult:
    """hgraph's if_ result schema: {true: REF[T], false: REF[T]}."""

    def __class_getitem__(cls, ts_type):
        from ._types import REF

        schema = type("BoolResult", (), {})
        schema.__annotations__ = {"true": REF[ts_type], "false": REF[ts_type]}
        return schema


_COMPOUND_SCALAR_CLASSES = []


class CompoundScalar:
    """Base class for nominal Bundle values.

    ``namespace`` defaults to the defining module plus any enclosing class or
    function scope. Hierarchy registration is completed lazily when the class
    is first materialised as an hgraph value type, after ``@dataclass`` has
    populated its inherited field set. Release/0.5 serialization markers are
    translated into the same native hierarchy metadata as the class options.
    """

    __compound_namespace__ = ""
    __compound_abstract__ = True
    __compound_discriminator__ = "__type__"

    def __init_subclass__(
        cls,
        *,
        namespace=None,
        abstract=False,
        discriminator="__type__",
        **options,
    ):
        # Some upstream hgraph classes still carry bridge-specific class
        # options (for example cpp_native). They are metadata here and must
        # not leak into object.__init_subclass__.
        super().__init_subclass__()
        enclosing = cls.__qualname__.rsplit(".", 1)[0] if "." in cls.__qualname__ else ""
        if namespace is None:
            namespace = cls.__module__ if not enclosing else f"{cls.__module__}.{enclosing}"

        # Release/0.5 described polymorphic JSON hierarchies with class-body
        # markers. Keep their historical child registry visible for code
        # which inspects it, while translating the behavior into the native
        # Bundle hierarchy used by every 0.8 codec and runtime boundary.
        legacy_parent = next(
            (
                base
                for base in cls.__mro__[1:]
                if getattr(base, "__serialise_children__", None) is not None
            ),
            None,
        )
        legacy_base = bool(cls.__dict__.get("__serialise_base__", False))
        discriminator_value = ""
        if legacy_parent is not None:
            discriminator = getattr(
                legacy_parent,
                "__serialise_discriminator_field__",
                "__type__",
            )
            discriminator_value = str(
                getattr(cls, discriminator, cls.__name__)
            )
            legacy_parent.__serialise_children__[discriminator_value] = cls
            cls.__serialise_base__ = False
        elif legacy_base:
            abstract = True
            discriminator = cls.__dict__.get(
                "__serialise_discriminator_field__",
                "__type__",
            )
            cls.__serialise_discriminator_field__ = discriminator
            cls.__serialise_children__ = {}

        cls.__compound_namespace__ = namespace
        cls.__compound_abstract__ = bool(abstract)
        cls.__compound_discriminator__ = discriminator
        cls.__compound_discriminator_value__ = discriminator_value
        cls.__compound_options__ = dict(options)
        _COMPOUND_SCALAR_CLASSES.append(cls)

    def to_dict(self):
        """Return populated schema fields using hgraph's public value shape."""
        from ._types import _compound_python_field_types

        result = {}
        for name in _compound_python_field_types(type(self)):
            value = getattr(self, name, None)
            if isinstance(value, CompoundScalar):
                value = value.to_dict()
            if value is not None:
                result[name] = value
        return result

    @classmethod
    def from_dict(cls, d: dict):
        """Construct from known fields, recursively adapting nested scalars."""
        from ._types import _compound_python_field_types

        field_types = _compound_python_field_types(cls)
        converted = {}
        for name, value in d.items():
            annotation = field_types.get(name)
            if annotation is None:
                continue
            if (isinstance(value, dict) and isinstance(annotation, type)
                    and issubclass(annotation, CompoundScalar)):
                value = annotation.from_dict(value)
            converted[name] = value
        return cls(**converted)

@dataclass(frozen=True)
class NodeError(CompoundScalar, namespace=""):
    """Structured error value emitted by native node and child-graph capture."""

    signature_name: str
    label: str
    wiring_path: str
    error_msg: str
    stack_trace: str
    activation_back_trace: str
    additional_context: str = None

    def __str__(self):
        label = f" labelled {self.label}" if self.label else ""
        path = f" at {self.wiring_path}" if self.wiring_path else ""
        context = f" :: {self.additional_context}" if self.additional_context else ""
        return (
            f"{self.signature_name}{label}{path}{context}\n"
            f"NodeError: {self.error_msg}\nStack trace:\n{self.stack_trace}\n"
            f"Activation Back Trace:\n{self.activation_back_trace}"
        )


class TryExceptResult:
    """Result schema for a protected value-producing graph."""

    def __class_getitem__(cls, output_type):
        from ._types import TS

        schema = type("TryExceptResult", (), {})
        schema.__annotations__ = {"exception": TS[NodeError], "out": output_type}
        return schema


class TryExceptTsdMapResult:
    """Result schema for keyed ``map_`` error capture."""

    def __class_getitem__(cls, parameters):
        from ._types import TSD, TS

        key_type, output_type = parameters
        schema = type("TryExceptTsdMapResult", (), {})
        schema.__annotations__ = {
            "exception": TSD[key_type, TS[NodeError]],
            "out": output_type,
        }
        return schema


def try_except(
    func,
    *args,
    __trace_back_depth__=1,
    __capture_values__=False,
    **kwargs,
):
    """Run a graph behind an exception boundary.

    A value-producing graph returns a bundle with ``out`` and ``exception``
    fields. A sink returns the exception stream. ``map_`` uses keyed error
    output so every child retains its own ``NodeError`` until that key is
    removed.

    :param func: Graph, node, or ``map_`` call to protect.
    :param args: Positional inputs forwarded to ``func``.
    :param __trace_back_depth__: Maximum graph traceback depth captured in each
        ``NodeError``.
    :param __capture_values__: Include current input values in captured errors.
    :param kwargs: Named inputs forwarded to ``func``.
    :return: Protected output paired with captured errors, or an error stream
        for a sink.

    Example::

        attempted = try_except(parse_message, payload)
        parsed = attempted.out
        errors = attempted.exception
    """
    from ._wiring import _PyNode, _as_wired, combine, map_, wire

    if func is map_:
        output = map_(*args, **kwargs)
        return combine(
            exception=exception_time_series(
                output, __trace_back_depth__, __capture_values__),
            out=output,
        )
    if isinstance(func, _PyNode) and func.has_output:
        output = func(*args, **kwargs)
        return combine(
            exception=exception_time_series(
                output, __trace_back_depth__, __capture_values__),
            out=output,
        )
    return wire(
        "try_except",
        _as_wired(func),
        *args,
        __trace_back_depth__=__trace_back_depth__,
        __capture_values__=__capture_values__,
        **kwargs,
    )


class JSON(str):
    """hgraph's JSON string newtype (a plain str scalar here)."""


class TimeSeriesReference:
    """The opaque reference-value API (Howard's ruling 2026-07-05):
    references are values - store, emit, compare - never dereferenced
    (.output is not exposed; code needing the dereferenced value accepts
    it as an input)."""

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "TimeSeriesReference instances come from REF inputs (ref.value) or make()")

    @staticmethod
    def is_instance(value):
        """True when ``value`` is an opaque native time-series reference."""
        import _hgraph

        return isinstance(value, _hgraph.TimeSeriesRef)

    @staticmethod
    def make():
        """An EMPTY reference (binds nothing)."""
        import _hgraph

        return _hgraph.empty_time_series_reference()
