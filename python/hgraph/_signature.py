"""Wiring-node signature introspection (hgraph parity). Everything here is
part of the PUBLIC hgraph surface (exported from the package root) — ported
code never reaches into private module paths.

Deviation from upstream (recorded ruling): HgTypeMetaData is NOT part of the
public API and is not ported. `input_types` / `output_type` carry the raw
annotations; scalar/unresolved classification is exposed through
`time_series_args` / `unresolved_args` on the signature itself."""
import enum
import inspect
from dataclasses import dataclass, field


class WiringNodeType(enum.Enum):
    GRAPH = enum.auto()
    COMPUTE_NODE = enum.auto()
    SINK_NODE = enum.auto()
    PULL_SOURCE_NODE = enum.auto()
    PUSH_SOURCE_NODE = enum.auto()
    OPERATOR = enum.auto()


@dataclass(frozen=True)
class WiringNodeSignature:
    name: str
    node_type: WiringNodeType
    args: tuple = ()
    defaults: dict = field(default_factory=dict)
    input_types: dict = field(default_factory=dict)
    output_type: object = None
    time_series_args: frozenset = frozenset()
    unresolved_args: frozenset = frozenset()


def _is_time_series(annotation):
    import typing

    from ._types import (_GenericTsExpr, _TsExpr, _TypeVarSentinel,
                         _type_var_is_scalar)

    return (
        isinstance(annotation, (_TsExpr, _GenericTsExpr))
        or (
            isinstance(annotation, (_TypeVarSentinel, typing.TypeVar))
            and not _type_var_is_scalar(annotation)
        )
    )


def _is_unresolved(annotation):
    import typing

    from ._types import (_GenericTsExpr, _TypeVarSentinel,
                         _type_var_is_scalar)

    return (
        isinstance(annotation, _GenericTsExpr)
        or (
            isinstance(annotation, (_TypeVarSentinel, typing.TypeVar))
            and not _type_var_is_scalar(annotation)
        )
    )


def extract_signature(fn, node_type) -> WiringNodeSignature:
    """Build the WiringNodeSignature for a wiring function."""
    sig = inspect.signature(fn, eval_str=True)
    args, defaults, input_types = [], {}, {}
    time_series, unresolved = set(), set()
    for name, param in sig.parameters.items():
        annotation = param.annotation
        args.append(name)
        if param.default is not inspect.Parameter.empty:
            defaults[name] = param.default
        annotation = annotation if annotation is not inspect.Parameter.empty else object
        input_types[name] = annotation
        if _is_time_series(annotation):
            time_series.add(name)
        if _is_unresolved(annotation):
            unresolved.add(name)
    output = sig.return_annotation
    output_type = None
    if output not in (inspect.Signature.empty, None):
        output_type = output
    return WiringNodeSignature(
        name=getattr(fn, "__name__", "<fn>"),
        node_type=node_type,
        args=tuple(args),
        defaults=defaults,
        input_types=input_types,
        output_type=output_type,
        time_series_args=frozenset(time_series),
        unresolved_args=frozenset(unresolved),
    )


def extract_kwargs(signature: WiringNodeSignature, *args, **kwargs) -> dict:
    """Map a call's args/kwargs onto the signature's parameter names
    (hgraph parity: violations raise SyntaxError)."""
    if len(args) > len(signature.args):
        raise SyntaxError(
            f"[{signature.name}] too many positional arguments: "
            f"expected at most {len(signature.args)}, got {len(args)}")
    out = dict(zip(signature.args, args))
    for name, value in kwargs.items():
        if name not in signature.args:
            raise SyntaxError(f"[{signature.name}] unexpected keyword argument '{name}'")
        if name in out:
            raise SyntaxError(f"[{signature.name}] got multiple values for argument '{name}'")
        out[name] = value
    return out
