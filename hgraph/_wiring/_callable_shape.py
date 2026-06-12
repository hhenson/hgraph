import inspect
from types import CodeType
from typing import Any, Callable

__all__ = ("callable_shape_key", "equal_lambdas")


def equal_lambdas(lhs: Callable[..., Any], rhs: Callable[..., Any]) -> bool:
    """
    Compare lambdas by shape.

    Lambdas are compared by a normalized bytecode fingerprint that ignores local
    binding names while preserving constants, called globals, attribute names,
    control flow, and nested code objects.
    All other callables fall back to normal equality / identity semantics.
    """
    return callable_shape_key(lhs) == callable_shape_key(rhs)


def callable_shape_key(fn: Any) -> Any:
    if _is_lambda(fn):
        return ("lambda", _normalized_lambda_shape(fn))
    return fn


def _is_lambda(fn: Callable[..., Any]) -> bool:
    return inspect.isfunction(fn) and fn.__name__ == "<lambda>"


def _normalized_lambda_shape(fn: Callable[..., Any]) -> tuple[Any, ...]:
    return (
        _normalized_parameter_shape(fn),
        _normalized_closure_shape(fn),
        _normalized_code_shape(fn.__code__),
    )


def _normalized_parameter_shape(fn: Callable[..., Any]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            parameter.kind,
            parameter.default is not inspect.Parameter.empty,
            _normalize_value(parameter.default) if parameter.default is not inspect.Parameter.empty else None,
        )
        for parameter in inspect.signature(fn).parameters.values()
    )


def _normalized_closure_shape(fn: Callable[..., Any]) -> tuple[Any, ...]:
    closure = fn.__closure__ or ()
    return tuple(_normalize_closure_cell(cell) for cell in closure)


def _normalize_closure_cell(cell) -> Any:
    try:
        return "cell", _normalize_value(cell.cell_contents)
    except ValueError:
        return ("empty-cell",)


def _normalized_code_shape(code: CodeType) -> tuple[Any, ...]:
    return (
        _normalized_code_header(code),
        code.co_code,
        code.co_exceptiontable,
        tuple(_normalize_value(const) for const in code.co_consts),
        tuple(_normalize_value(name) for name in code.co_names),
    )


def _normalized_code_header(code: CodeType) -> tuple[Any, ...]:
    return (
        code.co_argcount,
        code.co_posonlyargcount,
        code.co_kwonlyargcount,
        bool(code.co_flags & inspect.CO_VARARGS),
        bool(code.co_flags & inspect.CO_VARKEYWORDS),
        bool(code.co_flags & inspect.CO_GENERATOR),
        bool(code.co_flags & inspect.CO_COROUTINE),
        bool(code.co_flags & inspect.CO_ASYNC_GENERATOR),
    )


def _normalize_value(value: Any) -> Any:
    if isinstance(value, CodeType):
        return "code", _normalized_code_shape(value)

    if inspect.isfunction(value):
        return "callable", callable_shape_key(value)

    if isinstance(value, tuple):
        return "tuple", tuple(_normalize_value(item) for item in value)

    if isinstance(value, list):
        return "list", tuple(_normalize_value(item) for item in value)

    if isinstance(value, dict):
        return "dict", tuple(
            (_normalize_value(key), _normalize_value(item))
            for key, item in sorted(value.items(), key=lambda kv: repr((_normalize_value(kv[0]), _normalize_value(kv[1]))))
        )

    if isinstance(value, (set, frozenset)):
        return type(value).__name__, tuple(sorted((_normalize_value(item) for item in value), key=repr))

    try:
        hash(value)
    except TypeError:
        return "repr", type(value), repr(value)
    else:
        return "value", type(value), value
