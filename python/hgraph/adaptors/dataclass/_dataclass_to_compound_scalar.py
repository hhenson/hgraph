import inspect
import types
from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from types import UnionType
from typing import Any, Type, Union, get_args, get_origin, get_type_hints

from hgraph import CompoundScalar, ParseError

_CLASS_CACHE: dict[type, type[CompoundScalar]] = {}


class CS:
    """Convert a user-defined Python model class to a ``CompoundScalar`` type."""

    def __class_getitem__(cls, model):
        if isinstance(model, type) and issubclass(model, CompoundScalar):
            return model
        if not isinstance(model, type):
            raise TypeError(
                f"CS[...] requires a class, got instance of {type(model).__name__}"
            )
        annotations = getattr(model, "__annotations__", None)
        if not annotations or model.__module__ in ("builtins", "typing"):
            raise TypeError(
                f"CS[...] requires a user-defined class, got {model.__name__}"
            )
        converted = _CLASS_CACHE.get(model)
        if converted is None:
            converted = _model_to_compound_scalar(model)
            _CLASS_CACHE[model] = converted
        return converted


def _is_pydantic_model(model: type) -> bool:
    try:
        from pydantic import BaseModel
    except ImportError:
        return False
    return issubclass(model, BaseModel)


def _resolved_annotations(model: type) -> dict[str, Any]:
    try:
        return get_type_hints(model, include_extras=True)
    except (NameError, TypeError):
        return dict(getattr(model, "__annotations__", {}))


def _model_fields(model: type) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved = _resolved_annotations(model)
    if is_dataclass(model):
        annotations = {}
        defaults = {}
        for item in fields(model):
            annotations[item.name] = resolved.get(item.name, item.type)
            if item.default is not MISSING:
                defaults[item.name] = item.default
            elif item.default_factory is not MISSING:
                defaults[item.name] = field(default_factory=item.default_factory)
        return annotations, defaults

    if _is_pydantic_model(model):
        annotations = {}
        defaults = {}
        model_fields = getattr(model, "model_fields", None) or getattr(
            model, "__fields__", {}
        )
        for name, item in model_fields.items():
            annotations[name] = resolved.get(name, item.annotation)
            default = getattr(item, "default", MISSING)
            if default is not MISSING and type(default).__name__ not in {
                "PydanticUndefinedType",
                "UndefinedType",
            }:
                defaults[name] = default
            default_factory = getattr(item, "default_factory", None)
            if default_factory is not None:
                defaults[name] = field(default_factory=default_factory)
        return annotations, defaults

    annotations = resolved
    defaults = {
        name: getattr(model, name)
        for name in annotations
        if hasattr(model, name)
    }
    init = getattr(model, "__init__", None)
    if init is not None:
        try:
            signature = inspect.signature(init)
        except (TypeError, ValueError):
            signature = None
        if signature is not None:
            # Resolve postponed (string) annotations PER PARAMETER: only the
            # parameter annotations are read here, and a whole-signature
            # eval_str would also evaluate the unread RETURN annotation — a
            # locally declared model's self-referential ``-> Model`` exists
            # only in the enclosing local scope and would raise NameError.
            globalns = getattr(init, "__globals__", {})
            for name, parameter in signature.parameters.items():
                if name == "self":
                    continue
                annotation = parameter.annotation
                if isinstance(annotation, str):
                    try:
                        annotation = eval(annotation, globalns)  # noqa: S307
                    except (NameError, SyntaxError):
                        annotation = parameter.annotation
                if annotation is not inspect.Parameter.empty:
                    annotations.setdefault(name, annotation)
                if parameter.default is not inspect.Parameter.empty:
                    defaults.setdefault(name, parameter.default)
    return annotations, defaults


def _convert_type(annotation):
    if annotation is Any:
        return object

    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        members = tuple(item for item in get_args(annotation) if item is not type(None))
        if len(members) != 1:
            raise ParseError(
                f"unsupported union field type {annotation!r}; only Optional[T] is supported"
            )
        return _convert_type(members[0])

    if origin is not None:
        arguments = tuple(_convert_type(item) for item in get_args(annotation))
        if arguments == get_args(annotation):
            return annotation
        try:
            return origin[arguments[0] if len(arguments) == 1 else arguments]
        except TypeError:
            if hasattr(annotation, "copy_with"):
                return annotation.copy_with(arguments)
            return annotation

    if isinstance(annotation, type):
        if annotation.__module__ == "builtins" or annotation.__module__ in {
            "datetime",
            "decimal",
            "pathlib",
            "uuid",
        }:
            return annotation
        try:
            return CS[annotation]
        except (ParseError, TypeError) as error:
            raise ParseError(
                f"cannot convert field type {annotation.__qualname__!r}: {error}"
            ) from error
    return annotation


def _model_to_compound_scalar(model: Type) -> type[CompoundScalar]:
    annotations, defaults = _model_fields(model)
    converted = {name: _convert_type(value) for name, value in annotations.items()}
    for name, annotation in annotations.items():
        if name not in defaults and type(None) in get_args(annotation):
            defaults[name] = None

    enclosing = (
        model.__qualname__.rsplit(".", 1)[0]
        if "." in model.__qualname__
        else ""
    )
    namespace = model.__module__ if not enclosing else f"{model.__module__}.{enclosing}"

    def populate(class_namespace):
        class_namespace["__annotations__"] = converted
        class_namespace["__module__"] = model.__module__
        class_namespace["__qualname__"] = model.__qualname__
        class_namespace["__source_model__"] = model
        class_namespace.update(defaults)

    compound = types.new_class(
        model.__name__,
        (CompoundScalar,),
        {"namespace": namespace},
        populate,
    )
    return dataclass(frozen=True)(compound)
