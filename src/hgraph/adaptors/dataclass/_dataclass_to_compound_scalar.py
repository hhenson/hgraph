import inspect
from dataclasses import dataclass, field, fields, is_dataclass, MISSING
from typing import Any, Type, Union, get_origin, get_args

from hgraph import ParseError
from hgraph._types._scalar_types import is_compound_scalar, CompoundScalar

_DATACLASS_CS_CACHE: dict[Type, Type] = {}


class CS:
    """
    Convenience class for converting dataclass, Pydantic models, or plain Python classes to CompoundScalar types.

    Supports:
    - @dataclass decorated classes
    - Pydantic models (v1 and v2)
    - Plain Python classes with __annotations__

    Usage:
        from dataclasses import dataclass
        from hgraph import CS, TS

        @dataclass
        class MyModel:
            id: int
            name: str
            value: float = 0.0

        MyCS = CS[MyModel]

        @compute_node
        def process_data(ts: TS[CS[MyModel]]) -> TS[int]:
            return ts.value.id
    """

    def __class_getitem__(cls, item):
        if is_compound_scalar(item):
            return item

        if not isinstance(item, type):
            raise TypeError(f"CS[...] requires a class, got instance of {type(item).__name__}")

        if not hasattr(item, '__annotations__') or not item.__annotations__ or item.__module__ in ("builtins", "typing"):
            raise TypeError(f"CS[...] requires a user-defined class, got {item.__name__}")

        if item in _DATACLASS_CS_CACHE:
            return _DATACLASS_CS_CACHE[item]

        return dataclass_to_compound_scalar(item)


def _create_compound_scalar_class(
    name: str,
    annotations: dict[str, Type],
    defaults: dict[str, Any]
) -> Type:
    namespace = {
        '__annotations__': annotations,
        '__module__': __name__,
    }

    for field_name, default_val in defaults.items():
        namespace[field_name] = default_val

    cs_class = type(name, (CompoundScalar,), namespace)
    cs_class = dataclass(frozen=True)(cs_class)

    return cs_class


def _extract_class_fields(cls: Type) -> tuple[dict[str, Type], dict[str, Any]]:
    if is_dataclass(cls):
        annotations = {}
        defaults = {}
        for field_info in fields(cls):
            annotations[field_info.name] = field_info.type
            if field_info.default is not MISSING:
                defaults[field_info.name] = field_info.default
            elif field_info.default_factory is not MISSING:
                defaults[field_info.name] = field(default_factory=field_info.default_factory)
        return annotations, defaults
    elif _is_pydantic_model(cls):
        annotations = {}
        defaults = {}

        model_fields = getattr(cls, 'model_fields', None) or getattr(cls, '__fields__', {})

        for field_name, field_info in model_fields.items():
            annotations[field_name] = field_info.annotation

            if hasattr(field_info, 'default'):
                try:
                    from pydantic_core import PydanticUndefined
                    undefined = PydanticUndefined
                except ImportError:
                    try:
                        from pydantic.fields import Undefined
                        undefined = Undefined
                    except ImportError:
                        undefined = None

                if undefined is None or field_info.default is not undefined:
                    defaults[field_name] = field_info.default

            if hasattr(field_info, 'default_factory') and field_info.default_factory is not None:
                defaults[field_name] = field(default_factory=field_info.default_factory)

        return annotations, defaults
    else:
        annotations = dict(cls.__annotations__) if hasattr(cls, '__annotations__') else {}
        defaults = {}

        if hasattr(cls, '__init__'):
            sig = inspect.signature(cls.__init__)
            for param_name, param in sig.parameters.items():
                if param_name == 'self':
                    continue
                if param.annotation != inspect.Parameter.empty:
                    annotations[param_name] = param.annotation
                if param.default != inspect.Parameter.empty:
                    defaults[param_name] = param.default

        return annotations, defaults


def _is_pydantic_model(cls: Type) -> bool:
    try:
        from pydantic import BaseModel
        return isinstance(cls, type) and issubclass(cls, BaseModel)
    except ImportError:
        return False


def _convert_type(field_type: Type) -> Type:
    if isinstance(field_type, type) and field_type.__module__ == "builtins":
        return field_type

    if hasattr(field_type, '__origin__'):
        origin_type = get_origin(field_type)
        args = get_args(field_type)

        if origin_type is Union:
            if type(None) in args:
                non_none_types = [arg for arg in args if arg is not type(None)]
                if len(non_none_types) != 1:
                    raise ParseError(
                        f"Unsupported Union type with multiple non-None types: {field_type}"
                    )
                return _convert_type(non_none_types[0])
            else:
                raise ParseError(
                    f"Unsupported Union type without None: {field_type}"
                )

        if origin_type and args:
            converted_args = []
            for arg in args:
                converted_arg = _convert_type(arg)
                converted_args.append(converted_arg)

            if converted_args != list(args):
                return origin_type[tuple(converted_args)]

        return field_type

    if isinstance(field_type, type):
        try:
            return CS[field_type]
        except (TypeError, ParseError) as e:
            raise ParseError(
                f"Cannot convert type '{field_type.__name__}' to CompoundScalar: {e}"
            )

    return field_type


def dataclass_to_compound_scalar(dataclass_class: Type) -> Type:
    """
    Convert a dataclass, Pydantic model, or plain Python class to a CompoundScalar class.

    Supports:
    - @dataclass decorated classes
    - Pydantic models (v1 and v2)
    - Plain Python classes with __annotations__
    - Nested compound types (recursively converted)

    Results are cached.
    """
    raw_annotations, raw_defaults = _extract_class_fields(dataclass_class)

    annotations: dict[str, Type] = {}
    defaults: dict[str, Any] = {}

    for field_name, field_type in raw_annotations.items():
        annotations[field_name] = _convert_type(field_type)

        if field_name in raw_defaults:
            defaults[field_name] = raw_defaults[field_name]
        elif get_origin(field_type) is Union and type(None) in get_args(field_type):
            defaults[field_name] = None

    cs_class = _create_compound_scalar_class(
        dataclass_class.__name__,
        annotations,
        defaults
    )

    _DATACLASS_CS_CACHE[dataclass_class] = cs_class

    return cs_class
