from hashlib import shake_256
from inspect import get_annotations
from typing import TYPE_CHECKING, Type, TypeVar, KeysView, ItemsView, ValuesView, get_type_hints, ClassVar

from frozendict import frozendict

if TYPE_CHECKING:
    from hgraph._types._type_meta_data import HgTypeMetaData


__all__ = ("AbstractSchema",)


class AbstractSchema:
    """
    Describes the core concepts of a schema based object. The object contains a view of the schema in terms
    of the ``HgTypeMetaData`` representation. This provides additional information such as the resolved state
    (if any of the attributes are TypeVar templates) and con contain partial specialisations of templated types.
    There are two key implementations, namely the ``CompoundScalar`` and the ``TimeSeriesSchema``. These provide
    a scalar and time-series aggregated type information.
    """
    __meta_data_schema__: frozendict[str, "HgTypeMetaData"] = {}
    __resolved__: dict[str, Type["AbstractSchema"]] = {}  # Cache of resolved classes
    __partial_resolution__: frozendict[TypeVar, Type]
    __partial_resolution_parent__: Type["AbstractSchema"]

    @classmethod
    def _schema_index_of(cls, key: str) -> int:
        return list(cls.__meta_data_schema__.keys()).index(key)

    @classmethod
    def _schema_get(cls, key: str) -> "HgTypeMetaData":
        return cls.__meta_data_schema__.get(key)

    @classmethod
    def _schema_items(cls) -> ItemsView[str, "HgTypeMetaData"]:
        return cls.__meta_data_schema__.items()

    @classmethod
    def _schema_values(cls) -> ValuesView["HgTypeMetaData"]:
        return cls.__meta_data_schema__.values()

    @classmethod
    def _schema_keys(cls) -> KeysView[str]:
        return cls.__meta_data_schema__.keys()

    @classmethod
    def _parse_type(cls, tp: Type) -> "HgTypeMetaData":
        """
        Parse the type using the appropriate HgTypeMetaData instance.
        By default, we use the top level parser.
        """
        from hgraph._types._type_meta_data import HgTypeMetaData
        return HgTypeMetaData.parse_type(tp)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        from hgraph._types._type_meta_data import ParseError

        schema = dict(cls.__meta_data_schema__)
        for k, v in get_annotations(cls, eval_str=True).items():
            if getattr(v, "__origin__", None) == ClassVar:
                continue
            s = cls._parse_type(v)
            if s is None:
                raise ParseError(f"When parsing '{cls}', unable to parse item {k} with value {v}")
            if k in schema and not (s_p := schema[k]).matches(s):
                raise ParseError(f"Attribute: '{k}' in '{cls}' is already defined in a parent as '{str(s_p)}'"
                                 f" but attempted to be redefined as '{str(s)}")
            schema[k] = s
        cls.__meta_data_schema__ = frozendict(schema)

    @classmethod
    def _root_cls(cls) -> Type["AbstractSchema"]:
        """This class or the __partial_resolution_parent__ if this is a partially resolved class"""
        return getattr(cls, "__partial_resolution_parent__", None) or getattr(cls, "__root__", cls)

    @classmethod
    def _create_resolved_class(cls, schema: dict[str, "HgTypeMetaData"]) -> Type["AbstractSchema"]:
        """Create a 'resolved' instance class and cache as appropriate"""
        suffix = ','.join(f'{k}:{v}' for k, v in schema.items())
        root_cls = cls._root_cls()
        cls_name = f"{root_cls.__name__}_{shake_256(bytes(suffix, 'utf8')).hexdigest(6)}"
        r_cls: Type["AbstractSchema"]
        if (r_cls := cls.__resolved__.get(cls_name)) is None:
            r_cls = type(cls_name, (root_cls,), {})
            r_cls.__meta_data_schema__ = frozendict(schema)
            r_cls.__root__ = root_cls
            r_cls.__name__ = f"{root_cls.__name__}[{suffix}]"
            cls.__resolved__[cls_name] = r_cls
        return r_cls

    @classmethod
    def _create_partial_resolved_class(cls, resolution_dict) -> Type["AbstractSchema"]:
        suffix = ','.join(f"{k}:{str(v)}" for k, v in resolution_dict.items())
        cls_name = f"{cls.__name__}_{shake_256(bytes(suffix, 'utf8')).hexdigest(6)}"
        r_cls: Type["AbstractSchema"]
        if (r_cls := cls.__resolved__.get(cls_name)) is None:
            r_cls = type(cls_name, (cls,), {})
            r_cls.__partial_resolution__ = frozendict(resolution_dict)
            r_cls.__parameters__ = cls.__parameters__
            r_cls.__partial_resolution_parent__ = cls._root_cls()
            cls.__resolved__[cls_name] = r_cls
        return r_cls

    def __class_getitem__(cls, items):
        from hgraph._types._type_meta_data import ParseError
        resolution_dict = dict(getattr(cls, '__partial_resolution__', {}))
        if type(items) is not tuple:
            items = (items,)
        if len(items) > len(cls.__parameters__):
            raise ParseError(f"'{cls} was provided more elements then generic parameters")
        has_slice = False
        for item, parm in zip(items, cls.__parameters__):
            if isinstance(item, slice):
                has_slice = True
                k = item.start
                v = item.stop
            elif has_slice:
                raise ParseError(f"'{cls}' has supplied slice parameters already, "
                                 f"non-slice parameters are no longer accepted")
            else:
                k = parm
                v = item
            if not isinstance(k, TypeVar):
                raise ParseError(f"'{cls}' type '{k}' is not an instance of TypeVar as required")
            if k in resolution_dict:
                raise ParseError(f"'{cls}' has already defined '{k}'")
            if parsed_v := cls._parse_type(v):
                resolution_dict[k] = parsed_v
            else:
                raise ParseError(f"In '{cls}' type '{k}': '{v}' was unable to parse as a valid type")
        if len(resolution_dict) < len(cls.__parameters__):
            # Only a partial resolution is place
            tp = cls._create_partial_resolved_class(resolution_dict)
        else:
            v: HgTypeMetaData
            tp = cls._create_resolved_class({k: v.resolve(resolution_dict) for k, v in cls.__meta_data_schema__.items()})

        tp.__args__ = items
        return tp


AbstractSchema.__annotations__ = {}