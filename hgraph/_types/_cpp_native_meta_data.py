from typing import Type, TypeVar, Optional, _GenericAlias

from hgraph._types._type_meta_data import HgTypeMetaData, ParseError
from hgraph._types._scalar_type_meta_data import HgScalarTypeMetaData, HgCompoundScalarType
from hgraph._types._generic_rank_util import scale_rank

__all__ = ("HgCppNativeScalarType",)


class HgCppNativeScalarType(HgScalarTypeMetaData):
    """
    Metadata type for CppNative[T] wrapped CompoundScalar types.

    This wrapper indicates that the CompoundScalar should use C++ field expansion
    (storing fields in C++ memory) rather than opaque Python object storage.
    """

    is_atomic = False
    wrapped_type: HgCompoundScalarType

    def __init__(self, wrapped_type: HgCompoundScalarType):
        self.wrapped_type = wrapped_type

    @property
    def is_resolved(self) -> bool:
        return self.wrapped_type.is_resolved

    @property
    def py_type(self) -> Type:
        from hgraph._types._cpp_native_type import CppNative
        return CppNative[self.wrapped_type.py_type]

    @property
    def scalar_py_type(self) -> Type:
        """Returns the underlying CompoundScalar type (unwrapped)."""
        return self.wrapped_type.py_type

    @property
    def meta_data_schema(self) -> dict[str, "HgScalarTypeMetaData"]:
        return self.wrapped_type.meta_data_schema

    @property
    def cpp_type(self):
        """Get the C++ TypeMeta for this CppNative-wrapped compound scalar.

        Always returns the expanded Bundle TypeMeta (field-by-field C++ storage).
        """
        return self.wrapped_type._get_expanded_cpp_type()

    @property
    def type_vars(self):
        return self.wrapped_type.type_vars

    @property
    def generic_rank(self) -> dict[type, float]:
        return scale_rank(self.wrapped_type.generic_rank, 0.99)

    def matches(self, tp: "HgTypeMetaData") -> bool:
        if type(tp) is HgCppNativeScalarType:
            return self.wrapped_type.matches(tp.wrapped_type)
        # Also match against the unwrapped type
        if type(tp) is HgCompoundScalarType:
            return self.wrapped_type.matches(tp)
        return False

    def resolve(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], weak=False) -> "HgTypeMetaData":
        if self.is_resolved:
            return self
        resolved = self.wrapped_type.resolve(resolution_dict, weak)
        if isinstance(resolved, HgCompoundScalarType):
            return HgCppNativeScalarType(resolved)
        return resolved

    def do_build_resolution_dict(self, resolution_dict: dict[TypeVar, "HgTypeMetaData"], wired_type: "HgTypeMetaData"):
        if isinstance(wired_type, HgCppNativeScalarType):
            self.wrapped_type.build_resolution_dict(resolution_dict, wired_type.wrapped_type)
        elif isinstance(wired_type, HgCompoundScalarType):
            self.wrapped_type.build_resolution_dict(resolution_dict, wired_type)
        else:
            super().do_build_resolution_dict(resolution_dict, wired_type)

    @classmethod
    def parse_type(cls, value_tp) -> Optional["HgTypeMetaData"]:
        from hgraph._types._cpp_native_type import CppNative

        if isinstance(value_tp, _GenericAlias) and value_tp.__origin__ is CppNative:
            if len(value_tp.__args__) != 1:
                raise ParseError(f"CppNative requires exactly one type argument, got {len(value_tp.__args__)}")
            inner_type = value_tp.__args__[0]
            inner_meta = HgCompoundScalarType.parse_type(inner_type)
            if inner_meta is None:
                raise ParseError(f"CppNative requires a CompoundScalar type, got {inner_type}")
            if not isinstance(inner_meta, HgCompoundScalarType):
                raise ParseError(f"CppNative requires a CompoundScalar type, got {type(inner_meta).__name__}")
            return HgCppNativeScalarType(inner_meta)
        return None

    @classmethod
    def parse_value(cls, value) -> Optional["HgTypeMetaData"]:
        # CppNative is a type wrapper, not a value type
        return None

    def __eq__(self, o: object) -> bool:
        return type(o) is HgCppNativeScalarType and self.wrapped_type == o.wrapped_type

    def __str__(self) -> str:
        return f"CppNative[{str(self.wrapped_type)}]"

    def __repr__(self) -> str:
        return f"HgCppNativeScalarType({repr(self.wrapped_type)})"

    def __hash__(self) -> int:
        from hgraph._types._cpp_native_type import CppNative
        return hash(CppNative) ^ hash(self.wrapped_type)
