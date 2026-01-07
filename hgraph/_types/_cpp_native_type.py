from typing import Generic, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from hgraph._types._scalar_types import CompoundScalar

__all__ = ("CppNative",)

T = TypeVar("T", bound="CompoundScalar")


class CppNative(Generic[T]):
    """
    Type wrapper to indicate that a CompoundScalar should use C++ field expansion.

    By default, CompoundScalar types are stored as opaque Python objects in C++.
    Use this wrapper to enable field-by-field storage in C++ memory, which can
    improve performance for frequently accessed fields.

    Usage::

        # As a type annotation wrapper
        @compute_node
        def my_node(ts: TS[CppNative[MyScalar]]) -> TS[int]:
            return ts.value.field1

        # Alternatively, use the cpp_native class flag directly:
        @dataclass(frozen=True)
        class MyScalar(CompoundScalar, cpp_native=True):
            field1: int
            field2: str

    Note:
        - When using CppNative, the CompoundScalar fields are expanded in C++ memory
        - This is beneficial for performance when fields are accessed frequently
        - Without CppNative, the Python object identity is preserved (useful for hashing)
    """
    pass
