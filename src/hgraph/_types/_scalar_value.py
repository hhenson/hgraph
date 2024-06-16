from abc import ABC, abstractmethod
from typing import _SpecialGenericAlias, _tp_cache, TypeVar

import numpy as np

from hgraph._types._scalar_types import Size

__all__ = (
    "ScalarValue",
    "KeyableScalarValue",
    "Array",
)


class ScalarValue(ABC):
    """
    A basic wrapper that allows for basic operations on the value without knowing the type.
    This performs basic type erasure.
    It is possible to cast the value back to the original type.
    """

    @abstractmethod
    def __str__(self) -> str:
        """
        The string representation of the value. Useful for logging or cases where a string
        representation could be useful. Note a string representation is no required to be unique.
        """

    @abstractmethod
    def __eq__(self, other: "ScalarValue") -> bool:
        """
        Provide basic equality comparison.
        """

    @abstractmethod
    def __copy__(self) -> "ScalarValue":
        """
        Provide a copy of the value.
        """

    @abstractmethod
    def __lt__(self, other):
        """
        Provide the ability to partially order the value related to other scalar values.
        """

    @abstractmethod
    def cast(self, tp: type):
        """
        Cast the value to the type provided. This will raise an exception if it is not possible
        to cast to this type.
        """


class KeyableScalarValue(ScalarValue):
    """
    A scalar value that can be used as a key in a dictionary.
    """

    @abstractmethod
    def __hash__(self) -> int:
        """
        Provide a hash for the value.
        """


class _ArrayTypeclass(_SpecialGenericAlias, _root=True):
    @_tp_cache
    def __getitem__(self, params):
        if not isinstance(params, tuple):
            params = (params,)
        if len(params) == 0:
            raise TypeError("Array must have at least a type")
        if len(params) > 1:
            for param in params[1:]:
                if (isinstance(param, type) and issubclass(param, Size)) or isinstance(param, TypeVar):
                    continue
                raise TypeError("Array must have Size types after the data-type parameter")
        return self.copy_with(params)


Array = _ArrayTypeclass(np.ndarray, -1, inst=False, name="Array")
