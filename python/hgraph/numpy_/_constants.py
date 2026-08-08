from typing import TypeVar

from hgraph import Array

ARRAY = TypeVar("ARRAY", bound=Array)
ARRAY_1 = TypeVar("ARRAY_1", bound=Array)

__all__ = ("ARRAY", "ARRAY_1")
