from typing import TypeVar

from hgraph import Array

__all__ = ("ARRAY", "ARRAY_1")

ARRAY = TypeVar("ARRAY", bound=Array)
ARRAY_1 = TypeVar("ARRAY_1", bound=Array)
