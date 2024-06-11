from collections import deque
from dataclasses import dataclass, field

from hgraph import CompoundScalar

__all__ = ("_BufferState",)


@dataclass
class _BufferState(CompoundScalar):
    buffer: deque = field(default_factory=deque)


