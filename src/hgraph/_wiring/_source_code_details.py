from dataclasses import dataclass
from pathlib import Path


__all__ = ("SourceCodeDetails",)


@dataclass(frozen=True)
class SourceCodeDetails:
    file: Path
    start_line: int

    def __str__(self):
        return f"{str(self.file)}:{self.start_line}"

    def __lt__(self, other):
        return str(self) < str(other)