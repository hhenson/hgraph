from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SourceCodeDetails:
    file: Path
    start_line: int

    def __str__(self):
        return f"{str(self.file)}: {self.start_line}"
