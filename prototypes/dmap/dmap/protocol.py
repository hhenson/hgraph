"""Wire messages for the dmap_ prototype.

Serialisation is deliberately naive: pickle over a ``multiprocessing.Pipe``.
RFC 0037 names RFC 0017's binary value codec as the real answer; nothing here
should be read as a proposal for the wire format. The point of the prototype
is the *protocol shape* -- one request and one reply per engine cycle,
carrying only what changed -- not its encoding.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["Dispatch", "Result", "Shutdown", "partition_of"]


@dataclass(frozen=True)
class Dispatch:
    """One engine cycle's work for one worker.

    ``seq`` is the parent's cycle counter and exists only so a reply can be
    matched to its request while debugging; the protocol is strictly
    request/reply so ordering alone would do.
    """

    seq: int
    #: Partitioned values that ticked this cycle, restricted to this worker's keys.
    values: dict[str, float] = field(default_factory=dict)
    #: Keys that left the input this cycle, restricted to this worker's keys.
    removed: tuple[str, ...] = ()
    #: The broadcast input, present only on cycles where it ticked.
    shared: float | None = None
    shared_ticked: bool = False


@dataclass(frozen=True)
class Result:
    """One engine cycle's output from one worker."""

    seq: int
    #: Output values that ticked this cycle.
    values: dict[str, float] = field(default_factory=dict)
    #: Output keys removed this cycle.
    removed: tuple[str, ...] = ()
    error: str | None = None


@dataclass(frozen=True)
class Shutdown:
    pass


def partition_of(key: str, workers: int) -> int:
    """Stable key -> worker mapping.

    Deliberately NOT ``hash()``: CPython salts string hashing per process, so
    ``hash()`` would place a key differently in the parent and in a spawned
    worker. RFC 0037 requires the partitioner to be stable across processes,
    and this is the cheapest thing that is.
    """
    import zlib

    return zlib.crc32(key.encode("utf-8")) % workers
