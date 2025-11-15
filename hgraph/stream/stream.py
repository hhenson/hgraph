from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import Generic

from hgraph import (
    COMPOUND_SCALAR,
    Base,
    graph,
    TS,
    default,
    max_,
    compute_node,
    CompoundScalar,
    SCALAR,
    add_,
    TSB,
    WiringNodeClass,
    AUTO_RESOLVE,
    combine,
    convert,
)

__all__ = (
    "Data",
    "StreamStatus",
    "Stream",
    "combine_statuses",
    "combine_status_messages",
    "merge_join",
    "register_status_message_pattern"
)


class StreamStatus(Enum):
    # Values ordered by increasing severity
    OK = 0  # data is valid, up to date and ticking
    STALE = 1  # data exists but is out of date
    WAITING = 2  # data is waiting on dependencies (may or may not have a value yet)
    NA = 3  # data is not available for a valid request (e.g. out of hours)
    ERROR = 4  # data is invalid, there is a failure in the pipeline
    FATAL = 5  # data is invalid and is not expected to ever be valid


@dataclass(frozen=True)
class Stream(Base[COMPOUND_SCALAR], Generic[COMPOUND_SCALAR]):
    status: StreamStatus
    status_msg: str


@dataclass
class Data(CompoundScalar, Generic[SCALAR]):
    values: SCALAR
    timestamp: datetime


@graph
def combine_statuses(status1: TS[StreamStatus], status2: TS[StreamStatus]) -> TS[StreamStatus]:
    return default(max_(status1, status2, __strict__=True), StreamStatus.WAITING)


STATUS_MESSAGE_PATTERN_DUPLICATES = []


def register_status_message_pattern(pattern: str):
    # Register a pattern to search for when combining status messages, and collapse groups into a comma-separated list
    # The pattern should have a single (\w+) group in it - e.g. "For (\w+), price is stale".

    def _escape(s):
        return s.replace("(", r"\(").replace(")", r"\)")

    substr1, substr2 = pattern.split(r"(\w+)", maxsplit=1)
    pattern = f"^{_escape(substr1)}(.*){_escape(substr2)}$"

    STATUS_MESSAGE_PATTERN_DUPLICATES.append((pattern, substr1, substr2))


@compute_node(valid=())
def combine_status_messages(message1: TS[str], message2: TS[str]) -> TS[str]:
    message1 = message1.value
    message2 = message2.value
    if message1 is None:
        return message2
    elif message2 is None:
        return message1
    elif message1 in message2:
        return message2
    elif message2 in message1:
        return message1
    elif message1 and message2:
        components = set(message1.split("; ") + message2.split("; "))
    elif message1:
        components = set(message1.split("; "))
    elif message2:
        components = set(message2.split("; "))

    for pattern, substr1, substr2 in STATUS_MESSAGE_PATTERN_DUPLICATES:
        components = dedup_components(pattern, substr1, substr2, components)
    return "; ".join(sorted(components))


def dedup_components(pattern, substr1, substr2, components) -> str:
    component_messages = set()
    for comp1 in components:
        if not comp1:
            continue
        outer_done = False
        if m1 := re.search(pattern, comp1):
            for comp2 in components:
                if comp2 and comp1 != comp2:
                    if m2 := re.search(pattern, comp2):
                        ids = ", ".join(sorted(set(m1.group(1).split(", ") + m2.group(1).split(", "))))
                        component_messages.add(f"{substr1}{ids}{substr2}")
                        outer_done = True
                    else:
                        component_messages.add(comp2)
        if not outer_done:
            component_messages.add(comp1)
    return component_messages


@graph
def stream_op(
    lhs: TSB[Stream[COMPOUND_SCALAR]],
    rhs: TSB[Stream[COMPOUND_SCALAR]],
    op: WiringNodeClass,
    cs_: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
):
    return combine[TSB[Stream[cs_]]](
        **op(convert[TSB[cs_]](lhs), convert[TSB[cs_]](rhs)).as_dict(),
        status=combine_statuses(lhs.status, rhs.status),
        status_msg=combine_status_messages(lhs.status_msg, rhs.status_msg),
    )


@graph(overloads=add_)
def add_streams(lhs: TSB[Stream[COMPOUND_SCALAR]], rhs: TSB[Stream[COMPOUND_SCALAR]]) -> TSB[Stream[COMPOUND_SCALAR]]:
    return stream_op(lhs, rhs, add_)


@compute_node(valid=())
def merge_join(str1: TS[str], str2: TS[str], separator: str) -> TS[str]:
    str1 = str1.value
    str2 = str2.value
    if str1 is None:
        return str2
    elif str2 is None:
        return str1
    elif not str1 and not str2:
        return ""
    else:
        return separator.join(
            sorted({piece for piece in chain(str1.strip().split(separator), str2.strip().split(separator)) if piece})
        )
