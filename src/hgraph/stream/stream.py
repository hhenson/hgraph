from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import Generic, Tuple

from pytz import UTC

from hgraph import COMPOUND_SCALAR, Base, graph, TS, default, max_, compute_node, CompoundScalar, SCALAR, add_, TSB, \
    WiringNodeClass, AUTO_RESOLVE, combine, convert

__all__ = ("Data", "StreamStatus", "Stream", "combine_statuses", "combine_status_messages", "merge_join")


class StreamStatus(Enum):
    # Values ordered by increasing severity
    OK = 0   # price is valid, up to date and ticking
    STALE = 1 # price exists but is stale
    WAITING = 2  # waiting on dependencies to come up
    NA = 3  # outside of hours, or other reason why the price is not available for a valid request
    ERROR = 4  # price is invalid, there is a failure in the pricing pipeline
    FATAL = 5  # this price stream is invalid and will not be expected to ever produce a price


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


# Bloomberg bid/ask spread (x) for (symbol) is greater than permitted maximum ((z) ticks)


STATUS_MESSAGE_PATTERN_DUPLICATES = []

def register_status_message_pattern(pattern: str):
    # Register a pattern to search for when combining status messages, and collapse groups into a comma-separated list
    # The pattern should have a single (\w+) group in it - e.g. "For (\w+), price is stale".

    def _escape(s):
        return s.replace("(", r"\(").replace(")", r"\)")

    substr1, substr2 = pattern.split(r"(\w+)", maxsplit=1)
    pattern = f"^{_escape(substr1)}(.*){_escape(substr2)}$"

    STATUS_MESSAGE_PATTERN_DUPLICATES.append((pattern, substr1, substr2))


@compute_node
def combine_status_messages(message1: TS[str], message2: TS[str]) -> TS[str]:
    components = set(message1.value.split("; ") + message2.value.split("; "))
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
def stream_op(lhs: TSB[Stream[COMPOUND_SCALAR]], rhs: TSB[Stream[COMPOUND_SCALAR]], op: WiringNodeClass, cs_: type[COMPOUND_SCALAR] = AUTO_RESOLVE):
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
            sorted({piece
                    for piece in chain(str1.strip().split(separator), str2.strip().split(separator))
                    if piece}))
