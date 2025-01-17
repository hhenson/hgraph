from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import Generic

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


@graph
def combine_status_messages(message1: TS[str], message2: TS[str]) -> TS[str]:
    return merge_join(message1, message2, separator="; ")


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
