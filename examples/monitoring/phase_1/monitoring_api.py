from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from hgraph import graph, TSD, TS, CompoundScalar


class MonitorStatus(Enum):
    GREY = "GREY"
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    ORANGE = "ORANGE"
    RED = "RED"
    PURPLE = "PURPLE"


@dataclass
class ProcessConfiguration(CompoundScalar):
    process_identifier: str
    expected_start_time: datetime
    expected_end_time: datetime
    tolerance: timedelta
    dependencies: frozenset[str]


@graph
def register_process_configuration(config: TS[ProcessConfiguration]):
    ...


@graph
def monitoring_status() -> TSD[str, TS[MonitorStatus]]:
    """Subscribe to the status of the monitoring system"""


@graph
def mark_process_started(id: TS[str]):
    """Mark the process as started """


@graph
def mark_process_completed(id: TS[str], failure_reason: TS[str]):
    """Mark the process as completed"""