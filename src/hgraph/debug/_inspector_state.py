import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

import psutil
from perspective import Table

from hgraph import CompoundScalar
from hgraph._impl._runtime._node import _SenderReceiverState
from hgraph.adaptors.perspective import PerspectiveTablesManager
from hgraph.debug._inspector_observer import InspectionObserver


@dataclass
class InspectorState(CompoundScalar):
    observer: InspectionObserver = None
    manager: PerspectiveTablesManager = None
    table: Table = None
    total_cycle_table: Table = None

    process: psutil.Process = field(default_factory=lambda: psutil.Process(os.getpid()))

    requests: _SenderReceiverState = field(default_factory=_SenderReceiverState)
    last_request_process_time: datetime = None

    graph_subscriptions: dict = field(default_factory=lambda: dict())  # graph_id -> item_id
    node_subscriptions: dict = field(default_factory=lambda: defaultdict(set))  # node_id -> item_id
    node_item_subscriptions: dict = field(default_factory=lambda: defaultdict(set))  # node_id -> {item_ids}
    found_items: set = field(default_factory=set)

    value_data: list = field(default_factory=list)
    value_removals: set = field(default_factory=set)
    tick_data: dict = field(default_factory=dict)
    perf_data: list = field(default_factory=list)
    total_data_prev: list = field(default_factory=dict)
    total_data: list = field(default_factory=lambda: defaultdict(list))
    last_publish_time: datetime = None
    inspector_time: float = 0.
