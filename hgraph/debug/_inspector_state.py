import os
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
import threading
from typing import Callable

import psutil
from perspective import Table

from hgraph import CompoundScalar
from hgraph._impl._runtime._node import _SenderReceiverState
from hgraph.adaptors.perspective import PerspectiveTablesManager
from hgraph.debug._inspector_observer import InspectionObserver


@dataclass
class InspectorRequestsQueue:
    lock: threading.RLock = field(default_factory=threading.RLock)
    queue: deque = field(default_factory=deque)
    notify: Callable = None

    def __call__(self, value):
        self.enqueue(value)

    def enqueue(self, value):
        with self.lock:
            self.queue.append(value)
            self.notify()

    def dequeue(self):
        with self.lock:
            return self.queue.popleft() if self.queue else None

    def __bool__(self):
        with self.lock:
            return bool(self.queue)

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


@dataclass
class InspectorState(CompoundScalar):
    observer: InspectionObserver = None
    manager: PerspectiveTablesManager = None
    table: Table = None
    total_cycle_table: Table = None

    process: psutil.Process = field(default_factory=lambda: psutil.Process(os.getpid()))

    requests_queue: Callable = None
    requests: InspectorRequestsQueue = field(default_factory=InspectorRequestsQueue)
    last_request_process_time: datetime = None

    graph_subscriptions: dict = field(default_factory=lambda: dict())  # graph_id -> item_id
    node_subscriptions: dict = field(default_factory=lambda: defaultdict(set))  # node_id -> item_id
    node_item_subscriptions: dict = field(default_factory=lambda: defaultdict(set))  # node_id -> {item_ids}
    found_items: set = field(default_factory=set)

    value_removals: set = field(default_factory=set)
    tick_node_ids = set()

    value_data: dict = field(default_factory=dict)
    tick_data: dict = field(default_factory=dict)
    perf_data: dict = field(default_factory=dict)
    
    track_detailed_performance: bool = False
    detailed_perf_data: dict = field(default_factory=lambda: defaultdict(list))
    detailed_perf_data_time: datetime = datetime.utcnow()
    detailed_perf_data_node_times = dict()
    detailed_perf_data_graph_times = dict()
    
    total_data_prev: list = field(default_factory=dict)
    total_data: list = field(default_factory=lambda: defaultdict(list))
    last_publish_time: datetime = None
    inspector_time: float = 0.0
