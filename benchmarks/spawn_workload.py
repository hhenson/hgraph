"""Importable process stages for spawn_pipeline.py."""
import json
import os
from pathlib import Path
import hgraph as hg


@hg.graph
def identity(value: hg.TIME_SERIES_TYPE) -> hg.TIME_SERIES_TYPE:
    return value


@hg.compute_node
def cpu_stage(value: hg.TS[int], iterations: int) -> hg.TS[int]:
    accumulator = value.value
    for index in range(iterations):
        accumulator = (accumulator * 33 + index) % 1000000007
    return accumulator


@hg.sink_node
def consume(value: hg.TIME_SERIES_TYPE, path: str, state: hg.STATE = None):
    state.count = getattr(state, "count", 0) + 1


@consume.stop
def consume_stop(path: str, state: hg.STATE = None):
    Path(path).write_text(json.dumps({"count": getattr(state, "count", 0), "pid": os.getpid()}))
