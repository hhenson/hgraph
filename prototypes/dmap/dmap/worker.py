"""The worker process: hosts an ordinary ``map_`` over its key partition.

RFC 0037's claim is that a worker needs no new nested-graph machinery -- it
runs a normal ``map_`` and lets the existing per-key lifecycle do the work.
This module is the test of that claim, and it holds: the only worker-specific
code is the message-in node, the message-out node, and the loop between them.

Driving model (a prototype simplification, see README.md "Known gap"):
the worker graph runs in REAL_TIME and is driven by a ``push_queue``, so one
dispatch becomes exactly one engine cycle. Engine time inside the worker is the
worker's wall clock, NOT the parent's evaluation time. RFC 0037 wants the
parent's time supplied externally via ``GraphView::evaluate(evaluation_time)``,
which is not exposed to Python. The v0 kernels are all time-independent, so
this does not affect their results -- but it is the first thing to fix.
"""

from __future__ import annotations

import os
import queue
import threading
import traceback
from datetime import timedelta

from hgraph import (
    REMOVE,
    TS,
    TSD,
    EvaluationMode,
    compute_node,
    graph,
    map_,
    push_queue,
    run_graph,
    sink_node,
)

from .kernels import KERNELS, SHARED_ARG_KERNELS
from .protocol import Dispatch, Result, Shutdown

__all__ = ["worker_main"]

# Module-level rendezvous between the worker's main thread and its graph. A
# worker process hosts exactly one graph, so a module global is adequate here;
# it is not a pattern to carry into an implementation.
_SEND_INTO_GRAPH: dict[str, object] = {}
_RESULTS: "queue.Queue[Result]" = queue.Queue()
_READY = threading.Event()
_FAILURE: list[str] = []


@push_queue(TS[object])
def _incoming(sender) -> TS[object]:
    """The one and only way anything enters the worker graph.

    A single push_queue carrying one message object keeps a dispatch ATOMIC:
    the partitioned values, the removals and the broadcast value all land in
    the same engine cycle. Three separate queues would not -- the real-time
    executor could split them across cycles and the worker would evaluate a
    half-applied dispatch.
    """
    _SEND_INTO_GRAPH["send"] = sender
    # The push_queue function IS the node's start hook, so this is the exact
    # moment the graph can accept work. Signalling from anywhere else means
    # polling, and a busy-wait here holds the GIL and starves the very thread
    # being waited for.
    _READY.set()


@compute_node
def _to_tsd(msg: TS[object]) -> TSD[str, TS[float]]:
    """Message -> the multiplexed input, including key removal."""
    dispatch: Dispatch = msg.value
    out: dict[str, object] = dict(dispatch.values)
    for key in dispatch.removed:
        out[key] = REMOVE
    return out


@compute_node
def _to_shared(msg: TS[object]) -> TS[float]:
    """Message -> the broadcast input, ticking only on cycles where it ticked."""
    dispatch: Dispatch = msg.value
    if dispatch.shared_ticked:
        return dispatch.shared
    return None  # no tick


@sink_node
def _emit(out: TSD[str, TS[float]], msg: TS[object]):
    """One reply per dispatch, carrying only what ticked.

    Driven by BOTH the map_ output and the message. Taking ``out`` as an input
    makes this node rank after it, so the outputs are already computed; taking
    ``msg`` -- which ticks on every dispatch -- guarantees the node fires even
    on a cycle that produced no output at all. Without that, a dispatch whose
    keys all happened to produce nothing would never reply and the parent would
    block forever.
    """
    _RESULTS.put(
        Result(
            seq=msg.value.seq,
            values={k: v.value for k, v in out.modified_items()},
            removed=tuple(out.removed_keys()),
        )
    )


def _build(kernel_name: str):
    kernel = KERNELS[kernel_name]
    takes_shared = kernel_name in SHARED_ARG_KERNELS

    @graph
    def worker_graph():
        msg = _incoming()
        values = _to_tsd(msg)
        if takes_shared:
            out = map_(kernel, values, _to_shared(msg))
        else:
            out = map_(kernel, values)
        _emit(out, msg)

    return worker_graph


def worker_main(conn, kernel_name: str, run_seconds: int = 3600) -> None:
    """Worker process entry point."""
    def run():
        try:
            g = _build(kernel_name)
            run_graph(
                g,
                run_mode=EvaluationMode.REAL_TIME,
                end_time=timedelta(seconds=run_seconds),
            )
        except BaseException:  # pragma: no cover - surfaces as a worker error
            traceback.print_exc()
            _FAILURE.append(traceback.format_exc())
            _READY.set()

    threading.Thread(target=run, daemon=True).start()
    if not _READY.wait(timeout=60):
        conn.send("timed out building the worker graph")
        return
    if _FAILURE:
        conn.send(_FAILURE[0])
        return
    conn.send(f"ready:{os.getpid()}")

    while True:
        message = conn.recv()
        if isinstance(message, Shutdown):
            break
        try:
            _SEND_INTO_GRAPH["send"](message)
            conn.send(_RESULTS.get(timeout=60))
        except BaseException as exc:  # pragma: no cover - surfaces to the parent
            conn.send(Result(seq=message.seq, error=f"{type(exc).__name__}: {exc}"))

    # The graph thread is a daemon and the registries are immortal by design;
    # a normal interpreter exit can die in the final GC on Linux, so leave now.
    os._exit(0)
