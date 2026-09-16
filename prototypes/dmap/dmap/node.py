"""``dmap_``: the parent-side node.

RFC 0037's v1 shape, minus everything the RFC defers. Per cycle:

  partition what ticked -> dispatch to the workers that have work
  -> block for every reply -> merge into this node's output.

The barrier is synchronous, which is RFC 0037's v1 choice. ``graph.cpp`` has a
mid-cycle pause cursor that would let this be asynchronous, but a pause is
resolvable only by an enclosing ``mesh_`` today (the root graph throws
"root graph evaluation paused with no resolver"), so the prototype blocks.

Two node variants rather than one with an optional broadcast input: an
UNWIRED optional time-series input silently stops a node from ever evaluating
(see README.md, "What this cost us"), so ``shared`` is required where it is
declared at all.
"""

from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path

from hgraph import REMOVE, STATE, TS, TSD, compute_node

from .protocol import Dispatch, Result, Shutdown, partition_of
from .worker import worker_main

__all__ = ["dmap_", "dmap_shared_"]


class _Pool:
    """The worker processes and their pipes, owned by one dmap_ node."""

    def __init__(self, kernel_name: str, workers: int):
        ctx = mp.get_context("spawn")  # re-imports the module in the child,
        # which is how the worker ends up running the same wiring code.
        # Spawn starts a FRESH interpreter, so the child does not inherit this
        # process's sys.path and cannot import ``dmap.worker`` to unpickle the
        # target. PYTHONPATH is inherited through the environment, so the
        # prototype root goes there. (A real implementation installs the
        # package instead of carrying its own path.)
        root = str(Path(__file__).resolve().parents[1])
        existing = os.environ.get("PYTHONPATH", "")
        if root not in existing.split(os.pathsep):
            os.environ["PYTHONPATH"] = (
                f"{root}{os.pathsep}{existing}" if existing else root
            )
        self.workers = workers
        self.conns = []
        self.procs = []
        self.pids: list[int] = []
        for _ in range(workers):
            parent_conn, child_conn = ctx.Pipe()
            proc = ctx.Process(
                target=worker_main, args=(child_conn, kernel_name), daemon=True
            )
            proc.start()
            self.conns.append(parent_conn)
            self.procs.append(proc)
        for index, conn in enumerate(self.conns):
            if not conn.poll(timeout=120):
                raise RuntimeError(f"dmap_ worker {index} never became ready")
            hello = conn.recv()
            if not isinstance(hello, str) or not hello.startswith("ready:"):
                raise RuntimeError(f"dmap_ worker {index} failed to start: {hello}")
            self.pids.append(int(hello.split(":", 1)[1]))
        #: keys currently live in each partition, so a cycle that only ticks
        #: the broadcast input knows which workers actually have children.
        self.live: list[set[str]] = [set() for _ in range(workers)]
        if os.environ.get("DMAP_TRACE"):
            print(f"[dmap_] parent pid={os.getpid()} workers={self.pids}", flush=True)

    def close(self):
        for conn in self.conns:
            try:
                conn.send(Shutdown())
            except Exception:
                pass
        for proc in self.procs:
            proc.join(timeout=5)
            if proc.is_alive():
                proc.terminate()


def _cycle(pool: _Pool, seq: int, ts, shared_ticked: bool, shared_value):
    """One engine cycle: partition, dispatch, barrier, merge."""
    workers = pool.workers
    per_values: list[dict[str, float]] = [{} for _ in range(workers)]
    per_removed: list[list[str]] = [[] for _ in range(workers)]

    for key, view in ts.modified_items():
        index = partition_of(key, workers)
        per_values[index][key] = view.value
        pool.live[index].add(key)
    for key in ts.removed_keys():
        index = partition_of(key, workers)
        per_removed[index].append(key)
        pool.live[index].discard(key)

    # A worker is dispatched when it has changes of its own, or when the
    # broadcast input ticked and it holds any live key -- that second case is
    # what makes a shared input reach every child that exists.
    targets = [
        i
        for i in range(workers)
        if per_values[i] or per_removed[i] or (shared_ticked and pool.live[i])
    ]
    if not targets:
        return None

    for i in targets:
        pool.conns[i].send(
            Dispatch(
                seq=seq,
                values=per_values[i],
                removed=tuple(per_removed[i]),
                shared=shared_value,
                shared_ticked=shared_ticked,
            )
        )

    out: dict[str, object] = {}
    for i in targets:  # the barrier
        reply: Result = pool.conns[i].recv()
        if reply.error:
            raise RuntimeError(f"dmap_ worker {i} failed: {reply.error}")
        out.update(reply.values)
        for key in reply.removed:
            out[key] = REMOVE
    return out or None


@compute_node
def dmap_(
    ts: TSD[str, TS[float]],
    kernel: str,
    workers: int = 2,
    state: STATE = None,
) -> TSD[str, TS[float]]:
    """Evaluate ``kernel`` per key across ``workers`` processes.

    Contract: the result must equal ``map_(kernel, ts)`` exactly. Distribution
    is a throughput decision, never a semantic one -- see README.md for the
    cost, and RFC 0037 for why the equality has to hold.
    """
    state.seq += 1
    return _cycle(state.pool, state.seq, ts, False, None)


@dmap_.start
def _start(kernel: str, workers: int, state: STATE = None):
    state.seq = 0
    state.pool = _Pool(kernel, workers)


@dmap_.stop
def _stop(state: STATE = None):
    state.pool.close()


@compute_node
def dmap_shared_(
    ts: TSD[str, TS[float]],
    shared: TS[float],
    kernel: str,
    workers: int = 2,
    state: STATE = None,
) -> TSD[str, TS[float]]:
    """``dmap_`` with a broadcast input copied to every worker that has keys."""
    state.seq += 1
    return _cycle(
        state.pool,
        state.seq,
        ts,
        shared.modified,
        shared.value if shared.valid else None,
    )


@dmap_shared_.start
def _start_shared(kernel: str, workers: int, state: STATE = None):
    state.seq = 0
    state.pool = _Pool(kernel, workers)


@dmap_shared_.stop
def _stop_shared(state: STATE = None):
    state.pool.close()
