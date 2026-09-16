# `dmap_` prototype — validating RFC 0037

A naive, deliberately limited prototype of [RFC 0037](../../docs/source/rfc/rfc_0037_distributed_map.rst):
a `map_` whose per-key child graphs are evaluated in worker **processes**.

**Status: the v0 model is validated.** All five experiments produce output
identical to `map_`, at 1, 2 and 3 workers, with the work genuinely happening
in separate OS processes.

## Why it is partitioned from the tree

Everything lives under `prototypes/`, which is outside
`testpaths = ["python/tests"]` and `wheel.packages = ["python/hgraph"]` in
`pyproject.toml`, and is referenced by no CMakeLists. Nothing in the main tree
imports it. It can be deleted in one `rm -rf` without trace.

## What it does

```
parent graph                          worker process (x N)
------------                          --------------------
dmap_ node
  partition keys by crc32   --pipe->  push_queue -> map_(kernel) -> sink
  block on every reply      <-pipe--  one reply per dispatch
  merge into output TSD
```

Per RFC 0037's central claim, the worker hosts an **ordinary `map_`**. There is
no new nested-graph machinery: per-key child construction, teardown and state
are the existing, tested behaviour. The only worker-specific code is the
message-in node, the message-out node, and the loop between them.

## What v0 supports

| supported | not supported |
| --- | --- |
| `TSD[str, TS[float]]` partitioned input | any other key or value type |
| one `TS[float]` broadcast input | multiple broadcast inputs |
| per-key state in the child | references, services, contexts, shared outputs |
| keys added and removed mid-run | push sources inside a child |
| stateless and stateful kernels | children that schedule themselves |

The last row is the important restriction and it is deliberate: see
*Known gap* below.

## The cost — read this before using it

`dmap_` is **not** an optimisation to apply by default. Every cycle pays for
pickling a delta, two pipe traversals per worker with work, and a barrier on
the slowest worker. `experiments/cost.py` measures it rather than asserting it.

macOS, M-series, `--keys 64 --cycles 20`:

| per-key work | workers | `map_` | `dmap_` | ratio |
| --- | --- | --- | --- | --- |
| none (`spin=0`) | 1 | 0.003s | 0.112s | **36.3x slower** |
| none (`spin=0`) | 2 | 0.003s | 0.114s | **37.0x slower** |
| `spin=10000` | 1 | 0.274s | 0.388s | 1.42x slower |
| `spin=10000` | 2 | 0.274s | 0.268s | **0.98x — break-even** |

The shape is the point:

* For cheap children the IPC dominates absolutely, and `dmap_` is more than an
  order of magnitude worse. One worker is always worse than `map_` — it is
  pure overhead with no parallelism bought.
* The crossover is a property of *per-key work*, not of key count. Distributing
  10,000 trivial children is still a loss.
* Use `map_` unless you have measured this crossover for your own kernel on
  your own hardware. `cost.py --out results.json` writes the raw numbers.

## Running it

```sh
# correctness: every experiment must equal map_
.venv/bin/python prototypes/dmap/experiments/run.py --workers 1,2,3

# prove the work really leaves this process
DMAP_TRACE=1 .venv/bin/python prototypes/dmap/experiments/run.py --only E2 --workers 3

# cost
.venv/bin/python prototypes/dmap/experiments/cost.py --out results.json
```

## The experiments

Each is the same assertion in a different shape — `dmap_ == map_` — because
RFC 0037 makes determinism the contract: distribution changes throughput, never
semantics.

| | what it would catch |
| --- | --- |
| E1 stateless | partition routing, delta round trip, output merge |
| E2 stateful sum | per-key state landing in the wrong worker |
| E3 tick count | a dropped or duplicated dispatch (output is pure tick history) |
| E4 shared input | a broadcast value not reaching every worker holding keys |
| E5 key lifecycle | a child built or torn down in the wrong worker |

Running each at 1, 2 and 3 workers is the determinism criterion: the partition
count must not be observable in the result.

## Findings

Things the prototype established that the RFC could only assume:

1. **The worker really is just a `map_`.** No per-key lifecycle code had to be
   written. E5 passes because `map_` already handles it.
2. **One `push_queue` carrying one message object is required**, not three
   queues for values/removals/shared. Separate queues let the real-time
   executor split one dispatch across engine cycles, so the worker would
   evaluate a half-applied dispatch. A single message keeps it atomic.
3. **The reply sink must be driven by the message as well as the output.**
   Otherwise a dispatch whose keys happen to produce no output never replies
   and the parent blocks forever.
4. **`hash()` cannot be the partitioner.** CPython salts string hashing per
   process, so the parent and a spawned worker disagree about where a key
   lives. `zlib.crc32` is stable; RFC 0037 already requires this and the
   prototype confirms why.

### What this cost us (hgraph behaviours worth knowing)

* **An unwired optional `TS[...]` input silently stops a node from ever
  evaluating.** A node declaring `shared: TS[float] = None` and left unwired
  produced an empty result with no error — the node never fired, because a node
  waits for all its time-series inputs to be valid. The fix is
  `@compute_node(valid=(...))` naming only the inputs that must be valid; the
  prototype instead uses two node variants, which is simpler. Worth an RFC 0037
  note: a real `dmap_` with an optional broadcast input needs `valid=`.
* **`@generator` is pulled with one item of lookahead**, so it cannot drive a
  lockstep worker: the worker would block fetching cycle T+1 before evaluating
  T, while the parent waits for T's reply. This is why the worker is
  `push_queue`-driven and real-time. Measured, not assumed — see the trace in
  the session notes.
* **A `multiprocessing` spawn parent needs `if __name__ == "__main__"`.**
  Without it the child re-executes the parent module and spawns recursively.
* **Errors inside a node's `eval` can surface as an empty result**, not an
  exception. Per-node error capture is doing its job; it just means "no output
  and no error" is a plausible symptom of a crash.

## Known gap — engine time is not yet external

This is the one place the prototype does **not** implement RFC 0037.

The RFC's model is that the parent supplies the cycle's evaluation time, via
the existing `GraphView::evaluate(evaluation_time)`. That is not exposed to
Python, so the worker instead runs in `REAL_TIME` and its engine time is its
own wall clock. One dispatch is still exactly one engine cycle, so the
*sequence* is lockstep — but the *times* differ.

Every v0 kernel is time-independent, so this does not affect any result here.
It does mean the prototype has **not** validated:

* children using `schedule`, alarms, `lag` or any time-based operator;
* `next_scheduled_time` propagation back to the parent — the second half of
  the RFC's per-cycle contract is entirely unexercised;
* anything reading the clock.

Closing it needs either `GraphView::evaluate` exposed to Python, or a native
worker. That is the first thing to do next, and it is why the RFC's protocol
carries `evaluation_time` and `next_scheduled_time` even though this prototype
sends neither.

## Next steps, in order

1. Expose external time control (or build the worker natively) and re-run the
   experiments with a self-scheduling kernel. Until then the RFC's scheduling
   half is unproven.
2. Replace pickle with RFC 0017's codec and re-measure the crossover — the
   36x is partly pickle, and how much is worth knowing.
3. Add the fail-closed wiring rejections (REF, services, push sources). The
   prototype does not reject anything; it simply has no way to express them.
4. Only then consider the asynchronous barrier via the mid-cycle pause cursor.
