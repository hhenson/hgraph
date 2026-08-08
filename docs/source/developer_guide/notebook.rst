Notebook & Interactive Sessions
===============================

Status: design approved 2026-07-20 (clean-design ruling: the upstream
``hgraph.notebook`` API is a reference point, **not** a compatibility target —
there is no installed base to preserve). First implementation lands with this
page.

Goal
----

Support writing and evaluating graphs **incrementally** — cell by cell in a
Jupyter notebook, or statement by statement in a REPL:

.. code-block:: python

   from hgraph.notebook import session
   nb = session()                 # cell 1: open an ambient wiring session

   c = const(42)                  # cell 2: ordinary public wiring API
   c.eval()                       # -> [(engine_time, 42)]

   d = c + 1                      # cell 3: keep growing the SAME graph
   d.eval()                       # re-evaluates the graph as it now stands

   nb.reset()                     # start over

Two runtime capabilities make this work; both are general-purpose, not
notebook-specific:

1. **Ambient wiring session.** The bridge's wiring state is a single
   module-level stack (``_wiring/_core._wiring_stack`` — the no-thread-locals
   ruling). A session pushes one persistent ``Wiring`` and leaves it on the
   stack, so every public operator call in subsequent cells wires into it.
   This replaces upstream's hand-seeding of ``WiringGraphContext.__stack__``
   with the sanctioned single entry point.

2. **Snapshot-run** (the C++ change). ``Wiring::finish() &&`` is
   rvalue-consuming: it moves the wiring into the ``GraphBuilder``, so a
   wiring can run once. Interactive use needs *evaluate the graph so far,
   keep wiring*. The audit of ``finish()`` shows it is almost non-destructive
   already — ``build_ranked_graph`` reads ``impl_->instances`` by const
   reference, traits are read in place, and the only true move is the
   ``GlobalState`` hand-off (whose contract is copy-in/copy-out anyway).

Snapshot-run contract
---------------------

``GraphBuilder Wiring::snapshot() const`` — build a runnable graph from the
wiring **as it currently stands**, leaving the wiring open for further
wiring. Semantics:

- Same validation as ``finish()`` (no unterminated implementation scopes,
  all client service paths implemented).
- ``build_ranked_graph`` over the current instances; the wiring's
  ``GlobalState`` is **copied** onto the builder (not moved) — each snapshot
  run starts from the wiring-time entries as seeded.
- Service rank dependencies are applied idempotently:
  ``add_rank_dependency`` de-duplicates, so repeated snapshots do not
  accumulate duplicate rank edges.
- ``finish() &&`` keeps its exact contract (single-shot, moves the state);
  ``snapshot() const`` is the additive sibling. Nothing about the
  build-once path changes.

The bridge exposes it as ``Wiring.run(..., snapshot=True)``: build from the
live wiring and run, without marking the wiring finished. The default
(``snapshot=False``) is unchanged.

The ``hgraph.notebook`` module
------------------------------

A small Python-only layer over the two capabilities. Clean-design decisions
(deliberate departures from upstream):

- **A session object, not module globals.** ``session(start_time=…,
  end_time=…)`` returns a ``NotebookSession`` that owns the persistent
  ``Wiring``; ``reset()`` tears down and reopens deterministically
  (upstream re-seeded context stacks by hand and leaked the old state).
  Re-invoking ``session()`` while one is open resets it.
- **Port sugar installed once, at module import.** Importing
  ``hgraph.notebook`` adds ``eval()`` / ``plot()`` to the bridge
  ``WiringPort`` (we own the class — this is a documented extension point,
  not runtime monkey-patching per session). The methods raise a helpful
  error when no session is active.
- **One record sink per port, reused.** ``port.eval()`` wires a ``record``
  node keyed per port and **reuses it** on subsequent evals. (Upstream
  appended a fresh ``Eval_n`` record sink on every call — every prior key
  re-recorded on every later eval, growing without bound.)
- **Sparse recording.** The session seeds a ``GlobalState`` so ``record``
  selects the sparse absolute-time recorder (the ``IN_MEMORY`` model) —
  gap-tolerant and returning true engine times, which is what interactive
  inspection and plotting want. Results come back as
  ``[(engine_time, value)]``.
- **Reflection-based display.** Type dispatch (scalar series vs bundle) uses
  ``hgraph.reflection`` (``is_ts`` / ``is_bundle`` / ``fields``) — this
  module is the anticipated consumer of the ``Hg*TypeMetaData`` migration
  layer. Plots use ``matplotlib`` step rendering (truthful for step-function
  time-series semantics); matplotlib is imported lazily and is not a
  dependency of the package.
- **Simulation mode only** for now, matching the runtime's testing posture.

Deferred (recorded, not designed here):

- ``port.to_frame()`` — evaluate and return an Arrow table (the TABLE
  protocol), for polars/pandas display and analysis. Natural next step;
  needs the table-schema wiring decisions.
- Rich ``_repr_html_`` displays beyond the basic result table.
- **Real-time sessions** — a live background graph fed by push-source cells.
  Push senders are the sanctioned cross-thread entry point, so this is
  feasible on the current executor; it is a separate design when wanted.

Non-goals
---------

- Faithful reproduction of upstream ``start_wiring_graph`` /
  ``notebook_evaluate_graph`` names or behaviour (explicitly ruled out).
- Any parallel wiring representation on the Python side (a cell journal that
  re-wires per eval was considered and rejected — snapshot-run keeps the C++
  wiring the single source of truth).
