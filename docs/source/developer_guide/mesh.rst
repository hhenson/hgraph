Mesh
====

This page is the **authoritative design record** for ``mesh_``. It is written to
be complete enough to implement from: a normative **Expected behaviour** section
(what a correct mesh does, observably and testably) followed by an
**Implementation** section (how it is built on this runtime). No mesh code should
land that contradicts this page; change the page in the same change as the code.

``mesh_`` is a ``map_`` over a ``TSD`` whose per-key instances may read each
other's outputs by key, create instances on demand when an absent key is
referenced (so recursion works), and are evaluated in dependency order each
cycle. It mirrors the Python reference
(``ext/main/hgraph/_wiring/_mesh.py``, ``_impl/_runtime/_mesh_node.py``,
``nodes/_mesh_util.py``) and the bundled C++ reference
(``ext/main/cpp/.../mesh_node``); the deliberate adaptations to this runtime are
called out in the Implementation section.


Expected behaviour
------------------

Construction and output
~~~~~~~~~~~~~~~~~~~~~~~~~

``mesh_(func, *args, **kwargs)`` is the ``map_`` call surface restricted to the
``TSD`` form (no ``TSL`` mesh — rejected at wiring, as in Python). ``func`` is a
graph/operator whose output type ``OUT`` is the per-key result. The mesh's
output is an owned ``TSD<K, OUT>`` keyed by ``K``. The standard return is that
``TSD`` indexed by the key set (Python: ``port.out[__keys__]``).

Instances from keys
~~~~~~~~~~~~~~~~~~~~

The *requested key set* is the union of the multiplexed ``TSD`` key sets and any
explicit ``__keys__`` ``TSS[K]`` (exactly ``map_``'s key derivation). Each key in
that set has one instance of ``func``. With no cross-instance dependencies a mesh
is **observably identical to** ``map_``: each instance computes from its own
per-key inputs and writes ``output[key]``.

Cross-instance access and on-demand creation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inside ``func`` (or any graph wired within the mesh's scope),
``mesh_(func)[k]`` or ``get_mesh(func)[k]`` in Python
(``mesh_ref<OUT>(w, k)`` in C++) reads the result of the instance for key ``k``:

- if an instance for ``k`` already exists, the reference reads its output;
- if not, the instance for ``k`` is **created on demand** and the requesting
  computation reads its result;
- the requester thereby **depends on** ``k``: ``k`` must produce its result
  before the requester's result is valid.

``k`` is an ordinary ``TS[K]`` value computed at runtime (dynamic keys); a single
requester may reference different keys over time.

An instance may also inspect the enclosing mesh's live output key set through
the Python ``MeshWiringPort`` returned by ``mesh_(func)`` / ``get_mesh(func)``
or with ``mesh_keys_ref<K>(w[, name])`` in C++. This is a forwarding reference
to the mesh ``TSD`` output's ``key_set()`` projection, not a copied set value,
so ordinary operators such as ``contains_`` and ``len_`` observe the mesh key
set directly.

Same-cycle settlement
~~~~~~~~~~~~~~~~~~~~~~~

When a key (and the transitive closure of the instances it depends on) is
introduced in a cycle, the mesh produces the **fully settled** result for that
key **in the same cycle**, provided every required external input is already
available. Concretely: introducing ``fib(7)`` in one cycle yields ``{7: 13}``
that cycle, having created and evaluated ``6, 5, … , 1, 0`` in dependency order
within the cycle. (This is stronger than the Python reference, which settles a
new dependency depth per engine cycle; the tests assert the settled state and so
pass under either, but same-cycle settlement is the behaviour this runtime
targets — see *The mesh evaluation engine* below.)

.. mermaid::

   flowchart LR
      req["requested key: fib(7)"] --> f7["fib(7) instance"]
      f7 -->|"mesh_ref[6]"| f6["fib(6) — created on demand"]
      f7 -->|"mesh_ref[5]"| f5["fib(5)"]
      f6 --> f5
      f6 -->|"…"| f1["fib(1), fib(0)"]
      f5 -->|"…"| f1
      f1 -->|"evaluate leaf-first in dependency rank order,<br/>re-entering via the graph's pause/resume cursor"| out["settled output {7: 13}<br/>same engine cycle"]

Removal and reference counting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An instance is kept alive while it is **either** in the requested key set **or**
depended upon by at least one live instance. It is destroyed
(graph stopped, output element removed) only when **both** are false. A key
removed from the key set whose only remaining users are internal dependents
stays alive; once those dependents drop it, it is removed.

Cycle detection
~~~~~~~~~~~~~~~~

A dependency cycle among instances (``A`` depends on ``B`` … depends on ``A``) is
a **runtime error** ("mesh has a dependency cycle"), detected in the cycle the
closing edge is introduced.

Scoping / visibility
~~~~~~~~~~~~~~~~~~~~~~

A mesh is visible to **the graph it is wired in and that graph's sub-graphs, and
to nothing else**. A mesh reference outside that scope resolves to no mesh (an
error). This matches the reference (a wiring-time context stack pushed while the
mesh and its sub-graphs are wired and popped afterwards).
A consumer in the same graph as the mesh references it directly; a deeper
consumer references it through the scoped context.

Nested meshes may be named with Python's ``__name__`` argument or C++
``arg<"__name__">(Str{"name"})``. Python resolves them with
``mesh_("name")`` / ``get_mesh("name")``; C++ ``mesh_ref`` and
``mesh_keys_ref`` take the same optional name. Both resolve the innermost
matching mesh scope, which disambiguates nested meshes without changing runtime
graph structure.

The two request kinds
~~~~~~~~~~~~~~~~~~~~~~~

1. **Peer instantiation.** ``mesh_(func, keys/inputs…)`` with no cross-instance
   references — equals ``map_``.
2. **Internal request** — a ``mesh_(func)[k]`` inside an instance. Establishes a
   runtime dependency (requester instance → key ``k``) and may create ``k`` on
   demand.

A ``mesh_(func)[k]`` in the *enclosing* graph that requests a key not already in
the key set — an "external request" / dynamic external access — is **not
supported** (see *Deferrals*): with no inter-instance dependency it reduces to
``map_`` plus a key union, and the Python reference does not support it either.
Code in the enclosing graph reads the mesh's owned ``TSD`` output as an ordinary
consumer (keys are seeded only through ``__keys__`` / the multiplexed inputs).

Worked examples (the conformance tests)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These mirror ``ext/main`` ``test_mesh.py`` and define conformance:

- **Recursion (``fib``).** ``mesh_(fib, __keys__=i)`` where ``fib(n)`` returns
  ``mesh_(fib)[n-1] + mesh_(fib)[n-2]`` (with ``0/1`` base cases).
  ``[{7},{8},{9}]`` settles to ``{7:13, 8:21, 9:34}``.
- **Expression mesh.** Instances reference each other by name through a shared
  ``vars`` (``pass_through``) and the keys input; results propagate in dependency
  order, and a changed input re-propagates.
- **``contains_`` over the mesh.** An instance inspects the mesh's own key set.
- **Cycle.** A self-referential dependency raises "dependency cycle".
- **Removal.** Removing the only requested key tears the (now unreferenced)
  instances down.
- **Named / object keys.** ``mesh_("name")`` resolution and compound-scalar keys.


Implementation
--------------

Two ranking layers
~~~~~~~~~~~~~~~~~~~

Mesh ranking is **two independent problems**; conflating them is the trap.

- **Wiring-time (static):** scope, and where the mesh node and the nodes that
  consume it sit in the **outer** graph's evaluation order. Fixed at compile
  time.
- **Runtime (dynamic):** the order the mesh evaluates its **instances** within a
  cycle, from dependencies discovered as it runs.

Peer instantiation is entirely the static problem; only internal requests need
the dynamic engine.

Wiring layer
~~~~~~~~~~~~

- **Reuse of ``map_``.** Child compilation (``compile_subgraph``), per-key child
  input binding (``MapArgSource``: key / element / broadcast), and the child
  terminal **forwarding** output into the owned ``TSD`` element are taken
  unchanged from ``map_`` (see :doc:`nested_graphs`). Mesh adds a dependency
  engine and a scoped ``mesh_subscribe`` node that forwards sibling outputs.
- **Mesh-context scope.** A wiring-time stack of ``(element type, optional name)``
  entries on the **global wiring singleton** (``OperatorRegistry`` —
  ``push_mesh_scope`` / ``pop_mesh_scope`` / ``resolve_mesh_scope``), *not* on a
  ``Wiring`` instance: the mesh child compiles in a fresh ``Wiring``, so the scope must
  be reachable across instances. (The build is single-threaded, so this is a plain
  global, never a thread-local.) ``mesh_ref`` resolves the nearest enclosing
  entry; out-of-scope lookups error. Pushed before compiling the mesh child and
  popped after, so an outer/sibling graph cannot see an inner mesh.
- **Request classification.** A ``mesh_(...)[k]`` *inside* the child being
  compiled is an **internal** request → wires a ``mesh_subscribe`` node and is
  marked rank-sensitive (known at compile time). A ``mesh_(...)[k]`` in the
  enclosing scope is not a request at all — it is an ordinary read of the mesh's
  ``TSD`` output (external key injection is unsupported; see *Deferrals*). Keys
  are seeded solely from ``__keys__`` / the multiplexed inputs (``map_``'s
  derivation), so the ``map_`` equivalence of case 1 is structural.
- **Static rank of the mesh node.** The mesh output must be available to its
  consumers: the context publish/capture sits one rank above the mesh node and
  context clients (consumers reading the output through the scope) rank below it
  (the reference's ``max_context_rank`` + ``register_context_client``).

Runtime layer
~~~~~~~~~~~~~

**Stable instance store.** Instances live in a ``Value``-keyed, pointer-stable
store (the set of instances is a *superset* of ``__keys__`` — it also holds
on-demand instances). Each entry owns: the key ``Value``, a read-only ``TS<K>``
key source output over that owned key (the per-instance key, always present),
the child ``GraphValue``, and the instance's place in the dependency graph.
Stable addressing means create/remove never renumbers anything.
Declaration/destruction order: the child graph (a subscriber to the key source
and to the mesh self-output) tears down before those outputs, and the whole
instance store tears down before the owned ``TSD`` output (children forward
into it).

**Dependency graph.** A lightweight bidirectional graph over instances — edges
*dependent → dependency* and the reverse — accumulated from ``mesh_subscribe``
registrations. It records **only** internal (instance ↔ instance) edges and
orders **only** the call sequence of instance evaluation; each instance's
internal nodes keep their own scheduler.

**The mesh evaluation engine (pause / resume) — the core.** Because the
evaluation engine folds into ops in this runtime, the **mesh node is the
evaluation engine for its instances**: on its own evaluate it drives instance
evaluations in dependency order, and an instance's evaluation is **resumable**.
This rests on the generic **pause/resume cursor protocol** in the execution
layer (see :doc:`data_structures/overview/execution_layer` — node/graph eval
returns a ``bool``, the graph holds a node-id cursor, nested handlers relay a
pause, and the mesh node is the pause *boundary* that resolves it). The model:

- Instance evaluation proceeds node-by-node (the instance's own due/rank order).
  When a ``mesh_subscribe`` node finds its dependency **not yet satisfied this
  cycle** (the dependency instance does not exist, or exists but has not yet
  produced its result this cycle), it asks the mesh to satisfy it and the
  instance's evaluation **pauses** (yields) at that node, saving a resume cursor.
- The pause is a **coroutine yield**, not a restart. Every node evaluated
  **before** the yield is **committed for the cycle**: up to the yield rank order
  was certain, so — like any node in a normal cycle — it has had its at-most-once
  evaluation and is **not rescheduled while paused**, even if the dependency
  resolution later changes one of its inputs (that change is picked up next
  cycle, exactly as feedback is). Only the not-yet-evaluated (downstream) portion
  remains to run.
- Pausing does **not** freeze the engine. While the instance is parked the mesh
  evaluates the dependency, and any **downstream node of the paused graph**
  (anything past the yield) is scheduled for evaluation **as per normal** as its
  inputs become ready. There is no due-set snapshot to replay and no special
  re-derivation: ordinary scheduling continues; the committed prefix is simply
  off-limits for this cycle.
- The mesh, on the pause: creates the dependency on demand if absent, records the
  dependency edge, and ensures the dependency instance is **evaluated first**
  (placed ahead of the parked instance in the work order; it may itself yield for
  *its* dependencies — handled the same way).
- When the dependency (and its transitive closure) has produced its result, the
  mesh **resumes** the parked instance from its cursor — re-entering at the
  yielding ``mesh_subscribe`` node, which now reads the available result and
  produces its output, after which the downstream portion runs. The committed
  prefix is never re-entered. A dependency whose result was already available
  causes no pause. "Already available" is precise: the dependency settled
  *this* cycle, **or** it exists, is not itself paused, and has nothing
  scheduled at or before the current time — a **quiescent** instance's current
  output *is* its settled result and is read directly. Pausing on a quiescent
  dependency would deadlock: the resolver only evaluates due-or-paused
  instances, so the requester would re-pause on every pass until the settle
  guard threw (regression 2026-07-15, exposed by the comparative bench's churn
  scenario — a dependent re-registering against an instance that has no work
  this cycle). Rank order keeps the quiescence test sound: a due dependency is
  ranked below its requester and therefore ran earlier in the same pass.
- The orchestration is an **iterative worklist with a waiting stack** (not deep
  recursion): a parked instance is set aside, its dependency is run, then it is
  resumed. This bounds the C++ call stack regardless of dependency depth — the
  reason pause/resume is preferred over the inline re-entrant alternative.
- **Cycle detection:** if satisfying a dependency requires evaluating an instance
  already on the waiting stack, that is a dependency cycle → error, in the same
  cycle.

This requires a **resumable instance-evaluation entry point**: the instance
graph is evaluated by a mesh-owned evaluator that runs due nodes in order,
checks a yield request after each, and on a yield records the resume cursor (the
yielding node) and returns a *paused* status; on resume it re-enters at the
cursor and continues — the committed prefix (nodes strictly before the cursor) is
never re-run, while the downstream portion evaluates under ordinary scheduling.
Under the type-erased design this is a mesh-specific graph-evaluation op, not a
change to the standard executor.

**Stable state.** Once edges are known they give a stable dependency order, so in
a settled mesh no pauses occur: instances selected by input notification,
creation, or internal schedule are evaluated dependency-first and each
``mesh_subscribe`` finds its result already present. The mesh does not rebuild
and sort the complete live-instance order for a sparse tick. Its child schedule
observer and coalesced pull queue identify independently due instances; newly
created dependencies and sibling notifications join the same-cycle candidate
worklist. Source repoints retain a conservative all-instance rebind. Pauses arise
only when a **new** dependency (or a changed dependency key) is discovered.

**The ``mesh_subscribe`` node.** Wired inside an instance for ``mesh_(func)[k]``.
Inputs: ``item`` (``TS<K>``, active — the dependency key) and a dynamic ``value``
input seeded by ``nothing<OUT>``. It reaches the mesh node via the
``NestedGraphView::parent_node()`` walk (skipping intermediate nested graphs),
reads ``my_key`` from the mesh's current evaluation context, and on evaluation:
registers the dependency ``(my_key -> item)``; if unsatisfied, requests a pause;
once satisfied, binds ``value`` to the sibling element and publishes a
forwarding output to that same element. The forwarding output is what consumers
read; ``value`` keeps the subscription node reactive to sibling ticks. No
REF-input dereferencing and no data-object back-pointer.

**Lifecycle.** Create: build child, bind the key source, instantiate
``output[key]``, bind inputs + the forwarding output, start. Remove (refcount per
*Removal* above): stop child, erase element, drop edges, destroy entry. On node
stop: stop all instances, clear the output and dependency state.

Implementation status
~~~~~~~~~~~~~~~~~~~~~~~

The pause/resume engine is **implemented** (``runtime/mesh_node.{h,cpp}`` +
``lib/std/operators/impl/higher_order_impl.h``), replacing the original first-cut
multi-cycle-settle ordering:

- **Pause/resume substrate** (execution layer): node and graph ``evaluate`` return
  ``bool``; the graph holds a node-id **cursor** (``evaluation_cursor``) to resume
  mid-cycle (see :doc:`data_structures/overview/execution_layer`). The mesh node is
  the pause **boundary**.
- **``mesh_subscribe``** is a custom-ops bool node with inputs ``{item, value}``:
  ``item`` is the requested key (wired); ``value`` starts at a never-ticking
  ``nothing<OUT>`` placeholder and is rebound at runtime to the sibling element.
  It resolves the enclosing mesh via the ``parent_node()`` walk, reads ``my_key``
  from the mesh's ``current_eval_key``, registers the dependency, and **pauses**
  (returns ``false``) until the target has settled this cycle, then publishes a
  forwarding output bound to ``self[item]``. The ``value`` rebind keeps the node
  reactive: a later tick of the sibling reschedules it (cross-cycle
  re-propagation).
- **Resolver** (``mesh_evaluate``): a rank-ordered **sparse settle loop** —
  materialize only input-notified, newly created, paused, or internally due
  instances by ascending rank, then repeat while same-cycle dependency work is
  added. ``add_dependency`` creates the target on demand (same cycle, ranked
  below the requester) and ``re_rank`` keeps the requester above it; a cycle is
  a runtime error. The schedule queue's future minimum re-arms the mesh parent
  once after the settle loop.
- **Wiring**: the ``mesh_`` operator (``wire_mesh``, peer instantiation = ``map_``),
  the wiring-time **mesh-context stack** (pushed around the child compile), and
  ``mesh_(func)[k]`` access via ``mesh_ref<OUT>(w, key)`` (resolves the scope, wires a
  ``mesh_subscribe``). ``mesh_keys_ref<K>(w, name)`` resolves the same scope and
  wires a forwarding key-set view. ``WiredFn::output_schema()`` supplies the element
  type up front so the body's ``mesh_(func)[k]`` has a type without a
  self-referential compile.

The ``Value``-keyed stable instance store, refcount removal, and the dependency graph
carry over from the first cut. **Validated** end-to-end by ``tests/cpp/test_mesh.cpp``
(ASAN/UBSAN-clean): peer instantiation = ``map_`` (plain + key-consuming);
``switch_`` inside a mesh instance; a cross-instance dependency **chain** with
on-demand base creation settling in one cycle; **re-propagation** of a changed
input through the dependency graph (reactivity); a **``map_`` nested inside a
mesh instance** whose child pauses on ``mesh_(F)[peer]``; dependency retargeting,
invalid requests, removal of orphaned on-demand instances, named mesh key-set
access through ``contains_``; and a dependency **cycle** raising a runtime error.

**Remaining** (see *Deferrals*): external dynamic requests, ``TSL`` meshes, and
per-instance error capture.


Alternatives considered
-----------------------

- **Multi-cycle settle (Python reference).** Integer ranks; an unsatisfied
  dependency reschedules the dependent for the next cycle and re-ranks. Simplest,
  but a new dependency depth costs an engine cycle to settle. Rejected as the
  target behaviour (kept only as the current first-cut code, to be replaced).
- **Inline re-entrant evaluation.** The subscribe synchronously drives its
  dependency's evaluation mid-node. Same-cycle, but the C++ call stack grows with
  dependency depth (overflow risk on deep meshes). Rejected in favour of the
  iterative pause/resume worklist.
- **Re-run whole instance on re-rank.** Apply the re-rank within the cycle and
  re-evaluate the dependent instance from scratch. No continuation state, but
  O(depth²) re-evaluation. Rejected; pause/resume re-runs only the yielding node.


Files
-----

- ``include/hgraph/runtime/mesh_node.{h}`` / ``src/…/mesh_node.cpp`` — the mesh
  runtime node, the dependency graph, the pause/resume engine,
  ``MeshNodeView::add_dependency``/``remove_dependency``, the ``mesh_subscribe``
  node, and the forwarding ``mesh_key_set`` node.
- ``include/hgraph/types/subgraph_wiring.h`` (or a dedicated mesh wiring header)
  — the mesh-context stack, request classification, the ``mesh_<G>`` entry and
  ``mesh_(...)[k]`` access.
- Tests: ``tests/cpp/test_mesh.cpp`` (the worked examples above).


Deferrals
---------

- **External requests / dynamic external mesh access** — a ``mesh_(func)[k]`` in
  the enclosing graph that injects a key not already in the key set. Reviewed and
  dropped: with no inter-instance dependency it is ``map_`` plus a key union, and
  the Python reference does not support it. The enclosing graph reads the mesh
  output as an ordinary ``TSD`` consumer; keys are seeded only via ``__keys__`` /
  the multiplexed inputs.
- ``TSL`` meshes (Python rejects them too).
- Per-instance error capture (``TSD<K, TS<NodeError>>``).
- The intra-instance "bipartite split" (re-ranking only the result-consuming
  part of an instance) is **subsumed** by pause/resume at the node boundary; no
  separate static split is planned.
