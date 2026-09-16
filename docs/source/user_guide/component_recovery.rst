Completed-day component recovery
================================

A deterministic strategy can finish one bounded simulation run, persist a
complete component image, and continue in a new process from that image. Only
new input events are evaluated after recovery. This capability is opt-in and
separate from the existing component ``RECOVER`` mode, which seeds boundary
values and does not restore a running strategy's internal state.

The runtime mechanics are native C++. Durable storage and the Python store
adapter belong to ``hgraph-persistence``. Install matching core and extension
builds; extensions must be rebuilt when the runtime operations ABI changes.

Define the recovery boundary
----------------------------

Use a component with explicit input ports. Keep external data sources, order
submission, and other external effects outside the component. Inside it, use
pure compute nodes and ``RecordableState`` / ``RECORDABLE_STATE`` for values
that affect subsequent events. Configuration is a declaration that these
nodes are deterministic; the runtime cannot inspect arbitrary user code for
undeclared host observations or side effects.

Each external input must come directly from an owned pull-source endpoint,
dedicated to that component input. Its current endpoint image is included in
the boundary and restored before the source starts. This baseline matters for
collections: a future removal must reach a source that already knows the key
exists. The source retains responsibility for its cursor and scheduling;
supply only events belonging to the new interval. Source cursors are not
checkpointed. Computed or projected external inputs and sources shared with
other consumers are refused in this first version. Move preprocessing inside
the component, or supply a dedicated input source.

.. code-block:: python

   import hgraph as hg
   import hgraph_persistence as persistence

   class StrategyState(hg.TimeSeriesSchema):
       total: hg.TS[int]

   @hg.compute_node
   def accumulate(value: hg.TS[int],
                  state: hg.RECORDABLE_STATE[StrategyState] = None) -> hg.TS[int]:
       total = (state.total.value if state.total.valid else 0) + value.value
       state.total.value = total
       return total

   @hg.component(recordable_id="strategy")
   def strategy(value: hg.TS[int]) -> hg.TS[int]:
       return accumulate(value, __recordable_id__="total")

State is already an output-backed endpoint. No separate observer is required
to copy its updates into a second internal object. Checkpoints automatically
include it even when a state update produces no ordinary output.

A start hook sees restored state. Initialize a state field only when it is
invalid; overwriting a restored endpoint in ``start`` is rejected. Python
compute callbacks with captured closure state are refused because those values
are not part of the declared checkpoint contract. Supply immutable scalar
parameters explicitly and semantic mutable state through ``RECORDABLE_STATE``.

Configure each completed day
----------------------------

Create a durable store, select an explicit predecessor, and name the new image
before wiring the application:

.. code-block:: python

   store = persistence.ComponentCheckpointStore("./strategy-checkpoints")
   with hg.GlobalState() as state:
       persistence.configure_component_recovery(
           store, "strategy", "day-2", restore_key="day-1",
           revision="strategy-v1", global_state=state)
       result = hg.run_graph(application, start_time=day_start,
                             end_time=day_end)

``application`` supplies fresh input data to ``strategy``. ``day_start`` and
``day_end`` are explicit datetimes, and the end is exclusive. The first run
omits ``restore_key``. A recovery starts after the saved cut and at or after
the previous completed interval. A missing predecessor is an error, rather
than a silently fresh strategy.

The application ``revision`` identifies the strategy code. Change it when the
meaning of computation changes. Runtime validation additionally checks the
component's node topology, input policies, scalar configuration, endpoint
schemas, and compiled child graph contracts. It does not hash arbitrary
function bodies. The current configuration selects one component per run;
configuring another replaces that selection.

Explicit node IDs must be unique within a component. Automatic structural IDs
are available, but a topology change still requires compatible graph identity.
Nested components and mapped children retain their separate identities.

The native equivalent is:

.. code-block:: cpp

   #include <hgraph/persistence/component_checkpoint_store.h>

   hgraph::GlobalContext context;
   hgraph::persistence::store::FrameStoreConfig storage;
   storage.location = hgraph::persistence::store::LocalLocation{"./checkpoints"};
   hgraph::persistence::ComponentCheckpointStore store{storage};
   hgraph::persistence::configure_component_recovery(
       context.state().view(), store, "strategy", "day-2", "day-1", "strategy-v1");
   // Wire stdlib::component<Strategy>(...) and execute with finite bounds.

Core-only callers can configure ``hgraph::ComponentRecoveryConfig`` with
owned-image load and commit callbacks, for example an in-memory test store.
Use ``testing::eval_node_with_options`` with ``EvalNodeRunOptions`` to test
successive finite intervals through native public graph wiring.

The executable ``extensions/persistence/python/examples/completed_days.py``
runs one calendar day per process. With a new directory, run day 1 and then
day 2. The first produces totals 1 and 3; the second restores 3 and produces
6 and 10 without evaluating day 1 again.

For Python tests of resumed collection removals, supply deltas through a
generator/runtime source. The list-input form of ``eval_node`` validates
collection deltas against a fresh temporary collection before graph recovery,
so a removal of a key that exists only in the checkpoint can fail during test
setup. The mapped recovery regression demonstrates the generator form.

What is preserved
-----------------

The image contains owned component boundary inputs and their direct source
baselines, ordinary outputs, hidden recordable state and error outputs,
validity, and original modification times.
Fixed and dynamic lists, bundles, sets, and dictionaries use representation-owned
checkpoint operations. Unsupported representations refuse checkpointing.

Sets and dictionaries retain their integer slots, capacity, and free-slot
allocation order. Restore does not compact holes or reconstruct membership by
inserting keys in a different order. Deferred erasures are normalized to the
same free-slot order that the uninterrupted graph's next structural mutation
would produce. Thus existing keys retain their slots and the next insertion
chooses the same slot.

Supported keyed maps preserve membership and each live child graph at its
original slot, including child state and the key's modification time. Untouched
keys survive the next day's partial delta. A removed and subsequently re-added
key starts a fresh child rather than resurrecting a retired checkpoint state.
The parent owns mapped output storage; child forwarding outputs are rebound
to it instead of imported a second time.

Import does not publish ticks. The previous day's last value remains readable,
but it is not processed as a new event. User start hooks run after endpoint
import, and mapped child construction completes before fresh evaluation.
Input value/structural activation and passivation are restored after each
node's start hook. This preserves stream operators that stop observing an
input after receiving a value. Activity paths through fixed input bundles,
including Python argument bundles, are supported; per-element activation
inside a peered collection is refused at capture.

Completion and failure
----------------------

After the last successful root evaluation and its deferred notifications,
the runtime captures an owned image before teardown. It publishes that image
only after normal stop succeeds. A failed evaluation, failed stop, failed
encoding, or early stop request does not create a completed-day object.

A checkpoint is a single immutable object. Native local storage stages and
syncs the file, publishes it with a no-replace operation, and attempts to sync
the directory; actual power-loss durability depends on the filesystem. The
store does not publish several independent state streams and infer a completed
day from their presence. Existing checkpoint keys cannot be overwritten.

Before publication, the extension verifies that every stored value can be
decoded without loss. Unsupported payloads, including non-finite floats in
the current scalar codec, fail the commit and leave the predecessor intact.

The caller selects the predecessor and new key explicitly. There is no mutable
latest pointer, graph migration, multi-writer run-head protocol, broker
transaction, or exactly-once external-effect guarantee in this first version.
A process that dies after publication can find the completed key on restart.

Current limits
--------------

Error capture inside a recoverable component is refused: swallowing an
evaluation failure would allow a partial day to appear complete. Exceptions
must propagate to the run boundary.

Ordinary semantic ``State``, scheduler-driven nodes, windows, ``REF`` values,
external sources or sinks inside the boundary, and dynamic owners without
checkpoint operations are refused. The initial supported map form writes child
terminal outputs into owned parent elements; forwarding-map variants, general
``reduce``, and ``mesh`` require further topology/reference contracts.

An image is a full component checkpoint at a completed run boundary. Online
snapshot requests, suspend triggers, incremental physical chunks, and a durable
input journal for replaying the tail after a checkpoint remain later stages of
:doc:`../rfc/rfc_0023_graph_checkpoint_recovery`. Applications can choose daily
or larger completed intervals, but this API does not claim arbitrary running-
graph checkpoint support.
