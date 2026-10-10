RFC 0047: Plain partial-sum cells for lifted reduce combiners
=============================================================

:Status: Proposed
:Created: 2026-10-10
:Target: ``reduce_node.cpp`` (keyed and list reduce over a lifted scalar kernel), reduce checkpoint image

Problem
-------

``reduce(add_, tsd, 0)`` over ``TS[int]`` evaluates a balanced tree of binary
combiners. Since the kernel-combiner change (2026-10-08) each internal node of
that tree is computed by calling the lifted scalar kernel directly instead of
evaluating a nested graph, but every combiner still **owns a nested graph**:
``rebuild_structure`` constructs one per internal position
(``make_nested_graph`` into the combiner bank's graph memory), never starts
or binds it, and uses its lifted node's ``TS`` output as the partial-sum cell
(``CombinerEntry{graph, output}``). Each per-tick combiner evaluation then
opens a mutation scope on that output and runs the erased ``eval_into``
(``checked_as`` twice, ``apply``, ``copy_value_from``, ``mark_modified``).

The 2026-10-10 bake-off profile of main puts ``reduce_evaluate_impl`` at 49%
of ``tsd_dense_reduce_std`` (200 keys ticking every cycle) and about 19% of
the two dense ``tsd_dense_*`` cells; the slot-map change (RFC-less, PR
``perf/reduce-slot-to-leaf``) removed the two key hashes per ticking key and
left the cell work. Per combiner the cell costs a nested graph's memory
(graph header, node storage, ``TSOutput`` with tracking and observer set:
several hundred bytes) for a value of eight bytes.

Contract
--------

For a reduce whose spec resolved a binary ``lifted_kernel``:

* The combiner tree's internal positions hold **plain value cells**: one
  buffer of the kernel's result value type per position
  (``Value`` planned once per node from the kernel's result binding, laid out
  as a contiguous array of ``capacity - 1`` elements), plus a validity bit
  per position. No nested graph is constructed for a lifted combiner.
* ``evaluate_lifted_combiner`` reads its two operands as ``ValueView``\ s —
  a leaf from the source collection element (unchanged), an internal child
  from its cell — calls ``kernel->eval`` (not ``eval_into``) and stores the
  result into its own cell. No mutation scope, no tracking, no observers
  below the root.
* The **root publishes through one storage-held output**: the existing
  ``publication_snapshot`` (``std::optional<TSOutput>`` in
  ``ReduceNodeStorage``) becomes the always-active publication for lifted
  reduces. After the descending evaluation pass the root cell is copied into
  it with the typed native store and ``record_modified`` (the
  ``Out<TS<T>>::set`` commit), and the node's forwarding output targets that
  snapshot exactly as it does today after a keyed root re-point. **One
  identity for every shape:** the single-key and empty cases are copied into
  the same snapshot (the element's or the zero's value on their ticks)
  rather than aliased, so a downstream ``REF`` to a lifted reduce always
  designates the snapshot and never changes identity as the key count moves
  through zero and one. The direct aliasing stays for generic combiners.
* Emission semantics are unchanged: the root ticks on every cycle a live
  leaf ticks, whether or not the sum changed (no test pins this and the
  change is not made here).
* Generic (non-lifted) combiners are untouched: they keep their started
  nested graphs, bound inputs and output handles.

Checkpoint
~~~~~~~~~~

The lifted image currently stores one endpoint image per combiner
(``capture_ts_checkpoint(entry->output)``), validated against the exact
combiner inventory on restore, and ``visit_reduce_checkpoint_endpoints``
assigns every combiner output a stable ordinal because a downstream ``REF``
image may locate its target through one. With cells the image stores the
leaf order (already present) and the publication snapshot's endpoint only;
the cells are derived state and are **recomputed on restore** by a full
evaluation pass over the live combiners, which is deterministic for the
kernels in question. The reduce image format version moves from ``1`` to
``2``. A version ``1`` image of a lifted reduce is **not** loadable into the
cell layout: its combiner-endpoint ordinals have no counterpart (a ``REF``
locator recorded against a combiner output could not be fixed up), so
restore rejects it with a message naming the node and the image version.
Generic-combiner images are unaffected. Since the identity change above
also moves the published endpoint of singleton lifted reduces, this is a
checkpoint-incompatible change for lifted reduces and must be released as
one; existing checkpoints of graphs containing them need re-recording.

Invertible kernels (stage 2, optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``LiftedKernel`` is built at one site with designated initialisers, so a
defaulted ``inverse`` function pointer (and the existing
``associative``/``commutative`` flags) is source compatible. For kernels
whose inverse is exact — integer ``add``/``sub`` with wrapping arithmetic
(the kernel's own comment already describes the add as wrapping; the cast
must be made explicit) and ``bit_xor`` — the root can be maintained as an
accumulator: ``root = root ⊕ new ⊖ old`` per modified leaf, with a per-leaf
previous-value buffer (element size per leaf, needed for removals too). Not
applicable to floats (rounding), integer ``mul`` (zero), ``min``/``max``,
``and``/``or``. This stage pays off on sparse ticks (k of n keys), where the
tree already costs only ~8k combiner evaluations and hgraph leads csp
twenty-fold; it is the smaller win and is deferred until stage 1 is measured.

Design notes
------------

* The cell buffer lives in ``ReduceNodeStorage`` next to ``combiners``; the
  bank machinery (two ``InPlaceGraphSlotStore`` generations for capacity
  growth) is only needed for generic combiners, so for lifted reduces the
  banks stay empty and capacity growth reallocates the cell buffer (copying
  partials is unnecessary: a full structural rebuild recomputes them).
* ``aggregate_output`` returns a ``TSOutputView``; the lifted evaluation path
  gets an ``aggregate_value`` sibling returning a ``ValueView`` for cells and
  the element's value for leaves, so the generic path is unchanged.
* ``live_reduce_schedule`` already answers ``MAX_DT`` for lifted reduces
  (no started child graphs), and ``visit_reduce_checkpoint_endpoints`` skips
  combiner endpoints for them; both stay as they are.
* Expected effect: the 199 ``begin_mutation`` / ``copy_value_from`` /
  ``mark_modified`` triples per cycle of the dense cell become 199 typed
  adds and stores; memory per lifted reduce drops from one nested graph per
  combiner to one element per combiner.

Acceptance
----------

``tests/cpp/test_reduce.cpp`` (tree carry, signed-int tree, identity does not
supply the zero, zero cases, growth, dynamic TSL), the Python reduce suites
(``test_reduce_zero_semantics.py``, ``ported/_wiring/test_reduce.py``,
``test_benchmark_scenarios.py`` tick counts) and
``tests/cpp/test_reduce_checkpoint.cpp`` (every cut equals the uninterrupted
run, including a version-1 image restored into the cell layout) must pass
unchanged except for the new image version; a bake-off A/B on
``tsd_dense_reduce_std``, ``tsd_sparse_std`` and the two ``tsd_dense_*``
cells on both hosts.
