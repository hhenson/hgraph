RFC 0021: Recording Versions and Continued Recording
====================================================

:Status: Draft — not ready for review
:Author: Howard Henson
:Created: 2026-08-13
:Target: Record/replay frame store and the ``record`` / ``replay`` operators

.. note::

   **This is a placeholder capturing the shape of the problem, not a proposal
   to implement.** It exists so the decision recorded in
   :doc:`rfc_0019_native_table_recording` — that a native write to an existing
   key fails — has a named successor rather than an implied one. Nothing here
   is settled, and the open questions at the end outnumber the answers.

   **Scope revised by** :doc:`rfc_0025_hgraph_persistence` (2026-08-17):
   recording versions, continued recording, and their reconciliation with
   run/checkpoint lineage are ``hgraph-persistence`` scope — any eventual
   proposal here lands in the extension, not in core.

Summary
-------

RFC 0019 made a recording key identify exactly one run: a second native write
to the same key fails rather than replacing what is there. That is the right
default — a silent replace destroys a completed recording with no signal, and
cannot be told apart from the append that model deliberately does not support.

It leaves two real workflows without an answer:

* **Re-recording.** Re-running a graph under the same ``GlobalState`` now
  fails, and the remedy is to delete the old recording first. That is correct
  but blunt: the previous run is discarded to make room, so a comparison
  against it is no longer possible.
* **Continued recording.** Resuming a recording — replaying what exists and
  carrying on writing from where it stopped — has no expression at all. RFC
  0019 explicitly puts cross-run append out of scope, and says that if it is
  wanted later it must be *a deliberate change, not an accident of the flush
  policy*. This is that deliberate change.

Both point at the same missing concept: a key should identify a *series of
runs*, not a single one.

Motivation
----------

The delete-first remedy is fine for a scratch recording and wrong for anything
retained. Concretely:

* A backtest re-run overwrites the artefact you wanted to compare against.
* A long real-time capture that dies has no way to resume; the only options are
  to discard it or to write to a second key and reassemble by hand downstream.
* Provenance is lost. "Which run produced this?" is not answerable from the
  store, because the key remembers only whichever run happened to be last.

Sketch
------

A key resolves to an **ordered set of versions**. A run writes a new version
rather than colliding with an existing one; replay selects a version, defaulting
to the latest complete one.

This composes with the segment layout RFC 0019 already defines, which is why it
is worth recording now rather than inventing a parallel scheme later. That
design already has a marker object at the logical key, ``<key>.N`` segments and
a ``<key>.complete`` manifest — a version is the natural enclosing scope for
exactly that structure, and the manifest is where a version's row count, time
bounds and completion state already live.

Continued recording then has a shape: a new version declares the version it
continues, replay of that version reads its ancestor chain in order, and the
"replay then keep recording" workflow becomes *open the previous version, start
a new one that continues it*. No object is ever mutated, which keeps RFC 0016's
immutable-by-default store intact.

Open questions
--------------

These are the reasons this is a draft rather than a proposal:

* **Naming and discovery.** Versions need an ordering and a discovery rule, and
  ``FrameStoreOps`` has no enumeration — the same constraint that shaped
  segment discovery. Monotonic probing works for segments because a run writes
  them in order within one process; versions are written by different runs at
  different times, so the argument does not transfer unexamined.
* **What "latest" means.** Wall-clock order, logical time order and write order
  can disagree, particularly for a version that continues an older one.
* **Whether a continued version replays as one series or as a chain.** Reading
  an ancestor chain transparently is convenient and makes a version's content
  depend on objects it does not contain — which is exactly the property that
  made cross-run append unattractive in RFC 0019.
* **Interaction with the fixed-schema rule.** A key currently identifies one
  immutable schema. If two versions may differ in shape, schema evolution
  re-enters scope through the back door, and RFC 0019 removed it deliberately.
* **Retention.** Versions accumulate. Something has to say when one may be
  removed, and neither the store nor the recorder has a policy today.
* **Whether this belongs in the store or above it.** A version may be a
  first-class store concept, or purely a key-naming convention owned by
  record/replay. The former is more useful and a much larger commitment.

Relationship to other RFCs
--------------------------

* :doc:`rfc_0016_object_store_frame_persistence` — the immutable store this
  must not compromise.
* :doc:`rfc_0019_native_table_recording` — the source of the one-run rule, the
  duplicate-key rejection, and the segment layout a version would enclose.
* :doc:`rfc_0022_serializable_graph_manifest` — stable graph, binding, and run
  identity for deciding which recording or checkpoint can attach.
* :doc:`rfc_0023_graph_checkpoint_recovery` — proposes immutable checkpoint
  manifests, parent links, published run heads, and retention reachability.
  A future revision of this draft should use that lineage rather than invent a
  second version catalogue.
