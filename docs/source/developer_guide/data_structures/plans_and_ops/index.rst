Allocation, Plans and Ops
=========================

Where the *Schemas* chapter answers *what is this thing?*, this chapter
answers *how is it laid out, and how does it behave?*. Three concepts
sit side-by-side here, deliberately separated:

``Allocation``
    Acquiring and releasing raw memory of a given size and alignment.
    Allocation produces uninitialised storage and is **independent of
    construction and destruction**: an allocator hands back addresses;
    it does not call constructors. Symmetrically, deallocation reclaims
    storage but does not destroy any object that may still live in it.

``Plan``
    The memory layout for one data structure. A plan carries the
    information an allocator needs to size and align a region: total
    size, alignment, and field offsets for composites. It also carries
    a ``LifecycleOps`` table for the construction, copy / move, and
    destruction operations that bring an instance to life in
    already-allocated memory. A plan does **not** allocate or
    deallocate memory and holds no reference to any allocator; an
    allocator consumes the plan's size and alignment to acquire
    storage, and a separate caller (typically the owning Value) then
    invokes the lifecycle hooks on that storage. Plans are immutable
    and interned; consumers hold borrowed pointers to them.

``Ops``
    A struct of function pointers that defines behaviour over a value's
    memory. The first parameter of every op is always a pointer to the
    memory representing the structure; remaining parameters follow as
    needed. An ops table is the type-erasure vehicle for one
    implementation of a kind.

Separating allocation from construction is what lets the runtime mix
strategies — heap, arena, pool, inline storage — without rewriting the
construction logic, and lets the same plan be used unchanged across
all of them. The plan describes the layout; the allocator consumes
the plan's size and alignment to acquire memory; the lifecycle ops
then construct into that memory. Tearing down is symmetric: the
plan's ``destroy`` runs first, then the allocator (which the plan
knows nothing about) releases the buffer.

This chapter is organised as:

- *Erased Types* — the type-erased data structures (atomic value, list,
  set, map, …) and the ops vtables that expose their behaviour. This
  surface is shared by both scalar and time-series implementations.
- *Scalar Plans and Ops* — concrete layout strategies for scalar values
  that do not change over time.
- *Time-Series Plans and Ops* — runtime layout strategies for time-
  series, including the slot store family used by TSS and TSD.

Objectives
----------

- Make ownership explicit.
- Avoid Python-owned lifetime as a runtime requirement.
- Keep time-series storage cache-friendly.
- Support deterministic teardown.
- Make graph-level allocation strategies visible and testable.

Ownership Model
---------------

The runtime distinguishes:

- long-lived graph topology,
- per-run runtime state,
- node-owned resources,
- time-series value storage,
- transient evaluation state.

Graph topology is immutable after construction. Runtime state can then
refer to topology through stable identifiers or borrowed pointers
without defensive synchronisation for wiring changes.

Time-series values are stored in structures that make validity,
modification state, and value payload access cheap during evaluation.
Collection-shaped time-series values need clear delta ownership rules
to avoid accidental copies.

Allocation strategies (single arena per graph, pooled per shape, plain
heap) are not fixed at this layer. The plan/ops separation is what
keeps them swappable; the choice is taken at graph-construction time
and is described in *Refinement Topics*.

.. toctree::
   :maxdepth: 2

   erased_types
   scalar
   time_series
   proxy_state_management
