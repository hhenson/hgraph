HGL any values
==============

ANY-1–5 in ``external/hgraph_spec/language/docs/design/any-values.md``
defines an owning, dynamically typed ordinary box. HGL uses the native Any
value plan, whose storage is an owning ``Value``. It does not enumerate the
admitted payload types or retain endpoint/view handles. ``any(value)`` consumes
one ordinary observation, retains its concrete contents and flattens an
existing box. The prepared ordinary value plan supplies the Any binding;
execution performs no registry or type-name lookup.

Held and delta generic schema projections reconstruct the canonical Any
scalar shape from its ordinary metadata at preparation time. They retain that
shape without inspecting a box's contents.

Any is one scalar temporal leaf. Its ordinary C++ representation uses the
same owning ``Value`` and borrowed ``ValueView`` conventions as aggregates.
That representation also selects aggregate lexical borrowing for global-state
entries. An explicit constructor retains a separate snapshot of a borrowed
entry before a later replacement.

The shared ordinary-operation boundary preserves exact contained canonical
type identity, empty ordering and partial ordering. Capability failures use
``value.capability`` at execution. A statically known missing capability in a
required constant context uses source category ``type`` and code
``value.constant_capability`` at the operation or key expression. The source
annotation catalogue and execution assertion catalogue remain disjoint.
Required parameter defaults select their own constant context even when their
owner is an executed value function. Ordinary field/index selection retains
constructor provenance for these checks; it adds no Any inspection surface.
Internal GIR constant recipes retain their checked result type. Direct and generated
field/index defaults use the existing owning preparation plans to construct
complete targets before projection, including composed selectors with explicit
list context. This adds no list inference or alternate constant storage.
Boxed keys require equality and hashing and recursively enforce the existing
non-NaN key restriction. Wired box comparisons use the same checked
ordinary operation from a typed HGL node, so graph composition preserves the
same execution failures as an explicit ``when`` observation. Physical storage
operations do not grant source structural deltas new ordinary capabilities. During preparation, structural
delta and aggregate bindings receive distinct immutable type records with
resolved recursive capability flags. Preparation follows the declared member
types, so empty collections carry the same restrictions as populated
collections; an Any member remains a dynamic boundary whose actual contents
are checked. Timezone's source no-order restriction is resolved during
preparation. Raw native timezone views use the existing exact scalar metadata
predicate, which reads the canonical ops-table object without resolving a
schema. Ordinary operations read capability flags, without callback address
comparisons, lookup, or interning during execution. Linker folding of
identical callbacks therefore cannot change source capabilities. Storage copying
and hashing remain available to internal retained configuration values.

Atomic/delta normalization, sparse child publication, complete atomic payloads,
rolling arrivals and generic timed replay/record retain complete boxes through
the existing value plans. Empty and equal publications remain present ticks.

Focused language builds may disable ``HGL_BUILD_SPEC_STDLIB_CONFORMANCE``
to exclude the independent audit-owned harness. Full acceptance leaves this
option enabled; ordinary language tests and shared standard-library parts
remain available in either mode.
