Proxy State Management
======================

``TSDProxy`` is the slot-aligned proxy storage used when a ``TSD``
needs to expose a different value representation while keeping the
source dictionary's key structure. The first use is the output
to-``REF`` alternative:

.. code-block:: text

   source:    TSD[K, TS[int]]
   requested: TSD[K, REF[TS[int]]]

The proxy does not own keys and does not support external mutation. It
mirrors the source ``TSD`` slot ids, owns one value slot per mirrored
source slot, and materialises those value slots through a caller-
provided value builder.

The important maintenance rule is that structural state is driven by
the same stable-slot observer protocol used by normal ``TSD`` storage.
Source key insert/remove/erase events construct and destroy the
parallel proxy child slots. Time-bearing modification notification is
separate: the proxy observes normal TSData modification notification
from the source and uses the source added/removed slot surfaces to
populate child values and mark proxy deltas.

Ordinary source child value ticks are not structural events. For the
to-``REF`` alternative they do not rebuild or tick the proxy, because
the reference identity remains the same.

There are two distinct to-``REF`` shapes involving ``TSD``:

.. code-block:: text

   direct value conversion:
       source:    TSD[K, V]
       requested: TSD[K, REF[V]]

   keyed path conversion:
       source:    TSD[K, TSB/TSL/TSD[..., V]]
       requested: TSD[K, TSB/TSL/TSD[..., REF[V]]]

In the direct value conversion, the proxy element storage type is the
requested ``REF[V]`` child and the builder materialises a
``TimeSeriesReference`` for each source slot.

In the keyed path conversion, the ``TSD`` is only a structural prefix
on the path to the ``REF`` leaf. The proxy element storage type is the
normal requested child structure, such as a ``TSB`` or fixed ``TSL``.
The proxy exists to keep the dynamic key slots aligned; population then
continues through that child structure until the ``REF`` leaf is
reached. If another ``TSD`` appears below the child, that nested
dictionary gets its own ``TSDProxy``.

Main Participants
-----------------

``TSOutputAlternativeStore``
    Root-owned cache of alternative output representations. A to-``REF``
    alternative is keyed by the starting output view and requested
    schema.

``ToRefAlternativeState``
    Owns the alternative ``TSData`` instance for one requested schema.
    Its ``TSData`` is constructed with a role-specific type record; when the
    requested shape contains a ``TSD`` on the path to the first
    ``REF``, that alternative's role record selects ``TSDProxy`` storage.

``TSDProxy``
    TSData storage for a proxy dictionary. It stores the borrowed
    source view, an inline ``ValueSlotStore`` for proxy children,
    normal TSData tracking, and a slot-observer set for downstream
    proxy users. The store itself is allocated and constructed through
    a proxy ``TypeRecord`` and ``StoragePlan``. Its key-set and value
    projections use the normal ``ts.tsd.key-set.*`` and ``ts.tsd.value.*``
    records.

``TSDProxySlotSync``
    Slot-event adapter owned inline by ``TSDProxy``. This is the object
    registered with the source key-set slot ops and source TSData
    notification set. It forwards structural slot callbacks to the
    owning proxy storage and forwards source modification notifications
    so the proxy can process added/removed slots once an evaluation time is
    available.

``TSDProxyValueOps``
    Static operation table borrowed by ``TSDProxy``. Its required build
    operation materialises a proxy child at a source slot. Its optional,
    read-only identity operation checks whether an existing child still
    represents the current source child without allocating or mutating
    either tree. The to-``REF`` builder creates ``REF`` leaves as
    ``TimeSeriesReference`` values, recursively creates nested proxy
    dictionaries, and walks static ``TSB`` / fixed ``TSL`` structures
    directly. The operation-table pointer replaces the former builder
    pointer, so it does not enlarge each proxy instance or require a
    registry; the existing builder-context pointer is unchanged.

Code Navigation Map
-------------------

The proxy path crosses a few type-erased layers. When reading the
code, start at the level that matches the question being investigated:

.. list-table::
   :header-rows: 1
   :widths: 28 36 36

   * - Question
     - Start here
     - What to look for
   * - How is an alternative requested?
     - ``src/hgraph/types/time_series/ts_output/alternative.cpp``
     - ``TSOutputAlternativeStore::binding_for`` and
       ``TSOutputAlternativeStore::to_ref_binding``
   * - How is the requested to-``REF`` shape validated?
     - ``src/hgraph/types/time_series/ts_output/alternative.cpp``
     - ``is_to_ref_shape`` and
       ``schema_equivalent_after_dereference``
   * - How is a proxy-backed ``TSD`` type created?
     - ``src/hgraph/types/time_series/ts_data/proxy.cpp``
     - ``tsd_proxy_data_type_for`` / ``tsd_proxy_output_type_for`` and
       ``TSDProxyContext``
   * - Which ops make the proxy look like a normal ``TSD``?
     - ``src/hgraph/types/time_series/ts_data/proxy.cpp``
     - ``TSDProxyContext::configure_ts_ops``,
       ``configure_value_ops``, and ``bind_surfaces``
   * - Where are source slot changes emitted?
     - ``src/hgraph/types/metadata/ts_data_slot_ops.cpp``
     - ``TSSSlotStorage`` / ``TSDSlotStorage`` forward slot observer
       registration to their ``KeySlotStore``
   * - Where does the proxy subscribe to those slot events?
     - ``src/hgraph/types/time_series/ts_data/proxy.cpp``
     - ``TSDProxy::subscribe_source``,
       ``TSDProxy::unsubscribe_source``, and ``TSDProxySlotSync``
   * - Where are proxy children created or refreshed?
     - ``src/hgraph/types/time_series/ts_data/proxy.cpp``
     - ``TSDProxy::construct_child_at_slot`` for structural slot
       construction and ``TSDProxy::ensure_child_at_slot`` for
       time-bearing materialisation
   * - Where is the to-``REF`` child value built?
     - ``src/hgraph/types/time_series/ts_output/alternative.cpp``
     - ``build_to_ref_proxy_value`` and ``populate_to_ref_data``
   * - Where are executable examples?
     - ``tests/cpp/test_tsd_proxy.cpp`` and
       ``tests/cpp/test_time_series_reference.cpp``
     - Direct proxy use, to-``REF`` alternatives, and nested proxy
       behaviour

Public Usage: Binding an Output as a REF-Shaped View
----------------------------------------------------

Normal callers should not construct ``TSDProxy`` directly. They ask an
output view for the representation expected by an input. If the
requested schema is a to-``REF`` representation of the source schema,
the alternative store creates the proxy machinery internally.

The essential usage shape is:

.. code-block:: cpp

   using namespace hgraph;

   auto       &registry = TypeRegistry::instance();
   const auto *int_meta = registry.value_type("int");
   const auto *ts_int   = registry.ts(int_meta);
   const auto *ref_int  = registry.ref(ts_int);

   const auto *source_schema    = registry.tsd(int_meta, ts_int);
   const auto *requested_schema = registry.tsd(int_meta, ref_int);

   TSOutput output{*source_schema};
   Value    key{Int{1}};

   {
       Value value{Int{42}};
       auto  dict          = output.data_view().as_dict();
       auto  dict_mutation = dict.begin_mutation(MIN_ST);
       auto  child         = dict_mutation.at(key.view());
       auto  child_mutation = child.begin_mutation(MIN_ST);
       child_mutation.copy_value_from(value.view());
   }

   TSOutputHandle handle = output.view(MIN_ST).binding_for(*requested_schema);
   auto           view   = handle.view(MIN_ST).as_dict();

   TimeSeriesReference ref =
       view.at(key.view()).value().checked_as<TimeSeriesReference>();

The output handle returned here points at alternative data owned by
the output. If the source dictionary later adds or removes keys, the
proxy receives slot events from the source ``TSS`` / ``TSD`` ops and
keeps the alternative dictionary aligned.

Internal Usage: Building a Generic TSDProxy
-------------------------------------------

``TSDProxy`` is also a reusable internal component. The caller provides
the proxy TSData type, source dictionary, and a value builder. This
is useful for tests and for future alternative representations that
need to mirror a source ``TSD`` but own transformed values.

.. code-block:: cpp

   void copy_key_to_child(TSDProxy      &proxy,
                          std::size_t   slot,
                          TSDataView    target,
                          TSDataView    source,
                          DateTime modified_time,
                          const void   *context)
   {
       (void) source;
       (void) context;

       ValueView key = proxy.source_dict().key_at_slot(slot);

       auto mutation = target.begin_mutation(modified_time);
       if (!mutation.copy_value_from(key))
       {
           throw std::logic_error("proxy child key is incompatible");
       }
   }

   const auto *proxy_schema   = source_schema;
   const auto *element_schema = proxy_schema->element_ts();
   const auto source_type =
       TSDataPlanFactory::instance().data_type_for(source_schema);
   const auto element_type =
       TSDataPlanFactory::instance().data_type_for(element_schema);

   TSData source{source_type};
   TSData proxy{tsd_proxy_data_type_for(*proxy_schema,
                                        element_type.as_role())};

   bind_tsd_proxy(proxy.view(),
                  source.view().as_dict(),
                  &copy_key_to_child,
                  nullptr,
                  evaluation_time);

The direct proxy API should only be used by infrastructure code that
knows the source and proxy schemas are compatible. Public input/output
endpoint negotiation should continue to go through ``TSOutputView::binding_for`` so
the alternative store can validate the requested schema and reuse
cached alternative state.

Implementing a Slot Observer
----------------------------

Slot observers are for storage-level coordination between slot-shaped
TSData implementations. They are not user-facing graph observers. They
carry structural slot ids only; they do not carry evaluation time or delta
classification. A consumer implements the structural callbacks and
registers through the ``TSS`` view returned by
``TSDDataView::key_set()``.

.. code-block:: cpp

   class Mirror final : public SlotObserver
   {
     public:
       void on_capacity(std::size_t old_capacity, std::size_t new_capacity) override
       {
           reserve_mirror_slots(new_capacity);
       }

       void on_insert(std::size_t slot) override
       {
           construct_mirror_slot(slot);
       }

       void on_remove(std::size_t slot) override
       {
           retain_removed_mirror_slot(slot);
       }

       void on_erase(std::size_t slot) override { destroy_mirror_slot(slot); }
       void on_clear() override { clear_mirror_slots(); }
   };

   Mirror observer;
   auto   source_dict = source.view().as_dict();

   source_dict.key_set().subscribe_slot_observer(&observer);
   source_dict.key_set().unsubscribe_slot_observer(&observer);

``TSDProxy`` follows this pattern internally through its
``TSDProxySlotSync`` member. The sync adapter subscribes to the source
key-set, then forwards source slot events to the proxy storage, which
updates its inline value slots and emits downstream proxy slot events.
The proxy separately subscribes to source TSData modification
notification to process added/removed slot surfaces once the evaluation
time is known.

Creation Flow
-------------

The creation path starts when an input asks an output for a requested schema
in a requested schema. If the requested schema is a to-``REF`` view of
the source schema, the output allocates or reuses an alternative state.

.. mermaid::

   flowchart TD
      A["Input requests schema"]
      B["TSOutputView negotiates representation"]
      C["Alternative store builds cache key"]
      D["Alternative store validates to REF shape"]
      E["Create or rebind ToRefAlternativeState"]
      F["Resolve alternative TSData type"]
      G["Use TSDProxy type for keyed TSD"]
      H["Construct alternative TSData"]
      I["Populate alternative data"]
      J["Bind TSDProxy to source dictionary"]
      K["Subscribe TSDProxySlotSync to source slot ops"]
      L["Sync proxy from source slots"]
      M["Ensure proxy child at slot"]
      N["Run value builder"]
      O["Return alternative output handle"]
      P["Return output handle to input"]

      A --> B
      B --> C
      C --> D
      D --> E
      E --> F
      F --> G
      G --> H
      H --> I
      I --> J
      J --> K
      K --> L
      L --> M
      M --> N
      N --> O
      O --> P

For static structures, ``populate_to_ref_data`` walks the shape
immediately:

- ``REF`` writes a ``TimeSeriesReference`` value into the target child.
- ``TSB`` recurses over fields.
- fixed ``TSL`` recurses over indices.
- ``TSD`` binds a ``TSDProxy`` and then lets the proxy mirror the
  source key slots.

The ``TSD`` type chosen here depends on where the first ``REF``
appears below the dictionary. For ``TSD[K, REF[V]]`` the proxy child is
the ``REF[V]`` leaf and the builder writes a reference token directly.
For ``TSD[K, TSB/TSL/TSD[..., REF[V]]]`` the proxy child is the
requested child structure, and the builder recurses through that
structure before writing the eventual ``REF`` leaf.

The ``TSD`` case is the only dynamic case in the current to-``REF``
implementation. It is the only place where ongoing source maintenance
is required after the alternative is created.

State Layout
------------

The proxy storage is intentionally slot-aligned with the source. The
key store remains owned by the source ``TSD``. The proxy owns only the
alternative child payloads and tracking required to expose the
requested view.

.. mermaid::

   flowchart LR
      subgraph Source["source TSD storage"]
         Keys["KeySlotStore<br/>slot -> key<br/>live / pending erase"]
         SourceValues["source value slots<br/>slot -> child TSData"]
         SourceDelta["TSD delta bits<br/>added / removed / modified"]
         SourceObservers["SlotObserverList<br/>on source key store"]
         SourceNotify["TSData observers<br/>source modified time"]
      end

      subgraph Proxy["TSDProxy storage"]
         Sync["TSDProxySlotSync<br/>source slot adapter"]
         SourceHandle["source_ TSDataView<br/>storage type + memory"]
         ProxyValues["ValueSlotStore<br/>slot -> proxy child TSData"]
         ProxyTracking["TSDataTracking<br/>last_modified_time + parent"]
         ProxyObservers["SlotObserverList<br/>for downstream proxy users"]
         Builder["TSDProxyValueOps<br/>build + optional identity match"]
      end

      Keys --> SourceValues
      Keys --> SourceDelta
      SourceObservers -- structural slot events --> Sync
      SourceNotify -- modified time --> Sync
      Sync --> Proxy
      SourceHandle -. reads .-> Keys
      SourceHandle -. reads .-> SourceValues
      Proxy --> ProxyValues
      Proxy --> ProxyTracking
      Proxy --> ProxyObservers
      Builder --> ProxyValues

The invariant is:

.. code-block:: text

   if source slot S is live or removed in the current delta:
       proxy slot S may contain a child value

   if source slot S is physically erased:
       proxy slot S is destroyed

   proxy keys are always read from the source key store

Creation Details
----------------

The relevant call stack for creating a dynamic to-``REF`` dictionary is:

.. code-block:: text

   TSOutputView::binding_for(requested_schema)
     TSOutputAlternativeStore::binding_for(source_view, requested_schema)
       TSOutputAlternativeStore::to_ref_binding(...)
         ToRefAlternativeState::ToRefAlternativeState(...)
           to_ref_ts_data_type_for(requested_schema)
             tsd_proxy_data_type_for(tsd_schema, element_type)
               TSDProxyContext::configure_ts_ops()
               TSDProxyContext::configure_value_ops()
               TSDProxyContext::bind_surfaces()
           ToRefAlternativeState::rebind(source_view)
             ToRefAlternativeState::refresh(evaluation_time)
               populate_to_ref_data(target, source_view, requested_schema)
                 bind_tsd_proxy(target, source_view.data_view().as_dict(), value_ops)
                   TSDProxy::bind(...)
                     TSDProxy::sync_from_source(...)
                       TSDProxy::ensure_child_at_slot(slot)
                         TSDProxyValueOps::build(...)
                     TSDProxy::subscribe_source()
                       source.key_set().subscribe_slot_observer(source_sync_)
                       source.subscribe(source_sync_)

The type factory work is shape-level and cached. ``TSDProxyContext``
builds the type-erased ops that make the proxy look like a normal
``TSD`` to callers:

- key-set ``TSS`` ops read keys and added/removed sets from the
  source dictionary;
- dictionary ``TSD`` ops read values from proxy-owned inline value
  slots;
- value ops expose live, added, removed, and modified value surfaces;
- slot observer ops allow nested proxies to subscribe to this proxy in
  the same way they subscribe to a normal source ``TSD``.

Key Add Flow
------------

When a source key is inserted, the source ``KeySlotStore`` emits a
structural insert event. The proxy consumes that event immediately to
construct the aligned child slot. Once the source TSData modification
notification fires carrying an evaluation time, the proxy navigates the source
added slots and materialises the child value.

.. mermaid::

   flowchart TD
      A["source mutation at key"]
      B["source TSDSlotStorage insert key"]
      C["KeySlotStore notifies insert"]
      D["TSDProxySlotSync forwards insert"]
      E["TSDProxy constructs proxy child slot"]
      F["proxy slot observers notify insert"]
      G["source TSData marks modified"]
      H["TSDProxySlotSync receives modified time"]
      I["proxy scans source added slots"]
      J["run value builder for added slot"]
      K["mark proxy modified"]
      L["bubble parent modification"]

      A --> B
      B --> C
      C --> D
      D --> E
      E --> F
      F --> G
      G --> H
      H --> I
      I --> J
      J --> K
      K --> L

For the to-``REF`` alternative, the builder creates a
``TimeSeriesReference`` only when the proxy child is the ``REF`` leaf.
If the requested child is a structural prefix, the builder constructs
that normal requested child and continues population below it. If the
requested child is itself a ``TSD``, the builder creates a nested
``TSDProxy`` for that source child.

Key Remove, Resurrection, and Physical Erase
--------------------------------------------

Logical removal and physical erase are separate. On removal the proxy
stops the child tree, retains its stable value-slot address, clears the
slot's built stamp, and emits ``remove``. It does not invalidate or
destroy the child. Physical erase performs invalidation and destruction.

If the source reinserts the same key before physical erase, the proxy
resurrects the retained child in place. The builder rebinds the stopped
tree at the current lifecycle time before the proxy emits ``insert``.
This ordering means downstream observers never see an inserted but stale
child. It also preserves the child address and allocated slot capacity.
An erase followed by insertion in the same delta therefore cancels the
source added/removed delta bits while still producing exactly one
``remove`` and one ``insert`` lifecycle event.

.. mermaid::

   flowchart TD
      A["source mutation erase key"]
      B["source TSDSlotStorage remove key"]
      C["KeySlotStore notifies remove"]
      D["TSDProxySlotSync forwards remove"]
      E["TSDProxy stops and retains proxy child slot"]
      F["proxy slot observers notify remove"]
      G["source TSData marks modified"]
      H["proxy scans source removed slots"]
      I["mark proxy modified"]
      J["later cleanup or next delta"]
      K["KeySlotStore notifies erase"]
      L["TSDProxySlotSync forwards erase"]
      M["TSDProxy destroys proxy value slot"]
      N["source physically erases slot"]

      A --> B
      B --> C
      C --> D
      D --> E
      E --> F
      F --> G
      G --> H
      H --> I
      I --> J
      J --> K
      K --> L
      L --> M
      M --> N

If eager resurrection fails, the builder exception propagates, no
``insert`` event is emitted, and the build stamp retains its insert debt. A later
source notification can retry the rebind before normal child-tick
processing. Removed or pending slots are never restarted merely because
the source root ticks. The same rule applies when an alternative cache hit
rebinds and resynchronises the proxy: an occupied but non-live source slot
retains its stopped child without running the builder or emitting
``insert``. Storage existence and the active build stamp are separate
facts; a pending stamp does not mean the value slot is unconstructed.

``built_times`` uses two sentinel tags so lifecycle debt survives source
delta rollover without another side table. ``MIN_DT`` means a build is
pending but ``insert`` was already announced; this is the state created by a
new slot callback. ``MAX_DT`` means a build is pending and ``insert`` is
still owed; initial unannounced materialisation and stopped removal /
resurrection use this state. Values in ``[MIN_ST, MAX_ET]`` are concrete
successful build times. No code compares source and build times until both
have been classified as concrete.

Every retry preserves its sentinel if the builder throws. On success it
stores the concrete build time before notifying observers, then emits
``insert`` only for ``MAX_DT``. This covers same-time coalesced root
notifications, cache-hit resynchronisation, and retries after a later delta
has cleared ``slot_added`` / ``slot_removed`` without inferring lifecycle
debt from those transient surfaces.

``TSDProxy::bind`` validates its lifecycle time against
``[MIN_ST, MAX_ET]`` before changing configuration or subscriptions. This
applies even when the source is empty or has no child to build, so neither
``MIN_DT`` nor ``MAX_DT`` can partially configure a proxy or enter tracking
state.

Source Invalidation and Rebind
------------------------------

The source view is borrowed, so destruction of the source must invalidate
the proxy immediately. The root invalidation callback first marks the
subscription inactive, removes the key-set slot observer while the source
storage is still live, and then releases the source cursor. It does not
attempt to unsubscribe the root observer from inside that observer's
invalidation callback.

The proxy then stops and invalidates every constructed child tree,
invalidates its own root observers, emits one key-set ``clear``, destroys
the value slots, and resets built/delta-window state. Nested proxies receive
the same invalidation synchronously, so no descendant retains a cursor into
dead source storage. Source-dependent views report unavailable/empty after
this point rather than dereferencing the expired source.

The proxy retains its type, builder context, and allocated slot capacity.
A later ``bind`` can therefore attach it to a replacement source without
allocating a second state object. Normal stop or rebind removes both root
and slot subscriptions before releasing the source cursor. A later stop or
destructor after source invalidation is idempotent and does not emit another
``clear``.

Invalidation also uses the existing child-refresh byte as an internal
``Invalidating`` sentinel. The sentinel is installed only after the callback
identity has been validated and remains active through child stop and
invalidation, root-observer callbacks, slot ``clear`` callbacks, and child
destruction. A callback that attempts to rebind the same proxy receives a
``logic_error`` before any proxy state changes. The prior refresh policy is
restored on exit, including exceptional exit, so a later non-reentrant bind
remains valid. No additional per-instance guard or ownership state is needed.

Source Child Update Flow
------------------------

A source child update is not a key-set structural event. It may mark the
source ``TSD`` modified and set the source modified-slot surface, but it does
not construct or destroy proxy slots. Each live proxy child records the
evaluation time at which its value was built. A later source time rebuilds
the child. At the same source time, ``StructureOnly`` proxies keep the
existing child, while ``OnChildTick`` proxies call their required read-only
identity operation. A match performs no build; a mismatch rebuilds once at
that evaluation time. Repeated reads therefore compare identities without
rebuilding an already reconciled child.

The from-``REF`` identity operation compares peered target links using
``TSOutputHandle::same_as``. It also handles empty or unbound references,
recurses through fixed bundle/list shapes, and delegates nested dynamic
dictionaries to their proxy source cursor and child identities. This catches
erase/reinsert plus reference rewrites that occur at one evaluation time,
without relying on timestamps precise enough to distinguish the two source
identities.

Nested Proxy Flow
-----------------

Nested dynamic dictionaries use the same mechanism recursively. The
outer proxy builds an inner proxy child; that inner proxy subscribes to
the corresponding source child dictionary's slot ops.

.. mermaid::

   flowchart TD
      SourceOuter["source TSD[K1, TSD[K2, TS[int]]]"]
      ProxyOuter["outer TSDProxy<br/>requested TSD[K1, TSD[K2, REF[TS[int]]]]"]
      SourceInner["source child slot S<br/>TSD[K2, TS[int]]"]
      ProxyInner["inner TSDProxy<br/>requested TSD[K2, REF[TS[int]]]"]
      RefLeaf["REF leaf<br/>TimeSeriesReference to source TS[int]"]

      SourceOuter -- outer structural slots --> ProxyOuter
      ProxyOuter -- builder for outer slot S --> ProxyInner
      ProxyInner -- source_ points to --> SourceInner
      SourceInner -- inner structural slots --> ProxyInner
      ProxyInner -- builder for inner slot --> RefLeaf

The outer proxy does not scan or manually maintain the inner key set.
It owns the inner proxy child, and the inner proxy subscribes to the
inner source ``TSD`` slot ops.

Cleanup Flow
------------

The source storage owns delta cleanup and pending-slot reclamation. When
it advances the delta window, ``KeySlotStore`` emits ``erase`` before it
physically reuses a pending slot. ``TSDProxySlotSync`` forwards that event,
and the proxy invalidates and destroys the retained child at the same slot.

The proxy does not independently scan source occupancy during cleanup.
Doing so would bypass the slot observer protocol and could destroy retained
storage before downstream users have processed the removal. Child payload
delta cleanup remains the responsibility of each child's normal TSData
operations.

Design Constraints
------------------

- The proxy must not use generic ``Notifiable`` subscriptions to keep
  its structural slots aligned. Structural alignment comes from
  ``SlotObserver``. Normal TSData notification may be used only to learn
  the evaluation time and then read the source added/removed slot surfaces.
- The source key store is authoritative. Proxy keys are read from the
  source through ops; the proxy does not copy keys into an associative
  side map.
- Proxy value slots are stored inline in the ``TSDProxy`` TSData
  storage object. ``ValueSlotStore`` may allocate stable slot blocks as
  capacity grows, but the store object itself is not an optional
  sidecar.
- Slot ids are stable path ids. The same slot id is used for source
  key lookup, source child lookup, proxy child storage, delta
  reporting, and parent bubble-up.
- ``TSDProxy`` is read-only from the public TSData API. It is updated
  only by source slot ops and its value builder.
- Static ``TSB`` and fixed ``TSL`` alternatives are populated by
  direct recursive traversal. They do not need ongoing proxy
  maintenance unless a ``TSD`` appears below them.
