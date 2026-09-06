RFC 0035: A Python-Free Type Layer
==================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-06
:Target: ``include/hgraph/types/python_object.h`` (new),
         ``include/hgraph/types/python_ops.h`` (new),
         ``include/hgraph/types/value/value_ops.h``,
         ``include/hgraph/types/value/{value.h,value_view.h}``,
         ``include/hgraph/types/value/{compact,mutable}_container_ops.h``,
         ``include/hgraph/types/time_series/ts_data/{ops.h,base_view.h}``,
         ``include/hgraph/types/time_series/ts_input/{base_view.h,detail.h}``,
         ``include/hgraph/types/{frame,series,temporal,time_series_reference,value_callable,wired_fn}.h``,
         ``src/hgraph/types/metadata/{value_plan_factory,type_realization,type_registry}.cpp``,
         ``src/hgraph/types/metadata/ts_data_{atomic,slot,fixed_structured,dynamic_list,window}_ops.cpp``,
         ``src/hgraph/types/value/{any_ops,value_view,compact_container_ops}.cpp``,
         ``src/hgraph/types/value/impl/pooled_polymorphic_value_type.*``,
         ``src/hgraph/types/time_series/{ts_input.cpp,ts_data/proxy.cpp,ts_input/target_link_ops.cpp}``,
         ``include/hgraph/python/{conversion.h (new),native_scalar_registration.h,ts_data_conversion.h,retained_value.h}``,
         ``src/hgraph/python/impl/*`` (new units), ``python/module.cpp``,
         ``python/value_conversion.cpp``, ``include/hgraph/config.h``
:Related: RFC 0003 (Python scalar registration), RFC 0004 (python-owned
          structured scalars), RFC 0033 (type carriers; the ratchet
          discipline), developer guide ``python_bridge.rst`` ("No
          kind-switches in conversion", "One set of Python-object value
          primitives"), ``testing.rst`` ("Architecture ratchets"),
          ``architecture.rst`` (library layering), ``extension_policy.rst``

Summary
-------

The type layer (``include/hgraph/types``, ``src/hgraph/types``) compiles
from one source whether or not Python user nodes are enabled. Its ops tables
keep their Python slots -- conversion binding onto per-type ops is the
sanctioned design (ruling 2026-07-07) -- but the slots are typed on an
*opaque CPython reference* the type layer can name without any Python header,
and they are *filled from tables the bridge registers at load*, never from
code compiled into the type layer. Every conversion body that lives in the
type layer today moves to a bridge unit under ``src/hgraph/python/impl/``;
the bridge reaches private storage layouts through private detail headers of
the family it converts, and the type layer reaches the bridge only through
the registered ``PythonOps`` table.

The result is measured by two ratchets in
``python/tests/test_architecture_ratchets.py``:
``type-layer-python-conditionals`` (``HGRAPH_ENABLE_PYTHON_USER_NODES``
mentions under the type layer) goes from 160 to 0, and a new
``type-layer-nanobind`` ratchet (``nanobind`` / ``nb::`` mentions under the
type layer) is introduced at 0 when the migration completes. Behaviour is
preserved: the same Python values cross in both directions, the same errors
are raised for unconvertible schemas, and the per-tick conversion path is the
same function-pointer call it is today.

Motivation
----------

The 2026-09-04 retrospective of the fix series named "Python objects in the
type layer" as its sixth family: two ops tables for Python-object values
(consolidated by the ``python-object-hash-units`` ratchet and the retained
entry's move to the bridge, PR #698) and 160 ``HGRAPH_ENABLE_PYTHON_USER_NODES``
conditionals under the type layer. The inventory taken for this RFC
(2026-09-06) counts 156 guarded blocks across 34 files, with no ``#else``
anywhere: every guard removes code when Python is off. They fall into four
groups.

**Slot declarations and their single root (18 mentions).**
``include/hgraph/types/value/value_ops.h`` includes ``<nanobind/nanobind.h>``
(plus ``ndarray.h``, ``stl/string.h`` and ``hgraph/python/chrono.h``) under
the guard and declares ``namespace nb = nanobind`` for the whole type layer.
``ValueOps`` (three slots), ``TSDataOps`` (a ``python_ops`` table pointer and
three slots), the TS input shape ops (two slots) and the ``ValueView`` /
``Value`` / ``TSDataView`` / ``TSInputView`` convenience wrappers all exist
only when the guard is on. Every other type-layer header that spells ``nb::``
relies on that one include.

**Conversion bodies written against private layouts (about 100 mentions).**
``composite_value_to_python`` reads the plan factory's composite context,
``tss_to_python`` the slot ops' context, ``window_from_python`` the window
storage classes, ``to_python`` of a pooled polymorphic value its allocation
header. These bodies cannot move without either exposing the layouts or
rewriting them over erased ops.

**Per-kind tables selected by naming bridge symbols (8 mentions).**
``ts_data_atomic_ops.cpp`` writes
``.python_ops = &python_bridge::atomic_python_ts_data_ops()``; the slot,
fixed, dynamic, window and target-link factories do the same for their
families. The type layer calls upward into the bridge by link-time symbol
reference, under a guard, which is exactly the dependency direction the
layering forbids (``architecture.rst``; the ELF lesson in
``elf-hides-layering-violations``).

**Public headers exposing Python types (9 mentions).** ``frame.h``,
``series.h``, ``time_series_reference.h``, ``temporal.h``,
``value_callable.h`` and ``wired_fn.h`` specialise ``python_conversion_traits``
with ``nanobind::object`` / ``nanobind::handle`` hook pairs the module
installs at import.

Three build presets (core-only, ``python-user-nodes``, bindings) therefore
compile three different type layers from one source. The IDE override in
``include/hgraph/config.h`` (``__JETBRAINS_IDE__`` forces the macro on so
the guarded surface is parsed at all) records how much of the layer is
invisible to tooling. The bridge already owns two policies through plain
function-pointer tables the type layer consumes without a guard --
``ValuePlanFactory::PythonStorageProvider`` (retained values, PR #698) and
the storage provider's self-registration at load -- and that pattern is the
precedent this RFC generalises to every remaining site.

Terms
-----

*Opaque reference*
   A CPython object pointer the type layer can name without including any
   Python header: ``struct _object`` forward-declared, wrapped as
   ``PyRef`` (borrowed) or ``PyNewRef`` (a new reference the receiver owns).

*Slot*
   A function-pointer member of an ops table (``ValueOps::to_python_impl``,
   ``TSDataOps::delta_to_python_impl``, ...). Slots are the type layer's; their
   values are the bridge's.

*Family*
   One representation strategy with its own factory and private layout:
   compact list / set / map / cyclic buffer / queue, mutable list / set /
   map, composite, array, owned and shared entries, closed bundle, pooled
   polymorphic, ``Any``, enum, atomic TS, slot TS (TSS / TSD), fixed
   structured TS (TSB / TSL), dynamic list TS, window TS, TS input shapes,
   target links.

*Provider table*
   ``hgraph::PythonOps``: one struct of plain function pointers, grouped by
   family, that the bridge fills and registers once at load. Factories read
   it when they build an ops table; they never call it per tick.

*Bridge unit*
   A translation unit under ``src/hgraph/python/impl/`` compiled into
   ``hgraph_runtime`` exactly when Python user nodes are enabled
   (``src/CMakeLists.txt``, ``HGRAPH_BUILD_PYTHON_BINDINGS OR
   HGRAPH_ENABLE_PYTHON_USER_NODES``). It is the only place that includes
   nanobind below ``python/``.

Ownership boundary
------------------

* **The type layer owns** the opaque reference type, the slot declarations,
  the ``PythonOps`` contract and its registration slot, and the rule that a
  slot left empty means "no Python conversion is registered for this
  schema". It never includes a Python header, never calls a slot, and never
  names a bridge symbol.
* **The bridge owns** every implementation: the scalar conversions
  (``nb::cast`` for the castable primitives, the ``Time`` / ``Bytes`` /
  temporal / ``PyObj`` conversions, and the hook-forwarded ``Frame`` /
  ``Series`` / ``TimeSeriesReference`` / ``WiredFn`` / ``ValueCallable``
  conversions), the enum conversions, ``Any`` and JSON-``Any``, every
  container and structural conversion, the TS data authoring tables
  (``PythonTSDataOps``), the TS input and target-link facades, the retained
  cache invalidation, and the polymorphic Python-source resolver. It also
  owns the wrappers callers use (``python_bridge::to_python(const ValueView &)``
  and friends) and the ``PythonOps`` registration, performed at unit load and
  again, idempotently, by the module initializer.
* **The module** (``python/``) keeps what needs pyarrow or the DSL: the
  ``Frame`` / ``Series`` hooks, the enum class registry, value inference,
  JSON, and the registration of the stdlib enum conversions
  (``DivideByZero``, ``CmpResult``, ``ToTableMode``), which it performs
  before it registers the standard operators.
* **Extensions** keep specialising ``python_conversion_traits<T>`` for their
  scalars and enums; the primary template moves from ``value_ops.h`` to
  ``include/hgraph/python/native_scalar_registration.h`` (the RFC 0003
  header they already include), and an extension registers each conversion
  explicitly in its module initializer, before the type is first used.

C++ contract
------------

``PyRef`` / ``PyNewRef`` -- the opaque reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``include/hgraph/types/python_object.h``::

    struct _object;   // CPython's PyObject tag; compatible with <Python.h>

    namespace hgraph
    {
        /** A borrowed reference: the caller keeps ownership for the call. */
        struct PyRef    { ::_object *ptr{nullptr}; };
        /** A new reference: the receiver owns exactly one reference. */
        struct PyNewRef { ::_object *ptr{nullptr}; };
    }

The two wrappers are distinct types so ownership is visible at every slot
boundary without a binding library. A slot that fails throws (as today:
``nb::python_error`` and the ``std::`` exceptions propagate through the
function pointer unchanged); a slot never returns a null ``PyNewRef`` in
place of an exception. Only bridge code converts between the wrappers and
``nb::handle`` / ``nb::object``:

* ``python_bridge::borrow(nb::handle) -> PyRef``,
* ``python_bridge::give(nb::object &&) -> PyNewRef`` (``release().ptr()``),
* ``python_bridge::take(PyNewRef) -> nb::object`` (``nb::steal``).

Slots
~~~~~

Every slot exists unconditionally and defaults to ``nullptr``:

``ValueOps`` (``value_ops.h``)
   ``PyNewRef (*to_python_impl)(const void *context, const void *memory)``,
   ``void (*from_python_impl)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source)``,
   ``PyNewRef (*to_python_buffer_impl)(const void *context, const ValueTypeRef &binding, const ValueArraySource &source)``.
   ``ValueArrayElementAt`` / ``ValueArraySpan`` / ``ValueArraySource`` are
   plain C++ and lose their guard.

``TSDataOps`` (``ts_data/ops.h``)
   ``const python_bridge::PythonTSDataOps *python_ops{nullptr}`` (the struct
   stays forward-declared, as it is today),
   ``bool (*from_python_impl)(const void *, void *, PyRef, DateTime)``,
   ``PyNewRef (*to_python_impl)(const void *, const void *)``,
   ``PyNewRef (*delta_to_python_impl)(const void *, const void *, DateTime)``.
   The ``missing_from_python`` / ``missing_to_python`` /
   ``missing_delta_to_python`` thunks and ``missing_python_ts_data_ops()``
   leave the type layer; the bridge's accessor substitutes its throwing
   table for a null ``python_ops`` so Python-side dispatch still has no kind
   branch and no null branch of its own.

TS input shape ops (``ts_input/detail.h``)
   ``PyNewRef (*to_python)(const void *, const void *)``,
   ``PyNewRef (*delta_to_python)(const void *, const void *, DateTime)``.

The convenience members ``ValueOps::to_python`` / ``from_python`` /
``can_to_python_buffer`` / ``to_python_buffer``, ``ValueView::to_python`` /
``from_python`` / ``assign_from_python``, ``Value::to_python`` /
``from_python``, ``TSDataView::value_to_python`` / ``delta_value_to_python``,
``TSDataMutationView::from_python`` and ``TSInputView::value_to_python`` /
``delta_value_to_python`` are removed from the type layer and re-homed as
free functions in ``include/hgraph/python/conversion.h`` (below). The type
layer keeps one predicate it can evaluate without Python:
``ValueOps::has_python_conversion() noexcept`` (``to_python_impl != nullptr``),
which ``ValueStorageSelection`` and the plan factory already need.

``PythonOps`` -- the provider table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``include/hgraph/types/python_ops.h`` declares one struct of plain function
pointers, grouped by family. Each entry has the signature of the slot it
fills, so a factory copies a pointer and never wraps it::

    namespace hgraph
    {
        struct PythonOps
        {
            struct Scalars
            {
                /** The slots for a C++ scalar type, or nullptr when the bridge
                    has no conversion registered for it. Called once, when
                    ``ops_for<T>()`` constructs T's table. */
                const ScalarSlots *(*conversion_for)(const std::type_info &type){nullptr};
            } scalars;
            struct Enums     { to_python, from_python }                    enums;
            struct Any       { to_python, from_python, json_to_python, json_from_python } any;
            struct Compact   { list, tuple, array, list_from, set, set_from, map, map_from,
                               cyclic_buffer, cyclic_buffer_from, queue, queue_from,
                               map_key_adapter }                            compact;
            struct Mutable   { list, map, set }                             mutable_containers;
            struct Plan      { composite, composite_from, array, array_numpy, array_from,
                               owned_entry, shared_entry }                  plan;
            struct Realized  { closed_bundle, closed_bundle_from,
                               polymorphic_source_resolver }                realized;
            struct Retained  { void (*invalidate)(void *holder) noexcept }  retained;
            struct TSData    { atomic, ref, set, dict, list, bundle, window, target_link
                               /* PythonTSDataOps tables */,
                               atomic_from, atomic_to, atomic_delta /* x storage variant */,
                               tss_*, tsd_*, fixed_*, dynamic_*, window_*,
                               the delta / key-set / projection value-ops entries } ts_data;
            struct TSInput   { tsb, tsl, tsb_delta, tsl_delta, the projection entries } ts_input;
            struct TargetLink{ to_python, delta_to_python }                 target_link;
        };

        HGRAPH_EXPORT void set_python_ops(const PythonOps *ops);
        [[nodiscard]] HGRAPH_EXPORT const PythonOps *python_ops() noexcept;
    }

(The sketch names entries; the implementation PRs spell each signature,
which is the existing slot signature with ``nb::object`` → ``PyNewRef`` and
``nb::handle`` → ``PyRef``. Private context types stay ``const void *`` at
the table, as they are at the slots today.)

Rules:

1. **Pull at construction.** A factory reads ``python_ops()`` when it builds
   a table and copies the entries of its family; with no provider (or a
   null entry) the slot stays ``nullptr``. Nothing reads the provider per
   tick.
2. **Registered at load, before any table exists.** The bridge unit
   registers the table from a namespace-scope initializer, so a standalone
   ``python-user-nodes`` build has it without the ``_hgraph`` module; the
   module initializer registers it again, idempotently. ``set_python_ops``
   throws ``std::logic_error`` if an ops table has already been constructed
   (``value_ops_detail::tables_constructed()``, a relaxed atomic counter
   ``ops_for<T>()`` and the factories increment). The type layer is never
   used from static initialisers today (registration happens in
   ``register_*_operators()`` and lazily through ``static_schema.h``); the
   check keeps it that way.
3. **One direction.** The type layer never names ``python_bridge::`` symbols
   except the forward-declared ``PythonTSDataOps`` type; the bridge may
   include the type layer's private detail headers
   (``src/hgraph/types/**/detail/*.h``) because both compile into
   ``hgraph_runtime``.

Scalars
~~~~~~~

``ops_for<T>()`` no longer instantiates ``nb::cast`` or
``python_conversion_traits<T>``. It asks the provider once::

    if (const auto *ops = python_ops(); ops != nullptr && ops->scalars.conversion_for != nullptr)
        if (const auto *slots = ops->scalars.conversion_for(typeid(T)))
            { .to_python_impl = slots->to_python, ... }

The bridge keeps an ``ankerl::unordered_dense::map<std::type_index, ScalarSlots>``
filled at load with the core list -- ``bool``, ``Int``, ``double``,
``std::string``, ``Date``, ``DateTime``, ``TimeDelta``, ``Time``, ``Bytes``,
the twelve temporal types, ``Frame``, ``Series``, ``TimeSeriesReference``,
``WiredFn``, ``ValueCallable``, ``PyObj`` -- and extended by
``register_python_scalar_conversion<T>()`` (used by RFC 0003's
``register_native_scalar_type<T>`` and by the module for the stdlib enums).
The generic thunks (``nb::cast`` for the castable primitives, the string
strictness rule for numeric scalars, the buffer thunk) are bridge templates
instantiated per listed ``T``; the hook pairs for ``Frame`` and friends stay
hook pairs, in bridge headers, so the module still installs the pyarrow
glue after import.

**Late registration is an error, not a patch.** Registering a conversion
for a ``T`` whose schema the registry already holds throws
``std::logic_error("Python conversion for <T> registered after first use;
register it before the type is used")``. The rule replaces today's
compile-time rule ("specialisations must be visible wherever
``register_scalar<T>`` first instantiates") with a runtime one that fails
loudly, and it is what the installed-extension consumer check pins.

Enums
~~~~~

``enum_to_python_slot()`` / ``enum_from_python_slot()`` leave
``value_ops.h``; the registry's enum ops copy ``python_ops()->enums`` at
construction. The module still owns the meta → Python-enum-class registry and
installs it through bridge hooks the two enum entries forward to.

TS data authoring tables
~~~~~~~~~~~~~~~~~~~~~~~~

``PythonTSDataOps`` (``include/hgraph/python/ts_data_conversion.h``) is
unchanged in shape. The seven per-family tables and the target-link table are
still defined by the bridge, but the factories obtain them from
``python_ops()->ts_data`` instead of naming ``atomic_python_ts_data_ops()``
and the others by symbol. ``python_bridge::python_ts_data_ops(const TSDataOps &)``
returns the throwing ``missing`` table for a null pointer.

Retained cache invalidation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The atomic TS ops drop the retained ``PyObject`` when a native write lands
on a ``NativeWithPythonCache`` output (``invalidate_python_value``). Locating
the holder is pointer arithmetic on the planned offset (stays in the type
layer, Python-free); releasing the reference needs the interpreter and
becomes ``python_ops()->retained.invalidate(holder)``, one indirect call
where today's inline body performs ``Py_DECREF`` through the limited API --
itself a call. Only the ``InvalidatePythonCache`` instantiation pays it.

Polymorphic Python source
~~~~~~~~~~~~~~~~~~~~~~~~~

``PolymorphicPythonSourceResolver::resolve`` takes ``PyRef``; the resolver
that maps a Python bundle instance to its active ``ValueTypeRef`` (today a
lambda in ``type_realization.cpp`` over ``bundle_class_info_registry``) is
``python_ops()->realized.polymorphic_source_resolver``.

Bridge contract
---------------

``include/hgraph/python/conversion.h`` (new) is the header callers include
to convert; it is the only public spelling of the former convenience
members::

    namespace hgraph::python_bridge
    {
        nb::object to_python(const ValueView &view);
        void       from_python(const ValueView &view, nb::handle source);      // was assign_from_python
        nb::object to_python(const Value &value);
        Value      value_from_python(const ValueTypeRef &binding, nb::handle source);
        bool       can_to_python_buffer(const ValueView &view) noexcept;
        nb::object to_python_buffer(const ValueView &view, const ValueArraySource &source);
        nb::object value_to_python(const TSDataView &view);
        nb::object delta_value_to_python(const TSDataView &view, DateTime evaluation_time);
        bool       from_python(TSDataMutationView &view, nb::handle source, DateTime modified_time);
        nb::object value_to_python(const TSInputView &view);
        nb::object delta_value_to_python(const TSInputView &view);
        const PythonTSDataOps &python_ts_data_ops(const TSDataOps &ops) noexcept;
    }

A missing slot raises ``std::logic_error("no Python conversion is registered
for <schema name>")`` -- the same condition today's
``ValueOps::to_python is not available for this value type`` reports, with
the schema named.

``include/hgraph/python/native_scalar_registration.h`` gains the
``python_conversion_traits<T>`` primary template (moved from
``value_ops.h``, same customisation contract: ``static nb::object
to_python(const T &)``, ``static T from_python(nb::handle)``) and::

    template <typename T> void register_python_scalar_conversion();

which registers ``nb::cast``-based slots for castable ``T`` and
trait-based slots otherwise. ``register_native_scalar_type<T>`` calls it
before ``register_scalar<T>``.

Bridge units (``src/hgraph/python/impl/``): ``python_ops.cpp`` (the table,
its load-time registration, the wrappers), ``scalar_conversions.cpp``,
``enum_conversions.cpp``, ``container_conversions.cpp`` (compact + mutable +
proxies), ``plan_conversions.cpp`` (composite, array, owned/shared entries,
closed bundle, polymorphic), ``ts_data_conversions.cpp`` (the five TS data
families; absorbs today's ``ts_data_conversion.cpp``), ``ts_input_conversions.cpp``
(TS input shapes, target links). Each includes the private detail header of
the family it converts: the implementation PRs extract those contexts from
the anonymous namespaces where they live today into
``src/hgraph/types/**/detail/<family>_detail.h``, with no behaviour change.

Python contract
---------------

None visible. The DSL, ``eval_node``, record/replay and the adaptors observe
the same values and the same errors. The only user-visible text change is
the error for an unconvertible schema, which now names the schema.

Runtime representation and dispatch
-----------------------------------

No storage layout, plan, or schema changes. Ops tables keep their size and
field order; the three ``ValueOps`` slots and the four ``TSDataOps`` fields
keep their positions, so a realised plan is bit-identical with and without
Python. Dispatch through a slot is one indirect call, as today; there is no
registry access, no lock, and no branch on kind or on Python availability
on the per-tick path (``test_registry_snapshot`` keeps pinning the
type-system lock count across evaluation).

Compatibility and migration
---------------------------

* **Type layer ABI.** ``ValueOps`` / ``TSDataOps`` / the TS input shape ops
  change field *types* (``nb::object`` → ``PyNewRef``, ``nb::handle`` →
  ``PyRef``) but not layout. A downstream native extension built against
  the previous headers must rebuild; the SDK version in
  ``hgraphConfig.cmake`` advances.
* **``python_conversion_traits`` moves.** Extensions that specialise it
  (``extensions/web``, ``kafka``, ``persistence`` enum macros; the
  ``python_extension_consumer`` test) include
  ``hgraph/python/native_scalar_registration.h`` and call
  ``register_python_scalar_conversion<T>()`` in their module initializer
  before registering operators. The in-tree extensions and the consumer
  test change in the same PR as the move.
* **Convenience members.** ``ValueView::to_python()`` and the other members
  become ``python_bridge::to_python(view)`` etc.; all callers are under
  ``python/``, ``src/hgraph/python/`` and the guarded
  ``src/hgraph/runtime/mapped_key_source.h``, which are updated in the same
  PR. Extensions that used them switch to the free functions (one-line
  edits; listed in the release note).
* **``config.h``.** The ``__JETBRAINS_IDE__`` override becomes unnecessary
  for the type layer and is removed when the ratchet reaches zero; it
  remains needed for nothing else.
* **Serialisation.** None affected.

Performance and memory
----------------------

* Per-tick: unchanged. The slot call is the same indirect call; the retained
  cache invalidation swaps an inline limited-API call for an indirect call
  of the same cost.
* Build time: one hash lookup per scalar type when its ops table is first
  constructed (once per process per ``T``); factories copy pointers.
* Memory: ``PythonOps`` is one static table in the bridge unit.
* Evidence: the ``tests/benchmarks`` operator and wiring scenarios and the
  perf guard recorded for the 0.8.15 regression
  (``perf-regression-0815``) run before and after PR 4, which moves the
  hot TS data conversions.

Installed-extension and ABI consequences
----------------------------------------

The bridge SDK headers gain ``conversion.h`` and the relocated trait; the
type-layer headers lose every nanobind include, so an extension that only
uses native types no longer needs nanobind on its include path to compile
against the Python-enabled SDK. ``tests/python_extension_consumer`` and
``tests/install_consumer`` gain the ordering check (conversion registered
before first use passes; after first use raises).

Alternatives considered
-----------------------

*Forward-declare nanobind's classes in the type layer.* ``namespace nanobind
{ class handle; class object; }`` would keep today's slot types with no
include. nanobind declares its namespace as ``nanobind
__attribute__((visibility("hidden")))`` on GCC and Clang, so a plain
re-opening differs in visibility and the contract would be tied to one
binding library's class names. Rejected; the CPython pointer is the stable
ABI both sides already agree on.

*Patch slots after registration.* Keep the tables interned and immutable
except for a mutable Python slot group the bridge writes when it learns of
a type. Removes the ordering rule but needs two mechanisms (pull for
containers, patch for scalars) and a writable slot group inside tables
documented as immutable. Rejected in favour of one pull with a loud
ordering error.

*Rewrite every conversion over erased ops only.* Would avoid private detail
headers entirely, at the cost of an indirect call per element on the Python
path and a rewrite of ~100 bodies with no behaviour change. Not required
for the boundary; a family may adopt it when its detail header would be
larger than the erased-ops rewrite (the compact set / map key adapters are
candidates), and the choice is recorded per family in the implementation
PR.

*Leave the guards.* Status quo; three type layers from one source; rejected
by the ratchet discipline.

Unresolved questions
--------------------

* Whether the stdlib should register its own enum conversions through a
  stdlib-owned hook rather than relying on the module to do it before
  ``register_standard_operators()``. The module order is enough for 0.8;
  revisit if a second Python front end appears.
* Whether ``PythonTSDataOps`` should fold into ``PythonOps::TSData`` as
  entries rather than remain a separate struct the entries point to. Kept
  separate here so the seven tables stay addressable as units.

Acceptance criteria and test plan
---------------------------------

1. ``type-layer-python-conditionals`` at 0 and ``type-layer-nanobind`` at 0;
   the counts fall monotonically across the implementation PRs and each PR
   lowers the baseline in the same change.
2. The three presets compile the same type-layer headers
   (``hgraph_header_compile_check`` in the core-only preset gains the new
   headers).
3. C++: ``set_python_ops`` after a table exists throws; a standalone
   ``python-user-nodes`` test converts a scalar, a compact list, a TSB and a
   TSD through the registered table with no ``_hgraph`` module; the
   late-registration error is pinned.
4. Python: the full local gate (core, adaptor and extension suites),
   ``test_registry_snapshot`` unchanged, the type-carrier sweep unchanged,
   the ported operator suites unchanged.
5. Consumer checks: ``python_extension_consumer``, ``install_consumer`` and
   the fabric test package pass with the relocated trait and the explicit
   registration.
6. Platforms: macOS local gate, Linux GCC 14 core-only and Python builds,
   Windows MSVC, before each merge (the GCC dangling-reference and MSVC
   template-static lessons apply: a slot group is never a cross-DLL
   template static).

Implementation plan
-------------------

Five PRs, each green on the full gate, each lowering the ratchet:

1. **Slots and wrappers** (160 → 118): ``python_object.h``,
   ``python_ops.h``, the unconditional slots in ``value_ops.h`` /
   ``ts_data/ops.h`` / ``ts_input/detail.h``, the wrappers moved to
   ``conversion.h``, scalars and enums and ``Any`` through the provider,
   the six public-header trait specialisations relocated, the ordering
   check. Every remaining guarded body adds its own nanobind include and
   adapts its signature to the opaque reference.
2. **Containers** (118 → 90): compact and mutable container conversions and
   the proxy surfaces move to ``container_conversions.cpp``.
3. **Plan factory and realisation** (90 → 64): composite, array, owned and
   shared entries, closed bundle, pooled polymorphic.
4. **TS data families** (64 → 14): atomic, slot, fixed structured, dynamic
   list, window; the authoring tables through the provider; the retained
   invalidation entry; benchmark evidence.
5. **TS input and target links** (14 → 0): shape facades and target links;
   ``type-layer-nanobind`` ratchet introduced at 0; the ``config.h``
   override removed; this RFC ``Accepted``.

Implementation status
---------------------

Proposed; no implementation has merged.

References
----------

* CPython C API, *Reference Counting* and *Limited API*: ownership
  conventions for new and borrowed references.
* nanobind ``nb_defs.h`` (``NB_NAMESPACE`` visibility attribute).
* Repository: ``python/tests/test_architecture_ratchets.py``;
  ``include/hgraph/types/metadata/value_plan_factory.h``
  (``PythonStorageProvider``); ``src/hgraph/python/impl/retained_value.cpp``
  (load-time registration precedent); memory records
  ``fixes-series-retrospective`` and ``hardening-stack-2026-09``.
