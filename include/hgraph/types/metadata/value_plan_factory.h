#ifndef HGRAPH_CPP_ROOT_VALUE_PLAN_FACTORY_H
#define HGRAPH_CPP_ROOT_VALUE_PLAN_FACTORY_H

#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value_ops.h>

#include <cstddef>
#include <cstdint>
#include <mutex>

#include <hgraph/types/utils/counted_mutex.h>
#include <span>
#include <unordered_map>

namespace hgraph
{
    /**
     * Wiring-time request for the physical representation of an output value.
     *
     * The declared value schema is unchanged.  The factory may conservatively
     * downgrade either Python-aware request to ``Native`` when a schema cannot
     * preserve its Python boundary semantics without canonical C++ storage.
     */
    enum class ValueStorageVariant : std::uint8_t
    {
        Native,
        NativeWithPythonCache,
        PythonOnly,
    };

    struct HGRAPH_CLASS_EXPORT ValueStorageSelection
    {
        static constexpr std::size_t no_offset = static_cast<std::size_t>(-1);

        ValueStorageVariant effective{ValueStorageVariant::Native};
        ValueTypeRef value_binding{};
        const MemoryUtils::StoragePlan *storage_plan{nullptr};
        std::size_t value_offset{0};
        std::size_t python_value_offset{no_offset};

        [[nodiscard]] bool has_python_value() const noexcept
        {
            return python_value_offset != no_offset;
        }
    };

    /**
     * Factory that maps a value-layer ``ValueTypeMetaData`` schema to its
     * canonical ``MemoryUtils::StoragePlan`` and default
     * ``ValueTypeRef``.
     *
     * The schema describes *what* a value is; the plan describes *how* it
     * is laid out in memory. The factory bridges the two so callers never
     * have to reach into ``MemoryUtils`` directly when all they have is a
     * schema pointer. Results are cached against the schema, so repeated
     * lookups are O(1) and pointer-stable for the lifetime of the
     * registry.
     *
     * **Atomic kinds.** Atomic schemas have no intrinsic layout
     * information on the schema side; the canonical plan must be supplied
     * by the registry at registration time via ``register_atomic``. The
     * intended path is ``TypeRegistry::register_scalar<T>``, which passes
     * ``&MemoryUtils::plan_for<T>()`` through to the factory.
     *
     * **Composite kinds.** Tuple, bundle, and fixed-size list plans are
     * synthesised on first use by recursively resolving component
     * schemas and feeding the resulting plans into
     * ``MemoryUtils::tuple_plan`` / ``named_tuple_plan`` / ``array_plan``.
     * Because those builders intern their results, the factory's cache
     * lines up with the global plan cache.
     *
     * **Container kinds (Set, Map, CyclicBuffer, Queue, dynamic List).**
     * These use the compact value-layer storage shapes by default. Their
     * plans are resolved from the canonical bindings of their element/key
     * schemas so the storage lifecycle hooks and view ops agree on the
     * child layout.
     *
     * The factory is a process-wide singleton via ``instance()``;
     * non-copyable and non-movable.
     */
    class HGRAPH_CLASS_EXPORT ValuePlanFactory
    {
      public:
        /** Singleton accessor; returns a reference to the process-wide factory. */
        static ValuePlanFactory &instance();

        ValuePlanFactory(const ValuePlanFactory &)            = delete;
        ValuePlanFactory &operator=(const ValuePlanFactory &) = delete;
        ValuePlanFactory(ValuePlanFactory &&)                 = delete;
        ValuePlanFactory &operator=(ValuePlanFactory &&)      = delete;

        /**
         * Pair an atomic schema with its canonical storage plan.
         *
         * Called by ``TypeRegistry::register_scalar`` so that
         * ``plan_for(atomic_schema)`` can return the matching
         * ``MemoryUtils::plan_for<T>()`` afterwards. Re-registration with
         * the same plan is a no-op; re-registration with a different plan
         * throws.
         */
        void register_atomic(const ValueTypeMetaData *schema,
                             const MemoryUtils::StoragePlan *plan,
                             const ValueOps *ops);
        void register_atomic(const ValueTypeMetaData *schema,
                             const MemoryUtils::StoragePlan *plan,
                             const ValueOps *ops,
                             DebugAtomicKind atomic_kind);

        /**
         * Register the canonical binding for ``binding.type_meta``.
         *
         * ``TypeRegistry::register_scalar<T>`` calls this for atomics after
         * interning the ``(schema, plan_for<T>, ops_for<T>)`` triple. The
         * factory synthesises composite/container bindings lazily; callers
         * normally do not register those by hand.
         */
        void register_type(ValueTypeRef type);

        /**
         * Look up or synthesise the canonical plan for ``schema``.
         *
         * Returns ``nullptr`` when ``schema`` is null. For atomic schemas
         * not previously registered, throws. For tuple / bundle / fixed
         * list schemas, recursively resolves component plans and
         * synthesises via ``MemoryUtils`` builders. Dynamic list and
         * container schemas use the compact value-layer storage shapes.
         */
        const MemoryUtils::StoragePlan *plan_for(const ValueTypeMetaData *schema);

        /** Look up only; never synthesises. Returns ``nullptr`` when missing. */
        const MemoryUtils::StoragePlan *find(const ValueTypeMetaData *schema) const;

        /**
         * Look up or synthesise the canonical default binding for ``schema``.
         *
         * Atomic bindings must have been registered by ``TypeRegistry``.
         * Composite and container bindings are created lazily from child
         * bindings and interned through ``ValueTypeRef``.
         */
        [[nodiscard]] ValueTypeRef type_for(const ValueTypeMetaData *schema);

        /**
         * Select output-local value storage for ``native_binding``.
         *
         * ``NativeWithPythonCache`` keeps the canonical C++ value and an
         * optional retained Python object in the same planned allocation.
         * ``PythonOnly`` retains the Python object as the value itself.  The
         * latter is only a representation request: wiring must first prove
         * that no native reader or graph boundary can observe the output.
         */
        [[nodiscard]] ValueStorageSelection storage_for(
            ValueTypeRef native_binding,
            ValueStorageVariant requested);

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        /**
         * Validate and, where the canonical Python read shape requires it,
         * normalize an object before it is retained in Python-aware storage.
         */
        [[nodiscard]] nb::object prepare_python_storage_value(
            const ValueTypeMetaData *schema,
            nb::handle source) const;
#endif

        /**
         * Build a graph-realized Tuple/Bundle binding from explicit field
         * bindings. The schema stays canonical; only its storage plan and
         * indexed child ops differ.
         */
        [[nodiscard]] ValueTypeRef realized_composite_type_for(
            const ValueTypeMetaData *schema,
            std::span<const ValueTypeRef> field_bindings);

        /**
         * Build a graph-realized fixed List binding from an explicit element
         * binding. The schema and static length stay canonical while the
         * element stride follows the realized storage representation.
         */
        [[nodiscard]] ValueTypeRef realized_fixed_list_type_for(
            const ValueTypeMetaData *schema,
            ValueTypeRef element_binding);

        /**
         * Build a structural Tuple/Bundle projection from explicit field
         * bindings while retaining ``schema`` as the binding's nominal
         * identity. Unlike ``realized_composite_type_for``, this accepts
         * Owned schemas whose canonical storage is non-composite.
         */
        [[nodiscard]] ValueTypeRef projected_composite_type_for(
            const ValueTypeMetaData *schema,
            std::span<const ValueTypeRef> field_bindings);

        /** Binding lookup only; never synthesises. Returns ``nullptr`` when missing. */
        [[nodiscard]] ValueTypeRef find_type(const ValueTypeMetaData *schema) const;

        /**
         * Drop every cached schema → plan mapping. Test-only helper used to
         * isolate tests from each other; production code must not call it.
         */
        void reset() noexcept;

      private:
        ValuePlanFactory() = default;

        const MemoryUtils::StoragePlan *synthesise(const ValueTypeMetaData *schema);
        ValueTypeRef synthesise_type(const ValueTypeMetaData *schema);

        mutable TypeSystemMutex                                                         mutex_;
        std::unordered_map<const ValueTypeMetaData *, const MemoryUtils::StoragePlan *> cache_;
        std::unordered_map<const ValueTypeMetaData *, ValueTypeRef>                  type_cache_;
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_PLAN_FACTORY_H
