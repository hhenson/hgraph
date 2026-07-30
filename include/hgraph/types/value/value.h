// types/value/value.h — the value layer's owning type: Value owns storage
// described by an interned StoragePlan and constructed in place (arena
// principle: plans hold offsets, not pointers); ValueView is the borrowing,
// move-only two-word cursor over (memory, ops). This layer is 100% fn-ptr ops
// tables — no virtuals. Vocabulary + design record:
// docs/source/developer_guide/data_structures/core_concepts.rst.
#ifndef HGRAPH_CPP_ROOT_VALUE_H
#define HGRAPH_CPP_ROOT_VALUE_H

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_ops.h>
#include <hgraph/types/value/value_view.h>

#include <compare>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{
    /**
     * Owning value-layer instance.
     *
     * ``Value`` is a thin wrapper over ``MemoryUtils::ErasedOwner``
     * specialised on the value-layer ``ValueTypeRef``. The storage
     * owner holds the bytes (inline or heap, deciding via the policy) and
     * retains the binding identity. The binding carries the schema, plan,
     * and ops, and ``Value`` adds the
     * typed convenience surface: scalar construction, ``ValueView`` lift,
     * and ``hash`` / ``equals`` / ``compare`` / ``to_string`` dispatch
     * through the lifted ``ValueView``.
     *
     * A ``Value`` may also be typed-null: ``storage_`` preserves the
     * schema/binding while its payload is empty. ``view()`` is valid only
     * when storage is populated.
     */
    class Value
    {
      public:
        using storage_type = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        Value() noexcept = default;

        /**
         * Construct a typed-null ``Value`` for ``schema``. The canonical
         * binding is resolved and retained, but no payload is constructed
         * until the caller assigns or constructs from a binding/source.
         */
        explicit Value(const ValueTypeMetaData &schema)
        {
            const auto type = ValuePlanFactory::instance().type_for(&schema);
            if (!type) { throw std::logic_error("Value(schema): schema has no canonical value type"); }
            storage_ = storage_type::empty(*type.record());
        }

        /**
         * Construct a default-valued ``Value`` for the given binding (the
         * binding's plan default-constructs the payload).
         */
        explicit Value(const ValueTypeRef &binding)
            : storage_(*binding.record())
        {
        }

        /**
         * Copy-construct a payload from external storage using ``binding``.
         */
        Value(const ValueTypeRef &binding, const void *src)
            : storage_(storage_type::owning_copy(*binding.record(), src))
        {
        }

        /** Copy or bind-null construct from a non-owning view. */
        explicit Value(const ValueView &view)
        {
            const auto view_type = view.type();
            if (!view_type)
            {
                throw std::invalid_argument("Value(ValueView): view has no binding");
            }
            const auto &ops = view_type.ops_ref();
            const auto owning_type = ops.owning_type(view_type);
            if (view.data() == nullptr)
            {
                storage_ = storage_type::empty(*owning_type.record());
                return;
            }

            storage_ = storage_type::owning_constructed(*owning_type.record(), [&](void *dst) {
                ops.copy_construct_view(owning_type, dst, view.data());
            });
        }

        /**
         * Construct an owning value for an atomic ``T`` whose schema has
         * been registered through ``TypeRegistry::register_scalar<T>``.
         * The matching binding is looked up; the erased owner drives
         * allocation and construction.
         */
        template <typename T,
                  typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value> &&
                                              !std::is_same_v<std::decay_t<T>, ValueTypeRef>>>
        explicit Value(T value)
        {
            ValueTypeRef binding = TypeRegistry::instance().scalar_type<T>();
            if (!binding)
            {
                throw std::logic_error(
                    "Value(T): scalar type not registered — call register_scalar<T>(name) first");
            }
            storage_ = storage_type(*binding.record());
            // The owner has default-constructed the payload; assign the
            // caller-supplied value through the registered lifecycle table.
            // Besides keeping scalar construction on the erased contract,
            // this lets the owner select inline or heap storage without a
            // caller-side typed cast into its inline buffer.
            binding.move_assign_at(storage_.data(), std::addressof(value));
        }

        // Copy and move are inherited from ``ErasedOwner`` — copy
        // performs a deep allocation + copy-construct via the binding's
        // plan; move transfers ownership.
        Value(const Value &) = default;
        Value &operator=(const Value &) = default;
        Value(Value &&) noexcept = default;
        Value &operator=(Value &&) noexcept = default;
        ~Value() = default;

        /** True when storage is allocated and a value has been constructed in it. */
        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }

        /** The bound schema; ``nullptr`` for a default-constructed ``Value``. */
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            return binding().schema();
        }
        [[nodiscard]] ValueTypeRef type() const noexcept { return ValueTypeRef{storage_.binding()}; }
        [[nodiscard]] ValueTypeRef binding() const noexcept { return type(); }

        /** Tear down the constructed value (if any), preserving the binding/schema. */
        void reset() noexcept { storage_.reset_payload(); }

        // -- view access --
        [[nodiscard]] ValueView view()
        {
            return ValueView{binding(), has_value() ? storage_.data() : nullptr};
        }
        [[nodiscard]] ValueView view() const
        {
            return ValueView{binding(), has_value() ? storage_.data() : nullptr};
        }
        [[nodiscard]] ValueView begin_mutation()
        {
            return view().begin_mutation();
        }

        // -- atomic access shortcuts --
        [[nodiscard]] AtomicView as_atomic() const { return view().as_atomic(); }
        [[nodiscard]] AtomicView as_atomic() { return view().as_atomic(); }
        [[nodiscard]] std::optional<AtomicView> try_as_atomic() const { return view().try_as_atomic(); }
        [[nodiscard]] std::optional<AtomicView> try_as_atomic() { return view().try_as_atomic(); }

        template <typename T>
        [[nodiscard]] const T &as() const
        {
            return view().template checked_as<T>();
        }
        template <typename T>
        [[nodiscard]] const T &as()
        {
            return view().template checked_as<T>();
        }
        template <typename T>
        [[nodiscard]] const T *try_as() const noexcept
        {
            const ValueView v = view();
            return v.template try_as<T>();
        }
        template <typename T>
        [[nodiscard]] const T *try_as() noexcept
        {
            const ValueView v = view();
            return v.template try_as<T>();
        }

        [[nodiscard]] IndexedValueView as_indexed_view() const { return view().as_indexed_view(); }
        [[nodiscard]] IndexedValueView as_indexed_view() { return view().as_indexed_view(); }
        [[nodiscard]] std::optional<IndexedValueView> try_as_indexed_view() const
        {
            return view().try_as_indexed_view();
        }
        [[nodiscard]] std::optional<IndexedValueView> try_as_indexed_view()
        {
            return view().try_as_indexed_view();
        }
        [[nodiscard]] IndexedValueView as_indexed() const { return as_indexed_view(); }
        [[nodiscard]] IndexedValueView as_indexed() { return as_indexed_view(); }
        [[nodiscard]] std::optional<IndexedValueView> try_as_indexed() const { return try_as_indexed_view(); }
        [[nodiscard]] std::optional<IndexedValueView> try_as_indexed() { return try_as_indexed_view(); }
        [[nodiscard]] IndexedValueView indexed_view() const { return as_indexed_view(); }
        [[nodiscard]] IndexedValueView indexed_view() { return as_indexed_view(); }

        [[nodiscard]] TupleView as_tuple() const { return view().as_tuple(); }
        [[nodiscard]] TupleView as_tuple() { return view().as_tuple(); }
        [[nodiscard]] std::optional<TupleView> try_as_tuple() const { return view().try_as_tuple(); }
        [[nodiscard]] std::optional<TupleView> try_as_tuple() { return view().try_as_tuple(); }
        [[nodiscard]] BundleView as_bundle() const { return view().as_bundle(); }
        [[nodiscard]] BundleView as_bundle() { return view().as_bundle(); }
        [[nodiscard]] std::optional<BundleView> try_as_bundle() const { return view().try_as_bundle(); }
        [[nodiscard]] std::optional<BundleView> try_as_bundle() { return view().try_as_bundle(); }
        [[nodiscard]] ListView as_list() const { return view().as_list(); }
        [[nodiscard]] ListView as_list() { return view().as_list(); }
        [[nodiscard]] std::optional<ListView> try_as_list() const { return view().try_as_list(); }
        [[nodiscard]] std::optional<ListView> try_as_list() { return view().try_as_list(); }
        [[nodiscard]] SetView as_set() const { return view().as_set(); }
        [[nodiscard]] SetView as_set() { return view().as_set(); }
        [[nodiscard]] std::optional<SetView> try_as_set() const { return view().try_as_set(); }
        [[nodiscard]] std::optional<SetView> try_as_set() { return view().try_as_set(); }
        [[nodiscard]] MapView as_map() const { return view().as_map(); }
        [[nodiscard]] MapView as_map() { return view().as_map(); }
        [[nodiscard]] std::optional<MapView> try_as_map() const { return view().try_as_map(); }
        [[nodiscard]] std::optional<MapView> try_as_map() { return view().try_as_map(); }
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() const { return view().as_cyclic_buffer(); }
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() { return view().as_cyclic_buffer(); }
        [[nodiscard]] std::optional<CyclicBufferView> try_as_cyclic_buffer() const
        {
            return view().try_as_cyclic_buffer();
        }
        [[nodiscard]] std::optional<CyclicBufferView> try_as_cyclic_buffer()
        {
            return view().try_as_cyclic_buffer();
        }
        [[nodiscard]] QueueView as_queue() const { return view().as_queue(); }
        [[nodiscard]] QueueView as_queue() { return view().as_queue(); }
        [[nodiscard]] std::optional<QueueView> try_as_queue() const { return view().try_as_queue(); }
        [[nodiscard]] std::optional<QueueView> try_as_queue() { return view().try_as_queue(); }
        [[nodiscard]] AnyView as_any() const { return view().as_any(); }
        [[nodiscard]] AnyView as_any() { return view().as_any(); }
        [[nodiscard]] std::optional<AnyView> try_as_any() const { return view().try_as_any(); }
        [[nodiscard]] std::optional<AnyView> try_as_any() { return view().try_as_any(); }

        [[nodiscard]] TupleView tuple_view() const { return as_tuple(); }
        [[nodiscard]] TupleView tuple_view() { return as_tuple(); }
        [[nodiscard]] BundleView bundle_view() const { return as_bundle(); }
        [[nodiscard]] BundleView bundle_view() { return as_bundle(); }
        [[nodiscard]] ListView list_view() const { return as_list(); }
        [[nodiscard]] ListView list_view() { return as_list(); }
        [[nodiscard]] SetView set_view() const { return as_set(); }
        [[nodiscard]] SetView set_view() { return as_set(); }
        [[nodiscard]] MapView map_view() const { return as_map(); }
        [[nodiscard]] MapView map_view() { return as_map(); }
        [[nodiscard]] CyclicBufferView cyclic_buffer_view() const { return as_cyclic_buffer(); }
        [[nodiscard]] CyclicBufferView cyclic_buffer_view() { return as_cyclic_buffer(); }
        [[nodiscard]] QueueView queue_view() const { return as_queue(); }
        [[nodiscard]] QueueView queue_view() { return as_queue(); }

        // -- generic ops --
        [[nodiscard]] std::size_t hash() const { return view().hash(); }
        [[nodiscard]] bool equals(const Value &other) const
        {
            return view().equals(other.view());
        }
        [[nodiscard]] bool equals(const ValueView &other) const
        {
            return view().equals(other);
        }
        [[nodiscard]] std::partial_ordering compare(const Value &other) const noexcept
        {
            return view().compare(other.view());
        }
        [[nodiscard]] std::partial_ordering compare(const ValueView &other) const noexcept
        {
            return view().compare(other);
        }
        [[nodiscard]] bool operator==(const Value &other) const { return equals(other); }
        [[nodiscard]] std::partial_ordering operator<=>(const Value &other) const noexcept
        {
            return compare(other);
        }
        [[nodiscard]] Value clone() const { return binding() ? Value{view()} : Value{}; }

        /**
         * Replace this owning value with an owning copy of ``source``.
         *
         * This is the explicit assignment form of ``Value{source}``: it
         * clones the source payload when present, or preserves the source
         * binding as a typed-null value when the source has no payload.
         */
        Value &assign_from(const ValueView &source)
        {
            *this = Value{source};
            return *this;
        }

        [[nodiscard]] std::string to_string() const { return view().to_string(); }
        [[nodiscard]] std::string format_string() const { return view().format_string(); }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object to_python() const;
        void from_python(nb::handle source);
#endif

      private:
        storage_type storage_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_H
