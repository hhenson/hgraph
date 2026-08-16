#ifndef HGRAPH_CPP_ROOT_VALUE_VIEW_H
#define HGRAPH_CPP_ROOT_VALUE_VIEW_H

#include <hgraph/types/value/value_ops.h>
#include <cassert>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace hgraph
{
    namespace detail
    {
        struct ValueVisitorAccess;

        /**
         * Construction token used after value visitation has already checked
         * the semantic value kind.
         */
        class TrustedValueKind
        {
            constexpr TrustedValueKind() noexcept = default;
            friend struct ValueVisitorAccess;
        };
    }  // namespace detail

    class IndexedValueView;
    class MutableIndexedValueView;
    class AtomicView;
    class TupleView;
    class BundleView;
    class ListView;
    class SetView;
    class MutableSetView;
    class MapView;
    class CyclicBufferView;
    class QueueView;
    class MutableTupleView;
    class MutableBundleView;
    class MutableListView;
    class MutableCyclicBufferView;
    class MutableQueueView;
    class MutableMapView;
    class AnyView;
    class MutableAnyView;
    class Value;

    /**
     * Non-owning two-word reference to a value.
     *
     * The view carries a typed value pointer containing the canonical
     * ``TypeRecord`` and a pointer to the underlying memory. Constructing a view does not copy
     * the value; destroying it does not destroy the value. Use ``Value``
     * for owning storage.
     *
     * The first slice exposes the kind-agnostic surface used by every
     * value layer kind: ``hash``, ``equals``, ``compare``, ``to_string``
     * via the bound ops, plus typed scalar access through ``as<T>`` /
     * ``try_as<T>`` / ``checked_as<T>``. Kind-specialised casts such as
     * ``as_list()`` and ``as_map()`` are declared here and defined once
     * the specialised view classes are complete.
     *
     * A view created from ``void *`` storage is writable-capable but not
     * mutable. Mutating methods require an explicit ``begin_mutation()``
     * transition, and that transition is allowed only when the bound ops
     * table opts in.
     */
    class HGRAPH_EXPORT ValueView
    {
      public:
        constexpr ValueView() noexcept = default;

        ValueView(ValueTypeRef binding, void *data) noexcept
            : pointer_{data != nullptr ? binding.writable(data) : binding.typed_null()} {}

        ValueView(ValueTypeRef binding, const void *data) noexcept
            : pointer_{data != nullptr ? binding.read_only(data) : binding.typed_null()} {}

        ValueView(ValueTypeRef binding, std::nullptr_t) noexcept
            : pointer_{binding.typed_null()} {}

        ValueView(const ValueView &) = delete;
        ValueView &operator=(const ValueView &) = delete;
        ValueView(ValueView &&) noexcept = default;
        ValueView &operator=(ValueView &&) noexcept = default;

        /** True when both the trusted type record and the data pointer are non-null. */
        [[nodiscard]] bool valid() const noexcept { return pointer_.record() != nullptr && pointer_.data() != nullptr; }

        explicit operator bool() const noexcept { return valid(); }

        /** True when the view carries a binding/schema, even if the payload is currently absent. */
        [[nodiscard]] bool bound() const noexcept { return pointer_.record() != nullptr; }
        /** True when the view references a live payload. */
        [[nodiscard]] bool has_value() const noexcept { return pointer_.data() != nullptr; }
        /** True when the view may write through the payload pointer. */
        [[nodiscard]] bool mutable_payload() const noexcept
        {
            return valid() && pointer_.access_mode() == AccessMode::Mutation;
        }
        /** True when the view was built from writable storage, even if mutation is not open. */
        [[nodiscard]] bool writable_payload() const noexcept
        {
            const auto access = pointer_.access_mode();
            return valid() && (access == AccessMode::Writable || access == AccessMode::Mutation);
        }

        [[nodiscard]] ValueTypeRef type() const noexcept { return ValueTypeRef{pointer_.record()}; }
        [[nodiscard]] ValueTypeRef binding() const noexcept { return type(); }
        [[nodiscard]] const TypeRecord *record() const noexcept { return pointer_.record(); }
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            return type().schema();
        }
        [[nodiscard]] const void *data() const noexcept { return pointer_.data(); }
        /**
         * Project a polymorphic storage wrapper to its active concrete value.
         * Exact bindings return an equivalent view over the same memory.
         */
        [[nodiscard]] ValueView concrete() const
        {
            if (!valid()) { return ValueView{}; }
            const auto declared = binding();
            const auto concrete_type = declared.ops_ref().concrete_type(declared, data());
            if (writable_payload())
            {
                return ValueView{concrete_type,
                                 declared.ops_ref().writable_concrete_memory(
                                     const_cast<void *>(data()))};
            }
            return ValueView{concrete_type, declared.ops_ref().concrete_memory(data())};
        }
        [[nodiscard]] void *mutable_data() const
        {
            if (!valid()) { throw std::logic_error("ValueView::mutable_data on invalid view"); }
            if (!pointer_.mutation_access())
            {
                throw std::logic_error("ValueView::mutable_data requires begin_mutation");
            }
            return pointer_.mutable_data();
        }

        [[nodiscard]] bool can_begin_mutation() const noexcept
        {
            return writable_payload() && type().ops() != nullptr && type().ops()->can_begin_mutation();
        }

        [[nodiscard]] ValueView begin_mutation() const
        {
            if (!valid()) { throw std::logic_error("ValueView::begin_mutation on invalid view"); }
            if (!writable_payload())
            {
                throw std::logic_error("ValueView::begin_mutation requires writable storage");
            }
            if (type().ops() == nullptr || !type().ops()->can_begin_mutation())
            {
                throw std::logic_error("ValueView::begin_mutation is not supported by this value ops");
            }
            return ValueView{pointer_.begin_mutation(), TrustedPointer{}};
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        /**
         * Assign this view's storage FROM a python object through the
         * binding's ``from_python`` op (the type-erased conversion entry).
         * A LIFECYCLE-level write like ``copy_assign``: it requires writable
         * storage but not the mutation protocol - compact (immutable-API)
         * containers construct through it too.
         */
        void assign_from_python(nanobind::handle source) const
        {
            if (!valid()) { throw std::logic_error("ValueView::assign_from_python on invalid view"); }
            if (!writable_payload())
            {
                throw std::logic_error("ValueView::assign_from_python requires writable storage");
            }
            const auto bound = type();
            bound.ops_ref().from_python(bound, const_cast<void *>(data()), source);
        }
#endif

        // -- kind queries --
        [[nodiscard]] bool is_atomic() const noexcept
        {
            return valid() && schema()->try_value_kind() == ValueTypeKind::Atomic;
        }
        [[nodiscard]] bool is_tuple()  const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::Tuple; }
        [[nodiscard]] bool is_bundle() const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::Bundle; }
        [[nodiscard]] bool is_list()   const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::List; }
        [[nodiscard]] bool is_set()    const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::Set; }
        [[nodiscard]] bool is_map()    const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::Map; }
        [[nodiscard]] bool is_cyclic_buffer() const noexcept
        {
            return valid() && schema()->try_value_kind() == ValueTypeKind::CyclicBuffer;
        }
        [[nodiscard]] bool is_queue() const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::Queue; }
        [[nodiscard]] bool is_any()   const noexcept { return valid() && schema()->try_value_kind() == ValueTypeKind::Any; }
        [[nodiscard]] bool is_indexed() const noexcept
        {
            const auto *type = schema();
            if (!valid() || type == nullptr) { return false; }
            const auto kind = type->try_value_kind();
            if (!kind.has_value()) { return false; }
            switch (*kind)
            {
                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                case ValueTypeKind::List:
                case ValueTypeKind::Set:
                case ValueTypeKind::Map:
                case ValueTypeKind::CyclicBuffer:
                case ValueTypeKind::Queue:
                    return true;
                case ValueTypeKind::Atomic:
                case ValueTypeKind::Any:
                    return false;
            }
            return false;
        }
        [[nodiscard]] bool is_type(const ValueTypeMetaData *type) const noexcept { return schema() == type; }

        /**
         * True when the bound ops vtable is the canonical ``ops_for<T>``
         * for some scalar ``T``. Pointer comparison is sufficient because
         * ``ops_for<T>`` returns a stable function-local-static.
         */
        template <typename T>
        [[nodiscard]] bool holds_alternative() const noexcept
        {
            return is_atomic() && type().ops() == &ops_for<T>();
        }

        template <typename T>
        [[nodiscard]] bool is_scalar_type() const noexcept
        {
            return holds_alternative<T>();
        }

        // -- typed atomic access (debug-asserted) --
        template <typename T>
        [[nodiscard]] const T &as() const noexcept
        {
            assert(valid() && "as<T>() on invalid ValueView");
            assert(holds_alternative<T>() && "as<T>() type mismatch");
            return *static_cast<const T *>(data());
        }
        template <typename T>
        [[nodiscard]] T &as()
        {
            return checked_mutable_as<T>();
        }

        /** ``try_as<T>`` returns ``nullptr`` on invalid view, non-atomic kind, or type mismatch. */
        template <typename T>
        [[nodiscard]] const T *try_as() const noexcept
        {
            return holds_alternative<T>() ? static_cast<const T *>(data()) : nullptr;
        }

        /** Mutable variant of ``try_as<T>``; returns ``nullptr`` for read-only views. */
        template <typename T>
        [[nodiscard]] T *try_mutable_as() noexcept
        {
            return holds_alternative<T>() && mutable_payload() ? static_cast<T *>(const_cast<void *>(data()))
                                                               : nullptr;
        }

        /** ``checked_as<T>`` throws when the view is invalid, non-atomic, or T doesn't match. */
        template <typename T>
        [[nodiscard]] const T &checked_as() const
        {
            if (!valid()) { throw std::logic_error("checked_as<T> on invalid ValueView"); }
            if (!is_atomic()) { throw std::logic_error("checked_as<T> on non-atomic ValueView"); }
            if (!holds_alternative<T>()) { throw std::logic_error("checked_as<T> type mismatch"); }
            return *static_cast<const T *>(data());
        }
        template <typename T>
        [[nodiscard]] T &checked_mutable_as()
        {
            if (!valid()) { throw std::logic_error("checked_mutable_as<T> on invalid ValueView"); }
            if (!mutable_payload()) { throw std::logic_error("checked_mutable_as<T> requires begin_mutation"); }
            if (!is_atomic()) { throw std::logic_error("checked_mutable_as<T> on non-atomic ValueView"); }
            if (!holds_alternative<T>()) { throw std::logic_error("checked_mutable_as<T> type mismatch"); }
            return *static_cast<T *>(const_cast<void *>(data()));
        }

        template <typename T>
        void set(T &&value)
        {
            set_scalar(std::forward<T>(value));
        }

        template <typename T>
        void set_scalar(T &&value)
        {
            using value_type = std::remove_cvref_t<T>;
            if (!valid()) { throw std::logic_error("set_scalar<T> on invalid ValueView"); }
            if (!mutable_payload()) { throw std::logic_error("set_scalar<T> requires begin_mutation"); }
            if (!is_atomic()) { throw std::logic_error("set_scalar<T> on non-atomic ValueView"); }
            if (!holds_alternative<value_type>()) { throw std::logic_error("set_scalar<T> type mismatch"); }
            *static_cast<value_type *>(const_cast<void *>(data())) = std::forward<T>(value);
        }

        // -- kind-specialised view casts (definitions in specialised_views.h) --
        [[nodiscard]] AtomicView as_atomic() const;
        [[nodiscard]] std::optional<AtomicView> try_as_atomic() const;
        [[nodiscard]] IndexedValueView as_indexed_view() const;
        [[nodiscard]] std::optional<IndexedValueView> try_as_indexed_view() const;
        [[nodiscard]] IndexedValueView as_indexed() const;
        [[nodiscard]] std::optional<IndexedValueView> try_as_indexed() const;
        [[nodiscard]] TupleView as_tuple() const;
        [[nodiscard]] std::optional<TupleView> try_as_tuple() const;
        [[nodiscard]] BundleView as_bundle() const;
        [[nodiscard]] std::optional<BundleView> try_as_bundle() const;
        [[nodiscard]] ListView as_list() const;
        [[nodiscard]] std::optional<ListView> try_as_list() const;
        [[nodiscard]] SetView as_set() const;
        [[nodiscard]] std::optional<SetView> try_as_set() const;
        [[nodiscard]] MutableSetView as_mutable_set() const;
        [[nodiscard]] std::optional<MutableSetView> try_as_mutable_set() const;
        [[nodiscard]] MapView as_map() const;
        [[nodiscard]] std::optional<MapView> try_as_map() const;
        [[nodiscard]] CyclicBufferView as_cyclic_buffer() const;
        [[nodiscard]] std::optional<CyclicBufferView> try_as_cyclic_buffer() const;
        [[nodiscard]] QueueView as_queue() const;
        [[nodiscard]] std::optional<QueueView> try_as_queue() const;
        [[nodiscard]] MutableMapView as_mutable_map() const;
        [[nodiscard]] std::optional<MutableMapView> try_as_mutable_map() const;
        [[nodiscard]] AnyView as_any() const;
        [[nodiscard]] std::optional<AnyView> try_as_any() const;
        [[nodiscard]] MutableAnyView as_mutable_any() const;
        [[nodiscard]] std::optional<MutableAnyView> try_as_mutable_any() const;
        [[nodiscard]] MutableTupleView as_mutable_tuple() const;
        [[nodiscard]] std::optional<MutableTupleView> try_as_mutable_tuple() const;
        [[nodiscard]] MutableBundleView as_mutable_bundle() const;
        [[nodiscard]] std::optional<MutableBundleView> try_as_mutable_bundle() const;
        [[nodiscard]] MutableListView as_mutable_list() const;
        [[nodiscard]] std::optional<MutableListView> try_as_mutable_list() const;
        [[nodiscard]] MutableCyclicBufferView as_mutable_cyclic_buffer() const;
        [[nodiscard]] std::optional<MutableCyclicBufferView> try_as_mutable_cyclic_buffer() const;
        [[nodiscard]] MutableQueueView as_mutable_queue() const;
        [[nodiscard]] std::optional<MutableQueueView> try_as_mutable_queue() const;
        [[nodiscard]] MutableIndexedValueView as_mutable_indexed_view() const;
        [[nodiscard]] std::optional<MutableIndexedValueView> try_as_mutable_indexed_view() const;
        [[nodiscard]] MutableIndexedValueView as_mutable_indexed() const;
        [[nodiscard]] std::optional<MutableIndexedValueView> try_as_mutable_indexed() const;

        // -- generic ops.
        [[nodiscard]] std::size_t hash() const
        {
            if (!valid()) { throw std::logic_error("ValueView::hash requires a non-empty view"); }
            return type().ops_ref().hash(data());
        }
        [[nodiscard]] bool equals(const ValueView &other) const;
        [[nodiscard]] std::partial_ordering compare(const ValueView &other) const noexcept;
        [[nodiscard]] bool operator==(const ValueView &other) const { return equals(other); }
        [[nodiscard]] std::partial_ordering operator<=>(const ValueView &other) const noexcept
        {
            return compare(other);
        }

        [[nodiscard]] Value clone() const;
        void copy_from(const ValueView &other);
        [[nodiscard]] bool try_copy_from(const ValueView &other);

        [[nodiscard]] std::string to_string() const
        {
            if (!valid()) { return std::string{}; }
            return type().ops_ref().to_string(data());
        }

        [[nodiscard]] std::string format_string() const
        {
            if (!valid()) { return std::string{}; }
            return type().ops_ref().format_string(data());
        }

        /** Heap storage exclusively owned by the referenced payload. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return valid() ? type().ops_ref().dynamic_storage_metrics(data())
                           : DynamicStorageMetrics{};
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object to_python() const;
        void from_python(nb::handle source);
#endif

      private:
        friend struct detail::ValueVisitorAccess;

        struct TrustedPointer
        {};

        [[nodiscard]] ValueView borrowed_ref() const noexcept
        {
            ValueView result;
            result.pointer_ = pointer_;
            return result;
        }

        explicit ValueView(ValuePtr pointer, TrustedPointer) noexcept : pointer_{pointer} {}

        ValuePtr pointer_{};
    };

    static_assert(sizeof(ValueView) == sizeof(void *) * 2, "ValueView must remain a two-word handle");
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_VIEW_H
