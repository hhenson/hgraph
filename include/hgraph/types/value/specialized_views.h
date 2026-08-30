#ifndef HGRAPH_CPP_ROOT_VALUE_SPECIALIZED_VIEWS_H
#define HGRAPH_CPP_ROOT_VALUE_SPECIALIZED_VIEWS_H

#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/value_range.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace hgraph
{
    /**
     * Specialised read-only views over value-layer structured kinds.
     * See *Erased Types > Specialised Views* for the design narrative.
     *
     * Views never cast the data pointer to a concrete C++ storage type.
     * Read access goes through the bound kind-specific ops sub-class
     * (``ListValueOps``, ``SetValueOps``, ``MapValueOps``,
     * ``CyclicBufferValueOps``, ``QueueValueOps``). Tuple, bundle, and
     * fixed-size list views use factory-installed indexed ops backed by
     * structured ``StoragePlan`` offsets. When slot-store-backed
     * time-series variants register their own ops, the same view code
     * works unchanged.
     */

    namespace specialized_view_detail
    {
        inline void require_valid(bool valid, const char *what)
        {
            if (!valid) { throw std::logic_error(std::string{what} + " on invalid view"); }
        }

        template <typename Hook>
        [[nodiscard]] Hook require_mutation_hook(Hook hook, const char *view, const char *operation,
                                                 const char *hook_name)
        {
            if (hook == nullptr)
            {
                throw std::logic_error(std::string{view} + "::" + operation + ": binding has no " + hook_name +
                                       " mutation hook");
            }
            return hook;
        }

        inline void require_indexed_ops(const IndexedValueOps *ops, const char *what)
        {
            if (ops == nullptr || ops->size == nullptr || ops->element_at == nullptr ||
                ops->element_binding == nullptr || ops->make_range == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose indexed ops");
            }
        }

        [[nodiscard]] inline const IndexedValueOps *checked_indexed_ops(ValueTypeRef binding,
                                                                        const char *what)
        {
            const auto *ops = checked_value_ops<IndexedValueOps>(binding, what);
            require_indexed_ops(ops, what);
            return ops;
        }

        [[nodiscard]] inline const IndexedValueOps *checked_mutable_indexed_ops(ValueTypeRef binding,
                                                                                const char *what)
        {
            const auto *ops = checked_indexed_ops(binding, what);
            if (ops->make_mutable_range == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose mutable indexed ops");
            }
            return ops;
        }

        [[nodiscard]] inline const CyclicBufferValueOps *checked_cyclic_buffer_ops(ValueTypeRef binding,
                                                                                   const char *what)
        {
            const auto *ops = checked_value_ops<CyclicBufferValueOps>(binding, what);
            require_indexed_ops(ops, what);
            if (ops == nullptr || ops->head == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose cyclic-buffer ops");
            }
            return ops;
        }

        [[nodiscard]] inline const QueueValueOps *checked_queue_ops(ValueTypeRef binding,
                                                                    const char *what)
        {
            const auto *ops = checked_value_ops<QueueValueOps>(binding, what);
            require_indexed_ops(ops, what);
            if (ops == nullptr || ops->front == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose queue ops");
            }
            return ops;
        }

        [[nodiscard]] inline const SetValueOps *checked_set_ops(ValueTypeRef binding, const char *what)
        {
            const auto *ops = checked_value_ops<SetValueOps>(binding, what);
            require_indexed_ops(ops, what);
            if (ops == nullptr || ops->contains == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose set ops");
            }
            return ops;
        }

        [[nodiscard]] inline const MapValueOps *checked_map_ops(ValueTypeRef binding, const char *what)
        {
            const auto *ops = checked_value_ops<MapValueOps>(binding, what);
            require_indexed_ops(ops, what);
            if (ops == nullptr || ops->contains == nullptr || ops->value_at == nullptr ||
                ops->value_at_index == nullptr || ops->value_binding == nullptr ||
                ops->make_keys_range == nullptr || ops->make_values_range == nullptr ||
                ops->make_kv_range == nullptr || ops->key_set == nullptr)
            {
                throw std::logic_error(std::string{what} + ": binding does not expose map ops");
            }
            return ops;
        }

        [[nodiscard]] inline ValueView require_kind(ValueView base, ValueTypeKind kind, const char *what)
        {
            require_valid(base.valid(), what);
            if (base.schema() == nullptr || base.schema()->value_kind() != kind)
            {
                throw std::logic_error(std::string{what} + " on wrong value kind");
            }
            return base;
        }

        [[nodiscard]] inline ValueView require_mutable(ValueView base, const char *what)
        {
            require_valid(base.valid(), what);
            if (!base.mutable_payload()) { throw std::logic_error(std::string{what} + " on read-only view"); }
            return base;
        }

        [[nodiscard]] inline bool binding_matches(const ValueView &view,
                                                  ValueTypeRef expected) noexcept
        {
            return view.valid() && expected != nullptr && view.binding() == expected;
        }
    }  // namespace specialized_view_detail

    /** Shape-tagged read-only view over an open-ended atomic value. */
    class AtomicView : public ValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::Atomic;

        explicit AtomicView(ValueView base)
            : ValueView(specialized_view_detail::require_kind(std::move(base), kind, "AtomicView"))
        {
        }

      private:
        AtomicView(ValueView base, detail::TrustedValueKind) : ValueView(std::move(base)) {}

        friend struct detail::ValueVisitorAccess;
    };

    /**
     * Base for views over positionally-addressed kinds. Tuple, bundle,
     * fixed-size list, and compact dynamic containers all forward
     * through the bound ``IndexedValueOps``.
     */
    class IndexedValueView : public ValueView
    {
        friend class ValueView;

      protected:
        explicit IndexedValueView(ValueView base)
            : ValueView(std::move(base))
            , ops_{specialized_view_detail::checked_indexed_ops(binding(), "IndexedValueView")}
        {
        }

      public:
        IndexedValueView(IndexedValueView &&) noexcept = default;
        IndexedValueView &operator=(IndexedValueView &&) noexcept = default;

        [[nodiscard]] std::size_t size() const
        {
            return ops_->size(ops_->context, data());
        }

        /** True when the indexed element is logically set.

            Storage lifetime and element validity are separate: a list may
            retain a default-constructed slot for an UNSET element. The
            representation supplies the answer through ``IndexedValueOps``;
            older/dense strategies fall back to a non-null element address. */
        [[nodiscard]] bool element_valid(std::size_t index) const
        {
            const auto n = ops_->size(ops_->context, data());
            if (index >= n)
            {
                throw std::out_of_range(
                    "IndexedValueView::element_valid: index out of range");
            }
            return ops_->element_valid != nullptr
                       ? ops_->element_valid(ops_->context, data(), index)
                       : ops_->element_at(ops_->context, data(), index) != nullptr;
        }

        /** Returning ``const`` BY VALUE is deliberate; do not "clean it up".
            ``ValueView`` is one type for both read-only and mutable views, so
            the only thing distinguishing them at an unnamed call site is the
            cv-qualification of the prvalue. ``at`` hands back a view over the
            ops table's ``const void *`` - always read-only - and the ``const``
            is what steers ``at(i).as<T>()`` to the READING ``as`` overload.
            Drop it and the non-const overload wins, routing to
            ``checked_mutable_as<T>``, which throws "requires begin_mutation":
            a working read becomes a runtime failure. ``begin_mutation()``
            returns a NON-const prvalue for the same reason, so
            ``begin_mutation().as<T>() = x`` still resolves to the mutable
            overload (see tests/install_consumer/main.cpp).

            The cost is that ``at`` results cannot be moved from, which is why
            callers that need an owned ``ValueView`` reborrow instead. Locked in
            by "ValueView: as<T>() on an accessor result reads, it does not
            demand mutation" in tests/cpp/test_value.cpp. A type-level split of
            read-only and mutable views would remove the need for this trick,
            but that is an RFC-scale change to the value layer. */
        [[nodiscard]] const ValueView at(std::size_t index) const
        {
            const auto n = ops_->size(ops_->context, data());
            if (index >= n) { throw std::out_of_range("IndexedValueView::at: index out of range"); }
            const auto binding = ops_->element_binding(ops_->context, data(), index);
            if (ops_->element_valid != nullptr &&
                !ops_->element_valid(ops_->context, data(), index))
            {
                return ValueView{binding, nullptr};
            }
            return ValueView{binding, ops_->element_at(ops_->context, data(), index)};
        }

        [[nodiscard]] const ValueView operator[](std::size_t index) const { return at(index); }

        [[nodiscard]] Range<ValueView> elements() const
        {
            return ops_->make_range(ops_->context, data());
        }

        [[nodiscard]] Range<ValueView> values() const { return elements(); }

        // Convenience: ``begin()`` / ``end()`` go through ``elements()`` so
        // ``for (auto v : view)`` works directly. Each call to ``begin()``
        // builds a fresh range; callers that want a stable iterator
        // pair should grab ``elements()`` first.
        [[nodiscard]] auto begin() const { return elements().begin(); }
        [[nodiscard]] auto end() const { return elements().end(); }

      protected:
        IndexedValueView(ValueView base, const IndexedValueOps *ops) noexcept
            : ValueView(std::move(base)), ops_{ops}
        {
        }

        const IndexedValueOps *ops_{nullptr};
    };

    class MutableIndexedValueView : public IndexedValueView
    {
        friend class ValueView;

      protected:
        explicit MutableIndexedValueView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_mutable(std::move(base), "MutableIndexedValueView"))
        {
            ops_ = specialized_view_detail::checked_mutable_indexed_ops(binding(), "MutableIndexedValueView");
        }

      public:
        MutableIndexedValueView(MutableIndexedValueView &&) noexcept = default;
        MutableIndexedValueView &operator=(MutableIndexedValueView &&) noexcept = default;

        [[nodiscard]] ValueView at(std::size_t index) const
        {
            const auto n = ops_->size(ops_->context, data());
            if (index >= n) { throw std::out_of_range("MutableIndexedValueView::at: index out of range"); }
            void *element = ops_->mutable_element_at != nullptr
                                ? ops_->mutable_element_at(ops_->context, mutable_data(), index)
                                : const_cast<void *>(ops_->element_at(ops_->context, data(), index));
            return ValueView{ops_->element_binding(ops_->context, data(), index), element}
                .begin_mutation();
        }

        [[nodiscard]] ValueView operator[](std::size_t index) const { return at(index); }

        [[nodiscard]] Range<ValueView> elements() const
        {
            return ops_->make_mutable_range(ops_->context, mutable_data());
        }

        [[nodiscard]] Range<ValueView> values() const { return elements(); }
        [[nodiscard]] auto begin() const { return elements().begin(); }
        [[nodiscard]] auto end() const { return elements().end(); }

      protected:
        MutableIndexedValueView(ValueView base, const IndexedValueOps *ops) noexcept
            : IndexedValueView(std::move(base), ops)
        {
        }
    };

    /** Read-only view over a positional Tuple. */
    class TupleView : public IndexedValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::Tuple;

        explicit TupleView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(std::move(base), kind, "TupleView"))
        {
        }

        [[nodiscard]] MutableTupleView begin_mutation() const;

      private:
        TupleView(ValueView base, detail::TrustedValueKind) : IndexedValueView(std::move(base)) {}

        friend struct detail::ValueVisitorAccess;
    };

    class MutableTupleView : public MutableIndexedValueView
    {
      public:
        explicit MutableTupleView(ValueView base)
            : MutableIndexedValueView(specialized_view_detail::require_kind(std::move(base), ValueTypeKind::Tuple,
                                                                            "MutableTupleView"))
        {
        }
    };

    /** Read-only view over a named Bundle. */
    class BundleView : public IndexedValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::Bundle;

        using IndexedValueView::at;
        using IndexedValueView::operator[];

        explicit BundleView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(std::move(base), kind, "BundleView"))
        {
        }

        [[nodiscard]] MutableBundleView begin_mutation() const;

        [[nodiscard]] const ValueView at(std::string_view name) const
        {
            for (std::size_t index = 0; index < schema()->field_count; ++index)
            {
                const char *field_name = schema()->fields[index].name;
                if (field_name != nullptr && name == field_name) { return IndexedValueView::at(index); }
            }
            throw std::out_of_range("BundleView::at: field not found");
        }

        [[nodiscard]] bool has_field(std::string_view name) const noexcept
        {
            for (std::size_t index = 0; index < schema()->field_count; ++index)
            {
                const char *field_name = schema()->fields[index].name;
                if (field_name != nullptr && name == field_name) { return true; }
            }
            return false;
        }

        [[nodiscard]] const ValueView field(std::string_view name) const { return at(name); }
        [[nodiscard]] const ValueView operator[](std::string_view name) const { return at(name); }

      private:
        BundleView(ValueView base, detail::TrustedValueKind) : IndexedValueView(std::move(base)) {}

        friend struct detail::ValueVisitorAccess;
    };

    class MutableBundleView : public MutableIndexedValueView
    {
      public:
        using MutableIndexedValueView::at;
        using MutableIndexedValueView::operator[];

        explicit MutableBundleView(ValueView base)
            : MutableIndexedValueView(specialized_view_detail::require_kind(std::move(base), ValueTypeKind::Bundle,
                                                                            "MutableBundleView"))
        {
        }

        [[nodiscard]] ValueView at(std::string_view name) const
        {
            for (std::size_t index = 0; index < schema()->field_count; ++index)
            {
                const char *field_name = schema()->fields[index].name;
                if (field_name != nullptr && name == field_name) { return MutableIndexedValueView::at(index); }
            }
            throw std::out_of_range("MutableBundleView::at: field not found");
        }

        [[nodiscard]] bool has_field(std::string_view name) const noexcept
        {
            for (std::size_t index = 0; index < schema()->field_count; ++index)
            {
                const char *field_name = schema()->fields[index].name;
                if (field_name != nullptr && name == field_name) { return true; }
            }
            return false;
        }

        [[nodiscard]] ValueView field(std::string_view name) const { return at(name); }
        [[nodiscard]] ValueView operator[](std::string_view name) const { return at(name); }
    };

    /** Read-only view over a List. Inherits indexed access. */
    class ListView : public IndexedValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::List;

        explicit ListView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(std::move(base), kind, "ListView"))
        {
        }

        [[nodiscard]] MutableListView begin_mutation() const;

        [[nodiscard]] bool is_fixed() const noexcept { return schema()->fixed_size != 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] const ValueView front() const
        {
            if (empty()) { throw std::out_of_range("ListView::front on empty list"); }
            return at(0);
        }
        [[nodiscard]] const ValueView back() const
        {
            if (empty()) { throw std::out_of_range("ListView::back on empty list"); }
            return at(size() - 1);
        }

      private:
        ListView(ValueView base, detail::TrustedValueKind) : IndexedValueView(std::move(base)) {}

        friend struct detail::ValueVisitorAccess;
    };

    class MutableListView : public MutableIndexedValueView
    {
      public:
        explicit MutableListView(ValueView base)
            : MutableIndexedValueView(specialized_view_detail::require_kind(std::move(base), ValueTypeKind::List,
                                                                            "MutableListView"))
        {
        }

        [[nodiscard]] bool is_fixed() const noexcept { return schema()->fixed_size != 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] const ValueView front() const
        {
            if (empty()) { throw std::out_of_range("MutableListView::front on empty list"); }
            return at(0);
        }
        [[nodiscard]] const ValueView back() const
        {
            if (empty()) { throw std::out_of_range("MutableListView::back on empty list"); }
            return at(size() - 1);
        }

        /** Set the logical extent of a bounded list representation.
         *
         * Fixed shaped arrays use this to expose a prefix of their inline
         * capacity while warming up. Ordinary fixed lists do not install the
         * hook and therefore reject resizing.
         */
        void resize(std::size_t size) const
        {
            if (ops_->resize == nullptr)
            {
                throw std::logic_error("MutableListView::resize is not supported by this list representation");
            }
            ops_->resize(ops_->context, mutable_data(), size);
        }

        // -- structural mutation (only for a mutable list; throws otherwise) --

        /** Append a copy of ``element`` (its schema must match the element type). */
        void push_back(const ValueView &element) const
        {
            require_element(element, "push_back");
            const auto *ops = mutable_ops("push_back");
            specialized_view_detail::require_mutation_hook(ops->push_back, "MutableListView", "push_back",
                                                            "push_back")(nullptr, mutable_data(), element.binding(),
                                                                         element.data());
        }

        /** Replace the element at ``index`` with a copy of ``element``. */
        void set(std::size_t index, const ValueView &element) const
        {
            require_element(element, "set");
            const auto *ops = mutable_ops("set");
            specialized_view_detail::require_mutation_hook(ops->set_element, "MutableListView", "set",
                                                            "set_element")(nullptr, mutable_data(), index,
                                                                           element.binding(), element.data());
        }

        /** Remove the element at ``index``, shifting later elements down. */
        void erase(std::size_t index) const
        {
            const auto *ops = mutable_ops("erase");
            specialized_view_detail::require_mutation_hook(ops->erase, "MutableListView", "erase", "erase")(
                nullptr, mutable_data(), index);
        }

        /** Append an UNSET element - a hole (element validity). */
        void push_back_unset() const
        {
            const auto *ops = mutable_ops("push_back_unset");
            specialized_view_detail::require_mutation_hook(ops->push_back_unset, "MutableListView",
                                                            "push_back_unset", "push_back_unset")(
                nullptr, mutable_data());
        }

        /** Drop the last element. */
        void pop_back() const
        {
            const auto *ops = mutable_ops("pop_back");
            specialized_view_detail::require_mutation_hook(ops->pop_back, "MutableListView", "pop_back",
                                                            "pop_back")(nullptr, mutable_data());
        }

        /** Remove every element. */
        void clear() const
        {
            const auto *ops = mutable_ops("clear");
            specialized_view_detail::require_mutation_hook(ops->clear, "MutableListView", "clear", "clear")(
                nullptr, mutable_data());
        }

      private:
        [[nodiscard]] const MutableListValueOps *mutable_ops(const char *what) const
        {
            if (schema() == nullptr || !schema()->is_mutable())
            {
                throw std::logic_error(std::string{"MutableListView::"} + what + " requires a mutable list");
            }
            return checked_value_ops<MutableListValueOps>(binding(), "MutableListView");
        }
        void require_element(const ValueView &element, const char *what) const
        {
            const auto *ops = mutable_ops(what);
            const auto expected = ops->element_binding(ops->context, data(), 0);
            if (!element.valid() || !expected ||
                !expected.ops_ref().accepts_source(expected, element.binding()))
            {
                throw std::logic_error(std::string{"MutableListView::"} + what + ": element binding mismatch");
            }
        }
    };

    /** Read-only view over a CyclicBuffer. Adds head / empty. */
    class CyclicBufferView : public IndexedValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::CyclicBuffer;

        explicit CyclicBufferView(ValueView base)
            : IndexedValueView(
                  specialized_view_detail::require_kind(std::move(base), kind, "CyclicBufferView"))
        {
            cyclic_buffer_ops_ = specialized_view_detail::checked_cyclic_buffer_ops(binding(), "CyclicBufferView");
        }

        [[nodiscard]] MutableCyclicBufferView begin_mutation() const;

        [[nodiscard]] std::size_t head() const
        {
            return cyclic_buffer_ops_->head(data());
        }

        [[nodiscard]] std::size_t capacity() const noexcept { return schema()->fixed_size; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const { return capacity() != 0 && size() == capacity(); }
        [[nodiscard]] const ValueView front() const
        {
            if (empty()) { throw std::out_of_range("CyclicBufferView::front on empty buffer"); }
            return at(0);
        }
        [[nodiscard]] const ValueView back() const
        {
            if (empty()) { throw std::out_of_range("CyclicBufferView::back on empty buffer"); }
            return at(size() - 1);
        }

      private:
        CyclicBufferView(ValueView base, detail::TrustedValueKind)
            : IndexedValueView(std::move(base))
            , cyclic_buffer_ops_{specialized_view_detail::checked_cyclic_buffer_ops(binding(), "CyclicBufferView")}
        {
        }

        friend struct detail::ValueVisitorAccess;

        const CyclicBufferValueOps *cyclic_buffer_ops_{nullptr};
    };

    class MutableCyclicBufferView : public MutableIndexedValueView
    {
      public:
        explicit MutableCyclicBufferView(ValueView base)
            : MutableIndexedValueView(specialized_view_detail::require_kind(std::move(base), ValueTypeKind::CyclicBuffer,
                                                                            "MutableCyclicBufferView"))
        {
            cyclic_buffer_ops_ = specialized_view_detail::checked_cyclic_buffer_ops(binding(),
                                                                                    "MutableCyclicBufferView");
        }

        [[nodiscard]] std::size_t head() const { return cyclic_buffer_ops_->head(data()); }
        [[nodiscard]] std::size_t capacity() const noexcept { return schema()->fixed_size; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const { return capacity() != 0 && size() == capacity(); }
        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("MutableCyclicBufferView::front on empty buffer"); }
            return at(0);
        }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("MutableCyclicBufferView::back on empty buffer"); }
            return at(size() - 1);
        }

      private:
        const CyclicBufferValueOps *cyclic_buffer_ops_{nullptr};
    };

    /** Read-only view over a Queue. Adds front / empty. */
    class QueueView : public IndexedValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::Queue;

        explicit QueueView(ValueView base)
            : IndexedValueView(specialized_view_detail::require_kind(std::move(base), kind, "QueueView"))
        {
            queue_ops_ = specialized_view_detail::checked_queue_ops(binding(), "QueueView");
        }

        [[nodiscard]] MutableQueueView begin_mutation() const;

        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("QueueView::front on empty queue"); }
            return ValueView{queue_ops_->element_binding(queue_ops_->context, data(), 0),
                             queue_ops_->front(data())};
        }

        [[nodiscard]] std::size_t max_capacity() const noexcept { return schema()->fixed_size; }
        [[nodiscard]] bool has_max_capacity() const noexcept { return max_capacity() != 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const { return max_capacity() != 0 && size() == max_capacity(); }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("QueueView::back on empty queue"); }
            return at(size() - 1);
        }

      private:
        QueueView(ValueView base, detail::TrustedValueKind)
            : IndexedValueView(std::move(base))
            , queue_ops_{specialized_view_detail::checked_queue_ops(binding(), "QueueView")}
        {
        }

        friend struct detail::ValueVisitorAccess;

        const QueueValueOps *queue_ops_{nullptr};
    };

    class MutableQueueView : public MutableIndexedValueView
    {
      public:
        explicit MutableQueueView(ValueView base)
            : MutableIndexedValueView(specialized_view_detail::require_kind(std::move(base), ValueTypeKind::Queue,
                                                                            "MutableQueueView"))
        {
            queue_ops_ = specialized_view_detail::checked_queue_ops(binding(), "MutableQueueView");
        }

        [[nodiscard]] ValueView front() const
        {
            if (empty()) { throw std::out_of_range("MutableQueueView::front on empty queue"); }
            return ValueView{queue_ops_->element_binding(queue_ops_->context, data(), 0),
                             const_cast<void *>(queue_ops_->front(data()))}
                .begin_mutation();
        }

        [[nodiscard]] std::size_t max_capacity() const noexcept { return schema()->fixed_size; }
        [[nodiscard]] bool has_max_capacity() const noexcept { return max_capacity() != 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }
        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] bool full() const { return max_capacity() != 0 && size() == max_capacity(); }
        [[nodiscard]] ValueView back() const
        {
            if (empty()) { throw std::out_of_range("MutableQueueView::back on empty queue"); }
            return at(size() - 1);
        }

      private:
        const QueueValueOps *queue_ops_{nullptr};
    };

    /**
     * Read-only view over a Set. Membership query plus a member range
     * built from the bound ``IndexedValueOps`` (the set is unordered
     * so ``at(index)`` is not exposed publicly).
     */
    class MutableSetView;

    class SetView : public ValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::Set;

        explicit SetView(ValueView base)
            : ValueView(specialized_view_detail::require_kind(std::move(base), kind, "SetView"))
        {
            ops_ = specialized_view_detail::checked_set_ops(binding(), "SetView");
        }

        [[nodiscard]] std::size_t size() const
        {
            return ops_->size(ops_->context, data());
        }

        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] const ValueTypeMetaData *element_schema() const noexcept { return schema()->element_type; }

        [[nodiscard]] bool contains(const ValueView &key) const
        {
            if (!specialized_view_detail::binding_matches(
                    key, ops_->element_binding(ops_->context, data(), 0)))
            {
                return false;
            }
            return ops_->contains(ops_->context, data(), key.data());
        }

        [[nodiscard]] Range<ValueView> elements() const
        {
            return ops_->make_range(ops_->context, data());
        }

        [[nodiscard]] Range<ValueView> values() const { return elements(); }

        [[nodiscard]] auto begin() const { return elements().begin(); }
        [[nodiscard]] auto end() const { return elements().end(); }

        /** Open a writable view over a mutable set (requires mutable storage). */
        [[nodiscard]] MutableSetView begin_mutation() const;

      private:
        SetView(ValueView base, detail::TrustedValueKind)
            : ValueView(std::move(base))
            , ops_{specialized_view_detail::checked_set_ops(binding(), "SetView")}
        {
        }

        friend struct detail::ValueVisitorAccess;

        const SetValueOps *ops_{nullptr};
    };

    /**
     * Writable view over a **mutable** set (``ValueTypeFlags::Mutable``). Adds
     * add / remove / clear on top of the ``SetView`` read surface. On a compact
     * (immutable) set these throw.
     */
    class MutableSetView : public SetView
    {
      public:
        explicit MutableSetView(ValueView base)
            : SetView(specialized_view_detail::require_mutable(std::move(base), "MutableSetView"))
        {
        }

        /** Add ``key`` to the set; returns whether the set changed. */
        bool add(const ValueView &key) const
        {
            require_element(key, "add");
            const auto *ops = mutable_ops("add");
            return specialized_view_detail::require_mutation_hook(ops->add, "MutableSetView", "add", "add")(
                nullptr, mutable_data(), key.data());
        }
        /** Remove ``key`` if present; returns whether the set changed. */
        bool remove(const ValueView &key) const
        {
            require_element(key, "remove");
            const auto *ops = mutable_ops("remove");
            return specialized_view_detail::require_mutation_hook(ops->remove, "MutableSetView", "remove",
                                                                   "remove")(nullptr, mutable_data(), key.data());
        }
        /** Remove every element. */
        void clear() const
        {
            const auto *ops = mutable_ops("clear");
            specialized_view_detail::require_mutation_hook(ops->clear, "MutableSetView", "clear", "clear")(
                nullptr, mutable_data());
        }

      private:
        [[nodiscard]] const MutableSetValueOps *mutable_ops(const char *what) const
        {
            if (schema() == nullptr || !schema()->is_mutable())
            {
                throw std::logic_error(std::string{"MutableSetView::"} + what + " requires a mutable set");
            }
            return checked_value_ops<MutableSetValueOps>(binding(), "MutableSetView");
        }
        void require_element(const ValueView &key, const char *what) const
        {
            if (!key.valid() || key.schema() != element_schema())
            {
                throw std::logic_error(std::string{"MutableSetView::"} + what + ": element schema mismatch");
            }
        }
    };

    /**
     * Read-only view over a Map. ``contains`` / ``at`` / paired
     * iteration through the bound ops; ``key_set`` returns an adapted
     * ``SetView`` over the same memory.
     */
    class MapView : public ValueView
    {
      public:
        static constexpr ValueTypeKind kind = ValueTypeKind::Map;

        explicit MapView(ValueView base)
            : ValueView(specialized_view_detail::require_kind(std::move(base), kind, "MapView"))
        {
            ops_ = specialized_view_detail::checked_map_ops(binding(), "MapView");
        }

        [[nodiscard]] std::size_t size() const
        {
            return ops_->size(ops_->context, data());
        }

        [[nodiscard]] bool empty() const { return size() == 0; }
        [[nodiscard]] const ValueTypeMetaData *key_schema() const noexcept { return schema()->key_type; }
        [[nodiscard]] const ValueTypeMetaData *value_schema() const noexcept { return schema()->element_type; }

        [[nodiscard]] bool contains(const ValueView &key) const
        {
            if (!key_compatible(key)) { return false; }
            return ops_->contains(ops_->context, data(), key.data());
        }

        [[nodiscard]] const ValueView at(const ValueView &key) const
        {
            const void *found = key_compatible(key) ? ops_->value_at(ops_->context, data(), key.data()) : nullptr;
            if (found == nullptr) { throw std::out_of_range("MapView::at: key not present"); }
            return ValueView{ops_->value_binding(ops_->context, data()), found};
        }

        [[nodiscard]] const ValueView operator[](const ValueView &key) const { return at(key); }

        /** Returns a read-only ``SetView`` over the live keys. */
        [[nodiscard]] SetView key_set() const
        {
            return ops_->key_set(ops_->context, binding(), data());
        }

        /** Lazy range over the map's keys. */
        [[nodiscard]] Range<ValueView> keys() const
        {
            return ops_->make_keys_range(ops_->context, data());
        }

        /** Lazy range over the map's values. */
        [[nodiscard]] Range<ValueView> values() const
        {
            return ops_->make_values_range(ops_->context, data());
        }

        /** Lazy paired ``(key, value)`` range over live entries. */
        [[nodiscard]] KeyValueRange<ValueView, ValueView> entries() const
        {
            return ops_->make_kv_range(ops_->context, data());
        }

        [[nodiscard]] KeyValueRange<ValueView, ValueView> items() const { return entries(); }

        [[nodiscard]] auto begin() const { return entries().begin(); }
        [[nodiscard]] auto end() const { return entries().end(); }

        /** Open a writable view over a mutable map (requires mutable storage). */
        [[nodiscard]] MutableMapView begin_mutation() const;

      private:
        MapView(ValueView base, detail::TrustedValueKind)
            : ValueView(std::move(base))
            , ops_{specialized_view_detail::checked_map_ops(binding(), "MapView")}
        {
        }

        friend struct detail::ValueVisitorAccess;

        [[nodiscard]] bool key_compatible(const ValueView &key) const noexcept
        {
            return specialized_view_detail::binding_matches(
                key, ops_->element_binding(ops_->context, data(), 0));
        }

        const MapValueOps *ops_{nullptr};
    };

    /**
     * Writable view over a **mutable** map (``ValueTypeFlags::Mutable``). Adds
     * insert-or-replace, remove, and clear on top of the ``MapView`` read
     * surface. On a compact (immutable) map these throw.
     */
    class MutableMapView : public MapView
    {
      public:
        explicit MutableMapView(ValueView base)
            : MapView(specialized_view_detail::require_mutable(std::move(base), "MutableMapView"))
        {
        }

        /** Insert ``key`` -> ``value``, or replace the value if ``key`` is present. */
        void set_item(const ValueView &key, const ValueView &value) const
        {
            require_key(key, "set_item");
            require_value(value, "set_item");
            const auto *ops = mutable_ops("set_item");
            specialized_view_detail::require_mutation_hook(ops->insert, "MutableMapView", "set_item", "insert")(
                nullptr, mutable_data(), key.data(), value.data());
        }

        /**
         * Mutable view of the value at ``key``, creating a default-valued entry
         * when ``key`` is absent. Lets callers assign the value in place (e.g.
         * ``m.value(k).as_mutable_any().set(v)``) without building a temporary.
         */
        [[nodiscard]] ValueView value(const ValueView &key) const
        {
            require_key(key, "value");
            const auto *ops  = mutable_ops("value");
            void       *slot = specialized_view_detail::require_mutation_hook(
                ops->value_or_emplace, "MutableMapView", "value", "value_or_emplace")(
                nullptr, mutable_data(), key.data());
            return ValueView{ops->value_binding(nullptr, mutable_data()), slot}.begin_mutation();
        }

        /** Remove ``key`` if present; returns whether a key was removed. */
        bool remove(const ValueView &key) const
        {
            require_key(key, "remove");
            const auto *ops = mutable_ops("remove");
            const auto erase = specialized_view_detail::require_mutation_hook(ops->erase, "MutableMapView", "remove",
                                                                               "erase");
            const bool had = contains(key);
            erase(nullptr, mutable_data(), key.data());
            return had;
        }

        /** Remove every entry. */
        void clear() const
        {
            const auto *ops = mutable_ops("clear");
            specialized_view_detail::require_mutation_hook(ops->clear, "MutableMapView", "clear", "clear")(
                nullptr, mutable_data());
        }

      private:
        [[nodiscard]] const MutableMapValueOps *mutable_ops(const char *what) const
        {
            if (schema() == nullptr || !schema()->is_mutable())
            {
                throw std::logic_error(std::string{"MutableMapView::"} + what + " requires a mutable map");
            }
            return checked_value_ops<MutableMapValueOps>(binding(), "MutableMapView");
        }
        void require_key(const ValueView &key, const char *what) const
        {
            if (!key.valid() || key.schema() != key_schema())
            {
                throw std::logic_error(std::string{"MutableMapView::"} + what + ": key schema mismatch");
            }
        }
        void require_value(const ValueView &value, const char *what) const
        {
            if (!value.valid() || value.schema() != value_schema())
            {
                throw std::logic_error(std::string{"MutableMapView::"} + what + ": value schema mismatch");
            }
        }
    };

    /**
     * Read-only view over an ``Any`` value — the type-erased box holding an
     * embedded owning ``Value``. ``has_value`` reports whether content has
     * been assigned; ``get`` returns a read view of the contained value (an
     * invalid view when empty). Methods that touch the embedded ``Value`` are
     * defined out-of-line (in ``value_view.cpp``) where ``Value`` is complete.
     */
    class HGRAPH_CLASS_EXPORT AnyView : public ValueView
    {
      public:
        explicit AnyView(ValueView base)
            : ValueView(specialized_view_detail::require_kind(std::move(base), ValueTypeKind::Any, "AnyView"))
        {
        }

        /** True when a value has been assigned into the box. */
        [[nodiscard]] bool has_value() const noexcept;
        /** Read-only view of the contained value; an invalid view when empty. */
        [[nodiscard]] ValueView get() const;
        /** Schema of the contained value, or ``nullptr`` when empty. */
        [[nodiscard]] const ValueTypeMetaData *value_schema() const noexcept;

        /** Open a writable view over the box (requires mutable storage). */
        [[nodiscard]] MutableAnyView begin_mutation() const;
    };

    /**
     * Writable view over an ``Any`` value: replace or clear the contained
     * value. ``set`` deep-copies the supplied value into the box.
     */
    class HGRAPH_CLASS_EXPORT MutableAnyView : public AnyView
    {
      public:
        explicit MutableAnyView(ValueView base)
            : AnyView(specialized_view_detail::require_mutable(std::move(base), "MutableAnyView"))
        {
        }

        /** Replace the contained value with a deep copy of ``value``. */
        void set(const ValueView &value) const;
        /** Replace the contained value with a copy of ``value``. */
        void set(const Value &value) const;
        /** Replace the contained value by MOVING ``value`` into the box (no copy). */
        void set(Value &&value) const;
        /** Clear the box back to the empty state. */
        void clear() const;
    };

    inline IndexedValueView ValueView::as_indexed_view() const
    {
        if (!is_indexed()) { throw std::logic_error("ValueView::as_indexed_view on non-indexed view"); }
        return IndexedValueView{borrowed_ref()};
    }

    inline AtomicView ValueView::as_atomic() const
    {
        if (!is_atomic()) { throw std::logic_error("ValueView::as_atomic on non-atomic view"); }
        return AtomicView{borrowed_ref()};
    }

    inline std::optional<AtomicView> ValueView::try_as_atomic() const
    {
        return is_atomic() ? std::optional<AtomicView>{AtomicView{borrowed_ref()}} : std::nullopt;
    }

    inline std::optional<IndexedValueView> ValueView::try_as_indexed_view() const
    {
        return is_indexed() ? std::optional<IndexedValueView>{IndexedValueView{borrowed_ref()}} : std::nullopt;
    }

    inline IndexedValueView ValueView::as_indexed() const
    {
        return as_indexed_view();
    }

    inline std::optional<IndexedValueView> ValueView::try_as_indexed() const
    {
        return try_as_indexed_view();
    }

    inline TupleView ValueView::as_tuple() const
    {
        if (!is_tuple()) { throw std::logic_error("ValueView::as_tuple on non-tuple view"); }
        return TupleView{borrowed_ref()};
    }

    inline MutableTupleView TupleView::begin_mutation() const
    {
        return MutableTupleView{ValueView::begin_mutation()};
    }

    inline std::optional<TupleView> ValueView::try_as_tuple() const
    {
        return is_tuple() ? std::optional<TupleView>{TupleView{borrowed_ref()}} : std::nullopt;
    }

    inline BundleView ValueView::as_bundle() const
    {
        if (!is_bundle()) { throw std::logic_error("ValueView::as_bundle on non-bundle view"); }
        return BundleView{borrowed_ref()};
    }

    inline MutableBundleView BundleView::begin_mutation() const
    {
        return MutableBundleView{ValueView::begin_mutation()};
    }

    inline std::optional<BundleView> ValueView::try_as_bundle() const
    {
        return is_bundle() ? std::optional<BundleView>{BundleView{borrowed_ref()}} : std::nullopt;
    }

    inline ListView ValueView::as_list() const
    {
        if (!is_list()) { throw std::logic_error("ValueView::as_list on non-list view"); }
        return ListView{borrowed_ref()};
    }

    inline MutableListView ListView::begin_mutation() const
    {
        return MutableListView{ValueView::begin_mutation()};
    }

    inline std::optional<ListView> ValueView::try_as_list() const
    {
        return is_list() ? std::optional<ListView>{ListView{borrowed_ref()}} : std::nullopt;
    }

    inline SetView ValueView::as_set() const
    {
        if (!is_set()) { throw std::logic_error("ValueView::as_set on non-set view"); }
        return SetView{borrowed_ref()};
    }

    inline std::optional<SetView> ValueView::try_as_set() const
    {
        return is_set() ? std::optional<SetView>{SetView{borrowed_ref()}} : std::nullopt;
    }

    inline MutableSetView SetView::begin_mutation() const
    {
        return MutableSetView{ValueView::begin_mutation()};
    }

    inline MutableSetView ValueView::as_mutable_set() const
    {
        if (!is_set()) { throw std::logic_error("ValueView::as_mutable_set on non-set view"); }
        return MutableSetView{borrowed_ref()};
    }

    inline std::optional<MutableSetView> ValueView::try_as_mutable_set() const
    {
        return is_set() && mutable_payload() ? std::optional<MutableSetView>{MutableSetView{borrowed_ref()}}
                                             : std::nullopt;
    }

    inline MapView ValueView::as_map() const
    {
        if (!is_map()) { throw std::logic_error("ValueView::as_map on non-map view"); }
        return MapView{borrowed_ref()};
    }

    inline std::optional<MapView> ValueView::try_as_map() const
    {
        return is_map() ? std::optional<MapView>{MapView{borrowed_ref()}} : std::nullopt;
    }

    inline MutableMapView MapView::begin_mutation() const
    {
        return MutableMapView{ValueView::begin_mutation()};
    }

    inline MutableMapView ValueView::as_mutable_map() const
    {
        if (!is_map()) { throw std::logic_error("ValueView::as_mutable_map on non-map view"); }
        return MutableMapView{borrowed_ref()};
    }

    inline std::optional<MutableMapView> ValueView::try_as_mutable_map() const
    {
        return is_map() && mutable_payload() ? std::optional<MutableMapView>{MutableMapView{borrowed_ref()}}
                                             : std::nullopt;
    }

    inline CyclicBufferView ValueView::as_cyclic_buffer() const
    {
        if (!is_cyclic_buffer())
        {
            throw std::logic_error("ValueView::as_cyclic_buffer on non-cyclic-buffer view");
        }
        return CyclicBufferView{borrowed_ref()};
    }

    inline MutableCyclicBufferView CyclicBufferView::begin_mutation() const
    {
        return MutableCyclicBufferView{ValueView::begin_mutation()};
    }

    inline std::optional<CyclicBufferView> ValueView::try_as_cyclic_buffer() const
    {
        return is_cyclic_buffer() ? std::optional<CyclicBufferView>{CyclicBufferView{borrowed_ref()}} : std::nullopt;
    }

    inline QueueView ValueView::as_queue() const
    {
        if (!is_queue()) { throw std::logic_error("ValueView::as_queue on non-queue view"); }
        return QueueView{borrowed_ref()};
    }

    inline MutableQueueView QueueView::begin_mutation() const
    {
        return MutableQueueView{ValueView::begin_mutation()};
    }

    inline std::optional<QueueView> ValueView::try_as_queue() const
    {
        return is_queue() ? std::optional<QueueView>{QueueView{borrowed_ref()}} : std::nullopt;
    }

    inline AnyView ValueView::as_any() const
    {
        if (!is_any()) { throw std::logic_error("ValueView::as_any on non-any view"); }
        return AnyView{borrowed_ref()};
    }

    inline std::optional<AnyView> ValueView::try_as_any() const
    {
        return is_any() ? std::optional<AnyView>{AnyView{borrowed_ref()}} : std::nullopt;
    }

    inline MutableAnyView ValueView::as_mutable_any() const
    {
        if (!is_any()) { throw std::logic_error("ValueView::as_mutable_any on non-any view"); }
        return MutableAnyView{borrowed_ref()};
    }

    inline std::optional<MutableAnyView> ValueView::try_as_mutable_any() const
    {
        return is_any() && mutable_payload() ? std::optional<MutableAnyView>{MutableAnyView{borrowed_ref()}}
                                             : std::nullopt;
    }

    inline MutableTupleView ValueView::as_mutable_tuple() const
    {
        if (!is_tuple()) { throw std::logic_error("ValueView::as_mutable_tuple on non-tuple view"); }
        return MutableTupleView{borrowed_ref()};
    }

    inline std::optional<MutableTupleView> ValueView::try_as_mutable_tuple() const
    {
        return is_tuple() && mutable_payload() ? std::optional<MutableTupleView>{MutableTupleView{borrowed_ref()}}
                                               : std::nullopt;
    }

    inline MutableBundleView ValueView::as_mutable_bundle() const
    {
        if (!is_bundle()) { throw std::logic_error("ValueView::as_mutable_bundle on non-bundle view"); }
        return MutableBundleView{borrowed_ref()};
    }

    inline std::optional<MutableBundleView> ValueView::try_as_mutable_bundle() const
    {
        return is_bundle() && mutable_payload() ? std::optional<MutableBundleView>{MutableBundleView{borrowed_ref()}}
                                                : std::nullopt;
    }

    inline MutableListView ValueView::as_mutable_list() const
    {
        if (!is_list()) { throw std::logic_error("ValueView::as_mutable_list on non-list view"); }
        return MutableListView{borrowed_ref()};
    }

    inline std::optional<MutableListView> ValueView::try_as_mutable_list() const
    {
        return is_list() && mutable_payload() ? std::optional<MutableListView>{MutableListView{borrowed_ref()}}
                                              : std::nullopt;
    }

    inline MutableCyclicBufferView ValueView::as_mutable_cyclic_buffer() const
    {
        if (!is_cyclic_buffer())
        {
            throw std::logic_error("ValueView::as_mutable_cyclic_buffer on non-cyclic-buffer view");
        }
        return MutableCyclicBufferView{borrowed_ref()};
    }

    inline std::optional<MutableCyclicBufferView> ValueView::try_as_mutable_cyclic_buffer() const
    {
        return is_cyclic_buffer() && mutable_payload()
                   ? std::optional<MutableCyclicBufferView>{MutableCyclicBufferView{borrowed_ref()}}
                   : std::nullopt;
    }

    inline MutableQueueView ValueView::as_mutable_queue() const
    {
        if (!is_queue()) { throw std::logic_error("ValueView::as_mutable_queue on non-queue view"); }
        return MutableQueueView{borrowed_ref()};
    }

    inline std::optional<MutableQueueView> ValueView::try_as_mutable_queue() const
    {
        return is_queue() && mutable_payload() ? std::optional<MutableQueueView>{MutableQueueView{borrowed_ref()}}
                                               : std::nullopt;
    }

    inline MutableIndexedValueView ValueView::as_mutable_indexed_view() const
    {
        if (!is_indexed())
        {
            throw std::logic_error("ValueView::as_mutable_indexed_view on non-indexed view");
        }
        return MutableIndexedValueView{borrowed_ref()};
    }

    inline std::optional<MutableIndexedValueView> ValueView::try_as_mutable_indexed_view() const
    {
        return is_indexed() && mutable_payload()
                   ? std::optional<MutableIndexedValueView>{MutableIndexedValueView{borrowed_ref()}}
                   : std::nullopt;
    }

    inline MutableIndexedValueView ValueView::as_mutable_indexed() const
    {
        return as_mutable_indexed_view();
    }

    inline std::optional<MutableIndexedValueView> ValueView::try_as_mutable_indexed() const
    {
        return try_as_mutable_indexed_view();
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_SPECIALIZED_VIEWS_H
