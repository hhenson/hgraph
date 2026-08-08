#ifndef HGRAPH_CPP_ROOT_VALUE_BUILDER_H
#define HGRAPH_CPP_ROOT_VALUE_BUILDER_H

#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Per-kind builders for the value-layer compact container storage.
     *
     * The compact value-layer storage shapes (``ListStorage`` /
     * ``SetStorage`` / ``MapStorage`` / ``CyclicBufferStorage`` /
     * ``QueueStorage``) are immutable-after-construction by design:
     * once constructed they cannot grow, shrink, or replace individual
     * elements. Builders are the construction-time mutable shim that
     * lets callers accumulate elements piecemeal and produce the
     * immutable storage on ``build_storage()``.
     *
     * Each builder is single-use. The accumulated elements live in a
     * type-erased contiguous buffer driven by the bound element plan;
     * ``build_storage()`` copies them into a freshly-constructed
     * compact storage of the now-known size and the builder's buffer
     * is reset.
     */

    namespace builder_detail
    {
        /**
         * Type-erased growable contiguous buffer of constructed
         * elements. ``push_back_copy`` copies an element into the
         * trailing slot via the element plan; growth relocates
         * existing elements through the plan's move-construct hook,
         * falling back to ``memcpy`` for trivially-move-constructible
         * types.
         */
        class ElementAccumulator
        {
          public:
            explicit ElementAccumulator(const MemoryUtils::StoragePlan &plan) : plan_{&plan} {}

            ElementAccumulator(const ElementAccumulator &)            = delete;
            ElementAccumulator &operator=(const ElementAccumulator &) = delete;

            ElementAccumulator(ElementAccumulator &&other) noexcept
                : plan_{other.plan_}, bytes_{other.bytes_}, capacity_{other.capacity_}, size_{other.size_}
                , validity_{std::move(other.validity_)}
            {
                other.bytes_    = nullptr;
                other.capacity_ = 0;
                other.size_     = 0;
                other.validity_.clear();
            }

            ElementAccumulator &operator=(ElementAccumulator &&other) noexcept
            {
                if (this != &other)
                {
                    clear();
                    plan_           = other.plan_;
                    bytes_          = other.bytes_;
                    capacity_       = other.capacity_;
                    size_           = other.size_;
                    validity_       = std::move(other.validity_);
                    other.bytes_    = nullptr;
                    other.capacity_ = 0;
                    other.size_     = 0;
                    other.validity_.clear();
                }
                return *this;
            }

            ~ElementAccumulator() { clear(); }

            void push_back_copy(const void *src)
            {
                if (size_ == capacity_) { grow(std::max<std::size_t>(capacity_ * 2, 8)); }
                plan_->copy_construct(slot_address(size_), src);
                if (!validity_.empty()) { validity_.push_back(true); }
                ++size_;
            }

            /** Append a default-constructed HOLE (element validity): the slot
                is live memory but reads report it unset. The first hole sizes
                the (otherwise-empty, dense) validity bitset. */
            void push_back_unset()
            {
                if (size_ == capacity_) { grow(std::max<std::size_t>(capacity_ * 2, 8)); }
                plan_->default_construct(slot_address(size_));
                if (validity_.empty()) { validity_.assign(size_, true); }
                validity_.push_back(false);
                ++size_;
            }

            [[nodiscard]] const std::vector<bool> &validity() const noexcept { return validity_; }

            void pop_back() noexcept
            {
                if (size_ == 0) { return; }
                --size_;
                plan_->destroy(slot_address(size_));
            }

            [[nodiscard]] std::size_t size() const noexcept { return size_; }
            [[nodiscard]] bool        empty() const noexcept { return size_ == 0; }

            [[nodiscard]] const void *at(std::size_t index) const noexcept { return slot_address(index); }
            [[nodiscard]] void       *at(std::size_t index) noexcept { return slot_address(index); }

            [[nodiscard]] ElementSpan as_span() const noexcept
            {
                return ElementSpan{
                    .bytes  = size_ == 0 ? nullptr : bytes_,
                    .size   = size_,
                    .stride = plan_->layout.size,
                    .plan   = plan_,
                };
            }

            void clear() noexcept
            {
                validity_.clear();
                if (bytes_ == nullptr) { return; }
                for (std::size_t i = size_; i > 0; --i) { plan_->destroy(slot_address(i - 1)); }
                ::operator delete(bytes_, std::align_val_t{plan_->layout.alignment});
                bytes_    = nullptr;
                capacity_ = 0;
                size_     = 0;
            }

          private:
            [[nodiscard]] void *slot_address(std::size_t index) noexcept
            {
                return static_cast<std::byte *>(bytes_) + index * plan_->layout.size;
            }
            [[nodiscard]] const void *slot_address(std::size_t index) const noexcept
            {
                return static_cast<const std::byte *>(bytes_) + index * plan_->layout.size;
            }

            void grow(std::size_t new_capacity)
            {
                const std::size_t element_size = plan_->layout.size;
                if (element_size != 0 &&
                    new_capacity > std::numeric_limits<std::size_t>::max() / element_size)
                {
                    throw std::bad_array_new_length();
                }
                void *new_bytes =
                    ::operator new(new_capacity * element_size, std::align_val_t{plan_->layout.alignment});

                if (plan_->trivially_move_constructible && plan_->trivially_destructible)
                {
                    if (size_ != 0) { std::memcpy(new_bytes, bytes_, size_ * element_size); }
                }
                else
                {
                    std::size_t moved = 0;
                    auto        rollback = make_scope_exit([&]() noexcept {
                        for (std::size_t i = moved; i > 0; --i)
                        {
                            plan_->destroy(static_cast<std::byte *>(new_bytes) + (i - 1) * element_size);
                        }
                        ::operator delete(new_bytes, std::align_val_t{plan_->layout.alignment});
                    });
                    for (moved = 0; moved < size_; ++moved)
                    {
                        plan_->move_construct(static_cast<std::byte *>(new_bytes) + moved * element_size,
                                              slot_address(moved));
                    }
                    rollback.release();
                    // Destroy the old slots; their resources have been moved.
                    for (std::size_t i = size_; i > 0; --i) { plan_->destroy(slot_address(i - 1)); }
                }

                if (bytes_ != nullptr)
                {
                    ::operator delete(bytes_, std::align_val_t{plan_->layout.alignment});
                }
                bytes_    = new_bytes;
                capacity_ = new_capacity;
            }

            const MemoryUtils::StoragePlan *plan_{nullptr};
            void                           *bytes_{nullptr};
            std::size_t                     capacity_{0};
            std::size_t                     size_{0};
            std::vector<bool>            validity_{};
        };
    }  // namespace builder_detail

    // -----------------------------------------------------------------
    // ListBuilder
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator that produces an immutable ``ListStorage``
     * sized to the number of elements pushed onto it.
     */
    class ListBuilder
    {
      public:
        explicit ListBuilder(const ValueTypeRef &element_binding)
            : element_binding_{element_binding}, accumulator_{element_binding.checked_plan()}
        {
        }

        void push_back_copy(const void *src)
        {
            ensure_not_built();
            accumulator_.push_back_copy(src);
        }

        /** Append an UNSET element (a hole) - element validity. */
        void push_back_unset()
        {
            ensure_not_built();
            accumulator_.push_back_unset();
        }

        template <typename T>
        void push_back(const T &value)
        {
            push_back_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }
        [[nodiscard]] bool        empty() const noexcept { return accumulator_.empty(); }

        [[nodiscard]] ListStorage build_storage()
        {
            ensure_not_built();
            ListStorage storage{element_binding_, accumulator_.as_span(), accumulator_.validity()};
            accumulator_.clear();
            built_ = true;
            return storage;
        }

        [[nodiscard]] Value build()
        {
            ListStorage storage = build_storage();
            return Value{compact_list_type(element_binding_), &storage};
        }

      private:
        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("ListBuilder is single-use"); }
        }

        ValueTypeRef element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
        bool                                  built_{false};
    };

    // -----------------------------------------------------------------
    // CyclicBufferBuilder — fixed declared capacity, oldest evicted on
    // overflow.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``CyclicBufferStorage``. The declared
     * capacity is fixed at construction; ``push_back`` rotates after
     * the capacity is reached, preserving a sliding window of the most
     * recent ``capacity`` elements. The build's ``head`` matches the
     * logical oldest position.
     */
    class CyclicBufferBuilder
    {
      public:
        CyclicBufferBuilder(const ValueTypeRef &element_binding, std::size_t capacity)
            : element_binding_{element_binding}
            , accumulator_{element_binding.checked_plan()}
            , capacity_{capacity}
        {
            if (capacity == 0) { throw std::invalid_argument("CyclicBufferBuilder capacity must be greater than zero"); }
        }

        void push_back_copy(const void *src)
        {
            ensure_not_built();
            if (accumulator_.size() < capacity_)
            {
                accumulator_.push_back_copy(src);
            }
            else
            {
                // Replace the slot at `head_` (oldest) with the new value
                // by destroying it in place and copy-constructing into
                // its memory; advance the head.
                const auto &plan = element_binding_.checked_plan();
                if (!plan.can_copy_assign())
                {
                    throw std::logic_error(
                        "CyclicBufferBuilder replacement requires a copy-assignable element plan");
                }
                plan.copy_assign(accumulator_.at(head_), src);
                head_ = (head_ + 1) % capacity_;
            }
        }

        template <typename T>
        void push_back(const T &value)
        {
            push_back_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }
        [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

        [[nodiscard]] CyclicBufferStorage build_storage()
        {
            ensure_not_built();
            CyclicBufferStorage storage{element_binding_, accumulator_.as_span(), head_};
            accumulator_.clear();
            built_ = true;
            return storage;
        }

        [[nodiscard]] Value build()
        {
            CyclicBufferStorage storage = build_storage();
            return Value{compact_cyclic_buffer_type(element_binding_, capacity_), &storage};
        }

      private:
        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("CyclicBufferBuilder is single-use"); }
        }

        ValueTypeRef element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
        std::size_t                          capacity_{0};
        std::size_t                          head_{0};
        bool                                  built_{false};
    };

    // -----------------------------------------------------------------
    // QueueBuilder — bounded or unbounded FIFO; rejects on overflow
    // when bounded.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``QueueStorage``. ``push`` appends; if a
     * non-zero ``max_capacity`` was declared, pushing past it throws.
     * ``max_capacity == 0`` means unbounded.
     */
    class QueueBuilder
    {
      public:
        QueueBuilder(const ValueTypeRef &element_binding, std::size_t max_capacity = 0)
            : element_binding_{element_binding}
            , accumulator_{element_binding.checked_plan()}
            , max_capacity_{max_capacity}
        {
        }

        void push_copy(const void *src)
        {
            ensure_not_built();
            if (max_capacity_ != 0 && accumulator_.size() >= max_capacity_)
            {
                throw std::overflow_error("QueueBuilder: bounded queue is full");
            }
            accumulator_.push_back_copy(src);
        }

        template <typename T>
        void push(const T &value)
        {
            push_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }
        [[nodiscard]] std::size_t max_capacity() const noexcept { return max_capacity_; }

        [[nodiscard]] QueueStorage build_storage()
        {
            ensure_not_built();
            QueueStorage storage{element_binding_, accumulator_.as_span()};
            accumulator_.clear();
            built_ = true;
            return storage;
        }

        [[nodiscard]] Value build()
        {
            QueueStorage storage = build_storage();
            return Value{compact_queue_type(element_binding_, max_capacity_), &storage};
        }

      private:
        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("QueueBuilder is single-use"); }
        }

        ValueTypeRef element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
        std::size_t                          max_capacity_{0};
        bool                                  built_{false};
    };

    // -----------------------------------------------------------------
    // SetBuilder — content-keyed; ``insert`` is no-op on duplicate.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``SetStorage``. ``insert`` deduplicates
     * by content using the bound element ops; subsequent inserts of
     * existing keys are no-ops. ``build_storage`` produces a compact
     * set sized to the number of unique keys.
     */
    class SetBuilder
    {
      public:
        explicit SetBuilder(const ValueTypeRef &element_binding)
            : element_binding_{element_binding}, accumulator_{element_binding.checked_plan()}
        {
            const auto *meta = element_binding.schema();
            if (meta == nullptr || !meta->is_hashable() || !meta->is_equatable())
            {
                throw std::logic_error("SetBuilder requires a hashable and equatable element binding");
            }
            index_.reset(slot_context(), 0);
        }

        SetBuilder(const SetBuilder &)            = delete;
        SetBuilder &operator=(const SetBuilder &) = delete;

        SetBuilder(SetBuilder &&other)
            : element_binding_{other.element_binding_}
            , accumulator_{std::move(other.accumulator_)}
            , index_{std::move(other.index_)}
            , built_{other.built_}
        {
            index_.rebind(slot_context());
            other.element_binding_ = nullptr;
            other.built_           = true;
        }

        SetBuilder &operator=(SetBuilder &&other)
        {
            if (this != &other)
            {
                element_binding_ = other.element_binding_;
                accumulator_     = std::move(other.accumulator_);
                index_           = std::move(other.index_);
                built_           = other.built_;
                index_.rebind(slot_context());
                other.element_binding_ = nullptr;
                other.built_           = true;
            }
            return *this;
        }

        bool insert_copy(const void *src)
        {
            ensure_not_built();
            if (index_.contains(src)) { return false; }
            const auto slot = accumulator_.size();
            accumulator_.push_back_copy(src);
            auto rollback = make_scope_exit([&]() noexcept { accumulator_.pop_back(); });
            if (!index_.insert(slot)) { throw std::logic_error("SetBuilder index rejected a unique key"); }
            rollback.release();
            return true;
        }

        template <typename T>
        bool insert(const T &value)
        {
            return insert_copy(static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] bool contains(const void *src) const
        {
            return index_.contains(src);
        }

        [[nodiscard]] std::size_t size() const noexcept { return accumulator_.size(); }

        [[nodiscard]] SetStorage build_storage()
        {
            ensure_not_built();
            SetStorage storage{element_binding_, accumulator_.as_span()};
            accumulator_.clear();
            index_.clear();
            built_ = true;
            return storage;
        }

        [[nodiscard]] Value build()
        {
            SetStorage storage = build_storage();
            return Value{compact_set_type(element_binding_), &storage};
        }

      private:
        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("SetBuilder is single-use"); }
        }

        [[nodiscard]] compact_detail::SlotIndexContext slot_context() const noexcept
        {
            return compact_detail::SlotIndexContext{
                .binding = element_binding_,
                .owner   = this,
                .at      = &SetBuilder::slot_at_context,
            };
        }

        [[nodiscard]] static const void *slot_at_context(const void *owner, std::size_t slot) noexcept
        {
            return static_cast<const SetBuilder *>(owner)->accumulator_.at(slot);
        }

        ValueTypeRef element_binding_{nullptr};
        builder_detail::ElementAccumulator   accumulator_;
        compact_detail::SlotIndex            index_{};
        bool                                  built_{false};
    };

    // -----------------------------------------------------------------
    // MapBuilder — content-keyed; later writes overwrite earlier
    // values for the same key.
    // -----------------------------------------------------------------

    /**
     * Mutable accumulator for ``MapStorage``. ``set_item`` accumulates
     * key/value pairs; if an earlier write for the same key exists,
     * its value is replaced (the map remains content-keyed).
     * ``build_storage`` produces a compact map sized to the number of
     * unique keys.
     */
    class MapBuilder
    {
      public:
        MapBuilder(const ValueTypeRef &key_binding, const ValueTypeRef &value_binding)
            : key_binding_{key_binding}
            , value_binding_{value_binding}
            , key_acc_{key_binding.checked_plan()}
            , value_acc_{value_binding.checked_plan()}
        {
            const auto *km = key_binding.schema();
            if (km == nullptr || !km->is_hashable() || !km->is_equatable())
            {
                throw std::logic_error("MapBuilder requires a hashable and equatable key binding");
            }
            index_.reset(slot_context(), 0);
        }

        MapBuilder(const MapBuilder &)            = delete;
        MapBuilder &operator=(const MapBuilder &) = delete;

        MapBuilder(MapBuilder &&other)
            : key_binding_{other.key_binding_}
            , value_binding_{other.value_binding_}
            , key_acc_{std::move(other.key_acc_)}
            , value_acc_{std::move(other.value_acc_)}
            , index_{std::move(other.index_)}
            , built_{other.built_}
        {
            index_.rebind(slot_context());
            other.key_binding_   = nullptr;
            other.value_binding_ = nullptr;
            other.built_         = true;
        }

        MapBuilder &operator=(MapBuilder &&other)
        {
            if (this != &other)
            {
                key_binding_   = other.key_binding_;
                value_binding_ = other.value_binding_;
                key_acc_       = std::move(other.key_acc_);
                value_acc_     = std::move(other.value_acc_);
                index_         = std::move(other.index_);
                built_         = other.built_;
                index_.rebind(slot_context());
                other.key_binding_   = nullptr;
                other.value_binding_ = nullptr;
                other.built_         = true;
            }
            return *this;
        }

        void set_item_copy(const void *key, const void *value)
        {
            ensure_not_built();
            if (auto found = find_slot(key); found.has_value())
            {
                const auto &vp = value_binding_.checked_plan();
                if (!vp.can_copy_assign())
                {
                    throw std::logic_error("MapBuilder value replacement requires a copy-assignable value plan");
                }
                vp.copy_assign(value_acc_.at(*found), value);
                return;
            }
            const auto slot = key_acc_.size();
            key_acc_.push_back_copy(key);
            auto key_rollback = make_scope_exit([&]() noexcept { key_acc_.pop_back(); });
            value_acc_.push_back_copy(value);
            auto value_rollback = make_scope_exit([&]() noexcept { value_acc_.pop_back(); });
            if (!index_.insert(slot)) { throw std::logic_error("MapBuilder index rejected a unique key"); }
            value_rollback.release();
            key_rollback.release();
        }

        template <typename K, typename V>
        void set_item(const K &key, const V &value)
        {
            set_item_copy(static_cast<const void *>(std::addressof(key)),
                          static_cast<const void *>(std::addressof(value)));
        }

        [[nodiscard]] bool contains(const void *key) const { return find_slot(key).has_value(); }

        [[nodiscard]] std::size_t size() const noexcept { return key_acc_.size(); }

        /** Record ``key`` with an UNSET value - a None-valued mapping entry
            (value holes; element validity). Duplicate keys are rejected
            (dict/JSON sources never produce them for the unset form). */
        void set_item_unset(const void *key)
        {
            ensure_not_built();
            if (find_slot(key).has_value())
            {
                throw std::logic_error("MapBuilder::set_item_unset cannot replace an existing entry");
            }
            const auto slot = key_acc_.size();
            key_acc_.push_back_copy(key);
            auto key_rollback = make_scope_exit([&]() noexcept { key_acc_.pop_back(); });
            value_acc_.push_back_unset();
            auto value_rollback = make_scope_exit([&]() noexcept { value_acc_.pop_back(); });
            if (!index_.insert(slot)) { throw std::logic_error("MapBuilder index rejected a unique key"); }
            value_rollback.release();
            key_rollback.release();
        }

        [[nodiscard]] MapStorage build_storage()
        {
            ensure_not_built();
            MapStorage storage{key_binding_, value_binding_, key_acc_.as_span(), value_acc_.as_span(),
                               value_acc_.validity()};
            key_acc_.clear();
            value_acc_.clear();
            index_.clear();
            built_ = true;
            return storage;
        }

        [[nodiscard]] Value build()
        {
            MapStorage storage = build_storage();
            return Value{compact_map_type(key_binding_, value_binding_), &storage};
        }

      private:
        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("MapBuilder is single-use"); }
        }

        [[nodiscard]] std::optional<std::size_t> find_slot(const void *key) const
        {
            const auto slot = index_.find(key);
            if (!slot.has_value()) { return std::nullopt; }
            return static_cast<std::size_t>(*slot);
        }

        [[nodiscard]] compact_detail::SlotIndexContext slot_context() const noexcept
        {
            return compact_detail::SlotIndexContext{
                .binding = key_binding_,
                .owner   = this,
                .at      = &MapBuilder::key_at_context,
            };
        }

        [[nodiscard]] static const void *key_at_context(const void *owner, std::size_t slot) noexcept
        {
            return static_cast<const MapBuilder *>(owner)->key_acc_.at(slot);
        }

        ValueTypeRef key_binding_{nullptr};
        ValueTypeRef value_binding_{nullptr};
        builder_detail::ElementAccumulator   key_acc_;
        builder_detail::ElementAccumulator   value_acc_;
        compact_detail::SlotIndex            index_{};
        bool                                  built_{false};
    };

    // -----------------------------------------------------------------
    // BundleBuilder — assemble a compact (immutable) Bundle / Tuple
    // ``Value`` from prebuilt field ``Value``s.
    //
    // Composite fields cannot be populated through ``begin_mutation`` when
    // they are themselves immutable containers (``Set`` / ``Map`` / …):
    // ``MutableIndexedValueView::at`` begin-mutates each field, which an
    // immutable field refuses. Instead each field is set by a whole-value
    // ``copy_assign`` / ``move_assign`` at its layout offset, over the
    // default-constructed field. This is how the canonical delta bundles
    // (``Bundle{added: Set<T>, removed: Set<T>}`` for ``TSS``, the per-field
    // delta bundle for ``TSB``) are constructed for tests/wiring so they
    // match the runtime ``delta_value_schema`` exactly.
    // -----------------------------------------------------------------
    class BundleBuilder
    {
      public:
        explicit BundleBuilder(const ValueTypeRef &bundle_binding)
            : target_binding_{bundle_binding},
              binding_{assembly_binding(bundle_binding)},
              value_{binding_}
        {
        }

        /** Return the composite field-assembly binding for ``target``. */
        [[nodiscard]] static ValueTypeRef assembly_type(ValueTypeRef target)
        {
            return assembly_binding(target);
        }

        /** Set field ``index`` to a copy of ``field`` (whole-value copy-assign). */
        BundleBuilder &set(std::size_t index, const ValueView &field)
        {
            ensure_not_built();
            if (!field.has_value())
            {
                throw std::invalid_argument("BundleBuilder::set(ValueView) requires a live field value");
            }
            const auto &comp = component(index);
            const auto destination = field_binding(index);
            destination.ops_ref().copy_assign_from(
                destination, field_memory(comp.offset), field.binding(), field.data());
            mark_field(index);
            return *this;
        }

        /** Set the named field ``name`` to a copy of ``field``. */
        BundleBuilder &set(std::string_view name, const ValueView &field) { return set(index_of(name), field); }

        /** Set field ``index`` by moving an owned ``field`` value into the bundle. */
        BundleBuilder &set(std::size_t index, Value &&field)
        {
            ensure_not_built();
            if (!field.has_value())
            {
                throw std::invalid_argument("BundleBuilder::set(Value&&) requires a live field value");
            }
            const auto &comp = component(index);
            const auto destination = field_binding(index);
            const auto source = field.view().binding();
            destination.ops_ref().move_assign_from(
                destination, field_memory(comp.offset), source,
                const_cast<void *>(field.view().data()));
            mark_field(index);
            return *this;
        }

        /** Set the named field ``name`` by moving an owned ``field`` value into the bundle. */
        BundleBuilder &set(std::string_view name, Value &&field) { return set(index_of(name), std::move(field)); }

        [[nodiscard]] std::size_t size() const noexcept { return field_count(); }

        /** Bundle field validity (core_concepts.rst): set() marks the field
            live; Tuple composites are dense (no validity component). */
        void mark_field(std::size_t index)
        {
            const auto *meta = binding_.schema();
            if (meta == nullptr || meta->field_count == 0 ||
                (meta->value_kind() != ValueTypeKind::Bundle && meta->value_kind() != ValueTypeKind::Tuple))
            {
                return;
            }
            if (index >= meta->field_count) { throw std::out_of_range("BundleBuilder: field index out of range"); }
            const auto &plan = binding_.checked_plan();
            if (plan.component_count() <= meta->field_count) { return; }
            const auto components = plan.components();
            auto *words = static_cast<std::uint64_t *>(field_memory(components[meta->field_count].offset));
            constexpr std::size_t bits_per_word = sizeof(std::uint64_t) * 8U;
            words[index / bits_per_word] |= std::uint64_t{1} << (index % bits_per_word);
        }

        [[nodiscard]] Value build()
        {
            ensure_not_built();
            built_ = true;
            if (target_binding_ == binding_) { return std::move(value_); }

            Value result{target_binding_};
            target_binding_.ops_ref().move_assign_from(
                target_binding_,
                const_cast<void *>(result.view().data()),
                binding_,
                const_cast<void *>(value_.view().data()));
            return result;
        }

      private:
        [[nodiscard]] static ValueTypeRef assembly_binding(ValueTypeRef target)
        {
            if (!target)
            {
                throw std::invalid_argument("BundleBuilder requires a bound target");
            }
            if (target.checked_plan().is_composite()) { return target; }
            const auto *schema = target.schema();
            if (schema == nullptr || schema->try_value_kind() != ValueTypeKind::Bundle ||
                schema->wrapped_un_named == nullptr)
            {
                throw std::logic_error(
                    "BundleBuilder requires composite storage or a named Bundle with a structural twin");
            }
            const auto *indexed =
                checked_value_ops<IndexedValueOps>(target, "BundleBuilder target");
            std::vector<ValueTypeRef> fields;
            fields.reserve(schema->field_count);
            for (std::size_t index = 0; index < schema->field_count; ++index)
            {
                const auto field =
                    indexed->element_binding(indexed->context, nullptr, index);
                if (!field)
                {
                    throw std::logic_error(
                        "BundleBuilder target has an unresolved field binding");
                }
                fields.push_back(field);
            }
            const auto structural =
                ValuePlanFactory::instance().realized_composite_type_for(
                    schema->wrapped_un_named, fields);
            if (!structural || !structural.checked_plan().is_composite())
            {
                throw std::logic_error(
                    "BundleBuilder structural assembly binding is unavailable");
            }
            return structural;
        }

        [[nodiscard]] const MemoryUtils::CompositeState &state() const
        {
            const auto *s =
                static_cast<const MemoryUtils::CompositeState *>(binding_.checked_plan().lifecycle_context);
            if (s == nullptr) { throw std::logic_error("BundleBuilder: binding has no composite state"); }
            return *s;
        }

        [[nodiscard]] const MemoryUtils::CompositeComponent &component(std::size_t index) const
        {
            const auto &st = state();
            if (index >= field_count()) { throw std::out_of_range("BundleBuilder: field index out of range"); }
            return st.components()[index];
        }

        [[nodiscard]] ValueTypeRef field_binding(std::size_t index) const
        {
            const auto *ops = checked_value_ops<IndexedValueOps>(binding_, "BundleBuilder");
            const auto field = ops->element_binding(ops->context, value_.view().data(), index);
            if (!field) { throw std::logic_error("BundleBuilder: field binding is unresolved"); }
            return field;
        }

        [[nodiscard]] std::size_t index_of(std::string_view name) const
        {
            const auto &st = state();
            for (std::size_t i = 0; i < field_count(); ++i)
            {
                const auto &c = st.components()[i];
                if (c.name != nullptr && name == c.name) { return i; }
            }
            throw std::out_of_range("BundleBuilder: field name not found");
        }

        // The value_ storage is owned and being assembled here, so writing
        // through its raw base pointer is sound (no begin_mutation gating).
        [[nodiscard]] void *field_memory(std::size_t offset)
        {
            return static_cast<std::byte *>(const_cast<void *>(value_.view().data())) + offset;
        }

        [[nodiscard]] std::size_t field_count() const noexcept
        {
            const auto *meta = binding_.schema();
            if (meta != nullptr && meta->try_value_kind() == ValueTypeKind::Bundle) { return meta->field_count; }
            return state().component_count;
        }

        void ensure_not_built() const
        {
            if (built_) { throw std::logic_error("BundleBuilder is single-use"); }
        }

        ValueTypeRef target_binding_{nullptr};
        ValueTypeRef binding_{nullptr};
        Value                   value_{};
        bool                    built_{false};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_BUILDER_H
