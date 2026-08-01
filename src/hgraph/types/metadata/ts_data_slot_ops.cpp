#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/time_series/ts_data/empty_delta_fields.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include "../time_series/ts_data/ownership.h"
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <sul/dynamic_bitset.hpp>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <iterator>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    namespace
    {
        template <typename SourceBinding, typename SourceMemory>
        void copy_projected_bundle_fields(const ValueTypeRef &binding,
                                          void *destination_memory,
                                          std::size_t field_count,
                                          SourceBinding source_binding,
                                          SourceMemory source_memory)
        {
            const auto &plan = binding.checked_plan();
            const auto components = plan.components();
            for (std::size_t index = 0; index < field_count; ++index)
            {
                const auto destination = ValuePlanFactory::instance().type_for(
                    binding.schema()->fields[index].type);
                const auto source = source_binding(index);
                if (!destination || !source)
                {
                    throw std::logic_error("projected bundle field binding is unresolved");
                }
                const auto &source_ops = source.ops_ref();
                const auto owning_source = source_ops.owning_type(source);
                if (destination.plan() != owning_source.plan())
                {
                    throw std::logic_error("projected bundle field has an incompatible owning binding");
                }
                source_ops.copy_assign_view(
                    destination,
                    MemoryUtils::advance(destination_memory, components[index].offset),
                    source_memory(index));
            }

            // Bundle validity is the trailing composite component. Projected
            // TS deltas always expose every canonical delta field, including
            // explicitly-empty added/removed collections.
            if (components.size() > field_count)
            {
                auto *words = MemoryUtils::cast<std::uint64_t>(
                    MemoryUtils::advance(destination_memory, components[field_count].offset));
                constexpr std::size_t bits_per_word = sizeof(std::uint64_t) * 8U;
                for (std::size_t index = 0; index < field_count; ++index)
                {
                    words[index / bits_per_word] |= std::uint64_t{1} << (index % bits_per_word);
                }
            }
        }

        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        template <typename Block, typename Allocator>
        [[nodiscard]] DynamicStorageMetrics dynamic_bitset_metrics(
            const sul::dynamic_bitset<Block, Allocator> &bits) noexcept
        {
            return {
                .live_bytes = bits.num_blocks() * sizeof(Block),
                .reserved_bytes = (bits.capacity() / bits.bits_per_block) * sizeof(Block),
            };
        }

        template <typename Storage, typename Member>
        [[nodiscard]] std::size_t debug_offset(const Storage &storage, const Member &member) noexcept
        {
            return static_cast<std::size_t>(reinterpret_cast<const std::byte *>(&member) -
                                            reinterpret_cast<const std::byte *>(&storage));
        }

        template <typename Storage>
        [[nodiscard]] std::size_t debug_address_offset(const Storage &storage, const void *address) noexcept
        {
            return static_cast<std::size_t>(static_cast<const std::byte *>(address) -
                                            reinterpret_cast<const std::byte *>(&storage));
        }

        template <typename Storage>
        [[nodiscard]] DebugDynamicLayout tss_value_debug_layout(const Storage &sample,
                                                                 const ValueTypeRef &key_binding)
        {
            const auto &keys = sample.keys();
            const StableSlotDebugView slots = keys.debug_slot_view();
            DebugDynamicFlags flags = DebugDynamicFlags::DataIsIndirect |
                                      DebugDynamicFlags::DataIsPointerTable |
                                      DebugDynamicFlags::HasSlotState |
                                      DebugDynamicFlags::SlotIndexIsIndirect |
                                      DebugDynamicFlags::SlotSizeIsIndirect;
            if (slots.pointers_tagged) { flags = flags | DebugDynamicFlags::DataPointersAreTagged; }
            if (slots.state_tagged) { flags = flags | DebugDynamicFlags::SlotStateIsTaggedPointer; }
            return DebugDynamicLayout{
                .magic = DEBUG_DYNAMIC_LAYOUT_MAGIC,
                .abi_version = DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
                .kind = DebugDynamicKind::StableSlots,
                .flags = flags,
                .size_offset = slots.slot_count_offset,
                .data_offset = debug_address_offset(sample, slots.implementation_pointer),
                .stride = key_binding.checked_plan().layout.size,
                .state_offset = slots.state_offset,
                .auxiliary_offset = slots.pointer_table_offset,
            };
        }

        class TSSSlotStorage
        {
          public:
            explicit TSSSlotStorage(const ValueTypeRef &key_binding)
                : key_binding_(key_binding),
                  keys_(key_binding)
            {}

            // Slot-backed TSData is published by address through TSDataView
            // and TSParentLink. Once a slot or root storage address is visible,
            // growth and mutation must preserve it; generic relocation is not
            // a valid operation for this storage shape.
            TSSSlotStorage(const TSSSlotStorage &)            = delete;
            TSSSlotStorage &operator=(const TSSSlotStorage &) = delete;
            TSSSlotStorage(TSSSlotStorage &&)                 = delete;
            TSSSlotStorage &operator=(TSSSlotStorage &&)      = delete;
            ~TSSSlotStorage() = default;

            [[nodiscard]] ValueTypeRef key_binding() const { return key_binding_; }
            [[nodiscard]] const TSDataTracking &tracking() const noexcept { return tracking_; }
            [[nodiscard]] TSDataTracking &mutable_tracking() noexcept { return tracking_; }
            [[nodiscard]] const KeySlotStore &keys() const noexcept { return keys_; }
            [[nodiscard]] KeySlotStore &keys() noexcept { return keys_; }
            [[nodiscard]] std::size_t size() const noexcept { return keys_.size(); }
            [[nodiscard]] std::size_t slot_capacity() const noexcept { return keys_.slot_capacity(); }
            [[nodiscard]] DynamicStorageMetrics key_set_dynamic_storage_metrics() const noexcept
            {
                DynamicStorageMetrics result = keys_.dynamic_storage_metrics();
                result += dynamic_bitset_metrics(added_);
                result += dynamic_bitset_metrics(removed_);
                const auto &ops = key_binding_.ops_ref();
                for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
                {
                    if (keys_.slot_constructed(slot))
                    {
                        result += ops.dynamic_storage_metrics(keys_.key_memory(slot));
                    }
                }
                return result;
            }
            [[nodiscard]] bool slot_occupied(std::size_t slot) const noexcept { return keys_.slot_constructed(slot); }
            [[nodiscard]] bool slot_live(std::size_t slot) const noexcept { return keys_.slot_live(slot); }
            [[nodiscard]] bool slot_added(std::size_t slot) const noexcept
            {
                return slot < added_.size() && added_.test(slot);
            }
            [[nodiscard]] bool slot_removed(std::size_t slot) const noexcept
            {
                return slot < removed_.size() && removed_.test(slot);
            }
            [[nodiscard]] std::size_t next_added_slot(std::size_t previous) const noexcept
            {
                return next_delta_slot(added_, previous);
            }
            [[nodiscard]] std::size_t next_removed_slot(std::size_t previous) const noexcept
            {
                return next_delta_slot(removed_, previous);
            }
            [[nodiscard]] const void *key_at_slot(std::size_t slot) const { return keys_[slot]; }
            [[nodiscard]] std::size_t find_slot(const ValueView &key) const
            {
                const auto slot = keys_.find_slot(key);
                return slot == KeySlotStore::npos ? TS_DATA_NO_CHILD_ID : slot;
            }
            [[nodiscard]] bool contains(const ValueView &key) const
            {
                return keys_.contains(key);
            }

            void reserve(std::size_t capacity)
            {
                keys_.reserve_to(capacity);
                ensure_delta_capacity();
            }

            [[nodiscard]] bool touch(DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);
                return tracking_.last_modified_time != modified_time;
            }

            [[nodiscard]] SlotTSDataMutationResult insert_key(const ValueView &key, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                const auto result = keys_.insert(key);
                ensure_delta_capacity();
                if (!result.inserted) { return {.slot = result.slot, .changed = false}; }

                if (slot_removed(result.slot)) { removed_.reset(result.slot); }
                else { added_.set(result.slot); }
                return mutation_result(result.slot, result.constructed);
            }

            [[nodiscard]] SlotTSDataMutationResult insert_key_move(const ValueView &key, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                // The enclosing TSData move owns the source. Compact container
                // iteration still exposes read-only element views, so the
                // ownership fact is applied explicitly at this boundary.
                const auto result = key.binding() == key_binding_
                                        ? keys_.insert_move(const_cast<void *>(key.data()))
                                        : keys_.insert(key);
                ensure_delta_capacity();
                if (!result.inserted) { return {.slot = result.slot, .changed = false}; }

                if (slot_removed(result.slot)) { removed_.reset(result.slot); }
                else { added_.set(result.slot); }
                return mutation_result(result.slot, result.constructed);
            }

            [[nodiscard]] SlotTSDataMutationResult remove_key(const ValueView &key, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                const auto slot = keys_.find_slot(key);
                if (slot == KeySlotStore::npos) { return {.slot = TS_DATA_NO_CHILD_ID, .changed = false}; }
                if (!keys_.remove_slot(slot)) { return {.slot = slot, .changed = false}; }

                ensure_delta_capacity();
                if (slot_added(slot)) { added_.reset(slot); }
                else { removed_.set(slot); }
                return mutation_result(slot);
            }

            [[nodiscard]] SlotTSDataMutationResult remove_slot(std::size_t slot, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                if (slot == KeySlotStore::npos || !keys_.slot_live(slot))
                {
                    return {.slot = slot, .changed = false};
                }
                if (!keys_.remove_slot(slot)) { return {.slot = slot, .changed = false}; }

                ensure_delta_capacity();
                if (slot_added(slot)) { added_.reset(slot); }
                else { removed_.set(slot); }
                return mutation_result(slot);
            }

            void reset_delta() noexcept
            {
                added_.reset();
                removed_.reset();
                delta_time_ = MIN_DT;
            }

            void add_slot_observer(SlotObserver *observer)
            {
                keys_.add_slot_observer(observer);
            }

            void remove_slot_observer(SlotObserver *observer)
            {
                keys_.remove_slot_observer(observer);
            }

          protected:
            [[nodiscard]] static std::size_t next_delta_slot(const sul::dynamic_bitset<> &slots,
                                                             std::size_t previous) noexcept
            {
                const std::size_t slot = previous == TS_DATA_NO_CHILD_ID
                                             ? slots.find_first()
                                             : slots.find_next(previous);
                return slot == sul::dynamic_bitset<>::npos ? TS_DATA_NO_CHILD_ID : slot;
            }

            void validate_mutation_time(DateTime modified_time) const
            {
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("slot TSData mutation requires a concrete evaluation time");
                }
            }

            void prepare_delta(DateTime modified_time)
            {
                // Delta windows are monotonic: only a NEWER time rolls the
                // window. A record carrying an older time (a freshly bound
                // link replaying its source's history) joins the current
                // window — rebasing on it would erase sibling marks already
                // recorded this cycle.
                if (modified_time <= delta_time_)
                {
                    ensure_delta_capacity();
                    return;
                }

                keys_.erase_pending();
                reset_delta();
                delta_time_ = modified_time;
                ensure_delta_capacity();
            }

            void ensure_delta_capacity()
            {
                const auto capacity = keys_.slot_capacity();
                if (added_.size() == capacity) { return; }
                added_.resize(capacity);
                removed_.resize(capacity);
            }

            [[nodiscard]] SlotTSDataMutationResult mutation_result(std::size_t slot,
                                                                    bool constructed = false) const noexcept
            {
                return {.slot = slot, .changed = true, .constructed = constructed};
            }

            ValueTypeRef key_binding_{nullptr};
            TSDataTracking         tracking_{};
            KeySlotStore           keys_;
            sul::dynamic_bitset<>  added_{};
            sul::dynamic_bitset<>  removed_{};
            DateTime          delta_time_{MIN_DT};
        };

        class TSDSlotStorage
        {
          public:
            TSDSlotStorage(const ValueTypeRef &key_binding, TSRoleTypeRef element_type)
                : key_binding_(key_binding),
                  element_type_(element_type),
                  keys_(key_binding),
                  values_(keys_, element_type.checked_plan())
            {}

            // A TSD publishes both key slots and child TSData slots. Children
            // must be mutated in place so existing input/proxy bindings and
            // parent links remain valid across structural updates.
            TSDSlotStorage(const TSDSlotStorage &)            = delete;
            TSDSlotStorage &operator=(const TSDSlotStorage &) = delete;
            TSDSlotStorage(TSDSlotStorage &&)                 = delete;
            TSDSlotStorage &operator=(TSDSlotStorage &&)      = delete;
            ~TSDSlotStorage() = default;

            [[nodiscard]] ValueTypeRef key_binding() const { return key_binding_; }
            [[nodiscard]] TSRoleTypeRef element_type() const noexcept { return element_type_; }
            [[nodiscard]] const TSDataTracking &tracking() const noexcept { return tracking_; }
            [[nodiscard]] TSDataTracking &mutable_tracking() noexcept { return tracking_; }
            /**
             * Dedicated tracking for the KEY-SET projection — its OWN modified
             * time and OWN observer set: membership changes (insert/remove)
             * record + notify here, so key-set subscribers are woken only by
             * actual set changes, never by the dictionary's value ticks.
             */
            [[nodiscard]] const TSDataTracking &key_set_tracking() const noexcept { return key_set_tracking_; }
            [[nodiscard]] TSDataTracking &mutable_key_set_tracking() noexcept { return key_set_tracking_; }
            [[nodiscard]] const KeySlotStore &keys() const noexcept { return keys_; }
            [[nodiscard]] KeySlotStore &keys() noexcept { return keys_; }
            [[nodiscard]] std::size_t size() const noexcept { return keys_.size(); }
            [[nodiscard]] std::size_t slot_capacity() const noexcept { return keys_.slot_capacity(); }
            [[nodiscard]] DynamicStorageMetrics key_set_dynamic_storage_metrics() const noexcept
            {
                DynamicStorageMetrics result = keys_.dynamic_storage_metrics();
                result += dynamic_bitset_metrics(added_);
                result += dynamic_bitset_metrics(removed_);
                const auto &ops = key_binding_.ops_ref();
                for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
                {
                    if (keys_.slot_constructed(slot))
                    {
                        result += ops.dynamic_storage_metrics(keys_.key_memory(slot));
                    }
                }
                return result;
            }
            [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
            {
                DynamicStorageMetrics result = key_set_dynamic_storage_metrics();
                result += values_.dynamic_storage_metrics();
                result += dynamic_bitset_metrics(modified_);
                result += dynamic_bitset_metrics(value_published_);

                const TSDataOps *child_ops = element_type_.ops();
                if (child_ops == nullptr) { return result; }
                for (std::size_t slot = 0; slot < keys_.slot_capacity(); ++slot)
                {
                    if (!keys_.slot_constructed(slot)) { continue; }
                    result += child_ops->dynamic_storage_metrics_impl(
                        child_ops->context, values_.value_memory(slot));
                }
                return result;
            }
            [[nodiscard]] bool slot_occupied(std::size_t slot) const noexcept { return keys_.slot_constructed(slot); }
            [[nodiscard]] bool slot_live(std::size_t slot) const noexcept { return keys_.slot_live(slot); }
            [[nodiscard]] bool slot_added(std::size_t slot) const noexcept
            {
                return slot < added_.size() && added_.test(slot);
            }
            [[nodiscard]] bool slot_removed(std::size_t slot) const noexcept
            {
                return slot < removed_.size() && removed_.test(slot);
            }
            [[nodiscard]] std::size_t next_added_slot(std::size_t previous) const noexcept
            {
                return next_delta_slot(added_, previous);
            }
            [[nodiscard]] std::size_t next_removed_slot(std::size_t previous) const noexcept
            {
                return next_delta_slot(removed_, previous);
            }
            [[nodiscard]] bool slot_modified(std::size_t slot) const noexcept
            {
                return slot < modified_.size() && modified_.test(slot);
            }
            [[nodiscard]] std::size_t next_modified_slot(std::size_t previous) const noexcept
            {
                return next_delta_slot(modified_, previous);
            }
            [[nodiscard]] bool slot_value_published(std::size_t slot) const noexcept
            {
                return slot < value_published_.size() && value_published_.test(slot);
            }
            [[nodiscard]] const void *key_at_slot(std::size_t slot) const { return keys_[slot]; }
            [[nodiscard]] std::size_t find_slot(const ValueView &key) const
            {
                const auto slot = keys_.find_slot(key);
                return slot == KeySlotStore::npos ? TS_DATA_NO_CHILD_ID : slot;
            }
            [[nodiscard]] bool contains(const ValueView &key) const
            {
                return keys_.contains(key);
            }
            [[nodiscard]] const void *child_at_slot(std::size_t slot) const
            {
                return values_.value_memory(slot);
            }
            [[nodiscard]] const ValueSlotStore &debug_values() const noexcept
            {
                return values_.debug_values();
            }
            [[nodiscard]] void *child_memory_for_write(std::size_t slot)
            {
                return values_.value_memory(slot);
            }
            [[nodiscard]] bool child_has_current_value(std::size_t slot) const
            {
                const auto &ops = element_type_.ops_ref();
                return ops.has_current_value_impl(ops.context, values_.value_memory(slot));
            }

            void reserve(std::size_t capacity)
            {
                keys_.reserve_to(capacity);
                ensure_delta_capacity();
            }

            [[nodiscard]] bool touch(DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);
                return tracking_.last_modified_time != modified_time;
            }

            [[nodiscard]] SlotTSDataMutationResult insert_key(const ValueView &key, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                const auto result = keys_.insert(key);
                ensure_delta_capacity();
                if (!result.inserted) { return {.slot = result.slot, .changed = false}; }

                if (slot_removed(result.slot))
                {
                    removed_.reset(result.slot);
                    value_published_.set(result.slot);
                }
                else if (child_has_current_value(result.slot))
                {
                    value_published_.set(result.slot);
                    added_.set(result.slot);
                }
                (void)key_set_tracking_.record_modified(modified_time);
                return mutation_result(result.slot, result.constructed);
            }

            [[nodiscard]] SlotTSDataMutationResult insert_key_move(const ValueView &key, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                // See TSSSlotStorage::insert_key_move: the enclosing TSData
                // move, rather than the element view, owns the source.
                const auto result = key.binding() == key_binding_
                                        ? keys_.insert_move(const_cast<void *>(key.data()))
                                        : keys_.insert(key);
                ensure_delta_capacity();
                if (!result.inserted) { return {.slot = result.slot, .changed = false}; }

                if (slot_removed(result.slot))
                {
                    removed_.reset(result.slot);
                    value_published_.set(result.slot);
                }
                else if (child_has_current_value(result.slot))
                {
                    value_published_.set(result.slot);
                    added_.set(result.slot);
                }
                (void)key_set_tracking_.record_modified(modified_time);
                return mutation_result(result.slot, result.constructed);
            }

            [[nodiscard]] SlotTSDataMutationResult remove_key(const ValueView &key, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                const auto slot = keys_.find_slot(key);
                if (slot == KeySlotStore::npos) { return {.slot = TS_DATA_NO_CHILD_ID, .changed = false}; }
                detail::stop_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
                if (!keys_.remove_slot(slot)) { return {.slot = slot, .changed = false}; }

                ensure_delta_capacity();
                if (slot_value_published(slot))
                {
                    if (slot_added(slot)) { added_.reset(slot); }
                    else { removed_.set(slot); }
                    value_published_.reset(slot);
                }
                modified_.reset(slot);
                (void)key_set_tracking_.record_modified(modified_time);
                return mutation_result(slot);
            }

            [[nodiscard]] SlotTSDataMutationResult remove_slot(std::size_t slot, DateTime modified_time)
            {
                validate_mutation_time(modified_time);
                prepare_delta(modified_time);

                if (slot == KeySlotStore::npos || !keys_.slot_live(slot))
                {
                    return {.slot = slot, .changed = false};
                }
                detail::stop_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
                if (!keys_.remove_slot(slot)) { return {.slot = slot, .changed = false}; }

                ensure_delta_capacity();
                if (slot_value_published(slot))
                {
                    if (slot_added(slot)) { added_.reset(slot); }
                    else { removed_.set(slot); }
                    value_published_.reset(slot);
                }
                modified_.reset(slot);
                (void)key_set_tracking_.record_modified(modified_time);
                return mutation_result(slot);
            }

            void record_child_modified(std::size_t slot, DateTime modified_time)
            {
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSD child modification requires a concrete evaluation time");
                }
                if (!slot_live(slot)) { return; }
                prepare_delta(modified_time);

                if (!child_has_current_value(slot))
                {
                    modified_.reset(slot);
                    if (!slot_value_published(slot)) { return; }

                    value_published_.reset(slot);
                    if (slot_added(slot)) { added_.reset(slot); }
                    else { removed_.set(slot); }
                    return;
                }

                if (!slot_value_published(slot))
                {
                    value_published_.set(slot);
                    if (slot_removed(slot)) { removed_.reset(slot); }
                    else { added_.set(slot); }
                }
                modified_.set(slot);
            }

            void reset_delta() noexcept
            {
                added_.reset();
                removed_.reset();
                modified_.reset();
                delta_time_ = MIN_DT;
            }

            void add_slot_observer(SlotObserver *observer)
            {
                keys_.add_slot_observer(observer);
            }

            void remove_slot_observer(SlotObserver *observer)
            {
                keys_.remove_slot_observer(observer);
            }

          private:
            [[nodiscard]] static std::size_t next_delta_slot(const sul::dynamic_bitset<> &slots,
                                                             std::size_t previous) noexcept
            {
                const std::size_t slot = previous == TS_DATA_NO_CHILD_ID
                                             ? slots.find_first()
                                             : slots.find_next(previous);
                return slot == sul::dynamic_bitset<>::npos ? TS_DATA_NO_CHILD_ID : slot;
            }

            void validate_mutation_time(DateTime modified_time) const
            {
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSD TSData mutation requires a concrete evaluation time");
                }
            }

            void prepare_delta(DateTime modified_time)
            {
                // Monotonic delta window — see the slot-set variant above.
                if (modified_time <= delta_time_)
                {
                    ensure_delta_capacity();
                    return;
                }

                for (const std::size_t slot : keys_.pending_erase_slots())
                {
                    if (!keys_.slot_pending_erase(slot)) { continue; }
                    detail::invalidate_owned_ts_data_tree(
                        TSDataView{element_type_, values_.value_memory(slot)});
                }
                keys_.erase_pending();
                reset_delta();
                delta_time_ = modified_time;
                ensure_delta_capacity();
            }

            void ensure_delta_capacity()
            {
                const auto capacity = keys_.slot_capacity();
                if (added_.size() == capacity) { return; }
                added_.resize(capacity);
                removed_.resize(capacity);
                modified_.resize(capacity);
                value_published_.resize(capacity);
            }

            [[nodiscard]] SlotTSDataMutationResult mutation_result(std::size_t slot,
                                                                    bool constructed = false) const noexcept
            {
                return {.slot = slot, .changed = true, .constructed = constructed};
            }

            ValueTypeRef key_binding_{nullptr};
            TSRoleTypeRef             element_type_{};
            TSDataTracking              tracking_{};
            TSDataTracking          key_set_tracking_{};
            KeySlotStore                keys_;
            KeyMirroredValueSlotStore   values_;
            sul::dynamic_bitset<>       added_{};
            sul::dynamic_bitset<>       removed_{};
            sul::dynamic_bitset<>       modified_{};
            sul::dynamic_bitset<>       value_published_{};
            DateTime               delta_time_{MIN_DT};
        };

#if defined(__APPLE__) && defined(__aarch64__)
        // One reusable Value normalizes concrete closed-union leaves into the
        // preplanned key layout without per-operation allocation.
        static_assert(sizeof(TSSSlotStorage) <= 416);
        static_assert(sizeof(TSDSlotStorage) <= 744);
#endif

        struct TSSStoragePlanContext
        {
            ValueTypeRef key_binding{nullptr};
        };

        struct TSDStoragePlanContext
        {
            ValueTypeRef key_binding{nullptr};
            TSRoleTypeRef element_type{};
        };

        [[nodiscard]] const TSSStoragePlanContext &tss_plan_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("TSS storage requires lifecycle context"); }
            return *static_cast<const TSSStoragePlanContext *>(context);
        }

        [[nodiscard]] const TSDStoragePlanContext &tsd_plan_context(const void *context)
        {
            if (context == nullptr) { throw std::logic_error("TSD storage requires lifecycle context"); }
            return *static_cast<const TSDStoragePlanContext *>(context);
        }

        void tss_storage_construct(void *dst, const void *context)
        {
            const auto &state = tss_plan_context(context);
            if (state.key_binding == nullptr) { throw std::logic_error("TSS key binding is not resolved"); }
            std::construct_at(static_cast<TSSSlotStorage *>(dst), state.key_binding);
        }

        void tsd_storage_construct(void *dst, const void *context)
        {
            const auto &state = tsd_plan_context(context);
            if (state.key_binding == nullptr) { throw std::logic_error("TSD key binding is not resolved"); }
            if (!state.element_type) { throw std::logic_error("TSD element type is not resolved"); }
            std::construct_at(static_cast<TSDSlotStorage *>(dst), state.key_binding, state.element_type);
        }

        template <typename Storage>
        void slot_storage_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<Storage *>(memory));
        }

        struct SlotPlanEntry
        {
            TSSStoragePlanContext                        tss_context{};
            TSDStoragePlanContext                        tsd_context{};
            std::unique_ptr<MemoryUtils::StoragePlan>    storage_plan{};
            const MemoryUtils::StoragePlan              *root_plan{nullptr};
        };

        struct TSDSlotPlanKey
        {
            const TSValueTypeMetaData *schema{nullptr};
            ValueTypeRef              key_binding{};
            TSRoleTypeRef             element_type{};

            [[nodiscard]] bool operator==(const TSDSlotPlanKey &) const noexcept = default;
        };

        struct TSDSlotPlanKeyHash
        {
            [[nodiscard]] std::size_t operator()(const TSDSlotPlanKey &key) const noexcept
            {
                auto seed = std::hash<const TSValueTypeMetaData *>{}(key.schema);
                seed = combine_hash(seed, std::hash<ValueTypeRef>{}(key.key_binding));
                return combine_hash(seed, std::hash<const TypeRecord *>{}(key.element_type.record()));
            }
        };

        [[nodiscard]] std::unordered_map<TSDSlotPlanKey,
                                         std::unique_ptr<SlotPlanEntry>,
                                         TSDSlotPlanKeyHash> &
        custom_tsd_slot_plan_entries() noexcept
        {
            static std::unordered_map<TSDSlotPlanKey,
                                      std::unique_ptr<SlotPlanEntry>,
                                      TSDSlotPlanKeyHash> entries;
            return entries;
        }

        [[nodiscard]] std::recursive_mutex &slot_plan_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        template <typename Storage>
        [[nodiscard]] const Storage &storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("slot TSData requires live storage"); }
            return *MemoryUtils::cast<Storage>(memory);
        }

        template <typename Storage>
        [[nodiscard]] Storage &storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("slot TSData requires live storage"); }
            return *MemoryUtils::cast<Storage>(memory);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] Value value_from_python(const ValueTypeRef &binding, nb::handle source, const char *what)
        {
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None value"); }
            Value value{binding};
            binding.ops_ref().from_python(binding, const_cast<void *>(value.view().data()), source);
            return value;
        }

        template <typename Visitor>
        void for_each_python_iterable(nb::handle source, const char *what, Visitor visitor)
        {
            if (source.is_none()) { return; }
            nb::object object = nb::borrow<nb::object>(source);
            nb::iterator it = nb::iter(object);
            while (it != nb::iterator::sentinel())
            {
                nb::handle item = *it;
                if (item.is_none())
                {
                    throw std::invalid_argument(std::string{what} + " does not allow None elements");
                }
                visitor(item);
                ++it;
            }
        }

        [[nodiscard]] bool python_has_items(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::dict>(object) || nb::hasattr(object, "items");
        }

        template <typename Visitor>
        void for_each_python_mapping_item(nb::handle source, const char *what, Visitor visitor)
        {
            if (!python_has_items(source))
            {
                throw std::invalid_argument(std::string{what} + " expects a Python mapping");
            }
            nb::object   object = nb::borrow<nb::object>(source);
            nb::object   items  = object.attr("items")();
            nb::iterator it     = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2)
                {
                    throw std::invalid_argument(std::string{what} + " items() must yield key/value pairs");
                }
                visitor(nb::borrow<nb::object>(pair[0]), nb::borrow<nb::object>(pair[1]));
                ++it;
            }
        }

        [[nodiscard]] bool python_named_field(nb::handle source, const char *name, nb::object &out)
        {
            nb::object object = nb::borrow<nb::object>(source);
            if (nb::isinstance<nb::dict>(object))
            {
                nb::dict map = nb::cast<nb::dict>(object);
                nb::str  key{name};
                if (!map.contains(key)) { return false; }
                out = map[key];
                return true;
            }
            if (nb::hasattr(object, name))
            {
                out = nb::getattr(object, name);
                return true;
            }
            return false;
        }
#endif

        enum class SlotSetSurface
        {
            Live,
            Added,
            Removed,
        };

        enum class SlotMapSurface
        {
            Live,
            Modified,
        };

        struct ProjectionKey
        {
            const TypeRecord *type_record{nullptr};
            const TSDataOps *source_ops{nullptr};
            const TSValueTypeMetaData *schema{nullptr};
            TypeRole role{TypeRole::Invalid};
            [[nodiscard]] bool operator==(const ProjectionKey &) const noexcept = default;
        };

        struct ProjectionKeyHash
        {
            [[nodiscard]] std::size_t operator()(const ProjectionKey &key) const noexcept
            {
                auto seed = combine_hash(std::hash<const TypeRecord *>{}(key.type_record),
                                         std::hash<const TSDataOps *>{}(key.source_ops));
                seed = combine_hash(seed, std::hash<const TSValueTypeMetaData *>{}(key.schema));
                return combine_hash(seed, static_cast<std::size_t>(key.role));
            }
        };

        [[nodiscard]] std::recursive_mutex &projection_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        using OwnedProjectionOps = std::unique_ptr<TSDataOps, void (*)(TSDataOps *)>;

        template <typename Ops>
        [[nodiscard]] OwnedProjectionOps copy_projection_ops(const Ops &ops)
        {
            return OwnedProjectionOps{
                new Ops{ops},
                [](TSDataOps *value) noexcept { delete static_cast<Ops *>(value); }};
        }

        [[nodiscard]] OwnedProjectionOps copy_projection_ops(const TSDataOps &ops)
        {
            switch (ops.kind)
            {
            case TSTypeKind::TSS:
                return copy_projection_ops(static_cast<const TSSDataOps &>(ops));
            case TSTypeKind::TSD:
                return copy_projection_ops(static_cast<const TSDDataOps &>(ops));
            case TSTypeKind::TSL:
            case TSTypeKind::TSB:
                return copy_projection_ops(static_cast<const IndexedTSDataOps &>(ops));
            case TSTypeKind::TSW:
                return copy_projection_ops(static_cast<const TSWDataOps &>(ops));
            default:
                return copy_projection_ops<TSDataOps>(ops);
            }
        }

        [[nodiscard]] auto &projection_ops_cache() noexcept
        {
            static std::unordered_map<ProjectionKey, OwnedProjectionOps, ProjectionKeyHash> cache;
            return cache;
        }

        [[nodiscard]] TSRoleTypeRef intern_tsd_value_projection_type(TSRoleTypeRef element_type,
                                                                         TypeRole role)
        {
            const auto *schema = element_type.schema();
            if (schema == nullptr || !is_migrated_ts_root_schema(schema)) { return element_type; }
            const auto label = role == TypeRole::Data
                                   ? std::string_view{"ts.tsd.value.data"}
                                   : role == TypeRole::Input
                                         ? std::string_view{"ts.tsd.value.input"}
                                         : std::string_view{"ts.tsd.value.output"};
            if (const auto *record = element_type.record();
                record != nullptr && record->role == role && record->implementation_name() == label)
                return element_type;
            const auto &source_ops = element_type.ops_ref();
            const ProjectionKey key{element_type.record(), &source_ops, schema, role};
            std::lock_guard<std::recursive_mutex> lock(projection_mutex());
            const TSDataOps *projection_ops = nullptr;
            auto &cache = projection_ops_cache();
            if (const auto it = cache.find(key); it != cache.end())
                projection_ops = it->second.get();
            if (projection_ops == nullptr)
            {
                auto owned_ops = copy_projection_ops(source_ops);
                projection_ops = owned_ops.get();
                cache.emplace(key, std::move(owned_ops));
            }
            return TSRoleTypeRef{intern_ts_type(
                *schema, role, element_type.checked_plan(), *projection_ops, label)};
        }

        [[nodiscard]] std::string_view keyed_root_label(TSTypeKind kind, TypeRole role,
                                                        bool embedded, bool composite = false)
        {
            if (composite && kind == TSTypeKind::TSD && role == TypeRole::Input)
                return "ts.tsd.input.composite";
            if (embedded)
            {
                if (kind == TSTypeKind::TSS)
                    return role == TypeRole::Data ? "ts.tss.data.embedded"
                         : role == TypeRole::Input ? "ts.tss.input.embedded"
                                                   : "ts.tss.output.embedded";
                return role == TypeRole::Data ? "ts.tsd.data.embedded"
                     : role == TypeRole::Input ? "ts.tsd.input.embedded"
                                               : "ts.tsd.output.embedded";
            }
            if (kind == TSTypeKind::TSS)
                return role == TypeRole::Data ? "ts.tss.data.root"
                     : role == TypeRole::Input ? "ts.tss.input.owned"
                                               : "ts.tss.output.root";
            return role == TypeRole::Data ? "ts.tsd.data.root"
                 : role == TypeRole::Input ? "ts.tsd.input.owned"
                                           : "ts.tsd.output.root";
        }

        struct SlotContextBase
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            TSSDataOps                      set_ops{};
            TSSDataLayout                   set_layout{};
            SetValueOps                     value_set_ops{};
            IndexedValueOps                 delta_bundle_ops{};
            SetValueOps                     added_set_ops{};
            SetValueOps                     removed_set_ops{};
            ValueTypeRef added_set_binding{nullptr};
            ValueTypeRef removed_set_binding{nullptr};
            TSRoleTypeRef root_type{};

            virtual ~SlotContextBase() = default;
            virtual const TSDataOps &ops_ref() const noexcept = 0;
            virtual TSRoleTypeRef key_set_type() const noexcept { return {}; }
        };

        template <typename Storage>
        struct TSSContextBase : SlotContextBase
        {
            void initialise_tss_common(const TSValueTypeMetaData &schema_,
                                       const MemoryUtils::StoragePlan &plan_,
                                       const ValueTypeRef &key_binding,
                                       bool mutable_storage,
                                       TSRoleTypeRef sample_element_type = {})
            {
                schema = &schema_;
                plan   = &plan_;
                set_layout.key_binding     = key_binding;
                configure_set_value_ops();
                configure_delta_ops();
                const auto [debug_layout, tracking_offset] = [&] {
                    if constexpr (std::is_same_v<Storage, TSDSlotStorage>)
                    {
                        const Storage sample{key_binding, sample_element_type};
                        return std::pair{tss_value_debug_layout(sample, key_binding),
                                         debug_offset(sample, sample.key_set_tracking())};
                    }
                    else
                    {
                        const Storage sample{key_binding};
                        return std::pair{tss_value_debug_layout(sample, key_binding),
                                         debug_offset(sample, sample.tracking())};
                    }
                }();
                set_layout.tracking_offset = tracking_offset;
                const auto &debug = intern_dynamic_debug_descriptor(
                    schema->value_schema->header, *plan, DebugLayoutKind::Sequence,
                    nullptr, key_binding.record(), debug_layout, &value_set_ops);
                set_layout.value_binding = intern_value_type(
                    *schema->value_schema, *plan, value_set_ops, &debug);
                configure_tss_ops(mutable_storage);
                bind_tss_delta_surfaces();
            }

            const TSDataOps &ops_ref() const noexcept override { return set_ops; }

          protected:
            void configure_tss_ops(bool mutable_storage)
            {
                set_ops = TSSDataOps{};
                TSDataOps &base_ops = set_ops;
                base_ops = TSDataOps{
                    .context                   = this,
                    .kind                      = TSTypeKind::TSS,
                    .allows_mutation           = mutable_storage,
                    .layout_impl               = &tss_layout,
                    .tracking_impl             = &tss_tracking,
                    .mutable_tracking_impl     = &tss_mutable_tracking,
                    .has_current_value_impl    = &tss_has_current_value,
                    .all_valid_impl            = &tss_all_valid,
                    .dynamic_storage_metrics_impl = &tss_dynamic_storage_metrics,
                    .value_memory_impl         = &tss_value_memory,
                    .mutable_value_memory_impl = &tss_mutable_value_memory,
                    .delta_memory_impl         = &tss_delta_memory,
                    .mutable_delta_memory_impl = &tss_mutable_delta_memory,
                    .reset_delta_impl          = &tss_reset_delta,
                    .copy_value_from_impl      = &tss_copy_value_from,
                    .move_value_from_impl      = &tss_move_value_from,
                    .empty_delta_impl          = &ts_data_detail::empty_delta_tss,
                    .capture_delta_impl        = &ts_data_detail::capture_delta_tss,
                    .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_tss,
                    .apply_delta_impl          = &ts_data_detail::apply_delta_tss,
                    .clear_collection_impl     = &ts_data_detail::clear_tss_collection,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                    .from_python_impl          = &tss_from_python,
                    .to_python_impl            = &tss_to_python,
                    .delta_to_python_impl      = &tss_delta_to_python,
#endif
                };
                set_ops.size_impl                      = &tss_size;
                set_ops.slot_capacity_impl             = &tss_slot_capacity;
                set_ops.slot_occupied_impl             = &tss_slot_occupied;
                set_ops.slot_live_impl                 = &tss_slot_live;
                set_ops.slot_added_impl                = &tss_slot_added;
                set_ops.slot_removed_impl              = &tss_slot_removed;
                set_ops.next_added_slot_impl           = &tss_next_added_slot;
                set_ops.next_removed_slot_impl         = &tss_next_removed_slot;
                set_ops.key_at_slot_impl               = &tss_key_at_slot;
                set_ops.contains_impl                  = &tss_contains;
                set_ops.find_slot_impl                 = &tss_find_slot;
                set_ops.make_values_range_impl         = &tss_live_keys_range;
                set_ops.make_added_values_range_impl   = &tss_added_keys_range;
                set_ops.make_removed_values_range_impl = &tss_removed_keys_range;
                set_ops.insert_key_impl                = &tss_insert_key;
                set_ops.insert_key_move_impl           = &tss_insert_key_move;
                set_ops.remove_key_impl                = &tss_remove_key;
                set_ops.remove_slot_impl               = &tss_remove_slot;
                set_ops.touch_impl                     = &tss_touch;
                set_ops.reserve_impl                   = &tss_reserve;
                set_ops.subscribe_slot_observer_impl   = &tss_subscribe_slot_observer;
                set_ops.unsubscribe_slot_observer_impl = &tss_unsubscribe_slot_observer;
            }

            void configure_set_value_ops()
            {
                value_set_ops = set_ops_for_surface<SlotSetSurface::Live>();
                value_set_ops.dynamic_storage_metrics_impl = &tss_dynamic_storage_metrics;
            }

            void configure_delta_ops()
            {
                added_set_ops   = set_ops_for_surface<SlotSetSurface::Added>();
                removed_set_ops = set_ops_for_surface<SlotSetSurface::Removed>();
                delta_bundle_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &delta_bundle_hash, &delta_bundle_equals,
                     &delta_bundle_compare,
                     &delta_bundle_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &delta_bundle_to_python
#endif
                    },
                    &delta_bundle_size,
                    &delta_bundle_element_at,
                    &delta_bundle_element_binding,
                    &delta_bundle_make_range,
                    nullptr,
                };
                delta_bundle_ops.owning_type_impl      = &canonical_value_binding;
                delta_bundle_ops.copy_construct_view_impl = &delta_copy_construct_view;
                delta_bundle_ops.copy_assign_view_impl    = &delta_copy_assign_view;
            }

            void bind_tss_delta_surfaces()
            {
                const auto *delta_schema = schema->delta_value_schema;
                if (delta_schema == nullptr || delta_schema->value_kind() != ValueTypeKind::Bundle ||
                    delta_schema->field_count != 2)
                {
                    throw std::logic_error("TSS TSData delta schema must be Bundle{added, removed}");
                }
                added_set_binding   = intern_value_type(*delta_schema->fields[0].type, *plan, added_set_ops);
                removed_set_binding = intern_value_type(*delta_schema->fields[1].type, *plan, removed_set_ops);
                set_layout.delta_binding = intern_value_type(*delta_schema, *plan, delta_bundle_ops);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] SetValueOps set_ops_for_surface()
            {
                SetValueOps ops{
                    {{ValueOpsKind::Set, this, false, &set_hash<Surface>, &set_equals<Surface>, &set_compare<Surface>,
                      &set_to_string<Surface>
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &set_to_python<Surface>
#endif
                     },
                     &set_size<Surface>,
                     &set_element_at<Surface>,
                     &set_element_binding<Surface>,
                     &set_make_range<Surface>,
                     nullptr},
                    &set_contains<Surface>,
                };
                ops.owning_type_impl      = &canonical_value_binding;
                ops.copy_construct_view_impl = &set_copy_construct_view<Surface>;
                ops.copy_assign_view_impl    = &set_copy_assign_view<Surface>;
                return ops;
            }

            [[nodiscard]] static const TSSContextBase *ctx(const void *context) noexcept
            {
                return static_cast<const TSSContextBase *>(context);
            }

            [[nodiscard]] static ValueTypeRef
            canonical_value_binding(const void *, ValueTypeRef view_binding)
            {
                const auto binding = ValuePlanFactory::instance().type_for(view_binding.schema());
                if (binding == nullptr)
                {
                    throw std::logic_error("TSS value surface has no canonical owning binding");
                }
                return binding;
            }

            template <SlotSetSurface Surface>
            static void set_copy_construct_view(const void *context,
                                                const ValueTypeRef &binding,
                                                void *dst,
                                                const void *memory)
            {
                auto storage = build_set_storage<Surface>(context, binding, memory);
                std::construct_at(static_cast<SetStorage *>(dst), std::move(storage));
            }

            template <SlotSetSurface Surface>
            static void set_copy_assign_view(const void *context,
                                             const ValueTypeRef &binding,
                                             void *dst,
                                             const void *memory)
            {
                *static_cast<SetStorage *>(dst) = build_set_storage<Surface>(context, binding, memory);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static SetStorage build_set_storage(const void *context,
                                                              const ValueTypeRef &binding,
                                                              const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Set)
                {
                    throw std::logic_error("TSS set copy requires a canonical set binding");
                }
                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (key_binding == nullptr || key_binding != ctx(context)->set_layout.key_binding)
                {
                    throw std::logic_error("TSS set copy key binding is not resolved");
                }

                SetBuilder builder{key_binding};
                for (const auto key : set_make_range<Surface>(context, memory))
                {
                    builder.insert_copy(key.data());
                }
                return builder.build_storage();
            }

            static void delta_copy_construct_view(const void *context,
                                                  const ValueTypeRef &binding,
                                                  void *dst,
                                                  const void *memory)
            {
                const auto &plan = binding.checked_plan();
                plan.default_construct(dst);
                auto rollback = make_scope_exit([&]() noexcept { plan.destroy(dst); });
                delta_copy_assign_view(context, binding, dst, memory);
                rollback.release();
            }

            static void delta_copy_assign_view(const void *context,
                                               const ValueTypeRef &binding,
                                               void *dst,
                                               const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Bundle ||
                    binding.schema()->field_count != 2)
                {
                    throw std::logic_error("TSS delta copy requires canonical Bundle{added, removed}");
                }

                const auto &plan = binding.checked_plan();
                if (!plan.is_composite() || plan.component_count() < 2)
                {
                    throw std::logic_error("TSS delta copy requires a two-field structured plan");
                }

                copy_projected_bundle_fields(
                    binding, dst, 2,
                    [&](std::size_t index) {
                        return delta_bundle_element_binding(context, memory, index);
                    },
                    [&](std::size_t index) {
                        return delta_bundle_element_at(context, memory, index);
                    });
            }

          public:
            [[nodiscard]] static const TSDataLayout *tss_layout(const void *context) noexcept
            {
                return &ctx(context)->set_layout;
            }

            [[nodiscard]] static const TSDataTracking *tss_tracking(const void *, const void *memory) noexcept
            {
                return &storage<Storage>(memory).tracking();
            }

            [[nodiscard]] static TSDataTracking *tss_mutable_tracking(const void *, void *memory) noexcept
            {
                return &storage<Storage>(memory).mutable_tracking();
            }

            [[nodiscard]] static bool tss_has_current_value(const void *, const void *memory) noexcept
            {
                return storage<Storage>(memory).tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool tss_all_valid(const void *, const void *memory) noexcept
            {
                return storage<Storage>(memory).tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static DynamicStorageMetrics tss_dynamic_storage_metrics(
                const void *, const void *memory) noexcept
            {
                return storage<Storage>(memory).key_set_dynamic_storage_metrics();
            }

            [[nodiscard]] static const void *tss_value_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *tss_mutable_value_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static const void *tss_delta_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *tss_mutable_delta_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            static void tss_reset_delta(const void *, void *memory)
            {
                storage<Storage>(memory).reset_delta();
            }


            [[nodiscard]] static bool tss_copy_value_from(const void *context, void *memory, const ValueView &source,
                                                          DateTime modified_time)
            {
                if (!source.has_value()) { throw std::invalid_argument("TSS copy requires a live source value"); }
                if (source.schema() != ctx(context)->schema->value_schema)
                {
                    throw std::invalid_argument("TSS copy requires the set value schema");
                }

                auto     &target        = storage<Storage>(memory);
                auto      source_set    = source.as_set();
                const bool newly_touched = target.touch(modified_time);
                for (const auto key : source_set.values())
                {
                    static_cast<void>(target.insert_key(key, modified_time));
                }

                std::vector<Value> removals;
                for (const auto key : set_make_range<SlotSetSurface::Live>(context, memory))
                {
                    if (!source_set.contains(key)) { removals.emplace_back(key); }
                }
                for (const auto &key : removals)
                {
                    static_cast<void>(target.remove_key(key.view(), modified_time));
                }
                return newly_touched;
            }

            [[nodiscard]] static bool tss_move_value_from(const void *context, void *memory, ValueView source,
                                                          DateTime modified_time)
            {
                if (memory == nullptr) { throw std::logic_error("TSS move requires live storage"); }
                if (!source.has_value()) { throw std::invalid_argument("TSS move requires a live source value"); }
                if (!source.writable_payload())
                    throw std::invalid_argument("TSS move requires writable source storage");
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSS move requires a concrete evaluation time");
                }
                if (source.schema() != ctx(context)->schema->value_schema)
                {
                    throw std::invalid_argument("TSS move requires the set value schema");
                }

                auto       &target     = storage<Storage>(memory);
                auto        source_set = source.as_set();
                const auto *state      = ctx(context);

                std::vector<std::size_t> removal_slots;
                for (std::size_t slot = 0; slot < target.slot_capacity(); ++slot)
                {
                    if (!target.slot_live(slot)) { continue; }
                    ValueView key{state->set_layout.key_binding, target.key_at_slot(slot)};
                    if (!source_set.contains(key)) { removal_slots.push_back(slot); }
                }

                const bool newly_touched = target.touch(modified_time);
                for (const auto key : source_set.values())
                {
                    static_cast<void>(target.insert_key_move(key, modified_time));
                }
                for (const auto slot : removal_slots)
                {
                    static_cast<void>(target.remove_slot(slot, modified_time));
                }
                return newly_touched;
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object tss_to_python(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return state->set_layout.value_binding.ops_ref().to_python(memory);
            }

            [[nodiscard]] static nb::object tss_delta_to_python(const void *context,
                                                                const void *memory,
                                                                DateTime evaluation_time)
            {
                const auto *state = ctx(context);
                if (storage<Storage>(memory).tracking().last_modified_time != evaluation_time) { return nb::none(); }
                return state->set_layout.delta_binding.ops_ref().to_python(memory);
            }

            [[nodiscard]] static bool tss_from_python(const void *context,
                                                      void       *memory,
                                                      nb::handle  source,
                                                      DateTime modified_time)
            {
                if (memory == nullptr) { throw std::logic_error("TSS from_python requires live storage"); }
                if (source.is_none()) { throw std::invalid_argument("TSS from_python requires a non-None source"); }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSS from_python requires a concrete evaluation time");
                }

                auto       &target = storage<Storage>(memory);
                const auto *state  = ctx(context);

                nb::object added;
                nb::object removed;
                const bool has_added = python_named_field(source, "added", added);
                const bool has_removed = python_named_field(source, "removed", removed);
                if (has_added || has_removed)
                {
                    const bool first_for_parent = target.tracking().last_modified_time != modified_time;
                    if (has_added && !added.is_none())
                    {
                        for_each_python_iterable(added, "TSS added update", [&](nb::handle item) {
                            Value key = value_from_python(state->set_layout.key_binding, item, "TSS added update");
                            static_cast<void>(target.insert_key(key.view(), modified_time));
                        });
                    }
                    if (has_removed && !removed.is_none())
                    {
                        for_each_python_iterable(removed, "TSS removed update", [&](nb::handle item) {
                            Value key = value_from_python(state->set_layout.key_binding, item, "TSS removed update");
                            static_cast<void>(target.remove_key(key.view(), modified_time));
                        });
                    }
                    return first_for_parent;
                }

                nb::object object = nb::borrow<nb::object>(source);
                if (!nb::isinstance<nb::set>(object) && !nb::isinstance<nb::frozenset>(object) &&
                    !nb::isinstance<nb::list>(object) && !nb::isinstance<nb::tuple>(object))
                {
                    throw std::invalid_argument("TSS from_python expects a Python set, frozenset, list, or tuple");
                }

                std::vector<Value> replacement;
                if (nb::hasattr(object, "__len__"))
                {
                    replacement.reserve(static_cast<std::size_t>(nb::len(object)));
                }
                for_each_python_iterable(source, "TSS value", [&](nb::handle item) {
                    replacement.push_back(value_from_python(state->set_layout.key_binding, item, "TSS value"));
                });

                const bool newly_touched = target.touch(modified_time);
                for (const auto &key : replacement)
                {
                    static_cast<void>(target.insert_key(key.view(), modified_time));
                }

                const auto &key_ops = state->set_layout.key_binding.ops_ref();
                std::vector<Value> removals;
                for (const auto key : set_make_range<SlotSetSurface::Live>(context, memory))
                {
                    const bool keep = std::any_of(replacement.begin(), replacement.end(), [&](const Value &candidate) {
                        return key_ops.equals(key.data(), candidate.view().data());
                    });
                    if (!keep) { removals.emplace_back(key); }
                }
                for (const auto &key : removals)
                {
                    static_cast<void>(target.remove_key(key.view(), modified_time));
                }
                return newly_touched;
            }
#endif

            [[nodiscard]] static std::size_t tss_size(const void *, const void *memory) noexcept
            {
                return storage<Storage>(memory).size();
            }

            [[nodiscard]] static std::size_t tss_slot_capacity(const void *, const void *memory) noexcept
            {
                return storage<Storage>(memory).slot_capacity();
            }

            [[nodiscard]] static bool tss_slot_occupied(const void *, const void *memory, std::size_t slot)
            {
                return storage<Storage>(memory).slot_occupied(slot);
            }

            [[nodiscard]] static bool tss_slot_live(const void *, const void *memory, std::size_t slot)
            {
                return storage<Storage>(memory).slot_live(slot);
            }

            [[nodiscard]] static bool tss_slot_added(const void *, const void *memory, std::size_t slot)
            {
                return storage<Storage>(memory).slot_added(slot);
            }

            [[nodiscard]] static bool tss_slot_removed(const void *, const void *memory, std::size_t slot)
            {
                return storage<Storage>(memory).slot_removed(slot);
            }

            [[nodiscard]] static std::size_t tss_next_added_slot(const void *, const void *memory,
                                                                 std::size_t previous)
            {
                return storage<Storage>(memory).next_added_slot(previous);
            }

            [[nodiscard]] static std::size_t tss_next_removed_slot(const void *, const void *memory,
                                                                   std::size_t previous)
            {
                return storage<Storage>(memory).next_removed_slot(previous);
            }

            [[nodiscard]] static const void *tss_key_at_slot(const void *, const void *memory, std::size_t slot)
            {
                return storage<Storage>(memory).key_at_slot(slot);
            }

            [[nodiscard]] static bool tss_contains(const void *, const void *memory, const ValueView &key)
            {
                return storage<Storage>(memory).contains(key);
            }

            [[nodiscard]] static std::size_t tss_find_slot(const void *, const void *memory, const ValueView &key)
            {
                return storage<Storage>(memory).find_slot(key);
            }

            [[nodiscard]] static SlotTSDataMutationResult tss_insert_key(const void *context, void *memory,
                                                                         const ValueView &key,
                                                                         DateTime modified_time)
            {
                auto &store = storage<Storage>(memory);
                if constexpr (std::is_same_v<Storage, TSDSlotStorage>)
                {
                    const auto result = store.insert_key(key, modified_time);
                    if (result.constructed)
                    {
                        const auto *state = ctx(context);
                        TSDataView child{store.element_type(), store.child_memory_for_write(result.slot)};
                        detail::attach_owned_ts_data_parent(
                            child.borrowed_ref(), TSDataView{state->root_type, memory}, result.slot);
                    }
                    return result;
                }
                return store.insert_key(key, modified_time);
            }

            [[nodiscard]] static SlotTSDataMutationResult tss_insert_key_move(const void *context, void *memory,
                                                                              const ValueView &key,
                                                                              DateTime modified_time)
            {
                auto &store = storage<Storage>(memory);
                if constexpr (std::is_same_v<Storage, TSDSlotStorage>)
                {
                    const auto result = store.insert_key_move(key, modified_time);
                    if (result.constructed)
                    {
                        const auto *state = ctx(context);
                        TSDataView child{store.element_type(), store.child_memory_for_write(result.slot)};
                        detail::attach_owned_ts_data_parent(
                            child.borrowed_ref(), TSDataView{state->root_type, memory}, result.slot);
                    }
                    return result;
                }
                return store.insert_key_move(key, modified_time);
            }

            [[nodiscard]] static SlotTSDataMutationResult tss_remove_key(const void *, void *memory,
                                                                         const ValueView &key,
                                                                         DateTime modified_time)
            {
                return storage<Storage>(memory).remove_key(key, modified_time);
            }

            [[nodiscard]] static SlotTSDataMutationResult tss_remove_slot(const void *, void *memory,
                                                                          std::size_t slot,
                                                                          DateTime modified_time)
            {
                return storage<Storage>(memory).remove_slot(slot, modified_time);
            }

            [[nodiscard]] static bool tss_touch(const void *, void *memory, DateTime modified_time)
            {
                return storage<Storage>(memory).touch(modified_time);
            }

            static void tss_reserve(const void *, void *memory, std::size_t capacity)
            {
                storage<Storage>(memory).reserve(capacity);
            }

            static void tss_subscribe_slot_observer(const void *, void *memory, SlotObserver *observer)
            {
                storage<Storage>(memory).add_slot_observer(observer);
            }

            static void tss_unsubscribe_slot_observer(const void *, void *memory, SlotObserver *observer)
            {
                storage<Storage>(memory).remove_slot_observer(observer);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static bool set_slot_in_surface(const Storage &store, std::size_t slot) noexcept
            {
                if constexpr (Surface == SlotSetSurface::Live) { return store.slot_live(slot); }
                if constexpr (Surface == SlotSetSurface::Added) { return store.slot_added(slot); }
                return store.slot_removed(slot);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static std::size_t set_size(const void *, const void *memory) noexcept
            {
                const auto &store = storage<Storage>(memory);
                if constexpr (Surface == SlotSetSurface::Live) { return store.size(); }
                std::size_t count = 0;
                for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                {
                    if (set_slot_in_surface<Surface>(store, slot)) { ++count; }
                }
                return count;
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static const void *set_element_at(const void *, const void *memory, std::size_t index)
            {
                const auto &store = storage<Storage>(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                {
                    if (!set_slot_in_surface<Surface>(store, slot)) { continue; }
                    if (seen++ == index) { return store.key_at_slot(slot); }
                }
                throw std::out_of_range("slot set element index out of range");
            }

            template <SlotSetSurface>
            [[nodiscard]] static ValueTypeRef set_element_binding(const void *context, const void *,
                                                                             std::size_t) noexcept
            {
                return ctx(context)->set_layout.key_binding;
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static bool set_contains(const void *, const void *memory, const void *key)
            {
                const auto &store = storage<Storage>(memory);
                const auto  slot  = store.keys().find_stored_slot(key);
                return slot != KeySlotStore::npos && set_slot_in_surface<Surface>(store, slot);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static bool set_range_predicate(const void *, const void *memory, std::size_t slot)
            {
                return set_slot_in_surface<Surface>(storage<Storage>(memory), slot);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static ValueView set_range_projector(const void *context, const void *memory,
                                                               std::size_t slot)
            {
                return ValueView{ctx(context)->set_layout.key_binding, storage<Storage>(memory).key_at_slot(slot)};
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static Range<ValueView> set_make_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<Storage>(memory).slot_capacity(),
                    .predicate = &set_range_predicate<Surface>,
                    .projector = &set_range_projector<Surface>,
                };
            }

            [[nodiscard]] static Range<ValueView> tss_live_keys_range(const void *context, const void *memory)
            {
                return set_make_range<SlotSetSurface::Live>(context, memory);
            }

            [[nodiscard]] static Range<ValueView> tss_added_keys_range(const void *context, const void *memory)
            {
                return set_make_range<SlotSetSurface::Added>(context, memory);
            }

            [[nodiscard]] static Range<ValueView> tss_removed_keys_range(const void *context, const void *memory)
            {
                return set_make_range<SlotSetSurface::Removed>(context, memory);
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static std::size_t set_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &ops   = state->set_layout.key_binding.ops_ref();
                std::size_t result = 0;
                for (const auto key : set_make_range<Surface>(context, memory))
                {
                    result ^= ops.hash(key.data());
                }
                return result;
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static bool set_equals(const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    if (set_size<Surface>(context, lhs) != set_size<Surface>(context, rhs)) { return false; }
                    for (const auto key : set_make_range<Surface>(context, lhs))
                    {
                        if (!set_contains<Surface>(context, rhs, key.data())) { return false; }
                    }
                    return true;
                });
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static std::partial_ordering set_compare(const void *context, const void *lhs,
                                                                    const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                const auto lhs_size = set_size<Surface>(context, lhs);
                const auto rhs_size = set_size<Surface>(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return set_equals<Surface>(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                              : std::partial_ordering::unordered;
            }

            template <SlotSetSurface Surface>
            [[nodiscard]] static std::string set_to_string(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &ops   = state->set_layout.key_binding.ops_ref();
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                bool first = true;
                for (const auto key : set_make_range<Surface>(context, memory))
                {
                    if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                    first = false;
                    fmt::format_to(std::back_inserter(out), "{}", ops.to_string(key.data()));
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            template <SlotSetSurface Surface>
            [[nodiscard]] static nb::object set_to_python(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &ops   = state->set_layout.key_binding.ops_ref();
                nb::set     result;
                for (const auto key : set_make_range<Surface>(context, memory))
                {
                    result.add(ops.to_python(key.data()));
                }
                return result;
            }
#endif

            [[nodiscard]] static std::size_t delta_bundle_size(const void *, const void *) noexcept { return 2; }

            [[nodiscard]] static const void *delta_bundle_element_at(const void *, const void *memory,
                                                                     std::size_t index)
            {
                if (index >= 2) { throw std::out_of_range("TSS delta bundle index out of range"); }
                return memory;
            }

            [[nodiscard]] static ValueTypeRef delta_bundle_element_binding(const void *context,
                                                                                      const void *,
                                                                                      std::size_t index) noexcept
            {
                const auto *state = ctx(context);
                return index == 0 ? state->added_set_binding : state->removed_set_binding;
            }

            [[nodiscard]] static ValueView delta_bundle_projector(const void *context, const void *memory,
                                                                  std::size_t index)
            {
                return ValueView{delta_bundle_element_binding(context, memory, index),
                                 delta_bundle_element_at(context, memory, index)};
            }

            [[nodiscard]] static Range<ValueView> delta_bundle_make_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = 2,
                    .predicate = nullptr,
                    .projector = &delta_bundle_projector,
                };
            }

            [[nodiscard]] static std::size_t delta_bundle_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return combine_hash(state->added_set_binding.ops_ref().hash(memory),
                                    state->removed_set_binding.ops_ref().hash(memory));
            }

            [[nodiscard]] static bool delta_bundle_equals(const void *context, const void *lhs,
                                                          const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctx(context);
                    return state->added_set_binding.ops_ref().equals(lhs, rhs) &&
                           state->removed_set_binding.ops_ref().equals(lhs, rhs);
                });
            }

            [[nodiscard]] static std::partial_ordering delta_bundle_compare(const void *context, const void *lhs,
                                                                            const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                return delta_bundle_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                              : std::partial_ordering::unordered;
            }

            [[nodiscard]] static std::string delta_bundle_to_string(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return fmt::format("{{added: {}, removed: {}}}",
                                   state->added_set_binding.ops_ref().to_string(memory),
                                   state->removed_set_binding.ops_ref().to_string(memory));
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object delta_bundle_to_python(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                nb::dict    result;
                result[nb::str{"added"}] = state->added_set_binding.ops_ref().to_python(memory);
                result[nb::str{"removed"}] = state->removed_set_binding.ops_ref().to_python(memory);
                return result;
            }
#endif
        };

        struct TSSContext final : TSSContextBase<TSSSlotStorage>
        {
            TSSContext(const TSValueTypeMetaData &schema,
                       const MemoryUtils::StoragePlan &plan,
                       const ValueTypeRef &key_binding,
                       TypeRole role,
                       bool embedded)
            {
                initialise_tss_common(schema, plan, key_binding, true);
                root_type = TSRoleTypeRef{intern_ts_type(
                    schema, role, plan, set_ops, keyed_root_label(schema.kind, role, embedded))};
            }
        };

        struct TSDContext final : TSSContextBase<TSDSlotStorage>
        {
            const TSValueTypeMetaData *dict_schema{nullptr};
            TypeRole                  role{TypeRole::Invalid};
            TSDDataOps              dict_ops{};
            TSSDataOps              key_set_ts_ops{};
            TSDDataLayout           dict_layout{};
            MapValueOps             value_map_ops{};
            MapValueOps             modified_map_ops{};
            SetValueOps             key_set_value_ops{};
            IndexedValueOps          dict_delta_bundle_ops{};
            ValueTypeRef modified_map_binding{nullptr};
            // Projected delta's third field: strict removals never originate
            // from a live TSD; expose the interned always-empty set
            // (NON-OWNING: the empty-set registry is cleared before the type
            // records at reset — a context-owned Value would destruct after
            // its record is gone; see empty_delta_fields.h).
            const Value             *empty_strict_set{nullptr};
            ValueTypeRef key_set_value_binding{nullptr};
            TSRoleTypeRef          key_set_ts_type{};

            TSDContext(const TSValueTypeMetaData &schema,
                       const MemoryUtils::StoragePlan &plan,
                       const ValueTypeRef &key_binding,
                       TSRoleTypeRef element_type,
                       TypeRole role_,
                       bool embedded,
                       bool composite)
            {
                dict_schema = &schema;
                role = role_;
                element_type = intern_tsd_value_projection_type(element_type, role);
                const auto *key_set_schema = TypeRegistry::instance().tss(schema.key_type());
                if (key_set_schema == nullptr)
                {
                    throw std::logic_error("TSD key-set schema is not resolved");
                }
                initialise_tss_common(*key_set_schema, plan, key_binding, false, element_type);
                initialise_tsd(schema, plan, key_binding, element_type);
                root_type = TSRoleTypeRef{intern_ts_type(
                    schema, role, plan, dict_ops, keyed_root_label(schema.kind, role, embedded, composite))};
            }

            const TSDataOps &ops_ref() const noexcept override { return dict_ops; }
            TSRoleTypeRef key_set_type() const noexcept override { return key_set_ts_type; }

            [[nodiscard]] static const detail::TSDataOwnershipOps &ownership_ops() noexcept
            {
                static const detail::TSDataOwnershipOps ops{
                    .child_count = [](const void *, const void *memory) noexcept {
                        if (memory == nullptr) { return std::size_t{0}; }
                        const auto &store = storage<TSDSlotStorage>(memory);
                        std::size_t count = 1;
                        for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                            count += store.slot_occupied(slot) ? 1U : 0U;
                        return count;
                    },
                    .child_at = [](const void *context, void *memory, std::size_t index) noexcept {
                        if (context == nullptr || memory == nullptr) return detail::TSDataOwnedChild{};
                        const auto *state = static_cast<const TSDContext *>(context);
                        if (index == 0)
                            return detail::TSDataOwnedChild{
                                .type = state->key_set_ts_type,
                                .data = memory,
                                .attach_parent = false,
                            };
                        auto &store = storage<TSDSlotStorage>(memory);
                        std::size_t seen = 1;
                        for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                        {
                            if (!store.slot_occupied(slot)) { continue; }
                            if (seen++ == index)
                                return detail::TSDataOwnedChild{
                                    .type = state->dict_layout.element_type,
                                    .data = store.child_memory_for_write(slot),
                                    .parent_child_id = slot,
                                };
                        }
                        return detail::TSDataOwnedChild{};
                    },
                };
                return ops;
            }

          private:
            void initialise_tsd(const TSValueTypeMetaData &schema_,
                                const MemoryUtils::StoragePlan &plan_,
                                const ValueTypeRef &key_binding,
                                TSRoleTypeRef element_type)
            {
                const auto &element_ops = element_type.ops_ref();
                const auto *element_layout = element_ops.layout_impl(element_ops.context);
                if (element_layout == nullptr)
                {
                    throw std::logic_error("TSD element layout is not resolved");
                }

                dict_layout.key_binding           = key_binding;
                dict_layout.element_type          = element_type;
                dict_layout.element_layout        = element_layout;
                dict_layout.element_value_binding = element_layout->value_binding;
                dict_layout.element_delta_binding = element_layout->delta_binding;
                dict_layout.tracking_offset       = 0;

                configure_map_value_ops();
                configure_modified_map_ops();
                configure_tsd_ops();
                bind_tsd_surfaces(schema_, plan_);
            }

            [[nodiscard]] static const TSDataTracking *tsd_key_set_tracking(const void *,
                                                                             const void *memory) noexcept
            {
                return &storage<TSDSlotStorage>(memory).key_set_tracking();
            }

            [[nodiscard]] static TSDataTracking *tsd_key_set_mutable_tracking(const void *, void *memory) noexcept
            {
                return &storage<TSDSlotStorage>(memory).mutable_key_set_tracking();
            }

            [[nodiscard]] static bool tsd_key_set_has_current_value(const void *, const void *memory) noexcept
            {
                return storage<TSDSlotStorage>(memory).key_set_tracking().last_modified_time != MIN_DT;
            }

            void configure_tsd_ops()
            {
                dict_ops = TSDDataOps{};
                TSSDataOps &set_part = dict_ops;
                set_part = set_ops;
                TSDataOps &base_ops = dict_ops;
                base_ops.kind = TSTypeKind::TSD;
                base_ops.context = this;
                base_ops.allows_mutation = true;
                base_ops.ownership_ops = &ownership_ops();
                base_ops.layout_impl = &tsd_layout;
                base_ops.tracking_impl = &tsd_tracking;
                base_ops.mutable_tracking_impl = &tsd_mutable_tracking;
                base_ops.has_current_value_impl = &tsd_has_current_value;
                base_ops.all_valid_impl = &tsd_all_valid;
                base_ops.dynamic_storage_metrics_impl = &tsd_dynamic_storage_metrics;
                base_ops.value_memory_impl = &tsd_value_memory;
                base_ops.mutable_value_memory_impl = &tsd_mutable_value_memory;
                base_ops.delta_memory_impl = &tsd_delta_memory;
                base_ops.mutable_delta_memory_impl = &tsd_mutable_delta_memory;
                base_ops.reset_delta_impl = &tsd_reset_delta;
                base_ops.record_child_modified_impl = &tsd_record_child_modified;
                base_ops.copy_value_from_impl = &tsd_copy_value_from;
                base_ops.empty_delta_impl = &ts_data_detail::empty_delta_tsd;
                base_ops.capture_delta_impl = &ts_data_detail::capture_delta_tsd;
                base_ops.delta_has_effect_impl = &ts_data_detail::delta_has_effect_tsd;
                base_ops.apply_delta_impl = &ts_data_detail::apply_delta_tsd;
                base_ops.clear_collection_impl = &ts_data_detail::clear_tsd_collection;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                base_ops.from_python_impl = &tsd_from_python;
                base_ops.to_python_impl = &tsd_to_python;
                base_ops.delta_to_python_impl = &tsd_delta_to_python;
#endif

                dict_ops.child_at_slot_impl = &tsd_child_at_slot;
                dict_ops.slot_modified_impl = &tsd_slot_modified;
                dict_ops.next_modified_slot_impl = &tsd_next_modified_slot;
                dict_ops.make_ts_values_range_impl = &tsd_ts_values_range;
                dict_ops.make_valid_keys_range_impl = &tsd_valid_keys_range;
                dict_ops.make_valid_ts_values_range_impl = &tsd_valid_ts_values_range;
                dict_ops.make_modified_keys_range_impl = &tsd_modified_keys_range;
                dict_ops.make_modified_ts_values_range_impl = &tsd_modified_ts_values_range;
                dict_ops.make_added_ts_values_range_impl = &tsd_added_ts_values_range;
                dict_ops.make_removed_ts_values_range_impl = &tsd_removed_ts_values_range;
                dict_ops.make_ts_kv_range_impl = &tsd_ts_kv_range;
                dict_ops.make_valid_ts_kv_range_impl = &tsd_valid_ts_kv_range;
                dict_ops.make_modified_ts_kv_range_impl = &tsd_modified_ts_kv_range;
                dict_ops.make_added_ts_kv_range_impl = &tsd_added_ts_kv_range;
                dict_ops.make_removed_ts_kv_range_impl = &tsd_removed_ts_kv_range;
            }

            void configure_map_value_ops()
            {
                value_map_ops = map_ops_for_surface<SlotMapSurface::Live>();
                value_map_ops.dynamic_storage_metrics_impl = &tsd_dynamic_storage_metrics;
                value_map_ops.owning_type_impl      = &canonical_value_binding;
                value_map_ops.copy_construct_view_impl = &map_copy_construct_view<SlotMapSurface::Live>;
                value_map_ops.copy_assign_view_impl    = &map_copy_assign_view<SlotMapSurface::Live>;

                key_set_value_ops = SetValueOps{
                    {{ValueOpsKind::Set, this, false, &map_key_set_hash, &map_key_set_equals, &map_key_set_compare,
                      &map_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &map_key_set_to_python
#endif
                     },
                     &map_live_size,
                     &map_key_at_index,
                     &map_key_binding,
                     &map_keys_range,
                     nullptr},
                    &map_contains_key,
                };
                key_set_value_ops.dynamic_storage_metrics_impl = &tss_dynamic_storage_metrics;
                key_set_value_ops.owning_type_impl      = &canonical_value_binding;
                key_set_value_ops.copy_construct_view_impl = &set_copy_construct_view<SlotSetSurface::Live>;
                key_set_value_ops.copy_assign_view_impl    = &set_copy_assign_view<SlotSetSurface::Live>;
            }

            void configure_modified_map_ops()
            {
                modified_map_ops = map_ops_for_surface<SlotMapSurface::Modified>();
                modified_map_ops.owning_type_impl      = &canonical_value_binding;
                modified_map_ops.copy_construct_view_impl = &map_copy_construct_view<SlotMapSurface::Modified>;
                modified_map_ops.copy_assign_view_impl    = &map_copy_assign_view<SlotMapSurface::Modified>;

                dict_delta_bundle_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &dict_delta_hash, &dict_delta_equals, &dict_delta_compare,
                     &dict_delta_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &dict_delta_to_python
#endif
                    },
                    &dict_delta_size,
                    &dict_delta_element_at,
                    &dict_delta_element_binding,
                    &dict_delta_make_range,
                    nullptr,
                };
                dict_delta_bundle_ops.owning_type_impl      = &canonical_value_binding;
                dict_delta_bundle_ops.copy_construct_view_impl = &dict_delta_copy_construct_view;
                dict_delta_bundle_ops.copy_assign_view_impl    = &dict_delta_copy_assign_view;
            }

            void bind_tsd_surfaces(const TSValueTypeMetaData &schema_, const MemoryUtils::StoragePlan &plan_)
            {
                const auto *value_schema = schema_.value_schema;
                const auto *delta_schema = schema_.delta_value_schema;
                if (value_schema == nullptr || delta_schema == nullptr)
                {
                    throw std::logic_error("TSD schemas are not populated");
                }
                const auto key_binding = dict_layout.key_binding;
                const auto element_type = dict_layout.element_type;
                const TSDSlotStorage sample{key_binding, element_type};
                const auto &keys = sample.keys();
                const auto &values = sample.debug_values();
                const StableSlotDebugView key_slots = keys.debug_slot_view();
                const StableSlotDebugView value_slots = values.debug_slot_view();
                auto debug_layout = tss_value_debug_layout(sample, key_binding);
                DebugDynamicFlags flags = DebugDynamicFlags::DataIsIndirect |
                                          DebugDynamicFlags::KeyDataIsIndirect |
                                          DebugDynamicFlags::DataIsPointerTable |
                                          DebugDynamicFlags::KeyDataIsPointerTable |
                                          DebugDynamicFlags::HasSlotState |
                                          DebugDynamicFlags::SlotIndexIsIndirect |
                                          DebugDynamicFlags::SlotSizeIsIndirect;
                if (value_slots.pointers_tagged) { flags = flags | DebugDynamicFlags::DataPointersAreTagged; }
                if (key_slots.pointers_tagged) { flags = flags | DebugDynamicFlags::KeyPointersAreTagged; }
                if (key_slots.state_tagged) { flags = flags | DebugDynamicFlags::SlotStateIsTaggedPointer; }
                debug_layout.flags = flags;
                debug_layout.key_auxiliary_offset =
                    static_cast<std::uint32_t>(key_slots.pointer_table_offset);
                debug_layout.size_offset = key_slots.slot_count_offset;
                debug_layout.data_offset = debug_address_offset(sample, value_slots.implementation_pointer);
                debug_layout.stride = element_type.checked_plan().layout.size;
                debug_layout.key_data_offset = debug_address_offset(sample, key_slots.implementation_pointer);
                debug_layout.key_stride = key_binding.checked_plan().layout.size;
                debug_layout.state_offset = key_slots.state_offset;
                debug_layout.auxiliary_offset = value_slots.pointer_table_offset;
                const auto &debug = intern_dynamic_debug_descriptor(
                    value_schema->header, plan_, DebugLayoutKind::KeyedSlots,
                    key_binding.record(), dict_layout.element_value_binding.record(), debug_layout,
                    &value_map_ops);
                dict_layout.value_binding = intern_value_type(*value_schema, plan_, value_map_ops, &debug);
                dict_layout.tracking_offset = debug_offset(sample, sample.tracking());

                if (delta_schema->value_kind() != ValueTypeKind::Bundle || delta_schema->field_count != 3)
                {
                    throw std::logic_error(
                        "TSD delta schema must be Bundle{removed, modified, removed_strict}");
                }
                removed_set_binding = intern_value_type(*delta_schema->fields[0].type, plan_, removed_set_ops);
                modified_map_binding = intern_value_type(*delta_schema->fields[1].type, plan_, modified_map_ops);
                empty_strict_set = ts_data_detail::interned_empty_set(schema_.key_type());
                added_set_binding = intern_value_type(*TypeRegistry::instance().set(schema_.key_type()),
                                                              plan_, added_set_ops);
                dict_layout.delta_binding = intern_value_type(*delta_schema, plan_, dict_delta_bundle_ops);

                key_set_value_binding = intern_value_type(*TypeRegistry::instance().set(schema_.key_type()),
                                                                  plan_, key_set_value_ops);
                set_layout.value_binding = key_set_value_binding;

                // The key-set projection carries DEDICATED tracking: its
                // ``modified`` reports actual membership changes only, not the
                // dictionary's value ticks (subscriptions stay on the shared
                // root set, so notification wiring is unchanged).
                key_set_ts_ops = set_ops;
                {
                    // The projection is STRICTLY read-only: it reports the
                    // owner's mutations (dedicated tracking + observers) but
                    // can never perform one — every mutating entry point is
                    // stripped back to the throwing defaults, over and above
                    // the ``allows_mutation = false`` gate.
                    const TSDataOps  read_only_base{};
                    const TSSDataOps read_only_set{};
                    TSDataOps       &ks = key_set_ts_ops;
                    ks.allows_mutation           = false;
                    ks.tracking_impl             = &tsd_key_set_tracking;
                    ks.mutable_tracking_impl     = &tsd_key_set_mutable_tracking;   // subscriptions only
                    ks.has_current_value_impl    = &tsd_key_set_has_current_value;
                    ks.copy_value_from_impl      = read_only_base.copy_value_from_impl;
                    ks.apply_delta_impl          = read_only_base.apply_delta_impl;
                    ks.mutable_value_memory_impl = read_only_base.mutable_value_memory_impl;
                    ks.mutable_delta_memory_impl = read_only_base.mutable_delta_memory_impl;
                    ks.reset_delta_impl          = read_only_base.reset_delta_impl;
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                    ks.from_python_impl          = read_only_base.from_python_impl;
#endif
                    key_set_ts_ops.insert_key_impl = read_only_set.insert_key_impl;
                    key_set_ts_ops.insert_key_move_impl = read_only_set.insert_key_move_impl;
                    key_set_ts_ops.remove_key_impl = read_only_set.remove_key_impl;
                    key_set_ts_ops.remove_slot_impl = read_only_set.remove_slot_impl;
                    key_set_ts_ops.touch_impl      = read_only_set.touch_impl;
                }
                const auto *key_set_schema = TypeRegistry::instance().tss(schema_.key_type());
                const auto role_label = role == TypeRole::Data
                                            ? std::string_view{"ts.tsd.key-set.data"}
                                            : role == TypeRole::Input
                                                  ? std::string_view{"ts.tsd.key-set.input"}
                                                  : std::string_view{"ts.tsd.key-set.output"};
                key_set_ts_type = TSRoleTypeRef{intern_ts_type(
                    *key_set_schema, role, plan_, key_set_ts_ops, role_label)};
                dict_layout.key_set_type = key_set_ts_type;
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] MapValueOps map_ops_for_surface()
            {
                return MapValueOps{
                    {{ValueOpsKind::Map, this, false, &map_hash<Surface>, &map_equals<Surface>,
                      &map_compare<Surface>,
                      &map_to_string<Surface>
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &map_to_python<Surface>
#endif
                     },
                     &map_size<Surface>,
                     &map_key_at_index<Surface>,
                     &map_key_binding,
                     &map_keys_range<Surface>,
                     nullptr},
                    &map_contains<Surface>,
                    &map_value_at<Surface>,
                    &map_value_at_index<Surface>,
                    &map_value_binding<Surface>,
                    &map_keys_range<Surface>,
                    &map_values_range<Surface>,
                    &map_kv_range<Surface>,
                    &map_key_set,
                };
            }

            [[nodiscard]] static const TSDContext *ctxd(const void *context) noexcept
            {
                return static_cast<const TSDContext *>(context);
            }

            template <SlotMapSurface Surface>
            static void map_copy_construct_view(const void *context,
                                                const ValueTypeRef &binding,
                                                void *dst,
                                                const void *memory)
            {
                auto storage = build_map_storage<Surface>(context, binding, memory);
                std::construct_at(static_cast<MapStorage *>(dst), std::move(storage));
            }

            template <SlotMapSurface Surface>
            static void map_copy_assign_view(const void *context,
                                             const ValueTypeRef &binding,
                                             void *dst,
                                             const void *memory)
            {
                *static_cast<MapStorage *>(dst) = build_map_storage<Surface>(context, binding, memory);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static MapStorage build_map_storage(const void *context,
                                                              const ValueTypeRef &binding,
                                                              const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Map)
                {
                    throw std::logic_error("TSD map copy requires a canonical map binding");
                }

                const auto *state       = ctxd(context);
                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->key_type);
                const auto projected_value_binding = map_value_binding<Surface>(context, memory);
                const auto value_binding = projected_value_binding == nullptr
                                               ? ValueTypeRef{}
                                               : projected_value_binding.ops_ref().owning_type(
                                                     projected_value_binding);
                if (key_binding == nullptr || key_binding != state->dict_layout.key_binding)
                {
                    throw std::logic_error("TSD map copy key binding is not resolved");
                }
                if (value_binding == nullptr ||
                    value_binding.schema() != binding.schema()->element_type)
                {
                    throw std::logic_error("TSD map copy value binding is not resolved");
                }

                MapBuilder builder{key_binding, value_binding};
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    Value owned_value{value};
                    if (owned_value.binding() != value_binding)
                    {
                        throw std::logic_error("TSD map copy materialized the wrong owning value binding");
                    }
                    builder.set_item_copy(key.data(), owned_value.view().data());
                }
                return builder.build_storage();
            }

            static void dict_delta_copy_construct_view(const void *context,
                                                       const ValueTypeRef &binding,
                                                       void *dst,
                                                       const void *memory)
            {
                const auto &plan = binding.checked_plan();
                plan.default_construct(dst);
                auto rollback = make_scope_exit([&]() noexcept { plan.destroy(dst); });
                dict_delta_copy_assign_view(context, binding, dst, memory);
                rollback.release();
            }

            static void dict_delta_copy_assign_view(const void *context,
                                                    const ValueTypeRef &binding,
                                                    void *dst,
                                                    const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Bundle ||
                    binding.schema()->field_count != 3)
                {
                    throw std::logic_error(
                        "TSD delta copy requires canonical Bundle{removed, modified, removed_strict}");
                }

                const auto &plan = binding.checked_plan();
                if (!plan.is_composite() || plan.component_count() < 3)
                {
                    throw std::logic_error("TSD delta copy requires a three-field structured plan");
                }

                copy_projected_bundle_fields(
                    binding, dst, 3,
                    [&](std::size_t index) {
                        return dict_delta_element_binding(context, memory, index);
                    },
                    [&](std::size_t index) {
                        return dict_delta_element_at(context, memory, index);
                    });
            }

          public:
            [[nodiscard]] static const TSDataLayout *tsd_layout(const void *context) noexcept
            {
                return &ctxd(context)->dict_layout;
            }

            [[nodiscard]] static const TSDataTracking *tsd_tracking(const void *, const void *memory) noexcept
            {
                return &storage<TSDSlotStorage>(memory).tracking();
            }

            [[nodiscard]] static TSDataTracking *tsd_mutable_tracking(const void *, void *memory) noexcept
            {
                return &storage<TSDSlotStorage>(memory).mutable_tracking();
            }

            [[nodiscard]] static bool tsd_has_current_value(const void *, const void *memory) noexcept
            {
                return storage<TSDSlotStorage>(memory).tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool tsd_all_valid(const void *context, const void *memory)
            {
                if (!tsd_has_current_value(context, memory)) { return false; }

                const auto *state = ctxd(context);
                const auto &store = storage<TSDSlotStorage>(memory);
                const auto &child_ops = state->dict_layout.element_type.ops_ref();
                for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                {
                    if (!store.slot_live(slot)) { continue; }
                    if (!child_ops.all_valid_impl(child_ops.context, store.child_at_slot(slot))) { return false; }
                }
                return true;
            }

            [[nodiscard]] static const void *tsd_value_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *tsd_mutable_value_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static const void *tsd_delta_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *tsd_mutable_delta_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            static void tsd_reset_delta(const void *, void *memory)
            {
                storage<TSDSlotStorage>(memory).reset_delta();
            }


            static void tsd_record_child_modified(const void *, void *memory, std::size_t child_id,
                                                  DateTime modified_time)
            {
                storage<TSDSlotStorage>(memory).record_child_modified(child_id, modified_time);
            }

            [[nodiscard]] static bool tsd_copy_value_from(const void *context, void *memory, const ValueView &source,
                                                          DateTime modified_time)
            {
                (void)context;
                (void)memory;
                (void)source;
                (void)modified_time;
                throw std::logic_error(
                    "TSD copy_value_from must be performed through TSDDataMutationView so child notifications use TSParentLink");
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object tsd_to_python(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                return state->dict_layout.value_binding.ops_ref().to_python(memory);
            }

            [[nodiscard]] static nb::object tsd_delta_to_python(const void *context,
                                                                const void *memory,
                                                                DateTime evaluation_time)
            {
                const auto *state = ctxd(context);
                if (storage<TSDSlotStorage>(memory).tracking().last_modified_time != evaluation_time)
                {
                    return nb::none();
                }
                return state->dict_layout.delta_binding.ops_ref().to_python(memory);
            }

            [[nodiscard]] static bool tsd_from_python(const void *context,
                                                      void       *memory,
                                                      nb::handle  source,
                                                      DateTime modified_time)
            {
                if (memory == nullptr) { throw std::logic_error("TSD from_python requires live storage"); }
                if (source.is_none()) { throw std::invalid_argument("TSD from_python requires a non-None source"); }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("TSD from_python requires a concrete evaluation time");
                }
                if (!python_has_items(source))
                {
                    throw std::invalid_argument("TSD from_python expects a Python mapping");
                }

                auto       &target = storage<TSDSlotStorage>(memory);
                const auto *state  = ctxd(context);

                nb::object removed;
                nb::object modified;
                const bool has_removed = python_named_field(source, "removed", removed);
                const bool has_modified = python_named_field(source, "modified", modified);
                if (has_removed || has_modified)
                {
                    const bool first_for_parent = target.tracking().last_modified_time != modified_time;
                    if (has_removed && !removed.is_none())
                    {
                        for_each_python_iterable(removed, "TSD removed update", [&](nb::handle item) {
                            Value key = value_from_python(state->dict_layout.key_binding, item, "TSD removed update");
                            static_cast<void>(target.remove_key(key.view(), modified_time));
                        });
                    }

                    if (has_modified && !modified.is_none())
                    {
                        for_each_python_mapping_item(modified, "TSD modified update", [&](nb::handle key_source,
                                                                                          nb::handle value_source) {
                            if (value_source.is_none()) { return; }

                            Value key = value_from_python(state->dict_layout.key_binding, key_source,
                                                          "TSD modified update key");
                            const auto result = target.insert_key(key.view(), modified_time);

                            auto rollback_insert = make_scope_exit<true>([&] {
                                if (result.changed) { static_cast<void>(target.remove_key(key.view(), modified_time)); }
                            });

                            const auto &child_ops    = state->dict_layout.element_type.ops_ref();
                            void       *child_memory = target.child_memory_for_write(result.slot);
                            if (!child_ops.from_python_impl(child_ops.context, child_memory, value_source,
                                                            modified_time))
                            {
                                return;
                            }

                            auto *child_tracking = child_ops.mutable_tracking_impl(child_ops.context, child_memory);
                            if (child_tracking == nullptr)
                            {
                                throw std::logic_error("TSD child has no tracking record");
                            }
                            if (!child_tracking->record_modified(modified_time))
                            {
                                throw std::logic_error("TSD child reported a duplicate Python update modification");
                            }
                            target.record_child_modified(result.slot, modified_time);
                            rollback_insert.release();
                        });
                    }
                    return first_for_parent;
                }

                std::vector<std::pair<Value, nb::object>> entries;
                for_each_python_mapping_item(source, "TSD value", [&](nb::handle key_source, nb::handle value_source) {
                    if (value_source.is_none())
                    {
                        throw std::invalid_argument("TSD from_python does not allow None child values");
                    }
                    entries.emplace_back(
                        value_from_python(state->dict_layout.key_binding, key_source, "TSD value key"),
                        nb::borrow<nb::object>(value_source));
                });

                const bool newly_touched = target.touch(modified_time);
                for (const auto &[key, value_source] : entries)
                {
                    const auto result = target.insert_key(key.view(), modified_time);
                    const auto &child_ops = state->dict_layout.element_type.ops_ref();
                    void       *child_memory = target.child_memory_for_write(result.slot);
                    if (child_ops.from_python_impl(child_ops.context, child_memory, value_source, modified_time))
                    {
                        auto *child_tracking = child_ops.mutable_tracking_impl(child_ops.context, child_memory);
                        if (child_tracking == nullptr)
                        {
                            throw std::logic_error("TSD child has no tracking record");
                        }
                        if (!child_tracking->record_modified(modified_time))
                        {
                            throw std::logic_error("TSD child reported a duplicate Python value modification");
                        }
                        target.record_child_modified(result.slot, modified_time);
                    }
                }

                const auto &key_ops = state->dict_layout.key_binding.ops_ref();
                std::vector<Value> removals;
                for (const auto key : set_make_range<SlotSetSurface::Live>(context, memory))
                {
                    const bool keep = std::any_of(entries.begin(), entries.end(), [&](const auto &entry) {
                        return key_ops.equals(key.data(), entry.first.view().data());
                    });
                    if (!keep) { removals.emplace_back(key); }
                }
                for (const auto &key : removals)
                {
                    static_cast<void>(target.remove_key(key.view(), modified_time));
                }
                return newly_touched;
            }
#endif

            [[nodiscard]] static const void *tsd_child_at_slot(const void *, const void *memory, std::size_t slot)
            {
                return storage<TSDSlotStorage>(memory).child_at_slot(slot);
            }

            [[nodiscard]] static DynamicStorageMetrics tsd_dynamic_storage_metrics(
                const void *, const void *memory) noexcept
            {
                return storage<TSDSlotStorage>(memory).dynamic_storage_metrics();
            }

            [[nodiscard]] static bool tsd_slot_modified(const void *, const void *memory, std::size_t slot)
            {
                return storage<TSDSlotStorage>(memory).slot_modified(slot);
            }

            [[nodiscard]] static std::size_t tsd_next_modified_slot(const void *, const void *memory,
                                                                    std::size_t previous)
            {
                return storage<TSDSlotStorage>(memory).next_modified_slot(previous);
            }

            [[nodiscard]] static bool live_slot_predicate(const void *, const void *memory, std::size_t slot)
            {
                return storage<TSDSlotStorage>(memory).slot_live(slot);
            }

            [[nodiscard]] static bool valid_slot_predicate(const void *context, const void *memory, std::size_t slot)
            {
                const auto *state = ctxd(context);
                const auto &store = storage<TSDSlotStorage>(memory);
                if (!store.slot_live(slot)) { return false; }
                const auto &child_ops = state->dict_layout.element_type.ops_ref();
                const auto *child_memory = store.child_at_slot(slot);
                return child_ops.tracking_impl(child_ops.context, child_memory)->last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool modified_slot_predicate(const void *, const void *memory, std::size_t slot)
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                return store.slot_live(slot) && store.slot_modified(slot);
            }

            [[nodiscard]] static bool added_slot_predicate(const void *, const void *memory, std::size_t slot)
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                return store.slot_occupied(slot) && store.slot_added(slot);
            }

            [[nodiscard]] static bool removed_slot_predicate(const void *, const void *memory, std::size_t slot)
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                return store.slot_occupied(slot) && store.slot_removed(slot);
            }

            [[nodiscard]] static ValueView ts_key_projector(const void *context, const void *memory,
                                                            std::size_t slot)
            {
                const auto *state = ctxd(context);
                return ValueView{state->dict_layout.key_binding, storage<TSDSlotStorage>(memory).key_at_slot(slot)};
            }

            [[nodiscard]] static TSDataView ts_value_projector(const void *context, const void *memory,
                                                               std::size_t slot)
            {
                const auto *state = ctxd(context);
                return TSDataView{state->dict_layout.element_type,
                                  storage<TSDSlotStorage>(memory).child_at_slot(slot)};
            }

            [[nodiscard]] static std::pair<ValueView, TSDataView> ts_kv_projector(const void *context,
                                                                                  const void *memory,
                                                                                  std::size_t slot)
            {
                const auto *state = ctxd(context);
                const auto &store = storage<TSDSlotStorage>(memory);
                return {ValueView{state->dict_layout.key_binding, store.key_at_slot(slot)},
                        TSDataView{state->dict_layout.element_type, store.child_at_slot(slot)}};
            }

            [[nodiscard]] static Range<TSDataView> tsd_ts_values_range(const void *context, const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &live_slot_predicate,
                    .projector = &ts_value_projector,
                };
            }

            [[nodiscard]] static Range<ValueView> tsd_valid_keys_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &valid_slot_predicate,
                    .projector = &ts_key_projector,
                };
            }

            [[nodiscard]] static Range<TSDataView> tsd_valid_ts_values_range(const void *context,
                                                                             const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &valid_slot_predicate,
                    .projector = &ts_value_projector,
                };
            }

            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> tsd_ts_kv_range(const void *context,
                                                                                      const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &live_slot_predicate,
                    .projector = &ts_kv_projector,
                };
            }

            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> tsd_valid_ts_kv_range(const void *context,
                                                                                            const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &valid_slot_predicate,
                    .projector = &ts_kv_projector,
                };
            }

            [[nodiscard]] static Range<ValueView> tsd_modified_keys_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &modified_slot_predicate,
                    .projector = &ts_key_projector,
                };
            }

            [[nodiscard]] static Range<TSDataView> tsd_modified_ts_values_range(const void *context,
                                                                                const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &modified_slot_predicate,
                    .projector = &ts_value_projector,
                };
            }

            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> tsd_modified_ts_kv_range(const void *context,
                                                                                               const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &modified_slot_predicate,
                    .projector = &ts_kv_projector,
                };
            }

            [[nodiscard]] static Range<TSDataView> tsd_added_ts_values_range(const void *context, const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &added_slot_predicate,
                    .projector = &ts_value_projector,
                };
            }

            [[nodiscard]] static Range<TSDataView> tsd_removed_ts_values_range(const void *context, const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &removed_slot_predicate,
                    .projector = &ts_value_projector,
                };
            }

            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> tsd_added_ts_kv_range(const void *context,
                                                                                            const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &added_slot_predicate,
                    .projector = &ts_kv_projector,
                };
            }

            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> tsd_removed_ts_kv_range(const void *context,
                                                                                              const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &removed_slot_predicate,
                    .projector = &ts_kv_projector,
                };
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static bool map_slot_in_surface(const TSDSlotStorage &store, std::size_t slot) noexcept
            {
                if constexpr (Surface == SlotMapSurface::Live) { return store.slot_live(slot); }
                return store.slot_live(slot) && store.slot_modified(slot);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static std::size_t map_size(const void *, const void *memory) noexcept
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                if constexpr (Surface == SlotMapSurface::Live) { return store.size(); }
                std::size_t count = 0;
                for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                {
                    if (map_slot_in_surface<Surface>(store, slot)) { ++count; }
                }
                return count;
            }

            [[nodiscard]] static std::size_t map_live_size(const void *context, const void *memory) noexcept
            {
                return map_size<SlotMapSurface::Live>(context, memory);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static std::size_t nth_map_slot(const void *memory, std::size_t index)
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < store.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(store, slot)) { continue; }
                    if (seen++ == index) { return slot; }
                }
                throw std::out_of_range("slot map index out of range");
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static const void *map_key_at_index(const void *, const void *memory, std::size_t index)
            {
                return storage<TSDSlotStorage>(memory).key_at_slot(nth_map_slot<Surface>(memory, index));
            }

            [[nodiscard]] static const void *map_key_at_index(const void *context, const void *memory,
                                                              std::size_t index)
            {
                return map_key_at_index<SlotMapSurface::Live>(context, memory, index);
            }

            [[nodiscard]] static ValueTypeRef map_key_binding(const void *context, const void *,
                                                                         std::size_t) noexcept
            {
                return ctxd(context)->dict_layout.key_binding;
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static ValueTypeRef map_value_binding(const void *context, const void *) noexcept
            {
                const auto *state = ctxd(context);
                if constexpr (Surface == SlotMapSurface::Live) { return state->dict_layout.element_value_binding; }
                else { return state->dict_layout.element_delta_binding; }
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static const void *map_value_at_slot(const void *context, const void *memory,
                                                               std::size_t slot)
            {
                const auto *state = ctxd(context);
                const auto &store = storage<TSDSlotStorage>(memory);
                const auto &child_ops = state->dict_layout.element_type.ops_ref();
                const auto *child_memory = store.child_at_slot(slot);
                if constexpr (Surface == SlotMapSurface::Live)
                {
                    return child_ops.value_memory_impl(child_ops.context, child_memory);
                }
                else { return child_ops.delta_memory_impl(child_ops.context, child_memory); }
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static const void *map_value_at_index(const void *context, const void *memory,
                                                                std::size_t index)
            {
                return map_value_at_slot<Surface>(context, memory, nth_map_slot<Surface>(memory, index));
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static bool map_contains(const void *, const void *memory, const void *key)
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                const auto  slot  = store.keys().find_stored_slot(key);
                return slot != KeySlotStore::npos && map_slot_in_surface<Surface>(store, slot);
            }

            [[nodiscard]] static bool map_contains_key(const void *context, const void *memory, const void *key)
            {
                return map_contains<SlotMapSurface::Live>(context, memory, key);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static const void *map_value_at(const void *context, const void *memory, const void *key)
            {
                const auto &store = storage<TSDSlotStorage>(memory);
                const auto  slot  = store.keys().find_stored_slot(key);
                if (slot == KeySlotStore::npos || !map_slot_in_surface<Surface>(store, slot)) { return nullptr; }
                return map_value_at_slot<Surface>(context, memory, slot);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static bool map_range_predicate(const void *, const void *memory, std::size_t slot)
            {
                return map_slot_in_surface<Surface>(storage<TSDSlotStorage>(memory), slot);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static ValueView map_key_projector(const void *context, const void *memory,
                                                             std::size_t slot)
            {
                return ValueView{ctxd(context)->dict_layout.key_binding,
                                 storage<TSDSlotStorage>(memory).key_at_slot(slot)};
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static ValueView map_value_projector(const void *context, const void *memory,
                                                               std::size_t slot)
            {
                return ValueView{map_value_binding<Surface>(context, memory),
                                 map_value_at_slot<Surface>(context, memory, slot)};
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static std::pair<ValueView, ValueView> map_kv_projector(const void *context,
                                                                                  const void *memory,
                                                                                  std::size_t slot)
            {
                return {map_key_projector<Surface>(context, memory, slot),
                        map_value_projector<Surface>(context, memory, slot)};
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static Range<ValueView> map_keys_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &map_range_predicate<Surface>,
                    .projector = &map_key_projector<Surface>,
                };
            }

            [[nodiscard]] static Range<ValueView> map_keys_range(const void *context, const void *memory)
            {
                return map_keys_range<SlotMapSurface::Live>(context, memory);
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static Range<ValueView> map_values_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &map_range_predicate<Surface>,
                    .projector = &map_value_projector<Surface>,
                };
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static KeyValueRange<ValueView, ValueView> map_kv_range(const void *context,
                                                                                  const void *memory)
            {
                return KeyValueRange<ValueView, ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = storage<TSDSlotStorage>(memory).slot_capacity(),
                    .predicate = &map_range_predicate<Surface>,
                    .projector = &map_kv_projector<Surface>,
                };
            }

            [[nodiscard]] static SetView map_key_set(const void *context, ValueTypeRef, const void *memory)
            {
                return ValueView{ctxd(context)->key_set_value_binding, memory}.as_set();
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static std::size_t map_hash(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                const auto &key_ops = state->dict_layout.key_binding.ops_ref();
                const auto value_binding = map_value_binding<Surface>(context, memory);
                const auto &value_ops = value_binding.ops_ref();
                std::size_t result = 0;
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    const auto value_hash = value.has_value() ? value_ops.hash(value.data()) : std::size_t{0};
                    result ^= combine_hash(key_ops.hash(key.data()), value_hash);
                }
                return result;
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static bool map_equals(const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    if (map_size<Surface>(context, lhs) != map_size<Surface>(context, rhs)) { return false; }
                    const auto value_binding = map_value_binding<Surface>(context, lhs);
                    const auto &value_ops = value_binding.ops_ref();
                    for (const auto [key, value] : map_kv_range<Surface>(context, lhs))
                    {
                        if (!map_contains<Surface>(context, rhs, key.data())) { return false; }
                        const auto *rhs_value = map_value_at<Surface>(context, rhs, key.data());
                        if (!value.has_value())
                        {
                            if (rhs_value != nullptr) { return false; }
                            continue;
                        }
                        if (rhs_value == nullptr || !value_ops.equals(value.data(), rhs_value)) { return false; }
                    }
                    return true;
                });
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static std::partial_ordering map_compare(const void *context, const void *lhs,
                                                                    const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                return map_equals<Surface>(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                              : std::partial_ordering::unordered;
            }

            template <SlotMapSurface Surface>
            [[nodiscard]] static std::string map_to_string(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                const auto &key_ops = state->dict_layout.key_binding.ops_ref();
                const auto value_binding = map_value_binding<Surface>(context, memory);
                const auto &value_ops = value_binding.ops_ref();
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                bool first = true;
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                    first = false;
                    fmt::format_to(std::back_inserter(out), "{}: {}",
                                   key_ops.to_string(key.data()),
                                   value.has_value() ? value_ops.to_string(value.data()) : std::string{"<unset>"});
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            template <SlotMapSurface Surface>
            [[nodiscard]] static nb::object map_to_python(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                const auto &key_ops = state->dict_layout.key_binding.ops_ref();
                const auto value_binding = map_value_binding<Surface>(context, memory);
                const auto &value_ops = value_binding.ops_ref();
                nb::dict result;
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    result[key_ops.to_python(key.data())] =
                        value.has_value() ? value_ops.to_python(value.data()) : nb::none();
                }
                return result;
            }
#endif

            [[nodiscard]] static std::size_t map_key_set_hash(const void *context, const void *memory)
            {
                return set_hash<SlotSetSurface::Live>(context, memory);
            }

            [[nodiscard]] static bool map_key_set_equals(const void *context, const void *lhs,
                                                         const void *rhs) noexcept
            {
                return set_equals<SlotSetSurface::Live>(context, lhs, rhs);
            }

            [[nodiscard]] static std::partial_ordering map_key_set_compare(const void *context, const void *lhs,
                                                                           const void *rhs) noexcept
            {
                return set_compare<SlotSetSurface::Live>(context, lhs, rhs);
            }

            [[nodiscard]] static std::string map_key_set_to_string(const void *context, const void *memory)
            {
                return set_to_string<SlotSetSurface::Live>(context, memory);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object map_key_set_to_python(const void *context, const void *memory)
            {
                return set_to_python<SlotSetSurface::Live>(context, memory);
            }
#endif

            [[nodiscard]] static std::size_t dict_delta_size(const void *, const void *) noexcept { return 3; }

            [[nodiscard]] static const void *dict_delta_element_at(const void *context, const void *memory,
                                                                   std::size_t index)
            {
                if (index < 2) { return memory; }
                if (index == 2) { return ctxd(context)->empty_strict_set->view().data(); }
                throw std::out_of_range("TSD delta bundle index out of range");
            }

            [[nodiscard]] static ValueTypeRef dict_delta_element_binding(const void *context,
                                                                                    const void *,
                                                                                    std::size_t index) noexcept
            {
                const auto *state = ctxd(context);
                if (index == 2) { return state->empty_strict_set->binding(); }
                return index == 0 ? state->removed_set_binding : state->modified_map_binding;
            }

            [[nodiscard]] static ValueView dict_delta_projector(const void *context, const void *memory,
                                                                std::size_t index)
            {
                return ValueView{dict_delta_element_binding(context, memory, index),
                                 dict_delta_element_at(context, memory, index)};
            }

            [[nodiscard]] static Range<ValueView> dict_delta_make_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = 3,
                    .predicate = nullptr,
                    .projector = &dict_delta_projector,
                };
            }

            [[nodiscard]] static std::size_t dict_delta_hash(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                return combine_hash(combine_hash(state->removed_set_binding.ops_ref().hash(memory),
                                                 state->modified_map_binding.ops_ref().hash(memory)),
                                    state->empty_strict_set->binding().ops_ref().hash(
                                        state->empty_strict_set->view().data()));
            }

            [[nodiscard]] static bool dict_delta_equals(const void *context, const void *lhs,
                                                        const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctxd(context);
                    return state->removed_set_binding.ops_ref().equals(lhs, rhs) &&
                           state->modified_map_binding.ops_ref().equals(lhs, rhs);
                });
            }

            [[nodiscard]] static std::partial_ordering dict_delta_compare(const void *context, const void *lhs,
                                                                          const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                return dict_delta_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                            : std::partial_ordering::unordered;
            }

            [[nodiscard]] static std::string dict_delta_to_string(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                return fmt::format("{{removed: {}, modified: {}, removed_strict: {}}}",
                                   state->removed_set_binding.ops_ref().to_string(memory),
                                   state->modified_map_binding.ops_ref().to_string(memory),
                                   state->empty_strict_set->binding().ops_ref().to_string(
                                       state->empty_strict_set->view().data()));
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object dict_delta_to_python(const void *context, const void *memory)
            {
                const auto *state = ctxd(context);
                nb::dict result;
                result[nb::str{"removed"}] = state->removed_set_binding.ops_ref().to_python(memory);
                result[nb::str{"modified"}] = state->modified_map_binding.ops_ref().to_python(memory);
                result[nb::str{"removed_strict"}] = state->empty_strict_set->binding().ops_ref().to_python(
                    state->empty_strict_set->view().data());
                return result;
            }
#endif
        };

        struct SlotContextKey
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            std::size_t                     storage_offset{0};
            ValueTypeRef                    key_binding{};
            TSRoleTypeRef                   element_type{};
            TypeRole                        role{TypeRole::Invalid};
            bool                            embedded{false};
            bool                            composite{false};

            [[nodiscard]] bool operator==(const SlotContextKey &) const noexcept = default;
        };

        struct SlotContextKeyHash
        {
            [[nodiscard]] std::size_t operator()(const SlotContextKey &key) const noexcept
            {
                auto seed = combine_hash(std::hash<const TSValueTypeMetaData *>{}(key.schema),
                                         std::hash<const MemoryUtils::StoragePlan *>{}(key.plan));
                seed = combine_hash(seed, key.storage_offset);
                seed = combine_hash(seed, std::hash<ValueTypeRef>{}(key.key_binding));
                seed = combine_hash(seed, std::hash<const TypeRecord *>{}(key.element_type.record()));
                seed = combine_hash(seed, static_cast<std::size_t>(key.role));
                seed = combine_hash(seed, key.embedded ? 1U : 0U);
                return combine_hash(seed, key.composite ? 1U : 0U);
            }
        };

        using SlotContextMap =
            std::unordered_map<SlotContextKey, std::unique_ptr<SlotContextBase>, SlotContextKeyHash>;

        [[nodiscard]] SlotContextMap &slot_contexts() noexcept
        {
            static SlotContextMap contexts;
            return contexts;
        }

        [[nodiscard]] std::recursive_mutex &slot_context_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        [[nodiscard]] ValueTypeRef key_binding_for(const TSValueTypeMetaData &schema)
        {
            const ValueTypeMetaData *key_schema = nullptr;
            if (schema.kind == TSTypeKind::TSS)
            {
                key_schema = schema.value_schema != nullptr ? schema.value_schema->element_type : nullptr;
            }
            else if (schema.kind == TSTypeKind::TSD)
            {
                key_schema = schema.key_type();
            }
            if (key_schema == nullptr) { throw std::logic_error("slot TSData key schema is not resolved"); }
            const auto binding = ValuePlanFactory::instance().type_for(key_schema);
            if (!binding) { throw std::logic_error("slot TSData key binding is not resolved"); }
            return binding;
        }

        [[nodiscard]] TSRoleTypeRef element_type_for(const TSValueTypeMetaData &schema, TypeRole role)
        {
            const auto *element_schema = schema.element_ts();
            if (element_schema == nullptr) { throw std::logic_error("TSD element TS schema is not resolved"); }
            auto &factory = TSDataPlanFactory::instance();
            if (role == TypeRole::Output) return factory.output_type_for(element_schema).as_role();
            return factory.data_type_for(element_schema).as_role();
        }

        [[nodiscard]] std::unique_ptr<SlotPlanEntry> make_tsd_slot_plan_entry(
            const ValueTypeRef &key_binding,
            TSRoleTypeRef element_type)
        {
            auto entry = std::make_unique<SlotPlanEntry>();
            entry->tsd_context = TSDStoragePlanContext{.key_binding = key_binding,
                                                       .element_type = element_type};
            // Deliberately no copy/move lifecycle hooks: TSD values expose
            // stable child TSData addresses, so assignment is expressed as
            // in-place key/child mutation through TSDDataMutationView.
            entry->storage_plan = std::make_unique<MemoryUtils::StoragePlan>(MemoryUtils::StoragePlan{
                .layout                       = MemoryUtils::layout_for<TSDSlotStorage>(),
                .lifecycle                    = {.construct      = &tsd_storage_construct,
                                                 .destroy        = &slot_storage_destroy<TSDSlotStorage>,
                                                 .copy_construct = nullptr,
                                                 .move_construct = nullptr,
                                                 .copy_assign    = nullptr,
                                                 .move_assign    = nullptr},
                .lifecycle_context            = &entry->tsd_context,
                .composite_kind_tag           = MemoryUtils::CompositeKind::None,
                .trivially_destructible       = false,
                .trivially_copyable           = false,
                .trivially_move_constructible = false,
            });
            entry->root_plan = entry->storage_plan.get();
            return entry;
        }
    } // namespace

    [[nodiscard]] bool is_slot_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        switch (schema.kind)
        {
        case TSTypeKind::TSS:
            return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                   schema.value_schema->try_value_kind() == ValueTypeKind::Set;
        case TSTypeKind::TSD:
            return schema.value_schema != nullptr && schema.delta_value_schema != nullptr &&
                   schema.value_schema->try_value_kind() == ValueTypeKind::Map && schema.element_ts() != nullptr;
        default:
            return false;
        }
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_plan(const TSValueTypeMetaData &schema)
    {
        return synthesise_slot_plan(schema, key_binding_for(schema));
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_plan(
        const TSValueTypeMetaData &schema,
        ValueTypeRef key_binding)
    {
        if (!is_slot_ts_data(schema))
        {
            throw std::logic_error("TSDataPlanFactory: slot storage requires TSS or TSD schema");
        }
        const auto *key_schema = schema.kind == TSTypeKind::TSS
                                     ? schema.value_schema->element_type
                                     : schema.key_type();
        if (!key_binding || key_binding.schema() != key_schema)
        {
            throw std::logic_error("TSDataPlanFactory: slot key binding has the wrong schema");
        }

        std::lock_guard<std::recursive_mutex> lock(slot_plan_mutex());
        auto &entries = custom_tsd_slot_plan_entries();
        const TSDSlotPlanKey key{&schema, key_binding, {}};
        if (const auto it = entries.find(key); it != entries.end()) { return it->second->root_plan; }

        auto entry = std::make_unique<SlotPlanEntry>();
        if (schema.kind == TSTypeKind::TSS)
        {
            entry->tss_context = TSSStoragePlanContext{.key_binding = key_binding};
            // Deliberately no copy/move lifecycle hooks: TSS slots are stable
            // path targets for observers and key-set projections.
            entry->storage_plan = std::make_unique<MemoryUtils::StoragePlan>(MemoryUtils::StoragePlan{
                .layout                       = MemoryUtils::layout_for<TSSSlotStorage>(),
                .lifecycle                    = {.construct      = &tss_storage_construct,
                                                 .destroy        = &slot_storage_destroy<TSSSlotStorage>,
                                                 .copy_construct = nullptr,
                                                 .move_construct = nullptr,
                                                 .copy_assign    = nullptr,
                                                 .move_assign    = nullptr},
                .lifecycle_context            = &entry->tss_context,
                .composite_kind_tag           = MemoryUtils::CompositeKind::None,
                .trivially_destructible       = false,
                .trivially_copyable           = false,
                .trivially_move_constructible = false,
            });
        }
        else
        {
            entry = make_tsd_slot_plan_entry(key_binding, element_type_for(schema, TypeRole::Data));
        }

        entry->root_plan = entry->storage_plan.get();
        const auto *result = entry->root_plan;
        entries.emplace(key, std::move(entry));
        return result;
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_tsd_plan(
        const TSValueTypeMetaData &schema,
        TSRoleTypeRef           element_type)
    {
        return synthesise_slot_tsd_plan(schema, key_binding_for(schema), element_type);
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_slot_tsd_plan(
        const TSValueTypeMetaData &schema,
        ValueTypeRef key_binding,
        TSRoleTypeRef element_type)
    {
        if (!is_slot_ts_data(schema) || schema.kind != TSTypeKind::TSD)
        {
            throw std::logic_error("TSDataPlanFactory: custom slot TSD storage requires a TSD schema");
        }
        if (!time_series_schema_equivalent(schema.element_ts(), element_type.schema()))
        {
            throw std::logic_error("TSDataPlanFactory: custom slot TSD element binding has the wrong schema");
        }

        if (!key_binding || key_binding.schema() != schema.key_type())
        {
            throw std::logic_error("TSDataPlanFactory: custom slot TSD key binding has the wrong schema");
        }
        const TSDSlotPlanKey key{&schema, key_binding, element_type};

        std::lock_guard<std::recursive_mutex> lock(slot_plan_mutex());
        auto &entries = custom_tsd_slot_plan_entries();
        if (const auto it = entries.find(key); it != entries.end()) { return it->second->root_plan; }

        auto entry = make_tsd_slot_plan_entry(key_binding, element_type);
        const auto *result = entry->root_plan;
        entries.emplace(key, std::move(entry));
        return result;
    }

    [[nodiscard]] const TSDataOps &slot_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                    const MemoryUtils::StoragePlan &plan,
                                                    std::size_t storage_offset,
                                                    TypeRole role,
                                                    bool embedded)
    {
        return slot_ts_data_ops(
            schema, plan, storage_offset, key_binding_for(schema), role, embedded);
    }

    [[nodiscard]] const TSDataOps &slot_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                    const MemoryUtils::StoragePlan &plan,
                                                    std::size_t storage_offset,
                                                    ValueTypeRef key_binding,
                                                    TypeRole role,
                                                    bool embedded)
    {
        if (storage_offset != 0)
        {
            throw std::logic_error("slot TSData currently expects the storage object at the root");
        }

        std::lock_guard<std::recursive_mutex> lock(slot_context_mutex());
        auto                       &contexts = slot_contexts();
        const auto element_type = schema.kind == TSTypeKind::TSD ? element_type_for(schema, role)
                                                                 : TSRoleTypeRef{};
        const SlotContextKey key{
            &schema, &plan, storage_offset, key_binding, element_type, role, embedded, false};
        if (const auto it = contexts.find(key); it != contexts.end()) { return it->second->ops_ref(); }

        std::unique_ptr<SlotContextBase> context;
        if (schema.kind == TSTypeKind::TSS)
        {
            context = std::make_unique<TSSContext>(schema, plan, key_binding, role, embedded);
        }
        else if (schema.kind == TSTypeKind::TSD)
        {
            context = std::make_unique<TSDContext>(schema, plan, key_binding, element_type, role, embedded, false);
        }
        else
        {
            throw std::logic_error("slot TSData ops require TSS or TSD schema");
        }

        auto *result = context.get();
        contexts.emplace(key, std::move(context));
        return result->ops_ref();
    }

    [[nodiscard]] const TSDataOps &slot_tsd_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                        const MemoryUtils::StoragePlan &plan,
                                                        std::size_t storage_offset,
                                                        TSRoleTypeRef                element_type,
                                                        TypeRole storage_role,
                                                        bool embedded,
                                                        bool composite)
    {
        return slot_tsd_ts_data_ops(schema, plan, storage_offset, key_binding_for(schema),
                                    element_type, storage_role, embedded, composite);
    }

    [[nodiscard]] const TSDataOps &slot_tsd_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                        const MemoryUtils::StoragePlan &plan,
                                                        std::size_t storage_offset,
                                                        ValueTypeRef key_binding,
                                                        TSRoleTypeRef element_type,
                                                        TypeRole storage_role,
                                                        bool embedded,
                                                        bool composite)
    {
        if (schema.kind != TSTypeKind::TSD)
        {
            throw std::logic_error("slot TSD ops require a TSD schema");
        }
        if (storage_offset != 0)
        {
            throw std::logic_error("slot TSD currently expects the storage object at the root");
        }
        if (!time_series_schema_equivalent(schema.element_ts(), element_type.schema()))
        {
            throw std::logic_error("slot TSD element binding has the wrong schema");
        }

        std::lock_guard<std::recursive_mutex> lock(slot_context_mutex());
        auto                &contexts = slot_contexts();
        const SlotContextKey key{
            &schema, &plan, storage_offset, key_binding, element_type, storage_role, embedded, composite};
        if (const auto it = contexts.find(key); it != contexts.end()) { return it->second->ops_ref(); }

        auto context = std::make_unique<TSDContext>(
            schema, plan, key_binding, element_type, storage_role, embedded, composite);
        auto *result = context.get();
        contexts.emplace(key, std::move(context));
        return result->ops_ref();
    }

    TSRoleTypeRef tsd_value_projection_type(TSRoleTypeRef element_type, TypeRole role)
    {
        return intern_tsd_value_projection_type(element_type, role);
    }

    void clear_slot_ts_data_contexts() noexcept
    {
        {
            std::lock_guard<std::recursive_mutex> lock(slot_context_mutex());
            slot_contexts().clear();
        }
        {
            std::lock_guard<std::recursive_mutex> lock(slot_plan_mutex());
            custom_tsd_slot_plan_entries().clear();
        }
        {
            std::lock_guard<std::recursive_mutex> lock(projection_mutex());
            projection_ops_cache().clear();
        }
    }
} // namespace hgraph::ts_data_plan_factory_detail
