#include <hgraph/util/scope.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include "../time_series/ts_data/ownership.h"

#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::ts_data_plan_factory_detail
{
    namespace
    {
        [[nodiscard]] std::size_t dynamic_combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        struct HeapOnlyTSDataStoragePolicy
        {
            static constexpr std::size_t inline_bytes = sizeof(void *);

            [[nodiscard]] static constexpr std::size_t storage_alignment() noexcept { return alignof(void *); }

            [[nodiscard]] static constexpr bool can_store_inline(MemoryUtils::StorageLayout, bool, bool) noexcept
            {
                return false;
            }
        };

        using TSDataErasedOwner =
            MemoryUtils::ErasedOwner<HeapOnlyTSDataStoragePolicy, TypeRecord>;

        struct DynamicTSLIndexEntry
        {
            std::int64_t ordinal_key{0};
            std::size_t next_modified{0};
        };

        class DynamicTSLStorage
        {
          public:
            DynamicTSLStorage() : ordinal_keys_(1)
            {
                ordinal_keys_.front().ordinal_key = MIN_DT.time_since_epoch().count();
            }

            DynamicTSLStorage(const DynamicTSLStorage &)            = delete;
            DynamicTSLStorage &operator=(const DynamicTSLStorage &) = delete;
            DynamicTSLStorage(DynamicTSLStorage &&)                 = delete;
            DynamicTSLStorage &operator=(DynamicTSLStorage &&)      = delete;
            ~DynamicTSLStorage() = default;

            [[nodiscard]] TSRoleTypeRef element_type() const noexcept { return element_type_; }
            [[nodiscard]] const TSDataTracking &tracking() const noexcept { return tracking_; }
            [[nodiscard]] TSDataTracking &mutable_tracking() noexcept { return tracking_; }
            [[nodiscard]] std::size_t size() const noexcept { return elements_.size(); }

            [[nodiscard]] const void *child_memory(std::size_t index) const
            {
                return elements_.at(index).data();
            }

            [[nodiscard]] void *child_memory(std::size_t index)
            {
                return elements_.at(index).data();
            }

            [[nodiscard]] const std::int64_t &ordinal_key(std::size_t index) const
            {
                return ordinal_keys_.at(index + 1).ordinal_key;
            }

            void record_child_modified(std::size_t index, DateTime modified_time)
            {
                auto &header = ordinal_keys_.front();
                const DateTime window{std::chrono::microseconds{header.ordinal_key}};
                // Monotonic delta window: a record carrying an older time (a
                // freshly bound link replaying source history) must not
                // rebase the ring and erase sibling marks recorded this
                // cycle. It cannot join the ring either (re-appending an
                // already-linked entry would corrupt it), so it is dropped.
                if (modified_time < window) { return; }
                if (modified_time != window)
                {
                    header.ordinal_key = modified_time.time_since_epoch().count();
                    header.next_modified = 0;
                }
                // Child tracking notifies its parent only on the first mutation
                // at a given evaluation time. Keep a circular list whose header
                // points at the tail, preserving notification order in the
                // existing ordinal-key allocation.
                const std::size_t position = index + 1;
                auto &entry = ordinal_keys_.at(position);
                if (header.next_modified == 0)
                {
                    entry.next_modified = position;
                }
                else
                {
                    auto &tail = ordinal_keys_[header.next_modified];
                    entry.next_modified = tail.next_modified;
                    tail.next_modified = position;
                }
                header.next_modified = position;
            }

            [[nodiscard]] std::size_t modified_index_count() const noexcept
            {
                const auto &header = ordinal_keys_.front();
                if (DateTime{std::chrono::microseconds{header.ordinal_key}} != tracking_.last_modified_time ||
                    header.next_modified == 0)
                {
                    return 0;
                }

                const std::size_t first = ordinal_keys_[header.next_modified].next_modified;
                std::size_t count = 1;
                for (std::size_t position = ordinal_keys_[first].next_modified; position != first;
                     position = ordinal_keys_[position].next_modified)
                {
                    ++count;
                }
                return count;
            }

            [[nodiscard]] std::size_t modified_index_at(std::size_t ordinal) const
            {
                const auto &header = ordinal_keys_.front();
                if (DateTime{std::chrono::microseconds{header.ordinal_key}} != tracking_.last_modified_time ||
                    header.next_modified == 0)
                {
                    throw std::out_of_range("dynamic TSL has no current modified indices");
                }
                std::size_t position = ordinal_keys_[header.next_modified].next_modified;
                for (std::size_t current = 0; current < ordinal; ++current)
                {
                    position = ordinal_keys_.at(position).next_modified;
                    if (position == ordinal_keys_[header.next_modified].next_modified)
                    {
                        throw std::out_of_range("dynamic TSL modified index ordinal is out of range");
                    }
                }
                return position - 1;
            }

            void ensure_size(std::size_t size, TSRoleTypeRef element_type)
            {
                if (element_type.record() == nullptr)
                    throw std::logic_error("dynamic TSL elements require canonical TypeRecords");
                if (element_type.schema() == nullptr || element_type.plan() == nullptr)
                    throw std::logic_error("dynamic TSL element type is not resolved");
                if (element_type_ && element_type_ != element_type)
                    throw std::logic_error("dynamic TSL element type cannot change after growth");
                const bool newly_bound = !element_type_ && size > elements_.size();
                if (newly_bound) { element_type_ = element_type; }
                auto binding_rollback = UnwindCleanupGuard([&] {
                    if (newly_bound && elements_.empty()) { element_type_ = {}; }
                });
                while (elements_.size() < size)
                {
                    const auto index = elements_.size();
                    elements_.emplace_back(*element_type.record());
                    auto rollback = UnwindCleanupGuard([&] { elements_.pop_back(); });
                    ordinal_keys_.push_back(DynamicTSLIndexEntry{
                        .ordinal_key = static_cast<std::int64_t>(index),
                    });
                    rollback.release();
                }
                binding_rollback.release();
            }

          private:
            TSRoleTypeRef                   element_type_{};
            TSDataTracking                    tracking_{};
            // Handles may move as the vector grows, but the child TSData bytes
            // must not. The heap-only policy above keeps published child
            // addresses stable while preserving vector locality for handles.
            std::vector<TSDataErasedOwner>  elements_{};
            std::vector<DynamicTSLIndexEntry> ordinal_keys_{};
        };

        void dynamic_list_storage_construct(void *dst, const void *)
        {
            std::construct_at(static_cast<DynamicTSLStorage *>(dst));
        }

        void dynamic_list_storage_destroy(void *memory, const void *) noexcept
        {
            std::destroy_at(static_cast<DynamicTSLStorage *>(memory));
        }

        struct DynamicListPlanEntry
        {
            std::unique_ptr<MemoryUtils::StoragePlan> storage_plan{};
            const MemoryUtils::StoragePlan        *root_plan{nullptr};
        };

        [[nodiscard]] std::unordered_map<const TSValueTypeMetaData *, std::unique_ptr<DynamicListPlanEntry>> &
        dynamic_list_plan_entries() noexcept
        {
            static std::unordered_map<const TSValueTypeMetaData *, std::unique_ptr<DynamicListPlanEntry>> entries;
            return entries;
        }

        [[nodiscard]] std::recursive_mutex &dynamic_list_plan_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        [[nodiscard]] const DynamicTSLStorage &storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("dynamic TSL TSData requires live storage"); }
            return *MemoryUtils::cast<DynamicTSLStorage>(memory);
        }

        [[nodiscard]] DynamicTSLStorage &storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("dynamic TSL TSData requires live storage"); }
            return *MemoryUtils::cast<DynamicTSLStorage>(memory);
        }

        [[nodiscard]] const TSDataOps &child_ops(TSRoleTypeRef child)
        {
            return child.ops_ref();
        }

        struct DynamicTSLContext
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            FixedTSLDataLayout              list_layout{};
            IndexedTSDataOps                ops{};
            IndexedValueOps                 value_list_ops{};
            MapValueOps                     delta_map_ops{};
            SetValueOps                     delta_key_set_ops{};
            TSRoleTypeRef                 element_type{};
            TypeRole                        role{TypeRole::Invalid};
            bool                            embedded{false};
            ValueTypeRef element_value_binding{nullptr};
            ValueTypeRef element_delta_binding{nullptr};
            ValueTypeRef ordinal_key_binding{nullptr};
            ValueTypeRef delta_key_set_binding{nullptr};

            DynamicTSLContext(const TSValueTypeMetaData &schema_,
                              const MemoryUtils::StoragePlan &plan_,
                              TSRoleTypeRef element_type_,
                              TypeRole role_,
                              bool embedded_)
                : schema(&schema_), plan(&plan_), element_type(element_type_), role(role_), embedded(embedded_)
            {
                const auto &element_ops = child_ops(element_type);
                const auto *element_layout = element_ops.layout_impl(element_ops.context);
                if (element_layout == nullptr)
                {
                    throw std::logic_error("dynamic TSL element layout is not resolved");
                }

                element_value_binding = element_layout->value_binding;
                element_delta_binding = element_layout->delta_binding;
                if (element_value_binding == nullptr || element_delta_binding == nullptr)
                {
                    throw std::logic_error("dynamic TSL element value/delta bindings are not resolved");
                }

                list_layout.element_type   = element_type;
                list_layout.element_layout  = element_layout;
                list_layout.element_count   = 0;
                list_layout.value_offset    = 0;
                list_layout.tracking_offset = 0;

                configure_ts_ops();
                configure_value_ops();
            }

            void bind_surfaces()
            {
                const auto *value_schema = schema->value_schema;
                const auto *delta_schema = schema->delta_value_schema;
                if (value_schema == nullptr || delta_schema == nullptr)
                {
                    throw std::logic_error("dynamic TSL schemas are not populated");
                }
                if (value_schema->value_kind() != ValueTypeKind::List || delta_schema->value_kind() != ValueTypeKind::Map)
                {
                    throw std::logic_error("dynamic TSL requires List value and Map delta schemas");
                }

                list_layout.value_binding = intern_value_type(*value_schema, *plan, value_list_ops);

                ordinal_key_binding = ValuePlanFactory::instance().type_for(delta_schema->key_type);
                if (ordinal_key_binding == nullptr)
                {
                    throw std::logic_error("dynamic TSL ordinal key binding is not resolved");
                }
                const auto *key_set_schema = TypeRegistry::instance().set(delta_schema->key_type);
                delta_key_set_binding = intern_value_type(*key_set_schema, *plan, delta_key_set_ops);
                list_layout.delta_binding = intern_value_type(*delta_schema, *plan, delta_map_ops);
            }

            [[nodiscard]] static const detail::TSDataOwnershipOps &ownership_ops() noexcept
            {
                static const detail::TSDataOwnershipOps ops{
                    .child_count = &owned_child_count,
                    .child_at = &owned_child_at,
                };
                return ops;
            }

            [[nodiscard]] static std::size_t owned_child_count(const void *, const void *memory) noexcept
            {
                return memory != nullptr ? storage(memory).size() : 0;
            }

            [[nodiscard]] static detail::TSDataOwnedChild owned_child_at(const void *context,
                                                                         void *memory,
                                                                         std::size_t index) noexcept
            {
                if (context == nullptr || memory == nullptr) { return {}; }
                const auto *state = ctx(context);
                auto &store = storage(memory);
                if (index >= store.size()) { return {}; }
                return detail::TSDataOwnedChild{
                    .type = state->element_type,
                    .data = store.child_memory(index),
                    .parent_child_id = index,
                    .attach_parent = true,
                };
            }

          private:
            void configure_ts_ops()
            {
                ops = IndexedTSDataOps{};
                TSDataOps &base_ops = ops;
                base_ops = TSDataOps{
                    .context                   = this,
                    .kind                      = TSTypeKind::TSL,
                    .allows_mutation           = true,
                    .ownership_ops             = &ownership_ops(),
                    .layout_impl               = &dynamic_layout,
                    .tracking_impl             = &dynamic_tracking,
                    .mutable_tracking_impl     = &dynamic_mutable_tracking,
                    .has_current_value_impl    = &dynamic_has_current_value,
                    .all_valid_impl            = &dynamic_all_valid,
                    .value_memory_impl         = &dynamic_value_memory,
                    .mutable_value_memory_impl = &dynamic_mutable_value_memory,
                    .delta_memory_impl         = &dynamic_delta_memory,
                    .mutable_delta_memory_impl = &dynamic_mutable_delta_memory,
                    .record_child_modified_impl = &dynamic_record_child_modified,
                    .copy_value_from_impl      = &dynamic_copy_value_from,
                    .move_value_from_impl      = &dynamic_move_value_from,
                    .empty_delta_impl          = &ts_data_detail::empty_delta_tsl,
                    .capture_delta_impl        = &ts_data_detail::capture_delta_tsl,
                    .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_tsl,
                    .apply_delta_impl          = &ts_data_detail::apply_delta_tsl,
                    .indexed_child_count_impl  = &dynamic_indexed_size,
                    .indexed_child_binding_impl = &dynamic_indexed_element_binding,
                    .indexed_child_memory_impl = &dynamic_indexed_element_memory,
                    .mutable_indexed_child_memory_impl = &dynamic_mutable_indexed_element_memory,
                    .indexed_child_growth      = true,
                };
                ops.size_impl                   = &dynamic_indexed_size;
                ops.modified_index_count_impl   = &dynamic_modified_index_count;
                ops.modified_index_at_impl      = &dynamic_modified_index_at;
                ops.element_binding_impl        = &dynamic_indexed_element_binding;
                ops.element_memory_impl         = &dynamic_indexed_element_memory;
                ops.mutable_element_memory_impl = &dynamic_mutable_indexed_element_memory;
            }

            void configure_value_ops()
            {
                value_list_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &dynamic_value_hash, &dynamic_value_equals,
                     &dynamic_value_compare,
                     &dynamic_value_to_string},
                    &dynamic_value_size,
                    &dynamic_value_element_at,
                    &dynamic_value_element_binding,
                    &dynamic_value_make_range,
                    nullptr,
                };
                value_list_ops.owning_type_impl      = &canonical_value_binding;
                value_list_ops.copy_construct_view_impl = &dynamic_value_copy_construct_view;
                value_list_ops.copy_assign_view_impl    = &dynamic_value_copy_assign_view;

                delta_map_ops = MapValueOps{
                    {{ValueOpsKind::Map, this, false, &dynamic_delta_map_hash, &dynamic_delta_map_equals,
                      &dynamic_delta_map_compare,
                      &dynamic_delta_map_to_string},
                     &dynamic_delta_map_size,
                     &dynamic_delta_map_key_at_index,
                     &dynamic_delta_map_key_binding,
                     &dynamic_delta_map_make_keys_range,
                     nullptr},
                    &dynamic_delta_map_contains,
                    &dynamic_delta_map_value_at,
                    &dynamic_delta_map_value_at_index,
                    &dynamic_delta_map_value_binding,
                    &dynamic_delta_map_make_keys_range,
                    &dynamic_delta_map_make_values_range,
                    &dynamic_delta_map_make_kv_range,
                    &dynamic_delta_map_key_set,
                };
                delta_map_ops.owning_type_impl      = &canonical_value_binding;
                delta_map_ops.copy_construct_view_impl = &dynamic_delta_map_copy_construct_view;
                delta_map_ops.copy_assign_view_impl    = &dynamic_delta_map_copy_assign_view;

                delta_key_set_ops = SetValueOps{
                    {{ValueOpsKind::Set, this, false, &dynamic_delta_key_set_hash, &dynamic_delta_key_set_equals,
                      &dynamic_delta_key_set_compare, &dynamic_delta_key_set_to_string},
                     &dynamic_delta_map_size,
                     &dynamic_delta_map_key_at_index,
                     &dynamic_delta_map_key_binding,
                     &dynamic_delta_map_make_keys_range,
                     nullptr},
                    &dynamic_delta_key_set_contains,
                };
                delta_key_set_ops.owning_type_impl      = &canonical_value_binding;
                delta_key_set_ops.copy_construct_view_impl = &dynamic_delta_key_set_copy_construct_view;
                delta_key_set_ops.copy_assign_view_impl    = &dynamic_delta_key_set_copy_assign_view;
            }

            [[nodiscard]] static const DynamicTSLContext *ctx(const void *context) noexcept
            {
                return static_cast<const DynamicTSLContext *>(context);
            }

            [[nodiscard]] static const TSDataLayout *dynamic_layout(const void *context) noexcept
            {
                return &ctx(context)->list_layout;
            }

            [[nodiscard]] static const TSDataTracking *dynamic_tracking(const void *, const void *memory) noexcept
            {
                return &storage(memory).tracking();
            }

            [[nodiscard]] static TSDataTracking *dynamic_mutable_tracking(const void *, void *memory) noexcept
            {
                return &storage(memory).mutable_tracking();
            }

            [[nodiscard]] static bool dynamic_has_current_value(const void *, const void *memory) noexcept
            {
                return storage(memory).tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool dynamic_all_valid(const void *context, const void *memory)
            {
                if (!dynamic_has_current_value(context, memory)) { return false; }

                const auto *state = ctx(context);
                const auto &store = storage(memory);
                const auto &ops   = child_ops(state->element_type);
                for (std::size_t index = 0; index < store.size(); ++index)
                {
                    if (!ops.all_valid_impl(ops.context, store.child_memory(index))) { return false; }
                }
                return true;
            }

            [[nodiscard]] static const void *dynamic_value_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *dynamic_mutable_value_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static const void *dynamic_delta_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *dynamic_mutable_delta_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static ValueView child_value_view(const DynamicTSLContext *state,
                                                            const void *memory,
                                                            std::size_t index)
            {
                const auto &ops = child_ops(state->element_type);
                const auto *data = storage(memory).child_memory(index);
                return ValueView{state->element_value_binding, ops.value_memory_impl(ops.context, data)};
            }

            [[nodiscard]] static ValueView child_delta_view(const DynamicTSLContext *state,
                                                            const void *memory,
                                                            std::size_t index)
            {
                const auto &ops = child_ops(state->element_type);
                const auto *data = storage(memory).child_memory(index);
                const auto  parent_time = storage(memory).tracking().last_modified_time;
                const auto *child_tracking = ops.tracking_impl(ops.context, data);
                if (parent_time == MIN_DT || child_tracking == nullptr ||
                    child_tracking->last_modified_time != parent_time)
                {
                    return ValueView{state->element_delta_binding, nullptr};
                }
                return ValueView{state->element_delta_binding, ops.delta_memory_impl(ops.context, data)};
            }

            [[nodiscard]] static ValueTypeRef
            canonical_value_binding(const void *, ValueTypeRef view_binding)
            {
                const auto binding = ValuePlanFactory::instance().type_for(view_binding.schema());
                if (binding == nullptr)
                {
                    throw std::logic_error("dynamic TSL value surface has no canonical owning binding");
                }
                return binding;
            }

            [[nodiscard]] static std::size_t dynamic_indexed_size(const void *, const void *memory) noexcept
            {
                return storage(memory).size();
            }

            static void dynamic_record_child_modified(const void *, void *memory, std::size_t child_id,
                                                      DateTime modified_time)
            {
                storage(memory).record_child_modified(child_id, modified_time);
            }

            [[nodiscard]] static std::size_t dynamic_modified_index_count(const void *,
                                                                          const void *memory) noexcept
            {
                return storage(memory).modified_index_count();
            }

            [[nodiscard]] static std::size_t dynamic_modified_index_at(const void *, const void *memory,
                                                                       std::size_t ordinal)
            {
                return storage(memory).modified_index_at(ordinal);
            }

            [[nodiscard]] static TSRoleTypeRef dynamic_indexed_element_binding(const void *context,
                                                                                   const void *,
                                                                                   std::size_t) noexcept
            {
                return ctx(context)->element_type;
            }

            [[nodiscard]] static const void *dynamic_indexed_element_memory(const void *,
                                                                            const void *memory,
                                                                            std::size_t index)
            {
                const auto &store = storage(memory);
                if (index >= store.size()) { return nullptr; }
                return store.child_memory(index);
            }

            [[nodiscard]] static void *dynamic_mutable_indexed_element_memory(const void *context,
                                                                              void *memory,
                                                                              std::size_t index)
            {
                auto &store = storage(memory);
                store.ensure_size(index + 1, ctx(context)->element_type);
                return store.child_memory(index);
            }

            [[nodiscard]] static std::size_t dynamic_value_size(const void *context, const void *memory) noexcept
            {
                return dynamic_indexed_size(context, memory);
            }

            [[nodiscard]] static const void *dynamic_value_element_at(const void *context,
                                                                      const void *memory,
                                                                      std::size_t index)
            {
                return child_value_view(ctx(context), memory, index).data();
            }

            [[nodiscard]] static ValueTypeRef dynamic_value_element_binding(const void *context,
                                                                                       const void *,
                                                                                       std::size_t) noexcept
            {
                return ctx(context)->element_value_binding;
            }

            [[nodiscard]] static ValueView dynamic_value_projector(const void *context,
                                                                   const void *memory,
                                                                   std::size_t index)
            {
                return child_value_view(ctx(context), memory, index);
            }

            [[nodiscard]] static Range<ValueView> dynamic_value_make_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = dynamic_value_size(context, memory),
                    .predicate = nullptr,
                    .projector = &dynamic_value_projector,
                };
            }

            [[nodiscard]] static std::size_t view_hash(ValueView view)
            {
                if (!view.has_value())
                {
                    return std::hash<ValueTypeRef>{}(view.binding());
                }
                return view.hash();
            }

            [[nodiscard]] static std::size_t dynamic_value_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                std::size_t seed = 0;
                for (std::size_t index = 0; index < store.size(); ++index)
                {
                    seed = dynamic_combine_hash(seed, child_value_view(state, memory, index).hash());
                }
                return seed;
            }

            [[nodiscard]] static bool dynamic_value_equals(const void *context,
                                                           const void *lhs,
                                                           const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctx(context);
                    const auto lhs_size = storage(lhs).size();
                    if (lhs_size != storage(rhs).size()) { return false; }
                    for (std::size_t index = 0; index < lhs_size; ++index)
                    {
                        if (!child_value_view(state, lhs, index).equals(child_value_view(state, rhs, index)))
                        {
                            return false;
                        }
                    }
                    return true;
                });
            }

            [[nodiscard]] static std::partial_ordering dynamic_value_compare(const void *context,
                                                                             const void *lhs,
                                                                             const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
                return fallback_on_exception(std::partial_ordering::unordered, [&] {
                    const auto *state = ctx(context);
                    const auto lhs_size = storage(lhs).size();
                    const auto rhs_size = storage(rhs).size();
                    const auto n = std::min(lhs_size, rhs_size);
                    for (std::size_t index = 0; index < n; ++index)
                    {
                        const auto order =
                            child_value_view(state, lhs, index).compare(child_value_view(state, rhs, index));
                        if (order != 0) { return order; }
                    }
                    if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                    if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                    return std::partial_ordering::equivalent;
                });
            }

            [[nodiscard]] static std::string dynamic_value_to_string(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "[");
                for (std::size_t index = 0; index < store.size(); ++index)
                {
                    if (index > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                    fmt::format_to(std::back_inserter(out), "{}",
                                   child_value_view(state, memory, index).to_string());
                }
                fmt::format_to(std::back_inserter(out), "]");
                return fmt::to_string(out);
            }

            static void dynamic_value_copy_construct_view(const void *context,
                                                          const ValueTypeRef &binding,
                                                          void *dst,
                                                          const void *memory)
            {
                auto storage = build_dynamic_value_list_storage(context, binding, memory);
                std::construct_at(static_cast<ListStorage *>(dst), std::move(storage));
            }

            static void dynamic_value_copy_assign_view(const void *context,
                                                       const ValueTypeRef &binding,
                                                       void *dst,
                                                       const void *memory)
            {
                *static_cast<ListStorage *>(dst) = build_dynamic_value_list_storage(context, binding, memory);
            }

            [[nodiscard]] static ListStorage build_dynamic_value_list_storage(const void *context,
                                                                              const ValueTypeRef &binding,
                                                                              const void *memory)
            {
                const auto *state = ctx(context);
                if (binding.schema() != state->schema->value_schema ||
                    binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::List)
                {
                    throw std::logic_error("dynamic TSL value copy requires the canonical parent list schema");
                }
                const auto element_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (element_binding == nullptr)
                {
                    throw std::logic_error("dynamic TSL value copy element binding is not resolved");
                }

                ListBuilder builder{element_binding};
                const auto &store = storage(memory);
                for (std::size_t index = 0; index < store.size(); ++index)
                {
                    Value child{child_value_view(state, memory, index)};
                    if (child.binding() != element_binding)
                    {
                        throw std::logic_error("dynamic TSL value copy materialized the wrong element binding");
                    }
                    builder.push_back_copy(child.view().data());
                }
                return builder.build_storage();
            }

            [[nodiscard]] static bool child_modified_for_parent_time(const DynamicTSLContext *state,
                                                                     const void *memory,
                                                                     std::size_t index) noexcept
            {
                const auto &ops = child_ops(state->element_type);
                const auto *data = storage(memory).child_memory(index);
                const auto  time = storage(memory).tracking().last_modified_time;
                const auto *child_tracking = ops.tracking_impl(ops.context, data);
                return time != MIN_DT && child_tracking != nullptr && child_tracking->last_modified_time == time;
            }

            [[nodiscard]] static std::size_t dynamic_delta_map_size(const void *context,
                                                                    const void *memory) noexcept
            {
                static_cast<void>(context);
                return storage(memory).modified_index_count();
            }

            [[nodiscard]] static std::size_t nth_modified_child(const DynamicTSLContext *state,
                                                                const void *memory,
                                                                std::size_t ordinal)
            {
                static_cast<void>(state);
                return storage(memory).modified_index_at(ordinal);
            }

            [[nodiscard]] static const void *dynamic_delta_map_key_at_index(const void *context,
                                                                            const void *memory,
                                                                            std::size_t index)
            {
                const auto *state = ctx(context);
                const auto slot = nth_modified_child(state, memory, index);
                return &storage(memory).ordinal_key(slot);
            }

            [[nodiscard]] static ValueTypeRef dynamic_delta_map_key_binding(const void *context,
                                                                                       const void *,
                                                                                       std::size_t) noexcept
            {
                return ctx(context)->ordinal_key_binding;
            }

            [[nodiscard]] static const void *dynamic_delta_map_value_at_index(const void *context,
                                                                              const void *memory,
                                                                              std::size_t index)
            {
                const auto *state = ctx(context);
                return child_delta_view(state, memory, nth_modified_child(state, memory, index)).data();
            }

            [[nodiscard]] static ValueTypeRef dynamic_delta_map_value_binding(const void *context,
                                                                                         const void *) noexcept
            {
                return ctx(context)->element_delta_binding;
            }

            [[nodiscard]] static bool dynamic_delta_map_contains(const void *context,
                                                                 const void *memory,
                                                                 const void *key)
            {
                const auto *state = ctx(context);
                const auto index = *MemoryUtils::cast<std::int64_t>(key);
                return index >= 0 && static_cast<std::size_t>(index) < storage(memory).size() &&
                       child_modified_for_parent_time(state, memory, static_cast<std::size_t>(index));
            }

            [[nodiscard]] static const void *dynamic_delta_map_value_at(const void *context,
                                                                        const void *memory,
                                                                        const void *key)
            {
                const auto *state = ctx(context);
                const auto index = *MemoryUtils::cast<std::int64_t>(key);
                if (index < 0) { return nullptr; }
                const auto slot = static_cast<std::size_t>(index);
                if (slot >= storage(memory).size() || !child_modified_for_parent_time(state, memory, slot))
                {
                    return nullptr;
                }
                return child_delta_view(state, memory, slot).data();
            }

            [[nodiscard]] static ValueView dynamic_delta_map_key_projector(const void *context,
                                                                           const void *memory,
                                                                           std::size_t ordinal)
            {
                const auto index = nth_modified_child(ctx(context), memory, ordinal);
                return ValueView{ctx(context)->ordinal_key_binding, &storage(memory).ordinal_key(index)};
            }

            [[nodiscard]] static ValueView dynamic_delta_map_value_projector(const void *context,
                                                                             const void *memory,
                                                                             std::size_t ordinal)
            {
                const auto index = nth_modified_child(ctx(context), memory, ordinal);
                return child_delta_view(ctx(context), memory, index);
            }

            [[nodiscard]] static std::pair<ValueView, ValueView>
            dynamic_delta_map_kv_projector(const void *context, const void *memory, std::size_t index)
            {
                return {dynamic_delta_map_key_projector(context, memory, index),
                        dynamic_delta_map_value_projector(context, memory, index)};
            }

            [[nodiscard]] static Range<ValueView> dynamic_delta_map_make_keys_range(const void *context,
                                                                                   const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = dynamic_delta_map_size(context, memory),
                    .predicate = nullptr,
                    .projector = &dynamic_delta_map_key_projector,
                };
            }

            [[nodiscard]] static Range<ValueView> dynamic_delta_map_make_values_range(const void *context,
                                                                                     const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = dynamic_delta_map_size(context, memory),
                    .predicate = nullptr,
                    .projector = &dynamic_delta_map_value_projector,
                };
            }

            [[nodiscard]] static KeyValueRange<ValueView, ValueView> dynamic_delta_map_make_kv_range(
                const void *context,
                const void *memory)
            {
                return KeyValueRange<ValueView, ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = dynamic_delta_map_size(context, memory),
                    .predicate = nullptr,
                    .projector = &dynamic_delta_map_kv_projector,
                };
            }

            [[nodiscard]] static SetView dynamic_delta_map_key_set(const void *context,
                                                                   ValueTypeRef,
                                                                   const void *memory)
            {
                return ValueView{ctx(context)->delta_key_set_binding, memory}.as_set();
            }

            [[nodiscard]] static std::size_t dynamic_delta_map_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                std::size_t result = 0;
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    const auto key_hash = state->ordinal_key_binding.ops_ref().hash(&store.ordinal_key(index));
                    const auto value_hash = view_hash(child_delta_view(state, memory, index));
                    result ^= dynamic_combine_hash(key_hash, value_hash);
                }
                return result;
            }

            [[nodiscard]] static bool dynamic_delta_map_equals(const void *context,
                                                               const void *lhs,
                                                               const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctx(context);
                    const auto lhs_size = storage(lhs).size();
                    const auto rhs_size = storage(rhs).size();
                    const auto max_size = std::max(lhs_size, rhs_size);
                    if (dynamic_delta_map_size(context, lhs) != dynamic_delta_map_size(context, rhs))
                    {
                        return false;
                    }
                    for (std::size_t index = 0; index < max_size; ++index)
                    {
                        const bool lhs_modified = index < lhs_size && child_modified_for_parent_time(state, lhs, index);
                        const bool rhs_modified = index < rhs_size && child_modified_for_parent_time(state, rhs, index);
                        if (lhs_modified != rhs_modified) { return false; }
                        if (lhs_modified &&
                            !child_delta_view(state, lhs, index).equals(child_delta_view(state, rhs, index)))
                        {
                            return false;
                        }
                    }
                    return true;
                });
            }

            [[nodiscard]] static std::partial_ordering dynamic_delta_map_compare(const void *context,
                                                                                const void *lhs,
                                                                                const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
                return dynamic_delta_map_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                   : std::partial_ordering::unordered;
            }

            [[nodiscard]] static std::string dynamic_delta_map_to_string(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                bool first = true;
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                    first = false;
                    fmt::format_to(std::back_inserter(out), "{}: {}",
                                   store.ordinal_key(index),
                                   child_delta_view(state, memory, index).to_string());
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

            static void dynamic_delta_map_copy_construct_view(const void *context,
                                                              const ValueTypeRef &binding,
                                                              void *dst,
                                                              const void *memory)
            {
                auto storage = build_dynamic_delta_map_storage(context, binding, memory);
                std::construct_at(static_cast<MapStorage *>(dst), std::move(storage));
            }

            static void dynamic_delta_map_copy_assign_view(const void *context,
                                                           const ValueTypeRef &binding,
                                                           void *dst,
                                                           const void *memory)
            {
                *static_cast<MapStorage *>(dst) = build_dynamic_delta_map_storage(context, binding, memory);
            }

            [[nodiscard]] static MapStorage build_dynamic_delta_map_storage(const void *context,
                                                                            const ValueTypeRef &binding,
                                                                            const void *memory)
            {
                const auto *state = ctx(context);
                if (binding.schema() != state->schema->delta_value_schema ||
                    binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Map)
                {
                    throw std::logic_error("dynamic TSL delta copy requires the canonical parent delta map schema");
                }
                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->key_type);
                const auto value_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (key_binding == nullptr || key_binding != state->ordinal_key_binding)
                {
                    throw std::logic_error("dynamic TSL delta copy key binding is not resolved");
                }
                if (value_binding == nullptr)
                {
                    throw std::logic_error("dynamic TSL delta copy value binding is not resolved");
                }

                MapBuilder builder{key_binding, value_binding};
                const auto &store = storage(memory);
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    Value child_delta{child_delta_view(state, memory, index)};
                    if (child_delta.binding() != value_binding)
                    {
                        throw std::logic_error("dynamic TSL delta copy materialized the wrong value binding");
                    }
                    builder.set_item_copy(&store.ordinal_key(index), child_delta.view().data());
                }
                return builder.build_storage();
            }

            [[nodiscard]] static bool dynamic_delta_key_set_contains(const void *context,
                                                                     const void *memory,
                                                                     const void *key)
            {
                return dynamic_delta_map_contains(context, memory, key);
            }

            [[nodiscard]] static std::size_t dynamic_delta_key_set_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                std::size_t result = 0;
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    result ^= state->ordinal_key_binding.ops_ref().hash(&store.ordinal_key(index));
                }
                return result;
            }

            [[nodiscard]] static bool dynamic_delta_key_set_equals(const void *context,
                                                                   const void *lhs,
                                                                   const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctx(context);
                    const auto lhs_size = storage(lhs).size();
                    const auto rhs_size = storage(rhs).size();
                    const auto max_size = std::max(lhs_size, rhs_size);
                    for (std::size_t index = 0; index < max_size; ++index)
                    {
                        const bool lhs_modified = index < lhs_size && child_modified_for_parent_time(state, lhs, index);
                        const bool rhs_modified = index < rhs_size && child_modified_for_parent_time(state, rhs, index);
                        if (lhs_modified != rhs_modified) { return false; }
                    }
                    return true;
                });
            }

            [[nodiscard]] static std::partial_ordering dynamic_delta_key_set_compare(const void *context,
                                                                                    const void *lhs,
                                                                                    const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
                const auto lhs_size = dynamic_delta_map_size(context, lhs);
                const auto rhs_size = dynamic_delta_map_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return dynamic_delta_key_set_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                       : std::partial_ordering::unordered;
            }

            [[nodiscard]] static std::string dynamic_delta_key_set_to_string(const void *, const void *memory)
            {
                const auto &store = storage(memory);
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                bool first = true;
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                    first = false;
                    fmt::format_to(std::back_inserter(out), "{}", store.ordinal_key(index));
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

            static void dynamic_delta_key_set_copy_construct_view(const void *context,
                                                                  const ValueTypeRef &binding,
                                                                  void *dst,
                                                                  const void *memory)
            {
                auto storage = build_dynamic_delta_key_set_storage(context, binding, memory);
                std::construct_at(static_cast<SetStorage *>(dst), std::move(storage));
            }

            static void dynamic_delta_key_set_copy_assign_view(const void *context,
                                                               const ValueTypeRef &binding,
                                                               void *dst,
                                                               const void *memory)
            {
                *static_cast<SetStorage *>(dst) = build_dynamic_delta_key_set_storage(context, binding, memory);
            }

            [[nodiscard]] static SetStorage build_dynamic_delta_key_set_storage(const void *context,
                                                                                const ValueTypeRef &binding,
                                                                                const void *memory)
            {
                const auto *state = ctx(context);
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Set)
                {
                    throw std::logic_error("dynamic TSL delta key-set copy requires a canonical set schema");
                }
                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (key_binding == nullptr || key_binding != state->ordinal_key_binding)
                {
                    throw std::logic_error("dynamic TSL delta key-set copy key binding is not resolved");
                }

                SetBuilder builder{key_binding};
                const auto &store = storage(memory);
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    builder.insert_copy(&store.ordinal_key(index));
                }
                return builder.build_storage();
            }

            [[nodiscard]] static bool dynamic_copy_value_from(const void *context,
                                                              void *memory,
                                                              const ValueView &source,
                                                              DateTime modified_time)
            {
                if (memory == nullptr)
                {
                    throw std::logic_error("dynamic TSL copy requires live memory");
                }
                if (!source.has_value())
                {
                    throw std::invalid_argument("dynamic TSL copy requires a live source value");
                }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("dynamic TSL copy requires a concrete evaluation time");
                }

                const auto *state = ctx(context);
                if (source.schema() != state->schema->value_schema)
                {
                    throw std::invalid_argument("dynamic TSL copy requires the parent value schema");
                }
                const auto source_values = source.as_list();
                auto      &target = storage(memory);
                if (source_values.size() < target.size())
                {
                    throw std::invalid_argument(
                        "dynamic TSL copy cannot shrink because TSL delta has no removal surface");
                }

                const bool first_for_parent = target.tracking().last_modified_time != modified_time;
                target.ensure_size(source_values.size(), state->element_type);

                const auto &ops = child_ops(state->element_type);
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    void *data = target.child_memory(index);
                    if (!ops.copy_value_from_impl(ops.context, data, source_values.at(index), modified_time))
                    {
                        continue;
                    }
                    auto *tracking = ops.mutable_tracking_impl(ops.context, data);
                    if (tracking == nullptr) { throw std::logic_error("dynamic TSL child has no tracking record"); }
                    if (!tracking->record_modified(modified_time))
                    {
                        throw std::logic_error("dynamic TSL child reported a duplicate modification");
                    }
                    target.record_child_modified(index, modified_time);
                }

                return first_for_parent;
            }

            [[nodiscard]] static bool dynamic_move_value_from(const void *context,
                                                              void *memory,
                                                              ValueView source,
                                                              DateTime modified_time)
            {
                if (memory == nullptr)
                {
                    throw std::logic_error("dynamic TSL move requires live memory");
                }
                if (!source.has_value())
                {
                    throw std::invalid_argument("dynamic TSL move requires a live source value");
                }
                if (!source.writable_payload())
                {
                    throw std::invalid_argument("dynamic TSL move requires writable source storage");
                }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument("dynamic TSL move requires a concrete evaluation time");
                }

                const auto *state = ctx(context);
                if (source.schema() != state->schema->value_schema)
                {
                    throw std::invalid_argument("dynamic TSL move requires the parent value schema");
                }
                const auto source_values = source.as_list();
                auto      &target = storage(memory);
                if (source_values.size() < target.size())
                {
                    throw std::invalid_argument(
                        "dynamic TSL move cannot shrink because TSL delta has no removal surface");
                }

                const auto &ops = child_ops(state->element_type);
                if (ops.move_value_from_impl == &ts_data_detail::missing_move_value_from)
                {
                    throw std::logic_error("dynamic TSL move requires the child to support move_value_from");
                }
                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    auto source_value = source_values.at(index);
                    if (!source_value.valid())
                    {
                        throw std::invalid_argument("dynamic TSL move requires live child source values");
                    }
                }

                const bool first_for_parent = target.tracking().last_modified_time != modified_time;
                target.ensure_size(source_values.size(), state->element_type);

                for (std::size_t index = 0; index < source_values.size(); ++index)
                {
                    void *data = target.child_memory(index);
                    auto  source_value = source_values.at(index);
                    ValueView source_child{source_value.binding(), const_cast<void *>(source_value.data())};
                    if (!ops.move_value_from_impl(ops.context, data, std::move(source_child), modified_time))
                    {
                        continue;
                    }
                    auto *tracking = ops.mutable_tracking_impl(ops.context, data);
                    if (tracking == nullptr) { throw std::logic_error("dynamic TSL child has no tracking record"); }
                    if (!tracking->record_modified(modified_time))
                    {
                        throw std::logic_error("dynamic TSL child reported a duplicate modification");
                    }
                    target.record_child_modified(index, modified_time);
                }

                return first_for_parent;
            }
        };

        struct DynamicListContextKey
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            std::size_t                     storage_offset{0};
            const TypeRecord               *element_type{nullptr};
            TypeRole                        role{TypeRole::Invalid};
            bool                            embedded{false};

            [[nodiscard]] bool operator==(const DynamicListContextKey &) const noexcept = default;
        };

        struct DynamicListContextKeyHash
        {
            [[nodiscard]] std::size_t operator()(const DynamicListContextKey &key) const noexcept
            {
                auto seed = dynamic_combine_hash(std::hash<const TSValueTypeMetaData *>{}(key.schema),
                                                 std::hash<const MemoryUtils::StoragePlan *>{}(key.plan));
                seed = dynamic_combine_hash(seed, key.storage_offset);
                seed = dynamic_combine_hash(seed, std::hash<const TypeRecord *>{}(key.element_type));
                seed = dynamic_combine_hash(seed, static_cast<std::size_t>(key.role));
                seed = dynamic_combine_hash(seed, key.embedded);
                return seed;
            }
        };

        using DynamicListContextMap =
            std::unordered_map<DynamicListContextKey,
                               std::unique_ptr<DynamicTSLContext>,
                               DynamicListContextKeyHash>;

        [[nodiscard]] DynamicListContextMap &dynamic_list_contexts() noexcept
        {
            static DynamicListContextMap contexts;
            return contexts;
        }

        [[nodiscard]] std::recursive_mutex &dynamic_list_context_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }
    }  // namespace

    [[nodiscard]] bool is_dynamic_list_ts_data(const TSValueTypeMetaData &schema) noexcept
    {
        return schema.kind == TSTypeKind::TSL && schema.fixed_size() == 0 && schema.element_ts() != nullptr &&
               schema.value_schema != nullptr && schema.delta_value_schema != nullptr;
    }

    [[nodiscard]] const MemoryUtils::StoragePlan *synthesise_dynamic_list_plan(const TSValueTypeMetaData &schema)
    {
        if (!is_dynamic_list_ts_data(schema))
        {
            throw std::logic_error("TSDataPlanFactory: dynamic list storage requires dynamic TSL schema");
        }

        std::lock_guard<std::recursive_mutex> lock(dynamic_list_plan_mutex());
        auto                                 &entries = dynamic_list_plan_entries();
        if (const auto it = entries.find(&schema); it != entries.end()) { return it->second->root_plan; }

        auto entry = std::make_unique<DynamicListPlanEntry>();
        entry->storage_plan = std::make_unique<MemoryUtils::StoragePlan>(MemoryUtils::StoragePlan{
            .layout                       = MemoryUtils::layout_for<DynamicTSLStorage>(),
            .lifecycle                    = {.construct      = &dynamic_list_storage_construct,
                                             .destroy        = &dynamic_list_storage_destroy,
                                             .copy_construct = nullptr,
                                             .move_construct = nullptr,
                                             .copy_assign    = nullptr,
                                             .move_assign    = nullptr},
            .lifecycle_context            = nullptr,
            .composite_kind_tag           = MemoryUtils::CompositeKind::None,
            .trivially_destructible       = false,
            .trivially_copyable           = false,
            .trivially_move_constructible = false,
        });
        entry->root_plan = entry->storage_plan.get();

        const auto *result = entry->root_plan;
        entries.emplace(&schema, std::move(entry));
        return result;
    }

    [[nodiscard]] const TSDataOps &dynamic_list_ts_data_ops(const TSValueTypeMetaData      &schema,
                                                            const MemoryUtils::StoragePlan &plan,
                                                            std::size_t storage_offset,
                                                            TSRoleTypeRef element_type,
                                                            TypeRole role,
                                                            bool embedded)
    {
        if (storage_offset != 0)
        {
            throw std::logic_error("dynamic TSL currently expects the storage object at the root");
        }
        if (element_type.record() == nullptr || element_type.schema() != schema.element_ts())
            throw std::invalid_argument("dynamic TSL ops require the canonical element TypeRecord");
        if (element_type.role() != role)
            throw std::invalid_argument("dynamic TSL element role must match the parent role");

        std::lock_guard<std::recursive_mutex> lock(dynamic_list_context_mutex());
        auto                                 &contexts = dynamic_list_contexts();
        const DynamicListContextKey key{&schema, &plan, storage_offset, element_type.record(), role, embedded};
        if (const auto it = contexts.find(key); it != contexts.end()) { return it->second->ops; }

        auto context = std::make_unique<DynamicTSLContext>(schema, plan, element_type, role, embedded);
        auto *result = context.get();
        contexts.emplace(key, std::move(context));
        result->bind_surfaces();
        return result->ops;
    }

    void clear_dynamic_list_ts_data_contexts() noexcept
    {
        {
            std::lock_guard<std::recursive_mutex> lock(dynamic_list_context_mutex());
            dynamic_list_contexts().clear();
        }
        {
            std::lock_guard<std::recursive_mutex> lock(dynamic_list_plan_mutex());
            dynamic_list_plan_entries().clear();
        }
    }

}  // namespace hgraph::ts_data_plan_factory_detail
