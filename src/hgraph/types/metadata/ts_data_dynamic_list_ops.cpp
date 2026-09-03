#include <hgraph/util/scope.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/time_series/ts_data/impl/current_state_ops.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>
#include <hgraph/python/ts_data_conversion.h>
#endif

#include "../time_series/ts_data/ownership.h"

#include <fmt/format.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>

#include <hgraph/types/utils/counted_mutex.h>
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

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        /** Either REMOVE sentinel. Truncation is total, so hgraph's strict /
            lenient distinction has nothing to express for a list. */
        [[nodiscard]] bool is_removal_sentinel(nb::handle item) noexcept
        {
            const auto &strict = python_bridge::removed_sentinel_slot();
            if (strict.is_valid() && item.is(strict)) { return true; }
            const auto &lenient = python_bridge::remove_if_exists_sentinel_slot();
            return lenient.is_valid() && item.is(lenient);
        }
#endif

        using TSDataErasedOwner =
            MemoryUtils::ErasedOwner<MemoryUtils::HeapOnlyStoragePolicy, TypeRecord>;

        struct DynamicTSLIndexEntry
        {
            std::int64_t ordinal_key{0};
            std::size_t next_modified{0};
        };

        /**
         * Grow/shrink indexed child storage for a dynamic ``TSL`` (RFC 0031).
         *
         * Removal is TAIL TRUNCATION: ``live_size_`` is the list length and
         * ``elements_`` additionally retains the truncated tail for the rest
         * of the delta window so ``removed_values()`` can read it. The window
         * is opened lazily by the first mutation at a newer time, using the
         * same monotonic rule as the TSD slot store, and rolling it is what
         * invalidates and destroys the retained tail. Within one window a
         * re-grow RESURRECTS the retained child with its payload intact,
         * exactly as TSD reinsertion does.
         *
         * The physical extent is therefore ``max(live_size_, previous_size_)``
         * and the storage owns every element in it.
         */
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

            /** Live list length. */
            [[nodiscard]] std::size_t size() const noexcept { return live_size_; }
            /** Live plus retained (removed-this-window) extent. */
            [[nodiscard]] std::size_t retained_size() const noexcept { return elements_.size(); }
            /** Live length when the current delta window opened. */
            [[nodiscard]] std::size_t previous_size() const noexcept { return previous_size_; }
            /** Evaluation time the current delta window describes. The ring
                header doubles as its storage, so the modified ring and the
                structural window can never disagree. */
            [[nodiscard]] DateTime delta_time() const noexcept
            {
                return DateTime{std::chrono::microseconds{ordinal_keys_.front().ordinal_key}};
            }

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

            /**
             * Roll the delta window forward when ``modified_time`` is newer.
             *
             * Rolling flushes the retained tail: observers are invalidated
             * before the child storage is destroyed, matching the TSD
             * remove-then-erase ordering.
             */
            void prepare_delta(DateTime modified_time)
            {
                // Monotonic delta window: a record carrying an older time (a
                // freshly bound link replaying source history) joins the
                // current window rather than rebasing it and erasing sibling
                // marks recorded this cycle.
                if (modified_time <= delta_time()) { return; }
                // Clear the ring BEFORE flushing: invalidating a retained
                // child notifies its observers, and the ring positions it is
                // about to pop must not be reachable while that runs.
                auto &header = ordinal_keys_.front();
                header.next_modified = 0;
                flush_retained();
                header.ordinal_key = modified_time.time_since_epoch().count();
                previous_size_     = live_size_;
            }

            void record_child_modified(std::size_t index, DateTime modified_time)
            {
                // A stale record cannot join the ring either (re-appending an
                // already-linked entry would corrupt it), so it is dropped.
                if (modified_time < delta_time()) { return; }
                prepare_delta(modified_time);
                if (index >= live_size_) { return; }

                // Child tracking notifies its parent only on the first mutation
                // at a given evaluation time. Keep a circular list whose header
                // points at the tail, preserving notification order in the
                // existing ordinal-key allocation.
                auto &header = ordinal_keys_.front();
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
                std::size_t count = 0;
                for_each_modified_index([&](std::size_t) {
                    ++count;
                    return true;
                });
                return count;
            }

            [[nodiscard]] std::size_t modified_index_at(std::size_t ordinal) const
            {
                std::size_t result = TS_DATA_NO_CHILD_ID;
                std::size_t seen   = 0;
                for_each_modified_index([&](std::size_t index) {
                    if (seen++ != ordinal) { return true; }
                    result = index;
                    return false;
                });
                if (result == TS_DATA_NO_CHILD_ID)
                {
                    throw std::out_of_range("dynamic TSL modified index ordinal is out of range");
                }
                return result;
            }

            /**
             * Whether the open window describes the parent's current tick.
             *
             * The structural delta is only meaningful while the storage's
             * window and the parent's last modification agree, exactly as the
             * modified ring is.
             */
            [[nodiscard]] bool structural_delta_current() const noexcept
            {
                const auto window = delta_time();
                return window != MIN_DT && window == tracking_.last_modified_time;
            }

            [[nodiscard]] std::size_t added_index_count() const noexcept
            {
                if (!structural_delta_current() || live_size_ <= previous_size_) { return 0; }
                return live_size_ - previous_size_;
            }

            [[nodiscard]] std::size_t added_index_at(std::size_t ordinal) const
            {
                if (ordinal >= added_index_count())
                {
                    throw std::out_of_range("dynamic TSL added index ordinal is out of range");
                }
                return previous_size_ + ordinal;
            }

            [[nodiscard]] std::size_t removed_index_count() const noexcept
            {
                if (!structural_delta_current() || previous_size_ <= live_size_) { return 0; }
                return previous_size_ - live_size_;
            }

            [[nodiscard]] std::size_t removed_index_at(std::size_t ordinal) const
            {
                if (ordinal >= removed_index_count())
                {
                    throw std::out_of_range("dynamic TSL removed index ordinal is out of range");
                }
                return live_size_ + ordinal;
            }

            /** Grow the live list, constructing or resurrecting elements. */
            void ensure_size(std::size_t size, TSRoleTypeRef element_type, DateTime modified_time)
            {
                if (size <= live_size_) { return; }
                resize(size, element_type, modified_time);
            }

            /** Set the live list length, growing or truncating. */
            void resize(std::size_t size, TSRoleTypeRef element_type, DateTime modified_time)
            {
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument(
                        "dynamic TSL resize requires a concrete evaluation time");
                }
                prepare_delta(modified_time);
                if (size > live_size_) { grow_to(size, element_type); }
                else if (size < live_size_) { truncate_to(size); }
            }

            [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
            {
                DynamicStorageMetrics result{
                    .live_bytes = elements_.size() * sizeof(TSDataErasedOwner) +
                                  ordinal_keys_.size() * sizeof(DynamicTSLIndexEntry),
                    .reserved_bytes = elements_.capacity() * sizeof(TSDataErasedOwner) +
                                      ordinal_keys_.capacity() * sizeof(DynamicTSLIndexEntry),
                };
                const auto *ops = element_type_.ops();
                for (const auto &element : elements_)
                {
                    const auto bytes = element.plan()->layout.size;
                    result.live_bytes += bytes;
                    result.reserved_bytes += bytes;
                    if (ops != nullptr)
                    {
                        result += ops->dynamic_storage_metrics_impl(
                            ops->context, element.data());
                    }
                }
                return result;
            }

          private:
            /** Visit live ring members in notification order; ``fn`` returns
                false to stop (so ordinal lookup stays O(ordinal)). */
            template <typename Fn> void for_each_modified_index(Fn &&fn) const
            {
                const auto &header = ordinal_keys_.front();
                if (delta_time() != tracking_.last_modified_time || header.next_modified == 0) { return; }

                const std::size_t first = ordinal_keys_[header.next_modified].next_modified;
                std::size_t position = first;
                do
                {
                    // A child modified and then truncated in the same window
                    // is reported as REMOVED, not modified.
                    if (position - 1 < live_size_ && !fn(position - 1)) { return; }
                    position = ordinal_keys_[position].next_modified;
                } while (position != first);
            }

            void grow_to(std::size_t size, TSRoleTypeRef element_type)
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
                // Retained elements below `size` are RESURRECTED: the payload
                // and its ordinal key are already constructed.
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
                live_size_ = size;
            }

            void truncate_to(std::size_t size) noexcept
            {
                for (std::size_t index = size; index < live_size_; ++index)
                {
                    detail::stop_owned_ts_data_tree(TSDataView{element_type_, elements_[index].data()});
                }
                live_size_ = size;
            }

            /** Invalidate and destroy the tail retained by the closing window. */
            void flush_retained() noexcept
            {
                while (elements_.size() > live_size_)
                {
                    detail::invalidate_owned_ts_data_tree(
                        TSDataView{element_type_, elements_.back().data()});
                    elements_.pop_back();
                    ordinal_keys_.pop_back();
                }
            }

            TSRoleTypeRef                   element_type_{};
            TSDataTracking                    tracking_{};
            // Handles may move as the vector grows, but the child TSData bytes
            // must not. The heap-only policy above keeps published child
            // addresses stable while preserving vector locality for handles.
            std::vector<TSDataErasedOwner>  elements_{};
            std::vector<DynamicTSLIndexEntry> ordinal_keys_{};
            std::size_t                     live_size_{0};
            std::size_t                     previous_size_{0};
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

        [[nodiscard]] TypeSystemRecursiveMutex &dynamic_list_plan_mutex() noexcept
        {
            static TypeSystemRecursiveMutex mutex;
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
            SetValueOps                     removed_set_ops{};
            IndexedValueOps                 delta_bundle_ops{};
            TSRoleTypeRef                 element_type{};
            TypeRole                        role{TypeRole::Invalid};
            bool                            embedded{false};
            ValueTypeRef element_value_binding{nullptr};
            ValueTypeRef element_delta_binding{nullptr};
            ValueTypeRef ordinal_key_binding{nullptr};
            ValueTypeRef delta_key_set_binding{nullptr};
            ValueTypeRef removed_set_binding{nullptr};
            ValueTypeRef modified_map_binding{nullptr};

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
                if (value_schema->value_kind() != ValueTypeKind::List)
                {
                    throw std::logic_error("dynamic TSL requires a List value schema");
                }
                // RFC 0031: a dynamic TSL can shrink, so its delta carries the
                // same {removed, modified} shape a TSD does.
                if (delta_schema->value_kind() != ValueTypeKind::Bundle || delta_schema->field_count != 2)
                {
                    throw std::logic_error("dynamic TSL delta schema must be Bundle{removed, modified}");
                }
                const ValueTypeMetaData *removed_schema  = delta_schema->fields[0].type;
                const ValueTypeMetaData *modified_schema = delta_schema->fields[1].type;
                if (removed_schema == nullptr || modified_schema == nullptr ||
                    removed_schema->value_kind() != ValueTypeKind::Set ||
                    modified_schema->value_kind() != ValueTypeKind::Map)
                {
                    throw std::logic_error("dynamic TSL delta schema must be Bundle{Set<int>, Map<int, delta>}");
                }

                list_layout.value_binding = intern_value_type(*value_schema, *plan, value_list_ops);

                ordinal_key_binding = ValuePlanFactory::instance().type_for(modified_schema->key_type);
                if (ordinal_key_binding == nullptr)
                {
                    throw std::logic_error("dynamic TSL ordinal key binding is not resolved");
                }
                const auto *key_set_schema = TypeRegistry::instance().set(modified_schema->key_type);
                delta_key_set_binding = intern_value_type(*key_set_schema, *plan, delta_key_set_ops);
                removed_set_binding   = intern_value_type(*removed_schema, *plan, removed_set_ops);
                modified_map_binding  = intern_value_type(*modified_schema, *plan, delta_map_ops);
                list_layout.delta_binding = intern_value_type(*delta_schema, *plan, delta_bundle_ops);
            }

            [[nodiscard]] static const detail::TSDataOwnershipOps &ownership_ops() noexcept
            {
                static const detail::TSDataOwnershipOps ops{
                    .child_count = &owned_child_count,
                    .child_at = &owned_child_at,
                };
                return ops;
            }

            /** Ownership walks cover the RETAINED extent, not just the live
                list: the truncated tail is still owned until the delta window
                rolls, and destruction must reach it. */
            [[nodiscard]] static std::size_t owned_child_count(const void *, const void *memory) noexcept
            {
                return memory != nullptr ? storage(memory).retained_size() : 0;
            }

            [[nodiscard]] static detail::TSDataOwnedChild owned_child_at(const void *context,
                                                                         void *memory,
                                                                         std::size_t index) noexcept
            {
                if (context == nullptr || memory == nullptr) { return {}; }
                const auto *state = ctx(context);
                auto &store = storage(memory);
                if (index >= store.retained_size()) { return {}; }
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
                    .current_state_ops =
                        &ts_current_state_detail::current_state_ops_for(TSTypeKind::TSL),
                    .layout_impl               = &dynamic_layout,
                    .tracking_impl             = &dynamic_tracking,
                    .mutable_tracking_impl     = &dynamic_mutable_tracking,
                    .has_current_value_impl    = &dynamic_has_current_value,
                    .all_valid_impl            = &dynamic_all_valid,
                    .dynamic_storage_metrics_impl = &dynamic_storage_metrics,
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
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                    .python_ops               = &python_bridge::list_python_ts_data_ops(),
                    .from_python_impl          = &dynamic_from_python,
                    .to_python_impl            = &dynamic_to_python,
                    .delta_to_python_impl      = &dynamic_delta_to_python,
#endif
                };
                ops.size_impl                   = &dynamic_indexed_size;
                ops.modified_index_count_impl   = &dynamic_modified_index_count;
                ops.modified_index_at_impl      = &dynamic_modified_index_at;
                ops.element_binding_impl        = &dynamic_indexed_element_binding;
                ops.element_memory_impl         = &dynamic_indexed_element_memory;
                ops.mutable_element_memory_impl = &dynamic_mutable_indexed_element_memory;
                ops.structural_delta_impl       = &dynamic_structural_delta;
                ops.retained_element_memory_impl = &dynamic_retained_element_memory;
                ops.resize_impl                 = &dynamic_resize;
            }

            void configure_value_ops()
            {
                value_list_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &dynamic_value_hash, &dynamic_value_equals,
                     &dynamic_value_compare,
                     &dynamic_value_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &dynamic_value_projection_to_python
#endif
                    },
                    &dynamic_value_size,
                    &dynamic_value_element_at,
                    &dynamic_value_element_binding,
                    &dynamic_value_make_range,
                    nullptr,
                };
                value_list_ops.owning_type_impl      = &canonical_value_binding;
                value_list_ops.copy_construct_view_impl = &dynamic_value_copy_construct_view;
                value_list_ops.copy_assign_view_impl    = &dynamic_value_copy_assign_view;
                value_list_ops.dynamic_storage_metrics_impl = &dynamic_storage_metrics;

                delta_map_ops = MapValueOps{
                    {{ValueOpsKind::Map, this, false, &dynamic_delta_map_hash, &dynamic_delta_map_equals,
                      &dynamic_delta_map_compare,
                      &dynamic_delta_map_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &dynamic_delta_projection_to_python
#endif
                     },
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
                      &dynamic_delta_key_set_compare, &dynamic_delta_key_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &dynamic_delta_key_set_projection_to_python
#endif
                     },
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

                removed_set_ops = SetValueOps{
                    {{ValueOpsKind::Set, this, false, &dynamic_removed_set_hash, &dynamic_removed_set_equals,
                      &dynamic_removed_set_compare, &dynamic_removed_set_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &dynamic_removed_set_projection_to_python
#endif
                     },
                     &dynamic_removed_set_size,
                     &dynamic_removed_set_key_at_index,
                     &dynamic_delta_map_key_binding,
                     &dynamic_removed_set_make_range,
                     nullptr},
                    &dynamic_removed_set_contains,
                };
                removed_set_ops.owning_type_impl           = &canonical_value_binding;
                removed_set_ops.copy_construct_view_impl   = &dynamic_removed_set_copy_construct_view;
                removed_set_ops.copy_assign_view_impl      = &dynamic_removed_set_copy_assign_view;

                // The delta bundle is a two-field projection over the SAME
                // storage: field 0 is the removed index set, field 1 the
                // modified index map (mirrors the TSD dict delta bundle).
                delta_bundle_ops = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &dynamic_delta_bundle_hash,
                     &dynamic_delta_bundle_equals, &dynamic_delta_bundle_compare,
                     &dynamic_delta_bundle_to_string
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                     ,
                     &dynamic_delta_bundle_to_python
#endif
                    },
                    &dynamic_delta_bundle_size,
                    &dynamic_delta_bundle_element_at,
                    &dynamic_delta_bundle_element_binding,
                    &dynamic_delta_bundle_make_range,
                    nullptr,
                };
                delta_bundle_ops.owning_type_impl           = &canonical_value_binding;
                delta_bundle_ops.copy_construct_view_impl   = &dynamic_delta_bundle_copy_construct_view;
                delta_bundle_ops.copy_assign_view_impl      = &dynamic_delta_bundle_copy_assign_view;
            }

            [[nodiscard]] static const DynamicTSLContext *ctx(const void *context) noexcept
            {
                return static_cast<const DynamicTSLContext *>(context);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object dynamic_value_projection_to_python(
                const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return Value{ValueView{state->list_layout.value_binding, memory}}.to_python();
            }

            [[nodiscard]] static nb::object dynamic_delta_projection_to_python(
                const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return Value{ValueView{state->list_layout.delta_binding, memory}}.to_python();
            }

            [[nodiscard]] static nb::object dynamic_delta_key_set_projection_to_python(
                const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return Value{ValueView{state->delta_key_set_binding, memory}}.to_python();
            }

            /** Dynamic TSL Python export is a TSData strategy. It deliberately
                recurses through child TSDataOps; the public facade must never
                infer this representation from TSTypeKind. */
            [[nodiscard]] static nb::object dynamic_to_python(const void *context,
                                                               const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                const auto &ops = child_ops(state->element_type);
                nb::list result;
                for (std::size_t index = 0; index < store.size(); ++index)
                {
                    const auto *child = store.child_memory(index);
                    result.append(ops.has_current_value_impl(ops.context, child)
                                      ? ops.to_python_impl(ops.context, child)
                                      : nb::none());
                }
                return nb::tuple(result);
            }

            [[nodiscard]] static nb::object dynamic_delta_to_python(
                const void *context, const void *memory, DateTime evaluation_time)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                if (store.tracking().last_modified_time != evaluation_time)
                {
                    return nb::none();
                }
                const auto &ops = child_ops(state->element_type);
                nb::dict modified;
                for (std::size_t ordinal = 0; ordinal < store.modified_index_count(); ++ordinal)
                {
                    const auto index = store.modified_index_at(ordinal);
                    const auto *child = store.child_memory(index);
                    nb::object value = ops.delta_to_python_impl(
                        ops.context, child, evaluation_time);
                    if (!value.is_none()) { modified[nb::int_{index}] = std::move(value); }
                }
                // RFC 0031: the canonical {removed, modified} shape, which
                // hgraph's _simplify_delta rewrites into the friendly
                // {index: delta, removed_index: REMOVE} form.
                nb::dict result;
                result[nb::str{"removed"}] = state->removed_set_binding.ops_ref().to_python(memory);
                result[nb::str{"modified"}] = std::move(modified);
                return result;
            }

            /** Import current/replacement values through the child strategies.
                A sequence IS the list: it covers consecutive indices and
                resizes, so a shorter sequence truncates (RFC 0031). A mapping
                is a sparse replacement/update in which a ``REMOVE`` sentinel
                truncates to the lowest removed index. */
            [[nodiscard]] static bool dynamic_from_python(
                const void *context, void *memory, nb::handle source,
                DateTime modified_time)
            {
                if (memory == nullptr)
                {
                    throw std::logic_error(
                        "dynamic TSL from_python requires live storage");
                }
                if (source.is_none())
                {
                    throw std::invalid_argument(
                        "dynamic TSL from_python requires a non-None source");
                }
                if (modified_time == MIN_DT)
                {
                    throw std::invalid_argument(
                        "dynamic TSL from_python requires a concrete evaluation time");
                }

                const auto *state = ctx(context);
                auto &target = storage(memory);
                const auto &ops = child_ops(state->element_type);
                const bool first_for_parent =
                    target.tracking().last_modified_time != modified_time;
                bool touched = false;

                const auto update = [&](std::size_t index, nb::handle item) {
                    target.ensure_size(index + 1, state->element_type, modified_time);
                    void *child = target.child_memory(index);
                    if (!ops.from_python_impl(ops.context, child, item,
                                              modified_time))
                    {
                        return;
                    }
                    auto *tracking =
                        ops.mutable_tracking_impl(ops.context, child);
                    if (tracking == nullptr ||
                        !tracking->record_modified(modified_time))
                    {
                        throw std::logic_error(
                            "dynamic TSL child reported an invalid modification");
                    }
                    target.record_child_modified(index, modified_time);
                    touched = true;
                };

                if (nb::isinstance<nb::dict>(source))
                {
                    // Removal is resolved FIRST so a same-cycle re-grow through
                    // `modified` behaves exactly as apply_delta does.
                    auto truncate_to = static_cast<std::size_t>(-1);
                    for (auto [key, item] : nb::cast<nb::dict>(source))
                    {
                        if (item.is_none() || !is_removal_sentinel(item)) { continue; }
                        const auto index = nb::cast<std::int64_t>(key);
                        if (index < 0)
                        {
                            throw std::out_of_range(
                                "dynamic TSL from_python index must be non-negative");
                        }
                        truncate_to = std::min(truncate_to, static_cast<std::size_t>(index));
                    }
                    if (truncate_to < target.size())
                    {
                        target.resize(truncate_to, state->element_type, modified_time);
                        touched = true;
                    }
                    for (auto [key, item] : nb::cast<nb::dict>(source))
                    {
                        if (item.is_none() || is_removal_sentinel(item)) { continue; }
                        const auto index = nb::cast<std::int64_t>(key);
                        if (index < 0)
                        {
                            throw std::out_of_range(
                                "dynamic TSL from_python index must be non-negative");
                        }
                        update(static_cast<std::size_t>(index), item);
                    }
                    return first_for_parent && touched;
                }

                if (nb::isinstance<nb::str>(source) ||
                    !nb::isinstance<nb::sequence>(source))
                {
                    throw std::invalid_argument(
                        "dynamic TSL from_python expects a mapping or sequence");
                }
                const auto source_size = static_cast<std::size_t>(nb::len(source));
                if (source_size != target.size())
                {
                    target.resize(source_size, state->element_type, modified_time);
                    touched = true;
                }
                std::size_t index = 0;
                for (nb::handle item : source)
                {
                    if (!item.is_none()) { update(index, item); }
                    ++index;
                }
                return first_for_parent && touched;
            }
#endif

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

            // One-level check: ask each direct child for ``valid``, not for its
            // own ``all_valid``. See fixed_all_valid for the contract.
            [[nodiscard]] static bool dynamic_all_valid(const void *context, const void *memory)
            {
                if (!dynamic_has_current_value(context, memory)) { return false; }

                const auto *state = ctx(context);
                const auto &store = storage(memory);
                const auto &ops   = child_ops(state->element_type);
                for (std::size_t index = 0; index < store.size(); ++index)
                {
                    if (!ops.has_current_value_impl(ops.context, store.child_memory(index)))
                    {
                        return false;
                    }
                }
                return true;
            }

            [[nodiscard]] static DynamicStorageMetrics dynamic_storage_metrics(
                const void *, const void *memory) noexcept
            {
                return memory != nullptr ? storage(memory).dynamic_storage_metrics()
                                         : DynamicStorageMetrics{};
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

            /** Live-element access only. Growth needs an evaluation time so the
                structural delta can attribute the added indices, so it goes
                through ``dynamic_resize`` (RFC 0031). */
            [[nodiscard]] static void *dynamic_mutable_indexed_element_memory(const void *,
                                                                              void *memory,
                                                                              std::size_t index)
            {
                auto &store = storage(memory);
                if (index >= store.size())
                {
                    throw std::out_of_range(
                        "dynamic TSL growth requires an evaluation time; use a timed resize");
                }
                return store.child_memory(index);
            }

            static void dynamic_resize(const void *context, void *memory, std::size_t size,
                                       DateTime modified_time)
            {
                storage(memory).resize(size, ctx(context)->element_type, modified_time);
            }

            [[nodiscard]] static IndexedStructuralDelta dynamic_structural_delta(
                const void *, const void *memory) noexcept
            {
                if (memory == nullptr) { return {}; }
                const auto &store = storage(memory);
                if (!store.structural_delta_current())
                {
                    return IndexedStructuralDelta{
                        .time = MIN_DT, .previous_size = store.size(), .size = store.size()};
                }
                return IndexedStructuralDelta{
                    .time          = store.delta_time(),
                    .previous_size = store.previous_size(),
                    .size          = store.size(),
                };
            }

            /** Element memory for a live OR retained (removed this window) index. */
            [[nodiscard]] static const void *dynamic_retained_element_memory(const void *,
                                                                             const void *memory,
                                                                             std::size_t index)
            {
                const auto &store = storage(memory);
                if (index >= store.retained_size()) { return nullptr; }
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
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Map ||
                    binding.schema() != state->modified_map_binding.schema())
                {
                    throw std::logic_error("dynamic TSL delta copy requires the canonical modified map schema");
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

            // --- removed index set (delta bundle field 0, RFC 0031) ---

            [[nodiscard]] static std::size_t dynamic_removed_set_size(const void *,
                                                                      const void *memory) noexcept
            {
                return storage(memory).removed_index_count();
            }

            [[nodiscard]] static const void *dynamic_removed_set_key_at_index(const void *,
                                                                              const void *memory,
                                                                              std::size_t ordinal)
            {
                const auto &store = storage(memory);
                return &store.ordinal_key(store.removed_index_at(ordinal));
            }

            [[nodiscard]] static ValueView dynamic_removed_set_projector(const void *context,
                                                                          const void *memory,
                                                                          std::size_t ordinal)
            {
                return ValueView{ctx(context)->ordinal_key_binding,
                                 dynamic_removed_set_key_at_index(context, memory, ordinal)};
            }

            [[nodiscard]] static Range<ValueView> dynamic_removed_set_make_range(const void *context,
                                                                                  const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = dynamic_removed_set_size(context, memory),
                    .predicate = nullptr,
                    .projector = &dynamic_removed_set_projector,
                };
            }

            [[nodiscard]] static bool dynamic_removed_set_contains(const void *, const void *memory,
                                                                    const void *key)
            {
                const auto index = *MemoryUtils::cast<std::int64_t>(key);
                if (index < 0) { return false; }
                const auto &store = storage(memory);
                const auto count = store.removed_index_count();
                if (count == 0) { return false; }
                const auto value = static_cast<std::size_t>(index);
                return value >= store.size() && value < store.size() + count;
            }

            [[nodiscard]] static std::size_t dynamic_removed_set_hash(const void *context,
                                                                       const void *memory)
            {
                const auto *state = ctx(context);
                const auto &store = storage(memory);
                std::size_t result = 0;
                for (std::size_t ordinal = 0; ordinal < store.removed_index_count(); ++ordinal)
                {
                    result ^= state->ordinal_key_binding.ops_ref().hash(
                        &store.ordinal_key(store.removed_index_at(ordinal)));
                }
                return result;
            }

            [[nodiscard]] static bool dynamic_removed_set_equals(const void *context, const void *lhs,
                                                                  const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    static_cast<void>(context);
                    const auto &left  = storage(lhs);
                    const auto &right = storage(rhs);
                    return left.removed_index_count() == right.removed_index_count() &&
                           (left.removed_index_count() == 0 || left.size() == right.size());
                });
            }

            [[nodiscard]] static std::partial_ordering dynamic_removed_set_compare(
                const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
                const auto lhs_size = dynamic_removed_set_size(context, lhs);
                const auto rhs_size = dynamic_removed_set_size(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return dynamic_removed_set_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                      : std::partial_ordering::unordered;
            }

            [[nodiscard]] static std::string dynamic_removed_set_to_string(const void *, const void *memory)
            {
                const auto &store = storage(memory);
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                for (std::size_t ordinal = 0; ordinal < store.removed_index_count(); ++ordinal)
                {
                    if (ordinal > 0) { fmt::format_to(std::back_inserter(out), ", "); }
                    fmt::format_to(std::back_inserter(out), "{}",
                                   store.ordinal_key(store.removed_index_at(ordinal)));
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

            [[nodiscard]] static SetStorage build_dynamic_removed_set_storage(const void *context,
                                                                              const ValueTypeRef &binding,
                                                                              const void *memory)
            {
                const auto *state = ctx(context);
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Set)
                {
                    throw std::logic_error("dynamic TSL removed-set copy requires a canonical set schema");
                }
                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (key_binding == nullptr || key_binding != state->ordinal_key_binding)
                {
                    throw std::logic_error("dynamic TSL removed-set copy key binding is not resolved");
                }

                SetBuilder builder{key_binding};
                const auto &store = storage(memory);
                for (std::size_t ordinal = 0; ordinal < store.removed_index_count(); ++ordinal)
                {
                    builder.insert_copy(&store.ordinal_key(store.removed_index_at(ordinal)));
                }
                return builder.build_storage();
            }

            static void dynamic_removed_set_copy_construct_view(const void *context,
                                                                 const ValueTypeRef &binding,
                                                                 void *dst,
                                                                 const void *memory)
            {
                auto storage = build_dynamic_removed_set_storage(context, binding, memory);
                std::construct_at(static_cast<SetStorage *>(dst), std::move(storage));
            }

            static void dynamic_removed_set_copy_assign_view(const void *context,
                                                              const ValueTypeRef &binding,
                                                              void *dst,
                                                              const void *memory)
            {
                *static_cast<SetStorage *>(dst) = build_dynamic_removed_set_storage(context, binding, memory);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object dynamic_removed_set_projection_to_python(
                const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return Value{ValueView{state->removed_set_binding, memory}}.to_python();
            }
#endif

            // --- delta bundle {removed, modified} (RFC 0031) ---

            [[nodiscard]] static std::size_t dynamic_delta_bundle_size(const void *, const void *) noexcept
            {
                return 2;
            }

            [[nodiscard]] static const void *dynamic_delta_bundle_element_at(const void *, const void *memory,
                                                                             std::size_t index)
            {
                if (index < 2) { return memory; }
                throw std::out_of_range("dynamic TSL delta bundle index out of range");
            }

            [[nodiscard]] static ValueTypeRef dynamic_delta_bundle_element_binding(
                const void *context, const void *, std::size_t index) noexcept
            {
                const auto *state = ctx(context);
                return index == 0 ? state->removed_set_binding : state->modified_map_binding;
            }

            [[nodiscard]] static ValueView dynamic_delta_bundle_projector(const void *context,
                                                                          const void *memory,
                                                                          std::size_t index)
            {
                return ValueView{dynamic_delta_bundle_element_binding(context, memory, index),
                                 dynamic_delta_bundle_element_at(context, memory, index)};
            }

            [[nodiscard]] static Range<ValueView> dynamic_delta_bundle_make_range(const void *context,
                                                                                  const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = 2,
                    .predicate = nullptr,
                    .projector = &dynamic_delta_bundle_projector,
                };
            }

            [[nodiscard]] static std::size_t dynamic_delta_bundle_hash(const void *context,
                                                                        const void *memory)
            {
                const auto *state = ctx(context);
                return dynamic_combine_hash(state->removed_set_binding.ops_ref().hash(memory),
                                            state->modified_map_binding.ops_ref().hash(memory));
            }

            [[nodiscard]] static bool dynamic_delta_bundle_equals(const void *context, const void *lhs,
                                                                   const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctx(context);
                    return state->removed_set_binding.ops_ref().equals(lhs, rhs) &&
                           state->modified_map_binding.ops_ref().equals(lhs, rhs);
                });
            }

            [[nodiscard]] static std::partial_ordering dynamic_delta_bundle_compare(
                const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(lhs, rhs)) { return *order; }
                return dynamic_delta_bundle_equals(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                                       : std::partial_ordering::unordered;
            }

            [[nodiscard]] static std::string dynamic_delta_bundle_to_string(const void *context,
                                                                             const void *memory)
            {
                const auto *state = ctx(context);
                return fmt::format("{{removed: {}, modified: {}}}",
                                   state->removed_set_binding.ops_ref().to_string(memory),
                                   state->modified_map_binding.ops_ref().to_string(memory));
            }

            static void dynamic_delta_bundle_copy_assign_view(const void *context,
                                                               const ValueTypeRef &binding,
                                                               void *dst,
                                                               const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Bundle ||
                    binding.schema()->field_count != 2)
                {
                    throw std::logic_error("dynamic TSL delta copy requires canonical Bundle{removed, modified}");
                }
                const auto &plan = binding.checked_plan();
                if (!plan.is_composite() || plan.component_count() < 2)
                {
                    throw std::logic_error("dynamic TSL delta copy requires a two-field structured plan");
                }
                copy_projected_bundle_fields(
                    binding, dst, 2,
                    [&](std::size_t index) {
                        return dynamic_delta_bundle_element_binding(context, memory, index);
                    },
                    [&](std::size_t index) {
                        return dynamic_delta_bundle_element_at(context, memory, index);
                    });
            }

            static void dynamic_delta_bundle_copy_construct_view(const void *context,
                                                                  const ValueTypeRef &binding,
                                                                  void *dst,
                                                                  const void *memory)
            {
                const auto &plan = binding.checked_plan();
                plan.default_construct(dst);
                auto rollback = make_scope_exit([&]() noexcept { plan.destroy(dst); });
                dynamic_delta_bundle_copy_assign_view(context, binding, dst, memory);
                rollback.release();
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            [[nodiscard]] static nb::object dynamic_delta_bundle_to_python(const void *context,
                                                                            const void *memory)
            {
                const auto *state = ctx(context);
                nb::dict result;
                result[nb::str{"removed"}] = state->removed_set_binding.ops_ref().to_python(memory);
                result[nb::str{"modified"}] = state->modified_map_binding.ops_ref().to_python(memory);
                return result;
            }
#endif

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
                const bool first_for_parent = target.tracking().last_modified_time != modified_time;
                // A shorter source TRUNCATES (RFC 0031); the removed tail is
                // retained for the rest of the cycle by the storage.
                target.resize(source_values.size(), state->element_type, modified_time);

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
                target.resize(source_values.size(), state->element_type, modified_time);

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

        [[nodiscard]] TypeSystemRecursiveMutex &dynamic_list_context_mutex() noexcept
        {
            static TypeSystemRecursiveMutex mutex;
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

        std::lock_guard lock(dynamic_list_plan_mutex());
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

        std::lock_guard lock(dynamic_list_context_mutex());
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
            std::lock_guard lock(dynamic_list_context_mutex());
            dynamic_list_contexts().clear();
        }
        {
            std::lock_guard lock(dynamic_list_plan_mutex());
            dynamic_list_plan_entries().clear();
        }
    }

}  // namespace hgraph::ts_data_plan_factory_detail
