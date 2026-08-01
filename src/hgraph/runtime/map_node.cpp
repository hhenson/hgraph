#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/util/scope.h>

#include "mapped_child_bindings.h"
#include "mapped_key_source.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view map_storage_field_name{"map"};

        struct MapNodeStorage;

        /** Stable per-entry context for the out-of-band child schedule
            observer: identifies WHICH slot became due (the nested hook only
            knows when). */
        struct MapChildScheduleContext
        {
            MapNodeStorage *storage{nullptr};
            std::size_t     slot{0};
            // The current PULL-side heap entry for this child. Repeated outer
            // input visits often observe the same future child deadline;
            // coalesce those observations instead of growing the lazy heap
            // per tick. Push-side observations remain distinct because one
            // child graph may schedule multiple internal nodes.
            DateTime pulled_when{MAX_DT};
        };

        struct MapChildSchedule
        {
            DateTime    when{MAX_DT};
            std::size_t slot{0};
            bool        pulled{false};

            [[nodiscard]] bool operator>(const MapChildSchedule &other) const noexcept
            {
                if (when != other.when) { return when > other.when; }
                if (slot != other.slot) { return slot > other.slot; }
                return pulled > other.pulled;
            }
        };

        struct MapKeyEntry
        {
            explicit MapKeyEntry(Value key_)
                : key(std::move(key_))
            {
            }

            // Declaration order is load-bearing: members destroy in reverse, and
            // the child graph's inputs are subscribed to ``key_source`` — the
            // graph (the subscriber) must tear down BEFORE the source it
            // observes (and before ``schedule_context``, which the graph's
            // installed observer points at).
            Value                          key{};
            runtime_detail::MappedKeySource key_source{};
            MapChildScheduleContext        schedule_context{};
            GraphValue                     graph{};
        };

        struct MapNodeStorage final : SlotObserver
        {
            MapNodeStorage() = default;

            MapNodeStorage(const MapNodeStorage &)            = delete;
            MapNodeStorage &operator=(const MapNodeStorage &) = delete;
            MapNodeStorage(MapNodeStorage &&)                 = delete;
            MapNodeStorage &operator=(MapNodeStorage &&)      = delete;

            ~MapNodeStorage() override
            {
                unsubscribe_keys_noexcept();
                destroy_entries_without_output_noexcept();
            }

            // Slot ids and payload lifetime mirror the current __keys__ source:
            // logical removal stops the graph, while the source's later erase
            // callback destroys the entry in its stable slot.
            InPlaceGraphSlotStore<MapKeyEntry> entries{};
            // Source replacement may immediately reuse the same slot ids. The
            // inactive bank keeps the stopped old generation alive through the
            // replacement cycle.
            InPlaceGraphSlotStore<MapKeyEntry> previous_entries{};
            DateTime previous_entries_time{MIN_DT};
            // Cached bound-output handles of the outer inputs (tsd + broadcast
            // sources). Entry input bindings are established at creation and
            // refreshed only when an upstream source re-points.
            std::vector<TSOutputHandle> outer_sources{};
            TSOutputHandle              observed_keys_source{};
            bool                        observing_keys{false};
            bool                        keys_source_cleared{false};
            bool primed{false};

            // Rebinding is required for a wholesale source repoint, or for a
            // specific key whose membership changed in a multiplexed input.
            // Retain this cycle state across a paused child evaluation.
            bool               refresh_all_bindings{false};
            bool               selective_repoint_bindings{false};
            std::vector<Value> membership_changed_keys{};
            std::vector<Value> repoint_modified_keys{};

            // Candidate slots are sparse for ordinary multiplexed value ticks.
            // Full scans remain the conservative path for broadcast/repoint and
            // independently scheduled child graphs.
            SlotBitmap              evaluation_candidates{};
            std::vector<std::size_t> evaluation_slots{};
            std::size_t              resume_position_plus_one{0};
            // Priority queue of (when, slot) child schedules — a min-heap
            // popped for due slots each evaluation, fed by the nested
            // out-of-band hook and the evaluation loop's future schedules.
            // Entries are LAZY: a stale (rescheduled, stopped, or removed)
            // slot pops harmlessly — the evaluation loop re-checks due-ness.
            std::vector<MapChildSchedule> child_schedule_queue{};

            void push_child_schedule(MapChildSchedule schedule)
            {
                child_schedule_queue.push_back(schedule);
                std::push_heap(child_schedule_queue.begin(), child_schedule_queue.end(),
                               std::greater<>{});
            }

            void push_observed_child_schedule(DateTime when,
                                              const MapChildScheduleContext &schedule)
            {
                if (schedule.storage != this)
                {
                    return;
                }
                push_child_schedule(MapChildSchedule{when, schedule.slot, false});
            }

            void push_pulled_child_schedule(DateTime when,
                                            MapChildScheduleContext &schedule)
            {
                if (schedule.storage != this || schedule.pulled_when == when)
                {
                    return;
                }
                schedule.pulled_when = when;
                push_child_schedule(MapChildSchedule{when, schedule.slot, true});
            }

            [[nodiscard]] std::size_t active_count() const noexcept
            {
                std::size_t count = 0;
                for (std::size_t slot = 0; slot < entries.slot_capacity(); ++slot)
                {
                    const auto *entry = entries.entry_at(slot);
                    if (entry != nullptr && entry->graph.has_value() && entry->graph.view().started()) { ++count; }
                }
                return count;
            }

            [[nodiscard]] std::size_t child_graph_count() const noexcept
            {
                const auto count_bank = [](const auto &bank) {
                    std::size_t count = 0;
                    for (std::size_t slot = 0; slot < bank.slot_capacity(); ++slot)
                    {
                        const auto *entry = bank.entry_at(slot);
                        if (entry != nullptr && entry->graph.has_value()) { ++count; }
                    }
                    return count;
                };
                return count_bank(entries) + count_bank(previous_entries);
            }

            void destroy_previous_entries_before(DateTime evaluation_time) noexcept
            {
                if (previous_entries.has_entries() && previous_entries_time < evaluation_time)
                {
                    previous_entries.destroy_all();
                    previous_entries_time = MIN_DT;
                }
            }

            void retire_entries(DateTime evaluation_time)
            {
                if (previous_entries.has_entries())
                {
                    throw std::logic_error("map_ previous key-source generation is still occupied");
                }
                entries.swap(previous_entries);
                previous_entries_time = evaluation_time;
            }

            [[nodiscard]] MapKeyEntry *entry_at(std::size_t slot)
            {
                return entries.entry_at(slot);
            }

            [[nodiscard]] bool observe_keys_source(TSOutputHandle source)
            {
                if (source.same_as(observed_keys_source) && !keys_source_cleared) { return false; }

                unsubscribe_keys_noexcept();
                observed_keys_source = source;
                keys_source_cleared = false;
                if (source.bound())
                {
                    auto data = source.data_view();
                    auto set  = data.as_set();
                    entries.reserve_to(set.slot_capacity());
                    set.subscribe_slot_observer(this);
                    observing_keys = true;
                }
                return true;
            }

            void unsubscribe_keys_noexcept() noexcept
            {
                if (observing_keys)
                {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        auto data = observed_keys_source.data_view();
                        if (data.valid()) { data.as_set().unsubscribe_slot_observer(this); }
                        return true;
                    }));
                }
                observed_keys_source.reset();
                observing_keys = false;
                keys_source_cleared = false;
            }

            void destroy_entries_without_output_noexcept() noexcept
            {
                for (std::size_t slot = 0; slot < entries.slot_capacity(); ++slot)
                {
                    auto *entry = entry_at(slot);
                    if (entry == nullptr || !entry->graph.has_value()) { continue; }
                    static_cast<void>(fallback_on_exception(false, [&] {
                        entry->graph.view().stop();
                        return true;
                    }));
                }
                entries.destroy_all();
                primed = false;
            }

            void on_capacity(std::size_t, std::size_t new_capacity) override
            {
                entries.reserve_to(new_capacity);
            }

            void on_insert(std::size_t) override {}
            // The scheduled reconciliation performs logical delete/stop while
            // the current source identity and output binding are available.
            // A forwarding source can repoint during its mutation, so acting on
            // this slot callback alone could stop an unrelated replacement key.
            void on_remove(std::size_t) override {}
            void on_erase(std::size_t slot) override { entries.destroy_at(slot); }
            // Reconciliation must stop children and publish their removals
            // before an erase callback performs destruction.
            void on_clear() override { keys_source_cleared = true; }
        };

        struct MapNodeContext
        {
            MapNodeSpec spec{};
            std::size_t storage_offset{0};
            MemoryUtils::StorageLayout graph_layout{};
        };

        using MapNodeContextPtr = std::shared_ptr<const MapNodeContext>;

        struct SourceRepointStatus
        {
            bool mux_repointed{false};    ///< any MULTIPLEXED source re-pointed
            bool keys_repointed{false};   ///< the __keys__ source re-pointed
            bool broadcast_repointed{false};
        };

        [[nodiscard]] MapNodeContextPtr make_map_node_context(
            MapNodeSpec spec,
            std::size_t storage_offset,
            MemoryUtils::StorageLayout graph_layout)
        {
            return std::make_shared<MapNodeContext>(MapNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
                .graph_layout   = graph_layout,
            });
        }

        [[nodiscard]] const ValueTypeMetaData *map_node_context_schema()
        {
            return TypeRegistry::instance().register_scalar<MapNodeContextPtr>(
                "hgraph.runtime.map_node_context");
        }

        [[nodiscard]] const MapNodeContext &map_node_context(const NodeView &view)
        {
            const ValueView scalar_context = view.scalars();
            const auto &context = scalar_context.checked_as<MapNodeContextPtr>();
            if (!context)
            {
                throw std::logic_error("map node has no runtime context");
            }
            return *context;
        }

        [[nodiscard]] NodeStorageMetrics map_storage_metrics(
            const void *raw_context, const void *memory) noexcept
        {
            const auto &plan = *static_cast<const MemoryUtils::StoragePlan *>(raw_context);
            const auto &storage = *MemoryUtils::cast<const MapNodeStorage>(
                MemoryUtils::advance(
                    memory, plan.component(map_storage_field_name).offset));
            NodeStorageMetrics result{};
            for (const auto *bank : {&storage.entries, &storage.previous_entries})
            {
                result.nested_graph_count += bank->entry_count();
                result.nested_graph_capacity += bank->slot_capacity();
                result.nested_graph_blocks += bank->block_count();
                result.dynamic_live_bytes += bank->live_bytes();
                result.dynamic_reserved_bytes += bank->reserved_bytes();
            }
            return result;
        }

        [[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source)
        {
            if (!source.bound()) { return {}; }

            TSOutputHandle current = source.handle();
            while (source.forwarding())
            {
                TSOutputHandle target = source.forwarding_target();
                if (!target.bound() || target.same_as(current)) { break; }
                const auto *source_schema = source.schema();
                const auto *target_schema = target.schema();
                if (source_schema == nullptr || target_schema == nullptr ||
                    source_schema->kind != target_schema->kind ||
                    source.storage_type().ops_ref().kind != target.storage_type().ops_ref().kind)
                {
                    break;
                }
                current = target;
                source = target.view(source.evaluation_time());
            }
            return current;
        }

        [[nodiscard]] TSDDataView checked_dict_view(TSDataView data,
                                                    std::string_view stage,
                                                    std::size_t source_index)
        {
            if (!data.valid() || data.storage_type().ops_ref().kind != TSTypeKind::TSD)
            {
                const auto *schema = data.schema();
                const auto storage_type = data.storage_type();
                const auto storage_name = storage_type.record() != nullptr
                                              ? storage_type.record()->implementation_name()
                                              : std::string_view{};
                throw std::logic_error(
                    "map_: " + std::string{stage} + " source[" +
                    std::to_string(source_index) + "] with schema '" +
                    (schema != nullptr ? std::string{schema->name()} : std::string{"<untyped>"}) +
                    "' uses storage '" +
                    (storage_name.empty() ? std::string{"<unbound>"} : std::string{storage_name}) +
                    "', not TSD storage");
            }
            return data.as_dict();
        }

        [[nodiscard]] std::optional<TSDDataMutationView> begin_map_output_mutation(
            const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_output()) { return std::nullopt; }
            auto output = view.output(evaluation_time);
            auto dict   = output.as_dict();
            return dict.begin_mutation(evaluation_time);
        }

        [[nodiscard]] std::optional<TSDDataMutationView> begin_map_error_mutation(
            const NodeView &view, DateTime evaluation_time)
        {
            if (!view.has_error_output()) { return std::nullopt; }
            auto output = view.error_output(evaluation_time);
            auto dict   = output.as_dict();
            return dict.begin_mutation(evaluation_time);
        }

        [[nodiscard]] SourceRepointStatus update_source_handles(const TSInputView &root_input,
                                                                MapNodeStorage &storage,
                                                                const std::vector<std::size_t> &multiplexed_inputs,
                                                                std::size_t keys_input_index)
        {
            const std::size_t outer_count = root_input.as_bundle().size();
            const bool        initialized = storage.outer_sources.size() == outer_count;
            storage.outer_sources.resize(outer_count);

            SourceRepointStatus status;
            for (std::size_t i = 0; i < outer_count; ++i)
            {
                TSOutputHandle current = effective_output_handle(root_input.indexed_child_at(i).bound_output());
                if (!current.same_as(storage.outer_sources[i]))
                {
                    storage.outer_sources[i] = current;
                    if (initialized)
                    {
                        const bool multiplexed =
                            std::find(multiplexed_inputs.begin(), multiplexed_inputs.end(), i) !=
                            multiplexed_inputs.end();
                        if (multiplexed) { status.mux_repointed = true; }
                        if (i == keys_input_index) { status.keys_repointed = true; }
                        else if (!multiplexed) { status.broadcast_repointed = true; }
                    }
                }
            }
            return status;
        }

        void clear_entry_output_binding(const NodeView &view, const MapNodeContext &context,
                                        const MapKeyEntry &entry, DateTime evaluation_time)
        {
            if (!context.spec.child.output_binding.has_value()) { return; }
            runtime_detail::clear_mapped_output_element_binding(
                view, evaluation_time, entry.key.view(), context.spec.output_binding_mode);
        }

        void remove_entry_at_slot(const NodeView &view, const MapNodeContext &context,
                                  MapNodeStorage &storage, TSDDataMutationView *output_mutation,
                                  TSDDataMutationView *error_mutation,
                                  std::size_t slot, DateTime evaluation_time)
        {
            auto *entry = storage.entries.entry_at(slot);
            if (entry == nullptr) { return; }

            clear_entry_output_binding(view, context, *entry, evaluation_time);
            if (entry->graph.has_value() && entry->graph.view().started()) {
                entry->graph.view().stop(evaluation_time);
            }
            entry->schedule_context.pulled_when = MAX_DT;
            if (output_mutation != nullptr) { (void)output_mutation->erase(entry->key.view()); }
            if (error_mutation != nullptr && error_mutation->contains(entry->key.view()))
            {
                (void)error_mutation->erase(entry->key.view());
            }
        }

        void remove_all_entries(const NodeView &view, const MapNodeContext &context,
                                MapNodeStorage &storage, TSDDataMutationView *output_mutation,
                                TSDDataMutationView *error_mutation,
                                DateTime evaluation_time)
        {
            for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot)
            {
                remove_entry_at_slot(view, context, storage, output_mutation, error_mutation,
                                     slot, evaluation_time);
            }
        }

        void create_entry_at_slot(const NodeView &view, const MapNodeContext &context, MapNodeStorage &storage,
                                  TSDDataMutationView *output_mutation, const TSSDataView &keys_set,
                                  std::size_t slot, DateTime evaluation_time)
        {
            const MapNodeSpec &spec     = context.spec;
            const ValueView    key_view = keys_set.at_slot(slot);
            storage.entries.reserve_to(std::max(storage.entries.slot_capacity(), slot + 1));
            MapKeyEntry *existing = storage.entries.entry_at(slot);
            auto &entry = existing != nullptr
                              ? *existing
                              : storage.entries.construct_at(slot, Value{key_view});
            if (entry.graph.has_value() && entry.graph.view().started()) { return; }
            auto rollback = UnwindCleanupGuard([&] {
                clear_entry_output_binding(view, context, entry, evaluation_time);
                if (entry.graph.has_value() && entry.graph.view().started()) {
                    entry.graph.view().stop(evaluation_time);
                }
                if (output_mutation != nullptr) { (void)output_mutation->erase(entry.key.view()); }
                if (existing == nullptr) { storage.entries.destroy_at(slot); }
            });

            if (!entry.graph.has_value())
            {
                entry.graph = spec.child.graph_builder.make_nested_graph(
                    view.pointer(),
                    storage.entries.graph_memory(slot),
                    context.graph_layout);
            }
            if (spec.key_output_schema != nullptr)
            {
                entry.key_source.bind(*spec.key_output_schema, entry.key, evaluation_time);
            }

            // Output maps instantiate one stable parent element before binding
            // the child's terminal. Sink maps have no parent output.
            if (output_mutation != nullptr) { (void)(*output_mutation)[key_view]; }

            const TSOutputView key_source = entry.key_source.bound()
                                                ? entry.key_source.view(evaluation_time)
                                                : TSOutputView{};
            runtime_detail::bind_mapped_child_inputs(view, entry.graph.view(), evaluation_time,
                                                     spec.child, spec.args, entry.key.view(), key_source,
                                                     std::nullopt, false, true);
            runtime_detail::bind_mapped_child_output(view, entry.graph.view(), evaluation_time,
                                                     spec.child.output_binding, spec.args, entry.key.view(),
                                                     key_source,
                                                     spec.output_binding_mode);
            entry.graph.view().start(evaluation_time);
            // Out-of-band child schedules (a notification or scheduler firing
            // while the child is idle between map evaluations) report into
            // the schedule queue so the input-event fast path knows the slot
            // is due (issue #175).
            entry.schedule_context = MapChildScheduleContext{&storage, slot};
            entry.graph.view().set_child_schedule_observer(
                [](void *context, DateTime when) {
                    auto *schedule = static_cast<MapChildScheduleContext *>(context);
                    schedule->storage->push_observed_child_schedule(when, *schedule);
                },
                &entry.schedule_context);
            schedule_sampled_input_consumers(
                entry.graph.view(), evaluation_time, spec.child.input_bindings);
            rollback.release();
        }

        void create_live_key_entries(const NodeView &view, const MapNodeContext &context, MapNodeStorage &storage,
                                     TSDDataMutationView *output_mutation, const TSSDataView &keys_set,
                                     DateTime evaluation_time)
        {
            storage.entries.reserve_to(keys_set.slot_capacity());
            for (std::size_t slot = 0; slot < keys_set.slot_capacity(); ++slot)
            {
                if (keys_set.slot_live(slot))
                {
                    create_entry_at_slot(view, context, storage, output_mutation, keys_set, slot, evaluation_time);
                }
            }
        }

        [[nodiscard]] bool key_source_layout_compatible(const MapNodeStorage &storage,
                                                        const TSSDataView &keys_set)
        {
            for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot)
            {
                const MapKeyEntry *entry = storage.entries.entry_at(slot);
                if (entry == nullptr) { continue; }
                if (slot >= keys_set.slot_capacity() || !keys_set.slot_occupied(slot) ||
                    !entry->key.equals(keys_set.at_slot(slot)))
                {
                    return false;
                }
            }
            return true;
        }

        void reconcile_compatible_key_source(const NodeView &view, const MapNodeContext &context,
                                             MapNodeStorage &storage, const TSSDataView &keys_set,
                                             DateTime evaluation_time)
        {
            auto output_mutation = begin_map_output_mutation(view, evaluation_time);
            auto error_mutation  = begin_map_error_mutation(view, evaluation_time);
            auto *mutation       = output_mutation ? &*output_mutation : nullptr;
            auto *errors         = error_mutation ? &*error_mutation : nullptr;

            for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot)
            {
                if (storage.entries.entry_at(slot) == nullptr) { continue; }
                if (slot >= keys_set.slot_capacity() || !keys_set.slot_live(slot))
                {
                    remove_entry_at_slot(view, context, storage, mutation, errors,
                                         slot, evaluation_time);
                }
            }
            for (std::size_t slot = 0; slot < keys_set.slot_capacity(); ++slot)
            {
                if (!keys_set.slot_live(slot)) { continue; }
                const MapKeyEntry *entry = storage.entries.entry_at(slot);
                if (entry == nullptr || !entry->graph.has_value() || !entry->graph.view().started())
                {
                    create_entry_at_slot(view, context, storage, mutation, keys_set,
                                         slot, evaluation_time);
                }
            }
        }

        // Per-key lifecycle reconciliation (the cycle "setup"): re-point source handles,
        // create/remove children from the __keys__ set. Returns whether surviving children
        // need their inputs re-bound. Skipped on a pause/resume re-entry (the key set does
        // not change mid-cycle).
        [[nodiscard]] bool map_reconcile_keys(const NodeView &view, const MapNodeContext &context,
                                              MapNodeStorage &storage, DateTime evaluation_time)
        {
            const auto &spec       = context.spec;
            const auto  keys_index = *spec.keys_input_index;
            storage.destroy_previous_entries_before(evaluation_time);
            storage.entries.bind_graph_layout(context.graph_layout);
            storage.previous_entries.bind_graph_layout(context.graph_layout);

            auto root_input = view.input(evaluation_time);
            SourceRepointStatus source_status =
                update_source_handles(root_input.borrowed_ref(), storage, spec.multiplexed_inputs, keys_index);
            bool bindings_need_refresh = source_status.mux_repointed || source_status.broadcast_repointed;
            storage.selective_repoint_bindings = source_status.mux_repointed &&
                                                  !source_status.broadcast_repointed;
            storage.repoint_modified_keys.clear();
            if (storage.selective_repoint_bindings)
            {
                for (const std::size_t mux_index : spec.multiplexed_inputs)
                {
                    auto mux_input = root_input.indexed_child_at(mux_index);
                    if (!mux_input.valid() || !mux_input.modified()) { continue; }
                    auto dict = mux_input.as_dict();
                    for (const ValueView &key : dict.modified_keys())
                    {
                        storage.repoint_modified_keys.emplace_back(key);
                    }
                }
            }

            // Value ticks keep their stable element endpoint and must not
            // rebind every child. Only structural membership changes need a
            // per-key refresh (for example, one input of a multi-TSD map loses
            // a key while the union key set keeps that child alive).
            storage.membership_changed_keys.clear();
            for (const std::size_t mux_index : spec.multiplexed_inputs)
            {
                const TSOutputHandle &source = storage.outer_sources[mux_index];
                if (!source.bound()) { continue; }

                auto source_data = source.data_view();
                auto dict = checked_dict_view(std::move(source_data), "membership source", mux_index);
                if (!dict.modified(evaluation_time)) { continue; }

                for (std::size_t slot = dict.next_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = dict.next_added_slot(slot))
                {
                    storage.membership_changed_keys.emplace_back(dict.key_at_slot(slot));
                }
                const std::size_t first_removed_slot = dict.next_removed_slot();
                if (first_removed_slot != TS_DATA_NO_CHILD_ID && dict.slot_removed(first_removed_slot))
                {
                    for (std::size_t slot = first_removed_slot; slot != TS_DATA_NO_CHILD_ID;
                         slot = dict.next_removed_slot(slot))
                    {
                        storage.membership_changed_keys.emplace_back(dict.removed_key_at_slot(slot));
                    }
                }
                else
                {
                    // A sampled forwarding transition owns its removed-key
                    // surface independently of the current source slots.
                    for (const ValueView &removed_key : dict.removed_keys())
                    {
                        storage.membership_changed_keys.emplace_back(removed_key);
                    }
                }
            }

            // Explicit ``__keys__``: the TSS alone drives the lifecycle —
            // children exist exactly for its members. The multiplexed dicts
            // only feed elements; their membership changes re-bind surviving
            // entries through ``bindings_need_refresh`` above.
            auto keys_input = root_input.indexed_child_at(keys_index);
            auto keys_source = keys_input.bound_output();
            const bool keys_handle_changed = !keys_source.handle().same_as(storage.observed_keys_source);
            const bool keys_source_replaced = keys_handle_changed || source_status.keys_repointed ||
                                              storage.keys_source_cleared;
            bool preserve_compatible_entries = false;
            if (keys_source_replaced && storage.entries.has_entries() && keys_input.valid())
            {
                preserve_compatible_entries = key_source_layout_compatible(
                    storage, keys_input.data_view().as_set());
            }
            if (keys_source_replaced && storage.entries.has_entries() &&
                !preserve_compatible_entries)
            {
                auto output_mutation = begin_map_output_mutation(view, evaluation_time);
                auto error_mutation  = begin_map_error_mutation(view, evaluation_time);
                remove_all_entries(view, context, storage,
                                   output_mutation ? &*output_mutation : nullptr,
                                   error_mutation ? &*error_mutation : nullptr, evaluation_time);
                storage.unsubscribe_keys_noexcept();
                storage.retire_entries(evaluation_time);
                storage.primed = false;
            }
            const bool keys_observer_changed = storage.observe_keys_source(keys_source.handle());
            if (!keys_input.valid())
            {
                auto output_mutation = begin_map_output_mutation(view, evaluation_time);
                auto error_mutation  = begin_map_error_mutation(view, evaluation_time);
                remove_all_entries(view, context, storage,
                                   output_mutation ? &*output_mutation : nullptr,
                                   error_mutation ? &*error_mutation : nullptr, evaluation_time);
                storage.primed = false;
            }
            else
            {
                const auto &keys_data = keys_input.data_view();
                auto key_set = keys_data.as_set();
                storage.entries.reserve_to(key_set.slot_capacity());

                const bool rebuild = !storage.primed ||
                                     ((keys_source_replaced || keys_observer_changed) &&
                                      !preserve_compatible_entries);
                if (rebuild)
                {
                    const bool publish_initial_empty = key_set.empty() && view.has_output() &&
                                                       !view.output(evaluation_time).valid();
                    auto output_mutation = begin_map_output_mutation(view, evaluation_time);
                    auto error_mutation  = begin_map_error_mutation(view, evaluation_time);
                    auto *mutation       = output_mutation ? &*output_mutation : nullptr;
                    auto *errors         = error_mutation ? &*error_mutation : nullptr;
                    remove_all_entries(view, context, storage, mutation, errors, evaluation_time);
                    create_live_key_entries(view, context, storage, mutation, key_set, evaluation_time);
                    if (publish_initial_empty && output_mutation) { output_mutation->touch(); }
                    storage.primed = true;
                }
                else if (keys_source_replaced || keys_observer_changed)
                {
                    reconcile_compatible_key_source(view, context, storage, key_set,
                                                    evaluation_time);
                }
                else if (keys_input.modified())
                {
                    auto output_mutation = begin_map_output_mutation(view, evaluation_time);
                    auto error_mutation  = begin_map_error_mutation(view, evaluation_time);
                    auto *mutation       = output_mutation ? &*output_mutation : nullptr;
                    auto *errors         = error_mutation ? &*error_mutation : nullptr;

                    for (std::size_t slot = key_set.next_removed_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = key_set.next_removed_slot(slot))
                    {
                        remove_entry_at_slot(view, context, storage, mutation, errors,
                                             slot, evaluation_time);
                    }

                    for (std::size_t slot = key_set.next_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = key_set.next_added_slot(slot))
                    {
                        create_entry_at_slot(view, context, storage, mutation, key_set, slot,
                                             evaluation_time);
                    }
                }
            }
            return bindings_need_refresh;
        }

        [[nodiscard]] bool map_entry_membership_changed(
            const MapNodeStorage &storage,
            const ValueView &key)
        {
            for (const Value &changed_key : storage.membership_changed_keys)
            {
                if (changed_key.equals(key)) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool map_entry_repoint_modified(const MapNodeStorage &storage,
                                                      const ValueView &key)
        {
            for (const Value &modified_key : storage.repoint_modified_keys)
            {
                if (modified_key.equals(key)) { return true; }
            }
            return false;
        }

        void add_map_evaluation_slot(MapNodeStorage &storage, std::size_t slot)
        {
            if (slot == TS_DATA_NO_CHILD_ID || storage.entry_at(slot) == nullptr) { return; }
            storage.evaluation_candidates.set(slot);
        }

        void materialize_map_evaluation_slots(MapNodeStorage &storage)
        {
            storage.evaluation_slots.clear();
            for (std::size_t word_index = 0;
                 word_index < storage.evaluation_candidates.word_count(); ++word_index)
            {
                std::uint64_t word = storage.evaluation_candidates.words[word_index];
                while (word != 0)
                {
                    const auto bit = static_cast<std::size_t>(std::countr_zero(word));
                    storage.evaluation_slots.push_back(
                        word_index * SlotBitmap::bits_per_word + bit);
                    word &= word - 1;
                }
            }
        }

        void collect_all_map_evaluation_slots(MapNodeStorage &storage)
        {
            storage.evaluation_slots.reserve(storage.entries.slot_capacity());
            for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot)
            {
                add_map_evaluation_slot(storage, slot);
            }
        }

        void prepare_map_evaluation_slots(const NodeView &view, const MapNodeContext &context,
                                          MapNodeStorage &storage, DateTime evaluation_time,
                                          bool was_primed)
        {
            storage.evaluation_slots.clear();
            storage.evaluation_candidates.resize(storage.entries.slot_capacity());
            storage.evaluation_candidates.reset();
            storage.resume_position_plus_one = 0;

            auto root_input = view.input(evaluation_time);
            auto keys_input = root_input.indexed_child_at(*context.spec.keys_input_index);
            bool input_event = keys_input.modified();
            bool full_scan = storage.refresh_all_bindings || !was_primed;

            for (const MapArgSource &arg : context.spec.args)
            {
                if (arg.kind != MapArgSourceKind::OuterInput) { continue; }
                auto input = root_input.indexed_child_at(arg.outer_index);
                if (input.modified())
                {
                    input_event = true;
                    full_scan = true;
                    const auto *schema = input.schema();
                    if (schema != nullptr &&
                        (schema->kind == TSTypeKind::TSB ||
                         schema->kind == TSTypeKind::TSL))
                    {
                        // A structured forwarding boundary can keep its root
                        // handle while a REF resolution changes the projected
                        // field endpoints. Existing children must follow those
                        // new leaf routes before they evaluate.
                        storage.refresh_all_bindings = true;
                    }
                }
            }

            if (keys_input.valid())
            {
                const auto &keys_data = keys_input.data_view();
                auto keys = keys_data.as_set();
                const void *keys_storage = keys.base().storage_ref().data();
                if (keys_input.modified())
                {
                    for (std::size_t slot = keys.next_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = keys.next_added_slot(slot))
                    {
                        add_map_evaluation_slot(storage, slot);
                    }
                }

                for (const std::size_t mux_index : context.spec.multiplexed_inputs)
                {
                    const TSOutputHandle &source = storage.outer_sources[mux_index];
                    if (!source.bound()) { continue; }
                    auto source_data = source.data_view();
                    auto dict = checked_dict_view(std::move(source_data), "modified source", mux_index);
                    if (!dict.modified(evaluation_time)) { continue; }
                    const bool shares_key_slots =
                        dict.key_set().base().storage_ref().data() == keys_storage;

                    input_event = true;
                    for (std::size_t source_slot = dict.next_modified_slot();
                         source_slot != TS_DATA_NO_CHILD_ID;
                         source_slot = dict.next_modified_slot(source_slot))
                    {
                        add_map_evaluation_slot(
                            storage,
                            shares_key_slots ? source_slot : keys.find_slot(dict.key_at_slot(source_slot)));
                    }
                }

                for (const Value &key : storage.membership_changed_keys)
                {
                    add_map_evaluation_slot(storage, keys.find_slot(key.view()));
                }
            }

            // Children DUE by their own internal schedules (a service
            // response delivery, a scheduler alarm) pop from the schedule
            // queue — the fast path must not starve them when an outer tick
            // coincides with the wake-up cycle (issue #175: a request-reply
            // response dropped when a new key arrived in the delivery
            // cycle). Stale entries pop harmlessly: the evaluation loop
            // re-checks each child's due-ness.
            while (!storage.child_schedule_queue.empty() &&
                   storage.child_schedule_queue.front().when <= evaluation_time)
            {
                std::pop_heap(storage.child_schedule_queue.begin(),
                              storage.child_schedule_queue.end(), std::greater<>{});
                const MapChildSchedule schedule =
                    storage.child_schedule_queue.back();
                storage.child_schedule_queue.pop_back();
                auto *entry = storage.entry_at(schedule.slot);
                if (entry == nullptr)
                {
                    continue;
                }
                if (schedule.pulled)
                {
                    if (entry->schedule_context.pulled_when != schedule.when)
                    {
                        continue;
                    }
                    entry->schedule_context.pulled_when = MAX_DT;
                }
                add_map_evaluation_slot(storage, schedule.slot);
            }

            // With no outer input event the parent was woken by a nested
            // child's own dependency (e.g. a mesh resume); its identity is
            // not encoded in the graph schedule table, so retain the
            // conservative scan.
            if (!input_event) { full_scan = true; }
            if (full_scan)
            {
                storage.evaluation_slots.clear();
                collect_all_map_evaluation_slots(storage);
            }
            materialize_map_evaluation_slots(storage);
        }

        void write_map_error(const NodeView &view, const NodeView &failed_node,
                             const ValueView &key, DateTime evaluation_time,
                             std::string error_msg)
        {
            const NodeTypeMetaData *schema = view.schema();
            const ErrorCaptureOptions options = schema != nullptr ? schema->error_capture : ErrorCaptureOptions{};
            NodeErrorFields fields = capture_node_error(
                failed_node.valid() ? failed_node : view, evaluation_time, std::move(error_msg), options);

            Value error_value = make_node_error_value(fields);
            auto  output      = view.error_output(evaluation_time);
            auto  error_dict  = output.as_dict();
            auto  errors      = error_dict.begin_mutation(evaluation_time);
            auto  child       = errors[key];
            auto  mutation    = child.begin_mutation(evaluation_time);
            (void)mutation.move_value_from(std::move(error_value));
        }

        // Evaluates the keyed children, supporting pause/resume: a child that pauses (a
        // mesh nested in the child needs a sibling) propagates the pause — we save the slot
        // cursor and return false so the enclosing mesh resolves the dependency and
        // re-evaluates us. On resume the key reconciliation is skipped and the loop
        // continues from the saved slot; completion resets the cursor.
        bool map_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        map_view = view.as<MapNodeView>();
            const auto &context  = *static_cast<const MapNodeContext *>(map_view.internal_context());
            auto       &storage  = *MemoryUtils::cast<MapNodeStorage>(map_view.internal_storage());
            const auto &spec     = context.spec;

            const bool resuming = storage.resume_position_plus_one != 0;
            if (!resuming)
            {
                const bool was_primed = storage.primed;
                storage.refresh_all_bindings =
                    map_reconcile_keys(view, context, storage, evaluation_time);
                prepare_map_evaluation_slots(view, context, storage, evaluation_time, was_primed);
            }
            const bool captures_errors = view.has_error_output() && view.schema() != nullptr &&
                                         view.schema()->captures_errors;

            // Due children write their TSD elements directly via their terminal forwarding
            // outputs (no post-evaluation collection). A child evaluation propagates its own
            // next scheduled time back to this node; unevaluated children pull theirs up.
            const std::size_t start_position = resuming ? storage.resume_position_plus_one - 1 : 0;
            for (std::size_t position = start_position; position < storage.evaluation_slots.size(); ++position)
            {
                const std::size_t slot = storage.evaluation_slots[position];
                auto *entry = storage.entry_at(slot);
                if (entry == nullptr || !entry->graph.has_value()) { continue; }

                auto child = entry->graph.view();
                // A removed key's child is stopped but its entry (and any
                // schedule enqueued before the stop) lingers this cycle —
                // stopped children never evaluate.
                if (!child.started()) { continue; }
                const bool membership_changed =
                    map_entry_membership_changed(storage, entry->key.view());
                if (storage.refresh_all_bindings || membership_changed)
                {
                    const bool silent_repoint = storage.selective_repoint_bindings &&
                                                !membership_changed &&
                                                !map_entry_repoint_modified(storage, entry->key.view());
                    const TSOutputView key_source = entry->key_source.bound()
                                                        ? entry->key_source.view(evaluation_time)
                                                        : TSOutputView{};
                    runtime_detail::bind_mapped_child_inputs(view, child, evaluation_time, spec.child,
                                                             spec.args, entry->key.view(), key_source,
                                                             std::nullopt, silent_repoint);
                    runtime_detail::bind_mapped_child_output(view, child, evaluation_time,
                                                             spec.child.output_binding, spec.args,
                                                             entry->key.view(), key_source,
                                                             spec.output_binding_mode, silent_repoint);
                }

                const bool resume_this = resuming && position == start_position;
                if (child.next_scheduled_time() <= evaluation_time || resume_this)
                {
                    const bool completed = captures_errors
                                               ? fallback_on_exception(
                                                     true,
                                                     [&] { return child.evaluate(evaluation_time); },
                                                     [&](const char *error) {
                                                         NodeView failed = child.failed_node();
                                                         write_map_error(view, failed, entry->key.view(),
                                                                         evaluation_time, error);
                                                     })
                                               : child.evaluate(evaluation_time);
                    if (!completed)
                    {
                        storage.resume_position_plus_one = position + 1;
                        return false;
                    }
                    runtime_detail::finalize_mapped_child_output(
                        view, evaluation_time, spec.child.output_binding,
                        entry->key.view());
                }
                if (const DateTime next = child.next_scheduled_time(); next != MAX_DT && next > evaluation_time)
                {
                    // The PULL half: schedules created while the map drove
                    // the child land in the queue here; the out-of-band
                    // observer covers schedules arriving between map
                    // evaluations.
                    storage.push_pulled_child_schedule(
                        next, entry->schedule_context);
                }
                else
                {
                    // Invalidate a lazy entry when the child consumed or
                    // cancelled its previous deadline.
                    entry->schedule_context.pulled_when = MAX_DT;
                }
            }
            storage.resume_position_plus_one = 0;
            storage.evaluation_slots.clear();
            storage.refresh_all_bindings = false;
            storage.selective_repoint_bindings = false;
            storage.membership_changed_keys.clear();
            storage.repoint_modified_keys.clear();
            // Current-cycle observer callbacks can enqueue a due entry after
            // the queue was drained at the start of this evaluation (for
            // example while a newly created child samples a valid config
            // input). Do not let that stale minimum replace the future
            // deadline propagated by the child.
            while (!storage.child_schedule_queue.empty() &&
                   storage.child_schedule_queue.front().when <= evaluation_time)
            {
                std::pop_heap(storage.child_schedule_queue.begin(),
                              storage.child_schedule_queue.end(), std::greater<>{});
                const MapChildSchedule schedule =
                    storage.child_schedule_queue.back();
                storage.child_schedule_queue.pop_back();
                if (schedule.pulled)
                {
                    auto *entry = storage.entry_at(schedule.slot);
                    if (entry != nullptr &&
                        entry->schedule_context.pulled_when == schedule.when)
                    {
                        entry->schedule_context.pulled_when = MAX_DT;
                    }
                }
            }
            if (!storage.child_schedule_queue.empty())
            {
                // The heap, rather than the sparse candidate set, owns the
                // earliest child wake-up. Re-arm from its minimum so a
                // future child that was not input-driven this cycle cannot be
                // hidden by another candidate's later deadline.
                view.graph().schedule_node(
                    view.node_index(), storage.child_schedule_queue.front().when);
            }
            return true;
        }

        void map_node_stop(const NodeView &view, DateTime evaluation_time)
        {
            auto  map_view = view.as<MapNodeView>();
            auto &storage  = *MemoryUtils::cast<MapNodeStorage>(map_view.internal_storage());

            const auto &context  = *static_cast<const MapNodeContext *>(map_view.internal_context());
            // Graph shutdown is not a logical key removal and must not
            // publish erases. The terminal output may already have been
            // detached by its owning service or parent graph.
            remove_all_entries(view, context, storage, nullptr, nullptr,
                               evaluation_time);
            storage.unsubscribe_keys_noexcept();
            storage.primed = false;
            storage.refresh_all_bindings = false;
            storage.selective_repoint_bindings = false;
            storage.membership_changed_keys.clear();
            storage.repoint_modified_keys.clear();
            storage.evaluation_slots.clear();
            storage.resume_position_plus_one = 0;
            storage.child_schedule_queue.clear();
        }

        void validate_map_node_spec(const NodeTypeMetaData &meta, const MapNodeSpec &spec)
        {
            const bool has_output = meta.output_schema != nullptr;
            if (spec.child.output_binding.has_value() != has_output)
            {
                throw std::invalid_argument(
                    "map_node child output binding must be present exactly when the map has an output");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB)
            {
                throw std::invalid_argument("map_node requires a TSB input schema");
            }
            if (has_output && meta.output_schema->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("map_node output must be a TSD");
            }
            const auto *input_fields = meta.input_schema->fields();

            if (!spec.keys_input_index.has_value())
            {
                throw std::invalid_argument(
                    "map_node requires a __keys__ input (the lifecycle is keys-driven; map_ wiring derives it "
                    "from the multiplexed inputs when not supplied)");
            }
            if (*spec.keys_input_index >= meta.input_schema->field_count())
            {
                throw std::invalid_argument("map_node __keys__ input index is out of range");
            }
            const auto *keys_schema =
                TypeRegistry::instance().dereference(input_fields[*spec.keys_input_index].type);
            if (keys_schema == nullptr || keys_schema->kind != TSTypeKind::TSS ||
                keys_schema->value_schema == nullptr || keys_schema->value_schema->element_type == nullptr)
            {
                throw std::invalid_argument("map_node __keys__ input must be a concrete TSS");
            }
            const ValueTypeMetaData *mapped_key_type = keys_schema->value_schema->element_type;
            if (has_output && meta.output_schema->key_type() != mapped_key_type)
            {
                throw std::invalid_argument("map_node output key type must match the __keys__ element type");
            }

            std::vector<std::size_t> seen_mux_inputs;
            seen_mux_inputs.reserve(spec.multiplexed_inputs.size());
            for (const std::size_t mux_index : spec.multiplexed_inputs)
            {
                if (mux_index >= meta.input_schema->field_count())
                {
                    throw std::invalid_argument("map_node multiplexed input index is out of range");
                }
                if (std::find(seen_mux_inputs.begin(), seen_mux_inputs.end(), mux_index) !=
                    seen_mux_inputs.end())
                {
                    throw std::invalid_argument("map_node multiplexed input indices must be unique");
                }
                seen_mux_inputs.push_back(mux_index);
                const auto *tsd_schema = TypeRegistry::instance().dereference(input_fields[mux_index].type);
                if (tsd_schema == nullptr || tsd_schema->kind != TSTypeKind::TSD)
                {
                    throw std::invalid_argument("map_node multiplexed input index must select a TSD input field");
                }
                if (tsd_schema->key_type() != mapped_key_type)
                {
                    throw std::invalid_argument(
                        "map_node __keys__ element type must match every multiplexed input key type");
                }
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            if (spec.child.output_binding.has_value() && !spec.child.output_binding->target_path.empty())
            {
                throw std::invalid_argument("map_node child output binding must target the map element root");
            }

            bool key_source_seen = false;
            bool element_source_seen = false;
            auto mark_source_arg = [&](const MapArgSource &arg) {
                switch (arg.kind)
                {
                    case MapArgSourceKind::Key:
                        key_source_seen = true;
                        break;
                    case MapArgSourceKind::Element:
                        element_source_seen = true;
                        if (arg.outer_index >= meta.input_schema->field_count())
                        {
                            throw std::invalid_argument("map_node element source index is out of range");
                        }
                        if (std::find(spec.multiplexed_inputs.begin(), spec.multiplexed_inputs.end(),
                                      arg.outer_index) == spec.multiplexed_inputs.end())
                        {
                            throw std::invalid_argument(
                                "map_node element source index must select a multiplexed TSD input");
                        }
                        break;
                    case MapArgSourceKind::OuterInput:
                        if (arg.outer_index >= meta.input_schema->field_count())
                        {
                            throw std::invalid_argument("map_node outer input source index is out of range");
                        }
                        break;
                }
            };

            // Validate declared sources independently of whether compilation
            // retained a binding for each one. Unused formal parameters are
            // valid and disappear from the compiled child graph.
            for (const MapArgSource &arg : spec.args) { mark_source_arg(arg); }

            if (spec.child.output_binding.has_value())
            {
                const auto &output_binding = *spec.child.output_binding;
                switch (output_binding.kind)
                {
                    case NestedGraphOutputBinding::Kind::ChildOutput:
                        if (output_binding.source.node >= child_node_count)
                        {
                            throw std::invalid_argument("map_node child output source node is out of range");
                        }
                        if (spec.output_binding_mode == MapOutputBindingMode::OutputElementForwardsToParentSource)
                        {
                            throw std::invalid_argument(
                                "map_node child output binding cannot use parent-source forwarding mode");
                        }
                        break;

                    case NestedGraphOutputBinding::Kind::ParentInput:
                        if (output_binding.parent_source_path.empty())
                        {
                            throw std::invalid_argument("map_node parent-input output binding requires a source ordinal");
                        }
                        if (output_binding.parent_source_path[0] >= spec.args.size())
                        {
                            throw std::invalid_argument("map_node parent-input output source ordinal is out of range");
                        }
                        if (spec.output_binding_mode != MapOutputBindingMode::OutputElementForwardsToParentSource)
                        {
                            throw std::invalid_argument(
                                "map_node parent-input output binding requires parent-source forwarding mode");
                        }
                        break;
                }
            }

            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                if (binding.source_path.empty())
                {
                    throw std::invalid_argument("map_node child input binding requires a boundary argument ordinal");
                }
                if (binding.source_path[0] >= spec.args.size())
                {
                    throw std::invalid_argument("map_node child input binding source ordinal is out of range");
                }
                if (binding.target.node >= child_node_count)
                {
                    throw std::invalid_argument("map_node child input target node is out of range");
                }
            }
            if (!element_source_seen && !spec.multiplexed_inputs.empty())
            {
                throw std::invalid_argument("map_node requires one child argument sourced from the mapped TSD element");
            }

            if (key_source_seen)
            {
                if (spec.key_output_schema == nullptr)
                {
                    throw std::invalid_argument("map_node key argument requires a key output schema");
                }
                if (spec.key_output_schema->kind != TSTypeKind::TS ||
                    spec.key_output_schema->value_schema != mapped_key_type)
                {
                    throw std::invalid_argument("map_node key output schema must be TS<K> for the mapped key type");
                }
            }
            else if (spec.key_output_schema != nullptr)
            {
                throw std::invalid_argument("map_node key output schema was supplied but no key argument is bound");
            }
        }
    }  // namespace

    const void *MapNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    MapNodeView MapNodeView::from_node(NodeView view, const void *context)
    {
        static_cast<void>(context);
        const auto &typed_context = map_node_context(view);
        void       *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return MapNodeView{std::move(view), &typed_context, storage};
    }

    const NodeView &MapNodeView::node() const noexcept { return view_; }

    std::size_t MapNodeView::active_count() const noexcept
    {
        return MemoryUtils::cast<MapNodeStorage>(storage_)->active_count();
    }

    std::size_t MapNodeView::child_graph_count() const noexcept
    {
        return MemoryUtils::cast<MapNodeStorage>(storage_)->child_graph_count();
    }

    bool MapNodeView::child_graphs_use_in_place_storage() const noexcept
    {
        const auto &storage = *MemoryUtils::cast<MapNodeStorage>(storage_);
        for (std::size_t slot = 0; slot < storage.entries.slot_capacity(); ++slot)
        {
            const auto *entry = storage.entries.entry_at(slot);
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage()) { return false; }
        }
        for (std::size_t slot = 0; slot < storage.previous_entries.slot_capacity(); ++slot)
        {
            const auto *entry = storage.previous_entries.entry_at(slot);
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage()) { return false; }
        }
        return true;
    }

    MapNodeView::MapNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)),
          context_(context),
          storage_(storage)
    {
    }

    NodeBuilder map_node(NodeTypeMetaData meta, MapNodeSpec spec)
    {
        validate_map_node_spec(meta, spec);

        if (meta.scalar_schema != nullptr)
        {
            throw std::invalid_argument(
                "map_node reserves scalar configuration for its runtime context");
        }

        meta.requires_phase_runner =
            meta.requires_phase_runner || spec.child.graph_builder.requires_phase_runner();
        meta.node_kind = NodeKind::Nested;
        meta.scalar_schema = map_node_context_schema();
        meta.valid_inputs = std::vector<std::size_t>{};
        if (meta.output_schema != nullptr &&
            spec.output_binding_mode != MapOutputBindingMode::ChildTerminalWritesElement)
        {
            if (meta.output_schema->kind != TSTypeKind::TSD ||
                meta.output_schema->element_ts() == nullptr)
            {
                throw std::invalid_argument("map_node forwarding-element output requires a TSD output schema");
            }
            meta.output_endpoint_schema = TSEndpointSchema::non_peered_dict(
                meta.output_schema,
                TSEndpointSchema::peered(meta.output_schema->element_ts()));
        }

        const auto *keys_schema = TypeRegistry::instance().dereference(
            meta.input_schema->fields()[*spec.keys_input_index].type);
        const ValueTypeRef key_type = ValuePlanFactory::instance().type_for(
            keys_schema->value_schema->element_type);
        const GraphTypeRef child_graph_type = spec.child.graph_builder.nested_type();
        if (!key_type || !child_graph_type)
            throw std::logic_error("map_node could not resolve debugger child types");

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = map_storage_field_name,
            .plan = &MemoryUtils::plan_for<MapNodeStorage>(),
        }};
        // For output maps the field destroys before the owned TSD output: child
        // terminal forwarding outputs may hold links into it. Sink maps use the
        // same layout without an output component.
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, {}, fields);

        descriptor.callbacks.stop            = &map_node_stop;
        descriptor.ops.evaluate_impl         = &map_evaluate_impl;
        descriptor.ops.storage_metrics_impl  = &map_storage_metrics;
        descriptor.ops.extended_view_type_id = MapNodeView::node_view_type_id();
        const MemoryUtils::StorageLayout graph_layout = spec.child.graph_builder.nested_storage_layout();
        MapNodeStorage debug_exemplar;
        debug_exemplar.entries.bind_graph_layout(graph_layout);
        const std::size_t entries_offset = static_cast<std::size_t>(
            reinterpret_cast<const std::byte *>(&debug_exemplar.entries) -
            reinterpret_cast<const std::byte *>(&debug_exemplar));
        MapKeyEntry debug_entry{Value{key_type}};
        const std::size_t graph_pointer_offset = static_cast<std::size_t>(
            reinterpret_cast<const std::byte *>(&debug_entry.graph) -
            reinterpret_cast<const std::byte *>(&debug_entry)) + GraphValue::debug_pointer_offset();
        descriptor.dynamic_debug = NodeTypeDescriptor::DynamicDebug{
            .key_type = key_type.record(),
            .element_type = child_graph_type.record(),
            .layout = debug_exemplar.entries.debug_layout(
                descriptor.storage_plan->component(map_storage_field_name).offset + entries_offset,
                graph_pointer_offset, true),
        };
        MapNodeContextPtr context = make_map_node_context(
            std::move(spec),
            descriptor.storage_plan->component(map_storage_field_name).offset,
            graph_layout);
        descriptor.ops.extended_view_context = descriptor.storage_plan;

        static const std::byte runtime_type_id{};
        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), &runtime_type_id);
        builder.scalars(Value{std::move(context)});
        return builder;
    }

    NodeBuilder map_node_with_error_capture(const NodeBuilder &builder,
                                            const TSValueTypeMetaData *error_element_schema,
                                            ErrorCaptureOptions options)
    {
        if (error_element_schema == nullptr)
        {
            throw std::invalid_argument("map_node_with_error_capture requires an error element schema");
        }

        const NodeTypeRef type = builder.type();
        const NodeOps    &ops  = type.ops_ref();
        if (ops.extended_view_type_id != MapNodeView::node_view_type_id() ||
            ops.extended_view_context == nullptr)
        {
            throw std::invalid_argument("map_node_with_error_capture requires a TSD map node");
        }

        const ValueView scalar_context = builder.scalars().view();
        const auto &context = scalar_context.checked_as<MapNodeContextPtr>();
        if (!context)
        {
            throw std::logic_error(
                "map_node_with_error_capture requires a runtime context");
        }
        NodeTypeMetaData meta = *type.schema();
        meta.scalar_schema = nullptr;
        if (meta.output_schema == nullptr || meta.output_schema->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("map_node_with_error_capture requires a TSD map output");
        }
        meta.error_output_schema = TypeRegistry::instance().tsd(
            meta.output_schema->key_type(), error_element_schema);
        if (meta.captures_errors)
        {
            options.trace_back_depth =
                std::max(meta.error_capture.trace_back_depth, options.trace_back_depth);
            options.capture_values = meta.error_capture.capture_values || options.capture_values;
        }
        meta.captures_errors = true;
        meta.error_capture = options;

        NodeBuilder result = map_node(std::move(meta), context->spec);
        result.input_endpoint(builder.input_endpoint());
        if (!builder.output_endpoint().empty()) { result.output_endpoint(builder.output_endpoint()); }
        result.label(std::string{builder.label()});
        result.scalars(Value{builder.scalars()});
        return result;
    }
}  // namespace hgraph
