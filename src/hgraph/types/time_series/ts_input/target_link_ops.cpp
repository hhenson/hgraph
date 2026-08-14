#include "target_link_ops.h"
#include "../ts_data/ownership.h"
#include <hgraph/types/time_series/ts_data/impl/current_state_ops.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/ts_data_conversion.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#endif

#include <array>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph::detail
{
    struct TSInputTargetLinkSlotAccess
    {
        std::size_t (*size)(const TSDataView &target) = nullptr;
        std::size_t (*slot_capacity)(const TSDataView &target) = nullptr;
        bool (*slot_occupied)(const TSDataView &target, std::size_t slot) = nullptr;
        bool (*slot_live)(const TSDataView &target, std::size_t slot) = nullptr;
        bool (*slot_added)(const TSDataView &target, std::size_t slot) = nullptr;
        bool (*slot_removed)(const TSDataView &target, std::size_t slot) = nullptr;
        bool (*slot_published)(const TSDataView &target, std::size_t slot) = nullptr;
        const void *(*key_at_slot)(const TSDataView &target, std::size_t slot) = nullptr;
        bool (*contains)(const TSDataView &target, const ValueView &key) = nullptr;
        std::size_t (*find_slot)(const TSDataView &target, const ValueView &key) = nullptr;
    };

    struct TSInputTargetLinkIndexedAccess
    {
        std::size_t (*size)(const TSDataView &target) = nullptr;
        TSDataView (*child)(const TSDataView &target, std::size_t index) = nullptr;
    };

    namespace
    {
        [[nodiscard]] const TSDataOwnershipOps &target_link_ownership_ops() noexcept;

        template <typename Layout, typename Ops>
        struct TargetLinkContextFor final : TSInputTargetLinkContext
        {
            Layout layout{};
            Ops    ops{};
        };

        using TargetLinkBundleContext =
            TargetLinkContextFor<FixedTSBDataLayout, IndexedTSDataOps>;

        [[nodiscard]] std::size_t target_link_bundle_inspection_field_count(
            const void *context) noexcept
        {
            return static_cast<const TargetLinkBundleContext *>(context)->layout.fields.size();
        }

        [[nodiscard]] TSDataInspectionField target_link_bundle_inspection_field_at(
            const void *context, std::size_t index)
        {
            const auto *state = static_cast<const TargetLinkBundleContext *>(context);
            if (index >= state->layout.fields.size())
                throw std::out_of_range("target-link TSData inspection field index is out of range");
            const auto &field = state->layout.fields[index];
            return TSDataInspectionField{
                .name = state->schema->fields()[index].name,
                .data_offset = field.data_offset,
                .type = field.type,
            };
        }

        [[nodiscard]] const TSDataInspectionOps &target_link_bundle_inspection_ops() noexcept
        {
            static const TSDataInspectionOps ops{
                .field_count_impl = &target_link_bundle_inspection_field_count,
                .field_at_impl = &target_link_bundle_inspection_field_at,
            };
            return ops;
        }

        struct TargetLinkDictContext final : TSInputTargetLinkContext
        {
            TSDDataLayout dict_layout{};
            TSDDataOps    dict_ops{};
            TSSDataOps    key_set_ops{};
        };

        template <typename Context>
        [[nodiscard]] TSInputTargetLinkContextStorage make_target_link_context_storage()
        {
            const auto &context_plan = MemoryUtils::plan_for<Context>();
            return TSInputTargetLinkContextStorage::owning_constructed(
                context_plan, [](void *memory) {
                    std::construct_at(MemoryUtils::cast<Context>(memory));
                });
        }

        template <typename Context>
        [[nodiscard]] TSInputTargetLinkContextOwner finish_target_link_context(
            TSInputTargetLinkContextStorage storage, Context &context)
        {
            if (context.active_ops == nullptr)
            {
                throw std::logic_error("TSInput target-link context did not publish its operations result");
            }
            return TSInputTargetLinkContextOwner{
                std::move(storage), context, *context.active_ops};
        }

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] const TSDataLayout *target_link_layout(const void *context) noexcept
        {
            return static_cast<const TSInputTargetLinkContext *>(context)->active_layout;
        }

        [[nodiscard]] const TSDataTracking *target_link_tracking(const void *context, const void *memory) noexcept
        {
            return &target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] TSDataTracking *target_link_mutable_tracking(const void *context, void *memory) noexcept
        {
            return &target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory)->tracking;
        }

        [[nodiscard]] const TSDataTracking *target_link_key_set_tracking(const void *context,
                                                                         const void *memory)
        {
            return &target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory)
                        ->key_set_tracking();
        }

        [[nodiscard]] TSDataTracking *target_link_mutable_key_set_tracking(const void *context,
                                                                           void *memory)
        {
            return &target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory)
                        ->mutable_key_set_tracking();
        }

        [[nodiscard]] bool target_link_has_current_value(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.has_current_value();
        }

        [[nodiscard]] bool target_link_all_valid(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.all_valid();
        }

        [[nodiscard]] const void *target_link_value_memory(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.value().data();
        }

        [[nodiscard]] ValueView target_link_value_view(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.value() : ValueView{};
        }

        [[nodiscard]] const void *target_link_delta_memory(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            const auto  evaluation_time = link != nullptr ? link->tracking.last_modified_time : MIN_DT;
            return target.delta_value(evaluation_time).data();
        }

        [[nodiscard]] ValueView target_link_delta_view(const void *context,
                                                        const void *memory,
                                                        DateTime evaluation_time)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.valid() ? target.delta_value(evaluation_time) : ValueView{};
        }

        [[nodiscard]] TSDataView target_link_target_view(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            return link != nullptr ? link->target_view() : TSDataView{};
        }

        [[nodiscard]] const TSInputTargetLinkStorage *target_link_for(const void *context,
                                                                      const void *memory) noexcept
        {
            return target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
        }

        [[nodiscard]] bool target_link_dict_structural_delta_current(const void *context,
                                                                      const void *memory,
                                                                      DateTime evaluation_time)
        {
            const auto *link = target_link_for(context, memory);
            if (link != nullptr && link->structural_transition_active() &&
                link->structural_transition_time() == evaluation_time)
            {
                return true;
            }
            auto target = target_link_target_view(context, memory);
            return target.valid() && target.as_dict().structural_delta_current(evaluation_time);
        }

        [[nodiscard]] TSDataView target_link_previous_view(const void *context, const void *memory) noexcept
        {
            const auto *link = target_link_for(context, memory);
            return link != nullptr && link->structural_transition_active()
                       ? link->previous_target_view()
                       : TSDataView{};
        }

        [[nodiscard]] ValueView target_link_key_view(const TSInputTargetLinkContext &state,
                                                     const TSDataView &target,
                                                     std::size_t slot)
        {
            const auto *layout = static_cast<const TSSDataLayout *>(&target.layout());
            return ValueView{layout->key_binding, state.slot_access->key_at_slot(target, slot)};
        }

        [[nodiscard]] bool target_link_previous_slot_was_published(const void *context,
                                                                    const void *memory,
                                                                    std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            const auto *link = target_link_for(context, memory);
            const auto previous = target_link_previous_view(context, memory);
            if (link == nullptr || !previous.valid() || state->slot_access == nullptr ||
                !state->slot_access->slot_occupied(previous, slot))
            {
                return false;
            }

            const bool added_in_transition =
                previous.modified(link->structural_transition_time()) &&
                state->slot_access->slot_added(previous, slot);
            return state->slot_access->slot_published(previous, slot) && !added_in_transition;
        }

        [[nodiscard]] bool target_link_previous_contains_published(const void *context,
                                                                   const void *memory,
                                                                   const ValueView &key)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            const auto *link = target_link_for(context, memory);
            const auto previous = target_link_previous_view(context, memory);
            if (link == nullptr || !previous.valid() || state->slot_access == nullptr) { return false; }

            const auto capacity = state->slot_access->slot_capacity(previous);
            const auto live_slot = state->slot_access->find_slot(previous, key);
            if (live_slot < capacity)
            {
                return target_link_previous_slot_was_published(context, memory, live_slot);
            }
            if (!previous.modified(link->structural_transition_time())) { return false; }

            // find_slot deliberately exposes only live keys. A key removed
            // earlier in this transition can still have been published, so
            // fall back to the small per-cycle removed set for that case.
            for (std::size_t slot = 0; slot < capacity; ++slot)
            {
                if (state->slot_access->slot_removed(previous, slot) &&
                    target_link_previous_slot_was_published(context, memory, slot) &&
                    target_link_key_view(*state, previous, slot).equals(key))
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] std::size_t set_access_size(const TSDataView &target)
        {
            return target.as_set().size();
        }

        [[nodiscard]] std::size_t set_access_slot_capacity(const TSDataView &target)
        {
            return target.as_set().slot_capacity();
        }

        [[nodiscard]] bool set_access_slot_occupied(const TSDataView &target, std::size_t slot)
        {
            return target.as_set().slot_occupied(slot);
        }

        [[nodiscard]] bool set_access_slot_live(const TSDataView &target, std::size_t slot)
        {
            return target.as_set().slot_live(slot);
        }

        [[nodiscard]] bool set_access_slot_added(const TSDataView &target, std::size_t slot)
        {
            return target.as_set().slot_added(slot);
        }

        [[nodiscard]] bool set_access_slot_removed(const TSDataView &target, std::size_t slot)
        {
            return target.as_set().slot_removed(slot);
        }

        [[nodiscard]] bool set_access_slot_published(const TSDataView &target, std::size_t slot)
        {
            auto set = target.as_set();
            return set.slot_live(slot) || set.slot_removed(slot);
        }

        [[nodiscard]] const void *set_access_key_at_slot(const TSDataView &target, std::size_t slot)
        {
            return target.as_set().at_slot(slot).data();
        }

        [[nodiscard]] bool set_access_contains(const TSDataView &target, const ValueView &key)
        {
            return target.as_set().contains(key);
        }

        [[nodiscard]] std::size_t set_access_find_slot(const TSDataView &target, const ValueView &key)
        {
            return target.as_set().find_slot(key);
        }

        [[nodiscard]] std::size_t dict_access_size(const TSDataView &target)
        {
            return target.as_dict().size();
        }

        [[nodiscard]] std::size_t dict_access_slot_capacity(const TSDataView &target)
        {
            return target.as_dict().slot_capacity();
        }

        [[nodiscard]] bool dict_access_slot_occupied(const TSDataView &target, std::size_t slot)
        {
            return target.as_dict().slot_occupied(slot);
        }

        [[nodiscard]] bool dict_access_slot_live(const TSDataView &target, std::size_t slot)
        {
            return target.as_dict().slot_live(slot);
        }

        [[nodiscard]] bool dict_access_slot_added(const TSDataView &target, std::size_t slot)
        {
            return target.as_dict().slot_added(slot);
        }

        [[nodiscard]] bool dict_access_slot_removed(const TSDataView &target, std::size_t slot)
        {
            return target.as_dict().slot_removed(slot);
        }

        [[nodiscard]] bool dict_access_slot_published(const TSDataView &target, std::size_t slot)
        {
            auto dict = target.as_dict();
            return dict.slot_removed(slot) ||
                   (dict.slot_live(slot) && dict.at_slot(slot).has_current_value());
        }

        [[nodiscard]] const void *dict_access_key_at_slot(const TSDataView &target, std::size_t slot)
        {
            return target.as_dict().key_at_slot(slot).data();
        }

        [[nodiscard]] bool dict_access_contains(const TSDataView &target, const ValueView &key)
        {
            return target.as_dict().contains(key);
        }

        [[nodiscard]] std::size_t dict_access_find_slot(const TSDataView &target, const ValueView &key)
        {
            return target.as_dict().find_slot(key);
        }

        [[nodiscard]] std::size_t bundle_access_size(const TSDataView &target)
        {
            return target.as_bundle().size();
        }

        [[nodiscard]] TSDataView bundle_access_child(const TSDataView &target, std::size_t index)
        {
            auto bundle = target.as_bundle();
            return bundle.at(index);
        }

        [[nodiscard]] std::size_t list_access_size(const TSDataView &target)
        {
            return target.as_list().size();
        }

        [[nodiscard]] TSDataView list_access_child(const TSDataView &target, std::size_t index)
        {
            auto list = target.as_list();
            return list.at(index);
        }

        const TSInputTargetLinkSlotAccess target_link_set_access{
            .size = &set_access_size,
            .slot_capacity = &set_access_slot_capacity,
            .slot_occupied = &set_access_slot_occupied,
            .slot_live = &set_access_slot_live,
            .slot_added = &set_access_slot_added,
            .slot_removed = &set_access_slot_removed,
            .slot_published = &set_access_slot_published,
            .key_at_slot = &set_access_key_at_slot,
            .contains = &set_access_contains,
            .find_slot = &set_access_find_slot,
        };

        const TSInputTargetLinkSlotAccess target_link_dict_key_access{
            .size = &dict_access_size,
            .slot_capacity = &dict_access_slot_capacity,
            .slot_occupied = &dict_access_slot_occupied,
            .slot_live = &dict_access_slot_live,
            .slot_added = &dict_access_slot_added,
            .slot_removed = &dict_access_slot_removed,
            .slot_published = &dict_access_slot_published,
            .key_at_slot = &dict_access_key_at_slot,
            .contains = &dict_access_contains,
            .find_slot = &dict_access_find_slot,
        };

        const TSInputTargetLinkIndexedAccess target_link_bundle_access{
            .size = &bundle_access_size,
            .child = &bundle_access_child,
        };

        const TSInputTargetLinkIndexedAccess target_link_list_access{
            .size = &list_access_size,
            .child = &list_access_child,
        };

        [[nodiscard]] TSDDataView target_link_dict_view(const void *context, const void *memory)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { throw std::logic_error("TSInput target-link dict access requires a bound target"); }
            return target.as_dict();
        }

        [[nodiscard]] std::size_t target_link_slot_capacity_or_zero(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
                auto target = target_link_target_view(context, memory);
                return target.valid() && state->slot_access != nullptr ? state->slot_access->slot_capacity(target)
                                                                       : std::size_t{0};
            });
        }

        [[nodiscard]] Range<ValueView> target_link_empty_value_range() noexcept
        {
            return Range<ValueView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                    .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] Range<TSDataView> target_link_empty_ts_data_range() noexcept
        {
            return Range<TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                     .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_empty_ts_data_kv_range() noexcept
        {
            return KeyValueRange<ValueView, TSDataView>{.context = nullptr, .memory = nullptr, .limit = 0,
                                                        .predicate = nullptr, .projector = nullptr};
        }

        [[nodiscard]] std::size_t target_link_set_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
                auto target = target_link_target_view(context, memory);
                return target.valid() && state->slot_access != nullptr ? state->slot_access->size(target)
                                                                       : std::size_t{0};
            });
        }

        [[nodiscard]] std::size_t target_link_set_slot_capacity(const void *context, const void *memory) noexcept
        {
            return target_link_slot_capacity_or_zero(context, memory);
        }

        [[nodiscard]] bool target_link_set_slot_occupied(const void *context, const void *memory, std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            return target.valid() && state->slot_access != nullptr && state->slot_access->slot_occupied(target, slot);
        }

        [[nodiscard]] bool target_link_set_slot_live(const void *context, const void *memory, std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            return target.valid() && state->slot_access != nullptr && state->slot_access->slot_live(target, slot);
        }

        [[nodiscard]] bool target_link_set_slot_added(const void *context, const void *memory, std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            const auto *link = target_link_for(context, memory);
            if (target.valid() && link != nullptr && link->sampled_structural_transition() &&
                state->slot_access != nullptr && state->slot_access->slot_live(target, slot))
            {
                const auto key = target_link_key_view(*state, target, slot);
                return !target_link_previous_contains_published(context, memory, key);
            }
            return target.valid() && state->slot_access != nullptr && state->slot_access->slot_added(target, slot);
        }

        [[nodiscard]] bool target_link_set_slot_removed(const void *context, const void *memory, std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            const auto *link = target_link_for(context, memory);
            if (link != nullptr && link->structural_transition_active()) { return false; }
            return target.valid() && state->slot_access != nullptr && state->slot_access->slot_removed(target, slot);
        }

        [[nodiscard]] bool target_link_previous_slot_removed(const void *context,
                                                             const void *memory,
                                                             std::size_t slot);

        template <bool Added>
        [[nodiscard]] std::size_t target_link_set_next_delta_slot(const void *context,
                                                                  const void *memory,
                                                                  std::size_t previous)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            const auto *link = target_link_for(context, memory);
            auto target = target_link_target_view(context, memory);
            if (state->slot_access == nullptr) { return TS_DATA_NO_CHILD_ID; }

            const auto previous_view = target_link_previous_view(context, memory);
            const bool use_previous = !Added && link != nullptr && link->structural_transition_active() &&
                                      previous_view.valid();
            const auto &surface = use_previous ? previous_view : target;
            if (!surface.valid()) { return TS_DATA_NO_CHILD_ID; }

            const std::size_t capacity = state->slot_access->slot_capacity(surface);
            for (std::size_t slot = previous == TS_DATA_NO_CHILD_ID ? 0 : previous + 1;
                 slot < capacity; ++slot)
            {
                const bool selected = Added ? target_link_set_slot_added(context, memory, slot)
                                            : (use_previous
                                                   ? target_link_previous_slot_removed(context, memory, slot)
                                                   : target_link_set_slot_removed(context, memory, slot));
                if (selected) { return slot; }
            }
            return TS_DATA_NO_CHILD_ID;
        }

        [[nodiscard]] bool target_link_previous_slot_removed(const void *context,
                                                             const void *memory,
                                                             std::size_t slot)
        {
            if (!target_link_previous_slot_was_published(context, memory, slot)) { return false; }

            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            const auto previous = target_link_previous_view(context, memory);
            const auto current = target_link_target_view(context, memory);
            const auto key = target_link_key_view(*state, previous, slot);
            return !current.valid() || state->slot_access == nullptr ||
                   !state->slot_access->contains(current, key);
        }

        [[nodiscard]] const void *target_link_set_key_at_slot(const void *context,
                                                              const void *memory,
                                                              std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            if (!target.valid() || state->slot_access == nullptr)
            {
                throw std::logic_error("TSInput target-link set access requires a bound target");
            }
            return state->slot_access->key_at_slot(target, slot);
        }

        [[nodiscard]] bool target_link_set_contains(const void *context, const void *memory, const ValueView &key)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            return target.valid() && state->slot_access != nullptr && state->slot_access->contains(target, key);
        }

        [[nodiscard]] std::size_t target_link_set_find_slot(const void *context,
                                                            const void *memory,
                                                            const ValueView &key)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            return target.valid() && state->slot_access != nullptr ? state->slot_access->find_slot(target, key)
                                                                   : TS_DATA_NO_CHILD_ID;
        }

        [[nodiscard]] ValueView target_link_set_key_projector(const void *context,
                                                              const void *memory,
                                                              std::size_t slot)
        {
            const auto target = target_link_target_view(context, memory);
            const auto *layout = target.valid()
                                     ? static_cast<const TSSDataLayout *>(&target.layout())
                                     : nullptr;
            if (layout == nullptr)
            {
                throw std::logic_error("TSInput target-link key projection requires a bound target layout");
            }
            return ValueView{layout->key_binding, target_link_set_key_at_slot(context, memory, slot)};
        }

        [[nodiscard]] ValueView target_link_previous_key_projector(const void *context,
                                                                   const void *memory,
                                                                   std::size_t slot)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            return target_link_key_view(*state, target_link_previous_view(context, memory), slot);
        }

        [[nodiscard]] Range<ValueView> target_link_set_range(
            const void *context,
            const void *memory,
            Range<ValueView>::predicate_fn predicate)
        {
            if (target_link_target_view(context, memory).valid())
            {
                return Range<ValueView>{.context = context, .memory = memory,
                                        .limit = target_link_set_slot_capacity(context, memory),
                                        .predicate = predicate, .projector = &target_link_set_key_projector};
            }
            return target_link_empty_value_range();
        }

        [[nodiscard]] Range<ValueView> target_link_set_live_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_set_slot_live);
        }

        [[nodiscard]] Range<ValueView> target_link_set_added_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_set_slot_added);
        }

        [[nodiscard]] Range<ValueView> target_link_set_removed_range(const void *context, const void *memory)
        {
            const auto previous = target_link_previous_view(context, memory);
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            if (previous.valid() && state->slot_access != nullptr)
            {
                return Range<ValueView>{.context = context, .memory = memory,
                                        .limit = state->slot_access->slot_capacity(previous),
                                        .predicate = &target_link_previous_slot_removed,
                                        .projector = &target_link_previous_key_projector};
            }
            return target_link_set_range(context, memory, &target_link_set_slot_removed);
        }

        [[nodiscard]] SlotTSDataMutationResult target_link_set_insert_key(const void *context, void *memory,
                                                                          const ValueView &key,
                                                                          DateTime modified_time)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return {}; }

            auto set      = target.as_set();
            auto mutation = set.begin_mutation(modified_time);
            const bool changed = mutation.add(key);
            return SlotTSDataMutationResult{.slot = set.find_slot(key), .changed = changed};
        }

        [[nodiscard]] SlotTSDataMutationResult target_link_dict_insert_key(const void *context, void *memory,
                                                                           const ValueView &key,
                                                                           DateTime modified_time)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return {}; }

            auto dict = target.as_dict();
            const bool existed = dict.contains(key);
            auto mutation = dict.begin_mutation(modified_time);
            (void)mutation.at(key);
            return SlotTSDataMutationResult{.slot = dict.find_slot(key), .changed = !existed};
        }

        [[nodiscard]] SlotTSDataMutationResult target_link_set_remove_key(const void *context, void *memory,
                                                                          const ValueView &key,
                                                                          DateTime modified_time)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return {}; }

            auto set = target.as_set();
            const std::size_t slot = set.find_slot(key);
            auto mutation = set.begin_mutation(modified_time);
            const bool changed = mutation.remove(key);
            return SlotTSDataMutationResult{.slot = slot, .changed = changed};
        }

        [[nodiscard]] SlotTSDataMutationResult target_link_dict_remove_key(const void *context, void *memory,
                                                                           const ValueView &key,
                                                                           DateTime modified_time)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return {}; }

            auto dict = target.as_dict();
            const std::size_t slot = dict.find_slot(key);
            auto mutation = dict.begin_mutation(modified_time);
            const bool changed = mutation.erase(key);
            return SlotTSDataMutationResult{.slot = slot, .changed = changed};
        }

        [[nodiscard]] bool target_link_touch_slots(const void *context, void *memory, DateTime modified_time)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return false; }

            const auto &ops = static_cast<const TSSDataOps &>(target.ops());
            const bool touched = ops.touch_impl(ops.context, target.mutable_data(), modified_time);
            if (touched)
            {
                auto mutation = target.begin_mutation(modified_time);
                mutation.mark_modified();
            }
            return touched;
        }

        void target_link_reserve_slots(const void *context, void *memory, std::size_t capacity)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return; }

            const auto &ops = static_cast<const TSSDataOps &>(target.ops());
            ops.reserve_impl(ops.context, target.mutable_data(), capacity);
        }

        void target_link_subscribe_slot_observer(const void *context, void *memory, SlotObserver *observer)
        {
            auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            if (link == nullptr) { throw std::logic_error("Target-link slot observer requires live storage"); }
            link->add_slot_observer(observer);
        }

        void target_link_unsubscribe_slot_observer(const void *context, void *memory, SlotObserver *observer)
        {
            auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            if (link == nullptr) { throw std::logic_error("Target-link slot observer requires live storage"); }
            link->remove_slot_observer(observer);
        }

        [[nodiscard]] bool target_link_dict_slot_modified(const void *context,
                                                          const void *memory,
                                                          std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            const auto *link = target_link_for(context, memory);
            if (target.valid() && link != nullptr && link->sampled_structural_transition())
            {
                return target.as_dict().slot_live(slot);
            }
            return target.valid() && target.as_dict().slot_modified(slot);
        }

        [[nodiscard]] std::size_t target_link_dict_next_modified_slot(const void *context,
                                                                      const void *memory,
                                                                      std::size_t previous)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return TS_DATA_NO_CHILD_ID; }

            auto dict = target.as_dict();
            const auto *link = target_link_for(context, memory);
            if (link == nullptr || !link->sampled_structural_transition())
            {
                return dict.next_modified_slot(previous);
            }

            for (std::size_t slot = previous == TS_DATA_NO_CHILD_ID ? 0 : previous + 1;
                 slot < dict.slot_capacity(); ++slot)
            {
                if (dict.slot_live(slot)) { return slot; }
            }
            return TS_DATA_NO_CHILD_ID;
        }

        [[nodiscard]] const void *target_link_dict_child_at_slot(const void *context,
                                                                 const void *memory,
                                                                 std::size_t slot)
        {
            return target_link_dict_view(context, memory).at_slot(slot).data();
        }

        [[nodiscard]] TSRoleTypeRef target_link_dict_child_binding_at_slot(const void *context,
                                                                           const void *memory,
                                                                           std::size_t slot)
        {
            return target_link_dict_view(context, memory).at_slot(slot).storage_type();
        }

        [[nodiscard]] TSDataView target_link_dict_ts_projector(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
        {
            return target_link_dict_view(context, memory).at_slot(slot);
        }

        [[nodiscard]] TSDataView target_link_previous_dict_ts_projector(const void *context,
                                                                        const void *memory,
                                                                        std::size_t slot)
        {
            auto previous = target_link_previous_view(context, memory);
            return previous.as_dict().at_slot(slot);
        }

        [[nodiscard]] std::pair<ValueView, TSDataView> target_link_dict_kv_projector(const void *context,
                                                                                     const void *memory,
                                                                                     std::size_t slot)
        {
            return {target_link_set_key_projector(context, memory, slot),
                    target_link_dict_ts_projector(context, memory, slot)};
        }

        [[nodiscard]] std::pair<ValueView, TSDataView> target_link_previous_dict_kv_projector(
            const void *context,
            const void *memory,
            std::size_t slot)
        {
            return {target_link_previous_key_projector(context, memory, slot),
                    target_link_previous_dict_ts_projector(context, memory, slot)};
        }

        [[nodiscard]] bool target_link_dict_slot_valid(const void *context, const void *memory, std::size_t slot)
        {
            auto target = target_link_target_view(context, memory);
            return target.valid() && target.as_dict().slot_live(slot) && target.as_dict().at_slot(slot).valid();
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_ts_range(
            const void *context,
            const void *memory,
            Range<TSDataView>::predicate_fn predicate)
        {
            if (target_link_target_view(context, memory).valid())
            {
                return Range<TSDataView>{.context = context, .memory = memory,
                                         .limit = target_link_set_slot_capacity(context, memory),
                                         .predicate = predicate, .projector = &target_link_dict_ts_projector};
            }
            return target_link_empty_ts_data_range();
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_kv_range(
            const void *context,
            const void *memory,
            KeyValueRange<ValueView, TSDataView>::predicate_fn predicate)
        {
            if (target_link_target_view(context, memory).valid())
            {
                return KeyValueRange<ValueView, TSDataView>{.context = context, .memory = memory,
                                                            .limit = target_link_set_slot_capacity(context, memory),
                                                            .predicate = predicate,
                                                            .projector = &target_link_dict_kv_projector};
            }
            return target_link_empty_ts_data_kv_range();
        }

        [[nodiscard]] Range<ValueView> target_link_dict_valid_keys_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_dict_slot_valid);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_set_slot_live);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_valid_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_dict_slot_valid);
        }

        [[nodiscard]] Range<ValueView> target_link_dict_modified_keys_range(const void *context, const void *memory)
        {
            return target_link_set_range(context, memory, &target_link_dict_slot_modified);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_modified_values_range(const void *context,
                                                                               const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_dict_slot_modified);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_added_values_range(const void *context, const void *memory)
        {
            return target_link_dict_ts_range(context, memory, &target_link_set_slot_added);
        }

        [[nodiscard]] Range<TSDataView> target_link_dict_removed_values_range(const void *context, const void *memory)
        {
            const auto previous = target_link_previous_view(context, memory);
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            if (previous.valid() && state->slot_access != nullptr)
            {
                return Range<TSDataView>{.context = context, .memory = memory,
                                         .limit = state->slot_access->slot_capacity(previous),
                                         .predicate = &target_link_previous_slot_removed,
                                         .projector = &target_link_previous_dict_ts_projector};
            }
            return target_link_dict_ts_range(context, memory, &target_link_set_slot_removed);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_items_range(const void *context,
                                                                                        const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_set_slot_live);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_valid_items_range(const void *context,
                                                                                              const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_dict_slot_valid);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_modified_items_range(const void *context,
                                                                                                 const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_dict_slot_modified);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_added_items_range(const void *context,
                                                                                              const void *memory)
        {
            return target_link_dict_kv_range(context, memory, &target_link_set_slot_added);
        }

        [[nodiscard]] KeyValueRange<ValueView, TSDataView> target_link_dict_removed_items_range(const void *context,
                                                                                                const void *memory)
        {
            const auto previous = target_link_previous_view(context, memory);
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            if (previous.valid() && state->slot_access != nullptr)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context = context, .memory = memory,
                    .limit = state->slot_access->slot_capacity(previous),
                    .predicate = &target_link_previous_slot_removed,
                    .projector = &target_link_previous_dict_kv_projector};
            }
            return target_link_dict_kv_range(context, memory, &target_link_set_slot_removed);
        }

        [[nodiscard]] std::size_t target_link_indexed_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
                auto target = target_link_target_view(context, memory);
                return target.valid() && state->indexed_access != nullptr ? state->indexed_access->size(target)
                                                                          : std::size_t{0};
            });
        }

        [[nodiscard]] TSDataView target_link_indexed_child(const void *context,
                                                           const void *memory,
                                                           std::size_t index)
        {
            const auto *state = static_cast<const TSInputTargetLinkContext *>(context);
            auto target = target_link_target_view(context, memory);
            return target.valid() && state->indexed_access != nullptr ? state->indexed_access->child(target, index)
                                                                      : TSDataView{};
        }

        [[nodiscard]] TSRoleTypeRef target_link_indexed_element_binding(const void *context,
                                                                           const void *memory,
                                                                           std::size_t index) noexcept
        {
            return fallback_on_exception(TSRoleTypeRef{}, [&] {
                auto child = target_link_indexed_child(context, memory, index);
                return child.storage_type();
            });
        }

        [[nodiscard]] const void *target_link_indexed_element_memory(const void *context,
                                                                     const void *memory,
                                                                     std::size_t index) noexcept
        {
            return fallback_on_exception<const void *>(nullptr, [&] {
                return target_link_indexed_child(context, memory, index).data();
            });
        }

        [[nodiscard]] void *target_link_indexed_mutable_element_memory(const void *context,
                                                                       void *memory,
                                                                       std::size_t index) noexcept
        {
            return const_cast<void *>(target_link_indexed_element_memory(context, memory, index));
        }

        [[nodiscard]] TSWDataView target_link_window_view(const void *context, const void *memory)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { throw std::logic_error("TSInput target-link window access requires a bound target"); }
            return target.as_window();
        }

        [[nodiscard]] std::size_t target_link_window_size(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                return target.valid() ? target.as_window().size() : std::size_t{0};
            });
        }

        [[nodiscard]] const void *target_link_window_element_at(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
        {
            return target_link_window_view(context, memory).at(index).data();
        }

        [[nodiscard]] DateTime target_link_window_time_at(const void *context,
                                                               const void *memory,
                                                               std::size_t index)
        {
            return target_link_window_view(context, memory).time_at(index);
        }

        [[nodiscard]] const void *target_link_window_time_element_at(const void *context,
                                                                     const void *memory,
                                                                     std::size_t index)
        {
            return target_link_window_view(context, memory).time_value_at(index).data();
        }

        [[nodiscard]] std::size_t target_link_window_capacity(const void *context, const void *memory) noexcept
        {
            return fallback_on_exception(std::size_t{0}, [&] {
                auto target = target_link_target_view(context, memory);
                return target.valid() ? target.as_window().capacity() : std::size_t{0};
            });
        }

        [[nodiscard]] bool target_link_window_full(const void *context, const void *memory)
        {
            auto target = target_link_target_view(context, memory);
            return target.valid() && target.as_window().full();
        }

        void target_link_window_push(const void *, void *, const ValueView &, DateTime)
        {
            throw std::logic_error("TSInput target-link window mutation is not supported");
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] nb::object target_link_to_python(const void *context, const void *memory)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.value_to_python();
        }

        [[nodiscard]] nb::object target_link_delta_to_python(const void *context,
                                                             const void *memory,
                                                             DateTime evaluation_time)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            const auto  target = link != nullptr ? link->target_view() : TSDataView{};
            return target.delta_value_to_python(evaluation_time);
        }
#endif

        /**
         * Write-through: a value written to a bound link lands on the TARGET
         * output through its standard mutation path (modified tracking and
         * parent recording included). This is what lets a node whose output is
         * a forwarding endpoint (``map_`` child terminals re-homed onto the
         * parent's TSD elements) write externally-owned storage directly.
         */
        [[nodiscard]] bool target_link_copy_value_from(const void *context, void *memory, const ValueView &value,
                                                       DateTime modified_time)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            if (link == nullptr || !link->target_output().bound())
            {
                throw std::logic_error("TSInput target-link write-through requires a bound target output");
            }
            auto target_view = link->target_output().view(modified_time);
            auto mutation    = target_view.begin_mutation(modified_time);
            return mutation.copy_value_from(value);
        }

        [[nodiscard]] bool target_link_move_value_from(const void *context, void *memory, ValueView value,
                                                       DateTime modified_time)
        {
            const auto *link = target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), memory);
            if (link == nullptr || !link->target_output().bound())
            {
                throw std::logic_error("TSInput target-link write-through requires a bound target output");
            }
            auto target_view = link->target_output().view(modified_time);
            auto mutation    = target_view.begin_mutation(modified_time);
            return mutation.move_value_from(std::move(value));
        }

        /**
         * Delta write-through (the copy/move pattern extended): a delta
         * applied to a forwarding endpoint lands on the TARGET output through
         * its own ops - python map_ children apply canonical deltas to their
         * re-homed terminals.
         */
        [[nodiscard]] TSOutputView target_link_delta_target(const void *context, const TSOutputView &out)
        {
            const auto *link =
                target_link_storage_at(*static_cast<const TSInputTargetLinkContext *>(context), out.data_view().data());
            if (link == nullptr || !link->target_output().bound())
            {
                throw std::logic_error("TSInput target-link delta write-through requires a bound target output");
            }
            return link->target_output().view(out.evaluation_time());
        }

        [[nodiscard]] bool target_link_delta_has_effect_op(const TSOutputView &out, const ValueView &delta)
        {
            const auto &out_ops = out.data_view().ops();
            auto target = target_link_delta_target(out_ops.context, out);
            return target.data_view().ops().delta_has_effect_impl(target, delta);
        }

        void target_link_apply_delta_op(const TSOutputView &out, const ValueView &delta)
        {
            const auto &out_ops = out.data_view().ops();
            ::hgraph::apply_delta(target_link_delta_target(out_ops.context, out), delta);
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] TSRoleTypeRef target_link_canonical_data_type(
            TSRoleTypeRef type)
        {
            if (type.schema() == nullptr)
            {
                throw std::logic_error(
                    "target-link Python conversion requires a resolved schema");
            }
            return TSDataPlanFactory::instance()
                .data_type_for(type.schema())
                .as_role();
        }

        [[nodiscard]] const python_bridge::PythonTSDataOps &
        target_link_python_ops_for(TSRoleTypeRef type)
        {
            return *type.ops_ref().python_ops;
        }

        [[nodiscard]] bool target_link_requires_authored_delta(
            TSRoleTypeRef type, nanobind::handle source)
        {
            const auto canonical = target_link_canonical_data_type(type);
            return target_link_python_ops_for(canonical)
                .requires_authored_delta_impl(canonical, source);
        }

        [[nodiscard]] Value target_link_delta_from_python(
            TSRoleTypeRef type, nanobind::handle source, bool authored)
        {
            const auto canonical = target_link_canonical_data_type(type);
            return target_link_python_ops_for(canonical)
                .delta_from_python_impl(canonical, source, authored);
        }

        void target_link_apply_python_result(const TSOutputView &output,
                                             nanobind::handle result)
        {
            const auto &ops = output.data_view().ops();
            python_bridge::apply_python_result(
                target_link_delta_target(ops.context, output), result);
        }

        [[nodiscard]] const python_bridge::PythonTSDataOps &
        target_link_python_ts_data_ops() noexcept
        {
            static const python_bridge::PythonTSDataOps ops{
                .requires_authored_delta_impl =
                    &target_link_requires_authored_delta,
                .delta_from_python_impl = &target_link_delta_from_python,
                .apply_result_impl = &target_link_apply_python_result,
            };
            return ops;
        }
#endif

        /**
         * Child-modification notifications for children reached THROUGH a
         * link view: ``at_slot``/child projections stamp the accessing view
         * as the child's parent, so a child written via a forwarding link
         * carries the LINK as its parent. The link is a transparent alias -
         * the notification must land on the TARGET (slot/delta bits + the
         * target's own tracking and parent chain), exactly as if the child
         * had been reached through the target directly. The generic caller
         * (TSParentLink::notify_child_modified) separately records the
         * link's own tracking and continues the LINK's chain.
         */
        void target_link_record_child_modified(const void *context, void *memory, std::size_t child_id,
                                               DateTime modified_time)
        {
            auto target = target_link_target_view(context, memory);
            if (!target.valid()) { return; }
            const auto &ops = target.ops();
            ops.record_child_modified_impl(ops.context, const_cast<void *>(target.data()), child_id, modified_time);
            auto *state = ops.mutable_tracking_impl(ops.context, const_cast<void *>(target.data()));
            if (state != nullptr && state->record_modified(modified_time))
            {
                state->parent.notify_child_modified(modified_time);
            }
        }

        [[nodiscard]] TSDataOps target_link_base_ops(TSInputTargetLinkContext &context)
        {
            const auto capture_delta = [&]() -> Value (*)(const TSInputView &) {
                switch (context.schema->kind)
                {
                case TSTypeKind::SIGNAL: return &ts_data_detail::capture_delta_signal;
                case TSTypeKind::TSW: return &ts_data_detail::capture_delta_tsw;
                case TSTypeKind::TSS: return &ts_data_detail::capture_delta_tss;
                case TSTypeKind::TSD: return &ts_data_detail::capture_delta_tsd;
                case TSTypeKind::TSL: return &ts_data_detail::capture_delta_tsl;
                case TSTypeKind::TSB: return &ts_data_detail::capture_delta_tsb;
                case TSTypeKind::TS:
                case TSTypeKind::REF: return &ts_data_detail::capture_delta_ts;
                }
                return &ts_data_detail::capture_delta_ts;
            }();
            return TSDataOps{
                .context                   = &context,
                .kind                      = context.schema->kind,
                .allows_mutation           = true,
                .ownership_ops             = &target_link_ownership_ops(),
                .current_state_ops =
                    &ts_current_state_detail::current_state_ops_for(context.schema->kind),
                .layout_impl               = &target_link_layout,
                .tracking_impl             = &target_link_tracking,
                .mutable_tracking_impl     = &target_link_mutable_tracking,
                .has_current_value_impl    = &target_link_has_current_value,
                .all_valid_impl            = &target_link_all_valid,
                .value_view_impl           = &target_link_value_view,
                .delta_view_impl           = &target_link_delta_view,
                .value_memory_impl         = &target_link_value_memory,
                .delta_memory_impl         = &target_link_delta_memory,
                .record_child_modified_impl = &target_link_record_child_modified,
                .copy_value_from_impl      = &target_link_copy_value_from,
                .move_value_from_impl      = &target_link_move_value_from,
                .capture_delta_impl        = capture_delta,
                .delta_has_effect_impl     = &target_link_delta_has_effect_op,
                .apply_delta_impl          = &target_link_apply_delta_op,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .python_ops               = &target_link_python_ts_data_ops(),
                .to_python_impl            = &target_link_to_python,
                .delta_to_python_impl      = &target_link_delta_to_python,
#endif
            };
        }

        void configure_target_link_set_ops(
            TSSDataOps &ops,
            SlotTSDataMutationResult (*insert_key)(const void *, void *, const ValueView &, DateTime),
            SlotTSDataMutationResult (*remove_key)(const void *, void *, const ValueView &, DateTime))
        {
            ops.size_impl                      = &target_link_set_size;
            ops.slot_capacity_impl             = &target_link_set_slot_capacity;
            ops.slot_occupied_impl             = &target_link_set_slot_occupied;
            ops.slot_live_impl                 = &target_link_set_slot_live;
            ops.slot_added_impl                = &target_link_set_slot_added;
            ops.slot_removed_impl              = &target_link_set_slot_removed;
            ops.next_added_slot_impl           = &target_link_set_next_delta_slot<true>;
            ops.next_removed_slot_impl         = &target_link_set_next_delta_slot<false>;
            ops.key_at_slot_impl               = &target_link_set_key_at_slot;
            ops.contains_impl                  = &target_link_set_contains;
            ops.find_slot_impl                 = &target_link_set_find_slot;
            ops.make_values_range_impl         = &target_link_set_live_range;
            ops.make_added_values_range_impl   = &target_link_set_added_range;
            ops.make_removed_values_range_impl = &target_link_set_removed_range;
            ops.insert_key_impl                = insert_key;
            ops.remove_key_impl                = remove_key;
            ops.touch_impl                     = &target_link_touch_slots;
            ops.reserve_impl                   = &target_link_reserve_slots;
            ops.subscribe_slot_observer_impl   = &target_link_subscribe_slot_observer;
            ops.unsubscribe_slot_observer_impl = &target_link_unsubscribe_slot_observer;
        }

        template <typename Context>
        void initialise_target_link_context(Context &context,
                                            const TSValueTypeMetaData &schema,
                                            std::size_t storage_offset)
        {
            context.schema = &schema;
            context.storage_offset = storage_offset;
            context.storage_access = &target_link_storage_access_for(schema.kind);
        }

        [[nodiscard]] TSInputTargetLinkContextOwner
        make_base_target_link_context(const TSValueTypeMetaData &schema,
                                      const MemoryUtils::StoragePlan &,
                                      std::size_t storage_offset,
                                      const TSDataLayout &regular_layout)
        {
            using Context = TargetLinkContextFor<TSDataLayout, TSDataOps>;
            auto storage = make_target_link_context_storage<Context>();
            auto *context = storage.as<Context>();
            initialise_target_link_context(*context, schema, storage_offset);
            context->layout = regular_layout;
            context->layout.tracking_offset = storage_offset;
            context->ops = target_link_base_ops(*context);
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return finish_target_link_context(std::move(storage), *context);
        }

        [[nodiscard]] TSInputTargetLinkContextOwner
        make_set_target_link_context(const TSValueTypeMetaData &schema,
                                     const MemoryUtils::StoragePlan &,
                                     std::size_t storage_offset,
                                     const TSDataLayout &regular_layout)
        {
            using Context = TargetLinkContextFor<TSSDataLayout, TSSDataOps>;
            auto storage = make_target_link_context_storage<Context>();
            auto *context = storage.as<Context>();
            initialise_target_link_context(*context, schema, storage_offset);
            context->layout = static_cast<const TSSDataLayout &>(regular_layout);
            context->layout.tracking_offset = storage_offset;
            context->ops = TSSDataOps{};
            static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
            static_cast<TSDataOps &>(context->ops).clear_collection_impl = &ts_data_detail::clear_tss_collection;
            configure_target_link_set_ops(context->ops, &target_link_set_insert_key, &target_link_set_remove_key);
            context->slot_access = &target_link_set_access;
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return finish_target_link_context(std::move(storage), *context);
        }

        [[nodiscard]] TSInputTargetLinkContextOwner
        make_dict_target_link_context(const TSValueTypeMetaData &schema,
                                      const MemoryUtils::StoragePlan &root_plan,
                                      std::size_t storage_offset,
                                      const TSDataLayout &regular_layout)
        {
            auto storage = make_target_link_context_storage<TargetLinkDictContext>();
            auto *context = storage.as<TargetLinkDictContext>();
            initialise_target_link_context(*context, schema, storage_offset);
            context->slot_access = &target_link_dict_key_access;

            context->dict_layout = static_cast<const TSDDataLayout &>(regular_layout);
            context->dict_layout.tracking_offset = storage_offset;

            context->dict_ops = TSDDataOps{};
            static_cast<TSDataOps &>(context->dict_ops) = target_link_base_ops(*context);
            static_cast<TSDataOps &>(context->dict_ops).clear_collection_impl = &ts_data_detail::clear_tsd_collection;
            configure_target_link_set_ops(context->dict_ops, &target_link_dict_insert_key,
                                          &target_link_dict_remove_key);
            context->dict_ops.child_binding_at_slot_impl = &target_link_dict_child_binding_at_slot;
            context->dict_ops.structural_delta_current_impl = &target_link_dict_structural_delta_current;
            context->dict_ops.child_at_slot_impl = &target_link_dict_child_at_slot;
            context->dict_ops.slot_modified_impl = &target_link_dict_slot_modified;
            context->dict_ops.next_modified_slot_impl = &target_link_dict_next_modified_slot;
            context->dict_ops.make_ts_values_range_impl = &target_link_dict_values_range;
            context->dict_ops.make_valid_keys_range_impl = &target_link_dict_valid_keys_range;
            context->dict_ops.make_valid_ts_values_range_impl = &target_link_dict_valid_values_range;
            context->dict_ops.make_modified_keys_range_impl = &target_link_dict_modified_keys_range;
            context->dict_ops.make_modified_ts_values_range_impl = &target_link_dict_modified_values_range;
            context->dict_ops.make_added_ts_values_range_impl = &target_link_dict_added_values_range;
            context->dict_ops.make_removed_ts_values_range_impl = &target_link_dict_removed_values_range;
            context->dict_ops.make_ts_kv_range_impl = &target_link_dict_items_range;
            context->dict_ops.make_valid_ts_kv_range_impl = &target_link_dict_valid_items_range;
            context->dict_ops.make_modified_ts_kv_range_impl = &target_link_dict_modified_items_range;
            context->dict_ops.make_added_ts_kv_range_impl = &target_link_dict_added_items_range;
            context->dict_ops.make_removed_ts_kv_range_impl = &target_link_dict_removed_items_range;

            context->key_set_ops = TSSDataOps{};
            TSDataOps key_set_base_ops = target_link_base_ops(*context);
            key_set_base_ops.kind = TSTypeKind::TSS;
            key_set_base_ops.tracking_impl = &target_link_key_set_tracking;
            key_set_base_ops.mutable_tracking_impl = &target_link_mutable_key_set_tracking;
            key_set_base_ops.clear_collection_impl = &ts_data_detail::clear_tss_collection;
            static_cast<TSDataOps &>(context->key_set_ops) = key_set_base_ops;
            configure_target_link_set_ops(context->key_set_ops, &target_link_dict_insert_key,
                                          &target_link_dict_remove_key);
            const auto *key_set_schema = TypeRegistry::instance().tss(schema.key_type());
            if (key_set_schema == nullptr)
            {
                throw std::logic_error("TSInput target-link TSD key-set schema is not resolved");
            }
            context->dict_layout.key_set_type = TSRoleTypeRef{intern_ts_type(
                *key_set_schema, TypeRole::Input, root_plan, context->key_set_ops,
                std::string_view{"ts.tsd.key-set.input"})};

            context->active_layout = &context->dict_layout;
            context->active_ops = &context->dict_ops;
            return finish_target_link_context(std::move(storage), *context);
        }

        [[nodiscard]] TSInputTargetLinkContextOwner
        make_indexed_target_link_context(const TSValueTypeMetaData &schema,
                                         const MemoryUtils::StoragePlan &,
                                         std::size_t storage_offset,
                                         const TSDataLayout &regular_layout)
        {
            if (schema.kind == TSTypeKind::TSB)
            {
                using Context = TargetLinkBundleContext;
                auto storage = make_target_link_context_storage<Context>();
                auto *context = storage.as<Context>();
                initialise_target_link_context(*context, schema, storage_offset);
                context->layout = static_cast<const FixedTSBDataLayout &>(regular_layout);
                context->layout.tracking_offset = storage_offset;
                context->ops = IndexedTSDataOps{};
                static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
                static_cast<TSDataOps &>(context->ops).inspection_ops =
                    &target_link_bundle_inspection_ops();
                static_cast<TSDataOps &>(context->ops).indexed_child_count_impl = &target_link_indexed_size;
                static_cast<TSDataOps &>(context->ops).indexed_child_binding_impl = &target_link_indexed_element_binding;
                static_cast<TSDataOps &>(context->ops).indexed_child_memory_impl = &target_link_indexed_element_memory;
                static_cast<TSDataOps &>(context->ops).mutable_indexed_child_memory_impl =
                    &target_link_indexed_mutable_element_memory;
                context->ops.size_impl = &target_link_indexed_size;
                context->ops.element_binding_impl = &target_link_indexed_element_binding;
                context->ops.element_memory_impl = &target_link_indexed_element_memory;
                context->ops.mutable_element_memory_impl = &target_link_indexed_mutable_element_memory;
                context->indexed_access = &target_link_bundle_access;
                context->active_layout = &context->layout;
                context->active_ops = &context->ops;
                return finish_target_link_context(std::move(storage), *context);
            }
            using Context = TargetLinkContextFor<TSDataLayout, IndexedTSDataOps>;
            auto storage = make_target_link_context_storage<Context>();
            auto *context = storage.as<Context>();
            initialise_target_link_context(*context, schema, storage_offset);
            context->layout = regular_layout;
            context->layout.tracking_offset = storage_offset;
            context->ops = IndexedTSDataOps{};
            static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
            static_cast<TSDataOps &>(context->ops).indexed_child_count_impl = &target_link_indexed_size;
            static_cast<TSDataOps &>(context->ops).indexed_child_binding_impl = &target_link_indexed_element_binding;
            static_cast<TSDataOps &>(context->ops).indexed_child_memory_impl = &target_link_indexed_element_memory;
            static_cast<TSDataOps &>(context->ops).mutable_indexed_child_memory_impl =
                &target_link_indexed_mutable_element_memory;
            static_cast<TSDataOps &>(context->ops).indexed_child_growth =
                schema.kind == TSTypeKind::TSL && schema.fixed_size() == 0;
            context->ops.size_impl = &target_link_indexed_size;
            context->ops.element_binding_impl = &target_link_indexed_element_binding;
            context->ops.element_memory_impl = &target_link_indexed_element_memory;
            context->ops.mutable_element_memory_impl = &target_link_indexed_mutable_element_memory;
            context->indexed_access = schema.kind == TSTypeKind::TSB ? &target_link_bundle_access
                                                                     : &target_link_list_access;
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return finish_target_link_context(std::move(storage), *context);
        }

        [[nodiscard]] TSInputTargetLinkContextOwner
        make_window_target_link_context(const TSValueTypeMetaData &schema,
                                        const MemoryUtils::StoragePlan &,
                                        std::size_t storage_offset,
                                        const TSDataLayout &regular_layout)
        {
            if (schema.is_duration_based())
            {
                using Context = TargetLinkContextFor<TimeTSWDataLayout, TSWDataOps>;
                auto storage = make_target_link_context_storage<Context>();
                auto *context = storage.as<Context>();
                initialise_target_link_context(*context, schema, storage_offset);
                context->layout = static_cast<const TimeTSWDataLayout &>(regular_layout);
                context->layout.tracking_offset = storage_offset;
                context->ops = TSWDataOps{};
                static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
                context->ops.size_impl = &target_link_window_size;
                context->ops.element_at_impl = &target_link_window_element_at;
                context->ops.time_at_impl = &target_link_window_time_at;
                context->ops.time_element_at_impl = &target_link_window_time_element_at;
                context->ops.capacity_impl = &target_link_window_capacity;
                context->ops.full_impl = &target_link_window_full;
                context->ops.push_impl = &target_link_window_push;
                context->active_layout = &context->layout;
                context->active_ops = &context->ops;
                return finish_target_link_context(std::move(storage), *context);
            }

            using Context = TargetLinkContextFor<SizeTSWDataLayout, TSWDataOps>;
            auto storage = make_target_link_context_storage<Context>();
            auto *context = storage.as<Context>();
            initialise_target_link_context(*context, schema, storage_offset);
            context->layout = static_cast<const SizeTSWDataLayout &>(regular_layout);
            context->layout.tracking_offset = storage_offset;
            context->ops = TSWDataOps{};
            static_cast<TSDataOps &>(context->ops) = target_link_base_ops(*context);
            context->ops.size_impl = &target_link_window_size;
            context->ops.element_at_impl = &target_link_window_element_at;
            context->ops.time_at_impl = &target_link_window_time_at;
            context->ops.time_element_at_impl = &target_link_window_time_element_at;
            context->ops.capacity_impl = &target_link_window_capacity;
            context->ops.full_impl = &target_link_window_full;
            context->ops.push_impl = &target_link_window_push;
            context->active_layout = &context->layout;
            context->active_ops = &context->ops;
            return finish_target_link_context(std::move(storage), *context);
        }
    }  // namespace

    const TSInputTargetLinkStorage *target_link_storage_at(const TSInputTargetLinkContext &context,
                                                           const void *memory) noexcept
    {
        return context.storage_access->get_const(advance(memory, context.storage_offset));
    }

    TSInputTargetLinkStorage *target_link_storage_at(const TSInputTargetLinkContext &context,
                                                     void *memory) noexcept
    {
        return context.storage_access->get_mutable(advance(memory, context.storage_offset));
    }

    namespace
    {
        [[nodiscard]] const TSDataOwnershipOps &target_link_ownership_ops() noexcept
        {
            static const TSDataOwnershipOps ops{
                .child_count = [](const void *, const void *) noexcept { return std::size_t{0}; },
                .child_at = [](const void *, void *, std::size_t) noexcept { return TSDataOwnedChild{}; },
                .stop = [](const void *context, void *memory) noexcept {
                    const auto *target_context = static_cast<const TSInputTargetLinkContext *>(context);
                    if (target_context == nullptr || memory == nullptr) { return; }
                    if (auto *link = target_link_storage_at(*target_context, memory); link != nullptr)
                        link->unbind_noexcept();
                },
                .auxiliary_dynamic_storage = [](const void *context,
                                                const void *memory) noexcept {
                    const auto *target_context =
                        static_cast<const TSInputTargetLinkContext *>(context);
                    if (target_context == nullptr || memory == nullptr)
                        return DynamicStorageMetrics{};
                    const auto *link = target_link_storage_at(*target_context, memory);
                    return link != nullptr ? link->dynamic_storage_metrics()
                                           : DynamicStorageMetrics{};
                },
            };
            return ops;
        }
    }  // namespace

    const TSInputTargetLinkContext *target_link_context_for_ops(const TSDataOps *ops) noexcept
    {
        return ops != nullptr && ops->ownership_ops == &target_link_ownership_ops()
                   ? static_cast<const TSInputTargetLinkContext *>(ops->context)
                   : nullptr;
    }

    const TSInputTargetLinkContextBuilder &target_link_context_builder_for(TSTypeKind kind)
    {
        static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
        static const std::array<TSInputTargetLinkContextBuilder, kind_count> table{
            &make_base_target_link_context,
            &make_set_target_link_context,
            &make_dict_target_link_context,
            &make_indexed_target_link_context,
            &make_window_target_link_context,
            &make_indexed_target_link_context,
            &make_base_target_link_context,
            &make_base_target_link_context,
        };

        const auto index = ts_kind_index(kind);
        if (index >= table.size()) { return table.front(); }
        return table[index];
    }
}  // namespace hgraph::detail
