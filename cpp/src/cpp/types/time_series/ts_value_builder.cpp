#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/active_trie.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] size_t align_up(size_t value, size_t alignment) noexcept
        {
            const size_t mask = alignment - 1;
            return (value + mask) & ~mask;
        }

        [[nodiscard]] TimeSeriesStateParentPtr root_parent() noexcept
        {
            return {};
        }

        template <typename TInit>
        [[nodiscard]] std::unique_ptr<TimeSeriesStateV> make_state_node(TInit &&init)
        {
            auto state = std::make_unique<TimeSeriesStateV>();
            std::forward<TInit>(init)(*state);
            return state;
        }

        struct RawViewAccess : View
        {
            using View::data_of;
        };

        struct RawMapAccess : MapView
        {
            explicit RawMapAccess(const View &view) : MapView(view) {}

            using View::data;
            using View::data_of;
            using MapView::map_dispatch;
        };

        template <typename TState>
        void initialize_base_state(TState &state,
                                   TimeSeriesStateParentPtr parent,
                                   size_t index,
                                   engine_time_t modified_time = MIN_DT,
                                   TSStorageKind storage_kind = TSStorageKind::Native) noexcept
        {
            state.parent = parent;
            state.index = index;
            state.last_modified_time = modified_time;
            state.storage_kind = storage_kind;
            state.subscribers.clear();
        }

        template <typename TCollectionState>
        void initialize_collection_state(TCollectionState &state,
                                         TimeSeriesStateParentPtr parent,
                                         size_t index,
                                         engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time);
            state.reset_child_states();
            state.child_states.clear();
            state.modified_children.clear();
        }

        void initialize_collection_state(TSDState &state,
                                         TimeSeriesStateParentPtr parent,
                                         size_t index,
                                         engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time);
            state.reset_child_states();
            state.child_states.clear();
            state.modified_children.clear();
            state.active_tries.clear();
            state.element_schema = nullptr;
            state.map_dispatch = nullptr;
            state.map_value_data = nullptr;
            state.slot_observer_registered = false;
        }

        void initialize_ref_state(RefLinkState &state,
                                  TimeSeriesStateParentPtr parent,
                                  size_t index,
                                  engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time);
            state.source.clear();
            initialize_base_state(state.bound_link, static_cast<TSOutput *>(nullptr), 0, MIN_DT);
            state.bound_link.target.clear();
            state.bound_link.scheduling_notifier.set_target(nullptr);
            state.storage_kind = TSStorageKind::RefLink;
            state.bound_link.storage_kind = TSStorageKind::TargetLink;
        }

        [[nodiscard]] TimeSeriesStatePtr state_ptr(TimeSeriesStateV &state) noexcept
        {
            return std::visit([](auto &typed_state) -> TimeSeriesStatePtr { return &typed_state; }, state);
        }

        [[nodiscard]] TimeSeriesStateV *state_value(const std::unique_ptr<TimeSeriesStateV> &slot) noexcept
        {
            return slot.get();
        }

        [[nodiscard]] const TimeSeriesStateV *const_state_value(const std::unique_ptr<TimeSeriesStateV> &slot) noexcept
        {
            return slot.get();
        }

        [[nodiscard]] BaseState *state_address(const TimeSeriesStatePtr &state) noexcept
        {
            return std::visit([](auto *typed_state) -> BaseState * { return typed_state; }, state);
        }

        [[nodiscard]] BaseState *state_address(const std::unique_ptr<TimeSeriesStateV> &slot) noexcept
        {
            TimeSeriesStateV *state = state_value(slot);
            return state != nullptr ? state_address(state_ptr(*state)) : nullptr;
        }

        void sync_native_child_state(const TSViewContext &parent_context,
                                     const std::unique_ptr<TimeSeriesStateV> &slot,
                                     bool child_has_value,
                                     bool child_modified) noexcept
        {
            if (!child_has_value) { return; }

            BaseState *child_state = state_address(slot);
            if (child_state == nullptr || child_state->storage_kind != TSStorageKind::Native) { return; }

            BaseState *parent_state = parent_context.ts_state;
            if (parent_state == nullptr) { return; }

            const engine_time_t parent_modified_time = parent_state->last_modified_time;
            if (parent_modified_time == MIN_DT) { return; }

            if (child_modified) { child_state->last_modified_time = parent_modified_time; }
        }

        [[nodiscard]] bool list_slot_modified(const ListDeltaView &delta, size_t index) noexcept
        {
            for (const size_t updated_index : delta.updated_indices()) {
                if (updated_index == index) { return true; }
            }
            for (const size_t added_index : delta.added_indices()) {
                if (added_index == index) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool bundle_slot_modified(const BundleDeltaView &delta, size_t index) noexcept
        {
            for (const size_t updated_index : delta.updated_indices()) {
                if (updated_index == index) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool dict_slot_modified(const MapDeltaView &delta, size_t index) noexcept
        {
            return index < delta.slot_capacity() && (delta.slot_added(index) || delta.slot_updated(index));
        }

        [[nodiscard]] TSViewContext linked_child_context(const LinkedTSContext &target,
                                                         BaseState *local_state,
                                                         const TSMeta &schema,
                                                         const detail::ViewDispatch &value_dispatch,
                                                         const detail::TSDispatch &ts_dispatch) noexcept
        {
            return TSViewContext{
                target.schema != nullptr ? target.schema : &schema,
                target.value_dispatch != nullptr ? target.value_dispatch : &value_dispatch,
                target.ts_dispatch != nullptr ? target.ts_dispatch : &ts_dispatch,
                target.value_data,
                local_state,
            };
        }

        [[nodiscard]] TSViewContext child_context_from_slot(const std::unique_ptr<TimeSeriesStateV> &slot,
                                                            const TSMeta &schema,
                                                            const detail::ViewDispatch &value_dispatch,
                                                            const detail::TSDispatch &ts_dispatch,
                                                            void *native_value_data) noexcept
        {
            BaseState *local_state = state_address(slot);
            if (local_state != nullptr) {
                if (schema.kind == TSKind::TSD && local_state->storage_kind == TSStorageKind::Native && native_value_data != nullptr) {
                    static_cast<TSDState *>(local_state)->bind_value_storage(
                        *schema.element_ts(),
                        static_cast<const detail::MapViewDispatch &>(value_dispatch),
                        native_value_data);
                }
                if (const LinkedTSContext *target = local_state->linked_target(); target != nullptr) {
                    return linked_child_context(*target, local_state, schema, value_dispatch, ts_dispatch);
                }
            }

            return TSViewContext{
                &schema,
                &value_dispatch,
                &ts_dispatch,
                native_value_data,
                local_state,
            };
        }

        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSLState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSDState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSBState &state) noexcept { return &state; }

        template <typename TCollectionState>
        void ensure_child_slot_capacity(TCollectionState &state, size_t slot_count)
        {
            if (state.child_states.size() < slot_count) { state.child_states.resize(slot_count); }
        }

        void ensure_child_slot_capacity(TSDState &state, size_t slot_count)
        {
            ensure_child_slot_capacity(static_cast<BaseCollectionState &>(state), slot_count);
        }

        void initialize_state_tree(TimeSeriesStateV &state, const TSMeta &schema, TimeSeriesStateParentPtr parent, size_t index);

        template <typename TCollectionState>
        void install_child_state(TCollectionState &collection_state, size_t slot, std::unique_ptr<TimeSeriesStateV> child_state)
        {
            ensure_child_slot_capacity(collection_state, slot + 1);
            collection_state.child_states[slot] = std::move(child_state);
        }

        template <typename TCollectionState>
        void ensure_child_state(TCollectionState &collection_state, size_t slot, const TSMeta &child_schema)
        {
            ensure_child_slot_capacity(collection_state, slot + 1);
            if (state_value(collection_state.child_states[slot]) != nullptr) { return; }

            auto child_state = make_state_node([&](TimeSeriesStateV &state) {
                initialize_state_tree(state, child_schema, parent_ptr(collection_state), slot);
            });
            install_child_state(collection_state, slot, std::move(child_state));
        }

        void initialize_state_tree(TimeSeriesStateV &state, const TSMeta &schema, TimeSeriesStateParentPtr parent, size_t index)
        {
            switch (schema.kind) {
                case TSKind::TSValue:
                    initialize_base_state(state.emplace<TSState>(), parent, index);
                    break;

                case TSKind::TSS:
                    initialize_base_state(state.emplace<TSSState>(), parent, index);
                    break;

                case TSKind::TSD:
                    initialize_collection_state(state.emplace<TSDState>(), parent, index);
                    break;

                case TSKind::TSL:
                    {
                        auto &list_state = state.emplace<TSLState>();
                        initialize_collection_state(list_state, parent, index);

                        if (schema.fixed_size() > 0) { ensure_child_slot_capacity(list_state, schema.fixed_size()); }
                        break;
                    }

                case TSKind::TSW:
                    initialize_base_state(state.emplace<TSWState>(), parent, index);
                    break;

                case TSKind::TSB:
                    {
                        auto &bundle_state = state.emplace<TSBState>();
                        initialize_collection_state(bundle_state, parent, index);
                        ensure_child_slot_capacity(bundle_state, schema.field_count());
                        break;
                    }

                case TSKind::REF:
                    // Native REF storage is still a leaf carrying a
                    // TimeSeriesReference scalar payload. RefLinkState is
                    // reserved for TSOutput alternatives that dereference
                    // REF -> TS.
                    initialize_base_state(state.emplace<TSState>(), parent, index);
                    break;

                case TSKind::SIGNAL:
                    initialize_base_state(state.emplace<SignalState>(), parent, index);
                    break;
            }
        }

        [[nodiscard]] TSViewContext child_context_for_dict_slot(TSDState &state, size_t slot)
        {
            if (state.element_schema == nullptr || state.map_dispatch == nullptr || state.map_value_data == nullptr ||
                slot >= state.child_states.size() || state.child_states[slot] == nullptr) {
                return TSViewContext::none();
            }

            const auto &child_builder = TSValueBuilderFactory::checked_builder_for(state.element_schema);
            return child_context_from_slot(state.child_states[slot],
                                           *state.element_schema,
                                           child_builder.value_builder().dispatch(),
                                           child_builder.ts_dispatch(),
                                           state.map_dispatch->value_data(state.map_value_data, slot));
        }

        void replay_active_subtree(const TSViewContext &context, ActiveTrieNode *trie_node, Notifiable *notifier, bool subscribe)
        {
            if (!context.is_bound() || context.ts_state == nullptr || trie_node == nullptr || notifier == nullptr) { return; }

            if (trie_node->locally_active) {
                if (subscribe) {
                    context.ts_state->subscribe(notifier);
                } else {
                    context.ts_state->unsubscribe(notifier);
                }
            }

            if (trie_node->children.empty()) { return; }

            const TSViewContext resolved = context.resolved();
            const auto *collection = resolved.ts_dispatch != nullptr ? resolved.ts_dispatch->as_collection() : nullptr;
            if (collection == nullptr) { return; }

            for (const auto &[child_slot, child_trie] : trie_node->children) {
                if (!child_trie) { continue; }

                const TSViewContext child = collection->child_at(context, child_slot);
                if (!child.is_bound()) { continue; }
                replay_active_subtree(child, child_trie.get(), notifier, subscribe);
            }
        }

        void unbind_dynamic_dict_states_recursive(TimeSeriesStateV &state)
        {
            std::visit(
                [&](auto &typed_state) {
                    using T = std::remove_cvref_t<decltype(typed_state)>;

                    if constexpr (std::same_as<T, TSDState>) {
                        typed_state.unbind_value_storage();
                        for (auto &child : typed_state.child_states) {
                            if (child != nullptr) { unbind_dynamic_dict_states_recursive(*child); }
                        }
                    } else if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSBState>) {
                        for (auto &child : typed_state.child_states) {
                            if (child != nullptr) { unbind_dynamic_dict_states_recursive(*child); }
                        }
                    }
                },
                state);
        }

        [[nodiscard]] const TSMeta &binding_schema_for_state(const TSMeta &schema, const TimeSeriesStateV &state) noexcept
        {
            if (schema.kind != TSKind::REF || schema.element_ts() == nullptr) { return schema; }

            if (std::holds_alternative<TSDState>(state) || std::holds_alternative<TSLState>(state) ||
                std::holds_alternative<TSBState>(state)) {
                return *schema.element_ts();
            }

            return schema;
        }

        void bind_dynamic_dict_states_recursive(const TSMeta &schema, TimeSeriesStateV &state, void *value_data)
        {
            if (value_data == nullptr) { return; }

            const TSMeta &binding_schema = binding_schema_for_state(schema, state);
            const auto &builder = TSValueBuilderFactory::checked_builder_for(binding_schema);
            View root_view{&builder.value_builder().dispatch(), value_data, binding_schema.value_type};

            std::visit(
                [&](auto &typed_state) {
                    using T = std::remove_cvref_t<decltype(typed_state)>;

                    if constexpr (std::same_as<T, TSDState>) {
                        typed_state.bind_value_storage(*binding_schema.element_ts(),
                                                       static_cast<const detail::MapViewDispatch &>(builder.value_builder().dispatch()),
                                                       value_data);

                        MapDeltaView delta = root_view.as_map().delta();
                        for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
                            if (!detail::dict_slot_is_live(delta, slot) || slot >= typed_state.child_states.size() ||
                                typed_state.child_states[slot] == nullptr) {
                                continue;
                            }
                            bind_dynamic_dict_states_recursive(*binding_schema.element_ts(),
                                                               *typed_state.child_states[slot],
                                                               RawViewAccess::data_of(delta.value_at_slot(slot)));
                        }
                    } else if constexpr (std::same_as<T, TSBState>) {
                        auto bundle = root_view.as_bundle();
                        for (size_t index = 0;
                             index < typed_state.child_states.size() && index < binding_schema.field_count();
                             ++index) {
                            if (typed_state.child_states[index] == nullptr) { continue; }
                            bind_dynamic_dict_states_recursive(*binding_schema.fields()[index].ts_type,
                                                               *typed_state.child_states[index],
                                                               RawViewAccess::data_of(bundle.at(index)));
                        }
                    } else if constexpr (std::same_as<T, TSLState>) {
                        auto list = root_view.as_list();
                        const size_t limit = std::min(typed_state.child_states.size(), list.size());
                        for (size_t index = 0; index < limit; ++index) {
                            if (typed_state.child_states[index] == nullptr || binding_schema.element_ts() == nullptr) { continue; }
                            bind_dynamic_dict_states_recursive(*binding_schema.element_ts(),
                                                               *typed_state.child_states[index],
                                                               RawViewAccess::data_of(list.at(index)));
                        }
                    }
                },
                state);
        }

        void clone_state_tree(TimeSeriesStateV &dst, const TimeSeriesStateV &src, const TSMeta &schema, TimeSeriesStateParentPtr parent, size_t index)
        {
            if (const auto *src_state = std::get_if<TargetLinkState>(&src)) {
                auto &dst_state = dst.emplace<TargetLinkState>();
                initialize_base_state(dst_state, parent, index, src_state->last_modified_time, TSStorageKind::TargetLink);
                dst_state.target.clear();
                dst_state.scheduling_notifier.set_target(src_state->scheduling_notifier.get_target());
                if (src_state->target.is_bound()) { dst_state.set_target(src_state->target); }
                return;
            }

            if (const auto *src_state = std::get_if<RefLinkState>(&src)) {
                auto &dst_state = dst.emplace<RefLinkState>();
                initialize_ref_state(dst_state, parent, index, src_state->last_modified_time);
                if (src_state->source.is_bound()) { dst_state.set_source(src_state->source); }
                dst_state.bound_link.scheduling_notifier.set_target(src_state->bound_link.scheduling_notifier.get_target());
                dst_state.bound_link.last_modified_time = src_state->bound_link.last_modified_time;
                return;
            }

            switch (schema.kind) {
                case TSKind::TSValue:
                    {
                        const auto &src_state = std::get<TSState>(src);
                        initialize_base_state(dst.emplace<TSState>(), parent, index, src_state.last_modified_time);
                        break;
                    }

                case TSKind::TSS:
                    {
                        const auto &src_state = std::get<TSSState>(src);
                        auto &dst_state = dst.emplace<TSSState>();
                        initialize_base_state(dst_state, parent, index, src_state.last_modified_time);
                        dst_state.added_items = src_state.added_items;
                        dst_state.removed_items = src_state.removed_items;
                        break;
                    }

                case TSKind::TSD:
                    {
                        const auto &src_state = std::get<TSDState>(src);
                        auto &dst_state = dst.emplace<TSDState>();
                        initialize_collection_state(dst_state, parent, index, src_state.last_modified_time);
                        dst_state.modified_children = src_state.modified_children;
                        ensure_child_slot_capacity(dst_state, src_state.child_states.size());

                        if (schema.element_ts() != nullptr) {
                            for (size_t child_index = 0; child_index < src_state.child_states.size(); ++child_index) {
                                const TimeSeriesStateV *src_child = const_state_value(src_state.child_states[child_index]);
                                if (src_child == nullptr) { continue; }

                                auto child_state = make_state_node([&](TimeSeriesStateV &state) {
                                    clone_state_tree(state, *src_child, *schema.element_ts(), parent_ptr(dst_state), child_index);
                                });
                                install_child_state(dst_state, child_index, std::move(child_state));
                            }
                        }
                        break;
                    }

                case TSKind::TSL:
                    {
                        const auto &src_state = std::get<TSLState>(src);
                        auto &dst_state = dst.emplace<TSLState>();
                        initialize_collection_state(dst_state, parent, index, src_state.last_modified_time);
                        dst_state.modified_children = src_state.modified_children;
                        ensure_child_slot_capacity(dst_state, src_state.child_states.size());

                        if (schema.element_ts() != nullptr) {
                            for (size_t child_index = 0; child_index < src_state.child_states.size(); ++child_index) {
                                const TimeSeriesStateV *src_child = const_state_value(src_state.child_states[child_index]);
                                if (src_child == nullptr) { continue; }

                                auto child_state = make_state_node([&](TimeSeriesStateV &state) {
                                    clone_state_tree(state, *src_child, *schema.element_ts(), parent_ptr(dst_state), child_index);
                                });
                                install_child_state(dst_state, child_index, std::move(child_state));
                            }
                        }
                        break;
                    }

                case TSKind::TSW:
                    {
                        const auto &src_state = std::get<TSWState>(src);
                        initialize_base_state(dst.emplace<TSWState>(), parent, index, src_state.last_modified_time);
                        break;
                    }

                case TSKind::TSB:
                    {
                        const auto &src_state = std::get<TSBState>(src);
                        auto &dst_state = dst.emplace<TSBState>();
                        initialize_collection_state(dst_state, parent, index, src_state.last_modified_time);
                        dst_state.modified_children = src_state.modified_children;
                        ensure_child_slot_capacity(dst_state, src_state.child_states.size());

                        for (size_t child_index = 0; child_index < src_state.child_states.size(); ++child_index) {
                            const TimeSeriesStateV *src_child = const_state_value(src_state.child_states[child_index]);
                            if (src_child == nullptr) { continue; }

                            auto child_state = make_state_node([&](TimeSeriesStateV &state) {
                                clone_state_tree(state, *src_child, *schema.fields()[child_index].ts_type, parent_ptr(dst_state),
                                                 child_index);
                            });
                            install_child_state(dst_state, child_index, std::move(child_state));
                        }
                        break;
                    }

                case TSKind::REF:
                    {
                        const auto &src_state = std::get<TSState>(src);
                        initialize_base_state(dst.emplace<TSState>(), parent, index, src_state.last_modified_time);
                        break;
                    }

                case TSKind::SIGNAL:
                    {
                        const auto &src_state = std::get<SignalState>(src);
                        initialize_base_state(dst.emplace<SignalState>(), parent, index, src_state.last_modified_time);
                        break;
                    }
            }
        }

        [[nodiscard]] BaseState *resolved_collection_state(TSViewContext context) noexcept
        {
            BaseState *state = context.ts_state;
            return state != nullptr ? state->resolved_state() : nullptr;
        }

        [[nodiscard]] TSLState *list_state(TSViewContext context) noexcept
        {
            return static_cast<TSLState *>(resolved_collection_state(context));
        }

        [[nodiscard]] TSBState *bundle_state(TSViewContext context) noexcept
        {
            return static_cast<TSBState *>(resolved_collection_state(context));
        }

        [[nodiscard]] TSDState *dict_state(TSViewContext context) noexcept
        {
            return static_cast<TSDState *>(resolved_collection_state(context));
        }

        [[nodiscard]] bool child_modified(const BaseCollectionState *state, size_t index) noexcept
        {
            return state != nullptr && state->modified_children.contains(index);
        }

        struct LeafTSDispatch : detail::TSDispatch {};

        struct ListTSDispatch : detail::TSCollectionDispatch
        {
            ListTSDispatch(const TSMeta &element_schema,
                           const detail::ViewDispatch &element_value_dispatch,
                           const detail::TSDispatch &element_ts_dispatch) noexcept
                : m_element_schema(element_schema),
                  m_element_value_dispatch(element_value_dispatch),
                  m_element_ts_dispatch(element_ts_dispatch)
            {
            }

            [[nodiscard]] size_t size(const TSViewContext &context) const noexcept override
            {
                return context.value().as_list().size();
            }

            [[nodiscard]] View delta_value(const TSViewContext &context) const noexcept override
            {
                return context.value().as_list().delta();
            }

            [[nodiscard]] bool child_modified(const TSViewContext &context, size_t index) const noexcept override
            {
                if (context.ts_state != nullptr && context.ts_state->storage_kind == TSStorageKind::Native) {
                    const auto *state = list_state(context);
                    if (hgraph::child_modified(state, index)) { return true; }
                }

                const auto delta = context.value().as_list().delta();
                return list_slot_modified(delta, index);
            }

            [[nodiscard]] bool all_valid(const TSViewContext &context) const noexcept override
            {
                ListView list = context.value().as_list();
                TSLState *state = list_state(context);
                if (state == nullptr) { return false; }

                for (size_t index = 0; index < list.size(); ++index) {
                    ensure_child_state(*state, index, m_element_schema.get());
                    View child_value = list.at(index);
                    sync_native_child_state(
                        context,
                        state->child_states[index],
                        child_value.has_value(),
                        child_modified(context, index));
                    TSViewContext child = child_context_from_slot(state->child_states[index],
                                                                  m_element_schema.get(),
                                                                  m_element_value_dispatch.get(),
                                                                  m_element_ts_dispatch.get(),
                                                                  RawViewAccess::data_of(child_value));
                    if (!m_element_ts_dispatch.get().all_valid(child)) { return false; }
                }

                return true;
            }

            [[nodiscard]] TSViewContext child_at(const TSViewContext &context, size_t index) const override
            {
                ListView list = context.value().as_list();
                if (index >= list.size()) { return TSViewContext::none(); }

                TSLState *state = list_state(context);
                if (state == nullptr) { return TSViewContext::none(); }
                ensure_child_state(*state, index, m_element_schema.get());

                View child_value = list.at(index);
                sync_native_child_state(
                    context,
                    state->child_states[index],
                    child_value.has_value(),
                    child_modified(context, index));
                TSViewContext child = child_context_from_slot(state->child_states[index],
                                                              m_element_schema.get(),
                                                              m_element_value_dispatch.get(),
                                                              m_element_ts_dispatch.get(),
                                                              RawViewAccess::data_of(child_value));
                return child;
            }

          private:
            std::reference_wrapper<const TSMeta>               m_element_schema;
            std::reference_wrapper<const detail::ViewDispatch> m_element_value_dispatch;
            std::reference_wrapper<const detail::TSDispatch>   m_element_ts_dispatch;
        };

        struct BundleTSDispatch : detail::TSFieldDispatch
        {
            struct FieldLayout
            {
                std::string_view                               name;
                std::reference_wrapper<const TSMeta>           schema;
                std::reference_wrapper<const detail::ViewDispatch> value_dispatch;
                std::reference_wrapper<const detail::TSDispatch>   ts_dispatch;
            };

            explicit BundleTSDispatch(std::vector<FieldLayout> fields) : m_fields(std::move(fields)) {}

            [[nodiscard]] size_t size(const TSViewContext &context) const noexcept override
            {
                static_cast<void>(context);
                return m_fields.size();
            }

            [[nodiscard]] View delta_value(const TSViewContext &context) const noexcept override
            {
                return context.value().as_bundle().delta();
            }

            [[nodiscard]] bool child_modified(const TSViewContext &context, size_t index) const noexcept override
            {
                if (context.ts_state != nullptr && context.ts_state->storage_kind == TSStorageKind::Native) {
                    const auto *state = bundle_state(context);
                    if (hgraph::child_modified(state, index)) { return true; }
                }

                const auto delta = context.value().as_bundle().delta();
                return bundle_slot_modified(delta, index);
            }

            [[nodiscard]] bool all_valid(const TSViewContext &context) const noexcept override
            {
                TSBState *state = bundle_state(context);
                if (state == nullptr) { return false; }

                for (size_t index = 0; index < m_fields.size(); ++index) {
                    ensure_child_state(*state, index, m_fields[index].schema.get());
                    View child_value = context.value().as_bundle().at(index);
                    sync_native_child_state(
                        context,
                        state->child_states[index],
                        child_value.has_value(),
                        child_modified(context, index));
                    TSViewContext child = child_context_from_slot(state->child_states[index],
                                                                  m_fields[index].schema.get(),
                                                                  m_fields[index].value_dispatch.get(),
                                                                  m_fields[index].ts_dispatch.get(),
                                                                  RawViewAccess::data_of(child_value));
                    if (!m_fields[index].ts_dispatch.get().all_valid(child)) { return false; }
                }

                return true;
            }

            [[nodiscard]] TSViewContext child_at(const TSViewContext &context, size_t index) const override
            {
                if (index >= m_fields.size()) { return TSViewContext::none(); }

                TSBState *state = bundle_state(context);
                if (state == nullptr) { return TSViewContext::none(); }
                ensure_child_state(*state, index, m_fields[index].schema.get());

                View child_value = context.value().as_bundle().at(index);
                sync_native_child_state(
                    context,
                    state->child_states[index],
                    child_value.has_value(),
                    child_modified(context, index));
                TSViewContext child = child_context_from_slot(state->child_states[index],
                                                              m_fields[index].schema.get(),
                                                              m_fields[index].value_dispatch.get(),
                                                              m_fields[index].ts_dispatch.get(),
                                                              RawViewAccess::data_of(child_value));
                return child;
            }

            [[nodiscard]] TSViewContext child_field(const TSViewContext &context, std::string_view name) const override
            {
                for (size_t index = 0; index < m_fields.size(); ++index) {
                    if (m_fields[index].name == name) { return child_at(context, index); }
                }
                return TSViewContext::none();
            }

            [[nodiscard]] std::string_view child_name(size_t index) const noexcept override
            {
                return index < m_fields.size() ? m_fields[index].name : std::string_view{};
            }

          private:
            std::vector<FieldLayout> m_fields;
        };

        struct DictTSDispatch : detail::TSKeyDispatch
        {
            DictTSDispatch(const TSMeta &value_schema,
                           const detail::ViewDispatch &value_dispatch,
                           const detail::TSDispatch &value_ts_dispatch) noexcept
                : m_value_schema(value_schema),
                  m_value_dispatch(value_dispatch),
                  m_value_ts_dispatch(value_ts_dispatch)
            {
            }

            [[nodiscard]] size_t size(const TSViewContext &context) const noexcept override
            {
                return context.value().as_map().size();
            }

            [[nodiscard]] View delta_value(const TSViewContext &context) const noexcept override
            {
                return context.value().as_map().delta();
            }

            [[nodiscard]] bool child_modified(const TSViewContext &context, size_t index) const noexcept override
            {
                if (context.ts_state != nullptr && context.ts_state->storage_kind == TSStorageKind::Native) {
                    const auto *state = dict_state(context);
                    if (hgraph::child_modified(state, index)) { return true; }
                }

                const auto delta = context.value().as_map().delta();
                return dict_slot_modified(delta, index);
            }

            [[nodiscard]] bool all_valid(const TSViewContext &context) const noexcept override
            {
                MapDeltaView delta = context.value().as_map().delta();
                for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
                    if (!detail::dict_slot_is_live(delta, slot)) { continue; }
                    if (!m_value_ts_dispatch.get().all_valid(child_at_slot(context, delta, slot))) { return false; }
                }

                return true;
            }

            [[nodiscard]] TSViewContext child_at(const TSViewContext &context, size_t index) const override
            {
                MapDeltaView delta = context.value().as_map().delta();
                if (index >= delta.slot_capacity() || !delta.slot_occupied(index)) { return TSViewContext::none(); }
                return child_at_slot(context, delta, index);
            }

            [[nodiscard]] TSViewContext child_key(const TSViewContext &context, const View &key) const override
            {
                RawMapAccess map{context.value()};
                const auto *dispatch = map.map_dispatch();
                const size_t slot = dispatch->find(map.data(), RawMapAccess::data_of(key));
                if (slot == static_cast<size_t>(-1)) { return TSViewContext::none(); }

                MapDeltaView delta = map.delta();
                if (!detail::dict_slot_is_live(delta, slot)) { return TSViewContext::none(); }
                return child_at_slot(context, delta, slot);
            }

            [[nodiscard]] size_t iteration_limit(const TSViewContext &context) const noexcept override
            {
                return context.value().as_map().delta().slot_capacity();
            }

            [[nodiscard]] bool slot_is_live(const TSViewContext &context, size_t slot) const noexcept override
            {
                MapDeltaView delta = context.value().as_map().delta();
                return slot < delta.slot_capacity() && detail::dict_slot_is_live(delta, slot);
            }

            [[nodiscard]] View key_at_slot(const TSViewContext &context, size_t slot) const override
            {
                MapDeltaView delta = context.value().as_map().delta();
                return slot < delta.slot_capacity() ? delta.key_at_slot(slot)
                                                    : View::invalid_for(context.value().as_map().key_schema());
            }

          private:
            [[nodiscard]] TSViewContext child_at_slot(const TSViewContext &context,
                                                      const MapDeltaView &delta,
                                                      size_t slot) const
            {
                TSDState *state = dict_state(context);
                if (state == nullptr) { return TSViewContext::none(); }
                if (state->child_states.size() <= slot || state->child_states[slot] == nullptr) { return TSViewContext::none(); }

                const View child_value = delta.value_at_slot(slot);
                sync_native_child_state(
                    context,
                    state->child_states[slot],
                    child_value.has_value(),
                    child_modified(context, slot));
                TSViewContext child = child_context_from_slot(state->child_states[slot],
                                                              m_value_schema.get(),
                                                              m_value_dispatch.get(),
                                                              m_value_ts_dispatch.get(),
                                                              RawViewAccess::data_of(child_value));
                return child;
            }

            std::reference_wrapper<const TSMeta>               m_value_schema;
            std::reference_wrapper<const detail::ViewDispatch> m_value_dispatch;
            std::reference_wrapper<const detail::TSDispatch>   m_value_ts_dispatch;
        };

        struct SetTSDispatch final : detail::TSSetDispatch
        {
            [[nodiscard]] View delta_value(const TSViewContext &context) const noexcept override
            {
                return context.value().as_set().delta();
            }

            [[nodiscard]] size_t size(const TSViewContext &context) const noexcept override
            {
                return context.value().as_set().size();
            }

            [[nodiscard]] bool empty(const TSViewContext &context) const noexcept override
            {
                return context.value().as_set().empty();
            }

            [[nodiscard]] Range<View> values(const TSViewContext &context) const noexcept override
            {
                return Range<View>{&context,
                                   context.value().as_set().delta().slot_capacity(),
                                   [](const void *opaque, size_t slot) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       const auto delta = context.value().as_set().delta();
                                       return delta.slot_occupied(slot) && !delta.slot_removed(slot);
                                   },
                                   [](const void *opaque, size_t slot) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       return context.value().as_set().delta().at_slot(slot);
                                   }};
            }

            [[nodiscard]] Range<View> added_values(const TSViewContext &context) const noexcept override
            {
                return Range<View>{&context,
                                   context.value().as_set().delta().slot_capacity(),
                                   [](const void *opaque, size_t slot) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       const auto delta = context.value().as_set().delta();
                                       return delta.slot_occupied(slot) && delta.slot_added(slot);
                                   },
                                   [](const void *opaque, size_t slot) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       return context.value().as_set().delta().at_slot(slot);
                                   }};
            }

            [[nodiscard]] Range<View> removed_values(const TSViewContext &context) const noexcept override
            {
                return Range<View>{&context,
                                   context.value().as_set().delta().slot_capacity(),
                                   [](const void *opaque, size_t slot) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       const auto delta = context.value().as_set().delta();
                                       return delta.slot_occupied(slot) && delta.slot_removed(slot);
                                   },
                                   [](const void *opaque, size_t slot) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       return context.value().as_set().delta().at_slot(slot);
                                   }};
            }
        };

        struct WindowTSDispatch final : detail::TSWindowDispatch
        {
            [[nodiscard]] size_t size(const TSViewContext &context) const noexcept override
            {
                return context.value().as_cyclic_buffer().size();
            }

            [[nodiscard]] Range<View> values(const TSViewContext &context) const noexcept override
            {
                return Range<View>{&context, size(context), nullptr,
                                   [](const void *opaque, size_t index) {
                                       const auto &context = *static_cast<const TSViewContext *>(opaque);
                                       return context.value().as_cyclic_buffer().at(index);
                                   }};
            }

            [[nodiscard]] Range<engine_time_t> value_times(const TSViewContext &) const noexcept override
            {
                return Range<engine_time_t>{nullptr, 0, nullptr, nullptr};
            }
        };

        [[nodiscard]] const detail::TSDispatch &checked_dispatch_for(const TSMeta &schema)
        {
            static std::unordered_map<const TSMeta *, std::unique_ptr<detail::TSDispatch>> cache;
            static std::recursive_mutex mutex;

            std::lock_guard lock(mutex);
            if (const auto it = cache.find(&schema); it != cache.end()) { return *it->second; }

            std::unique_ptr<detail::TSDispatch> dispatch;
            switch (schema.kind) {
                case TSKind::TSValue:
                case TSKind::REF:
                case TSKind::SIGNAL:
                    dispatch = std::make_unique<LeafTSDispatch>();
                    break;

                case TSKind::TSS:
                    dispatch = std::make_unique<SetTSDispatch>();
                    break;

                case TSKind::TSW:
                    dispatch = std::make_unique<WindowTSDispatch>();
                    break;

                case TSKind::TSL:
                    {
                        const TSMeta *element_schema = schema.element_ts();
                        if (element_schema == nullptr || element_schema->value_type == nullptr) {
                            throw std::invalid_argument("TSL dispatch requires an element schema with a value type");
                        }

                        dispatch = std::make_unique<ListTSDispatch>(
                            *element_schema,
                            ValueBuilderFactory::checked_builder_for(element_schema->value_type, MutationTracking::Delta).dispatch(),
                            checked_dispatch_for(*element_schema));
                        break;
                    }

                case TSKind::TSB:
                    {
                        std::vector<BundleTSDispatch::FieldLayout> fields;
                        fields.reserve(schema.field_count());

                        for (size_t index = 0; index < schema.field_count(); ++index) {
                            const TSMeta *field_schema = schema.fields()[index].ts_type;
                            if (field_schema == nullptr || field_schema->value_type == nullptr) {
                                throw std::invalid_argument("TSB dispatch requires field schemas with value types");
                            }

                            fields.push_back(BundleTSDispatch::FieldLayout{
                                .name = schema.fields()[index].name,
                                .schema = std::cref(*field_schema),
                                .value_dispatch = std::cref(
                                    ValueBuilderFactory::checked_builder_for(field_schema->value_type, MutationTracking::Delta).dispatch()),
                                .ts_dispatch = std::cref(checked_dispatch_for(*field_schema)),
                            });
                        }

                        dispatch = std::make_unique<BundleTSDispatch>(std::move(fields));
                        break;
                    }

                case TSKind::TSD:
                    {
                        const TSMeta *value_schema = schema.element_ts();
                        if (value_schema == nullptr || value_schema->value_type == nullptr) {
                            throw std::invalid_argument("TSD dispatch requires a value schema with a value type");
                        }

                        dispatch = std::make_unique<DictTSDispatch>(
                            *value_schema,
                            ValueBuilderFactory::checked_builder_for(value_schema->value_type, MutationTracking::Delta).dispatch(),
                            checked_dispatch_for(*value_schema));
                        break;
                    }
            }

            const auto [it, inserted] = cache.emplace(&schema, std::move(dispatch));
            static_cast<void>(inserted);
            return *it->second;
        }

        struct RootTSStateOps : detail::TSBuilderOps
        {
            [[nodiscard]] detail::TSBuilderLayout layout(const TSMeta &schema,
                                                         const ValueBuilder &value_builder) const noexcept override
            {
                static_cast<void>(schema);

                const size_t value_offset = 0;
                const size_t value_size = value_builder.size();
                const size_t value_alignment = value_builder.alignment();
                const size_t ts_alignment = alignof(TimeSeriesStateV);
                const size_t ts_offset = align_up(value_size, ts_alignment);
                const size_t total_size = ts_offset + sizeof(TimeSeriesStateV);
                const size_t total_alignment = std::max(value_alignment, ts_alignment);
                return detail::TSBuilderLayout{
                    .value_offset = value_offset,
                    .ts_offset = ts_offset,
                    .size = total_size,
                    .alignment = total_alignment,
                };
            }

            void construct(void *memory, const TSMeta &schema) const override
            {
                auto *state = new (memory) TimeSeriesStateV();
                initialize_state_tree(*state, schema, root_parent(), 0);
            }

            void destruct(void *memory) const noexcept override
            {
                std::destroy_at(static_cast<TimeSeriesStateV *>(memory));
            }

            void copy_construct(void *dst, const void *src, const TSMeta &schema) const override
            {
                auto *dst_state = new (dst) TimeSeriesStateV();
                clone_state_tree(*dst_state, *static_cast<const TimeSeriesStateV *>(src), schema, root_parent(), 0);
            }

            void move_construct(void *dst, void *src, const TSMeta &schema) const override
            {
                auto *dst_state = new (dst) TimeSeriesStateV();
                clone_state_tree(*dst_state, *static_cast<const TimeSeriesStateV *>(src), schema, root_parent(), 0);
            }
        };

        [[nodiscard]] const detail::TSBuilderOps &root_builder_ops() noexcept
        {
            static RootTSStateOps ops;
            return ops;
        }
    }  // namespace

    std::unique_ptr<TimeSeriesStateV> make_time_series_state_node(const TSMeta &schema,
                                                                  TimeSeriesStateParentPtr parent,
                                                                  size_t index)
    {
        return make_state_node([&](TimeSeriesStateV &state) { initialize_state_tree(state, schema, parent, index); });
    }

    void TSDState::bind_value_storage(const TSMeta &element_schema_,
                                      const detail::MapViewDispatch &dispatch,
                                      void *value_data)
    {
        if (slot_observer_registered && element_schema == &element_schema_ && map_dispatch == &dispatch && map_value_data == value_data) {
            return;
        }

        unbind_value_storage();
        element_schema = &element_schema_;
        map_dispatch = &dispatch;
        map_value_data = value_data;
        map_dispatch->add_slot_observer(map_value_data, this);
        slot_observer_registered = true;
        sync_with_value_storage();
    }

    void TSDState::unbind_value_storage() noexcept
    {
        if (slot_observer_registered && map_dispatch != nullptr && map_value_data != nullptr) {
            map_dispatch->remove_slot_observer(map_value_data, this);
        }

        slot_observer_registered = false;
        element_schema = nullptr;
        map_dispatch = nullptr;
        map_value_data = nullptr;
    }

    void TSDState::sync_with_value_storage()
    {
        if (map_dispatch == nullptr || map_value_data == nullptr) { return; }

        on_capacity(child_states.size(), map_dispatch->slot_capacity(map_value_data));
        const size_t slot_capacity = map_dispatch->slot_capacity(map_value_data);
        for (size_t slot = 0; slot < slot_capacity; ++slot) {
            const bool occupied = map_dispatch->slot_occupied(map_value_data, slot);
            const bool removed = occupied && map_dispatch->slot_removed(map_value_data, slot);
            if (occupied && !removed) {
                on_insert(slot);
            } else if (removed) {
                on_remove(slot);
            } else if (slot < child_states.size() && child_states[slot] != nullptr) {
                on_erase(slot);
            }
        }
    }

    void TSDState::on_capacity(size_t old_capacity, size_t new_capacity)
    {
        static_cast<void>(old_capacity);
        if (child_states.size() < new_capacity) { child_states.resize(new_capacity); }
    }

    void TSDState::on_insert(size_t slot)
    {
        if (element_schema == nullptr || map_dispatch == nullptr || map_value_data == nullptr) { return; }

        if (child_states.size() < slot + 1) { child_states.resize(slot + 1); }
        if (child_states[slot] == nullptr) {
            child_states[slot] = make_time_series_state_node(*element_schema, this, slot);
        }

        const TSViewContext child = child_context_for_dict_slot(*this, slot);
        if (!child.is_bound() || active_tries.empty()) { return; }

        const View key{&map_dispatch->key_dispatch(), map_dispatch->key_data(map_value_data, slot), &map_dispatch->key_schema()};

        for (auto &[notifier, trie_node] : active_tries) {
            if (notifier == nullptr || trie_node == nullptr) { continue; }
            if (auto *resolved = trie_node->resolve_pending(key, slot)) {
                replay_active_subtree(child, resolved, notifier, true);
            }
        }
    }

    void TSDState::on_remove(size_t slot)
    {
        if (!active_tries.empty()) {
            for (auto &[notifier, trie_node] : active_tries) {
                static_cast<void>(notifier);
                if (trie_node != nullptr) { trie_node->evict_to_pending(slot); }
            }
        }
    }

    void TSDState::on_erase(size_t slot)
    {
        if (slot < child_states.size()) { child_states[slot].reset(); }
        modified_children.erase(slot);
    }

    void TSDState::on_clear()
    {
        for (size_t slot = 0; slot < child_states.size(); ++slot) { on_erase(slot); }
    }

    TSValueBuilder::TSValueBuilder(const TSMeta &schema,
                                   const ValueBuilder &value_builder,
                                   const detail::TSBuilderOps &builder_ops,
                                   const detail::TSDispatch &ts_dispatch) noexcept
        : m_schema(schema), m_value_builder(value_builder), m_builder_ops(builder_ops), m_ts_dispatch(ts_dispatch)
    {
        const auto layout = builder_ops.layout(schema, value_builder);
        m_value_offset = layout.value_offset;
        m_ts_offset = layout.ts_offset;
        m_size = layout.size;
        m_alignment = layout.alignment;
    }

    TSValue TSValueBuilder::make_value() const
    {
        TSValue value;
        construct_value(value);
        return value;
    }

    void TSValueBuilder::construct_value(TSValue &value) const
    {
        value.clear_storage();
        value.reset_binding();
        value.rebind_builder(*this, TSValue::StorageOwnership::Owned);

        void *memory = allocate();
        auto reset_binding = hgraph::make_scope_exit([&] { value.reset_binding(); });
        auto cleanup_memory = hgraph::make_scope_exit([&] { deallocate(memory); });
        construct(memory);
        value.attach_storage(memory);
        cleanup_memory.release();
        reset_binding.release();
    }

    void TSValueBuilder::copy_construct_value(TSValue &value, const TSValue &other) const
    {
        if (other.m_builder != this) {
            throw std::invalid_argument("TSValueBuilder::copy_construct_value requires matching builder");
        }

        value.clear_storage();
        value.reset_binding();
        value.rebind_builder(*this, TSValue::StorageOwnership::Owned);

        void *memory = allocate();
        auto reset_binding = hgraph::make_scope_exit([&] { value.reset_binding(); });
        auto cleanup_memory = hgraph::make_scope_exit([&] { deallocate(memory); });
        copy_construct(memory, other.storage_memory(), other.builder());
        value.attach_storage(memory);
        cleanup_memory.release();
        reset_binding.release();
    }

    void TSValueBuilder::move_construct_value(TSValue &value, TSValue &other) const
    {
        if (other.m_builder != this) {
            throw std::invalid_argument("TSValueBuilder::move_construct_value requires matching builder");
        }

        value.clear_storage();
        value.reset_binding();
        value.rebind_builder(*this, TSValue::StorageOwnership::Owned);
        value.m_storage = other.m_storage;
        other.reset_binding();
    }

    void TSValueBuilder::destruct_value(TSValue &value) const noexcept
    {
        if (value.m_builder != this || value.storage_memory() == nullptr) { return; }
        if (!value.owns_storage()) {
            value.reset_binding();
            return;
        }
        destruct(value.storage_memory());
        deallocate(value.storage_memory());
        value.reset_binding();
    }

    void *TSValueBuilder::allocate() const
    {
        return ::operator new(m_size, std::align_val_t{m_alignment});
    }

    void TSValueBuilder::deallocate(void *memory) const noexcept
    {
        ::operator delete(memory, std::align_val_t{m_alignment});
    }

    void TSValueBuilder::construct(void *memory) const
    {
        value_builder().construct(value_memory(memory));
        auto cleanup_value = UnwindCleanupGuard([&] { value_builder().destruct(value_memory(memory)); });
        m_builder_ops.get().construct(ts_memory(memory), schema());
        auto cleanup_ts_state = UnwindCleanupGuard([&] { m_builder_ops.get().destruct(ts_memory(memory)); });
        bind_dynamic_dict_states_recursive(schema(), *static_cast<TimeSeriesStateV *>(ts_memory(memory)), value_memory(memory));
    }

    void TSValueBuilder::destruct(void *memory) const noexcept
    {
        unbind_dynamic_dict_states_recursive(*static_cast<TimeSeriesStateV *>(ts_memory(memory)));
        m_builder_ops.get().destruct(ts_memory(memory));
        value_builder().destruct(value_memory(memory));
    }

    void TSValueBuilder::copy_construct(void *dst, const void *src, const TSValueBuilder &src_builder) const
    {
        if (this != &src_builder) { throw std::invalid_argument("TSValue copy construction requires matching builder"); }

        value_builder().copy_construct(value_memory(dst), src_builder.value_memory(src), value_builder());
        auto cleanup_value = UnwindCleanupGuard([&] { value_builder().destruct(value_memory(dst)); });
        m_builder_ops.get().copy_construct(ts_memory(dst), src_builder.ts_memory(src), schema());
        auto cleanup_ts_state = UnwindCleanupGuard([&] { m_builder_ops.get().destruct(ts_memory(dst)); });
        bind_dynamic_dict_states_recursive(schema(), *static_cast<TimeSeriesStateV *>(ts_memory(dst)), value_memory(dst));
    }

    void TSValueBuilder::move_construct(void *dst, void *src, const TSValueBuilder &src_builder) const
    {
        if (this != &src_builder) { throw std::invalid_argument("TSValue move construction requires matching builder"); }

        unbind_dynamic_dict_states_recursive(*static_cast<TimeSeriesStateV *>(ts_memory(src)));
        value_builder().move_construct(value_memory(dst), src_builder.value_memory(src), value_builder());
        auto cleanup_value = UnwindCleanupGuard([&] { value_builder().destruct(value_memory(dst)); });
        m_builder_ops.get().move_construct(ts_memory(dst), src_builder.ts_memory(src), schema());
        auto cleanup_ts_state = UnwindCleanupGuard([&] { m_builder_ops.get().destruct(ts_memory(dst)); });
        bind_dynamic_dict_states_recursive(schema(), *static_cast<TimeSeriesStateV *>(ts_memory(dst)), value_memory(dst));
    }

    void *TSValueBuilder::value_memory(void *memory) const noexcept
    {
        return static_cast<std::byte *>(memory) + m_value_offset;
    }

    const void *TSValueBuilder::value_memory(const void *memory) const noexcept
    {
        return static_cast<const std::byte *>(memory) + m_value_offset;
    }

    void *TSValueBuilder::ts_memory(void *memory) const noexcept
    {
        return static_cast<std::byte *>(memory) + m_ts_offset;
    }

    const void *TSValueBuilder::ts_memory(const void *memory) const noexcept
    {
        return static_cast<const std::byte *>(memory) + m_ts_offset;
    }

    const TSValueBuilder *TSValueBuilderFactory::builder_for(const TSMeta &schema)
    {
        return builder_for(&schema);
    }

    const TSValueBuilder *TSValueBuilderFactory::builder_for(const TSMeta *schema)
    {
        if (schema == nullptr) { return nullptr; }
        if (schema->value_type == nullptr) { return nullptr; }

        static std::unordered_map<const TSMeta *, TSValueBuilder> cache;
        static std::recursive_mutex mutex;

        std::lock_guard lock(mutex);
        if (const auto it = cache.find(schema); it != cache.end()) { return &it->second; }

        const ValueBuilder &value_builder = ValueBuilderFactory::checked_builder_for(schema->value_type, MutationTracking::Delta);
        const auto [it, inserted] = cache.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(schema),
            std::forward_as_tuple(std::cref(*schema), std::cref(value_builder), std::cref(root_builder_ops()),
                                  std::cref(checked_dispatch_for(*schema))));
        static_cast<void>(inserted);
        return &it->second;
    }

    const TSValueBuilder &TSValueBuilderFactory::checked_builder_for(const TSMeta &schema)
    {
        return checked_builder_for(&schema);
    }

    const TSValueBuilder &TSValueBuilderFactory::checked_builder_for(const TSMeta *schema)
    {
        if (const auto *builder = builder_for(schema); builder != nullptr) { return *builder; }
        throw std::invalid_argument("TSValueBuilderFactory requires a schema with a value_type");
    }
}  // namespace hgraph
