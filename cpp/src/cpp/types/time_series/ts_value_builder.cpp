#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/ts_view.h>

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

        void initialize_ref_state(RefLinkState &state,
                                  TimeSeriesStateParentPtr parent,
                                  size_t index,
                                  engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time);
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
            if (const TimeSeriesStateV *state = const_state_value(slot); state != nullptr) {
                if (const auto *link = std::get_if<TargetLinkState>(state)) {
                    return linked_child_context(link->target, local_state, schema, value_dispatch, ts_dispatch);
                }
                if (const auto *link = std::get_if<RefLinkState>(state)) {
                    return linked_child_context(link->bound_link.target, local_state, schema, value_dispatch, ts_dispatch);
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
            if (state.slot_key_hashes.size() < slot_count) { state.slot_key_hashes.resize(slot_count, 0); }
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

        void ensure_child_state(TSDState &collection_state, size_t slot, const TSMeta &child_schema, size_t key_hash)
        {
            ensure_child_slot_capacity(collection_state, slot + 1);

            if (state_value(collection_state.child_states[slot]) != nullptr && collection_state.slot_key_hashes[slot] == key_hash) { return; }

            auto child_state = make_state_node([&](TimeSeriesStateV &state) {
                initialize_state_tree(state, child_schema, parent_ptr(collection_state), slot);
            });
            install_child_state(collection_state, slot, std::move(child_state));
            collection_state.slot_key_hashes[slot] = key_hash;
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
                    initialize_ref_state(state.emplace<RefLinkState>(), parent, index);
                    break;

                case TSKind::SIGNAL:
                    initialize_base_state(state.emplace<SignalState>(), parent, index);
                    break;
            }
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
                dst_state.bound_link.last_modified_time = src_state->bound_link.last_modified_time;
                dst_state.bound_link.scheduling_notifier.set_target(src_state->bound_link.scheduling_notifier.get_target());
                if (src_state->bound_link.target.is_bound()) { dst_state.bound_link.set_target(src_state->bound_link.target); }
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
                        dst_state.slot_key_hashes = src_state.slot_key_hashes;
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
                        const auto &src_state = std::get<RefLinkState>(src);
                        auto &dst_state = dst.emplace<RefLinkState>();
                        initialize_ref_state(dst_state, parent, index, src_state.last_modified_time);
                        dst_state.bound_link.last_modified_time = src_state.bound_link.last_modified_time;
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

        [[nodiscard]] TSLState *list_state(TSViewContext context) noexcept
        {
            return static_cast<TSLState *>(context.ts_state);
        }

        [[nodiscard]] TSBState *bundle_state(TSViewContext context) noexcept
        {
            return static_cast<TSBState *>(context.ts_state);
        }

        [[nodiscard]] TSDState *dict_state(TSViewContext context) noexcept
        {
            return static_cast<TSDState *>(context.ts_state);
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

            [[nodiscard]] TSViewContext child_at(const TSViewContext &context, size_t index) const noexcept override
            {
                ListView list = context.value().as_list();
                if (index >= list.size()) { return TSViewContext::none(); }

                TSLState *state = list_state(context);
                if (state == nullptr) { return TSViewContext::none(); }
                ensure_child_state(*state, index, m_element_schema.get());

                View child_value = list.at(index);
                return child_context_from_slot(state->child_states[index],
                                               m_element_schema.get(),
                                               m_element_value_dispatch.get(),
                                               m_element_ts_dispatch.get(),
                                               RawViewAccess::data_of(child_value));
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

            [[nodiscard]] TSViewContext child_at(const TSViewContext &context, size_t index) const noexcept override
            {
                if (index >= m_fields.size()) { return TSViewContext::none(); }

                TSBState *state = bundle_state(context);
                if (state == nullptr) { return TSViewContext::none(); }
                ensure_child_state(*state, index, m_fields[index].schema.get());

                View child_value = context.value().as_bundle().at(index);
                return child_context_from_slot(state->child_states[index],
                                               m_fields[index].schema.get(),
                                               m_fields[index].value_dispatch.get(),
                                               m_fields[index].ts_dispatch.get(),
                                               RawViewAccess::data_of(child_value));
            }

            [[nodiscard]] TSViewContext child_field(const TSViewContext &context, std::string_view name) const noexcept override
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

            [[nodiscard]] TSViewContext child_at(const TSViewContext &context, size_t index) const noexcept override
            {
                MapDeltaView delta = context.value().as_map().delta();
                if (index >= delta.slot_capacity() || !delta.slot_occupied(index)) { return TSViewContext::none(); }
                return child_at_slot(context, delta, index);
            }

            [[nodiscard]] TSViewContext child_key(const TSViewContext &context, const View &key) const noexcept override
            {
                RawMapAccess map{context.value()};
                const auto *dispatch = map.map_dispatch();
                const size_t slot = dispatch->find(map.data(), RawMapAccess::data_of(key));
                if (slot == static_cast<size_t>(-1)) { return TSViewContext::none(); }

                MapDeltaView delta = map.delta();
                if (!detail::dict_slot_is_live(delta, slot)) { return TSViewContext::none(); }
                return child_at_slot(context, delta, slot);
            }

          private:
            [[nodiscard]] TSViewContext child_at_slot(const TSViewContext &context,
                                                      const MapDeltaView &delta,
                                                      size_t slot) const noexcept
            {
                TSDState *state = dict_state(context);
                if (state == nullptr) { return TSViewContext::none(); }

                const View key = delta.key_at_slot(slot);
                ensure_child_state(*state, slot, m_value_schema.get(), key.hash());

                return child_context_from_slot(state->child_states[slot],
                                               m_value_schema.get(),
                                               m_value_dispatch.get(),
                                               m_value_ts_dispatch.get(),
                                               RawViewAccess::data_of(delta.value_at_slot(slot)));
            }

            std::reference_wrapper<const TSMeta>               m_value_schema;
            std::reference_wrapper<const detail::ViewDispatch> m_value_dispatch;
            std::reference_wrapper<const detail::TSDispatch>   m_value_ts_dispatch;
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
                case TSKind::TSS:
                case TSKind::TSW:
                case TSKind::REF:
                case TSKind::SIGNAL:
                    dispatch = std::make_unique<LeafTSDispatch>();
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
        try {
            construct(memory);
            value.attach_storage(memory);
        } catch (...) {
            value.reset_binding();
            deallocate(memory);
            throw;
        }
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
        try {
            copy_construct(memory, other.storage_memory(), other.builder());
            value.attach_storage(memory);
        } catch (...) {
            value.reset_binding();
            deallocate(memory);
            throw;
        }
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
        try {
            m_builder_ops.get().construct(ts_memory(memory), schema());
        } catch (...) {
            value_builder().destruct(value_memory(memory));
            throw;
        }
    }

    void TSValueBuilder::destruct(void *memory) const noexcept
    {
        m_builder_ops.get().destruct(ts_memory(memory));
        value_builder().destruct(value_memory(memory));
    }

    void TSValueBuilder::copy_construct(void *dst, const void *src, const TSValueBuilder &src_builder) const
    {
        if (this != &src_builder) { throw std::invalid_argument("TSValue copy construction requires matching builder"); }

        value_builder().copy_construct(value_memory(dst), src_builder.value_memory(src), value_builder());
        try {
            m_builder_ops.get().copy_construct(ts_memory(dst), src_builder.ts_memory(src), schema());
        } catch (...) {
            value_builder().destruct(value_memory(dst));
            throw;
        }
    }

    void TSValueBuilder::move_construct(void *dst, void *src, const TSValueBuilder &src_builder) const
    {
        if (this != &src_builder) { throw std::invalid_argument("TSValue move construction requires matching builder"); }

        value_builder().move_construct(value_memory(dst), src_builder.value_memory(src), value_builder());
        try {
            m_builder_ops.get().move_construct(ts_memory(dst), src_builder.ts_memory(src), schema());
        } catch (...) {
            value_builder().destruct(value_memory(dst));
            throw;
        }
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
