#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/v2/ref.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        using TSPath = std::vector<size_t>;

        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSBState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSLState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSDState &state) noexcept { return &state; }

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
        void install_target_link(TCollectionState &parent_state, size_t slot, LinkedTSContext target)
        {
            auto link_state = std::make_unique<TimeSeriesStateV>();
            auto &typed_state = link_state->template emplace<TargetLinkState>();
            initialize_base_state(typed_state,
                                  parent_ptr(parent_state),
                                  slot,
                                  target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT,
                                  TSStorageKind::TargetLink);
            typed_state.target.clear();
            typed_state.scheduling_notifier.set_target(nullptr);
            typed_state.set_target(std::move(target));
            parent_state.child_states[slot] = std::move(link_state);
        }

        void initialize_ref_link_state(RefLinkState &state,
                                       TimeSeriesStateParentPtr parent,
                                       size_t index,
                                       engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time, TSStorageKind::RefLink);
            state.source.clear();
            initialize_base_state(state.bound_link, static_cast<TSOutput *>(nullptr), 0, MIN_DT, TSStorageKind::TargetLink);
            state.bound_link.target.clear();
            state.bound_link.scheduling_notifier.set_target(nullptr);
        }

        template <typename TCollectionState>
        void install_ref_link(TCollectionState &parent_state, size_t slot, LinkedTSContext ref_source)
        {
            auto link_state = std::make_unique<TimeSeriesStateV>();
            auto &typed_state = link_state->template emplace<RefLinkState>();
            initialize_ref_link_state(typed_state, parent_ptr(parent_state), slot);
            typed_state.set_source(std::move(ref_source));
            parent_state.child_states[slot] = std::move(link_state);
        }

        [[nodiscard]] bool supports_alternative_cast(const TSMeta &source_schema, const TSMeta &target_schema)
        {
            if (&source_schema == &target_schema) { return true; }

            // The two schema-changing cases supported so far are:
            // - TS  -> REF : wrap as a TimeSeriesReference value
            // - REF -> TS  : dereference through RefLinkState
            if (target_schema.kind == TSKind::REF) { return target_schema.element_ts() == &source_schema; }
            if (source_schema.kind == TSKind::REF) {
                return source_schema.element_ts() == &target_schema ||
                       supports_alternative_cast(*source_schema.element_ts(), target_schema);
            }

            if (source_schema.kind != target_schema.kind) { return false; }

            switch (source_schema.kind) {
                case TSKind::TSB:
                    if (source_schema.field_count() != target_schema.field_count()) { return false; }
                    for (size_t i = 0; i < source_schema.field_count(); ++i) {
                        const auto &source_field = source_schema.fields()[i];
                        const auto &target_field = target_schema.fields()[i];
                        if (std::string_view{source_field.name} != std::string_view{target_field.name}) { return false; }
                        if (!supports_alternative_cast(*source_field.ts_type, *target_field.ts_type)) { return false; }
                    }
                    return true;

                case TSKind::TSL:
                    if (source_schema.fixed_size() == 0 || source_schema.fixed_size() != target_schema.fixed_size()) { return false; }
                    return supports_alternative_cast(*source_schema.element_ts(), *target_schema.element_ts());

                case TSKind::TSD:
                    return source_schema.key_type() == target_schema.key_type() &&
                           supports_alternative_cast(*source_schema.element_ts(), *target_schema.element_ts());

                default:
                    return false;
            }
        }

        [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, size_t slot)
        {
            switch (schema.kind) {
                case TSKind::TSB:
                    if (slot >= schema.field_count()) { throw std::out_of_range("TSOutput alternative child slot is out of range for TSB"); }
                    return schema.fields()[slot].ts_type;

                case TSKind::TSL:
                    if (schema.fixed_size() == 0) {
                        throw std::invalid_argument("TSOutput alternatives do not yet support dynamic TSL path prefixes");
                    }
                    if (slot >= schema.fixed_size()) { throw std::out_of_range("TSOutput alternative child slot is out of range for TSL"); }
                    return schema.element_ts();

                default:
                    throw std::invalid_argument("TSOutput alternative child navigation only supports TSB and fixed-size TSL");
            }
        }

        [[nodiscard]] const BaseState *base_state_of(const TimeSeriesStateV &state) noexcept
        {
            return std::visit([](const auto &typed_state) -> const BaseState * { return &typed_state; }, state);
        }

        [[nodiscard]] BaseState *base_state_of(TimeSeriesStateV &state) noexcept
        {
            return std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, state);
        }

        [[nodiscard]] const BaseState *parent_collection_of(const BaseState *state) noexcept
        {
            if (state == nullptr) { return nullptr; }

            const BaseState *parent_state = nullptr;
            hgraph::visit(
                state->parent,
                [&](const auto *parent) {
                    using T = std::remove_pointer_t<decltype(parent)>;
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState>) {
                        parent_state = parent;
                    }
                },
                [] {});
            return parent_state;
        }

        [[nodiscard]] bool is_descendant_state(const BaseState *state, const BaseState *ancestor) noexcept
        {
            for (auto *current = state; current != nullptr; current = parent_collection_of(current)) {
                if (current == ancestor) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool find_state_path(const BaseState *state, const TSMeta *schema, const BaseState *target, TSPath &path)
        {
            if (state == nullptr || schema == nullptr) { return false; }
            if (state == target) { return true; }

            switch (schema->kind) {
                case TSKind::TSB:
                    {
                        if (state->storage_kind != TSStorageKind::Native) { return false; }
                        const auto &bundle_state = *static_cast<const TSBState *>(state);
                        for (size_t i = 0; i < bundle_state.child_states.size(); ++i) {
                            const auto &child = bundle_state.child_states[i];
                            if (!child) { continue; }
                            path.push_back(i);
                            if (find_state_path(base_state_of(*child), child_schema_at(*schema, i), target, path)) { return true; }
                            path.pop_back();
                        }
                        return false;
                    }

                case TSKind::TSL:
                    {
                        if (state->storage_kind != TSStorageKind::Native) { return false; }
                        const auto &list_state = *static_cast<const TSLState *>(state);
                        for (size_t i = 0; i < list_state.child_states.size(); ++i) {
                            const auto &child = list_state.child_states[i];
                            if (!child) { continue; }
                            path.push_back(i);
                            if (find_state_path(base_state_of(*child), child_schema_at(*schema, i), target, path)) { return true; }
                            path.pop_back();
                        }
                        return false;
                    }

                case TSKind::TSD:
                    throw std::invalid_argument("TSOutput alternatives do not yet support TSD child paths");

                default:
                    return false;
            }
        }

        [[nodiscard]] TSPath source_path_from_root(const TSOutputView &root_view, const TSOutputView &source)
        {
            if (source.owning_output() != nullptr && source.owning_output() != root_view.owning_output()) {
                throw std::logic_error("TSOutput alternative source view is not owned by the requested output");
            }

            TSPath path;
            BaseState *target_state = source.context_ref().ts_state;
            if (target_state == nullptr) { return path; }

            if (!find_state_path(root_view.context_ref().ts_state, root_view.ts_schema(), target_state, path)) {
                throw std::logic_error("TSOutput alternative source view is not reachable from the owning output root");
            }
            return path;
        }

        [[nodiscard]] const TSMeta *
        replace_schema_at_path(const TSMeta &source_schema, const TSPath &path, size_t depth, const TSMeta &replacement_schema)
        {
            if (depth == path.size()) { return &replacement_schema; }

            const size_t slot = path[depth];
            auto &registry = TSTypeRegistry::instance();

            switch (source_schema.kind) {
                case TSKind::TSB:
                    {
                        if (slot >= source_schema.field_count()) {
                            throw std::out_of_range("TSOutput alternative replacement path is out of range for TSB");
                        }

                        const TSMeta *updated_child =
                            replace_schema_at_path(*source_schema.fields()[slot].ts_type, path, depth + 1, replacement_schema);
                        if (updated_child == source_schema.fields()[slot].ts_type) { return &source_schema; }

                        std::vector<std::pair<std::string, const TSMeta *>> fields;
                        fields.reserve(source_schema.field_count());
                        for (size_t i = 0; i < source_schema.field_count(); ++i) {
                            fields.emplace_back(
                                source_schema.fields()[i].name,
                                i == slot ? updated_child : source_schema.fields()[i].ts_type);
                        }

                        return registry.tsb(
                            fields,
                            source_schema.bundle_name() != nullptr ? source_schema.bundle_name() : "",
                            source_schema.python_type());
                    }

                case TSKind::TSL:
                    {
                        if (source_schema.fixed_size() == 0) {
                            throw std::invalid_argument("TSOutput alternatives do not yet support dynamic TSL path prefixes");
                        }
                        if (slot >= source_schema.fixed_size()) {
                            throw std::out_of_range("TSOutput alternative replacement path is out of range for TSL");
                        }

                        const TSMeta *updated_child =
                            replace_schema_at_path(*source_schema.element_ts(), path, depth + 1, replacement_schema);
                        return updated_child == source_schema.element_ts() ? &source_schema
                                                                           : registry.tsl(updated_child, source_schema.fixed_size());
                    }

                default:
                    throw std::invalid_argument("TSOutput alternative replacement paths only support TSB and fixed-size TSL");
            }
        }

        [[nodiscard]] TSOutputView traverse_output_path(TSOutputView view, const TSMeta *schema, const TSPath &path)
        {
            const TSMeta *current_schema = schema;
            for (const size_t slot : path) {
                if (current_schema == nullptr) { throw std::invalid_argument("TSOutput alternative traversal requires a schema"); }

                switch (current_schema->kind) {
                    case TSKind::TSB:
                        view = view.as_bundle()[slot];
                        break;

                    case TSKind::TSL:
                        view = view.as_list()[slot];
                        break;

                    default:
                        throw std::invalid_argument("TSOutput alternative traversal only supports TSB and fixed-size TSL");
                }

                current_schema = child_schema_at(*current_schema, slot);
            }

            return view;
        }
    }  // namespace

    namespace detail
    {
        namespace
        {
            struct DefaultTSOutputViewOps final : TSOutputViewOps
            {
                [[nodiscard]] LinkedTSContext linked_context(const TSOutputView &view) const noexcept override
                {
                    const TSViewContext &context = view.context_ref();
                    const TSViewContext resolved = context.resolved();
                    return LinkedTSContext{
                        resolved.schema,
                        resolved.value_dispatch,
                        resolved.ts_dispatch,
                        resolved.value_data,
                        context.ts_state,
                    };
                }
            };
        }  // namespace

        const TSOutputViewOps &default_output_view_ops() noexcept
        {
            static DefaultTSOutputViewOps ops;
            return ops;
        }
    }  // namespace detail

    struct TSOutput::AlternativeOutput final : TSValue, Notifiable
    {
        struct WrappedRefNotifier final : Notifiable
        {
            explicit WrappedRefNotifier(BaseState *target_state_) noexcept
                : target_state(target_state_)
            {
            }

            void notify(engine_time_t modified_time) override
            {
                if (target_state != nullptr) { target_state->mark_modified(modified_time); }
            }

            BaseState *target_state{nullptr};
        };

        AlternativeOutput(const TSOutputView &source, const TSMeta &schema)
            : TSValue(schema)
        {
            const TSMeta *source_schema = source.ts_schema();
            if (source_schema == nullptr || !supports_alternative_cast(*source_schema, schema)) {
                throw std::invalid_argument("TSOutput alternative does not support the requested wrap cast");
            }

            configure_branch(view(nullptr), source);
        }

        ~AlternativeOutput() override
        {
            for (auto &binding : m_dynamic_dict_bindings) {
                if (binding != nullptr && binding->source_context.ts_state != nullptr) {
                    binding->source_context.ts_state->unsubscribe(&binding->source_notifier);
                }
            }
            for (auto &subscription : m_wrapped_ref_subscriptions) {
                if (subscription.source_state != nullptr && subscription.notifier) {
                    subscription.source_state->unsubscribe(subscription.notifier.get());
                }
            }
        }

        AlternativeOutput(const AlternativeOutput &) = delete;
        AlternativeOutput &operator=(const AlternativeOutput &) = delete;
        AlternativeOutput(AlternativeOutput &&) = delete;
        AlternativeOutput &operator=(AlternativeOutput &&) = delete;

        [[nodiscard]] TSOutputView view(TSOutput *owning_output, engine_time_t evaluation_time = MIN_DT)
        {
            TSViewContext context = view_context();
            return TSOutputView{context, TSViewContext::none(), evaluation_time, owning_output, &detail::default_output_view_ops()};
        }

        void notify(engine_time_t modified_time) override
        {
            if (m_dynamic_dict_bindings.empty()) {
                if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
            }
        }

      private:
        struct WrappedRefSubscription
        {
            BaseState *source_state{nullptr};
            BaseState *target_state{nullptr};
            std::unique_ptr<WrappedRefNotifier> notifier;
        };

        struct DynamicDictBinding
        {
            struct SourceNotifier final : Notifiable
            {
                void notify(engine_time_t modified_time) override
                {
                    if (binding != nullptr && binding->owner != nullptr) {
                        binding->owner->sync_dynamic_dict(*binding, modified_time, false);
                    }
                }

                DynamicDictBinding *binding{nullptr};
            };

            // One binding records "this alternative TSD subtree mirrors that
            // source TSD subtree". The stored target context stays rooted at
            // the alternative dict boundary, while source_context identifies
            // the live producer-side dict we replay from.
            DynamicDictBinding(AlternativeOutput *owner_, const TSOutputView &target_view, const TSOutputView &source_view) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(source_view.linked_context())
            {
                source_notifier.binding = this;
                last_source_root_state = source_context.ts_state != nullptr ? source_context.ts_state->resolved_state() : nullptr;
            }

            DynamicDictBinding(AlternativeOutput *owner_,
                               const TSOutputView &target_view,
                               std::unique_ptr<TimeSeriesStateV> source_bridge_state_,
                               LinkedTSContext source_context_) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(std::move(source_context_)),
                  source_bridge_state(std::move(source_bridge_state_))
            {
                source_notifier.binding = this;
                last_source_root_state = source_context.ts_state != nullptr ? source_context.ts_state->resolved_state() : nullptr;
            }

            AlternativeOutput *owner{nullptr};
            TSViewContext target_context{TSViewContext::none()};
            LinkedTSContext source_context{};
            std::unique_ptr<TimeSeriesStateV> source_bridge_state{};
            BaseState *last_source_root_state{nullptr};
            SourceNotifier source_notifier{};
        };

        [[nodiscard]] std::unique_ptr<DynamicDictBinding>
        make_dereferenced_dynamic_dict_binding(const TSOutputView &target_view, const TSOutputView &ref_source_view)
        {
            const TSMeta *source_schema = ref_source_view.ts_schema();
            if (source_schema == nullptr || source_schema->kind != TSKind::REF || source_schema->element_ts() == nullptr ||
                source_schema->element_ts()->kind != TSKind::TSD) {
                throw std::invalid_argument("TSOutput dereferenced dynamic dict binding requires REF[TSD[...]] source");
            }

            auto source_bridge_state = std::make_unique<TimeSeriesStateV>();
            auto &bridge_state = source_bridge_state->template emplace<RefLinkState>();
            initialize_ref_link_state(bridge_state, TimeSeriesStateParentPtr{}, 0);
            bridge_state.set_source(ref_source_view.linked_context());

            LinkedTSContext source_context{
                source_schema->element_ts(),
                nullptr,
                nullptr,
                nullptr,
                base_state_of(*source_bridge_state),
            };

            return std::make_unique<DynamicDictBinding>(
                this,
                target_view,
                std::move(source_bridge_state),
                std::move(source_context));
        }

        [[nodiscard]] TSOutputView output_view_for(const TSViewContext &context, engine_time_t evaluation_time = MIN_DT) const
        {
            return TSOutputView{context, TSViewContext::none(), evaluation_time, nullptr, &detail::default_output_view_ops()};
        }

        [[nodiscard]] TSOutputView output_view_for(const LinkedTSContext &context, engine_time_t evaluation_time = MIN_DT) const
        {
            return output_view_for(
                TSViewContext{context.schema, context.value_dispatch, context.ts_dispatch, context.value_data, context.ts_state},
                evaluation_time);
        }

        void release_wrapped_ref_subscriptions(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            // Dynamic TSD replay can replace an entire child subtree when a
            // key is removed or a stable slot is reused for a different key.
            // Any wrap subscriptions owned by that subtree must disappear with
            // it or the old source branch would keep driving stale state.
            auto it = m_wrapped_ref_subscriptions.begin();
            while (it != m_wrapped_ref_subscriptions.end()) {
                if (it->target_state != nullptr && is_descendant_state(it->target_state, subtree_root)) {
                    if (it->source_state != nullptr && it->notifier) {
                        it->source_state->unsubscribe(it->notifier.get());
                    }
                    it = m_wrapped_ref_subscriptions.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void release_dynamic_dict_bindings(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            for (auto it = m_dynamic_dict_bindings.begin(); it != m_dynamic_dict_bindings.end();) {
                if ((*it)->target_context.ts_state != nullptr && is_descendant_state((*it)->target_context.ts_state, subtree_root)) {
                    if ((*it)->source_context.ts_state != nullptr) {
                        (*it)->source_context.ts_state->unsubscribe(&(*it)->source_notifier);
                    }
                    it = m_dynamic_dict_bindings.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void clear_dynamic_child_slot(TSDState &state, size_t slot)
        {
            if (slot >= state.child_states.size() || state.child_states[slot] == nullptr) { return; }

            // The stable slot is being vacated from the alternative map. Tear
            // down every owned subtree artifact first, then drop the child
            // state so the slot can be rebuilt from the value-layer insert
            // event if a key later reuses it.
            BaseState *child_root = base_state_of(*state.child_states[slot]);
            release_wrapped_ref_subscriptions(child_root);
            release_dynamic_dict_bindings(child_root);
            state.on_erase(slot);
        }

        void install_ref_link_for_target(const TSOutputView &target_view, const TSOutputView &source_view)
        {
            BaseState *target_state = target_view.linked_context().ts_state;
            if (target_state == nullptr) {
                throw std::logic_error("TSOutput alternative REF dereference requires a live target state");
            }

            const LinkedTSContext ref_source = source_view.linked_context();
            bool replaced = false;
            hgraph::visit(
                target_state->parent,
                [&](auto *parent_state) {
                    using T = std::remove_pointer_t<decltype(parent_state)>;

                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSBState> || std::same_as<T, TSDState>) {
                        if (parent_state == nullptr) {
                            throw std::logic_error("TSOutput alternative REF dereference requires a live parent collection");
                        }
                        install_ref_link(*parent_state, target_state->index, ref_source);
                        replaced = true;
                    }
                },
                [&] {
                    // Root-level REF -> TS casts replace the alternative's
                    // root state node itself rather than a collection child.
                    auto &root_state = state_variant();
                    auto &typed_state = root_state.template emplace<RefLinkState>();
                    initialize_ref_link_state(typed_state, TimeSeriesStateParentPtr{}, 0);
                    typed_state.set_source(ref_source);
                    replaced = true;
                });

            if (!replaced) {
                throw std::logic_error("TSOutput alternative REF dereference could not replace the target state");
            }
        }

        void install_dynamic_child(const TSOutputView &target_dict_view,
                                   const View &key,
                                   const TSOutputView &source_child,
                                   engine_time_t modified_time,
                                   bool mark_child_modified)
        {
            // The mutation layer has already ensured the key exists in the
            // alternative map value. Resolve the corresponding output-side TS
            // slot and then install either:
            // - a direct TargetLink for unchanged child schema, or
            // - a recursively transformed child subtree when the value schema
            //   itself still needs wrapping/dereference work below this point.
            auto target_dict = target_dict_view.as_dict();
            TSOutputView target_child = target_dict.at(key);
            BaseState *target_child_state = target_child.context_ref().ts_state;
            auto &target_state = *static_cast<TSDState *>(target_dict_view.context_ref().ts_state);
            if (target_child_state == nullptr) {
                constexpr size_t no_slot = static_cast<size_t>(-1);
                auto target_map = target_dict_view.value().as_map();
                const auto target_delta = target_map.delta();
                for (size_t slot = target_map.first_live_slot(); slot != no_slot; slot = target_map.next_live_slot(slot)) {
                    if (!target_delta.key_at_slot(slot).equals(key)) { continue; }
                    target_state.on_insert(slot);
                    target_child = target_dict.at(key);
                    target_child_state = target_child.context_ref().ts_state;
                    break;
                }
            }
            if (target_child_state == nullptr) {
                throw std::logic_error("TSOutput alternative dynamic dict child requires a live target state");
            }

            const size_t target_slot = target_child_state->index;
            const TSMeta *target_child_schema = target_child.ts_schema();
            const TSMeta *source_child_schema = source_child.ts_schema();
            if (target_child_schema == nullptr || source_child_schema == nullptr) {
                throw std::logic_error("TSOutput alternative dynamic dict child requires bound source and target schemas");
            }

            if (target_child_schema == source_child_schema) {
                install_target_link(target_state, target_slot, source_child.linked_context());
            } else {
                configure_branch(target_child, source_child);
            }

            BaseState *installed_state =
                target_slot < target_state.child_states.size() && target_state.child_states[target_slot] != nullptr
                    ? base_state_of(*target_state.child_states[target_slot])
                    : nullptr;
            // Newly materialized children need an explicit modification mark
            // so the alternative child becomes valid immediately at the source
            // tick that introduced or updated the key.
            if (mark_child_modified && installed_state != nullptr) { installed_state->mark_modified(modified_time); }
        }

        void sync_dynamic_dict(DynamicDictBinding &binding, engine_time_t modified_time, bool initializing)
        {
            TSOutputView target_root = output_view_for(binding.target_context, modified_time);
            TSOutputView source_root = output_view_for(binding.source_context, modified_time);

            auto *target_root_state = static_cast<TSDState *>(target_root.context_ref().ts_state);
            if (target_root_state == nullptr) {
                throw std::logic_error("TSOutput alternative dynamic dict sync requires a live target TSD state");
            }

            const View source_value = source_root.value();
            auto target_map = target_root.value().as_map();
            auto target_dict = target_root.as_dict();
            auto mutation = target_map.begin_mutation();
            bool map_value_changed = false;
            constexpr size_t no_slot = static_cast<size_t>(-1);
            const BaseState *current_source_root_state =
                binding.source_context.ts_state != nullptr ? binding.source_context.ts_state->resolved_state() : nullptr;
            const bool source_rebound = !initializing && current_source_root_state != binding.last_source_root_state;
            const bool full_resync = initializing || source_rebound;

            auto remove_target_key = [&](const View &key) {
                TSOutputView existing_child = target_dict.at(key);
                if (BaseState *existing_state = existing_child.context_ref().ts_state; existing_state != nullptr) {
                    clear_dynamic_child_slot(*target_root_state, existing_state->index);
                }
                static_cast<void>(mutation.remove(key));
                map_value_changed = true;
            };

            if (!source_value.has_value()) {
                if (full_resync) {
                    std::vector<Value> stale_keys;
                    for (size_t slot = target_map.first_live_slot(); slot != no_slot; slot = target_map.next_live_slot(slot)) {
                        stale_keys.emplace_back(target_map.delta().key_at_slot(slot));
                    }
                    for (const auto &key : stale_keys) { remove_target_key(key.view()); }
                }

                binding.last_source_root_state = const_cast<BaseState *>(current_source_root_state);
                if (!initializing && map_value_changed) {
                    if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
                }
                return;
            }

            const auto source_delta = source_value.as_map().delta();
            const auto source_map = source_value.as_map();

            // TSD key flow:
            // 1. The source map delta is authoritative for structural changes.
            // 2. We replay removals first, because stable slots may be reused.
            // 3. We then replay live slots into the alternative map value.
            // 4. Finally, for each live key we ensure the TS child subtree at
            //    that key mirrors the source child shape (link, REF wrap, or a
            //    deeper recursive alternative).
            //
            // The map value and TS child state move in lock-step: the value
            // layer owns key/slot membership, while the TS state layer owns the
            // per-key binding subtree that hangs off each occupied slot.

            // Remove stale keys before replaying live slots so reused logical
            // positions tear down their old subtree subscriptions first.
            if (full_resync) {
                std::vector<Value> stale_keys;
                for (size_t slot = target_map.first_live_slot(); slot != no_slot; slot = target_map.next_live_slot(slot)) {
                    const View key = target_map.delta().key_at_slot(slot);
                    if (!source_map.contains(key)) { stale_keys.emplace_back(key); }
                }
                for (const auto &key : stale_keys) { remove_target_key(key.view()); }
            } else {
                for (size_t slot = source_delta.first_removed_slot(); slot != no_slot; slot = source_delta.next_removed_slot(slot)) {
                    remove_target_key(source_delta.key_at_slot(slot));
                }
            }

            const bool has_added_slots = source_delta.first_added_slot() != no_slot;
            const bool has_updated_slots = source_delta.first_updated_slot() != no_slot;
            if (!full_resync && !map_value_changed && !has_added_slots && !has_updated_slots) {
                binding.last_source_root_state = const_cast<BaseState *>(current_source_root_state);
                return;
            }

            auto replay_live_slot = [&](size_t slot, bool source_updated, bool rebuild_child) {
                const View key = source_delta.key_at_slot(slot);
                TSOutputView target_child = target_dict.at(key);
                const bool target_has_key = target_child.context_ref().is_bound();

                if (!target_has_key) {
                    // Insert the key into the alternative map before touching
                    // TS child state. That guarantees dict navigation can
                    // resolve a stable slot for install_dynamic_child().
                    Value placeholder{*target_root.ts_schema()->element_ts()->value_type};
                    mutation.set(key, placeholder.view());
                    map_value_changed = true;
                } else if (source_updated) {
                    // Preserve the existing alternative payload object while
                    // still surfacing the map-level delta for "this key
                    // changed". The child subtree carries the real wrapped or
                    // linked semantics.
                    mutation.set(key, target_child.value());
                    map_value_changed = true;
                }

                if (rebuild_child && target_has_key) {
                    if (BaseState *existing_state = target_child.context_ref().ts_state; existing_state != nullptr) {
                        clear_dynamic_child_slot(*target_root_state, existing_state->index);
                    }
                }

                if (rebuild_child || !target_has_key) {
                    TSOutputView source_child = source_root.as_dict().at(key);
                    install_dynamic_child(target_root, key, source_child, modified_time, modified_time != MIN_DT && source_updated);
                }
            };

            if (full_resync) {
                for (size_t slot = source_map.first_live_slot(); slot != no_slot; slot = source_map.next_live_slot(slot)) {
                    replay_live_slot(slot, true, true);
                }
            } else {
                for (size_t slot = source_delta.first_added_slot(); slot != no_slot; slot = source_delta.next_added_slot(slot)) {
                    replay_live_slot(slot, true, true);
                }
                for (size_t slot = source_delta.first_updated_slot(); slot != no_slot; slot = source_delta.next_updated_slot(slot)) {
                    if (source_delta.slot_added(slot)) { continue; }
                    replay_live_slot(slot, true, false);
                }
            }

            binding.last_source_root_state = const_cast<BaseState *>(current_source_root_state);

            if (!initializing && map_value_changed) {
                // The dynamic dict binding handles child-level validity and
                // modification directly. Only publish a root-level change when
                // replay actually changed the alternative map value. Pure
                // child-state updates already flow through the installed
                // subtree links and would otherwise notify the root twice.
                if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
            }
        }

        void configure_branch(const TSOutputView &target_view, const TSOutputView &source_view)
        {
            const TSMeta *target_schema = target_view.ts_schema();
            const TSMeta *source_schema = source_view.ts_schema();
            if (target_schema == nullptr || source_schema == nullptr) {
                throw std::invalid_argument("TSOutput alternative configuration requires bound source and target schemas");
            }

            BaseState *target_state = target_view.linked_context().ts_state;
            BaseState *source_state = source_view.linked_context().ts_state;
            if (target_state != nullptr) {
                target_state->last_modified_time = source_state != nullptr ? source_state->last_modified_time : MIN_DT;
            }

            if (source_schema->kind == TSKind::REF) {
                if (source_schema->element_ts() == target_schema) {
                    // REF -> TS does not copy target data into the alternative.
                    // Instead the alternative position becomes a RefLinkState that
                    // tracks the REF source and exposes the current dereferenced
                    // target through the normal TS view surface.
                    install_ref_link_for_target(target_view, source_view);
                    return;
                }

                if (target_schema->kind == TSKind::TSD && source_schema->element_ts() != nullptr &&
                    source_schema->element_ts()->kind == TSKind::TSD &&
                    supports_alternative_cast(*source_schema->element_ts(), *target_schema)) {
                    auto binding = make_dereferenced_dynamic_dict_binding(target_view, source_view);
                    auto &binding_ref = *binding;
                    auto unsubscribe_on_failure = make_scope_exit([&] {
                        if (binding_ref.source_context.ts_state != nullptr) {
                            binding_ref.source_context.ts_state->unsubscribe(&binding_ref.source_notifier);
                        }
                    });
                    if (binding_ref.source_context.ts_state != nullptr) {
                        binding_ref.source_context.ts_state->subscribe(&binding_ref.source_notifier);
                    }
                    sync_dynamic_dict(binding_ref, source_state != nullptr ? source_state->last_modified_time : MIN_DT, true);
                    unsubscribe_on_failure.release();
                    m_dynamic_dict_bindings.push_back(std::move(binding));
                    return;
                }

                throw std::invalid_argument("TSOutput alternative REF dereference requires matching or recursively compatible schema");
            }

            if (target_schema->kind == TSKind::REF) {
                if (target_schema->element_ts() != source_schema) {
                    throw std::invalid_argument("TSOutput alternative REF wrapping requires matching referenced schema");
                }

                target_view.value().as_atomic().set(hgraph::v2::TimeSeriesReference::make(source_view));
                auto notifier = std::make_unique<WrappedRefNotifier>(target_state);
                if (source_state != nullptr) { source_state->subscribe(notifier.get()); }
                m_wrapped_ref_subscriptions.push_back(
                    WrappedRefSubscription{.source_state = source_state, .target_state = target_state, .notifier = std::move(notifier)});
                return;
            }

            if (target_schema == source_schema) {
                throw std::logic_error("TSOutput alternative configuration should not recurse into schema-identical branches");
            }

            if (target_schema->kind != source_schema->kind) {
                throw std::invalid_argument("TSOutput alternative configuration requires matching collection kinds");
            }

            switch (target_schema->kind) {
                case TSKind::TSB:
                    {
                        auto &target_state_ref = *static_cast<TSBState *>(target_view.linked_context().ts_state);
                        auto source_bundle = source_view.as_bundle();
                        auto target_bundle = target_view.as_bundle();
                        for (size_t i = 0; i < target_schema->field_count(); ++i) {
                            const TSMeta *target_child_schema = target_schema->fields()[i].ts_type;
                            const TSMeta *source_child_schema = source_schema->fields()[i].ts_type;
                            TSOutputView source_child = source_bundle[i];
                            if (target_child_schema == source_child_schema) {
                                install_target_link(target_state_ref, i, source_child.linked_context());
                            } else {
                                configure_branch(target_bundle[i], source_child);
                            }
                        }
                        return;
                    }

                case TSKind::TSL:
                    {
                        if (target_schema->fixed_size() == 0 || target_schema->fixed_size() != source_schema->fixed_size()) {
                            throw std::invalid_argument("TSOutput alternatives only support fixed-size TSL wrap casts");
                        }
                        auto &target_state_ref = *static_cast<TSLState *>(target_view.linked_context().ts_state);
                        auto source_list = source_view.as_list();
                        auto target_list = target_view.as_list();
                        for (size_t i = 0; i < target_schema->fixed_size(); ++i) {
                            const TSMeta *target_child_schema = target_schema->element_ts();
                            const TSMeta *source_child_schema = source_schema->element_ts();
                            TSOutputView source_child = source_list[i];
                            if (target_child_schema == source_child_schema) {
                                install_target_link(target_state_ref, i, source_child.linked_context());
                            } else {
                                configure_branch(target_list[i], source_child);
                            }
                        }
                        return;
                    }

                case TSKind::TSD:
                    {
                        if (source_schema->key_type() != target_schema->key_type()) {
                            throw std::invalid_argument("TSOutput alternatives only support TSD casts with matching key schemas");
                        }

                        // Dynamic dict alternatives are kept live after
                        // construction. Instead of eagerly recursing every
                        // possible child, we subscribe to the source dict root
                        // and replay key-level structural changes on demand.
                        auto binding = std::make_unique<DynamicDictBinding>(this, target_view, source_view);
                        auto &binding_ref = *binding;
                        auto unsubscribe_on_failure = make_scope_exit([&] {
                            if (binding_ref.source_context.ts_state != nullptr) {
                                binding_ref.source_context.ts_state->unsubscribe(&binding_ref.source_notifier);
                            }
                        });
                        if (binding_ref.source_context.ts_state != nullptr) {
                            binding_ref.source_context.ts_state->subscribe(&binding_ref.source_notifier);
                        }
                        sync_dynamic_dict(binding_ref, source_state != nullptr ? source_state->last_modified_time : MIN_DT, true);
                        unsubscribe_on_failure.release();
                        m_dynamic_dict_bindings.push_back(std::move(binding));
                        return;
                    }

                default:
                    throw std::invalid_argument("TSOutput alternatives only support collection wrap casts through TSB, TSL, and TSD");
            }
        }

        std::vector<WrappedRefSubscription> m_wrapped_ref_subscriptions;
        std::vector<std::unique_ptr<DynamicDictBinding>> m_dynamic_dict_bindings;
    };

    TSOutput::TSOutput(const TSOutputBuilder &builder)
    {
        builder.construct_output(*this);
    }

    TSOutput::TSOutput(const TSOutput &other)
    {
        if (other.m_builder != nullptr) { other.builder().copy_construct_output(*this, other); }
    }

    TSOutput::TSOutput(TSOutput &&other) noexcept
    {
        if (other.m_builder != nullptr) { other.builder().move_construct_output(*this, other); }
    }

    TSOutput &TSOutput::operator=(const TSOutput &other)
    {
        if (this == &other) { return *this; }
        TSOutput replacement(other);
        return *this = std::move(replacement);
    }

    TSOutput &TSOutput::operator=(TSOutput &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        if (other.m_builder == nullptr) {
            m_builder = nullptr;
            m_alternatives.clear();
            return *this;
        }
        other.builder().move_construct_output(*this, other);
        return *this;
    }

    TSOutput::~TSOutput()
    {
        clear_storage();
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time)
    {
        TSViewContext context = view_context();
        return TSOutputView{context, TSViewContext::none(), evaluation_time, this, &detail::default_output_view_ops()};
    }

    TSOutputView TSOutput::bindable_view(const TSOutputView &source, const TSMeta *schema)
    {
        if (schema == nullptr) { throw std::invalid_argument("TSOutput::bindable_view requires a non-null target schema"); }

        const LinkedTSContext source_context = source.linked_context();
        if (!source_context.is_bound()) { throw std::invalid_argument("TSOutput::bindable_view requires a bound source view"); }
        if (source_context.schema == schema) { return source; }

        if (source_context.schema == nullptr || !supports_alternative_cast(*source_context.schema, *schema)) {
            throw std::invalid_argument("TSOutput::bindable_view does not support the requested schema cast");
        }

        TSOutputView root_source = view(source.evaluation_time());
        const TSPath source_path = source_path_from_root(root_source, source);
        const TSMeta *root_source_schema = root_source.ts_schema();
        if (root_source_schema == nullptr) { throw std::logic_error("TSOutput::bindable_view requires a rooted source schema"); }

        // Alternatives are owned and cached at the output boundary. Child
        // casts therefore rebuild a rooted target schema with only the
        // requested subtree transformed, then project the requested child view
        // back out of that rooted alternative.
        const TSMeta *alternative_schema = replace_schema_at_path(*root_source_schema, source_path, 0, *schema);

        const auto [it, inserted] = m_alternatives.try_emplace(alternative_schema, nullptr);
        if (inserted) { it->second.reset(new AlternativeOutput(root_source, *alternative_schema)); }

        TSOutputView alternative_view = it->second->view(this, source.evaluation_time());
        return traverse_output_path(alternative_view, alternative_schema, source_path);
    }

    void TSOutput::clear_storage() noexcept
    {
        if (m_builder == nullptr) {
            m_alternatives.clear();
            return;
        }
        builder().destruct_output(*this);
    }

    void TSOutput::AlternativeOutputDeleter::operator()(AlternativeOutput *value) const noexcept
    {
        delete value;
    }
}  // namespace hgraph
