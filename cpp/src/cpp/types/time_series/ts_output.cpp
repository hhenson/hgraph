#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/v2/ref.h>

#include <memory>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSBState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSLState &state) noexcept { return &state; }

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

        [[nodiscard]] bool supports_wrap_cast(const TSMeta &source_schema, const TSMeta &target_schema)
        {
            if (&source_schema == &target_schema) { return true; }

            if (target_schema.kind == TSKind::REF) { return target_schema.element_ts() == &source_schema; }

            if (source_schema.kind != target_schema.kind) { return false; }

            switch (source_schema.kind) {
                case TSKind::TSB:
                    if (source_schema.field_count() != target_schema.field_count()) { return false; }
                    for (size_t i = 0; i < source_schema.field_count(); ++i) {
                        const auto &source_field = source_schema.fields()[i];
                        const auto &target_field = target_schema.fields()[i];
                        if (std::string_view{source_field.name} != std::string_view{target_field.name}) { return false; }
                        if (!supports_wrap_cast(*source_field.ts_type, *target_field.ts_type)) { return false; }
                    }
                    return true;

                case TSKind::TSL:
                    if (source_schema.fixed_size() == 0 || source_schema.fixed_size() != target_schema.fixed_size()) { return false; }
                    return supports_wrap_cast(*source_schema.element_ts(), *target_schema.element_ts());

                default:
                    return false;
            }
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
            if (source_schema == nullptr || !supports_wrap_cast(*source_schema, schema)) {
                throw std::invalid_argument("TSOutput alternative does not support the requested wrap cast");
            }

            configure_branch(view(nullptr), source);
        }

        ~AlternativeOutput() override
        {
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
            if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
        }

      private:
        struct WrappedRefSubscription
        {
            BaseState *source_state{nullptr};
            std::unique_ptr<WrappedRefNotifier> notifier;
        };

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

            if (target_schema->kind == TSKind::REF) {
                if (target_schema->element_ts() != source_schema) {
                    throw std::invalid_argument("TSOutput alternative REF wrapping requires matching referenced schema");
                }

                target_view.value().as_atomic().set(hgraph::v2::TimeSeriesReference::make(source_view));
                auto notifier = std::make_unique<WrappedRefNotifier>(target_state);
                if (source_state != nullptr) { source_state->subscribe(notifier.get()); }
                m_wrapped_ref_subscriptions.push_back(
                    WrappedRefSubscription{.source_state = source_state, .notifier = std::move(notifier)});
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

                default:
                    throw std::invalid_argument("TSOutput alternatives only support fixed-shape collection wrap casts");
            }
        }

        std::vector<WrappedRefSubscription> m_wrapped_ref_subscriptions;
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

        if (source_context.schema == nullptr || !supports_wrap_cast(*source_context.schema, *schema)) {
            throw std::invalid_argument("TSOutput::bindable_view does not support the requested schema cast");
        }

        const auto [it, inserted] = m_alternatives.try_emplace(schema, nullptr);
        if (inserted) { it->second.reset(new AlternativeOutput(source, *schema)); }
        return it->second->view(this);
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
