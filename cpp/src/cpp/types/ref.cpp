#include <hgraph/types/ref.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] bool linked_context_equal(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept
        {
            return lhs.schema == rhs.schema && lhs.value_dispatch == rhs.value_dispatch && lhs.ts_dispatch == rhs.ts_dispatch &&
                   lhs.value_data == rhs.value_data && lhs.ts_state == rhs.ts_state &&
                   lhs.notification_state == rhs.notification_state &&
                   lhs.owning_output == rhs.owning_output && lhs.output_view_ops == rhs.output_view_ops &&
                   lhs.pending_dict_child.parent_schema == rhs.pending_dict_child.parent_schema &&
                   lhs.pending_dict_child.parent_value_dispatch == rhs.pending_dict_child.parent_value_dispatch &&
                   lhs.pending_dict_child.parent_ts_dispatch == rhs.pending_dict_child.parent_ts_dispatch &&
                   lhs.pending_dict_child.parent_value_data == rhs.pending_dict_child.parent_value_data &&
                   lhs.pending_dict_child.parent_ts_state == rhs.pending_dict_child.parent_ts_state &&
                   lhs.pending_dict_child.parent_owning_output == rhs.pending_dict_child.parent_owning_output &&
                   lhs.pending_dict_child.parent_output_view_ops == rhs.pending_dict_child.parent_output_view_ops &&
                   lhs.pending_dict_child.parent_notification_state == rhs.pending_dict_child.parent_notification_state &&
                   lhs.pending_dict_child.key.equals(rhs.pending_dict_child.key);
        }

        [[nodiscard]] size_t linked_context_hash(const LinkedTSContext &context) noexcept
        {
            size_t seed = 0;
            const auto mix = [&seed](const void *ptr) {
                seed ^= std::hash<const void *>{}(ptr) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            };
            mix(context.schema);
            mix(context.value_dispatch);
            mix(context.ts_dispatch);
            mix(context.value_data);
            mix(context.ts_state);
            mix(context.notification_state);
            mix(context.owning_output);
            mix(context.output_view_ops);
            mix(context.pending_dict_child.parent_schema);
            mix(context.pending_dict_child.parent_value_dispatch);
            mix(context.pending_dict_child.parent_ts_dispatch);
            mix(context.pending_dict_child.parent_value_data);
            mix(context.pending_dict_child.parent_ts_state);
            mix(context.pending_dict_child.parent_owning_output);
            mix(context.pending_dict_child.parent_output_view_ops);
            mix(context.pending_dict_child.parent_notification_state);
            return seed;
        }

        [[nodiscard]] TSOutputView view_from_target(const LinkedTSContext &target, engine_time_t evaluation_time)
        {
            TSViewContext context{TSContext{
                target.schema,
                target.value_dispatch,
                target.ts_dispatch,
                target.value_data,
                target.ts_state,
                target.owning_output,
                target.output_view_ops,
                target.notification_state != nullptr ? target.notification_state : target.ts_state,
                target.pending_dict_child,
            }};
            return TSOutputView{
                context,
                TSViewContext::none(),
                evaluation_time,
                target.owning_output,
                target.output_view_ops != nullptr ? target.output_view_ops : &detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] LinkedTSContext linked_context_from_input(const TSInputView &input) noexcept
        {
            if (const LinkedTSContext *target = input.linked_target(); target != nullptr && target->is_bound()) { return *target; }

            const TSViewContext &context  = input.context_ref();
            const TSViewContext  resolved = context.resolved();
            const bool use_resolved = resolved.is_bound();
            return LinkedTSContext{
                use_resolved ? resolved.schema : context.schema,
                use_resolved ? resolved.value_dispatch : context.value_dispatch,
                use_resolved ? resolved.ts_dispatch : context.ts_dispatch,
                use_resolved ? resolved.value_data : context.value_data,
                use_resolved ? resolved.ts_state : context.ts_state,
                use_resolved ? resolved.owning_output : context.owning_output,
                use_resolved ? resolved.output_view_ops : context.output_view_ops,
                use_resolved ? (resolved.notification_state != nullptr ? resolved.notification_state : resolved.ts_state)
                             : context.notification_state,
                use_resolved ? resolved.pending_dict_child : context.pending_dict_child,
            };
        }

        [[nodiscard]] LinkedTSContext reference_context_from_output(const TSOutputView &output) noexcept
        {
            const TSViewContext &context = output.context_ref();
            if (context.is_bound()) {
                return LinkedTSContext{
                    context.schema,
                    context.value_dispatch,
                    context.ts_dispatch,
                    context.value_data,
                    context.ts_state,
                    context.owning_output != nullptr ? context.owning_output : output.owning_output(),
                    context.output_view_ops != nullptr ? context.output_view_ops : output.output_view_ops(),
                    context.notification_state != nullptr ? context.notification_state : context.ts_state,
                    context.pending_dict_child,
                };
            }

            const TSViewContext resolved = context.resolved();
            if (!resolved.is_bound()) { return {}; }

            return LinkedTSContext{
                resolved.schema,
                resolved.value_dispatch,
                resolved.ts_dispatch,
                resolved.value_data,
                resolved.ts_state,
                resolved.owning_output != nullptr ? resolved.owning_output : output.owning_output(),
                resolved.output_view_ops != nullptr ? resolved.output_view_ops : output.output_view_ops(),
                resolved.notification_state != nullptr ? resolved.notification_state : resolved.ts_state,
                resolved.pending_dict_child,
            };
        }

    }  // namespace

    TimeSeriesReference atomic_default_value(std::type_identity<TimeSeriesReference>)
    {
        return TimeSeriesReference::make();
    }

    size_t atomic_hash(const TimeSeriesReference &value)
    {
        switch (value.kind()) {
            case TimeSeriesReference::Kind::EMPTY: return 0;
            case TimeSeriesReference::Kind::PEERED: return linked_context_hash(value.target());
            case TimeSeriesReference::Kind::NON_PEERED:
                {
                    size_t seed = 0;
                    for (const auto &item : value.items()) {
                        seed ^= atomic_hash(item) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
                    }
                    return seed;
                }
        }
        return 0;
    }

    std::partial_ordering atomic_compare(const TimeSeriesReference &lhs, const TimeSeriesReference &rhs)
    {
        return lhs == rhs ? std::partial_ordering::equivalent : std::partial_ordering::unordered;
    }

    std::string to_string(const TimeSeriesReference &value)
    {
        return value.to_string();
    }

    TimeSeriesReference::TimeSeriesReference(LinkedTSContext target) noexcept
        : m_kind(Kind::PEERED), m_observed_time(MIN_DT), m_storage(std::move(target))
    {
        register_invalidator();
    }

    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items)
        : m_kind(Kind::NON_PEERED), m_observed_time(MIN_DT), m_storage(std::move(items))
    {
    }

    TimeSeriesReference::TimeSeriesReference(const TimeSeriesReference &other)
        : m_kind(other.m_kind), m_observed_time(other.m_observed_time), m_storage(other.m_storage)
    {
        // Copies need their own invalidator registered with the same target,
        // because each TimeSeriesReference instance is severed independently
        // when its target dies. The source's invalidator stays attached to
        // the source.
        register_invalidator();
    }

    TimeSeriesReference::TimeSeriesReference(TimeSeriesReference &&other) noexcept
        : m_kind(other.m_kind),
          m_observed_time(other.m_observed_time),
          m_storage(std::move(other.m_storage)),
          m_invalidator(std::move(other.m_invalidator))
    {
        // Re-seat the invalidator's owner pointer so future invalidate_ref
        // calls land on us, not the moved-from husk.
        if (m_invalidator) { m_invalidator->owner = this; }
        other.m_kind = Kind::EMPTY;
        other.m_observed_time = MIN_DT;
    }

    TimeSeriesReference &TimeSeriesReference::operator=(const TimeSeriesReference &other)
    {
        if (this == &other) { return *this; }
        unregister_invalidator();
        m_kind = other.m_kind;
        m_observed_time = other.m_observed_time;
        m_storage = other.m_storage;
        register_invalidator();
        return *this;
    }

    TimeSeriesReference &TimeSeriesReference::operator=(TimeSeriesReference &&other) noexcept
    {
        if (this == &other) { return *this; }
        unregister_invalidator();
        m_kind = other.m_kind;
        m_observed_time = other.m_observed_time;
        m_storage = std::move(other.m_storage);
        m_invalidator = std::move(other.m_invalidator);
        if (m_invalidator) { m_invalidator->owner = this; }
        other.m_kind = Kind::EMPTY;
        other.m_observed_time = MIN_DT;
        return *this;
    }

    TimeSeriesReference::~TimeSeriesReference() noexcept
    {
        unregister_invalidator();
    }

    void TimeSeriesReference::register_invalidator() noexcept
    {
        if (m_kind != Kind::PEERED) { return; }
        const auto *bound = std::get_if<LinkedTSContext>(&m_storage);
        if (bound == nullptr || bound->ts_state == nullptr) { return; }
        m_invalidator = std::make_unique<ReferenceInvalidator>();
        m_invalidator->owner = this;
        bound->ts_state->register_ref_invalidator(m_invalidator.get());
    }

    void TimeSeriesReference::unregister_invalidator() noexcept
    {
        if (!m_invalidator) { return; }
        // If the target has already been destroyed, skip the unregister call
        // — the BaseState whose set we'd be touching is gone.
        if (!m_invalidator->target_destroyed) {
            if (auto *bound = std::get_if<LinkedTSContext>(&m_storage); bound != nullptr && bound->ts_state != nullptr) {
                bound->ts_state->unregister_ref_invalidator(m_invalidator.get());
            }
        }
        m_invalidator.reset();
    }

    void invalidate_ref(ReferenceInvalidator &invalidator) noexcept
    {
        invalidator.target_destroyed = true;
        if (TimeSeriesReference *owner = invalidator.owner; owner != nullptr) {
            // Drop the LinkedTSContext (its ts_state is about to be freed) and
            // flip the ref to EMPTY. The owning ref's destructor will see
            // target_destroyed == true and skip the unregister call back into
            // the now-defunct state.
            owner->m_kind = TimeSeriesReference::Kind::EMPTY;
            owner->m_storage = std::monostate{};
        }
    }

    bool TimeSeriesReference::is_valid() const
    {
        switch (m_kind) {
            case Kind::EMPTY: return false;
            case Kind::PEERED: return target().is_bound() && target_view().valid();
            case Kind::NON_PEERED:
                return std::any_of(items().begin(), items().end(), [](const auto &item) { return !item.is_empty(); });
        }
        return false;
    }

    const LinkedTSContext &TimeSeriesReference::target() const
    {
        if (m_kind != Kind::PEERED) { throw std::runtime_error("TimeSeriesReference::target() called on a non-peered ref"); }
        return std::get<LinkedTSContext>(m_storage);
    }

    TSOutputView TimeSeriesReference::target_view(engine_time_t evaluation_time) const
    {
        const LinkedTSContext &bound_target = target();
        if (!bound_target.is_bound()) { return TSOutputView{TSViewContext::none(), TSViewContext::none(), evaluation_time, nullptr}; }
        return view_from_target(bound_target, evaluation_time);
    }

    const std::vector<TimeSeriesReference> &TimeSeriesReference::items() const
    {
        if (m_kind != Kind::NON_PEERED) {
            throw std::runtime_error("TimeSeriesReference::items() called on a non-non-peered ref");
        }
        return std::get<std::vector<TimeSeriesReference>>(m_storage);
    }

    const TimeSeriesReference &TimeSeriesReference::operator[](size_t ndx) const
    {
        return items()[ndx];
    }

    bool TimeSeriesReference::operator==(const TimeSeriesReference &other) const noexcept
    {
        if (m_kind != other.m_kind) { return false; }

        switch (m_kind) {
            case Kind::EMPTY: return true;
            case Kind::PEERED: return linked_context_equal(target(), other.target());
            case Kind::NON_PEERED: return items() == other.items();
        }
        return false;
    }

    std::string TimeSeriesReference::to_string() const
    {
        switch (m_kind) {
            case Kind::EMPTY: return "REF[<Unset>]";
            case Kind::PEERED:
                return fmt::format("REF[target@{:p} value@{:p}]",
                                   static_cast<const void *>(target().ts_state),
                                   target().value_data);
            case Kind::NON_PEERED:
                {
                    std::vector<std::string> parts;
                    parts.reserve(items().size());
                    for (const auto &item : items()) { parts.push_back(item.to_string()); }
                    return fmt::format("REF[{}]", fmt::join(parts, ", "));
                }
        }
        return "REF[?]";
    }

    TimeSeriesReference TimeSeriesReference::make()
    {
        return {};
    }

    TimeSeriesReference TimeSeriesReference::make(const TSOutputView &output)
    {
        if (const TSMeta *schema = output.ts_schema(); schema != nullptr && schema->kind == TSKind::REF) {
            if (!output.valid()) { return make(); }

            const auto *ref = output.value().as_atomic().template try_as<TimeSeriesReference>();
            if (ref == nullptr) { return make(); }
            if (!ref->is_peered()) { return *ref; }

            const TSMeta *declared_target_schema = schema->element_ts();
            TSOutputView target_view = ref->target_view(output.evaluation_time());
            TimeSeriesReference refreshed = make(target_view);
            if (refreshed.is_empty()) { return *ref; }

            const bool original_matches_declared =
                declared_target_schema == nullptr ||
                equivalent_ts_schema(ref->target().schema, declared_target_schema);
            const bool refreshed_matches_declared =
                refreshed.is_peered() &&
                (declared_target_schema == nullptr ||
                 equivalent_ts_schema(refreshed.target().schema, declared_target_schema));

            if (original_matches_declared && !refreshed_matches_declared) { return *ref; }
            return refreshed;
        }

        LinkedTSContext context = reference_context_from_output(output);
        if (!context.is_bound()) { return make(); }

        TimeSeriesReference ref{std::move(context)};
        ref.m_observed_time = output.last_modified_time();
        return ref;
    }

    TimeSeriesReference TimeSeriesReference::make(const TSInputView &input)
    {
        const LinkedTSContext *target = input.linked_target();
        const LinkedTSContext resolved_context = linked_context_from_input(input);
        const auto make_peered = [&](const LinkedTSContext &context) {
            TimeSeriesReference ref{context};
            ref.m_observed_time = input.last_modified_time();
            return ref;
        };

        if (const TSMeta *schema = input.ts_schema(); schema != nullptr) {
            switch (schema->kind) {
                case TSKind::REF:
                    {
                        if (target != nullptr && target->is_bound() && (target->schema == nullptr || target->schema->kind != TSKind::REF)) {
                            const TSMeta *declared_target_schema = schema->element_ts();
                            const bool target_matches_declared =
                                declared_target_schema == nullptr || equivalent_ts_schema(target->schema, declared_target_schema);
                            const bool resolved_matches_declared =
                                resolved_context.is_bound() &&
                                (declared_target_schema == nullptr ||
                                 equivalent_ts_schema(resolved_context.schema, declared_target_schema));

                            const LinkedTSContext &ref_context =
                                !target_matches_declared && resolved_matches_declared ? resolved_context : *target;

                            TimeSeriesReference ref{ref_context};
                            ref.m_observed_time = input.last_modified_time();
                            return ref;
                        }

                        const auto value = input.value();
                        if (!value.has_value()) { return make(); }
                        TimeSeriesReference ref = value.as_atomic().checked_as<TimeSeriesReference>();
                        if (ref.observed_time() == MIN_DT) { ref.m_observed_time = input.last_modified_time(); }
                        return ref;
                    }

                case TSKind::TSB:
                    if (resolved_context.is_bound()) { return make_peered(resolved_context); }
                    {
                        std::vector<TimeSeriesReference> refs;
                        auto bundle = input.as_bundle();
                        refs.reserve(bundle.size());
                        for (size_t i = 0; i < bundle.size(); ++i) {
                            TimeSeriesReference item = make(bundle[i]);
                            refs.push_back(item.is_valid() ? std::move(item) : make());
                        }
                        return make(std::move(refs));
                    }

                case TSKind::TSL:
                    if (resolved_context.is_bound()) { return make_peered(resolved_context); }
                    {
                        std::vector<TimeSeriesReference> refs;
                        auto list = input.as_list();
                        refs.reserve(list.size());
                        for (size_t i = 0; i < list.size(); ++i) {
                            TimeSeriesReference item = make(list[i]);
                            refs.push_back(item.is_valid() ? std::move(item) : make());
                        }
                        return make(std::move(refs));
                    }

                case TSKind::TSD:
                case TSKind::TSS:
                case TSKind::TSW:
                case TSKind::TSValue:
                case TSKind::SIGNAL:
                    if (resolved_context.is_bound()) { return make_peered(resolved_context); }
                    break;
            }
        }

        return make();
    }

    TimeSeriesReference TimeSeriesReference::make(std::vector<TimeSeriesReference> items)
    {
        return items.empty() ? make() : TimeSeriesReference(std::move(items));
    }

    const TimeSeriesReference &TimeSeriesReference::empty()
    {
        static const TimeSeriesReference empty_ref;
        return empty_ref;
    }
}  // namespace hgraph
