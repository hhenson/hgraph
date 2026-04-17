#include <hgraph/types/v2/ref.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace hgraph::v2
{
    namespace
    {
        [[nodiscard]] bool linked_context_equal(const LinkedTSContext &lhs, const LinkedTSContext &rhs) noexcept
        {
            return lhs.schema == rhs.schema && lhs.value_dispatch == rhs.value_dispatch && lhs.ts_dispatch == rhs.ts_dispatch &&
                   lhs.value_data == rhs.value_data && lhs.ts_state == rhs.ts_state &&
                   lhs.notification_state == rhs.notification_state &&
                   lhs.owning_output == rhs.owning_output && lhs.output_view_ops == rhs.output_view_ops;
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
            return seed;
        }

        [[nodiscard]] TSOutputView view_from_target(const LinkedTSContext &target, engine_time_t evaluation_time)
        {
            TSViewContext context{target.schema, target.value_dispatch, target.ts_dispatch, target.value_data, target.ts_state};
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
            const TSViewContext &context  = input.context_ref();
            const TSViewContext  resolved = context.resolved();
            return LinkedTSContext{
                resolved.schema,
                resolved.value_dispatch,
                resolved.ts_dispatch,
                resolved.value_data,
                context.ts_state,
                resolved.owning_output,
                resolved.output_view_ops,
                context.notification_state,
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
    }

    TimeSeriesReference::TimeSeriesReference(std::vector<TimeSeriesReference> items)
        : m_kind(Kind::NON_PEERED), m_observed_time(MIN_DT), m_storage(std::move(items))
    {
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
        if (m_kind != Kind::PEERED) { throw std::runtime_error("v2::TimeSeriesReference::target() called on a non-peered ref"); }
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
            throw std::runtime_error("v2::TimeSeriesReference::items() called on a non-non-peered ref");
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
        LinkedTSContext context = output.linked_context();
        if (!context.is_bound()) { return make(); }

        TimeSeriesReference ref{std::move(context)};
        ref.m_observed_time = output.last_modified_time();
        return ref;
    }

    TimeSeriesReference TimeSeriesReference::make(const TSInputView &input)
    {
        const LinkedTSContext *target = input.linked_target();

        if (const TSMeta *schema = input.ts_schema(); schema != nullptr) {
            switch (schema->kind) {
                case TSKind::REF:
                    {
                        if (target != nullptr && target->is_bound() && (target->schema == nullptr || target->schema->kind != TSKind::REF)) {
                            TimeSeriesReference ref{linked_context_from_input(input)};
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
                    if (target != nullptr && target->is_bound()) { return TimeSeriesReference(linked_context_from_input(input)); }
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
                    if (target != nullptr && target->is_bound()) { return TimeSeriesReference(linked_context_from_input(input)); }
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
                    if (target != nullptr && target->is_bound()) {
                        TimeSeriesReference ref{linked_context_from_input(input)};
                        ref.m_observed_time = input.last_modified_time();
                        return ref;
                    }
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
}  // namespace hgraph::v2
