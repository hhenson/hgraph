#ifndef HGRAPH_TYPES_FRAME_SELECTORS_H
#define HGRAPH_TYPES_FRAME_SELECTORS_H

#include <hgraph/types/static_node_core.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>

namespace hgraph
{
    template <fixed_string Name, typename TElement, auto... TPolicies>
    class In<Name, TS<SeriesOf<TElement>>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TS<SeriesOf<TElement>>;
        using value_type                 = Series;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] const Series &value() const { return TSInputView::value().template checked_as<Series>(); }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    template <fixed_string Name, typename TSchema, typename TMetadata, auto... TPolicies>
    class In<Name, TS<FrameOf<TSchema, TMetadata>>, TPolicies...> : public TSInputView
    {
      public:
        using schema                     = TS<FrameOf<TSchema, TMetadata>>;
        using value_type                 = Frame;
        static constexpr auto field_name = Name;
        static constexpr auto activity   = static_node_detail::resolved_input_activity<TPolicies...>();
        static constexpr auto validity   = static_node_detail::resolved_input_validity<TPolicies...>();

        explicit In(TSInputView view) noexcept : TSInputView(std::move(view)) {}

        [[nodiscard]] const Frame &value() const { return TSInputView::value().template checked_as<Frame>(); }
        [[nodiscard]] const TSInputView &base() const noexcept { return *this; }
    };

    template <typename TElement>
    class Out<TS<SeriesOf<TElement>>> : public TSOutputView
    {
      public:
        using schema     = TS<SeriesOf<TElement>>;
        using value_type = Series;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void set(Series value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            auto destination = mutation.value();
            const ValueView source{destination.binding(), static_cast<const void *>(&value)};
            static_cast<void>(mutation.copy_value_from(source));
        }

        void apply(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }
    };

    template <typename TSchema, typename TMetadata>
    class Out<TS<FrameOf<TSchema, TMetadata>>> : public TSOutputView
    {
      public:
        using schema     = TS<FrameOf<TSchema, TMetadata>>;
        using value_type = Frame;

        Out(TSOutputView view, DateTime /*evaluation_time*/) noexcept : TSOutputView(std::move(view)) {}

        void set(Frame value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            auto destination = mutation.value();
            const ValueView source{destination.binding(), static_cast<const void *>(&value)};
            static_cast<void>(mutation.copy_value_from(source));
        }

        void apply(const ValueView &value) const
        {
            auto mutation = TSOutputView::begin_mutation(evaluation_time());
            static_cast<void>(mutation.copy_value_from(value));
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_FRAME_SELECTORS_H
