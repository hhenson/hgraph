#ifndef HGRAPH_CPP_ROOT_TIME_SERIES_VISITOR_H
#define HGRAPH_CPP_ROOT_TIME_SERIES_VISITOR_H

#include <hgraph/types/detail/visitor.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>

#include <concepts>
#include <functional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph
{
    /**
     * Shape-tagged borrowed input view for leaf time-series kinds.
     *
     * The collection kinds retain their existing specialised view classes.
     * Like every endpoint view, this object is a transient cursor and does not
     * extend the lifetime of the endpoint storage.
     */
    template <TSTypeKind Kind>
        requires(Kind == TSTypeKind::TS || Kind == TSTypeKind::REF || Kind == TSTypeKind::SIGNAL)
    class TSLeafInputView : public TSInputTypedView<TSLeafInputView<Kind>>
    {
      public:
        static constexpr TSTypeKind kind = Kind;

        explicit TSLeafInputView(TSInputView view) : TSInputTypedView<TSLeafInputView<Kind>>(std::move(view))
        {
            validate_kind();
        }

      private:
        TSLeafInputView(TSInputView view, detail::TrustedTSEndpointKind)
            : TSInputTypedView<TSLeafInputView<Kind>>(std::move(view))
        {
        }

        void validate_kind() const
        {
            if (this->schema() == nullptr || this->schema()->kind != Kind)
            {
                throw std::invalid_argument("TSLeafInputView requires a matching time-series shape");
            }
        }

        friend struct detail::TSEndpointVisitorAccess;
    };

    /**
     * Shape-tagged borrowed output view for leaf time-series kinds.
     *
     * The collection kinds retain their existing specialised view classes.
     * Like every endpoint view, this object is a transient cursor and does not
     * extend the lifetime of the endpoint storage.
     */
    template <TSTypeKind Kind>
        requires(Kind == TSTypeKind::TS || Kind == TSTypeKind::REF || Kind == TSTypeKind::SIGNAL)
    class TSLeafOutputView : public TSOutputTypedView<TSLeafOutputView<Kind>>
    {
      public:
        static constexpr TSTypeKind kind = Kind;

        explicit TSLeafOutputView(TSOutputView view) : TSOutputTypedView<TSLeafOutputView<Kind>>(std::move(view))
        {
            validate_kind();
        }

      private:
        TSLeafOutputView(TSOutputView view, detail::TrustedTSEndpointKind)
            : TSOutputTypedView<TSLeafOutputView<Kind>>(std::move(view))
        {
        }

        void validate_kind() const
        {
            if (this->schema() == nullptr || this->schema()->kind != Kind)
            {
                throw std::invalid_argument("TSLeafOutputView requires a matching time-series shape");
            }
        }

        friend struct detail::TSEndpointVisitorAccess;
    };

    using TSValueInputView = TSLeafInputView<TSTypeKind::TS>;
    using TSReferenceInputView = TSLeafInputView<TSTypeKind::REF>;
    using TSSignalInputView = TSLeafInputView<TSTypeKind::SIGNAL>;

    using TSValueOutputView = TSLeafOutputView<TSTypeKind::TS>;
    using TSReferenceOutputView = TSLeafOutputView<TSTypeKind::REF>;
    using TSSignalOutputView = TSLeafOutputView<TSTypeKind::SIGNAL>;

    namespace detail
    {
        struct TSEndpointVisitorAccess
        {
            template <typename View>
            [[nodiscard]] static View input(const TSInputView &view)
            {
                return View{view.borrowed_ref(), TrustedTSEndpointKind{}};
            }

            template <typename View>
            [[nodiscard]] static View output(const TSOutputView &view)
            {
                return View{view.borrowed_ref(), TrustedTSEndpointKind{}};
            }
        };

        template <typename Visitor, typename SpecialisedView, typename BaseView>
        inline constexpr bool endpoint_branch_invocable_v =
            visitor_branch_invocable_v<Visitor, SpecialisedView, BaseView>;

        template <typename Visitor, typename SpecialisedView, typename BaseView>
        using endpoint_branch_result_t = visitor_branch_result_t<Visitor, SpecialisedView, BaseView>;

        template <typename SpecialisedView, typename Visitor>
        decltype(auto) invoke_input_endpoint_visitor(Visitor &visitor, const TSInputView &view)
        {
            if constexpr (std::invocable<Visitor &, SpecialisedView>)
            {
                return std::invoke(visitor, TSEndpointVisitorAccess::input<SpecialisedView>(view));
            }
            else
            {
                return std::invoke(visitor, view.borrowed_ref());
            }
        }

        template <typename SpecialisedView, typename Visitor>
        decltype(auto) invoke_output_endpoint_visitor(Visitor &visitor, const TSOutputView &view)
        {
            if constexpr (std::invocable<Visitor &, SpecialisedView>)
            {
                return std::invoke(visitor, TSEndpointVisitorAccess::output<SpecialisedView>(view));
            }
            else
            {
                return std::invoke(visitor, view.borrowed_ref());
            }
        }

        template <typename Result, typename Visitor, typename SpecialisedView, typename BaseView>
        inline constexpr bool endpoint_branch_same_result_v =
            visitor_branch_same_result_v<Result, Visitor, SpecialisedView, BaseView>;

        template <typename Result>
        inline constexpr bool endpoint_result_safe_v = visitor_result_safe_v<Result>;
    }  // namespace detail

    /**
     * Visit one input endpoint according to its semantic time-series kind.
     *
     * A shape-specific handler takes precedence over a ``TSInputView``
     * catch-all. Every reachable handler must return void or the same safe
     * value type. References and lazy hgraph ranges are rejected because the
     * selected wrapper is a temporary borrowed cursor.
     */
    template <typename... Handlers>
    decltype(auto) visit(const TSInputView &view, Handlers &&...handlers)
    {
        static_assert(sizeof...(Handlers) > 0, "time-series endpoint visit requires at least one handler");
        auto visitor = detail::VisitorOverload<std::decay_t<Handlers>...>{std::forward<Handlers>(handlers)...};
        using Visitor = decltype(visitor);

        constexpr bool complete = detail::endpoint_branch_invocable_v<Visitor, TSValueInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSSInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSDInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSLInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSWInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSBInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSReferenceInputView, TSInputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSSignalInputView, TSInputView>;

        if constexpr (!complete)
        {
            static_assert(complete, "time-series endpoint visitor must handle every input kind directly or "
                                    "through a TSInputView catch-all");
        }
        else
        {
            using Result = detail::endpoint_branch_result_t<Visitor, TSValueInputView, TSInputView>;
            static_assert(detail::endpoint_result_safe_v<Result>,
                          "time-series endpoint visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from a temporary view; consume the range in the handler or return an owned collection");
            static_assert(
                detail::endpoint_branch_same_result_v<Result, Visitor, TSSInputView, TSInputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSDInputView, TSInputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSLInputView, TSInputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSWInputView, TSInputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSBInputView, TSInputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSReferenceInputView, TSInputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSSignalInputView, TSInputView>,
                "every time-series endpoint visitor branch must return the same type");

            if (view.schema() == nullptr)
            {
                throw std::invalid_argument("cannot visit an input endpoint without a time-series schema");
            }

            switch (view.schema()->kind)
            {
            case TSTypeKind::TS:
                return detail::invoke_input_endpoint_visitor<TSValueInputView>(visitor, view);
            case TSTypeKind::TSS:
                return detail::invoke_input_endpoint_visitor<TSSInputView>(visitor, view);
            case TSTypeKind::TSD:
                return detail::invoke_input_endpoint_visitor<TSDInputView>(visitor, view);
            case TSTypeKind::TSL:
                return detail::invoke_input_endpoint_visitor<TSLInputView>(visitor, view);
            case TSTypeKind::TSW:
                return detail::invoke_input_endpoint_visitor<TSWInputView>(visitor, view);
            case TSTypeKind::TSB:
                return detail::invoke_input_endpoint_visitor<TSBInputView>(visitor, view);
            case TSTypeKind::REF:
                return detail::invoke_input_endpoint_visitor<TSReferenceInputView>(visitor, view);
            case TSTypeKind::SIGNAL:
                return detail::invoke_input_endpoint_visitor<TSSignalInputView>(visitor, view);
            }

            throw std::invalid_argument("cannot visit an input endpoint with an unknown time-series kind");
        }
    }

    /**
     * Visit one output endpoint according to its semantic time-series kind.
     *
     * A shape-specific handler takes precedence over a ``TSOutputView``
     * catch-all. Every reachable handler must return void or the same safe
     * value type. References and lazy hgraph ranges are rejected because the
     * selected wrapper is a temporary borrowed cursor.
     */
    template <typename... Handlers>
    decltype(auto) visit(const TSOutputView &view, Handlers &&...handlers)
    {
        static_assert(sizeof...(Handlers) > 0, "time-series endpoint visit requires at least one handler");
        auto visitor = detail::VisitorOverload<std::decay_t<Handlers>...>{std::forward<Handlers>(handlers)...};
        using Visitor = decltype(visitor);

        constexpr bool complete = detail::endpoint_branch_invocable_v<Visitor, TSValueOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSSOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSDOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSLOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSWOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSBOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSReferenceOutputView, TSOutputView> &&
                                  detail::endpoint_branch_invocable_v<Visitor, TSSignalOutputView, TSOutputView>;

        if constexpr (!complete)
        {
            static_assert(complete, "time-series endpoint visitor must handle every "
                                    "output kind directly or "
                                    "through a TSOutputView catch-all");
        }
        else
        {
            using Result = detail::endpoint_branch_result_t<Visitor, TSValueOutputView, TSOutputView>;
            static_assert(detail::endpoint_result_safe_v<Result>,
                          "time-series endpoint visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from a temporary view; consume the range in the handler or return an owned collection");
            static_assert(
                detail::endpoint_branch_same_result_v<Result, Visitor, TSSOutputView, TSOutputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSDOutputView, TSOutputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSLOutputView, TSOutputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSWOutputView, TSOutputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSBOutputView, TSOutputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSReferenceOutputView, TSOutputView> &&
                    detail::endpoint_branch_same_result_v<Result, Visitor, TSSignalOutputView, TSOutputView>,
                "every time-series endpoint visitor branch must return the same type");

            if (view.schema() == nullptr)
            {
                throw std::invalid_argument("cannot visit an output endpoint without a time-series schema");
            }

            switch (view.schema()->kind)
            {
            case TSTypeKind::TS:
                return detail::invoke_output_endpoint_visitor<TSValueOutputView>(visitor, view);
            case TSTypeKind::TSS:
                return detail::invoke_output_endpoint_visitor<TSSOutputView>(visitor, view);
            case TSTypeKind::TSD:
                return detail::invoke_output_endpoint_visitor<TSDOutputView>(visitor, view);
            case TSTypeKind::TSL:
                return detail::invoke_output_endpoint_visitor<TSLOutputView>(visitor, view);
            case TSTypeKind::TSW:
                return detail::invoke_output_endpoint_visitor<TSWOutputView>(visitor, view);
            case TSTypeKind::TSB:
                return detail::invoke_output_endpoint_visitor<TSBOutputView>(visitor, view);
            case TSTypeKind::REF:
                return detail::invoke_output_endpoint_visitor<TSReferenceOutputView>(visitor, view);
            case TSTypeKind::SIGNAL:
                return detail::invoke_output_endpoint_visitor<TSSignalOutputView>(visitor, view);
            }

            throw std::invalid_argument("cannot visit an output endpoint with an unknown time-series kind");
        }
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TIME_SERIES_VISITOR_H
