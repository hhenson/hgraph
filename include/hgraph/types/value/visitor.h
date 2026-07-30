#ifndef HGRAPH_CPP_ROOT_VALUE_VISITOR_H
#define HGRAPH_CPP_ROOT_VALUE_VISITOR_H

#include <hgraph/types/detail/visitor.h>
#include <hgraph/types/value/specialized_views.h>

#include <functional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph
{
    namespace detail
    {
        struct ValueVisitorAccess
        {
            [[nodiscard]] static ValueView borrow(const ValueView &view) noexcept { return view.borrowed_ref(); }

            template <typename View>
            [[nodiscard]] static View project(const ValueView &view)
            {
                return View{view.borrowed_ref(), TrustedValueKind{}};
            }
        };

        template <typename SpecialisedView, typename Visitor>
        decltype(auto) invoke_value_visitor(Visitor &visitor, const ValueView &view)
        {
            if constexpr (std::invocable<Visitor &, SpecialisedView>)
            {
                return std::invoke(visitor, ValueVisitorAccess::project<SpecialisedView>(view));
            }
            else
            {
                return std::invoke(visitor, ValueVisitorAccess::borrow(view));
            }
        }
    }  // namespace detail

    /**
     * Visit a live erased value according to its semantic value shape.
     *
     * ``Any`` is a transparent owning box rather than a visitor alternative:
     * every populated Any layer is peeled before dispatch. An empty Any has no
     * value to visit and is rejected.
     *
     * A shape-specific handler takes precedence over a ``ValueView`` catch-all.
     * Every reachable handler must return void or the same safe value type.
     * References and lazy hgraph ranges are rejected because the selected
     * wrapper is a temporary borrowed cursor.
     */
    template <typename... Handlers>
    decltype(auto) visit(const ValueView &value, Handlers &&...handlers)
    {
        static_assert(sizeof...(Handlers) > 0, "value visit requires at least one handler");
        auto visitor  = detail::VisitorOverload<std::decay_t<Handlers>...>{std::forward<Handlers>(handlers)...};
        using Visitor = decltype(visitor);

        constexpr bool complete = detail::visitor_branch_invocable_v<Visitor, AtomicView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, TupleView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, BundleView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, ListView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, SetView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, MapView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, CyclicBufferView, ValueView> &&
                                  detail::visitor_branch_invocable_v<Visitor, QueueView, ValueView>;

        if constexpr (!complete)
        {
            static_assert(complete, "value visitor must handle every concrete value kind directly or "
                                    "through a ValueView catch-all");
        }
        else
        {
            using Result = detail::visitor_branch_result_t<Visitor, AtomicView, ValueView>;
            static_assert(detail::visitor_result_safe_v<Result>,
                          "value visitor cannot return a reference or lazy hgraph range that may borrow "
                          "from a temporary view; consume the range in the handler or return an owned collection");
            static_assert(detail::visitor_branch_same_result_v<Result, Visitor, TupleView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, BundleView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, ListView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, SetView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, MapView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, CyclicBufferView, ValueView> &&
                              detail::visitor_branch_same_result_v<Result, Visitor, QueueView, ValueView>,
                          "every value visitor branch must return the same type");

            auto current = detail::ValueVisitorAccess::borrow(value);
            for (;;)
            {
                if (!current.valid()) { throw std::invalid_argument("cannot visit a value without a live payload"); }

                const auto kind = current.schema()->try_value_kind();
                if (!kind.has_value()) { throw std::invalid_argument("cannot visit a value with an unknown value kind"); }

                switch (*kind)
                {
                case ValueTypeKind::Atomic:
                    return detail::invoke_value_visitor<AtomicView>(visitor, current);
                case ValueTypeKind::Tuple:
                    return detail::invoke_value_visitor<TupleView>(visitor, current);
                case ValueTypeKind::Bundle:
                    return detail::invoke_value_visitor<BundleView>(visitor, current);
                case ValueTypeKind::List:
                    return detail::invoke_value_visitor<ListView>(visitor, current);
                case ValueTypeKind::Set:
                    return detail::invoke_value_visitor<SetView>(visitor, current);
                case ValueTypeKind::Map:
                    return detail::invoke_value_visitor<MapView>(visitor, current);
                case ValueTypeKind::CyclicBuffer:
                    return detail::invoke_value_visitor<CyclicBufferView>(visitor, current);
                case ValueTypeKind::Queue:
                    return detail::invoke_value_visitor<QueueView>(visitor, current);
                case ValueTypeKind::Any: {
                    auto boxed = current.as_any();
                    if (!boxed.has_value()) { throw std::invalid_argument("cannot visit an empty Any value"); }
                    current = boxed.get();
                    break;
                }
                }
            }
        }
    }
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_VALUE_VISITOR_H
