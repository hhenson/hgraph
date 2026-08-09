#ifndef HGRAPH_CPP_ROOT_TYPES_DETAIL_VISITOR_H
#define HGRAPH_CPP_ROOT_TYPES_DETAIL_VISITOR_H

#include <hgraph/types/value/value_range.h>

#include <concepts>
#include <type_traits>

namespace hgraph::detail
{
    template <typename... Handlers> struct VisitorOverload : Handlers...
    {
        using Handlers::operator()...;
    };

    template <typename... Handlers> VisitorOverload(Handlers...) -> VisitorOverload<Handlers...>;

    template <typename Visitor, typename SpecialisedView, typename BaseView>
    inline constexpr bool visitor_branch_invocable_v =
        std::invocable<Visitor &, SpecialisedView> || std::invocable<Visitor &, BaseView>;

    template <typename Visitor, typename SpecialisedView, typename BaseView,
              bool HasSpecialised = std::invocable<Visitor &, SpecialisedView>>
    struct VisitorBranchResult;

    template <typename Visitor, typename SpecialisedView, typename BaseView>
    struct VisitorBranchResult<Visitor, SpecialisedView, BaseView, true>
    {
        using type = std::invoke_result_t<Visitor &, SpecialisedView>;
    };

    template <typename Visitor, typename SpecialisedView, typename BaseView>
    struct VisitorBranchResult<Visitor, SpecialisedView, BaseView, false>
    {
        using type = std::invoke_result_t<Visitor &, BaseView>;
    };

    template <typename Visitor, typename SpecialisedView, typename BaseView>
    using visitor_branch_result_t = typename VisitorBranchResult<Visitor, SpecialisedView, BaseView>::type;

    template <typename Result, typename Visitor, typename SpecialisedView, typename BaseView>
    inline constexpr bool visitor_branch_same_result_v =
        std::same_as<Result, visitor_branch_result_t<Visitor, SpecialisedView, BaseView>>;

    template <typename Result> struct VisitorBorrowedRangeResult : std::false_type
    {
    };

    template <typename Value> struct VisitorBorrowedRangeResult<Range<Value>> : std::true_type
    {
    };

    template <typename Key, typename Value> struct VisitorBorrowedRangeResult<KeyValueRange<Key, Value>> : std::true_type
    {
    };

    template <typename Result>
    inline constexpr bool visitor_result_safe_v =
        !std::is_reference_v<Result> && !VisitorBorrowedRangeResult<std::remove_cv_t<Result>>::value;
}  // namespace hgraph::detail

#endif  // HGRAPH_CPP_ROOT_TYPES_DETAIL_VISITOR_H
