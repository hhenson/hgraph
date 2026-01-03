#pragma once

/**
 * @file visitor.h
 * @brief Callable-based visitor pattern for runtime-typed Value structures.
 *
 * Provides flexible runtime dispatch based on TypeKind without static
 * dependencies on specific scalar types.
 *
 * Key design principles:
 * - No static dependency on specific scalar types
 * - Dispatch based on TypeKind (Scalar, Tuple, Bundle, List, Set, Map, CyclicBuffer, Queue)
 * - Scalar values are passed as ConstValueView - caller can check type if needed
 * - Overloaded handlers combined into single visitor
 *
 * Reference: ts_design_docs/Value_USER_GUIDE.md Section 8
 *
 * Usage:
 * @code
 * // Visit with type-specific handlers
 * std::string result = visit(value.const_view(),
 *     [](ConstValueView v) { return "scalar: " + v.to_string(); },
 *     [](ConstTupleView t) { return "tuple[" + std::to_string(t.size()) + "]"; },
 *     [](ConstBundleView b) { return "bundle"; },
 *     [](ConstListView l) { return "list"; },
 *     [](ConstSetView s) { return "set"; },
 *     [](ConstMapView m) { return "map"; }
 * );
 *
 * // Partial handlers with catch-all
 * std::string result = visit(value.const_view(),
 *     [](ConstListView l) { return "list[" + std::to_string(l.size()) + "]"; },
 *     [](ConstMapView m) { return "map"; },
 *     [](ConstValueView v) { return "other: " + v.to_string(); }  // catch-all
 * );
 * @endcode
 */

#include <hgraph/types/value/value_view.h>

#include <type_traits>
#include <utility>

namespace hgraph::value {

// ============================================================================
// Overloaded Pattern
// ============================================================================

/**
 * @brief Combines multiple callables into a single overloaded callable.
 */
template<typename... Fs>
struct overloaded : Fs... {
    using Fs::operator()...;
};

template<typename... Fs>
overloaded(Fs...) -> overloaded<Fs...>;

// ============================================================================
// Visit
// ============================================================================

/**
 * @brief Visit a Value with type-specific handlers.
 *
 * Combines handlers using the overloaded pattern and dispatches based on
 * TypeKind. Each handler should accept the appropriate view type:
 * - Scalar: ConstValueView (use is_scalar_type<T>() to check specific types)
 * - Tuple: ConstTupleView
 * - Bundle: ConstBundleView
 * - List: ConstListView
 * - Set: ConstSetView
 * - Map: ConstMapView
 * - CyclicBuffer: ConstCyclicBufferView
 * - Queue: ConstQueueView
 *
 * A handler accepting ConstValueView can serve as a catch-all for unhandled types.
 *
 * @tparam Handlers Callable types
 * @param view The value to visit
 * @param handlers Callables that handle specific view types
 * @return Result of the matching handler
 */
template<typename... Handlers>
auto visit(ConstValueView view, Handlers&&... handlers) {
    auto visitor = overloaded{std::forward<Handlers>(handlers)...};

    switch (view.schema()->kind) {
        case TypeKind::Scalar:
            return visitor(view);
        case TypeKind::Tuple:
            return visitor(view.as_tuple());
        case TypeKind::Bundle:
            return visitor(view.as_bundle());
        case TypeKind::List:
            return visitor(view.as_list());
        case TypeKind::Set:
            return visitor(view.as_set());
        case TypeKind::Map:
            return visitor(view.as_map());
        case TypeKind::CyclicBuffer:
            return visitor(view.as_cyclic_buffer());
        case TypeKind::Queue:
            return visitor(view.as_queue());
        default:
            return visitor(view);  // Fall back to ConstValueView handler
    }
}

/**
 * @brief Visit a mutable Value with type-specific handlers.
 */
template<typename... Handlers>
auto visit(ValueView view, Handlers&&... handlers) {
    auto visitor = overloaded{std::forward<Handlers>(handlers)...};

    switch (view.schema()->kind) {
        case TypeKind::Scalar:
            return visitor(view);
        case TypeKind::Tuple:
            return visitor(view.as_tuple());
        case TypeKind::Bundle:
            return visitor(view.as_bundle());
        case TypeKind::List:
            return visitor(view.as_list());
        case TypeKind::Set:
            return visitor(view.as_set());
        case TypeKind::Map:
            return visitor(view.as_map());
        case TypeKind::CyclicBuffer:
            return visitor(view.as_cyclic_buffer());
        case TypeKind::Queue:
            return visitor(view.as_queue());
        default:
            return visitor(view);
    }
}

// ============================================================================
// Match Pattern (Declarative API)
// ============================================================================

/**
 * @brief Match on TypeKind using a declarative pattern.
 *
 * @code
 * auto result = match<std::string>(view,
 *     when<TypeKind::Scalar>([](ConstValueView v) { return v.to_string(); }),
 *     when<TypeKind::List>([](ConstListView l) { return "list"; }),
 *     otherwise([](ConstValueView) { return "other"; })
 * );
 * @endcode
 */

template<TypeKind K, typename F>
struct WhenCase {
    F handler;

    template<typename R>
    bool try_match(ConstValueView view, R& result) const {
        if (view.schema()->kind == K) {
            if constexpr (K == TypeKind::Scalar) {
                if constexpr (std::is_void_v<R>) {
                    handler(view);
                } else {
                    result = handler(view);
                }
            } else if constexpr (K == TypeKind::Tuple) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_tuple());
                } else {
                    result = handler(view.as_tuple());
                }
            } else if constexpr (K == TypeKind::Bundle) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_bundle());
                } else {
                    result = handler(view.as_bundle());
                }
            } else if constexpr (K == TypeKind::List) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_list());
                } else {
                    result = handler(view.as_list());
                }
            } else if constexpr (K == TypeKind::Set) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_set());
                } else {
                    result = handler(view.as_set());
                }
            } else if constexpr (K == TypeKind::Map) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_map());
                } else {
                    result = handler(view.as_map());
                }
            } else if constexpr (K == TypeKind::CyclicBuffer) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_cyclic_buffer());
                } else {
                    result = handler(view.as_cyclic_buffer());
                }
            } else if constexpr (K == TypeKind::Queue) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_queue());
                } else {
                    result = handler(view.as_queue());
                }
            }
            return true;
        }
        return false;
    }
};

template<TypeKind K, typename F>
WhenCase<K, F> when(F&& handler) {
    return WhenCase<K, F>{std::forward<F>(handler)};
}

template<typename F>
struct OtherwiseCase {
    F handler;

    template<typename R>
    bool try_match(ConstValueView view, R& result) const {
        if constexpr (std::is_void_v<R>) {
            handler(view);
        } else {
            result = handler(view);
        }
        return true;
    }
};

template<typename F>
OtherwiseCase<F> otherwise(F&& handler) {
    return OtherwiseCase<F>{std::forward<F>(handler)};
}

namespace detail {

template<typename R, typename Case, typename... Rest>
bool try_match_cases(ConstValueView view, R& result, const Case& c, const Rest&... rest) {
    if (c.template try_match<R>(view, result)) {
        return true;
    }
    if constexpr (sizeof...(Rest) > 0) {
        return try_match_cases<R>(view, result, rest...);
    }
    return false;
}

} // namespace detail

/**
 * @brief Match on a Value with when/otherwise cases.
 */
template<typename R, typename... Cases>
R match(ConstValueView view, const Cases&... cases) {
    R result{};
    if (!detail::try_match_cases<R>(view, result, cases...)) {
        throw std::runtime_error("match: no case matched for value type");
    }
    return result;
}

} // namespace hgraph::value
