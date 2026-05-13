#pragma once

/**
 * @file visitor.h
 * @brief Callable-based visitor utilities for the active value/view runtime.
 *
 * The visitor surface dispatches on the logical value kind and presents the
 * corresponding typed view wrapper to the matching handler. This keeps the
 * calling code readable without reintroducing the old schema ops table.
 */

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/sequence.h>

#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph
{

    /**
     * Combine multiple handlers into one overloaded callable.
     */
    template <typename... Fs>
    struct overloaded : Fs...
    {
        using Fs::operator()...;
    };

    template <typename... Fs>
    overloaded(Fs...) -> overloaded<Fs...>;

    /**
     * Visit a value-layer view with handlers for the supported concrete view
     * types. Atomic values are presented as `View`, with composite kinds
     * projected to their concrete wrapper types.
     */
    template <typename... Handlers>
    auto visit(View view, Handlers &&...handlers)
    {
        auto visitor = overloaded{std::forward<Handlers>(handlers)...};

        switch (view.schema()->kind) {
            case value::TypeKind::Atomic:
                return visitor(view.as_atomic());
            case value::TypeKind::Tuple:
                return visitor(view.as_tuple());
            case value::TypeKind::Bundle:
                return visitor(view.as_bundle());
            case value::TypeKind::List:
                return visitor(view.as_list());
            case value::TypeKind::Set:
                return visitor(view.as_set());
            case value::TypeKind::Map:
                return visitor(view.as_map());
            case value::TypeKind::CyclicBuffer:
                return visitor(view.as_cyclic_buffer());
            case value::TypeKind::Queue:
                return visitor(view.as_queue());
            default:
                return visitor(view);
        }
    }

    template <value::TypeKind K, typename F>
    struct WhenCase
    {
        F handler;

        template <typename R>
        bool try_match(View view, R &result) const
        {
            if (view.schema()->kind != K) { return false; }

            if constexpr (K == value::TypeKind::Atomic) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_atomic());
                } else {
                    result = handler(view);
                }
            } else if constexpr (K == value::TypeKind::Tuple) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_tuple());
                } else {
                    result = handler(view.as_tuple());
                }
            } else if constexpr (K == value::TypeKind::Bundle) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_bundle());
                } else {
                    result = handler(view.as_bundle());
                }
            } else if constexpr (K == value::TypeKind::List) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_list());
                } else {
                    result = handler(view.as_list());
                }
            } else if constexpr (K == value::TypeKind::Set) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_set());
                } else {
                    result = handler(view.as_set());
                }
            } else if constexpr (K == value::TypeKind::Map) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_map());
                } else {
                    result = handler(view.as_map());
                }
            } else if constexpr (K == value::TypeKind::CyclicBuffer) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_cyclic_buffer());
                } else {
                    result = handler(view.as_cyclic_buffer());
                }
            } else if constexpr (K == value::TypeKind::Queue) {
                if constexpr (std::is_void_v<R>) {
                    handler(view.as_queue());
                } else {
                    result = handler(view.as_queue());
                }
            }
            return true;
        }
    };

    template <value::TypeKind K, typename F>
    WhenCase<K, F> when(F &&handler)
    {
        return WhenCase<K, F>{std::forward<F>(handler)};
    }

    template <typename F>
    struct OtherwiseCase
    {
        F handler;

        template <typename R>
        bool try_match(View view, R &result) const
        {
            if constexpr (std::is_void_v<R>) {
                handler(view);
            } else {
                result = handler(view);
            }
            return true;
        }
    };

    template <typename F>
    OtherwiseCase<F> otherwise(F &&handler)
    {
        return OtherwiseCase<F>{std::forward<F>(handler)};
    }

    namespace detail
    {

        template <typename R, typename Case, typename... Rest>
        bool try_match_cases(View view, R &result, const Case &c, const Rest &...rest)
        {
            if (c.template try_match<R>(view, result)) { return true; }
            if constexpr (sizeof...(Rest) > 0) { return try_match_cases<R>(view, result, rest...); }
            return false;
        }

    } // namespace detail

    /**
     * Declarative matching over value kinds using `when(...)` / `otherwise(...)`.
     */
    template <typename R, typename... Cases>
    R match(View view, const Cases &...cases)
    {
        R result{};
        if (!detail::try_match_cases<R>(view, result, cases...)) {
            throw std::runtime_error("match: no case matched for value type");
        }
        return result;
    }

} // namespace hgraph
