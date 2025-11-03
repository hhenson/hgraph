#pragma once

#include "any_value.h"
#include <type_traits>

namespace hgraph
{
    /**
     * @brief Helper utilities for converting between typed values and AnyValue<>
     *
     * These are used by TimeSeriesValueOutput<T>/Input<T> to delegate to
     * the type-erased TSOutput/TSInput implementation.
     */

    /**
     * @brief Emplace a typed value into an AnyValue<>
     *
     * @tparam T The value type
     * @param any The AnyValue to populate
     * @param value The value to store
     */
    template <typename T>
    inline void emplace_any(AnyValue<>& any, const T& value)
    {
        any.emplace<T>(value);
    }

    /**
     * @brief Emplace a typed value into an AnyValue<> (move version)
     *
     * @tparam T The value type
     * @param any The AnyValue to populate
     * @param value The value to store (will be moved)
     */
    template <typename T>
    inline void emplace_any(AnyValue<>& any, T&& value)
    {
        any.emplace<T>(std::forward<T>(value));
    }

    /**
     * @brief Extract a typed value from an AnyValue<>
     *
     * @tparam T The value type
     * @param any The AnyValue to extract from
     * @return const T& Reference to the stored value
     * @throws std::bad_cast if T doesn't match the stored type
     */
    template <typename T>
    inline const T& get_from_any(const AnyValue<>& any)
    {
        const T* ptr = any.get_if<T>();
        if (!ptr)
        {
            throw std::bad_cast();
        }
        return *ptr;
    }

    /**
     * @brief Create an AnyValue<> from a typed value (convenience)
     *
     * @tparam T The value type (deduced)
     * @param value The value to store
     * @return AnyValue<> containing the value
     */
    template <typename T>
    inline AnyValue<> make_any_value(const T& value)
    {
        AnyValue<> any;
        any.emplace<T>(value);
        return any;
    }

    /**
     * @brief Create an AnyValue<> from a typed value (move version)
     *
     * @tparam T The value type (deduced)
     * @param value The value to store (will be moved)
     * @return AnyValue<> containing the value
     */
    template <typename T>
    inline AnyValue<> make_any_value(T&& value)
    {
        AnyValue<> any;
        any.emplace<T>(std::forward<T>(value));
        return any;
    }
} // namespace hgraph