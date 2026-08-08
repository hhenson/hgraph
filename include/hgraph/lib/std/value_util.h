#ifndef HGRAPH_LIB_STD_VALUE_UTIL_H
#define HGRAPH_LIB_STD_VALUE_UTIL_H

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

#include <concepts>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph::stdlib
{
    /**
     * Small value-layer construction helpers for ordinary scalar containers.
     *
     * These functions are the convenient public counterpart to the lower-level
     * ``ListBuilder`` / ``SetBuilder`` / ``MapBuilder`` APIs. They use the static
     * scalar descriptors to register/find canonical scalar bindings, then produce
     * immutable compact ``Value`` containers with the standard value-layer schema.
     */

    template <typename T>
    [[nodiscard]] ValueTypeRef scalar_value_binding()
    {
        const auto *meta    = scalar_descriptor<std::remove_cvref_t<T>>::value_meta();
        const auto binding = ValuePlanFactory::instance().type_for(meta);
        if (binding == nullptr) { throw std::logic_error("scalar value type has no canonical binding"); }
        return binding;
    }

    template <typename T, typename U>
    requires std::constructible_from<std::remove_cvref_t<T>, U &&>
    [[nodiscard]] Value value(U &&input)
    {
        using ValueT = std::remove_cvref_t<T>;
        return Value{ValueT{std::forward<U>(input)}};
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_list(TIterator first, TSentinel last)
    {
        using ValueT = std::remove_cvref_t<T>;
        ListBuilder builder{scalar_value_binding<ValueT>()};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            builder.push_back(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_list(std::initializer_list<T> values)
    {
        return make_list<std::remove_cv_t<T>>(values.begin(), values.end());
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_set(TIterator first, TSentinel last)
    {
        using ValueT = std::remove_cvref_t<T>;
        SetBuilder builder{scalar_value_binding<ValueT>()};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            (void)builder.insert(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_set(std::initializer_list<T> values)
    {
        return make_set<std::remove_cv_t<T>>(values.begin(), values.end());
    }

    template <typename TKey, typename TValue, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    [[nodiscard]] Value make_map(TIterator first, TSentinel last)
    {
        using KeyT   = std::remove_cvref_t<TKey>;
        using ValueT = std::remove_cvref_t<TValue>;

        MapBuilder builder{scalar_value_binding<KeyT>(), scalar_value_binding<ValueT>()};
        for (; first != last; ++first)
        {
            KeyT   key{(*first).first};
            ValueT value{(*first).second};
            builder.set_item(key, value);
        }
        return builder.build();
    }

    template <typename TKey, typename TValue>
    [[nodiscard]] Value make_map(std::initializer_list<std::pair<TKey, TValue>> values)
    {
        return make_map<TKey, TValue>(values.begin(), values.end());
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_cyclic_buffer(std::size_t capacity, TIterator first, TSentinel last)
    {
        using ValueT = std::remove_cvref_t<T>;
        CyclicBufferBuilder builder{scalar_value_binding<ValueT>(), capacity};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            builder.push_back(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_cyclic_buffer(std::size_t capacity, std::initializer_list<T> values)
    {
        return make_cyclic_buffer<std::remove_cv_t<T>>(capacity, values.begin(), values.end());
    }

    template <typename T, std::input_iterator TIterator, std::sentinel_for<TIterator> TSentinel>
    requires std::constructible_from<std::remove_cvref_t<T>, std::iter_reference_t<TIterator>>
    [[nodiscard]] Value make_queue(TIterator first, TSentinel last, std::size_t max_capacity = 0)
    {
        using ValueT = std::remove_cvref_t<T>;
        QueueBuilder builder{scalar_value_binding<ValueT>(), max_capacity};
        for (; first != last; ++first)
        {
            ValueT value{*first};
            builder.push(value);
        }
        return builder.build();
    }

    template <typename T>
    [[nodiscard]] Value make_queue(std::initializer_list<T> values, std::size_t max_capacity = 0)
    {
        return make_queue<std::remove_cv_t<T>>(values.begin(), values.end(), max_capacity);
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_VALUE_UTIL_H
