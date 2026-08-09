#ifndef HGRAPH_CPP_TS_OUTPUT_VIEW_COMMON_H
#define HGRAPH_CPP_TS_OUTPUT_VIEW_COMMON_H

#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <stdexcept>
#include <string>

namespace hgraph::detail
{
    inline void validate_output_view_kind(const TSValueTypeMetaData *schema, TSTypeKind expected, const char *what)
    {
        if (schema == nullptr || schema->kind != expected)
        {
            throw std::invalid_argument(std::string{what} + " requires a matching time-series shape");
        }
    }

    template <typename T>
    [[nodiscard]] Range<T> empty_output_range() noexcept
    {
        return Range<T>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                        .projector = nullptr};
    }

    template <typename K, typename V>
    [[nodiscard]] KeyValueRange<K, V> empty_output_kv_range() noexcept
    {
        return KeyValueRange<K, V>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                   .projector = nullptr};
    }
}

#endif  // HGRAPH_CPP_TS_OUTPUT_VIEW_COMMON_H
