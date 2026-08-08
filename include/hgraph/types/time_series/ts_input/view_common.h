#ifndef HGRAPH_CPP_TS_INPUT_VIEW_COMMON_H
#define HGRAPH_CPP_TS_INPUT_VIEW_COMMON_H

#include <hgraph/types/time_series/ts_input/detail.h>

namespace hgraph::detail
{
    template <typename T>
    [[nodiscard]] Range<T> empty_input_range() noexcept
    {
        return Range<T>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                        .projector = nullptr};
    }

    template <typename K, typename V>
    [[nodiscard]] KeyValueRange<K, V> empty_input_kv_range() noexcept
    {
        return KeyValueRange<K, V>{.context = nullptr, .memory = nullptr, .limit = 0, .predicate = nullptr,
                                   .projector = nullptr};
    }
}

#endif  // HGRAPH_CPP_TS_INPUT_VIEW_COMMON_H
