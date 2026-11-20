#pragma once

#include <cstddef>
#include <limits>

namespace hgraph::ts_data_plan_factory_detail
{
    // A scalar edit nominates one position to check first. Repeated reads of
    // sparse lists reuse a valid witness or the result of finding no other value.
    struct ListValidity
    {
        static constexpr auto unknown = std::numeric_limits<std::size_t>::max();

        void changed(std::size_t index = unknown) noexcept
        {
            candidate = index;
            other_positions_empty = false;
        }

        template <typename Valid>
        bool any(std::size_t size, Valid valid) const
        {
            if (candidate < size && valid(candidate)) { return true; }
            if (other_positions_empty) { return false; }
            for (std::size_t i = 0; i < size; ++i) {
                if (i != candidate && valid(i)) {
                    candidate = i;
                    return true;
                }
            }
            other_positions_empty = true;
            return false;
        }

        mutable std::size_t candidate{unknown};
        mutable bool other_positions_empty{false};
    };
}
