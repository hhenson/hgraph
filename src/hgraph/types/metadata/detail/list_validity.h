#pragma once

#include <cstddef>
#include <limits>

namespace hgraph::ts_data_plan_factory_detail
{
    // A scalar edit nominates one position to check first. Repeated reads of
    // sparse lists reuse a valid witness or the result of finding no other value.
    struct ListValidity
    {
        // Each position occupies addressable storage. Its index fits in the
        // low bits, leaving one bit for the negative result of the last scan.
        static constexpr int index_bits = std::numeric_limits<std::size_t>::digits - 1;
        static constexpr std::size_t unknown = std::numeric_limits<std::size_t>::max() >> 1;

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

        mutable std::size_t candidate : index_bits = unknown;
        mutable std::size_t other_positions_empty : 1 = false;
    };
}
