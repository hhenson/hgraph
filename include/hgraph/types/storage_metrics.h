#ifndef HGRAPH_TYPES_STORAGE_METRICS_H
#define HGRAPH_TYPES_STORAGE_METRICS_H

#include <cstddef>

namespace hgraph
{
    /** Occupied and retained bytes owned by a dynamically allocated storage surface. */
    struct DynamicStorageMetrics
    {
        /** Bytes occupied by live elements and their required indexing metadata. */
        std::size_t live_bytes{0};
        /** Complete byte capacity retained by the owner for reuse. */
        std::size_t reserved_bytes{0};

        constexpr DynamicStorageMetrics &operator+=(const DynamicStorageMetrics &other) noexcept
        {
            live_bytes += other.live_bytes;
            reserved_bytes += other.reserved_bytes;
            return *this;
        }
    };

    [[nodiscard]] constexpr DynamicStorageMetrics operator+(
        DynamicStorageMetrics lhs, const DynamicStorageMetrics &rhs) noexcept
    {
        lhs += rhs;
        return lhs;
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_STORAGE_METRICS_H
