#include <hgraph/types/utils/counted_mutex.h>

#include <atomic>

namespace hgraph
{
    namespace
    {
        std::atomic<std::uint64_t> &counter() noexcept
        {
            static std::atomic<std::uint64_t> value{0};
            return value;
        }
    }  // namespace

    namespace detail
    {
        void count_type_system_lock() noexcept
        {
            counter().fetch_add(1, std::memory_order_relaxed);
        }
    }  // namespace detail

    std::uint64_t type_system_lock_count() noexcept
    {
        return counter().load(std::memory_order_relaxed);
    }
}  // namespace hgraph
