#ifndef HGRAPH_TYPES_UTILS_COUNTED_MUTEX_H
#define HGRAPH_TYPES_UTILS_COUNTED_MUTEX_H

#include <hgraph/hgraph_export.h>

#include <cstdint>
#include <mutex>

namespace hgraph
{
    namespace detail
    {
        /** Record one type-system lock acquisition (relaxed; diagnostic only). */
        HGRAPH_EXPORT void count_type_system_lock() noexcept;
    }  // namespace detail

    /**
     * Process-lifetime count of type-system lock acquisitions.
     *
     * Build-time machinery (interning, registries, plan factories) is the only
     * sanctioned home for mutexes outside the push-source boundary; the
     * per-tick runtime path must never acquire one (ruling 2026-07-02).  Every
     * such mutex is a TypeSystemMutex, so a wired graph's evaluation leaves
     * this counter unchanged: equal counts for an N-cycle and a 2N-cycle run
     * of the same graph is the enforceable form of the invariant.
     */
    [[nodiscard]] HGRAPH_EXPORT std::uint64_t type_system_lock_count() noexcept;

    /**
     * Mutex wrapper that counts acquisitions in type_system_lock_count().
     *
     * Satisfies Lockable, so std::lock_guard/std::unique_lock work unchanged.
     * The count rides the acquisition attempt path with a relaxed atomic
     * increment; contention behaviour is the wrapped mutex's own.
     */
    template <typename Mutex>
    class CountedTypeSystemMutex
    {
      public:
        void lock()
        {
            detail::count_type_system_lock();
            inner_.lock();
        }

        [[nodiscard]] bool try_lock()
        {
            detail::count_type_system_lock();
            return inner_.try_lock();
        }

        void unlock() { inner_.unlock(); }

      private:
        Mutex inner_;
    };

    using TypeSystemMutex = CountedTypeSystemMutex<std::mutex>;
    using TypeSystemRecursiveMutex = CountedTypeSystemMutex<std::recursive_mutex>;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_UTILS_COUNTED_MUTEX_H
