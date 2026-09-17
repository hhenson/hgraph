#ifndef HGRAPH_RUNTIME_EXECUTOR_ACTIVITY_H
#define HGRAPH_RUNTIME_EXECUTOR_ACTIVITY_H

#include <hgraph/hgraph_export.h>
#include <hgraph/util/date_time.h>

#include <memory>

namespace hgraph
{
    namespace detail
    {
        struct ExecutorActivityWakeState;
        struct ExecutorActivityAccess;
    }

    /** Owner-thread callbacks for an independently executing owned child.
     * next_time(false) never blocks. next_time(true) settles outstanding work
     * and returns the next requested parent time, or MAX_DT when quiescent.
     * Requested times must be later than the last completed parent time.
     * completed publishes a successfully completed cycle, after notifications.
     * Callbacks may throw; they must not attach/detach activities reentrantly.
     */
    struct ExecutorActivityOps
    {
        DateTime (*next_time)(void *context, bool wait);
        void (*completed)(void *context, DateTime time);
    };

    /** Borrowed activity. The context and ops table must outlive registration.
     * A registered context is non-null and unique within its executor.
     */
    class HGRAPH_CLASS_EXPORT ExecutorActivity
    {
      public:
        ExecutorActivity() noexcept;
        ExecutorActivity(void *context, const ExecutorActivityOps &ops);
        [[nodiscard]] DateTime next_time(bool wait) const;
        void completed(DateTime time) const;
        [[nodiscard]] void *context() const noexcept { return context_; }
        [[nodiscard]] bool operator==(const ExecutorActivity &) const noexcept = default;

      private:
        void *context_{nullptr};
        const ExecutorActivityOps *ops_;
    };

    /** Thread-safe owned notification capability. Wakes the owner to inspect
     * activity requests, without creating an input tick or evaluating a cycle.
     * Copies become inert on detach or executor destruction. Worker threads
     * retain this capability, never a borrowed executor or graph view.
     */
    class HGRAPH_CLASS_EXPORT ExecutorActivityWake
    {
      public:
        ExecutorActivityWake() noexcept = default;
        void notify() const noexcept;

      private:
        friend struct detail::ExecutorActivityAccess;
        std::shared_ptr<detail::ExecutorActivityWakeState> state_{};
    };
}
#endif
