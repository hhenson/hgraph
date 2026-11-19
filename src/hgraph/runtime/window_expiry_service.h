#pragma once

#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

namespace hgraph
{
    class GraphView;
    class WindowExpiryService;

    /** One live, owned duration window. Its address is stable until detachment. */
    class WindowExpiryAlarm
    {
      public:
        using Expire = void (*)(void *, DateTime);
        using Refresh = void (*)(void *);

        WindowExpiryAlarm(const GraphView &graph, void *subscriber, Expire expire, Refresh refresh);
        ~WindowExpiryAlarm();
        WindowExpiryAlarm(const WindowExpiryAlarm &) = delete;
        WindowExpiryAlarm &operator=(const WindowExpiryAlarm &) = delete;

        /** MAX_DT disarms the alarm. */
        void set_deadline(DateTime when);
        /** Re-read the final window state after same-cycle writes have coalesced. */
        void request_refresh() noexcept;

      private:
        friend class WindowExpiryService;
        static constexpr auto unarmed = std::numeric_limits<std::size_t>::max();
        std::weak_ptr<WindowExpiryService> service_;
        void *subscriber_;
        Expire expire_;
        Refresh refresh_;
        std::uint64_t sequence_;
        DateTime deadline_{MAX_DT};
        std::size_t heap_index_{unarmed};
        WindowExpiryAlarm *dirty_prev_{nullptr};
        WindowExpiryAlarm *dirty_next_{nullptr};
        bool dirty_{false};
    };

    /** Root pull source. The root owns the only strong reference. All mutations
        and callbacks run on its evaluation thread. */
    class WindowExpiryService
    {
      public:
        [[nodiscard]] DateTime next_time(DateTime graph_time) const noexcept;
        void evaluate(DateTime now);
        void refresh();
        void finish_cycle(DateTime now);

      private:
        friend class WindowExpiryAlarm;
        /** An indexed binary heap contains exactly the armed windows. Updating
            or removing a deadline cannot leave an old entry or reused address. */
        std::vector<WindowExpiryAlarm *> heap_;
        WindowExpiryAlarm *dirty_head_{nullptr};
        WindowExpiryAlarm *dirty_tail_{nullptr};
        std::uint64_t next_sequence_{0};
        DateTime finished_time_{MIN_DT};
        DateTime pulling_time_{MIN_DT};

        static bool earlier(const WindowExpiryAlarm &left, const WindowExpiryAlarm &right) noexcept;
        void set_deadline(WindowExpiryAlarm &alarm, DateTime when);
        void remove(WindowExpiryAlarm &alarm) noexcept;
        void reorder(std::size_t index) noexcept;
        void mark_dirty(WindowExpiryAlarm &alarm) noexcept;
        void unmark_dirty(WindowExpiryAlarm &alarm) noexcept;
    };

    /** Executors flush duration deadlines after all end-of-cycle callbacks have
        drained. A directly evaluated root closes the cycle after its observers. */
    [[nodiscard]] bool evaluate_root_nodes(const GraphView &root, DateTime now);
    void finish_root_window_expiry(const GraphView &root, DateTime now);

    /** Bind on first eligible owner. Nested graphs resolve the root's reference. */
    [[nodiscard]] std::weak_ptr<WindowExpiryService> root_window_expiry_service(const GraphView &graph);
}
