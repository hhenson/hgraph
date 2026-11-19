#pragma once

#include "window_expiry_service.h"

#include <hgraph/runtime/graph.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_output/base_view.h>

#include <deque>
#include <memory>
#include <optional>
#include <vector>

namespace hgraph
{
    /** Planned once per graph definition. A fixed-list step represents every
        index, so the size of the plan is independent of the list's capacity. */
    struct WindowExpiryPath
    {
        std::size_t node;
        GraphEdgeSourceKind source;
        std::vector<std::size_t> steps;
    };

    /** Instance policy of a node constructed by GraphBuilder. */
    [[nodiscard]] bool node_replays_output_events(const NodeView &node) noexcept;

    void plan_window_expiry(std::vector<WindowExpiryPath> &paths, const NodeBuilder &node, std::size_t index);

    class WindowExpirySubscription final : public Notifiable
    {
      public:
        WindowExpirySubscription(const GraphView &graph, const TSOutputView &window);
        ~WindowExpirySubscription() override;
        WindowExpirySubscription(const WindowExpirySubscription &) = delete;
        WindowExpirySubscription &operator=(const WindowExpirySubscription &) = delete;
        void notify(DateTime now) override;
        void source_invalidated(const TSDataTracking *) noexcept override;

      private:
        TSOutputHandle output_;
        std::optional<WindowExpiryAlarm> alarm_;
        void refresh();
        void expire(DateTime now);
    };

    /** Stable subscriber addresses for one graph instance. Only allocated
        for instances that actually own a statically addressable duration window. */
    using WindowExpirySubscriptions = std::deque<WindowExpirySubscription>;
    std::unique_ptr<WindowExpirySubscriptions> attach_window_expiry(
        const GraphView &graph, const std::vector<WindowExpiryPath> &paths, DateTime now);
}
