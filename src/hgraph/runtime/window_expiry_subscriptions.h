#pragma once

#include "window_expiry_service.h"

#include <hgraph/runtime/graph.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_output/base_view.h>

#include <deque>
#include <forward_list>
#include <memory>
#include <optional>
#include <vector>

namespace hgraph
{
    /** A bundle stores only its eligible fields. A list or dictionary stores
        one element plan shared by every live index or slot. */
    struct WindowExpiryBranch
    {
        std::size_t field{};
        std::vector<WindowExpiryBranch> children;
    };

    struct WindowExpiryPath
    {
        std::size_t node;
        GraphEdgeSourceKind source;
        WindowExpiryBranch branch;
    };

    /** Instance policy of a node constructed by GraphBuilder. */
    [[nodiscard]] bool node_replays_output_events(const NodeView &node) noexcept;

    void plan_window_expiry(std::vector<WindowExpiryPath> &paths, const NodeBuilder &node, std::size_t index);

    class WindowExpirySubscription final : public Notifiable
    {
      public:
        WindowExpirySubscription(const GraphView &graph, const TSOutputView &window, bool newly_inserted);
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

    class WindowCollectionSubscription;

    /** The graph owns static leaves in blocks. Dynamic registrations use
        stable addresses and release each slot's subtree on logical removal. */
    struct WindowExpirySubscriptions
    {
        WindowExpirySubscriptions();
        ~WindowExpirySubscriptions();
        void add_window(const GraphView &root, const TSOutputView &window, bool newly_inserted);
        std::optional<std::deque<WindowExpirySubscription>> windows;
        std::forward_list<WindowCollectionSubscription> collections;
    };
    std::unique_ptr<WindowExpirySubscriptions> attach_window_expiry(
        const GraphView &graph, const std::vector<WindowExpiryPath> &paths, DateTime now);
}
