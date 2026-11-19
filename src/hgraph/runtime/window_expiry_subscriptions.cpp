#include "window_expiry_subscriptions.h"

#include <hgraph/types/time_series/ts_output.h>
#include <span>

namespace hgraph
{
    namespace
    {
        constexpr auto each_list_element = unbounded_tsl_size;

        void append_paths(std::vector<WindowExpiryPath> &paths, WindowExpiryPath &path,
                          const TSValueTypeMetaData *schema)
        {
            if (schema->is_duration_based()) { paths.push_back(path); }
            else if (schema->kind == TSTypeKind::TSB)
            {
                for (std::size_t i = 0; i < schema->field_count(); ++i)
                {
                    path.steps.push_back(i);
                    append_paths(paths, path, schema->fields()[i].type);
                    path.steps.pop_back();
                }
            }
            else if (schema->kind == TSTypeKind::TSL && !schema->is_unbounded_tsl() && schema->fixed_size() > 0)
            {
                path.steps.push_back(each_list_element);
                append_paths(paths, path, schema->element_ts());
                path.steps.pop_back();
            }
        }

        void attach_path(const GraphView &graph, const TSOutputView &output, std::span<const std::size_t> steps,
                         std::unique_ptr<WindowExpirySubscriptions> &subscriptions)
        {
            // Forwarding outputs borrow another producer's history. The owner
            // remains responsible even when the forwarding target is changed.
            if (output.forwarding()) { return; }
            if (steps.empty())
            {
                if (!subscriptions) { subscriptions = std::make_unique<WindowExpirySubscriptions>(); }
                subscriptions->emplace_back(graph, output);
                return;
            }
            if (steps.front() == each_list_element)
            {
                for (std::size_t i = 0; i < output.schema()->fixed_size(); ++i)
                {
                    attach_path(graph, output.indexed_child_at(i), steps.subspan(1), subscriptions);
                }
            }
            else { attach_path(graph, output.indexed_child_at(steps.front()), steps.subspan(1), subscriptions); }
        }
    }

    void plan_window_expiry(std::vector<WindowExpiryPath> &paths, const NodeBuilder &node, std::size_t index)
    {
        const auto &schema = *node.type().schema();
        // The graph type and its path plan can be shared by instances with
        // different replay policies or forwarding endpoints. Ordinary schemas
        // need no path storage. Walk each eligible output tree once to emit paths.
        auto plan = [&](GraphEdgeSourceKind source, const TSValueTypeMetaData *endpoint) {
            if (endpoint && endpoint->contains_fixed_duration_window())
            {
                WindowExpiryPath path{index, source, {}};
                append_paths(paths, path, endpoint);
            }
        };
        plan(GraphEdgeSourceKind::Output, schema.output_schema);
        plan(GraphEdgeSourceKind::ErrorOutput, schema.error_output_schema);
        plan(GraphEdgeSourceKind::RecordableState, schema.recordable_state_schema);
    }

    std::unique_ptr<WindowExpirySubscriptions> attach_window_expiry(
        const GraphView &graph, const std::vector<WindowExpiryPath> &paths, DateTime now)
    {
        std::unique_ptr<WindowExpirySubscriptions> subscriptions;
        if (paths.empty()) { return subscriptions; }
        const auto root = graph.root();
        for (const auto &path : paths)
        {
            auto node = graph.node_at(path.node);
            switch (path.source)
            {
                case GraphEdgeSourceKind::Output:
                    if (!node_replays_output_events(node))
                    {
                        attach_path(root, node.output(now), path.steps, subscriptions);
                    }
                    break;
                case GraphEdgeSourceKind::ErrorOutput:
                    attach_path(root, node.error_output(now), path.steps, subscriptions);
                    break;
                case GraphEdgeSourceKind::RecordableState:
                    attach_path(root, node.recordable_state(now), path.steps, subscriptions);
                    break;
            }
        }
        return subscriptions;
    }

    WindowExpirySubscription::WindowExpirySubscription(const GraphView &graph, const TSOutputView &window)
        : output_(window.handle()),
          alarm_(std::in_place, graph, this,
                 [](void *self, DateTime now) { static_cast<WindowExpirySubscription *>(self)->expire(now); },
                 [](void *self) { static_cast<WindowExpirySubscription *>(self)->refresh(); })
    {
        refresh();
        window.subscribe(this);
    }

    WindowExpirySubscription::~WindowExpirySubscription()
    {
        if (output_.bound()) { output_.data_view().unsubscribe(this); }
    }

    void WindowExpirySubscription::source_invalidated(const TSDataTracking *) noexcept
    {
        output_.reset();
        alarm_.reset();
    }

    void WindowExpirySubscription::refresh()
    {
        auto data = output_.data_view();
        alarm_->set_deadline(data.as_window().next_expiry_time());
    }

    void WindowExpirySubscription::notify(DateTime)
    {
        // The observer fires only on the first modification at a given time.
        // A later clear or append in this same cycle may change the deadline.
        refresh();
        alarm_->request_refresh();
    }

    void WindowExpirySubscription::expire(DateTime now)
    {
        auto data = output_.data_view();
        auto window = data.as_window();
        window.begin_mutation(now).expire();
    }
}
