#include <hgraph/types/wiring_observer.h>

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/metadata/value_type_meta_data.h>

#include <fmt/format.h>

namespace hgraph
{
    WiringTypeHandle::WiringTypeHandle(const ValueTypeMetaData *type) noexcept
        : schema(type != nullptr ? &type->header : nullptr), metadata(type)
    {
    }

    WiringTypeHandle::WiringTypeHandle(const TSValueTypeMetaData *type) noexcept
        : schema(type != nullptr ? &type->header : nullptr), metadata(type)
    {
    }

    TypeFamily WiringTypeHandle::family() const noexcept
    {
        return schema != nullptr ? schema->family : TypeFamily::Invalid;
    }

    std::string_view WiringTypeHandle::name() const noexcept
    {
        return schema != nullptr && schema->label != nullptr
                   ? std::string_view{schema->label}
                   : std::string_view{};
    }

    const ValueTypeMetaData *WiringTypeHandle::value_type() const noexcept
    {
        return family() == TypeFamily::Value
                   ? static_cast<const ValueTypeMetaData *>(metadata)
                   : nullptr;
    }

    const TSValueTypeMetaData *WiringTypeHandle::time_series_type() const noexcept
    {
        return family() == TypeFamily::TimeSeries
                   ? static_cast<const TSValueTypeMetaData *>(metadata)
                   : nullptr;
    }

    namespace
    {
        [[nodiscard]] std::string render_path(std::span<const std::string> path)
        {
            std::string rendered;
            for (const std::string &component : path)
            {
                if (!rendered.empty()) { rendered += "/"; }
                rendered += component;
            }
            return rendered;
        }

        [[nodiscard]] std::string_view scope_name(WiringScopeKind kind) noexcept
        {
            switch (kind)
            {
                case WiringScopeKind::Graph: return "graph";
                case WiringScopeKind::NestedGraph: return "nested graph";
                case WiringScopeKind::Node: return "node";
            }
            return "scope";
        }
    }

    WiringTracer::WiringTracer(std::string filter, bool graph, bool node)
        : filter_(std::move(filter)), graph_(graph), node_(node)
    {
    }

    bool WiringTracer::accepts(std::span<const std::string> path) const
    {
        return filter_.empty() || render_path(path).contains(filter_);
    }

    void WiringTracer::append_scope(std::string_view action,
                                    const WiringScopeEvent &event,
                                    std::string_view error)
    {
        if (!accepts(event.path)) { return; }
        const std::string path = render_path(event.path);
        std::string line = fmt::format("{} wiring {} {}", action,
                                       scope_name(event.kind), path);
        if (!event.signature.empty() && event.signature != event.label)
        {
            line += fmt::format(" [{}]", event.signature);
        }
        if (!error.empty()) { line += fmt::format(": {}", error); }
        lines_.push_back(std::move(line));
    }

    void WiringTracer::on_enter_graph_wiring(const WiringScopeEvent &event)
    {
        if (graph_) { append_scope("Begin", event); }
    }

    void WiringTracer::on_exit_graph_wiring(const WiringScopeEvent &event,
                                            std::string_view error)
    {
        if (graph_) { append_scope(error.empty() ? "Completed" : "Failed", event, error); }
    }

    void WiringTracer::on_enter_nested_graph_wiring(const WiringScopeEvent &event)
    {
        if (graph_) { append_scope("Begin", event); }
    }

    void WiringTracer::on_exit_nested_graph_wiring(const WiringScopeEvent &event,
                                                   std::string_view error)
    {
        if (graph_) { append_scope(error.empty() ? "Completed" : "Failed", event, error); }
    }

    void WiringTracer::on_enter_node_wiring(const WiringScopeEvent &event)
    {
        if (node_) { append_scope("Begin", event); }
    }

    void WiringTracer::on_exit_node_wiring(const WiringScopeEvent &event,
                                           std::string_view error)
    {
        if (node_) { append_scope(error.empty() ? "Completed" : "Failed", event, error); }
    }

    void WiringTracer::on_overload_resolution(const WiringResolutionEvent &event)
    {
        if (!node_ || !accepts(event.path)) { return; }
        const std::string path = render_path(event.path);
        if (event.selected.has_value())
        {
            lines_.push_back(fmt::format(
                "Resolved operator {} at {} to {} [rank {}]",
                event.operator_name, path, event.selected->label,
                event.selected->rank));
        }
        else
        {
            lines_.push_back(fmt::format("Failed to resolve operator {} at {}: {}",
                                         event.operator_name, path, event.error));
        }
        for (const auto &candidate : event.ambiguous)
        {
            lines_.push_back(fmt::format("  Ambiguous {} [rank {}]",
                                         candidate.label, candidate.rank));
        }
        for (const auto &candidate : event.rejected)
        {
            lines_.push_back(fmt::format("  Rejected {} [rank {}]: {}",
                                         candidate.label, candidate.rank,
                                         candidate.rejection_reason));
        }
    }
}
