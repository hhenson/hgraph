#include <hgraph/types/wiring_inspection.h>
#include <hgraph/types/subgraph_wiring.h>
#include <deque>
#include <unordered_map>

namespace hgraph
{
namespace
{
    bool boundary_matches(const WiringPortRef &port, const WiringPortRef &boundary)
    {
        if (port.is_peered_source() && boundary.is_peered_source())
        {
            const auto &path = port.peered_path();
            const auto &prefix = boundary.peered_path();
            return port.peered_node() == boundary.peered_node() &&
                port.peered_output_kind() == boundary.peered_output_kind() &&
                path.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), path.begin());
        }
        if (port.is_delayed_source() && boundary.is_delayed_source()) {
            const auto &path = port.delayed_path();
            const auto &prefix = boundary.delayed_path();
            return port.delayed_state() == boundary.delayed_state() && path.size() >= prefix.size() &&
                std::equal(prefix.begin(), prefix.end(), path.begin());
        }
        if (port.same_source_as(boundary)) { return true; }
        if (!port.is_structural_source() && boundary.is_structural_source()) {
            return std::any_of(boundary.structural_children().begin(), boundary.structural_children().end(),
                               [&](const WiringPortRef &child) { return boundary_matches(port, child); });
        }
        return false;
    }

    WiringPortRef project(WiringPortRef source, std::span<const std::size_t> path)
    {
        for (const auto index : path)
        {
            const auto *schema = TypeRegistry::instance().dereference(source.schema);
            if (schema->kind == TSTypeKind::TSB)
            {
                if (index >= schema->field_count()) { throw std::out_of_range("Delayed bundle projection"); }
                source = subgraph_wiring_detail::tsb_field_ref(source, index, schema->fields()[index].type);
            }
            else if (schema->kind == TSTypeKind::TSL)
            {
                if (schema->is_unbounded_tsl() || index >= schema->fixed_size()) { throw std::out_of_range("Delayed list projection"); }
                source = subgraph_wiring_detail::tsl_element_ref(source, index, schema->element_ts());
            }
            else { throw std::invalid_argument("Cannot inspect a delayed projection of this schema"); }
        }
        return source;
    }
}

std::vector<WiringDependency> inspect_wiring_dependencies(
    const WiringPortRef &output, std::span<const WiringPortRef> sources)
{
    struct Pending { WiringPortRef port; bool passive; };
    std::deque<Pending> pending{{output, false}};
    std::vector<unsigned> found(sources.size());
    std::unordered_map<const WiringInstance *, unsigned> visited;
    std::vector<Pending> delayed;
    while (!pending.empty())
    {
        auto [port, passive] = std::move(pending.front());
        pending.pop_front();
        passive |= port.arg_tag == WiringPortRef::ArgTag::Passive;
        bool matched = false;
        for (std::size_t index = 0; index < sources.size(); ++index)
        {
            if (boundary_matches(port, sources[index]))
            {
                found[index] |= passive ? 1U : 2U;
                matched = true;
            }
        }
        if (matched) { continue; }
        switch (port.source_kind())
        {
            case WiringPortRef::SourceKind::Null: break;
            case WiringPortRef::SourceKind::Structural:
                for (const auto &child : port.structural_children()) { pending.push_back({child, passive}); }
                break;
            case WiringPortRef::SourceKind::Delayed:
            {
                const auto &state = port.delayed_state();
                if (!state->source) { throw std::invalid_argument("Cannot inspect an unbound delayed input"); }
                if (std::any_of(delayed.begin(), delayed.end(), [&](const Pending &item) {
                    return item.passive == passive && item.port.same_source_as(port);
                })) { break; }
                delayed.push_back({port, passive});
                pending.push_back({project(*state->source, port.delayed_path()), passive});
                break;
            }
            case WiringPortRef::SourceKind::Peered:
            {
                const auto *node = port.peered_node();
                const auto bit = passive ? 1U : 2U;
                if ((visited[node] & bit) != 0) { break; }
                visited[node] |= bit;
                bool compiled = false;
                node->builder.visit_child_graphs(&compiled, [](void *context, ChildGraphInspectionView) {
                    *static_cast<bool *>(context) = true;
                });
                if (compiled) { throw std::invalid_argument("Wiring dependency inspection requires explicit boundaries at compiled child graphs"); }
                // Structural packing is flattened with its target path intact:
                // activity belongs to the consuming leaf, not the bundle root.
                const auto visit_input = [&](auto &&self, const WiringPortRef &source,
                                             std::vector<std::size_t> path, bool weak) -> void {
                    weak |= source.arg_tag == WiringPortRef::ArgTag::Passive;
                    if (source.is_structural_source()) {
                        std::size_t index = 0;
                        for (const auto &child : source.structural_children()) {
                            auto child_path = path;
                            child_path.push_back(index++);
                            self(self, child, std::move(child_path), weak);
                        }
                    } else {
                        pending.push_back({source, weak || !node->builder.wiring_input_is_active(path)});
                    }
                };
                for (std::size_t index = 0; index < node->inputs.size(); ++index) {
                    const auto &input = node->inputs[index];
                    auto path = input.target_path.empty() ? std::vector<std::size_t>{index} : input.target_path;
                    visit_input(visit_input, input.source, std::move(path), passive);
                }
                break;
            }
            case WiringPortRef::SourceKind::Boundary:
                throw std::invalid_argument("Wiring dependency inspection reached a compiled graph boundary");
            case WiringPortRef::SourceKind::Unbound:
                throw std::invalid_argument("Wiring dependency inspection reached an unbound input");
        }
    }
    std::vector<WiringDependency> result;
    for (std::size_t index = 0; index < found.size(); ++index) {
        if (found[index]) { result.push_back({index, (found[index] & 2U) == 0}); }
    }
    return result;
}
}
