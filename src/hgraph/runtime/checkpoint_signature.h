#ifndef HGRAPH_RUNTIME_CHECKPOINT_SIGNATURE_DETAIL_H
#define HGRAPH_RUNTIME_CHECKPOINT_SIGNATURE_DETAIL_H

#include <hgraph/manifest/canonical.h>
#include <hgraph/runtime/nested_graph_node.h>

#include <span>
#include <stdexcept>

namespace hgraph::node_checkpoint_detail
{
    inline void append_path(manifest::CanonicalWriter &writer, std::span<const std::size_t> path)
    {
        writer.varint(path.size());
        for (const auto part : path) { writer.varint(part); }
    }

    inline const NodeCheckpointIdentity &child_identity(const GraphBuilder &graph, std::size_t index)
    {
        if (index >= graph.node_count())
            throw std::invalid_argument("component checkpoint: child binding node is out of range");
        return graph.nodes()[index].checkpoint_identity();
    }

    inline void append_identity(manifest::CanonicalWriter &writer, const NodeCheckpointIdentity &identity)
    {
        writer.string_field(identity.component);
        writer.string_field(identity.id);
    }

    // Physical positions include transient sinks. Only semantic bindings to
    // checkpoint members belong in the contract, named by stable identities.
    inline void append_input_bindings(manifest::CanonicalWriter &writer, const GraphBuilder &graph,
                                      std::span<const NestedGraphInputBinding> bindings)
    {
        std::size_t count = 0;
        for (const auto &binding : bindings) { count += !child_identity(graph, binding.target.node).transient; }
        writer.varint(count);
        for (const auto &binding : bindings)
        {
            const auto &identity = child_identity(graph, binding.target.node);
            if (identity.transient) { continue; }
            append_path(writer, binding.source_path);
            append_identity(writer, identity);
            append_path(writer, binding.target.path);
        }
    }

    inline void append_output_binding(manifest::CanonicalWriter &writer, const GraphBuilder &graph,
                                      const NestedGraphOutputBinding &binding)
    {
        writer.varint(static_cast<unsigned>(binding.kind));
        if (binding.kind == NestedGraphOutputBinding::Kind::ChildOutput)
        {
            append_identity(writer, child_identity(graph, binding.source.node));
            append_path(writer, binding.source.path);
        }
        append_path(writer, binding.parent_source_path);
        append_path(writer, binding.target_path);
    }
}
#endif
