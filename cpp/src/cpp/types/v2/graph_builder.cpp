#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cstddef>
#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        [[nodiscard]] constexpr size_t align_up(size_t value, size_t alignment) noexcept
        {
            if (alignment == 0) { return value; }
            const size_t remainder = value % alignment;
            return remainder == 0 ? value : value + (alignment - remainder);
        }

        [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, int64_t slot)
        {
            if (slot < 0) { throw std::out_of_range("v2 path navigation requires non-negative slots"); }

            switch (schema.kind) {
                case TSKind::TSB:
                    if (static_cast<size_t>(slot) >= schema.field_count()) {
                        throw std::out_of_range("v2 TSB path navigation is out of range");
                    }
                    return schema.fields()[slot].ts_type;

                case TSKind::TSL:
                    if (schema.fixed_size() == 0) {
                        throw std::invalid_argument("v2 path navigation does not support dynamic TSL prefixes");
                    }
                    if (static_cast<size_t>(slot) >= schema.fixed_size()) {
                        throw std::out_of_range("v2 TSL path navigation is out of range");
                    }
                    return schema.element_ts();

                default:
                    throw std::invalid_argument("v2 path navigation only supports TSB and fixed-size TSL");
            }
        }

        [[nodiscard]] TSInputView traverse_input(TSInputView view, const TSMeta *schema, PathView path)
        {
            const TSMeta *current_schema = schema;
            for (const int64_t slot : path) {
                if (current_schema == nullptr) { throw std::invalid_argument("v2 input navigation requires a schema"); }

                switch (current_schema->kind) {
                    case TSKind::TSB:
                        view = view.as_bundle()[slot];
                        break;

                    case TSKind::TSL:
                        view = view.as_list()[slot];
                        break;

                    default:
                        throw std::invalid_argument("v2 input navigation only supports TSB and fixed-size TSL");
                }

                current_schema = child_schema_at(*current_schema, slot);
            }

            return view;
        }

        [[nodiscard]] TSOutputView traverse_output(TSOutputView view, const TSMeta *schema, PathView path)
        {
            const TSMeta *current_schema = schema;
            for (const int64_t slot : path) {
                if (current_schema == nullptr) { throw std::invalid_argument("v2 output navigation requires a schema"); }

                switch (current_schema->kind) {
                    case TSKind::TSB:
                        view = view.as_bundle()[slot];
                        break;

                    case TSKind::TSL:
                        view = view.as_list()[slot];
                        break;

                    default:
                        throw std::invalid_argument("v2 output navigation only supports TSB and fixed-size TSL");
                }

                current_schema = child_schema_at(*current_schema, slot);
            }

            return view;
        }

        [[nodiscard]] std::vector<std::vector<TSInputConstructionEdge>>
        compile_inbound_edges(const std::vector<NodeBuilder> &node_builders, const std::vector<Edge> &edges)
        {
            std::vector<std::vector<TSInputConstructionEdge>> inbound_edges(node_builders.size());

            for (const auto &edge : edges) {
                if (edge.src_node < 0 || edge.dst_node < 0 || static_cast<size_t>(edge.src_node) >= node_builders.size() ||
                    static_cast<size_t>(edge.dst_node) >= node_builders.size()) {
                    throw std::out_of_range("v2 graph builder edge references an invalid node index");
                }

                inbound_edges[edge.dst_node].push_back(TSInputConstructionEdge{
                    .input_path = edge.input_path,
                    .binding = TSInputBindingRef{.src_node = edge.src_node, .output_path = edge.output_path},
                });
            }

            return inbound_edges;
        }

        struct GraphMemoryLayout
        {
            size_t total_size{0};
            size_t alignment{alignof(std::max_align_t)};
            std::vector<size_t> node_offsets;
        };

        [[nodiscard]] GraphMemoryLayout describe_layout(const std::vector<NodeBuilder> &node_builders,
                                                        const std::vector<std::vector<TSInputConstructionEdge>> &inbound_edges)
        {
            GraphMemoryLayout layout;
            layout.node_offsets.reserve(node_builders.size());

            // The graph allocation starts with NodeEntry[N] and is followed by
            // one variable-sized chunk per node.
            size_t offset = sizeof(NodeEntry) * node_builders.size();
            layout.alignment = std::max(layout.alignment, alignof(NodeEntry));

            for (size_t i = 0; i < node_builders.size(); ++i) {
                const size_t node_alignment = node_builders[i].alignment(inbound_edges[i]);
                layout.alignment = std::max(layout.alignment, node_alignment);
                offset = align_up(offset, node_alignment);
                layout.node_offsets.push_back(offset);
                offset += node_builders[i].size(inbound_edges[i]);
            }

            layout.total_size = offset;
            return layout;
        }

        [[nodiscard]] int64_t validate_push_source_nodes(const NodeEntry *entries, size_t node_count)
        {
            bool seen_non_push = false;
            int64_t push_source_nodes_end = static_cast<int64_t>(node_count);
            for (size_t i = 0; i < node_count; ++i) {
                const Node *node = entries[i].node;
                if (node == nullptr) { continue; }

                if (node->is_push_source_node()) {
                    if (seen_non_push) {
                        throw std::logic_error("v2 graph requires push source nodes to appear before all other nodes");
                    }
                } else if (!seen_non_push) {
                    seen_non_push = true;
                    push_source_nodes_end = static_cast<int64_t>(i);
                }
            }

            return push_source_nodes_end;
        }
    }  // namespace

    GraphBuilder &GraphBuilder::add_node(NodeBuilder node_builder)
    {
        node_builder.validate_complete();
        m_node_builders.emplace_back(std::move(node_builder));
        return *this;
    }

    GraphBuilder &GraphBuilder::add_edge(Edge edge)
    {
        m_edges.emplace_back(std::move(edge));
        return *this;
    }

    size_t GraphBuilder::size() const
    {
        const auto inbound_edges = compile_inbound_edges(m_node_builders, m_edges);
        return describe_layout(m_node_builders, inbound_edges).total_size;
    }

    size_t GraphBuilder::alignment() const
    {
        const auto inbound_edges = compile_inbound_edges(m_node_builders, m_edges);
        return describe_layout(m_node_builders, inbound_edges).alignment;
    }

    Graph GraphBuilder::make_graph(GraphEvaluationEngine evaluation_engine) const
    {
        if (!evaluation_engine) { throw std::logic_error("v2 graph builder requires an attached evaluation engine"); }

        const auto inbound_edges = compile_inbound_edges(m_node_builders, m_edges);
        const auto layout = describe_layout(m_node_builders, inbound_edges);

        Graph graph(evaluation_engine);
        if (m_node_builders.empty()) { return graph; }

        void *storage = ::operator new(layout.total_size, std::align_val_t(layout.alignment));
        auto *base = static_cast<std::byte *>(storage);
        auto *entries = reinterpret_cast<NodeEntry *>(base);
        size_t constructed_nodes = 0;

        {
            auto cleanup_storage = UnwindCleanupGuard([&] { ::operator delete(storage, std::align_val_t(layout.alignment)); });
            auto cleanup_nodes = UnwindCleanupGuard([&] {
                for (size_t i = constructed_nodes; i > 0; --i) { m_node_builders[i - 1].destruct_at(*entries[i - 1].node); }
            });

            for (size_t i = 0; i < m_node_builders.size(); ++i) {
                auto *node = m_node_builders[i].construct_at(base + layout.node_offsets[i], static_cast<int64_t>(i), inbound_edges[i]);
                entries[i] = NodeEntry{MIN_DT, node};
                ++constructed_nodes;
            }

            const int64_t push_source_nodes_end = validate_push_source_nodes(entries, m_node_builders.size());
            if (push_source_nodes_end > 0 && evaluation_engine.push_message_receiver() == nullptr) {
                throw std::logic_error("v2 push-source graphs require an attached push-message receiver");
            }
            graph.adopt_storage(storage, layout.alignment, m_node_builders.size(), push_source_nodes_end);
        }

        // Bind edges only after every node exists so TSInput construction can
        // resolve inter-node references against stable Node addresses.
        for (const auto &edge : m_edges) {
            auto &src_node = graph.node_at(static_cast<size_t>(edge.src_node));
            auto &dst_node = graph.node_at(static_cast<size_t>(edge.dst_node));

            if (!src_node.has_output()) { throw std::logic_error("v2 graph builder source node has no output"); }
            if (!dst_node.has_input()) { throw std::logic_error("v2 graph builder destination node has no input"); }

            TSOutputView output = traverse_output(src_node.output_view(MIN_DT), src_node.output_schema(), edge.output_path);
            TSInputView input = traverse_input(dst_node.input_view(MIN_DT), dst_node.input_schema(), edge.input_path);
            input.bind_output(output);
        }

        return graph;
    }
}  // namespace hgraph::v2
