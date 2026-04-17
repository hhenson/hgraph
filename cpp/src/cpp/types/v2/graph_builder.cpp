#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/path_constants.h>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cstddef>
#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        constexpr int64_t error_path = -1;
        constexpr int64_t state_path = -2;

        [[nodiscard]] constexpr size_t align_up(size_t value, size_t alignment) noexcept
        {
            if (alignment == 0) { return value; }
            const size_t remainder = value % alignment;
            return remainder == 0 ? value : value + (alignment - remainder);
        }

        [[nodiscard]] const TSMeta *navigable_child_schema_at(const TSMeta &schema, int64_t slot)
        {
            const TSMeta *collection_schema = &schema;
            if (schema.kind == TSKind::REF && schema.element_ts() != nullptr) { collection_schema = schema.element_ts(); }

            if (slot == k_key_set_path) {
                if (collection_schema->kind != TSKind::TSD) {
                    throw std::invalid_argument("v2 key_set path navigation requires a TSD schema");
                }
                return TSTypeRegistry::instance().tss(collection_schema->key_type());
            }
            if (slot < 0) { throw std::out_of_range("v2 path navigation requires non-negative slots"); }

            switch (collection_schema->kind) {
                case TSKind::TSB:
                    if (static_cast<size_t>(slot) >= collection_schema->field_count()) {
                        throw std::out_of_range("v2 TSB path navigation is out of range");
                    }
                    return collection_schema->fields()[slot].ts_type;

                case TSKind::TSL:
                    if (collection_schema->fixed_size() == 0) {
                        throw std::invalid_argument("v2 path navigation does not support dynamic TSL prefixes");
                    }
                    if (static_cast<size_t>(slot) >= collection_schema->fixed_size()) {
                        throw std::out_of_range("v2 TSL path navigation is out of range");
                    }
                    return collection_schema->element_ts();

                case TSKind::SIGNAL:
                    throw std::logic_error("v2 SIGNAL child schema resolution requires planned signal state metadata");

                default:
                    throw std::invalid_argument("v2 path navigation only supports TSB and fixed-size TSL");
            }
        }

        [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, const BaseState *state, int64_t slot)
        {
            const TSMeta *collection_schema = &schema;
            if (schema.kind == TSKind::REF && schema.element_ts() != nullptr) { collection_schema = schema.element_ts(); }

            if (collection_schema->kind == TSKind::SIGNAL) {
                const auto *signal_state = state != nullptr ? static_cast<const SignalState *>(state->resolved_state()) : nullptr;
                if (signal_state == nullptr || signal_state->bound_schema == nullptr) {
                    throw std::logic_error("v2 SIGNAL input navigation requires a planned bound schema");
                }
                return navigable_child_schema_at(*signal_state->bound_schema, slot);
            }

            return navigable_child_schema_at(*collection_schema, slot);
        }

        [[nodiscard]] TSViewContext navigation_parent_context(const TSInputView &view) noexcept
        {
            return view.context_ref();
        }

        [[nodiscard]] BaseState *child_state_at(BaseState *state, const TSMeta &schema, int64_t slot)
        {
            if (state == nullptr) { return nullptr; }

            const TSMeta *collection_schema = &schema;
            if (schema.kind == TSKind::REF && schema.element_ts() != nullptr) { collection_schema = schema.element_ts(); }

            switch (collection_schema->kind) {
                case TSKind::TSB:
                    {
                        auto *bundle_state = static_cast<TSBState *>(state->resolved_state());
                        if (bundle_state == nullptr || static_cast<size_t>(slot) >= bundle_state->child_states.size()) { return nullptr; }
                        const auto &child = bundle_state->child_states[slot];
                        return child != nullptr ? std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, *child) : nullptr;
                    }

                case TSKind::TSL:
                    {
                        auto *list_state = static_cast<TSLState *>(state->resolved_state());
                        if (list_state == nullptr || static_cast<size_t>(slot) >= list_state->child_states.size()) { return nullptr; }
                        const auto &child = list_state->child_states[slot];
                        return child != nullptr ? std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, *child) : nullptr;
                    }

                case TSKind::SIGNAL:
                    {
                        auto *signal_state = static_cast<SignalState *>(state->resolved_state());
                        if (signal_state == nullptr || static_cast<size_t>(slot) >= signal_state->child_states.size()) { return nullptr; }
                        const auto &child = signal_state->child_states[slot];
                        return child != nullptr ? std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, *child) : nullptr;
                    }

                default:
                    throw std::invalid_argument("v2 input navigation only supports TSB and fixed-size TSL");
            }
        }

        [[nodiscard]] TSInputView traverse_input_child(TSInputView view, const TSMeta &schema, int64_t slot)
        {
            BaseState *state = view.context_ref().ts_state;
            BaseState *child_state = child_state_at(state, schema, slot);
            if (child_state == nullptr) {
                throw std::logic_error("v2 input navigation requires a planned child state for the selected path");
            }

            TSViewContext child_context;
            child_context.schema = child_schema_at(schema, state, slot);
            child_context.ts_state = child_state;
            return view.make_child_view_impl(
                child_context,
                navigation_parent_context(view),
                view.evaluation_time());
        }

        [[nodiscard]] TSInputView traverse_input(TSInputView view, const TSMeta *schema, PathView path)
        {
            const TSMeta *current_schema = schema;
            for (const int64_t slot : path) {
                if (current_schema == nullptr) { throw std::invalid_argument("v2 input navigation requires a schema"); }
                view = traverse_input_child(view, *current_schema, slot);
                current_schema = view.context_ref().schema;
            }

            return view;
        }

        [[nodiscard]] TSOutputView traverse_output(TSOutputView view, const TSMeta *schema, PathView path)
        {
            const TSMeta *current_schema = schema;
            for (const int64_t slot : path) {
                if (current_schema == nullptr) { throw std::invalid_argument("v2 output navigation requires a schema"); }

                if (slot == k_key_set_path) {
                    const TSMeta *target_schema = navigable_child_schema_at(*current_schema, slot);
                    TSOutput *owning_output = view.owning_output();
                    if (owning_output == nullptr) {
                        throw std::logic_error("v2 key_set output navigation requires an owning output endpoint");
                    }
                    view = owning_output->bindable_view(view, target_schema);
                    current_schema = target_schema;
                    continue;
                }

                const TSMeta *collection_schema = current_schema;
                if (current_schema->kind == TSKind::REF && current_schema->element_ts() != nullptr) {
                    collection_schema = current_schema->element_ts();
                }

                switch (collection_schema->kind) {
                    case TSKind::TSB:
                        view = view.as_bundle()[slot];
                        break;

                    case TSKind::TSL:
                        view = view.as_list()[slot];
                        break;

                    default:
                        throw std::invalid_argument("v2 output navigation only supports TSB and fixed-size TSL");
                }

                current_schema = navigable_child_schema_at(*current_schema, slot);
            }

            return view;
        }

        [[nodiscard]] size_t node_count(const GraphBuilder &builder) noexcept
        {
            return builder.node_builder_count();
        }

        [[nodiscard]] const TSMeta *edge_source_schema(const GraphBuilder &graph_builder, const Edge &edge) noexcept
        {
            const auto &src_builder = graph_builder.node_builder_at(static_cast<size_t>(edge.src_node));
            if (!edge.output_path.empty() && edge.output_path.front() == error_path) {
                return src_builder.error_output_schema();
            }
            if (!edge.output_path.empty() && edge.output_path.front() == state_path) {
                return src_builder.recordable_state_schema();
            }
            return src_builder.output_schema();
        }

        [[nodiscard]] std::pair<TSOutputView, const TSMeta *> select_source_output(Node &src_node, const Edge &edge)
        {
            if (edge.output_path.empty()) {
                if (!src_node.has_output()) { throw std::logic_error("v2 graph builder source node has no output"); }
                return {src_node.output_view(MIN_DT), src_node.output_schema()};
            }

            if (edge.output_path.front() == error_path) {
                if (!src_node.has_error_output()) { throw std::logic_error("v2 graph builder source node has no error output"); }
                return {src_node.error_output_view(MIN_DT), src_node.error_output_schema()};
            }

            if (edge.output_path.front() == state_path) {
                if (!src_node.has_recordable_state()) {
                    throw std::logic_error("v2 graph builder source node has no recordable state output");
                }
                return {src_node.recordable_state_view(MIN_DT), src_node.recordable_state_schema()};
            }

            if (!src_node.has_output()) { throw std::logic_error("v2 graph builder source node has no output"); }
            return {src_node.output_view(MIN_DT), src_node.output_schema()};
        }

        [[nodiscard]] std::vector<std::vector<TSInputConstructionEdge>>
        compile_inbound_edges(const GraphBuilder &graph_builder, const std::vector<Edge> &edges)
        {
            std::vector<std::vector<TSInputConstructionEdge>> inbound_edges(node_count(graph_builder));

            for (const auto &edge : edges) {
                if (edge.src_node < 0 || edge.dst_node < 0 || static_cast<size_t>(edge.src_node) >= node_count(graph_builder) ||
                    static_cast<size_t>(edge.dst_node) >= node_count(graph_builder)) {
                    throw std::out_of_range("v2 graph builder edge references an invalid node index");
                }

                inbound_edges[edge.dst_node].push_back(TSInputConstructionEdge{
                    .input_path = edge.input_path,
                    .binding = TSInputBindingRef{.src_node = edge.src_node, .output_path = edge.output_path},
                    .source_schema = edge_source_schema(graph_builder, edge),
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

        [[nodiscard]] GraphMemoryLayout describe_layout(
            const GraphBuilder &graph_builder,
            const std::vector<std::vector<TSInputConstructionEdge>> &inbound_edges)
        {
            GraphMemoryLayout layout;
            layout.node_offsets.reserve(node_count(graph_builder));

            size_t offset = sizeof(NodeEntry) * node_count(graph_builder);
            layout.alignment = std::max(layout.alignment, alignof(NodeEntry));

            for (size_t i = 0; i < node_count(graph_builder); ++i) {
                const auto &node_builder = graph_builder.node_builder_at(i);
                const size_t node_alignment = node_builder.alignment(inbound_edges[i]);
                layout.alignment = std::max(layout.alignment, node_alignment);
                offset = align_up(offset, node_alignment);
                layout.node_offsets.push_back(offset);
                offset += node_builder.size(inbound_edges[i]);
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

    bool Edge::operator<(const Edge &other) const noexcept
    {
        if (src_node != other.src_node) { return src_node < other.src_node; }
        if (output_path != other.output_path) { return output_path < other.output_path; }
        if (dst_node != other.dst_node) { return dst_node < other.dst_node; }
        return input_path < other.input_path;
    }

    GraphBuilder::GraphBuilder(std::vector<NodeBuilder> node_builders, std::vector<Edge> edges)
    {
        m_node_builders.reserve(node_builders.size());
        m_edges.reserve(edges.size());

        for (auto &node_builder : node_builders) { add_node(std::move(node_builder)); }
        for (auto &edge : edges) { add_edge(std::move(edge)); }
    }

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

    const NodeBuilder &GraphBuilder::node_builder_at(size_t index) const
    {
        if (index >= m_node_builders.size()) { throw std::out_of_range("v2 graph builder node builder index is out of range"); }
        return m_node_builders[index];
    }

    size_t GraphBuilder::size() const
    {
        const auto inbound_edges = compile_inbound_edges(*this, m_edges);
        return describe_layout(*this, inbound_edges).total_size;
    }

    size_t GraphBuilder::alignment() const
    {
        const auto inbound_edges = compile_inbound_edges(*this, m_edges);
        return describe_layout(*this, inbound_edges).alignment;
    }

    size_t GraphBuilder::memory_size() const
    {
        return size();
    }

    Graph GraphBuilder::make_graph(GraphEvaluationEngine evaluation_engine) const
    {
        if (!evaluation_engine) { throw std::logic_error("v2 graph builder requires an attached evaluation engine"); }

        const auto inbound_edges = compile_inbound_edges(*this, m_edges);
        const auto layout = describe_layout(*this, inbound_edges);

        Graph graph(evaluation_engine);
        if (m_node_builders.empty()) { return graph; }

        void *storage = ::operator new(layout.total_size, std::align_val_t(layout.alignment));
        auto *base = static_cast<std::byte *>(storage);
        auto *entries = reinterpret_cast<NodeEntry *>(base);
        size_t constructed_nodes = 0;

        {
            auto cleanup_storage = UnwindCleanupGuard([&] { ::operator delete(storage, std::align_val_t(layout.alignment)); });
            auto cleanup_nodes = UnwindCleanupGuard([&] {
                for (size_t i = constructed_nodes; i > 0; --i) {
                    const size_t order_index = i - 1;
                    node_builder_at(order_index).destruct_at(*entries[order_index].node);
                }
            });

            for (size_t i = 0; i < m_node_builders.size(); ++i) {
                auto *node =
                    node_builder_at(i).construct_at(base + layout.node_offsets[i], static_cast<int64_t>(i), inbound_edges[i]);
                entries[i] = NodeEntry{MIN_DT, node};
                ++constructed_nodes;
            }

            const int64_t push_source_nodes_end = validate_push_source_nodes(entries, m_node_builders.size());
            if (push_source_nodes_end > 0 && evaluation_engine.push_message_receiver() == nullptr) {
                throw std::logic_error("v2 push-source graphs require an attached push-message receiver");
            }
            graph.adopt_storage(storage, layout.alignment, m_node_builders.size(), push_source_nodes_end);
        }

        for (const auto &edge : m_edges) {
            auto &src_node = graph.node_at(static_cast<size_t>(edge.src_node));
            auto &dst_node = graph.node_at(static_cast<size_t>(edge.dst_node));

            if (edge.input_path.empty()) { continue; }
            if (!dst_node.has_input()) { throw std::logic_error("v2 graph builder destination node has no input"); }

            auto [source_view, source_schema] = select_source_output(src_node, edge);
            const auto navigation_path =
                !edge.output_path.empty() && (edge.output_path.front() == error_path || edge.output_path.front() == state_path)
                    ? PathView{edge.output_path}.subspan(1)
                    : PathView{edge.output_path};

            TSOutputView output = traverse_output(source_view, source_schema, navigation_path);
            TSInputView input = traverse_input(dst_node.input_view(MIN_DT), dst_node.input_schema(), edge.input_path);
            input.bind_output(output);
        }

        return graph;
    }
}  // namespace hgraph::v2
