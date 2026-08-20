#include <hgraph/manifest/graph_manifest.h>

#include <hgraph/manifest/canonical.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/time_series/endpoint_schema.h>

#include <fmt/format.h>

#include <algorithm>
#include <optional>

namespace hgraph::manifest
{
    namespace
    {
        // Graph-level tags.
        constexpr std::uint32_t k_graph_tag_version = 1;
        constexpr std::uint32_t k_graph_tag_label = 2;
        constexpr std::uint32_t k_graph_tag_nodes = 3;
        constexpr std::uint32_t k_graph_tag_edges = 4;

        // Node-level tags.
        constexpr std::uint32_t k_node_tag_semantic_name = 1;
        constexpr std::uint32_t k_node_tag_implementation = 2;
        constexpr std::uint32_t k_node_tag_kind = 3;
        constexpr std::uint32_t k_node_tag_behaviour = 4;
        constexpr std::uint32_t k_node_tag_input_schema = 5;
        constexpr std::uint32_t k_node_tag_output_schema = 6;
        constexpr std::uint32_t k_node_tag_error_schema = 7;
        constexpr std::uint32_t k_node_tag_recordable_schema = 8;
        constexpr std::uint32_t k_node_tag_state_schema = 9;
        constexpr std::uint32_t k_node_tag_scalar_schema = 10;
        constexpr std::uint32_t k_node_tag_input_endpoint = 11;
        constexpr std::uint32_t k_node_tag_output_endpoint = 12;
        constexpr std::uint32_t k_node_tag_selectors = 13;
        constexpr std::uint32_t k_node_tag_scalars = 14;

        constexpr std::uint32_t k_node_tag_error_capture = 15;

        // Edge-level tags.
        constexpr std::uint32_t k_edge_tag_source_node = 1;
        constexpr std::uint32_t k_edge_tag_source_kind = 2;
        constexpr std::uint32_t k_edge_tag_source_path = 3;
        constexpr std::uint32_t k_edge_tag_target_node = 4;
        constexpr std::uint32_t k_edge_tag_target_path = 5;

        // Behaviour bit positions (canonical; append-only).
        enum : std::uint64_t
        {
            k_behaviour_uses_scheduler = 1u << 0,
            k_behaviour_uses_global_state = 1u << 1,
            k_behaviour_uses_evaluation_clock = 1u << 2,
            k_behaviour_uses_python_values = 1u << 3,
            // Bit 4 was briefly assigned to a simulation push-source
            // exception. It remains reserved so manifest bits stay stable.
            k_behaviour_requires_phase_runner = 1u << 5,
            k_behaviour_schedule_on_start = 1u << 6,
            k_behaviour_captures_errors = 1u << 7,
        };

        void append_endpoint_annotation(CanonicalWriter &writer, const TSEndpointSchema &endpoint)
        {
            CanonicalWriter scope;
            if (!endpoint.empty())
            {
                scope.varint(static_cast<std::uint64_t>(endpoint.role()));
                scope.varint(endpoint.child_count());
                for (std::size_t i = 0; i < endpoint.child_count(); ++i)
                {
                    append_endpoint_annotation(scope, endpoint.child(i));
                }
            }
            writer.scope(scope);
        }

        void append_selector(CanonicalWriter &writer,
                             const std::optional<std::vector<std::size_t>> &selector)
        {
            if (!selector.has_value())
            {
                writer.varint(0);  // absent
                return;
            }
            writer.varint(1);  // engaged
            writer.varint(selector->size());
            for (const auto index : *selector) { writer.varint(index); }
        }

        void append_selector(CanonicalWriter &writer, const std::vector<std::size_t> &selector)
        {
            writer.varint(1);
            writer.varint(selector.size());
            for (const auto index : selector) { writer.varint(index); }
        }

        void append_node(CanonicalWriter &writer, const NodeBuilder &node, std::size_t ordinal)
        {
            const auto *schema = node.type().schema();
            const auto *record = node.type().record();
            if (schema == nullptr)
            {
                throw ManifestCaptureError(fmt::format("node[{}]", ordinal),
                                           "node has no type metadata");
            }

            CanonicalWriter scope;
            scope.tag(k_node_tag_semantic_name);
            scope.string_field(schema->name());
            scope.tag(k_node_tag_implementation);
            scope.string_field(record != nullptr ? record->implementation_name()
                                                 : std::string_view{});
            scope.tag(k_node_tag_kind);
            scope.varint(static_cast<std::uint64_t>(schema->node_kind));

            std::uint64_t behaviour = 0;
            if (schema->uses_scheduler) { behaviour |= k_behaviour_uses_scheduler; }
            if (schema->uses_global_state) { behaviour |= k_behaviour_uses_global_state; }
            if (schema->uses_evaluation_clock) { behaviour |= k_behaviour_uses_evaluation_clock; }
            if (schema->uses_python_values) { behaviour |= k_behaviour_uses_python_values; }
            if (schema->requires_phase_runner) { behaviour |= k_behaviour_requires_phase_runner; }
            if (schema->schedule_on_start) { behaviour |= k_behaviour_schedule_on_start; }
            if (schema->captures_errors) { behaviour |= k_behaviour_captures_errors; }
            scope.tag(k_node_tag_behaviour);
            scope.varint(behaviour);

            scope.tag(k_node_tag_input_schema);
            append_ts_descriptor(scope, schema->input_schema);
            scope.tag(k_node_tag_output_schema);
            append_ts_descriptor(scope, schema->output_schema);
            scope.tag(k_node_tag_error_schema);
            append_ts_descriptor(scope, schema->error_output_schema);
            scope.tag(k_node_tag_recordable_schema);
            append_ts_descriptor(scope, schema->recordable_state_schema);
            scope.tag(k_node_tag_state_schema);
            append_value_descriptor(scope, schema->state_schema);
            scope.tag(k_node_tag_scalar_schema);
            append_value_descriptor(scope, schema->scalar_schema);

            scope.tag(k_node_tag_input_endpoint);
            append_endpoint_annotation(scope, node.input_endpoint());
            // The EFFECTIVE annotation: runtime construction falls back to the
            // schema annotation when the per-instance override is empty
            // (nested owners rely on it for forwarding/ownership roles).
            scope.tag(k_node_tag_output_endpoint);
            append_endpoint_annotation(scope, !node.output_endpoint().empty()
                                                  ? node.output_endpoint()
                                                  : schema->output_endpoint_schema);

            scope.tag(k_node_tag_selectors);
            {
                CanonicalWriter selectors;
                append_selector(selectors, schema->active_inputs);
                append_selector(selectors, schema->structural_inputs);
                append_selector(selectors, schema->valid_inputs);
                append_selector(selectors, schema->all_valid_inputs);
                scope.scope(selectors);
            }

            scope.tag(k_node_tag_scalars);
            {
                CanonicalWriter scalars;
                const Value &values = node.scalars();
                if (values.has_value())
                {
                    try
                    {
                        encode_manifest_scalar(scalars, values.view());
                    }
                    catch (const UnsupportedManifestValue &error)
                    {
                        throw ManifestCaptureError(
                            fmt::format("node[{}]:{}", ordinal, schema->name()), error.what());
                    }
                }
                scope.scope(scalars);
            }

            scope.tag(k_node_tag_error_capture);
            {
                // The capture options change the emitted NodeError content
                // (traceback depth, captured values), so they are identity
                // alongside the captures_errors bit.
                CanonicalWriter capture;
                capture.varint(schema->error_capture.trace_back_depth);
                capture.varint(schema->error_capture.capture_values ? 1u : 0u);
                scope.scope(capture);
            }

            writer.scope(scope);
        }

        void append_edge(CanonicalWriter &writer, const GraphEdge &edge)
        {
            CanonicalWriter scope;
            scope.tag(k_edge_tag_source_node);
            scope.varint(graph_edge_source_node(edge.source_node));
            scope.tag(k_edge_tag_source_kind);
            scope.varint(static_cast<std::uint64_t>(graph_edge_source_kind(edge.source_node)));
            scope.tag(k_edge_tag_source_path);
            scope.varint(edge.source_path.size());
            for (const auto component : edge.source_path) { scope.varint(component); }
            scope.tag(k_edge_tag_target_node);
            scope.varint(edge.target_node);
            scope.tag(k_edge_tag_target_path);
            scope.varint(edge.target_path.size());
            for (const auto component : edge.target_path) { scope.varint(component); }
            writer.scope(scope);
        }

        [[nodiscard]] ManifestId compute_id(std::uint16_t format_version,
                                            std::span<const std::byte> descriptor)
        {
            // Domain separation: the version participates in the digest input
            // but the id field itself never does (no fixed-point).
            util::Sha256 hasher;
            const std::array<std::byte, 2> version_bytes{
                static_cast<std::byte>(format_version & 0xffu),
                static_cast<std::byte>((format_version >> 8u) & 0xffu)};
            hasher.update(version_bytes);
            hasher.update(descriptor);
            return ManifestId{hasher.finish()};
        }

        // ------------------------------------------------------------------
        // Decoded model (validation only; the bytes remain the identity).

        struct DecodedEndpoint
        {
            std::optional<std::uint64_t> role{};
            std::vector<DecodedEndpoint> children{};

            friend bool operator==(const DecodedEndpoint &, const DecodedEndpoint &) = default;
        };

        struct DecodedNode
        {
            std::string semantic_name;
            std::string implementation;
            std::uint64_t kind{};
            std::uint64_t behaviour{};
            std::vector<std::byte> input_schema;
            std::vector<std::byte> output_schema;
            std::vector<std::byte> error_schema;
            std::vector<std::byte> recordable_schema;
            std::vector<std::byte> state_schema;
            std::vector<std::byte> scalar_schema;
            DecodedEndpoint input_endpoint;
            DecodedEndpoint output_endpoint;
            std::vector<std::byte> selectors;
            std::vector<std::byte> scalars;
            std::vector<std::byte> error_capture;
        };

        struct DecodedEdge
        {
            std::uint64_t source_node{};
            std::uint64_t source_kind{};
            std::vector<std::uint64_t> source_path;
            std::uint64_t target_node{};
            std::vector<std::uint64_t> target_path;
        };

        struct DecodedGraph
        {
            std::uint64_t format_version{};
            std::string label;
            std::vector<DecodedNode> nodes;
            std::vector<DecodedEdge> edges;
        };

        // Required-field discipline: every tag in a scope appears exactly
        // once; a missing or duplicated required field rejects the descriptor
        // (RFC 0022: readers never guess what an omission means).
        void mark_seen(std::uint32_t &seen, std::uint32_t tag, const char *scope_name)
        {
            if (tag == 0 || tag > 31)
            {
                throw CanonicalDecodeError(
                    fmt::format("unknown required field in {} descriptor", scope_name));
            }
            const std::uint32_t bit = 1u << tag;
            if ((seen & bit) != 0)
            {
                throw CanonicalDecodeError(
                    fmt::format("duplicate field {} in {} descriptor", tag, scope_name));
            }
            seen |= bit;
        }

        void require_seen(std::uint32_t seen, std::uint32_t first_tag, std::uint32_t last_tag,
                          const char *scope_name)
        {
            for (std::uint32_t tag = first_tag; tag <= last_tag; ++tag)
            {
                if ((seen & (1u << tag)) == 0)
                {
                    throw CanonicalDecodeError(
                        fmt::format("missing required field {} in {} descriptor", tag,
                                    scope_name));
                }
            }
        }

        DecodedEndpoint decode_endpoint(CanonicalReader reader)
        {
            DecodedEndpoint endpoint;
            if (reader.at_end()) { return endpoint; }
            endpoint.role = reader.varint();
            const auto child_count = reader.varint();
            endpoint.children.reserve(static_cast<std::size_t>(child_count));
            for (std::uint64_t i = 0; i < child_count; ++i)
            {
                endpoint.children.push_back(decode_endpoint(reader.scope()));
            }
            return endpoint;
        }

        std::vector<std::byte> owned_bytes(std::span<const std::byte> bytes)
        {
            return {bytes.begin(), bytes.end()};
        }

        DecodedNode decode_node(CanonicalReader reader)
        {
            DecodedNode node;
            std::uint32_t seen = 0;
            while (!reader.at_end())
            {
                const auto field_tag = reader.tag();
                mark_seen(seen, field_tag, "node");
                switch (field_tag)
                {
                case k_node_tag_semantic_name: node.semantic_name = reader.string_field(); break;
                case k_node_tag_implementation: node.implementation = reader.string_field(); break;
                case k_node_tag_kind: node.kind = reader.varint(); break;
                case k_node_tag_behaviour: node.behaviour = reader.varint(); break;
                case k_node_tag_input_schema: node.input_schema = owned_bytes(reader.bytes_field()); break;
                case k_node_tag_output_schema: node.output_schema = owned_bytes(reader.bytes_field()); break;
                case k_node_tag_error_schema: node.error_schema = owned_bytes(reader.bytes_field()); break;
                case k_node_tag_recordable_schema:
                    node.recordable_schema = owned_bytes(reader.bytes_field());
                    break;
                case k_node_tag_state_schema: node.state_schema = owned_bytes(reader.bytes_field()); break;
                case k_node_tag_scalar_schema:
                    node.scalar_schema = owned_bytes(reader.bytes_field());
                    break;
                case k_node_tag_input_endpoint: node.input_endpoint = decode_endpoint(reader.scope()); break;
                case k_node_tag_output_endpoint:
                    node.output_endpoint = decode_endpoint(reader.scope());
                    break;
                case k_node_tag_selectors: node.selectors = owned_bytes(reader.bytes_field()); break;
                case k_node_tag_scalars: node.scalars = owned_bytes(reader.bytes_field()); break;
                case k_node_tag_error_capture:
                    node.error_capture = owned_bytes(reader.bytes_field());
                    break;
                default:
                    throw CanonicalDecodeError("unknown required field in node descriptor");
                }
            }
            require_seen(seen, k_node_tag_semantic_name, k_node_tag_error_capture, "node");
            return node;
        }

        DecodedEdge decode_edge(CanonicalReader reader)
        {
            DecodedEdge edge;
            std::uint32_t seen = 0;
            while (!reader.at_end())
            {
                const auto field_tag = reader.tag();
                mark_seen(seen, field_tag, "edge");
                switch (field_tag)
                {
                case k_edge_tag_source_node: edge.source_node = reader.varint(); break;
                case k_edge_tag_source_kind: edge.source_kind = reader.varint(); break;
                case k_edge_tag_source_path: {
                    const auto count = reader.varint();
                    for (std::uint64_t i = 0; i < count; ++i)
                    {
                        edge.source_path.push_back(reader.varint());
                    }
                    break;
                }
                case k_edge_tag_target_node: edge.target_node = reader.varint(); break;
                case k_edge_tag_target_path: {
                    const auto count = reader.varint();
                    for (std::uint64_t i = 0; i < count; ++i)
                    {
                        edge.target_path.push_back(reader.varint());
                    }
                    break;
                }
                default:
                    throw CanonicalDecodeError("unknown required field in edge descriptor");
                }
            }
            require_seen(seen, k_edge_tag_source_node, k_edge_tag_target_path, "edge");
            return edge;
        }

        DecodedGraph decode_descriptor(std::span<const std::byte> descriptor)
        {
            DecodedGraph graph;
            CanonicalReader reader{descriptor};
            std::uint32_t seen = 0;
            while (!reader.at_end())
            {
                const auto field_tag = reader.tag();
                mark_seen(seen, field_tag, "graph");
                switch (field_tag)
                {
                case k_graph_tag_version: graph.format_version = reader.varint(); break;
                case k_graph_tag_label: graph.label = reader.string_field(); break;
                case k_graph_tag_nodes: {
                    const auto count = reader.varint();
                    graph.nodes.reserve(static_cast<std::size_t>(count));
                    for (std::uint64_t i = 0; i < count; ++i)
                    {
                        graph.nodes.push_back(decode_node(reader.scope()));
                    }
                    break;
                }
                case k_graph_tag_edges: {
                    const auto count = reader.varint();
                    graph.edges.reserve(static_cast<std::size_t>(count));
                    for (std::uint64_t i = 0; i < count; ++i)
                    {
                        graph.edges.push_back(decode_edge(reader.scope()));
                    }
                    break;
                }
                default:
                    throw CanonicalDecodeError("unknown required field in graph descriptor");
                }
            }
            require_seen(seen, k_graph_tag_version, k_graph_tag_edges, "graph");
            return graph;
        }

        constexpr std::size_t k_max_reported_differences = 32;

        void report(ValidationResult &result, std::string path, std::string expected,
                    std::string actual)
        {
            if (result.differences.size() >= k_max_reported_differences) { return; }
            result.differences.push_back(
                {std::move(path), std::move(expected), std::move(actual)});
        }

        std::string hex_preview(std::span<const std::byte> bytes)
        {
            constexpr std::size_t k_preview = 12;
            std::string out;
            for (std::size_t i = 0; i < bytes.size() && i < k_preview; ++i)
            {
                out += fmt::format("{:02x}", static_cast<unsigned>(bytes[i]));
            }
            if (bytes.size() > k_preview) { out += "…"; }
            return out.empty() ? std::string{"<empty>"} : out;
        }

        void compare_bytes(ValidationResult &result, const std::string &path,
                           const std::vector<std::byte> &expected,
                           const std::vector<std::byte> &actual)
        {
            if (expected != actual)
            {
                report(result, path, hex_preview(expected), hex_preview(actual));
            }
        }

        std::string path_list(const std::vector<std::uint64_t> &path)
        {
            std::string out = "[";
            for (std::size_t i = 0; i < path.size(); ++i)
            {
                if (i != 0) { out += ","; }
                out += fmt::format("{}", path[i]);
            }
            return out + "]";
        }
    }  // namespace

    ManifestCaptureError::ManifestCaptureError(std::string path, const std::string &message)
        : std::runtime_error(fmt::format("{}: {}", path, message))
        , path_(std::move(path))
    {
    }

    GraphManifest::GraphManifest(std::uint16_t format_version, std::vector<std::byte> descriptor)
        : format_version_(format_version)
        , descriptor_(std::move(descriptor))
        , id_(compute_id(format_version_, descriptor_))
    {
    }

    GraphManifest capture(const GraphBuilder &graph)
    {
        CanonicalWriter writer;
        writer.tag(k_graph_tag_version);
        writer.varint(k_manifest_format_version);
        writer.tag(k_graph_tag_label);
        writer.string_field(graph.label());

        writer.tag(k_graph_tag_nodes);
        writer.varint(graph.nodes().size());
        for (std::size_t i = 0; i < graph.nodes().size(); ++i)
        {
            append_node(writer, graph.nodes()[i], i);
        }

        writer.tag(k_graph_tag_edges);
        writer.varint(graph.edges().size());
        for (const auto &edge : graph.edges()) { append_edge(writer, edge); }

        return GraphManifest{k_manifest_format_version, writer.take()};
    }

    ValidationResult validate(const GraphManifest &expected, const GraphManifest &actual)
    {
        ValidationResult result;
        if (expected.canonical_descriptor().size() == actual.canonical_descriptor().size() &&
            std::equal(expected.canonical_descriptor().begin(),
                       expected.canonical_descriptor().end(),
                       actual.canonical_descriptor().begin()))
        {
            return result;
        }

        const auto expected_graph = decode_descriptor(expected.canonical_descriptor());
        const auto actual_graph = decode_descriptor(actual.canonical_descriptor());

        if (expected_graph.format_version != actual_graph.format_version)
        {
            report(result, "graph/format_version",
                   fmt::format("{}", expected_graph.format_version),
                   fmt::format("{}", actual_graph.format_version));
        }
        if (expected_graph.label != actual_graph.label)
        {
            report(result, "graph/label", expected_graph.label, actual_graph.label);
        }
        if (expected_graph.nodes.size() != actual_graph.nodes.size())
        {
            report(result, "graph/node_count",
                   fmt::format("{}", expected_graph.nodes.size()),
                   fmt::format("{}", actual_graph.nodes.size()));
        }
        const std::size_t node_count =
            std::min(expected_graph.nodes.size(), actual_graph.nodes.size());
        for (std::size_t i = 0; i < node_count; ++i)
        {
            const auto &lhs = expected_graph.nodes[i];
            const auto &rhs = actual_graph.nodes[i];
            const std::string base = fmt::format("node[{}]:{}", i, lhs.semantic_name);
            if (lhs.semantic_name != rhs.semantic_name)
            {
                report(result, base + "/semantic_name", lhs.semantic_name, rhs.semantic_name);
            }
            if (lhs.implementation != rhs.implementation)
            {
                report(result, base + "/implementation", lhs.implementation, rhs.implementation);
            }
            if (lhs.kind != rhs.kind)
            {
                report(result, base + "/kind", fmt::format("{}", lhs.kind),
                       fmt::format("{}", rhs.kind));
            }
            if (lhs.behaviour != rhs.behaviour)
            {
                report(result, base + "/behaviour", fmt::format("{:#x}", lhs.behaviour),
                       fmt::format("{:#x}", rhs.behaviour));
            }
            compare_bytes(result, base + "/input_schema", lhs.input_schema, rhs.input_schema);
            compare_bytes(result, base + "/output_schema", lhs.output_schema, rhs.output_schema);
            compare_bytes(result, base + "/error_schema", lhs.error_schema, rhs.error_schema);
            compare_bytes(result, base + "/recordable_state_schema", lhs.recordable_schema,
                          rhs.recordable_schema);
            compare_bytes(result, base + "/state_schema", lhs.state_schema, rhs.state_schema);
            compare_bytes(result, base + "/scalar_schema", lhs.scalar_schema, rhs.scalar_schema);
            if (lhs.input_endpoint != rhs.input_endpoint)
            {
                report(result, base + "/input_endpoint", "<endpoint annotation>",
                       "<differs>");
            }
            if (lhs.output_endpoint != rhs.output_endpoint)
            {
                report(result, base + "/output_endpoint", "<endpoint annotation>",
                       "<differs>");
            }
            compare_bytes(result, base + "/selectors", lhs.selectors, rhs.selectors);
            compare_bytes(result, base + "/scalars", lhs.scalars, rhs.scalars);
            compare_bytes(result, base + "/error_capture", lhs.error_capture,
                          rhs.error_capture);
        }

        if (expected_graph.edges.size() != actual_graph.edges.size())
        {
            report(result, "graph/edge_count",
                   fmt::format("{}", expected_graph.edges.size()),
                   fmt::format("{}", actual_graph.edges.size()));
        }
        const std::size_t edge_count =
            std::min(expected_graph.edges.size(), actual_graph.edges.size());
        for (std::size_t i = 0; i < edge_count; ++i)
        {
            const auto &lhs = expected_graph.edges[i];
            const auto &rhs = actual_graph.edges[i];
            if (lhs.source_node != rhs.source_node || lhs.source_kind != rhs.source_kind ||
                lhs.source_path != rhs.source_path || lhs.target_node != rhs.target_node ||
                lhs.target_path != rhs.target_path)
            {
                report(result, fmt::format("edge[{}]", i),
                       fmt::format("{}#{}{} -> {}{}", lhs.source_node, lhs.source_kind,
                                   path_list(lhs.source_path), lhs.target_node,
                                   path_list(lhs.target_path)),
                       fmt::format("{}#{}{} -> {}{}", rhs.source_node, rhs.source_kind,
                                   path_list(rhs.source_path), rhs.target_node,
                                   path_list(rhs.target_path)));
            }
        }

        if (result.differences.empty())
        {
            // Byte inequality with no structural difference found: report the
            // raw divergence rather than claiming identity.
            report(result, "graph/descriptor", hex_preview(expected.canonical_descriptor()),
                   hex_preview(actual.canonical_descriptor()));
        }
        return result;
    }

    std::vector<std::byte> encode(const GraphManifest &manifest)
    {
        CanonicalWriter writer;
        writer.varint(manifest.format_version());
        writer.bytes_field(manifest.canonical_descriptor());
        return writer.take();
    }

    GraphManifest decode_graph(std::span<const std::byte> bytes)
    {
        CanonicalReader reader{bytes};
        const auto version = reader.varint();
        if (version != k_manifest_format_version)
        {
            throw CanonicalDecodeError(
                fmt::format("unsupported manifest format version {}", version));
        }
        auto descriptor = reader.bytes_field();
        if (!reader.at_end())
        {
            throw CanonicalDecodeError("trailing bytes after manifest descriptor");
        }
        // Decoding validates structure (and therefore rejects torn input).
        (void)decode_descriptor(descriptor);
        return GraphManifest{static_cast<std::uint16_t>(version),
                             {descriptor.begin(), descriptor.end()}};
    }
}  // namespace hgraph::manifest
