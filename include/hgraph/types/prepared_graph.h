#ifndef HGRAPH_TYPES_PREPARED_GRAPH_H
#define HGRAPH_TYPES_PREPARED_GRAPH_H

#include <hgraph/runtime/nested_graph_node.h>

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

namespace hgraph
{
    /** A prepared node's ID belongs to its declaring graph. IDs need not be dense or ordered. */
    struct PreparedNode
    {
        std::size_t id;
        NodeBuilder builder;
    };

    struct PreparedOutput
    {
        std::size_t                 node;
        std::vector<std::size_t>    path{};
        const TSValueTypeMetaData *view_schema{nullptr};
        GraphEdgeSourceKind        kind{GraphEdgeSourceKind::Output};
    };

    /** A declared argument of the child, optionally projected below the argument root.
        view_schema is the required schema at the selected path. */
    struct PreparedParentInput
    {
        std::size_t                 argument;
        std::vector<std::size_t>    path{};
        const TSValueTypeMetaData *view_schema{nullptr};
    };

    /** Logical output in the immediate enclosing graph. A child can capture it more than once. */
    struct PreparedCapture
    {
        PreparedOutput outer;
    };

    using PreparedSource = std::variant<PreparedOutput, PreparedParentInput, PreparedCapture>;

    /** Both choices emit a data edge. A delayed or passive input can omit its same-cycle
        ordering constraint when its node's runtime semantics allow that. The builder
        supplies the scheduling or passive-input behavior. */
    enum class PreparedEdgeTiming { SameCycle, NoRankDependency };

    struct PreparedConnection
    {
        PreparedSource      source;
        NestedGraphEndpoint target;  // logical target node ID and explicit, non-empty input path
        PreparedEdgeTiming  timing{PreparedEdgeTiming::SameCycle};
    };

    /** Same-cycle ordering without a data connection. */
    struct PreparedOrdering
    {
        std::size_t before;
        std::size_t after;
    };

    struct PreparedGraphResult
    {
        GraphBuilder                                 graph_builder;
        std::unordered_map<std::size_t, std::size_t> index_of;  // reachable node IDs only
    };

    struct PreparedChildResult
    {
        SingleNestedGraphNodeSpec                    child;
        std::unordered_map<std::size_t, std::size_t> index_of;
        /** Bind these parent-graph outputs to the owner's capture inputs in this order.
            Child boundary argument ordinals start at the declared argument count. */
        std::vector<PreparedOutput>                  captures;
    };

    /** Finalise an already-resolved graph using HGraph's production reachability, ranking
        and edge binding. Declaration order breaks ties. Sinks and the child result are
        roots. No operator lookup or node interning occurs. */
    struct HGRAPH_CLASS_EXPORT PreparedGraph
    {
        std::vector<PreparedNode>       nodes{};
        std::vector<PreparedConnection> connections{};
        std::vector<PreparedOrdering>   ordering{};

        [[nodiscard]] PreparedGraphResult finish() const;
        [[nodiscard]] PreparedChildResult finish_child(
            std::size_t declared_arguments, std::optional<PreparedSource> output = std::nullopt) const;
    };
}  // namespace hgraph

#endif
