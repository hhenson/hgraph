#ifndef HGRAPH_RUNTIME_SERVICE_NODE_H
#define HGRAPH_RUNTIME_SERVICE_NODE_H

#include <hgraph/runtime/node.h>
#include <hgraph/types/primitive_types.h>

#include <string>

namespace hgraph
{
    /** Allocate the process-unique identifier used by service clients. */
    [[nodiscard]] HGRAPH_EXPORT Int next_request_id() noexcept;

    /** Build a source that allocates one process-unique request id per runtime node instance. */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_request_id_source_node();

    /**
     * Build a source node that owns a service subscription key set output.
     *
     * The output schema is ``TSS<key_schema>``. Paired capture nodes record
     * per-client key add/remove intents into this source's graph-local storage;
     * the source applies those intents to its own output during evaluation.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_subscription_key_source_node(
        std::string path,
        const ValueTypeMetaData &key_schema);

    /**
     * Build a sink node that captures one client's current subscription key.
     *
     * Input field ``key`` is ``TS<key_schema>`` and field ``subscriptions`` is
     * the paired source node's ``TSS<key_schema>`` output. Only ``key`` is active;
     * ``subscriptions`` is a passive binding used to locate the source node.
     *
     * Runtime builder example:
     *
     * .. code-block:: cpp
     *
     *    auto path = "svc://prices/subscriptions";
     *    gb.add_node(client_key_source);
     *    gb.add_node(make_subscription_key_source_node(path, *int_meta));
     *    gb.add_node(make_subscription_key_capture_node(path, *int_meta));
     *    gb.add_edge(GraphEdge{.source_node = 0, .target_node = 2, .target_path = {0}});
     *    gb.add_edge(GraphEdge{.source_node = 1, .target_node = 2, .target_path = {1}});
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_subscription_key_capture_node(
        std::string path,
        const ValueTypeMetaData &key_schema,
        bool same_cycle = true);

    /**
     * Build the response boundary for one subscription client.
     *
     * A newly-live service key immediately invalidates any response sampled
     * through the shared dictionary. The first fresh implementation value is
     * published either immediately or one cycle after it arrives, according to
     * the immutable wiring-time transport. Both forms prevent cached values
     * from leaking on re-add. A client joining a key already kept live by
     * another client samples it immediately.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_subscription_response_gate_node(
        const ValueTypeMetaData &key_schema,
        const TSValueTypeMetaData &response_schema,
        bool response_same_cycle = false);

    /**
     * Build a source node that owns a request/reply service request dictionary.
     *
     * The output schema is ``TSD<int, request_schema>``. Paired capture nodes
     * update a source-owned request delta state; the source applies that delta
     * to its own output and resets it during evaluation.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_request_input_source_node(
        std::string path,
        const TSValueTypeMetaData &request_schema);

    /**
     * Build a sink node that captures one request/reply or service-adaptor
     * client's request.
     *
     * Input field ``request`` is ``request_schema`` and field ``requests`` is
     * the paired source node's ``TSD<int, request_schema>`` output. Only
     * ``request`` is active; ``requests`` is a passive binding used to locate
     * the source node. Captures with ``same_cycle == false`` schedule the next
     * engine tick during normal evaluation. The request/reply transport planner,
     * reply-less services, and service adaptors set ``same_cycle`` when they can
     * prove the paired source ranks later in the owning graph. Dynamically
     * started child graphs hand work to the enclosing source on the following
     * engine cycle because its outer rank may already have passed.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_request_input_capture_node(
        std::string path,
        const TSValueTypeMetaData &request_schema,
        bool same_cycle = false);
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_SERVICE_NODE_H
