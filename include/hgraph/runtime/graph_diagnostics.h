#ifndef HGRAPH_RUNTIME_GRAPH_DIAGNOSTICS_H
#define HGRAPH_RUNTIME_GRAPH_DIAGNOSTICS_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/evaluation_profiler.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/frame.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace hgraph
{
    enum class GraphDiagnosticEntityKind : std::uint8_t
    {
        Graph,
        Node,
    };

    /** One owned source-to-output navigation edge captured for the inspector.
        Path components are JSON fragments so scalar dictionary keys retain
        their type without retaining runtime objects. */
    struct HGRAPH_EXPORT GraphDiagnosticTarget
    {
        std::vector<std::string> source_path{};
        std::uint64_t node_id{0};
        std::vector<std::string> target_path{};
    };

    /** Owned rendering of one scalar or time-series endpoint. */
    struct HGRAPH_EXPORT GraphDiagnosticValue
    {
        bool available{false};
        bool valid{false};
        DateTime last_modified{MIN_DT};
        std::string schema_label{};
        std::string json{};
        std::string error{};
        /** Optional failure of the tabular projection. The owned JSON value
            remains usable when a schema has no released table form. */
        std::string table_error{};
        /** Immutable Arrow table retained for the inspector's tabular value
            view.  This is an owned external-resource handle, never a runtime
            graph, node, or time-series pointer. */
        Frame frame{};
        /** Owning diagnostic node ids for unambiguous source/reference
            navigation. Empty or multiple ids are presented without a link. */
        std::vector<std::uint64_t> target_node_ids{};
        /** Exact source and destination paths for REF navigation, including
            independently targeted composite fields. */
        std::vector<GraphDiagnosticTarget> targets{};
        /** Exact direct producer bindings for input ``output`` navigation.
            A REF input deliberately has both a direct bound target (the REF
            producer) and a dereferenced target above; retaining both is what
            lets the Python inspector preserve the released distinction
            between its ``output`` and ``ref`` commands. */
        std::vector<GraphDiagnosticTarget> bound_targets{};
    };

    /** One owned graph/node record in a diagnostics snapshot. */
    struct HGRAPH_EXPORT GraphDiagnosticEntry
    {
        std::uint64_t id{0};
        std::uint64_t parent_id{0};
        std::vector<std::uint64_t> children{};
        std::string path{};
        std::string label{};
        std::string schema_label{};
        std::string implementation_label{};
        GraphDiagnosticEntityKind kind{GraphDiagnosticEntityKind::Node};
        NodeKind node_kind{NodeKind::Compute};
        std::size_t node_index{0};
        bool started{false};
        bool stopped{false};
        DateTime evaluation_time{MIN_DT};
        DateTime scheduled_time{MIN_DT};
        NodeStorageMetrics storage{};
        NodeStorageMetrics peak_storage{};
        NodeInspectionMetrics inspection{};
        GraphDiagnosticValue input{};
        GraphDiagnosticValue output{};
        GraphDiagnosticValue scalars{};
        EvaluationProfilePhase start{};
        EvaluationProfilePhase evaluation{};
        EvaluationProfilePhase stop{};
    };

    /** Self-contained graph diagnostics result. */
    struct HGRAPH_EXPORT GraphDiagnosticsSnapshot
    {
        std::uint64_t graph_cycles{0};
        TimeDelta wall_time{0};
        TimeDelta root_evaluation_time{0};
        TimeDelta scheduling_lag_total{0};
        TimeDelta scheduling_lag_max{0};
        std::uint64_t scheduling_lag_samples{0};
        double runtime_load{0.0};
        std::size_t planned_bytes{0};
        std::size_t dynamic_live_bytes{0};
        std::size_t dynamic_reserved_bytes{0};
        std::size_t peak_dynamic_live_bytes{0};
        std::size_t peak_dynamic_reserved_bytes{0};
        std::vector<GraphDiagnosticEntry> entries{};
    };

    struct HGRAPH_EXPORT GraphDiagnosticsOptions
    {
        std::size_t recent_window{100};
        /** Copy endpoint/scalar values as JSON. Off by default because this is
            materially more expensive than structural diagnostics. */
        bool capture_values{false};
    };

    /**
     * Native graph diagnostics collector.
     *
     * Lifecycle callbacks copy runtime state into owned records. ``snapshot``
     * therefore remains valid after nested graph stop/delete/erase and never
     * walks borrowed graph or node pointers. Copies share one collector state
     * so a caller-owned handle can observe an executor-owned observer copy.
     */
    class HGRAPH_EXPORT GraphDiagnostics final : public LifecycleObserver
    {
      public:
        struct State;

        explicit GraphDiagnostics(GraphDiagnosticsOptions options = {});
        explicit GraphDiagnostics(std::size_t recent_window);

        [[nodiscard]] GraphDiagnosticsSnapshot snapshot() const;
        void reset();

        void on_before_start_graph(const GraphView &graph) override;
        void on_after_start_graph(const GraphView &graph) override;
        void on_start_graph_failed(const GraphView &graph) override;
        void on_before_start_node(const NodeView &node) override;
        void on_after_start_node(const NodeView &node) override;
        void on_start_node_failed(const NodeView &node) override;
        void on_before_graph_evaluation(const GraphView &graph) override;
        void on_after_graph_evaluation(const GraphView &graph) override;
        void on_before_node_evaluation(const NodeView &node) override;
        void on_after_node_evaluation(const NodeView &node) override;
        void on_after_graph_push_nodes_evaluation(const GraphView &graph) override;
        void on_before_stop_node(const NodeView &node) override;
        void on_after_stop_node(const NodeView &node) override;
        void on_stop_node_failed(const NodeView &node) override;
        void on_before_stop_graph(const GraphView &graph) override;
        void on_after_stop_graph(const GraphView &graph) override;
        void on_stop_graph_failed(const GraphView &graph) override;

      private:
        GraphDiagnosticsOptions options_{};
        std::shared_ptr<State> state_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_GRAPH_DIAGNOSTICS_H
