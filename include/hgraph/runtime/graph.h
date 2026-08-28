// runtime/graph.h — the flattened, rank-ordered runtime graph: GraphBuilder /
// GraphValue / GraphView plus the per-node schedule table, which is the ONLY
// activation gate (input notification, node schedulers, schedule_on_start and
// nested delegation all funnel into schedule_node). Scheduling invariants —
// MIN_DT = never-scheduled sentinel, MAX_DT = idle, lazy cleanup — are
// documented in docs/source/developer_guide/architecture.rst.
#ifndef HGRAPH_RUNTIME_GRAPH_H
#define HGRAPH_RUNTIME_GRAPH_H

#include <hgraph/runtime/evaluation_clock.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/graph_type_ref.h>
#include <hgraph/runtime/node.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace spdlog
{
    class logger;
}

namespace hgraph
{
    class GraphBuilder;
    class GraphExecutorView;
    class GraphValue;
    class LifecycleObserverList;
    class NestedGraphView;
    class RootGraphView;
    class GraphView;
    class TypeRealizationSnapshot;
    struct LoggerOps;
    /** Parent role for a graph runtime allocation. */
    enum class GraphParentKind : std::uint8_t
    {
        Root,
        Nested,
    };

    /** Root output endpoint for a graph edge source. */
    enum class GraphEdgeSourceKind : std::uint8_t
    {
        Output,
        ErrorOutput,
        RecordableState,
    };

    inline constexpr std::size_t graph_edge_source_kind_bits = 2;
    inline constexpr std::size_t graph_edge_source_kind_shift =
        sizeof(std::size_t) * 8U - graph_edge_source_kind_bits;
    inline constexpr std::size_t graph_edge_source_kind_value_mask =
        (std::size_t{1} << graph_edge_source_kind_bits) - 1U;
    inline constexpr std::size_t graph_edge_source_kind_mask =
        graph_edge_source_kind_value_mask << graph_edge_source_kind_shift;
    inline constexpr std::size_t graph_edge_source_node_mask = ~graph_edge_source_kind_mask;

    [[nodiscard]] inline std::size_t make_graph_edge_source(
        std::size_t source_node,
        GraphEdgeSourceKind source_kind = GraphEdgeSourceKind::Output)
    {
        if ((source_node & graph_edge_source_kind_mask) != 0)
        {
            throw std::out_of_range("Graph edge source node index is too large to pack the source kind");
        }

        const auto source_kind_value = static_cast<std::size_t>(source_kind);
        if (source_kind_value > graph_edge_source_kind_value_mask)
        {
            throw std::invalid_argument("Graph edge source kind is invalid");
        }
        return source_node | (source_kind_value << graph_edge_source_kind_shift);
    }

    [[nodiscard]] inline constexpr std::size_t graph_edge_source_node(std::size_t source) noexcept
    {
        return source & graph_edge_source_node_mask;
    }

    [[nodiscard]] inline constexpr GraphEdgeSourceKind graph_edge_source_kind(std::size_t source) noexcept
    {
        return static_cast<GraphEdgeSourceKind>((source & graph_edge_source_kind_mask) >>
                                                graph_edge_source_kind_shift);
    }

    /** Directed edge from one output endpoint/path to one input position. */
        /**
     * Reserved source-path component addressing a ``TSD``'s **key set** (the
     * ``TSS`` projection): a path ``{…, dict, ts_key_set_path_component}``
     * resolves the dict output's ``key_set()`` view. Only valid as the final
     * component, on the source (output) side.
     */
    inline constexpr std::size_t ts_key_set_path_component = static_cast<std::size_t>(-1);

struct HGRAPH_EXPORT GraphEdge
    {
        std::size_t source_node{0};
        std::vector<std::size_t> source_path{};
        std::size_t target_node{0};
        std::vector<std::size_t> target_path{};
    };

    /** Schema-side node entry retained by ``GraphTypeMetaData``. */
    struct HGRAPH_EXPORT GraphNodeEntry
    {
        const NodeTypeMetaData *node_schema{nullptr};
        std::size_t             index{0};
    };

    /** Interned graph schema descriptor. */
    struct HGRAPH_EXPORT GraphTypeMetaData
    {
        SchemaHeader header{};
        const char *display_name{nullptr};
        std::vector<GraphNodeEntry> nodes{};
        std::vector<GraphEdge> edges{};
        std::size_t push_source_nodes_end{0};

        [[nodiscard]] std::string_view name() const noexcept;
    };

    /** Type-erased graph operation table. */
    struct HGRAPH_EXPORT GraphOps
    {
        const void *context{nullptr};
        GraphParentKind parent_kind{GraphParentKind::Root};

        void (*attach_nodes_impl)(const void *context, void *memory, GraphValue *graph) = nullptr;
        void (*start_impl)(const void *context, const GraphView &graph, DateTime start_time) = nullptr;
        void (*stop_impl)(const void *context, const GraphView &graph,
                          DateTime stop_time) = nullptr;
        // Returns true when the graph completed the cycle, false when a node requested
        // a pause and the graph must be re-evaluated to resume from its cursor.
        bool (*evaluate_impl)(const void *context, const GraphView &graph, DateTime evaluation_time) = nullptr;
        void (*schedule_node_impl)(const void *context, const GraphView &graph, std::size_t node_index,
                                   DateTime when) = nullptr;

        bool (*started_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*starting_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*stopping_impl)(const void *context, const void *memory) noexcept = nullptr;
        bool (*evaluating_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*evaluation_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*next_scheduled_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        std::size_t (*node_count_impl)(const void *context, const void *memory) noexcept = nullptr;
        NodeView (*node_at_impl)(const void *context, void *memory, std::size_t index) = nullptr;
        NodeView (*failed_node_impl)(const void *context, void *memory) noexcept = nullptr;
        DateTime (*node_scheduled_time_impl)(const void *context, const void *memory,
                                                           std::size_t index) noexcept = nullptr;
        GlobalStateView (*global_state_impl)(const void *context, void *memory) = nullptr;
        /** This graph's OWN trait entry (no parent walk); invalid view when absent. */
        ValueView (*trait_impl)(const void *context, void *memory,
                                              std::string_view name) noexcept = nullptr;
        RootGraphView (*root_impl)(const void *context, const GraphView &graph) = nullptr;
        GraphExecutorView (*graph_executor_impl)(const void *context, void *memory) = nullptr;
        NodeView (*parent_node_impl)(const void *context, void *memory) = nullptr;
        /** Keyed-parent hook (nested graphs only): observe OUT-OF-BAND child
            schedules — a notification or scheduler firing while the child is
            idle — so the parent can track WHICH child is due without
            scanning its slots. */
        void (*set_child_schedule_observer_impl)(const void *context, void *memory,
                                                 void (*observer)(void *, DateTime),
                                                 void *observer_context) = nullptr;
        /** Cached pointer to the shared (executor-owned) lifecycle observer list; never null once constructed. */
        LifecycleObserverList *(*lifecycle_observers_impl)(const void *context, const void *memory) noexcept = nullptr;
        const TypeRealizationSnapshot *(*type_realization_impl)(const void *context,
                                                                const void *memory) noexcept = nullptr;
        /** Cached borrowed pointer to the executor-owned run logger. */
        spdlog::logger *(*logger_impl)(const void *context, const void *memory) noexcept = nullptr;
        /** Cached non-null passive emission policy for the run logger. */
        const LoggerOps *(*logger_ops_impl)(const void *context,
                                            const void *memory) noexcept = nullptr;
    };

    /** Borrowed type-erased view over graph runtime storage. */
    class HGRAPH_EXPORT GraphView
    {
      public:
        GraphView() noexcept;
        explicit GraphView(GraphPtr pointer) noexcept;
        GraphView(GraphTypeRef type, void *memory) noexcept;
        GraphView(const GraphView &) = delete;
        GraphView &operator=(const GraphView &) = delete;
        GraphView(GraphView &&) noexcept = default;
        GraphView &operator=(GraphView &&) noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] GraphTypeRef type() const noexcept;
        [[nodiscard]] GraphPtr pointer() const noexcept;
        [[nodiscard]] const GraphTypeMetaData *schema() const noexcept;
        [[nodiscard]] void *data() const noexcept;

        [[nodiscard]] bool started() const noexcept;
        /** True only while this graph's native start transition is running. */
        [[nodiscard]] bool is_starting() const noexcept;
        /** True only while this graph's native stop transition is running. */
        [[nodiscard]] bool is_stopping() const noexcept;
        [[nodiscard]] bool evaluating() const noexcept;
        [[nodiscard]] DateTime evaluation_time() const noexcept;
        [[nodiscard]] DateTime next_scheduled_time() const noexcept;
        [[nodiscard]] std::size_t node_count() const noexcept;
        [[nodiscard]] NodeView node_at(std::size_t index) const;
        /** Node whose exception most recently escaped this graph evaluation. */
        [[nodiscard]] NodeView failed_node() const noexcept;

        /** A view over the graph's shared ``GlobalState`` (the owning value lives on the graph). */
        [[nodiscard]] GlobalStateView global_state() const;

        /**
         * Graph traits: a small parent-chained key-value store (design
         * record: *Record/replay, tables and const_fn*, P5). Written at
         * build time (``GraphBuilder::trait`` / ``Wiring::set_trait``), read
         * at runtime. Two accessors mirroring Python's ``Traits``:
         *
         * - ``trait(name)`` — the CHAINED lookup (Python ``get_trait``):
         *   checks this graph, then BUBBLES UP the nested parent chain
         *   (a child inherits its parent's traits unless it shadows them).
         * - ``trait_or(name)`` — this graph's OWN entry only (Python
         *   ``get_trait_or``), no bubbling.
         *
         * Both return an invalid view when absent (the C++ absence idiom;
         * Python's ``get_trait`` throws instead — callers that need the
         * throw check ``valid()``).
         */
        [[nodiscard]] ValueView trait(std::string_view name) const noexcept;
        [[nodiscard]] ValueView trait_or(std::string_view name) const noexcept;
        [[nodiscard]] GraphParentKind parent_kind() const;
        [[nodiscard]] bool is_root() const;
        [[nodiscard]] bool is_nested() const;
        [[nodiscard]] RootGraphView as_root() const;
        [[nodiscard]] NestedGraphView as_nested() const;
        [[nodiscard]] GraphExecutorView executor() const;
        [[nodiscard]] RootGraphView root() const;

        /**
         * The lifecycle observer list shared by this graph's whole executor
         * run (design record: architecture.rst, "Lifecycle Observers"). Root
         * and nested graphs alike return the SAME list — a single
         * registration on the executor observes every graph/node in the
         * run. Cached at construction; O(1) regardless of nesting depth.
         */
        [[nodiscard]] LifecycleObserverList &lifecycle_observers() const;
        /** Borrowed executor-owned logger, cached by root and nested graphs. */
        [[nodiscard]] spdlog::logger *logger() const noexcept;
        [[nodiscard]] const LoggerOps *logger_ops() const noexcept;
        /** Closed Bundle hierarchy snapshot used by this graph instance. */
        [[nodiscard]] const TypeRealizationSnapshot *type_realization() const noexcept;
        void start(DateTime start_time = MIN_ST) const;
        void stop() const;
        void stop(DateTime stop_time) const;
        /// Returns true if the graph completed the cycle, false if it paused (resume by
        /// calling evaluate again at the same time; the cursor is held in graph state).
        bool evaluate(DateTime evaluation_time) const;
        void schedule_node(std::size_t node_index, DateTime when) const;
        /** Install the keyed-parent OUT-OF-BAND schedule observer on this
            NESTED child graph (see GraphOps). Pass nullptrs to clear.
            Throws for root graphs, which have no parent to notify. */
        void set_child_schedule_observer(void (*observer)(void *, DateTime),
                                         void *observer_context) const;

        /** The graph-schedule entry for one node (``MIN_DT`` = not scheduled). */
        [[nodiscard]] DateTime node_scheduled_time(std::size_t node_index) const noexcept;

        /**
         * Human-readable snapshot of the graph for diagnostics: the graph name
         * and lifecycle state, then one line per node — index, display
         * name/label, and its current graph-schedule entry. Intended for
         * logging/debugging (the on-call "which of these nodes is live?"
         * question); not a stable machine-readable format.
         */
        [[nodiscard]] std::string dump() const;

      protected:
        [[nodiscard]] const GraphOps &ops() const;

      private:
        GraphPtr pointer_{};
    };

    /** Root-specific graph view. A root graph has a graph executor parent. */
    class HGRAPH_EXPORT RootGraphView : public GraphView
    {
      public:
        RootGraphView() noexcept;
        explicit RootGraphView(GraphView graph);

        [[nodiscard]] GraphExecutorView executor() const;
    };

    /** Nested-specific graph view. A nested graph has a node parent. */
    class HGRAPH_EXPORT NestedGraphView : public GraphView
    {
      public:
        NestedGraphView() noexcept;
        explicit NestedGraphView(GraphView graph);

        [[nodiscard]] NodeView parent_node() const;
    };

    /**
     * Node-level injectable over the owning graph's traits (the runtime half
     * of the graph-traits primitive; design record P5). Declare a
     * ``TraitsView`` parameter on a node hook to read traits:
     * ``trait(name)`` is the chained lookup bubbling up the nested parent
     * chain; ``trait_or(name)`` reads this graph's own entry only. A
     * transparent, stateless injectable (the ``SingleShotScheduler``
     * pattern): no signature footprint, no per-node slot.
     */
    class HGRAPH_EXPORT TraitsView
    {
      public:
        TraitsView() noexcept = default;
        explicit TraitsView(GraphPtr graph) noexcept : graph_(graph) {}

        [[nodiscard]] ValueView trait(std::string_view name) const noexcept;
        [[nodiscard]] ValueView trait_or(std::string_view name) const noexcept;

      private:
        GraphPtr graph_{};
    };

    /** Graph lifetime wrapper: owns ordinary graphs and points into slot-owned nested storage. */
    class HGRAPH_EXPORT GraphValue
    {
      public:
        using storage_type = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        GraphValue() noexcept;
        GraphValue(const GraphBuilder &builder, ExecutorPtr root_executor);
        GraphValue(const GraphBuilder &builder, NodePtr parent_node);
        ~GraphValue();

        GraphValue(const GraphValue &) = delete;
        GraphValue &operator=(const GraphValue &) = delete;
        GraphValue(GraphValue &&other) noexcept;
        GraphValue &operator=(GraphValue &&other) noexcept;

        [[nodiscard]] bool has_value() const noexcept;
        /** True when the graph payload is constructed in caller-owned storage. */
        [[nodiscard]] bool uses_external_storage() const noexcept;
        [[nodiscard]] GraphTypeRef type() const noexcept;
        [[nodiscard]] const GraphTypeMetaData *schema() const noexcept;

        [[nodiscard]] GraphView view();
        [[nodiscard]] GraphView view() const;

        /** Stable offset consumed by the data-only debugger graph-pointer protocol. */
        [[nodiscard]] static constexpr std::size_t debug_pointer_offset() noexcept
        {
            return offsetof(GraphValue, pointer_);
        }

        void schedule_node(std::size_t node_index, DateTime when);

      private:
        GraphValue(const GraphBuilder &builder, NodePtr parent_node,
                   void *external_memory, MemoryUtils::StorageLayout available_layout);
        void reset() noexcept;
        void attach_nodes();

        GraphPtr    pointer_{};
        storage_type storage_{};

        friend class GraphBuilder;
    };

    /** Reusable graph construction recipe. */
    class HGRAPH_EXPORT GraphBuilder
    {
      public:
        GraphBuilder();

        GraphBuilder &label(std::string label);
        GraphBuilder &add_node(NodeBuilder node);
        GraphBuilder &add_edge(GraphEdge edge);

        /**
         * A view over the graph's initial ``GlobalState``, populated at wiring
         * time. The owning state is copied onto each ``GraphValue`` produced by
         * ``make_root_graph`` / ``make_nested_graph`` (so the builder stays
         * reusable and each graph instance gets its own runtime state seeded
         * with these wiring-time entries).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;
        /** Replace the initial ``GlobalState`` (used by the wiring layer's ``finish``). */
        GraphBuilder &global_state(GlobalState state);
        GraphBuilder &type_realization(std::shared_ptr<const TypeRealizationSnapshot> snapshot);
        [[nodiscard]] std::shared_ptr<const TypeRealizationSnapshot> type_realization() const;

        /**
         * Set a graph trait (parent-chained key-value metadata; see
         * ``GraphView::trait_or``). Copied onto every graph instance the
         * builder produces.
         */
        GraphBuilder &trait(std::string_view name, const ValueView &value);
        GraphBuilder &trait(std::string_view name, Value &&value);
        /** The trait store (a value-layer ``Map<string, Any>``, like ``GlobalState``). */
        [[nodiscard]] GlobalStateView traits() noexcept;

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] std::size_t node_count() const noexcept;
        [[nodiscard]] const std::vector<NodeBuilder> &nodes() const noexcept;
        /** True when this graph or a nested child plan needs a phase runner. */
        [[nodiscard]] bool requires_phase_runner() const noexcept;
        /** Mutable access to a wired node builder (nested operators adjust per-instance endpoints). */
        [[nodiscard]] NodeBuilder &node_at(std::size_t index);
        [[nodiscard]] const std::vector<GraphEdge> &edges() const noexcept;
        [[nodiscard]] GraphTypeRef type() const;
        [[nodiscard]] GraphTypeRef root_type() const;
        [[nodiscard]] GraphTypeRef nested_type() const;
        /** Storage required by one nested instance of this graph. */
        [[nodiscard]] MemoryUtils::StorageLayout nested_storage_layout() const;
        [[nodiscard]] GraphValue make_root_graph(ExecutorPtr root_executor) const;
        [[nodiscard]] GraphValue make_nested_graph(NodePtr parent_node) const;
        /** Construct a nested graph in caller-owned, suitably aligned storage. */
        [[nodiscard]] GraphValue make_nested_graph(NodePtr parent_node,
                                                   void *external_memory,
                                                   MemoryUtils::StorageLayout available_layout) const;

      private:
        friend class GraphValue;

        void invalidate_types() noexcept;

        std::string                   label_{};
        std::vector<NodeBuilder>      nodes_{};
        std::vector<GraphEdge>        edges_{};
        GlobalState                   global_state_{};
        GlobalState                   traits_{};   // trait store: same value-layer Map<string, Any> shape
        mutable GraphTypeRef          root_type_{};
        mutable GraphTypeRef          nested_type_{};
        mutable bool                  types_compiled_{false};
        mutable std::shared_ptr<const TypeRealizationSnapshot> type_realization_{};
    };

    HGRAPH_EXPORT void clear_graph_runtime_types() noexcept;

    static_assert(offsetof(GraphTypeMetaData, header) == 0);

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_GRAPH_H
