#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    struct Graph;
    struct NodeBuilder;

    using Path = std::vector<int64_t>;
    using PathView = std::span<const int64_t>;

    struct HGRAPH_EXPORT Node;
    struct HGRAPH_EXPORT NodeScheduler;

    /** Runtime node category, aligned with the existing Python/C++ node type names. */
    enum class NodeTypeEnum
    {
        PUSH_SOURCE_NODE = 0,
        PULL_SOURCE_NODE = 1,
        COMPUTE_NODE = 2,
        SINK_NODE = 3,
    };

    /**
     * Runtime behavior for a family of node layouts.
     *
     * Node itself is intentionally tiny and type-erased. The runtime ops know
     * how to interpret the node-local payload, expose input/output views, and
     * invoke the node-family-specific start/stop/eval hooks.
     */
    struct HGRAPH_EXPORT NodeRuntimeOps
    {
        void (*start)(Node &node, engine_time_t evaluation_time);
        void (*stop)(Node &node, engine_time_t evaluation_time);
        void (*eval)(Node &node, engine_time_t evaluation_time);

        [[nodiscard]] bool (*has_input)(const Node &node) noexcept;
        [[nodiscard]] bool (*has_output)(const Node &node) noexcept;
        [[nodiscard]] bool (*has_error_output)(const Node &node) noexcept;
        [[nodiscard]] bool (*has_recordable_state)(const Node &node) noexcept;
        [[nodiscard]] TSInputView (*input_view)(Node &node, engine_time_t evaluation_time);
        [[nodiscard]] TSOutputView (*output_view)(Node &node, engine_time_t evaluation_time);
        [[nodiscard]] TSOutputView (*error_output_view)(Node &node, engine_time_t evaluation_time);
        [[nodiscard]] TSOutputView (*recordable_state_view)(Node &node, engine_time_t evaluation_time);
        [[nodiscard]] std::string (*runtime_label)(const Node &node);
    };

    /** Runtime behavior used only by push-source node families. */
    struct HGRAPH_EXPORT PushSourceNodeRuntimeOps
    {
        [[nodiscard]] bool (*apply_message)(Node &node, const value::Value &message, engine_time_t evaluation_time);
    };

    /**
     * Immutable per-node build product embedded in the node's slab chunk.
     *
     * The spec describes the node-local payload layout and carries the static
     * metadata needed by the runtime. It is produced by NodeBuilder and then
     * read by the type-erased Node at evaluation time.
     */
    struct HGRAPH_EXPORT BuiltNodeSpec
    {
        const NodeRuntimeOps *runtime_ops{nullptr};
        const PushSourceNodeRuntimeOps *push_source_runtime_ops{nullptr};
        void (*destruct)(Node &node) noexcept{nullptr};
        size_t scheduler_offset{0};
        size_t runtime_data_offset{0};
        bool uses_scheduler{false};

        std::string_view label;
        NodeTypeEnum node_type{NodeTypeEnum::COMPUTE_NODE};
        const TSMeta *input_schema{nullptr};
        const TSMeta *output_schema{nullptr};
        const TSMeta *error_output_schema{nullptr};
        const TSMeta *recordable_state_schema{nullptr};

        std::span<const size_t> active_inputs{};
        std::span<const size_t> valid_inputs{};
        std::span<const size_t> all_valid_inputs{};
    };

    /**
     * Generic per-node scheduler state used by scheduler injectables.
     *
     * This is wrapper-owned node state, not runtime-family-specific payload.
     * Both static nodes and Python-backed nodes use the same scheduling
     * semantics through Node's generic start/eval/stop wrappers.
     */
    struct HGRAPH_EXPORT NodeScheduler
    {
        explicit NodeScheduler(Node *node) noexcept : m_node(node) {}

        NodeScheduler(const NodeScheduler &) = delete;
        NodeScheduler &operator=(const NodeScheduler &) = delete;
        NodeScheduler(NodeScheduler &&) = default;
        NodeScheduler &operator=(NodeScheduler &&) = default;
        ~NodeScheduler() = default;

        [[nodiscard]] engine_time_t next_scheduled_time() const noexcept;
        [[nodiscard]] bool requires_scheduling() const noexcept;
        [[nodiscard]] bool is_scheduled() const noexcept;
        [[nodiscard]] bool is_scheduled_now() const noexcept;
        [[nodiscard]] bool has_tag(std::string_view tag) const;
        [[nodiscard]] engine_time_t pop_tag(std::string_view tag, engine_time_t default_time = MIN_DT);
        void schedule(engine_time_t when, std::optional<std::string> tag = std::nullopt, bool on_wall_clock = false);
        void schedule(engine_time_delta_t when, std::optional<std::string> tag = std::nullopt, bool on_wall_clock = false);
        void un_schedule(const std::string &tag);
        void un_schedule();
        void reset();
        void advance();

      private:
        Node *m_node{nullptr};
        std::set<std::pair<engine_time_t, std::string>> m_scheduled_events;
        std::unordered_map<std::string, engine_time_t> m_tags;
    };

    /**
     * Type-erased runtime node.
     *
     * A Node is placement-constructed at the start of a variable-sized chunk
     * owned by Graph. The rest of the chunk stores the BuiltNodeSpec and any
     * generic wrapper state such as NodeScheduler, plus node-family-specific
     * payload such as TSInput, TSOutput, local state, and copied selector
     * metadata.
     */
    struct HGRAPH_EXPORT Node : Notifiable
    {
        Node(int64_t node_index, const BuiltNodeSpec *spec) noexcept;

        Node(const Node &) = delete;
        Node &operator=(const Node &) = delete;
        Node(Node &&) = delete;
        Node &operator=(Node &&) = delete;
        ~Node() override = default;

        void set_graph(Graph *graph) noexcept;

        [[nodiscard]] Graph *graph() const noexcept;
        [[nodiscard]] int64_t node_index() const noexcept;
        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] std::string runtime_label() const;
        [[nodiscard]] NodeTypeEnum node_type() const noexcept;
        /** Push sources are drained by the realtime engine before the normal scheduled pass. */
        [[nodiscard]] bool is_push_source_node() const noexcept;
        /** Pull sources remain in the normal scheduled evaluation pass. */
        [[nodiscard]] bool is_pull_source_node() const noexcept;
        [[nodiscard]] const TSMeta *input_schema() const noexcept;
        [[nodiscard]] const TSMeta *output_schema() const noexcept;
        [[nodiscard]] const TSMeta *error_output_schema() const noexcept;
        [[nodiscard]] const TSMeta *recordable_state_schema() const noexcept;
        [[nodiscard]] bool has_input() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] bool has_error_output() const noexcept;
        [[nodiscard]] bool has_recordable_state() const noexcept;
        [[nodiscard]] bool started() const noexcept;
        [[nodiscard]] bool uses_scheduler() const noexcept;
        [[nodiscard]] bool has_scheduler() const noexcept;
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;
        void set_started(bool value) noexcept;
        [[nodiscard]] NodeScheduler &scheduler();
        [[nodiscard]] NodeScheduler *scheduler_if_present() noexcept;
        [[nodiscard]] const NodeScheduler *scheduler_if_present() const noexcept;
        [[nodiscard]] void *data() noexcept;
        [[nodiscard]] const void *data() const noexcept;

        /** View access is delegated to the node family via NodeRuntimeOps. */
        [[nodiscard]] TSInputView input_view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView output_view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView error_output_view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView recordable_state_view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] const BuiltNodeSpec &spec() const noexcept;

        /** Apply top-level valid/all_valid gating before calling eval. */
        [[nodiscard]] bool ready_to_eval(engine_time_t evaluation_time);

        /** Apply generic start semantics and then invoke the bespoke start hook. */
        void start(engine_time_t evaluation_time);
        /** Apply generic stop semantics and then invoke the bespoke stop hook. */
        void stop(engine_time_t evaluation_time);
        /** Apply generic readiness gating and then invoke the bespoke eval hook. */
        void eval(engine_time_t evaluation_time);
        void notify(engine_time_t et) override;

      private:
        friend struct Graph;
        friend struct NodeBuilder;

        void destruct() noexcept;

        [[nodiscard]] TSInputView resolve_input_slot(size_t slot, engine_time_t evaluation_time);

        Graph *m_graph{nullptr};
        const BuiltNodeSpec *m_spec{nullptr};
        int64_t m_node_index{-1};
        bool m_started{false};
    };

}  // namespace hgraph::v2
