#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>

#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    struct Graph;
    struct NodeBuilder;

    using Path = std::vector<int64_t>;
    using PathView = std::span<const int64_t>;

    struct HGRAPH_EXPORT Node;

    /**
     * Runtime behavior for a family of node layouts.
     *
     * Node itself is intentionally tiny and type-erased. The runtime ops know
     * how to interpret the node-local payload, expose input/output views, and
     * drive the start/stop/eval lifecycle for that payload.
     */
    struct HGRAPH_EXPORT NodeRuntimeOps
    {
        void (*start)(Node &node, engine_time_t evaluation_time);
        void (*stop)(Node &node, engine_time_t evaluation_time);
        void (*eval)(Node &node, engine_time_t evaluation_time);

        [[nodiscard]] bool (*has_input)(const Node &node) noexcept;
        [[nodiscard]] bool (*has_output)(const Node &node) noexcept;
        [[nodiscard]] TSInputView (*input_view)(Node &node, engine_time_t evaluation_time);
        [[nodiscard]] TSOutputView (*output_view)(Node &node, engine_time_t evaluation_time);
        [[nodiscard]] std::string (*runtime_label)(const Node &node);
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
        void (*destruct)(Node &node) noexcept{nullptr};
        size_t runtime_data_offset{0};

        std::string_view label;
        const TSMeta *input_schema{nullptr};
        const TSMeta *output_schema{nullptr};

        std::span<const size_t> active_inputs{};
        std::span<const size_t> valid_inputs{};
        std::span<const size_t> all_valid_inputs{};
    };

    /**
     * Type-erased runtime node.
     *
     * A Node is placement-constructed at the start of a variable-sized chunk
     * owned by Graph. The rest of the chunk stores the BuiltNodeSpec and any
     * node-family-specific payload such as TSInput, TSOutput, local state, and
     * copied selector metadata.
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
        [[nodiscard]] const TSMeta *input_schema() const noexcept;
        [[nodiscard]] const TSMeta *output_schema() const noexcept;
        [[nodiscard]] bool has_input() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] bool started() const noexcept;
        [[nodiscard]] engine_time_t evaluation_time() const noexcept;
        void set_started(bool value) noexcept;
        [[nodiscard]] void *data() noexcept;
        [[nodiscard]] const void *data() const noexcept;

        /** View access is delegated to the node family via NodeRuntimeOps. */
        [[nodiscard]] TSInputView input_view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] TSOutputView output_view(engine_time_t evaluation_time = MIN_DT);
        [[nodiscard]] const BuiltNodeSpec &spec() const noexcept;

        /** Apply top-level valid/all_valid gating before calling eval. */
        [[nodiscard]] bool ready_to_eval(engine_time_t evaluation_time);

        void start(engine_time_t evaluation_time);
        void stop(engine_time_t evaluation_time);
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
