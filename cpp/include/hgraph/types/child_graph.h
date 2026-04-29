#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/boundary_binding.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/graph_builder.h>
#include <hgraph/types/nested_clock.h>

#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace hgraph
{
    // ---- ChildGraphTemplate ----

    struct ChildGraphTemplateFlags
    {
        bool has_output : 1       = false;
        bool requires_context : 1 = false;
    };

    /**
     * Immutable compile-time artifact for nested operators.
     *
     * Templates are registry-owned and referenced by pointer from builders and
     * runtime child-graph instances. The pointed-to template must therefore
     * remain stable for longer than any graph created from it.
     *
     * Produced by compile() which:
     * 1. Extracts boundary structure from wiring-layer stubs
     * 2. Removes stub nodes from the GraphBuilder
     * 3. Packages the cleaned builder + plan + metadata
     */
    struct HGRAPH_EXPORT ChildGraphTemplate
    {
        GraphBuilder            graph_builder;
        BoundaryBindingPlan     boundary_plan;
        std::string             default_label;
        ChildGraphTemplateFlags flags{};

        /**
         * Factory: compile a wiring-layer child graph (with stubs) into a template.
         *
         * The returned pointer is registry-owned and remains valid until the
         * registry is reset.
         */
        [[nodiscard]] static const ChildGraphTemplate *create(GraphBuilder graph_builder, BoundaryBindingPlan boundary_plan,
                                                              std::string label, ChildGraphTemplateFlags flags = {});
    };

    /**
     * Process-global owner for immutable child-graph templates.
     *
     * The registry provides stable template addresses, mirroring the lifetime
     * contract used by the type registries. It is intentionally single-threaded.
     * `reset()` exists for unit tests and should only be used when no graph can
     * still reference a registered template.
     */
    struct HGRAPH_EXPORT ChildGraphTemplateRegistry
    {
        ChildGraphTemplateRegistry()                                              = default;
        ChildGraphTemplateRegistry(const ChildGraphTemplateRegistry &)            = delete;
        ChildGraphTemplateRegistry &operator=(const ChildGraphTemplateRegistry &) = delete;

        [[nodiscard]] static ChildGraphTemplateRegistry &instance();

        [[nodiscard]] const ChildGraphTemplate *register_template(ChildGraphTemplate tmpl);
        [[nodiscard]] const ChildGraphTemplate *create(GraphBuilder graph_builder, BoundaryBindingPlan boundary_plan,
                                                       std::string label, ChildGraphTemplateFlags flags = {});

        void                 reset();
        [[nodiscard]] size_t size() const noexcept { return m_templates.size(); }

      private:
        std::deque<ChildGraphTemplate> m_templates;
    };

    struct GraphStorageReservation
    {
        void  *data{nullptr};
        size_t size{0};
        size_t alignment{alignof(std::max_align_t)};
    };

    // ---- ChildGraphInstance ----

    /**
     * Runtime handle for one active child graph.
     *
     * Manages the full lifecycle:
     *   create → initialise → start → evaluate* → stop → dispose → destroy
     *
     * Owned by a nested operator's runtime state. The operator may own one
     * instance (nested_graph), a keyed map (map_), or a tree (reduce).
     */
    struct HGRAPH_EXPORT ChildGraphInstance
    {
        ChildGraphInstance() = default;
        ~ChildGraphInstance();
        ChildGraphInstance(const ChildGraphInstance &)            = delete;
        ChildGraphInstance &operator=(const ChildGraphInstance &) = delete;
        ChildGraphInstance(ChildGraphInstance &&) noexcept;
        ChildGraphInstance &operator=(ChildGraphInstance &&) noexcept;

        /**
         * Create the child graph from a template.
         *
         * @param tmpl         Registry-owned immutable template
         * @param parent_node  The parent node that owns this instance
         * @param graph_id     The owning_graph_id for child graph nodes
         *                     (= parent_node.node_id for simple nesting,
         *                      includes key encoding for keyed operators).
         *                     Keyed operators currently append negative
         *                     monotonic child-instance ids so keyed path
         *                     segments remain distinguishable from ordinary
         *                     non-negative node ids in the owning graph path.
         * @param label        Instance label (empty = use template default)
         */
        void initialise(const ChildGraphTemplate &tmpl, Node &parent_node, std::vector<int64_t> graph_id, std::string label = {},
                        GraphStorageReservation storage = {});

        /**
         * Start the child graph. Starts all child nodes.
         */
        void start(engine_time_t eval_time);

        /**
         * Full evaluation protocol:
         * 1. Set child clock evaluation_time
         * 2. Evaluate child graph
         * 3. Update last_evaluation_time
         * 4. Preserve nested_next_scheduled so the owning operator can
         *    propagate child wake-ups to the parent scheduler.
         */
        void evaluate(engine_time_t eval_time);

        /**
         * Stop the child graph. Stops all child nodes.
         */
        void stop(engine_time_t eval_time);

        /**
         * Two-phase removal: stop the graph but defer destruction.
         * Used by map_ death-time semantics.
         */
        void dispose(engine_time_t eval_time);

        /** Next time the child graph needs evaluation. */
        [[nodiscard]] engine_time_t next_scheduled_time() const noexcept;

        /** Access the underlying graph. */
        [[nodiscard]] Graph       *graph() noexcept;
        [[nodiscard]] const Graph *graph() const noexcept;

        /** The graph_id assigned to this instance. */
        [[nodiscard]] const std::vector<int64_t> &graph_id() const noexcept { return m_graph_id; }

        [[nodiscard]] bool is_initialised() const noexcept { return m_template != nullptr; }
        [[nodiscard]] bool is_started() const noexcept { return m_started; }
        [[nodiscard]] bool is_disposed() const noexcept { return m_disposed; }

        /** Access the nested clock state (for testing and inspection). */
        [[nodiscard]] const NestedClockState &clock_state() const noexcept { return m_clock_state; }

        /** Access the boundary plan from the template. */
        [[nodiscard]] const BoundaryBindingPlan &boundary_plan() const;

      private:
        const ChildGraphTemplate *m_template{nullptr};
        std::optional<Graph>      m_graph;
        NestedClockState          m_clock_state;
        std::vector<int64_t>      m_graph_id;
        std::string               m_label;
        bool                      m_started{false};
        bool                      m_disposed{false};
    };

}  // namespace hgraph
