#include <hgraph/types/v2/child_graph.h>

#include <cassert>
#include <stdexcept>
#include <utility>

namespace hgraph::v2
{
    namespace
    {
        /**
         * Minimal engine state for a nested child graph.
         *
         * Owns only the nested clock state pointer.
         *
         * Nested scheduling still propagates through the parent node via the
         * nested clock, but lifecycle observer callbacks are currently
         * intentionally no-op for child graphs.
         */
        struct NestedEvaluationEngineState
        {
            NestedClockState *clock_state{nullptr};

            [[nodiscard]] EngineEvaluationClock engine_evaluation_clock() const noexcept {
                return {clock_state, &nested_clock_ops()};
            }
        };

        // EvaluationEngineOps dispatch for NestedEvaluationEngineState.
        // Most operations delegate to the parent engine. The clock is the
        // only thing that differs.

        EvaluationMode nested_evaluation_mode(const void * /*impl*/) noexcept { return EvaluationMode::SIMULATION; }

        engine_time_t nested_start_time(const void *impl) noexcept {
            return static_cast<const NestedEvaluationEngineState *>(impl)->clock_state->evaluation_time;
        }

        engine_time_t nested_end_time(const void * /*impl*/) noexcept { return MAX_DT; }

        Graph &nested_graph(void * /*impl*/) { throw std::logic_error("nested engine state does not own a graph"); }

        const Graph &nested_const_graph(const void * /*impl*/) {
            throw std::logic_error("nested engine state does not own a graph");
        }

        EvaluationEngineApi nested_evaluation_engine_api(void * /*impl*/) noexcept {
            // Nested graphs don't expose a full engine API.
            // Returning an invalid facade is safe because nodes access the clock
            // through Graph::evaluation_clock(), not through the API.
            return {};
        }

        EngineEvaluationClock nested_engine_evaluation_clock(void *impl) {
            return static_cast<NestedEvaluationEngineState *>(impl)->engine_evaluation_clock();
        }

        void nested_notify_before_evaluation(void * /*impl*/) {}
        void nested_notify_after_evaluation(void * /*impl*/) {}

        void nested_advance_engine_time(void *impl) {
            auto *state                               = static_cast<NestedEvaluationEngineState *>(impl);
            state->clock_state->evaluation_time       = state->clock_state->nested_next_scheduled;
            state->clock_state->nested_next_scheduled = MAX_DT;
        }

        // Lifecycle notifications are no-ops for now.
        // When lifecycle observers need to see nested graph activity (profiling/tracing),
        // these can be wired through to the parent engine's observer list.
        void nested_notify_before_start_graph(void * /*impl*/, Graph & /*graph*/) {}
        void nested_notify_after_start_graph(void * /*impl*/, Graph & /*graph*/) {}
        void nested_notify_before_start_node(void * /*impl*/, Node & /*node*/) {}
        void nested_notify_after_start_node(void * /*impl*/, Node & /*node*/) {}
        void nested_notify_before_graph_evaluation(void * /*impl*/, Graph & /*graph*/) {}
        void nested_notify_after_graph_evaluation(void * /*impl*/, Graph & /*graph*/) {}
        void nested_notify_after_push_nodes_evaluation(void * /*impl*/, Graph & /*graph*/) {}

        void nested_evaluate_push_source_nodes(void * /*impl*/, Graph & /*graph*/, engine_time_t /*when*/) {
            // Nested graphs do not support push source nodes for now
        }

        void nested_notify_before_node_evaluation(void * /*impl*/, Node & /*node*/) {}
        void nested_notify_after_node_evaluation(void * /*impl*/, Node & /*node*/) {}
        void nested_notify_before_stop_node(void * /*impl*/, Node & /*node*/) {}
        void nested_notify_after_stop_node(void * /*impl*/, Node & /*node*/) {}
        void nested_notify_before_stop_graph(void * /*impl*/, Graph & /*graph*/) {}
        void nested_notify_after_stop_graph(void * /*impl*/, Graph & /*graph*/) {}

        SenderReceiverState *nested_push_message_receiver(void * /*impl*/) noexcept { return nullptr; }

        const SenderReceiverState *nested_const_push_message_receiver(const void * /*impl*/) noexcept { return nullptr; }

        void nested_run(void * /*impl*/) { throw std::logic_error("nested evaluation engine does not support run()"); }

        void nested_destruct(void *impl) noexcept { delete static_cast<NestedEvaluationEngineState *>(impl); }

        const EvaluationEngineOps s_nested_engine_ops{
            nested_evaluation_mode,
            nested_start_time,
            nested_end_time,
            nested_graph,
            nested_const_graph,
            nested_evaluation_engine_api,
            nested_engine_evaluation_clock,
            nested_notify_before_evaluation,
            nested_notify_after_evaluation,
            nested_advance_engine_time,
            nested_notify_before_start_graph,
            nested_notify_after_start_graph,
            nested_notify_before_start_node,
            nested_notify_after_start_node,
            nested_notify_before_graph_evaluation,
            nested_notify_after_graph_evaluation,
            nested_notify_after_push_nodes_evaluation,
            nested_evaluate_push_source_nodes,
            nested_notify_before_node_evaluation,
            nested_notify_after_node_evaluation,
            nested_notify_before_stop_node,
            nested_notify_after_stop_node,
            nested_notify_before_stop_graph,
            nested_notify_after_stop_graph,
            nested_push_message_receiver,
            nested_const_push_message_receiver,
            nested_run,
            nested_destruct,
        };

    }  // namespace

    // ---- ChildGraphTemplate ----

    const ChildGraphTemplate *ChildGraphTemplate::create(GraphBuilder graph_builder_arg, BoundaryBindingPlan boundary_plan_arg,
                                                         std::string label, ChildGraphTemplateFlags flags_arg) {
        return ChildGraphTemplateRegistry::instance().register_template(ChildGraphTemplate{
            std::move(graph_builder_arg),
            std::move(boundary_plan_arg),
            std::move(label),
            flags_arg,
        });
    }

    ChildGraphTemplateRegistry &ChildGraphTemplateRegistry::instance() {
        // Keep the registry alive until process exit without running its
        // destructor during Python interpreter teardown. Tests call reset()
        // explicitly when they need a clean slate.
        static auto *registry = new ChildGraphTemplateRegistry();
        return *registry;
    }

    const ChildGraphTemplate *ChildGraphTemplateRegistry::register_template(ChildGraphTemplate tmpl) {
        m_templates.push_back(std::move(tmpl));
        return &m_templates.back();
    }

    const ChildGraphTemplate *ChildGraphTemplateRegistry::create(GraphBuilder graph_builder, BoundaryBindingPlan boundary_plan,
                                                                 std::string label, ChildGraphTemplateFlags flags) {
        return register_template(ChildGraphTemplate{
            std::move(graph_builder),
            std::move(boundary_plan),
            std::move(label),
            flags,
        });
    }

    void ChildGraphTemplateRegistry::reset() { m_templates.clear(); }

    // ---- ChildGraphInstance ----

    ChildGraphInstance::~ChildGraphInstance() {
        if (m_started && m_graph.has_value()) {
            try {
                m_graph->stop();
            } catch (...) {}
        }
    }

    ChildGraphInstance::ChildGraphInstance(ChildGraphInstance &&other) noexcept
        : m_template(other.m_template), m_graph(std::move(other.m_graph)), m_clock_state(other.m_clock_state),
          m_graph_id(std::move(other.m_graph_id)), m_label(std::move(other.m_label)), m_started(other.m_started),
          m_disposed(other.m_disposed) {
        other.m_template = nullptr;
        other.m_started  = false;
        other.m_disposed = false;
    }

    ChildGraphInstance &ChildGraphInstance::operator=(ChildGraphInstance &&other) noexcept {
        if (this != &other) {
            if (m_started && m_graph.has_value()) {
                try {
                    m_graph->stop();
                } catch (...) {}
            }
            m_template       = other.m_template;
            m_graph          = std::move(other.m_graph);
            m_clock_state    = other.m_clock_state;
            m_graph_id       = std::move(other.m_graph_id);
            m_label          = std::move(other.m_label);
            m_started        = other.m_started;
            m_disposed       = other.m_disposed;
            other.m_template = nullptr;
            other.m_started  = false;
            other.m_disposed = false;
        }
        return *this;
    }

    void ChildGraphInstance::initialise(const ChildGraphTemplate &tmpl, Node &parent_node, std::vector<int64_t> graph_id_arg,
                                        std::string label) {
        if (m_template != nullptr) {
            throw std::logic_error("ChildGraphInstance::initialise called on already-initialised instance");
        }

        m_template = &tmpl;
        m_graph_id = std::move(graph_id_arg);
        m_label    = label.empty() ? tmpl.default_label : std::move(label);
        m_started  = false;
        m_disposed = false;

        // Set up the nested clock state
        m_clock_state.parent_node           = &parent_node;
        m_clock_state.nested_next_scheduled = MAX_DT;
        m_clock_state.evaluation_time       = MIN_DT;

        // Create the nested evaluation engine state (heap-allocated, owned by the engine ops destruct)
        auto *engine_state        = new NestedEvaluationEngineState{};
        engine_state->clock_state = &m_clock_state;

        // Start the child clock from the parent graph's current evaluation time
        // so the nested graph begins on the same engine tick as its owner.
        if (parent_node.graph() != nullptr) {
            m_clock_state.evaluation_time = parent_node.graph()->evaluation_time();
        }

        GraphEvaluationEngine nested_engine{engine_state, &s_nested_engine_ops};
        m_graph.emplace(tmpl.graph_builder.make_graph(nested_engine));
        m_graph->set_identity(m_graph_id, &parent_node, m_label);
    }

    void ChildGraphInstance::start(engine_time_t eval_time) {
        if (!m_graph.has_value()) { throw std::logic_error("ChildGraphInstance::start called before initialise"); }
        if (m_started) { return; }

        m_clock_state.evaluation_time = eval_time;
        m_graph->start();
        m_started = true;
    }

    void ChildGraphInstance::evaluate(engine_time_t eval_time) {
        if (!m_started) { throw std::logic_error("ChildGraphInstance::evaluate called on non-started instance"); }

        while (true) {
            m_clock_state.reset_next_scheduled();

            // Graph::evaluate() calls set_evaluation_time(eval_time) at the top of the
            // cycle, so m_clock_state.evaluation_time is updated before any child node
            // runs.
            m_graph->evaluate(eval_time);

            if (!m_clock_state.consume_immediate_evaluation_request()) { break; }
        }
    }

    void ChildGraphInstance::stop(engine_time_t /*eval_time*/) {
        if (!m_started) { return; }

        m_graph->stop();
        m_started = false;
    }

    void ChildGraphInstance::dispose(engine_time_t eval_time) {
        if (m_disposed) { return; }
        if (m_started) { stop(eval_time); }
        m_disposed = true;
    }

    engine_time_t ChildGraphInstance::next_scheduled_time() const noexcept { return m_clock_state.nested_next_scheduled; }

    Graph *ChildGraphInstance::graph() noexcept { return m_graph.has_value() ? &*m_graph : nullptr; }

    const Graph *ChildGraphInstance::graph() const noexcept { return m_graph.has_value() ? &*m_graph : nullptr; }

    const BoundaryBindingPlan &ChildGraphInstance::boundary_plan() const {
        if (m_template == nullptr) { throw std::logic_error("ChildGraphInstance::boundary_plan called before initialise"); }
        return m_template->boundary_plan;
    }

}  // namespace hgraph::v2
