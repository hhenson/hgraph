#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/evaluation_clock.h>
#include <hgraph/types/v2/sender_receiver_state.h>

#include <functional>
#include <memory>
#include <vector>

namespace hgraph::v2
{
    struct Graph;
    struct GraphBuilder;
    struct Node;

    enum class EvaluationMode
    {
        REAL_TIME = 0,
        SIMULATION = 1,
    };

    /**
     * Observer interface for graph and node evaluation lifecycle events.
     *
     * These callbacks mirror the existing Python/runtime lifecycle observer
     * semantics. They are intended for inspection, profiling, tracing, and
     * debugging. Each additional observer adds cost to the evaluation loop, so
     * they should be used deliberately.
     *
     * Example:
     *
     * @code
     * struct TraceObserver : EvaluationLifeCycleObserver
     * {
     *     void on_before_node_evaluation(Node &node) override
     *     {
     *         std::cout << "evaluating " << node.runtime_label() << '\n';
     *     }
     * };
     * @endcode
     */
    struct HGRAPH_EXPORT EvaluationLifeCycleObserver
    {
        virtual ~EvaluationLifeCycleObserver() = default;

        /** Called before the graph is started. */
        virtual void on_before_start_graph(Graph &) {}
        /** Called after the graph is started. */
        virtual void on_after_start_graph(Graph &) {}
        /** Called before a node is started. */
        virtual void on_before_start_node(Node &) {}
        /** Called after a node is started. */
        virtual void on_after_start_node(Node &) {}
        /** Called before the graph is evaluated. */
        virtual void on_before_graph_evaluation(Graph &) {}
        /** Called after the graph is evaluated. */
        virtual void on_after_graph_evaluation(Graph &) {}
        /** Called before a node is evaluated. */
        virtual void on_before_node_evaluation(Node &) {}
        /** Called after a node is evaluated. */
        virtual void on_after_node_evaluation(Node &) {}
        /** Called after the graph has evaluated all of its push nodes. */
        virtual void on_after_graph_push_nodes_evaluation(Graph &) {}
        /** Called before a node is stopped. */
        virtual void on_before_stop_node(Node &) {}
        /** Called after a node is stopped. */
        virtual void on_after_stop_node(Node &) {}
        /** Called before the graph is stopped. */
        virtual void on_before_stop_graph(Graph &) {}
        /** Called after the graph is stopped. */
        virtual void on_after_stop_graph(Graph &) {}
    };

    /** Type-erased API exposed to nodes and graphs during evaluation. */
    struct HGRAPH_EXPORT EvaluationEngineApiOps
    {
        [[nodiscard]] EvaluationMode (*evaluation_mode)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*start_time)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*end_time)(const void *impl) noexcept;
        [[nodiscard]] EvaluationClock (*evaluation_clock)(const void *impl);
        void (*request_engine_stop)(void *impl);
        [[nodiscard]] bool (*is_stop_requested)(const void *impl) noexcept;
        void (*add_before_evaluation_notification)(void *impl, std::function<void()> fn);
        void (*add_after_evaluation_notification)(void *impl, std::function<void()> fn);
        void (*add_life_cycle_observer)(void *impl, EvaluationLifeCycleObserver *observer);
        void (*remove_life_cycle_observer)(void *impl, EvaluationLifeCycleObserver *observer);
    };

    /**
     * User-visible evaluation engine API.
     *
     * This facade exposes the same conceptual surface as the existing Python
     * EvaluationEngineApi: execution mode, run bounds, the read-only evaluation
     * clock, stop requests, one-shot evaluation notifications, and lifecycle
     * observer registration.
     *
     * Example:
     *
     * @code
     * EvaluationEngine engine = EvaluationEngineBuilder{}
     *     .graph_builder(std::move(graph_builder))
     *     .start_time(start_time)
     *     .end_time(end_time)
     *     .build();
     *
     * EvaluationEngineApi api = engine.graph().evaluation_engine_api();
     * api.add_before_evaluation_notification([] { std::puts("before"); });
     * api.request_engine_stop();
     * @endcode
     */
    class HGRAPH_EXPORT EvaluationEngineApi
    {
      public:
        EvaluationEngineApi() = default;
        EvaluationEngineApi(void *impl, const EvaluationEngineApiOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        /** True when this facade is bound to concrete engine state. */
        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        /** Convenience validity check for `if (api) { ... }` style usage. */
        explicit operator bool() const noexcept { return valid(); }

        /** Current evaluation mode for the owning engine. */
        [[nodiscard]] EvaluationMode evaluation_mode() const noexcept { return ops().evaluation_mode(m_impl); }
        /** Inclusive lower bound of the engine run. */
        [[nodiscard]] engine_time_t start_time() const noexcept { return ops().start_time(m_impl); }
        /** Exclusive upper bound of the engine run. */
        [[nodiscard]] engine_time_t end_time() const noexcept { return ops().end_time(m_impl); }
        /** Read-only clock view for the current evaluation cycle. */
        [[nodiscard]] EvaluationClock evaluation_clock() const { return ops().evaluation_clock(m_impl); }

        /**
         * Request that the engine stop after the current evaluation cycle.
         *
         * This does not interrupt the graph immediately; it is observed when
         * the current cycle completes.
         */
        void request_engine_stop() const { ops().request_engine_stop(m_impl); }

        /** True when the engine has been asked to stop. */
        [[nodiscard]] bool is_stop_requested() const noexcept { return ops().is_stop_requested(m_impl); }

        /**
         * Register a one-shot callback to run before the next evaluation cycle.
         *
         * Notifications are drained iteratively. If a callback registers more
         * before-evaluation notifications while it is running, those
         * additional callbacks are also executed before
         * `notify_before_evaluation()` returns.
         *
         * Example:
         *
         * @code
         * api.add_before_evaluation_notification([] { std::puts("next cycle"); });
         * @endcode
         */
        void add_before_evaluation_notification(std::function<void()> fn) const
        {
            ops().add_before_evaluation_notification(m_impl, std::move(fn));
        }

        /**
         * Register a one-shot callback to run after the current evaluation
         * cycle completes.
         *
         * Like before-evaluation notifications, after-evaluation
         * notifications are drained iteratively. If a callback registers more
         * after-evaluation notifications while it is running, those
         * additional callbacks are also executed before
         * `notify_after_evaluation()` returns.
         *
         * Example:
         *
         * @code
         * api.add_after_evaluation_notification([] { std::puts("after cycle"); });
         * @endcode
         */
        void add_after_evaluation_notification(std::function<void()> fn) const
        {
            ops().add_after_evaluation_notification(m_impl, std::move(fn));
        }

        /**
         * Add a lifecycle observer.
         *
         * Events are delivered immediately and continue until the observer is
         * removed.
         */
        void add_life_cycle_observer(EvaluationLifeCycleObserver *observer) const
        {
            ops().add_life_cycle_observer(m_impl, observer);
        }

        /** Remove a previously registered lifecycle observer immediately. */
        void remove_life_cycle_observer(EvaluationLifeCycleObserver *observer) const
        {
            ops().remove_life_cycle_observer(m_impl, observer);
        }

      private:
        [[nodiscard]] const EvaluationEngineApiOps &ops() const
        {
            if (!valid()) { throw std::logic_error("v2 EvaluationEngineApi is not bound to runtime state"); }
            return *m_ops;
        }

        void *m_impl{nullptr};
        const EvaluationEngineApiOps *m_ops{nullptr};
    };

    /** Public runner surface returned by EvaluationEngineBuilder::build(). */
    struct HGRAPH_EXPORT EvaluationEngineOps
    {
        [[nodiscard]] EvaluationMode (*evaluation_mode)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*start_time)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*end_time)(const void *impl) noexcept;
        [[nodiscard]] Graph &(*graph)(void *impl);
        [[nodiscard]] const Graph &(*const_graph)(const void *impl);
        [[nodiscard]] EvaluationEngineApi (*evaluation_engine_api)(void *impl) noexcept;
        [[nodiscard]] EngineEvaluationClock (*engine_evaluation_clock)(void *impl);
        void (*notify_before_evaluation)(void *impl);
        void (*notify_after_evaluation)(void *impl);
        void (*advance_engine_time)(void *impl);
        void (*notify_before_start_graph)(void *impl, Graph &graph);
        void (*notify_after_start_graph)(void *impl, Graph &graph);
        void (*notify_before_start_node)(void *impl, Node &node);
        void (*notify_after_start_node)(void *impl, Node &node);
        void (*notify_before_graph_evaluation)(void *impl, Graph &graph);
        void (*notify_after_graph_evaluation)(void *impl, Graph &graph);
        void (*notify_after_push_nodes_evaluation)(void *impl, Graph &graph);
        void (*evaluate_push_source_nodes)(void *impl, Graph &graph, engine_time_t when);
        void (*notify_before_node_evaluation)(void *impl, Node &node);
        void (*notify_after_node_evaluation)(void *impl, Node &node);
        void (*notify_before_stop_node)(void *impl, Node &node);
        void (*notify_after_stop_node)(void *impl, Node &node);
        void (*notify_before_stop_graph)(void *impl, Graph &graph);
        void (*notify_after_stop_graph)(void *impl, Graph &graph);
        [[nodiscard]] SenderReceiverState *(*push_message_receiver)(void *impl) noexcept;
        [[nodiscard]] const SenderReceiverState *(*const_push_message_receiver)(const void *impl) noexcept;
        void (*run)(void *impl);
        void (*destruct)(void *impl) noexcept;
    };

    /**
     * Non-owning graph-facing view of an attached evaluation engine.
     *
     * Graphs do not need the full owning EvaluationEngine runner surface. They
     * only need a read-only engine API, the mutable engine clock, and the
     * lifecycle/evaluation notifications that the current graph
     * implementations call during start, evaluation, and stop.
     */
    class HGRAPH_EXPORT GraphEvaluationEngine
    {
      public:
        GraphEvaluationEngine() = default;
        GraphEvaluationEngine(void *impl, const EvaluationEngineOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] EvaluationEngineApi evaluation_engine_api() const noexcept
        {
            return valid() ? m_ops->evaluation_engine_api(m_impl) : EvaluationEngineApi{};
        }
        [[nodiscard]] EngineEvaluationClock engine_evaluation_clock() const
        {
            return ops().engine_evaluation_clock(m_impl);
        }
        [[nodiscard]] EvaluationClock evaluation_clock() const
        {
            return engine_evaluation_clock();
        }
        void notify_before_start_graph(Graph &graph) const { ops().notify_before_start_graph(m_impl, graph); }
        void notify_after_start_graph(Graph &graph) const { ops().notify_after_start_graph(m_impl, graph); }
        void notify_before_start_node(Node &node) const { ops().notify_before_start_node(m_impl, node); }
        void notify_after_start_node(Node &node) const { ops().notify_after_start_node(m_impl, node); }
        void notify_before_graph_evaluation(Graph &graph) const { ops().notify_before_graph_evaluation(m_impl, graph); }
        void notify_after_graph_evaluation(Graph &graph) const { ops().notify_after_graph_evaluation(m_impl, graph); }
        void notify_after_push_nodes_evaluation(Graph &graph) const { ops().notify_after_push_nodes_evaluation(m_impl, graph); }
        void evaluate_push_source_nodes(Graph &graph, engine_time_t when) const
        {
            ops().evaluate_push_source_nodes(m_impl, graph, when);
        }
        void notify_before_node_evaluation(Node &node) const { ops().notify_before_node_evaluation(m_impl, node); }
        void notify_after_node_evaluation(Node &node) const { ops().notify_after_node_evaluation(m_impl, node); }
        void notify_before_stop_node(Node &node) const { ops().notify_before_stop_node(m_impl, node); }
        void notify_after_stop_node(Node &node) const { ops().notify_after_stop_node(m_impl, node); }
        void notify_before_stop_graph(Graph &graph) const { ops().notify_before_stop_graph(m_impl, graph); }
        void notify_after_stop_graph(Graph &graph) const { ops().notify_after_stop_graph(m_impl, graph); }
        [[nodiscard]] SenderReceiverState *push_message_receiver() const noexcept
        {
            return valid() ? m_ops->push_message_receiver(m_impl) : nullptr;
        }

      private:
        [[nodiscard]] const EvaluationEngineOps &ops() const
        {
            if (!valid()) { throw std::logic_error("v2 GraphEvaluationEngine is not bound to runtime state"); }
            return *m_ops;
        }

        void *m_impl{nullptr};
        const EvaluationEngineOps *m_ops{nullptr};
    };

    /**
     * Owning runnable evaluation engine.
     *
     * Unlike the internal runtime/API surfaces, this is the top-level object a
     * caller builds and runs. It owns the concrete engine state and the root
     * graph for the duration of the run.
     *
     * Example:
     *
     * @code
     * EvaluationEngine engine = EvaluationEngineBuilder{}
     *     .graph_builder(std::move(graph_builder))
     *     .evaluation_mode(EvaluationMode::SIMULATION)
     *     .start_time(start_time)
     *     .end_time(end_time)
     *     .build();
     *
     * engine.run();
     * @endcode
     */
    class HGRAPH_EXPORT EvaluationEngine
    {
      public:
        EvaluationEngine() = default;
        ~EvaluationEngine();
        EvaluationEngine(const EvaluationEngine &) = delete;
        EvaluationEngine &operator=(const EvaluationEngine &) = delete;
        EvaluationEngine(EvaluationEngine &&other) noexcept;
        EvaluationEngine &operator=(EvaluationEngine &&other) noexcept;

        /** True when this runner owns concrete engine state. */
        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        /** Convenience validity check for `if (engine) { ... }` style usage. */
        explicit operator bool() const noexcept { return valid(); }

        /** Current evaluation mode for this engine. */
        [[nodiscard]] EvaluationMode evaluation_mode() const noexcept { return ops().evaluation_mode(m_impl); }
        /** Inclusive lower bound of the run. */
        [[nodiscard]] engine_time_t start_time() const noexcept { return ops().start_time(m_impl); }
        /** Exclusive upper bound of the run. */
        [[nodiscard]] engine_time_t end_time() const noexcept { return ops().end_time(m_impl); }

        /**
         * Access the root graph owned by this engine.
         *
         * This is typically used for inspection, testing, or to access the
         * EvaluationEngineApi once the engine has been built.
         */
        [[nodiscard]] Graph &graph() { return ops().graph(m_impl); }
        /** Const access to the root graph owned by this engine. */
        [[nodiscard]] const Graph &graph() const { return ops().const_graph(m_impl); }
        /** Real-time push receiver; null in simulation mode. */
        [[nodiscard]] SenderReceiverState *push_message_receiver() noexcept
        {
            return valid() ? ops().push_message_receiver(m_impl) : nullptr;
        }
        [[nodiscard]] const SenderReceiverState *push_message_receiver() const noexcept
        {
            return valid() ? ops().const_push_message_receiver(m_impl) : nullptr;
        }

        /**
         * Execute the evaluation loop.
         *
         * In simulation mode this runs from start_time() until end_time() or
         * until a stop request is observed. The exact mode semantics are
         * provided by the concrete engine implementation.
         */
        void run() { ops().run(m_impl); }

      private:
        friend class EvaluationEngineBuilder;
        friend struct Graph;

        EvaluationEngine(void *impl, const EvaluationEngineOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        [[nodiscard]] EvaluationEngineApi evaluation_engine_api() const noexcept
        {
            return valid() ? ops().evaluation_engine_api(m_impl) : EvaluationEngineApi{};
        }
        [[nodiscard]] EngineEvaluationClock engine_evaluation_clock() const
        {
            return ops().engine_evaluation_clock(m_impl);
        }
        void notify_before_evaluation() const { ops().notify_before_evaluation(m_impl); }
        void notify_after_evaluation() const { ops().notify_after_evaluation(m_impl); }
        void advance_engine_time() const { ops().advance_engine_time(m_impl); }
        void notify_before_start_graph(Graph &graph) const { ops().notify_before_start_graph(m_impl, graph); }
        void notify_after_start_graph(Graph &graph) const { ops().notify_after_start_graph(m_impl, graph); }
        void notify_before_start_node(Node &node) const { ops().notify_before_start_node(m_impl, node); }
        void notify_after_start_node(Node &node) const { ops().notify_after_start_node(m_impl, node); }
        void notify_before_graph_evaluation(Graph &graph) const { ops().notify_before_graph_evaluation(m_impl, graph); }
        void notify_after_graph_evaluation(Graph &graph) const { ops().notify_after_graph_evaluation(m_impl, graph); }
        void notify_after_push_nodes_evaluation(Graph &graph) const
        {
            ops().notify_after_push_nodes_evaluation(m_impl, graph);
        }
        void notify_before_node_evaluation(Node &node) const { ops().notify_before_node_evaluation(m_impl, node); }
        void notify_after_node_evaluation(Node &node) const { ops().notify_after_node_evaluation(m_impl, node); }
        void notify_before_stop_node(Node &node) const { ops().notify_before_stop_node(m_impl, node); }
        void notify_after_stop_node(Node &node) const { ops().notify_after_stop_node(m_impl, node); }
        void notify_before_stop_graph(Graph &graph) const { ops().notify_before_stop_graph(m_impl, graph); }
        void notify_after_stop_graph(Graph &graph) const { ops().notify_after_stop_graph(m_impl, graph); }

        [[nodiscard]] const EvaluationEngineOps &ops() const
        {
            if (!valid()) { throw std::logic_error("v2 EvaluationEngine is not bound to runtime state"); }
            return *m_ops;
        }

        void reset() noexcept;

        void *m_impl{nullptr};
        const EvaluationEngineOps *m_ops{nullptr};
    };

    /**
     * Fluent builder for a runnable evaluation engine.
     *
     * Example:
     *
     * @code
     * GraphBuilder graph_builder;
     * graph_builder.add_node(...).add_node(...).add_edge(...);
     *
     * EvaluationEngine engine = EvaluationEngineBuilder{}
     *     .graph_builder(std::move(graph_builder))
     *     .evaluation_mode(EvaluationMode::SIMULATION)
     *     .start_time(MIN_DT)
     *     .end_time(MAX_DT)
     *     .build();
     *
     * engine.run();
     * @endcode
     *
     * The builder owns only configuration. The returned EvaluationEngine owns
     * the built Graph plus the concrete clock / observer state needed to run it.
     */
    class HGRAPH_EXPORT EvaluationEngineBuilder
    {
      public:
        EvaluationEngineBuilder();
        ~EvaluationEngineBuilder();
        EvaluationEngineBuilder(const EvaluationEngineBuilder &other);
        EvaluationEngineBuilder &operator=(const EvaluationEngineBuilder &other);
        EvaluationEngineBuilder(EvaluationEngineBuilder &&other) noexcept;
        EvaluationEngineBuilder &operator=(EvaluationEngineBuilder &&other) noexcept;

        EvaluationEngineBuilder &graph_builder(GraphBuilder graph_builder);
        EvaluationEngineBuilder &evaluation_mode(EvaluationMode evaluation_mode) noexcept;
        EvaluationEngineBuilder &start_time(engine_time_t start_time) noexcept;
        EvaluationEngineBuilder &end_time(engine_time_t end_time) noexcept;
        EvaluationEngineBuilder &cleanup_on_error(bool cleanup_on_error) noexcept;
        EvaluationEngineBuilder &add_life_cycle_observer(EvaluationLifeCycleObserver *observer);

        [[nodiscard]] EvaluationEngine build() const;

      private:
        std::unique_ptr<GraphBuilder> m_graph_builder;
        EvaluationMode m_evaluation_mode{EvaluationMode::SIMULATION};
        engine_time_t m_start_time{MIN_DT};
        engine_time_t m_end_time{MAX_DT};
        bool m_cleanup_on_error{true};
        std::vector<EvaluationLifeCycleObserver *> m_life_cycle_observers;
    };
}  // namespace hgraph::v2
