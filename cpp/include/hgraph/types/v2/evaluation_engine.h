#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/evaluation_clock.h>

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

    struct HGRAPH_EXPORT EvaluationLifeCycleObserver
    {
        virtual ~EvaluationLifeCycleObserver() = default;

        virtual void on_before_start_graph(Graph &) {}
        virtual void on_after_start_graph(Graph &) {}
        virtual void on_before_start_node(Node &) {}
        virtual void on_after_start_node(Node &) {}
        virtual void on_before_graph_evaluation(Graph &) {}
        virtual void on_after_graph_evaluation(Graph &) {}
        virtual void on_before_node_evaluation(Node &) {}
        virtual void on_after_node_evaluation(Node &) {}
        virtual void on_after_graph_push_nodes_evaluation(Graph &) {}
        virtual void on_before_stop_node(Node &) {}
        virtual void on_after_stop_node(Node &) {}
        virtual void on_before_stop_graph(Graph &) {}
        virtual void on_after_stop_graph(Graph &) {}
    };

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

    class HGRAPH_EXPORT EvaluationEngineApi
    {
      public:
        EvaluationEngineApi() = default;
        EvaluationEngineApi(void *impl, const EvaluationEngineApiOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] EvaluationMode evaluation_mode() const noexcept { return ops().evaluation_mode(m_impl); }
        [[nodiscard]] engine_time_t start_time() const noexcept { return ops().start_time(m_impl); }
        [[nodiscard]] engine_time_t end_time() const noexcept { return ops().end_time(m_impl); }
        [[nodiscard]] EvaluationClock evaluation_clock() const { return ops().evaluation_clock(m_impl); }
        void request_engine_stop() const { ops().request_engine_stop(m_impl); }
        [[nodiscard]] bool is_stop_requested() const noexcept { return ops().is_stop_requested(m_impl); }
        void add_before_evaluation_notification(std::function<void()> fn) const
        {
            ops().add_before_evaluation_notification(m_impl, std::move(fn));
        }
        void add_after_evaluation_notification(std::function<void()> fn) const
        {
            ops().add_after_evaluation_notification(m_impl, std::move(fn));
        }
        void add_life_cycle_observer(EvaluationLifeCycleObserver *observer) const
        {
            ops().add_life_cycle_observer(m_impl, observer);
        }
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

    struct HGRAPH_EXPORT EvaluationRuntimeOps
    {
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
        void (*notify_before_node_evaluation)(void *impl, Node &node);
        void (*notify_after_node_evaluation)(void *impl, Node &node);
        void (*notify_before_stop_node)(void *impl, Node &node);
        void (*notify_after_stop_node)(void *impl, Node &node);
        void (*notify_before_stop_graph)(void *impl, Graph &graph);
        void (*notify_after_stop_graph)(void *impl, Graph &graph);
    };

    class HGRAPH_EXPORT EvaluationRuntime
    {
      public:
        EvaluationRuntime() = default;
        EvaluationRuntime(void *impl, const EvaluationRuntimeOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        explicit operator bool() const noexcept { return valid(); }

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

      private:
        [[nodiscard]] const EvaluationRuntimeOps &ops() const
        {
            if (!valid()) { throw std::logic_error("v2 EvaluationRuntime is not bound to runtime state"); }
            return *m_ops;
        }

        void *m_impl{nullptr};
        const EvaluationRuntimeOps *m_ops{nullptr};
    };

    struct HGRAPH_EXPORT EvaluationEngineOps
    {
        [[nodiscard]] EvaluationMode (*evaluation_mode)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*start_time)(const void *impl) noexcept;
        [[nodiscard]] engine_time_t (*end_time)(const void *impl) noexcept;
        [[nodiscard]] Graph &(*graph)(void *impl);
        [[nodiscard]] const Graph &(*const_graph)(const void *impl);
        void (*run)(void *impl);
        void (*destruct)(void *impl) noexcept;
    };

    class HGRAPH_EXPORT EvaluationEngine
    {
      public:
        EvaluationEngine() = default;
        ~EvaluationEngine();
        EvaluationEngine(const EvaluationEngine &) = delete;
        EvaluationEngine &operator=(const EvaluationEngine &) = delete;
        EvaluationEngine(EvaluationEngine &&other) noexcept;
        EvaluationEngine &operator=(EvaluationEngine &&other) noexcept;

        [[nodiscard]] bool valid() const noexcept { return m_impl != nullptr && m_ops != nullptr; }
        explicit operator bool() const noexcept { return valid(); }

        [[nodiscard]] EvaluationMode evaluation_mode() const noexcept { return ops().evaluation_mode(m_impl); }
        [[nodiscard]] engine_time_t start_time() const noexcept { return ops().start_time(m_impl); }
        [[nodiscard]] engine_time_t end_time() const noexcept { return ops().end_time(m_impl); }
        [[nodiscard]] Graph &graph() { return ops().graph(m_impl); }
        [[nodiscard]] const Graph &graph() const { return ops().const_graph(m_impl); }
        void run() { ops().run(m_impl); }

      private:
        friend class EvaluationEngineBuilder;

        EvaluationEngine(void *impl, const EvaluationEngineOps *ops) noexcept : m_impl(impl), m_ops(ops) {}

        [[nodiscard]] const EvaluationEngineOps &ops() const
        {
            if (!valid()) { throw std::logic_error("v2 EvaluationEngine is not bound to runtime state"); }
            return *m_ops;
        }

        void reset() noexcept;

        void *m_impl{nullptr};
        const EvaluationEngineOps *m_ops{nullptr};
    };

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
        EvaluationEngineBuilder &add_life_cycle_observer(EvaluationLifeCycleObserver *observer);

        [[nodiscard]] EvaluationEngine build() const;

      private:
        std::unique_ptr<GraphBuilder> m_graph_builder;
        EvaluationMode m_evaluation_mode{EvaluationMode::SIMULATION};
        engine_time_t m_start_time{MIN_DT};
        engine_time_t m_end_time{MAX_DT};
        std::vector<EvaluationLifeCycleObserver *> m_life_cycle_observers;
    };
}  // namespace hgraph::v2
