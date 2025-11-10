//
// Created by Howard Henson on 05/05/2024.
//

#ifndef EVALUATION_ENGINE_H
#define EVALUATION_ENGINE_H

#include <condition_variable>
#include <hgraph/runtime/graph_executor.h>
#include <hgraph/util/lifecycle.h>

namespace hgraph {
    struct HGRAPH_EXPORT EvaluationClock : nb::intrusive_base {
        using ptr = nanobind::ref<EvaluationClock>;

        [[nodiscard]] virtual engine_time_t evaluation_time() const = 0;

        [[nodiscard]] virtual engine_time_t now() const = 0;

        [[nodiscard]] virtual engine_time_t next_cycle_evaluation_time() const = 0;

        [[nodiscard]] virtual engine_time_delta_t cycle_time() const = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT EngineEvaluationClock : EvaluationClock {
        using ptr = nanobind::ref<EngineEvaluationClock>;

        virtual void set_evaluation_time(engine_time_t et) = 0;

        [[nodiscard]] virtual engine_time_t next_scheduled_evaluation_time() const = 0;

        virtual void update_next_scheduled_evaluation_time(engine_time_t et) = 0;

        virtual void advance_to_next_scheduled_time() = 0;

        virtual void mark_push_node_requires_scheduling() = 0;

        [[nodiscard]] virtual bool push_node_requires_scheduling() const = 0;

        virtual void reset_push_node_requires_scheduling() = 0;

        // Performance: Direct access to evaluation time for caching
        [[nodiscard]] virtual const engine_time_t* evaluation_time_ptr() const = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT EngineEvaluationClockDelegate : EngineEvaluationClock {
        explicit EngineEvaluationClockDelegate(ptr clock);

        [[nodiscard]] engine_time_t evaluation_time() const override;

        [[nodiscard]] engine_time_t now() const override;

        [[nodiscard]] engine_time_t next_cycle_evaluation_time() const override;

        [[nodiscard]] engine_time_delta_t cycle_time() const override;

        void set_evaluation_time(engine_time_t et) override;

        [[nodiscard]] engine_time_t next_scheduled_evaluation_time() const override;

        void update_next_scheduled_evaluation_time(engine_time_t et) override;

        void advance_to_next_scheduled_time() override;

        void mark_push_node_requires_scheduling() override;

        [[nodiscard]] bool push_node_requires_scheduling() const override;

        void reset_push_node_requires_scheduling() override;

        [[nodiscard]] const engine_time_t* evaluation_time_ptr() const override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        ptr _engine_evalaution_clock;
    };

    struct Graph;
    struct Node;

    struct HGRAPH_EXPORT EvaluationEngineApi : ComponentLifeCycle {
        using ptr = nanobind::ref<EvaluationEngineApi>;

        [[nodiscard]] virtual EvaluationMode evaluation_mode() const = 0;

        [[nodiscard]] virtual engine_time_t start_time() const = 0;

        [[nodiscard]] virtual engine_time_t end_time() const = 0;

        EvaluationClock::ptr evaluation_clock() const {
            return const_cast<EvaluationEngineApi *>(this)->evaluation_clock();
        };

        [[nodiscard]] virtual EvaluationClock::ptr evaluation_clock() = 0;

        virtual void request_engine_stop() = 0;

        virtual bool is_stop_requested() = 0;

        virtual void add_before_evaluation_notification(std::function<void()> &&fn) = 0;

        virtual void add_after_evaluation_notification(std::function<void()> &&fn) = 0;

        virtual void add_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) = 0;

        virtual void remove_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct EvaluationEngineDelegate;

    struct EvaluationEngine : EvaluationEngineApi {
        using ptr = nanobind::ref<EvaluationEngine>;

        virtual EngineEvaluationClock::ptr engine_evaluation_clock() = 0;

        EngineEvaluationClock::ptr engine_evaluation_clock() const {
            return const_cast<EvaluationEngine *>(this)->engine_evaluation_clock();
        };

        virtual void advance_engine_time() = 0;

        virtual void notify_before_evaluation() = 0;

        virtual void notify_after_evaluation() = 0;

        virtual void notify_before_start_graph(graph_ptr graph) = 0;

        virtual void notify_after_start_graph(graph_ptr graph) = 0;

        virtual void notify_before_start_node(node_ptr node) = 0;

        virtual void notify_after_start_node(node_ptr node) = 0;

        virtual void notify_before_graph_evaluation(graph_ptr graph) = 0;

        virtual void notify_after_graph_evaluation(graph_ptr graph) = 0;

        virtual void notify_after_push_nodes_evaluation(graph_ptr graph) = 0;

        virtual void notify_before_node_evaluation(node_ptr node) = 0;

        virtual void notify_after_node_evaluation(node_ptr node) = 0;

        virtual void notify_before_stop_node(node_ptr node) = 0;

        virtual void notify_after_stop_node(node_ptr node) = 0;

        virtual void notify_before_stop_graph(graph_ptr graph) = 0;

        virtual void notify_after_stop_graph(graph_ptr graph) = 0;

        static void register_with_nanobind(nb::module_ &m);

        friend EvaluationEngineDelegate;
    };

    struct NotifyGraphEvaluation {
        NotifyGraphEvaluation(EvaluationEngine::ptr evaluation_engine, graph_ptr graph);

        ~NotifyGraphEvaluation() noexcept;

    private:
        EvaluationEngine::ptr _evaluation_engine;
        graph_ptr _graph;
    };

    struct NotifyNodeEvaluation {
        NotifyNodeEvaluation(EvaluationEngine::ptr evaluation_engine, node_ptr node);

        ~NotifyNodeEvaluation() noexcept;

    private:
        EvaluationEngine::ptr _evaluation_engine;
        node_ptr _node;
    };

    struct HGRAPH_EXPORT EvaluationEngineDelegate : EvaluationEngine {
        explicit EvaluationEngineDelegate(ptr api);

        [[nodiscard]] EvaluationMode evaluation_mode() const override;

        [[nodiscard]] engine_time_t start_time() const override;

        [[nodiscard]] engine_time_t end_time() const override;

        [[nodiscard]] EvaluationClock::ptr evaluation_clock() override;

        EngineEvaluationClock::ptr engine_evaluation_clock() override;

        void request_engine_stop() override;

        bool is_stop_requested() override;

        void add_before_evaluation_notification(std::function<void()> &&fn) override;

        void add_after_evaluation_notification(std::function<void()> &&fn) override;

        void add_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) override;

        void remove_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) override;

        void advance_engine_time() override;

        void notify_before_evaluation() override;

        void notify_after_evaluation() override;

        void notify_before_start_graph(graph_ptr graph) override;

        void notify_after_start_graph(graph_ptr graph) override;

        void notify_before_start_node(node_ptr node) override;

        void notify_after_start_node(node_ptr node) override;

        void notify_before_graph_evaluation(graph_ptr graph) override;

        void notify_after_graph_evaluation(graph_ptr graph) override;

        void notify_after_push_nodes_evaluation(graph_ptr graph) override;

        void notify_before_node_evaluation(node_ptr node) override;

        void notify_after_node_evaluation(node_ptr node) override;

        void notify_before_stop_node(node_ptr node) override;

        void notify_after_stop_node(node_ptr node) override;

        void notify_before_stop_graph(graph_ptr graph) override;

        void notify_after_stop_graph(graph_ptr graph) override;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        void initialise() override;

        void start() override;

        void stop() override;

        void dispose() override;

    private:
        ptr _evaluation_engine;
    };

    struct BaseEvaluationClock : EngineEvaluationClock {
        explicit BaseEvaluationClock(engine_time_t start_time);

        void set_evaluation_time(engine_time_t et) override;

        [[nodiscard]] engine_time_t evaluation_time() const override;

        [[nodiscard]] engine_time_t next_cycle_evaluation_time() const override;

        [[nodiscard]] engine_time_t next_scheduled_evaluation_time() const override;

        void update_next_scheduled_evaluation_time(engine_time_t scheduled_time) override;

        // Performance: Direct access to evaluation time for caching
        [[nodiscard]] const engine_time_t* evaluation_time_ptr() const override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        engine_time_t _evaluation_time;
        engine_time_t _next_scheduled_evaluation_time;
    };

    struct SimulationEvaluationClock : BaseEvaluationClock {
        using ptr = nanobind::ref<SimulationEvaluationClock>;

        explicit SimulationEvaluationClock(engine_time_t current_time);

        void set_evaluation_time(engine_time_t value) override;

        [[nodiscard]] engine_time_t now() const override;

        [[nodiscard]] engine_time_delta_t cycle_time() const override;

        void advance_to_next_scheduled_time() override;

        void mark_push_node_requires_scheduling() override;

        [[nodiscard]] bool push_node_requires_scheduling() const override;

        void reset_push_node_requires_scheduling() override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        engine_time_t _system_clock_at_start_of_evaluation;
    };

    struct RealTimeEvaluationClock : BaseEvaluationClock {
        using ptr = nanobind::ref<RealTimeEvaluationClock>;

        explicit RealTimeEvaluationClock(engine_time_t start_time);

        engine_time_t now() const override;

        engine_time_delta_t cycle_time() const override;

        void mark_push_node_requires_scheduling() override;

        bool push_node_requires_scheduling() const override;

        void advance_to_next_scheduled_time() override;

        void reset_push_node_requires_scheduling() override;

        void set_alarm(engine_time_t alarm_time, const std::string &name, std::function<void(engine_time_t)> callback);

        void cancel_alarm(const std::string &name);

        static void register_with_nanobind(nb::module_ &m);

    private:
        bool _push_node_requires_scheduling;
        bool _ready_to_push;
        engine_time_t _last_time_allowed_push;

        mutable std::mutex _condition_mutex;
        std::condition_variable _push_node_requires_scheduling_condition;

        std::set<std::pair<engine_time_t, std::string> > _alarms;
        std::map<std::pair<engine_time_t, std::string>, std::function<void(engine_time_t)> > _alarm_callbacks;
    };

    struct EvaluationEngineImpl : EvaluationEngine {
        EvaluationEngineImpl(EngineEvaluationClock::ptr clock, engine_time_t start_time, engine_time_t end_time,
                             EvaluationMode run_mode);

    protected:
        void initialise() override;

        void start() override;

        void stop() override;

        void dispose() override;

    public:
        [[nodiscard]] EvaluationMode evaluation_mode() const override;

        [[nodiscard]] engine_time_t start_time() const override;

        [[nodiscard]] engine_time_t end_time() const override;

        [[nodiscard]] EvaluationClock::ptr evaluation_clock() override;

        void request_engine_stop() override;

        bool is_stop_requested() override;

        void add_before_evaluation_notification(std::function<void()> &&fn) override;

        void add_after_evaluation_notification(std::function<void()> &&fn) override;

        void add_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) override;

        void remove_life_cycle_observer(EvaluationLifeCycleObserver::ptr observer) override;

        EngineEvaluationClock::ptr engine_evaluation_clock() override;

        void advance_engine_time() override;

        void notify_before_evaluation() override;

        void notify_after_evaluation() override;

        void notify_before_start_graph(graph_ptr graph) override;

        void notify_after_start_graph(graph_ptr graph) override;

        void notify_before_start_node(node_ptr node) override;

        void notify_after_start_node(node_ptr node) override;

        void notify_before_graph_evaluation(graph_ptr graph) override;

        void notify_after_graph_evaluation(graph_ptr graph) override;

        void notify_after_push_nodes_evaluation(graph_ptr graph) override;

        void notify_before_node_evaluation(node_ptr node) override;

        void notify_after_node_evaluation(node_ptr node) override;

        void notify_before_stop_node(node_ptr node) override;

        void notify_after_stop_node(node_ptr node) override;

        void notify_before_stop_graph(graph_ptr graph) override;

        void notify_after_stop_graph(graph_ptr graph) override;

        static void register_with_nanobind(nb::module_ &m);

    private:
        EngineEvaluationClock::ptr _clock;
        engine_time_t _start_time;
        engine_time_t _end_time;
        EvaluationMode _run_mode;
        bool _stop_requested{false};
        std::vector<EvaluationLifeCycleObserver::ptr> _life_cycle_observers{};
        std::vector<std::function<void()> > _before_evaluation_notification{};
        std::vector<std::function<void()> > _after_evaluation_notification{};
    };
} // namespace hgraph

#endif  // EVALUATION_ENGINE_H