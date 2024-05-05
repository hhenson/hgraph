//
// Created by Howard Henson on 05/05/2024.
//

#ifndef EVALUATION_ENGINE_H
#define EVALUATION_ENGINE_H

#include<functional>
#include<hgraph/util/lifecycle.h>
#include<hgraph/util/date_time.h>

namespace hgraph {
    enum class EvaluationMode : char8_t {
        REAL_TIME = 0,
        SIMULATION = 1,
        RECORDING = 2,
        REPLAY = 3
    };

    struct HGRAPH_EXPORT EvaluationClock {
        virtual ~EvaluationClock();

        [[nodiscard]] virtual engine_time_t evaluation_time() = 0;

        [[nodiscard]] virtual engine_time_t now() = 0;

        [[nodiscard]] virtual engine_time_t next_cycle_evaluation_time() = 0;

        [[nodiscard]] virtual engine_time_delta_t cycle_time() = 0;
    };

    struct HGRAPH_EXPORT EngineEvalautionClock : EvaluationClock {
        [[nodiscard]] virtual engine_time_t set_evaluation_time(engine_time_t et) = 0;

        [[nodiscard]] virtual engine_time_t next_scheduled_evaluation_time() = 0;

        virtual void update_next_scheduled_evaluation_time(engine_time_t et) = 0;

        virtual void advance_to_next_scheduled_time() = 0;

        virtual void mark_push_node_requires_scheduling() = 0;

        [[nodiscard]] virtual bool push_node_requires_scheduling() = 0;

        virtual void reset_push_node_requires_scheduling() = 0;
    };

    struct HGRAPH_EXPORT EngineEvalautionClockDelegate : EngineEvalautionClock {
        explicit EngineEvalautionClockDelegate(EngineEvalautionClock *clock);

        [[nodiscard]] engine_time_t evaluation_time() override;

        [[nodiscard]] engine_time_t now() override;

        [[nodiscard]] engine_time_t next_cycle_evaluation_time() override;

        [[nodiscard]] engine_time_delta_t cycle_time() override;

        [[nodiscard]] engine_time_t set_evaluation_time(engine_time_t et) override;

        [[nodiscard]] engine_time_t next_scheduled_evaluation_time() override;

        void update_next_scheduled_evaluation_time(engine_time_t et) override;

        void advance_to_next_scheduled_time() override;

        void mark_push_node_requires_scheduling() override;

        [[nodiscard]] bool push_node_requires_scheduling() override;

        void reset_push_node_requires_scheduling() override;

    private:
        EngineEvalautionClock *_engine_evalaution_clock;
    };

    struct Graph;
    struct Node;

    struct HGRAPH_EXPORT EvaluationLifeCycleObserver {
        using s_ptr = std::shared_ptr<EvaluationLifeCycleObserver>;

        virtual ~EvaluationLifeCycleObserver() = default;

        virtual void on_before_start_graph(Graph &) {
        };

        virtual void on_after_start_graph(Graph &) {
        };

        virtual void on_before_start_node(Node &) {
        };

        virtual void on_after_start_node(Node &) {
        };

        virtual void on_before_graph_evaluation(Graph &) {
        };

        virtual void on_after_graph_evaluation(Graph &) {
        };

        virtual void on_before_node_evaluation(Node &) {
        };

        virtual void on_after_node_evaluation(Node &) {
        };

        virtual void on_before_stop_node(Node &) {
        };

        virtual void on_after_stop_node(Node &) {
        };

        virtual void on_before_stop_graph(Graph &) {
        };

        virtual void on_after_stop_graph(Graph &) {
        };
    };

    struct HGRAPH_EXPORT EvaluationEngineApi : ComponentLifeCycle {
        [[nodiscard]] virtual EvaluationMode evaluation_mode() const = 0;

        [[nodiscard]] virtual engine_time_t start_time() const = 0;

        [[nodiscard]] virtual engine_time_t end_time() const = 0;

        [[nodiscard]] virtual EvaluationClock &evaluation_clock() = 0;

        virtual void request_engine_stop() = 0;

        virtual bool is_stop_requested() = 0;

        virtual void add_before_evalaution_notification(std::function<void()> &&fn) = 0;

        virtual void add_after_evalaution_notification(std::function<void()> &&fn) = 0;

        virtual void add_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer) = 0;

        virtual void remove_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer) = 0;
    };

    struct EvalautionEngineClock : EvaluationClock {

        virtual void set_evaluation_time(engine_time_t value) = 0;

        virtual engine_time_t next_scheduled_evalaution_time() = 0;

        virtual void update_next_scheduled_evaluation_time(engine_time_t next_time) = 0;

        virtual void advance_to_next_scheduled_time() = 0;

        virtual void mark_push_node_requires_scheduling() = 0;

        [[nodiscard]] virtual bool push_node_requires_scheduling() = 0;

        virtual void reset_push_node_requires_scheduling() = 0;
    };

    struct EvaluationEngineDelegate;

    struct EvaluationEngine : EvaluationEngineApi {
        virtual EngineEvalautionClock& engine_evaluation_clock() = 0;

        virtual void advance_engine_time() = 0;

        virtual void notify_before_evaluation() = 0;

        virtual void notify_after_evaluation() = 0;

        virtual void notify_before_start_graph(Graph& graph) = 0;
        virtual void notify_after_start_graph(Graph& graph) = 0;

        virtual void notify_before_start_node(Node& node) = 0;
        virtual void notify_after_start_node(Node& node) = 0;

        virtual void notify_before_graph_evaluation(Graph& graph) = 0;
        virtual void notify_after_graph_evaluation(Graph& graph) = 0;

        virtual void notify_before_node_evaluation(Node& node) = 0;
        virtual void notify_after_node_evaluation(Node& node) = 0;

        virtual void notify_before_stop_node(Node& node) = 0;
        virtual void notify_after_stop_node(Node& node) = 0;

        virtual void notify_before_stop_graph(Graph& graph) = 0;
        virtual void notify_after_stop_graph(Graph& graph) = 0;

        friend EvaluationEngineDelegate;
    };

    struct HGRAPH_EXPORT EvaluationEngineDelegate : EvaluationEngine {
        explicit EvaluationEngineDelegate(EvaluationEngine *api);

        [[nodiscard]] EvaluationMode evaluation_mode() const override;

        [[nodiscard]] engine_time_t start_time() const override;

        [[nodiscard]] engine_time_t end_time() const override;

        [[nodiscard]] EvaluationClock &evaluation_clock() override;

        EngineEvalautionClock & engine_evaluation_clock() override;

        void request_engine_stop() override;

        bool is_stop_requested() override;

        void add_before_evalaution_notification(std::function<void()> &&fn) override;

        void add_after_evalaution_notification(std::function<void()> &&fn) override;

        void add_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer) override;

        void remove_life_cycle_observer(EvaluationLifeCycleObserver::s_ptr observer) override;

        void advance_engine_time() override;

        void notify_before_evaluation() override;

        void notify_after_evaluation() override;

        void notify_before_start_graph(Graph &graph) override;

        void notify_after_start_graph(Graph &graph) override;

        void notify_before_start_node(Node &node) override;

        void notify_after_start_node(Node &node) override;

        void notify_before_graph_evaluation(Graph &graph) override;

        void notify_after_graph_evaluation(Graph &graph) override;

        void notify_before_node_evaluation(Node &node) override;

        void notify_after_node_evaluation(Node &node) override;

        void notify_before_stop_node(Node &node) override;

        void notify_after_stop_node(Node &node) override;

        void notify_before_stop_graph(Graph &graph) override;

        void notify_after_stop_graph(Graph &graph) override;

    protected:
        void initialise() override;

        void start() override;

        void stop() override;

        void dispose() override;

    private:
        EvaluationEngine *_evaluation_engine;
    };
}

#endif //EVALUATION_ENGINE_H
