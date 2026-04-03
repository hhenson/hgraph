#include <hgraph/types/v2/evaluation_engine.h>

#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/node.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        struct BaseClockState
        {
            explicit BaseClockState(engine_time_t start_time)
                : m_evaluation_time(start_time), m_next_scheduled_evaluation_time(MAX_DT)
            {
            }

            [[nodiscard]] engine_time_t evaluation_time() const noexcept { return m_evaluation_time; }
            [[nodiscard]] engine_time_t next_cycle_evaluation_time() const noexcept { return m_evaluation_time + MIN_TD; }
            [[nodiscard]] engine_time_t next_scheduled_evaluation_time() const noexcept
            {
                return m_next_scheduled_evaluation_time;
            }

            void update_next_scheduled_evaluation_time(engine_time_t scheduled_time)
            {
                if (scheduled_time == m_evaluation_time) { return; }
                m_next_scheduled_evaluation_time =
                    std::max(next_cycle_evaluation_time(), std::min(m_next_scheduled_evaluation_time, scheduled_time));
            }

            [[nodiscard]] const engine_time_t *evaluation_time_ptr() const noexcept { return &m_evaluation_time; }

          protected:
            void set_base_evaluation_time(engine_time_t evaluation_time)
            {
                m_evaluation_time = evaluation_time;
                m_next_scheduled_evaluation_time = MAX_DT;
            }

            engine_time_t m_evaluation_time{MIN_DT};
            engine_time_t m_next_scheduled_evaluation_time{MAX_DT};
        };

        struct SimulationClockState : BaseClockState
        {
            explicit SimulationClockState(engine_time_t start_time)
                : BaseClockState(start_time),
                  m_system_clock_at_start_of_evaluation(
                      std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now()))
            {
            }

            [[nodiscard]] engine_time_t now() const
            {
                return evaluation_time() + cycle_time();
            }

            [[nodiscard]] engine_time_delta_t cycle_time() const
            {
                return std::chrono::duration_cast<engine_time_delta_t>(
                    std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now()) -
                    m_system_clock_at_start_of_evaluation);
            }

            void set_evaluation_time(engine_time_t evaluation_time)
            {
                set_base_evaluation_time(evaluation_time);
                m_system_clock_at_start_of_evaluation =
                    std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
            }

            void advance_to_next_scheduled_time()
            {
                set_evaluation_time(next_scheduled_evaluation_time());
            }

            void mark_push_node_requires_scheduling()
            {
                throw std::runtime_error("Simulation mode does not support push nodes.");
            }

            [[nodiscard]] bool push_node_requires_scheduling() const noexcept
            {
                return false;
            }

            void reset_push_node_requires_scheduling()
            {
                throw std::runtime_error("Simulation mode does not support push nodes.");
            }

            engine_time_t m_system_clock_at_start_of_evaluation{MIN_DT};
        };

        struct RealTimeClockState : BaseClockState
        {
            explicit RealTimeClockState(engine_time_t start_time) : BaseClockState(start_time) {}

            [[nodiscard]] engine_time_t now() const
            {
                return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
            }

            [[nodiscard]] engine_time_delta_t cycle_time() const
            {
                return std::chrono::duration_cast<engine_time_delta_t>(now() - evaluation_time());
            }

            void set_evaluation_time(engine_time_t evaluation_time)
            {
                set_base_evaluation_time(evaluation_time);
            }

            void mark_push_node_requires_scheduling()
            {
                std::unique_lock<std::mutex> lock(m_condition_mutex);
                m_push_node_requires_scheduling = true;
                m_push_node_requires_scheduling_condition.notify_all();
            }

            [[nodiscard]] bool push_node_requires_scheduling() const noexcept
            {
                if (!m_ready_to_push) { return false; }
                std::unique_lock<std::mutex> lock(m_condition_mutex);
                return m_push_node_requires_scheduling;
            }

            void advance_to_next_scheduled_time()
            {
                engine_time_t next_scheduled_time = next_scheduled_evaluation_time();
                engine_time_t current_time = now();

                m_ready_to_push = false;
                if (next_scheduled_time > evaluation_time() + MIN_TD ||
                    current_time > m_last_time_allowed_push + std::chrono::seconds(15)) {
                    {
                        std::unique_lock<std::mutex> lock(m_condition_mutex);
                        m_ready_to_push = true;
                        m_last_time_allowed_push = current_time;

                        while (current_time < next_scheduled_time && !m_push_node_requires_scheduling) {
                            const auto sleep_time =
                                std::chrono::duration_cast<std::chrono::milliseconds>(next_scheduled_time - current_time);
                            m_push_node_requires_scheduling_condition.wait_for(
                                lock,
                                std::min(std::chrono::milliseconds{10000}, sleep_time));
                            current_time = now();
                        }
                    }
                }

                set_evaluation_time(std::min(next_scheduled_time, std::max(next_cycle_evaluation_time(), current_time)));
            }

            void reset_push_node_requires_scheduling()
            {
                std::unique_lock<std::mutex> lock(m_condition_mutex);
                m_push_node_requires_scheduling = false;
            }

            bool m_push_node_requires_scheduling{false};
            bool m_ready_to_push{false};
            engine_time_t m_last_time_allowed_push{MIN_DT};
            mutable std::mutex m_condition_mutex;
            std::condition_variable m_push_node_requires_scheduling_condition;
        };

        template <typename TClockState>
        struct ClockDispatch
        {
            [[nodiscard]] static engine_time_t evaluation_time_impl(const void *impl) noexcept
            {
                return static_cast<const TClockState *>(impl)->evaluation_time();
            }

            [[nodiscard]] static engine_time_t now_impl(const void *impl)
            {
                return static_cast<const TClockState *>(impl)->now();
            }

            [[nodiscard]] static engine_time_delta_t cycle_time_impl(const void *impl)
            {
                return static_cast<const TClockState *>(impl)->cycle_time();
            }

            [[nodiscard]] static engine_time_t next_cycle_evaluation_time_impl(const void *impl) noexcept
            {
                return static_cast<const TClockState *>(impl)->next_cycle_evaluation_time();
            }

            static void set_evaluation_time_impl(void *impl, engine_time_t evaluation_time)
            {
                static_cast<TClockState *>(impl)->set_evaluation_time(evaluation_time);
            }

            [[nodiscard]] static engine_time_t next_scheduled_evaluation_time_impl(const void *impl) noexcept
            {
                return static_cast<const TClockState *>(impl)->next_scheduled_evaluation_time();
            }

            static void update_next_scheduled_evaluation_time_impl(void *impl, engine_time_t evaluation_time)
            {
                static_cast<TClockState *>(impl)->update_next_scheduled_evaluation_time(evaluation_time);
            }

            static void advance_to_next_scheduled_time_impl(void *impl)
            {
                static_cast<TClockState *>(impl)->advance_to_next_scheduled_time();
            }

            static void mark_push_node_requires_scheduling_impl(void *impl)
            {
                static_cast<TClockState *>(impl)->mark_push_node_requires_scheduling();
            }

            [[nodiscard]] static bool push_node_requires_scheduling_impl(const void *impl) noexcept
            {
                return static_cast<const TClockState *>(impl)->push_node_requires_scheduling();
            }

            static void reset_push_node_requires_scheduling_impl(void *impl)
            {
                static_cast<TClockState *>(impl)->reset_push_node_requires_scheduling();
            }

            [[nodiscard]] static const engine_time_t *evaluation_time_ptr_impl(const void *impl) noexcept
            {
                return static_cast<const TClockState *>(impl)->evaluation_time_ptr();
            }

            static const EngineEvaluationClockOps value;
        };

        template <typename TClockState>
        const EngineEvaluationClockOps ClockDispatch<TClockState>::value{
            &ClockDispatch<TClockState>::evaluation_time_impl,
            &ClockDispatch<TClockState>::now_impl,
            &ClockDispatch<TClockState>::cycle_time_impl,
            &ClockDispatch<TClockState>::next_cycle_evaluation_time_impl,
            &ClockDispatch<TClockState>::set_evaluation_time_impl,
            &ClockDispatch<TClockState>::next_scheduled_evaluation_time_impl,
            &ClockDispatch<TClockState>::update_next_scheduled_evaluation_time_impl,
            &ClockDispatch<TClockState>::advance_to_next_scheduled_time_impl,
            &ClockDispatch<TClockState>::mark_push_node_requires_scheduling_impl,
            &ClockDispatch<TClockState>::push_node_requires_scheduling_impl,
            &ClockDispatch<TClockState>::reset_push_node_requires_scheduling_impl,
            &ClockDispatch<TClockState>::evaluation_time_ptr_impl,
        };

        template <typename TClockState, EvaluationMode TMode>
        struct EvaluationEngineState
        {
            TClockState clock;
            engine_time_t start_time{MIN_DT};
            engine_time_t end_time{MAX_DT};
            bool stop_requested{false};
            std::vector<EvaluationLifeCycleObserver *> life_cycle_observers;
            std::vector<std::function<void()>> before_evaluation_notifications;
            std::vector<std::function<void()>> after_evaluation_notifications;
            Graph graph;

            EvaluationEngineState(Graph graph, engine_time_t start_time, engine_time_t end_time)
                : clock(start_time), start_time(start_time), end_time(end_time), graph(std::move(graph))
            {
            }

            [[nodiscard]] static EvaluationEngineState &state_from(void *impl)
            {
                return *static_cast<EvaluationEngineState *>(impl);
            }

            [[nodiscard]] static const EvaluationEngineState &state_from(const void *impl)
            {
                return *static_cast<const EvaluationEngineState *>(impl);
            }

            [[nodiscard]] static EvaluationMode evaluation_mode_impl(const void *impl) noexcept
            {
                static_cast<void>(impl);
                return TMode;
            }

            [[nodiscard]] static engine_time_t start_time_impl(const void *impl) noexcept
            {
                return state_from(impl).start_time;
            }

            [[nodiscard]] static engine_time_t end_time_impl(const void *impl) noexcept
            {
                return state_from(impl).end_time;
            }

            [[nodiscard]] static EvaluationClock evaluation_clock_impl(const void *impl)
            {
                return {&state_from(impl).clock, &ClockDispatch<TClockState>::value};
            }

            [[nodiscard]] static EvaluationEngineApi evaluation_engine_api_impl(void *impl) noexcept
            {
                return {impl, &k_api_ops};
            }

            [[nodiscard]] static EngineEvaluationClock engine_evaluation_clock_impl(void *impl)
            {
                return {&state_from(impl).clock, &ClockDispatch<TClockState>::value};
            }

            static void request_engine_stop_impl(void *impl)
            {
                state_from(impl).stop_requested = true;
            }

            [[nodiscard]] static bool is_stop_requested_impl(const void *impl) noexcept
            {
                return state_from(impl).stop_requested;
            }

            static void add_before_evaluation_notification_impl(void *impl, std::function<void()> fn)
            {
                state_from(impl).before_evaluation_notifications.emplace_back(std::move(fn));
            }

            static void add_after_evaluation_notification_impl(void *impl, std::function<void()> fn)
            {
                state_from(impl).after_evaluation_notifications.emplace_back(std::move(fn));
            }

            static void add_life_cycle_observer_impl(void *impl, EvaluationLifeCycleObserver *observer)
            {
                if (observer == nullptr) { return; }
                state_from(impl).life_cycle_observers.push_back(observer);
            }

            static void remove_life_cycle_observer_impl(void *impl, EvaluationLifeCycleObserver *observer)
            {
                auto &observers = state_from(impl).life_cycle_observers;
                const auto it = std::find(observers.begin(), observers.end(), observer);
                if (it == observers.end()) {
                    throw std::invalid_argument("EvaluationLifeCycleObserver was not registered");
                }
                observers.erase(it);
            }

            static void notify_before_evaluation_impl(void *impl)
            {
                auto &state = state_from(impl);
                // Match the existing C++ engine semantics: callbacks queued while draining
                // are processed in the same notification pass rather than deferred.
                while (!state.before_evaluation_notifications.empty()) {
                    auto notifications = std::move(state.before_evaluation_notifications);
                    state.before_evaluation_notifications.clear();
                    for (auto &notification : notifications) { notification(); }
                }
            }

            static void notify_after_evaluation_impl(void *impl)
            {
                auto &state = state_from(impl);
                // Match the existing C++ engine semantics: callbacks queued while draining
                // are processed in the same notification pass rather than deferred.
                while (!state.after_evaluation_notifications.empty()) {
                    auto notifications = std::move(state.after_evaluation_notifications);
                    state.after_evaluation_notifications.clear();
                    for (auto it = notifications.rbegin(); it != notifications.rend(); ++it) { (*it)(); }
                }
            }

            static void advance_engine_time_impl(void *impl)
            {
                auto &state = state_from(impl);
                if (state.stop_requested) {
                    state.clock.set_evaluation_time(state.end_time + MIN_TD);
                    return;
                }

                state.clock.update_next_scheduled_evaluation_time(state.end_time + MIN_TD);
                state.clock.advance_to_next_scheduled_time();
            }

            static void notify_before_start_graph_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_before_start_graph(graph); });
            }

            static void notify_after_start_graph_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_after_start_graph(graph); });
            }

            static void notify_before_start_node_impl(void *impl, Node &node)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_before_start_node(node); });
            }

            static void notify_after_start_node_impl(void *impl, Node &node)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_after_start_node(node); });
            }

            static void notify_before_graph_evaluation_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl),
                                 [&](EvaluationLifeCycleObserver &observer) { observer.on_before_graph_evaluation(graph); });
            }

            static void notify_after_graph_evaluation_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl),
                                 [&](EvaluationLifeCycleObserver &observer) { observer.on_after_graph_evaluation(graph); });
            }

            static void notify_after_push_nodes_evaluation_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl),
                                 [&](EvaluationLifeCycleObserver &observer) { observer.on_after_graph_push_nodes_evaluation(graph); });
            }

            static void notify_before_node_evaluation_impl(void *impl, Node &node)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_before_node_evaluation(node); });
            }

            static void notify_after_node_evaluation_impl(void *impl, Node &node)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_after_node_evaluation(node); });
            }

            static void notify_before_stop_node_impl(void *impl, Node &node)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_before_stop_node(node); });
            }

            static void notify_after_stop_node_impl(void *impl, Node &node)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_after_stop_node(node); });
            }

            static void notify_before_stop_graph_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_before_stop_graph(graph); });
            }

            static void notify_after_stop_graph_impl(void *impl, Graph &graph)
            {
                notify_observers(state_from(impl), [&](EvaluationLifeCycleObserver &observer) { observer.on_after_stop_graph(graph); });
            }

            [[nodiscard]] static Graph &graph_impl(void *impl)
            {
                return state_from(impl).graph;
            }

            [[nodiscard]] static const Graph &const_graph_impl(const void *impl)
            {
                return state_from(impl).graph;
            }

            static void run_impl(void *impl)
            {
                auto &state = state_from(impl);
                if (state.end_time <= state.start_time) {
                    if (state.end_time < state.start_time) {
                        throw std::invalid_argument("End time cannot be before the start time");
                    }
                    throw std::invalid_argument("End time cannot be equal to the start time");
                }

                state.graph.start();
                try {
                    while (state.clock.evaluation_time() < state.end_time) {
                        notify_before_evaluation_impl(impl);
                        state.graph.evaluate(state.clock.evaluation_time());
                        notify_after_evaluation_impl(impl);
                        advance_engine_time_impl(impl);
                    }
                } catch (...) {
                    try {
                        state.graph.stop();
                    } catch (...) {
                    }
                    throw;
                }

                state.graph.stop();
            }

            static void destruct_impl(void *impl) noexcept
            {
                delete static_cast<EvaluationEngineState *>(impl);
            }

            template <typename TNotify>
            static void notify_observers(const EvaluationEngineState &state, TNotify &&notify)
            {
                for (auto *observer : state.life_cycle_observers) {
                    if (observer != nullptr) { notify(*observer); }
                }
            }

            static const EvaluationEngineApiOps k_api_ops;
            static const EvaluationEngineOps k_engine_ops;
        };

        template <typename TClockState, EvaluationMode TMode>
        const EvaluationEngineApiOps EvaluationEngineState<TClockState, TMode>::k_api_ops{
            &EvaluationEngineState<TClockState, TMode>::evaluation_mode_impl,
            &EvaluationEngineState<TClockState, TMode>::start_time_impl,
            &EvaluationEngineState<TClockState, TMode>::end_time_impl,
            &EvaluationEngineState<TClockState, TMode>::evaluation_clock_impl,
            &EvaluationEngineState<TClockState, TMode>::request_engine_stop_impl,
            &EvaluationEngineState<TClockState, TMode>::is_stop_requested_impl,
            &EvaluationEngineState<TClockState, TMode>::add_before_evaluation_notification_impl,
            &EvaluationEngineState<TClockState, TMode>::add_after_evaluation_notification_impl,
            &EvaluationEngineState<TClockState, TMode>::add_life_cycle_observer_impl,
            &EvaluationEngineState<TClockState, TMode>::remove_life_cycle_observer_impl,
        };

        template <typename TClockState, EvaluationMode TMode>
        const EvaluationEngineOps EvaluationEngineState<TClockState, TMode>::k_engine_ops{
            &EvaluationEngineState<TClockState, TMode>::evaluation_mode_impl,
            &EvaluationEngineState<TClockState, TMode>::start_time_impl,
            &EvaluationEngineState<TClockState, TMode>::end_time_impl,
            &EvaluationEngineState<TClockState, TMode>::graph_impl,
            &EvaluationEngineState<TClockState, TMode>::const_graph_impl,
            &EvaluationEngineState<TClockState, TMode>::evaluation_engine_api_impl,
            &EvaluationEngineState<TClockState, TMode>::engine_evaluation_clock_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::advance_engine_time_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_start_graph_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_start_graph_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_start_node_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_start_node_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_graph_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_graph_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_push_nodes_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_node_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_node_evaluation_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_stop_node_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_stop_node_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_before_stop_graph_impl,
            &EvaluationEngineState<TClockState, TMode>::notify_after_stop_graph_impl,
            &EvaluationEngineState<TClockState, TMode>::run_impl,
            &EvaluationEngineState<TClockState, TMode>::destruct_impl,
        };

        using SimulationEvaluationEngineState = EvaluationEngineState<SimulationClockState, EvaluationMode::SIMULATION>;
        using RealTimeEvaluationEngineState = EvaluationEngineState<RealTimeClockState, EvaluationMode::REAL_TIME>;
    }  // namespace

    EvaluationEngine::~EvaluationEngine()
    {
        reset();
    }

    EvaluationEngine::EvaluationEngine(EvaluationEngine &&other) noexcept : m_impl(other.m_impl), m_ops(other.m_ops)
    {
        other.m_impl = nullptr;
        other.m_ops = nullptr;
    }

    EvaluationEngine &EvaluationEngine::operator=(EvaluationEngine &&other) noexcept
    {
        if (this != &other) {
            reset();
            m_impl = other.m_impl;
            m_ops = other.m_ops;
            other.m_impl = nullptr;
            other.m_ops = nullptr;
        }
        return *this;
    }

    void EvaluationEngine::reset() noexcept
    {
        if (m_impl != nullptr && m_ops != nullptr) { m_ops->destruct(m_impl); }
        m_impl = nullptr;
        m_ops = nullptr;
    }

    EvaluationEngineBuilder::EvaluationEngineBuilder() = default;

    EvaluationEngineBuilder::~EvaluationEngineBuilder() = default;

    EvaluationEngineBuilder::EvaluationEngineBuilder(const EvaluationEngineBuilder &other)
        : m_graph_builder(other.m_graph_builder ? std::make_unique<GraphBuilder>(*other.m_graph_builder) : nullptr),
          m_evaluation_mode(other.m_evaluation_mode),
          m_start_time(other.m_start_time),
          m_end_time(other.m_end_time),
          m_life_cycle_observers(other.m_life_cycle_observers)
    {
    }

    EvaluationEngineBuilder &EvaluationEngineBuilder::operator=(const EvaluationEngineBuilder &other)
    {
        if (this != &other) {
            m_graph_builder = other.m_graph_builder ? std::make_unique<GraphBuilder>(*other.m_graph_builder) : nullptr;
            m_evaluation_mode = other.m_evaluation_mode;
            m_start_time = other.m_start_time;
            m_end_time = other.m_end_time;
            m_life_cycle_observers = other.m_life_cycle_observers;
        }
        return *this;
    }

    EvaluationEngineBuilder::EvaluationEngineBuilder(EvaluationEngineBuilder &&other) noexcept = default;

    EvaluationEngineBuilder &EvaluationEngineBuilder::operator=(EvaluationEngineBuilder &&other) noexcept = default;

    EvaluationEngineBuilder &EvaluationEngineBuilder::graph_builder(GraphBuilder graph_builder)
    {
        m_graph_builder = std::make_unique<GraphBuilder>(std::move(graph_builder));
        return *this;
    }

    EvaluationEngineBuilder &EvaluationEngineBuilder::evaluation_mode(EvaluationMode evaluation_mode) noexcept
    {
        m_evaluation_mode = evaluation_mode;
        return *this;
    }

    EvaluationEngineBuilder &EvaluationEngineBuilder::start_time(engine_time_t start_time) noexcept
    {
        m_start_time = start_time;
        return *this;
    }

    EvaluationEngineBuilder &EvaluationEngineBuilder::end_time(engine_time_t end_time) noexcept
    {
        m_end_time = end_time;
        return *this;
    }

    EvaluationEngineBuilder &EvaluationEngineBuilder::add_life_cycle_observer(EvaluationLifeCycleObserver *observer)
    {
        if (observer != nullptr) { m_life_cycle_observers.push_back(observer); }
        return *this;
    }

    EvaluationEngine EvaluationEngineBuilder::build() const
    {
        if (!m_graph_builder) { throw std::logic_error("v2 evaluation engine builder requires a graph builder"); }

        switch (m_evaluation_mode) {
            case EvaluationMode::SIMULATION: {
                // Hold the state under temporary ownership until the engine is attached and observer registration completes.
                auto state = std::make_unique<SimulationEvaluationEngineState>(m_graph_builder->make_graph(), m_start_time, m_end_time);
                state->graph.set_evaluation_engine(state.get(), &SimulationEvaluationEngineState::k_engine_ops);
                for (auto *observer : m_life_cycle_observers) {
                    SimulationEvaluationEngineState::add_life_cycle_observer_impl(state.get(), observer);
                }
                return {state.release(), &SimulationEvaluationEngineState::k_engine_ops};
            }

            case EvaluationMode::REAL_TIME: {
                // Hold the state under temporary ownership until the engine is attached and observer registration completes.
                auto state = std::make_unique<RealTimeEvaluationEngineState>(m_graph_builder->make_graph(), m_start_time, m_end_time);
                state->graph.set_evaluation_engine(state.get(), &RealTimeEvaluationEngineState::k_engine_ops);
                for (auto *observer : m_life_cycle_observers) {
                    RealTimeEvaluationEngineState::add_life_cycle_observer_impl(state.get(), observer);
                }
                return {state.release(), &RealTimeEvaluationEngineState::k_engine_ops};
            }
        }

        throw std::runtime_error("Unknown v2 evaluation mode");
    }
}  // namespace hgraph::v2
