#include <hgraph/types/v2/evaluation_engine.h>

#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/node.h>

#include <algorithm>
#include <chrono>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        struct SimulationEvaluationEngineState
        {
            struct ClockState
            {
                engine_time_t evaluation_time{MIN_DT};
                engine_time_t next_scheduled_evaluation_time{MAX_DT};
                engine_time_t system_clock_at_start_of_evaluation{MIN_DT};
            };

            ClockState clock;
            EvaluationMode evaluation_mode{EvaluationMode::SIMULATION};
            engine_time_t start_time{MIN_DT};
            engine_time_t end_time{MAX_DT};
            bool stop_requested{false};
            std::vector<EvaluationLifeCycleObserver *> life_cycle_observers;
            std::vector<std::function<void()>> before_evaluation_notifications;
            std::vector<std::function<void()>> after_evaluation_notifications;
            Graph graph;

            SimulationEvaluationEngineState(Graph graph, EvaluationMode evaluation_mode, engine_time_t start_time, engine_time_t end_time)
                : graph(std::move(graph)), evaluation_mode(evaluation_mode), start_time(start_time), end_time(end_time)
            {
                clock_set_evaluation_time_impl(&clock, start_time);
            }

            [[nodiscard]] static SimulationEvaluationEngineState &state_from(void *impl)
            {
                return *static_cast<SimulationEvaluationEngineState *>(impl);
            }

            [[nodiscard]] static const SimulationEvaluationEngineState &state_from(const void *impl)
            {
                return *static_cast<const SimulationEvaluationEngineState *>(impl);
            }

            [[nodiscard]] static EvaluationMode evaluation_mode_impl(const void *impl) noexcept
            {
                return state_from(impl).evaluation_mode;
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
                return {&state_from(impl).clock, &k_clock_ops};
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

                auto &observers = state_from(impl).life_cycle_observers;
                if (std::find(observers.begin(), observers.end(), observer) == observers.end()) {
                    observers.push_back(observer);
                }
            }

            static void remove_life_cycle_observer_impl(void *impl, EvaluationLifeCycleObserver *observer)
            {
                auto &observers = state_from(impl).life_cycle_observers;
                observers.erase(std::remove(observers.begin(), observers.end(), observer), observers.end());
            }

            [[nodiscard]] static EvaluationEngineApi evaluation_engine_api_impl(void *impl) noexcept
            {
                return {impl, &k_api_ops};
            }

            [[nodiscard]] static EngineEvaluationClock engine_evaluation_clock_impl(void *impl)
            {
                return {&state_from(impl).clock, &k_clock_ops};
            }

            static void notify_before_evaluation_impl(void *impl)
            {
                auto &notifications = state_from(impl).before_evaluation_notifications;
                for (auto &notification : notifications) { notification(); }
                notifications.clear();
            }

            static void notify_after_evaluation_impl(void *impl)
            {
                auto &state = state_from(impl);
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
                    clock_set_evaluation_time_impl(&state.clock, state.end_time + MIN_TD);
                    return;
                }

                clock_update_next_scheduled_evaluation_time_impl(&state.clock, state.end_time + MIN_TD);
                clock_advance_to_next_scheduled_time_impl(&state.clock);
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

                if (state.evaluation_mode != EvaluationMode::SIMULATION) {
                    throw std::runtime_error("v2 evaluation engine currently supports only simulation mode");
                }

                state.graph.start();
                try {
                    while (state.clock.evaluation_time < state.end_time) {
                        notify_before_evaluation_impl(impl);
                        state.graph.evaluate(state.clock.evaluation_time);
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
                delete static_cast<SimulationEvaluationEngineState *>(impl);
            }

            [[nodiscard]] static engine_time_t clock_evaluation_time_impl(const void *impl) noexcept
            {
                return static_cast<const ClockState *>(impl)->evaluation_time;
            }

            [[nodiscard]] static engine_time_t clock_now_impl(const void *impl)
            {
                return clock_evaluation_time_impl(impl) + clock_cycle_time_impl(impl);
            }

            [[nodiscard]] static engine_time_delta_t clock_cycle_time_impl(const void *impl)
            {
                const auto &clock = *static_cast<const ClockState *>(impl);
                if (clock.system_clock_at_start_of_evaluation == MIN_DT) { return engine_time_delta_t::zero(); }

                return std::chrono::duration_cast<engine_time_delta_t>(
                    std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now()) - clock.system_clock_at_start_of_evaluation);
            }

            [[nodiscard]] static engine_time_t clock_next_cycle_evaluation_time_impl(const void *impl) noexcept
            {
                return clock_evaluation_time_impl(impl) + MIN_TD;
            }

            static void clock_set_evaluation_time_impl(void *impl, engine_time_t evaluation_time)
            {
                auto &clock = *static_cast<ClockState *>(impl);
                clock.evaluation_time = evaluation_time;
                clock.next_scheduled_evaluation_time = MAX_DT;
                clock.system_clock_at_start_of_evaluation =
                    std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
            }

            [[nodiscard]] static engine_time_t clock_next_scheduled_evaluation_time_impl(const void *impl) noexcept
            {
                return static_cast<const ClockState *>(impl)->next_scheduled_evaluation_time;
            }

            static void clock_update_next_scheduled_evaluation_time_impl(void *impl, engine_time_t evaluation_time)
            {
                auto &clock = *static_cast<ClockState *>(impl);
                if (evaluation_time == clock.evaluation_time) { return; }

                clock.next_scheduled_evaluation_time =
                    std::max(clock.evaluation_time + MIN_TD, std::min(clock.next_scheduled_evaluation_time, evaluation_time));
            }

            static void clock_advance_to_next_scheduled_time_impl(void *impl)
            {
                auto &clock = *static_cast<ClockState *>(impl);
                clock_set_evaluation_time_impl(&clock, clock.next_scheduled_evaluation_time);
            }

            static void clock_mark_push_node_requires_scheduling_impl(void *impl)
            {
                static_cast<void>(impl);
                throw std::runtime_error("Simulation mode does not support push nodes.");
            }

            [[nodiscard]] static bool clock_push_node_requires_scheduling_impl(const void *impl) noexcept
            {
                static_cast<void>(impl);
                return false;
            }

            static void clock_reset_push_node_requires_scheduling_impl(void *impl)
            {
                static_cast<void>(impl);
            }

            [[nodiscard]] static const engine_time_t *clock_evaluation_time_ptr_impl(const void *impl) noexcept
            {
                return &static_cast<const ClockState *>(impl)->evaluation_time;
            }

            template <typename TNotify>
            static void notify_observers(const SimulationEvaluationEngineState &state, TNotify &&notify)
            {
                for (auto *observer : state.life_cycle_observers) {
                    if (observer != nullptr) { notify(*observer); }
                }
            }

            static const EngineEvaluationClockOps k_clock_ops;
            static const EvaluationEngineApiOps k_api_ops;
            static const EvaluationEngineOps k_engine_ops;
        };

        const EngineEvaluationClockOps SimulationEvaluationEngineState::k_clock_ops{
            &SimulationEvaluationEngineState::clock_evaluation_time_impl,
            &SimulationEvaluationEngineState::clock_now_impl,
            &SimulationEvaluationEngineState::clock_cycle_time_impl,
            &SimulationEvaluationEngineState::clock_next_cycle_evaluation_time_impl,
            &SimulationEvaluationEngineState::clock_set_evaluation_time_impl,
            &SimulationEvaluationEngineState::clock_next_scheduled_evaluation_time_impl,
            &SimulationEvaluationEngineState::clock_update_next_scheduled_evaluation_time_impl,
            &SimulationEvaluationEngineState::clock_advance_to_next_scheduled_time_impl,
            &SimulationEvaluationEngineState::clock_mark_push_node_requires_scheduling_impl,
            &SimulationEvaluationEngineState::clock_push_node_requires_scheduling_impl,
            &SimulationEvaluationEngineState::clock_reset_push_node_requires_scheduling_impl,
            &SimulationEvaluationEngineState::clock_evaluation_time_ptr_impl,
        };

        const EvaluationEngineApiOps SimulationEvaluationEngineState::k_api_ops{
            &SimulationEvaluationEngineState::evaluation_mode_impl,
            &SimulationEvaluationEngineState::start_time_impl,
            &SimulationEvaluationEngineState::end_time_impl,
            &SimulationEvaluationEngineState::evaluation_clock_impl,
            &SimulationEvaluationEngineState::request_engine_stop_impl,
            &SimulationEvaluationEngineState::is_stop_requested_impl,
            &SimulationEvaluationEngineState::add_before_evaluation_notification_impl,
            &SimulationEvaluationEngineState::add_after_evaluation_notification_impl,
            &SimulationEvaluationEngineState::add_life_cycle_observer_impl,
            &SimulationEvaluationEngineState::remove_life_cycle_observer_impl,
        };

        const EvaluationEngineOps SimulationEvaluationEngineState::k_engine_ops{
            &SimulationEvaluationEngineState::evaluation_mode_impl,
            &SimulationEvaluationEngineState::start_time_impl,
            &SimulationEvaluationEngineState::end_time_impl,
            &SimulationEvaluationEngineState::graph_impl,
            &SimulationEvaluationEngineState::const_graph_impl,
            &SimulationEvaluationEngineState::evaluation_engine_api_impl,
            &SimulationEvaluationEngineState::engine_evaluation_clock_impl,
            &SimulationEvaluationEngineState::notify_before_evaluation_impl,
            &SimulationEvaluationEngineState::notify_after_evaluation_impl,
            &SimulationEvaluationEngineState::advance_engine_time_impl,
            &SimulationEvaluationEngineState::notify_before_start_graph_impl,
            &SimulationEvaluationEngineState::notify_after_start_graph_impl,
            &SimulationEvaluationEngineState::notify_before_start_node_impl,
            &SimulationEvaluationEngineState::notify_after_start_node_impl,
            &SimulationEvaluationEngineState::notify_before_graph_evaluation_impl,
            &SimulationEvaluationEngineState::notify_after_graph_evaluation_impl,
            &SimulationEvaluationEngineState::notify_after_push_nodes_evaluation_impl,
            &SimulationEvaluationEngineState::notify_before_node_evaluation_impl,
            &SimulationEvaluationEngineState::notify_after_node_evaluation_impl,
            &SimulationEvaluationEngineState::notify_before_stop_node_impl,
            &SimulationEvaluationEngineState::notify_after_stop_node_impl,
            &SimulationEvaluationEngineState::notify_before_stop_graph_impl,
            &SimulationEvaluationEngineState::notify_after_stop_graph_impl,
            &SimulationEvaluationEngineState::run_impl,
            &SimulationEvaluationEngineState::destruct_impl,
        };
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

        // Hold the state under temporary ownership until the runtime is attached and observer registration completes.
        auto state =
            std::make_unique<SimulationEvaluationEngineState>(m_graph_builder->make_graph(), m_evaluation_mode, m_start_time, m_end_time);
        state->graph.set_evaluation_engine(state.get(), &SimulationEvaluationEngineState::k_engine_ops);
        for (auto *observer : m_life_cycle_observers) {
            SimulationEvaluationEngineState::add_life_cycle_observer_impl(state.get(), observer);
        }

        return {state.release(), &SimulationEvaluationEngineState::k_engine_ops};
    }
}  // namespace hgraph::v2
