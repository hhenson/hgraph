#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/executor.h>

#include "registry_snapshot_detail.h"

#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/util/scope.h>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        struct GraphExecutorPhaseActionAccess
        {
            static GraphExecutorPhaseAction make(void *context,
                                                 GraphExecutorPhaseAction::Invoke invoke) noexcept
            {
                return GraphExecutorPhaseAction{context, invoke};
            }
        };
    }

    namespace
    {
        struct SimulationExecutorStorage;
        struct RealTimeExecutorStorage;

        template <typename Storage>
        void stop_storage(Storage &state);

        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        struct SimulationExecutorStorage
        {
            SimulationExecutorStorage(const GraphExecutorBuilder &builder,
                                      ExecutorTypeRef type,
                                      void *executor_memory)
                : logger(builder.logger() != nullptr ? builder.logger()
                                                     : log::shared_logger()),
                  logger_ops(&builder.logger_ops()),
                  graph(builder.graph_builder().make_root_graph(type.writable(executor_memory))),
                  start_time(builder.start_time()),
                  end_time(builder.end_time()),
                  evaluation_time(start_time),
                  cycle_wall_start(current_wall_time()),
                  error_capture_options(builder.error_capture_options()),
                  cleanup_on_error(builder.cleanup_on_error()),
                  phase_runner(builder.phase_runner()),
                  run_logging_enabled(builder.logger() != nullptr)
            {
                immediate_cycle_limit = builder.max_consecutive_immediate_cycles();
                for (LifecycleObserver *observer : builder.lifecycle_observers()) { lifecycle_observers.add(observer); }
            }

            ~SimulationExecutorStorage();

            void set_evaluation_time(DateTime value) noexcept
            {
                evaluation_time = value;
                cycle_wall_start = current_wall_time();
            }

            LifecycleObserverList lifecycle_observers{}; // declared first so it is constructed before graph
            std::shared_ptr<spdlog::logger> logger{};
            const LoggerOps *logger_ops{&plain_logger_ops()};
            GraphValue       graph{};
            DateTime         start_time{MIN_ST};
            DateTime         end_time{MAX_ET};
            DateTime         evaluation_time{MIN_ST};
            DateTime         cycle_wall_start{current_wall_time()};
            std::uint32_t    immediate_cycle_limit{0};
            std::uint32_t    consecutive_immediate_cycles{0};
            ErrorCaptureOptions error_capture_options{};
            // One-shot cycle-boundary notifications (2026-08-01): drained by
            // the run loop at the root cycle boundaries; eval-thread only.
            std::vector<std::function<void()>> before_evaluation_notifications{};
            std::vector<std::function<void()>> after_evaluation_notifications{};
            std::atomic_bool stop_requested{false};
            std::atomic_bool push_update_pending{false};
            bool                     cleanup_on_error{true};
            GraphExecutorPhaseRunner phase_runner{};
            bool                     run_logging_enabled{false};
        };

        struct RealTimeExecutorStorage
        {
            RealTimeExecutorStorage(const GraphExecutorBuilder &builder,
                                    ExecutorTypeRef type,
                                    void *executor_memory)
                : logger(builder.logger() != nullptr ? builder.logger()
                                                     : log::shared_logger()),
                  logger_ops(&builder.logger_ops()),
                  graph(builder.graph_builder().make_root_graph(type.writable(executor_memory))),
                  start_time(builder.start_time()),
                  end_time(builder.end_time()),
                  evaluation_time(start_time),
                  error_capture_options(builder.error_capture_options()),
                  cleanup_on_error(builder.cleanup_on_error()),
                  phase_runner(builder.phase_runner()),
                  run_logging_enabled(builder.logger() != nullptr)
            {
                immediate_cycle_limit = builder.max_consecutive_immediate_cycles();
                for (LifecycleObserver *observer : builder.lifecycle_observers()) { lifecycle_observers.add(observer); }
            }

            ~RealTimeExecutorStorage();

            void set_evaluation_time(DateTime value) noexcept
            {
                evaluation_time = value;
            }

            LifecycleObserverList lifecycle_observers{}; // declared first so it is constructed before graph
            std::shared_ptr<spdlog::logger> logger{};
            const LoggerOps               *logger_ops{&plain_logger_ops()};
            GraphValue                   graph{};
            DateTime                     start_time{MIN_ST};
            DateTime                     end_time{MAX_ET};
            DateTime                     evaluation_time{MIN_ST};
            DateTime                     last_time_allowed_push{MIN_DT};
            std::uint32_t                immediate_cycle_limit{0};
            std::uint32_t                consecutive_immediate_cycles{0};
            ErrorCaptureOptions          error_capture_options{};
            bool                         cleanup_on_error{true};
            // One-shot cycle-boundary notifications (2026-08-01): drained by
            // the run loop at the root cycle boundaries; eval-thread only.
            std::vector<std::function<void()>> before_evaluation_notifications{};
            std::vector<std::function<void()>> after_evaluation_notifications{};

            mutable std::mutex           mutex{};
            std::condition_variable      condition{};
            std::atomic_bool             stop_requested{false};
            bool                         push_update_pending{false};
            bool                         ready_to_push{false};
            GraphExecutorPhaseRunner     phase_runner{};
            bool                         run_logging_enabled{false};
        };

        template <typename Storage, typename Fn>
        void run_executor_phase(Storage &state, GraphExecutorPhase phase, Fn &&fn)
        {
            if (!state.phase_runner)
            {
                std::forward<Fn>(fn)();
                return;
            }

            using Callable = std::remove_reference_t<Fn>;
            auto invoke = [](void *context) {
                (*static_cast<Callable *>(context))();
            };
            state.phase_runner(
                phase,
                detail::GraphExecutorPhaseActionAccess::make(std::addressof(fn), invoke));
        }

        [[nodiscard]] SimulationExecutorStorage &simulation_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Simulation executor storage is null"); }
            return *MemoryUtils::cast<SimulationExecutorStorage>(memory);
        }

        [[nodiscard]] const SimulationExecutorStorage &simulation_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Simulation executor storage is null"); }
            return *MemoryUtils::cast<SimulationExecutorStorage>(memory);
        }

        [[nodiscard]] RealTimeExecutorStorage &realtime_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Real-time executor storage is null"); }
            return *MemoryUtils::cast<RealTimeExecutorStorage>(memory);
        }

        [[nodiscard]] const RealTimeExecutorStorage &realtime_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Real-time executor storage is null"); }
            return *MemoryUtils::cast<RealTimeExecutorStorage>(memory);
        }

        [[nodiscard]] bool graph_has_push_sources(
            const GraphBuilder &builder) noexcept
        {
            for (const NodeBuilder &node : builder.nodes())
            {
                const auto *schema = node.type().schema();
                if (schema->node_kind == NodeKind::PushSource)
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] TimeDelta elapsed_since(DateTime start) noexcept
        {
            const TimeDelta elapsed = current_wall_time() - start;
            return elapsed >= TimeDelta{0} ? elapsed : TimeDelta{0};
        }

        [[nodiscard]] DateTime simulation_clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).evaluation_time;
        }

        [[nodiscard]] DateTime simulation_clock_now_impl(const void *, const void *memory) noexcept
        {
            const auto &state = simulation_storage(memory);
            return state.evaluation_time + elapsed_since(state.cycle_wall_start);
        }

        [[nodiscard]] TimeDelta simulation_clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            return elapsed_since(simulation_storage(memory).cycle_wall_start);
        }

        [[nodiscard]] DateTime simulation_clock_next_cycle_evaluation_time_impl(const void *,
                                                                                const void *memory) noexcept
        {
            return simulation_storage(memory).evaluation_time + MIN_TD;
        }

        [[nodiscard]] DateTime realtime_clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).evaluation_time;
        }

        [[nodiscard]] DateTime realtime_clock_now_impl(const void *, const void *) noexcept
        {
            return current_wall_time();
        }

        [[nodiscard]] TimeDelta realtime_clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            return elapsed_since(realtime_storage(memory).evaluation_time);
        }

        [[nodiscard]] DateTime realtime_clock_next_cycle_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).evaluation_time + MIN_TD;
        }

        [[nodiscard]] const EvaluationClockOps &simulation_clock_ops() noexcept
        {
            static const EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &simulation_clock_evaluation_time_impl,
                .now_impl = &simulation_clock_now_impl,
                .cycle_time_impl = &simulation_clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &simulation_clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        [[nodiscard]] const EvaluationClockOps &realtime_clock_ops() noexcept
        {
            static const EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &realtime_clock_evaluation_time_impl,
                .now_impl = &realtime_clock_now_impl,
                .cycle_time_impl = &realtime_clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &realtime_clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        struct ExecutorRuntimeContext
        {
            ClockTypeRef clock_type{};
        };

        [[nodiscard]] const ExecutorRuntimeContext &executor_context(const void *context) noexcept
        {
            return *static_cast<const ExecutorRuntimeContext *>(context);
        }

        [[nodiscard]] ClockPtr simulation_clock_ptr_impl(const void *context, void *memory) noexcept
        {
            return executor_context(context).clock_type.read_only(memory);
        }

        [[nodiscard]] ClockPtr realtime_clock_ptr_impl(const void *context, void *memory) noexcept
        {
            return executor_context(context).clock_type.read_only(memory);
        }

        void simulation_mark_push_update_pending_impl(const void *, void *memory)
        {
            auto &state = simulation_storage(memory);
            if (!state.stop_requested.load(std::memory_order_acquire))
            {
                state.push_update_pending.store(true, std::memory_order_release);
            }
        }

        bool simulation_is_push_update_pending_impl(const void *, void *memory) noexcept
        {
            return simulation_storage(memory).push_update_pending.load(
                std::memory_order_acquire);
        }

        bool simulation_reset_push_update_pending_impl(const void *, void *memory) noexcept
        {
            return simulation_storage(memory).push_update_pending.exchange(
                false, std::memory_order_acq_rel);
        }

        void realtime_mark_push_update_pending_impl(const void *, void *memory)
        {
            auto &state = realtime_storage(memory);
            {
                std::lock_guard lock{state.mutex};
                if (state.stop_requested.load(std::memory_order_acquire)) { return; }
                state.push_update_pending = true;
            }
            state.condition.notify_all();
        }

        bool realtime_is_push_update_pending_impl(const void *, void *memory) noexcept
        {
            auto &state = realtime_storage(memory);
            std::lock_guard lock{state.mutex};
            return state.push_update_pending;
        }

        bool realtime_reset_push_update_pending_impl(const void *, void *memory) noexcept
        {
            auto &state = realtime_storage(memory);
            std::lock_guard lock{state.mutex};
            const bool pending = state.push_update_pending;
            state.push_update_pending = false;
            return pending;
        }

        [[nodiscard]] DateTime advance_simulation(SimulationExecutorStorage &state, DateTime next_scheduled_time)
        {
            const DateTime pending_time =
                state.push_update_pending.load(std::memory_order_acquire)
                    ? state.evaluation_time + MIN_TD
                    : next_scheduled_time;
            const DateTime next = std::min(pending_time, state.end_time);
            state.set_evaluation_time(next);
            return next;
        }

        // Consecutive MIN_TD-only cycles tolerated once the wall clock has
        // passed end_time before the run is cut off. Deep enough for any sane
        // immediate cascade (feedback chains) inside a drain; a busy retry
        // loop exceeds it almost immediately.
        constexpr std::uint32_t max_immediate_drain_cycles = 1024;

        [[nodiscard]] DateTime advance_realtime(RealTimeExecutorStorage &state, DateTime next_scheduled_time)
        {
            const DateTime target = std::min(next_scheduled_time, state.end_time);
            DateTime       wall_now = current_wall_time();

            {
                std::lock_guard lock{state.mutex};
                state.ready_to_push = false;
            }

            const DateTime next_cycle = state.evaluation_time + MIN_TD;
            if (target > next_cycle || wall_now > state.last_time_allowed_push + TimeDelta{15'000'000})
            {
                std::unique_lock lock{state.mutex};
                state.ready_to_push = true;
                state.last_time_allowed_push = wall_now;

                while (!state.stop_requested.load(std::memory_order_acquire) &&
                       wall_now < target &&
                       !state.push_update_pending)
                {
                    const TimeDelta sleep_time = std::min(target - wall_now, TimeDelta{10'000'000});
                    state.condition.wait_for(lock, sleep_time);
                    wall_now = current_wall_time();
                }
            }

            wall_now = current_wall_time();
            const DateTime next = std::min(target, std::max(next_cycle, wall_now));
            if (wall_now >= state.end_time && next <= next_cycle &&
                state.consecutive_immediate_cycles >= max_immediate_drain_cycles)
            {
                // Past wall-clock end_time the executor only drains: a lagging
                // graph still evaluates its scheduled work at the scheduled
                // times, but a graph advancing exactly MIN_TD per cycle is
                // making no material logical progress (the shape of a failing
                // retry loop) and would starve the end_time bound indefinitely
                // (see execution_layer.rst, end-of-run enforcement). The
                // counter is maintained by the run loop.
                state.set_evaluation_time(state.end_time);
                return state.end_time;
            }
            state.set_evaluation_time(next);
            return next;
        }

        void validate_times(DateTime start_time, DateTime end_time)
        {
            if (end_time <= start_time)
            {
                throw std::invalid_argument("GraphExecutor end_time must be after start_time");
            }
        }

        [[nodiscard]] bool waits_for_push_sources(
            const SimulationExecutorStorage &state,
            const GraphView &) noexcept
        {
            return state.push_update_pending.load(std::memory_order_acquire);
        }

        [[nodiscard]] bool waits_for_push_sources(const RealTimeExecutorStorage &, const GraphView &graph) noexcept
        {
            return graph.schema()->push_source_nodes_end > 0;
        }

        /**
         * Records the diagnostic path of every node evaluated during one
         * cycle for the opt-in recursion guard (execution_layer.rst).
         */
        struct ImmediateCycleRecorder final : LifecycleObserver
        {
            static constexpr std::size_t max_recorded_nodes = 256;

            std::vector<std::string> paths{};
            std::size_t              dropped{0};

            void on_before_node_evaluation(const NodeView &node) override
            {
                if (paths.size() < max_recorded_nodes) { paths.push_back(diagnostic::node_path(node)); }
                else { ++dropped; }
            }

            void reset()
            {
                paths.clear();
                dropped = 0;
            }
        };

        [[noreturn]] void throw_recursive_evaluation_error(spdlog::logger &logger,
                                                           std::uint32_t consecutive_cycles,
                                                           DateTime evaluation_time,
                                                           const ImmediateCycleRecorder &recorder)
        {
            std::string message = fmt::format(
                "potential recursive evaluation loop: {} consecutive MIN_TD evaluation cycles at {:%Y-%m-%d %H:%M:%S}; "
                "nodes evaluated in the last cycle:",
                consecutive_cycles, evaluation_time);
            for (const std::string &path : recorder.paths)
            {
                message += "\n  ";
                message += path;
            }
            if (recorder.dropped > 0) { message += fmt::format("\n  ... and {} further node evaluations", recorder.dropped); }
            logger.error(message);
            throw RecursiveEvaluationError(message);
        }

        /** Drain one root-cycle boundary to completion. Before callbacks are
            FIFO; after callbacks are LIFO so cleanup unwinds in reverse
            registration order. Swapping each batch makes re-entrant
            registration safe while the outer loop keeps it on THIS boundary. */
        inline void drain_evaluation_notifications(std::vector<std::function<void()>> &queue,
                                                    bool before)
        {
            while (!queue.empty())
            {
                std::vector<std::function<void()>> pending;
                pending.swap(queue);
                if (before)
                {
                    for (auto &fn : pending) { fn(); }
                }
                else
                {
                    for (auto it = pending.rbegin(); it != pending.rend(); ++it) { (*it)(); }
                }
            }
        }

        template <typename Storage>
        void stop_storage(Storage &state)
        {
            auto graph = state.graph.view();
            run_executor_phase(state, GraphExecutorPhase::Stop, [&] {
                // Stopping nodes can enqueue more cleanup. Always attempt both
                // queues even when stop or an earlier queue reports a failure.
                FirstExceptionRecorder cleanup_errors;
                cleanup_errors.capture([&] { graph.stop(); });
                cleanup_errors.capture([&] {
                    drain_evaluation_notifications(state.after_evaluation_notifications, false);
                });
                cleanup_errors.capture([&] {
                    drain_evaluation_notifications(state.before_evaluation_notifications, true);
                });
                cleanup_errors.rethrow_if_any();
            });
        }

        SimulationExecutorStorage::~SimulationExecutorStorage()
        {
            if (graph.has_value() && graph.view().started())
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    stop_storage(*this);
                    return true;
                }));
            }
        }

        RealTimeExecutorStorage::~RealTimeExecutorStorage()
        {
            if (graph.has_value() && graph.view().started())
            {
                static_cast<void>(fallback_on_exception(false, [&] {
                    stop_storage(*this);
                    return true;
                }));
            }
        }

        template <typename Storage, typename Advance>
        void run_storage(Storage &state, Advance advance)
        {
            validate_times(state.start_time, state.end_time);
            state.stop_requested.store(false, std::memory_order_release);
            state.set_evaluation_time(state.start_time);

            auto graph = state.graph.view();
            run_executor_phase(state, GraphExecutorPhase::Start, [&] {
                graph.start(state.start_time);
            });
            auto stop_graph = UnwindCleanupGuard([&] {
                if (state.cleanup_on_error || std::uncaught_exceptions() == 0)
                {
                    stop_storage(state);
                }
            });

            ImmediateCycleRecorder recorder;
            bool                   recorded_cycle = false;

            while (!state.stop_requested.load(std::memory_order_acquire))
            {
                DateTime next = graph.next_scheduled_time();
                if (next == MAX_DT || next >= state.end_time)
                {
                    if (!waits_for_push_sources(state, graph)) { break; }
                    next = state.end_time;
                }

                const DateTime previous_evaluation_time = state.evaluation_time;
                const DateTime evaluation_time = advance(state, next);
                if (state.stop_requested.load(std::memory_order_acquire) ||
                    evaluation_time == MAX_DT ||
                    evaluation_time >= state.end_time)
                {
                    break;
                }

                if (evaluation_time == previous_evaluation_time + MIN_TD) { ++state.consecutive_immediate_cycles; }
                else { state.consecutive_immediate_cycles = 0; }

                const bool guard_tripped = state.immediate_cycle_limit > 0 &&
                    state.consecutive_immediate_cycles >= state.immediate_cycle_limit;
                if (guard_tripped && recorded_cycle)
                {
                    throw_recursive_evaluation_error(*state.logger,
                                                     state.consecutive_immediate_cycles,
                                                     evaluation_time,
                                                     recorder);
                }
                if (!guard_tripped && recorded_cycle)
                {
                    // The loop settled during the recording cycle; disarm.
                    recorder.reset();
                    recorded_cycle = false;
                }
                const bool recording_this_cycle = guard_tripped && !recorded_cycle;
                bool completed = false;
                run_executor_phase(state, GraphExecutorPhase::Evaluation, [&] {
                    if (recording_this_cycle) { state.lifecycle_observers.add(&recorder); }
                    auto remove_recorder = UnwindCleanupGuard([&] {
                        if (recording_this_cycle) { state.lifecycle_observers.remove(&recorder); }
                    });
                    drain_evaluation_notifications(state.before_evaluation_notifications, true);
                    // Match Python's try/finally cycle contract: even a partial
                    // graph evaluation must run the after queue before teardown.
                    auto drain_after = UnwindCleanupGuard([&] {
                        drain_evaluation_notifications(state.after_evaluation_notifications, false);
                    });
                    completed = graph.evaluate(evaluation_time);
                    drain_after.complete();
                    remove_recorder.complete();
                    if (recording_this_cycle) { recorded_cycle = true; }
                });
                if (!completed)
                {
                    // The root graph has no enclosing mesh to resolve a pause; a false here
                    // means a pausing node (e.g. a mesh_subscribe) escaped its mesh scope.
                    throw std::logic_error("root graph evaluation paused with no resolver");
                }
            }

            stop_graph.complete();
        }

        void simulation_run_impl(const void *, const GraphExecutorView &executor)
        {
            auto &state = simulation_storage(executor.data());
            run_storage(state,
                        [](SimulationExecutorStorage &storage, DateTime next) {
                            return advance_simulation(storage, next);
                        });
        }

        void realtime_run_impl(const void *, const GraphExecutorView &executor)
        {
            auto &state = realtime_storage(executor.data());
            run_storage(state,
                        [](RealTimeExecutorStorage &storage, DateTime next) {
                            return advance_realtime(storage, next);
                        });
        }

        void simulation_request_stop_impl(const void *, void *memory) noexcept
        {
            simulation_storage(memory).stop_requested.store(true, std::memory_order_release);
        }

        void realtime_request_stop_impl(const void *, void *memory) noexcept
        {
            auto &state = realtime_storage(memory);
            state.stop_requested.store(true, std::memory_order_release);
            std::lock_guard lock{state.mutex};
            state.condition.notify_all();
        }

        void simulation_add_evaluation_notification_impl(const void *, void *memory,
                                                          std::function<void()> fn, bool before)
        {
            auto &state = simulation_storage(memory);
            (before ? state.before_evaluation_notifications
                    : state.after_evaluation_notifications).push_back(std::move(fn));
        }

        void realtime_add_evaluation_notification_impl(const void *, void *memory,
                                                       std::function<void()> fn, bool before)
        {
            auto &state = realtime_storage(memory);
            (before ? state.before_evaluation_notifications
                    : state.after_evaluation_notifications).push_back(std::move(fn));
        }

        bool simulation_stop_requested_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).stop_requested.load(std::memory_order_acquire);
        }

        bool realtime_stop_requested_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).stop_requested.load(std::memory_order_acquire);
        }

        DateTime simulation_start_time_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).start_time;
        }

        DateTime realtime_start_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).start_time;
        }

        DateTime simulation_end_time_impl(const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).end_time;
        }

        DateTime realtime_end_time_impl(const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).end_time;
        }

        GraphView simulation_graph_impl(const void *, void *memory)
        {
            return simulation_storage(memory).graph.view();
        }

        GraphView realtime_graph_impl(const void *, void *memory)
        {
            return realtime_storage(memory).graph.view();
        }

        LifecycleObserverList *simulation_lifecycle_observers_impl(const void *, void *memory) noexcept
        {
            return &simulation_storage(memory).lifecycle_observers;
        }

        LifecycleObserverList *realtime_lifecycle_observers_impl(const void *, void *memory) noexcept
        {
            return &realtime_storage(memory).lifecycle_observers;
        }

        spdlog::logger *simulation_logger_impl(const void *, void *memory) noexcept
        {
            return simulation_storage(memory).logger.get();
        }

        spdlog::logger *realtime_logger_impl(const void *, void *memory) noexcept
        {
            return realtime_storage(memory).logger.get();
        }

        const LoggerOps *simulation_logger_ops_impl(const void *,
                                                    const void *memory) noexcept
        {
            return simulation_storage(memory).logger_ops;
        }

        const LoggerOps *realtime_logger_ops_impl(const void *,
                                                  const void *memory) noexcept
        {
            return realtime_storage(memory).logger_ops;
        }

        bool simulation_run_logging_enabled_impl(const void *,
                                                 const void *memory) noexcept
        {
            return simulation_storage(memory).run_logging_enabled;
        }

        bool realtime_run_logging_enabled_impl(const void *,
                                               const void *memory) noexcept
        {
            return realtime_storage(memory).run_logging_enabled;
        }

        ErrorCaptureOptions simulation_error_capture_options_impl(
            const void *, const void *memory) noexcept
        {
            return simulation_storage(memory).error_capture_options;
        }

        ErrorCaptureOptions realtime_error_capture_options_impl(
            const void *, const void *memory) noexcept
        {
            return realtime_storage(memory).error_capture_options;
        }

        bool simulation_cleanup_on_error_impl(const void *,
                                              const void *memory) noexcept
        {
            return simulation_storage(memory).cleanup_on_error;
        }

        bool realtime_cleanup_on_error_impl(const void *,
                                            const void *memory) noexcept
        {
            return realtime_storage(memory).cleanup_on_error;
        }

        [[nodiscard]] GraphExecutorOps simulation_executor_ops(const ExecutorRuntimeContext *context)
        {
            return GraphExecutorOps{
                .context = context,
                .run_impl = &simulation_run_impl,
                .request_stop_impl = &simulation_request_stop_impl,
                .add_evaluation_notification_impl = &simulation_add_evaluation_notification_impl,
                .stop_requested_impl = &simulation_stop_requested_impl,
                .start_time_impl = &simulation_start_time_impl,
                .end_time_impl = &simulation_end_time_impl,
                .graph_impl = &simulation_graph_impl,
                .evaluation_clock_ptr_impl = &simulation_clock_ptr_impl,
                .mark_push_update_pending_impl = &simulation_mark_push_update_pending_impl,
                .is_push_update_pending_impl = &simulation_is_push_update_pending_impl,
                .reset_push_update_pending_impl = &simulation_reset_push_update_pending_impl,
                .lifecycle_observers_impl = &simulation_lifecycle_observers_impl,
                .logger_impl = &simulation_logger_impl,
                .logger_ops_impl = &simulation_logger_ops_impl,
                .run_logging_enabled_impl = &simulation_run_logging_enabled_impl,
                .error_capture_options_impl = &simulation_error_capture_options_impl,
                .cleanup_on_error_impl = &simulation_cleanup_on_error_impl,
            };
        }

        [[nodiscard]] GraphExecutorOps realtime_executor_ops(const ExecutorRuntimeContext *context)
        {
            return GraphExecutorOps{
                .context = context,
                .run_impl = &realtime_run_impl,
                .request_stop_impl = &realtime_request_stop_impl,
                .add_evaluation_notification_impl = &realtime_add_evaluation_notification_impl,
                .stop_requested_impl = &realtime_stop_requested_impl,
                .start_time_impl = &realtime_start_time_impl,
                .end_time_impl = &realtime_end_time_impl,
                .graph_impl = &realtime_graph_impl,
                .evaluation_clock_ptr_impl = &realtime_clock_ptr_impl,
                .mark_push_update_pending_impl = &realtime_mark_push_update_pending_impl,
                .is_push_update_pending_impl = &realtime_is_push_update_pending_impl,
                .reset_push_update_pending_impl = &realtime_reset_push_update_pending_impl,
                .lifecycle_observers_impl = &realtime_lifecycle_observers_impl,
                .logger_impl = &realtime_logger_impl,
                .logger_ops_impl = &realtime_logger_ops_impl,
                .run_logging_enabled_impl = &realtime_run_logging_enabled_impl,
                .error_capture_options_impl = &realtime_error_capture_options_impl,
                .cleanup_on_error_impl = &realtime_cleanup_on_error_impl,
            };
        }

        struct ExecutorRuntimeRegistry
        {
          struct Key {
            GraphExecutorMode mode{GraphExecutorMode::Simulation};
            std::string label{};

            [[nodiscard]] bool operator==(const Key &) const noexcept = default;
          };

          struct KeyHash {
            [[nodiscard]] std::size_t
            operator()(const Key &key) const noexcept {
              std::size_t result = std::hash<std::string>{}(key.label);
              return result ^
                     (static_cast<std::size_t>(key.mode) +
                      0x9e3779b97f4a7c15ULL + (result << 6U) + (result >> 2U));
            }
          };

            struct Entry
            {
                GraphExecutorTypeMetaData schema{};
                ExecutorRuntimeContext    context{};
                GraphExecutorOps          ops{};
                ExecutorTypeRef           type{};
            };

            ExecutorTypeRef make_type(const GraphExecutorBuilder &builder)
            {
              Key key{builder.mode(), std::string{builder.label()}};
              if (const auto found = canonical_types.find(key);
                  found != canonical_types.end()) {
                return found->second;
              }
                names.push_back(std::make_unique<std::string>(std::string{builder.label()}));
                entries.push_back({});
                auto &entry = entries.back();
                if (!names.back()->empty()) { entry.schema.display_name = names.back()->c_str(); }
                entry.schema.mode = builder.mode();
                entry.schema.header = SchemaHeader{
                    TypeFamily::Executor,
                    static_cast<TypeKind>(entry.schema.mode),
                    entry.schema.display_name != nullptr && entry.schema.display_name[0] != '\0'
                        ? entry.schema.display_name
                        : "graph_executor"};

                switch (builder.mode())
                {
                    case GraphExecutorMode::Simulation: {
                        const auto &plan = MemoryUtils::plan_for<SimulationExecutorStorage>();
                        entry.context.clock_type = intern_clock_type(
                            detail::evaluation_clock_schema(), plan, simulation_clock_ops(),
                            "hgraph.clock.simulation");
                        entry.ops = simulation_executor_ops(&entry.context);
                        entry.type = intern_executor_type(entry.schema, plan, entry.ops,
                                                          "hgraph.executor.simulation");
                        canonical_types.emplace(std::move(key), entry.type);
                        return entry.type;
                    }
                    case GraphExecutorMode::RealTime: {
                        const auto &plan = MemoryUtils::plan_for<RealTimeExecutorStorage>();
                        entry.context.clock_type = intern_clock_type(
                            detail::evaluation_clock_schema(), plan, realtime_clock_ops(),
                            "hgraph.clock.realtime");
                        entry.ops = realtime_executor_ops(&entry.context);
                        entry.type = intern_executor_type(entry.schema, plan, entry.ops,
                                                          "hgraph.executor.realtime");
                        canonical_types.emplace(std::move(key), entry.type);
                        return entry.type;
                    }
                }
                throw std::logic_error("Unknown graph executor mode");
            }

            void clear() noexcept
            {
              canonical_types.clear();
              entries.clear();
              names.clear();
            }

            std::deque<Entry>                          entries{};
            std::vector<std::unique_ptr<std::string>>  names{};
            std::unordered_map<Key, ExecutorTypeRef, KeyHash> canonical_types{};
        };

        ExecutorRuntimeRegistry &executor_runtime_registry()
        {
            static ExecutorRuntimeRegistry registry;
            return registry;
        }

    }  // namespace

    namespace
    {
        void validate_executor_record(const TypeRecord &record)
        {
            if (!record.valid() || record.schema->family != TypeFamily::Executor ||
                record.role != TypeRole::Runtime)
            {
                throw std::invalid_argument("ExecutorTypeRef requires an Executor/Runtime TypeRecord");
            }
            const auto *schema = reinterpret_cast<const GraphExecutorTypeMetaData *>(record.schema);
            if (record.schema->kind != static_cast<TypeKind>(schema->mode))
            {
                throw std::invalid_argument("ExecutorTypeRef requires matching common and executor schema kinds");
            }
            if (record.ops_abi_version != EXECUTOR_OPS_ABI_VERSION || record.ops == nullptr)
            {
                throw std::invalid_argument(
                    "ExecutorTypeRef requires executor ops ABI version " +
                    std::to_string(EXECUTOR_OPS_ABI_VERSION));
            }
            if (record.capabilities != executor_type_capabilities(*record.plan))
            {
                throw std::invalid_argument("ExecutorTypeRef capabilities do not match its storage plan");
            }
        }
    }  // namespace

    TypeCapabilities executor_type_capabilities(const MemoryUtils::StoragePlan &plan)
    {
        TypeCapabilities result = TypeCapabilities::Viewable | TypeCapabilities::Mutable;
        if (plan.can_default_construct()) result |= TypeCapabilities::Constructible;
        if (plan.trivially_destructible || plan.lifecycle.can_destroy())
            result |= TypeCapabilities::Destructible;
        if (plan.can_copy_construct()) result |= TypeCapabilities::Copyable;
        if (plan.can_move_construct()) result |= TypeCapabilities::Movable;
        return result;
    }

    ExecutorTypeRef intern_executor_type(const GraphExecutorTypeMetaData &schema,
                                         const MemoryUtils::StoragePlan &plan,
                                         const GraphExecutorOps &ops,
                                         std::string_view implementation_label)
    {
        if (!schema.header.valid() || schema.header.family != TypeFamily::Executor ||
            schema.header.kind != static_cast<TypeKind>(schema.mode))
        {
            throw std::invalid_argument("intern_executor_type requires a valid executor schema header");
        }
        const TypeRecordDefinition definition{
            .key = TypeRecordKey{.schema = &schema.header,
                                 .role = TypeRole::Runtime,
                                 .plan = &plan,
                                 .ops = &ops,
                                 .debug = nullptr,
                                 .implementation_label = implementation_label},
            .ops_abi_version = EXECUTOR_OPS_ABI_VERSION,
            .capabilities = executor_type_capabilities(plan),
        };
        return ExecutorTypeRef{&TypeRecordRegistry::instance().intern(definition)};
    }

    ExecutorTypeRef ExecutorTypeRef::checked(AnyPtr pointer)
    {
        if (pointer.is_unbound()) return {};
        if (!pointer.well_formed() || pointer.record() == nullptr)
            throw std::invalid_argument("ExecutorTypeRef requires a well-formed pointer");
        validate_executor_record(*pointer.record());
        return ExecutorTypeRef{pointer.record()};
    }

    bool ExecutorTypeRef::valid() const noexcept
    {
        if (record_ == nullptr) return false;
        return fallback_on_exception(false, [&] {
            validate_executor_record(*record_);
            return true;
        });
    }

    const GraphExecutorTypeMetaData *ExecutorTypeRef::schema() const noexcept
    {
        return record_ != nullptr ? reinterpret_cast<const GraphExecutorTypeMetaData *>(record_->schema) : nullptr;
    }

    const MemoryUtils::StoragePlan &ExecutorTypeRef::checked_plan() const
    {
        if (plan() == nullptr) throw std::logic_error("ExecutorTypeRef is unbound");
        return *plan();
    }

    ExecutorPtr ExecutorTypeRef::typed_null() const noexcept
    {
        return ExecutorPtr{AnyPtr{record_, nullptr, AccessMode::ReadOnly}, ExecutorPtr::UncheckedTag{}};
    }

    ExecutorPtr ExecutorTypeRef::read_only(const void *data) const noexcept
    {
        return ExecutorPtr{AnyPtr{record_, data, AccessMode::ReadOnly}, ExecutorPtr::UncheckedTag{}};
    }

    ExecutorPtr ExecutorTypeRef::writable(void *data) const noexcept
    {
        return ExecutorPtr{AnyPtr{record_, data, AccessMode::Writable}, ExecutorPtr::UncheckedTag{}};
    }

    std::string_view GraphExecutorTypeMetaData::name() const noexcept
    {
        return display_name != nullptr ? std::string_view{display_name} : std::string_view{};
    }

    GraphExecutorPhaseAction::GraphExecutorPhaseAction(void *context, Invoke invoke) noexcept
        : context_(context), invoke_(invoke)
    {
    }

    void GraphExecutorPhaseAction::operator()() const
    {
        if (invoke_ == nullptr)
        {
            throw std::logic_error("GraphExecutorPhaseAction is empty");
        }
        invoke_(context_);
    }

    GraphExecutorPhaseAction::operator bool() const noexcept
    {
        return invoke_ != nullptr;
    }

    PushQueueEngineView::PushQueueEngineView() noexcept = default;

    PushQueueEngineView::PushQueueEngineView(ExecutorPtr pointer) noexcept : pointer_(pointer) {}

    bool PushQueueEngineView::valid() const noexcept
    {
        return pointer_.has_value();
    }

    bool PushQueueEngineView::stop_requested() const noexcept
    {
        return valid() && ops().stop_requested_impl(ops().context, pointer_.data());
    }

    bool PushQueueEngineView::is_push_update_pending() const noexcept
    {
        if (!valid()) return false;
        return ops().is_push_update_pending_impl(ops().context, const_cast<void *>(pointer_.data()));
    }

    void PushQueueEngineView::mark_push_update_pending() const
    {
        if (!valid())
            throw std::logic_error("PushQueueEngineView::mark_push_update_pending requires a live executor");
        if (!pointer_.writable_access())
            throw std::logic_error("PushQueueEngineView requires writable executor access");
        ops().mark_push_update_pending_impl(ops().context, const_cast<void *>(pointer_.data()));
    }

    bool PushQueueEngineView::reset_push_update_pending() const noexcept
    {
        if (!valid() || !pointer_.writable_access()) return false;
        return ops().reset_push_update_pending_impl(ops().context, const_cast<void *>(pointer_.data()));
    }

    const GraphExecutorOps &PushQueueEngineView::ops() const
    {
        return ExecutorTypeRef{pointer_.record()}.ops_ref();
    }

    EngineControlView::EngineControlView() noexcept = default;

    EngineControlView::EngineControlView(ExecutorPtr pointer) noexcept : pointer_(pointer) {}

    bool EngineControlView::valid() const noexcept { return pointer_.valid(); }

    GraphExecutorMode EngineControlView::mode() const noexcept
    {
        const auto *metadata = valid() ? GraphExecutorView{pointer_}.schema() : nullptr;
        return metadata != nullptr ? metadata->mode : GraphExecutorMode::Simulation;
    }

    DateTime EngineControlView::start_time() const noexcept
    {
        return GraphExecutorView{pointer_}.start_time();
    }

    DateTime EngineControlView::end_time() const noexcept
    {
        return GraphExecutorView{pointer_}.end_time();
    }

    bool EngineControlView::stop_requested() const noexcept
    {
        return GraphExecutorView{pointer_}.stop_requested();
    }

    EvaluationClockView EngineControlView::evaluation_clock() const noexcept
    {
        return GraphExecutorView{pointer_}.evaluation_clock();
    }

    void EngineControlView::request_stop() const noexcept
    {
        GraphExecutorView{pointer_}.request_stop();
    }

    void EngineControlView::add_before_evaluation_notification(std::function<void()> fn) const
    {
        GraphExecutorView view{pointer_};
        if (!view.valid()) { throw std::logic_error("no active graph executor"); }
        const ExecutorTypeRef executor_type = view.type();
        const auto           &table         = executor_type.ops_ref();
        if (table.add_evaluation_notification_impl == nullptr)
        {
            throw std::logic_error("executor does not support evaluation notifications");
        }
        table.add_evaluation_notification_impl(table.context, view.data(), std::move(fn), true);
    }

    void EngineControlView::add_after_evaluation_notification(std::function<void()> fn) const
    {
        GraphExecutorView view{pointer_};
        if (!view.valid()) { throw std::logic_error("no active graph executor"); }
        const ExecutorTypeRef executor_type = view.type();
        const auto           &table         = executor_type.ops_ref();
        if (table.add_evaluation_notification_impl == nullptr)
        {
            throw std::logic_error("executor does not support evaluation notifications");
        }
        table.add_evaluation_notification_impl(table.context, view.data(), std::move(fn), false);
    }

    GraphExecutorView::GraphExecutorView() noexcept = default;

    GraphExecutorView::GraphExecutorView(ExecutorPtr pointer) noexcept : pointer_(pointer) {}

    GraphExecutorView::GraphExecutorView(ExecutorTypeRef type, void *memory) noexcept
        : pointer_(type && memory != nullptr ? type.writable(memory) : ExecutorPtr{})
    {
    }

    bool GraphExecutorView::valid() const noexcept { return pointer_.valid(); }
    ExecutorTypeRef GraphExecutorView::type() const noexcept { return ExecutorTypeRef{pointer_.record()}; }
    ExecutorPtr GraphExecutorView::pointer() const noexcept { return pointer_; }
    const GraphExecutorTypeMetaData *GraphExecutorView::schema() const noexcept
    {
        return type().schema();
    }
    void *GraphExecutorView::data() const noexcept { return const_cast<void *>(pointer_.data()); }

    DateTime GraphExecutorView::start_time() const noexcept
    {
        return valid() ? ops().start_time_impl(ops().context, data()) : MIN_ST;
    }

    DateTime GraphExecutorView::end_time() const noexcept
    {
        return valid() ? ops().end_time_impl(ops().context, data()) : MAX_ET;
    }

    bool GraphExecutorView::stop_requested() const noexcept
    {
        return valid() && ops().stop_requested_impl(ops().context, data());
    }

    GraphView GraphExecutorView::graph() const
    {
        return valid() ? ops().graph_impl(ops().context, data()) : GraphView{};
    }

    ClockPtr GraphExecutorView::evaluation_clock_ptr() const noexcept
    {
        return valid() ? ops().evaluation_clock_ptr_impl(ops().context, data()) : ClockPtr{};
    }

    EvaluationClockView GraphExecutorView::evaluation_clock() const noexcept
    {
        return EvaluationClockView{evaluation_clock_ptr()};
    }

    PushQueueEngineView GraphExecutorView::push_queue_engine() const noexcept
    {
        return PushQueueEngineView{pointer_};
    }

    EngineControlView GraphExecutorView::engine_control() const noexcept
    {
        return EngineControlView{pointer_};
    }

    LifecycleObserverList &GraphExecutorView::lifecycle_observers() const
    {
        if (!valid()) { throw std::logic_error("Graph executor is missing its lifecycle observer list"); }
        auto *list =
            ops().lifecycle_observers_impl == nullptr ? nullptr : ops().lifecycle_observers_impl(ops().context, data());
        if (list == nullptr) { throw std::logic_error("Graph executor is missing its lifecycle observer list"); }
        return *list;
    }

    spdlog::logger *GraphExecutorView::logger() const noexcept
    {
        return valid() && ops().logger_impl != nullptr
                   ? ops().logger_impl(ops().context, data())
                   : nullptr;
    }

    const LoggerOps *GraphExecutorView::logger_ops() const noexcept
    {
        return valid() && ops().logger_ops_impl != nullptr
                   ? ops().logger_ops_impl(ops().context, data())
                   : &plain_logger_ops();
    }

    bool GraphExecutorView::run_logging_enabled() const noexcept
    {
        return valid() && ops().run_logging_enabled_impl != nullptr &&
               ops().run_logging_enabled_impl(ops().context, data());
    }

    ErrorCaptureOptions GraphExecutorView::error_capture_options() const noexcept
    {
        return valid() && ops().error_capture_options_impl != nullptr
                   ? ops().error_capture_options_impl(ops().context, data())
                   : ErrorCaptureOptions{};
    }

    bool GraphExecutorView::cleanup_on_error() const noexcept
    {
        return !valid() || ops().cleanup_on_error_impl == nullptr ||
               ops().cleanup_on_error_impl(ops().context, data());
    }

    void GraphExecutorView::run() const
    {
        if (!valid()) { throw std::logic_error("GraphExecutorView::run requires a live executor"); }
        const bool log_run = run_logging_enabled();
        LoggerView run_log{log_run ? logger() : nullptr, {}, logger_ops()};
        run_log.debug("Starting graph run");
        auto finished = make_scope_exit<true>([&] {
            run_log.debug("Finished graph run");
        });
        auto failed = UnwindCleanupGuard([&] {
            run_log.error("Graph run failed");
        });
        ops().run_impl(ops().context, *this);
        failed.release();
    }

    void GraphExecutorView::request_stop() const noexcept
    {
        if (valid() && pointer_.writable_access()) ops().request_stop_impl(ops().context, data());
    }

    const GraphExecutorOps &GraphExecutorView::ops() const
    {
        return type().ops_ref();
    }

    GraphExecutorValue::GraphExecutorValue() noexcept = default;

    GraphExecutorValue::GraphExecutorValue(const GraphExecutorBuilder &builder)
    {
        if (builder.mode() != GraphExecutorMode::RealTime &&
            graph_has_push_sources(builder.graph_builder()))
        {
            throw std::invalid_argument("Push source nodes require a real-time graph executor");
        }

        const auto type = builder.type();
        storage_ = storage_type::owning_constructed(*type.record(), [&](void *dst) {
            switch (builder.mode())
            {
                case GraphExecutorMode::Simulation:
                    std::construct_at(MemoryUtils::cast<SimulationExecutorStorage>(dst), builder, type, dst);
                    return;
                case GraphExecutorMode::RealTime:
                    std::construct_at(MemoryUtils::cast<RealTimeExecutorStorage>(dst), builder, type, dst);
                    return;
            }
            throw std::logic_error("Unknown graph executor mode");
        });
    }

    GraphExecutorValue::~GraphExecutorValue() = default;

    bool GraphExecutorValue::has_value() const noexcept { return storage_.has_value(); }

    GraphExecutorView GraphExecutorValue::view()
    {
        return GraphExecutorView{ExecutorTypeRef{storage_.binding()}, storage_.data()};
    }

    GraphExecutorView GraphExecutorValue::view() const
    {
        return GraphExecutorView{ExecutorTypeRef{storage_.binding()}, const_cast<void *>(storage_.data())};
    }

    GraphExecutorBuilder::GraphExecutorBuilder() = default;

    GraphExecutorBuilder &GraphExecutorBuilder::label(std::string label)
    {
        label_ = std::move(label);
        type_ = {};
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::graph_builder(GraphBuilder graph_builder)
    {
        graph_builder_ = std::move(graph_builder);
        type_ = {};
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::mode(GraphExecutorMode mode) noexcept
    {
        mode_ = mode;
        type_ = {};
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::start_time(DateTime start_time) noexcept
    {
        start_time_ = start_time;
        start_time_set_ = true;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::end_time(DateTime end_time) noexcept
    {
        end_time_ = end_time;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::logger(std::shared_ptr<spdlog::logger> logger,
                                                       const LoggerOps *ops)
    {
        if (logger != nullptr && ops != nullptr && ops->emit_impl == nullptr)
        {
            throw std::invalid_argument("GraphExecutorBuilder logger operations require an emission callback");
        }
        logger_ = std::move(logger);
        logger_ops_ = logger_ != nullptr ? ops : nullptr;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::error_capture_options(
        ErrorCaptureOptions options) noexcept
    {
        error_capture_options_ = options;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::cleanup_on_error(bool value) noexcept
    {
        cleanup_on_error_ = value;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::phase_runner(GraphExecutorPhaseRunner runner)
    {
        phase_runner_ = std::move(runner);
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::max_consecutive_immediate_cycles(std::uint32_t limit) noexcept
    {
        max_consecutive_immediate_cycles_ = limit;
        return *this;
    }

    GraphExecutorBuilder &GraphExecutorBuilder::add_lifecycle_observer(LifecycleObserver *observer)
    {
        if (observer != nullptr) { lifecycle_observers_.push_back(observer); }
        return *this;
    }

    std::string_view GraphExecutorBuilder::label() const noexcept
    {
        return label_;
    }

    const GraphBuilder &GraphExecutorBuilder::graph_builder() const noexcept
    {
        return graph_builder_;
    }

    GraphExecutorMode GraphExecutorBuilder::mode() const noexcept
    {
        return mode_;
    }

    DateTime GraphExecutorBuilder::start_time() const noexcept
    {
        if (start_time_set_) { return start_time_; }
        return mode_ == GraphExecutorMode::RealTime ? current_wall_time() : MIN_ST;
    }

    DateTime GraphExecutorBuilder::end_time() const noexcept
    {
        return end_time_;
    }

    const std::shared_ptr<spdlog::logger> &GraphExecutorBuilder::logger() const noexcept
    {
        return logger_;
    }

    const LoggerOps &GraphExecutorBuilder::logger_ops() const noexcept
    {
        return logger_ops_ != nullptr ? *logger_ops_ : plain_logger_ops();
    }

    ErrorCaptureOptions GraphExecutorBuilder::error_capture_options() const noexcept
    {
        return error_capture_options_;
    }

    bool GraphExecutorBuilder::cleanup_on_error() const noexcept
    {
        return cleanup_on_error_;
    }

    const GraphExecutorPhaseRunner &GraphExecutorBuilder::phase_runner() const noexcept
    {
        return phase_runner_;
    }

    std::uint32_t GraphExecutorBuilder::max_consecutive_immediate_cycles() const noexcept
    {
        return max_consecutive_immediate_cycles_;
    }

    const std::vector<LifecycleObserver *> &GraphExecutorBuilder::lifecycle_observers() const noexcept
    {
        return lifecycle_observers_;
    }

    GraphTypeRef GraphExecutorBuilder::graph_type() const
    {
        return graph_builder_.type();
    }

    ExecutorTypeRef GraphExecutorBuilder::type() const
    {
        if (!type_) { type_ = executor_runtime_registry().make_type(*this); }
        return type_;
    }

    GraphExecutorValue GraphExecutorBuilder::make_executor() const
    {
        return GraphExecutorValue{*this};
    }

    void clear_executor_runtime_types() noexcept
    {
        executor_runtime_registry().clear();
    }

    std::size_t detail::executor_runtime_type_count() noexcept
    {
        return executor_runtime_registry().entries.size();
    }

}  // namespace hgraph
