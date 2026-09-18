// runtime/executor.h — the graph executor ops: the evaluation loop and all
// run-level state (mode, evaluation time, stop/wake machinery) live HERE, in
// a type-erased ops table — there is deliberately no separate
// EvaluationEngine/EvaluationClock object (recorded decision);
// EvaluationClockView is a borrowed read-only projection over this storage.
// Modes: GraphExecutorMode::{Simulation, RealTime, ExternallyDriven}.
// Design record:
// docs/source/developer_guide/architecture.rst.
#ifndef HGRAPH_RUNTIME_EXECUTOR_H
#define HGRAPH_RUNTIME_EXECUTOR_H

#include <hgraph/runtime/executor_type_ref.h>
#include <hgraph/runtime/executor_activity.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node_error.h>

#include <functional>
#include <memory>
#include <stdexcept>

namespace spdlog
{
    class logger;
}

namespace hgraph
{
    namespace detail
    {
        struct GraphExecutorPhaseActionAccess;
    }

    class GraphExecutorBuilder;
    class GraphExecutorValue;
    class GraphExecutorView;
    class EngineControlView;
    struct LifecycleObserver;
    class LifecycleObserverList;
    struct LoggerOps;
    class PushQueueEngineView;

    /** Engine execution mode for the first-pass graph executor. */
    enum class GraphExecutorMode : std::uint8_t
    {
        Simulation,
        RealTime,
        /**
         * Neither the schedule nor the wall clock decides when a cycle runs:
         * the caller does. The executor is **stepped**, not run -- ``run()``
         * throws for this mode -- so ``start_external`` / ``step`` /
         * ``stop_external`` replace the run loop and the caller's thread does
         * the driving. There is no queue and no extra thread.
         *
         * This is the substrate for a distributed nested graph (RFC 0037):
         * a worker is handed an evaluation time, evaluates one cycle, and
         * reports what its children want next via
         * ``GraphView::next_scheduled_time()``. Because the time comes from
         * outside, the result is identical to the same graph evaluated in one
         * process, whatever the transport costs.
         */
        ExternallyDriven,
    };

    /** Complete root-executor phases that may be wrapped by an embedding
        runtime. Native executors have no wrapper by default. */
    enum class GraphExecutorPhase : std::uint8_t
    {
        Start,
        Evaluation,
        Stop,
    };

    /**
     * Non-owning invocation of one complete executor phase.
     *
     * A ``GraphExecutorPhaseRunner`` must invoke this action exactly once and
     * synchronously. The action is valid only for the duration of the runner
     * call and must not be retained.
     */
    class HGRAPH_CLASS_EXPORT GraphExecutorPhaseAction
    {
      public:
        GraphExecutorPhaseAction() noexcept = default;

        void operator()() const;
        [[nodiscard]] explicit operator bool() const noexcept;

      private:
        friend struct detail::GraphExecutorPhaseActionAccess;

        using Invoke = void (*)(void *);

        GraphExecutorPhaseAction(void *context, Invoke invoke) noexcept;

        void *context_{nullptr};
        Invoke invoke_{nullptr};
    };

    /** Optional embedding wrapper around each complete executor phase. */
    using GraphExecutorPhaseRunner =
        std::function<void(GraphExecutorPhase, GraphExecutorPhaseAction)>;

    /**
     * Thrown by ``GraphExecutorView::run()`` when the opt-in recursion guard
     * (``GraphExecutorBuilder::max_consecutive_immediate_cycles``) trips: the
     * graph evaluated the configured number of consecutive cycles that each
     * advanced evaluation time by exactly ``MIN_TD`` — the evaluation-graph
     * shape of infinite recursion. The message carries the per-node
     * evaluation path of the last cycle (design record: execution_layer.rst,
     * *Opt-in recursion guard*).
     */
    class HGRAPH_CLASS_EXPORT RecursiveEvaluationError : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /** Schema/config descriptor for a graph executor. */
    struct HGRAPH_CLASS_EXPORT GraphExecutorTypeMetaData
    {
        SchemaHeader header{};
        const char *display_name{nullptr};
        GraphExecutorMode mode{GraphExecutorMode::Simulation};

        [[nodiscard]] std::string_view name() const noexcept;
    };

    /** Type-erased executor operation table. */
    struct HGRAPH_CLASS_EXPORT GraphExecutorOps
    {
        const void *context{nullptr};

        void (*run_impl)(const void *context, const GraphExecutorView &executor) = nullptr;
        // The run loop turned inside out: the caller owns the iteration.
        // Populated by EVERY mode -- the looping modes bind a canonical
        // refusal table rather than leaving these null, so a caller dispatches
        // through the contract instead of testing for a missing slot
        // (AGENTS.md, "Keep erased ops pointers non-null").
        void (*external_start_impl)(const void *context, const GraphExecutorView &executor,
                                    DateTime start_time) = nullptr;
        bool (*external_step_impl)(const void *context, const GraphExecutorView &executor,
                                   DateTime evaluation_time) = nullptr;
        void (*external_stop_impl)(const void *context, const GraphExecutorView &executor) = nullptr;
        // Whole-graph recovery of a stepped graph (RFC 0039). Same rule: every
        // mode binds them, and the looping modes bind the refusal.
        void (*external_start_restored_impl)(const void *context, const GraphExecutorView &executor,
                                             DateTime start_time,
                                             const GraphCheckpointImage &image) = nullptr;
        GraphCheckpointImage (*external_capture_impl)(const void *context,
                                                      const GraphExecutorView &executor) = nullptr;
        void (*request_stop_impl)(const void *context, void *memory) noexcept = nullptr;
        /** One-shot cycle-boundary notification (2026-08-01): ``before``
            selects the FIFO queue drained just before the next root
            evaluation; otherwise the LIFO queue drained right after it.
            Each boundary drains re-entrant registrations to completion.
            Eval-thread only. */
        void (*add_evaluation_notification_impl)(const void *context, void *memory,
                                                 std::function<void()> fn, bool before) = nullptr;
        ExecutorActivityWake (*attach_activity_impl)(const void *context, void *memory, ExecutorActivity activity) = nullptr;
        void (*detach_activity_impl)(const void *context, void *memory, ExecutorActivity activity) = nullptr;
        bool (*stop_requested_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*start_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*end_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        GraphView (*graph_impl)(const void *context, void *memory) = nullptr;
        ClockPtr (*evaluation_clock_ptr_impl)(const void *context, void *memory) noexcept = nullptr;
        void (*mark_push_update_pending_impl)(const void *context, void *memory) = nullptr;
        bool (*is_push_update_pending_impl)(const void *context, void *memory) noexcept = nullptr;
        bool (*reset_push_update_pending_impl)(const void *context, void *memory) noexcept = nullptr;
        /** The executor-owned lifecycle observer list; never null once constructed. */
        LifecycleObserverList *(*lifecycle_observers_impl)(const void *context, void *memory) noexcept = nullptr;
        /** Borrowed run logger; owned by the executor storage. */
        spdlog::logger *(*logger_impl)(const void *context, void *memory) noexcept = nullptr;
        /** Non-null logger emission policy selected by the executor builder. */
        const LoggerOps *(*logger_ops_impl)(const void *context,
                                            const void *memory) noexcept = nullptr;
        bool (*run_logging_enabled_impl)(const void *context,
                                         const void *memory) noexcept = nullptr;
        ErrorCaptureOptions (*error_capture_options_impl)(
            const void *context, const void *memory) noexcept = nullptr;
        bool (*cleanup_on_error_impl)(const void *context,
                                      const void *memory) noexcept = nullptr;
    };

    /** Real-time push queue projection over the root graph executor. */
    class HGRAPH_CLASS_EXPORT PushQueueEngineView
    {
      public:
        PushQueueEngineView() noexcept;
        explicit PushQueueEngineView(ExecutorPtr pointer) noexcept;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] bool stop_requested() const noexcept;
        [[nodiscard]] bool is_push_update_pending() const noexcept;

        void mark_push_update_pending() const;
        [[nodiscard]] bool reset_push_update_pending() const noexcept;

      private:
        [[nodiscard]] const GraphExecutorOps &ops() const;

        ExecutorPtr pointer_{};
    };

    /**
     * Borrowed, copyable control projection over the active graph executor.
     *
     * This is the user-node injectable for run-level control. It deliberately
     * exposes neither ``run()`` nor graph/push-queue internals: the owning
     * ``GraphExecutorValue`` remains the only executor lifetime owner, while
     * authored nodes can inspect run configuration and request an orderly
     * stop after the current evaluation cycle.
     */
    class HGRAPH_CLASS_EXPORT EngineControlView
    {
      public:
        EngineControlView() noexcept;
        explicit EngineControlView(ExecutorPtr pointer) noexcept;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] GraphExecutorMode mode() const noexcept;
        [[nodiscard]] DateTime start_time() const noexcept;
        [[nodiscard]] DateTime end_time() const noexcept;
        [[nodiscard]] bool stop_requested() const noexcept;
        [[nodiscard]] EvaluationClockView evaluation_clock() const noexcept;

        void request_stop() const noexcept;

        /** One-shot notifications fired at the applicable root cycle
            boundary and drained there to completion (the C++-primary
            facility behind python's
            ``EvaluationEngineApi.add_*_evaluation_notification``). */
        void add_before_evaluation_notification(std::function<void()> fn) const;
        void add_after_evaluation_notification(std::function<void()> fn) const;

        /** Owner-thread only. Externally driven executors refuse activities. */
        [[nodiscard]] ExecutorActivityWake attach_activity(ExecutorActivity activity) const;
        void detach_activity(ExecutorActivity activity) const;

      private:
        ExecutorPtr pointer_{};
    };

    static_assert(sizeof(EngineControlView) == sizeof(ExecutorPtr));
    static_assert(std::is_trivially_copyable_v<EngineControlView>);

    /** Borrowed type-erased executor view. */
    class HGRAPH_CLASS_EXPORT GraphExecutorView
    {
      public:
        GraphExecutorView() noexcept;
        explicit GraphExecutorView(ExecutorPtr pointer) noexcept;
        GraphExecutorView(ExecutorTypeRef type, void *memory) noexcept;
        GraphExecutorView(const GraphExecutorView &) = delete;
        GraphExecutorView &operator=(const GraphExecutorView &) = delete;
        GraphExecutorView(GraphExecutorView &&) noexcept = default;
        GraphExecutorView &operator=(GraphExecutorView &&) noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] ExecutorTypeRef type() const noexcept;
        [[nodiscard]] ExecutorPtr pointer() const noexcept;
        [[nodiscard]] const GraphExecutorTypeMetaData *schema() const noexcept;
        [[nodiscard]] void *data() const noexcept;

        [[nodiscard]] DateTime start_time() const noexcept;
        [[nodiscard]] DateTime end_time() const noexcept;
        [[nodiscard]] bool stop_requested() const noexcept;
        [[nodiscard]] GraphView graph() const;
        [[nodiscard]] ClockPtr evaluation_clock_ptr() const noexcept;
        [[nodiscard]] EvaluationClockView evaluation_clock() const noexcept;
        [[nodiscard]] PushQueueEngineView push_queue_engine() const noexcept;
        [[nodiscard]] EngineControlView engine_control() const noexcept;

        /**
         * The lifecycle observer list for this run (design record:
         * architecture.rst, "Lifecycle Observers"). Add/remove observers
         * directly on the returned list at any point before or during the
         * run; every graph reached via ``GraphView::lifecycle_observers()``
         * (root and nested alike) shares this same instance.
         */
        [[nodiscard]] LifecycleObserverList &lifecycle_observers() const;
        /** Borrowed logger configured for this run. */
        [[nodiscard]] spdlog::logger *logger() const noexcept;
        /** Selected passive emission policy for the run logger. */
        [[nodiscard]] const LoggerOps *logger_ops() const noexcept;
        [[nodiscard]] bool run_logging_enabled() const noexcept;
        /** Detail included when a node exception escapes the root graph. */
        [[nodiscard]] ErrorCaptureOptions error_capture_options() const noexcept;
        /** Whether a failed run stops the graph before propagating its error. */
        [[nodiscard]] bool cleanup_on_error() const noexcept;

        /**
         * Run this executor on the calling thread.
         *
         * Distinct executors may run concurrently on different threads without
         * coordinating their evaluation. The owning ``GraphExecutorValue`` for
         * this view must outlive the run. A single executor is mutable run state:
         * invoking ``run()`` concurrently on two views of the same executor is
         * not supported. User callbacks remain responsible for synchronising any
         * external state they deliberately share between graphs.
         */
        void run() const;

        /**
         * Drive an ``ExternallyDriven`` executor one cycle at a time.
         *
         * ``start_external`` runs the start phase; ``step`` evaluates exactly
         * one cycle at the supplied time and returns whether the cycle
         * completed (``false`` means a node requested a mid-cycle pause and
         * the same time must be stepped again); ``stop_external`` runs the
         * stop phase. Each throws ``std::logic_error`` on an executor that is
         * not ``ExternallyDriven``.
         *
         * The caller reads what the graph wants next from
         * ``graph().next_scheduled_time()`` after a completed step -- that
         * value is the whole of the scheduling contract a distributed parent
         * needs back from its child.
         *
         * ``step`` refuses an evaluation time later than
         * ``next_scheduled_time()``. A node runs only when its scheduled slot
         * is exactly the evaluation time, so overrunning due work would
         * discard it silently; the caller is expected to honour the reported
         * time exactly as a local nested parent does.
         *
         * Lifecycle: a throwing ``step`` applies the builder's
         * ``cleanup_on_error`` policy, as ``run()`` does -- the graph is
         * stopped unless the caller asked to keep it for inspection.
         * Destruction stops a still-started graph either way, and
         * ``stop_external`` is a no-op once stopped, so a caller may call it
         * unconditionally from a catch block.
         *
         * Unlike ``run()``, nothing here bounds the cycle against ``end_time``,
         * applies the consecutive immediate-cycle guard, or observes
         * ``request_stop`` -- the caller supplies every time, so it polls
         * ``stop_requested()`` between steps and decides when to finish.
         */
        void start_external(DateTime start_time) const;
        [[nodiscard]] bool step(DateTime evaluation_time) const;
        void stop_external() const;

        /**
         * Whole-graph recovery of a stepped graph (RFC 0039).
         *
         * ``capture_external`` returns the owned image of every node at the
         * last completed step. Between two calls a stepped executor has no
         * cycle in flight, so that boundary is the consistency cut; a node
         * with a schedule still pending beyond it is refused. So is a graph
         * with work still DUE at the cut -- a fresh start that has not been
         * stepped at ``next_scheduled_time()`` -- because a restored start
         * discards bootstrap schedules and that work would never run. A
         * restored graph may be captured again without a step.
         *
         * ``start_external_restored`` replaces ``start_external``: it imports
         * ``image`` into the unstarted graph and then runs the start phase, so
         * start hooks see restored state. Every restored timestamp must
         * precede ``start_time``. A refused image leaves the graph unstarted.
         * Afterwards ``graph().next_scheduled_time()`` reports what the
         * restored graph wants, as it does after any step.
         *
         * The graph must be wired inside a checkpoint scope
         * (``Wiring::checkpoint_component``): an image names its nodes by
         * checkpoint identity. Both throw ``std::logic_error`` on an executor
         * that is not ``ExternallyDriven``. Neither is component recovery --
         * that completed-day policy stays refused on this mode.
         */
        void start_external_restored(DateTime start_time, const GraphCheckpointImage &image) const;
        [[nodiscard]] GraphCheckpointImage capture_external() const;
        void request_stop() const noexcept;

      private:
        [[nodiscard]] const GraphExecutorOps &ops() const;

        ExecutorPtr pointer_{};
    };

    /** Owning graph executor value. */
    class HGRAPH_CLASS_EXPORT GraphExecutorValue
    {
      public:
        using storage_type = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        GraphExecutorValue() noexcept;
        explicit GraphExecutorValue(const GraphExecutorBuilder &builder);
        ~GraphExecutorValue();

        GraphExecutorValue(const GraphExecutorValue &) = delete;
        GraphExecutorValue &operator=(const GraphExecutorValue &) = delete;
        GraphExecutorValue(GraphExecutorValue &&) noexcept = default;
        GraphExecutorValue &operator=(GraphExecutorValue &&) noexcept = default;

        [[nodiscard]] bool has_value() const noexcept;
        [[nodiscard]] GraphExecutorView view();
        [[nodiscard]] GraphExecutorView view() const;

      private:
        storage_type storage_{};
    };

    /** Reusable construction recipe for graph executors. */
    class HGRAPH_CLASS_EXPORT GraphExecutorBuilder
    {
      public:
        GraphExecutorBuilder();

        GraphExecutorBuilder &label(std::string label);
        GraphExecutorBuilder &graph_builder(GraphBuilder graph_builder);
        GraphExecutorBuilder &mode(GraphExecutorMode mode) noexcept;
        /**
         * Set the first evaluation time explicitly. When omitted, simulation
         * starts at MIN_ST and real-time execution captures the wall clock
         * while constructing the executor.
         */
        GraphExecutorBuilder &start_time(DateTime start_time) noexcept;
        GraphExecutorBuilder &end_time(DateTime end_time) noexcept;
        /**
         * Select the executor-owned spdlog logger and its passive emission
         * policy. ``nullptr`` operations select ``plain_logger_ops()``. A
         * custom table must have static (or otherwise executor-long) lifetime
         * and may recover its concrete logger only inside its callback.
         */
        GraphExecutorBuilder &logger(std::shared_ptr<spdlog::logger> logger,
                                     const LoggerOps *ops = nullptr);
        GraphExecutorBuilder &error_capture_options(ErrorCaptureOptions options) noexcept;
        GraphExecutorBuilder &cleanup_on_error(bool value) noexcept;
        /**
         * Wrap complete root start, evaluation-cycle, and stop phases. The
         * runner must call its action exactly once and synchronously. This is
         * intended for embedding runtimes that need a lexical context around
         * a whole phase; ordinary native execution leaves it unset.
         */
        GraphExecutorBuilder &phase_runner(GraphExecutorPhaseRunner runner);
        /**
         * Arm the opt-in recursion guard: after ``limit`` consecutive cycles
         * that each advance evaluation time by exactly ``MIN_TD``, the run
         * records one further cycle's per-node evaluation path and throws
         * ``RecursiveEvaluationError``. ``0`` (the default) disables the
         * guard. Applies to both run modes.
         */
        GraphExecutorBuilder &max_consecutive_immediate_cycles(std::uint32_t limit) noexcept;
        /**
         * Longest single wait the real-time run loop performs before it
         * re-reads the wall clock and the pending-push flag.
         *
         * A producer's ``mark_push_update_pending`` sets that flag under the
         * run loop's mutex before notifying. The wait uses the pending-push
         * and stop flags as its predicate, so a spurious wake does not return
         * control to the run loop. A slice timeout only refreshes the wall
         * clock and re-enters the wait unless the target is due.
         *
         * The finite slice also avoids overflowing the duration conversion
         * inside ``wait_for`` when an idle graph targets ``MAX_ET``. The
         * default is 10 seconds, matching the Python runtime. Ignored in
         * simulation mode.
         */
        GraphExecutorBuilder &max_wait_slice(TimeDelta slice) noexcept;
        /** Register a lifecycle observer for this executor's run (see ``LifecycleObserver``). */
        GraphExecutorBuilder &add_lifecycle_observer(LifecycleObserver *observer);

        [[nodiscard]] std::string_view label() const noexcept;
        [[nodiscard]] const GraphBuilder &graph_builder() const noexcept;
        [[nodiscard]] GraphExecutorMode mode() const noexcept;
        [[nodiscard]] DateTime start_time() const noexcept;
        [[nodiscard]] DateTime end_time() const noexcept;
        [[nodiscard]] const std::shared_ptr<spdlog::logger> &logger() const noexcept;
        [[nodiscard]] const LoggerOps &logger_ops() const noexcept;
        [[nodiscard]] ErrorCaptureOptions error_capture_options() const noexcept;
        [[nodiscard]] bool cleanup_on_error() const noexcept;
        [[nodiscard]] const GraphExecutorPhaseRunner &phase_runner() const noexcept;
        [[nodiscard]] std::uint32_t max_consecutive_immediate_cycles() const noexcept;
        [[nodiscard]] TimeDelta max_wait_slice() const noexcept;
        [[nodiscard]] const std::vector<LifecycleObserver *> &lifecycle_observers() const noexcept;
        [[nodiscard]] GraphTypeRef graph_type() const;
        [[nodiscard]] ExecutorTypeRef type() const;
        [[nodiscard]] GraphExecutorValue make_executor() const;

      private:
        friend class GraphExecutorValue;

        std::string                     label_{};
        GraphBuilder                    graph_builder_{};
        DateTime                        start_time_{MIN_ST};
        bool                            start_time_set_{false};
        DateTime                        end_time_{MAX_ET};
        GraphExecutorMode               mode_{GraphExecutorMode::Simulation};
        std::shared_ptr<spdlog::logger>  logger_{};
        const LoggerOps                 *logger_ops_{nullptr};
        ErrorCaptureOptions             error_capture_options_{};
        bool                            cleanup_on_error_{true};
        GraphExecutorPhaseRunner        phase_runner_{};
        std::uint32_t                   max_consecutive_immediate_cycles_{0};
        TimeDelta                       max_wait_slice_{10'000'000};
        std::vector<LifecycleObserver *> lifecycle_observers_{};
        mutable ExecutorTypeRef          type_{};
    };

    HGRAPH_EXPORT void clear_executor_runtime_types() noexcept;

    static_assert(offsetof(GraphExecutorTypeMetaData, header) == 0);

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EXECUTOR_H
