// runtime/executor.h — the graph executor ops: the evaluation loop and all
// run-level state (mode, evaluation time, stop/wake machinery) live HERE, in
// a type-erased ops table — there is deliberately no separate
// EvaluationEngine/EvaluationClock object (recorded decision);
// EvaluationClockView is a borrowed read-only projection over this storage.
// Modes: GraphExecutorMode::{Simulation, RealTime}. Design record:
// docs/source/developer_guide/architecture.rst.
#ifndef HGRAPH_RUNTIME_EXECUTOR_H
#define HGRAPH_RUNTIME_EXECUTOR_H

#include <hgraph/runtime/executor_type_ref.h>
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
        struct AsyncNodeWakeAccess;
        struct GraphExecutorPhaseActionAccess;
    }

    class GraphExecutorBuilder;
    class GraphExecutorValue;
    class GraphExecutorView;
    class EngineControlView;
    class AsyncNodeWakeSender;
    struct LifecycleObserver;
    class LifecycleObserverList;
    struct LoggerOps;
    class PushQueueEngineView;

    /** Engine execution mode for the first-pass graph executor. */
    enum class GraphExecutorMode : std::uint8_t
    {
        Simulation,
        RealTime,
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
    class HGRAPH_EXPORT GraphExecutorPhaseAction
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
    class HGRAPH_EXPORT RecursiveEvaluationError : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /** Schema/config descriptor for a graph executor. */
    struct HGRAPH_EXPORT GraphExecutorTypeMetaData
    {
        SchemaHeader header{};
        const char *display_name{nullptr};
        GraphExecutorMode mode{GraphExecutorMode::Simulation};

        [[nodiscard]] std::string_view name() const noexcept;
    };

    /** Type-erased executor operation table. */
    struct HGRAPH_EXPORT GraphExecutorOps
    {
        const void *context{nullptr};

        void (*run_impl)(const void *context, const GraphExecutorView &executor) = nullptr;
        void (*request_stop_impl)(const void *context, void *memory) noexcept = nullptr;
        /** One-shot cycle-boundary notification (2026-08-01): ``before``
            selects the FIFO queue drained just before the next root
            evaluation; otherwise the LIFO queue drained right after it.
            Each boundary drains re-entrant registrations to completion.
            Eval-thread only. */
        void (*add_evaluation_notification_impl)(const void *context, void *memory,
                                                 std::function<void()> fn, bool before) = nullptr;
        /** Thread-safe ingress wake. The executor admits the node schedule at
            the next root-cycle boundary, so nested graph scheduling remains
            on the evaluation thread. */
        void (*wake_node_impl)(const void *context, void *memory,
                               AsyncNodeWakeSender sender) = nullptr;
        bool (*stop_requested_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*start_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        DateTime (*end_time_impl)(const void *context, const void *memory) noexcept = nullptr;
        GraphView (*graph_impl)(const void *context, void *memory) = nullptr;
        ClockPtr (*evaluation_clock_ptr_impl)(const void *context, void *memory) noexcept = nullptr;
        void (*mark_push_update_pending_impl)(const void *context, void *memory) = nullptr;
        bool (*is_push_update_pending_impl)(const void *context, void *memory) noexcept = nullptr;
        bool (*reset_push_update_pending_impl)(const void *context, void *memory) noexcept = nullptr;
        bool (*begin_push_cycle_impl)(const void *context, void *memory,
                                      DateTime evaluation_time) = nullptr;
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
    class HGRAPH_EXPORT PushQueueEngineView
    {
      public:
        PushQueueEngineView() noexcept;
        explicit PushQueueEngineView(ExecutorPtr pointer) noexcept;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] bool stop_requested() const noexcept;
        [[nodiscard]] bool is_push_update_pending() const noexcept;

        void mark_push_update_pending() const;
        [[nodiscard]] bool reset_push_update_pending() const noexcept;
        /** Atomically claim the pending push flag and queued async node wakes,
            then admit those node schedules on the evaluation thread. */
        [[nodiscard]] bool begin_push_cycle(DateTime evaluation_time) const;

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
    class HGRAPH_EXPORT EngineControlView
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

      private:
        friend class AsyncNodeWakeSender;

        void wake_node(AsyncNodeWakeSender sender) const;

        ExecutorPtr pointer_{};
    };

    static_assert(sizeof(EngineControlView) == sizeof(ExecutorPtr));
    static_assert(std::is_trivially_copyable_v<EngineControlView>);

    /** Copyable thread-safe wake handle for an authored node.

        ``wake`` never evaluates or mutates the graph on the caller's thread.
        It queues one deduplicated schedule with the root executor, wakes an
        idle real-time run, and admits the schedule on the next evaluation
        boundary. This is the callback-side companion to ``NodeScheduler`` for
        notifier-driven sources, including sources owned by nested graphs. */
    class HGRAPH_EXPORT AsyncNodeWakeSender
    {
      public:
        AsyncNodeWakeSender() noexcept = default;
        AsyncNodeWakeSender(EngineControlView engine, GraphPtr graph,
                            std::size_t node_index);

        [[nodiscard]] bool valid() const noexcept;
        void wake() const;
        /** Invalidate every copy and make a racing callback wake a no-op. */
        void close() noexcept;

        friend bool operator==(const AsyncNodeWakeSender &,
                               const AsyncNodeWakeSender &) noexcept = default;

      private:
        friend struct detail::AsyncNodeWakeAccess;

        struct State;
        std::shared_ptr<State> state_{};
    };

    /** Borrowed type-erased executor view. */
    class HGRAPH_EXPORT GraphExecutorView
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
        void request_stop() const noexcept;

      private:
        [[nodiscard]] const GraphExecutorOps &ops() const;

        ExecutorPtr pointer_{};
    };

    /** Owning graph executor value. */
    class HGRAPH_EXPORT GraphExecutorValue
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
    class HGRAPH_EXPORT GraphExecutorBuilder
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
        std::vector<LifecycleObserver *> lifecycle_observers_{};
        mutable ExecutorTypeRef          type_{};
    };

    HGRAPH_EXPORT void clear_executor_runtime_types() noexcept;

    static_assert(offsetof(GraphExecutorTypeMetaData, header) == 0);

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_EXECUTOR_H
