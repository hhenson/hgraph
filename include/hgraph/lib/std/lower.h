#ifndef HGRAPH_LIB_STD_LOWER_H
#define HGRAPH_LIB_STD_LOWER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/wired_fn.h>

#include <optional>
#include <span>
#include <string>
#include <utility>

namespace hgraph
{
    struct LifecycleObserver;
}

namespace hgraph::stdlib
{
    /** Configuration for lowering a reactive graph to an Arrow-frame call. */
    struct LowerOptions
    {
        std::string date_column{"date"};
        std::string as_of_column{"as_of"};
        std::string key_column{"key"};
        std::string value_column{"value"};
        /** When false, select the latest visible input row per (date, key). */
        bool no_as_of_support{true};
        DateTime start_time{MIN_ST};
        DateTime end_time{MAX_ET};
        std::optional<DateTime> as_of{};
        /** Borrowed; must outlive the prepared execution and its run. */
        LifecycleObserver *observer{nullptr};
        /** Optional lexical wrapper for each complete executor phase. */
        GraphExecutorPhaseRunner phase_runner{};
        /** Install ``phase_runner`` only when the finished graph or a nested
            child plan opts into it. */
        bool phase_runner_requires_graph_opt_in{false};
    };

    /**
     * One prepared native ``lower`` execution.
     *
     * Preparation wires each Arrow frame through ``from_data_frame``, invokes
     * the supplied ``WiredFn``, and snapshots its output with
     * ``to_data_frame``. ``run`` executes the ordinary graph executor and
     * retains the collected output frame. The private result is removed from
     * graph ``GlobalState`` before the normal copy-out to an active
     * ``GlobalContext``.
     */
    class HGRAPH_CLASS_EXPORT LowerExecution
    {
      public:
        LowerExecution() noexcept;
        ~LowerExecution();
        LowerExecution(const LowerExecution &) = delete;
        LowerExecution &operator=(const LowerExecution &) = delete;
        LowerExecution(LowerExecution &&) noexcept;
        LowerExecution &operator=(LowerExecution &&) noexcept;

        void run();
        [[nodiscard]] bool ran() const noexcept;
        [[nodiscard]] bool has_output() const noexcept;
        [[nodiscard]] GlobalStateView global_state() const;
        [[nodiscard]] const std::optional<Frame> &result() const;

      private:
        friend HGRAPH_EXPORT LowerExecution prepare_lower(const WiredFn &, std::span<const Frame>, LowerOptions);

        explicit LowerExecution(GraphExecutorValue executor, bool has_output);

        GraphExecutorValue executor_{};
        std::optional<Frame> result_{};
        bool has_output_{false};
        bool ran_{false};
    };

    /**
     * Prepare an Arrow-native execution of ``function`` over input frames.
     * Standard operator overloads must already be registered.
     */
    [[nodiscard]] HGRAPH_EXPORT LowerExecution prepare_lower(const WiredFn &function, std::span<const Frame> inputs,
                                                             LowerOptions options = {});

    /** Prepare, run, and return the output frame (``nullopt`` for sink graphs). */
    [[nodiscard]] HGRAPH_EXPORT std::optional<Frame> lower(const WiredFn &function, std::span<const Frame> inputs,
                                                           LowerOptions options = {});

    /** C++ graph convenience: input frames follow the graph's Port parameter order.
     */
    template <typename Graph>
    [[nodiscard]] std::optional<Frame> lower(std::span<const Frame> inputs, LowerOptions options = {})
    {
        return lower(fn<Graph>(), inputs, std::move(options));
    }
} // namespace hgraph::stdlib

#endif // HGRAPH_LIB_STD_LOWER_H
