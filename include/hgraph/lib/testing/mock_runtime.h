#ifndef HGRAPH_LIB_TESTING_MOCK_RUNTIME_H
#define HGRAPH_LIB_TESTING_MOCK_RUNTIME_H

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/lifecycle_observer.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph::testing
{
    namespace mock_runtime_detail
    {
        [[nodiscard]] inline DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
        }

        struct MockGraphExecutorStorage
        {
            MockGraphExecutorStorage(const GraphBuilder &builder,
                                     ExecutorTypeRef type,
                                     void *executor_memory,
                                     DateTime start,
                                     DateTime end)
                : graph_value(builder.make_root_graph(type.writable(executor_memory))),
                  start_time(start),
                  end_time(end),
                  evaluation_time(start),
                  cycle_wall_start(current_wall_time())
            {
            }

            void set_evaluation_time(DateTime value) noexcept
            {
                evaluation_time = value;
                cycle_wall_start = current_wall_time();
            }

            [[nodiscard]] GraphView graph() const noexcept
            {
                return graph_value.view();
            }

            LifecycleObserverList lifecycle_observers{};
            GraphValue      graph_value{};
            DateTime        start_time{MIN_ST};
            DateTime        end_time{MAX_ET};
            DateTime        evaluation_time{MIN_ST};
            DateTime        cycle_wall_start{current_wall_time()};
            bool            stop_requested{false};
        };

        [[nodiscard]] inline MockGraphExecutorStorage &mock_executor_storage(void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Mock executor storage is null"); }
            return *MemoryUtils::cast<MockGraphExecutorStorage>(memory);
        }

        [[nodiscard]] inline const MockGraphExecutorStorage &mock_executor_storage(const void *memory)
        {
            if (memory == nullptr) { throw std::logic_error("Mock executor storage is null"); }
            return *MemoryUtils::cast<MockGraphExecutorStorage>(memory);
        }

        [[nodiscard]] inline TimeDelta elapsed_since(DateTime start) noexcept
        {
            const TimeDelta elapsed = current_wall_time() - start;
            return elapsed >= TimeDelta{0} ? elapsed : TimeDelta{0};
        }

        [[nodiscard]] inline DateTime clock_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).evaluation_time;
        }

        [[nodiscard]] inline DateTime clock_now_impl(const void *, const void *memory) noexcept
        {
            const auto &state = mock_executor_storage(memory);
            return state.evaluation_time + elapsed_since(state.cycle_wall_start);
        }

        [[nodiscard]] inline TimeDelta clock_cycle_time_impl(const void *, const void *memory) noexcept
        {
            return elapsed_since(mock_executor_storage(memory).cycle_wall_start);
        }

        [[nodiscard]] inline DateTime clock_next_cycle_evaluation_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).evaluation_time + MIN_TD;
        }

        [[nodiscard]] inline const EvaluationClockOps &mock_clock_ops() noexcept
        {
            static constexpr EvaluationClockOps table{
                .context = nullptr,
                .evaluation_time_impl = &clock_evaluation_time_impl,
                .now_impl = &clock_now_impl,
                .cycle_time_impl = &clock_cycle_time_impl,
                .next_cycle_evaluation_time_impl = &clock_next_cycle_evaluation_time_impl,
            };
            return table;
        }

        struct MockRuntimeContext
        {
            ClockTypeRef clock_type{};
        };

        [[nodiscard]] inline MockRuntimeContext &mock_runtime_context() noexcept
        {
            static MockRuntimeContext context;
            return context;
        }

        inline void run_impl(const void *, const GraphExecutorView &)
        {
            throw std::logic_error("MockGraphExecutor does not run graphs");
        }

        inline void request_stop_impl(const void *, void *memory) noexcept
        {
            mock_executor_storage(memory).stop_requested = true;
        }

        [[nodiscard]] inline bool stop_requested_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).stop_requested;
        }

        [[nodiscard]] inline DateTime start_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).start_time;
        }

        [[nodiscard]] inline DateTime end_time_impl(const void *, const void *memory) noexcept
        {
            return mock_executor_storage(memory).end_time;
        }

        [[nodiscard]] inline GraphView graph_impl(const void *, void *memory)
        {
            return mock_executor_storage(memory).graph();
        }

        [[nodiscard]] inline ClockPtr evaluation_clock_ptr_impl(const void *context, void *memory) noexcept
        {
            return static_cast<const MockRuntimeContext *>(context)->clock_type.read_only(memory);
        }

        [[nodiscard]] inline LifecycleObserverList *lifecycle_observers_impl(const void *, void *memory) noexcept
        {
            return &mock_executor_storage(memory).lifecycle_observers;
        }

        [[nodiscard]] inline const GraphExecutorOps &mock_executor_ops() noexcept
        {
            static const GraphExecutorOps table{
                .context = &mock_runtime_context(),
                .run_impl = &run_impl,
                .request_stop_impl = &request_stop_impl,
                .stop_requested_impl = &stop_requested_impl,
                .start_time_impl = &start_time_impl,
                .end_time_impl = &end_time_impl,
                .graph_impl = &graph_impl,
                .evaluation_clock_ptr_impl = &evaluation_clock_ptr_impl,
                .lifecycle_observers_impl = &lifecycle_observers_impl,
            };
            return table;
        }

        [[nodiscard]] inline ExecutorTypeRef mock_executor_type()
        {
            static const GraphExecutorTypeMetaData meta{
                .header = SchemaHeader{TypeFamily::Executor,
                                       static_cast<TypeKind>(GraphExecutorMode::Simulation),
                                       "mock_graph_executor"},
                .display_name = "mock_graph_executor",
                .mode = GraphExecutorMode::Simulation,
            };
            const auto &plan = MemoryUtils::plan_for<MockGraphExecutorStorage>();
            mock_runtime_context().clock_type = intern_clock_type(
                ::hgraph::detail::evaluation_clock_schema(), plan, mock_clock_ops(), "hgraph.clock.mock");
            return intern_executor_type(meta, plan, mock_executor_ops(), "hgraph.executor.mock");
        }
    }  // namespace mock_runtime_detail

    class MockGraphExecutor
    {
      public:
        using storage_type = MemoryUtils::ErasedOwner<MemoryUtils::InlineStoragePolicy<>, TypeRecord>;

        MockGraphExecutor() noexcept = default;

        explicit MockGraphExecutor(const GraphBuilder &builder,
                                   DateTime start_time = MIN_ST,
                                   DateTime end_time = MAX_ET)
        {
            const auto type = mock_runtime_detail::mock_executor_type();
            type_ = type;
            storage_ = storage_type::owning_constructed(*type.record(), [&](void *dst) {
                std::construct_at(
                    MemoryUtils::cast<mock_runtime_detail::MockGraphExecutorStorage>(dst),
                    builder,
                    type,
                    dst,
                    start_time,
                    end_time);
            });
        }

        [[nodiscard]] bool has_value() const noexcept { return storage_.has_value(); }

        [[nodiscard]] GraphExecutorView view() noexcept
        {
            return GraphExecutorView{type_, storage_.data()};
        }

        [[nodiscard]] GraphExecutorView view() const noexcept
        {
            return GraphExecutorView{type_, const_cast<void *>(storage_.data())};
        }

        void set_evaluation_time(DateTime value) noexcept
        {
            mock_runtime_detail::mock_executor_storage(storage_.data()).set_evaluation_time(value);
        }

      private:
        ExecutorTypeRef type_{};
        storage_type storage_{};
    };

    class MockRootGraph
    {
      public:
        explicit MockRootGraph(const GraphBuilder &builder,
                               DateTime start_time = MIN_ST,
                               DateTime end_time = MAX_ET)
            : executor_(builder, start_time, end_time)
        {
        }

        [[nodiscard]] GraphView graph() noexcept { return executor_.view().graph(); }
        [[nodiscard]] GraphView graph() const noexcept { return executor_.view().graph(); }
        [[nodiscard]] GraphExecutorView executor() noexcept { return executor_.view(); }
        [[nodiscard]] GraphExecutorView executor() const noexcept { return executor_.view(); }

      private:
        MockGraphExecutor  executor_{};
    };
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_MOCK_RUNTIME_H
