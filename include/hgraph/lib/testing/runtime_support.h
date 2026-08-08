#ifndef HGRAPH_LIB_TESTING_RUNTIME_SUPPORT_H
#define HGRAPH_LIB_TESTING_RUNTIME_SUPPORT_H

#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::testing
{
    /** Wall-clock time in the engine's DateTime representation. */
    [[nodiscard]] inline DateTime wall_now() noexcept
    {
        return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
    }

    /** Copy an already-erased borrowed value view into a node output for the current evaluation cycle. */
    inline void set_output_value(const NodeView &view, DateTime evaluation_time, ValueView value)
    {
        auto mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
        if (!mutation.copy_value_from(value))
        {
            throw std::logic_error("testing::set_output_value failed to copy value into output");
        }
    }

    /** Move an owned erased value into a node output for the current evaluation cycle. */
    inline void set_output_value(const NodeView &view, DateTime evaluation_time, Value &&value)
    {
        auto mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
        if (!mutation.move_value_from(std::move(value)))
        {
            throw std::logic_error("testing::set_output_value failed to move value into output");
        }
    }

    /** Copy a borrowed owning value into a node output for the current evaluation cycle. */
    inline void set_output_value(const NodeView &view, DateTime evaluation_time, const Value &value)
    {
        set_output_value(view, evaluation_time, value.view());
    }

    /** Convenience overload for scalar values. */
    template <typename T,
              typename = std::enable_if_t<!std::is_same_v<std::remove_cvref_t<T>, Value> &&
                                          !std::is_same_v<std::remove_cvref_t<T>, ValueView>>>
    inline void set_output_value(const NodeView &view, DateTime evaluation_time, T &&value)
    {
        Value wrapped{std::forward<T>(value)};
        set_output_value(view, evaluation_time, std::move(wrapped));
    }

    /** Standard one-field input bundle schema used by test sinks. */
    [[nodiscard]] inline const TSValueTypeMetaData *single_input_schema(
        const TSValueTypeMetaData &input_ts,
        std::string_view field_name = "in")
    {
        return TypeRegistry::instance().un_named_tsb({{std::string{field_name}, &input_ts}});
    }

    /** Standard one-field peered endpoint used by test sinks. */
    [[nodiscard]] inline TSEndpointSchema single_input_endpoint(
        const TSValueTypeMetaData &input_schema,
        const TSValueTypeMetaData &input_ts)
    {
        return TSEndpointSchema::non_peered(
            &input_schema,
            {
                TSEndpointSchema::peered(&input_ts),
            });
    }

    /**
     * Build a sink that records the scalar current value of its single input.
     *
     * By default the sink requests executor stop after it samples one value,
     * which keeps real-time tests from depending on timeout expiry.
     */
    template <typename T, typename Counter>
    [[nodiscard]] inline NodeBuilder recording_scalar_sink(
        const TSValueTypeMetaData &input_schema,
        const TSValueTypeMetaData &input_ts,
        T &observed_value,
        Counter &eval_count,
        bool request_stop = true)
    {
        NodeTypeMetaData schema;
        schema.display_name = "testing_recording_scalar_sink";
        schema.input_schema = &input_schema;
        schema.node_kind = NodeKind::Sink;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&observed_value, &eval_count, request_stop](const NodeView &view,
                                                                          DateTime evaluation_time) {
            ++eval_count;
            auto root = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto input = bundle[0];
            observed_value = input.value().checked_as<T>();
            if (request_stop) { view.graph().executor().request_stop(); }
        };

        return NodeBuilder::native(std::move(schema),
                                   std::move(callbacks),
                                   single_input_endpoint(input_schema, input_ts));
    }

    /** Build a sink that appends each scalar current value of its single input. */
    template <typename T>
    [[nodiscard]] inline NodeBuilder collecting_scalar_sink(
        const TSValueTypeMetaData &input_schema,
        const TSValueTypeMetaData &input_ts,
        std::vector<T> &observed_values,
        std::size_t stop_after_count = 0)
    {
        NodeTypeMetaData schema;
        schema.display_name = "testing_collecting_scalar_sink";
        schema.input_schema = &input_schema;
        schema.node_kind = NodeKind::Sink;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&observed_values, stop_after_count](const NodeView &view, DateTime evaluation_time) {
            auto root = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto input = bundle[0];
            observed_values.push_back(input.value().checked_as<T>());
            if (stop_after_count != 0 && observed_values.size() >= stop_after_count)
            {
                view.graph().executor().request_stop();
            }
        };

        return NodeBuilder::native(std::move(schema),
                                   std::move(callbacks),
                                   single_input_endpoint(input_schema, input_ts));
    }

    /** Build a sink that records the erased current value of its single input. */
    template <typename Counter>
    [[nodiscard]] inline NodeBuilder recording_value_sink(
        const TSValueTypeMetaData &input_schema,
        const TSValueTypeMetaData &input_ts,
        Value &observed_value,
        Counter &eval_count,
        bool request_stop = true)
    {
        NodeTypeMetaData schema;
        schema.display_name = "testing_recording_value_sink";
        schema.input_schema = &input_schema;
        schema.node_kind = NodeKind::Sink;

        NodeCallbacks callbacks;
        callbacks.evaluate = [&observed_value, &eval_count, request_stop](const NodeView &view,
                                                                          DateTime evaluation_time) {
            ++eval_count;
            auto root = view.input(evaluation_time);
            auto bundle = root.as_bundle();
            auto input = bundle[0];
            observed_value = Value{input.value()};
            if (request_stop) { view.graph().executor().request_stop(); }
        };

        return NodeBuilder::native(std::move(schema),
                                   std::move(callbacks),
                                   single_input_endpoint(input_schema, input_ts));
    }

    /** Push source that stores the sender handed out during node start. */
    [[nodiscard]] inline NodeBuilder capturing_push_source(
        const TSValueTypeMetaData &output_schema,
        PushSourceSender &sender)
    {
        return make_push_source_node(output_schema, [&sender](PushSourceSender started_sender) {
            sender = std::move(started_sender);
        });
    }

    /** Run a graph executor on a background thread and rethrow failures on join. */
    class AsyncGraphExecutorRun
    {
      public:
        explicit AsyncGraphExecutorRun(GraphExecutorView &executor)
            : executor_(&executor),
              runner_([this, &executor] {
                errors_.capture([&executor] {
                    executor.run();
                });
            })
        {
        }

        AsyncGraphExecutorRun(const AsyncGraphExecutorRun &) = delete;
        AsyncGraphExecutorRun &operator=(const AsyncGraphExecutorRun &) = delete;
        AsyncGraphExecutorRun(AsyncGraphExecutorRun &&) = delete;
        AsyncGraphExecutorRun &operator=(AsyncGraphExecutorRun &&) = delete;

        ~AsyncGraphExecutorRun()
        {
            if (!runner_.joinable()) { return; }
            executor_->request_stop();
            runner_.join();
        }

        void join()
        {
            if (runner_.joinable()) { runner_.join(); }
            errors_.rethrow_if_any();
        }

      private:
        GraphExecutorView     *executor_{nullptr};
        FirstExceptionRecorder errors_{};
        std::thread            runner_{};
    };

    /** Build and run a graph, returning the owning executor so tests can inspect state. */
    [[nodiscard]] inline GraphExecutorValue run_graph(
        GraphBuilder graph_builder,
        DateTime start_time = MIN_ST,
        DateTime end_time = MIN_ST + TimeDelta{3},
        GraphExecutorMode mode = GraphExecutorMode::Simulation)
    {
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph_builder))
            .mode(mode)
            .start_time(start_time)
            .end_time(end_time);

        GraphExecutorValue executor = executor_builder.make_executor();
        executor.view().run();
        return executor;
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RUNTIME_SUPPORT_H
