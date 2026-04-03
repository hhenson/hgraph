#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/python_export.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace hgraph::v2::test_detail
{
    using PairSchema = TSB<Field<"lhs", TS<int>>, Field<"rhs", TS<int>>>;
    using DictSchema = TSD<int, TS<int>>;

    static_assert(std::same_as<typename In<"pair", PairSchema>::view_type, TSBView<TSInputView>>);
    static_assert(std::same_as<typename Out<PairSchema>::view_type, TSBView<TSOutputView>>);
    static_assert(std::same_as<typename In<"dict", DictSchema>::view_type, TSDView<TSInputView>>);
    static_assert(std::same_as<typename Out<DictSchema>::view_type, TSDView<TSOutputView>>);
    static_assert(std::same_as<decltype(std::declval<const In<"lhs", TS<int>> &>().delta_value()), const int &>);
    static_assert(std::same_as<decltype(std::declval<const In<"pair", PairSchema> &>().delta_value()), View>);

    struct NoopNode
    {
        NoopNode() = delete;
        ~NoopNode() = delete;

        static void eval() {}
    };

    struct SumScalarInputs
    {
        SumScalarInputs() = delete;
        ~SumScalarInputs() = delete;

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.delta_value() + rhs.delta_value());
        }
    };

    struct SumNestedPair
    {
        SumNestedPair() = delete;
        ~SumNestedPair() = delete;

        using Pair = PairSchema;

        static void eval(In<"pair", Pair, InputActivity::Passive, InputValidity::Unchecked> pair, Out<TS<int>> out)
        {
            const int lhs = pair.view().field("lhs").value().as_atomic().as<int>();
            const int rhs = pair.view().field("rhs").value().as_atomic().as<int>();
            out.set(lhs + rhs);
        }
    };

    // Simple downstream compute node used by the push-source tests. It proves
    // that once a push message is applied to node 0, the rest of the graph
    // sees the result through the ordinary TS input/output path.
    struct PassThroughInput
    {
        PassThroughInput() = delete;
        ~PassThroughInput() = delete;

        static void eval(In<"ts", TS<int>> ts, Out<TS<int>> out)
        {
            out.set(ts.value());
        }
    };

    // Minimal push-source example.
    //
    // Push-source nodes receive external messages through the realtime engine's
    // push-message receiver:
    //   engine.push_message_receiver()->enqueue({target_node_index, Value{payload}})
    //
    // The queued Value payload is decoded and passed to apply_message(...) as
    // the typed `value` argument below. Returning false tells the graph that
    // the node could not consume the message yet, so the same payload should be
    // requeued and retried on the next push scheduling pass.
    struct PushEchoNode
    {
        PushEchoNode() = delete;
        ~PushEchoNode() = delete;

        static void eval() {}

        static bool apply_message(int value, Out<TS<int>> out)
        {
            if (value < 0) { return false; }
            out.set(value);
            return true;
        }
    };

    // Minimal pull-source example. Unlike PushEchoNode, this node never
    // receives work from the realtime push-message receiver; it only runs when
    // the graph schedules it for a time-based evaluation.
    struct PullTickNode
    {
        PullTickNode() = delete;
        ~PullTickNode() = delete;

        static constexpr auto node_type = NodeTypeEnum::PULL_SOURCE_NODE;

        static void eval(Out<TS<int>> out)
        {
            out.set(42);
        }
    };

    struct ExportedSumNode
    {
        ExportedSumNode() = delete;
        ~ExportedSumNode() = delete;

        static constexpr auto name = "exported_sum";

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct PolicyMetadataNode
    {
        PolicyMetadataNode() = delete;
        ~PolicyMetadataNode() = delete;

        static void eval(In<"lhs", TS<int>> lhs,
                         In<"rhs", TS<int>, InputActivity::Passive, InputValidity::Unchecked> rhs,
                         In<"strict", TS<int>, InputActivity::Active, InputValidity::AllValid> strict,
                         Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value() + strict.value());
        }
    };

    struct GenericGetItemNode
    {
        GenericGetItemNode() = delete;
        ~GenericGetItemNode() = delete;

        static constexpr auto name = "generic_get_item";

        using K = ScalarVar<"K">;
        using V = TsVar<"V">;

        static void eval(In<"ts", TSD<K, V>> ts, In<"key", TS<K>> key, Out<V> out)
        {
            static_cast<void>(ts);
            static_cast<void>(key);
            static_cast<void>(out);
        }
    };

    struct SumWithTypedState
    {
        SumWithTypedState() = delete;
        ~SumWithTypedState() = delete;

        static constexpr auto name = "typed_state_sum";

        static void start(State<int> state)
        {
            state.view().set_scalar(0);
        }

        static void eval(In<"lhs", TS<int>> lhs, State<int> state, Out<TS<int>> out)
        {
            auto &sum = state.view().template checked_as<int>();
            sum += lhs.value();
            out.set(sum);
        }
    };

    struct PreviousValueFromRecordableState
    {
        PreviousValueFromRecordableState() = delete;
        ~PreviousValueFromRecordableState() = delete;

        static constexpr auto name = "previous_value";
        using LocalState = TSB<Field<"last", TS<int>>>;

        static void eval(In<"lhs", TS<int>> lhs, RecordableState<LocalState> state, Out<TS<int>> out)
        {
            auto last = state.view().field("last");
            if (last.valid()) {
                out.set(last.value().as_atomic().as<int>());
            } else {
                out.set(-1);
            }
            last.value().set_scalar(lhs.value());
            state.mark_modified(last);
        }
    };

    struct ClockCaptureNode
    {
        ClockCaptureNode() = delete;
        ~ClockCaptureNode() = delete;

        static inline engine_time_t seen_evaluation_time{MIN_DT};
        static inline engine_time_t seen_next_cycle_time{MIN_DT};
        static inline bool saw_now_at_or_after_evaluation_time{false};
        static inline bool saw_non_negative_cycle_time{false};

        static void reset()
        {
            seen_evaluation_time = MIN_DT;
            seen_next_cycle_time = MIN_DT;
            saw_now_at_or_after_evaluation_time = false;
            saw_non_negative_cycle_time = false;
        }

        static void eval(In<"lhs", TS<int>> lhs, EvaluationClock clock, Out<TS<int>> out)
        {
            seen_evaluation_time = clock.evaluation_time();
            seen_next_cycle_time = clock.next_cycle_evaluation_time();
            saw_now_at_or_after_evaluation_time = clock.now() >= clock.evaluation_time();
            saw_non_negative_cycle_time = clock.cycle_time() >= engine_time_delta_t::zero();
            out.set(lhs.value());
        }
    };

    struct ThrowOnEval
    {
        ThrowOnEval() = delete;
        ~ThrowOnEval() = delete;

        static void eval(In<"lhs", TS<int>> lhs)
        {
            static_cast<void>(lhs);
            throw std::runtime_error("v2 test eval failure");
        }
    };

    struct StopThrowsNode
    {
        StopThrowsNode() = delete;
        ~StopThrowsNode() = delete;

        static inline int stop_calls{0};

        static void reset()
        {
            stop_calls = 0;
        }

        static void stop()
        {
            ++stop_calls;
            throw std::runtime_error("v2 test stop failure");
        }

        static void eval() {}
    };

    struct StopTracksNode
    {
        StopTracksNode() = delete;
        ~StopTracksNode() = delete;

        static inline int stop_calls{0};

        static void reset()
        {
            stop_calls = 0;
        }

        static void stop()
        {
            ++stop_calls;
        }

        static void eval() {}
    };

    struct StartThrowsNode
    {
        StartThrowsNode() = delete;
        ~StartThrowsNode() = delete;

        static inline int start_calls{0};
        static inline int stop_calls{0};

        static void reset()
        {
            start_calls = 0;
            stop_calls = 0;
        }

        static void start()
        {
            ++start_calls;
            throw std::runtime_error("v2 test start failure");
        }

        static void stop()
        {
            ++stop_calls;
        }

        static void eval() {}
    };

    struct StartTracksNode
    {
        StartTracksNode() = delete;
        ~StartTracksNode() = delete;

        static inline int start_calls{0};
        static inline int stop_calls{0};

        static void reset()
        {
            start_calls = 0;
            stop_calls = 0;
        }

        static void start()
        {
            ++start_calls;
        }

        static void stop()
        {
            ++stop_calls;
        }

        static void eval() {}
    };

    struct CountingObserver : EvaluationLifeCycleObserver
    {
        int before_start_graph{0};
        int after_start_graph{0};
        int before_start_node{0};
        int after_start_node{0};
        int before_graph_evaluation{0};
        int after_graph_evaluation{0};
        int before_node_evaluation{0};
        int after_node_evaluation{0};
        int before_stop_graph{0};
        int after_stop_graph{0};
        int before_stop_node{0};
        int after_stop_node{0};
        int after_push_nodes_evaluation{0};

        void on_before_start_graph(Graph &) override
        {
            ++before_start_graph;
        }

        void on_after_start_graph(Graph &) override
        {
            ++after_start_graph;
        }

        void on_before_start_node(Node &) override
        {
            ++before_start_node;
        }

        void on_after_start_node(Node &) override
        {
            ++after_start_node;
        }

        void on_before_graph_evaluation(Graph &) override
        {
            ++before_graph_evaluation;
        }

        void on_after_graph_evaluation(Graph &) override
        {
            ++after_graph_evaluation;
        }

        void on_before_node_evaluation(Node &) override
        {
            ++before_node_evaluation;
        }

        void on_after_node_evaluation(Node &) override
        {
            ++after_node_evaluation;
        }

        void on_before_stop_graph(Graph &) override
        {
            ++before_stop_graph;
        }

        void on_after_stop_graph(Graph &) override
        {
            ++after_stop_graph;
        }

        void on_before_stop_node(Node &) override
        {
            ++before_stop_node;
        }

        void on_after_stop_node(Node &) override
        {
            ++after_stop_node;
        }

        void on_after_graph_push_nodes_evaluation(Graph &) override
        {
            ++after_push_nodes_evaluation;
        }
    };

    [[nodiscard]] engine_time_t tick(int offset)
    {
        return MIN_DT + std::chrono::microseconds{offset};
    }

    [[nodiscard]] engine_time_t utc_now_tick()
    {
        return std::chrono::time_point_cast<std::chrono::microseconds>(engine_clock::now());
    }

    [[nodiscard]] EvaluationEngine make_engine(GraphBuilder graph_builder,
                                               engine_time_t start_time,
                                               engine_time_t end_time = MAX_DT,
                                               EvaluationMode evaluation_mode = EvaluationMode::SIMULATION)
    {
        return EvaluationEngineBuilder{}
            .graph_builder(std::move(graph_builder))
            .evaluation_mode(evaluation_mode)
            .start_time(start_time)
            .end_time(end_time)
            .build();
    }

    [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, int64_t slot)
    {
        if (slot < 0) { throw std::out_of_range("v2 test path navigation requires non-negative slots"); }

        switch (schema.kind) {
            case TSKind::TSB:
                if (static_cast<size_t>(slot) >= schema.field_count()) {
                    throw std::out_of_range("v2 test TSB path navigation is out of range");
                }
                return schema.fields()[slot].ts_type;

            case TSKind::TSL:
                if (schema.fixed_size() == 0) {
                    throw std::invalid_argument("v2 test path navigation does not support dynamic TSL prefixes");
                }
                if (static_cast<size_t>(slot) >= schema.fixed_size()) {
                    throw std::out_of_range("v2 test TSL path navigation is out of range");
                }
                return schema.element_ts();

            default:
                throw std::invalid_argument("v2 test output navigation only supports TSB and fixed-size TSL");
        }
    }

    [[nodiscard]] TSOutputView traverse_output(TSOutputView view, const TSMeta *schema, PathView path)
    {
        const TSMeta *current_schema = schema;
        for (const int64_t slot : path) {
            if (current_schema == nullptr) { throw std::invalid_argument("v2 test output navigation requires a schema"); }

            switch (current_schema->kind) {
                case TSKind::TSB:
                    view = view.as_bundle()[slot];
                    break;

                case TSKind::TSL:
                    view = view.as_list()[slot];
                    break;

                default:
                    throw std::invalid_argument("v2 test output navigation only supports TSB and fixed-size TSL");
            }

            current_schema = child_schema_at(*current_schema, slot);
        }

        return view;
    }

    template <typename T>
    void publish_scalar_output(Node &node, const Path &path, T &&value, engine_time_t modified_time)
    {
        auto output = traverse_output(node.output_view(modified_time), node.output_schema(), path);
        output.value().set_scalar(std::forward<T>(value));

        LinkedTSContext context = output.linked_context();
        if (context.ts_state == nullptr) { throw std::logic_error("v2 test output leaf has no state to mark modified"); }
        context.ts_state->mark_modified(modified_time);
    }

    void ensure_python_hgraph_importable()
    {
        setenv("HGRAPH_USE_CPP", "0", 1);

        if (!Py_IsInitialized()) {
            PyConfig config;
            PyConfig_InitPythonConfig(&config);

            const auto throw_if_status_error = [&](PyStatus status, const char *action) {
                if (!PyStatus_Exception(status)) { return; }

                std::string message = action;
                if (status.err_msg != nullptr) {
                    message += ": ";
                    message += status.err_msg;
                }
                PyConfig_Clear(&config);
                throw std::runtime_error(message);
            };

#ifdef HGRAPH_TEST_PYTHON_EXECUTABLE
            throw_if_status_error(
                PyConfig_SetBytesString(&config, &config.program_name, HGRAPH_TEST_PYTHON_EXECUTABLE),
                "failed to configure Python executable");

            const auto python_home = std::filesystem::path{HGRAPH_TEST_PYTHON_HOME}.string();
            throw_if_status_error(
                PyConfig_SetBytesString(&config, &config.home, python_home.c_str()),
                "failed to configure Python home");
#endif

            const PyStatus status = Py_InitializeFromConfig(&config);
            if (PyStatus_Exception(status)) {
                std::string message = "failed to initialise Python";
                if (status.err_msg != nullptr) {
                    message += ": ";
                    message += status.err_msg;
                }
                PyConfig_Clear(&config);
                throw std::runtime_error(message);
            }

            PyConfig_Clear(&config);
        }

        nb::gil_scoped_acquire guard;
        nb::module_ sys = nb::module_::import_("sys");
        nb::list path = nb::borrow<nb::list>(sys.attr("path"));
        path.insert(0, "/Users/hhenson/CLionProjects/hgraph_2");
    }
}  // namespace hgraph::v2::test_detail

TEST_CASE("v2 graph wires scalar sources into a compute node with validity gating", "[v2][graph]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *input_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Inputs");

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("rhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sum").implementation<hgraph::v2::test_detail::SumScalarInputs>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::v2::Edge{.src_node = 1, .dst_node = 2, .input_path = {1}});

    auto engine = hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 2, hgraph::v2::test_detail::tick(1));
    CHECK(graph.scheduled_time(2) == hgraph::v2::test_detail::tick(1));
    CHECK(graph.node_at(0).output_view(hgraph::v2::test_detail::tick(1)).modified());
    CHECK_FALSE(graph.node_at(0).output_view(hgraph::v2::test_detail::tick(2)).modified());

    graph.evaluate(hgraph::v2::test_detail::tick(1));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 0);

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(1), {}, 3, hgraph::v2::test_detail::tick(2));
    CHECK(graph.scheduled_time(2) == hgraph::v2::test_detail::tick(2));

    graph.evaluate(hgraph::v2::test_detail::tick(2));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 5);
    CHECK(graph.node_at(2).output_view(hgraph::v2::test_detail::tick(2)).modified());
    CHECK(graph.scheduled_time(2) == hgraph::v2::test_detail::tick(2));
}

TEST_CASE("v2 graph binds bundle output child paths into nested input paths", "[v2][graph]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
    const auto *nested_input_schema = ts_registry.tsb({{"pair", pair_schema}}, "NestedInput");

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("pair_source").output_schema(pair_schema).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sum").input_schema(nested_input_schema).implementation<hgraph::v2::test_detail::SumNestedPair>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .output_path = {0}, .dst_node = 1, .input_path = {0, 0}})
        .add_edge(hgraph::v2::Edge{.src_node = 0, .output_path = {1}, .dst_node = 1, .input_path = {0, 1}});

    auto engine = hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {0}, 7, hgraph::v2::test_detail::tick(11));
    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {1}, 13, hgraph::v2::test_detail::tick(11));

    graph.schedule_node(1, hgraph::v2::test_detail::tick(11));
    graph.evaluate(hgraph::v2::test_detail::tick(11));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 20);
}

TEST_CASE("v2 graph drains push source messages before normal scheduled evaluation", "[v2][graph][push]")
{
    using namespace std::chrono_literals;

    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto start_time = hgraph::v2::test_detail::utc_now_tick();
    hgraph::v2::test_detail::CountingObserver observer;

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}
                      .label("push_source")
                      .node_type(hgraph::v2::NodeTypeEnum::PUSH_SOURCE_NODE)
                      .output_schema(scalar_ts)
                      .implementation<hgraph::v2::test_detail::PushEchoNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sink").implementation<hgraph::v2::test_detail::PassThroughInput>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::v2::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .evaluation_mode(hgraph::v2::EvaluationMode::REAL_TIME)
                      .start_time(start_time)
                      .end_time(start_time + 1s)
                      .add_life_cycle_observer(&observer)
                      .build();
    auto &graph = engine.graph();
    graph.start();

    {
        nb::gil_scoped_acquire guard;
        auto clock = graph.engine_evaluation_clock();
        auto *receiver = engine.push_message_receiver();
        REQUIRE(receiver != nullptr);
        clock.update_next_scheduled_evaluation_time(hgraph::v2::test_detail::utc_now_tick() + 250ms);
        // Queue one external message for push-source node 0. The payload is the
        // integer that PushEchoNode::apply_message(...) receives as `value`.
        receiver->enqueue({0, hgraph::value::Value{7}});
        clock.advance_to_next_scheduled_time();
        const auto when = clock.evaluation_time();
        graph.evaluate(when);
    }

    const auto when = graph.last_evaluation_time();

    CHECK(graph.push_source_nodes_end() == 1);
    CHECK(observer.after_push_nodes_evaluation == 1);
    CHECK(graph.node_at(0).output_view(when).modified());
    CHECK(graph.scheduled_time(1) == when);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 7);
}

TEST_CASE("v2 push source message application requeues failed messages", "[v2][graph][push]")
{
    using namespace std::chrono_literals;

    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto start_time = hgraph::v2::test_detail::utc_now_tick();
    hgraph::v2::test_detail::CountingObserver observer;

    hgraph::v2::GraphBuilder builder;
    builder.add_node(hgraph::v2::NodeBuilder{}
                         .label("push_source")
                         .node_type(hgraph::v2::NodeTypeEnum::PUSH_SOURCE_NODE)
                         .output_schema(scalar_ts)
                         .implementation<hgraph::v2::test_detail::PushEchoNode>());

    auto engine = hgraph::v2::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .evaluation_mode(hgraph::v2::EvaluationMode::REAL_TIME)
                      .start_time(start_time)
                      .end_time(start_time + 1s)
                      .add_life_cycle_observer(&observer)
                      .build();
    auto &graph = engine.graph();
    graph.start();

    {
        nb::gil_scoped_acquire guard;
        auto clock = graph.engine_evaluation_clock();
        auto *receiver = engine.push_message_receiver();
        REQUIRE(receiver != nullptr);
        clock.update_next_scheduled_evaluation_time(hgraph::v2::test_detail::utc_now_tick() + 250ms);
        // Negative values make PushEchoNode reject the message. The graph
        // should preserve the same payload by requeueing it at the front.
        receiver->enqueue({0, hgraph::value::Value{-1}});
        clock.advance_to_next_scheduled_time();
        graph.evaluate(clock.evaluation_time());
    }

    CHECK(observer.after_push_nodes_evaluation == 1);
    CHECK(static_cast<bool>(*engine.push_message_receiver()));
    CHECK(graph.engine_evaluation_clock().push_node_requires_scheduling());
}

TEST_CASE("v2 stopped receivers ignore enqueues instead of throwing", "[v2][graph][push]")
{
    hgraph::v2::SenderReceiverState receiver;
    receiver.mark_stopped();

    CHECK_NOTHROW(receiver.enqueue({0, hgraph::value::Value{7}}));
    CHECK_NOTHROW(receiver.enqueue_front({0, hgraph::value::Value{11}}));
    CHECK(receiver.stopped());
    CHECK_FALSE(static_cast<bool>(receiver));
    CHECK_FALSE(receiver.dequeue().has_value());
}

TEST_CASE("v2 node builder rejects push source implementations without apply_message", "[v2][graph][push]")
{
    hgraph::v2::NodeBuilder push_node_then_impl;
    CHECK_THROWS_AS(
        push_node_then_impl.node_type(hgraph::v2::NodeTypeEnum::PUSH_SOURCE_NODE).implementation<hgraph::v2::test_detail::NoopNode>(),
        std::logic_error);

    hgraph::v2::NodeBuilder impl_then_push_node;
    impl_then_push_node.implementation<hgraph::v2::test_detail::NoopNode>();
    CHECK_THROWS_AS(impl_then_push_node.node_type(hgraph::v2::NodeTypeEnum::PUSH_SOURCE_NODE), std::logic_error);
}

TEST_CASE("v2 graph builder rejects nodes without implementations when added", "[v2][graph][builder]")
{
    hgraph::v2::GraphBuilder builder;
    CHECK_THROWS_AS(builder.add_node(hgraph::v2::NodeBuilder{}.label("incomplete")), std::invalid_argument);
}

TEST_CASE("v2 simulation engines reject push source graphs at build time", "[v2][engine][push]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::GraphBuilder builder;
    builder.add_node(hgraph::v2::NodeBuilder{}
                         .label("push_source")
                         .node_type(hgraph::v2::NodeTypeEnum::PUSH_SOURCE_NODE)
                         .output_schema(scalar_ts)
                         .implementation<hgraph::v2::test_detail::PushEchoNode>());

    CHECK_THROWS_AS(
        hgraph::v2::EvaluationEngineBuilder{}
            .graph_builder(std::move(builder))
            .evaluation_mode(hgraph::v2::EvaluationMode::SIMULATION)
            .start_time(hgraph::v2::test_detail::tick(0))
            .end_time(hgraph::v2::test_detail::tick(1))
            .build(),
        std::logic_error);
}

TEST_CASE("v2 pull source nodes remain on the normal scheduled evaluation path", "[v2][graph][source]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::GraphBuilder builder;
    builder.add_node(hgraph::v2::NodeBuilder{}
                         .label("pull_source")
                         .output_schema(scalar_ts)
                         .implementation<hgraph::v2::test_detail::PullTickNode>());

    auto engine = hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(240));
    auto &graph = engine.graph();
    graph.start();

    CHECK(graph.push_source_nodes_end() == 0);
    graph.schedule_node(0, hgraph::v2::test_detail::tick(241));
    graph.evaluate(hgraph::v2::test_detail::tick(241));
    CHECK(graph.node_at(0).output_view().value().as_atomic().as<int>() == 42);
}

TEST_CASE("v2 nodes support typed local state", "[v2][graph][state]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sum").implementation<hgraph::v2::test_detail::SumWithTypedState>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 5, hgraph::v2::test_detail::tick(31));
    graph.evaluate(hgraph::v2::test_detail::tick(31));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 5);

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::v2::test_detail::tick(32));
    graph.evaluate(hgraph::v2::test_detail::tick(32));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 12);
}

TEST_CASE("v2 nodes support recordable state", "[v2][graph][state]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("previous").implementation<hgraph::v2::test_detail::PreviousValueFromRecordableState>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 11, hgraph::v2::test_detail::tick(41));
    graph.evaluate(hgraph::v2::test_detail::tick(41));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == -1);

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 13, hgraph::v2::test_detail::tick(42));
    graph.evaluate(hgraph::v2::test_detail::tick(42));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 11);
}

TEST_CASE("v2 nodes support evaluation clock injection", "[v2][graph][clock]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::test_detail::ClockCaptureNode::reset();

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("clock_capture").implementation<hgraph::v2::test_detail::ClockCaptureNode>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::v2::test_detail::tick(51));
    graph.evaluate(hgraph::v2::test_detail::tick(51));

    CHECK(hgraph::v2::test_detail::ClockCaptureNode::seen_evaluation_time == hgraph::v2::test_detail::tick(51));
    CHECK(hgraph::v2::test_detail::ClockCaptureNode::seen_next_cycle_time == hgraph::v2::test_detail::tick(51) + hgraph::MIN_TD);
    CHECK(hgraph::v2::test_detail::ClockCaptureNode::saw_now_at_or_after_evaluation_time);
    CHECK(hgraph::v2::test_detail::ClockCaptureNode::saw_non_negative_cycle_time);
}

TEST_CASE("v2 evaluation engine builder produces a runnable simulation engine", "[v2][engine]")
{
    hgraph::v2::GraphBuilder builder;
    auto engine =
        hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(60), hgraph::v2::test_detail::tick(70));
    auto api = engine.graph().evaluation_engine_api();

    int before_count = 0;
    int after_count = 0;

    api.add_before_evaluation_notification([&] {
        ++before_count;
        if (before_count == 1) { api.add_before_evaluation_notification([&] { ++before_count; }); }
    });
    api.add_after_evaluation_notification([&] {
        ++after_count;
        if (after_count == 1) { api.add_after_evaluation_notification([&] { ++after_count; }); }
    });

    CHECK(engine.evaluation_mode() == hgraph::v2::EvaluationMode::SIMULATION);
    CHECK(engine.start_time() == hgraph::v2::test_detail::tick(60));
    CHECK(engine.end_time() == hgraph::v2::test_detail::tick(70));
    CHECK(engine.graph().engine_evaluation_clock().evaluation_time() == hgraph::v2::test_detail::tick(60));

    engine.run();
    CHECK(before_count == 2);
    CHECK(after_count == 2);
    CHECK(engine.graph().engine_evaluation_clock().evaluation_time() == hgraph::v2::test_detail::tick(71));
}

TEST_CASE("v2 graph scheduling matches current graph overwrite rules", "[v2][graph][schedule]")
{
    hgraph::v2::GraphBuilder builder;
    builder.add_node(hgraph::v2::NodeBuilder{}.label("node").implementation<hgraph::v2::test_detail::NoopNode>());

    auto engine =
        hgraph::v2::test_detail::make_engine(std::move(builder), hgraph::v2::test_detail::tick(80), hgraph::v2::test_detail::tick(90));
    auto &graph = engine.graph();

    graph.schedule_node(0, hgraph::v2::test_detail::tick(80));
    CHECK(graph.scheduled_time(0) == hgraph::v2::test_detail::tick(80));

    graph.schedule_node(0, hgraph::v2::test_detail::tick(85));
    CHECK(graph.scheduled_time(0) == hgraph::v2::test_detail::tick(85));

    graph.schedule_node(0, hgraph::v2::test_detail::tick(88), true);
    CHECK(graph.scheduled_time(0) == hgraph::v2::test_detail::tick(88));
}

TEST_CASE("v2 graph notifies after evaluation even when node evaluation throws", "[v2][graph][engine]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::test_detail::CountingObserver observer;

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("throws").implementation<hgraph::v2::test_detail::ThrowOnEval>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::v2::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .start_time(hgraph::v2::test_detail::tick(100))
                      .end_time(hgraph::v2::test_detail::tick(110))
                      .add_life_cycle_observer(&observer)
                      .build();
    auto &graph = engine.graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::v2::test_detail::tick(101));

    CHECK_THROWS_AS(graph.evaluate(hgraph::v2::test_detail::tick(101)), std::runtime_error);
    CHECK(graph.last_evaluation_time() == hgraph::v2::test_detail::tick(101));
    CHECK(observer.before_graph_evaluation == 1);
    CHECK(observer.after_graph_evaluation == 1);
    CHECK(observer.before_node_evaluation == 1);
    CHECK(observer.after_node_evaluation == 1);
}

TEST_CASE("v2 graph stop continues through node failures before rethrowing", "[v2][graph][lifecycle]")
{
    hgraph::v2::test_detail::StopThrowsNode::reset();
    hgraph::v2::test_detail::StopTracksNode::reset();
    hgraph::v2::test_detail::CountingObserver observer;

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("throws_on_stop").implementation<hgraph::v2::test_detail::StopThrowsNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("tracks_stop").implementation<hgraph::v2::test_detail::StopTracksNode>());

    auto engine = hgraph::v2::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .start_time(hgraph::v2::test_detail::tick(120))
                      .end_time(hgraph::v2::test_detail::tick(130))
                      .add_life_cycle_observer(&observer)
                      .build();
    auto &graph = engine.graph();
    graph.start();

    CHECK_THROWS_AS(graph.stop(), std::runtime_error);
    CHECK(hgraph::v2::test_detail::StopThrowsNode::stop_calls == 1);
    CHECK(hgraph::v2::test_detail::StopTracksNode::stop_calls == 1);
    CHECK(observer.before_stop_graph == 1);
    CHECK(observer.after_stop_graph == 1);
    CHECK(observer.before_stop_node == 2);
    CHECK(observer.after_stop_node == 2);
}

TEST_CASE("v2 graph start rolls back started nodes when startup fails", "[v2][graph][lifecycle]")
{
    hgraph::v2::test_detail::StartThrowsNode::reset();
    hgraph::v2::test_detail::StartTracksNode::reset();
    hgraph::v2::test_detail::CountingObserver observer;

    {
        hgraph::v2::GraphBuilder builder;
        builder
            .add_node(hgraph::v2::NodeBuilder{}.label("tracks_start").implementation<hgraph::v2::test_detail::StartTracksNode>())
            .add_node(hgraph::v2::NodeBuilder{}.label("throws_on_start").implementation<hgraph::v2::test_detail::StartThrowsNode>());

        auto engine = hgraph::v2::EvaluationEngineBuilder{}
                          .graph_builder(std::move(builder))
                          .start_time(hgraph::v2::test_detail::tick(140))
                          .end_time(hgraph::v2::test_detail::tick(150))
                          .add_life_cycle_observer(&observer)
                          .build();
        auto &graph = engine.graph();

        CHECK_THROWS_AS(graph.start(), std::runtime_error);
        CHECK_NOTHROW(graph.stop());
        CHECK(hgraph::v2::test_detail::StartTracksNode::start_calls == 1);
        CHECK(hgraph::v2::test_detail::StartTracksNode::stop_calls == 1);
        CHECK(hgraph::v2::test_detail::StartThrowsNode::start_calls == 1);
        CHECK(hgraph::v2::test_detail::StartThrowsNode::stop_calls == 0);
        CHECK(observer.before_start_graph == 1);
        CHECK(observer.after_start_graph == 0);
        CHECK(observer.before_start_node == 2);
        CHECK(observer.after_start_node == 1);
        CHECK(observer.before_stop_graph == 1);
        CHECK(observer.after_stop_graph == 1);
        CHECK(observer.before_stop_node == 1);
        CHECK(observer.after_stop_node == 1);
    }

    CHECK(hgraph::v2::test_detail::StartTracksNode::stop_calls == 1);
    CHECK(hgraph::v2::test_detail::StartThrowsNode::stop_calls == 0);
}

TEST_CASE("v2 real-time engine clock advances to scheduled wall-clock time", "[v2][engine][realtime]")
{
    using namespace std::chrono_literals;

    hgraph::v2::GraphBuilder builder;
    const auto start_time = hgraph::v2::test_detail::utc_now_tick();
    auto engine = hgraph::v2::test_detail::make_engine(
        std::move(builder), start_time, start_time + 500ms, hgraph::v2::EvaluationMode::REAL_TIME);

    auto clock = engine.graph().engine_evaluation_clock();
    const auto scheduled_time = hgraph::v2::test_detail::utc_now_tick() + 30ms;

    clock.update_next_scheduled_evaluation_time(scheduled_time);
    clock.advance_to_next_scheduled_time();

    const auto after = hgraph::v2::test_detail::utc_now_tick();
    CHECK(clock.evaluation_time() >= scheduled_time);
    CHECK(clock.evaluation_time() <= after);
}

TEST_CASE("v2 real-time engine clock wakes early when push scheduling is requested", "[v2][engine][realtime]")
{
    using namespace std::chrono_literals;

    hgraph::v2::GraphBuilder builder;
    const auto start_time = hgraph::v2::test_detail::utc_now_tick();
    auto engine = hgraph::v2::test_detail::make_engine(
        std::move(builder), start_time, start_time + 1s, hgraph::v2::EvaluationMode::REAL_TIME);

    auto clock = engine.graph().engine_evaluation_clock();
    const auto scheduled_time = hgraph::v2::test_detail::utc_now_tick() + 250ms;
    clock.update_next_scheduled_evaluation_time(scheduled_time);

    const auto before = hgraph::v2::test_detail::utc_now_tick();
    std::thread notifier([clock] {
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        clock.mark_push_node_requires_scheduling();
    });

    clock.advance_to_next_scheduled_time();
    notifier.join();

    const auto after = hgraph::v2::test_detail::utc_now_tick();
    CHECK(after < scheduled_time);
    CHECK(clock.evaluation_time() < scheduled_time);
    CHECK(clock.evaluation_time() >= before);
    CHECK(clock.push_node_requires_scheduling());
    clock.reset_push_node_requires_scheduling();
    CHECK_FALSE(clock.push_node_requires_scheduling());
}

TEST_CASE("v2 static node signatures export Python wiring metadata", "[v2][python]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    nb::gil_scoped_acquire guard;
    nb::object signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::ExportedSumNode>::wiring_signature();

    CHECK(nb::cast<std::string>(signature.attr("name")) == "exported_sum");
    CHECK(nb::cast<std::string>(signature.attr("signature")) == "exported_sum(lhs: TS[int], rhs: TS[int]) -> TS[int]");
    CHECK(nb::len(nb::cast<nb::object>(signature.attr("args"))) == 2);
}

TEST_CASE("v2 static node signatures derive selector policies from In metadata", "[v2][signature]")
{
    using signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::PolicyMetadataNode>;

    CHECK(signature::active_input_names() == std::vector<std::string>{"lhs", "strict"});
    CHECK(signature::valid_input_names() == std::vector<std::string>{"lhs"});
    CHECK(signature::all_valid_input_names() == std::vector<std::string>{"strict"});
}

TEST_CASE("v2 static node signatures export linked template type variables", "[v2][python][signature]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    using signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::GenericGetItemNode>;

    CHECK(signature::input_schema() == nullptr);
    CHECK(signature::output_schema() == nullptr);
    CHECK(signature::unresolved_input_names() == std::vector<std::string>{"ts", "key"});

    nb::gil_scoped_acquire guard;
    nb::object wiring_signature = signature::wiring_signature();
    const auto unresolved_args = nb::cast<std::unordered_set<std::string>>(wiring_signature.attr("unresolved_args"));

    CHECK(nb::cast<std::string>(wiring_signature.attr("signature")) == "generic_get_item(ts: TSD[K, V], key: TS[K]) -> V");
    CHECK(unresolved_args.contains("ts"));
    CHECK(unresolved_args.contains("key"));
}

TEST_CASE("v2 static node signatures export declared node type metadata", "[v2][python][signature]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    nb::gil_scoped_acquire guard;
    nb::object signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::PullTickNode>::wiring_signature();
    nb::object expected =
        nb::module_::import_("hgraph._wiring._wiring_node_signature").attr("WiringNodeType").attr("PULL_SOURCE_NODE");

    CHECK(nb::cast<int>(signature.attr("node_type").attr("value")) == nb::cast<int>(expected.attr("value")));
}

TEST_CASE("v2 static node signatures export state injectables", "[v2][python][signature]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    using typed_state_signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::SumWithTypedState>;
    using recordable_state_signature =
        hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::PreviousValueFromRecordableState>;

    CHECK(typed_state_signature::has_state());
    CHECK_FALSE(typed_state_signature::has_recordable_state());
    CHECK(recordable_state_signature::has_recordable_state());

    nb::gil_scoped_acquire guard;
    nb::object state_signature = typed_state_signature::wiring_signature();
    nb::object recordable_signature = recordable_state_signature::wiring_signature();

    const auto state_args = nb::cast<std::vector<std::string>>(state_signature.attr("args"));
    const auto recordable_args = nb::cast<std::vector<std::string>>(recordable_signature.attr("args"));

    CHECK(state_args == std::vector<std::string>{"lhs", "_state"});
    CHECK(recordable_args == std::vector<std::string>{"lhs", "_recordable_state"});
    CHECK(nb::cast<bool>(state_signature.attr("uses_state")));
    CHECK(nb::cast<bool>(recordable_signature.attr("uses_recordable_state")));
}

TEST_CASE("v2 static node signatures export evaluation clock injectables", "[v2][python][signature]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    using signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::ClockCaptureNode>;

    CHECK(signature::has_clock());

    nb::gil_scoped_acquire guard;
    nb::object wiring_signature = signature::wiring_signature();
    const auto args = nb::cast<std::vector<std::string>>(wiring_signature.attr("args"));

    CHECK(args == std::vector<std::string>{"lhs", "_clock"});
    CHECK(nb::cast<bool>(wiring_signature.attr("uses_clock")));
}
