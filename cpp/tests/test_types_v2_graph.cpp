#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/graph_builder.h>
#include <hgraph/types/python_export.h>
#include <hgraph/types/ref.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace hgraph::test_detail
{
    using PairSchema = TSB<Field<"lhs", TS<int>>, Field<"rhs", TS<int>>>;
    using RefPairSchema = TSB<Field<"lhs", REF<TS<int>>>, Field<"rhs", REF<TS<int>>>>;
    using RefWrappedPairSchema = TSB<Field<"lhs", REF<TS<int>>>, Field<"rhs", TS<int>>>;
    using PlainListSchema = TSL<TS<int>, 2>;
    using RefListSchema = TSL<REF<TS<int>>, 2>;
    using NestedWrappedListSchema = TSB<Field<"items", RefListSchema>, Field<"tail", TS<int>>>;
    using DictSchema = TSD<int, TS<int>>;

    static_assert(std::same_as<typename In<"pair", PairSchema>::view_type, TSBView<TSInputView>>);
    static_assert(std::same_as<typename Out<PairSchema>::view_type, TSBView<TSOutputView>>);
    static_assert(std::same_as<typename In<"dict", DictSchema>::view_type, TSDView<TSInputView>>);
    static_assert(std::same_as<typename Out<DictSchema>::view_type, TSDView<TSOutputView>>);
    static_assert(std::same_as<decltype(std::declval<const In<"lhs", TS<int>> &>().delta_value()), const int &>);
    static_assert(std::same_as<decltype(std::declval<const In<"pair", PairSchema> &>().delta_value()), View>);
    static_assert(In<"lhs", TS<int>, InputValidity::Unchecked>::activity == InputActivity::Active);
    static_assert(In<"lhs", TS<int>, InputValidity::Unchecked>::validity == InputValidity::Unchecked);
    static_assert(
        In<"lhs", TS<int>, InputValidity::Unchecked, InputActivity::Passive>::activity == InputActivity::Passive);
    static_assert(
        In<"lhs", TS<int>, InputValidity::Unchecked, InputActivity::Passive>::validity == InputValidity::Unchecked);

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

        static void eval(In<"pair", Pair, InputValidity::Unchecked, InputActivity::Passive> pair, Out<TS<int>> out)
        {
            const int lhs = pair.view().field("lhs").value().as_atomic().as<int>();
            const int rhs = pair.view().field("rhs").value().as_atomic().as<int>();
            out.set(lhs + rhs);
        }
    };

    struct SumPairInput
    {
        SumPairInput() = delete;
        ~SumPairInput() = delete;

        static void eval(In<"pair", PairSchema> pair, Out<TS<int>> out)
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
                         In<"strict", TS<int>, InputValidity::AllValid> strict,
                         Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value() + strict.value());
        }
    };

    struct ExplicitEmptyPolicyNode
    {
        ExplicitEmptyPolicyNode() = delete;
        ~ExplicitEmptyPolicyNode() = delete;

        static constexpr auto name = "explicit_empty_policy";

        static void eval(In<"lhs", TS<int>, InputValidity::Unchecked, InputActivity::Passive> lhs,
                         In<"rhs", TS<int>, InputValidity::Unchecked, InputActivity::Passive> rhs,
                         Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
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

    // Port of the simple scheduler test node from
    // hgraph_unit_tests/_runtime/test_scheduler.py::my_scheduler.
    struct SchedulerEchoNode
    {
        SchedulerEchoNode() = delete;
        ~SchedulerEchoNode() = delete;

        static constexpr auto name = "scheduler_echo";

        static void eval(In<"ts", TS<int>> ts, NodeScheduler &scheduler, Out<TS<int>> out)
        {
            if (ts.modified()) {
                scheduler.schedule(hgraph::MIN_TD * ts.value());
                out.set(ts.value());
            } else if (scheduler.is_scheduled_now()) {
                out.set(-1);
            }
        }
    };

    // Port of the tagged scheduler test node from
    // hgraph_unit_tests/_runtime/test_scheduler.py::schedule_bool.
    struct TaggedSchedulerBoolNode
    {
        TaggedSchedulerBoolNode() = delete;
        ~TaggedSchedulerBoolNode() = delete;

        static constexpr auto name = "tagged_scheduler_bool";

        static void eval(In<"ts", TS<bool>, InputValidity::Unchecked> ts,
                         In<"ts1", TS<int>> ts1,
                         NodeScheduler &scheduler,
                         Out<TS<bool>> out)
        {
            if (ts.modified() || ts1.modified()) {
                scheduler.schedule(hgraph::MIN_TD * ts1.value(), std::string{"TAG"});
                if (ts.modified()) { out.set(true); }
            } else if (scheduler.is_scheduled_now()) {
                out.set(false);
            }
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

    // Mirrors the PythonContextStubSourceNode startup pattern: kick the node
    // once during start so it runs in the initial evaluation cycle, without
    // opting into full scheduler semantics.
    struct StartupNotifyWithoutSchedulerNode
    {
        StartupNotifyWithoutSchedulerNode() = delete;
        ~StartupNotifyWithoutSchedulerNode() = delete;

        static inline int start_calls{0};
        static inline int eval_calls{0};
        static inline engine_time_t last_eval_time{MIN_DT};

        static void reset()
        {
            start_calls = 0;
            eval_calls = 0;
            last_eval_time = MIN_DT;
        }

        static void start(Node &node, engine_time_t evaluation_time)
        {
            ++start_calls;
            node.notify(evaluation_time + hgraph::MIN_TD * 5);
        }

        static void eval(EvaluationClock clock, Out<TS<int>> out)
        {
            ++eval_calls;
            last_eval_time = clock.evaluation_time();
            out.set(1);
        }
    };

    struct EmitEmptyRefNode
    {
        EmitEmptyRefNode() = delete;
        ~EmitEmptyRefNode() = delete;

        static void eval(In<"trigger", TS<bool>> trigger, Out<REF<TS<int>>> out)
        {
            static_cast<void>(trigger);
            out.set(TimeSeriesReference::make());
        }
    };

    struct RefPassthroughNode
    {
        RefPassthroughNode() = delete;
        ~RefPassthroughNode() = delete;

        static void eval(In<"ref", REF<TS<int>>> ref, Out<REF<TS<int>>> out)
        {
            out.set(ref.value());
        }
    };

    struct WrapInputAsRefNode
    {
        WrapInputAsRefNode() = delete;
        ~WrapInputAsRefNode() = delete;

        static void eval(In<"source", TS<int>> source, Out<REF<TS<int>>> out)
        {
            out.set(hgraph::TimeSeriesReference::make(source.view()));
        }
    };

    struct RefStateProbeNode
    {
        RefStateProbeNode() = delete;
        ~RefStateProbeNode() = delete;

        static void eval(In<"ref", REF<TS<int>>> ref, Out<TS<int>> out)
        {
            const auto &value = ref.value();
            if (!ref.valid()) {
                out.set(-1);
            } else if (value.is_empty()) {
                out.set(1);
            } else if (value.is_peered()) {
                out.set(2);
            } else {
                out.set(3);
            }
        }
    };

    struct DereferenceRefNode
    {
        DereferenceRefNode() = delete;
        ~DereferenceRefNode() = delete;

        static void eval(In<"ref", REF<TS<int>>> ref, EvaluationClock clock, Out<TS<int>> out)
        {
            const auto &value = ref.value();
            if (!value.is_peered()) { return; }
            out.set(value.target_view(clock.evaluation_time()).value().as_atomic().as<int>());
        }
    };

    struct SumWrappedPair
    {
        SumWrappedPair() = delete;
        ~SumWrappedPair() = delete;

        static void eval(In<"pair", RefWrappedPairSchema> pair, EvaluationClock clock, Out<TS<int>> out)
        {
            const auto lhs_ref = pair.view().field("lhs").value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
            const int lhs = lhs_ref.target_view(clock.evaluation_time()).value().as_atomic().as<int>();
            const int rhs = pair.view().field("rhs").value().as_atomic().as<int>();
            out.set(lhs + rhs);
        }
    };

    struct SumWrappedList
    {
        SumWrappedList() = delete;
        ~SumWrappedList() = delete;

        static void eval(In<"items", RefListSchema> items, EvaluationClock clock, Out<TS<int>> out)
        {
            int sum = 0;
            auto list = items.view().as_list();
            for (size_t i = 0; i < list.size(); ++i) {
                const auto ref = list[i].value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
                sum += ref.target_view(clock.evaluation_time()).value().as_atomic().as<int>();
            }
            out.set(sum);
        }
    };

    struct SumPlainList
    {
        SumPlainList() = delete;
        ~SumPlainList() = delete;

        static void eval(In<"items", PlainListSchema> items, Out<TS<int>> out)
        {
            int sum = 0;
            auto list = items.view().as_list();
            for (size_t i = 0; i < list.size(); ++i) { sum += list[i].value().as_atomic().as<int>(); }
            out.set(sum);
        }
    };

    struct SumNestedWrappedList
    {
        SumNestedWrappedList() = delete;
        ~SumNestedWrappedList() = delete;

        static void eval(In<"bundle", NestedWrappedListSchema> bundle, EvaluationClock clock, Out<TS<int>> out)
        {
            int sum = bundle.view().field("tail").value().as_atomic().as<int>();
            auto list = bundle.view().field("items").as_list();
            for (size_t i = 0; i < list.size(); ++i) {
                const auto ref = list[i].value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
                sum += ref.target_view(clock.evaluation_time()).value().as_atomic().as<int>();
            }
            out.set(sum);
        }
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
}  // namespace hgraph::test_detail

TEST_CASE("v2 graph wires scalar sources into a compute node with validity gating", "[v2][graph]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *input_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Inputs");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("sum").implementation<hgraph::test_detail::SumScalarInputs>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 1, .dst_node = 2, .input_path = {1}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 2, hgraph::test_detail::tick(1));
    CHECK(graph.scheduled_time(2) == hgraph::test_detail::tick(1));
    CHECK(graph.node_at(0).output_view(hgraph::test_detail::tick(1)).modified());
    CHECK_FALSE(graph.node_at(0).output_view(hgraph::test_detail::tick(2)).modified());

    graph.evaluate(hgraph::test_detail::tick(1));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 0);

    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 3, hgraph::test_detail::tick(2));
    CHECK(graph.scheduled_time(2) == hgraph::test_detail::tick(2));

    graph.evaluate(hgraph::test_detail::tick(2));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 5);
    CHECK(graph.node_at(2).output_view(hgraph::test_detail::tick(2)).modified());
    CHECK(graph.scheduled_time(2) == hgraph::test_detail::tick(2));
}

TEST_CASE("v2 graph binds bundle output child paths into nested input paths", "[v2][graph]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
    const auto *nested_input_schema = ts_registry.tsb({{"pair", pair_schema}}, "NestedInput");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("pair_source").output_schema(pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("sum").input_schema(nested_input_schema).implementation<hgraph::test_detail::SumNestedPair>())
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {0}, .dst_node = 1, .input_path = {0, 0}})
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {1}, .dst_node = 1, .input_path = {0, 1}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0}, 7, hgraph::test_detail::tick(11));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 13, hgraph::test_detail::tick(11));

    graph.schedule_node(1, hgraph::test_detail::tick(11));
    graph.evaluate(hgraph::test_detail::tick(11));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 20);
}

TEST_CASE("v2 graph drains push source messages before normal scheduled evaluation", "[v2][graph][push]")
{
    using namespace std::chrono_literals;

    hgraph::test_detail::ensure_python_hgraph_importable();

    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto start_time = hgraph::test_detail::utc_now_tick();
    hgraph::test_detail::CountingObserver observer;

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}
                      .label("push_source")
                      .node_type(hgraph::NodeTypeEnum::PUSH_SOURCE_NODE)
                      .output_schema(scalar_ts)
                      .implementation<hgraph::test_detail::PushEchoNode>())
        .add_node(hgraph::NodeBuilder{}.label("sink").implementation<hgraph::test_detail::PassThroughInput>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .evaluation_mode(hgraph::EvaluationMode::REAL_TIME)
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
        clock.update_next_scheduled_evaluation_time(hgraph::test_detail::utc_now_tick() + 250ms);
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

    hgraph::test_detail::ensure_python_hgraph_importable();

    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto start_time = hgraph::test_detail::utc_now_tick();
    hgraph::test_detail::CountingObserver observer;

    hgraph::GraphBuilder builder;
    builder.add_node(hgraph::NodeBuilder{}
                         .label("push_source")
                         .node_type(hgraph::NodeTypeEnum::PUSH_SOURCE_NODE)
                         .output_schema(scalar_ts)
                         .implementation<hgraph::test_detail::PushEchoNode>());

    auto engine = hgraph::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .evaluation_mode(hgraph::EvaluationMode::REAL_TIME)
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
        clock.update_next_scheduled_evaluation_time(hgraph::test_detail::utc_now_tick() + 250ms);
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
    hgraph::SenderReceiverState receiver;
    receiver.mark_stopped();

    CHECK_NOTHROW(receiver.enqueue({0, hgraph::value::Value{7}}));
    CHECK_NOTHROW(receiver.enqueue_front({0, hgraph::value::Value{11}}));
    CHECK(receiver.stopped());
    CHECK_FALSE(static_cast<bool>(receiver));
    CHECK_FALSE(receiver.dequeue().has_value());
}

TEST_CASE("v2 node builder rejects push source implementations without apply_message", "[v2][graph][push]")
{
    hgraph::NodeBuilder push_node_then_impl;
    CHECK_THROWS_AS(
        push_node_then_impl.node_type(hgraph::NodeTypeEnum::PUSH_SOURCE_NODE).implementation<hgraph::test_detail::NoopNode>(),
        std::logic_error);

    hgraph::NodeBuilder impl_then_push_node;
    impl_then_push_node.implementation<hgraph::test_detail::NoopNode>();
    CHECK_THROWS_AS(impl_then_push_node.node_type(hgraph::NodeTypeEnum::PUSH_SOURCE_NODE), std::logic_error);
}

TEST_CASE("v2 graph builder rejects nodes without implementations when added", "[v2][graph][builder]")
{
    hgraph::GraphBuilder builder;
    CHECK_THROWS_AS(builder.add_node(hgraph::NodeBuilder{}.label("incomplete")), std::invalid_argument);
}

TEST_CASE("v2 simulation engines reject push source graphs at build time", "[v2][engine][push]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder.add_node(hgraph::NodeBuilder{}
                         .label("push_source")
                         .node_type(hgraph::NodeTypeEnum::PUSH_SOURCE_NODE)
                         .output_schema(scalar_ts)
                         .implementation<hgraph::test_detail::PushEchoNode>());

    CHECK_THROWS_AS(
        hgraph::EvaluationEngineBuilder{}
            .graph_builder(std::move(builder))
            .evaluation_mode(hgraph::EvaluationMode::SIMULATION)
            .start_time(hgraph::test_detail::tick(0))
            .end_time(hgraph::test_detail::tick(1))
            .build(),
        std::logic_error);
}

TEST_CASE("v2 pull source nodes remain on the normal scheduled evaluation path", "[v2][graph][source]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder.add_node(hgraph::NodeBuilder{}
                         .label("pull_source")
                         .output_schema(scalar_ts)
                         .implementation<hgraph::test_detail::PullTickNode>());

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(240));
    auto &graph = engine.graph();
    graph.start();

    CHECK(graph.push_source_nodes_end() == 0);
    graph.schedule_node(0, hgraph::test_detail::tick(241));
    graph.evaluate(hgraph::test_detail::tick(241));
    CHECK(graph.node_at(0).output_view().value().as_atomic().as<int>() == 42);
}

TEST_CASE("v2 nodes support typed local state", "[v2][graph][state]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("sum").implementation<hgraph::test_detail::SumWithTypedState>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 5, hgraph::test_detail::tick(31));
    graph.evaluate(hgraph::test_detail::tick(31));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 5);

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::test_detail::tick(32));
    graph.evaluate(hgraph::test_detail::tick(32));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 12);
}

TEST_CASE("v2 nodes support recordable state", "[v2][graph][state]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("previous").implementation<hgraph::test_detail::PreviousValueFromRecordableState>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 11, hgraph::test_detail::tick(41));
    graph.evaluate(hgraph::test_detail::tick(41));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == -1);

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 13, hgraph::test_detail::tick(42));
    graph.evaluate(hgraph::test_detail::tick(42));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 11);
}

TEST_CASE("v2 nodes support evaluation clock injection", "[v2][graph][clock]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::test_detail::ClockCaptureNode::reset();

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("clock_capture").implementation<hgraph::test_detail::ClockCaptureNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::test_detail::tick(51));
    graph.evaluate(hgraph::test_detail::tick(51));

    CHECK(hgraph::test_detail::ClockCaptureNode::seen_evaluation_time == hgraph::test_detail::tick(51));
    CHECK(hgraph::test_detail::ClockCaptureNode::seen_next_cycle_time == hgraph::test_detail::tick(51) + hgraph::MIN_TD);
    CHECK(hgraph::test_detail::ClockCaptureNode::saw_now_at_or_after_evaluation_time);
    CHECK(hgraph::test_detail::ClockCaptureNode::saw_non_negative_cycle_time);
}

TEST_CASE("v2 nodes support simple scheduler injection", "[v2][graph][scheduler]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("scheduler_echo").implementation<hgraph::test_detail::SchedulerEchoNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 2, hgraph::test_detail::tick(1));
    graph.evaluate(hgraph::test_detail::tick(1));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 2);
    CHECK(graph.scheduled_time(1) == hgraph::test_detail::tick(3));

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 3, hgraph::test_detail::tick(2));
    graph.evaluate(hgraph::test_detail::tick(2));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 3);
    CHECK(graph.scheduled_time(1) == hgraph::test_detail::tick(3));

    graph.evaluate(hgraph::test_detail::tick(3));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == -1);
    CHECK(graph.scheduled_time(1) == hgraph::test_detail::tick(5));

    graph.evaluate(hgraph::test_detail::tick(4));
    CHECK_FALSE(graph.node_at(1).output_view(hgraph::test_detail::tick(4)).modified());

    graph.evaluate(hgraph::test_detail::tick(5));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == -1);
}

TEST_CASE("v2 nodes support tagged scheduler injection", "[v2][graph][scheduler]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *bool_type = value_registry.register_type<bool>("bool");
    const auto *scalar_int_ts = ts_registry.ts(int_type);
    const auto *scalar_bool_ts = ts_registry.ts(bool_type);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("bool_source").output_schema(scalar_bool_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("delay_source").output_schema(scalar_int_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("tagged_scheduler").implementation<hgraph::test_detail::TaggedSchedulerBoolNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 1, .dst_node = 2, .input_path = {1}});

    auto engine = hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(0));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 3, hgraph::test_detail::tick(1));
    graph.evaluate(hgraph::test_detail::tick(1));
    CHECK_FALSE(graph.node_at(2).output_view(hgraph::test_detail::tick(1)).modified());
    CHECK(graph.scheduled_time(2) == hgraph::test_detail::tick(4));

    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 5, hgraph::test_detail::tick(2));
    graph.evaluate(hgraph::test_detail::tick(2));
    CHECK_FALSE(graph.node_at(2).output_view(hgraph::test_detail::tick(2)).modified());
    CHECK(graph.scheduled_time(2) == hgraph::test_detail::tick(7));

    graph.evaluate(hgraph::test_detail::tick(4));
    CHECK_FALSE(graph.node_at(2).output_view(hgraph::test_detail::tick(4)).modified());

    graph.evaluate(hgraph::test_detail::tick(7));
    CHECK_FALSE(graph.node_at(2).output_view().value().as_atomic().as<bool>());

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, true, hgraph::test_detail::tick(8));
    graph.evaluate(hgraph::test_detail::tick(8));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<bool>());
    CHECK(graph.scheduled_time(2) == hgraph::test_detail::tick(13));

    graph.evaluate(hgraph::test_detail::tick(13));
    CHECK_FALSE(graph.node_at(2).output_view().value().as_atomic().as<bool>());
}

TEST_CASE("v2 evaluation engine builder produces a runnable simulation engine", "[v2][engine]")
{
    hgraph::GraphBuilder builder;
    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(60), hgraph::test_detail::tick(70));
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

    CHECK(engine.evaluation_mode() == hgraph::EvaluationMode::SIMULATION);
    CHECK(engine.start_time() == hgraph::test_detail::tick(60));
    CHECK(engine.end_time() == hgraph::test_detail::tick(70));
    CHECK(engine.graph().engine_evaluation_clock().evaluation_time() == hgraph::test_detail::tick(60));

    engine.run();
    CHECK(before_count == 2);
    CHECK(after_count == 2);
    CHECK(engine.graph().engine_evaluation_clock().evaluation_time() == hgraph::test_detail::tick(71));
}

TEST_CASE("v2 graph scheduling matches current graph overwrite rules", "[v2][graph][schedule]")
{
    hgraph::GraphBuilder builder;
    builder.add_node(hgraph::NodeBuilder{}.label("node").implementation<hgraph::test_detail::NoopNode>());

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(80), hgraph::test_detail::tick(90));
    auto &graph = engine.graph();

    graph.schedule_node(0, hgraph::test_detail::tick(80));
    CHECK(graph.scheduled_time(0) == hgraph::test_detail::tick(80));

    graph.schedule_node(0, hgraph::test_detail::tick(85));
    CHECK(graph.scheduled_time(0) == hgraph::test_detail::tick(85));

    graph.schedule_node(0, hgraph::test_detail::tick(88), true);
    CHECK(graph.scheduled_time(0) == hgraph::test_detail::tick(88));
}

TEST_CASE("v2 startup notify without a declared scheduler only schedules the startup cycle", "[v2][graph][schedule]")
{
    hgraph::test_detail::StartupNotifyWithoutSchedulerNode::reset();

    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder.add_node(
        hgraph::NodeBuilder{}.label("startup_notify").output_schema(scalar_ts).implementation<
            hgraph::test_detail::StartupNotifyWithoutSchedulerNode>());

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(300), hgraph::test_detail::tick(301));
    auto &graph = engine.graph();
    graph.start();

    CHECK(hgraph::test_detail::StartupNotifyWithoutSchedulerNode::start_calls == 1);
    CHECK(graph.scheduled_time(0) == hgraph::test_detail::tick(300));

    graph.evaluate(hgraph::test_detail::tick(300));

    CHECK(hgraph::test_detail::StartupNotifyWithoutSchedulerNode::eval_calls == 1);
    CHECK(hgraph::test_detail::StartupNotifyWithoutSchedulerNode::last_eval_time == hgraph::test_detail::tick(300));
    CHECK(graph.node_at(0).output_view().value().as_atomic().as<int>() == 1);
}

TEST_CASE("v2 static nodes can pass through REF values when wired as REF", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *bool_type = value_registry.register_type<bool>("bool");
    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_bool_ts = ts_registry.ts(bool_type);
    const auto *scalar_int_ref_ts = ts_registry.ref(ts_registry.ts(int_type));

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("trigger").output_schema(scalar_bool_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("emit_empty_ref").implementation<hgraph::test_detail::EmitEmptyRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("ref_passthrough").implementation<hgraph::test_detail::RefPassthroughNode>())
        .add_node(hgraph::NodeBuilder{}.label("ref_probe").implementation<hgraph::test_detail::RefStateProbeNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 1, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 2, .dst_node = 3, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(320), hgraph::test_detail::tick(321));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, true, hgraph::test_detail::tick(320));
    graph.evaluate(hgraph::test_detail::tick(320));

    const auto &ref_value =
        graph.node_at(2).output_view().value().as_atomic().template checked_as<hgraph::TimeSeriesReference>();
    CHECK(graph.node_at(2).output_schema() == scalar_int_ref_ts);
    CHECK(ref_value.is_empty());
    CHECK_FALSE(ref_value.is_valid());
    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 1);
}

TEST_CASE("v2 binds TS outputs to REF inputs through shared output alternatives", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *scalar_ref_ts = ts_registry.ref(scalar_ts);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("probe_a").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("probe_b").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 2, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(325), hgraph::test_detail::tick(326));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 19, hgraph::test_detail::tick(325));
    graph.evaluate(hgraph::test_detail::tick(325));

    const auto input_a = graph.node_at(1).input_view(hgraph::test_detail::tick(325)).as_bundle()[0];
    const auto input_b = graph.node_at(2).input_view(hgraph::test_detail::tick(325)).as_bundle()[0];
    const hgraph::LinkedTSContext *target_a = input_a.linked_target();
    const hgraph::LinkedTSContext *target_b = input_b.linked_target();

    REQUIRE(target_a != nullptr);
    REQUIRE(target_b != nullptr);
    CHECK(target_a->schema == scalar_ref_ts);
    CHECK(target_b->schema == scalar_ref_ts);
    CHECK(target_a->ts_state == target_b->ts_state);
    CHECK(target_a->value_data == target_b->value_data);

    const auto &wrapped_ref = input_a.value().as_atomic().template checked_as<hgraph::TimeSeriesReference>();
    CHECK(wrapped_ref.is_peered());
    CHECK(wrapped_ref.is_valid());
    CHECK(wrapped_ref.target_view(hgraph::test_detail::tick(325)).value().as_atomic().as<int>() == 19);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 19);
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 19);

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 23, hgraph::test_detail::tick(326));
    graph.evaluate(hgraph::test_detail::tick(326));

    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 23);
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 23);
}

TEST_CASE("v2 child TS outputs share REF alternatives by logical position", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *scalar_ref_ts = ts_registry.ref(scalar_ts);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("pair_source").output_schema(pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("lhs_probe_a").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("lhs_probe_b").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs_probe").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {0}, .dst_node = 1, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {0}, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {1}, .dst_node = 3, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(326), hgraph::test_detail::tick(327));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0}, 11, hgraph::test_detail::tick(326));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 13, hgraph::test_detail::tick(326));
    graph.evaluate(hgraph::test_detail::tick(326));

    const auto lhs_input_a = graph.node_at(1).input_view(hgraph::test_detail::tick(326)).as_bundle()[0];
    const auto lhs_input_b = graph.node_at(2).input_view(hgraph::test_detail::tick(326)).as_bundle()[0];
    const auto rhs_input = graph.node_at(3).input_view(hgraph::test_detail::tick(326)).as_bundle()[0];
    const hgraph::LinkedTSContext *lhs_target_a = lhs_input_a.linked_target();
    const hgraph::LinkedTSContext *lhs_target_b = lhs_input_b.linked_target();
    const hgraph::LinkedTSContext *rhs_target = rhs_input.linked_target();

    REQUIRE(lhs_target_a != nullptr);
    REQUIRE(lhs_target_b != nullptr);
    REQUIRE(rhs_target != nullptr);
    CHECK(lhs_target_a->schema == scalar_ref_ts);
    CHECK(lhs_target_b->schema == scalar_ref_ts);
    CHECK(rhs_target->schema == scalar_ref_ts);
    CHECK(lhs_target_a->ts_state == lhs_target_b->ts_state);
    CHECK(lhs_target_a->value_data == lhs_target_b->value_data);
    CHECK(rhs_target->ts_state != lhs_target_a->ts_state);
    CHECK(rhs_target->value_data != lhs_target_a->value_data);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 11);
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 11);
    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 13);
}

TEST_CASE("v2 binds mixed TSB wrap alternatives at the bundle boundary", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("pair_source").output_schema(pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::SumWrappedPair>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(327), hgraph::test_detail::tick(328));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0}, 11, hgraph::test_detail::tick(327));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 13, hgraph::test_detail::tick(327));
    graph.evaluate(hgraph::test_detail::tick(327));

    auto pair_input = graph.node_at(1).input_view(hgraph::test_detail::tick(327)).as_bundle()[0].as_bundle();
    const auto lhs_ref = pair_input.field("lhs").value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
    const hgraph::LinkedTSContext *rhs_target = pair_input.field("rhs").linked_target();

    CHECK(lhs_ref.is_peered());
    REQUIRE(rhs_target != nullptr);
    CHECK(rhs_target->schema == scalar_ts);
    CHECK(lhs_ref.target_view(hgraph::test_detail::tick(327)).value().as_atomic().as<int>() == 11);
    CHECK(pair_input.field("rhs").value().as_atomic().as<int>() == 13);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 24);
}

TEST_CASE("v2 binds fixed-size TSL wrap alternatives at the list boundary", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *list_schema = ts_registry.tsl(scalar_ts, 2);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("list_source").output_schema(list_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::SumWrappedList>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(328), hgraph::test_detail::tick(329));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0}, 2, hgraph::test_detail::tick(328));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 4, hgraph::test_detail::tick(328));
    graph.evaluate(hgraph::test_detail::tick(328));

    auto list_input = graph.node_at(1).input_view(hgraph::test_detail::tick(328)).as_bundle()[0].as_list();
    const auto ref0 = list_input[0].value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
    const auto ref1 = list_input[1].value().as_atomic().checked_as<hgraph::TimeSeriesReference>();

    CHECK(ref0.is_peered());
    CHECK(ref1.is_peered());
    CHECK(ref0.target_view(hgraph::test_detail::tick(328)).value().as_atomic().as<int>() == 2);
    CHECK(ref1.target_view(hgraph::test_detail::tick(328)).value().as_atomic().as<int>() == 4);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 6);
}

TEST_CASE("v2 binds nested fixed TSL wrap alternatives recursively", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *list_schema = ts_registry.tsl(scalar_ts, 2);
    const auto *source_schema = ts_registry.tsb({{"items", list_schema}, {"tail", scalar_ts}}, "NestedList");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("source").output_schema(source_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::SumNestedWrappedList>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(329), hgraph::test_detail::tick(330));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0, 0}, 3, hgraph::test_detail::tick(329));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0, 1}, 5, hgraph::test_detail::tick(329));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 7, hgraph::test_detail::tick(329));
    graph.evaluate(hgraph::test_detail::tick(329));

    auto bundle_input = graph.node_at(1).input_view(hgraph::test_detail::tick(329)).as_bundle()[0].as_bundle();
    auto list_input = bundle_input.field("items").as_list();
    const auto ref0 = list_input[0].value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
    const auto ref1 = list_input[1].value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
    const hgraph::LinkedTSContext *tail_target = bundle_input.field("tail").linked_target();

    CHECK(ref0.target_view(hgraph::test_detail::tick(329)).value().as_atomic().as<int>() == 3);
    CHECK(ref1.target_view(hgraph::test_detail::tick(329)).value().as_atomic().as<int>() == 5);
    REQUIRE(tail_target != nullptr);
    CHECK(tail_target->schema == scalar_ts);
    CHECK(bundle_input.field("tail").value().as_atomic().as<int>() == 7);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 15);
}

TEST_CASE("v2 nested child TS outputs share REF alternatives by logical position", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *scalar_ref_ts = ts_registry.ref(scalar_ts);
    const auto *list_schema = ts_registry.tsl(scalar_ts, 2);
    const auto *source_schema = ts_registry.tsb({{"items", list_schema}, {"tail", scalar_ts}}, "NestedList");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("source").output_schema(source_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("probe_a").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("probe_b").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("probe_c").implementation<hgraph::test_detail::DereferenceRefNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {0, 0}, .dst_node = 1, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {0, 0}, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 0, .output_path = {0, 1}, .dst_node = 3, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(330), hgraph::test_detail::tick(331));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0, 0}, 3, hgraph::test_detail::tick(330));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0, 1}, 5, hgraph::test_detail::tick(330));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 7, hgraph::test_detail::tick(330));
    graph.evaluate(hgraph::test_detail::tick(330));

    const auto input_a = graph.node_at(1).input_view(hgraph::test_detail::tick(330)).as_bundle()[0];
    const auto input_b = graph.node_at(2).input_view(hgraph::test_detail::tick(330)).as_bundle()[0];
    const auto input_c = graph.node_at(3).input_view(hgraph::test_detail::tick(330)).as_bundle()[0];
    const hgraph::LinkedTSContext *target_a = input_a.linked_target();
    const hgraph::LinkedTSContext *target_b = input_b.linked_target();
    const hgraph::LinkedTSContext *target_c = input_c.linked_target();

    REQUIRE(target_a != nullptr);
    REQUIRE(target_b != nullptr);
    REQUIRE(target_c != nullptr);
    CHECK(target_a->schema == scalar_ref_ts);
    CHECK(target_b->schema == scalar_ref_ts);
    CHECK(target_c->schema == scalar_ref_ts);
    CHECK(target_a->ts_state == target_b->ts_state);
    CHECK(target_a->value_data == target_b->value_data);
    CHECK(target_c->ts_state != target_a->ts_state);
    CHECK(target_c->value_data != target_a->value_data);
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 3);
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 3);
    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 5);
}

TEST_CASE("v2 binds REF outputs to TS inputs through dereference alternatives", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("a").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("b").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("ref_source").output_schema(ref_scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::PassThroughInput>())
        .add_edge(hgraph::Edge{.src_node = 2, .dst_node = 3, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(331), hgraph::test_detail::tick(333));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 17, hgraph::test_detail::tick(331));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 29, hgraph::test_detail::tick(331));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(331))),
        hgraph::test_detail::tick(331));
    graph.evaluate(hgraph::test_detail::tick(331));

    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 17);

    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {},
        hgraph::TimeSeriesReference::make(graph.node_at(1).output_view(hgraph::test_detail::tick(332))),
        hgraph::test_detail::tick(332));
    graph.evaluate(hgraph::test_detail::tick(332));

    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 29);
}

TEST_CASE("v2 child REF outputs retarget through shared dereference alternatives", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *ref_pair_schema = ts_registry.tsb({{"lhs", ref_scalar_ts}, {"rhs", ref_scalar_ts}}, "RefPair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("a").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("b").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("c").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("pair_ref_source").output_schema(ref_pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("lhs_consumer_a").implementation<hgraph::test_detail::PassThroughInput>())
        .add_node(hgraph::NodeBuilder{}.label("lhs_consumer_b").implementation<hgraph::test_detail::PassThroughInput>())
        .add_edge(hgraph::Edge{.src_node = 3, .output_path = {0}, .dst_node = 4, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 3, .output_path = {0}, .dst_node = 5, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(333), hgraph::test_detail::tick(335));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 5, hgraph::test_detail::tick(333));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 7, hgraph::test_detail::tick(334));
    hgraph::test_detail::publish_scalar_output(graph.node_at(2), {}, 11, hgraph::test_detail::tick(333));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(3),
        {0},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(333))),
        hgraph::test_detail::tick(333));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(3),
        {1},
        hgraph::TimeSeriesReference::make(graph.node_at(2).output_view(hgraph::test_detail::tick(333))),
        hgraph::test_detail::tick(333));
    graph.evaluate(hgraph::test_detail::tick(333));

    const auto input_a_before = graph.node_at(4).input_view(hgraph::test_detail::tick(333)).as_bundle()[0];
    const auto input_b_before = graph.node_at(5).input_view(hgraph::test_detail::tick(333)).as_bundle()[0];
    const hgraph::LinkedTSContext *target_a_before = input_a_before.linked_target();
    const hgraph::LinkedTSContext *target_b_before = input_b_before.linked_target();

    REQUIRE(target_a_before != nullptr);
    REQUIRE(target_b_before != nullptr);
    CHECK(target_a_before->ts_state == target_b_before->ts_state);
    CHECK(graph.node_at(4).output_view().value().as_atomic().as<int>() == 5);
    CHECK(graph.node_at(5).output_view().value().as_atomic().as<int>() == 5);

    hgraph::test_detail::publish_scalar_output(
        graph.node_at(3),
        {0},
        hgraph::TimeSeriesReference::make(graph.node_at(1).output_view(hgraph::test_detail::tick(334))),
        hgraph::test_detail::tick(334));
    graph.evaluate(hgraph::test_detail::tick(334));

    const auto input_a_after = graph.node_at(4).input_view(hgraph::test_detail::tick(334)).as_bundle()[0];
    const auto input_b_after = graph.node_at(5).input_view(hgraph::test_detail::tick(334)).as_bundle()[0];
    const hgraph::LinkedTSContext *target_a_after = input_a_after.linked_target();
    const hgraph::LinkedTSContext *target_b_after = input_b_after.linked_target();

    REQUIRE(target_a_after != nullptr);
    REQUIRE(target_b_after != nullptr);
    CHECK(target_a_after->ts_state == target_a_before->ts_state);
    CHECK(target_b_after->ts_state == target_b_before->ts_state);
    CHECK(target_a_after->ts_state == target_b_after->ts_state);
    CHECK(graph.node_at(4).output_view().value().as_atomic().as<int>() == 7);
    CHECK(graph.node_at(5).output_view().value().as_atomic().as<int>() == 7);
}

TEST_CASE("v2 child REF outputs share TS alternatives by logical position", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *ref_pair_schema = ts_registry.tsb({{"lhs", ref_scalar_ts}, {"rhs", ref_scalar_ts}}, "RefPair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("pair_ref_source").output_schema(ref_pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("lhs_consumer_a").implementation<hgraph::test_detail::PassThroughInput>())
        .add_node(hgraph::NodeBuilder{}.label("lhs_consumer_b").implementation<hgraph::test_detail::PassThroughInput>())
        .add_node(hgraph::NodeBuilder{}.label("rhs_consumer").implementation<hgraph::test_detail::PassThroughInput>())
        .add_edge(hgraph::Edge{.src_node = 2, .output_path = {0}, .dst_node = 3, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 2, .output_path = {0}, .dst_node = 4, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 2, .output_path = {1}, .dst_node = 5, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(332), hgraph::test_detail::tick(333));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 5, hgraph::test_detail::tick(332));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 7, hgraph::test_detail::tick(332));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {0},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(332))),
        hgraph::test_detail::tick(332));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {1},
        hgraph::TimeSeriesReference::make(graph.node_at(1).output_view(hgraph::test_detail::tick(332))),
        hgraph::test_detail::tick(332));
    graph.evaluate(hgraph::test_detail::tick(332));

    const auto lhs_input_a = graph.node_at(3).input_view(hgraph::test_detail::tick(332)).as_bundle()[0];
    const auto lhs_input_b = graph.node_at(4).input_view(hgraph::test_detail::tick(332)).as_bundle()[0];
    const auto rhs_input = graph.node_at(5).input_view(hgraph::test_detail::tick(332)).as_bundle()[0];
    const hgraph::LinkedTSContext *lhs_target_a = lhs_input_a.linked_target();
    const hgraph::LinkedTSContext *lhs_target_b = lhs_input_b.linked_target();
    const hgraph::LinkedTSContext *rhs_target = rhs_input.linked_target();

    REQUIRE(lhs_target_a != nullptr);
    REQUIRE(lhs_target_b != nullptr);
    REQUIRE(rhs_target != nullptr);
    CHECK(lhs_target_a->schema == scalar_ts);
    CHECK(lhs_target_b->schema == scalar_ts);
    CHECK(rhs_target->schema == scalar_ts);
    CHECK(lhs_target_a->ts_state == lhs_target_b->ts_state);
    CHECK(rhs_target->ts_state != lhs_target_a->ts_state);
    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 5);
    CHECK(graph.node_at(4).output_view().value().as_atomic().as<int>() == 5);
    CHECK(graph.node_at(5).output_view().value().as_atomic().as<int>() == 7);
}

TEST_CASE("v2 nested child REF outputs share TS alternatives by logical position", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *ref_list_schema = ts_registry.tsl(ref_scalar_ts, 2);
    const auto *source_schema = ts_registry.tsb({{"items", ref_list_schema}, {"tail", ref_scalar_ts}}, "NestedRefList");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("tail").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("nested_ref_source").output_schema(source_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("probe_a").implementation<hgraph::test_detail::PassThroughInput>())
        .add_node(hgraph::NodeBuilder{}.label("probe_b").implementation<hgraph::test_detail::PassThroughInput>())
        .add_node(hgraph::NodeBuilder{}.label("probe_c").implementation<hgraph::test_detail::PassThroughInput>())
        .add_edge(hgraph::Edge{.src_node = 3, .output_path = {0, 0}, .dst_node = 4, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 3, .output_path = {0, 0}, .dst_node = 5, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 3, .output_path = {0, 1}, .dst_node = 6, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(335), hgraph::test_detail::tick(336));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 2, hgraph::test_detail::tick(335));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 4, hgraph::test_detail::tick(335));
    hgraph::test_detail::publish_scalar_output(graph.node_at(2), {}, 6, hgraph::test_detail::tick(335));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(3),
        {0, 0},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(335))),
        hgraph::test_detail::tick(335));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(3),
        {0, 1},
        hgraph::TimeSeriesReference::make(graph.node_at(1).output_view(hgraph::test_detail::tick(335))),
        hgraph::test_detail::tick(335));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(3),
        {1},
        hgraph::TimeSeriesReference::make(graph.node_at(2).output_view(hgraph::test_detail::tick(335))),
        hgraph::test_detail::tick(335));
    graph.evaluate(hgraph::test_detail::tick(335));

    const auto input_a = graph.node_at(4).input_view(hgraph::test_detail::tick(335)).as_bundle()[0];
    const auto input_b = graph.node_at(5).input_view(hgraph::test_detail::tick(335)).as_bundle()[0];
    const auto input_c = graph.node_at(6).input_view(hgraph::test_detail::tick(335)).as_bundle()[0];
    const hgraph::LinkedTSContext *target_a = input_a.linked_target();
    const hgraph::LinkedTSContext *target_b = input_b.linked_target();
    const hgraph::LinkedTSContext *target_c = input_c.linked_target();

    REQUIRE(target_a != nullptr);
    REQUIRE(target_b != nullptr);
    REQUIRE(target_c != nullptr);
    CHECK(target_a->schema == scalar_ts);
    CHECK(target_b->schema == scalar_ts);
    CHECK(target_c->schema == scalar_ts);
    CHECK(target_a->ts_state == target_b->ts_state);
    CHECK(target_c->ts_state != target_a->ts_state);
    CHECK(graph.node_at(4).output_view().value().as_atomic().as<int>() == 2);
    CHECK(graph.node_at(5).output_view().value().as_atomic().as<int>() == 2);
    CHECK(graph.node_at(6).output_view().value().as_atomic().as<int>() == 4);
}

TEST_CASE("v2 rooted alternatives can mix wrap and dereference conversions", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *source_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", ref_scalar_ts}}, "WrapDerefSourcePair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("rhs_target").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("source").output_schema(source_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::SumWrappedPair>())
        .add_edge(hgraph::Edge{.src_node = 1, .dst_node = 2, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(336), hgraph::test_detail::tick(337));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 13, hgraph::test_detail::tick(336));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {0}, 11, hgraph::test_detail::tick(336));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(1),
        {1},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(336))),
        hgraph::test_detail::tick(336));
    graph.evaluate(hgraph::test_detail::tick(336));

    auto pair_input = graph.node_at(2).input_view(hgraph::test_detail::tick(336)).as_bundle()[0].as_bundle();
    const auto lhs_ref = pair_input.field("lhs").value().as_atomic().checked_as<hgraph::TimeSeriesReference>();
    const hgraph::LinkedTSContext *rhs_target = pair_input.field("rhs").linked_target();

    CHECK(lhs_ref.is_peered());
    CHECK(lhs_ref.target_view(hgraph::test_detail::tick(336)).value().as_atomic().as<int>() == 11);
    REQUIRE(rhs_target != nullptr);
    CHECK(rhs_target->schema == scalar_ts);
    CHECK(pair_input.field("rhs").value().as_atomic().as<int>() == 13);
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 24);
}

TEST_CASE("v2 binds fixed-shape REF bundle outputs to TS bundle inputs", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *ref_pair_schema = ts_registry.tsb({{"lhs", ref_scalar_ts}, {"rhs", ref_scalar_ts}}, "RefPair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("pair_ref_source").output_schema(ref_pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::SumPairInput>())
        .add_edge(hgraph::Edge{.src_node = 2, .dst_node = 3, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(333), hgraph::test_detail::tick(334));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 5, hgraph::test_detail::tick(333));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 7, hgraph::test_detail::tick(333));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {0},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(333))),
        hgraph::test_detail::tick(333));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {1},
        hgraph::TimeSeriesReference::make(graph.node_at(1).output_view(hgraph::test_detail::tick(333))),
        hgraph::test_detail::tick(333));
    graph.evaluate(hgraph::test_detail::tick(333));

    auto pair_input = graph.node_at(3).input_view(hgraph::test_detail::tick(333)).as_bundle()[0].as_bundle();
    CHECK(pair_input.field("lhs").value().as_atomic().as<int>() == 5);
    CHECK(pair_input.field("rhs").value().as_atomic().as<int>() == 7);
    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 12);
}

TEST_CASE("v2 binds fixed-size REF list outputs to TS list inputs", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *ref_scalar_ts = ts_registry.ref(scalar_ts);
    const auto *ref_list_schema = ts_registry.tsl(ref_scalar_ts, 2);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("list_ref_source").output_schema(ref_list_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("consumer").implementation<hgraph::test_detail::SumPlainList>())
        .add_edge(hgraph::Edge{.src_node = 2, .dst_node = 3, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(334), hgraph::test_detail::tick(335));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 2, hgraph::test_detail::tick(334));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 4, hgraph::test_detail::tick(334));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {0},
        hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(334))),
        hgraph::test_detail::tick(334));
    hgraph::test_detail::publish_scalar_output(
        graph.node_at(2),
        {1},
        hgraph::TimeSeriesReference::make(graph.node_at(1).output_view(hgraph::test_detail::tick(334))),
        hgraph::test_detail::tick(334));
    graph.evaluate(hgraph::test_detail::tick(334));

    auto list_input = graph.node_at(3).input_view(hgraph::test_detail::tick(334)).as_bundle()[0].as_list();
    CHECK(list_input[0].value().as_atomic().as<int>() == 2);
    CHECK(list_input[1].value().as_atomic().as<int>() == 4);
    CHECK(graph.node_at(3).output_view().value().as_atomic().as<int>() == 6);
}

TEST_CASE("v2 time-series references capture bound output targets through views", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("sink").implementation<hgraph::test_detail::PassThroughInput>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(330), hgraph::test_detail::tick(331));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 17, hgraph::test_detail::tick(330));

    const auto output_ref = hgraph::TimeSeriesReference::make(graph.node_at(0).output_view(hgraph::test_detail::tick(330)));
    CHECK(output_ref.is_peered());
    CHECK(output_ref.is_valid());
    CHECK(output_ref.target_view(hgraph::test_detail::tick(330)).value().as_atomic().as<int>() == 17);

    const auto input_ref =
        hgraph::TimeSeriesReference::make(graph.node_at(1).input_view(hgraph::test_detail::tick(330)).as_bundle()[0]);
    CHECK(input_ref.is_peered());
    CHECK(input_ref.is_valid());
    CHECK(input_ref == output_ref);
    CHECK(input_ref.target_view(hgraph::test_detail::tick(330)).value().as_atomic().as<int>() == 17);
}

TEST_CASE("v2 time-series references made from REF inputs return the carried ref value", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("wrap").implementation<hgraph::test_detail::WrapInputAsRefNode>())
        .add_node(hgraph::NodeBuilder{}.label("ref_passthrough").implementation<hgraph::test_detail::RefPassthroughNode>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 1, .dst_node = 2, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(340), hgraph::test_detail::tick(341));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 23, hgraph::test_detail::tick(340));
    graph.evaluate(hgraph::test_detail::tick(340));

    const auto &wrapped_ref =
        graph.node_at(1).output_view().value().as_atomic().template checked_as<hgraph::TimeSeriesReference>();
    const auto ref_input =
        hgraph::TimeSeriesReference::make(graph.node_at(2).input_view(hgraph::test_detail::tick(340)).as_bundle()[0]);

    CHECK(wrapped_ref.is_peered());
    CHECK(ref_input.is_peered());
    CHECK(ref_input == wrapped_ref);
    CHECK(ref_input.target_view(hgraph::test_detail::tick(340)).value().as_atomic().as<int>() == 23);
}

TEST_CASE("v2 time-series references made from peered bundle inputs stay peered", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
    const auto *nested_input_schema = ts_registry.tsb({{"pair", pair_schema}}, "NestedInput");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("pair_source").output_schema(pair_schema).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("sum").input_schema(nested_input_schema).implementation<hgraph::test_detail::SumNestedPair>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(350), hgraph::test_detail::tick(351));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {0}, 41, hgraph::test_detail::tick(350));
    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {1}, 43, hgraph::test_detail::tick(350));
    graph.evaluate(hgraph::test_detail::tick(350));

    const auto pair_ref =
        hgraph::TimeSeriesReference::make(graph.node_at(1).input_view(hgraph::test_detail::tick(350)).as_bundle()[0]);
    CHECK(pair_ref.is_peered());
    auto target = pair_ref.target_view(hgraph::test_detail::tick(350)).as_bundle();
    CHECK(target.field("lhs").value().as_atomic().as<int>() == 41);
    CHECK(target.field("rhs").value().as_atomic().as<int>() == 43);
}

TEST_CASE("v2 time-series references made from non-peered bundle inputs become composite refs", "[v2][graph][ref]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("rhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("sum").input_schema(pair_schema).implementation<hgraph::test_detail::SumNestedPair>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::Edge{.src_node = 1, .dst_node = 2, .input_path = {1}});

    auto engine =
        hgraph::test_detail::make_engine(std::move(builder), hgraph::test_detail::tick(360), hgraph::test_detail::tick(361));
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 47, hgraph::test_detail::tick(360));
    hgraph::test_detail::publish_scalar_output(graph.node_at(1), {}, 53, hgraph::test_detail::tick(360));
    graph.evaluate(hgraph::test_detail::tick(360));

    const auto pair_ref =
        hgraph::TimeSeriesReference::make(graph.node_at(2).input_view(hgraph::test_detail::tick(360)));
    CHECK(pair_ref.is_non_peered());
    REQUIRE(pair_ref.items().size() == 2);
    CHECK(pair_ref[0].is_peered());
    CHECK(pair_ref[1].is_peered());
    CHECK(pair_ref[0].target_view(hgraph::test_detail::tick(360)).value().as_atomic().as<int>() == 47);
    CHECK(pair_ref[1].target_view(hgraph::test_detail::tick(360)).value().as_atomic().as<int>() == 53);
}

TEST_CASE("v2 graph notifies after evaluation even when node evaluation throws", "[v2][graph][engine]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::test_detail::CountingObserver observer;

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::test_detail::NoopNode>())
        .add_node(hgraph::NodeBuilder{}.label("throws").implementation<hgraph::test_detail::ThrowOnEval>())
        .add_edge(hgraph::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto engine = hgraph::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .start_time(hgraph::test_detail::tick(100))
                      .end_time(hgraph::test_detail::tick(110))
                      .add_life_cycle_observer(&observer)
                      .build();
    auto &graph = engine.graph();
    graph.start();

    hgraph::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::test_detail::tick(101));

    CHECK_THROWS_AS(graph.evaluate(hgraph::test_detail::tick(101)), std::runtime_error);
    CHECK(graph.last_evaluation_time() == hgraph::test_detail::tick(101));
    CHECK(observer.before_graph_evaluation == 1);
    CHECK(observer.after_graph_evaluation == 1);
    CHECK(observer.before_node_evaluation == 1);
    CHECK(observer.after_node_evaluation == 1);
}

TEST_CASE("v2 graph stop continues through node failures before rethrowing", "[v2][graph][lifecycle]")
{
    hgraph::test_detail::StopThrowsNode::reset();
    hgraph::test_detail::StopTracksNode::reset();
    hgraph::test_detail::CountingObserver observer;

    hgraph::GraphBuilder builder;
    builder
        .add_node(hgraph::NodeBuilder{}.label("throws_on_stop").implementation<hgraph::test_detail::StopThrowsNode>())
        .add_node(hgraph::NodeBuilder{}.label("tracks_stop").implementation<hgraph::test_detail::StopTracksNode>());

    auto engine = hgraph::EvaluationEngineBuilder{}
                      .graph_builder(std::move(builder))
                      .start_time(hgraph::test_detail::tick(120))
                      .end_time(hgraph::test_detail::tick(130))
                      .add_life_cycle_observer(&observer)
                      .build();
    auto &graph = engine.graph();
    graph.start();

    CHECK_THROWS_AS(graph.stop(), std::runtime_error);
    CHECK(hgraph::test_detail::StopThrowsNode::stop_calls == 1);
    CHECK(hgraph::test_detail::StopTracksNode::stop_calls == 1);
    CHECK(observer.before_stop_graph == 1);
    CHECK(observer.after_stop_graph == 1);
    CHECK(observer.before_stop_node == 2);
    CHECK(observer.after_stop_node == 2);
}

TEST_CASE("v2 graph start rolls back started nodes when startup fails", "[v2][graph][lifecycle]")
{
    hgraph::test_detail::StartThrowsNode::reset();
    hgraph::test_detail::StartTracksNode::reset();
    hgraph::test_detail::CountingObserver observer;

    {
        hgraph::GraphBuilder builder;
        builder
            .add_node(hgraph::NodeBuilder{}.label("tracks_start").implementation<hgraph::test_detail::StartTracksNode>())
            .add_node(hgraph::NodeBuilder{}.label("throws_on_start").implementation<hgraph::test_detail::StartThrowsNode>());

        auto engine = hgraph::EvaluationEngineBuilder{}
                          .graph_builder(std::move(builder))
                          .start_time(hgraph::test_detail::tick(140))
                          .end_time(hgraph::test_detail::tick(150))
                          .add_life_cycle_observer(&observer)
                          .build();
        auto &graph = engine.graph();

        CHECK_THROWS_AS(graph.start(), std::runtime_error);
        CHECK_NOTHROW(graph.stop());
        CHECK(hgraph::test_detail::StartTracksNode::start_calls == 1);
        CHECK(hgraph::test_detail::StartTracksNode::stop_calls == 1);
        CHECK(hgraph::test_detail::StartThrowsNode::start_calls == 1);
        CHECK(hgraph::test_detail::StartThrowsNode::stop_calls == 0);
        CHECK(observer.before_start_graph == 1);
        CHECK(observer.after_start_graph == 0);
        CHECK(observer.before_start_node == 2);
        CHECK(observer.after_start_node == 1);
        CHECK(observer.before_stop_graph == 1);
        CHECK(observer.after_stop_graph == 1);
        CHECK(observer.before_stop_node == 1);
        CHECK(observer.after_stop_node == 1);
    }

    CHECK(hgraph::test_detail::StartTracksNode::stop_calls == 1);
    CHECK(hgraph::test_detail::StartThrowsNode::stop_calls == 0);
}

TEST_CASE("v2 real-time engine clock advances to scheduled wall-clock time", "[v2][engine][realtime]")
{
    using namespace std::chrono_literals;

    hgraph::GraphBuilder builder;
    const auto start_time = hgraph::test_detail::utc_now_tick();
    auto engine = hgraph::test_detail::make_engine(
        std::move(builder), start_time, start_time + 500ms, hgraph::EvaluationMode::REAL_TIME);

    auto clock = engine.graph().engine_evaluation_clock();
    const auto scheduled_time = hgraph::test_detail::utc_now_tick() + 30ms;

    clock.update_next_scheduled_evaluation_time(scheduled_time);
    clock.advance_to_next_scheduled_time();

    const auto after = hgraph::test_detail::utc_now_tick();
    CHECK(clock.evaluation_time() >= scheduled_time);
    CHECK(clock.evaluation_time() <= after);
}

TEST_CASE("v2 real-time engine clock wakes early when push scheduling is requested", "[v2][engine][realtime]")
{
    using namespace std::chrono_literals;

    hgraph::GraphBuilder builder;
    const auto start_time = hgraph::test_detail::utc_now_tick();
    auto engine = hgraph::test_detail::make_engine(
        std::move(builder), start_time, start_time + 1s, hgraph::EvaluationMode::REAL_TIME);

    auto clock = engine.graph().engine_evaluation_clock();
    const auto scheduled_time = hgraph::test_detail::utc_now_tick() + 250ms;
    clock.update_next_scheduled_evaluation_time(scheduled_time);

    const auto before = hgraph::test_detail::utc_now_tick();
    std::thread notifier([clock] {
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        clock.mark_push_node_requires_scheduling();
    });

    clock.advance_to_next_scheduled_time();
    notifier.join();

    const auto after = hgraph::test_detail::utc_now_tick();
    CHECK(after < scheduled_time);
    CHECK(clock.evaluation_time() < scheduled_time);
    CHECK(clock.evaluation_time() >= before);
    CHECK(clock.push_node_requires_scheduling());
    clock.reset_push_node_requires_scheduling();
    CHECK_FALSE(clock.push_node_requires_scheduling());
}

TEST_CASE("v2 static node signatures export Python wiring metadata", "[v2][python]")
{
    hgraph::test_detail::ensure_python_hgraph_importable();

    nb::gil_scoped_acquire guard;
    nb::object signature = hgraph::StaticNodeSignature<hgraph::test_detail::ExportedSumNode>::wiring_signature();

    CHECK(nb::cast<std::string>(signature.attr("name")) == "exported_sum");
    CHECK(nb::cast<std::string>(signature.attr("signature")) == "exported_sum(lhs: TS[int], rhs: TS[int]) -> TS[int]");
    CHECK(nb::len(nb::cast<nb::object>(signature.attr("args"))) == 2);
}

TEST_CASE("v2 static node signatures derive selector policies from In metadata", "[v2][signature]")
{
    using signature = hgraph::StaticNodeSignature<hgraph::test_detail::PolicyMetadataNode>;

    CHECK(signature::active_input_names() == std::vector<std::string>{"lhs", "strict"});
    CHECK(signature::valid_input_names() == std::vector<std::string>{"lhs"});
    CHECK(signature::all_valid_input_names() == std::vector<std::string>{"strict"});
}

TEST_CASE("v2 static node signatures preserve explicit empty selector policies", "[v2][signature]")
{
    using signature = hgraph::StaticNodeSignature<hgraph::test_detail::ExplicitEmptyPolicyNode>;

    CHECK(signature::active_input_names().empty());
    CHECK(signature::valid_input_names().empty());
    CHECK(signature::has_explicit_activity_policy());
    CHECK(signature::has_explicit_valid_input_policy());

    hgraph::test_detail::ensure_python_hgraph_importable();

    nb::gil_scoped_acquire guard;
    nb::object wiring_signature = signature::wiring_signature();
    nb::object active_inputs = wiring_signature.attr("active_inputs");
    nb::object valid_inputs = wiring_signature.attr("valid_inputs");

    CHECK_FALSE(active_inputs.is_none());
    CHECK_FALSE(valid_inputs.is_none());
    CHECK(nb::len(active_inputs) == 0);
    CHECK(nb::len(valid_inputs) == 0);
    CHECK(wiring_signature.attr("all_valid_inputs").is_none());
}

TEST_CASE("v2 static node builders preserve explicit empty active selector policies", "[v2][signature]")
{
    hgraph::NodeBuilder builder;
    builder.implementation<hgraph::test_detail::ExplicitEmptyPolicyNode>();

    CHECK(builder.has_explicit_active_inputs());
    CHECK(builder.active_inputs().empty());
}

TEST_CASE("v2 static node signatures export linked template type variables", "[v2][python][signature]")
{
    hgraph::test_detail::ensure_python_hgraph_importable();

    using signature = hgraph::StaticNodeSignature<hgraph::test_detail::GenericGetItemNode>;

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
    hgraph::test_detail::ensure_python_hgraph_importable();

    nb::gil_scoped_acquire guard;
    nb::object signature = hgraph::StaticNodeSignature<hgraph::test_detail::PullTickNode>::wiring_signature();
    nb::object expected =
        nb::module_::import_("hgraph._wiring._wiring_node_signature").attr("WiringNodeType").attr("PULL_SOURCE_NODE");

    CHECK(nb::cast<int>(signature.attr("node_type").attr("value")) == nb::cast<int>(expected.attr("value")));
}

TEST_CASE("v2 static node signatures export state injectables", "[v2][python][signature]")
{
    hgraph::test_detail::ensure_python_hgraph_importable();

    using typed_state_signature = hgraph::StaticNodeSignature<hgraph::test_detail::SumWithTypedState>;
    using recordable_state_signature =
        hgraph::StaticNodeSignature<hgraph::test_detail::PreviousValueFromRecordableState>;

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
    hgraph::test_detail::ensure_python_hgraph_importable();

    using signature = hgraph::StaticNodeSignature<hgraph::test_detail::ClockCaptureNode>;

    CHECK(signature::has_clock());

    nb::gil_scoped_acquire guard;
    nb::object wiring_signature = signature::wiring_signature();
    const auto args = nb::cast<std::vector<std::string>>(wiring_signature.attr("args"));

    CHECK(args == std::vector<std::string>{"lhs", "_clock"});
    CHECK(nb::cast<bool>(wiring_signature.attr("uses_clock")));
}

TEST_CASE("v2 static node signatures export node scheduler injectables", "[v2][python][signature]")
{
    hgraph::test_detail::ensure_python_hgraph_importable();

    using signature = hgraph::StaticNodeSignature<hgraph::test_detail::SchedulerEchoNode>;

    CHECK(signature::has_scheduler());

    nb::gil_scoped_acquire guard;
    nb::object wiring_signature = signature::wiring_signature();
    const auto args = nb::cast<std::vector<std::string>>(wiring_signature.attr("args"));

    CHECK(args == std::vector<std::string>{"ts", "_scheduler"});
    CHECK(nb::cast<bool>(wiring_signature.attr("uses_scheduler")));
}
