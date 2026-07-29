#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <array>
#include <stdexcept>
#include <string>
#include <typeindex>

using namespace std::string_literals;

namespace
{
    using namespace hgraph;

    // A compute node that throws when its input is negative; otherwise doubles.
    struct ThrowOnNegative
    {
        static constexpr auto name = "throw_on_negative";
        static void           eval(In<"x", TS<Int>> x, Out<TS<Int>> out)
        {
            if (x.value() < 0) { throw std::runtime_error("negative input"); }
            out.set(x.value() * 2);
        }
    };

    // Sub-graph form of the above (for try_except over a graph).
    struct DoublerOrThrowG
    {
        static constexpr auto name = "doubler_or_throw_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> x) { return wire<ThrowOnNegative>(w, x); }
    };

    struct ThrowUnknownOnNegative
    {
        static constexpr auto name = "throw_unknown_on_negative";
        static void           eval(In<"x", TS<Int>> x, Out<TS<Int>> out)
        {
            if (x.value() < 0) { throw 7; }
            out.set(x.value() * 3);
        }
    };

    struct TriplerOrUnknownG
    {
        static constexpr auto name = "tripler_or_unknown_g";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> x) { return wire<ThrowUnknownOnNegative>(w, x); }
    };

    // A sink node that throws when its input is negative (no output).
    struct SinkOrThrow
    {
        static constexpr auto name = "sink_or_throw";
        static void           eval(In<"x", TS<Int>> x)
        {
            if (x.value() < 0) { throw std::runtime_error("sink negative"); }
        }
    };

    struct SinkOrThrowG
    {
        static constexpr auto name = "sink_or_throw_g";
        static void           compose(Wiring &w, Port<TS<Int>> x) { wire<SinkOrThrow>(w, x); }
    };

    struct PauseOnceTag
    {
    };

    bool pause_once_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
    {
        const Int count = view.state().checked_as<Int>();
        if (count == Int{0})
        {
            view.replace_state(Value{Int{1}});
            return false;
        }

        auto root = view.input(evaluation_time);
        auto bundle = root.as_bundle();
        auto input = bundle[0];
        testing::set_output_value(view, evaluation_time, input.value().checked_as<Int>() * Int{2});
        return true;
    }

    NodeBuilder pause_once_node_builder()
    {
        const auto *ts_int = ts_type<TS<Int>>();

        NodeTypeMetaData meta;
        meta.display_name  = "pause_once";
        meta.input_schema  = TypeRegistry::instance().un_named_tsb({{"x", ts_int}});
        meta.output_schema = ts_int;
        meta.state_schema  = ts_int->value_schema;

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);
        descriptor.callbacks.start = [](const NodeView &view, DateTime) {
            view.replace_state(Value{Int{0}});
        };
        descriptor.ops.evaluate_impl = &pause_once_evaluate_impl;
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }

    struct PauseOnceG
    {
        static constexpr auto name = "pause_once_g";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            const std::array<WiringPortRef, 1> inputs{x.erased()};
            WiringPortRef out = w.add_node(std::type_index(typeid(PauseOnceTag)),
                                           pause_once_node_builder(),
                                           std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                           Value{});
            return Port<TS<Int>>{w, std::move(out)};
        }
    };

    // Extracts the ``error_msg`` field out of a TS<NodeError> as a string.
    struct ErrorMsgOf
    {
        static constexpr auto name = "error_msg_of";
        static void           eval(In<"e", TS<NodeError>> e, Out<TS<Str>> out)
        {
            out.set(e.base().value().as_bundle().at("error_msg").checked_as<Str>());
        }
    };

    struct ErrorTraceOf
    {
        static constexpr auto name = "error_trace_of";
        static void           eval(In<"e", TS<NodeError>> e, Out<TS<Str>> out)
        {
            out.set(e.base().value().as_bundle().at("activation_back_trace").checked_as<Str>());
        }
    };

    struct DivideOrThrow
    {
        static constexpr auto name = "divide_or_throw";

        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            if (rhs.value() == 0) { throw std::runtime_error("division by zero"); }
            out.set(lhs.value() / rhs.value());
        }
    };

    struct DivideOrThrowG
    {
        static constexpr auto name = "divide_or_throw_g";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            return wire<DivideOrThrow>(w, lhs, rhs);
        }
    };

    struct UncaughtErrorGraph
    {
        static constexpr auto name = "uncaught_error_graph";

        static void compose(Wiring &w)
        {
            auto value = wire<stdlib::replay_impl, TS<Int>>(w, Str{"input"});
            static_cast<void>(wire<ThrowOnNegative>(w, value));
        }
    };

    struct ErrorMsgG
    {
        static constexpr auto name = "error_msg_g";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<NodeError>> error)
        {
            return wire<ErrorMsgOf>(w, error);
        }
    };

    struct ErrorTraceG
    {
        static constexpr auto name = "error_trace_g";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<NodeError>> error)
        {
            return wire<ErrorTraceOf>(w, error);
        }
    };

    struct MappedErrorMessagesGraph
    {
        static constexpr auto name = "mapped_error_messages_graph";

        static Port<TSD<Int, TS<Str>>> compose(Wiring &w,
                                               Port<TSD<Int, TS<Int>>> lhs,
                                               Port<TSD<Int, TS<Int>>> rhs)
        {
            auto mapped = wire<stdlib::map_>(w, fn<DivideOrThrowG>(), lhs, rhs)
                              .as<TSD<Int, TS<Int>>>();
            Port<TSD<Int, TS<NodeError>>> errors = exception_time_series(mapped);
            return wire<stdlib::map_>(w, fn<ErrorMsgG>(), errors).as<TSD<Int, TS<Str>>>();
        }
    };

    struct MappedErrorTracesGraph
    {
        static constexpr auto name = "mapped_error_traces_graph";

        static Port<TSD<Int, TS<Str>>> compose(Wiring &w,
                                               Port<TSD<Int, TS<Int>>> lhs,
                                               Port<TSD<Int, TS<Int>>> rhs)
        {
            auto mapped = wire<stdlib::map_>(w, fn<DivideOrThrowG>(), lhs, rhs)
                              .as<TSD<Int, TS<Int>>>();
            auto errors = exception_time_series(
                mapped, ErrorCaptureOptions{.trace_back_depth = 2, .capture_values = true});
            return wire<stdlib::map_>(w, fn<ErrorTraceG>(), errors).as<TSD<Int, TS<Str>>>();
        }
    };

    using TryIntResult = UnNamedTSB<Field<"exception", TS<NodeError>>, Field<"out", TS<Int>>>;

    // Splits the try_except TSB result: the ``out`` value when it ticks.
    struct TryOutValue
    {
        static constexpr auto name = "try_out_value";
        static void           eval(In<"r", TryIntResult, InputValidity::Unchecked> r, Out<TS<Int>> out)
        {
            auto field = r.template field<"out">();
            if (field.valid() && field.modified()) { out.set(field.value()); }
        }
    };

    // Splits the try_except TSB result: the exception's error_msg when it ticks.
    struct TryExcMsg
    {
        static constexpr auto name = "try_exc_msg";
        static void           eval(In<"r", TryIntResult, InputValidity::Unchecked> r, Out<TS<Str>> out)
        {
            auto field = r.template field<"exception">();
            if (field.valid() && field.modified())
            {
                out.set(field.base().value().as_bundle().at("error_msg").checked_as<Str>());
            }
        }
    };

    struct TryExcTrace
    {
        static constexpr auto name = "try_exc_trace";
        static void           eval(In<"r", TryIntResult, InputValidity::Unchecked> r, Out<TS<Str>> out)
        {
            auto field = r.template field<"exception">();
            if (field.valid() && field.modified())
            {
                out.set(field.base().value().as_bundle().at("activation_back_trace").checked_as<Str>());
            }
        }
    };

    // --- whole-graph wrappers (the agreed lightweight-graph testing pattern) ---

    struct PerNodeCaptureGraph
    {
        static constexpr auto name = "per_node_capture_graph";
        static void           compose(Wiring &w)
        {
            auto x       = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto doubled = wire<ThrowOnNegative>(w, x);
            auto err     = exception_time_series(doubled);
            wire<stdlib::dense_record_impl>(w, doubled, Str{"out"});
            wire<stdlib::dense_record_impl>(w, wire<ErrorMsgOf>(w, err), Str{"err"});
        }
    };

    struct TryExceptValueGraph
    {
        static constexpr auto name = "try_except_value_graph";
        static void           compose(Wiring &w)
        {
            auto x      = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto result = try_except_<DoublerOrThrowG>(w, x).as<TryIntResult>();
            wire<stdlib::dense_record_impl>(w, wire<TryOutValue>(w, result), Str{"out"});
            wire<stdlib::dense_record_impl>(w, wire<TryExcMsg>(w, result), Str{"err"});
        }
    };

    struct PerNodeUnknownCaptureGraph
    {
        static constexpr auto name = "per_node_unknown_capture_graph";
        static void           compose(Wiring &w)
        {
            auto x       = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto tripled = wire<ThrowUnknownOnNegative>(w, x);
            auto err     = exception_time_series(tripled);
            wire<stdlib::dense_record_impl>(w, tripled, Str{"out"});
            wire<stdlib::dense_record_impl>(w, wire<ErrorMsgOf>(w, err), Str{"err"});
        }
    };

    struct TryExceptUnknownGraph
    {
        static constexpr auto name = "try_except_unknown_graph";
        static void           compose(Wiring &w)
        {
            auto x      = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto result = try_except_<TriplerOrUnknownG>(w, x).as<TryIntResult>();
            wire<stdlib::dense_record_impl>(w, wire<TryOutValue>(w, result), Str{"out"});
            wire<stdlib::dense_record_impl>(w, wire<TryExcMsg>(w, result), Str{"err"});
        }
    };

    struct TryExceptSinkGraph
    {
        static constexpr auto name = "try_except_sink_graph";
        static void           compose(Wiring &w)
        {
            auto x   = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto err = try_except_<SinkOrThrowG>(w, x).as<TS<NodeError>>();
            wire<stdlib::dense_record_impl>(w, wire<ErrorMsgOf>(w, err), Str{"err"});
        }
    };

    struct TryExceptPauseGraph
    {
        static constexpr auto name = "try_except_pause_graph";
        static void           compose(Wiring &w)
        {
            auto x      = wire<stdlib::replay_impl, TS<Int>>(w, Str{"x"});
            auto result = try_except_<PauseOnceG>(w, x).as<TryIntResult>();
            wire<stdlib::dense_record_impl>(w, wire<TryOutValue>(w, result), Str{"out"});
        }
    };

    struct ErasedTryValueGraph
    {
        static constexpr auto name = "erased_try_value_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto result = wire<stdlib::try_except>(w, fn<DoublerOrThrowG>(), x)
                              .as<TryIntResult>();
            return wire<TryOutValue>(w, result);
        }
    };

    struct ErasedTryErrorGraph
    {
        static constexpr auto name = "erased_try_error_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto result = wire<stdlib::try_except>(w, fn<DoublerOrThrowG>(), x)
                              .as<TryIntResult>();
            return wire<TryExcMsg>(w, result);
        }
    };

    struct ErasedTrySinkGraph
    {
        static constexpr auto name = "erased_try_sink_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto error = wire<stdlib::try_except>(w, fn<SinkOrThrowG>(), x)
                             .as<TS<NodeError>>();
            return wire<ErrorMsgOf>(w, error);
        }
    };

    struct DirectCaptureValueGraph
    {
        static constexpr auto name = "direct_capture_value_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto output = wire<ThrowOnNegative>(w, x);
            (void)exception_time_series(output);
            return output;
        }
    };

    struct DirectCaptureErrorGraph
    {
        static constexpr auto name = "direct_capture_error_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto output = wire<ThrowOnNegative>(w, x);
            return wire<ErrorMsgOf>(w, exception_time_series(output));
        }
    };

    struct LiftedCaptureErrorGraph
    {
        static constexpr auto name = "lifted_capture_error_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            auto output = wire<stdlib::div_>(w, lhs, rhs);
            return wire<ErrorMsgOf>(w, exception_time_series(output));
        }
    };

    struct DirectCaptureTraceGraph
    {
        static constexpr auto name = "direct_capture_trace_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto output = wire<ThrowOnNegative>(w, x);
            auto error = exception_time_series(
                output, ErrorCaptureOptions{.trace_back_depth = 2, .capture_values = true});
            return wire<ErrorTraceOf>(w, error);
        }
    };

    struct ErasedTryTraceGraph
    {
        static constexpr auto name = "erased_try_trace_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto result = wire<stdlib::try_except>(
                              w, fn<DoublerOrThrowG>(), x,
                              arg<"__trace_back_depth__">(Int{2}),
                              arg<"__capture_values__">(Bool{true}))
                              .as<TryIntResult>();
            return wire<TryExcTrace>(w, result);
        }
    };

    using TracePair = TSB<"TracePair",
                          Field<"lhs", TS<Int>>,
                          Field<"rhs", TS<Int>>>;

    struct ThrowOnZeroBundle
    {
        static constexpr auto name = "throw_on_zero_bundle";

        static void eval(In<"values", TracePair> values, Out<TS<Int>> out)
        {
            auto lhs = values.template field<"lhs">();
            auto rhs = values.template field<"rhs">();
            if (rhs.value() == 0) { throw std::runtime_error("bundle zero"); }
            out.set(lhs.value() / rhs.value());
        }
    };

    struct StructuredBundleCaptureTraceGraph
    {
        static constexpr auto name = "structured_bundle_capture_trace_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            auto output = wire<ThrowOnZeroBundle>(w, {{"lhs", lhs}, {"rhs", rhs}});
            auto error = exception_time_series(
                output, ErrorCaptureOptions{.trace_back_depth = 2, .capture_values = true});
            return wire<ErrorTraceOf>(w, error);
        }
    };

    struct ThrowOnZeroList
    {
        static constexpr auto name = "throw_on_zero_list";

        static void eval(In<"values", TSL<TS<Int>, 2>> values, Out<TS<Int>> out)
        {
            if (values[1].value() == 0) { throw std::runtime_error("list zero"); }
            out.set(values[0].value() / values[1].value());
        }
    };

    struct StructuredListCaptureTraceGraph
    {
        static constexpr auto name = "structured_list_capture_trace_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            auto output = wire<ThrowOnZeroList>(w, {lhs, rhs});
            auto error = exception_time_series(
                output, ErrorCaptureOptions{.trace_back_depth = 2, .capture_values = true});
            return wire<ErrorTraceOf>(w, error);
        }
    };

    struct DirectCaptureDepthZeroGraph
    {
        static constexpr auto name = "direct_capture_depth_zero_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> x)
        {
            auto output = wire<ThrowOnNegative>(w, x);
            auto error = exception_time_series(
                output, ErrorCaptureOptions{.trace_back_depth = 0});
            return wire<ErrorTraceOf>(w, error);
        }
    };
}  // namespace

TEST_CASE("uncaught errors use the executor diagnostic policy")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();
    GraphBuilder graph = build_graph<UncaughtErrorGraph>();
    set_replay_values<Int>(graph.global_state(), "input", values<Int>(-3));

    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(graph))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{2})
        .error_capture_options(ErrorCaptureOptions{
            .trace_back_depth = 2,
            .capture_values = true,
        });

    GraphExecutorValue executor = builder.make_executor();
    const ErrorCaptureOptions expected_options{
        .trace_back_depth = 2,
        .capture_values = true,
    };
    CHECK(executor.view().error_capture_options() == expected_options);
    try
    {
        executor.view().run();
        FAIL("the throwing node did not fail the graph run");
    }
    catch (const std::runtime_error &error)
    {
        const std::string message{error.what()};
        CHECK(message.find("negative input") != std::string::npos);
        CHECK(message.find("Activation Back Trace") != std::string::npos);
        CHECK(message.find("throw_on_negative") != std::string::npos);
        CHECK(message.find("value=-3") != std::string::npos);
        CHECK(message.find("replay") != std::string::npos);
    }
}

TEST_CASE("NodeError: a value-layer bundle with the reference fields")
{
    using namespace hgraph;

    REQUIRE(node_error_ts_meta() != nullptr);

    NodeErrorFields fields;
    fields.signature_name = "my_node";
    fields.error_msg      = "boom";
    Value error = make_node_error_value(fields);

    auto bundle = error.as_bundle();
    CHECK(bundle.at("signature_name").checked_as<Str>() == "my_node");
    CHECK(bundle.at("error_msg").checked_as<Str>() == "boom");
    CHECK(bundle.at("label").checked_as<Str>().empty());
    CHECK_FALSE(bundle.at("additional_context").valid());
}

TEST_CASE("error handling: exception_time_series captures a node throw")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<PerNodeCaptureGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    auto               gs = ex.view().graph().global_state();

    // The throw cycle (x=-3) ticks the error. This node throws before writing
    // output; write-then-throw output is deliberately unspecified.
    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(10, none, 14));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "negative input"s));
}

TEST_CASE("error handling: exception_time_series catches non-standard exceptions as unknown errors")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<PerNodeUnknownCaptureGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    auto gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(15, none, 21));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "unknown error"s));
}

TEST_CASE("error handling: mapped child errors are keyed and erased with child lifetime")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();

    const auto lhs = values<Value>(
        dict_delta<Int, TS<Int>>({{0, 10}}),
        dict_delta<Int, TS<Int>>({{1, 9}}),
        dict_delta<Int, TS<Int>>({{2, 8}}),
        dict_delta<Int, TS<Int>>({}, {1}));
    const auto rhs = values<Value>(
        dict_delta<Int, TS<Int>>({{0, 2}}),
        dict_delta<Int, TS<Int>>({{1, 0}}),
        dict_delta<Int, TS<Int>>({{2, 4}}),
        dict_delta<Int, TS<Int>>({}, {1}));

    CHECK_OUTPUT(eval_node<MappedErrorMessagesGraph>(lhs, rhs),
                 values<Value>(none,
                               dict_delta<Int, TS<Str>>({{1, "division by zero"s}}),
                               none,
                               dict_delta<Int, TS<Str>>({}, {1})));

    const auto traces = eval_node<MappedErrorTracesGraph>(lhs, rhs);
    REQUIRE(traces[1].has_value());
    const std::string trace_delta = traces[1]->to_string();
    CHECK(trace_delta.find("divide_or_throw") != std::string::npos);
    CHECK(trace_delta.find("*rhs*: value=0, delta=0") != std::string::npos);
}

TEST_CASE("error handling: a clean run never ticks the error output")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<PerNodeCaptureGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(1, 2, 3));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    auto gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(2, 4, 6));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>());
}

TEST_CASE("error handling: try_except over a sub-graph routes value and exception")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptValueGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    auto gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(10, none, 14));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "negative input"s));
}

TEST_CASE("error handling: try_except catches non-standard exceptions as unknown errors")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptUnknownGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    auto gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Int>(gs, "out"), values<Int>(15, none, 21));
    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "unknown error"s));
}

TEST_CASE("error handling: try_except over a sink sub-graph yields just the error series")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptSinkGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5, -3, 7));

    GraphExecutorValue ex = run_graph(std::move(gb), MIN_ST, MAX_ET);
    auto gs = ex.view().graph().global_state();

    CHECK_OUTPUT(get_recorded_values<Str>(gs, "err"), values<Str>(none, "sink negative"s));
}

TEST_CASE("error handling: registered try_except wires value graphs and sinks")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();

    const auto input = values<Int>(5, -3, 7);
    CHECK_OUTPUT(eval_node<ErasedTryValueGraph>(input), values<Int>(10, none, 14));
    CHECK_OUTPUT(eval_node<ErasedTryErrorGraph>(input), values<Str>(none, "negative input"s, none));
    CHECK_OUTPUT(eval_node<ErasedTrySinkGraph>(input), values<Str>(none, "sink negative"s, none));
    CHECK_OUTPUT(eval_node<DirectCaptureValueGraph>(input), values<Int>(10, none, 14));
    CHECK_OUTPUT(eval_node<DirectCaptureErrorGraph>(input), values<Str>(none, "negative input"s, none));
    CHECK_OUTPUT(eval_node<LiftedCaptureErrorGraph>(values<Int>(4, 4), values<Int>(2, 0)),
                 values<Str>(none, "div_: division by zero"s));
}

TEST_CASE("error handling: capture options include the failed node and input values")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();

    const auto direct = eval_node<DirectCaptureTraceGraph>(values<Int>(-3));
    REQUIRE(direct[0].has_value());
    CHECK(direct[0]->find("throw_on_negative") != std::string::npos);
    CHECK(direct[0]->find("*x*: value=-3, delta=-3") != std::string::npos);

    const auto nested = eval_node<ErasedTryTraceGraph>(values<Int>(-5));
    REQUIRE(nested[0].has_value());
    CHECK(nested[0]->find("throw_on_negative") != std::string::npos);
    CHECK(nested[0]->find("*x*: value=-5, delta=-5") != std::string::npos);

    const auto depth_zero = eval_node<DirectCaptureDepthZeroGraph>(values<Int>(-7));
    REQUIRE(depth_zero[0].has_value());
    CHECK(depth_zero[0]->find("throw_on_negative") != std::string::npos);
    CHECK(depth_zero[0]->find("*x*") == std::string::npos);
}

TEST_CASE("error handling: activation traces traverse structurally wired endpoint inputs")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    const auto bundle = eval_node<StructuredBundleCaptureTraceGraph>(
        values<Int>(9), values<Int>(0));
    REQUIRE(bundle[0].has_value());
    CHECK(bundle[0]->find("throw_on_zero_bundle") != std::string::npos);
    CHECK(bundle[0]->find("*values*: value={lhs: 9, rhs: 0}") != std::string::npos);
    CHECK(bundle[0]->find("replay") != std::string::npos);

    const auto list = eval_node<StructuredListCaptureTraceGraph>(
        values<Int>(9), values<Int>(0));
    REQUIRE(list[0].has_value());
    CHECK(list[0]->find("throw_on_zero_list") != std::string::npos);
    CHECK(list[0]->find("*values*: value=[9, 0]") != std::string::npos);
    CHECK(list[0]->find("replay") != std::string::npos);
}

TEST_CASE("error handling: try_except propagates child graph pauses")
{
    using namespace hgraph;
    using namespace hgraph::testing;

    GraphBuilder gb = build_graph<TryExceptPauseGraph>();
    set_replay_values<Int>(gb.global_state(), "x", values<Int>(5));

    MockRootGraph root{gb, MIN_ST, MAX_ET};
    auto          graph = root.graph();
    graph.start(MIN_ST);

    NodeView try_node;
    for (std::size_t index = 0; index < graph.node_count(); ++index)
    {
        auto node = graph.node_at(index);
        if (node.is<SingleNestedGraphNodeView>())
        {
            try_node = std::move(node);
            break;
        }
    }
    REQUIRE(try_node.valid());
    auto forwarded_out = walk_forwarding_target_path(try_node.output(MIN_ST), {1});
    CHECK(forwarded_out.forwarding());
    CHECK(forwarded_out.forwarding_bound());

    CHECK_FALSE(graph.evaluate(MIN_ST));
    CHECK(graph.evaluate(MIN_ST));
    graph.stop();

    CHECK_OUTPUT(get_recorded_values<Int>(graph.global_state(), "out"), values<Int>(10));
}
