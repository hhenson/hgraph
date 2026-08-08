#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <span>
#include <string>
#include <vector>

// The Python-bridge contract test: everything here wires graphs WITHOUT any
// C++ operator/schema templates — operators resolve by NAME with runtime
// WiringArg values against interned runtime schemas, exactly the three calls a
// Python frontend makes (build args -> OperatorRegistry::resolve ->
// impl->wire). A capability that cannot be exercised this way is, by
// definition, unreachable from Python (memory: python-port-operator-compat).

namespace
{
    using namespace hgraph;

    WiringArg ts_arg(WiringPortRef port, std::string name = {})
    {
        WiringArg arg;
        arg.kind = WiringArg::Kind::TimeSeries;
        arg.port = std::move(port);
        arg.name = std::move(name);
        return arg;
    }

    WiringArg scalar_arg(Value value, const ValueTypeMetaData *meta, std::string name = {})
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_value = std::move(value);
        arg.scalar_meta  = meta;
        arg.name         = std::move(name);
        return arg;
    }

    // The whole bridge surface: resolve by name, wire the winner. Passes the
    // wiring's operator state exactly as the Python bridge (PyWiring::wire)
    // does, so record/replay model selection matches.
    OperatorWireResult call_operator(Wiring &w,
                                     std::string_view name,
                                     std::vector<WiringArg> args,
                                     std::optional<bool> output_required = std::nullopt,
                                     const TSValueTypeMetaData *expected_output = nullptr)
    {
        ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
            name, std::span<const WiringArg>{args.data(), args.size()}, output_required, expected_output, {},
            w.operator_state(), &w);
        return resolved.impl->wire(w, resolved.map, resolved.args, resolved.kwargs);
    }

    // The raw stateless bridge records densely (mirrors PyWiring's default):
    // by-name record/replay bind to the cycle-aligned harness backend.
    void use_dense_recording(Wiring &w)
    {
        record_replay::set_config(
            w.global_state(),
            record_replay::Config{.model = std::string{record_replay::IN_MEMORY_DENSE}});
    }

    struct RuntimeMetas
    {
        const ValueTypeMetaData   *int_meta{nullptr};
        const ValueTypeMetaData   *str_meta{nullptr};
        const TSValueTypeMetaData *ts_int{nullptr};
    };

    RuntimeMetas runtime_metas()
    {
        auto &registry = TypeRegistry::instance();
        RuntimeMetas metas;
        metas.int_meta = registry.register_scalar<Int>("int");
        metas.str_meta = registry.register_scalar<Str>("str");
        metas.ts_int   = registry.ts(metas.int_meta);
        return metas;
    }

    std::vector<std::optional<Value>> run_and_read(Wiring &&w, std::string_view out_key)
    {
        GraphBuilder gb = std::move(w).finish();

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();
        return testing::get_recorded_deltas(view.graph().global_state(), out_key);
    }
}  // namespace

TEST_CASE("erased wiring: a full graph wires and runs via name-resolved operators only")
{
    hgraph::stdlib::register_standard_operators();
    const auto metas = runtime_metas();

    Wiring w;
    use_dense_recording(w);
    auto lhs = call_operator(w, "const", {scalar_arg(Value{Int{42}}, metas.int_meta)}, true, metas.ts_int);
    auto rhs = call_operator(w, "const", {scalar_arg(Value{Int{8}}, metas.int_meta)}, true, metas.ts_int);
    REQUIRE(lhs.has_output);
    REQUIRE(rhs.has_output);

    auto sum = call_operator(w, "add_", {ts_arg(lhs.output.erased()), ts_arg(rhs.output.erased())}, true);
    REQUIRE(sum.has_output);

    auto sink = call_operator(
        w, "record",
        {ts_arg(sum.output.erased()), scalar_arg(Value{Str{"erased::out"}}, metas.str_meta)},
        false);
    CHECK_FALSE(sink.has_output);

    auto recorded = run_and_read(std::move(w), "erased::out");
    REQUIRE(recorded.size() == 1);
    REQUIRE(recorded[0].has_value());
    CHECK(recorded[0]->view().checked_as<Int>() == Int{50});
}

TEST_CASE("erased wiring: keyword arguments and defaults resolve as in Python calls")
{
    hgraph::stdlib::register_standard_operators();
    const auto metas = runtime_metas();

    Wiring w;
    use_dense_recording(w);
    // const(value=7): keyword scalar; the ``delay`` param materialises from its default.
    auto src = call_operator(
        w, "const", {scalar_arg(Value{Int{7}}, metas.int_meta, "value")}, true, metas.ts_int);
    REQUIRE(src.has_output);

    // add_(rhs=…, lhs=…): keyword time-series in swapped order.
    auto sum = call_operator(
        w, "add_",
        {ts_arg(src.output.erased(), "rhs"), ts_arg(src.output.erased(), "lhs")}, true);
    REQUIRE(sum.has_output);

    static_cast<void>(call_operator(
        w, "record",
        {ts_arg(sum.output.erased()), scalar_arg(Value{Str{"erased::kw"}}, metas.str_meta, "key")},
        false));

    auto recorded = run_and_read(std::move(w), "erased::kw");
    REQUIRE(recorded.size() == 1);
    REQUIRE(recorded[0].has_value());
    CHECK(recorded[0]->view().checked_as<Int>() == Int{14});
}

TEST_CASE("erased wiring: expected-output schema drives generic source resolution")
{
    hgraph::stdlib::register_standard_operators();
    const auto metas = runtime_metas();

    // ``replay`` has no input to infer from — Out<TsVar<"O">> resolves purely
    // from the expected output schema, the exact shape a Python ``replay[TS[int]]``
    // call produces.
    Wiring w;
    use_dense_recording(w);
    auto src = call_operator(
        w, "replay", {scalar_arg(Value{Str{"erased::in"}}, metas.str_meta)}, true, metas.ts_int);
    REQUIRE(src.has_output);

    auto sum = call_operator(
        w, "add_", {ts_arg(src.output.erased()), ts_arg(src.output.erased())}, true);
    static_cast<void>(call_operator(
        w, "record",
        {ts_arg(sum.output.erased()), scalar_arg(Value{Str{"erased::doubled"}}, metas.str_meta)},
        false));

    GraphBuilder gb = std::move(w).finish();
    testing::set_replay_deltas(gb.global_state(), "erased::in",
                               {Value{Int{1}}, std::nullopt, Value{Int{3}}});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
    GraphExecutorValue executor = eb.make_executor();
    auto               view     = executor.view();
    view.run();

    auto recorded = testing::get_recorded_deltas(view.graph().global_state(), "erased::doubled");
    REQUIRE(recorded.size() == 3);
    CHECK(recorded[0]->view().checked_as<Int>() == Int{2});
    CHECK_FALSE(recorded[1].has_value());
    CHECK(recorded[2]->view().checked_as<Int>() == Int{6});
}

TEST_CASE("erased wiring: resolution failures surface as OperatorResolutionError")
{
    hgraph::stdlib::register_standard_operators();
    const auto metas = runtime_metas();

    Wiring w;
    CHECK_THROWS_AS(
        call_operator(w, "no_such_operator", {scalar_arg(Value{Int{1}}, metas.int_meta)}),
        OperatorResolutionError);
    // Arity/type mismatch on a known name is also a resolution error.
    CHECK_THROWS_AS(call_operator(w, "add_", {}), OperatorResolutionError);
}

TEST_CASE("wiring snapshot: build and run the graph-so-far, keep wiring (notebook contract)")
{
    // Design record: developer_guide/notebook.rst. snapshot() builds a
    // runnable graph from the CURRENT wiring state without consuming it -
    // the wiring keeps accepting nodes and can snapshot again, and the
    // consuming finish() contract is unchanged afterwards.
    hgraph::stdlib::register_standard_operators();
    const auto metas = runtime_metas();

    Wiring w;
    use_dense_recording(w);

    const auto run_snapshot = [&](std::string_view key) {
        GraphBuilder gb = w.snapshot();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();
        return testing::get_recorded_deltas(view.graph().global_state(), key);
    };

    // "cell 1": a recorded source; snapshot-run evaluates it.
    auto first = call_operator(w, "const", {scalar_arg(Value{Int{42}}, metas.int_meta)}, true, metas.ts_int);
    REQUIRE(first.has_output);
    static_cast<void>(call_operator(
        w, "record",
        {ts_arg(first.output.erased()), scalar_arg(Value{Str{"snap::first"}}, metas.str_meta)}, false));
    auto ticks = run_snapshot("snap::first");
    REQUIRE(ticks.size() == 1);
    CHECK(ticks[0].value().view().checked_as<Int>() == 42);

    // "cell 2": the SAME wiring keeps accepting nodes; a second snapshot
    // evaluates the grown graph.
    auto second = call_operator(w, "const", {scalar_arg(Value{Int{7}}, metas.int_meta)}, true, metas.ts_int);
    REQUIRE(second.has_output);
    static_cast<void>(call_operator(
        w, "record",
        {ts_arg(second.output.erased()), scalar_arg(Value{Str{"snap::second"}}, metas.str_meta)}, false));
    ticks = run_snapshot("snap::second");
    REQUIRE(ticks.size() == 1);
    CHECK(ticks[0].value().view().checked_as<Int>() == 7);

    // The consuming finish() path is untouched and still runs afterwards.
    auto final_ticks = run_and_read(std::move(w), "snap::first");
    REQUIRE(final_ticks.size() == 1);
    CHECK(final_ticks[0].value().view().checked_as<Int>() == 42);
}
