#include "spawn_test_graphs.h"
#include <hgraph/lib/testing/check_output.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
namespace {
    void compare(const Trace &actual, const Trace &expected)
    {
        actual.load();
        REQUIRE(actual.starts == 1);
        REQUIRE(actual.stops == 1);
        REQUIRE(actual.samples.size() == expected.samples.size());
        for (std::size_t i = 0; i < actual.samples.size(); ++i)
        {
            CAPTURE(i);
            CHECK(actual.samples[i].time == expected.samples[i].time);
            CHECK(actual.samples[i].valid == expected.samples[i].valid);
            CHECK(actual.samples[i].window_times == expected.samples[i].window_times);
            CHECK(actual.samples[i].value == expected.samples[i].value);
            CHECK(actual.samples[i].delta == expected.samples[i].delta);
            CHECK(actual.samples[i].pid != process_id());
        }
    }
    template <typename S, typename T>
    void boundary_oracle(const std::vector<std::optional<T>> &input)
    {
        Trace expected, direct, pipeline;
        (void)eval_node<CaptureGraph<S, false>>(input, arg<"trace">(&expected));
        (void)eval_node<CaptureGraph<S, true>>(input, arg<"trace">(&direct));
        (void)eval_node<CaptureGraph<S, true, true>>(input, arg<"trace">(&pipeline));
        compare(direct, expected);
        compare(pipeline, expected);
    }
}

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("spawn: direct sink and pass-through pipeline preserve scalar and signal traces", "[spawn]")
{
    prepare();
    boundary_oracle<TS<Int>>(values<Int>(1, none, 3, 4, none, 6));
    boundary_oracle<SIGNAL>(values<bool>(true, none, true));
}

TEST_CASE("spawn: collection boundaries preserve values deltas and membership", "[spawn]")
{
    prepare();
    SECTION("set")
    { boundary_oracle<TSS<Int>>(values<Value>(set_delta<Int>({1, 2}, {}), none,
                                             set_delta<Int>({3}, {1}), set_delta<Int>({}, {2, 3}))); }
    SECTION("dictionary")
    { boundary_oracle<Dict>(values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
        dict_delta<Str, TS<Int>>({}, {"a"}), dict_delta<Str, TS<Int>>({{"a", 3}}))); }
    SECTION("fixed list")
    { boundary_oracle<TSL<TS<Int>, 2>>(values<Value>(list_delta<TS<Int>>({1, std::nullopt}),
        list_delta<TS<Int>>({std::nullopt, 2}), list_delta<TS<Int>>({3, 4}))); }
    SECTION("dynamic list")
    { boundary_oracle<TSL<TS<Int>>>(values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {3, 4}}),
        dynamic_list_delta<TS<Int>>({}, {3}), dynamic_list_delta<TS<Int>>({{3, 5}}))); }
    SECTION("partial nested bundle")
    {
        using Row = TSB<"SpawnTestRow", Field<"value", TS<Int>>, Field<"label", TS<Str>>>;
        boundary_oracle<Row>(values<Value>(tsb_delta<Row>(Int{1}, std::nullopt),
            tsb_delta<Row>(std::nullopt, Str{"one"}), tsb_delta<Row>(Int{2}, Str{"two"})));
        boundary_oracle<TSD<Str, TSS<Int>>>(values<Value>(
            dict_delta<Str, TSS<Int>>({{"x", set_delta<Int>({1, 2}, {})}}),
            dict_delta<Str, TSS<Int>>({{"x", set_delta<Int>({3}, {1})}}),
            dict_delta<Str, TSS<Int>>({}, {"x"})));
    }
}

TEST_CASE("spawn: count and duration windows preserve history through stage boundaries", "[spawn]")
{
    prepare();
    boundary_oracle<TSW<Int, 3, 1>>(values<Int>(1, 2, none, 4, 5, 6));
    boundary_oracle<TSWDuration<Int, 3, 0>>(values<Int>(1, none, 3, none, none, none, 7));
}

TEST_CASE("spawn: named external inputs align with flow at original timestamps", "[spawn]")
{
    prepare();
    Trace expected, actual;
    auto flow = values<Int>(1, none, 3, none, 5);
    auto side = values<Int>(10, 20, 30, 40, 50);
    (void)eval_node<BoundGraph<false>>(flow, side, arg<"trace">(&expected));
    (void)eval_node<BoundGraph<true>>(flow, side, arg<"trace">(&actual));
    compare(actual, expected);
}

TEST_CASE("spawn: no-input child requests scheduled work after parent inputs finish", "[spawn]")
{
    prepare();
    Trace trace;
    (void)eval_node<SpawnTimer>(values<Int>(7), arg<"trace">(&trace));
    trace.load();
    REQUIRE(trace.starts == 1);
    REQUIRE(trace.stops == 1);
    REQUIRE(trace.samples.size() == 3);
    for (std::size_t i = 0; i < 3; ++i)
    {
        CHECK(trace.samples[i].time == MIN_ST + MIN_TD * static_cast<Int>(i + 1));
        CHECK(trace.samples[i].pid != process_id());
    }
}

TEST_CASE("spawn: failures propagate and invalid plans fail closed", "[spawn]")
{
    prepare();
    CHECK_THROWS((eval_node<SpawnFailure>(values<Int>(1))));
    Trace trace;
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Output>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Empty>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Missing>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Duplicate>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Ambiguous>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::IntermediateSink>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::ZeroCapacity>>(values<Int>(1), arg<"trace">(&trace))));
    CHECK_THROWS((eval_node<InvalidGraph<Invalid::Oversize>>(values<Int>(1), arg<"trace">(&trace))));
}

TEST_CASE("spawn: ordinary map reduce and mesh retain keyed semantics inside a stage", "[spawn]")
{
    prepare();
    const auto input = values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
        dict_delta<Str, TS<Int>>({{"b", 4}}), dict_delta<Str, TS<Int>>({}, {"a"}),
        dict_delta<Str, TS<Int>>({{"a", 5}}));
    auto check = [&]<NestedKind Kind>() {
        Trace expected, actual;
        (void)eval_node<NestedGraph<Kind, false>>(input, arg<"trace">(&expected));
        (void)eval_node<NestedGraph<Kind, true>>(input, arg<"trace">(&actual));
        compare(actual, expected);
    };
    check.template operator()<NestedKind::Map>();
    check.template operator()<NestedKind::Reduce>();
    check.template operator()<NestedKind::Mesh>();
}

TEST_CASE("spawn: reference retargeting materializes values at each boundary", "[spawn]")
{
    prepare();
    Trace expected, actual;
    const auto choose = values<Bool>(false, none, true, none, false);
    const auto lhs = values<Int>(1, 2, 3, 4, 5);
    const auto rhs = values<Int>(10, 20, 30, 40, 50);
    (void)eval_node<ReferenceGraph<false>>(choose, lhs, rhs, arg<"trace">(&expected));
    (void)eval_node<ReferenceGraph<true>>(choose, lhs, rhs, arg<"trace">(&actual));
    compare(actual, expected);
}

TEST_CASE("spawn: downstream timers request progress through idle stages", "[spawn]")
{
    prepare();
    Trace trace;
    (void)eval_node<IdlePipeline>(values<Int>(7), arg<"trace">(&trace));
    trace.load();
    REQUIRE(trace.samples.size() == 3);
    for (std::size_t i = 0; i < 3; ++i)
        CHECK(trace.samples[i].time == MIN_ST + MIN_TD * static_cast<Int>(i + 1));
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: no-input timers respect the enclosing exclusive end time", "[spawn]")
{
    prepare();
    Trace trace;
    (void)eval_node_with_options<SpawnTimer>(
        {MIN_ST, MIN_ST + MIN_TD * 2}, values<Int>(7), arg<"trace">(&trace));
    trace.load();
    REQUIRE(trace.samples.size() == 1);
    CHECK(trace.samples.front().time == MIN_ST + MIN_TD);
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: child stop fails the owning run instead of dropping accepted input", "[spawn]")
{
    prepare();
    CHECK_THROWS_WITH((eval_node<SpawnStop>(values<Int>(1, 2, 3))),
                      Catch::Matchers::ContainsSubstring("child requested stop"));
}

TEST_CASE("spawn: failed owner cycle never grants its pending input frame", "[spawn]")
{
    prepare();
    Trace trace;
    CHECK_THROWS_WITH((eval_node<FailingOwner>(values<Int>(1), arg<"trace">(&trace))),
                      Catch::Matchers::ContainsSubstring("owner cycle failed"));
    trace.load();
    CHECK(trace.samples.empty());
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: child stop during start fails and joins the worker", "[spawn]")
{
    prepare();
    CHECK_THROWS_WITH((eval_node<SpawnStopOnStart>(values<Int>(1))),
                      Catch::Matchers::ContainsSubstring("child requested stop during start"));
}

TEST_CASE("spawn: crashed and unresponsive processes fail within the configured deadline", "[spawn]")
{
    prepare();
    auto before = std::chrono::steady_clock::now();
    CHECK_THROWS((eval_node<ProcessFailureGraph<Crash>>(values<Int>(1, 2))));
    CHECK_THROWS((eval_node<ProcessFailureGraph<Hang>>(values<Int>(1, 2))));
    CHECK(std::chrono::steady_clock::now() - before < std::chrono::seconds{10});
}

TEST_CASE("spawn: start and stop failures cross the process boundary", "[spawn]")
{
    prepare();
    CHECK_THROWS_WITH((eval_node<ProcessFailureGraph<StartFailure>>(values<Int>(1))),
        Catch::Matchers::ContainsSubstring("spawn start failure"));
    CHECK_THROWS_WITH((eval_node<ProcessFailureGraph<StopFailure>>(values<Int>(1))),
        Catch::Matchers::ContainsSubstring("spawn stop failure"));
}

TEST_CASE("spawn: mapped workers start at their activation time", "[spawn]")
{
    prepare();
    GlobalContext context;
    Trace trace;
    (void)eval_node<MappedSpawnTimer>(values<Value>(none, none, none, none,
        dict_delta<Str, TS<Int>>({{"a", 1}})), arg<"trace">(&trace));
    trace.load();
    REQUIRE(trace.samples.size() == 3);
    for (std::size_t i = 0; i < 3; ++i)
        CHECK(trace.samples[i].time == MIN_ST + MIN_TD * static_cast<Int>(5 + i));
    CHECK(trace.starts == 1);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: real-time end guard bounds a self-rescheduling child", "[spawn]")
{
    prepare();
    Trace trace;
    // An already elapsed wall-clock bound makes this deterministic and fast.
    (void)eval_node_with_options<SpawnRescheduling>(
        {MIN_ST, MIN_ST + TimeDelta{1'000'000}, GraphExecutorMode::RealTime},
        values<Int>(1), arg<"trace">(&trace));
    trace.load();
    REQUIRE_FALSE(trace.samples.empty());
    CHECK(trace.samples.size() <= 1025);
    CHECK(trace.stops == 1);
}

TEST_CASE("spawn: idle worker exit wakes a real-time owner before end time", "[spawn]")
{
    prepare();
    const auto start = std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
    const auto before = std::chrono::steady_clock::now();
    CHECK_THROWS_WITH((eval_node_with_options<ProcessFailureGraph<IdleCrash>>(
        {start, start + TimeDelta{10'000'000}, GraphExecutorMode::RealTime}, values<Int>(1))),
        Catch::Matchers::ContainsSubstring("worker process exited while idle (31)"));
    CHECK(std::chrono::steady_clock::now() - before < std::chrono::seconds{5});
}
