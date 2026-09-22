#include <runtime.h>

#include "wiring/backend.h"

#include <hgraph/lib/std/component.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace runtime = hgl::codegen::runtime;

namespace
{
    void session()
    {
        hgl::wiring::ensure_session();
        runtime::register_operators();
    }
}  // namespace

TEST_CASE("generated module registration owns a removable provider generation", "[codegen][runtime][lifecycle]")
{
    hgl::wiring::ensure_session();
    auto provider = runtime::register_operators();
    auto same = runtime::register_operators();
    CHECK(provider.valid());
    CHECK(provider.active());
    CHECK(same.active());
    CHECK(provider.key() == "hgl.codegen.runtime");
    CHECK(OperatorRegistry::instance().remove_provider(provider));
    CHECK_FALSE(provider.active());
    CHECK_FALSE(same.active());

    auto replacement = runtime::register_operators();
    CHECK(replacement.active());
    CHECK_FALSE(OperatorRegistry::instance().remove_provider(provider));
    CHECK(OperatorRegistry::instance().remove_provider(replacement));
}

TEST_CASE("generated runtime impl functions register as node overloads", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("a generated composition can wire a generated operator implementation", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute_graph>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("a generated runtime operator consumes a homogeneous argument pack", "[codegen][runtime][parameter-pack]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::all_runtime_graph>(values<Bool>(true, true), values<Bool>(false, true)),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<runtime::operators::positional_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})), values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::named_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})), values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::homogeneous_schema_count_graph>(values<Float>(1.0), values<Float>(2.0)),
                 values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::triggered_schema_count_graph>(
                     values<Float>(1.0, none, 2.0), values<Float>(none, 10.0, none), values<Float>(none, none, 20.0)),
                 values<Int>(2, none, 2));
    CHECK_OUTPUT(eval_node<runtime::operators::positional_schema_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})),
                 values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::named_schema_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})),
                 values<Int>(2));
    REQUIRE_THROWS_AS(eval_node<runtime::operators::all_runtime>(values<Bool>(true)), OperatorResolutionError);
}

TEST_CASE("generated runtime control flow definitely assigns typed locals", "[codegen][runtime][locals]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute_local>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("generated runtime predicates use modified-or and valid-and semantics", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::add_when_ready>(values<Float>(1.0, none, 3.0),
                                                        values<Float>(none, 10.0, 20.0)),
                 values<Float>(none, 11.0, 23.0));
}

TEST_CASE("generated activation analysis leaves sampled inputs passive", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::sample_on_trigger>(values<Float>(1.0, none, 2.0),
                                                           values<Float>(10.0, 20.0, none)),
                 values<Float>(10.0, none, 20.0));
}

TEST_CASE("generated runtime metadata reads the input selector", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::updated_at>(values<Float>(1.0, none, 2.0)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD));
}

TEST_CASE("generated signal inputs observe ticks without exposing payloads", "[codegen][runtime][signal]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::count_ticks>(values<Float>(1.0, 2.0, 3.0)), values<Int>(1, 2, 3));
    CHECK_OUTPUT(eval_node<runtime::operators::count_float_ticks>(values<Float>(1.0, 2.0, 3.0)), values<Int>(1, 2, 3));
}

TEST_CASE("generated runtime handlers share recordable state and run in source order", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::combined_total>(values<Float>(10.0, none, 2.0),
                                                        values<Float>(3.0, 1.0, none)),
                 values<Float>(7.0, 6.0, 8.0));
}

TEST_CASE("a generated composition can wire a generated runtime node", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::combined_total_graph>(values<Float>(10.0, none, 2.0),
                                                              values<Float>(3.0, 1.0, none)),
                 values<Float>(7.0, 6.0, 8.0));
}

TEST_CASE("generated inject out exposes the previous value and writes non-terminally", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::running_total>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("generated HGL mutates set outputs through the functional facade", "[codegen][runtime][collection]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::mutate_set>(values<Int>(1, 2)),
                 values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1})));
}

TEST_CASE("generated HGL mutates map outputs through the functional facade", "[codegen][runtime][collection]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::mutate_map>(values<Int>(1, 2, 3)),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
                               dict_delta<Str, TS<Int>>({{"b", 4}, {"c", 5}}, {"a"}),
                               dict_delta<Str, TS<Int>>({}, {"b", "c"})));
}

TEST_CASE("generated HGL mutates unbounded list outputs through the functional facade",
          "[codegen][runtime][collection]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::mutate_list>(values<Int>(1, 2, 3)),
                 values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                               dynamic_list_delta<TS<Int>>({{1, 3}}),
                               dynamic_list_delta<TS<Int>>({}, {0, 1})));
}

TEST_CASE("generated runtime lifecycle hooks run around evaluation", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::lifecycle_value>(values<Float>(4.0, 5.0)),
                 values<Float>(4.0, 5.0));
}

TEST_CASE("generated state initializers can use const parameters", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::configured_total>(values<Float>(1.0, 2.0), arg<"initial">(Float{5.0})),
                 values<Float>(6.0, 8.0));
}

TEST_CASE("generated runtime functions without when use ordinary input policy", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::unconditional_total>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("a generated composition can wire a private generated runtime node", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::private_total_graph>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("generated cache fields share one native slot and reinitialize on a new run", "[codegen][runtime][cache]") {
    session();
    for (int run = 0; run != 2; ++run) {
        CHECK_OUTPUT(eval_node<runtime::operators::cache_bundle>(values<Int>(2, 4, 9)), values<Float>(2.0, 3.0, 5.0));
    }
}

TEST_CASE("generated fixed-list scalar reads use guarded child validity", "[codegen][runtime]") {
    session();
    CHECK_OUTPUT((eval_node<runtime::operators::first_scalar, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{1, 9}}), list_delta<TS<Int>>({{0, 3}}), list_delta<TS<Int>>({{1, 10}}),
                                   list_delta<TS<Int>>({{0, 7}})))),
                 values<Int>(none, 3, 3, 7));
}

namespace
{
    template <typename Op> struct RecoverySource
    {
        static Port<TS<Int>> compose(Wiring &w) { return wire<Op>(w).template as<TS<Int>>(); }
    };
    template <typename Op> struct RecoverySourceComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>)
        {
            return stdlib::component<RecoverySource<Op>>(w, "source");
        }
    };
}

TEST_CASE("generated sources with caches or external clocks refuse checkpoint admission", "[codegen][runtime][checkpoint]")
{
    session();
    GlobalContext context;
    configure_component_recovery(context.state().view(), {.component_id = "source", .commit = [](const ComponentCheckpoint &) {}});
    CHECK_THROWS_WITH(eval_node<RecoverySourceComponent<runtime::operators::cached_source>>(values<Int>(none)),
                      Catch::Matchers::ContainsSubstring("unsupported node"));
    CHECK_THROWS_WITH(eval_node<RecoverySourceComponent<runtime::operators::clock_source>>(values<Int>(none)),
                      Catch::Matchers::ContainsSubstring("unsupported node"));
}

namespace
{
    template <typename Op> struct MixedStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input) { return wire<Op>(w, input).template as<TS<Int>>(); }
    };
    template <typename Op> struct MixedComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<MixedStrategy<Op>>(w, "strategy", input);
        }
    };
    EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }
}

// The pair that ADR 0011 gates the mixed case on: one generated node owning a
// `RecordableState<>` and a `State<>` at once, and the two behaving
// DIFFERENTLY across a restore. `mixed_total` returns `total * 10 + seen`, so
// one output reads both storages.
TEST_CASE("a generated node restores its state and rebuilds its cache", "[codegen][runtime][cache][checkpoint]")
{
    session();
    GlobalContext                      context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(),
                                 {.component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<MixedComponent<runtime::operators::mixed_total>>(interval(0, 2), values<Int>(1, 2)),
                 values<Int>(11, 32));
    REQUIRE(completed);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {.component_id = "strategy",
                                                          .load         = [&] { return std::optional{prior}; },
                                                          .commit = [&](const auto &image) { completed = image; }});
    // `total` is recordable and resumes at 3: 3+3=6, then 6+4=10. `seen` is a
    // cache, so `start` rebuilds it from its initializer and it counts 1, 2
    // again -- had it been restored too, this would read 63 and 104.
    CHECK_OUTPUT(
        eval_node_with_options<MixedComponent<runtime::operators::mixed_total>>(interval(2, 5), values<Int>(none, 3, 4)),
        values<Int>(none, 61, 102));
}

TEST_CASE("a generated node's state schema and cache struct both survive a restore whole",
          "[codegen][runtime][cache][checkpoint]")
{
    // Two fields on each side, so neither storage can be standing in for the
    // other: `(total + seen) * scale + count`.
    session();
    GlobalContext                      context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(),
                                 {.component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<MixedComponent<runtime::operators::mixed_bundle>>(interval(0, 2), values<Int>(1, 2)),
                 values<Int>(5, 12));
    REQUIRE(completed);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {.component_id = "strategy",
                                                          .load         = [&] { return std::optional{prior}; },
                                                          .commit = [&](const auto &image) { completed = image; }});
    // Both state fields resume (total 3, seen 2); both cache fields are rebuilt
    // (count 0, scale 2), so (6+3)*2+1 = 19 and then (10+4)*2+2 = 30.
    CHECK_OUTPUT(
        eval_node_with_options<MixedComponent<runtime::operators::mixed_bundle>>(interval(2, 5), values<Int>(none, 3, 4)),
        values<Int>(none, 19, 30));
}

TEST_CASE("a generated mixed node re-initializes both storages on a fresh run", "[codegen][runtime][cache]")
{
    // No checkpoint: `state` has nothing to restore, so a second run repeats
    // the first exactly. This is the construction half of the contract.
    session();
    for (int run = 0; run != 2; ++run) {
        CHECK_OUTPUT(eval_node<runtime::operators::mixed_total>(values<Int>(1, 2)), values<Int>(11, 32));
        CHECK_OUTPUT(eval_node<runtime::operators::mixed_bundle>(values<Int>(1, 2)), values<Int>(5, 12));
    }
}
