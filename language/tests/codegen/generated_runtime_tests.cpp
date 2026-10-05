#include <runtime.h>
#include <sources.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/util/scope.h>
#include <spdlog/sinks/ostream_sink.h>
#include <sstream>

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

// The ordering the two storages depend on, checked by value rather than by
// reading the emitted text: `total` is seeded from the cache `seed`, and the
// cache `echo` is rebuilt from `total` AFTER a restore has supplied it.
TEST_CASE("a generated node seeds its storages in declaration order", "[codegen][runtime][cache][checkpoint]")
{
    session();
    GlobalContext                      context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(),
                                 {.component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    // seed 7 -> total 7 -> echo 7, so 8*10+7 and 10*10+7. States-first seeding
    // gave total the cache's default 0 and produced 10 and 30.
    CHECK_OUTPUT(eval_node_with_options<MixedComponent<runtime::operators::ordered_seed>>(interval(0, 2), values<Int>(1, 2)),
                 values<Int>(87, 107));
    REQUIRE(completed);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {.component_id = "strategy",
                                                          .load         = [&] { return std::optional{prior}; },
                                                          .commit = [&](const auto &image) { completed = image; }});
    // `total` resumes at 10, so `echo` rebuilds to 10 -- a cache taking its
    // value from restored state, which is the pairing ADR 0011 exists for.
    CHECK_OUTPUT(
        eval_node_with_options<MixedComponent<runtime::operators::ordered_seed>>(interval(2, 5), values<Int>(none, 3, 4)),
        values<Int>(none, 140, 180));
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


TEST_CASE("generated yields admit strictly increasing targets across skips and resumptions", "[codegen][runtime][adr-0015]") {
    namespace source = hgl::codegen::sources;
    using Catch::Matchers::ContainsSubstring;
    CHECK_OUTPUT(eval_node<source::ordered_targets>(MIN_ST - MIN_TD, MIN_ST), values<Int>(2));
    CHECK_OUTPUT(eval_node<source::increasing_past_targets>(), values<Int>(3));
    CHECK_OUTPUT(eval_node<source::ordered_targets>(MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2),
                 values<Int>(none, -1, 2));
    // Every invocation starts without a predecessor, even before the epoch.
    CHECK_OUTPUT(eval_node<source::increasing_past_targets>(), values<Int>(3));
    for (const auto first : {MIN_ST - MIN_TD, MIN_ST, MIN_ST + MIN_TD * 2}) {
        CHECK_THROWS_WITH(eval_node<source::ordered_targets>(first, first), ContainsSubstring("non-increasing time"));
        CHECK_THROWS_WITH(eval_node<source::ordered_targets>(first, first - MIN_TD), ContainsSubstring("non-increasing time"));
    }
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(MIN_TD, TimeDelta::zero()),
                      ContainsSubstring("non-increasing time"));
}

TEST_CASE("generated relative yields reject negative durations before checked target addition", "[codegen][runtime][adr-0015]") {
    namespace source = hgl::codegen::sources;
    using Catch::Matchers::ContainsSubstring;
    CHECK_OUTPUT(eval_node<source::relative_targets>(TimeDelta::zero(), MIN_TD), values<Int>(-1, 2));
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(-MIN_TD, MIN_TD), ContainsSubstring("negative duration"));
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(MIN_TD, -MIN_TD), ContainsSubstring("negative duration"));
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(TimeDelta::min(), MIN_TD), ContainsSubstring("negative duration"));
    CHECK_THROWS(eval_node<source::relative_targets>(TimeDelta::max(), MIN_TD));
}


TEST_CASE("generated yield operands run once in order, including skipped and rejected yields", "[codegen][runtime][adr-0015]") {
    namespace source = hgl::codegen::sources;
    std::ostringstream captured;
    const auto captured_lines = [&] {
        auto text = captured.str();
        for (auto pos = text.find("\r\n"); pos != std::string::npos; pos = text.find("\r\n", pos)) {
            text.erase(pos, 1);
        }
        return text;
    };
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
    auto logger = std::make_shared<spdlog::logger>("generator-operands-test", sink);
    logger->set_pattern("%v");
    log::set_logger(logger);
    const auto restore = make_scope_exit([]() noexcept { log::set_logger(nullptr); });
    SECTION("future operands are not reevaluated on resumption") {
        CHECK_OUTPUT(eval_node<source::observed_relative>(MIN_TD * 2), values<Int>(none, none, 7));
        CHECK(captured_lines() == "time\npayload\nafter\n");
    }
    SECTION("negative admission follows both operands and prevents continuation") {
        CHECK_THROWS_WITH(eval_node<source::observed_relative>(-MIN_TD),
                          Catch::Matchers::ContainsSubstring("negative duration"));
        CHECK(captured_lines() == "time\npayload\n");
    }
    SECTION("implicit overflow follows both operands and prevents continuation") {
        CHECK_THROWS(eval_node<source::observed_relative>(TimeDelta::max()));
        CHECK(captured_lines() == "time\npayload\n");
    }
    SECTION("past absolute targets evaluate their payload") {
        CHECK_OUTPUT(eval_node<source::observed_past>(), values<Int>(2));
        CHECK(captured_lines() == "payload\nafter\n");
    }
}

TEST_CASE("generated ordinary storage preserves nested owners and yielded temporaries", "[codegen][runtime][ordinary][adr-0015]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_literal_result>(values<Int>(1)), values<Int>(14));
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_nested_result>(values<Int>(1)), values<Bool>(true));
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_owned_delta_result>(values<Int>(1, 1)), values<Int>(10, 11));
}

TEST_CASE("generated const ordinary aggregates normalize configured storage before hooks", "[codegen][runtime][ordinary]") {
    session();
    ListBuilder builder{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
    builder.push_back(Int{10});
    builder.push_back(Int{20});
    auto configured = builder.build();
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_configured_lengths>(values<Int>(1), configured), values<Int>(23));
    CHECK(configured.as_list().size() == 2);
}

TEST_CASE("generated atomic list temporaries publish immediately and survive resumptions", "[codegen][runtime][ordinary][atomic]") {
    namespace source = hgl::codegen::sources;
    hgl::wiring::ensure_session();
    source::register_operators();
    const auto recorded = eval_node<source::operators::atomic_owned_yields>();
    const hgl::ordinary::PreparedValuePlan plan{scalar_descriptor<hgl::ordinary::List<Int>>::value_meta()};
    const auto snapshot = [&](std::initializer_list<Int> items) {
        auto value = plan.empty_list();
        for (const auto item : items) { plan.push(value.view(), Value{item}.view()); }
        return value;
    };
    REQUIRE(recorded.size() == 4);
    REQUIRE(recorded[0]);
    CHECK_FALSE(recorded[1]);
    REQUIRE(recorded[2]);
    REQUIRE(recorded[3]);
    CHECK(recorded[0]->equals(snapshot({1, 2})));
    CHECK(recorded[2]->equals(snapshot({3, 4})));
    CHECK(recorded[3]->equals(snapshot({})));
}

TEST_CASE("generated generic compositions preserve scalar and structural signal observations", "[codegen][runtime][signal]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::generic_scalar_signal_count>(values<Int>(none, 0, 0, none, -7)),
                 values<Int>(none, 1, 2, none, 3));
    CHECK_OUTPUT((eval_node<runtime::operators::generic_structural_signal_count, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{1, 0}}), none, list_delta<TS<Int>>({{1, 0}})))),
                 values<Int>(1, none, 2));
}

TEST_CASE("generated public enum markers retain nominal schemas and owning values", "[codegen][runtime][enum]") {
    session();
    using Mode = runtime::RuntimeMode;
    const auto low = Mode::member(INT64_MIN);
    const auto high = Mode::member(INT64_MAX);
    const auto first = Mode::member(-7);
    REQUIRE(low.schema() == scalar_descriptor<Mode>::value_meta());
    CHECK(low.schema()->is_enum());
    CHECK(low.schema() != scalar_descriptor<Int>::value_meta());
    CHECK_THROWS_AS(Mode::member(0), std::invalid_argument);
    CHECK_OUTPUT((eval_node<runtime::operators::enum_forward, TS<Mode>>(
        values<Value>(none, low, low, none, first, high, none))),
        values<Value>(none, low, low, none, first, high, none));
    CHECK_OUTPUT(eval_node<runtime::operators::enum_default_source>(values<Int>(1, 1)), values<Value>(first, first));
    CHECK_OUTPUT((eval_node<runtime::operators::enum_value_forward, TS<Mode>>(values<Value>(first, high))),
                 values<Value>(first, high));
    const auto sparse = [&](const Value &value) {
        MapBuilder builder{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta()), value.binding()};
        const Int index = 1;
        builder.set_item_copy(&index, value.view().data());
        return builder.build();
    };
    CHECK_OUTPUT((eval_node<runtime::operators::enum_fixed_forward, TSL<TS<Mode>, 2>>(
        values<Value>(sparse(first), none, sparse(high)))),
        values<Value>(sparse(first), none, sparse(high)));
}

TEST_CASE("generated scalar keys preserve signed zero and both infinities", "[codegen][runtime][scalar-keys]") {
    session();
    const Float infinity = std::numeric_limits<Float>::infinity();
    CHECK_OUTPUT((eval_node<runtime::operators::scalar_float_set, TSS<Float>>(
        values<Value>(set_delta<Float>({-0.0, infinity, -infinity}, {}), none,
                      set_delta<Float>({}, {0.0, infinity}), set_delta<Float>({infinity}, {-infinity})))),
        values<Value>(set_delta<Float>({0.0, -infinity, infinity}, {}), none,
                      set_delta<Float>({}, {-0.0, infinity}), set_delta<Float>({infinity}, {-infinity})));
    CHECK_OUTPUT((eval_node<runtime::operators::scalar_float_map, TSD<Float, TS<Int>>>(
        values<Value>(dict_delta<Float, TS<Int>>({{-0.0, 1}, {infinity, 2}, {-infinity, 3}}),
                      dict_delta<Float, TS<Int>>({{0.0, 1}}), none,
                      dict_delta<Float, TS<Int>>({}, {-0.0, infinity})))),
        values<Value>(dict_delta<Float, TS<Int>>({{0.0, 1}, {-infinity, 3}, {infinity, 2}}),
                      dict_delta<Float, TS<Int>>({{-0.0, 1}}), none,
                      dict_delta<Float, TS<Int>>({}, {0.0, infinity})));
}

TEST_CASE("generated scalar map recipes retain exact keys", "[codegen][runtime][scalar-keys]") {
    session();
    CHECK_OUTPUT((eval_node<runtime::operators::scalar_string_recipe>(values<Int>(1, none, 2))),
        values<Value>(dict_delta<Str, TS<Int>>({{"first", 1}, {"second", 2}}), none,
                      dict_delta<Str, TS<Int>>({{"second", 2}, {"first", 1}})));
}

TEST_CASE("generated ordinary collections retain runtime children and reject duplicates", "[codegen][runtime][atomic-collections]") {
    session();
    const auto snapshot = [](const Str &key, Int item) {
        ListBuilder row{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta()),
                        *scalar_descriptor<hgl::ordinary::List<Int>>::value_meta()};
        row.push_back(item);
        auto child = row.build();
        MapBuilder map{ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta()), child.binding()};
        map.set_item(Value{key}.view(), child.view());
        return map.build();
    };
    CHECK_OUTPUT(eval_node<runtime::operators::atomic_map_recipe>(values<Str>("row", none, "other"), values<Int>(1, none, 2)),
        values<Value>(snapshot("row", 1), none, snapshot("other", 2)));
    SetBuilder members{ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta())};
    members.insert(Value{Str{"alpha"}}.view());
    members.insert(Value{Str{"beta"}}.view());
    CHECK_OUTPUT(eval_node<runtime::operators::atomic_set_recipe>(values<Str>("alpha"), values<Str>("beta")),
        values<Value>(members.build()));
    CHECK_THROWS_WITH(eval_node<runtime::operators::atomic_set_recipe>(values<Str>("same"), values<Str>("same")),
        Catch::Matchers::ContainsSubstring("duplicate"));
}

TEST_CASE("generated map defaults retain the type of empty nested lists", "[codegen][runtime][atomic-collections]") {
    session();
    const auto recorded = eval_node<runtime::operators::atomic_collection_default>(values<Int>(1));
    REQUIRE(recorded.size() == 1);
    REQUIRE(recorded[0]);
    const auto map = recorded[0]->as_bundle().field("values").as_map();
    REQUIRE(map.size() == 1);
    const auto child = map.at(Value{Str{"empty"}}.view());
    CHECK(child.as_list().empty());
    CHECK(child.schema() == scalar_descriptor<hgl::ordinary::List<Int>>::value_meta());
}
