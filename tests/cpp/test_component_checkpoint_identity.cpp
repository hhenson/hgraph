#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

namespace {
using namespace hgraph;
using namespace hgraph::testing;

using ResultPair = TSB<"CheckpointIdentityPair", Field<"left", TS<Int>>, Field<"right", TS<Int>>>;
struct ProducePair {
    static void eval(In<"ts", TS<Int>> ts, Out<ResultPair> out) {
        out.field<"left">().set(ts.value() * 10);
        out.field<"right">().set(ts.value() * 100);
    }
};
template<bool Right> struct ProjectedBody {
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts) {
        const auto pair = wire<ProducePair>(w, ts);
        return Port<TS<Int>>{w, pair.node(), {Right ? 1U : 0U}};
    }
};
template<bool Swap> struct StructuralBody {
    static Port<ResultPair> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts) {
        const auto pair = wire<ProducePair>(w, ts);
        const Port<TS<Int>> left{w, pair.node(), {0}};
        const Port<TS<Int>> right{w, pair.node(), {1}};
        return stdlib::to_tsb<ResultPair>(w, Swap ? right : left, Swap ? left : right);
    }
};
template<bool Swap> struct PartialStructuralBody {
    static Port<ResultPair> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts) {
        const Port<TS<Int>> absent{w, WiringPortRef::null_source(schema_descriptor<TS<Int>>::ts_meta())};
        return stdlib::to_tsb<ResultPair>(w, Swap ? absent : Port<TS<Int>>{ts}, Swap ? Port<TS<Int>>{ts} : absent);
    }
};
template<bool Right> struct NestedBody {
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts) {
        return stdlib::component<ProjectedBody<Right>>(w, "inner", ts);
    }
};
template<typename Body> struct SelectedComponent {
    static auto compose(Wiring &w, Port<TS<Int>> ts) {
        return stdlib::component<Body>(w, "selection", ts);
    }
};
template<bool Right> struct ConstantBody {
    static Port<TS<Int>> compose(Wiring &w) {
        auto left = wire<stdlib::const_>(w, Int{10}).as<TS<Int>>();
        auto right = wire<stdlib::const_>(w, Int{100}).as<TS<Int>>();
        return Right ? right : left;
    }
};
template<bool Right> struct ConstantComponent {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>) {
        return stdlib::component<ConstantBody<Right>>(w, "selection");
    }
};

EvalNodeRunOptions interval(Int begin, Int end) {
    return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
}
template<typename Original, typename Changed> void rejects_changed_result() {
    GlobalContext context;
    std::optional<ComponentCheckpoint> saved;
    std::size_t commits{};
    configure_component_recovery(context.state().view(), {
        .component_id = "selection", .load = [&] { return saved; },
        .commit = [&](const auto &image) { saved = image; ++commits; }});
    const auto first = eval_node_with_options<Original>(interval(0, 1), values<Int>(1));
    REQUIRE(first.size() == 1);
    REQUIRE(first[0].has_value());
    REQUIRE(saved);
    auto resumed = eval_node_with_options<Original>(interval(1, 2), values<Int>(none));
    REQUIRE(resumed.size() <= 1);
    resumed.resize(1);
    CHECK_FALSE(resumed[0].has_value());
    REQUIRE(commits == 2);
    CHECK_THROWS_WITH(eval_node_with_options<Changed>(interval(2, 3), values<Int>(none)),
                      Catch::Matchers::ContainsSubstring("incompatible"));
    CHECK(commits == 2);
}

struct CopyMappedDelta {
    static void eval(In<"value", TsVar<"S">> value, Out<TsVar<"S">> out) {
        apply_delta(out, capture_delta(value.base()).view());
    }
};
struct SignalMapBody {
    static Port<TSD<Str, SIGNAL>> compose(Wiring &w, NamedPort<"values", TSD<Str, SIGNAL>> values) {
        return wire<stdlib::map_>(w, fn<CopyMappedDelta>(), values).as<TSD<Str, SIGNAL>>();
    }
};
struct SignalMapComponent {
    static Port<TSD<Str, SIGNAL>> compose(Wiring &w, Port<TSD<Str, SIGNAL>> values) {
        return stdlib::component<SignalMapBody>(w, "signals", values);
    }
};
}

TEST_CASE("component checkpoint identity includes projected and structural results", "[checkpoint][component][identity]") {
    stdlib::register_standard_operators();
    SECTION("projected output") {
        rejects_changed_result<SelectedComponent<ProjectedBody<false>>, SelectedComponent<ProjectedBody<true>>>();
    }
    SECTION("structural output") {
        rejects_changed_result<SelectedComponent<StructuralBody<false>>, SelectedComponent<StructuralBody<true>>>();
    }
    SECTION("structural output with an invalid field") {
        rejects_changed_result<SelectedComponent<PartialStructuralBody<false>>, SelectedComponent<PartialStructuralBody<true>>>();
    }
    SECTION("nested component output") {
        rejects_changed_result<SelectedComponent<NestedBody<false>>, SelectedComponent<NestedBody<true>>>();
    }
    SECTION("zero-input component output") {
        rejects_changed_result<ConstantComponent<false>, ConstantComponent<true>>();
    }
}

TEST_CASE("component checkpoint resumes a mapped signal through removal and recreation", "[checkpoint][component][map]") {
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> saved;
    configure_component_recovery(context.state().view(), {
        .component_id = "signals", .load = [&] { return saved; },
        .commit = [&](const auto &image) { saved = image; }});
    const auto tick = dict_delta<Str, SIGNAL>({{"a", true}});
    const auto removed = dict_delta<Str, SIGNAL>({}, {"a"});
    CHECK_OUTPUT(eval_node_with_options<SignalMapComponent>(interval(0, 1), values<Value>(tick)), values<Value>(tick));
    REQUIRE(saved);
    CHECK_OUTPUT(eval_node_with_options<SignalMapComponent>(interval(1, 4), values<Value>(none, removed, tick)),
                 values<Value>(none, removed, tick));
}
