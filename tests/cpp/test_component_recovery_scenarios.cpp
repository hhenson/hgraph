#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>

#include <catch2/catch_test_macros.hpp>

namespace {
using namespace hgraph;
using namespace hgraph::testing;
using namespace std::string_literals;

struct CopyDelta {
    static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Out<TsVar<"S">> out) {
        if (ts.modified()) { apply_delta(out, capture_delta(ts.base()).view()); }
    }
};
template<typename S> struct CopyBody {
    static Port<S> compose(Wiring &w, NamedPort<"ts", S> ts) {
        return wire<CopyDelta>(w, ts).template as<S>();
    }
};
template<typename S> struct CopyComponent {
    static Port<S> compose(Wiring &w, Port<S> ts) {
        return stdlib::component<CopyBody<S>>(w, "schema-scenario", ts);
    }
};

using Quote = TSB<"RecoveryScenarioQuote", Field<"price", TS<Float>>, Field<"quantity", TS<Int>>>;
using Portfolio = TSB<"RecoveryScenarioPortfolio", Field<"positions", TSD<Str, Quote>>,
                      Field<"flags", TSS<Str>>, Field<"samples", TSL<TS<Int>, 2>>>;
using Total = TSB<"RecoveryScenarioTotal", Field<"value", TS<Int>>>;
struct Accumulate {
    static void eval(In<"ts", TS<Int>> ts, RecordableState<Total> state, Out<TS<Int>> out) {
        auto value = state.field<"value">();
        const Int next = (value.valid() ? value.value().checked_as<Int>() : 0) + ts.value();
        value.set(next);
        out.set(next);
    }
};
struct InnerMap {
    static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TSD<Str, TS<Int>>> ts) {
        return wire<stdlib::map_>(w, fn<Accumulate>(), ts).as<TSD<Str, TS<Int>>>();
    }
};
using NestedDict = TSD<Str, TSD<Str, TS<Int>>>;
struct NestedMapBody {
    static Port<NestedDict> compose(Wiring &w, NamedPort<"ts", NestedDict> ts) {
        return wire<stdlib::map_>(w, fn<InnerMap>(), ts).as<NestedDict>();
    }
};
struct NestedMapComponent {
    static Port<NestedDict> compose(Wiring &w, Port<NestedDict> ts) {
        return stdlib::component<NestedMapBody>(w, "schema-scenario", ts);
    }
};

EvalNodeRunOptions interval(std::size_t begin, std::size_t end) {
    return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
            .end_time = MIN_ST + MIN_TD * static_cast<Int>(end)};
}
template<typename Graph, typename T>
void every_cut(const std::vector<std::optional<T>> &input) {
    std::vector<std::optional<T>> expected;
    {
        GlobalContext context;
        expected = eval_node_with_options<Graph>(interval(0, input.size()), input);
        expected.resize(input.size());
    }
    // Each individual cut, followed by one campaign that restarts every tick.
    for (std::size_t split = 1; split <= input.size(); ++split) {
        CAPTURE(split);
        std::optional<ComponentCheckpoint> completed;
        std::vector<std::optional<T>> actual;
        std::size_t begin = 0;
        while (begin < input.size()) {
            const auto end = split == input.size() ? begin + 1 : (begin == 0 ? split : input.size());
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "schema-scenario", .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            const std::vector<std::optional<T>> day{input.begin() + begin, input.begin() + end};
            auto result = eval_node_with_options<Graph>(interval(begin, end), day);
            REQUIRE(result.size() <= end - begin);
            result.resize(end - begin);
            actual.insert(actual.end(), result.begin(), result.end());
            REQUIRE(completed);
            begin = end;
        }
        CHECK_OUTPUT(actual, expected);
    }
}
}

TEST_CASE("component recovery schema matrix matches uninterrupted output at every cut", "[checkpoint][scenario]") {
    SECTION("integer") { every_cut<CopyComponent<TS<Int>>>(values<Int>(none, 1, 2, none, -1, 0, 7, none)); }
    SECTION("float") { every_cut<CopyComponent<TS<Float>>>(values<Float>(none, -0., 1.25, none, -2.5, 0., 1e-12, none)); }
    SECTION("owned string") { every_cut<CopyComponent<TS<Str>>>(values<Str>(none, "", "alpha", none, "β", "", "omega", none)); }
    SECTION("datetime") { every_cut<CopyComponent<TS<DateTime>>>(values<DateTime>(none, MIN_ST, MIN_ST + MIN_TD, none,
                                                                                 MIN_ST + MIN_TD * 3, MIN_ST, MIN_ST + MIN_TD * 7, none)); }
    SECTION("signal") { every_cut<CopyComponent<SIGNAL>>(values<Bool>(none, true, true, none, true, none, true, none)); }
    SECTION("set") { every_cut<CopyComponent<TSS<Str>>>(values<Value>(none, set_delta<Str>({"a", "b"}, {}),
        set_delta<Str>({}, {"a"}), none, set_delta<Str>({"c"}, {}), set_delta<Str>({}, {"b", "c"}), set_delta<Str>({"a"}, {}), none)); }
    SECTION("fixed list") { every_cut<CopyComponent<TSL<TS<Int>, 3>>>(values<Value>(none, list_delta<TS<Int>>({{0, 1}}),
        list_delta<TS<Int>>({{2, 3}}), none, list_delta<TS<Int>>({{1, 2}}), list_delta<TS<Int>>({{0, -1}, {2, 0}}), list_delta<TS<Int>>({{1, 7}}), none)); }
    SECTION("dynamic list") { every_cut<CopyComponent<TSL<TS<Int>>>>(values<Value>(none,
        dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}), dynamic_list_delta<TS<Int>>({}, {1, 2}), none,
        dynamic_list_delta<TS<Int>>({{1, 4}, {2, 5}}), dynamic_list_delta<TS<Int>>({}, {0, 1, 2}), dynamic_list_delta<TS<Int>>({{0, 7}}), none)); }
    SECTION("dictionary") { every_cut<CopyComponent<TSD<Str, TS<Int>>>>(values<Value>(none,
        dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}), dict_delta<Str, TS<Int>>({}, {"a"}), none,
        dict_delta<Str, TS<Int>>({{"c", 3}}), dict_delta<Str, TS<Int>>({}, {"b", "c"}), dict_delta<Str, TS<Int>>({{"a", 7}}), none)); }
    SECTION("partial bundle") { every_cut<CopyComponent<Quote>>(values<Value>(none,
        tsb_delta<Quote>(1.5, none), tsb_delta<Quote>(none, Int{2}), none, tsb_delta<Quote>(2.5, none),
        tsb_delta<Quote>(none, Int{0}), tsb_delta<Quote>(-1., Int{7}), none)); }
    SECTION("dictionary of bundles") { every_cut<CopyComponent<TSD<Str, Quote>>>(values<Value>(none,
        dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(1.5, none)}, {"b", tsb_delta<Quote>(none, Int{2})}}),
        dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(none, Int{4})}}), none, dict_delta<Str, Quote>({}, {"a"}),
        dict_delta<Str, Quote>({{"b", tsb_delta<Quote>(2.5, none)}}), dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(7., none)}}), none)); }
    SECTION("dictionary of sets") { every_cut<CopyComponent<TSD<Str, TSS<Str>>>>(values<Value>(none,
        dict_delta<Str, TSS<Str>>({{"a", set_delta<Str>({"x", "y"}, {})}, {"b", set_delta<Str>({"z"}, {})}}),
        dict_delta<Str, TSS<Str>>({{"a", set_delta<Str>({}, {"x"})}}), none, dict_delta<Str, TSS<Str>>({}, {"b"}),
        dict_delta<Str, TSS<Str>>({{"a", set_delta<Str>({"z"}, {})}}), dict_delta<Str, TSS<Str>>({{"b", set_delta<Str>({"new"}, {})}}), none)); }
    SECTION("mixed nested bundle") { every_cut<CopyComponent<Portfolio>>(values<Value>(none,
        tsb_delta<Portfolio>(dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(1., none)}}), set_delta<Str>({"open"}, {}), list_delta<TS<Int>>({{0, 1}})),
        tsb_delta<Portfolio>(dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(none, Int{2})}}), none, list_delta<TS<Int>>({{1, 3}})), none,
        tsb_delta<Portfolio>(dict_delta<Str, Quote>({}, {"a"}), set_delta<Str>({}, {"open"}), none),
        tsb_delta<Portfolio>(none, none, list_delta<TS<Int>>({{0, 4}})),
        tsb_delta<Portfolio>(dict_delta<Str, Quote>({{"b", tsb_delta<Quote>(7., none)}}), set_delta<Str>({"closed"}, {}), none), none)); }
}

TEST_CASE("nested stateful maps recover at every membership and quiet boundary", "[checkpoint][scenario][map]") {
    stdlib::register_standard_operators();
    every_cut<NestedMapComponent>(values<Value>(none,
        dict_delta<Str, TSD<Str, TS<Int>>>({{"x", dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}})}, {"y", dict_delta<Str, TS<Int>>({{"c", 10}})}}),
        dict_delta<Str, TSD<Str, TS<Int>>>({{"x", dict_delta<Str, TS<Int>>({{"a", 3}})}}), none,
        dict_delta<Str, TSD<Str, TS<Int>>>({{"x", dict_delta<Str, TS<Int>>({}, {"b"})}}),
        dict_delta<Str, TSD<Str, TS<Int>>>({}, {"y"}),
        dict_delta<Str, TSD<Str, TS<Int>>>({{"x", dict_delta<Str, TS<Int>>({{"b", 7}})}, {"y", dict_delta<Str, TS<Int>>({{"c", 4}})}}), none));
}
