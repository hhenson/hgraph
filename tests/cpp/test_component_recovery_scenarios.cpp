#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/time_series_reference.h>

#include <catch2/catch_test_macros.hpp>

#include <tuple>

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

template<typename S, bool Mesh> struct CompositeChildrenBody {
    static Port<TSD<Str, S>> compose(Wiring &w, NamedPort<"ts", TSD<Str, S>> ts) {
        if constexpr (Mesh) { return wire<stdlib::mesh_>(w, fn<CopyDelta>(), ts).template as<TSD<Str, S>>(); }
        else { return wire<stdlib::map_>(w, fn<CopyDelta>(), ts).template as<TSD<Str, S>>(); }
    }
};
template<typename S, bool Mesh> struct CompositeChildrenComponent {
    static Port<TSD<Str, S>> compose(Wiring &w, Port<TSD<Str, S>> ts) {
        return stdlib::component<CompositeChildrenBody<S, Mesh>>(w, "schema-scenario", ts);
    }
};

template<typename S> struct SelectWholeReference {
    static void eval(In<"pick", TS<Int>> pick,
                     In<"left", REF<S>, InputActivity::Passive, InputValidity::Unchecked> left,
                     In<"right", REF<S>, InputActivity::Passive, InputValidity::Unchecked> right,
                     Out<REF<S>> out) {
        out.set(pick.value() == 0 ? TimeSeriesReference::empty(schema_descriptor<S>::ts_meta())
                                 : pick.value() == 1 ? left.value() : right.value());
    }
};
template<typename S> struct ObserveWholeReference {
    static void eval(In<"sample", SIGNAL>,
                     In<"value", S, InputActivity::Passive, InputValidity::Unchecked> value,
                     Out<TS<Str>> out) {
        const auto &ts = value.base();
        // Sampling also observes invalid or empty references without attempting
        // to apply one dictionary's deltas to a different dictionary baseline.
        out.set(std::to_string(ts.valid()) + ":" + std::to_string(ts.all_valid()) + ":" +
                std::to_string(ts.modified()) + ":" +
                std::to_string(ts.last_modified_time().time_since_epoch().count()) + ":" +
                (ts.valid() ? ts.value().to_string() : "INVALID") + ":" +
                (ts.valid() && ts.modified() ? capture_delta(ts).view().to_string() : "QUIET"));
    }
};
template<typename S> struct WholeReferenceBody {
    static Port<TS<Str>> compose(Wiring &w, NamedPort<"pick", TS<Int>> pick,
                                 NamedPort<"left", S> left, NamedPort<"right", S> right,
                                 NamedPort<"sample", SIGNAL> sample) {
        return wire<ObserveWholeReference<S>>(w, sample, wire<SelectWholeReference<S>>(w, pick, left, right));
    }
};
template<typename S> struct WholeReferenceComponent {
    static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> pick, Port<S> left, Port<S> right, Port<SIGNAL> sample) {
        return stdlib::component<WholeReferenceBody<S>>(w, "schema-scenario", pick, left, right, sample);
    }
};

template<bool Duration> struct WholeWindowBody {
    using Window = std::conditional_t<Duration, TSWDuration<Int, 3, 1>, TSW<Int, 3, 2>>;
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"pick", TS<Int>> pick,
                                 NamedPort<"left", TS<Int>> left, NamedPort<"right", TS<Int>> right,
                                 NamedPort<"reset", SIGNAL> reset) {
        const auto window = [&](Port<TS<Int>> input, bool resets) {
            if constexpr (Duration) {
                if (resets) { return wire<stdlib::to_window>(w, input, MIN_TD * 3, MIN_TD, reset); }
                return wire<stdlib::to_window>(w, input, MIN_TD * 3, MIN_TD);
            } else {
                if (resets) { return wire<stdlib::to_window>(w, input, Int{3}, Int{2}, reset); }
                return wire<stdlib::to_window>(w, input, Int{3}, Int{2});
            }
        };
        return wire<stdlib::sum_>(w, wire<SelectWholeReference<Window>>(w, pick, window(left, true), window(right, false)))
            .template as<TS<Int>>();
    }
};
template<bool Duration> struct WholeWindowComponent {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> pick, Port<TS<Int>> left,
                                 Port<TS<Int>> right, Port<SIGNAL> reset) {
        return stdlib::component<WholeWindowBody<Duration>>(w, "schema-scenario", pick, left, right, reset);
    }
};

// Sinks inside a component (RFC 0039, ruling 2026-09-19). A sink has no output,
// so nothing in the recovered graph can observe what it forgot: one with no
// recordable state is TRANSIENT -- outside the image and the contract, fresh on
// every run -- and one with recordable state is recovered through it.
std::vector<Int> published;
struct Publish {
    static void eval(In<"ts", TS<Int>> ts) { published.push_back(ts.value()); }
};
// Everything a transient sink may hold: ordinary state, a scheduler, a
// start-time alarm. It flushes what it has counted one tick after each start.
std::vector<std::pair<DateTime, Int>> flushed;
struct CountingFlusher {
    static void start(NodeScheduler scheduler) { scheduler.schedule(scheduler.now() + MIN_TD); }
    static void eval(In<"ts", TS<Int>, InputValidity::Unchecked> ts, State<Int> seen, NodeScheduler scheduler, DateTime now) {
        if (ts.modified()) { ++seen.modify(); }
        if (scheduler.is_scheduled_now()) { flushed.emplace_back(now, seen.get()); }
    }
};
// What has to survive is in recordable state, so it does.
using Sequence = TSB<"RecoveryScenarioSequence", Field<"count", TS<Int>>>;
std::vector<std::pair<Int, Int>> sequenced;
struct SequencedPublish {
    static void eval(In<"ts", TS<Int>> ts, RecordableState<Sequence> state) {
        auto count = state.field<"count">();
        const Int next = (count.valid() ? count.value().checked_as<Int>() : 0) + 1;
        count.set(next);
        sequenced.emplace_back(next, ts.value());
    }
};
// Recoverable AND scheduled: recordable state puts it in the image, so the
// coordinator sees its schedule -- and has to leave it alone.
std::vector<std::pair<DateTime, Int>> sequence_flushed;
struct SequencedFlusher {
    static void start(NodeScheduler scheduler) { scheduler.schedule(scheduler.now() + MIN_TD); }
    static void eval(In<"ts", TS<Int>, InputValidity::Unchecked> ts, RecordableState<Sequence> state,
                     NodeScheduler scheduler, DateTime now) {
        auto count = state.field<"count">();
        if (ts.modified()) { count.set((count.valid() ? count.value().checked_as<Int>() : 0) + 1); }
        if (scheduler.is_scheduled_now()) { sequence_flushed.emplace_back(now, count.valid() ? count.value().checked_as<Int>() : 0); }
    }
};
template<typename... Sinks> struct PublishingBody {
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> ts) {
        auto total = wire<Accumulate>(w, ts);
        (wire<Sinks>(w, total), ...);
        return total;
    }
};
template<typename... Sinks> struct PublishingComponent {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts) {
        return stdlib::component<PublishingBody<Sinks...>>(w, "schema-scenario", ts);
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

template<typename Graph, typename... T>
void composite_cuts(const std::vector<std::optional<T>> &...inputs) {
    const auto count = std::get<0>(std::tie(inputs...)).size();
    auto expected = [&] {
        GlobalContext context;
        auto result = eval_node_with_options<Graph>(interval(0, count), inputs...);
        result.resize(count);
        return result;
    }();
    for (std::size_t cut : {std::size_t{1}, std::size_t{3}, std::size_t{5}, count}) {
        CAPTURE(cut);
        std::optional<ComponentCheckpoint> completed;
        decltype(expected) actual;
        std::size_t begin = 0;
        while (begin < count) {
            const auto end = cut == count ? begin + 1 : (begin == 0 ? cut : count);
            const auto slice = [&](const auto &input) {
                using Values = std::remove_cvref_t<decltype(input)>;
                return Values{input.begin() + begin, input.begin() + end};
            };
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "schema-scenario", .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            auto result = eval_node_with_options<Graph>(interval(begin, end), slice(inputs)...);
            REQUIRE(result.size() <= end - begin);
            result.resize(end - begin);
            actual.insert(actual.end(), result.begin(), result.end());
            REQUIRE(completed);
            begin = end;
        }
        CHECK_OUTPUT(actual, expected);
    }
}

template<typename S, typename T> void composite_shape(T first, T second) {
    const auto events = values<Value>(none, dict_delta<Str, S>({{"a", first}}),
        dict_delta<Str, S>({{"a", second}}), none, dict_delta<Str, S>({}, {"a"}),
        dict_delta<Str, S>({{"a", first}, {"b", first}}),
        dict_delta<Str, S>({{"a", second}, {"b", second}}), none);
    composite_cuts<CompositeChildrenComponent<S, false>>(events);
    composite_cuts<CompositeChildrenComponent<S, true>>(events);
    composite_cuts<WholeReferenceComponent<S>>(values<Int>(1, none, none, 2, none, 0, 1, none),
        values<T>(none, first, second, none, first, second, first, none),
        values<T>(none, none, first, second, none, first, second, first),
        values<Bool>(true, true, true, true, true, true, true, true));
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

TEST_CASE("composite children and whole references recover values deltas and validity", "[checkpoint][scenario][composite]") {
    stdlib::register_standard_operators();
    SECTION("signal") { composite_shape<SIGNAL>(true, true); }
    SECTION("set") { composite_shape<TSS<Str>>(set_delta<Str>({"a", "b"}, {}), set_delta<Str>({}, {"a"})); }
    SECTION("fixed list") { composite_shape<TSL<TS<Int>, 3>>(list_delta<TS<Int>>({{0, 1}}), list_delta<TS<Int>>({{2, 3}})); }
    SECTION("dynamic list") { composite_shape<TSL<TS<Int>>>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}), dynamic_list_delta<TS<Int>>({}, {1})); }
    SECTION("dictionary") { composite_shape<TSD<Str, TS<Int>>>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}), dict_delta<Str, TS<Int>>({}, {"a"})); }
    SECTION("partial bundle") { composite_shape<Quote>(tsb_delta<Quote>(1.5, none), tsb_delta<Quote>(none, Int{2})); }
    SECTION("dictionary of bundles") { composite_shape<TSD<Str, Quote>>(
        dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(1.5, none)}}), dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(none, Int{2})}})); }
    SECTION("dictionary of sets") { composite_shape<TSD<Str, TSS<Str>>>(
        dict_delta<Str, TSS<Str>>({{"a", set_delta<Str>({"x", "y"}, {})}}), dict_delta<Str, TSS<Str>>({{"a", set_delta<Str>({}, {"x"})}})); }
    SECTION("mixed bundle") { composite_shape<Portfolio>(
        tsb_delta<Portfolio>(dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(1., none)}}), set_delta<Str>({"open"}, {}), list_delta<TS<Int>>({{0, 1}})),
        tsb_delta<Portfolio>(dict_delta<Str, Quote>({{"a", tsb_delta<Quote>(none, Int{2})}}), none, list_delta<TS<Int>>({{1, 3}}))); }
}

TEST_CASE("whole window references recover warmup reset and retargeting", "[checkpoint][scenario][composite][window]") {
    stdlib::register_standard_operators();
    const auto pick = values<Int>(1, none, 2, none, 1, 0, 2, none);
    const auto left = values<Int>(none, 1, 2, 3, none, 4, 5, 6);
    const auto right = values<Int>(10, 20, none, 30, 40, 50, 60, 70);
    const auto reset = values<Bool>(none, none, none, true, none, none, none, none);
    composite_cuts<WholeWindowComponent<false>>(pick, left, right, reset);
    composite_cuts<WholeWindowComponent<true>>(pick, left, right, reset);
}

TEST_CASE("a sink with no recordable state is transient: in the component, outside recovery",
          "[checkpoint][scenario][sink]") {
    stdlib::register_standard_operators();
    published.clear();
    every_cut<PublishingComponent<Publish>, Int>(values<Int>(1, 2, none, 3));
    // every_cut runs the four ticks once uninterrupted and then once per
    // restart campaign. Each campaign published each running total exactly
    // once: a restart restores the total and re-publishes nothing.
    REQUIRE(published.size() % 3 == 0);
    for (std::size_t run = 0; run < published.size(); run += 3) {
        CHECK(std::vector<Int>{published.begin() + run, published.begin() + run + 3} == std::vector<Int>{1, 3, 6});
    }
}

TEST_CASE("a transient sink may hold state and a scheduler, and its schedule is its own",
          "[checkpoint][scenario][sink]") {
    stdlib::register_standard_operators();
    // Refused outright before the ruling: ordinary State and a scheduler. Its
    // alarm is still pending at the first cut, which would block a capture if
    // it were anyone else's; and after the restart its start hook has to
    // re-arm, which discarding a restored node's bootstrap would prevent.
    std::optional<ComponentCheckpoint> completed;
    const auto day = [&](std::size_t begin, std::size_t end, const std::vector<std::optional<Int>> &ticks) {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "schema-scenario", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
        return eval_node_with_options<PublishingComponent<CountingFlusher>>(interval(begin, end), ticks);
    };
    flushed.clear();
    auto first = day(0, 1, values<Int>(1));
    REQUIRE(completed);
    REQUIRE(flushed.empty());                       // its alarm was pending at the cut, and the day still completed
    auto second = day(1, 4, values<Int>(2, none, 3));
    first.insert(first.end(), second.begin(), second.end());
    first.resize(4);
    CHECK_OUTPUT(first, values<Int>(1, 3, none, 6));   // the component recovered
    // The sink started again: it re-armed, and counted only what it saw this run.
    REQUIRE(flushed.size() == 1);
    CHECK(flushed.front() == std::pair<DateTime, Int>{MIN_ST + MIN_TD * 2, 1});
}

TEST_CASE("a sink with recordable state is recovered through it", "[checkpoint][scenario][sink]") {
    stdlib::register_standard_operators();
    sequenced.clear();
    every_cut<PublishingComponent<SequencedPublish>, Int>(values<Int>(1, 2, none, 3));
    // In every campaign the sequence runs 1, 2, 3 across the restarts: the
    // count came back from the image rather than starting again.
    REQUIRE(sequenced.size() % 3 == 0);
    for (std::size_t run = 0; run < sequenced.size(); run += 3) {
        CHECK(std::vector<std::pair<Int, Int>>{sequenced.begin() + run, sequenced.begin() + run + 3} ==
              std::vector<std::pair<Int, Int>>{{1, 1}, {2, 3}, {3, 6}});
    }
}

TEST_CASE("a transient sink is no part of the contract: adding one does not refuse a checkpoint",
          "[checkpoint][scenario][sink]") {
    stdlib::register_standard_operators();
    std::optional<ComponentCheckpoint> completed;
    const auto configure = [&](GlobalContext &context) {
        configure_component_recovery(context.state().view(), {
            .component_id = "schema-scenario", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
    };
    {
        GlobalContext context;
        configure(context);
        (void)eval_node_with_options<PublishingComponent<>>(interval(0, 2), values<Int>(1, 2));
        REQUIRE(completed);
    }
    // The next run has grown two sinks. Neither has an id, so the nodes that
    // do keep theirs, and the saved image still fits.
    published.clear();
    GlobalContext context;
    configure(context);
    auto resumed = eval_node_with_options<PublishingComponent<Publish, CountingFlusher>>(interval(2, 3), values<Int>(3));
    resumed.resize(1);
    CHECK_OUTPUT(resumed, values<Int>(6));
    CHECK(published == std::vector<Int>{6});
}

TEST_CASE("a recoverable sink keeps its own schedule too: pending at the cut, re-armed after it",
          "[checkpoint][scenario][sink]") {
    stdlib::register_standard_operators();
    std::optional<ComponentCheckpoint> completed;
    const auto day = [&](std::size_t begin, std::size_t end, const std::vector<std::optional<Int>> &ticks) {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "schema-scenario", .load = [&] { return completed; },
            .commit = [&](const auto &image) { completed = image; }});
        (void)eval_node_with_options<PublishingComponent<SequencedFlusher>>(interval(begin, end), ticks);
    };
    sequence_flushed.clear();
    day(0, 1, values<Int>(1));          // its alarm is pending at the cut; anyone else's would block the capture
    REQUIRE(completed);
    REQUIRE(sequence_flushed.empty());
    day(1, 4, values<Int>(2, none, 3));
    // It re-armed after the restart -- discarding a restored node's bootstrap
    // would have silenced it for good -- and the count it flushed is the
    // recovered one: 1 from the first day, 2 by the time the alarm fires.
    REQUIRE(sequence_flushed.size() == 1);
    CHECK(sequence_flushed.front() == std::pair<DateTime, Int>{MIN_ST + MIN_TD * 2, 2});
}
