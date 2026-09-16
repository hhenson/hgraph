#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/time_series_reference.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

namespace {
using namespace hgraph;
using namespace hgraph::testing;

struct SelectReference {
    static void eval(In<"pick", TS<Int>> pick,
                     In<"left", TS<Int>, InputValidity::Unchecked> left,
                     In<"right", TS<Int>, InputValidity::Unchecked> right,
                     Out<REF<TS<Int>>> out) {
        if (!pick.modified()) { return; }
        out.set(pick.value() < 0 ? TimeSeriesReference::empty(schema_descriptor<TS<Int>>::ts_meta())
                               : pick.value() == 0 ? left.reference() : right.reference());
    }
};
struct CopyValue {
    static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out) { out.set(ts.value()); }
};
struct RememberReference {
    static void eval(In<"trigger", TS<Int>> trigger, In<"ts", TS<Int>, InputValidity::Unchecked> ts,
                     Out<REF<TS<Int>>> out) {
        if (trigger.modified()) { out.set(ts.reference()); }
    }
};
struct SelectorBody {
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"pick", TS<Int>> pick,
                                NamedPort<"left", TS<Int>> left, NamedPort<"right", TS<Int>> right) {
        return wire<CopyValue>(w, wire<SelectReference>(w, pick, left, right));
    }
};
struct MovingAliasBody {
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"pick", TS<Int>> pick,
                                NamedPort<"left", TS<Int>> left, NamedPort<"right", TS<Int>> right) {
        auto selected = wire<SelectReference>(w, pick, left, right);
        auto once = wire<stdlib::const_>(w, Int{1}).as<TS<Int>>();
        return wire<CopyValue>(w, wire<RememberReference>(w, once, selected));
    }
};
using SavedReference = TSB<"CheckpointReferenceState", Field<"selected", REF<TS<Int>>>>;
struct SelectRecordedReference {
    static void eval(In<"pick", TS<Int>> pick,
                     In<"left", TS<Int>, InputValidity::Unchecked> left,
                     In<"right", TS<Int>, InputValidity::Unchecked> right,
                     RecordableState<SavedReference> state, Out<REF<TS<Int>>> out) {
        auto selected = state.field<"selected">();
        if (pick.modified()) { selected.set(pick.value() == 0 ? left.reference() : right.reference()); }
        if (selected.valid()) { out.set(selected.value().checked_as<TimeSeriesReference>()); }
    }
};
struct RecordedBody {
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"pick", TS<Int>> pick,
                                NamedPort<"left", TS<Int>> left, NamedPort<"right", TS<Int>> right) {
        return wire<CopyValue>(w, wire<SelectRecordedReference>(w, pick, left, right));
    }
};
using ReferencePair = TSB<"ReferenceCheckpointPair", Field<"a", TS<Int>>, Field<"b", TS<Int>>>;
template<typename S> struct SelectAggregate {
    static void eval(In<"pick", TS<Int>> pick, In<"lhs", REF<S>> lhs, In<"rhs", REF<S>> rhs,
                     Out<REF<S>> out) {
        if (pick.modified()) { out.set(pick.value() == 0 ? lhs.value() : rhs.value()); }
    }
};
template<typename S> struct ObserveAggregate {
    static void eval(In<"ts", S, InputValidity::Unchecked> ts, Out<TS<Int>> out) {
        auto a = ts.base().indexed_child_at(0);
        auto b = ts.base().indexed_child_at(1);
        // Encode values, validity and independent child modification flags in
        // the observable trace, including a partially valid aggregate.
        out.set((a.valid() ? a.value().template checked_as<Int>() : 0) +
                (b.valid() ? b.value().template checked_as<Int>() * 100 : 0) +
                (a.valid() ? 10000 : 0) + (b.valid() ? 20000 : 0) +
                (a.modified() ? 40000 : 0) + (b.modified() ? 80000 : 0));
    }
};
template<bool Bundle> struct AggregateBody {
    using S = std::conditional_t<Bundle, ReferencePair, TSL<TS<Int>, 2>>;
    static Port<TS<Int>> compose(Wiring &w, NamedPort<"pick", TS<Int>> pick,
                                NamedPort<"left", TS<Int>> left, NamedPort<"right", TS<Int>> right) {
        const auto combine = [&](Port<TS<Int>> a, Port<TS<Int>> b) {
            if constexpr (Bundle) { return stdlib::to_tsb<S>(w, a, b); }
            else { return stdlib::to_tsl<S>(w, a, b); }
        };
        return wire<ObserveAggregate<S>>(w, wire<SelectAggregate<S>>(w, pick, combine(left, right), combine(right, left)));
    }
};
template<typename Body> struct ReferenceComponent {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> pick, Port<TS<Int>> left, Port<TS<Int>> right) {
        return stdlib::component<Body>(w, "references", pick, left, right);
    }
};
EvalNodeRunOptions interval(std::size_t begin, std::size_t end) {
    return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
            .end_time = MIN_ST + MIN_TD * static_cast<Int>(end)};
}
template<typename Body> void compare_restarts(const std::vector<std::optional<Int>> &pick) {
    const auto left = values<Int>(none, 2, none, 4, none, none, 7, none);
    const auto right = values<Int>(10, none, 30, none, 50, none, none, 80);
    std::vector<std::optional<Int>> expected;
    {
        GlobalContext context;
        expected = eval_node_with_options<ReferenceComponent<Body>>(interval(0, pick.size()), pick, left, right);
        expected.resize(pick.size());
    }
    for (std::size_t cut = 1; cut <= pick.size(); ++cut) {
        CAPTURE(cut);
        std::optional<ComponentCheckpoint> saved;
        std::vector<std::optional<Int>> actual;
        std::size_t begin = 0;
        while (begin < pick.size()) {
            const auto end = cut == pick.size() ? begin + 1 : begin == 0 ? cut : pick.size();
            const auto slice = [&](const auto &input) {
                return std::vector<std::optional<Int>>(input.begin() + begin, input.begin() + end);
            };
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "references", .load = [&] { return saved; },
                .commit = [&](const auto &image) { saved = image; }});
            auto result = eval_node_with_options<ReferenceComponent<Body>>(interval(begin, end), slice(pick), slice(left), slice(right));
            result.resize(end - begin);
            actual.insert(actual.end(), result.begin(), result.end());
            begin = end;
        }
        REQUIRE(actual.size() == expected.size());
        for (std::size_t index = 0; index < actual.size(); ++index) {
            CAPTURE(index, actual[index].value_or(-99999), expected[index].value_or(-99999));
            CHECK(actual[index] == expected[index]);
        }
    }
}
}

TEST_CASE("component reference recovery preserves retargeting and quiet resume", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    compare_restarts<SelectorBody>(values<Int>(1, none, 0, none, 1, none, 0, none));
}

TEST_CASE("component reference recovery preserves typed empty and invalid targets", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    compare_restarts<SelectorBody>(values<Int>(0, none, -1, none, 1, -1, 0, none));
}

TEST_CASE("component reference recovery preserves moving adapter identity", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    compare_restarts<MovingAliasBody>(values<Int>(1, none, 0, none, 1, none, 0, none));
}

TEST_CASE("component reference recovery restores references in recorded state", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    compare_restarts<RecordedBody>(values<Int>(1, none, 0, none, 1, none, 0, none));
}

TEST_CASE("component reference recovery preserves composite child clocks and validity", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    SECTION("non-peered fixed list") {
        compare_restarts<AggregateBody<false>>(values<Int>(1, none, 0, none, 1, none, 0, none));
    }
    SECTION("non-peered bundle") {
        compare_restarts<AggregateBody<true>>(values<Int>(1, none, 0, none, 1, none, 0, none));
    }
}

TEST_CASE("component reference recovery rejects a missing node before starting", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> saved;
    std::size_t commits = 0;
    configure_component_recovery(context.state().view(), {
        .component_id = "references", .load = [&] { return saved; },
        .commit = [&](const auto &image) { saved = image; ++commits; }});
    (void)eval_node_with_options<ReferenceComponent<SelectorBody>>(interval(0, 1),
        values<Int>(1), values<Int>(2), values<Int>(3));
    REQUIRE(saved);
    bool corrupted = false;
    for (auto &node : saved->graph.nodes) {
        if (node.output && node.output->reference && node.output->reference->target) {
            node.output->reference->target->node = saved->graph.nodes.size();
            corrupted = true;
        }
    }
    REQUIRE(corrupted);
    REQUIRE_THROWS_WITH(eval_node_with_options<ReferenceComponent<SelectorBody>>(interval(1, 2),
        values<Int>(none), values<Int>(4), values<Int>(5)),
        Catch::Matchers::ContainsSubstring("reference graph or node ordinal is invalid"));
    CHECK(commits == 1);
}

TEST_CASE("component reference recovery rejects incomplete and malformed adapter inventories", "[checkpoint][reference]") {
    stdlib::register_standard_operators();
    for (int corruption = 0; corruption < 4; ++corruption) {
        CAPTURE(corruption);
        GlobalContext context;
        std::optional<ComponentCheckpoint> saved;
        std::size_t commits = 0;
        configure_component_recovery(context.state().view(), {
            .component_id = "references", .load = [&] { return saved; },
            .commit = [&](const auto &image) { saved = image; ++commits; }});
        (void)eval_node_with_options<ReferenceComponent<SelectorBody>>(interval(0, 1),
            values<Int>(1), values<Int>(2), values<Int>(3));
        REQUIRE(saved);
        bool corrupted = false;
        for (auto &node : saved->graph.nodes) {
            if (node.alternatives.empty()) { continue; }
            switch (corruption) {
                case 0: node.alternatives.clear(); break;
                case 1: node.alternatives.push_back(node.alternatives.front()); break;
                case 2: node.alternatives.front().binding.bindings.front().requested_schema = nullptr; break;
                case 3: node.alternatives.front().binding.endpoint = 99; break;
            }
            corrupted = true;
            break;
        }
        REQUIRE(corrupted);
        REQUIRE_THROWS_WITH(eval_node_with_options<ReferenceComponent<SelectorBody>>(interval(1, 2),
            values<Int>(none), values<Int>(4), values<Int>(5)), Catch::Matchers::ContainsSubstring("checkpoint"));
        CHECK(commits == 1);
    }
}
