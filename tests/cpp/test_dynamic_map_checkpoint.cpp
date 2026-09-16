#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using List = TSL<TS<Int>>;
    using RunningState = TSB<"dynamic_map_checkpoint_state", Field<"total", TS<Int>>>;

    struct IndexedTotal
    {
        static constexpr auto name = "dynamic_map_checkpoint_total";
        static void eval(In<"ndx", TS<Int>> index, In<"ts", TS<Int>> input,
                         RecordableState<RunningState> state, Out<TS<Int>> out)
        {
            auto total = state.field<"total">();
            const Int value = (total.valid() ? total.value().checked_as<Int>() : 0) + input.value();
            total.set(value);
            out.set(value + 100 * index.value());
        }
    };

    struct DynamicStrategy
    {
        static Port<List> compose(Wiring &w, NamedPort<"ts", List> input)
        {
            return wire<stdlib::map_>(w, fn<IndexedTotal>(), input).as<List>();
        }
    };
    struct DynamicComponent
    {
        static Port<List> compose(Wiring &w, Port<List> input)
        {
            return stdlib::component<DynamicStrategy>(w, "dynamic-map", input);
        }
    };

    struct PairTotal
    {
        static constexpr auto name = "dynamic_map_checkpoint_pair_total";
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         RecordableState<RunningState> state, Out<TS<Int>> out)
        {
            auto total = state.field<"total">();
            const Int value = (total.valid() ? total.value().checked_as<Int>() : 0) + lhs.value() + rhs.value();
            total.set(value);
            out.set(value);
        }
    };
    struct PairStrategy
    {
        static Port<List> compose(Wiring &w, NamedPort<"lhs", List> lhs, NamedPort<"rhs", List> rhs)
        {
            return wire<stdlib::map_>(w, fn<PairTotal>(), lhs, rhs).as<List>();
        }
    };
    struct PairComponent
    {
        static Port<List> compose(Wiring &w, Port<List> lhs, Port<List> rhs)
        {
            return stdlib::component<PairStrategy>(w, "dynamic-map", lhs, rhs);
        }
    };
    std::vector<Int> stopped_values;
    struct StopFailure
    {
        static constexpr auto name = "dynamic_map_checkpoint_stop_failure";
        static void eval(In<"ts", TS<Int>> input, RecordableState<RunningState> state, Out<TS<Int>> out)
        {
            state.field<"total">().set(input.value());
            out.set(input.value());
        }
        static void stop(RecordableState<RunningState> state)
        {
            auto total = state.field<"total">();
            if (!total.valid()) { return; }
            const auto value = total.value().checked_as<Int>();
            stopped_values.push_back(value);
            if (value < 0) { throw std::runtime_error("mapped child stop failed"); }
        }
    };
    struct StopFailureStrategy
    {
        static Port<List> compose(Wiring &w, NamedPort<"ts", List> input)
        {
            return wire<stdlib::map_>(w, fn<StopFailure>(), input).as<List>();
        }
    };
    struct StopFailureComponent
    {
        static Port<List> compose(Wiring &w, Port<List> input)
        {
            return stdlib::component<StopFailureStrategy>(w, "dynamic-map", input);
        }
    };
    EvalNodeRunOptions interval(std::size_t begin, std::size_t end)
    {
        return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
                .end_time = MIN_ST + MIN_TD * static_cast<Int>(end)};
    }
    template<typename T>
    std::vector<std::optional<T>> slice(const std::vector<std::optional<T>> &input,
                                      std::size_t begin, std::size_t end)
    {
        return {input.begin() + begin, input.begin() + end};
    }

    std::vector<std::optional<Value>> input_events()
    {
        return values<Value>(
            dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}),
            none,
            dynamic_list_delta<TS<Int>>({{1, 4}}),
            dynamic_list_delta<TS<Int>>({{0, 5}}, {1, 2}),
            none,
            dynamic_list_delta<TS<Int>>({{1, 6}}),
            dynamic_list_delta<TS<Int>>({}, {0, 1}),
            dynamic_list_delta<TS<Int>>({{0, 7}}));
    }
}

TEST_CASE("dynamic map checkpoint: child state and indices survive every split", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    const auto input = input_events();
    std::vector<std::optional<Value>> continuous;
    {
        GlobalContext context;
        continuous = eval_node_with_options<DynamicComponent>(interval(0, input.size()), input);
    }
    const auto split = static_cast<std::size_t>(GENERATE(1, 2, 3, 4, 5, 6, 7));
    std::optional<ComponentCheckpoint> checkpoint;
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "dynamic-map", .commit = [&](const auto &image) { checkpoint = image; }});
        CHECK_OUTPUT(eval_node_with_options<DynamicComponent>(interval(0, split), slice(input, 0, split)),
                     slice(continuous, 0, split));
    }
    REQUIRE(checkpoint);
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "dynamic-map", .load = [&] { return checkpoint; },
            .commit = [&](const auto &image) { checkpoint = image; }});
        CHECK_OUTPUT(eval_node_with_options<DynamicComponent>(interval(split, input.size()),
                     slice(input, split, input.size())), slice(continuous, split, continuous.size()));
    }
}

TEST_CASE("dynamic map checkpoint: repeated one-cycle runs retire and regrow fresh children", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    const auto input = input_events();
    const auto expected = values<Value>(
        dynamic_list_delta<TS<Int>>({{0, 1}, {1, 102}, {2, 203}}), none,
        dynamic_list_delta<TS<Int>>({{1, 106}}),
        dynamic_list_delta<TS<Int>>({{0, 6}}, {1, 2}), none,
        dynamic_list_delta<TS<Int>>({{1, 106}}),
        dynamic_list_delta<TS<Int>>({}, {0, 1}),
        dynamic_list_delta<TS<Int>>({{0, 7}}));
    std::optional<ComponentCheckpoint> checkpoint;
    for (std::size_t index = 0; index < input.size(); ++index)
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "dynamic-map", .load = [&] { return checkpoint; },
            .commit = [&](const auto &image) { checkpoint = image; }});
        CHECK_OUTPUT(eval_node_with_options<DynamicComponent>(interval(index, index + 1),
                     slice(input, index, index + 1)), slice(expected, index, index + 1));
        REQUIRE(checkpoint);
    }
}

TEST_CASE("dynamic map checkpoint: shorter peer list retains phantom bindings until growth", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    const auto lhs = values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}), none,
                                   dynamic_list_delta<TS<Int>>({{1, 3}}), none);
    const auto rhs = values<Value>(dynamic_list_delta<TS<Int>>({{0, 10}}), none,
                                   dynamic_list_delta<TS<Int>>({{1, 20}}),
                                   dynamic_list_delta<TS<Int>>({{0, 30}}));
    const auto split = static_cast<std::size_t>(GENERATE(1, 2, 3));
    std::vector<std::optional<Value>> continuous;
    {
        GlobalContext context;
        continuous = eval_node_with_options<PairComponent>(interval(0, 4), lhs, rhs);
    }
    std::optional<ComponentCheckpoint> checkpoint;
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "dynamic-map", .commit = [&](const auto &image) { checkpoint = image; }});
        CHECK_OUTPUT(eval_node_with_options<PairComponent>(interval(0, split),
                     slice(lhs, 0, split), slice(rhs, 0, split)), slice(continuous, 0, split));
    }
    REQUIRE(checkpoint);
    {
        GlobalContext context;
        configure_component_recovery(context.state().view(), {
            .component_id = "dynamic-map", .load = [&] { return checkpoint; },
            .commit = [&](const auto &image) { checkpoint = image; }});
        CHECK_OUTPUT(eval_node_with_options<PairComponent>(interval(split, 4),
                     slice(lhs, split, lhs.size()), slice(rhs, split, rhs.size())), slice(continuous, split, continuous.size()));
    }
}

TEST_CASE("dynamic map checkpoint: failed child stop cannot publish a completed day", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> checkpoint;
    configure_component_recovery(context.state().view(), {
        .component_id = "dynamic-map", .commit = [&](const auto &image) { checkpoint = image; }});
    CHECK_THROWS_WITH(eval_node_with_options<StopFailureComponent>(interval(0, 1),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, -1}, {1, -2}}))),
        Catch::Matchers::ContainsSubstring("mapped child stop failed"));
    CHECK_FALSE(checkpoint);
}

TEST_CASE("dynamic map checkpoint: failed tail retirement finishes cleanup without committing", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> checkpoint;
    std::size_t commits{};
    configure_component_recovery(context.state().view(), {
        .component_id = "dynamic-map", .load = [&] { return checkpoint; },
        .commit = [&](const auto &image) { checkpoint = image; ++commits; }});
    CHECK_OUTPUT(eval_node_with_options<StopFailureComponent>(interval(0, 1),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}))),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}})));
    REQUIRE(checkpoint);
    const auto previous_cut = checkpoint->cut;
    stopped_values.clear();
    CHECK_THROWS_WITH(eval_node_with_options<StopFailureComponent>(interval(1, 3),
        values<Value>(dynamic_list_delta<TS<Int>>({{1, -1}, {2, -2}}),
                      dynamic_list_delta<TS<Int>>({}, {1, 2}))),
        Catch::Matchers::ContainsSubstring("mapped child stop failed"));
    CHECK(commits == 1);
    CHECK(checkpoint->cut == previous_cut);
    std::sort(stopped_values.begin(), stopped_values.end());
    CHECK(stopped_values == std::vector<Int>{-2, -1, 1});
    CHECK_OUTPUT(eval_node_with_options<StopFailureComponent>(interval(1, 2),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 4}}))),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 4}})));
    CHECK(commits == 2);
}
