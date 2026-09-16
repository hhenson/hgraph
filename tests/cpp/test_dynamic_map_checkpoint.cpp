#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/util/scope.h>

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
    std::optional<Int> failed_reference_start_key;
    std::size_t reference_child_starts{};
    struct ChooseMappedReference
    {
        static void start(In<"key", TS<Int>> key)
        {
            if (failed_reference_start_key && key.valid() && key.value() == *failed_reference_start_key)
            {
                throw std::runtime_error("mapped reference child start failed");
            }
            ++reference_child_starts;
        }
        static void eval(In<"key", TS<Int>> key, In<"ts", TS<Int>> input,
                         In<"ts_ref", REF<TS<Int>>> input_ref, Out<REF<TS<Int>>> out)
        {
            out.set(input.value() < 0 ? key.base().reference() : input_ref.value());
        }
    };
    struct ReadMappedReference
    {
        static void eval(In<"ts", TS<Int>> input, RecordableState<RunningState> state, Out<TS<Int>> out)
        {
            auto total = state.field<"total">();
            const Int value = (total.valid() ? total.value().checked_as<Int>() : 0) + input.value();
            total.set(value);
            out.set(value);
        }
    };
    struct KeyReferenceChild
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key, NamedPort<"ts", TS<Int>> input)
        {
            return wire<ReadMappedReference>(w, wire<ChooseMappedReference>(w, key, input, input));
        }
    };
    struct IndexReferenceChild
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ndx", TS<Int>> index, NamedPort<"ts", TS<Int>> input)
        {
            return wire<ReadMappedReference>(w, wire<ChooseMappedReference>(w, index, input, input));
        }
    };
    template <bool Dynamic> using ReferenceCollection = std::conditional_t<Dynamic, List, TSD<Int, TS<Int>>>;
    template <bool Dynamic> struct ReferenceStrategy
    {
        static Port<ReferenceCollection<Dynamic>> compose(Wiring &w, NamedPort<"ts", ReferenceCollection<Dynamic>> input)
        {
            using Child = std::conditional_t<Dynamic, IndexReferenceChild, KeyReferenceChild>;
            return wire<stdlib::map_>(w, fn<Child>(), input).template as<ReferenceCollection<Dynamic>>();
        }
    };
    template <bool Dynamic> struct ReferenceComponent
    {
        static Port<ReferenceCollection<Dynamic>> compose(Wiring &w, Port<ReferenceCollection<Dynamic>> input)
        {
            return stdlib::component<ReferenceStrategy<Dynamic>>(w, "dynamic-map", input);
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

TEST_CASE("mapped internal references restore element and synthetic key targets through churn", "[checkpoint][map][reference]")
{
    stdlib::register_standard_operators();
    const auto run = []<bool Dynamic>() {
        const auto input = Dynamic
            ? values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, -1}, {2, 3}}), none,
                            dynamic_list_delta<TS<Int>>({{0, -1}, {1, 4}}),
                            dynamic_list_delta<TS<Int>>({}, {1, 2}), none,
                            dynamic_list_delta<TS<Int>>({{1, -1}, {2, 6}}),
                            dynamic_list_delta<TS<Int>>({{0, 7}}), none)
            : values<Value>(dict_delta<Int, TS<Int>>({{0, 1}, {1, -1}, {2, 3}}), none,
                            dict_delta<Int, TS<Int>>({{0, -1}, {1, 4}}),
                            dict_delta<Int, TS<Int>>({}, {1, 2}), none,
                            dict_delta<Int, TS<Int>>({{1, -1}, {2, 6}}),
                            dict_delta<Int, TS<Int>>({{0, 7}}), none);
        std::vector<std::optional<Value>> continuous;
        {
            GlobalContext context;
            continuous = eval_node_with_options<ReferenceComponent<Dynamic>>(interval(0, input.size()), input);
            continuous.resize(input.size());
        }
        for (std::size_t cut = 1; cut <= input.size(); ++cut)
        {
            CAPTURE(Dynamic, cut);
            std::optional<ComponentCheckpoint> saved;
            std::size_t begin = 0;
            while (begin < input.size())
            {
                const auto end = cut == input.size() ? begin + 1 : begin == 0 ? cut : input.size();
                GlobalContext context;
                configure_component_recovery(context.state().view(), {
                    .component_id = "dynamic-map", .load = [&] { return saved; },
                    .commit = [&](const auto &image) { saved = image; }});
                CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<Dynamic>>(
                    interval(begin, end), slice(input, begin, end)), slice(continuous, begin, end));
                REQUIRE(saved);
                begin = end;
            }
        }
    };
    SECTION("dictionary slots and mapped keys") { run.template operator()<false>(); }
    SECTION("dynamic list slots and mapped indices") { run.template operator()<true>(); }
}

TEST_CASE("mapped restored references detach before failed child startup rollback", "[checkpoint][map][reference]")
{
    stdlib::register_standard_operators();
    const auto reset_failure = make_scope_exit([] { failed_reference_start_key.reset(); });
    const bool during_restore = GENERATE(true, false);
    const auto run = [&]<bool Dynamic>() {
        std::optional<ComponentCheckpoint> saved;
        std::size_t commits{};
        const auto initial = Dynamic ? dynamic_list_delta<TS<Int>>({{0, -1}, {1, -1}})
                                     : dict_delta<Int, TS<Int>>({{0, -1}, {1, -1}});
        {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "dynamic-map", .commit = [&](const auto &image) { saved = image; ++commits; }});
            (void)eval_node_with_options<ReferenceComponent<Dynamic>>(interval(0, 1), values<Value>(initial));
        }
        REQUIRE(saved);
        const auto original_cut = saved->cut;
        reference_child_starts = 0;
        failed_reference_start_key = during_restore ? 0 : 2;
        {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "dynamic-map", .load = [&] { return saved; },
                .commit = [&](const auto &image) { saved = image; ++commits; }});
            // The second path retires restored storage before a later child fails,
            // so startup cleanup must no longer retain the original graph pointers.
            const auto input = during_restore ? values<Value>(none, none)
                : Dynamic ? values<Value>(dynamic_list_delta<TS<Int>>({}, {0, 1}),
                                          dynamic_list_delta<TS<Int>>({{0, -1}, {1, -1}, {2, -1}}))
                          : values<Value>(dict_delta<Int, TS<Int>>({}, {0}),
                                          dict_delta<Int, TS<Int>>({{2, -1}}));
            CHECK_THROWS_WITH(eval_node_with_options<ReferenceComponent<Dynamic>>(interval(1, 3), input),
                              Catch::Matchers::ContainsSubstring("mapped reference child start failed"));
        }
        CHECK(commits == 1);
        CHECK(saved->cut == original_cut);
        if (during_restore) { CHECK(reference_child_starts == 0); }
        failed_reference_start_key.reset();
        {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "dynamic-map", .load = [&] { return saved; },
                .commit = [&](const auto &image) { saved = image; ++commits; }});
            const auto next = Dynamic ? dynamic_list_delta<TS<Int>>({{1, 3}})
                                     : dict_delta<Int, TS<Int>>({{1, 3}});
            const auto expected = Dynamic ? dynamic_list_delta<TS<Int>>({{1, 4}})
                                         : dict_delta<Int, TS<Int>>({{1, 4}});
            CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<Dynamic>>(interval(1, 2), values<Value>(next)),
                         values<Value>(expected));
        }
        CHECK(commits == 2);
    };
    SECTION("dictionary slots") { run.template operator()<false>(); }
    SECTION("dynamic list slots") { run.template operator()<true>(); }
}
