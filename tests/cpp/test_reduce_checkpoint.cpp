#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/types/time_series/ts_input.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <string_view>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using Calls = TSB<"reduce_checkpoint_calls", Field<"count", TS<Int>>>;
    std::size_t evaluations = 0;
    std::size_t failed_stops = 0;

    struct StatefulSum
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         RecordableState<Calls> state, Out<TS<Int>> output)
        {
            ++evaluations;
            auto count = state.field<"count">();
            const Int next = count.valid() ? count.value().checked_as<Int>() + 1 : 1;
            count.set(next);
            output.set(lhs.value() * 10 + rhs.value() + next * 100);
        }
    };

    struct LiftedSum
    {
        static constexpr const char *name = "checkpoint_lifted_sum";
        static constexpr std::array<std::string_view, 2> parameter_names{"lhs", "rhs"};
        static constexpr bool associative = true;
        static constexpr bool commutative = true;
        static Int apply(Int lhs, Int rhs) { return lhs + rhs; }
    };

    struct FailCombinerStop
    {
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         RecordableState<Calls> state, Out<TS<Int>> output)
        {
            state.field<"count">().set(Int{1});
            output.set(lhs.value() + rhs.value());
        }
        static void stop(RecordableState<Calls> state)
        {
            if (state.field<"count">().valid())
            {
                ++failed_stops;
                throw std::runtime_error("reduction child stop failed");
            }
        }
    };

    template <bool Ordered>
    struct StopFailureStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"values", TSD<Int, TS<Int>>> input,
                                     NamedPort<"zero", TS<Int>> zero)
        {
            if constexpr (Ordered)
            {
                return wire<stdlib::reduce_>(w, fn<FailCombinerStop>(), input, zero, Bool{false}).as<TS<Int>>();
            }
            return wire<stdlib::reduce_>(w, fn<FailCombinerStop>(), input, zero).as<TS<Int>>();
        }
    };
    template <bool Ordered>
    struct StopFailureComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> input, Port<TS<Int>> zero)
        {
            return stdlib::component<StopFailureStrategy<Ordered>>(w, "strategy", input, zero);
        }
    };

    template <typename Collection, bool Ordered = false, bool Lifted = false>
    struct ReduceStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"values", Collection> input,
                                     NamedPort<"zero", TS<Int>> zero)
        {
            if constexpr (Ordered)
            {
                return wire<stdlib::reduce_>(w, fn<StatefulSum>(), input, zero, Bool{false}).template as<TS<Int>>();
            }
            else if constexpr (Lifted)
            {
                return wire<stdlib::reduce_>(w, lift<LiftedSum>(), input, zero).template as<TS<Int>>();
            }
            else
            {
                return wire<stdlib::reduce_>(w, fn<StatefulSum>(), input, zero).template as<TS<Int>>();
            }
        }
    };

    template <typename Collection, bool Ordered = false, bool Lifted = false>
    struct ReduceComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<Collection> input, Port<TS<Int>> zero)
        {
            return stdlib::component<ReduceStrategy<Collection, Ordered, Lifted>>(w, "strategy", input, zero);
        }
    };

    template <typename Collection>
    struct NoZeroStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"values", Collection> input)
        {
            return wire<stdlib::reduce_>(w, fn<StatefulSum>(), input).template as<TS<Int>>();
        }
    };

    struct FixedTreeStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"values", TSL<TS<Int>, 4>> input)
        {
            return wire<stdlib::reduce_>(w, fn<StatefulSum>(), input, Int{0}).as<TS<Int>>();
        }
    };
    struct FixedTreeComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TSL<TS<Int>, 4>> input)
        {
            return stdlib::component<FixedTreeStrategy>(w, "strategy", input);
        }
    };
    template <typename Collection>
    struct NoZeroComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<Collection> input)
        {
            return stdlib::component<NoZeroStrategy<Collection>>(w, "strategy", input);
        }
    };

    using Dictionary = TSD<Int, TS<Int>>;
    struct SelectLeftDictionary
    {
        static void eval(In<"lhs", Dictionary> lhs, In<"rhs", Dictionary>, Out<Dictionary> output)
        {
            auto mutation = output.begin_mutation(output.evaluation_time());
            static_cast<void>(mutation.copy_value_from(lhs.base().value()));
        }
    };
    struct DictionaryStrategy
    {
        static Port<Dictionary> compose(Wiring &w, NamedPort<"values", TSD<Str, Dictionary>> input)
        {
            return wire<stdlib::reduce_>(w, fn<SelectLeftDictionary>(), input).as<Dictionary>();
        }
    };
    struct DictionaryComponent
    {
        static Port<Dictionary> compose(Wiring &w, Port<TSD<Str, Dictionary>> input)
        {
            return stdlib::component<DictionaryStrategy>(w, "strategy", input);
        }
    };

    EvalNodeRunOptions interval(std::size_t begin, std::size_t end)
    {
        return {.start_time = MIN_ST + MIN_TD * static_cast<Int>(begin),
                .end_time = MIN_ST + MIN_TD * static_cast<Int>(end)};
    }

    template <typename T>
    auto slice(const std::vector<std::optional<T>> &values, std::size_t begin, std::size_t end)
    {
        return std::vector<std::optional<T>>{values.begin() + static_cast<std::ptrdiff_t>(begin),
                                             values.begin() + static_cast<std::ptrdiff_t>(end)};
    }

    template <typename Graph, typename... Inputs>
    void compare_every_cut(const std::vector<std::optional<Value>> &input, const Inputs &...other_inputs)
    {
        using Output = typename eval_node_detail::graph_output_element<Graph>::type;
        std::vector<std::optional<Output>> uninterrupted;
        std::size_t uninterrupted_evaluations;
        {
            GlobalContext context;
            evaluations = 0;
            uninterrupted = eval_node_with_options<Graph>(interval(0, input.size()), input, other_inputs...);
            uninterrupted_evaluations = evaluations;
        }
        for (std::size_t cut = 1; cut < input.size(); ++cut)
        {
            INFO("checkpoint after input index " << cut - 1);
            GlobalContext context;
            std::optional<ComponentCheckpoint> completed;
            configure_component_recovery(context.state().view(), {
                .component_id = "strategy", .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            evaluations = 0;
            const auto first = eval_node_with_options<Graph>(interval(0, cut), slice(input, 0, cut),
                                                             slice(other_inputs, 0, cut)...);
            REQUIRE(completed);
            CHECK_OUTPUT(first, slice(uninterrupted, 0, cut));
            const auto second = eval_node_with_options<Graph>(interval(cut, input.size()), slice(input, cut, input.size()),
                                                              slice(other_inputs, cut, input.size())...);
            CHECK_OUTPUT(second, slice(uninterrupted, cut, input.size()));
            CHECK(evaluations == uninterrupted_evaluations);
        }
    }
}

TEST_CASE("reduce checkpoint preserves stateful combiner history and dense key slots across every cut", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSD<Str, TS<Int>>>>(
        values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}}),
                      dict_delta<Str, TS<Int>>({}, {"b"}),
                      none,
                      dict_delta<Str, TS<Int>>({{"a", 10}}),
                      dict_delta<Str, TS<Int>>({{"e", 5}, {"f", 6}}),
                      dict_delta<Str, TS<Int>>({}, {"a", "c", "d", "e"}),
                      dict_delta<Str, TS<Int>>({}, {"f"}),
                      dict_delta<Str, TS<Int>>({{"a", 20}}), none),
        values<Int>(7, none, none, none, none, none, 9, none, none));
}

TEST_CASE("reduce checkpoint supports lifted kernels without starting or replaying combiner graphs", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSD<Int, TS<Int>>, false, true>>(
        values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}, {3, 3}}),
                      none, dict_delta<Int, TS<Int>>({{1, 4}}),
                      dict_delta<Int, TS<Int>>({}, {2, 3}),
                      dict_delta<Int, TS<Int>>({}, {1}),
                      dict_delta<Int, TS<Int>>({{4, 10}, {5, 20}})),
        values<Int>(0, none, none, none, none, none));
}

TEST_CASE("reduce checkpoint preserves keyed source allocation order after the collection empties", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSD<Str, TS<Int>>>>(
        values<Value>(none, dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}, {"c", 3}}),
                      dict_delta<Str, TS<Int>>({{"b", 7}}), dict_delta<Str, TS<Int>>({}, {"a"}),
                      dict_delta<Str, TS<Int>>({{"d", 4}, {"e", 5}}),
                      dict_delta<Str, TS<Int>>({}, {"b", "c", "d", "e"}),
                      dict_delta<Str, TS<Int>>({{"a", 8}, {"e", 9}}), none),
        values<Int>(0, none, none, none, none, 5, none, none));
}

TEST_CASE("reduce checkpoint supports fixed list trees with partial validity and a scalar zero", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<FixedTreeComponent>(values<Value>(none, list_delta<TS<Int>>({{0, 1}, {1, 2}}),
        list_delta<TS<Int>>({{2, 3}}), list_delta<TS<Int>>({{0, 7}}), list_delta<TS<Int>>({{3, 4}}),
        none, list_delta<TS<Int>>({{1, 8}, {3, 9}}), none));
}

TEST_CASE("reduce checkpoint preserves dynamic list shrink growth and partial leaf state", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSL<TS<Int>>>>(
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}),
                      dynamic_list_delta<TS<Int>>({}, {1, 2}), none,
                      dynamic_list_delta<TS<Int>>({{1, 5}}),
                      dynamic_list_delta<TS<Int>>({{2, 7}, {3, 9}}),
                      dynamic_list_delta<TS<Int>>({{0, 4}})),
        values<Int>(0, none, none, none, none, none));
}

TEST_CASE("reduce checkpoint keeps empty and singleton no-zero behavior", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<NoZeroComponent<TSD<Str, TS<Int>>>>(
        values<Value>(dict_delta<Str, TS<Int>>({}), none,
                      dict_delta<Str, TS<Int>>({{"a", 3}}),
                      dict_delta<Str, TS<Int>>({{"b", 5}}),
                      dict_delta<Str, TS<Int>>({}, {"a", "b"}), none));
}

TEST_CASE("ordered reduce checkpoint preserves chain state with updates and topology rebuilds", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSD<Int, TS<Int>>, true>>(
        values<Value>(dict_delta<Int, TS<Int>>({{0, 1}, {1, 2}, {2, 3}}), none,
                      dict_delta<Int, TS<Int>>({{1, 7}}),
                      dict_delta<Int, TS<Int>>({}, {2}),
                      dict_delta<Int, TS<Int>>({{2, 9}}),
                      dict_delta<Int, TS<Int>>({}, {0, 1, 2}),
                      dict_delta<Int, TS<Int>>({{0, 4}}), none),
        values<Int>(0, none, none, none, none, 5, none, none));
}

TEST_CASE("ordered reduce checkpoint preserves dynamic list chain history", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSL<TS<Int>>, true>>(
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}), none,
                      dynamic_list_delta<TS<Int>>({{0, 5}}),
                      dynamic_list_delta<TS<Int>>({}, {1}),
                      dynamic_list_delta<TS<Int>>({{1, 8}, {2, 9}})),
        values<Int>(0, none, none, none, none));
}

TEST_CASE("ordered fixed list recovery restores internal references at every cut", "[checkpoint][reduce][reference]")
{
    stdlib::register_standard_operators();
    compare_every_cut<ReduceComponent<TSL<TS<Int>, 4>, true>>(
        values<Value>(none, list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                      list_delta<TS<Int>>({{2, 3}}), none,
                      list_delta<TS<Int>>({{0, 7}, {3, 4}}),
                      list_delta<TS<Int>>({{1, 8}}), none),
        values<Int>(0, none, none, 5, none, none, none));
}

TEST_CASE("reduce checkpoint restores hidden keyed publication snapshots after root changes", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    compare_every_cut<DictionaryComponent>(values<Value>(
        dict_delta<Str, Dictionary>({{"a", dict_delta<Int, TS<Int>>({{1, 10}})}}),
        dict_delta<Str, Dictionary>({{"b", dict_delta<Int, TS<Int>>({{2, 20}})}}),
        dict_delta<Str, Dictionary>({}, {"a"}), none,
        dict_delta<Str, Dictionary>({{"b", dict_delta<Int, TS<Int>>({{2, 30}})}}),
        dict_delta<Str, Dictionary>({{"c", dict_delta<Int, TS<Int>>({{3, 40}})},
                                     {"d", dict_delta<Int, TS<Int>>({{4, 50}})}}),
        dict_delta<Str, Dictionary>({}, {"b", "c", "d"})));
}

TEST_CASE("reduction checkpoint refuses failed child stops after stopping every live combiner", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    const auto check = []<bool Ordered>() {
        GlobalContext context;
        std::size_t commits = 0;
        configure_component_recovery(context.state().view(), {
            .component_id = "strategy", .commit = [&](const auto &) { ++commits; }});
        failed_stops = 0;
        CHECK_THROWS_WITH(eval_node_with_options<StopFailureComponent<Ordered>>(interval(0, 1),
            values<Value>(dict_delta<Int, TS<Int>>({{0, 1}, {1, 2}, {2, 3}, {3, 4}})), values<Int>(0)),
            Catch::Matchers::ContainsSubstring("reduction child stop failed"));
        CHECK(failed_stops == (Ordered ? 4 : 3));
        CHECK(commits == 0);
    };
    SECTION("tree") { check.template operator()<false>(); }
    SECTION("ordered chain") { check.template operator()<true>(); }
}

TEST_CASE("reduction checkpoint refuses failed retired child stops during topology changes", "[checkpoint][reduce]")
{
    stdlib::register_standard_operators();
    const auto check = []<bool Ordered>() {
        GlobalContext context;
        std::size_t commits = 0;
        configure_component_recovery(context.state().view(), {
            .component_id = "strategy", .commit = [&](const auto &) { ++commits; }});
        failed_stops = 0;
        CHECK_THROWS_WITH(eval_node_with_options<StopFailureComponent<Ordered>>(interval(0, 2),
            values<Value>(dict_delta<Int, TS<Int>>({{0, 1}, {1, 2}, {2, 3}, {3, 4}}),
                          dict_delta<Int, TS<Int>>({}, {2, 3})), values<Int>(0, none)),
            Catch::Matchers::ContainsSubstring("reduction child stop failed"));
        CHECK(failed_stops >= (Ordered ? 4 : 1));
        CHECK(commits == 0);
    };
    SECTION("tree") { check.template operator()<false>(); }
    SECTION("ordered chain") { check.template operator()<true>(); }
}

TEST_CASE("forwarding checkpoint quietly restores historical aliases including empty sources", "[checkpoint][reduce]")
{
    struct Observer final : Notifiable
    {
        std::size_t count{0};
        void notify(DateTime) override { ++count; }
    } observer;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *schema = registry.tsd(integer, registry.ts(integer));
    TSOutput source{schema};
    {
        auto output = source.view(MIN_ST);
        auto mutation = output.as_dict().begin_mutation(MIN_ST);
        auto element = mutation.at(Value{Int{1}}.view());
        auto write = element.begin_mutation(MIN_ST);
        REQUIRE(write.copy_value_from(Value{Int{7}}.view()));
    }
    const auto endpoint = TSEndpointSchema::peered(schema);
    TSOutput alias{endpoint};
    alias.view(MIN_ST + MIN_TD).bind_forwarding_target_sampled(source.view(MIN_ST + MIN_TD));
    const auto image = alias.view(MIN_ST + MIN_TD).checkpoint_forwarding();
    auto malformed = image;
    malformed.window_times.push_back(MIN_ST);
    TSOutput invalid_target{endpoint};
    CHECK_THROWS_AS(invalid_target.view(MIN_ST + 2 * MIN_TD)
                        .validate_checkpoint_forwarding(malformed), std::invalid_argument);
    TSOutput restored{endpoint};
    auto restored_view = restored.view(MIN_ST + 2 * MIN_TD);
    restored_view.subscribe(&observer);
    restored_view.restore_checkpoint_forwarding(source.view(MIN_ST + 2 * MIN_TD), image);
    CHECK(observer.count == 0);
    CHECK(restored_view.last_modified_time() == MIN_ST + MIN_TD);
    CHECK_FALSE(restored_view.modified());
    CHECK(restored_view.value().equals(alias.view(MIN_ST + MIN_TD).value()));
    restored_view.unsubscribe(&observer);

    TSOutput empty{endpoint};
    TSOutput fresh{endpoint};
    fresh.view(MIN_ST).restore_checkpoint_forwarding({}, empty.view(MIN_ST).checkpoint_forwarding());
    CHECK_FALSE(fresh.view(MIN_ST).forwarding_bound());
}
