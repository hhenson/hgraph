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
#include <chrono>
#include <iostream>
#include <limits>
#include <numeric>

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
    struct ScalingMapStrategy
    {
        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, NamedPort<"ts", TSD<Int, TS<Int>>> input)
        {
            return wire<stdlib::map_>(w, fn<ReadMappedReference>(), input).as<TSD<Int, TS<Int>>>();
        }
    };
    struct ScalingMapComponent
    {
        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> input)
        {
            return stdlib::component<ScalingMapStrategy>(w, "dynamic-map", input);
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

    Value dense_integer_dict(const std::vector<Int> &items)
    {
        const auto integer = TypeRegistry::instance().scalar_type<Int>();
        const auto empty_delta = dict_delta<Int, TS<Int>>({});
        const auto empty_fields = empty_delta.as_bundle();
        MapBuilder modified{integer, integer};
        for (std::size_t index = 0; index < items.size(); ++index)
            modified.set_item(static_cast<Int>(index), items[index]);
        BundleBuilder delta{ValuePlanFactory::instance().type_for(empty_delta.schema())};
        delta.set("removed", Value{empty_fields.at("removed")});
        delta.set("modified", modified.build());
        return delta.build();
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

TEST_CASE("keyed map checkpoint bounds child allocation by its encoded slot partition", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    const auto malformed = GENERATE(0, 1, 2);
    CAPTURE(malformed);
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    std::size_t commits{};
    configure_component_recovery(context.state().view(), {
        .component_id = "dynamic-map", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; ++commits; }});
    CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<false>>(interval(0, 1),
        values<Value>(dict_delta<Int, TS<Int>>({{0, 1}}))),
        values<Value>(dict_delta<Int, TS<Int>>({{0, 1}})));
    REQUIRE(completed);
    const auto original = *completed;
    bool changed{};
    for (auto &node : completed->graph.nodes)
    {
        if (node.custom.children.empty()) { continue; }
        if (malformed == 0)
        {
            // A single child ordinal must never drive an enormous allocation.
            node.custom.children.front().slot = std::numeric_limits<std::size_t>::max() - 1;
        }
        else
        {
            const auto old_metadata = node.custom.payload.as_list();
            REQUIRE(old_metadata.size() > 4);
            ListBuilder metadata{TypeRegistry::instance().scalar_type<Int>()};
            for (std::size_t i = 0; i < old_metadata.size(); ++i)
            {
                const auto value = malformed == 1 && i == 2 ? std::numeric_limits<Int>::max()
                    : malformed == 2 && i == 4 ? static_cast<Int>(node.custom.children.front().slot)
                    : old_metadata.at(i).checked_as<Int>();
                metadata.push_back(value);
            }
            node.custom.payload = metadata.build();
        }
        changed = true;
        break;
    }
    REQUIRE(changed);
    reference_child_starts = 0;
    CHECK_THROWS_WITH(eval_node_with_options<ReferenceComponent<false>>(interval(1, 2), values<Value>(none)),
        Catch::Matchers::ContainsSubstring("component checkpoint: map"));
    CHECK(reference_child_starts == 0);
    CHECK(commits == 1);
    CHECK(completed->cut == original.cut);

    completed = original;
    CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<false>>(interval(1, 2),
        values<Value>(dict_delta<Int, TS<Int>>({{0, 2}}))),
        values<Value>(dict_delta<Int, TS<Int>>({{0, 3}})));
    CHECK(commits == 2);
}

TEST_CASE("keyed map checkpoint preserves a sparse high source slot and later slot reuse", "[checkpoint][map]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "dynamic-map", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    (void)eval_node_with_options<ReferenceComponent<false>>(interval(0, 2), values<Value>(
        dict_delta<Int, TS<Int>>({{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}}),
        dict_delta<Int, TS<Int>>({}, {0, 1, 2, 3, 4, 5, 6})));
    REQUIRE(completed);
    bool sparse{};
    for (const auto &node : completed->graph.nodes)
    {
        if (node.custom.children.empty()) { continue; }
        REQUIRE(node.custom.children.size() == 1);
        CHECK(node.custom.children.front().slot == 7);
        CHECK(node.custom.payload.as_list().at(2).checked_as<Int>() >= 8);
        sparse = true;
    }
    REQUIRE(sparse);
    CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<false>>(interval(2, 4), values<Value>(
        dict_delta<Int, TS<Int>>({{7, 2}}), dict_delta<Int, TS<Int>>({{0, 4}}))),
        values<Value>(dict_delta<Int, TS<Int>>({{7, 3}}), dict_delta<Int, TS<Int>>({{0, 4}})));
}

TEST_CASE("mapped key references restore a complete custom endpoint inventory before evaluation", "[checkpoint][map][reference]")
{
    stdlib::register_standard_operators();
    constexpr std::size_t count = 256;
    std::vector<Int> expected(count);
    std::iota(expected.begin(), expected.end(), Int{0});
    GlobalContext context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(), {
        .component_id = "dynamic-map", .load = [&] { return completed; },
        .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<false>>(interval(0, 1),
        values<Value>(dense_integer_dict(std::vector<Int>(count, -1)))), values<Value>(dense_integer_dict(expected)));
    REQUIRE(completed);
    std::size_t key_references{};
    for (const auto &owner : completed->graph.nodes)
        for (const auto &child : owner.custom.children)
            for (const auto &node : child.graph->nodes)
                if (node.output && node.output->reference && node.output->reference->target &&
                    node.output->reference->target->endpoint == 4) { ++key_references; }
    REQUIRE(key_references == count);
    CHECK_OUTPUT(eval_node_with_options<ReferenceComponent<false>>(interval(1, 3),
        values<Value>(none, dict_delta<Int, TS<Int>>({{255, 2}, {0, 3}}))),
        values<Value>(none, dict_delta<Int, TS<Int>>({{255, 257}, {0, 3}})));
}

TEST_CASE("mapped child checkpoint capture and recovery scaling", "[.][checkpoint-scaling]")
{
    stdlib::register_standard_operators();
    const auto measure = []<typename Graph>(bool key_references) {
        for (const std::size_t count : {1000, 2000, 4000, 8000})
        {
            const auto input = dense_integer_dict(std::vector<Int>(count, key_references ? -1 : 1));
            std::vector<Int> expected(count, 1);
            if (key_references) { std::iota(expected.begin(), expected.end(), Int{0}); }
            // The same graph with no recovery configured: what the run costs
            // before a checkpoint is asked for.
            double unmanaged_ms{};
            {
                GlobalContext unmanaged;
                const auto unmanaged_start = std::chrono::steady_clock::now();
                (void)eval_node_with_options<Graph>(interval(0, 1), values<Value>(input));
                unmanaged_ms = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - unmanaged_start).count();
            }
            GlobalContext context;
            std::optional<ComponentCheckpoint> completed;
            configure_component_recovery(context.state().view(), {
                .component_id = "dynamic-map", .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            const auto first_start = std::chrono::steady_clock::now();
            const auto first = eval_node_with_options<Graph>(interval(0, 1), values<Value>(input));
            const auto fresh_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - first_start).count();
            REQUIRE(completed);
            CHECK_OUTPUT(first, values<Value>(dense_integer_dict(expected)));
            std::size_t captured_children{};
            for (const auto &node : completed->graph.nodes) { captured_children += node.custom.children.size(); }
            REQUIRE(captured_children == count);

            const auto resume_start = std::chrono::steady_clock::now();
            CHECK_OUTPUT(eval_node_with_options<Graph>(interval(1, 2), values<Value>(none)), values<Value>(none));
            const auto resume_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - resume_start).count();
            // Both measurements include public wiring and teardown. The resumed
            // quiet run performs real endpoint/child import and a new capture.
            std::cout << "checkpoint_mapped_children mode=" << (key_references ? "key_reference" : "stateful")
                      << " count=" << count << " unmanaged_run_ms=" << unmanaged_ms
                      << " fresh_run_ms=" << fresh_ms
                      << " resumed_quiet_run_ms=" << resume_ms << '\n';
            const auto last = static_cast<Int>(count - 1);
            CHECK_OUTPUT(eval_node_with_options<Graph>(interval(2, 3),
                values<Value>(dict_delta<Int, TS<Int>>({{last, 2}}))),
                values<Value>(dict_delta<Int, TS<Int>>({{last, (key_references ? last : Int{1}) + 2}})));
        }
    };
    SECTION("stateful children") { measure.template operator()<ScalingMapComponent>(false); }
    SECTION("synthetic key references") { measure.template operator()<ReferenceComponent<false>>(true); }
}
