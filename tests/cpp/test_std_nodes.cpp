// Tests for the small standard library nodes/operators (lib/std): const_,
// null_sink, pass_through_node and debug_print. Use eval_node where the operator
// has an output; keep wired graphs for sinks and structural wiring helpers.

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/specialized_views.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>

#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::literals;
    using hgraph::testing::none;

    using ConstPairTSB = TSB<"ConstPair",
                             Field<"left", TS<Int>>,
                             Field<"right", TS<Int>>>;
    using ConstPairValue = UnNamedBundle<Field<"left", Int>, Field<"right", Int>>;
    using StrPairTSB = TSB<"StrPair",
                           Field<"left", TS<Str>>,
                           Field<"right", TS<Str>>>;
    using StrPairUnNamedTSB = UnNamedTSB<Field<"a", TS<Str>>, Field<"b", TS<Str>>>;

    Value make_const_pair_value(Int left, Int right)
    {
        const auto binding = ValuePlanFactory::instance().type_for(
            value_schema_descriptor<ConstPairValue>::value_meta());
        BundleBuilder builder{binding};
        Value         left_value{left};
        Value         right_value{right};
        builder.set("left", left_value.view());
        builder.set("right", right_value.view());
        return builder.build();
    }

    struct ToTslConstRecordGraph
    {
        static constexpr auto name = "to_tsl_const_record_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<stdlib::const_>(w, "a"_str);
            auto b = wire<stdlib::const_>(w, "b"_str);
            auto c = stdlib::to_tsl<TSL<TS<Str>>>(w, a, b);
            wire<stdlib::dense_record_impl>(w, c, "out"_str);
        }
    };

    struct ToTslReplayRecordGraph
    {
        static constexpr auto name = "to_tsl_replay_record_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<stdlib::replay_impl, TS<Str>>(w, "a"_str);
            auto b = wire<stdlib::replay_impl, TS<Str>>(w, "b"_str);
            auto c = stdlib::to_tsl(w, a, b);
            wire<stdlib::dense_record_impl>(w, c, "out"_str);
        }
    };

    struct ToTsbConstRecordGraph
    {
        static constexpr auto name = "to_tsb_const_record_graph";
        static void           compose(Wiring &w)
        {
            auto left  = wire<stdlib::const_>(w, "a"_str);
            auto right = wire<stdlib::const_>(w, "b"_str);
            auto out   = stdlib::to_tsb<StrPairTSB>(w, left, right);
            wire<stdlib::dense_record_impl>(w, out, "out"_str);
        }
    };

    struct ToTsbReplayRecordGraph
    {
        static constexpr auto name = "to_tsb_replay_record_graph";
        static void           compose(Wiring &w)
        {
            auto a   = wire<stdlib::replay_impl, TS<Str>>(w, "a"_str);
            auto b   = wire<stdlib::replay_impl, TS<Str>>(w, "b"_str);
            auto out = stdlib::to_tsb<StrPairUnNamedTSB>(w, a, b);
            wire<stdlib::dense_record_impl>(w, out, "out"_str);
        }
    };

    // const_(5) -> null_sink: the sink consumes the tick without effect.
    struct NullSinkGraph
    {
        static constexpr auto name = "null_sink_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 5_i);
            wire<stdlib::null_sink>(w, c);
        }
    };

    // replay("in") -> pass_through_node -> record("out").
    struct PassThroughGraph
    {
        static constexpr auto name = "pass_through_graph";
        static void           compose(Wiring &w)
        {
            auto input = wire<stdlib::replay_impl, TS<Int>>(w, "in"_str);
            auto out   = wire<stdlib::pass_through_node>(w, input);
            wire<stdlib::dense_record_impl>(w, out, "out"_str);
        }
    };

    // const_(3) -> debug_print: prints one line.
    struct DebugPrintGraph
    {
        static constexpr auto name = "debug_print_graph";
        static void           compose(Wiring &w)
        {
            auto c = wire<stdlib::const_>(w, 3_i);
            wire<stdlib::debug_print>(w, "demo"_str, c);   // operator order: (label, ts)
        }
    };

    inline std::int32_t retained_unconsumed_evaluations{};

    struct RetainedUnconsumedNode
    {
        static constexpr auto name = "retained_unconsumed_node";

        static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out)
        {
            ++retained_unconsumed_evaluations;
            out.set(in.value());
        }
    };

    struct RetainedUnconsumedGraph
    {
        static constexpr auto name = "retained_unconsumed_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto value = wire<stdlib::const_>(w, 1_i).as<TS<Int>>();
            static_cast<void>(wire<RetainedUnconsumedNode>(w, value));
            return value;
        }
    };

    struct PolymorphicTsdConstGraph
    {
        static constexpr auto name = "polymorphic_tsd_const_graph";

        static Port<void> compose(Wiring &w)
        {
            auto &registry = TypeRegistry::instance();
            const auto *integer = registry.value_type("int");
            const auto *text = registry.value_type("str");
            if (integer == nullptr || text == nullptr)
            {
                throw std::logic_error("polymorphic const test requires primitive schemas");
            }
            const auto *base = registry.bundle(
                "tests.const", "PolymorphicValue", {{"id", integer}}, {}, true);
            const auto *leaf = registry.bundle(
                "tests.const", "PolymorphicLeaf", {{"id", integer}, {"label", text}}, {base});
            const auto realization = TypeRealizationSnapshot::capture(registry);
            const auto base_binding = realization->type_for(base);
            const auto leaf_binding = ValuePlanFactory::instance().type_for(leaf);

            Value leaf_value{leaf_binding};
            auto leaf_fields = leaf_value.as_bundle().begin_mutation();
            leaf_fields["id"].set(Int{7});
            leaf_fields["label"].set(Str{"seven"});
            Value union_value{base_binding};
            base_binding.ops_ref().copy_assign_from(
                base_binding, union_value.begin_mutation().mutable_data(),
                leaf_binding, leaf_value.view().data());

            const Value key{Str{"item"}};
            MapBuilder values{ValuePlanFactory::instance().type_for(text), base_binding};
            values.set_item_copy(key.view().data(), union_value.view().data());
            Value configured = values.build();

            WiringArg argument;
            argument.kind = WiringArg::Kind::Scalar;
            argument.scalar_meta = configured.schema();
            argument.scalar_value = std::move(configured);
            const auto *output_schema = registry.tsd(text, registry.ts(base));
            OperatorWireResult output = wire_operator(
                w, "const", std::span<const WiringArg>{&argument, 1}, true, output_schema);
            if (!output.has_output) { throw std::logic_error("const must produce an output"); }
            return output.output;
        }
    };

    struct WidePolymorphicSchemas
    {
        const ValueTypeMetaData *base{nullptr};
        const ValueTypeMetaData *small{nullptr};
        const ValueTypeMetaData *large{nullptr};
        const TSValueTypeMetaData *ts{nullptr};
        const TSValueTypeMetaData *tss{nullptr};
        const TSValueTypeMetaData *tsd{nullptr};
        std::shared_ptr<const TypeRealizationSnapshot> realization{};
    };

    [[nodiscard]] WidePolymorphicSchemas wide_polymorphic_schemas()
    {
        auto &registry = TypeRegistry::instance();
        const auto *integer = registry.value_type("int");
        const auto *text = registry.value_type("str");
        if (integer == nullptr || text == nullptr)
        {
            throw std::logic_error(
                "wide polymorphic graph test requires primitive schemas");
        }
        const auto *base = registry.bundle(
            "tests.graph_pool", "Value", {{"id", integer}}, {}, true);
        const auto *small = registry.bundle(
            "tests.graph_pool", "Small", {{"id", integer}}, {base});
        const auto *large = registry.bundle(
            "tests.graph_pool", "Large",
            {{"id", integer}, {"a", text}, {"b", text}, {"c", text}},
            {base});
        return WidePolymorphicSchemas{
            .base = base,
            .small = small,
            .large = large,
            .ts = registry.ts(base),
            .tss = registry.tss(base),
            .tsd = registry.tsd(base, registry.ts(integer)),
            .realization = TypeRealizationSnapshot::capture(
                registry,
                TypeRealizationOptions{
                    .polymorphic_compound_storage =
                        PolymorphicCompoundStoragePolicy::Pooled,
                }),
        };
    }

    [[nodiscard]] Value make_polymorphic_leaf(
        const ValueTypeMetaData *schema, Int id, std::string label = {})
    {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema)};
        builder.set("id", Value{id});
        if (schema->field_count > 1)
        {
            builder.set("a", Value{Str{label + "-a"}});
            builder.set("b", Value{Str{label + "-b"}});
            builder.set("c", Value{Str{label + "-c"}});
        }
        return builder.build();
    }

    [[nodiscard]] Value make_polymorphic_union(
        const WidePolymorphicSchemas &schemas, const Value &leaf)
    {
        const auto binding = schemas.realization->type_for(schemas.base);
        Value result{binding};
        binding.ops_ref().copy_assign_from(
            binding, result.begin_mutation().mutable_data(), leaf.binding(),
            leaf.view().data());
        return result;
    }

    [[nodiscard]] Value make_polymorphic_set_delta(
        const WidePolymorphicSchemas &schemas,
        std::span<const Value> added,
        std::span<const Value> removed)
    {
        const auto element = schemas.realization->type_for(schemas.base);
        SetBuilder added_builder{element};
        for (const Value &value : added)
        {
            const Value union_value = make_polymorphic_union(schemas, value);
            static_cast<void>(added_builder.insert_copy(union_value.view().data()));
        }
        SetBuilder removed_builder{element};
        for (const Value &value : removed)
        {
            const Value union_value = make_polymorphic_union(schemas, value);
            static_cast<void>(removed_builder.insert_copy(union_value.view().data()));
        }
        BundleBuilder delta{
            schemas.realization->type_for(schemas.tss->delta_value_schema)};
        delta.set("added", added_builder.build());
        delta.set("removed", removed_builder.build());
        return delta.build();
    }

    [[nodiscard]] Value make_polymorphic_dict_delta(
        const WidePolymorphicSchemas &schemas,
        std::span<const std::pair<const Value *, Int>> modified,
        std::span<const Value> removed)
    {
        const auto key = schemas.realization->type_for(schemas.base);
        const auto integer = ValuePlanFactory::instance().type_for(
            TypeRegistry::instance().value_type("int"));
        MapBuilder modified_builder{key, integer};
        for (const auto &[leaf, value] : modified)
        {
            const Value union_key = make_polymorphic_union(schemas, *leaf);
            modified_builder.set_item_copy(union_key.view().data(),
                                           std::addressof(value));
        }
        SetBuilder removed_builder{key};
        for (const Value &leaf : removed)
        {
            const Value union_key = make_polymorphic_union(schemas, leaf);
            static_cast<void>(removed_builder.insert_copy(union_key.view().data()));
        }
        BundleBuilder delta{
            schemas.realization->type_for(schemas.tsd->delta_value_schema)};
        delta.set("removed", removed_builder.build());
        delta.set("modified", modified_builder.build());
        return delta.build();
    }

    [[nodiscard]] WiringArg scalar_wiring_arg(Value value)
    {
        WiringArg result;
        result.kind = WiringArg::Kind::Scalar;
        result.scalar_meta = value.schema();
        result.scalar_value = std::move(value);
        return result;
    }

    [[nodiscard]] Port<void> runtime_replay(
        Wiring &w, std::string_view key, const TSValueTypeMetaData *schema)
    {
        std::array<WiringArg, 1> args{
            scalar_wiring_arg(Value{Str{key}})};
        auto result = wire_operator(
            w, "replay", std::span<const WiringArg>{args}, true, schema);
        if (!result.has_output)
        {
            throw std::logic_error("runtime replay did not produce an output");
        }
        return result.output;
    }

    struct PolymorphicIntIdentity
    {
        static constexpr auto name = "polymorphic_int_identity";
        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value)
        {
            return value;
        }
    };

    struct WidePolymorphicGraph
    {
        static constexpr auto name = "wide_polymorphic_graph";

        static void compose(Wiring &w)
        {
            const auto schemas = wide_polymorphic_schemas();
            auto scalar = runtime_replay(w, "pool::ts", schemas.ts);
            auto set = runtime_replay(w, "pool::tss", schemas.tss);
            auto dict = runtime_replay(w, "pool::tsd", schemas.tsd);

            auto mapped = wire<stdlib::map_>(
                w, fn<PolymorphicIntIdentity>(), dict);
            auto meshed = wire<stdlib::mesh_>(
                w, fn<PolymorphicIntIdentity>(), dict);
            auto reduced = wire<stdlib::reduce_>(
                w, fn<stdlib::add_>(), mapped, Int{0});

            wire<stdlib::dense_record_impl>(w, scalar, Str{"pool::ts::out"});
            wire<stdlib::dense_record_impl>(w, set, Str{"pool::tss::out"});
            wire<stdlib::dense_record_impl>(w, dict, Str{"pool::tsd::out"});
            wire<stdlib::dense_record_impl>(w, mapped,
                                             Str{"pool::map::out"});
            wire<stdlib::dense_record_impl>(w, meshed,
                                             Str{"pool::mesh::out"});
            wire<stdlib::dense_record_impl>(w, reduced,
                                             Str{"pool::reduce::out"});
        }
    };

}  // namespace

TEST_CASE("stdlib::const_ emits its configured value once at start")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_OUTPUT(testing::eval_node<stdlib::const_>(7_i), {Value{Int{7}}});
}

TEST_CASE("stdlib::const_ delays its single tick by the configured delay")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    // const_(7, delay=2*MIN_TD): no tick until start + 2 cycles, then the value once.
    // Matches Python `eval_node(const, 7, delay=MIN_TD * 2) == [None, None, 7]`.
    CHECK_OUTPUT(testing::eval_node<stdlib::const_>(7_i, MIN_TD * 2),
                 {none, none, Value{Int{7}}});
}

TEST_CASE("stdlib::const_ accepts an explicit scalar output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TS<Int>>(11_i)), {Value{Int{11}}});
}

TEST_CASE("stdlib::const_ accepts an explicit collection output resolution")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TSS<Int>>(stdlib::make_set<Int>({1_i, 2_i}))),
                 {set_delta<Int>({1, 2}, {})});
}

TEST_CASE("stdlib::const_ creates a non-peered fixed TSL from a list value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Value expected = list_delta<TS<Int>>({
        std::pair<std::size_t, Int>{0, 1_i},
        std::pair<std::size_t, Int>{1, 2_i},
        std::pair<std::size_t, Int>{2, 3_i},
    });
    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TSL<TS<Int>, 3>>(
                     stdlib::make_list<Int>({1_i, 2_i, 3_i}))),
                 {expected});
}

TEST_CASE("stdlib::const_ creates a TSD from a map value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Value expected = dict_delta<Str, TS<Int>>({
        std::pair<Str, Int>{Str{"alpha"}, 11_i},
        std::pair<Str, Int>{Str{"beta"}, 22_i},
    });
    CHECK_OUTPUT((testing::eval_node<stdlib::const_, TSD<Str, TS<Int>>>(
                     stdlib::make_map<Str, Int>({{Str{"alpha"}, 11_i}, {Str{"beta"}, 22_i}}))),
                 {expected});
}

TEST_CASE("stdlib::const_ keeps polymorphic TSD values in the graph realization")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto recorded = testing::eval_node<PolymorphicTsdConstGraph>();
    const Value key{Str{"item"}};
    REQUIRE(recorded.size() == 1);
    REQUIRE(recorded.front().has_value());
    const ValueView child = recorded.front()->as_bundle()["modified"].as_map().at(key.view());
    REQUIRE(std::string{child.concrete().schema()->name()} == "tests.const::PolymorphicLeaf");
    REQUIRE(child.concrete().as_bundle()["label"].checked_as<Str>() == "seven");
}

TEST_CASE("wide polymorphic values preserve public graph behaviour across keyed operators")
{
    using namespace hgraph;
    stdlib::register_standard_operators();
    const auto schemas = wide_polymorphic_schemas();
    REQUIRE(schemas.realization->inspect(schemas.base).representation ==
            GraphValueRepresentation::PooledUnion);

    const Value small = make_polymorphic_leaf(schemas.small, 1);
    const Value large = make_polymorphic_leaf(schemas.large, 2, "large");
    const Value small_update = make_polymorphic_leaf(schemas.small, 3);

    const std::vector<std::optional<Value>> scalar_ticks{
        make_polymorphic_union(schemas, small),
        make_polymorphic_union(schemas, large),
        make_polymorphic_union(schemas, small_update),
        make_polymorphic_union(schemas, small),
    };
    const std::array<Value, 2> both{small, large};
    const std::array<Value, 1> only_large{large};
    const std::array<Value, 1> only_small{small};
    const std::vector<std::optional<Value>> set_ticks{
        make_polymorphic_set_delta(schemas, both, {}),
        make_polymorphic_set_delta(schemas, {}, only_large),
        make_polymorphic_set_delta(schemas, only_large, {}),
        make_polymorphic_set_delta(schemas, {}, both),
    };
    const std::array<std::pair<const Value *, Int>, 2> initial{
        std::pair{&small, Int{2}}, std::pair{&large, Int{5}}};
    const std::array<std::pair<const Value *, Int>, 1> update{
        std::pair{&small, Int{3}}};
    const std::vector<std::optional<Value>> dict_ticks{
        make_polymorphic_dict_delta(schemas, initial, {}),
        make_polymorphic_dict_delta(schemas, update, {}),
        make_polymorphic_dict_delta(schemas, {}, only_large),
        make_polymorphic_dict_delta(schemas, {}, only_small),
    };

    std::vector<std::optional<Value>> recorded_scalar;
    std::vector<std::optional<Value>> recorded_set;
    std::vector<std::optional<Value>> recorded_dict;
    std::vector<std::optional<Value>> recorded_map;
    std::vector<std::optional<Value>> recorded_mesh;
    {
        GlobalState graph_state;
        set_pooled_compound_scalar_storage(graph_state.view());
        GlobalContext graph_context{graph_state};
        GraphBuilder graph = build_graph<WidePolymorphicGraph>();
        testing::set_replay_deltas(graph.global_state(), "pool::ts",
                                   scalar_ticks);
        testing::set_replay_deltas(graph.global_state(), "pool::tss",
                                   set_ticks);
        testing::set_replay_deltas(graph.global_state(), "pool::tsd",
                                   dict_ticks);

        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph))
            .start_time(MIN_ST)
            .end_time(MAX_ET);
        GraphExecutorValue executor = executor_builder.make_executor();
        auto view = executor.view();
        view.run();

        const auto pools = view.graph().compound_scalar_storage().inspect();
        REQUIRE(pools.leaf_pool_count >= 2);
        REQUIRE(pools.live_slot_count > 0);
        REQUIRE(pools.slot_capacity >= pools.live_slot_count);
        const auto metrics =
            view.graph().compound_scalar_storage().metrics();
        REQUIRE(metrics.live_bytes > 0);
        REQUIRE(metrics.reserved_bytes >= metrics.live_bytes);

        const auto state = view.graph().global_state();
        recorded_scalar =
            testing::get_recorded_deltas(state, "pool::ts::out");
        recorded_set = testing::get_recorded_deltas(state, "pool::tss::out");
        recorded_dict = testing::get_recorded_deltas(state, "pool::tsd::out");
        recorded_map = testing::get_recorded_deltas(state, "pool::map::out");
        recorded_mesh = testing::get_recorded_deltas(state, "pool::mesh::out");
        REQUIRE(testing::get_recorded_values<Int>(
                    state, "pool::reduce::out") ==
                std::vector<std::optional<Int>>{Int{7}, Int{8}, Int{3},
                                                 Int{0}});
    }

    REQUIRE(recorded_scalar.size() == scalar_ticks.size());
    REQUIRE(recorded_set.size() == set_ticks.size());
    REQUIRE(recorded_dict.size() == dict_ticks.size());
    REQUIRE(recorded_map.size() == dict_ticks.size());
    REQUIRE(recorded_mesh.size() == dict_ticks.size());
    for (std::size_t index = 0; index < scalar_ticks.size(); ++index)
    {
        if (!scalar_ticks[index].has_value())
        {
            REQUIRE_FALSE(recorded_scalar[index].has_value());
            continue;
        }
        REQUIRE(recorded_scalar[index].has_value());
        REQUIRE(recorded_scalar[index]->equals(*scalar_ticks[index]));
    }
    for (std::size_t index = 0; index < set_ticks.size(); ++index)
    {
        REQUIRE(recorded_set[index].has_value());
        REQUIRE(recorded_set[index]->equals(*set_ticks[index]));
    }
    for (std::size_t index = 0; index < dict_ticks.size(); ++index)
    {
        REQUIRE(recorded_dict[index].has_value());
        REQUIRE(recorded_map[index].has_value());
        REQUIRE(recorded_mesh[index].has_value());
        REQUIRE(recorded_dict[index]->equals(*dict_ticks[index]));
        REQUIRE(recorded_map[index]->equals(*dict_ticks[index]));
        REQUIRE(recorded_mesh[index]->equals(*dict_ticks[index]));
    }

    REQUIRE(recorded_scalar[1]
                ->view()
                .concrete()
                .as_bundle()["a"]
                .checked_as<Str>() == "large-a");
}

TEST_CASE("stdlib::const_ creates a non-peered TSB from a structural bundle value")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    Value expected = tsb_delta<ConstPairTSB>(34_i, 55_i);
    CHECK_OUTPUT((testing::eval_node<stdlib::const_, ConstPairTSB>(make_const_pair_value(34_i, 55_i))),
                 {expected});
}

TEST_CASE("stdlib::to_tsl wires const outputs into a fixed non-peered TSL")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<ToTslConstRecordGraph>());
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());

    Value expected = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{0, Str{"a"}},
        std::pair<std::size_t, Str>{1, Str{"b"}},
    });
    CHECK(out[0]->equals(expected));
}

TEST_CASE("stdlib::to_tsl forwards sparse child deltas as inputs become valid")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<ToTslReplayRecordGraph>();
    testing::set_replay_values<Str>(gb.global_state(), "a",
                                    {std::optional<Str>{Str{"a"}},
                                     std::nullopt,
                                     std::optional<Str>{Str{"aa"}}});
    testing::set_replay_values<Str>(gb.global_state(), "b",
                                    {std::nullopt,
                                     std::optional<Str>{Str{"b"}},
                                     std::nullopt});

    GraphExecutorValue executor = testing::run_graph(std::move(gb));
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    REQUIRE(out[0].has_value());
    REQUIRE(out[1].has_value());
    REQUIRE(out[2].has_value());

    Value first = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{0, Str{"a"}},
    });
    Value second = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{1, Str{"b"}},
    });
    Value update = list_delta<TS<Str>>({
        std::pair<std::size_t, Str>{0, Str{"aa"}},
    });
    CHECK(out[0]->equals(first));
    CHECK(out[1]->equals(second));
    CHECK(out[2]->equals(update));
}

TEST_CASE("stdlib::to_tsb wires const outputs into a non-peered TSB")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<ToTsbConstRecordGraph>());
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());

    Value expected = tsb_delta<StrPairTSB>(Str{"a"}, Str{"b"});
    CHECK(out[0]->equals(expected));
}

TEST_CASE("stdlib::to_tsb emits partial field deltas as inputs become valid")
{
    using namespace hgraph;
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<ToTsbReplayRecordGraph>();
    testing::set_replay_values<Str>(gb.global_state(), "a",
                                    {std::optional<Str>{Str{"a"}},
                                     std::nullopt,
                                     std::optional<Str>{Str{"aa"}}});
    testing::set_replay_values<Str>(gb.global_state(), "b",
                                    {std::nullopt,
                                     std::optional<Str>{Str{"b"}},
                                     std::nullopt});

    GraphExecutorValue executor = testing::run_graph(std::move(gb));
    const auto         out      = testing::get_recorded_deltas(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    REQUIRE(out[0].has_value());
    REQUIRE(out[1].has_value());
    REQUIRE(out[2].has_value());

    Value first  = tsb_delta<StrPairUnNamedTSB>(Str{"a"}, std::nullopt);
    Value second = tsb_delta<StrPairUnNamedTSB>(std::nullopt, Str{"b"});
    Value third  = tsb_delta<StrPairUnNamedTSB>(Str{"aa"}, std::nullopt);
    CHECK(out[0]->equals(first));
    CHECK(out[1]->equals(second));
    CHECK(out[2]->equals(third));
}

TEST_CASE("stdlib::const_ rejects explicit output resolution when the value schema differs")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    // const_ is now an operator: a value-schema mismatch makes the only candidate reject,
    // so dispatch reports no matching overload rather than the node's logic_error.
    Wiring w;
    CHECK_THROWS_AS((wire<stdlib::const_, TSS<Int>>(w, Int{7})), OperatorResolutionError);
}

TEST_CASE("stdlib value utilities build compact scalar containers")
{
    using namespace hgraph;

    Value list = stdlib::make_list<Int>({1, 2, 3});
    ListView list_view{list.view()};
    REQUIRE(list_view.size() == 3);
    CHECK(list_view.at(0).checked_as<Int>() == 1);
    CHECK(list_view.at(2).checked_as<Int>() == 3);

    Value set = stdlib::make_set<Int>({1, 2, 2});
    SetView set_view{set.view()};
    REQUIRE(set_view.size() == 2);
    Value one{Int{1}};
    Value three{Int{3}};
    CHECK(set_view.contains(one.view()));
    CHECK_FALSE(set_view.contains(three.view()));

    Value map = stdlib::make_map<Int, Str>({{1, "one"}, {2, "two"}, {2, "deux"}});
    MapView map_view{map.view()};
    REQUIRE(map_view.size() == 2);
    Value two{Int{2}};
    CHECK(map_view.at(two.view()).checked_as<Str>() == "deux");

    Value queue = stdlib::make_queue<Int>({4, 5}, 4);
    QueueView queue_view{queue.view()};
    REQUIRE(queue_view.size() == 2);
    CHECK(queue_view.max_capacity() == 4);
    CHECK(queue_view.front().checked_as<Int>() == 4);
    CHECK(queue_view.back().checked_as<Int>() == 5);

    Value buffer = stdlib::make_cyclic_buffer<Int>(2, {7, 8, 9});
    CyclicBufferView buffer_view{buffer.view()};
    REQUIRE(buffer_view.size() == 2);
    CHECK(buffer_view.capacity() == 2);
    CHECK(buffer_view.front().checked_as<Int>() == 8);
    CHECK(buffer_view.back().checked_as<Int>() == 9);
}

TEST_CASE("stdlib::null_sink consumes its input without error")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<NullSinkGraph>());
    // The source ticked and the sink consumed it; reaching here (no throw) is the check.
    CHECK(executor.view().graph().node_count() == 2);
}

TEST_CASE("stdlib::pass_through_node preserves input ticks")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    GraphBuilder gb = build_graph<PassThroughGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in",
                                    {std::optional<Int>{Int{1}}, std::nullopt, std::optional<Int>{Int{3}}});
    GraphExecutorValue executor = testing::run_graph(std::move(gb));
    const auto         out      = testing::get_recorded_values<Int>(executor.view().graph().global_state(), "out");
    REQUIRE(out.size() == 3);
    CHECK(out[0] == std::optional<Int>{Int{1}});
    CHECK_FALSE(out[1].has_value());
    CHECK(out[2] == std::optional<Int>{Int{3}});
}

TEST_CASE("stdlib::debug_print runs over a tick")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();

    GraphExecutorValue executor = testing::run_graph(build_graph<DebugPrintGraph>());
    CHECK(executor.view().graph().node_count() == 2);
}

TEST_CASE("explicitly wired nodes are retained when their output is unconsumed")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    retained_unconsumed_evaluations = 0;
    CHECK_OUTPUT(eval_node<RetainedUnconsumedGraph>(), values<Int>(1));
    CHECK(retained_unconsumed_evaluations == 1);
}

namespace
{
    // pass_through_node is TsVar-generic; eval_node needs the concrete schema,
    // so drive it through lightweight wrapper graphs (the agreed pattern).
    struct TimeEchoGraph
    {
        [[maybe_unused]] static constexpr auto name = "time_echo_graph";

        static hgraph::Port<hgraph::TS<hgraph::Time>> compose(hgraph::Wiring &w,
                                                              hgraph::Port<hgraph::TS<hgraph::Time>> ts)
        {
            return wire<hgraph::stdlib::pass_through_node>(w, ts).as<hgraph::TS<hgraph::Time>>();
        }
    };

    struct BytesEchoGraph
    {
        [[maybe_unused]] static constexpr auto name = "bytes_echo_graph";

        static hgraph::Port<hgraph::TS<hgraph::Bytes>> compose(hgraph::Wiring &w,
                                                               hgraph::Port<hgraph::TS<hgraph::Bytes>> ts)
        {
            return wire<hgraph::stdlib::pass_through_node>(w, ts).as<hgraph::TS<hgraph::Bytes>>();
        }
    };
}  // namespace

TEST_CASE("time and bytes atoms round-trip through the executor as TS payloads")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    const Time morning = time_of_day(9, 15);
    const Time close   = time_of_day(16, 30, 0, 500);
    CHECK_OUTPUT(eval_node<TimeEchoGraph>(values<Time>(morning, none, close)),
                 values<Time>(morning, none, close));

    const Bytes ping = bytes_("ping");
    const Bytes raw  = bytes_(std::string_view{"\x00\xff", 2});
    CHECK_OUTPUT(eval_node<BytesEchoGraph>(values<Bytes>(ping, raw)),
                 values<Bytes>(ping, raw));
}
