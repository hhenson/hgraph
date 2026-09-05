// Operator overload dispatch (Phase 1): one logical name (`add_`) collecting several
// implementations, with the most specific selected at the `wire<>` call. Proves the
// runtime registry + TypePattern matcher end to end — specific beats generic, generic
// is the fallback, and no-match / ambiguity raise.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/convert_target.h>
#include <hgraph/lib/std/operators/impl/conversion_impl.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/type_pattern.h>
#include <hgraph/types/type_resolution.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    // The operator: a name + general signature. Not executable — it only anchors the
    // overload set under the name "add".
    struct add_ : Operator<"add", In<"lhs", TsVar<"S">>, In<"rhs", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };

    // Specific implementation: integer add (rank 0 — fully concrete).
    struct add_ints
    {
        static constexpr auto name = "add_ints";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // A second integer add with the identical signature — used only to provoke an
    // ambiguity (two candidates tied at the minimum rank).
    struct add_ints_dup
    {
        static constexpr auto name = "add_ints_dup";
        static void           eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    // Generic fallback (a stand-in for a real generic add): authored over a deferred
    // ``S`` and emits its left input — enough to prove it was the one selected. Rank is
    // high (two top-level variables), so a concrete impl always beats it.
    struct add_generic
    {
        static constexpr auto name = "add_generic";
        static void eval(In<"lhs", TsVar<"S">> lhs, In<"rhs", TsVar<"S">> rhs, Out<TsVar<"S">> out)
        {
            static_cast<void>(rhs);
            const Value delta = capture_delta(lhs.base());
            apply_delta(out, delta.view());
        }
    };

    template <fixed_string Name, typename T>
    struct exact_auto_const_ : Operator<Name, In<"ts", TS<T>>, Out<TS<T>>>
    {
    };

    template <typename T>
    struct exact_auto_const_impl
    {
        static void eval(In<"ts", TS<T>> ts, Out<TS<T>> out) { out.set(ts.value()); }
    };

    using exact_int_auto_const_ = exact_auto_const_<"exact_int_auto_const", Int>;
    using exact_int8_auto_const_ = exact_auto_const_<"exact_int8_auto_const", std::int8_t>;
    using exact_float_auto_const_ = exact_auto_const_<"exact_float_auto_const", Float>;
    using exact_float32_auto_const_ = exact_auto_const_<"exact_float32_auto_const", float>;

    struct requires_auto_const_
        : Operator<"requires_auto_const", In<"ts", TS<Float>>,
                   Scalar<"option", Int>, Out<TS<Float>>>
    {
    };

    struct requires_auto_const_impl
    {
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            return operator_type_resolution::ts_value_schema_at(context, 0) ==
                       scalar_descriptor<Float>::value_meta() &&
                   operator_type_resolution::time_series_schema_at(context, 1) == nullptr;
        }

        static void eval(In<"ts", TS<Float>> ts, Scalar<"option", Int>, Out<TS<Float>> out)
        {
            out.set(ts.value());
        }
    };

    // --- a scalar-argument operator: in * factor (exercises scalar matching + bundle) ---
    struct scale_ : Operator<"scale", In<"in", TsVar<"S">>, Scalar<"factor", ScalarVar<"T">>, Out<TsVar<"S">>>
    {
    };
    struct scale_int
    {
        static constexpr auto name = "scale_int";
        static void           eval(In<"in", TS<Int>> in, Scalar<"factor", Int> factor, Out<TS<Int>> out)
        {
            out.set(in.value() * factor.value());
        }
    };

    struct accept_any_scalar_
        : Operator<"accept_any_scalar", In<"ts", TS<Int>>,
                   Scalar<"option", ScalarVar<"O">>, Out<TS<Int>>>
    {
    };

    struct accept_any_scalar_impl
    {
        static void eval(In<"ts", TS<Int>> ts,
                         Scalar<"option", ScalarVar<"O">> option,
                         Out<TS<Int>> out)
        {
            static_cast<void>(option);
            out.set(ts.value());
        }
    };

    // --- a requires_-gated operator: the gate vetoes the specific overload ---
    struct gated_ : Operator<"gated", In<"in", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
    struct gated_passthrough
    {
        static void eval(In<"in", TsVar<"S">> in, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(in.base());
            apply_delta(out, delta.view());
        }
    };
    struct gated_int_on  // specific, accepted
    {
        static bool requires_(const ResolutionMap &) { return true; }
        static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value()); }
    };
    struct gated_int_off  // specific, vetoed
    {
        static bool requires_(const ResolutionMap &) { return false; }
        static void eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value()); }
    };

    struct sink_ : Operator<"sink", In<"ts", TsVar<"S">>>
    {
    };
    struct sink_any
    {
        static void eval(In<"ts", TsVar<"S">> ts) { static_cast<void>(ts); }
    };

    struct frame_identity_
        : Operator<"frame_identity",
                   In<"ts", TS<FrameOf<ScalarVar<"S">>>>,
                   Out<TS<FrameOf<ScalarVar<"S">>>>>
    {
    };

    struct frame_identity_impl
    {
        static void eval(In<"ts", TS<FrameOf<ScalarVar<"S">>>> ts,
                         Out<TS<FrameOf<ScalarVar<"S">>>> out)
        {
            out.set(ts.value());
        }
    };

    struct route_shape_ : Operator<"route_shape", In<"condition", TS<Bool>>, In<"ts", TsVar<"S">>,
                                   In<"pulse", SIGNAL>>
    {
    };
    struct route_shape_any
    {
        static void eval(In<"condition", TS<Bool>> condition, In<"ts", TsVar<"S">> ts,
                         In<"pulse", SIGNAL> pulse)
        {
            static_cast<void>(condition);
            static_cast<void>(ts);
            static_cast<void>(pulse);
        }
    };
    struct route_shape_labeled
    {
        static auto defaults() { return std::tuple{arg<"label">(Str{})}; }

        static void eval(In<"condition", TS<Bool>> condition, In<"ts", TsVar<"S">> ts,
                         In<"pulse", SIGNAL> pulse, Scalar<"label", Str> label)
        {
            static_cast<void>(condition);
            static_cast<void>(ts);
            static_cast<void>(pulse);
            static_cast<void>(label);
        }
    };

    struct double_ : Operator<"double", In<"in", TS<Int>>, Out<TS<Int>>>
    {
    };
    struct double_graph
    {
        static constexpr auto name = "double_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<TS<Int>> in)
        {
            return wire<add_ints>(w, in, in);
        }
    };

    struct any_window_mean_ : Operator<"any_window_mean", In<"window", TSWAny<Float>>, Out<TS<Float>>>
    {};
    struct any_window_mean_graph
    {
        static constexpr auto  name = "any_window_mean_graph";
        static Port<TS<Float>> compose(Wiring &w, Port<TSWAny<Float>> window) {
            return wire<stdlib::mean>(w, window).as<TS<Float>>();
        }
    };

    struct count_signal_ : Operator<"count_signal_op", In<"pulse", SIGNAL>, Out<TS<Int>>>
    {
    };
    struct count_signal_node
    {
        static constexpr auto name = "count_signal_node";
        static void           eval(In<"pulse", SIGNAL> pulse, State<Int> count, Out<TS<Int>> out)
        {
            if (!pulse.ticked()) { return; }
            const Int next = count.get() + 1;
            count.set(next);
            out.set(next);
        }
    };

    struct count_signal_graph_ : Operator<"count_signal_graph_op", In<"pulse", SIGNAL>, Out<TS<Int>>>
    {
    };
    struct count_signal_graph
    {
        static constexpr auto name = "count_signal_graph";
        static Port<TS<Int>>  compose(Wiring &w, Port<SIGNAL> pulse)
        {
            return wire<count_signal_node>(w, pulse);
        }
    };

    struct choose_ : Operator<"choose", In<"lhs", TS<Int>>, In<"rhs", TS<Int>>,
                                      Scalar<"side", Str>, Out<TS<Int>>>
    {
    };
    struct choose_lhs
    {
        static constexpr auto name = "choose_lhs";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const Str *side = context.scalar_as<Str>("side");
            return side != nullptr && *side == "lhs";
        }
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"side", Str> side, Out<TS<Int>> out)
        {
            static_cast<void>(rhs);
            static_cast<void>(side);
            out.set(lhs.value());
        }
    };
    struct choose_rhs
    {
        static constexpr auto name = "choose_rhs";
        static bool requires_(const ResolutionMap &, OperatorCallContext context)
        {
            const Str *side = context.scalar_as<Str>("side");
            return side != nullptr && *side == "rhs";
        }
        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs,
                         Scalar<"side", Str> side, Out<TS<Int>> out)
        {
            static_cast<void>(lhs);
            static_cast<void>(side);
            out.set(rhs.value());
        }
    };

    struct rank_ : Operator<"rank", In<"lhs", TsVar<"L">>, In<"rhs", TsVar<"R">>, Out<TsVar<"O">>>
    {
    };
    struct rank_aligned
    {
        static constexpr auto name = "rank_aligned";
        static void eval(In<"lhs", TS<ScalarVar<"T">>> lhs, In<"rhs", TS<ScalarVar<"T">>> rhs,
                         Out<TS<ScalarVar<"T">>> out)
        {
            static_cast<void>(rhs);
            out.apply(lhs.base().value());
        }
    };
    struct rank_independent
    {
        static constexpr auto name = "rank_independent";
        static void eval(In<"lhs", TS<ScalarVar<"A">>> lhs, In<"rhs", TS<ScalarVar<"B">>> rhs,
                         Out<TS<ScalarVar<"A">>> out)
        {
            static_cast<void>(rhs);
            out.apply(lhs.base().value());
        }
    };

    struct variadic_rank_ : Operator<"variadic_rank",
                                     In<"a", TS<Int>>,
                                     In<"b", TS<Int>>,
                                     In<"c", TS<Int>>,
                                     Out<TS<Int>>>
    {
    };
    struct variadic_rank_fixed
    {
        static constexpr auto name = "variadic_rank_fixed";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> first, Port<TS<Int>>, Port<TS<Int>>)
        {
            return first;
        }
    };
    struct variadic_rank_fallback
    {
        static constexpr auto name = "variadic_rank_fallback";
        static Port<TS<Int>>  compose(Wiring &, Port<TS<Int>> first, VarIn<"rest", TS<Int>> rest)
        {
            static_cast<void>(rest);
            return first;
        }
    };

    struct packed_varin_prefer_ : Operator<"packed_varin_prefer", In<"ts", TsVar<"S">>, Out<TS<Int>>>
    {
    };
    struct packed_varin_prefer_tsl
    {
        static constexpr auto name = "packed_varin_prefer_tsl";
        static Port<TS<Int>>  compose(Wiring &, Port<TSL<TS<Int>>>)
        {
            return {};
        }
    };
    struct packed_varin_prefer_variadic
    {
        static constexpr auto name = "packed_varin_prefer_variadic";
        static Port<TS<Int>>  compose(Wiring &, VarIn<"ts", TS<Int>>)
        {
            return {};
        }
    };

    struct constrained_ : Operator<"constrained", In<"in", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
    struct constrained_int
    {
        static void eval(In<"in", TS<ScalarVar<"T", Int>>> in,
                         Out<TS<ScalarVar<"T", Int>>> out)
        {
            out.apply(in.base().value());
        }
    };

    struct echo_ : Operator<"echo", In<"in", TsVar<"S">>, Out<TsVar<"S">>>
    {
    };
    [[nodiscard]] inline WiringArg ts_arg(const TSValueTypeMetaData *schema)
    {
        WiringArg arg;
        arg.kind        = WiringArg::Kind::TimeSeries;
        arg.port.schema = schema;
        return arg;
    }

    struct SinkGraph
    {
        static constexpr auto name = "sink_graph";
        static void           compose(Wiring &w)
        {
            auto a = wire<stdlib::replay_impl, TS<Int>>(w, Str{"a"});
            wire<sink_>(w, a);
        }
    };

    struct EchoSetGraph
    {
        static constexpr auto name = "echo_set_graph";
        static void           compose(Wiring &w)
        {
            auto out = wire<echo_, TSS<Int>>(w, stdlib::make_set<Int>({Int{1}, Int{2}}));
            wire<stdlib::dense_record_impl>(w, out, Str{"out"});
        }
    };

    template <typename Graph, typename Seed>
    GraphExecutorValue run_graph(Seed seed)
    {
        GraphBuilder gb = build_graph<Graph>();
        seed(gb.global_state());
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return ex;
    }
}  // namespace

TEST_CASE("operators: the most specific overload is selected (TS<Int> beats the generic)")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<add_, add_ints>();
    register_overload<add_, add_generic>();

    // add_ints ran (sums); had the generic won, the output would equal "a".
    CHECK_OUTPUT(eval_node<add_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30)), values<Int>(11, 22, 33));
}

TEST_CASE("operators: the generic overload is the fallback when no specific one matches")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    register_overload<add_, add_ints>();     // rejects TS<Str>
    register_overload<add_, add_generic>();  // matches anything

    // add_generic ran: it emits its left input.
    CHECK_OUTPUT(eval_node<add_>(values<Str>(Str{"x"}, Str{"y"}), values<Str>(Str{"p"}, Str{"q"})),
                 values<Str>(Str{"x"}, Str{"y"}));
}

TEST_CASE("operators: no matching overload raises")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    register_overload<add_, add_ints>();  // only the int overload — nothing matches TS<Str>

    REQUIRE_THROWS_AS(eval_node<add_>(values<Str>(Str{"x"}), values<Str>(Str{"y"})), OperatorResolutionError);
}

TEST_CASE("operators: an ambiguous overload (tied at the minimum rank) raises")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<add_, add_ints>();
    register_overload<add_, add_ints_dup>();  // identical signature -> same rank

    REQUIRE_THROWS_AS(eval_node<add_>(values<Int>(1), values<Int>(2)), OperatorResolutionError);
}

TEST_CASE("operators: a nominal leaf overload beats inherited Bundle inputs")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.value_type("int");
    const auto *animal = registry.bundle("tests.operator", "Animal", {{"id", integer}});
    const auto *dog = registry.bundle(
        "tests.operator", "Dog", {{"id", integer}, {"barks", registry.value_type("bool")}}, {animal});
    const auto *puppy = registry.bundle(
        "tests.operator", "Puppy",
        {{"id", integer}, {"barks", registry.value_type("bool")}, {"young", registry.value_type("bool")}},
        {dog});

    const auto register_candidate = [&](const ValueTypeMetaData *schema, std::string label) {
        OperatorImpl impl;
        impl.name = "nominal_overload";
        impl.label = std::move(label);
        impl.params.push_back(ParamPattern{
            .kind = ParamPattern::Kind::Input,
            .name = "value",
            .ts = TypePattern::concrete(registry.ts(schema)),
        });
        impl.rank = operator_dispatch_detail::operator_rank(impl.params);
        OperatorRegistry::instance().register_overload(std::move(impl));
    };
    register_candidate(animal, "animal");
    register_candidate(dog, "dog");

    std::array<WiringArg, 1> args{ts_arg(registry.ts(puppy))};
    const auto resolved = OperatorRegistry::instance().resolve(
        "nominal_overload", std::span<const WiringArg>{args}, false);
    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.impl->label == "dog");
    CHECK(registry.value_inheritance_distance(puppy, dog) == 1);
    CHECK(registry.value_inheritance_distance(puppy, animal) == 2);
}

TEST_CASE("operators: a narrower Frame row selects the nearest nominal overload")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.value_type("int");
    const auto *animal = registry.bundle(
        "tests.operator.frame", "Animal", {{"id", integer}}, {}, true);
    const auto *dog = registry.bundle(
        "tests.operator.frame", "Dog",
        {{"id", integer}, {"barks", registry.value_type("bool")}}, {animal});
    const auto *puppy = registry.bundle(
        "tests.operator.frame", "Puppy",
        {{"id", integer}, {"barks", registry.value_type("bool")},
         {"young", registry.value_type("bool")}},
        {dog});

    const auto register_candidate = [&](const ValueTypeMetaData *row,
                                        std::string label) {
        OperatorImpl impl;
        impl.name = "frame_nominal_overload";
        impl.label = std::move(label);
        impl.params.push_back(ParamPattern{
            .kind = ParamPattern::Kind::Input,
            .name = "value",
            .ts = TypePattern::ts(ScalarPattern::frame(
                ScalarPattern::concrete(row))),
        });
        impl.rank = operator_dispatch_detail::operator_rank(impl.params);
        OperatorRegistry::instance().register_overload(std::move(impl));
    };
    register_candidate(animal, "animal");
    register_candidate(dog, "dog");

    std::array<WiringArg, 1> args{
        ts_arg(registry.ts(registry.frame(puppy)))};
    const auto resolved = OperatorRegistry::instance().resolve(
        "frame_nominal_overload", std::span<const WiringArg>{args}, false);
    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.impl->label == "dog");
}

TEST_CASE("operators: opaque nominal values preserve covariance, ranking, and conversion")
{
    auto &registry = TypeRegistry::instance();
    const auto *object = registry.any();
    const auto *animal = registry.opaque_python("tests.operator.opaque.Animal", {object});
    const auto *dog = registry.opaque_python("tests.operator.opaque.Dog", {animal});
    const auto *puppy = registry.opaque_python("tests.operator.opaque.Puppy", {dog});

    const auto register_candidate = [&](const ValueTypeMetaData *schema, std::string label) {
        OperatorImpl impl;
        impl.name = "opaque_nominal_overload";
        impl.label = std::move(label);
        impl.params.push_back(ParamPattern{
            .kind = ParamPattern::Kind::Input,
            .name = "value",
            .ts = TypePattern::concrete(registry.ts(schema)),
        });
        impl.rank = operator_dispatch_detail::operator_rank(impl.params);
        OperatorRegistry::instance().register_overload(std::move(impl));
    };
    register_candidate(object, "object");
    register_candidate(animal, "animal");
    register_candidate(dog, "dog");

    std::array<WiringArg, 1> args{ts_arg(registry.ts(puppy))};
    const auto resolved = OperatorRegistry::instance().resolve(
        "opaque_nominal_overload", std::span<const WiringArg>{args}, false);
    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.impl->label == "dog");

    stdlib::register_conversion_operators();
    Wiring wiring{WiringKind::SubGraph};
    const auto source = WiringPortRef::boundary_source(0, {}, registry.ts(puppy));
    const auto adapted = graph_wiring_detail::adapt_source_for_input(
        wiring, registry.ts(animal), source);
    CHECK(adapted.schema == registry.ts(animal));
    CHECK(adapted.is_peered_source());

    const auto upcast = OperatorRegistry::instance().resolve(
        "convert", std::span<const WiringArg>{args}, true, registry.ts(animal));
    REQUIRE(upcast.impl != nullptr);
    CHECK(upcast.impl->label.find("convert_bundle_upcast") != std::string::npos);

    std::array<WiringArg, 1> base_args{ts_arg(registry.ts(animal))};
    const auto downcast = OperatorRegistry::instance().resolve(
        "convert", std::span<const WiringArg>{base_args}, true, registry.ts(dog));
    REQUIRE(downcast.impl != nullptr);
    CHECK(downcast.impl->label.find("convert_opaque_downcast") != std::string::npos);

    Value puppy_value{ValuePlanFactory::instance().type_for(puppy)};
    puppy_value.as_any().begin_mutation().set(Value{Int{7}});

    WiringArg puppy_scalar;
    puppy_scalar.kind = WiringArg::Kind::Scalar;
    puppy_scalar.scalar_value = puppy_value;
    puppy_scalar.scalar_meta = puppy;
    std::array<WiringArg, 1> scalar_args{puppy_scalar};
    const auto scalar_resolved = OperatorRegistry::instance().resolve(
        "opaque_nominal_overload", std::span<const WiringArg>{scalar_args}, false);
    REQUIRE(scalar_resolved.impl != nullptr);
    CHECK(scalar_resolved.impl->label == "dog");

    register_overload<add_, add_generic>();
    std::array<WiringArg, 2> bound_args{
        ts_arg(registry.ts(animal)), puppy_scalar};
    const auto bound_resolved = OperatorRegistry::instance().resolve(
        "add", std::span<const WiringArg>{bound_args}, true);
    REQUIRE(bound_resolved.impl != nullptr);
    CHECK(bound_resolved.map.find_ts("S") == registry.ts(animal));

    stdlib::register_standard_operators();
    const auto scalar_variable_resolved = OperatorRegistry::instance().resolve(
        "eq_", std::span<const WiringArg>{bound_args}, true);
    REQUIRE(scalar_variable_resolved.impl != nullptr);
    CHECK(scalar_variable_resolved.impl->label.find("eq_any") != std::string::npos);
    CHECK(scalar_variable_resolved.map.find_scalar("T") == animal);

    Wiring scalar_wiring{WiringKind::SubGraph};
    const auto lifted = operator_dispatch_detail::wire_scalar_const(
        scalar_wiring, puppy_scalar, registry.ts(animal));
    CHECK(lifted.schema == registry.ts(animal));

    const auto coerced = operator_dispatch_detail::coerce_scalar_value_to_meta(
        puppy_value, animal);
    REQUIRE(coerced.has_value());
    CHECK(coerced->schema() == animal);
    REQUIRE(coerced->as_any().get().valid());
    CHECK(coerced->as_any().get().checked_as<Int>() == 7);

    const auto *unrelated = registry.opaque_python(
        "tests.operator.opaque.Unrelated", {object});
    CHECK_FALSE(operator_dispatch_detail::coerce_scalar_value_to_meta(
                    puppy_value, unrelated)
                    .has_value());
}

TEST_CASE("operators: input adaptation preserves explicit REF fields in mixed bundles")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts_int = registry.ts(registry.value_type("int"));
    const auto *ref_int = registry.ref(ts_int);
    const auto *source_schema = registry.un_named_tsb({{"keep", ref_int}, {"read", ref_int}});
    const auto *input_schema = registry.un_named_tsb({{"keep", ref_int}, {"read", ts_int}});

    Wiring wiring{WiringKind::SubGraph};
    auto source = WiringPortRef::boundary_source(0, {}, source_schema);
    const auto adapted = graph_wiring_detail::adapt_source_for_input(
        wiring, input_schema, std::move(source));

    REQUIRE(adapted.schema == input_schema);
    REQUIRE(adapted.schema->field_count() == 2);
    CHECK(adapted.schema->fields()[0].type == ref_int);
    CHECK(adapted.schema->fields()[1].type == ts_int);
    CHECK(adapted.is_boundary_source());
}

TEST_CASE("operators: an unregistered operator name raises")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    // No overloads registered this case (the registry is reset between cases).
    REQUIRE_THROWS_AS(eval_node<add_>(values<Int>(1), values<Int>(2)), OperatorResolutionError);
}

TEST_CASE("operators: resolution skips candidates that cannot beat the winner")
{
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<Int>("int");

    ParamPattern input{
        .kind = ParamPattern::Kind::Input,
        .name = "value",
        .ts = TypePattern::concrete(ts_type<TS<Int>>()),
    };
    OperatorImpl winner;
    winner.name = "rank_pruning_test";
    winner.label = "winner";
    winner.params = {input};
    winner.rank = 1;
    winner.requires_predicate = [](const ResolutionMap &, OperatorCallContext) {
        return true;
    };

    int losing_requires_calls = 0;
    OperatorImpl loser = winner;
    loser.label = "loser";
    loser.rank = 100;
    loser.requires_predicate = [&](const ResolutionMap &, OperatorCallContext) {
        ++losing_requires_calls;
        return true;
    };

    OperatorRegistry::instance().register_overload(std::move(winner));
    OperatorRegistry::instance().register_overload(std::move(loser));
    std::array<WiringArg, 1> args{ts_arg(ts_type<TS<Int>>())};

    const auto resolved = OperatorRegistry::instance().resolve(
        "rank_pruning_test", std::span<const WiringArg>{args}, false);

    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.impl->label == "winner");
    CHECK(losing_requires_calls == 0);
}

TEST_CASE("operators: a scalar argument is coerced and forwarded to the resolved node")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<scale_, scale_int>();

    // The int32 factor is a wiring-time scalar argument; the resolved node coerces it to Int.
    CHECK_OUTPUT(eval_node<scale_>(values<Int>(1, 2, 3), std::int32_t{3}), values<Int>(3, 6, 9));
}

TEST_CASE("operators: an unconstrained scalar accepts concrete and absent values")
{
    register_overload<accept_any_scalar_, accept_any_scalar_impl>();

    CHECK_OUTPUT(eval_node<accept_any_scalar_>(values<Int>(1, 2), Int{7}),
                 values<Int>(1, 2));

    WiringArg absent;
    absent.kind = WiringArg::Kind::Scalar;
    std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), std::move(absent)};
    const auto resolved = OperatorRegistry::instance().resolve(
        "accept_any_scalar", std::span<const WiringArg>{args});
    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.map.find_scalar("O") == nullptr);
}

TEST_CASE("operators: a requires_ predicate that passes keeps the specific overload")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<gated_, gated_int_on>();      // specific, accepted
    register_overload<gated_, gated_passthrough>();  // generic fallback (high rank)

    std::array<WiringArg, 1> args{ts_arg(ts_type<TS<Int>>())};
    auto [impl, map, call_args, call_kwargs] = OperatorRegistry::instance().resolve("gated", std::span<const WiringArg>{args});
    REQUIRE(impl != nullptr);
    REQUIRE(impl->params.size() == 1);
    CHECK(ts_pattern_to_string(impl->params[0].ts) == "TS[int]");
}

TEST_CASE("operators: a requires_ predicate that fails vetoes the specific overload")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    register_overload<gated_, gated_int_off>();      // specific, vetoed
    register_overload<gated_, gated_passthrough>();  // generic fallback (high rank)

    std::array<WiringArg, 1> args{ts_arg(ts_type<TS<Int>>())};
    auto [impl, map, call_args, call_kwargs] = OperatorRegistry::instance().resolve("gated", std::span<const WiringArg>{args});
    CHECK(impl->rank > 0);  // the gate rejected the specific overload; the generic was selected
}

TEST_CASE("operators: the TypePattern interpreter matches and ranks a nested TSL")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const TSValueTypeMetaData *concrete = ts_type<TSL<TS<Int>, 2>>();

    // TSL[TS[~T], 2] — a partially-generic nested pattern.
    const TypePattern pattern = to_pattern<TSL<TS<ScalarVar<"T">>, 2>>();
    ResolutionMap     map;
    REQUIRE(ts_pattern_match(pattern, concrete, map));
    CHECK(map.find_scalar("T") == scalar_type<Int>());                 // the inner variable bound to int
    CHECK(ts_pattern_rank(pattern) < ts_pattern_rank(TypePattern::var("S")));  // nested var beats a bare var

    // A different fixed size does not match.
    ResolutionMap other;
    CHECK_FALSE(ts_pattern_match(to_pattern<TSL<TS<ScalarVar<"T">>, 3>>(), concrete, other));
}

TEST_CASE("operators: TSL TypePattern supports named SIZE variables")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const TSValueTypeMetaData *concrete = ts_type<TSL<TS<Int>, 2>>();
    const TypePattern pattern = to_pattern<TSL<TS<ScalarVar<"T">>, SIZE<"N">>>();

    ResolutionMap map;
    REQUIRE(ts_pattern_match(pattern, concrete, map));
    CHECK(map.find_scalar("T") == scalar_type<Int>());
    REQUIRE(map.find_size("N").has_value());
    CHECK(*map.find_size("N") == 2);
    CHECK(ts_pattern_resolve(pattern, map) == concrete);
    CHECK(ts_pattern_to_string(pattern) == "TSL[TS[~T], ~N]");

    using PairPattern = UnNamedTSB<Field<"lhs", TSL<TS<ScalarVar<"T">>, SIZE<"N">>>,
                                   Field<"rhs", TSL<TS<ScalarVar<"T">>, SIZE<"N">>>>;
    using PairConcreteMismatch = UnNamedTSB<Field<"lhs", TSL<TS<Int>, 2>>,
                                            Field<"rhs", TSL<TS<Int>, 3>>>;
    ResolutionMap repeated;
    CHECK_FALSE(ts_pattern_match(to_pattern<PairPattern>(), ts_type<PairConcreteMismatch>(), repeated));
}

TEST_CASE("operators: native concrete TypePatterns lower recursively")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    (void)TypeRegistry::instance().register_scalar<Str>("str");

    const TypePattern list_pattern = to_pattern<TSL<TS<Int>>>();
    CHECK(ts_pattern_to_string(list_pattern) == "TSL[TS[int], 0]");

    ResolutionMap list_match;
    REQUIRE(ts_pattern_match(list_pattern, ts_type<TSL<TS<Int>, 2>>(), list_match));

    ResolutionMap list_mismatch;
    CHECK_FALSE(ts_pattern_match(list_pattern, ts_type<TSL<TS<Float>, 2>>(), list_mismatch));

    const TypePattern dict_pattern = to_pattern<TSD<Str, TS<Int>>>();
    CHECK(ts_pattern_to_string(dict_pattern) == "TSD[str, TS[int]]");

    ResolutionMap dict_match;
    REQUIRE(ts_pattern_match(dict_pattern, ts_type<TSD<Str, TS<Int>>>(), dict_match));
}

TEST_CASE("operators: operator helpers match recursive time-series patterns")
{
    namespace type_resolution = hgraph::operator_type_resolution;

    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");

    const auto *duration_window =
        TypeRegistry::instance().tsw_duration(scalar_type<Int>(), TimeDelta{10}, TimeDelta{2});
    using Bundle = UnNamedTSB<Field<"x", TS<Int>>>;
    std::array<WiringArg, 7> args{
        ts_arg(ts_type<TSL<TS<Int>, 2>>()),
        ts_arg(ts_type<TS<Int>>()),
        ts_arg(ts_type<TSS<Int>>()),
        ts_arg(ts_type<TSD<Str, TS<Int>>>()),
        ts_arg(ts_type<Bundle>()),
        ts_arg(ts_type<TSW<Int, 3, 1>>()),
        ts_arg(duration_window),
    };
    OperatorCallContext context{std::span<const WiringArg>{args}};

    CHECK(type_resolution::time_series_arg_matches<TSL<TS<ScalarVar<"T">>, SIZE<"N">>>(context, 0));
    CHECK(type_resolution::time_series_arg_matches<type_resolution::AnyTSL>(context, 0));
    CHECK(type_resolution::fixed_tsl_arg(context, 0) == ts_type<TSL<TS<Int>, 2>>());

    CHECK_FALSE(type_resolution::time_series_arg_matches<type_resolution::AnyTSL>(context, 1));
    CHECK(type_resolution::time_series_arg_matches<TS<ScalarVar<"T">>>(context, 1));

    const auto *tss = type_resolution::time_series_schema_at(context, 2);
    CHECK(type_resolution::time_series_schema_matches<type_resolution::AnyTSS>(tss));
    CHECK(tss == ts_type<TSS<Int>>());

    const auto *tsd = type_resolution::time_series_schema_at(context, 3);
    CHECK(type_resolution::time_series_schema_matches<type_resolution::AnyTSD>(tsd));
    CHECK(tsd == ts_type<TSD<Str, TS<Int>>>());

    const auto *tsb = type_resolution::time_series_schema_at(context, 4);
    CHECK(type_resolution::time_series_schema_matches<type_resolution::AnyTSB>(tsb));
    CHECK(tsb == ts_type<Bundle>());

    const auto *fixed_window = type_resolution::time_series_schema_at(context, 5);
    CHECK(type_resolution::time_series_schema_matches<type_resolution::AnyTSW>(fixed_window));
    CHECK(fixed_window == ts_type<TSW<Int, 3, 1>>());

    const auto *duration_window_arg = type_resolution::time_series_schema_at(context, 6);
    CHECK(type_resolution::time_series_schema_matches<type_resolution::AnyTSW>(duration_window_arg));
    CHECK(duration_window_arg == duration_window);
}

TEST_CASE("operators: shared TypePattern input matcher mirrors wiring semantics")
{
    namespace type_resolution = hgraph::operator_type_resolution;

    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const TypePattern signal = TypePattern::signal();
    ResolutionMap     strict_signal;
    CHECK_FALSE(ts_pattern_match(signal, ts_type<TS<Int>>(), strict_signal));

    ResolutionMap input_signal;
    CHECK(input_ts_pattern_match(signal, ts_type<TS<Int>>(), input_signal));
    CHECK(type_resolution::time_series_schema_matches<SIGNAL>(ts_type<TS<Int>>()));

    const TypePattern generic_ref = to_pattern<REF<TS<ScalarVar<"T">>>>();
    ResolutionMap     ref_input;
    REQUIRE(input_ts_pattern_match(generic_ref, ts_type<TS<Int>>(), ref_input));
    CHECK(ref_input.find_scalar("T") == scalar_type<Int>());

    ResolutionMap ref_output;
    CHECK_FALSE(output_ts_pattern_match(generic_ref, ts_type<TS<Int>>(), ref_output));

    const TypePattern nested_ref = to_pattern<TSL<REF<TS<ScalarVar<"U">>>, 2>>();
    ResolutionMap     nested_input;
    REQUIRE(input_ts_pattern_match(nested_ref, ts_type<TSL<TS<Int>, 2>>(), nested_input));
    CHECK(nested_input.find_scalar("U") == scalar_type<Int>());

    ResolutionMap nested_strict;
    CHECK_FALSE(ts_pattern_match(nested_ref, ts_type<TSL<TS<Int>, 2>>(), nested_strict));
}

TEST_CASE("operators: TypePattern supports recursive scalar container patterns")
{
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<Int>("int");
    (void)registry.register_scalar<Str>("str");

    {
        const TypePattern pattern = to_pattern<TS<HomogeneousTuple<ScalarVar<"T">>>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, registry.ts(registry.list(scalar_type<Int>(), 0, true)), map));
        CHECK(map.find_scalar("T") == scalar_type<Int>());
        CHECK(ts_pattern_to_string(pattern) == "TS[tuple[~T, ...]]");
    }

    {
        const TypePattern pattern = to_pattern<TS<Tuple<Int, ScalarVar<"T">>>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(
            pattern,
            registry.ts(registry.tuple({scalar_type<Int>(), scalar_type<Str>()})),
            map));
        CHECK(map.find_scalar("T") == scalar_type<Str>());
    }

    {
        const TypePattern pattern = to_pattern<TS<Map<Str, ScalarVar<"V">>>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, registry.ts(registry.map(scalar_type<Str>(), scalar_type<Int>())), map));
        CHECK(map.find_scalar("V") == scalar_type<Int>());
    }

    {
        const TypePattern pattern = to_pattern<TS<Set<ScalarVar<"E">>>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, registry.ts(registry.set(scalar_type<Int>())), map));
        CHECK(map.find_scalar("E") == scalar_type<Int>());
        CHECK(ts_pattern_to_string(pattern) == "TS[set[~E]]");
    }

    {
        const TypePattern pattern = to_pattern<TS<UnknownTuple<ScalarVar<"U">>>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, registry.ts(registry.tuple({scalar_type<Int>(), scalar_type<Int>()})), map));
        CHECK(map.find_scalar("U") == scalar_type<Int>());
        CHECK(ts_pattern_to_string(pattern) == "TS[UnknownTuple[~U]]");
    }
}

TEST_CASE("operators: typed broad schema aliases match runtime schemas")
{
    using namespace hgraph::operator_type_resolution;

    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<Int>("int");
    (void)registry.register_scalar<Str>("str");

    CHECK(time_series_schema_matches<AnyTS>(ts_type<TS<Int>>()));
    CHECK_FALSE(time_series_schema_matches<AnyTS>(ts_type<TSS<Int>>()));
    CHECK(time_series_schema_matches<AnyTSS>(ts_type<TSS<Int>>()));
    CHECK(time_series_schema_matches<AnyTSD>(ts_type<TSD<Int, TS<Str>>>()));
    CHECK(time_series_schema_matches<AnyTSL>(ts_type<TSL<TS<Int>, 2>>()));
    CHECK(time_series_schema_matches<AnyTSW>(ts_type<TSW<Int, 3, 1>>()));

    using Bundle = UnNamedTSB<Field<"x", TS<Int>>>;
    CHECK(time_series_schema_matches<AnyTSB>(ts_type<Bundle>()));
    CHECK(time_series_schema_matches<AnyREF>(registry.ref(ts_type<TS<Int>>())));
}

TEST_CASE("operators: a bare tuple conversion target resolves a Series element type")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.value_type("int");
    const auto *series = registry.ts(registry.series(integer));
    const auto pattern = to_pattern<TS<UnknownTuple<>>>();

    const auto *resolved = stdlib::resolve_convert_target(pattern, {&series, 1});

    REQUIRE(resolved != nullptr);
    REQUIRE(resolved->value_schema != nullptr);
    CHECK(resolved->value_schema->has(ValueTypeFlags::VariadicTuple));
    CHECK(resolved->value_schema->element_type == integer);
}

TEST_CASE("operators: TypePattern supports TSB schema variables")
{
    using Bundle = UnNamedTSB<Field<"x", TS<Int>>>;

    const TypePattern pattern = TypePattern::tsb_var("SCHEMA");
    ResolutionMap map;
    REQUIRE(ts_pattern_match(pattern, ts_type<Bundle>(), map));
    CHECK(map.find_ts("SCHEMA") == ts_type<Bundle>());
    CHECK(ts_pattern_to_string(pattern) == "TSB[~SCHEMA]");

    ResolutionMap other;
    CHECK_FALSE(ts_pattern_match(pattern, ts_type<TS<Int>>(), other));
}

TEST_CASE("operators: a repeated type-variable name forces the operands to be aligned")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    // add_generic is In<~S>, In<~S>, Out<~S> — the repeated ``S`` aligns both operands.
    register_overload<add_, add_generic>();

    // Aligned operands (both TS<Int>): ``S`` binds consistently -> matches.
    {
        std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), ts_arg(ts_type<TS<Int>>())};
        auto [impl, map, call_args, call_kwargs] = OperatorRegistry::instance().resolve("add", std::span<const WiringArg>{args});
        CHECK(impl != nullptr);
        CHECK(map.find_ts("S") == ts_type<TS<Int>>());
    }

    // Misaligned operands (TS<Int>, TS<Float>): ``S`` cannot be both -> no candidate.
    {
        std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), ts_arg(ts_type<TS<Float>>())};
        REQUIRE_THROWS_AS(OperatorRegistry::instance().resolve("add", std::span<const WiringArg>{args}),
                          OperatorResolutionError);
    }
}

TEST_CASE("operators: scalar values auto-wire as const inputs for TS parameters")
{
    register_overload<add_, add_ints>();

    // The Int{3} second argument is a scalar where add_ expects a TS input; it auto-wires as
    // a const TS<Int> and is added per cycle.
    CHECK_OUTPUT(eval_node<add_>(values<Int>(1, 2, 3), Int{3}), values<Int>(4, 5, 6));
}

TEST_CASE("operators: requires predicates see the effective auto-const input schema")
{
    register_overload<requires_auto_const_, requires_auto_const_impl>();

    CHECK_OUTPUT(eval_node<requires_auto_const_>(Float{1.25}, Int{7}), values<Float>(1.25));
}

TEST_CASE("operators: scalar auto-const inputs require exact atomic schemas")
{
    register_overload<exact_int_auto_const_, exact_auto_const_impl<Int>>();
    register_overload<exact_int8_auto_const_, exact_auto_const_impl<std::int8_t>>();
    register_overload<exact_float_auto_const_, exact_auto_const_impl<Float>>();
    register_overload<exact_float32_auto_const_, exact_auto_const_impl<float>>();

    CHECK_OUTPUT(eval_node<exact_int_auto_const_>(Int{7}), values<Int>(7));
    CHECK_OUTPUT(eval_node<exact_float32_auto_const_>(float{1.25F}), values<float>(1.25F));

    // Numeric family membership is irrelevant to template matching: neither
    // widening nor narrowing may change the scalar's atomic schema.
    REQUIRE_THROWS_AS(eval_node<exact_int_auto_const_>(std::int32_t{7}), OperatorResolutionError);
    REQUIRE_THROWS_AS(eval_node<exact_int8_auto_const_>(Int{7}), OperatorResolutionError);
    REQUIRE_THROWS_AS(eval_node<exact_float_auto_const_>(float{1.25F}), OperatorResolutionError);
    REQUIRE_THROWS_AS(eval_node<exact_float32_auto_const_>(Float{1.25}), OperatorResolutionError);
}

TEST_CASE("operators: a scalar must exactly match an already-bound time-series variable")
{
    register_overload<add_, add_generic>();

    CHECK_OUTPUT(eval_node<add_>(values<Int>(1), Int{2}), values<Int>(1));
    REQUIRE_THROWS_AS(eval_node<add_>(values<Int>(1), std::int32_t{2}), OperatorResolutionError);
}

TEST_CASE("operators: runtime concrete patterns require exact scalar auto-const schemas")
{
    OperatorImpl impl;
    impl.name = "runtime_concrete_auto_const";
    impl.label = "runtime_concrete_auto_const";
    impl.params.push_back(ParamPattern{
        .kind = ParamPattern::Kind::Input,
        .name = "ts",
        .ts = TypePattern::concrete(ts_type<TS<Int>>()),
    });
    impl.rank = operator_dispatch_detail::operator_rank(impl.params);
    OperatorRegistry::instance().register_overload(std::move(impl));

    WiringArg exact;
    exact.kind = WiringArg::Kind::Scalar;
    exact.scalar_value = Value{Int{7}};
    exact.scalar_meta = exact.scalar_value.schema();
    std::array<WiringArg, 1> exact_args{std::move(exact)};
    CHECK(OperatorRegistry::instance()
              .resolve("runtime_concrete_auto_const", std::span<const WiringArg>{exact_args})
              .impl != nullptr);

    WiringArg widened;
    widened.kind = WiringArg::Kind::Scalar;
    widened.scalar_value = Value{std::int32_t{7}};
    widened.scalar_meta = widened.scalar_value.schema();
    std::array<WiringArg, 1> widened_args{std::move(widened)};
    REQUIRE_THROWS_AS(
        OperatorRegistry::instance().resolve(
            "runtime_concrete_auto_const", std::span<const WiringArg>{widened_args}),
        OperatorResolutionError);
}

TEST_CASE("operators: sink operators return void and do not expose a null output port")
{
    register_overload<sink_, sink_any>();

    auto ex = run_graph<SinkGraph>(
        [](const GlobalStateView &gs) { set_replay_values<Int>(gs, "a", {1, 2, 3}); });
    static_cast<void>(ex);
}

TEST_CASE("operators: parameter shape distinguishes fixed and generic inputs")
{
    register_overload<route_shape_, route_shape_any>();
    register_overload<route_shape_, route_shape_labeled>();

    const auto shape = OperatorRegistry::instance().parameter_shape("route_shape");
    REQUIRE(shape.has_value());
    CHECK_FALSE(shape->variadic);
    REQUIRE(shape->parameters.size() == 3);

    const OperatorParameterShape &condition = shape->parameters[0];
    CHECK(condition.name == "condition");
    CHECK(condition.kind == ParamPattern::Kind::Input);
    CHECK_FALSE(condition.type_variable);
    CHECK(condition.fixed_ts == ts_type<TS<Bool>>());

    const OperatorParameterShape &ts = shape->parameters[1];
    CHECK(ts.name == "ts");
    CHECK(ts.kind == ParamPattern::Kind::Input);
    CHECK(ts.type_variable);
    CHECK(ts.fixed_ts == nullptr);

    const OperatorParameterShape &pulse = shape->parameters[2];
    CHECK(pulse.name == "pulse");
    CHECK(pulse.kind == ParamPattern::Kind::Input);
    CHECK_FALSE(pulse.type_variable);
    CHECK(pulse.fixed_ts == nullptr);
}

TEST_CASE("operators: overload signature inspection preserves the complete public call shape")
{
    register_overload<route_shape_, route_shape_any>();
    register_overload<route_shape_, route_shape_labeled>();

    const auto signatures = OperatorRegistry::instance().overload_signatures("route_shape");
    REQUIRE(signatures.size() == 2);

    const OperatorOverloadSignature &signature = signatures.front();
    CHECK_FALSE(signature.variadic);
    CHECK_FALSE(signature.has_kwargs);
    CHECK_FALSE(signature.has_output);
    CHECK_FALSE(signature.output_pattern.has_value());
    CHECK(signature.positional_params == signature.parameters.size());
    REQUIRE(signature.parameters.size() == 3);
    CHECK(signature.parameters[0].name == "condition");
    CHECK(signature.parameters[0].kind == ParamPattern::Kind::Input);
    CHECK(signature.parameters[0].type_pattern == "TS[bool]");
    CHECK_FALSE(signature.parameters[0].has_default);
    CHECK(signature.parameters[1].name == "ts");
    CHECK_FALSE(signature.parameters[1].type_pattern.empty());
    CHECK(signature.parameters[2].name == "pulse");
    CHECK(signature.parameters[2].type_pattern == "SIGNAL");

    const OperatorOverloadSignature &defaulted = signatures.back();
    REQUIRE(defaulted.parameters.size() == 4);
    CHECK(defaulted.parameters.back().name == "label");
    CHECK(defaulted.parameters.back().kind == ParamPattern::Kind::Scalar);
    CHECK(defaulted.parameters.back().type_pattern == "str");
    CHECK(defaulted.parameters.back().has_default);

    CHECK(OperatorRegistry::instance().overload_signatures("not_registered").empty());
}

TEST_CASE("operators: graph implementations can be registered as overloads")
{
    register_graph_overload<double_, double_graph>();

    CHECK_OUTPUT(eval_node<double_>(values<Int>(1, 2, 3)), values<Int>(2, 4, 6));
}

TEST_CASE("operators: graph implementations preserve a concrete schema through "
          "a window wildcard") {
    stdlib::register_standard_operators();
    register_graph_overload<any_window_mean_, any_window_mean_graph>();

    Wiring wiring;
    auto   window = wire<stdlib::replay_impl, TSW<Float, 3, 1>>(wiring, std::string{"window"});
    auto   result = wire<any_window_mean_>(wiring, window);

    CHECK(result.erased().schema == schema_descriptor<TS<Float>>::ts_meta());
    CHECK_NOTHROW(std::move(wiring).finish());
}

TEST_CASE("operators: SIGNAL node overloads accept any time-series input")
{
    register_overload<count_signal_, count_signal_node>();

    CHECK_OUTPUT(eval_node<count_signal_>(values<Int>(1, none, 2)), values<Int>(1, none, 2));
}

TEST_CASE("operators: SIGNAL graph overloads accept any time-series input")
{
    register_graph_overload<count_signal_graph_, count_signal_graph>();

    CHECK_OUTPUT(eval_node<count_signal_graph_>(values<Int>(1, none, 2)), values<Int>(1, none, 2));
}

TEST_CASE("operators: requires_ can inspect named scalar arguments")
{
    register_overload<choose_, choose_lhs>();
    register_overload<choose_, choose_rhs>();

    // The "side" scalar selects the rhs implementation via its requires_ predicate.
    CHECK_OUTPUT(eval_node<choose_>(values<Int>(1, 2, 3), values<Int>(10, 20, 30), Str{"rhs"}),
                 values<Int>(10, 20, 30));
}

TEST_CASE("operators: repeated generic variables rank ahead of independent variables")
{
    register_overload<rank_, rank_independent>();
    register_overload<rank_, rank_aligned>();

    std::array<WiringArg, 2> args{ts_arg(ts_type<TS<Int>>()), ts_arg(ts_type<TS<Int>>())};
    auto [impl, map, call_args, call_kwargs] = OperatorRegistry::instance().resolve("rank", std::span<const WiringArg>{args});
    REQUIRE(impl != nullptr);
    CHECK(impl->label.find("rank_aligned") != std::string::npos);
}

TEST_CASE("operators: exact fixed graph overload ranks ahead of a variadic fallback")
{
    register_graph_overload<variadic_rank_, variadic_rank_fallback>();
    register_graph_overload<variadic_rank_, variadic_rank_fixed>();

    std::array<WiringArg, 3> args{ts_arg(ts_type<TS<Int>>()),
                                  ts_arg(ts_type<TS<Int>>()),
                                  ts_arg(ts_type<TS<Int>>())};
    auto [impl, map, call_args, call_kwargs] = OperatorRegistry::instance().resolve("variadic_rank", std::span<const WiringArg>{args});
    REQUIRE(impl != nullptr);
    CHECK(impl->label.find("variadic_rank_fixed") != std::string::npos);
}

TEST_CASE("operators: packed VarIn tails prefer variadic overloads over converted TSL overloads")
{
    register_graph_overload<packed_varin_prefer_, packed_varin_prefer_tsl>();
    register_graph_overload<packed_varin_prefer_, packed_varin_prefer_variadic>();

    const TSValueTypeMetaData *element = ts_type<TS<Int>>();
    std::vector<WiringPortRef> children{
        WiringPortRef::null_source(element),
        WiringPortRef::null_source(element),
    };

    WiringArg packed;
    packed.kind               = WiringArg::Kind::TimeSeries;
    packed.from_variadic_tail = true;
    packed.port = WiringPortRef::structural_source(TypeRegistry::instance().tsl(element, children.size()),
                                                   std::move(children));

    std::array<WiringArg, 1> args{std::move(packed)};
    auto [impl, map, call_args, call_kwargs] =
        OperatorRegistry::instance().resolve("packed_varin_prefer", std::span<const WiringArg>{args});
    REQUIRE(impl != nullptr);
    CHECK(impl->label.find("packed_varin_prefer_variadic") != std::string::npos);
    CHECK(call_args.size() == 2);
}

TEST_CASE("operators: scalar variable constraints reject unsupported scalar types")
{
    register_overload<constrained_, constrained_int>();

    std::array<WiringArg, 1> int_args{ts_arg(ts_type<TS<Int>>())};
    auto [impl, map, call_args, call_kwargs] = OperatorRegistry::instance().resolve("constrained", std::span<const WiringArg>{int_args});
    CHECK(impl != nullptr);
    CHECK(map.find_scalar("T") == scalar_type<Int>());

    std::array<WiringArg, 1> float_args{ts_arg(ts_type<TS<Float>>())};
    REQUIRE_THROWS_AS(OperatorRegistry::instance().resolve("constrained", std::span<const WiringArg>{float_args}),
                      OperatorResolutionError);

    ResolutionMap late_constraint;
    REQUIRE(ts_pattern_match(to_pattern<TS<ScalarVar<"T">>>(), ts_type<TS<Float>>(), late_constraint));
    CHECK_FALSE(ts_pattern_match(to_pattern<TS<ScalarVar<"T", Int>>>(), ts_type<TS<Float>>(), late_constraint));
}

TEST_CASE("operators: TypePattern matches generic TSW and TSB structures")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    {
        const TypePattern pattern = to_pattern<TSW<ScalarVar<"T">, 3, 1>>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, ts_type<TSW<Int, 3, 1>>(), map));
        CHECK(map.find_scalar("T") == scalar_type<Int>());
        ResolutionMap other;
        CHECK_FALSE(ts_pattern_match(pattern, ts_type<TSW<Int, 4, 1>>(), other));
    }

    {
        const auto *duration_window =
            TypeRegistry::instance().tsw_duration(scalar_type<Int>(), TimeDelta{10}, TimeDelta{2});
        const TypePattern any_window = to_pattern<TSWAny<ScalarVar<"T">>>();

        ResolutionMap tick_map;
        REQUIRE(ts_pattern_match(any_window, ts_type<TSW<Int, 3, 1>>(), tick_map));
        CHECK(tick_map.find_scalar("T") == scalar_type<Int>());

        ResolutionMap duration_map;
        REQUIRE(ts_pattern_match(any_window, duration_window, duration_map));
        CHECK(duration_map.find_scalar("T") == scalar_type<Int>());
        CHECK(ts_pattern_resolve(any_window, duration_map) == nullptr);
        CHECK(ts_pattern_to_string(any_window) == "TSW[~T, *]");

        ResolutionMap concrete_window;
        CHECK_FALSE(ts_pattern_match(to_pattern<TSW<ScalarVar<"T">, 3, 1>>(), duration_window, concrete_window));
    }

    {
        using GenericBundle = UnNamedTSB<Field<"x", TS<ScalarVar<"T">>>,
                                         Field<"w", TSW<ScalarVar<"T">, 3, 1>>>;
        using ConcreteBundle = UnNamedTSB<Field<"x", TS<Int>>, Field<"w", TSW<Int, 3, 1>>>;
        const TypePattern pattern = to_pattern<GenericBundle>();
        ResolutionMap map;
        REQUIRE(ts_pattern_match(pattern, ts_type<ConcreteBundle>(), map));
        CHECK(map.find_scalar("T") == scalar_type<Int>());

        const TypePattern renamed = substitute_scalar_patterns(
            pattern,
            {{"T", ScalarPattern::var("U")}});
        ResolutionMap renamed_map;
        REQUIRE(ts_pattern_match(renamed, ts_type<ConcreteBundle>(), renamed_map));
        CHECK(renamed_map.find_scalar("T") == nullptr);
        CHECK(renamed_map.find_scalar("U") == scalar_type<Int>());

        const TypePattern specialized = substitute_scalar_patterns(
            pattern,
            {{"T", ScalarPattern::concrete(scalar_type<Int>())}});
        CHECK(ts_pattern_resolve(specialized, ResolutionMap{}) == ts_type<ConcreteBundle>());
    }
}

TEST_CASE("operators: generic Bundle patterns retain nominal origin and bind arguments")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.value_type("int");
    const auto *text = registry.value_type("str");
    REQUIRE(integer != nullptr);
    REQUIRE(text != nullptr);

    const auto *integer_box = registry.bundle(
        "tests.pattern", "Box[int]", {{"value", integer}}, {}, false, "__type__", {integer});
    const auto *text_box = registry.bundle(
        "tests.pattern", "Box[str]", {{"value", text}}, {}, false, "__type__", {text});
    const auto *integer_other = registry.bundle(
        "tests.pattern", "Other[int]", {{"value", integer}}, {}, false, "__type__", {integer});

    const ScalarPattern pattern = ScalarPattern::bundle_generic(
        "__box", "tests.pattern::Box", {ScalarPattern::var("T")});
    ResolutionMap integer_map;
    REQUIRE(scalar_pattern_match(pattern, integer_box, integer_map));
    CHECK(integer_map.find_scalar("T") == integer);
    CHECK(scalar_pattern_resolve(pattern, integer_map) == integer_box);

    ResolutionMap text_map;
    REQUIRE(scalar_pattern_match(pattern, text_box, text_map));
    CHECK(text_map.find_scalar("T") == text);

    ResolutionMap other_map;
    CHECK_FALSE(scalar_pattern_match(pattern, integer_other, other_map));
    CHECK(scalar_pattern_to_string(pattern) == "tests.pattern::Box[~T]");

    const auto *request = registry.bundle(
        "tests.pattern", "Request", {{"url", text}}, {}, true);
    const auto *create = registry.bundle(
        "tests.pattern", "Create", {{"url", text}, {"value", integer}}, {request});
    stdlib::register_conversion_operators();
    std::array<WiringArg, 1> args{ts_arg(registry.ts(create))};
    const auto resolved = OperatorRegistry::instance().resolve(
        "convert", std::span<const WiringArg>{args}, true, registry.ts(request));
    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.impl->label.find("convert_bundle_upcast") != std::string::npos);

    Wiring wiring{WiringKind::SubGraph};
    const auto source = WiringPortRef::boundary_source(0, {}, registry.ts(create));
    const auto adapted = graph_wiring_detail::adapt_source_for_input(
        wiring, registry.ts(request), source);
    CHECK(adapted.schema == registry.ts(request));
    CHECK(adapted.is_peered_source());
}

TEST_CASE("operators: generic nominal TSB patterns retain their Bundle origin")
{
    using GenericBox = NominalBundle<"tests.pattern", "Box", false, BundleParents<>, BundleArguments<ScalarVar<"T">>,
                                     Field<"value", ScalarVar<"T">>>;
    using IntegerBox =
        NominalBundle<"tests.pattern", "Box", false, BundleParents<>, BundleArguments<Int>, Field<"value", Int>>;
    using IntegerOther =
        NominalBundle<"tests.pattern", "Other", false, BundleParents<>, BundleArguments<Int>, Field<"value", Int>>;
    using GenericBoxTS = NominalTSB<GenericBox, Field<"value", TS<ScalarVar<"T">>>>;
    using IntegerBoxTS = NominalTSB<IntegerBox, Field<"value", TS<Int>>>;
    using IntegerOtherTS = NominalTSB<IntegerOther, Field<"value", TS<Int>>>;

    const TypePattern pattern = to_pattern<GenericBoxTS>();
    ResolutionMap map;
    REQUIRE(input_ts_pattern_match(pattern, ts_type<IntegerBoxTS>(), map));
    CHECK(map.find_scalar("T") == scalar_type<Int>());
    CHECK(ts_pattern_resolve(pattern, map) == ts_type<IntegerBoxTS>());
    CHECK(ts_pattern_to_string(pattern).find("tests.pattern::Box[~T]") != std::string::npos);

    ResolutionMap other_map;
    CHECK_FALSE(input_ts_pattern_match(pattern, ts_type<IntegerOtherTS>(), other_map));
}

TEST_CASE("operators: typed Series and Frame patterns preserve their scalar structure")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_type<Int>();
    const auto *text = scalar_type<Str>();
    const auto *row = registry.bundle("tests.pattern", "Row", {{"value", integer}});

    const ScalarPattern series_pattern = to_scalar_pattern<SeriesOf<ScalarVar<"T">>>();
    ResolutionMap series_map;
    REQUIRE(scalar_pattern_match(series_pattern, registry.series(integer), series_map));
    CHECK(series_map.find_scalar("T") == integer);
    CHECK(scalar_pattern_resolve(series_pattern, series_map) == registry.series(integer));
    CHECK_FALSE(scalar_pattern_match(series_pattern, registry.frame(row), series_map));
    CHECK(scalar_pattern_to_string(series_pattern) == "Series[~T]");

    const ScalarPattern frame_pattern = to_scalar_pattern<FrameOf<ScalarVar<"S">>>();
    ResolutionMap frame_map;
    REQUIRE(scalar_pattern_match(frame_pattern, registry.frame(row), frame_map));
    CHECK(frame_map.find_scalar("S") == row);
    CHECK(scalar_pattern_resolve(frame_pattern, frame_map) == registry.frame(row));
    CHECK_FALSE(scalar_pattern_match(frame_pattern, registry.series(text), frame_map));
    CHECK(scalar_pattern_to_string(frame_pattern) == "Frame[~S]");

    CHECK(scalar_type<SeriesOf<Int>>() == registry.series(integer));
    CHECK(scalar_type<FrameOf<Bundle<"tests.pattern::Row", Field<"value", Int>>>>() ==
          registry.frame(registry.bundle("tests.pattern::Row", {{"value", integer}})));

    const auto *metadata = registry.bundle("tests.pattern::Metadata", {{"version", integer}});
    const ScalarPattern metadata_frame_pattern =
        to_scalar_pattern<FrameOf<ScalarVar<"ROWS">, ScalarVar<"META">>>();
    ResolutionMap metadata_frame_map;
    const auto *typed_frame = registry.frame(row, metadata);
    REQUIRE(scalar_pattern_match(metadata_frame_pattern, typed_frame, metadata_frame_map));
    CHECK(metadata_frame_map.find_scalar("ROWS") == row);
    CHECK(metadata_frame_map.find_scalar("META") == metadata);
    CHECK(scalar_pattern_resolve(metadata_frame_pattern, metadata_frame_map) == typed_frame);
    CHECK(scalar_pattern_to_string(metadata_frame_pattern) == "Frame[~ROWS, ~META]");
    CHECK_FALSE(scalar_pattern_match(frame_pattern, typed_frame, frame_map));
    CHECK_THROWS_AS(registry.frame(row, integer), std::invalid_argument);
}

TEST_CASE("operators: shaped Array patterns resolve element and dimension variables")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_type<Int>();
    const auto *matrix = registry.array(integer, std::vector<std::size_t>{3, 2});

    const ScalarPattern pattern =
        to_scalar_pattern<ArrayOf<ScalarVar<"T">, SIZE<"ROWS">, 2>>();
    ResolutionMap resolution;
    REQUIRE(scalar_pattern_match(pattern, matrix, resolution));
    CHECK(resolution.find_scalar("T") == integer);
    CHECK(resolution.find_size("ROWS") == 3);
    CHECK(scalar_pattern_resolve(pattern, resolution) == matrix);
    CHECK(scalar_pattern_to_string(pattern) == "Array[~T, ~ROWS, 2]");

    ResolutionMap mismatch;
    CHECK_FALSE(scalar_pattern_match(
        pattern, registry.array(integer, std::vector<std::size_t>{3, 4}), mismatch));
}

TEST_CASE("operators: shaped dimensions can be specialized before resolution")
{
    const TypePattern pattern = TypePattern::ts(ScalarPattern::array(
        ScalarPattern::var("T"), {DimensionPattern::var("N")}));
    const TypePattern specialized = substitute_size_patterns(
        pattern, {{"N", DimensionPattern::fixed(4)}});
    ResolutionMap resolution;
    resolution.bind_scalar("T", scalar_descriptor<Int>::value_meta());
    CHECK(ts_pattern_resolve(specialized, resolution) ==
          TypeRegistry::instance().ts(
              TypeRegistry::instance().array(scalar_descriptor<Int>::value_meta(), 4)));
}

TEST_CASE("operators: typed Frame generics are first-class C++ overloads")
{
    auto &registry = TypeRegistry::instance();
    const auto *row = registry.bundle("tests.pattern", "FrameRow", {{"value", scalar_type<Int>()}});
    const auto *frame_ts = registry.ts(registry.frame(row));

    register_overload<frame_identity_, frame_identity_impl>();
    std::array<WiringArg, 1> args{ts_arg(frame_ts)};
    auto resolved = OperatorRegistry::instance().resolve(
        "frame_identity", std::span<const WiringArg>{args});

    REQUIRE(resolved.impl != nullptr);
    CHECK(resolved.map.find_scalar("S") == row);
    CHECK(ts_pattern_resolve(resolved.impl->output, resolved.map) == frame_ts);
}

TEST_CASE("operators: explicit output schemas participate in operator resolution")
{
    // zero_int composes const_, so the conversion family supplies both.
    stdlib::register_conversion_operators();

    CHECK_OUTPUT((eval_node<stdlib::zero_, TS<Int>>(fn<stdlib::add_>())), values<Int>(0));
}

TEST_CASE("operators: caller-supplied type bindings seed resolution")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.value_type("int");
    const auto *text = registry.value_type("str");

    OperatorImpl impl;
    impl.name = "explicit_type_binding";
    impl.label = "explicit_type_binding";
    impl.params.push_back(ParamPattern{
        .kind = ParamPattern::Kind::Input,
        .name = "value",
        .ts = to_pattern<TS<ScalarVar<"IN">>>(),
    });
    impl.has_output = true;
    impl.output = to_pattern<TS<ScalarVar<"OUT">>>();
    impl.rank = operator_dispatch_detail::operator_rank(impl.params);
    OperatorRegistry::instance().register_overload(std::move(impl));

    ResolutionMap initial;
    initial.bind_scalar("OUT", text);
    std::array<WiringArg, 1> args{ts_arg(registry.ts(integer))};
    const auto resolved = OperatorRegistry::instance().resolve(
        "explicit_type_binding", std::span<const WiringArg>{args}, true,
        nullptr, {}, {}, nullptr, &initial);

    CHECK(resolved.map.find_scalar("IN") == integer);
    CHECK(resolved.map.find_scalar("OUT") == text);
    CHECK(ts_pattern_resolve(resolved.impl->output, resolved.map) == registry.ts(text));
}

TEST_CASE("operators: explicit collection output schema drives scalar auto-const matching")
{
    register_overload<echo_, stdlib::pass_through_node>();

    auto ex = run_graph<EchoSetGraph>([](const GlobalStateView &) {});
    const auto out = get_recorded_deltas(ex.view().graph().global_state(), "out");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].has_value());
    CHECK(out[0]->equals(set_delta<Int>({1, 2}, {})));
}
