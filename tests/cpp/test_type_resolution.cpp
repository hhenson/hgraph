// Phase 3 of the type-var node work: a node authored ONCE over deferred schemas
// (TsVar / ScalarVar) is resolved to a concrete schema at WIRING time — from a
// connected input port, from an inferred scalar value, or from an explicit type.
// These hand-authored generic nodes exercise that resolution end-to-end alongside
// the real utility nodes (replay/record/const_).

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::literals;
    using namespace hgraph::testing;

    // Generic compute node: re-emit the input's per-cycle delta. Authored once over
    // a deferred TS type ``S`` (input and output share it), driven entirely by the
    // erased runtime delta machinery — works for any resolved kind.
    struct Passthrough
    {
        static constexpr auto name = "rt_passthrough";
        static void           eval(In<"in", TsVar<"S">> in, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(in.base());
            apply_delta(out, delta.view());
        }
    };

    using ConstrainedTs = TsVar<"S", TS<Int>, TS<Str>>;

    struct ConstrainedPassthrough
    {
        static constexpr auto name = "rt_constrained_passthrough";

        static void eval(In<"in", ConstrainedTs> in, Out<ConstrainedTs> out)
        {
            const Value delta = capture_delta(in.base());
            apply_delta(out, delta.view());
        }
    };

    using ConstrainedScalarTs = TS<ScalarVar<"T", Int, Str>>;

    struct ConstrainedScalarPassthrough
    {
        static constexpr auto name = "rt_constrained_scalar_passthrough";

        static void eval(In<"in", ConstrainedScalarTs> in, Out<ConstrainedScalarTs> out)
        {
            const Value delta = capture_delta(in.base());
            apply_delta(out, delta.view());
        }
    };

    using GenericScalarBundle = UnNamedTSB<Field<"p1", TS<ScalarVar<"T">>>>;
    using IntScalarBundle     = UnNamedTSB<Field<"p1", TS<Int>>>;

    using GenericEditsBundle = UnNamedTSB<
        Field<"edits", TSD<ScalarVar<"K">, TsVar<"V">>>,
        Field<"removes", TSS<ScalarVar<"K">>>>;
    using ConcreteEditsBundle = UnNamedTSB<
        Field<"edits", TSD<Int, TS<Str>>>,
        Field<"removes", TSS<Int>>>;

    struct GenericBundleFromScalar
    {
        static constexpr auto name = "rt_generic_bundle_from_scalar";

        static void eval(In<"p1", TS<ScalarVar<"T">>> p1, Out<GenericScalarBundle> out)
        {
            const Value delta = capture_delta(p1.base());
            apply_delta(out.field<"p1">(), delta.view());
        }
    };

    struct GenericEditsFromInputs
    {
        static constexpr auto name = "rt_generic_edits_from_inputs";

        static void eval(In<"edits", TSD<ScalarVar<"K">, TsVar<"V">>> edits,
                         In<"removes", TSS<ScalarVar<"K">>> removes,
                         Out<GenericEditsBundle> out)
        {
            if (edits.modified())
            {
                const Value delta = capture_delta(edits.base());
                apply_delta(out.field<"edits">().base(), delta.view());
            }
            if (removes.modified())
            {
                const Value delta = capture_delta(removes.base());
                apply_delta(out.field<"removes">().base(), delta.view());
            }
        }
    };

    // Generic source: emit a configured constant. The scalar variable ``T`` is
    // inferred from the supplied value; the default resolver binds output ``S`` to
    // ``TS<T>`` when no explicit output schema is supplied.
    struct GenConst
    {
        static constexpr auto name             = "rt_gen_const";
        static constexpr bool schedule_on_start = true;
        static void resolve_default_types(ResolutionMap &resolution)
        {
            resolution.bind_ts("S", TypeRegistry::instance().ts(resolution.scalar("T")));
        }
        static void eval(Scalar<"value", ScalarVar<"T">> value, Out<TsVar<"S">> out)
        {
            out.apply(value.value());
        }
    };

    // The concrete graph signature lets eval_node supply the harness while the
    // wrapped node still resolves its generic schema from the connected port.
    struct PassthroughGraph
    {
        static constexpr auto name = "rt_passthrough_graph";
        static Port<TS<Int>>   compose(Wiring &w, Port<TS<Int>> in)
        {
            return wire<Passthrough>(w, in).as<TS<Int>>();
        }
    };

    // Same Passthrough definition, now resolved to TSS<Int> from its input port.
    struct PassthroughSetGraph
    {
        static constexpr auto name = "rt_passthrough_set_graph";
        static Port<TSS<Int>>  compose(Wiring &w, Port<TSS<Int>> in)
        {
            return wire<Passthrough>(w, in).as<TSS<Int>>();
        }
    };

    struct ConstrainedIntGraph
    {
        static constexpr auto name = "rt_constrained_int_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> in)
        {
            return wire<ConstrainedPassthrough>(w, in).as<TS<Int>>();
        }
    };

    struct ConstrainedStrGraph
    {
        static constexpr auto name = "rt_constrained_str_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> in)
        {
            return wire<ConstrainedPassthrough>(w, in).as<TS<Str>>();
        }
    };

    struct ConstrainedFloatGraph
    {
        static constexpr auto name = "rt_constrained_float_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> in)
        {
            return wire<ConstrainedPassthrough>(w, in).as<TS<Float>>();
        }
    };

    struct ConstrainedScalarIntGraph
    {
        static constexpr auto name = "rt_constrained_scalar_int_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> in)
        {
            return wire<ConstrainedScalarPassthrough>(w, in).as<TS<Int>>();
        }
    };

    struct ConstrainedScalarStrGraph
    {
        static constexpr auto name = "rt_constrained_scalar_str_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> in)
        {
            return wire<ConstrainedScalarPassthrough>(w, in).as<TS<Str>>();
        }
    };

    struct ConstrainedScalarFloatGraph
    {
        static constexpr auto name = "rt_constrained_scalar_float_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> in)
        {
            return wire<ConstrainedScalarPassthrough>(w, in).as<TS<Float>>();
        }
    };

    // The leaf input, not an already-resolved bundle, forces T = Int on the
    // generic node. The concrete return type makes that resolution visible to
    // eval_node's output harness.
    struct GenericBundleFromIntGraph
    {
        static constexpr auto name = "rt_generic_bundle_from_int_graph";

        static Port<IntScalarBundle> compose(Wiring &w, Port<TS<Int>> p1)
        {
            return wire<GenericBundleFromScalar>(w, p1).as<IntScalarBundle>();
        }
    };

    struct GenericEditsGraph
    {
        static constexpr auto name = "rt_generic_edits_graph";

        static Port<ConcreteEditsBundle> compose(Wiring &w, Port<TSD<Int, TS<Str>>> edits,
                                                  Port<TSS<Int>> removes)
        {
            return wire<GenericEditsFromInputs>(w, edits, removes).as<ConcreteEditsBundle>();
        }
    };

}  // namespace

TEST_CASE("type_resolution: a generic node resolves its TS type from the connected input port")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    CHECK_OUTPUT(eval_node<PassthroughGraph>(values<Int>(1, none, 3)), {1, none, 3});
}

TEST_CASE("type_resolution: resolved-variable queries share one typed contract")
{
    ResolutionMap resolution;

    CHECK_FALSE(resolution.is_resolved("S"));
    CHECK_FALSE(resolution.is_resolved<ResolutionKind::TimeSeries>("S"));
    CHECK_FALSE(resolution.is_resolved<ResolutionKind::Scalar>("S"));
    CHECK_FALSE(resolution.is_resolved<ResolutionKind::Size>("S"));

    resolution.bind_ts("S", ts_type<TS<Int>>());
    CHECK(resolution.is_resolved("S"));
    CHECK(resolution.is_resolved<ResolutionKind::TimeSeries>("S"));
    CHECK_FALSE(resolution.is_resolved<ResolutionKind::Scalar>("S"));
    CHECK_FALSE(resolution.is_resolved<ResolutionKind::Size>("S"));

    resolution.bind_scalar("T", scalar_descriptor<Str>::value_meta());
    resolution.bind_size("N", 3);
    CHECK(resolution.is_resolved<ResolutionKind::Scalar>("T"));
    CHECK(resolution.is_resolved<ResolutionKind::Size>("N"));
    CHECK(resolution.is_resolved("T"));
    CHECK(resolution.is_resolved("N"));
}

TEST_CASE("type_resolution: the same generic node resolves to TSS from a set-valued port")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const std::vector<std::optional<Value>> deltas{set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1})};
    CHECK_OUTPUT(eval_node<PassthroughSetGraph>(deltas), deltas);
}

TEST_CASE("type_resolution: a constrained generic node accepts each declared TS schema")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    CHECK_OUTPUT(eval_node<ConstrainedIntGraph>(values<Int>(1, 2)), values<Int>(1, 2));
    CHECK_OUTPUT(eval_node<ConstrainedStrGraph>(values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"a"}, Str{"b"}));
}

TEST_CASE("type_resolution: a constrained generic node rejects an undeclared TS schema")
{
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    REQUIRE_THROWS(eval_node<ConstrainedFloatGraph>(values<Float>(1.0, 2.0)));
}

TEST_CASE("type_resolution: scalar constraints apply through public generic-node wiring")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    CHECK_OUTPUT(eval_node<ConstrainedScalarIntGraph>(values<Int>(1, 2)), values<Int>(1, 2));
    CHECK_OUTPUT(eval_node<ConstrainedScalarStrGraph>(values<Str>(Str{"a"}, Str{"b"})),
                 values<Str>(Str{"a"}, Str{"b"}));
    REQUIRE_THROWS(eval_node<ConstrainedScalarFloatGraph>(values<Float>(1.0, 2.0)));
}

TEST_CASE("type_resolution: a concrete input resolves a scalar-generic TSB output")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    CHECK_OUTPUT(eval_node<GenericBundleFromIntGraph>(values<Int>(1, 2)),
                 values<Value>(tsb_delta<IntScalarBundle>(Int{1}),
                               tsb_delta<IntScalarBundle>(Int{2})));
}

TEST_CASE("type_resolution: scalar and time-series variables resolve together inside a TSB")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");

    const auto edits = values<Value>(dict_delta<Int, TS<Str>>({{1, Str{"one"}}}),
                                     dict_delta<Int, TS<Str>>({{2, Str{"two"}}}, {1}));
    const auto removes = values<Value>(set_delta<Int>({}, {}), set_delta<Int>({1}, {}));

    CHECK_OUTPUT(
        eval_node<GenericEditsGraph>(edits, removes),
        values<Value>(
            tsb_delta<ConcreteEditsBundle>(dict_delta<Int, TS<Str>>({{1, Str{"one"}}}),
                                           set_delta<Int>({}, {})),
            tsb_delta<ConcreteEditsBundle>(dict_delta<Int, TS<Str>>({{2, Str{"two"}}}, {1}),
                                           set_delta<Int>({1}, {}))));
}

TEST_CASE("type_resolution: a generic source infers its type from the configured scalar value")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    CHECK_OUTPUT(eval_node<GenConst>(7_i), values<Value>(Value{7_i}));
}

TEST_CASE("type_resolution: a second resolution of the same generic source does not collide")
{
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    CHECK_OUTPUT(eval_node<GenConst>(2.5_f), values<Value>(Value{2.5_f}));
}

TEST_CASE("type_resolution: pattern_variables reports declared variables in order")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");

    // The ordering contract the wiring layer maps unnamed pre-resolution items
    // onto: children are visited in declaration order.
    CHECK(pattern_variables(to_pattern<TS<ScalarVar<"T">>>()) == std::vector<std::string>{"T"});

    // A TSD reports its key before its value.
    CHECK(pattern_variables(to_pattern<TSD<ScalarVar<"K">, TS<ScalarVar<"V">>>>())
          == std::vector<std::string>{"K", "V"});

    // A TSL reports its element before its size, matching TSL<element, size>.
    CHECK(pattern_variables(to_pattern<TSL<TS<ScalarVar<"E">>, SIZE<"N">>>())
          == std::vector<std::string>{"E", "N"});

    // A whole-time-series variable is itself a declared variable.
    CHECK(pattern_variables(to_pattern<TsVar<"OUT">>()) == std::vector<std::string>{"OUT"});

    // A concrete pattern declares nothing.
    CHECK(pattern_variables(to_pattern<TS<Int>>()).empty());

    // Repeats collapse to first appearance.
    CHECK(pattern_variables(to_pattern<TSD<ScalarVar<"K">, TS<ScalarVar<"K">>>>())
          == std::vector<std::string>{"K"});
}

TEST_CASE("type_resolution: pattern_variables omits synthesized bundle variables")
{
    // A generic nominal bundle binds its whole schema to a manufactured
    // variable so the matcher can unify the bundle as well as its arguments.
    // That name is internal: reporting it would make a signature look as though
    // it declared a type variable its author never wrote.
    ScalarPattern generic_bundle = ScalarPattern::bundle_generic(
        "__bundle__module::Box", "module::Box", {ScalarPattern::var("T")});
    CHECK(pattern_variables(generic_bundle) == std::vector<std::string>{"T"});

    CHECK(pattern_variables(TypePattern::ts(generic_bundle)) == std::vector<std::string>{"T"});
}
