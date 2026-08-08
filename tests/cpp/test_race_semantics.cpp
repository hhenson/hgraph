#include <catch2/catch_test_macros.hpp>
#include <hgraph/lib/std/operators/impl/control_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>

using namespace hgraph;

TEST_CASE("race over structural TSLs: output schema is REF<TSL>")
{
    stdlib::register_standard_operators();
    Wiring w;
    auto  a1 = wire<stdlib::const_, TS<Int>>(w, Int{21});
    auto  a2 = wire<stdlib::const_, TS<Int>>(w, Int{22});
    auto  b1 = wire<stdlib::const_, TS<Int>>(w, Int{31});
    auto  b2 = wire<stdlib::const_, TS<Int>>(w, Int{32});
    auto &registry = TypeRegistry::instance();
    const auto *tsl_schema = registry.tsl(registry.ts(scalar_descriptor<Int>::value_meta()), 2);

    WiringPortRef tsl_a = WiringPortRef::structural_source(tsl_schema, {a1.erased(), a2.erased()});
    WiringPortRef tsl_b = WiringPortRef::structural_source(tsl_schema, {b1.erased(), b2.erased()});

    WiringArg lhs;
    lhs.kind = WiringArg::Kind::TimeSeries;
    lhs.port = tsl_a;
    WiringArg rhs;
    rhs.kind = WiringArg::Kind::TimeSeries;
    rhs.port = tsl_b;
    std::array<WiringArg, 2> args{lhs, rhs};
    auto result = wire_operator(w, "race", args);
    const auto *schema = result.output.erased().schema;
    REQUIRE(schema != nullptr);
    INFO("out kind " << static_cast<int>(schema->kind));
    CHECK(schema->kind == TSTypeKind::REF);
    REQUIRE(schema->referenced_ts() != nullptr);
    INFO("referenced kind " << static_cast<int>(schema->referenced_ts()->kind));
    CHECK(schema->referenced_ts()->kind == TSTypeKind::TSL);
}

namespace
{
    using namespace hgraph::stdlib;

    // The hgraph revocation scenario (test_race_tsls): candidate 1 goes
    // INVALID (if_ stops publishing on the false branch) - race must re-race
    // to candidate 2 and the consumer must tick the new winner's value.
    struct RaceRevokeGraph
    {
        static constexpr auto name = "race_revoke_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> invalidate)
        {
            auto v11  = wire<if_, UnNamedTSB<Field<"true", REF<TS<Int>>>, Field<"false", REF<TS<Int>>>>>(
                            w, invalidate, wire<const_, TS<Int>>(w, Int{11}));
            auto v21 = wire<const_, TS<Int>>(w, Int{21});
            auto item = wire<getitem_>(w, v11, Str{"false"});
            std::array<WiringArg, 2> args{};
            args[0].kind = WiringArg::Kind::TimeSeries;
            args[0].port = item.erased();
            args[1].kind = WiringArg::Kind::TimeSeries;
            args[1].port = v21.erased();
            auto raced = wire_operator(w, "race", args);
            return Port<void>{w, raced.output.erased()}.as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("race re-races when the winner goes invalid")
{
    stdlib::register_standard_operators();
    using namespace hgraph::testing;
    CHECK_OUTPUT(eval_node<RaceRevokeGraph>(values<Bool>(false, true)),
                 values<Int>(11, 21));
}

namespace
{
    /** The EXACT python test_race_tsls shape: race over two structural TSLs
        (candidate 1's elements are if_ false-branch refs), then [0]. */
    struct RaceTslRevokeGraph
    {
        static constexpr auto name = "race_tsl_revoke_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> invalidate)
        {
            using IfBundle = UnNamedTSB<Field<"true", REF<TS<Int>>>, Field<"false", REF<TS<Int>>>>;
            auto &registry = TypeRegistry::instance();
            const auto *tsl_schema = registry.tsl(registry.ts(scalar_descriptor<Int>::value_meta()), 2);

            auto v11 = wire<getitem_>(w, wire<if_, IfBundle>(w, invalidate, wire<const_, TS<Int>>(w, Int{11})),
                                      Str{"false"});
            auto v12 = wire<getitem_>(w, wire<if_, IfBundle>(w, invalidate, wire<const_, TS<Int>>(w, Int{12})),
                                      Str{"false"});
            WiringPortRef tsl1 = WiringPortRef::structural_source(tsl_schema, {v11.erased(), v12.erased()});

            auto v21 = wire<const_, TS<Int>>(w, Int{21});
            auto v22 = wire<const_, TS<Int>>(w, Int{22});
            WiringPortRef tsl2 = WiringPortRef::structural_source(tsl_schema, {v21.erased(), v22.erased()});

            std::array<WiringArg, 2> args{};
            args[0].kind = WiringArg::Kind::TimeSeries;
            args[0].port = tsl1;
            args[1].kind = WiringArg::Kind::TimeSeries;
            args[1].port = tsl2;
            auto raced = wire_operator(w, "race", args);

            std::array<WiringArg, 2> item_args{};
            item_args[0].kind = WiringArg::Kind::TimeSeries;
            item_args[0].port = raced.output.erased();
            item_args[1].kind = WiringArg::Kind::TimeSeries;
            item_args[1].port = wire<const_, TS<Int>>(w, Int{0}).erased();
            auto item = wire_operator(w, "getitem_", item_args);
            return Port<void>{w, item.output.erased()}.as<TS<Int>>();
        }
    };
}  // namespace

TEST_CASE("race over TSLs re-races when the winner goes invalid")
{
    stdlib::register_standard_operators();
    using namespace hgraph::testing;
    CHECK_OUTPUT(eval_node<RaceTslRevokeGraph>(values<Bool>(false, true)),
                 values<Int>(11, 21));
}

namespace
{
    /** The python test_race_tsbs shape: race over two structural TSBs. */
    struct RaceTsbRevokeGraph
    {
        static constexpr auto name = "race_tsb_revoke_graph";
        using PairBundle = UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Int>>>;

        static Port<PairBundle> compose(Wiring &w, Port<TS<Bool>> invalidate)
        {
            using IfBundle = UnNamedTSB<Field<"true", REF<TS<Int>>>, Field<"false", REF<TS<Int>>>>;
            const auto *tsb_schema = schema_descriptor<PairBundle>::ts_meta();

            auto v11 = wire<getitem_>(w, wire<if_, IfBundle>(w, invalidate, wire<const_, TS<Int>>(w, Int{11})),
                                      Str{"false"});
            auto v12 = wire<getitem_>(w, wire<if_, IfBundle>(w, invalidate, wire<const_, TS<Int>>(w, Int{12})),
                                      Str{"false"});
            WiringPortRef tsb1 = WiringPortRef::structural_source(tsb_schema, {v11.erased(), v12.erased()});

            auto v21 = wire<const_, TS<Int>>(w, Int{21});
            auto v22 = wire<const_, TS<Int>>(w, Int{22});
            WiringPortRef tsb2 = WiringPortRef::structural_source(tsb_schema, {v21.erased(), v22.erased()});

            std::array<WiringArg, 2> args{};
            args[0].kind = WiringArg::Kind::TimeSeries;
            args[0].port = tsb1;
            args[1].kind = WiringArg::Kind::TimeSeries;
            args[1].port = tsb2;
            auto raced = wire_operator(w, "race", args);
            return Port<void>{w, raced.output.erased()}.as<PairBundle>();
        }
    };

    struct RaceDirectBundleTsdGraph
    {
        static constexpr auto name = "race_direct_bundle_tsd_graph";
        using PairBundle = RaceTsbRevokeGraph::PairBundle;

        static Port<PairBundle> compose(Wiring &w, Port<TSD<Int, PairBundle>> tsd)
        {
            return wire<stdlib::reduce_tsd_of_bundles_with_race>(w, tsd).template as<PairBundle>();
        }
    };

    using RaceSourceBundle =
        UnNamedTSB<Field<"a", TS<Int>>, Field<"b", TS<Int>>, Field<"active", TS<Bool>>>;

    struct RaceStructuralBundleTerminal
    {
        static Port<RaceTsbRevokeGraph::PairBundle> compose(
            Wiring &, Port<REF<RaceTsbRevokeGraph::PairBundle>> bundle)
        {
            return bundle.template as<RaceTsbRevokeGraph::PairBundle>();
        }
    };

    struct RaceLiveBundleBranch
    {
        static Port<RaceTsbRevokeGraph::PairBundle> compose(
            Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b, Port<TS<Bool>>)
        {
            auto bundle = stdlib::to_tsb<RaceTsbRevokeGraph::PairBundle>(w, a, b);
            return wire<RaceStructuralBundleTerminal>(w, bundle);
        }
    };

    struct RaceInvalidBundleBranch
    {
        static Port<RaceTsbRevokeGraph::PairBundle> compose(
            Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b, Port<TS<Bool>> active)
        {
            auto bundle = stdlib::to_tsb<RaceTsbRevokeGraph::PairBundle>(w, a, b);
            return wire<stdlib::filter_>(w, active, bundle)
                .template as<RaceTsbRevokeGraph::PairBundle>();
        }
    };

    struct RaceMappedSwitchBody
    {
        static Port<RaceTsbRevokeGraph::PairBundle> compose(
            Wiring &w, Port<RaceSourceBundle> source)
        {
            auto a = wire<stdlib::getitem_>(w, source, Str{"a"}).template as<TS<Int>>();
            auto b = wire<stdlib::getitem_>(w, source, Str{"b"}).template as<TS<Int>>();
            auto active = wire<stdlib::getitem_>(w, source, Str{"active"}).template as<TS<Bool>>();
            return wire<stdlib::switch_>(
                       w, active,
                       stdlib::switch_cases(
                           {{Value{true}, fn<RaceLiveBundleBranch>()},
                            {Value{false}, fn<RaceInvalidBundleBranch>()}})
                           .reload(),
                       a, b, active)
                .template as<RaceTsbRevokeGraph::PairBundle>();
        }
    };

    struct RaceMappedSwitchGraph
    {
        static Port<RaceTsbRevokeGraph::PairBundle> compose(
            Wiring &w, Port<TSD<Int, RaceSourceBundle>> source)
        {
            auto mapped = wire<stdlib::map_>(w, fn<RaceMappedSwitchBody>(), source)
                              .template as<TSD<Int, RaceTsbRevokeGraph::PairBundle>>();
            return wire<stdlib::reduce_tsd_of_bundles_with_race>(w, mapped)
                .template as<RaceTsbRevokeGraph::PairBundle>();
        }
    };
}  // namespace

TEST_CASE("race over TSBs delivers the re-raced winner's values")
{
    stdlib::register_standard_operators();
    using namespace hgraph::testing;
    auto pair_delta = [](Int a, Int b) {
        const auto *schema = schema_descriptor<RaceTsbRevokeGraph::PairBundle>::ts_meta();
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema->delta_value_schema)};
        builder.set("a", Value{a});
        builder.set("b", Value{b});
        return builder.build();
    };
    CHECK_OUTPUT(eval_node<RaceTsbRevokeGraph>(values<Bool>(false, true)),
                 values<Value>(pair_delta(11, 12), pair_delta(21, 22)));
}

TEST_CASE("race over a TSD of direct bundles exposes bundle references")
{
    stdlib::register_standard_operators();
    using namespace hgraph::testing;
    using PairBundle = RaceDirectBundleTsdGraph::PairBundle;

    CHECK_OUTPUT(
        eval_node<RaceDirectBundleTsdGraph>(
            values<Value>(dict_delta<Int, PairBundle>(
                              {{1, tsb_delta<PairBundle>(Int{11}, Int{12})},
                               {2, tsb_delta<PairBundle>(Int{21}, Int{22})}}),
                          dict_delta<Int, PairBundle>({}, {1}))),
        values<Value>(tsb_delta<PairBundle>(Int{11}, Int{12}),
                      tsb_delta<PairBundle>(Int{21}, Int{22})));
}

TEST_CASE("race over mapped switches falls back when the winning bundle becomes invalid")
{
    stdlib::register_standard_operators();
    using namespace hgraph::testing;
    using PairBundle = RaceTsbRevokeGraph::PairBundle;

    CHECK_OUTPUT(
        eval_node<RaceMappedSwitchGraph>(values<Value>(
            dict_delta<Int, RaceSourceBundle>(
                {{1, tsb_delta<RaceSourceBundle>(Int{11}, Int{12}, true)},
                 {2, tsb_delta<RaceSourceBundle>(Int{21}, Int{22}, true)}}),
            dict_delta<Int, RaceSourceBundle>(
                {{1, tsb_delta<RaceSourceBundle>(none, none, false)}}))),
        values<Value>(tsb_delta<PairBundle>(Int{11}, Int{12}),
                      tsb_delta<PairBundle>(Int{21}, Int{22})));
}
