// The runtime specification's wiring cases through native C++ wiring
// (docs/source/runtime_spec/cases_wiring.md; rules in wiring.md). Each case
// mirrors one in validation/wiring/cases.py, which checks the same
// expectations through the Python surface: a graph records the types wiring
// decided, and eval_node observes what its nodes see.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    // What a graph's wiring decided, read after eval_node wires it.
    struct Seen
    {
        const TSValueTypeMetaData *source{nullptr};
        const TSValueTypeMetaData *bound{nullptr};
        const TSValueTypeMetaData *other{nullptr};
    };
    Seen seen;

    /** A generic node that publishes its input: its output type is what its
        variable bound (WIR-7). */
    struct Identity
    {
        static constexpr auto name = "wiring_contract_identity";

        static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"S">> out)
        {
            const Value delta = capture_delta(ts.base());
            apply_delta(out, delta.view());
        }
    };

    struct ToRef
    {
        static constexpr auto name = "wiring_contract_to_ref";

        static void eval(In<"ts", REF<TS<Int>>> ts, Out<REF<TS<Int>>> out) { out.set(ts.value()); }
    };

    /** A variable beneath a declared reference (WIR-10). */
    struct RefIdentity
    {
        static constexpr auto name = "wiring_contract_ref_identity";

        static void eval(In<"ts", REF<TsVar<"S">>> ts, Out<REF<TsVar<"S">>> out) { out.set(ts.value()); }
    };

    struct HoldsReference
    {
        static constexpr auto name = "wiring_contract_holds_reference";

        static void eval(In<"ts", REF<TsVar<"S">>> ts, Out<TS<Bool>> out) { out.set(!ts.value().is_empty()); }
    };

    struct Same
    {
        static constexpr auto name = "wiring_contract_same";

        static void eval(In<"a", TsVar<"S">>, In<"b", TsVar<"S">>, Out<TS<Bool>> out) { out.set(true); }
    };

    using RefFields = TSB<"WiringContractRefFields", Field<"routed", REF<TS<Int>>>, Field<"plain", TS<Int>>>;

    struct MakeRefFields
    {
        static constexpr auto name = "wiring_contract_ref_fields";

        static void eval(In<"routed", REF<TS<Int>>> routed, In<"plain", TS<Int>> plain, Out<RefFields> out)
        {
            out.field<"routed">().set(routed.value());
            out.field<"plain">().set(plain.value());
        }
    };

    /** WIRE-GENERIC-DEPTH: a map of references, as combine_tsd publishes it. */
    struct GenericDepthGraph
    {
        static constexpr auto name = "wiring_contract_generic_depth";

        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b, Port<TS<Int>> c)
        {
            auto routed  = wire<stdlib::combine_tsd>(w, stdlib::make_list<Str>({Str{"a"}, Str{"b"}, Str{"c"}}), a, b, c);
            seen.source  = routed.erased().schema;
            auto through = wire<Identity>(w, routed);
            seen.bound   = through.erased().schema;
            return through.as<TSD<Str, TS<Int>>>();
        }
    };

    /** WIRE-GENERIC-TOP. */
    struct GenericTopGraph
    {
        static constexpr auto name = "wiring_contract_generic_top";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto routed  = wire<ToRef>(w, value);
            seen.source  = routed.erased().schema;
            auto through = wire<Identity>(w, routed);
            seen.bound   = through.erased().schema;
            return through.as<TS<Int>>();
        }
    };

    /** WIRE-REF-PATTERN. */
    struct RefPatternGraph
    {
        static constexpr auto name = "wiring_contract_ref_pattern";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b)
        {
            auto routed = wire<stdlib::combine_tsd>(w, stdlib::make_list<Str>({Str{"a"}, Str{"b"}}), a, b);
            seen.bound  = wire<RefIdentity>(w, routed).erased().schema;
            return wire<HoldsReference>(w, a).as<TS<Bool>>();
        }
    };

    /** WIRE-REQUESTED: nothing's output is a bare variable. */
    struct RequestedGraph
    {
        static constexpr auto name = "wiring_contract_requested";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            seen.source = wire<stdlib::nothing>(w, ts_type<TSD<Str, REF<TS<Int>>>>()).erased().schema;
            seen.bound  = wire<stdlib::nothing>(w, ts_type<REF<TS<Int>>>()).erased().schema;
            seen.other  = wire<stdlib::nothing>(w, ts_type<TSD<Str, TS<Int>>>()).erased().schema;
            return value;
        }
    };

    /** WIRE-PROJECTION: getitem_ and getattr_ on a bundle with a REF field. */
    struct ProjectionGraph
    {
        static constexpr auto name = "wiring_contract_projection";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto fields = wire<MakeRefFields>(w, wire<ToRef>(w, value), value);
            auto by_key = wire<stdlib::getitem_>(w, fields, Str{"routed"});
            seen.source = by_key.erased().schema;
            seen.bound  = wire<stdlib::getattr_>(w, fields, Str{"routed"}).erased().schema;
            seen.other  = wire<stdlib::getitem_>(w, fields, Str{"plain"}).erased().schema;
            return wire<Identity>(w, by_key).as<TS<Int>>();
        }
    };

    /** WIRE-PROJECTION-THROUGH-REF: if_ publishes a bundle of references. */
    struct ProjectionThroughRefGraph
    {
        static constexpr auto name = "wiring_contract_projection_through_ref";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> condition, Port<TS<Int>> value)
        {
            auto field  = wire<stdlib::getitem_>(w, wire<stdlib::if_>(w, condition, value), Str{"true"});
            seen.source = field.erased().schema;
            return wire<Identity>(w, field).as<TS<Int>>();
        }
    };

    /** WIRE-REPEATED. */
    struct RepeatedGraph
    {
        static constexpr auto name = "wiring_contract_repeated";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> value)
        {
            return wire<Same>(w, wire<ToRef>(w, value), value).as<TS<Bool>>();
        }
    };

    struct RepeatedMismatchGraph
    {
        static constexpr auto name = "wiring_contract_repeated_mismatch";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> i, Port<TS<Float>> f)
        {
            return wire<Same>(w, i, f).as<TS<Bool>>();
        }
    };

    // WIRE-BUNDLE-IDENTITY: two named bundles and an unnamed one, all with
    // the single field a: TS[int].
    using Foo     = TSB<"WiringContractFoo", Field<"a", TS<Int>>>;
    using Bar     = TSB<"WiringContractBar", Field<"a", TS<Int>>>;
    using Unnamed = UnNamedTSB<Field<"a", TS<Int>>>;

    template <typename Bundle>
    struct MakeBundle
    {
        static constexpr auto name = "wiring_contract_make_bundle";

        static void eval(In<"v", TS<Int>> v, Out<Bundle> out) { out.template field<"a">().set(v.value()); }
    };

    template <typename Bundle>
    struct TakesBundle
    {
        static constexpr auto name = "wiring_contract_takes_bundle";

        static void eval(In<"b", Bundle> b, Out<TS<Int>> out) { out.set(b.template field<"a">().value()); }
    };

    // A generic named bundle pattern (dispatched through the runtime matcher)
    // and a constrained variable (static unifier) apply WIR-15 too.
    using FooOf = TSB<"WiringContractFoo", Field<"a", TS<ScalarVar<"T">>>>;
    struct takes_foo_ : Operator<"wiring_contract_takes_foo", In<"b", TsVar<"S">>, Out<TS<Bool>>>
    {
    };
    struct TakesFooOf
    {
        static void eval(In<"b", FooOf>, Out<TS<Bool>> out) { out.set(true); }
    };
    struct TakesUnnamedConstrained
    {
        static constexpr auto name = "wiring_contract_takes_unnamed_constrained";
        static void eval(In<"b", TsVar<"S", Unnamed>>, Out<TS<Bool>> out) { out.set(true); }
    };

    template <typename Supplied>
    struct TakesFooOfGraph
    {
        static constexpr auto name = "wiring_contract_takes_foo_of";
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> v)
        {
            return wire<takes_foo_>(w, wire<MakeBundle<Supplied>>(w, v)).template as<TS<Bool>>();
        }
    };

    struct ConstrainedGraph
    {
        static constexpr auto name = "wiring_contract_constrained";
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> v)
        {
            return wire<TakesUnnamedConstrained>(w, wire<MakeBundle<Foo>>(w, v)).as<TS<Bool>>();
        }
    };

    template <typename Left, typename Right>
    struct SameBundlesGraph
    {
        static constexpr auto name = "wiring_contract_same_bundles";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> v)
        {
            return wire<Same>(w, wire<MakeBundle<Left>>(w, v), wire<MakeBundle<Right>>(w, v)).template as<TS<Bool>>();
        }
    };

    template <typename Declared, typename Supplied>
    struct TakesBundleGraph
    {
        static constexpr auto name = "wiring_contract_takes_bundle_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> v)
        {
            // An erased port: the supplied bundle is matched against the
            // declared input when the call is wired, not at compile time.
            const Port<void> supplied{w, wire<MakeBundle<Supplied>>(w, v).erased()};
            return wire<TakesBundle<Declared>>(w, supplied);
        }
    };

    // WIRE-SPECIFICITY and WIRE-FAILURES: operators registered for these cases.
    struct pick_ : Operator<"wiring_contract_pick", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };
    struct PickInt
    {
        static void eval(In<"ts", TS<Int>>, Out<TS<Str>> out) { out.set(Str{"int"}); }
    };
    struct PickGeneric
    {
        static void eval(In<"ts", TsVar<"S">>, Out<TS<Str>> out) { out.set(Str{"generic"}); }
    };
    struct PickList
    {
        static void eval(In<"ts", TSL<TsVar<"E">, SIZE<"N">>>, Out<TS<Str>> out) { out.set(Str{"tsl-generic"}); }
    };

    struct tied_ : Operator<"wiring_contract_tied", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };
    struct TiedA
    {
        static void eval(In<"ts", TS<Int>>, Out<TS<Str>> out) { out.set(Str{"a"}); }
    };
    struct TiedB
    {
        static void eval(In<"ts", TS<Int>>, Out<TS<Str>> out) { out.set(Str{"b"}); }
    };

    struct only_int_ : Operator<"wiring_contract_only_int", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };
    struct OnlyInt
    {
        static void eval(In<"ts", TS<Int>>, Out<TS<Str>> out) { out.set(Str{"int"}); }
    };

    // WIRE-OPERATOR-CONTRACT: a candidate may add a parameter (WIR-22) and
    // refine a declared type (WIR-23).
    struct declares_generic_ : Operator<"wiring_contract_declares_generic", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };
    struct WithExtra
    {
        static auto defaults() { return std::tuple{arg<"scale">(Int{2})}; }
        static void eval(In<"ts", TS<Int>>, Scalar<"scale", Int> scale, Out<TS<Str>> out)
        {
            out.set(Str{"extra " + std::to_string(scale.value())});
        }
    };
    struct refinable_ : Operator<"wiring_contract_refinable", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };
    struct Refined
    {
        static void eval(In<"ts", TS<Int>>, Out<TS<Str>> out) { out.set(Str{"refined"}); }
    };

    // A candidate whose extra parameter has no default matches only a call
    // that supplies it (WIR-22).
    struct needs_extra_ : Operator<"wiring_contract_needs_extra", In<"ts", TsVar<"S">>, Out<TS<Str>>>
    {
    };
    struct NeedsExtraFallback
    {
        static void eval(In<"ts", TsVar<"S">>, Out<TS<Str>> out) { out.set(Str{"fallback"}); }
    };
    struct NeedsExtraScaled
    {
        static void eval(In<"ts", TS<Int>>, Scalar<"scale", Int> scale, Out<TS<Str>> out)
        {
            out.set(Str{"scaled " + std::to_string(scale.value())});
        }
    };
    struct ExtraMissingGraph
    {
        static constexpr auto name = "wiring_contract_extra_missing";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v) { return wire<needs_extra_>(w, v).as<TS<Str>>(); }
    };
    struct ExtraSuppliedGraph
    {
        static constexpr auto name = "wiring_contract_extra_supplied";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v)
        {
            return wire<needs_extra_>(w, v, arg<"scale">(Int{3})).as<TS<Str>>();
        }
    };

    struct DefaultUsedGraph
    {
        static constexpr auto name = "wiring_contract_default_used";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v) { return wire<declares_generic_>(w, v).as<TS<Str>>(); }
    };
    struct ExtraPassedGraph
    {
        static constexpr auto name = "wiring_contract_extra_passed";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v)
        {
            return wire<declares_generic_>(w, v, arg<"scale">(Int{5})).as<TS<Str>>();
        }
    };
    struct RefinedGraph
    {
        static constexpr auto name = "wiring_contract_refined";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v) { return wire<refinable_>(w, v).as<TS<Str>>(); }
    };

    void register_case_operators()
    {
        register_overload<pick_, PickInt>();
        register_overload<pick_, PickGeneric>();
        register_overload<pick_, PickList>();
        register_overload<tied_, TiedA>();
        register_overload<tied_, TiedB>();
        register_overload<only_int_, OnlyInt>();
        register_overload<declares_generic_, WithExtra>();
        register_overload<refinable_, Refined>();
        register_overload<needs_extra_, NeedsExtraFallback>();
        register_overload<needs_extra_, NeedsExtraScaled>();
        register_overload<takes_foo_, TakesFooOf>();
    }

    struct PickIntGraph
    {
        static constexpr auto name = "wiring_contract_pick_int";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v) { return wire<pick_>(w, v).as<TS<Str>>(); }
    };
    struct PickFloatGraph
    {
        static constexpr auto name = "wiring_contract_pick_float";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Float>> v) { return wire<pick_>(w, v).as<TS<Str>>(); }
    };
    struct PickListGraph
    {
        static constexpr auto name = "wiring_contract_pick_list";
        static Port<TS<Str>> compose(Wiring &w, Port<TSL<TS<Int>, 2>> v) { return wire<pick_>(w, v).as<TS<Str>>(); }
    };
    struct PickRefGraph
    {
        static constexpr auto name = "wiring_contract_pick_ref";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v)
        {
            return wire<pick_>(w, wire<ToRef>(w, v)).as<TS<Str>>();
        }
    };
    struct TiedGraph
    {
        static constexpr auto name = "wiring_contract_tied_graph";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> v) { return wire<tied_>(w, v).as<TS<Str>>(); }
    };
    struct NoCandidateGraph
    {
        static constexpr auto name = "wiring_contract_no_candidate";
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> v) { return wire<only_int_>(w, v).as<TS<Str>>(); }
    };
}  // namespace

TEST_CASE("wiring contract: WIRE-GENERIC-DEPTH binds with every reference removed (WIR-7, WIR-8)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<GenericDepthGraph>(values<Int>(1, none), values<Int>(2, 30), values<Int>(3, none))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}, {"c"s, 3}}),
                               dict_delta<Str, TS<Int>>({{"b"s, 30}})));
    CHECK(seen.source == ts_type<TSD<Str, REF<TS<Int>>>>());
    CHECK(seen.bound == ts_type<TSD<Str, TS<Int>>>());
}

TEST_CASE("wiring contract: WIRE-GENERIC-TOP follows a top-level reference (WIR-7, WIR-8)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<GenericTopGraph>(values<Int>(1, 2)), values<Int>(1, 2));
    CHECK(seen.source == ts_type<REF<TS<Int>>>());
    CHECK(seen.bound == ts_type<TS<Int>>());
}

TEST_CASE("wiring contract: WIRE-REF-PATTERN binds beneath a declared reference (WIR-10)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RefPatternGraph>(values<Int>(1), values<Int>(2)), values<Bool>(true));
    CHECK(seen.bound == ts_type<REF<TSD<Str, TS<Int>>>>());
}

TEST_CASE("wiring contract: WIRE-REQUESTED keeps a requested reference (WIR-12)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RequestedGraph>(values<Int>(1)), values<Int>(1));
    CHECK(seen.source == ts_type<TSD<Str, REF<TS<Int>>>>());
    CHECK(seen.bound == ts_type<REF<TS<Int>>>());
    CHECK(seen.other == ts_type<TSD<Str, TS<Int>>>());
}

TEST_CASE("wiring contract: WIRE-PROJECTION keeps a reference field (WIR-5, WIR-13)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ProjectionGraph>(values<Int>(1, 2)), values<Int>(1, 2));
    CHECK(seen.source == ts_type<REF<TS<Int>>>());
    CHECK(seen.bound == ts_type<REF<TS<Int>>>());
    CHECK(seen.other == ts_type<TS<Int>>());
}

TEST_CASE("wiring contract: WIRE-PROJECTION-THROUGH-REF publishes a reference to the field (WIR-5)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ProjectionThroughRefGraph>(values<Bool>(true, false), values<Int>(1, 2)),
                 values<Int>(1, none));
    CHECK(seen.source == ts_type<REF<TS<Int>>>());
}

TEST_CASE("wiring contract: WIRE-REPEATED binds a repeated variable once, dereferenced (WIR-7, WIR-17)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RepeatedGraph>(values<Int>(1)), values<Bool>(true));
    CHECK_THROWS(eval_node<RepeatedMismatchGraph>(values<Int>(1), values<Float>(1.0)));
}

TEST_CASE("wiring contract: WIRE-BUNDLE-IDENTITY counts names only when both bundles are named (WIR-15, WIR-17)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT((eval_node<SameBundlesGraph<Foo, Unnamed>>(values<Int>(1))), values<Bool>(true));
    CHECK_OUTPUT((eval_node<SameBundlesGraph<Foo, Foo>>(values<Int>(1))), values<Bool>(true));
    CHECK_THROWS((eval_node<SameBundlesGraph<Foo, Bar>>(values<Int>(1))));
    CHECK_OUTPUT((eval_node<TakesBundleGraph<Foo, Unnamed>>(values<Int>(1))), values<Int>(1));
    CHECK_OUTPUT((eval_node<TakesBundleGraph<Unnamed, Foo>>(values<Int>(1))), values<Int>(1));
    CHECK_THROWS((eval_node<TakesBundleGraph<Foo, Bar>>(values<Int>(1))));
    // Through a generic named pattern and a constrained variable too.
    register_case_operators();
    CHECK_OUTPUT((eval_node<TakesFooOfGraph<Unnamed>>(values<Int>(1))), values<Bool>(true));
    CHECK_OUTPUT((eval_node<TakesFooOfGraph<Foo>>(values<Int>(1))), values<Bool>(true));
    CHECK_THROWS((eval_node<TakesFooOfGraph<Bar>>(values<Int>(1))));
    CHECK_OUTPUT(eval_node<ConstrainedGraph>(values<Int>(1)), values<Bool>(true));
}

namespace
{
    // WIRE-BUNDLE-IDENTITY, field order: fields pair by name (WIR-15).
    using Pair      = TSB<"WiringContractPair", Field<"a", TS<Int>>, Field<"b", TS<Int>>>;
    using Riap      = TSB<"WiringContractRiap", Field<"b", TS<Int>>, Field<"a", TS<Int>>>;
    using UnnamedBA = UnNamedTSB<Field<"b", TS<Int>>, Field<"a", TS<Int>>>;

    template <typename Bundle>
    struct MakeAB
    {
        static constexpr auto name = "wiring_contract_make_ab";

        static void eval(In<"v", TS<Int>> v, Out<Bundle> out)
        {
            out.template field<"a">().set(v.value());
            out.template field<"b">().set(v.value() + 1);
        }
    };

    template <typename Bundle>
    struct TakesAB
    {
        static constexpr auto name = "wiring_contract_takes_ab";

        static void eval(In<"p", Bundle> p, Out<TS<Int>> out)
        {
            out.set(p.template field<"a">().value() * 10 + p.template field<"b">().value());
        }
    };

    template <typename Declared, typename Supplied>
    struct TakesABGraph
    {
        static constexpr auto name = "wiring_contract_takes_ab_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> v)
        {
            const Port<void> supplied{w, wire<MakeAB<Supplied>>(w, v).erased()};
            return wire<TakesAB<Declared>>(w, supplied);
        }
    };
}  // namespace

TEST_CASE("wiring contract: bundles pair fields by name; different names never bind (WIR-15)")
{
    stdlib::register_standard_operators();
    const auto *pair       = schema_descriptor<Pair>::ts_meta();
    const auto *riap       = schema_descriptor<Riap>::ts_meta();
    const auto *unnamed_ba = schema_descriptor<UnnamedBA>::ts_meta();
    // Names conflict only when both bundles are named, at any depth.
    CHECK(time_series_bundle_names_conflict(pair, riap));
    CHECK_FALSE(time_series_bundle_names_conflict(pair, unnamed_ba));
    CHECK_FALSE(time_series_bundle_names_conflict(pair, pair));
    CHECK(time_series_bundle_names_conflict(schema_descriptor<TSL<Pair, 2>>::ts_meta(),
                                            schema_descriptor<TSL<Riap, 2>>::ts_meta()));
    CHECK(time_series_bundle_names_conflict(schema_descriptor<REF<Pair>>::ts_meta(), riap));
    // Wiring refuses them whatever alternative could carry the value.
    CHECK_FALSE(graph_wiring_detail::input_accepts_output_schema(pair, riap));
    CHECK_THROWS((eval_node<TakesABGraph<Pair, Riap>>(values<Int>(1))));
    // A reordered unnamed bundle pairs by name: a * 10 + b is 12.
    CHECK_OUTPUT((eval_node<TakesABGraph<Pair, UnnamedBA>>(values<Int>(1))), values<Int>(12));
    CHECK_OUTPUT((eval_node<TakesABGraph<UnnamedBA, Pair>>(values<Int>(1))), values<Int>(12));
}

TEST_CASE("wiring contract: WIRE-SPECIFICITY selects the most specific candidate (WIR-16, WIR-18)")
{
    stdlib::register_standard_operators();
    register_case_operators();
    CHECK_OUTPUT(eval_node<PickIntGraph>(values<Int>(1)), values<Str>("int"s));
    CHECK_OUTPUT(eval_node<PickFloatGraph>(values<Float>(1.0)), values<Str>("generic"s));
    CHECK_OUTPUT(eval_node<PickListGraph>(values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}))), values<Str>("tsl-generic"s));
    CHECK_OUTPUT(eval_node<PickRefGraph>(values<Int>(1)), values<Str>("int"s));
}

TEST_CASE("wiring contract: WIRE-OPERATOR-CONTRACT allows a superset and a refinement (WIR-21 to WIR-23)")
{
    stdlib::register_standard_operators();
    register_case_operators();
    CHECK_OUTPUT(eval_node<DefaultUsedGraph>(values<Int>(1)), values<Str>("extra 2"s));
    CHECK_OUTPUT(eval_node<ExtraPassedGraph>(values<Int>(1)), values<Str>("extra 5"s));
    CHECK_OUTPUT(eval_node<RefinedGraph>(values<Int>(1)), values<Str>("refined"s));
    // An extra parameter without a default: no match without it, a match with it.
    CHECK_OUTPUT(eval_node<ExtraMissingGraph>(values<Int>(1)), values<Str>("fallback"s));
    CHECK_OUTPUT(eval_node<ExtraSuppliedGraph>(values<Int>(1)), values<Str>("scaled 3"s));
}

TEST_CASE("wiring contract: WIRE-FAILURES fails a tie and a call with no candidate (WIR-4, WIR-16)")
{
    stdlib::register_standard_operators();
    register_case_operators();
    CHECK_THROWS(eval_node<TiedGraph>(values<Int>(1)));
    CHECK_THROWS(eval_node<NoCandidateGraph>(values<Str>("x"s)));
}
