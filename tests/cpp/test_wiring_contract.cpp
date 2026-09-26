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

    void register_case_operators()
    {
        register_overload<pick_, PickInt>();
        register_overload<pick_, PickGeneric>();
        register_overload<pick_, PickList>();
        register_overload<tied_, TiedA>();
        register_overload<tied_, TiedB>();
        register_overload<only_int_, OnlyInt>();
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

TEST_CASE("wiring contract: WIRE-REPEATED binds a repeated variable once, dereferenced (WIR-7, WIR-16)")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RepeatedGraph>(values<Int>(1)), values<Bool>(true));
    CHECK_THROWS(eval_node<RepeatedMismatchGraph>(values<Int>(1), values<Float>(1.0)));
}

TEST_CASE("wiring contract: WIRE-SPECIFICITY selects the most specific candidate (WIR-15, WIR-17)")
{
    stdlib::register_standard_operators();
    register_case_operators();
    CHECK_OUTPUT(eval_node<PickIntGraph>(values<Int>(1)), values<Str>("int"s));
    CHECK_OUTPUT(eval_node<PickFloatGraph>(values<Float>(1.0)), values<Str>("generic"s));
    CHECK_OUTPUT(eval_node<PickListGraph>(values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}))), values<Str>("tsl-generic"s));
    CHECK_OUTPUT(eval_node<PickRefGraph>(values<Int>(1)), values<Str>("int"s));
}

TEST_CASE("wiring contract: WIRE-FAILURES fails a tie and a call with no candidate (WIR-4, WIR-15)")
{
    stdlib::register_standard_operators();
    register_case_operators();
    CHECK_THROWS(eval_node<TiedGraph>(values<Int>(1)));
    CHECK_THROWS(eval_node<NoCandidateGraph>(values<Str>("x"s)));
}
