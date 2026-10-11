#include <reference-routing.h>
#include <nested-reference-fields.h>
#include <reference-identity-equality.h>
#include <reference-wiring-projections.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace reference_routing = examples::reference_routing;
namespace reference_identity = examples::reference_identity_equality;

namespace
{
    struct RouteFirst
    {
        static Port<TS<Float>>
        compose(Wiring &w, Port<TS<Float>> first, Port<TS<Float>> second, Port<TS<Float>> third) {
            auto index = wire<stdlib::const_, TS<Int>>(w, Int{0});
            auto values = stdlib::to_tsl<TSL<TS<Float>, 3>>(w, first, second, third);
            return wire<reference_routing::operators::route3>(w, index, values).as<TS<Float>>();
        }
    };

    struct CompareChildTrees
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> first, Port<TS<Int>> second,
                                     Port<TS<Int>> equal_contents, Port<TS<Int>> step) {
            auto left = stdlib::to_tsl(w, first, second);
            auto right = stdlib::to_tsl(w, first, equal_contents);
            return wire<reference_identity::compare_lists>(w, left, right, step);
        }
    };

    struct CompareAliasedChildTrees
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> first, Port<TS<Int>> second, Port<TS<Int>> step) {
            auto left = stdlib::to_tsl(w, first, second);
            auto right = stdlib::to_tsl(w, first, second);
            return wire<reference_identity::compare_lists>(w, left, right, step);
        }
    };

    using ReferenceChildBundle = TSB<"ProjectedReferenceChild", Field<"child", REF<TS<Int>>>>;

    struct SelectReferenceChild
    {
        static void eval(In<"index", TS<Int>> index, In<"first", REF<TS<Int>>> first,
                         In<"second", REF<TS<Int>>> second, Out<ReferenceChildBundle> out) {
            out.field<"child">().set(index.value() == 0 ? first.value() : second.value());
        }
    };

    struct ForwardReferenceParent
    {
        static void eval(In<"value", REF<ReferenceChildBundle>> value, Out<REF<ReferenceChildBundle>> out) {
            out.set(value.value());
        }
    };

    struct ReadProjectedInt
    {
        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value()); }
    };

    struct ProjectReferenceChild
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> index, Port<TS<Int>> first, Port<TS<Int>> second) {
            auto source = wire<SelectReferenceChild>(w, index, first, second);
            auto parent = wire<ForwardReferenceParent>(w, source);
            auto child = wire<stdlib::getattr_>(w, parent, Str{"child"});
            return wire<ReadProjectedInt>(w, child);
        }
    };

    struct IncompatibleProjectedOutput
    {
        static Port<REF<TS<Str>>> compose(Wiring &w, Port<ReferenceChildBundle> source) {
            auto parent = wire<ForwardReferenceParent>(w, source);
            return wire<stdlib::getattr_, REF<TS<Str>>>(w, parent, Str{"child"});
        }
    };
}  // namespace

TEST_CASE("generated reference operators retain their public schema", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_routing::register_operators();

    CHECK(hgl::wiring::has_operator("examples.reference_routing.forward"));
}

TEST_CASE("generated reference routing follows the selected source", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_routing::register_operators();

    CHECK(eval_node<RouteFirst>(values<Float>(1.0, 2.0), values<Float>(10.0, 20.0), values<Float>(100.0, 200.0)) ==
          values<Float>(1.0, 2.0));
}

TEST_CASE("generated reference identity compares endpoints through public composition", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_identity::register_operators();
    const auto source = values<Int>(7, 8, std::nullopt, 7);
    const auto step = values<Int>(1, 1, 1, 1);
    CHECK(eval_node<reference_identity::public_forwarded>(source) == source);
    CHECK(eval_node<reference_identity::public_previous_reference>(source, step) == source);
    CHECK(eval_node<reference_identity::public_self_assigned_reference>(source, step) == source);
    CHECK(eval_node<reference_identity::public_aliased>(source, step) == values<Bool>(true, true, true, true));
    CHECK(eval_node<reference_identity::public_aliased_unequal>(source, step) == values<Bool>(false, false, false, false));
    CHECK(eval_node<reference_identity::public_distinct>(source, source, step) == values<Bool>(false, false, false, false));
    CHECK(eval_node<reference_identity::public_distinct_unequal>(source, source, step) == values<Bool>(true, true, true, true));
    CHECK(eval_node<reference_identity::generic_aliased>(source, step) == values<Bool>(true, true, true, true));
    const auto floating = values<Float>(7.0, 8.0, std::nullopt, 7.0);
    CHECK(eval_node<reference_identity::generic_distinct>(floating, floating, step) == values<Bool>(false, false, false, false));
}

TEST_CASE("generated composite reference identity preserves distinct endpoints", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    (void)reference_identity::register_operators();
    const auto step = values<Int>(1, 1, 1);
    const auto list = values<Value>(list_delta<TS<Int>>({{0, 7}, {1, 8}}));
    CHECK(eval_node<reference_identity::aliased_list>(list, step) == values<Bool>(true, true, true));
    CHECK(eval_node<reference_identity::distinct_lists>(list, list, step) == values<Bool>(false, false, false));
    using Fields = reference_identity::IdentityFields::time_series;
    const auto bundle = values<Value>(tsb_delta<Fields>(Int{7}, Int{8}));
    CHECK(eval_node<reference_identity::aliased_bundle>(bundle, step) == values<Bool>(true, true, true));
    CHECK(eval_node<reference_identity::distinct_bundles>(bundle, bundle, step) == values<Bool>(false, false, false));
    const auto first = values<Int>(7);
    const auto second = values<Int>(8);
    CHECK(eval_node<CompareAliasedChildTrees>(first, second, step) == values<Bool>(true, true, true));
    CHECK(eval_node<CompareChildTrees>(first, second, second, step) == values<Bool>(false, false, false));
}

TEST_CASE("prepared reference borrowing preserves the absent payload guard", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    const auto *schema = schema_descriptor<REF<TS<Int>>>::ts_meta();
    const auto binding = ValuePlanFactory::instance().type_for(schema->value_schema);
    ValueView missing{binding, nullptr};
    CHECK_THROWS_WITH(hgl::ordinary::reference_argument(missing), "checked_as<T> on invalid ValueView");
}

TEST_CASE("generated selected-child guard reads only the proven partial bundle field", "[codegen][generated][ref][validity]") {
    hgl::wiring::ensure_session();
    (void)reference_identity::register_operators();
    using Fields = reference_identity::IdentityFields::time_series;
    CHECK(eval_node<reference_identity::guarded_first>(values<Value>(
        tsb_delta<Fields>(std::nullopt, Int{20}), tsb_delta<Fields>(Int{0}, std::nullopt), std::nullopt,
        tsb_delta<Fields>(std::nullopt, Int{21}), tsb_delta<Fields>(Int{0}, std::nullopt))) ==
        values<Int>(std::nullopt, 0, std::nullopt, std::nullopt, 0));
}

TEST_CASE("generated reference projections follow only their selected declared child", "[codegen][generated][ref][projection]") {
    namespace projections = examples::reference_wiring_projections;
    hgl::wiring::ensure_session();
    (void)projections::register_operators();
    using Quote = projections::WiringQuote::time_series;
    const auto first = values<Value>(tsb_delta<Quote>(Int{7}, std::nullopt),
        tsb_delta<Quote>(std::nullopt, Int{20}), tsb_delta<Quote>(Int{7}, std::nullopt), std::nullopt);
    CHECK(eval_node<projections::public_bid>(first) == values<Int>(7, std::nullopt, 7, std::nullopt));
    CHECK(eval_node<projections::public_second>(values<Value>(list_delta<TS<Int>>({{0, 7}}),
        list_delta<TS<Int>>({{1, 20}}), std::nullopt, list_delta<TS<Int>>({{0, 8}, {1, 21}}))) ==
        values<Int>(std::nullopt, 20, std::nullopt, 21));
    CHECK(eval_node<projections::public_plain_second>(values<Value>(list_delta<TS<Int>>({{0, 7}}),
        list_delta<TS<Int>>({{1, 20}}), std::nullopt, list_delta<TS<Int>>({{0, 8}}),
        list_delta<TS<Int>>({{1, 20}}))) == values<Int>(std::nullopt, 20, std::nullopt, std::nullopt, 20));
    CHECK(eval_node<projections::public_selected_bid>(values<Int>(0, std::nullopt, 1, std::nullopt, 1),
        values<Value>(tsb_delta<Quote>(Int{7}, std::nullopt), tsb_delta<Quote>(std::nullopt, Int{20})),
        values<Value>(tsb_delta<Quote>(Int{7}, std::nullopt), std::nullopt, std::nullopt,
            tsb_delta<Quote>(Int{9}, std::nullopt))) == values<Int>(7, std::nullopt, 7, 9, std::nullopt));
    CHECK(eval_node<projections::public_nested_bid>(values<Int>(0), values<Int>(0, 1, std::nullopt, 1, 0),
        values<Int>(0, std::nullopt, 1), values<Value>(tsb_delta<Quote>(Int{7}, std::nullopt)),
        values<Value>(tsb_delta<Quote>(Int{8}, std::nullopt)), values<Value>(tsb_delta<Quote>(Int{20}, std::nullopt))) ==
        values<Int>(7, 8, std::nullopt, std::nullopt, 7));
}

TEST_CASE("prepared reference projection follows a REF child while its parent designation is unchanged",
          "[codegen][generated][ref][projection]") {
    hgl::wiring::ensure_session();
    CHECK(eval_node<ProjectReferenceChild>(values<Int>(0, 1, std::nullopt, std::nullopt, 0),
        values<Int>(7, std::nullopt, 9, std::nullopt, 11),
        values<Int>(8, std::nullopt, std::nullopt, 10)) == values<Int>(7, 8, std::nullopt, 10, 11));
}

TEST_CASE("reference projection rejects an incompatible declared output during wiring",
          "[codegen][generated][ref][projection]") {
    hgl::wiring::ensure_session();
    CHECK_THROWS(eval_node<IncompatibleProjectedOutput>(values<Value>()));
}

TEST_CASE("generated REF bearing nominal fields follow child and parent retargeting", "[codegen][generated][ref]") {
    hgl::wiring::ensure_session();
    namespace nested = codegen::nested_reference_fields;
    CHECK(eval_node<nested::child_changes>(values<Int>(0, std::nullopt, 1, std::nullopt, 0, std::nullopt),
                                           values<Int>(7, 8, 9, 10, 11, 12),
                                           values<Int>(18, 19, 20, 21, 22, 23)) == values<Int>(7, 8, 20, 21, 11, 12));
    CHECK(eval_node<nested::generic_changes>(values<Int>(0, std::nullopt, 1, std::nullopt, 0, std::nullopt),
                                             values<Int>(7, 8, 9, 10, 11, 12),
                                             values<Int>(18, 19, 20, 21, 22, 23)) == values<Int>(7, 8, 20, 21, 11, 12));
    CHECK(eval_node<nested::parent_and_child_changes>(values<Int>(0, std::nullopt, 1, std::nullopt, 0), values<Int>(0, 1),
                                                      values<Int>(7, 8, 9, 10, 11), values<Int>(19, 20, 20, 20, 21),
                                                      values<Int>(28, 29, 30, 31, 32)) == values<Int>(7, 20, 30, 31, 21));
}
