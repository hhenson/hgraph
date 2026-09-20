// The generated side of backend parity (developer guide, "Backend parity"):
// `parity.hgl` compiled by `hgl emit-cpp` through `hgl_add_module`, wired
// and evaluated with hgraph's own harness. The expectations are the ones the
// module's `test` blocks assert under `hgl test`.
#include <conditional-early-return.h>
#include <conditional-forwarding.h>
#include <conditional-mixed-results.h>
#include <conditional-omitted-else.h>
#include <conditional-result.h>
#include <conditional-results.h>
#include <conditional-sinks.h>
#include <parity.h>
#include <parameter-packs.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using namespace hgraph;
using namespace hgraph::testing;
namespace parity              = hgl::codegen::parity;
namespace parameter_packs     = checks::parameter_packs;
namespace conditional_early   = examples::conditional_early_return;
namespace conditional_forward = examples::conditional_forwarding;
namespace conditional_mixed   = examples::conditional_mixed_results;
namespace conditional_omitted = examples::conditional_omitted_else;
namespace conditional_result  = examples::conditional_result;
namespace conditional_results = examples::conditional_results;
namespace conditional_sinks   = examples::conditional_sinks;

namespace
{
    struct route_packs
    {
        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> price, Port<TS<Str>> symbol) {
            wire<parameter_packs::operators::route>(w, price, symbol);
            return price;
        }
    };

    void session() {
        hgl::wiring::ensure_session();
        parity::register_operators();
    }

    /// Reading(value: value, note: note), with `unit` at its default.
    Value reading(double value, std::optional<std::string> note = std::nullopt) {
        Value result{ValuePlanFactory::instance().type_for(scalar_descriptor<typename parity::Reading::value_type>::value_meta())};
        auto  fields = result.as_bundle().begin_mutation();
        fields["value"].set(value);
        fields["unit"].set(std::string{"C"});
        if (note) { fields["note"].set(*note); }
        return result;
    }
}  // namespace

TEST_CASE("generated heterogeneous pack calls retain concrete endpoint schemas", "[codegen][generated][parameter-pack]") {
    session();
    parameter_packs::register_operators();
    CHECK_OUTPUT(eval_node<route_packs>(values<Float>(1.5, none, 2.5), values<Str>(Str{"a"}, Str{"b"}, none)),
                 values<Float>(1.5, none, 2.5));
    CHECK_OUTPUT(eval_node<parameter_packs::operators::route_constants>(values<Float>(1.5, none, 2.5)),
                 values<Float>(1.5, none, 2.5));
    CHECK_OUTPUT(eval_node<parameter_packs::operators::route_all>(values<Bool>(true, true), values<Bool>(false, true)),
                 values<Bool>(false, true));
}

TEST_CASE("generated plus records the ticks hgl test asserts", "[codegen][generated]") {
    session();
    CHECK(eval_node<parity::plus>(values<Float>(1.0, 2.0), values<Float>(10.0, 20.0)) == values<Float>(11.0, 22.0));
}

TEST_CASE("generated compositions wire helpers, constants and kernels", "[codegen][generated]") {
    session();
    CHECK(eval_node<parity::scaled_sum>(values<Float>(1.0), values<Float>(2.0), Float{3.0}) == values<Float>(9.0));
    CHECK(eval_node<parity::above>(values<Float>(1.0, 3.0), Float{2.0}) == values<Bool>(false, true));
    CHECK(eval_node<parity::maybe_double>(values<Float>(1.5), Bool{true}) == values<Float>(3.0));
    CHECK(eval_node<parity::maybe_double>(values<Float>(1.5), Bool{false}) == values<Float>(1.5));
    CHECK(eval_node<parity::offset_by>(values<Float>(1.0, 2.5), Int{3}) == values<Float>(7.0, 8.5));
}

TEST_CASE("generated atomic construction aggregates the fields that have a value", "[codegen][generated][struct]") {
    session();
    CHECK_OUTPUT(eval_node<parity::operators::reading>(values<Float>(1.0, 2.0)), values<Value>(reading(1.0), reading(2.0)));
    CHECK_OUTPUT(eval_node<parity::operators::cleared_reading>(values<Float>(1.0)), values<Value>(reading(1.0)));
    CHECK_OUTPUT((eval_node<parity::operators::annotated_reading>(values<Float>(1.0, 2.0), values<Str>(none, Str{"n"}))),
                 values<Value>(none, reading(2.0, "n")));
    CHECK_OUTPUT(eval_node<parity::operators::fixed_reading>(values<Float>(0.0, 1.0)), values<Value>(reading(1.5), none));

    const Value empty_tag{ValuePlanFactory::instance().type_for(scalar_descriptor<typename parity::Tag::value_type>::value_meta())};
    CHECK_OUTPUT(eval_node<parity::operators::empty_tag>(values<Float>(0.0, 1.0)), values<Value>(empty_tag, none));
}

TEST_CASE("generated temporal conditionals match scripted switch behavior", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<parity::choose>(values<Bool>(true, true, false), values<Int>(1, 2, 3), values<Int>(10, 20, 30)) ==
          values<Int>(2, 3, 29));
}

TEST_CASE("generated temporal conditionals embedded in an expression feed the enclosing operator",
          "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<parity::choose_embedded>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(2, 21));
}

TEST_CASE("generated temporal early returns place the continuation in the falling branch",
          "[codegen][generated][conditional][continuation]") {
    session();
    CHECK(eval_node<conditional_early::choose>(values<Bool>(true, true, false), values<Int>(1, 2, 3), values<Int>(10, 20, 30)) ==
          values<Int>(2, 3, 58));
    CHECK(eval_node<conditional_early::choose_tail>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(2, 38));
    CHECK(eval_node<conditional_early::choose_returning_tail>(values<Bool>(true, false), values<Int>(1, 2),
                                                              values<Int>(10, 20)) == values<Int>(2, 19));
    CHECK(eval_node<conditional_early::choose_assigned>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(2, 38));
}

TEST_CASE("generated nested temporal early returns retain every enclosing continuation",
          "[codegen][generated][conditional][continuation]") {
    session();
    CHECK(eval_node<conditional_early::choose_nested>(values<Bool>(true, true, false), values<Bool>(true, false, false),
                                                      values<Int>(1, 2, 3), values<Int>(10, 20, 30),
                                                      values<Int>(100, 200, 300)) == values<Int>(2, 38, 900));
    CHECK(eval_node<conditional_early::choose_deep>(values<Bool>(true, false, false, false), values<Bool>(false, false, true, true),
                                                    values<Bool>(false, false, true, false), values<Int>(1, 2, 3, 4),
                                                    values<Int>(10, 20, 30, 40), values<Int>(100, 200, 300, 400),
                                                    values<Int>(1000, 2000, 3000, 4000)) == values<Int>(1, 2000, 30, 400));
}

TEST_CASE("generated outputless temporal early returns retain the falling continuation",
          "[codegen][generated][conditional][continuation]") {
    session();
    Wiring w;
    auto   enabled = wire<stdlib::const_, TS<Bool>>(w, Bool{true});
    auto   value   = wire<stdlib::const_, TS<Float>>(w, Float{2.0});
    conditional_early::observe::compose(w, enabled, value);
    conditional_early::observe_nested::compose(w, enabled, enabled, value);
    CHECK_NOTHROW(std::move(w).finish());
}

TEST_CASE("generated outputless temporal conditionals wire a sink switch", "[codegen][generated][conditional]") {
    session();
    Wiring w;
    auto   enabled = wire<stdlib::const_, TS<Bool>>(w, Bool{true});
    auto   value   = wire<stdlib::const_, TS<Float>>(w, Float{2.0});
    conditional_sinks::observe::compose(w, enabled, value);
    const GraphBuilder graph = std::move(w).finish();

    const auto switches = std::ranges::count_if(graph.nodes(), [](const NodeBuilder &node) {
        const NodeTypeMetaData *type = node.type().schema();
        return type != nullptr && type->node_kind == NodeKind::Nested && type->display_name != nullptr &&
               std::string_view{type->display_name} == "switch_";
    });
    CHECK(switches == 1U);
}

TEST_CASE("generated outputless conditionals compose inside value graphs", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_sinks::observe_and_forward>(values<Bool>(false, true), values<Float>(1.0, 2.0)) ==
          values<Float>(1.0, 2.0));
}

TEST_CASE("generated temporal conditionals remap one assigned result", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_result::adjusted>(values<Bool>(true, true, false), values<Int>(1, 2, 3), values<Int>(10, 20, 30)) ==
          values<Int>(4, 6, 58));
}

TEST_CASE("generated temporal branches reuse results assigned earlier in the branch", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_result::adjusted_twice>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(4, 57));
}

TEST_CASE("generated temporal conditionals remap several assigned results", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_results::adjusted>(values<Bool>(true, true, false), values<Int>(1, 2, 3),
                                                   values<Int>(10, 20, 30)) == values<Int>(5, 8, 88));
}

TEST_CASE("generated temporal conditionals combine an expression result with an escaping assignment",
          "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_mixed::adjusted>(values<Bool>(true, true, false), values<Int>(1, 2, 3), values<Int>(10, 20, 30)) ==
          values<Int>(4, 7, 119));
}

TEST_CASE("generated inline mixed temporal conditionals sequence their escaping projection", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_mixed::adjusted_inline>(values<Bool>(true, true, false), values<Int>(1, 2, 3),
                                                        values<Int>(10, 20, 30)) == values<Int>(4, 7, 119));
}

TEST_CASE("generated temporal conditionals forward an existing binding by reference", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_forward::adjusted>(values<Bool>(false, true, false), values<Int>(1, 2, 3)) == values<Int>(1, 3, 3));
}

TEST_CASE("generated temporal conditional captures named key remain positional", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_forward::adjusted_key>(values<Bool>(false, true, false), values<Int>(1, 2, 3)) ==
          values<Int>(1, 3, 3));
}

TEST_CASE("generated temporal conditionals forward structural result fields independently", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_forward::adjusted_pair>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(12, 23));
}

TEST_CASE("generated value-producing temporal conditionals synthesize an omitted else", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<conditional_omitted::choose>(values<Bool>(false, true, false), values<Int>(1, 2, 3)) ==
          values<Int>(none, 3, none));
}

TEST_CASE("generated exports are registered by module-qualified name with their defaults", "[codegen][generated]") {
    session();
    CHECK(hgl::wiring::has_operator("hgl.codegen.parity.plus"));
    CHECK(hgl::wiring::has_operator("hgl.codegen.parity.scaled_sum"));
    CHECK(hgl::wiring::has_operator("hgl.codegen.parity.choose"));
    // Through the registry the const default applies, as it would from Python.
    CHECK_OUTPUT(eval_node<parity::operators::scaled_sum>(values<Float>(1.0), values<Float>(2.0)), values<Float>(6.0));
    CHECK_OUTPUT(eval_node<parity::operators::maybe_double>(values<Float>(1.5)), values<Float>(3.0));
}

TEST_CASE("compiled wiring checks integer overflow before graph execution", "[codegen][parity][arithmetic]") {
    hgl::wiring::ensure_session();
    parity::register_operators();
    const Int lo = std::numeric_limits<Int>::min();
    const Int hi = std::numeric_limits<Int>::max();
    CHECK_OUTPUT(eval_node<parity::operators::checked_add>(values<Int>(0), arg<"x">(hi - 1), arg<"y">(Int{1})), values<Int>(hi));
    CHECK_OUTPUT(eval_node<parity::operators::checked_sub>(values<Int>(0), arg<"x">(lo + 1), arg<"y">(Int{1})), values<Int>(lo));
    CHECK_OUTPUT(eval_node<parity::operators::checked_mul>(values<Int>(0), arg<"x">(lo / 2), arg<"y">(Int{2})), values<Int>(lo));
    CHECK_OUTPUT(eval_node<parity::operators::checked_floor>(values<Int>(0), arg<"x">(Int{-7}), arg<"y">(Int{3})), values<Int>(-3));
    CHECK_OUTPUT(eval_node<parity::operators::checked_rem>(values<Int>(0), arg<"x">(lo), arg<"y">(Int{-1})), values<Int>(0));
    CHECK_THROWS(eval_node<parity::operators::checked_add>(values<Int>(0), arg<"x">(hi), arg<"y">(Int{1})));
    CHECK_THROWS(eval_node<parity::operators::checked_sub>(values<Int>(0), arg<"x">(lo), arg<"y">(Int{1})));
    CHECK_THROWS(eval_node<parity::operators::checked_mul>(values<Int>(0), arg<"x">(lo), arg<"y">(Int{-1})));
    CHECK_THROWS(eval_node<parity::operators::checked_neg>(values<Int>(0), arg<"x">(lo)));
    CHECK_THROWS(eval_node<parity::operators::checked_floor>(values<Int>(0), arg<"x">(lo), arg<"y">(Int{-1})));
    CHECK_THROWS(eval_node<parity::operators::checked_rem>(values<Int>(0), arg<"x">(Int{1}), arg<"y">(Int{0})));
}
