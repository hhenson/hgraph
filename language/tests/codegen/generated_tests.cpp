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

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <optional>
#include <string_view>
#include <vector>

using namespace hgraph;
using namespace hgraph::testing;
namespace parity              = hgl::codegen::parity;
namespace conditional_early   = examples::conditional_early_return;
namespace conditional_forward = examples::conditional_forwarding;
namespace conditional_mixed   = examples::conditional_mixed_results;
namespace conditional_omitted = examples::conditional_omitted_else;
namespace conditional_result  = examples::conditional_result;
namespace conditional_results = examples::conditional_results;
namespace conditional_sinks   = examples::conditional_sinks;

namespace
{
    void session() {
        hgl::wiring::ensure_session();
        parity::register_operators();
    }
}  // namespace

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

TEST_CASE("generated temporal conditionals match scripted switch behavior", "[codegen][generated][conditional]") {
    session();
    CHECK(eval_node<parity::choose>(values<Bool>(true, true, false), values<Int>(1, 2, 3), values<Int>(10, 20, 30)) ==
          values<Int>(2, 3, 29));
}

TEST_CASE("generated temporal early returns place the continuation in the falling branch",
          "[codegen][generated][conditional][continuation]") {
    session();
    CHECK(eval_node<conditional_early::choose>(values<Bool>(true, true, false), values<Int>(1, 2, 3), values<Int>(10, 20, 30)) ==
          values<Int>(2, 3, 58));
    CHECK(eval_node<conditional_early::choose_tail>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(2, 38));
    CHECK(eval_node<conditional_early::choose_assigned>(values<Bool>(true, false), values<Int>(1, 2), values<Int>(10, 20)) ==
          values<Int>(2, 38));
}

TEST_CASE("generated outputless temporal early returns retain the falling continuation",
          "[codegen][generated][conditional][continuation]") {
    session();
    Wiring w;
    auto   enabled = wire<stdlib::const_, TS<Bool>>(w, Bool{true});
    auto   value   = wire<stdlib::const_, TS<Float>>(w, Float{2.0});
    conditional_early::observe::compose(w, enabled, value);
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
