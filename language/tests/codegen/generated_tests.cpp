// The generated side of backend parity (developer guide, "Backend parity"):
// `parity.hgl` compiled by `hgl emit-cpp` through `hgl_add_module`, wired
// and evaluated with hgraph's own harness. The expectations are the ones the
// module's `test` blocks assert under `hgl test`.
#include <parity.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <vector>

using namespace hgraph;
using namespace hgraph::testing;
namespace parity = hgl::codegen::parity;

namespace
{
    void session()
    {
        hgl::wiring::ensure_session();
        parity::register_operators();
    }
}  // namespace

TEST_CASE("generated plus records the ticks hgl test asserts", "[codegen][generated]")
{
    session();
    CHECK(eval_node<parity::plus>(values<Float>(1.0, 2.0), values<Float>(10.0, 20.0)) == values<Float>(11.0, 22.0));
}

TEST_CASE("generated compositions wire helpers, constants and kernels", "[codegen][generated]")
{
    session();
    CHECK(eval_node<parity::scaled_sum>(values<Float>(1.0), values<Float>(2.0), Float{3.0}) == values<Float>(9.0));
    CHECK(eval_node<parity::above>(values<Float>(1.0, 3.0), Float{2.0}) == values<Bool>(false, true));
    CHECK(eval_node<parity::maybe_double>(values<Float>(1.5), Bool{true}) == values<Float>(3.0));
    CHECK(eval_node<parity::maybe_double>(values<Float>(1.5), Bool{false}) == values<Float>(1.5));
    CHECK(eval_node<parity::offset_by>(values<Float>(1.0, 2.5), Int{3}) == values<Float>(7.0, 8.5));
}

TEST_CASE("generated exports are registered by module-qualified name with their defaults", "[codegen][generated]")
{
    session();
    CHECK(hgl::wiring::has_operator("hgl.codegen.parity.plus"));
    CHECK(hgl::wiring::has_operator("hgl.codegen.parity.scaled_sum"));
    // Through the registry the const default applies, as it would from Python.
    CHECK_OUTPUT(eval_node<parity::operators::scaled_sum>(values<Float>(1.0), values<Float>(2.0)), values<Float>(6.0));
    CHECK_OUTPUT(eval_node<parity::operators::maybe_double>(values<Float>(1.5)), values<Float>(3.0));
}
