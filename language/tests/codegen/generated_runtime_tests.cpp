#include <runtime.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace runtime = hgl::codegen::runtime;

namespace
{
    void session()
    {
        hgl::wiring::ensure_session();
        runtime::register_operators();
    }
}  // namespace

TEST_CASE("generated module registration owns a removable provider generation", "[codegen][runtime][lifecycle]")
{
    hgl::wiring::ensure_session();
    auto provider = runtime::register_operators();
    auto same = runtime::register_operators();
    CHECK(provider.valid());
    CHECK(provider.active());
    CHECK(same.active());
    CHECK(provider.key() == "hgl.codegen.runtime");
    CHECK(OperatorRegistry::instance().remove_provider(provider));
    CHECK_FALSE(provider.active());
    CHECK_FALSE(same.active());

    auto replacement = runtime::register_operators();
    CHECK(replacement.active());
    CHECK_FALSE(OperatorRegistry::instance().remove_provider(provider));
    CHECK(OperatorRegistry::instance().remove_provider(replacement));
}

TEST_CASE("generated runtime impl functions register as node overloads", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("a generated composition can wire a generated operator implementation", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute_graph>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("generated runtime predicates use modified-or and valid-and semantics", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::add_when_ready>(values<Float>(1.0, none, 3.0),
                                                        values<Float>(none, 10.0, 20.0)),
                 values<Float>(none, 11.0, 23.0));
}

TEST_CASE("generated activation analysis leaves sampled inputs passive", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::sample_on_trigger>(values<Float>(1.0, none, 2.0),
                                                           values<Float>(10.0, 20.0, none)),
                 values<Float>(10.0, none, 20.0));
}

TEST_CASE("generated runtime metadata reads the input selector", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::updated_at>(values<Float>(1.0, none, 2.0)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD));
}

TEST_CASE("generated runtime handlers share recordable state and run in source order", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::combined_total>(values<Float>(10.0, none, 2.0),
                                                        values<Float>(3.0, 1.0, none)),
                 values<Float>(7.0, 6.0, 8.0));
}

TEST_CASE("a generated composition can wire a generated runtime node", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::combined_total_graph>(values<Float>(10.0, none, 2.0),
                                                              values<Float>(3.0, 1.0, none)),
                 values<Float>(7.0, 6.0, 8.0));
}

TEST_CASE("generated inject out exposes the previous value and writes non-terminally", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::running_total>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("generated runtime lifecycle hooks run around evaluation", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::lifecycle_value>(values<Float>(4.0, 5.0)),
                 values<Float>(4.0, 5.0));
}

TEST_CASE("generated state initializers can use const parameters", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::configured_total>(values<Float>(1.0, 2.0), arg<"initial">(Float{5.0})),
                 values<Float>(6.0, 8.0));
}

TEST_CASE("generated runtime functions without when use ordinary input policy", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::unconditional_total>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("a generated composition can wire a private generated runtime node", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::private_total_graph>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}
