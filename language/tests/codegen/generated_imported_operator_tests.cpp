#include <consumer.h>
#include <contracts.h>
#include <provider.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("separate HGL providers implement the original imported operator identity", "[codegen][runtime][imports]") {
    hgl::wiring::ensure_session();
    auto      &registry = OperatorRegistry::instance();
    const auto provider = checks::imported_provider::register_operators();
    const auto consumer = checks::imported_consumer::register_operators();
    auto       cleanup  = make_scope_exit<true>([&] {
        (void)registry.remove_provider(consumer);
        (void)registry.remove_provider(provider);
    });

    CHECK_OUTPUT(eval_node<checks::imported_contracts::operators::adjust>(values<Int>(1, none, 3), Int{2}),
                 values<Int>(3, none, 5));
    CHECK_OUTPUT(eval_node<checks::imported_contracts::operators::adjust>(values<Float>(1.0, none, 3.0), Float{0.5}),
                 values<Float>(1.5, none, 3.5));
    CHECK_OUTPUT(eval_node<checks::imported_contracts::operators::relay>(values<Bool>(true, none, false)),
                 values<Bool>(true, none, false));
    CHECK_OUTPUT(eval_node<checks::imported_consumer::operators::integer>(values<Int>(1, 3)), values<Int>(3, 5));
    CHECK_OUTPUT(eval_node<checks::imported_consumer::operators::real>(values<Float>(1.0, 3.0)), values<Float>(1.5, 3.5));
    CHECK_OUTPUT(eval_node<checks::imported_consumer::operators::forward>(values<Bool>(true, false)), values<Bool>(true, false));

    REQUIRE(hgl::wiring::has_operator("checks.imported_contracts.adjust"));
    CHECK_FALSE(hgl::wiring::has_operator("checks.imported_provider.adjust"));
    CHECK(registry.remove_provider(consumer));
    CHECK(registry.remove_provider(provider));
    cleanup.release();
    CHECK_THROWS_AS(eval_node<checks::imported_contracts::operators::adjust>(values<Int>(1), Int{2}), OperatorResolutionError);
}
