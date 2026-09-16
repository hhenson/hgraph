#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <catch2/catch_test_macros.hpp>

namespace {
using namespace hgraph;
using namespace hgraph::testing;
template<Int V> struct ConstantBody {
    static Port<TS<Int>> compose(Wiring &w) {
        return wire<stdlib::const_>(w, Int{V}).template as<TS<Int>>();
    }
};
template<Int V> struct ConstantComponent {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>) {
        return stdlib::component<ConstantBody<V>>(w, "constant");
    }
};
}

TEST_CASE("component constant restores without a bootstrap tick and validates its scalar", "[checkpoint][constant]") {
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> saved;
    std::size_t commits = 0;
    configure_component_recovery(context.state().view(), {
        .component_id = "constant", .load = [&] { return saved; },
        .commit = [&](const auto &image) { saved = image; ++commits; }});
    const auto cut = MIN_ST + MIN_TD;
    CHECK_OUTPUT(eval_node_with_options<ConstantComponent<3>>({.start_time = MIN_ST, .end_time = cut}, values<Int>(none)), values<Int>(3));
    REQUIRE(saved);
    auto resumed = eval_node_with_options<ConstantComponent<3>>({.start_time = cut, .end_time = cut + MIN_TD}, values<Int>(none));
    REQUIRE(resumed.size() <= 1);
    resumed.resize(1);
    CHECK_OUTPUT(resumed, values<Int>(none));
    REQUIRE(commits == 2);
    REQUIRE_THROWS(eval_node_with_options<ConstantComponent<4>>({.start_time = cut + MIN_TD, .end_time = cut + MIN_TD * 2}, values<Int>(none)));
    CHECK(commits == 2);
}
