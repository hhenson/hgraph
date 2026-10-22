#include <hgraph/types/wiring_inspection.h>
#include <hgraph/types/subgraph_wiring.h>
#include <catch2/catch_test_macros.hpp>
#include <array>

namespace {
using namespace hgraph;
struct Source { static void eval(Out<TS<Int>>) {} };
struct OtherSource { static void eval(Out<TS<Int>>) {} };
struct Join {
    static void eval(In<"left", TS<Int>>, In<"right", TS<Int>>, Out<TS<Int>>) {}
};
struct Sample {
    static void eval(In<"clock", TS<Int>>, In<"value", TS<Int>, InputValidity::Unchecked,
                     InputActivity::Passive>, Out<TS<Int>>) {}
};
struct Membership {
    static void eval(In<"values", TSL<TS<Int>, 2>, InputValidity::Unchecked,
                     InputActivity::Structural>, Out<TS<Int>>) {}
};
struct Packed {
    static WiringInputActivityOps wiring_input_activity() {
        return {[](const NodeBuilder &, std::span<const std::size_t> path) { return path[1] == 0; }};
    }
    static void eval(In<"values", TSL<TS<Int>, 2>, InputValidity::Unchecked,
                     InputActivity::Passive>, Out<TS<Int>>) {}
};
struct Inline {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b) {
        return wire<Sample>(w, a, b);
    }
};
}
TEST_CASE("Wiring inspection finds boundaries and active paths take precedence", "[wiring][inspection]") {
    Wiring w;
    auto a = wire<Source>(w);
    auto b = wire<OtherSource>(w);
    auto sampled = wire<Inline>(w, a, b);
    std::array sources{a.erased(), b.erased()};
    REQUIRE(inspect_wiring_dependencies(sampled.erased(), sources) ==
            std::vector<WiringDependency>{{0, false}, {1, true}});
    auto diamond = wire<Join>(w, sampled, b);
    REQUIRE(inspect_wiring_dependencies(diamond.erased(), sources) ==
            std::vector<WiringDependency>{{0, false}, {1, false}});
    std::array boundaries{sampled.erased(), a.erased(), b.erased()};
    REQUIRE(inspect_wiring_dependencies(sampled.erased(), boundaries) ==
            std::vector<WiringDependency>{{0, false}});
}
TEST_CASE("Wiring inspection respects usage passivity and structural projection", "[wiring][inspection]") {
    Wiring w;
    auto a = wire<Source>(w);
    auto b = wire<OtherSource>(w);
    auto weak = Port<TS<Int>>{w, b.erased().with_arg_tag(WiringPortRef::ArgTag::Passive)};
    auto output = wire<Join>(w, a, weak);
    std::array sources{a.erased(), b.erased()};
    REQUIRE(inspect_wiring_dependencies(output.erased(), sources) ==
            std::vector<WiringDependency>{{0, false}, {1, true}});
    auto structure = WiringPortRef::structural_source(schema_descriptor<TSL<TS<Int>, 2>>::ts_meta(),
                                                     {a.erased(), b.erased()});
    auto projected = subgraph_wiring_detail::tsl_element_ref(structure, 1, b.erased().schema);
    REQUIRE(inspect_wiring_dependencies(projected, sources) ==
            std::vector<WiringDependency>{{1, false}});
}
TEST_CASE("Wiring inspection resolves delayed sources and rejects unresolved ones", "[wiring][inspection]") {
    Wiring w;
    auto a = wire<Source>(w);
    auto state = std::make_shared<WiringDelayedBindingState>();
    state->wiring = &w;
    state->schema = a.erased().schema;
    auto delayed = WiringPortRef::delayed_source(state, {}, state->schema);
    std::array sources{a.erased()};
    REQUIRE_THROWS_AS(inspect_wiring_dependencies(delayed, sources), std::invalid_argument);
    state->source = a.erased();
    REQUIRE(inspect_wiring_dependencies(delayed, sources) == std::vector<WiringDependency>{{0, false}});
    state->source = delayed;
    REQUIRE(inspect_wiring_dependencies(delayed, sources).empty());
    state->source.reset();
}

TEST_CASE("Wiring inspection rejects compiled child graphs unless supplied as boundaries", "[wiring][inspection]") {
    Wiring w;
    auto a = wire<Source>(w);
    auto b = wire<OtherSource>(w);
    auto child = nested_<Inline>(w, a, b);
    std::array sources{a.erased(), b.erased()};
    REQUIRE_THROWS_AS(inspect_wiring_dependencies(child.erased(), sources), std::invalid_argument);
    std::array boundary{child.erased()};
    REQUIRE(inspect_wiring_dependencies(child.erased(), boundary) == std::vector<WiringDependency>{{0, false}});
}

TEST_CASE("Wiring inspection uses packed input activity and structural boundaries", "[wiring][inspection]") {
    Wiring w;
    auto a = wire<Source>(w);
    auto b = wire<OtherSource>(w);
    auto structure = WiringPortRef::structural_source(schema_descriptor<TSL<TS<Int>, 2>>::ts_meta(),
                                                     {a.erased(), b.erased()});
    auto packed = Port<TSL<TS<Int>, 2>>{w, structure};
    auto output = wire<Packed>(w, packed);
    std::array sources{a.erased(), b.erased()};
    REQUIRE(inspect_wiring_dependencies(output.erased(), sources) ==
            std::vector<WiringDependency>{{0, false}, {1, true}});
    (void)exception_time_series(output);
    REQUIRE(inspect_wiring_dependencies(output.erased(), sources) ==
            std::vector<WiringDependency>{{0, false}, {1, true}});
    auto membership = wire<Membership>(w, packed);
    REQUIRE(inspect_wiring_dependencies(membership.erased(), sources) ==
            std::vector<WiringDependency>{{0, false}, {1, false}});
    std::array boundary{structure};
    REQUIRE(inspect_wiring_dependencies(output.erased(), boundary) == std::vector<WiringDependency>{{0, false}});
}
