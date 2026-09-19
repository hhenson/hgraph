// Recursive struct fields (ADR 0012) through generated C++. The module is
// tests/wiring/recursive-structs.hgl, whose `hgl test` cases run the same
// programs through direct wiring; each case here asserts the same ticks, so
// the two backends agree tick for tick.
#include <recursive-structs.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

using namespace hgraph;
using namespace hgraph::testing;
namespace recursive = tests::recursive_structs;

namespace
{
    using RecursiveNode = typename recursive::Node::value_type;

    using Three    = Operator<"tests.recursive_structs.three", In<"x", TS<Int>>, Out<TS<RecursiveNode>>>;
    using PassNode = Operator<"tests.recursive_structs.pass_node", In<"x", TS<RecursiveNode>>, Out<TS<RecursiveNode>>>;
    using Head     = Operator<"tests.recursive_structs.head", In<"x", TS<RecursiveNode>>, Out<TS<RecursiveNode>>>;
    using PassPair = Operator<"tests.recursive_structs.pass_pair", In<"x", TS<typename recursive::A::value_type>>,
                              Out<TS<typename recursive::A::value_type>>>;
    using PassTree = Operator<"tests.recursive_structs.pass_tree", In<"x", TS<typename recursive::Tree<Int>::value_type>>,
                              Out<TS<typename recursive::Tree<Int>::value_type>>>;
    using Unwrap = Operator<"tests.recursive_structs.unwrap", In<"v", TS<Int>>, In<"n", TS<RecursiveNode>>, Out<TS<RecursiveNode>>>;
    using RootValue = Operator<"tests.recursive_structs.root_value", In<"v", TS<Int>>, In<"n", TS<RecursiveNode>>, Out<TS<Int>>>;

    template <typename Fields> void fill_chain(Fields fields, std::span<const std::int64_t> values) {
        fields["value"].set(values.front());
        if (values.size() > 1U) { fill_chain(fields["next"].as_bundle().begin_mutation(), values.subspan(1)); }
    }

    /// Node(value: values[0], next: Node(value: values[1], ...)).
    Value chain(std::vector<std::int64_t> values) {
        Value root{ValuePlanFactory::instance().type_for(scalar_descriptor<RecursiveNode>::value_meta())};
        fill_chain(root.as_bundle().begin_mutation(), values);
        return root;
    }

    /// A(tag: outer, b: B(a: A(tag: inner))).
    Value mutual(const std::string &outer, const std::string &inner) {
        Value root{ValuePlanFactory::instance().type_for(scalar_descriptor<typename recursive::A::value_type>::value_meta())};
        auto  fields = root.as_bundle().begin_mutation();
        fields["tag"].set(outer);
        fields["b"].as_bundle().begin_mutation()["a"].as_bundle().begin_mutation()["tag"].set(inner);
        return root;
    }

    /// Tree<i64>(value: first, left: Tree<i64>(value: second)).
    Value tree(std::int64_t first, std::int64_t second) {
        Value root{
            ValuePlanFactory::instance().type_for(scalar_descriptor<typename recursive::Tree<Int>::value_type>::value_meta())};
        auto fields = root.as_bundle().begin_mutation();
        fields["value"].set(first);
        fields["left"].as_bundle().begin_mutation()["value"].set(second);
        return root;
    }
}  // namespace

TEST_CASE("generated recursive structs register their edges as owners", "[codegen][generated][recursive]") {
    auto       &registry = TypeRegistry::instance();
    const auto *node     = scalar_descriptor<RecursiveNode>::value_meta();
    REQUIRE(node->field_count == 2U);
    REQUIRE(node->fields[1].type->is_owned());
    CHECK(node->fields[1].type->element_type == node);
    CHECK(node->is_hashable());

    const auto *a = scalar_descriptor<typename recursive::A::value_type>::value_meta();
    const auto *b = scalar_descriptor<typename recursive::B::value_type>::value_meta();
    CHECK(a->fields[1].type->element_type == b);
    CHECK(b->fields[0].type->element_type == a);

    // Generated C++ and direct wiring spell a specialization the same way.
    const auto *pair = scalar_descriptor<typename recursive::Pair<Int, Str>::value_type>::value_meta();
    CHECK(std::string{pair->name()} == "tests.recursive_structs::Pair[int, str]");
    CHECK(std::string{pair->fields[1].type->element_type->name()} == "tests.recursive_structs::Pair[str, int]");

    // An edge to an abstract parent owns that parent's schema.
    const auto *add  = scalar_descriptor<typename recursive::Add::value_type>::value_meta();
    const auto *expr = scalar_descriptor<typename recursive::Expr::value_type>::value_meta();
    CHECK(add->fields[1].type->element_type == expr);

    // The temporal shape's edge is one endpoint; its value schema is the struct.
    const auto *temporal = schema_descriptor<typename recursive::Node::time_series>::ts_meta();
    CHECK(temporal->value_schema == node);
    CHECK(temporal->fields()[1].type == registry.ts(registry.owned(node)));
}

TEST_CASE("generated recursive structs agree with direct wiring tick for tick", "[codegen][generated][recursive]") {
    hgl::wiring::ensure_session();
    recursive::register_operators();

    SECTION("three_deep") { CHECK_OUTPUT(eval_node<Three>(values<Int>(0)), values<Value>(chain({1, 2, 3}))); }
    SECTION("round_trip") {
        CHECK_OUTPUT((eval_node<PassNode, TS<RecursiveNode>>(values<Value>(chain({1, 2, 3}), chain({4})))),
                     values<Value>(chain({1, 2, 3}), chain({4})));
    }
    SECTION("compared_at_depth") {
        const auto result = eval_node<PassNode, TS<RecursiveNode>>(values<Value>(chain({1, 2, 3})));
        REQUIRE(result.size() == 1U);
        REQUIRE(result.front().has_value());
        CHECK_FALSE(result.front()->view().equals(chain({1, 2, 4}).view()));
    }
    SECTION("field_access") {
        CHECK_OUTPUT((eval_node<Head, TS<RecursiveNode>>(values<Value>(chain({1, 2, 3})))), values<Value>(chain({2, 3})));
    }
    SECTION("mutual_pair") {
        CHECK_OUTPUT((eval_node<PassPair, TS<typename recursive::A::value_type>>(values<Value>(mutual("x", "y")))),
                     values<Value>(mutual("x", "y")));
    }
    SECTION("generic_edges") {
        CHECK_OUTPUT((eval_node<PassTree, TS<typename recursive::Tree<Int>::value_type>>(values<Value>(tree(1, 2)))),
                     values<Value>(tree(1, 2)));
    }
    SECTION("temporal_shape") {
        CHECK_OUTPUT((eval_node<Unwrap, TS<RecursiveNode>>(values<Int>(1), values<Value>(chain({2, 3})))),
                     values<Value>(chain({2, 3})));
        CHECK_OUTPUT((eval_node<RootValue, TS<RecursiveNode>>(values<Int>(1), values<Value>(chain({2})))), values<Int>(1));
    }
}
