#include <hgl/ordinary_patterns.h>
#include <catch2/catch_test_macros.hpp>

namespace {
    template <typename Shape>
    using Timed = hgraph::NominalBundle<"ordinary.patterns", "Timed", false, hgraph::BundleParents<>,
        hgraph::BundleArguments<hgl::ordinary::Origin<Shape>>,
        hgraph::Field<"time", hgraph::TimeDelta>, hgraph::Field<"value", hgl::ordinary::Delta<Shape>>>;
}

TEST_CASE("ordinary typed empty replay infers the complete temporal shape", "[ordinary][patterns]") {
    using namespace hgraph;
    using Shape = TSD<Int, TSL<TS<Int>, 2>>;
    using Actual = hgl::ordinary::List<Timed<Shape>>;
    using Pattern = hgl::ordinary::List<Timed<TsVar<"T">>>;
    const auto *actual = scalar_descriptor<Actual>::value_meta();
    const hgl::ordinary::PreparedValuePlan plan{actual};
    const auto empty = plan.empty_list();
    CHECK(plan.len(empty.view()) == 0);

    ResolutionMap map;
    scalar_unifier<Pattern>::unify(actual, map);
    CHECK(map.ts("T") == schema_descriptor<Shape>::ts_meta());
    CHECK(scalar_resolver<Pattern>::resolve(map) == actual);
    CHECK(scalar_pattern_resolve(to_scalar_pattern<Pattern>(), map) == actual);
    CHECK(pattern_variables(to_scalar_pattern<Pattern>()) == std::vector<std::string>{"T"});

    ResolutionMap scalar_map;
    using ScalarActual = hgl::ordinary::List<Timed<TS<Int>>>;
    CHECK(scalar_pattern_match(to_scalar_pattern<Pattern>(), scalar_descriptor<ScalarActual>::value_meta(), scalar_map));
    CHECK(scalar_map.ts("T") == schema_descriptor<TS<Int>>::ts_meta());
    CHECK_FALSE(scalar_pattern_match(to_scalar_pattern<Pattern>(), scalar_descriptor<ScalarActual>::value_meta(), map));
}

TEST_CASE("ordinary record metadata resolves from its temporal input", "[ordinary][patterns]") {
    using namespace hgraph;
    using Shape = TSL<TS<Int>, 0>;
    using Pattern = hgl::ordinary::List<Timed<TsVar<"T">>>;
    ResolutionMap map;
    ts_unifier<TsVar<"T">>::unify(schema_descriptor<Shape>::ts_meta(), map);
    CHECK(scalar_resolver<Pattern>::resolve(map) == scalar_descriptor<hgl::ordinary::List<Timed<Shape>>>::value_meta());
}

TEST_CASE("ordinary lists reject tuple aliases and mismatched fixed extents", "[ordinary][patterns]") {
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto dynamic = to_scalar_pattern<hgl::ordinary::List<ScalarVar<"E">>>();
    const auto fixed = to_scalar_pattern<hgl::ordinary::List<ScalarVar<"E">, 0>>();
    ResolutionMap map;
    CHECK_FALSE(scalar_pattern_match(dynamic, registry.list(integer, 0, true), map));
    CHECK_FALSE(scalar_pattern_match(dynamic, registry.tuple({integer}), map));
    CHECK_FALSE(scalar_pattern_match(dynamic, registry.fixed_list(integer, 0), map));
    CHECK_FALSE(scalar_pattern_match(fixed, registry.list(integer), map));
    CHECK_FALSE(scalar_pattern_match(fixed, registry.fixed_list(integer, 1), map));
    CHECK(scalar_pattern_match(fixed, registry.fixed_list(integer, 0), map));
    CHECK(scalar_pattern_resolve(fixed, map) == registry.fixed_list(integer, 0));
    CHECK_FALSE(scalar_pattern_covers(dynamic, fixed));
}

TEST_CASE("ordinary delta inference preserves nominal identity and originating extent", "[ordinary][patterns]") {
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *scalar = schema_descriptor<TS<Int>>::ts_meta();
    const auto *first = registry.tsb("ordinary.patterns.First", {{"value", scalar}});
    const auto *second = registry.tsb("ordinary.patterns.Second", {{"value", scalar}});
    const auto pattern = to_scalar_pattern<hgl::ordinary::Delta<TsVar<"T">>>();
    ResolutionMap map;
    CHECK(scalar_pattern_match(pattern, hgl::ordinary::delta_schema(first), map));
    CHECK(map.ts("T") == first);
    CHECK_FALSE(scalar_pattern_match(pattern, hgl::ordinary::delta_schema(second), map));
    CHECK_FALSE(scalar_pattern_match(pattern, first->value_schema, map));

    ResolutionMap list_map;
    const auto *two = registry.tsl(scalar, 2);
    const auto *three = registry.tsl(scalar, 3);
    REQUIRE(two->delta_value_schema == three->delta_value_schema);
    CHECK(scalar_pattern_match(pattern, hgl::ordinary::delta_schema(two), list_map));
    CHECK(list_map.ts("T") == two);
    CHECK_FALSE(scalar_pattern_match(pattern, hgl::ordinary::delta_schema(three), list_map));
    CHECK(scalar_pattern_resolve(pattern, list_map) == hgl::ordinary::delta_schema(two));
}

TEST_CASE("ordinary projected patterns retain variable constraints and substitution", "[ordinary][patterns]") {
    using namespace hgraph;
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto constrained = to_scalar_pattern<hgl::ordinary::Held<TsVar<"T", TS<Int>>>>();
    ResolutionMap map;
    CHECK(scalar_pattern_match(constrained, integer, map));
    CHECK_FALSE(scalar_pattern_match(constrained, scalar_descriptor<Str>::value_meta(), map));
    const auto symbolic = to_scalar_pattern<hgl::ordinary::Held<TSL<TS<ScalarVar<"E">>, SIZE<"N">>>>();
    auto replaced = substitute_scalar_patterns(symbolic, {{"E", ScalarPattern::concrete(integer)}});
    replaced = substitute_size_patterns(replaced, {{"N", DimensionPattern::fixed(2)}});
    ResolutionMap empty;
    CHECK(scalar_pattern_resolve(replaced, empty) == TypeRegistry::instance().fixed_list(integer, 2));
    CHECK(pattern_variables(replaced).empty());
    PatternVariableUses uses;
    scalar_pattern_variable_uses(to_scalar_pattern<hgl::ordinary::Held<TsVar<"T">>>(),
                                ScalarPattern::concrete(integer), uses);
    REQUIRE(uses.size() == 1);
    CHECK(uses[0].first == "ts:T");
}

TEST_CASE("ordinary tuple generic shape round trips through timed list metadata", "[ordinary][patterns]") {
    using namespace hgraph;
    using Shape = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TSL<TS<Int>, 2>>>;
    using Actual = hgl::ordinary::List<Timed<Shape>>;
    using Generic = hgl::ordinary::List<Timed<TsVar<"T">>>;
    ResolutionMap map;
    const auto *actual = scalar_descriptor<Actual>::value_meta();
    scalar_unifier<Generic>::unify(actual, map);
    CHECK(map.ts("T") == schema_descriptor<Shape>::ts_meta());
    CHECK(scalar_resolver<Generic>::resolve(map) == actual);
}

TEST_CASE("ordinary projection and structural candidate use the same concrete identity", "[ordinary][patterns]") {
    using namespace hgraph;
    using Shape = TSD<Int, TS<Int>>;
    PatternVariableUses uses;
    scalar_pattern_variable_uses(to_scalar_pattern<hgl::ordinary::Held<TsVar<"T">>>(),
                                ScalarPattern::concrete(schema_descriptor<Shape>::ts_meta()->value_schema), uses);
    ts_pattern_variable_uses(to_pattern<TsVar<"T">>(), to_pattern<Shape>(), uses);
    REQUIRE(uses.size() == 2);
    CHECK(uses[0] == uses[1]);

    auto &registry = TypeRegistry::instance();
    const auto *integer = schema_descriptor<TS<Int>>::ts_meta();
    const auto *spaced = registry.tsb("ordinary.patterns.A B", {{"value", integer}});
    const auto *compact = registry.tsb("ordinary.patterns.AB", {{"value", integer}});
    PatternVariableUses distinct;
    ts_pattern_variable_uses(to_pattern<TsVar<"T">>(), TypePattern::concrete(spaced), distinct);
    ts_pattern_variable_uses(to_pattern<TsVar<"T">>(), TypePattern::concrete(compact), distinct);
    REQUIRE(distinct.size() == 2);
    CHECK(distinct[0].second != distinct[1].second);
}

TEST_CASE("atomic origins survive generic timed values and nested structural children", "[ordinary][patterns][atomic]") {
    using namespace hgraph;
    using Atomic = TS<hgl::ordinary::List<Int>>;
    using Nested = TSD<Int, Atomic>;
    using Pattern = hgl::ordinary::List<Timed<TsVar<"T">>>;
    for (const auto *shape : {schema_descriptor<Atomic>::ts_meta(), schema_descriptor<Nested>::ts_meta()}) {
        const auto *origin = hgl::ordinary::origin_schema(shape);
        CHECK(hgl::ordinary::origin_source(origin) == shape);
        const auto *sample = TypeRegistry::instance().bundle("ordinary.patterns", "Timed[" + std::string{origin->name()} + "]",
            {{"time", scalar_descriptor<TimeDelta>::value_meta()}, {"value", hgl::ordinary::delta_schema(shape)}},
            {}, false, "__type__", {origin});
        ResolutionMap map;
        CHECK(scalar_pattern_match(to_scalar_pattern<Pattern>(), TypeRegistry::instance().list(sample), map));
        CHECK(map.ts("T") == shape);
    }
    using FixedAtomic = TS<hgl::ordinary::List<Int, 2>>;
    using FixedStructural = TSL<TS<Int>, 2>;
    REQUIRE(schema_descriptor<FixedAtomic>::ts_meta()->value_schema == schema_descriptor<FixedStructural>::ts_meta()->value_schema);
    CHECK(scalar_descriptor<Timed<FixedAtomic>>::value_meta() != scalar_descriptor<Timed<FixedStructural>>::value_meta());
    const auto delta = to_scalar_pattern<hgl::ordinary::Delta<TsVar<"T">>>();
    ResolutionMap unresolved;
    CHECK_FALSE(scalar_pattern_match(delta, scalar_descriptor<hgl::ordinary::List<Int>>::value_meta(), unresolved));
    ResolutionMap known;
    ts_unifier<TsVar<"T">>::unify(schema_descriptor<Atomic>::ts_meta(), known);
    CHECK(scalar_pattern_match(delta, scalar_descriptor<hgl::ordinary::List<Int>>::value_meta(), known));
}

TEST_CASE("ordinary delta scalar selectors borrow reduced and structural payloads", "[ordinary][patterns]") {
    using namespace hgraph;
    using Generic = hgl::ordinary::Delta<TsVar<"T">>;
    for (const auto *shape : {schema_descriptor<TS<Int>>::ts_meta(), schema_descriptor<TSD<Int, TS<Int>>>::ts_meta()}) {
        const hgl::ordinary::PreparedValuePlan plan{hgl::ordinary::delta_schema(shape)};
        const Value payload{plan.binding()};
        const Scalar<"publication", Generic> selector{payload.view()};
        CHECK(selector.value().schema() == payload.schema());
        CHECK(selector.value().data() == payload.view().data());
        ResolutionMap bindings;
        CHECK(scalar_pattern_match(to_scalar_pattern<Generic>(), selector.value().schema(), bindings));
        CHECK(bindings.ts("T") == shape);
    }
}

TEST_CASE("ordinary generic held inference preserves bytes leaves and nested shapes", "[ordinary][patterns][bytes]") {
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *bytes    = scalar_descriptor<Bytes>::value_meta();
    const auto *leaf     = registry.ts(bytes);
    const auto *list     = registry.tsl(leaf, 2);
    const auto *bundle   = registry.un_named_tsb({{"value", leaf}, {"items", list}});
    const auto  pattern  = to_scalar_pattern<hgl::ordinary::Held<TsVar<"T">>>();
    for (const auto *shape : {leaf, list, bundle}) {
        ResolutionMap bindings;
        REQUIRE(scalar_pattern_match(pattern, shape->value_schema, bindings));
        CHECK(bindings.ts("T") == shape);
        CHECK(scalar_pattern_resolve(pattern, bindings) == shape->value_schema);
        CHECK(scalar_resolver<hgl::ordinary::Held<TsVar<"T">>>::resolve(bindings) == shape->value_schema);
    }
}

TEST_CASE("ordinary generic projections infer canonical Any scalar identity", "[ordinary][patterns][any]") {
    using namespace hgraph;
    const auto *value = scalar_descriptor<hgl::ordinary::Any>::value_meta();
    const auto *shape = schema_descriptor<TS<hgl::ordinary::Any>>::ts_meta();
    const auto  held  = to_scalar_pattern<hgl::ordinary::Held<TsVar<"T">>>();
    const auto  delta = to_scalar_pattern<hgl::ordinary::Delta<TsVar<"T">>>();
    for (const auto &pattern : {held, delta}) {
        ResolutionMap bindings;
        REQUIRE(scalar_pattern_match(pattern, value, bindings));
        CHECK(bindings.ts("T") == shape);
        CHECK(scalar_pattern_resolve(pattern, bindings) == value);
    }
}
