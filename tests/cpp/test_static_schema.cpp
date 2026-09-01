// Tests for the compile-time static-schema vocabulary and the descriptor
// traits that bridge it to the runtime ``TypeRegistry``. The descriptors
// must produce the same canonical metadata pointers as direct registry
// factory calls, and must report ``is_concrete()`` correctly for schemas
// that contain unresolved type variables.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <string>

TEST_CASE("static_schema: scalar_descriptor maps built-ins to standard registry names")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    REQUIRE(scalar_descriptor<std::int32_t>::is_concrete());
    REQUIRE(scalar_descriptor<std::int32_t>::value_meta() == registry.register_scalar<std::int32_t>("int32"));
    REQUIRE(scalar_descriptor<Int>::value_meta() == registry.value_type("int"));
    REQUIRE(std::string{scalar_descriptor<Int>::value_meta()->name()} == "int");
    REQUIRE(scalar_descriptor<double>::value_meta() == registry.value_type("float"));
    REQUIRE(scalar_descriptor<double>::value_meta() == registry.register_scalar<double>("double"));
    REQUIRE(scalar_descriptor<Str>::value_meta() == registry.value_type("str"));
    REQUIRE(std::string{scalar_descriptor<Str>::value_meta()->name()} == "str");
    REQUIRE(scalar_descriptor<bool>::value_meta() == registry.register_scalar<bool>("bool"));
}

TEST_CASE("static_schema: scalar_descriptor maps value containers to registry schemas")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *int_meta = registry.value_type("int");
    const auto *str_meta = registry.value_type("str");

    REQUIRE_FALSE(scalar_descriptor<UnknownTuple<>>::is_concrete());
    REQUIRE(scalar_descriptor<UnknownTuple<>>::value_meta() == nullptr);

    REQUIRE(scalar_descriptor<HomogeneousTuple<Int>>::is_concrete());
    REQUIRE(scalar_descriptor<HomogeneousTuple<Int>>::value_meta() ==
            registry.list(int_meta, 0, true));

    REQUIRE(scalar_descriptor<Tuple<Int, Str>>::is_concrete());
    REQUIRE(scalar_descriptor<Tuple<Int, Str>>::value_meta() ==
            registry.tuple({int_meta, str_meta}));

    REQUIRE(scalar_descriptor<Set<Int>>::is_concrete());
    REQUIRE(scalar_descriptor<Set<Int>>::value_meta() == registry.set(int_meta));

    REQUIRE(scalar_descriptor<Map<Str, Int>>::is_concrete());
    REQUIRE(scalar_descriptor<Map<Str, Int>>::value_meta() == registry.map(str_meta, int_meta));

    using OwnedPoint = Owned<Bundle<"OwnedPoint", Field<"value", Int>>>;
    const auto *owned_point = scalar_descriptor<OwnedPoint>::value_meta();
    REQUIRE(owned_point->is_owned());
    REQUIRE_FALSE(owned_point->is_un_named_bundle());
    REQUIRE(owned_point->element_type ==
            scalar_descriptor<Bundle<"OwnedPoint", Field<"value", Int>>>::value_meta());

    using SharedPoint = Shared<Bundle<"SharedPoint", Field<"value", Int>>>;
    const auto *shared_point = scalar_descriptor<SharedPoint>::value_meta();
    REQUIRE(shared_point->is_shared());
    REQUIRE_FALSE(shared_point->is_un_named_bundle());
    REQUIRE(shared_point->element_type ==
            scalar_descriptor<Bundle<"SharedPoint", Field<"value", Int>>>::value_meta());

    REQUIRE_FALSE(scalar_descriptor<Map<Str, ScalarVar<"V">>>::is_concrete());
    REQUIRE(scalar_descriptor<Map<Str, ScalarVar<"V">>>::value_meta() == nullptr);
}

TEST_CASE("static_schema: TS<T> descriptor matches registry.ts(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *expected = registry.ts(registry.register_scalar<std::int32_t>("int32"));
    REQUIRE(schema_descriptor<TS<std::int32_t>>::is_concrete());
    REQUIRE(schema_descriptor<TS<std::int32_t>>::ts_meta() == expected);
}

TEST_CASE("static_schema: TSS<T> descriptor matches registry.tss(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *expected = registry.tss(registry.value_type("str"));
    REQUIRE(schema_descriptor<TSS<std::string>>::is_concrete());
    REQUIRE(schema_descriptor<TSS<std::string>>::ts_meta() == expected);
}

TEST_CASE("static_schema: TSD<K, V> descriptor matches registry.tsd(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *str_meta = registry.value_type("str");
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *expected = registry.tsd(str_meta, registry.ts(int_meta));

    using DictSchema = TSD<Str, TS<std::int32_t>>;
    REQUIRE(schema_descriptor<DictSchema>::is_concrete());
    REQUIRE(schema_descriptor<DictSchema>::ts_meta() == expected);
}

TEST_CASE("static_schema: TSL<T, N> descriptor matches registry.tsl(...) for fixed and dynamic forms")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *float_meta = registry.value_type("float");
    const auto *ts_float   = registry.ts(float_meta);

    REQUIRE(schema_descriptor<TSL<TS<Float>, 4>>::ts_meta() == registry.tsl(ts_float, 4));
    REQUIRE(schema_descriptor<TSL<TS<Float>>>::ts_meta() == registry.tsl(ts_float, 0));
    REQUIRE_FALSE(schema_descriptor<TSL<TS<Float>, SIZE<"N">>>::is_concrete());
    REQUIRE(schema_descriptor<TSL<TS<Float>, SIZE<"N">>>::ts_meta() == nullptr);
}

TEST_CASE("static_schema: TSW<T, period, min_period> descriptor matches registry.tsw(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *float_meta = registry.value_type("float");
    REQUIRE(schema_descriptor<TSW<Float, 10, 3>>::ts_meta() == registry.tsw(float_meta, 10, 3));
    STATIC_REQUIRE(std::same_as<TSW<Float, 5>, TSW<Float, 5, 5>>);
    REQUIRE(schema_descriptor<TSW<Float, 5>>::ts_meta() == registry.tsw(float_meta, 5, 5));
    REQUIRE_FALSE(schema_descriptor<TSWAny<ScalarVar<"T">>>::is_concrete());
    REQUIRE(schema_descriptor<TSWAny<ScalarVar<"T">>>::ts_meta() == nullptr);
}

TEST_CASE("static_schema: REF<T> descriptor matches registry.ref(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    REQUIRE(schema_descriptor<REF<TS<std::int32_t>>>::ts_meta() == registry.ref(ts_int));
}

TEST_CASE("static_schema: SIGNAL descriptor matches registry.signal()")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    REQUIRE(schema_descriptor<SIGNAL>::is_concrete());
    REQUIRE(schema_descriptor<SIGNAL>::ts_meta() == registry.signal());
}

TEST_CASE("static_schema: UnNamedTSB resolves to registry.un_named_tsb(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *float_meta = registry.value_type("float");
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_float   = registry.ts(float_meta);
    const auto *ts_int      = registry.ts(int_meta);

    using BundleSchema = UnNamedTSB<Field<"price", TS<Float>>, Field<"size", TS<std::int32_t>>>;
    const auto *got = schema_descriptor<BundleSchema>::ts_meta();

    REQUIRE(got != nullptr);
    REQUIRE(got->is_un_named_tsb());
    REQUIRE(got == registry.un_named_tsb({{"price", ts_float}, {"size", ts_int}}));
}

TEST_CASE("static_schema: TSB<\"Name\", ...> resolves to registry.tsb(name, ...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *float_meta = registry.value_type("float");
    const auto *int_meta   = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_float   = registry.ts(float_meta);
    const auto *ts_int      = registry.ts(int_meta);

    using NamedBundle = TSB<"PriceTick", Field<"price", TS<Float>>, Field<"size", TS<std::int32_t>>>;
    const auto *got = schema_descriptor<NamedBundle>::ts_meta();

    REQUIRE(got != nullptr);
    REQUIRE(got->is_named_tsb());
    REQUIRE(std::string(got->name()) == std::string("PriceTick"));
    REQUIRE(got == registry.tsb("PriceTick", {{"price", ts_float}, {"size", ts_int}}));
}

TEST_CASE("static_schema: UnNamedBundle (value layer) resolves to registry.un_named_bundle(...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.value_type("str");

    using PointSchema = UnNamedBundle<Field<"x", std::int32_t>, Field<"label", Str>>;
    const auto *got = value_schema_descriptor<PointSchema>::value_meta();

    REQUIRE(got != nullptr);
    REQUIRE(got->is_un_named_bundle());
    REQUIRE(got == registry.un_named_bundle({{"x", int_meta}, {"label", str_meta}}));
}

TEST_CASE("static_schema: Bundle<\"Name\", ...> resolves to registry.bundle(name, ...)")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.value_type("str");

    using LabelledPoint = Bundle<"LabelledPoint", Field<"x", std::int32_t>, Field<"label", Str>>;
    const auto *got = value_schema_descriptor<LabelledPoint>::value_meta();

    REQUIRE(got != nullptr);
    REQUIRE(got->is_named_bundle());
    REQUIRE(std::string(got->name()) == std::string("LabelledPoint"));
    REQUIRE(got == registry.bundle("LabelledPoint", {{"x", int_meta}, {"label", str_meta}}));
}

TEST_CASE("static_schema: TSBFromScalar lifts named and structural Bundle fields")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    using Child = Bundle<"LiftedChild", Field<"label", Str>>;
    using Model = Bundle<"LiftedModel",
                         Field<"number", Int>,
                         Field<"items", HomogeneousTuple<Int>>,
                         Field<"child", Child>>;
    using Lifted = TSBFromScalar<Model>;
    using Expected = TSB<"LiftedModel",
                         Field<"number", TS<Int>>,
                         Field<"items", TS<HomogeneousTuple<Int>>>,
                         Field<"child", TS<Child>>>;
    STATIC_REQUIRE(std::is_same_v<Lifted, Expected>);

    using Structural = UnNamedBundle<Field<"number", Int>, Field<"label", Str>>;
    using StructuralLifted = TSBFromScalar<Structural>;
    using StructuralExpected =
        UnNamedTSB<Field<"number", TS<Int>>, Field<"label", TS<Str>>>;
    STATIC_REQUIRE(std::is_same_v<StructuralLifted, StructuralExpected>);

    const auto *model = value_schema_descriptor<Model>::value_meta();
    REQUIRE(schema_descriptor<Lifted>::ts_meta() == registry.tsb(model));
}

TEST_CASE("static_schema: TsVar / ScalarVar render schemas non-concrete")
{
    using namespace hgraph;

    REQUIRE_FALSE(scalar_descriptor<ScalarVar<"T">>::is_concrete());
    REQUIRE(scalar_descriptor<ScalarVar<"T">>::value_meta() == nullptr);

    REQUIRE_FALSE(schema_descriptor<TsVar<"X">>::is_concrete());
    REQUIRE(schema_descriptor<TsVar<"X">>::ts_meta() == nullptr);

    // A composite carrying an unresolved variable is also non-concrete.
    using GenericDict = TSD<ScalarVar<"K">, TS<std::int32_t>>;
    REQUIRE_FALSE(schema_descriptor<GenericDict>::is_concrete());
    REQUIRE(schema_descriptor<GenericDict>::ts_meta() == nullptr);

    using GenericTSB = UnNamedTSB<Field<"a", TsVar<"X">>>;
    REQUIRE_FALSE(schema_descriptor<GenericTSB>::is_concrete());
    REQUIRE(schema_descriptor<GenericTSB>::ts_meta() == nullptr);
}

TEST_CASE("static_schema: nested compositions resolve recursively")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    // TSD<Str, TSL<TS<Float>, 4>> — keyed by str, value is a list of TS[float].
    using NestedSchema = TSD<Str, TSL<TS<Float>, 4>>;

    const auto *str_meta   = registry.value_type("str");
    const auto *float_meta = registry.value_type("float");
    const auto *ts_float   = registry.ts(float_meta);
    const auto *expected   = registry.tsd(str_meta, registry.tsl(ts_float, 4));

    REQUIRE(schema_descriptor<NestedSchema>::is_concrete());
    REQUIRE(schema_descriptor<NestedSchema>::ts_meta() == expected);
}

TEST_CASE("static_schema: shaped arrays retain rank and dimensions")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *matrix = scalar_descriptor<ArrayOf<Int, 3, 2>>::value_meta();
    REQUIRE(matrix != nullptr);
    CHECK(TypeRegistry::is_array(matrix));
    CHECK(TypeRegistry::array_element(matrix) == integer);
    CHECK(TypeRegistry::array_dimensions(matrix) == std::vector<std::size_t>{3, 2});
    CHECK(matrix == registry.array(integer, std::vector<std::size_t>{3, 2}));

    const auto *dynamic = scalar_descriptor<ArrayOf<Int>>::value_meta();
    REQUIRE(dynamic != nullptr);
    CHECK(TypeRegistry::array_dimensions(dynamic) == std::vector<std::size_t>{0});
    CHECK(dynamic != registry.list(integer, 0, true));
}
