/**
 * Unit tests for hgraph::value type system
 *
 * Tests the type metadata system, type registry, and value/view classes.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/value/all.h>
#include <string>
#include <vector>

using namespace hgraph::value;

// ============================================================================
// Scalar Type Tests
// ============================================================================

TEST_CASE("ScalarTypeMeta - int type properties", "[value][scalar]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();

    REQUIRE(int_meta != nullptr);
    REQUIRE(int_meta->size == sizeof(int));
    REQUIRE(int_meta->alignment == alignof(int));
    REQUIRE(int_meta->kind == TypeKind::Scalar);
    REQUIRE(int_meta->is_trivially_copyable());
    REQUIRE(int_meta->is_buffer_compatible());
    REQUIRE(int_meta->is_hashable());
    REQUIRE(int_meta->is_comparable());
}

TEST_CASE("ScalarTypeMeta - double type properties", "[value][scalar]") {
    const TypeMeta* double_meta = scalar_type_meta<double>();

    REQUIRE(double_meta->size == sizeof(double));
    REQUIRE(double_meta->alignment == alignof(double));
    REQUIRE(double_meta->is_buffer_compatible());
}

TEST_CASE("TypedValue - creation and access", "[value][scalar]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();

    TypedValue val = TypedValue::create(int_meta);
    REQUIRE(val.valid());

    val.as<int>() = 42;
    REQUIRE(val.as<int>() == 42);
}

TEST_CASE("TypedValue - equality", "[value][scalar]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();

    TypedValue val1 = TypedValue::create(int_meta);
    TypedValue val2 = TypedValue::create(int_meta);

    val1.as<int>() = 42;
    val2.as<int>() = 42;

    REQUIRE(val1.equals(val2));

    val2.as<int>() = 99;
    REQUIRE_FALSE(val1.equals(val2));
}

TEST_CASE("TypedValue - hash", "[value][scalar]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();

    TypedValue val1 = TypedValue::create(int_meta);
    TypedValue val2 = TypedValue::create(int_meta);

    val1.as<int>() = 42;
    val2.as<int>() = 42;

    REQUIRE(val1.hash() == val2.hash());
}

// ============================================================================
// Bundle Type Tests
// ============================================================================

TEST_CASE("BundleTypeBuilder - simple bundle", "[value][bundle]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");

    REQUIRE(meta != nullptr);
    REQUIRE(meta->kind == TypeKind::Bundle);
    REQUIRE(meta->field_count() == 2);
    REQUIRE(meta->field_by_name("x") != nullptr);
    REQUIRE(meta->field_by_name("y") != nullptr);
    REQUIRE(meta->field_by_name("z") == nullptr);
}

TEST_CASE("BundleValue - field access by name", "[value][bundle]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<double>("y")
        .build();

    BundleValue bundle(meta.get());
    REQUIRE(bundle.valid());

    bundle.set<int>("x", 10);
    bundle.set<double>("y", 20.5);

    REQUIRE(bundle.get<int>("x") == 10);
    REQUIRE(bundle.get<double>("y") == 20.5);
}

TEST_CASE("BundleValue - field access by index", "[value][bundle]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("a")
        .add_field<int>("b")
        .build();

    BundleValue bundle(meta.get());
    bundle.set<int>(0, 100);
    bundle.set<int>(1, 200);

    REQUIRE(bundle.get<int>(0) == 100);
    REQUIRE(bundle.get<int>(1) == 200);
}

TEST_CASE("BundleValue - equality", "[value][bundle]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build();

    BundleValue b1(meta.get());
    BundleValue b2(meta.get());

    b1.set<int>("x", 10);
    b1.set<int>("y", 20);
    b2.set<int>("x", 10);
    b2.set<int>("y", 20);

    REQUIRE(b1.equals(b2));
}

TEST_CASE("BundleValue - nested bundles", "[value][bundle]") {
    auto inner_meta = BundleTypeBuilder()
        .add_field<int>("a")
        .add_field<int>("b")
        .build("Inner");

    auto outer_meta = BundleTypeBuilder()
        .add_field<double>("value")
        .add_field("inner", inner_meta.get())
        .build("Outer");

    BundleValue outer(outer_meta.get());
    outer.set<double>("value", 3.14);

    auto inner_ptr = outer.field("inner");
    REQUIRE(inner_ptr.valid());

    auto inner_bundle_meta = static_cast<const BundleTypeMeta*>(inner_ptr.meta);
    inner_bundle_meta->field_ptr(inner_ptr.ptr, "a").as<int>() = 100;
    inner_bundle_meta->field_ptr(inner_ptr.ptr, "b").as<int>() = 200;

    REQUIRE(inner_bundle_meta->field_ptr(inner_ptr.ptr, "a").as<int>() == 100);
    REQUIRE(inner_bundle_meta->field_ptr(inner_ptr.ptr, "b").as<int>() == 200);
}

// ============================================================================
// List Type Tests
// ============================================================================

TEST_CASE("ListTypeBuilder - basic list", "[value][list]") {
    auto meta = ListTypeBuilder()
        .element<double>()
        .count(5)
        .build("DoubleList5");

    REQUIRE(meta != nullptr);
    REQUIRE(meta->kind == TypeKind::List);
    REQUIRE(meta->count == 5);
    REQUIRE(meta->element_type == scalar_type_meta<double>());
    REQUIRE(meta->size == sizeof(double) * 5);
    REQUIRE(meta->is_buffer_compatible());
}

TEST_CASE("ListView - element access", "[value][list]") {
    auto meta = ListTypeBuilder()
        .element<int>()
        .count(3)
        .build();

    ListView list(meta.get());
    REQUIRE(list.valid());
    REQUIRE(list.size() == 3);

    list.set<int>(0, 10);
    list.set<int>(1, 20);
    list.set<int>(2, 30);

    REQUIRE(list.get<int>(0) == 10);
    REQUIRE(list.get<int>(1) == 20);
    REQUIRE(list.get<int>(2) == 30);
}

TEST_CASE("ListView - buffer info", "[value][list]") {
    auto meta = ListTypeBuilder()
        .element<double>()
        .count(10)
        .build();

    ListView list(meta.get());
    auto buf = list.buffer_info();

    REQUIRE(buf.ptr != nullptr);
    REQUIRE(buf.itemsize == sizeof(double));
    REQUIRE(buf.count == 10);
}

TEST_CASE("ListTypeBuilder - list of bundles", "[value][list]") {
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build();

    auto list_meta = ListTypeBuilder()
        .element_type(point_meta.get())
        .count(3)
        .build();

    REQUIRE(list_meta->count == 3);

    ListView list(list_meta.get());
    auto elem = list.at(0);
    REQUIRE(elem.valid());
}

// ============================================================================
// Set Type Tests
// ============================================================================

TEST_CASE("SetTypeBuilder - basic set", "[value][set]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build("IntSet");

    REQUIRE(meta != nullptr);
    REQUIRE(meta->kind == TypeKind::Set);
    REQUIRE(meta->is_hashable());
}

TEST_CASE("SetView - add and contains", "[value][set]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    REQUIRE(set.valid());
    REQUIRE(set.empty());

    REQUIRE(set.add(10));
    REQUIRE(set.add(20));
    REQUIRE(set.add(30));
    REQUIRE_FALSE(set.add(10));  // Duplicate

    REQUIRE(set.size() == 3);
    REQUIRE(set.contains(10));
    REQUIRE(set.contains(20));
    REQUIRE(set.contains(30));
    REQUIRE_FALSE(set.contains(99));
}

TEST_CASE("SetView - remove", "[value][set]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    set.add(10);
    set.add(20);

    REQUIRE(set.remove(10));
    REQUIRE(set.size() == 1);
    REQUIRE_FALSE(set.contains(10));
    REQUIRE(set.contains(20));
}

// ============================================================================
// Dict Type Tests
// ============================================================================

TEST_CASE("DictTypeBuilder - basic dict", "[value][dict]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build("IntDoubleDict");

    REQUIRE(meta != nullptr);
    REQUIRE(meta->kind == TypeKind::Dict);
}

TEST_CASE("DictView - insert and get", "[value][dict]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    REQUIRE(dict.valid());
    REQUIRE(dict.empty());

    dict.insert(1, 1.1);
    dict.insert(2, 2.2);
    dict.insert(3, 3.3);

    REQUIRE(dict.size() == 3);
    REQUIRE(dict.contains(1));
    REQUIRE(dict.contains(2));
    REQUIRE(dict.contains(3));
    REQUIRE_FALSE(dict.contains(99));

    double* v1 = dict.get<int, double>(1);
    double* v2 = dict.get<int, double>(2);
    REQUIRE(v1 != nullptr);
    REQUIRE(v2 != nullptr);
    REQUIRE(*v1 == 1.1);
    REQUIRE(*v2 == 2.2);
}

TEST_CASE("DictView - update value", "[value][dict]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.0);
    dict.insert(1, 100.0);  // Update

    REQUIRE(dict.size() == 1);
    REQUIRE(*dict.get<int, double>(1) == 100.0);
}

TEST_CASE("DictView - remove", "[value][dict]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.0);
    dict.insert(2, 2.0);

    REQUIRE(dict.remove(1));
    REQUIRE(dict.size() == 1);
    REQUIRE_FALSE(dict.contains(1));
}

// ============================================================================
// Type Registry Tests
// ============================================================================

TEST_CASE("TypeRegistry - builtin scalars", "[value][registry]") {
    TypeRegistry registry;

    REQUIRE(registry.get("int") != nullptr);
    REQUIRE(registry.get("double") != nullptr);
    REQUIRE(registry.get("bool") != nullptr);
    REQUIRE(registry.get("int64") != nullptr);
    REQUIRE(registry.get("float32") != nullptr);
}

TEST_CASE("TypeRegistry - register custom type", "[value][registry]") {
    TypeRegistry registry;

    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");

    const BundleTypeMeta* registered = registry.register_type("Point", std::move(point_meta));

    REQUIRE(registered != nullptr);
    REQUIRE(registry.contains("Point"));
    REQUIRE(registry.get("Point") == registered);
}

TEST_CASE("TypeRegistry - require throws on missing", "[value][registry]") {
    TypeRegistry registry;

    REQUIRE_THROWS_AS(registry.require("NonExistent"), std::runtime_error);
}

TEST_CASE("TypeRegistry - duplicate registration throws", "[value][registry]") {
    TypeRegistry registry;

    auto meta1 = BundleTypeBuilder().add_field<int>("x").build();
    auto meta2 = BundleTypeBuilder().add_field<int>("y").build();

    registry.register_type("Test", std::move(meta1));
    REQUIRE_THROWS_AS(registry.register_type("Test", std::move(meta2)), std::runtime_error);
}

TEST_CASE("TypeRegistry - type_names", "[value][registry]") {
    TypeRegistry registry;

    registry.register_type("Custom1",
        BundleTypeBuilder().add_field<int>("a").build());
    registry.register_type("Custom2",
        BundleTypeBuilder().add_field<int>("b").build());

    auto names = registry.type_names();
    REQUIRE(names.size() > 2);  // Includes builtins

    // Check our custom types are present
    bool has_custom1 = false, has_custom2 = false;
    for (const auto& name : names) {
        if (name == "Custom1") has_custom1 = true;
        if (name == "Custom2") has_custom2 = true;
    }
    REQUIRE(has_custom1);
    REQUIRE(has_custom2);
}

// ============================================================================
// Value and ValueView Tests
// ============================================================================

TEST_CASE("Value - creation and access", "[value][value]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();

    Value val(int_meta);
    REQUIRE(val.valid());
    REQUIRE(val.schema() == int_meta);

    val.as<int>() = 42;
    REQUIRE(val.as<int>() == 42);
}

TEST_CASE("Value - type checking", "[value][value]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();
    const TypeMeta* double_meta = scalar_type_meta<double>();

    Value val(int_meta);
    REQUIRE(val.is_type(int_meta));
    REQUIRE_FALSE(val.is_type(double_meta));
}

TEST_CASE("Value - same_type_as", "[value][value]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();
    const TypeMeta* double_meta = scalar_type_meta<double>();

    Value int_val(int_meta);
    Value int_val2(int_meta);
    Value double_val(double_meta);

    REQUIRE(int_val.same_type_as(int_val2));
    REQUIRE_FALSE(int_val.same_type_as(double_val));
}

TEST_CASE("ValueView - type information preserved", "[value][value]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<double>("y")
        .build();

    Value val(meta.get());
    ValueView view = val.view();

    REQUIRE(view.valid());
    REQUIRE(view.schema() == meta.get());
    REQUIRE(view.is_bundle());
    REQUIRE(view.field_count() == 2);
}

TEST_CASE("ValueView - field navigation preserves type", "[value][value]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<double>("y")
        .build();

    Value val(meta.get());
    ValueView view = val.view();

    ValueView x_view = view.field("x");
    REQUIRE(x_view.valid());
    REQUIRE(x_view.is_scalar());
    REQUIRE(x_view.is_type(scalar_type_meta<int>()));

    ValueView y_view = view.field("y");
    REQUIRE(y_view.is_type(scalar_type_meta<double>()));
}

TEST_CASE("ConstValueView - try_as type safety", "[value][value]") {
    const TypeMeta* int_meta = scalar_type_meta<int>();

    Value val(int_meta);
    val.as<int>() = 42;

    ConstValueView view = val.const_view();

    const int* correct = view.try_as<int>();
    REQUIRE(correct != nullptr);
    REQUIRE(*correct == 42);

    const double* wrong = view.try_as<double>();
    REQUIRE(wrong == nullptr);
}

TEST_CASE("Value - copy", "[value][value]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build();

    Value original(meta.get());
    original.view().field("x").as<int>() = 10;
    original.view().field("y").as<int>() = 20;

    Value copy = Value::copy(original);

    REQUIRE(copy.valid());
    REQUIRE(copy.same_type_as(original));
    REQUIRE(copy.equals(original));

    // Modify copy, original unchanged
    copy.view().field("x").as<int>() = 99;
    REQUIRE_FALSE(copy.equals(original));
}

TEST_CASE("make_scalar helper", "[value][value]") {
    Value val = make_scalar(42);

    REQUIRE(val.valid());
    REQUIRE(val.is_type(scalar_type_meta<int>()));
    REQUIRE(val.as<int>() == 42);
}

// ============================================================================
// Complex Nested Type Tests
// ============================================================================

TEST_CASE("Complex nested - Canvas with Rectangles", "[value][complex]") {
    TypeRegistry registry;

    // Point { x: int, y: int }
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");
    const BundleTypeMeta* point_type = registry.register_type("Point", std::move(point_meta));

    // Rectangle { top_left: Point, bottom_right: Point }
    auto rect_meta = BundleTypeBuilder()
        .add_field("top_left", point_type)
        .add_field("bottom_right", point_type)
        .build("Rectangle");
    const BundleTypeMeta* rect_type = registry.register_type("Rectangle", std::move(rect_meta));

    // RectangleList3 - array of 3 rectangles
    auto rect_list_meta = ListTypeBuilder()
        .element_type(rect_type)
        .count(3)
        .build("RectangleList3");
    const ListTypeMeta* rect_list_type = registry.register_type("RectangleList3", std::move(rect_list_meta));

    // Canvas { id: int, rectangles: RectangleList3 }
    auto canvas_meta = BundleTypeBuilder()
        .add_field<int>("id")
        .add_field("rectangles", rect_list_type)
        .build("Canvas");
    const BundleTypeMeta* canvas_type = registry.register_type("Canvas", std::move(canvas_meta));

    // Create and populate
    Value canvas(canvas_type);
    ValueView cv = canvas.view();

    cv.field("id").as<int>() = 42;

    ValueView rects = cv.field("rectangles");
    REQUIRE(rects.is_list());
    REQUIRE(rects.list_size() == 3);

    ValueView rect0 = rects.element(0);
    REQUIRE(rect0.is_bundle());

    rect0.field("top_left").field("x").as<int>() = 0;
    rect0.field("top_left").field("y").as<int>() = 0;
    rect0.field("bottom_right").field("x").as<int>() = 100;
    rect0.field("bottom_right").field("y").as<int>() = 50;

    // Verify via const view
    ConstValueView ccv = canvas.const_view();
    REQUIRE(ccv.field("id").as<int>() == 42);
    REQUIRE(ccv.field("rectangles").element(0).field("top_left").field("x").as<int>() == 0);
    REQUIRE(ccv.field("rectangles").element(0).field("bottom_right").field("x").as<int>() == 100);

    // Type checking at all levels
    REQUIRE(ccv.is_type(canvas_type));
    REQUIRE(ccv.field("rectangles").is_type(rect_list_type));
    REQUIRE(ccv.field("rectangles").element(0).is_type(rect_type));
    REQUIRE(ccv.field("rectangles").element(0).field("top_left").is_type(point_type));
}

// ============================================================================
// Type Flags Propagation Tests
// ============================================================================

TEST_CASE("TypeFlags - trivial bundle", "[value][flags]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("a")
        .add_field<double>("b")
        .build();

    REQUIRE(meta->is_trivially_copyable());
    REQUIRE(meta->is_buffer_compatible());
    REQUIRE(meta->is_hashable());
}

TEST_CASE("TypeFlags - list of trivial", "[value][flags]") {
    auto meta = ListTypeBuilder()
        .element<int>()
        .count(10)
        .build();

    REQUIRE(meta->is_buffer_compatible());
    REQUIRE(meta->is_trivially_copyable());
}

// ============================================================================
// Type Safety Tests (as<T> validation)
// ============================================================================

TEST_CASE("is_scalar_type - correct type", "[value][typesafe]") {
    Value val(scalar_type_meta<int>());
    val.as<int>() = 42;

    REQUIRE(val.is_scalar_type<int>());
    REQUIRE_FALSE(val.is_scalar_type<double>());
    REQUIRE_FALSE(val.is_scalar_type<float>());
}

TEST_CASE("try_as - returns value on match", "[value][typesafe]") {
    Value val(scalar_type_meta<double>());
    val.as<double>() = 3.14;

    double* ptr = val.try_as<double>();
    REQUIRE(ptr != nullptr);
    REQUIRE(*ptr == 3.14);
}

TEST_CASE("try_as - returns nullptr on mismatch", "[value][typesafe]") {
    Value val(scalar_type_meta<int>());
    val.as<int>() = 42;

    double* wrong = val.try_as<double>();
    REQUIRE(wrong == nullptr);

    float* also_wrong = val.try_as<float>();
    REQUIRE(also_wrong == nullptr);
}

TEST_CASE("checked_as - returns value on match", "[value][typesafe]") {
    Value val(scalar_type_meta<int>());
    val.as<int>() = 100;

    REQUIRE_NOTHROW(val.checked_as<int>());
    REQUIRE(val.checked_as<int>() == 100);
}

TEST_CASE("checked_as - throws on mismatch", "[value][typesafe]") {
    Value val(scalar_type_meta<int>());

    REQUIRE_THROWS_AS(val.checked_as<double>(), std::runtime_error);
}

TEST_CASE("checked_as - throws on invalid", "[value][typesafe]") {
    Value empty;  // Invalid/empty value

    REQUIRE_THROWS_AS(empty.checked_as<int>(), std::runtime_error);
}

TEST_CASE("ValueView type safety", "[value][typesafe]") {
    auto meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<double>("y")
        .build();

    Value bundle(meta.get());
    ValueView bv = bundle.view();

    // Navigate to int field
    ValueView x_view = bv.field("x");
    REQUIRE(x_view.is_scalar_type<int>());
    REQUIRE_FALSE(x_view.is_scalar_type<double>());

    // try_as works
    int* x_ptr = x_view.try_as<int>();
    REQUIRE(x_ptr != nullptr);
    *x_ptr = 42;

    // checked_as works
    REQUIRE(x_view.checked_as<int>() == 42);
    REQUIRE_THROWS_AS(x_view.checked_as<double>(), std::runtime_error);

    // Navigate to double field
    ValueView y_view = bv.field("y");
    REQUIRE(y_view.is_scalar_type<double>());

    double* y_ptr = y_view.try_as<double>();
    REQUIRE(y_ptr != nullptr);
    *y_ptr = 3.14;

    REQUIRE(y_view.checked_as<double>() == 3.14);
}

TEST_CASE("ConstValueView type safety", "[value][typesafe]") {
    Value val(scalar_type_meta<int>());
    val.as<int>() = 99;

    ConstValueView cv = val.const_view();

    // is_scalar_type
    REQUIRE(cv.is_scalar_type<int>());
    REQUIRE_FALSE(cv.is_scalar_type<double>());

    // try_as
    const int* ptr = cv.try_as<int>();
    REQUIRE(ptr != nullptr);
    REQUIRE(*ptr == 99);

    const double* wrong = cv.try_as<double>();
    REQUIRE(wrong == nullptr);

    // checked_as
    REQUIRE(cv.checked_as<int>() == 99);
    REQUIRE_THROWS_AS(cv.checked_as<double>(), std::runtime_error);
}
