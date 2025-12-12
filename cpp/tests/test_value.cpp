/**
 * Unit tests for hgraph::value type system
 *
 * Tests the type metadata system, type registry, and value/view classes.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/value/all.h>
#include <string>
#include <vector>
#include <algorithm>

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

// ============================================================================
// Set Iteration Tests
// ============================================================================

TEST_CASE("SetStorage - iteration", "[value][set][iteration]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    set.add(10);
    set.add(20);
    set.add(30);

    // Collect all elements via iteration
    std::vector<int> elements;
    for (auto elem : *set.storage()) {
        elements.push_back(*static_cast<const int*>(elem.ptr));
    }

    REQUIRE(elements.size() == 3);
    // Check all elements present (order not guaranteed)
    std::sort(elements.begin(), elements.end());
    REQUIRE(elements[0] == 10);
    REQUIRE(elements[1] == 20);
    REQUIRE(elements[2] == 30);
}

TEST_CASE("SetStorage - iteration after removal", "[value][set][iteration]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    set.add(1);
    set.add(2);
    set.add(3);
    set.add(4);
    set.add(5);

    // Remove some elements
    set.remove(2);
    set.remove(4);

    // Iteration should only yield remaining elements
    std::vector<int> elements;
    for (auto elem : *set.storage()) {
        elements.push_back(*static_cast<const int*>(elem.ptr));
    }

    REQUIRE(elements.size() == 3);
    std::sort(elements.begin(), elements.end());
    REQUIRE(elements[0] == 1);
    REQUIRE(elements[1] == 3);
    REQUIRE(elements[2] == 5);
}

TEST_CASE("SetStorage - empty iteration", "[value][set][iteration]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());

    int count = 0;
    for (auto elem : *set.storage()) {
        (void)elem;
        ++count;
    }
    REQUIRE(count == 0);
}

// ============================================================================
// Set Equality and Hash Tests
// ============================================================================

TEST_CASE("SetStorage - equality", "[value][set][equality]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set1(meta.get());
    SetView set2(meta.get());

    // Empty sets are equal
    REQUIRE(SetTypeOps::equals(set1.storage(), set2.storage(), meta.get()));

    // Same elements, same order
    set1.add(1);
    set1.add(2);
    set1.add(3);

    set2.add(1);
    set2.add(2);
    set2.add(3);

    REQUIRE(SetTypeOps::equals(set1.storage(), set2.storage(), meta.get()));
}

TEST_CASE("SetStorage - equality different order", "[value][set][equality]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set1(meta.get());
    SetView set2(meta.get());

    // Different insertion order, same elements
    set1.add(1);
    set1.add(2);
    set1.add(3);

    set2.add(3);
    set2.add(1);
    set2.add(2);

    REQUIRE(SetTypeOps::equals(set1.storage(), set2.storage(), meta.get()));
}

TEST_CASE("SetStorage - inequality", "[value][set][equality]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set1(meta.get());
    SetView set2(meta.get());

    set1.add(1);
    set1.add(2);

    set2.add(1);
    set2.add(3);

    REQUIRE_FALSE(SetTypeOps::equals(set1.storage(), set2.storage(), meta.get()));
}

TEST_CASE("SetStorage - hash consistency", "[value][set][hash]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set1(meta.get());
    SetView set2(meta.get());

    // Same elements should have same hash
    set1.add(10);
    set1.add(20);
    set1.add(30);

    set2.add(30);  // Different order
    set2.add(10);
    set2.add(20);

    size_t hash1 = SetTypeOps::hash(set1.storage(), meta.get());
    size_t hash2 = SetTypeOps::hash(set2.storage(), meta.get());

    REQUIRE(hash1 == hash2);
}

// ============================================================================
// Set Clear and Copy Tests
// ============================================================================

TEST_CASE("SetStorage - clear", "[value][set]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    set.add(1);
    set.add(2);
    set.add(3);

    REQUIRE(set.size() == 3);
    set.clear();
    REQUIRE(set.size() == 0);
    REQUIRE(set.empty());

    // Can add after clear
    set.add(100);
    REQUIRE(set.size() == 1);
    REQUIRE(set.contains(100));
}

TEST_CASE("SetStorage - copy via TypeOps", "[value][set][copy]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView src(meta.get());
    src.add(10);
    src.add(20);
    src.add(30);

    // Copy construct
    alignas(SetStorage) char buffer[sizeof(SetStorage)];
    SetTypeOps::copy_construct(buffer, src.storage(), meta.get());
    auto* copy = reinterpret_cast<SetStorage*>(buffer);

    REQUIRE(copy->size() == 3);
    int v10 = 10, v20 = 20, v30 = 30;
    REQUIRE(copy->contains(&v10));
    REQUIRE(copy->contains(&v20));
    REQUIRE(copy->contains(&v30));

    // Modify original doesn't affect copy
    src.add(40);
    REQUIRE(src.size() == 4);
    REQUIRE(copy->size() == 3);

    SetTypeOps::destruct(buffer, meta.get());
}

// ============================================================================
// Dict Iteration Tests
// ============================================================================

TEST_CASE("DictStorage - iteration", "[value][dict][iteration]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.1);
    dict.insert(2, 2.2);
    dict.insert(3, 3.3);

    // Collect all key-value pairs
    std::vector<std::pair<int, double>> pairs;
    for (auto kv : *dict.storage()) {
        int k = *static_cast<const int*>(kv.key.ptr);
        double v = *static_cast<const double*>(kv.value.ptr);
        pairs.emplace_back(k, v);
    }

    REQUIRE(pairs.size() == 3);

    // Sort by key for consistent checking
    std::sort(pairs.begin(), pairs.end());
    REQUIRE(pairs[0].first == 1);
    REQUIRE(pairs[0].second == 1.1);
    REQUIRE(pairs[1].first == 2);
    REQUIRE(pairs[1].second == 2.2);
    REQUIRE(pairs[2].first == 3);
    REQUIRE(pairs[2].second == 3.3);
}

TEST_CASE("DictStorage - iteration after removal", "[value][dict][iteration]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.0);
    dict.insert(2, 2.0);
    dict.insert(3, 3.0);
    dict.insert(4, 4.0);

    dict.remove(2);
    dict.remove(4);

    std::vector<int> keys;
    for (auto kv : *dict.storage()) {
        keys.push_back(*static_cast<const int*>(kv.key.ptr));
    }

    REQUIRE(keys.size() == 2);
    std::sort(keys.begin(), keys.end());
    REQUIRE(keys[0] == 1);
    REQUIRE(keys[1] == 3);
}

TEST_CASE("DictStorage - const iteration", "[value][dict][iteration]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.1);
    dict.insert(2, 2.2);

    const DictStorage* const_storage = dict.storage();

    int count = 0;
    for (auto kv : *const_storage) {
        REQUIRE(kv.key.meta == scalar_type_meta<int>());
        REQUIRE(kv.value.meta == scalar_type_meta<double>());
        ++count;
    }
    REQUIRE(count == 2);
}

// ============================================================================
// Dict Equality and Hash Tests
// ============================================================================

TEST_CASE("DictStorage - equality", "[value][dict][equality]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict1(meta.get());
    DictView dict2(meta.get());

    // Empty dicts are equal
    REQUIRE(DictTypeOps::equals(dict1.storage(), dict2.storage(), meta.get()));

    // Same key-value pairs
    dict1.insert(1, 1.1);
    dict1.insert(2, 2.2);

    dict2.insert(2, 2.2);  // Different order
    dict2.insert(1, 1.1);

    REQUIRE(DictTypeOps::equals(dict1.storage(), dict2.storage(), meta.get()));
}

TEST_CASE("DictStorage - inequality", "[value][dict][equality]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict1(meta.get());
    DictView dict2(meta.get());

    dict1.insert(1, 1.0);
    dict2.insert(1, 1.1);  // Different value

    REQUIRE_FALSE(DictTypeOps::equals(dict1.storage(), dict2.storage(), meta.get()));
}

TEST_CASE("DictStorage - hash consistency", "[value][dict][hash]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict1(meta.get());
    DictView dict2(meta.get());

    dict1.insert(1, 1.1);
    dict1.insert(2, 2.2);

    dict2.insert(2, 2.2);
    dict2.insert(1, 1.1);

    size_t hash1 = DictTypeOps::hash(dict1.storage(), meta.get());
    size_t hash2 = DictTypeOps::hash(dict2.storage(), meta.get());

    REQUIRE(hash1 == hash2);
}

// ============================================================================
// Dict Clear and Copy Tests
// ============================================================================

TEST_CASE("DictStorage - clear", "[value][dict]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.0);
    dict.insert(2, 2.0);

    REQUIRE(dict.size() == 2);
    dict.clear();
    REQUIRE(dict.size() == 0);
    REQUIRE(dict.empty());

    // Can insert after clear
    dict.insert(100, 100.0);
    REQUIRE(dict.size() == 1);
    REQUIRE(dict.contains(100));
}

TEST_CASE("DictStorage - copy via TypeOps", "[value][dict][copy]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView src(meta.get());
    src.insert(1, 1.1);
    src.insert(2, 2.2);

    alignas(DictStorage) char buffer[sizeof(DictStorage)];
    DictTypeOps::copy_construct(buffer, src.storage(), meta.get());
    auto* copy = reinterpret_cast<DictStorage*>(buffer);

    REQUIRE(copy->size() == 2);

    int key1 = 1, key2 = 2;
    REQUIRE(copy->contains(&key1));
    REQUIRE(copy->contains(&key2));
    REQUIRE(*static_cast<const double*>(copy->get(&key1)) == 1.1);

    DictTypeOps::destruct(buffer, meta.get());
}

// ============================================================================
// Composable Collection Tests
// ============================================================================

TEST_CASE("Set of bundles", "[value][set][composite]") {
    // Create Point bundle type
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");

    // Create set of Points
    auto set_meta = SetTypeBuilder()
        .element_type(point_meta.get())
        .build("PointSet");

    REQUIRE(set_meta->is_hashable());

    Value set_val(set_meta.get());
    auto* storage = static_cast<SetStorage*>(set_val.data());

    // Create points to add
    Value p1(point_meta.get());
    p1.view().field("x").as<int>() = 10;
    p1.view().field("y").as<int>() = 20;

    Value p2(point_meta.get());
    p2.view().field("x").as<int>() = 30;
    p2.view().field("y").as<int>() = 40;

    Value p3(point_meta.get());  // Same as p1
    p3.view().field("x").as<int>() = 10;
    p3.view().field("y").as<int>() = 20;

    // Add points
    REQUIRE(storage->add(p1.data()));
    REQUIRE(storage->add(p2.data()));
    REQUIRE_FALSE(storage->add(p3.data()));  // Duplicate

    REQUIRE(storage->size() == 2);
    REQUIRE(storage->contains(p1.data()));
    REQUIRE(storage->contains(p3.data()));  // Same value as p1
}

TEST_CASE("Dict with bundle values", "[value][dict][composite]") {
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");

    auto dict_meta = DictTypeBuilder()
        .key<int>()
        .value_type(point_meta.get())
        .build("IntToPointDict");

    Value dict_val(dict_meta.get());
    auto* storage = static_cast<DictStorage*>(dict_val.data());

    // Create point value
    Value p1(point_meta.get());
    p1.view().field("x").as<int>() = 100;
    p1.view().field("y").as<int>() = 200;

    // Insert
    int key = 1;
    storage->insert(&key, p1.data());

    REQUIRE(storage->size() == 1);
    REQUIRE(storage->contains(&key));

    // Get and verify
    const void* val_ptr = storage->get(&key);
    REQUIRE(val_ptr != nullptr);

    auto* point_bundle_meta = static_cast<const BundleTypeMeta*>(point_meta.get());
    int x = point_bundle_meta->field_ptr(val_ptr, "x").as<int>();
    int y = point_bundle_meta->field_ptr(val_ptr, "y").as<int>();
    REQUIRE(x == 100);
    REQUIRE(y == 200);
}

TEST_CASE("Dict with bundle keys", "[value][dict][composite]") {
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");

    auto dict_meta = DictTypeBuilder()
        .key_type(point_meta.get())
        .value<double>()
        .build("PointToDoubleDict");

    Value dict_val(dict_meta.get());
    auto* storage = static_cast<DictStorage*>(dict_val.data());

    // Create point keys
    Value p1(point_meta.get());
    p1.view().field("x").as<int>() = 1;
    p1.view().field("y").as<int>() = 2;

    Value p2(point_meta.get());
    p2.view().field("x").as<int>() = 3;
    p2.view().field("y").as<int>() = 4;

    double v1 = 1.1, v2 = 2.2;
    storage->insert(p1.data(), &v1);
    storage->insert(p2.data(), &v2);

    REQUIRE(storage->size() == 2);
    REQUIRE(storage->contains(p1.data()));
    REQUIRE(storage->contains(p2.data()));

    REQUIRE(*static_cast<const double*>(storage->get(p1.data())) == 1.1);
    REQUIRE(*static_cast<const double*>(storage->get(p2.data())) == 2.2);
}

// ============================================================================
// Move Semantics Tests
// ============================================================================

TEST_CASE("SetStorage - move constructor", "[value][set][move]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetStorage src(meta->element_type);
    int vals[] = {10, 20, 30};
    src.add(&vals[0]);
    src.add(&vals[1]);
    src.add(&vals[2]);

    SetStorage dest(std::move(src));

    // dest has the data
    REQUIRE(dest.size() == 3);
    REQUIRE(dest.contains(&vals[0]));
    REQUIRE(dest.contains(&vals[1]));
    REQUIRE(dest.contains(&vals[2]));

    // src is empty
    REQUIRE(src.size() == 0);
    REQUIRE(src.element_type() == nullptr);
}

TEST_CASE("SetStorage - move assignment", "[value][set][move]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetStorage src(meta->element_type);
    int vals[] = {10, 20, 30};
    src.add(&vals[0]);
    src.add(&vals[1]);
    src.add(&vals[2]);

    SetStorage dest(meta->element_type);
    int other = 99;
    dest.add(&other);

    dest = std::move(src);

    REQUIRE(dest.size() == 3);
    REQUIRE(dest.contains(&vals[0]));
    REQUIRE_FALSE(dest.contains(&other));  // Old data gone
}

TEST_CASE("DictStorage - move constructor", "[value][dict][move]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictStorage src(meta->key_type, meta->value_type);
    int k1 = 1, k2 = 2;
    double v1 = 1.1, v2 = 2.2;
    src.insert(&k1, &v1);
    src.insert(&k2, &v2);

    DictStorage dest(std::move(src));

    REQUIRE(dest.size() == 2);
    REQUIRE(dest.contains(&k1));
    REQUIRE(*static_cast<const double*>(dest.get(&k1)) == 1.1);

    REQUIRE(src.size() == 0);
}

TEST_CASE("DictStorage - move assignment", "[value][dict][move]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictStorage src(meta->key_type, meta->value_type);
    int k = 1;
    double v = 1.1;
    src.insert(&k, &v);

    DictStorage dest(meta->key_type, meta->value_type);
    int k2 = 99;
    double v2 = 99.9;
    dest.insert(&k2, &v2);

    dest = std::move(src);

    REQUIRE(dest.size() == 1);
    REQUIRE(dest.contains(&k));
    REQUIRE_FALSE(dest.contains(&k2));
}

// ============================================================================
// Edge Case Tests
// ============================================================================

TEST_CASE("Set - single element", "[value][set][edge]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    set.add(42);

    REQUIRE(set.size() == 1);
    REQUIRE(set.contains(42));

    // Iteration works
    int count = 0;
    for (auto elem : *set.storage()) {
        REQUIRE(*static_cast<const int*>(elem.ptr) == 42);
        ++count;
    }
    REQUIRE(count == 1);

    // Remove single element
    set.remove(42);
    REQUIRE(set.empty());
}

TEST_CASE("Dict - single element", "[value][dict][edge]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.1);

    REQUIRE(dict.size() == 1);
    REQUIRE(*dict.get<int, double>(1) == 1.1);

    dict.remove(1);
    REQUIRE(dict.empty());
}

TEST_CASE("Set - remove non-existent", "[value][set][edge]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());
    set.add(1);

    REQUIRE_FALSE(set.remove(999));  // Not present
    REQUIRE(set.size() == 1);
}

TEST_CASE("Dict - get non-existent", "[value][dict][edge]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());
    dict.insert(1, 1.1);

    double* ptr = dict.get<int, double>(999);
    REQUIRE(ptr == nullptr);
}

// ============================================================================
// Stress Tests
// ============================================================================

TEST_CASE("Set - many elements", "[value][set][stress]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());

    // Add 1000 elements
    for (int i = 0; i < 1000; ++i) {
        REQUIRE(set.add(i));
    }

    REQUIRE(set.size() == 1000);

    // Verify all present
    for (int i = 0; i < 1000; ++i) {
        REQUIRE(set.contains(i));
    }

    // Remove half
    for (int i = 0; i < 1000; i += 2) {
        REQUIRE(set.remove(i));
    }

    REQUIRE(set.size() == 500);

    // Verify remaining
    for (int i = 0; i < 1000; ++i) {
        if (i % 2 == 0) {
            REQUIRE_FALSE(set.contains(i));
        } else {
            REQUIRE(set.contains(i));
        }
    }
}

TEST_CASE("Dict - many elements", "[value][dict][stress]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());

    // Add 1000 elements
    for (int i = 0; i < 1000; ++i) {
        dict.insert(i, static_cast<double>(i) * 1.5);
    }

    REQUIRE(dict.size() == 1000);

    // Verify all present
    for (int i = 0; i < 1000; ++i) {
        REQUIRE(dict.contains(i));
        REQUIRE(*dict.get<int, double>(i) == static_cast<double>(i) * 1.5);
    }

    // Update all
    for (int i = 0; i < 1000; ++i) {
        dict.insert(i, static_cast<double>(i) * 2.0);
    }

    REQUIRE(dict.size() == 1000);  // Size unchanged

    // Verify updates
    for (int i = 0; i < 1000; ++i) {
        REQUIRE(*dict.get<int, double>(i) == static_cast<double>(i) * 2.0);
    }
}

TEST_CASE("Set - iteration count matches size", "[value][set][stress]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    SetView set(meta.get());

    for (int i = 0; i < 100; ++i) {
        set.add(i);
    }

    // Remove some
    for (int i = 0; i < 100; i += 3) {
        set.remove(i);
    }

    size_t iter_count = 0;
    for (auto elem : *set.storage()) {
        (void)elem;
        ++iter_count;
    }

    REQUIRE(iter_count == set.size());
}

TEST_CASE("Dict - iteration count matches size", "[value][dict][stress]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    DictView dict(meta.get());

    for (int i = 0; i < 100; ++i) {
        dict.insert(i, static_cast<double>(i));
    }

    for (int i = 0; i < 100; i += 4) {
        dict.remove(i);
    }

    size_t iter_count = 0;
    for (auto kv : *dict.storage()) {
        (void)kv;
        ++iter_count;
    }

    REQUIRE(iter_count == dict.size());
}

// ============================================================================
// Value Integration with Set/Dict
// ============================================================================

TEST_CASE("Value - set via ValueView", "[value][set][integration]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    Value set_val(meta.get());
    ValueView sv = set_val.view();

    REQUIRE(sv.is_set());
    REQUIRE(sv.set_size() == 0);

    sv.set_add(10);
    sv.set_add(20);

    REQUIRE(sv.set_size() == 2);

    ConstValueView csv = set_val.const_view();
    REQUIRE(csv.set_contains(10));
    REQUIRE(csv.set_contains(20));
}

TEST_CASE("Value - dict via ValueView", "[value][dict][integration]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    Value dict_val(meta.get());
    ValueView dv = dict_val.view();

    REQUIRE(dv.is_dict());
    REQUIRE(dv.dict_size() == 0);

    dv.dict_insert(1, 1.1);
    dv.dict_insert(2, 2.2);

    REQUIRE(dv.dict_size() == 2);

    ConstValueView cdv = dict_val.const_view();
    REQUIRE(cdv.dict_contains(1));
    REQUIRE(cdv.dict_get(1).as<double>() == 1.1);
}

TEST_CASE("Value - copy set", "[value][set][copy]") {
    auto meta = SetTypeBuilder()
        .element<int>()
        .build();

    Value original(meta.get());
    original.view().set_add(10);
    original.view().set_add(20);
    original.view().set_add(30);

    Value copy = Value::copy(original);

    REQUIRE(copy.const_view().set_size() == 3);
    REQUIRE(copy.const_view().set_contains(10));
    REQUIRE(copy.const_view().set_contains(20));
    REQUIRE(copy.const_view().set_contains(30));

    // Modify original
    original.view().set_add(40);
    REQUIRE(original.const_view().set_size() == 4);
    REQUIRE(copy.const_view().set_size() == 3);  // Copy unchanged
}

TEST_CASE("Value - copy dict", "[value][dict][copy]") {
    auto meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build();

    Value original(meta.get());
    original.view().dict_insert(1, 1.1);
    original.view().dict_insert(2, 2.2);

    Value copy = Value::copy(original);

    REQUIRE(copy.const_view().dict_size() == 2);
    REQUIRE(copy.const_view().dict_get(1).as<double>() == 1.1);

    // Modify original
    original.view().dict_insert(1, 99.9);
    REQUIRE(original.const_view().dict_get(1).as<double>() == 99.9);
    REQUIRE(copy.const_view().dict_get(1).as<double>() == 1.1);  // Copy unchanged
}
