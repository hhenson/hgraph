/**
 * hgraph::value Type System - Comprehensive Examples
 *
 * This file demonstrates the usage of the value type system including:
 * 1. Simple scalar types
 * 2. All collection types (bundle, list, set, dict)
 * 3. Complex nested type structures
 * 4. Type checking and schema comparison
 *
 * Can be compiled standalone or used as Catch2 tests.
 */

#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/value/all.h>
#include <iostream>
#include <algorithm>

using namespace hgraph::value;

// ============================================================================
// Example 1: Simple Scalar Types
// ============================================================================

TEST_CASE("Example 1: Simple Scalar Types", "[value][examples]") {
    // TypeRegistry provides access to registered types
    // Built-in scalar types are pre-registered
    TypeRegistry registry;

    // Access scalar type metadata
    const TypeMeta* int_type = registry.get("int");
    const TypeMeta* double_type = registry.get("double");
    const TypeMeta* bool_type = registry.get("bool");

    REQUIRE(int_type != nullptr);
    REQUIRE(double_type != nullptr);
    REQUIRE(bool_type != nullptr);

    // Check type properties
    REQUIRE(int_type->size == sizeof(int));
    REQUIRE(int_type->alignment == alignof(int));
    REQUIRE(int_type->kind == TypeKind::Scalar);
    REQUIRE(int_type->is_trivially_copyable());
    REQUIRE(int_type->is_buffer_compatible());

    // Create values using the Value class (owning container)
    Value int_val(int_type);
    Value double_val(double_type);

    // Set and read values via direct access
    int_val.as<int>() = 42;
    double_val.as<double>() = 3.14159;

    REQUIRE(int_val.as<int>() == 42);
    REQUIRE(double_val.as<double>() == 3.14159);

    // Type checking - schema pointer comparison
    REQUIRE(int_val.is_type(int_type));
    REQUIRE_FALSE(int_val.is_type(double_type));

    // Create scalar values using helper
    Value quick = make_scalar(100);
    REQUIRE(quick.as<int>() == 100);
    REQUIRE(quick.is_type(scalar_type_meta<int>()));
}

// ============================================================================
// Example 2: Bundle Type (struct-like)
// ============================================================================

TEST_CASE("Example 2: Bundle Type", "[value][examples]") {
    TypeRegistry registry;

    // Define a Point type using BundleTypeBuilder
    // Similar to: struct Point { int x; int y; };
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");

    // Register the type (takes ownership)
    const BundleTypeMeta* point_type = registry.register_type("Point", std::move(point_meta));

    REQUIRE(point_type->kind == TypeKind::Bundle);
    REQUIRE(point_type->field_count() == 2);

    // Create a Point value
    Value point(point_type);
    REQUIRE(point.valid());

    // Access fields via ValueView
    ValueView pv = point.view();
    pv.field("x").as<int>() = 10;
    pv.field("y").as<int>() = 20;

    // Read via ConstValueView
    ConstValueView cpv = point.const_view();
    REQUIRE(cpv.field("x").as<int>() == 10);
    REQUIRE(cpv.field("y").as<int>() == 20);

    // Access by index
    REQUIRE(cpv.field(0).as<int>() == 10);  // x
    REQUIRE(cpv.field(1).as<int>() == 20);  // y

    // Create another point and compare
    Value point2(point_type);
    point2.view().field("x").as<int>() = 10;
    point2.view().field("y").as<int>() = 20;

    REQUIRE(point.equals(point2));
    REQUIRE(point.same_type_as(point2));
}

// ============================================================================
// Example 3: List Type (fixed-size array)
// ============================================================================

TEST_CASE("Example 3: List Type", "[value][examples]") {
    TypeRegistry registry;

    // Create a list of 5 doubles
    // Similar to: double values[5];
    auto list_meta = ListTypeBuilder()
        .element<double>()
        .count(5)
        .build("DoubleList5");

    const ListTypeMeta* list_type = registry.register_type("DoubleList5", std::move(list_meta));

    REQUIRE(list_type->kind == TypeKind::List);
    REQUIRE(list_type->count == 5);
    REQUIRE(list_type->element_type == scalar_type_meta<double>());
    REQUIRE(list_type->size == sizeof(double) * 5);
    REQUIRE(list_type->is_buffer_compatible());

    // Create a list value
    Value list(list_type);
    ValueView lv = list.view();

    // Set values
    for (size_t i = 0; i < lv.list_size(); ++i) {
        lv.element(i).as<double>() = static_cast<double>(i) * 1.5;
    }

    // Read values
    ConstValueView clv = list.const_view();
    REQUIRE(clv.element(0).as<double>() == 0.0);
    REQUIRE(clv.element(1).as<double>() == 1.5);
    REQUIRE(clv.element(2).as<double>() == 3.0);

    // Type checking on elements
    REQUIRE(clv.element(0).is_type(scalar_type_meta<double>()));
    REQUIRE(clv.element_type() == scalar_type_meta<double>());
}

// ============================================================================
// Example 4: Set Type (hash set)
// ============================================================================

TEST_CASE("Example 4: Set Type", "[value][examples]") {
    TypeRegistry registry;

    // Create a set of ints
    auto set_meta = SetTypeBuilder()
        .element<int>()
        .build("IntSet");

    const SetTypeMeta* set_type = registry.register_type("IntSet", std::move(set_meta));

    REQUIRE(set_type->kind == TypeKind::Set);
    REQUIRE(set_type->is_hashable());

    // Create a set value
    Value set(set_type);
    ValueView sv = set.view();

    // Add elements
    REQUIRE(sv.set_add(10));
    REQUIRE(sv.set_add(20));
    REQUIRE(sv.set_add(30));
    REQUIRE_FALSE(sv.set_add(10));  // Duplicate returns false

    // Check contents via const view
    ConstValueView csv = set.const_view();
    REQUIRE(csv.set_size() == 3);
    REQUIRE(csv.set_contains(10));
    REQUIRE(csv.set_contains(20));
    REQUIRE(csv.set_contains(30));
    REQUIRE_FALSE(csv.set_contains(99));

    // Remove element
    REQUIRE(sv.set_remove(20));
    REQUIRE(csv.set_size() == 2);
    REQUIRE_FALSE(csv.set_contains(20));
}

// ============================================================================
// Example 5: Dict Type (hash map)
// ============================================================================

TEST_CASE("Example 5: Dict Type", "[value][examples]") {
    TypeRegistry registry;

    // Create a dict of int -> double
    auto dict_meta = DictTypeBuilder()
        .key<int>()
        .value<double>()
        .build("IntDoubleDict");

    const DictTypeMeta* dict_type = registry.register_type("IntDoubleDict", std::move(dict_meta));

    REQUIRE(dict_type->kind == TypeKind::Dict);

    // Create a dict value
    Value dict(dict_type);
    ValueView dv = dict.view();

    // Insert key-value pairs
    dv.dict_insert(1, 1.1);
    dv.dict_insert(2, 2.2);
    dv.dict_insert(3, 3.3);

    // Check contents
    ConstValueView cdv = dict.const_view();
    REQUIRE(cdv.dict_size() == 3);
    REQUIRE(cdv.dict_contains(1));
    REQUIRE(cdv.dict_contains(2));
    REQUIRE(cdv.dict_contains(3));
    REQUIRE_FALSE(cdv.dict_contains(99));

    // Get values - returns ValueView for further type checking
    ConstValueView v1 = cdv.dict_get(1);
    REQUIRE(v1.valid());
    REQUIRE(v1.as<double>() == 1.1);
    REQUIRE(v1.is_type(scalar_type_meta<double>()));

    // Update value (re-insert with same key)
    dv.dict_insert(2, 22.22);
    REQUIRE(cdv.dict_get(2).as<double>() == 22.22);
    REQUIRE(cdv.dict_size() == 3);  // Size unchanged

    // Type checking
    REQUIRE(cdv.key_type() == scalar_type_meta<int>());
    REQUIRE(cdv.value_type() == scalar_type_meta<double>());
}

// ============================================================================
// Example 6: Complex Nested Types
// ============================================================================

TEST_CASE("Example 6: Complex Nested Types", "[value][examples]") {
    TypeRegistry registry;

    // Level 1: Point { x: int, y: int }
    auto point_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point");
    const BundleTypeMeta* point_type = registry.register_type("Point", std::move(point_meta));

    // Level 2: Rectangle { top_left: Point, bottom_right: Point }
    auto rect_meta = BundleTypeBuilder()
        .add_field("top_left", point_type)
        .add_field("bottom_right", point_type)
        .build("Rectangle");
    const BundleTypeMeta* rect_type = registry.register_type("Rectangle", std::move(rect_meta));

    // Level 3: Create a list of rectangles
    auto rect_list_meta = ListTypeBuilder()
        .element_type(rect_type)
        .count(3)
        .build("RectangleList3");
    const ListTypeMeta* rect_list_type = registry.register_type("RectangleList3", std::move(rect_list_meta));

    // Level 4: Canvas { id: int, rectangles: RectangleList3 }
    auto canvas_meta = BundleTypeBuilder()
        .add_field<int>("id")
        .add_field("rectangles", rect_list_type)
        .build("Canvas");
    const BundleTypeMeta* canvas_type = registry.register_type("Canvas", std::move(canvas_meta));

    // Create a Canvas value
    Value canvas(canvas_type);
    ValueView cv = canvas.view();

    // Set canvas id
    cv.field("id").as<int>() = 42;

    // Navigate to nested structures
    ValueView rects = cv.field("rectangles");
    REQUIRE(rects.is_list());
    REQUIRE(rects.list_size() == 3);

    // Set first rectangle
    ValueView rect0 = rects.element(0);
    REQUIRE(rect0.is_bundle());

    rect0.field("top_left").field("x").as<int>() = 0;
    rect0.field("top_left").field("y").as<int>() = 0;
    rect0.field("bottom_right").field("x").as<int>() = 100;
    rect0.field("bottom_right").field("y").as<int>() = 50;

    // Set second rectangle via chained navigation
    rects.element(1).field("top_left").field("x").as<int>() = 10;
    rects.element(1).field("top_left").field("y").as<int>() = 10;
    rects.element(1).field("bottom_right").field("x").as<int>() = 60;
    rects.element(1).field("bottom_right").field("y").as<int>() = 40;

    // Verify via const view
    ConstValueView ccv = canvas.const_view();
    REQUIRE(ccv.field("id").as<int>() == 42);

    ConstValueView crects = ccv.field("rectangles");
    ConstValueView rect0_tl = crects.element(0).field("top_left");
    REQUIRE(rect0_tl.field("x").as<int>() == 0);
    REQUIRE(rect0_tl.field("y").as<int>() == 0);

    // Deep type checking - schema preserved at all levels
    REQUIRE(ccv.is_type(canvas_type));
    REQUIRE(ccv.field("rectangles").is_type(rect_list_type));
    REQUIRE(ccv.field("rectangles").element(0).is_type(rect_type));
    REQUIRE(ccv.field("rectangles").element(0).field("top_left").is_type(point_type));
    REQUIRE(ccv.field("rectangles").element(0).field("top_left").field("x").is_type(scalar_type_meta<int>()));
}

// ============================================================================
// Example 7: Type Checking and Schema Comparison
// ============================================================================

TEST_CASE("Example 7: Type Checking and Schema Comparison", "[value][examples]") {
    TypeRegistry registry;

    // Define two different types with identical structure
    auto point2d_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Point2D");
    const BundleTypeMeta* point2d = registry.register_type("Point2D", std::move(point2d_meta));

    auto vector2d_meta = BundleTypeBuilder()
        .add_field<int>("x")
        .add_field<int>("y")
        .build("Vector2D");
    const BundleTypeMeta* vector2d = registry.register_type("Vector2D", std::move(vector2d_meta));

    // Create values
    Value p(point2d);
    Value v(vector2d);

    // Even with identical structure, they are different types
    // (nominal typing, not structural)
    REQUIRE_FALSE(p.same_type_as(v));

    // Same type comparison
    Value p2(point2d);
    REQUIRE(p.same_type_as(p2));

    // Type lookup from registry
    REQUIRE(p.is_type(registry.get("Point2D")));
    REQUIRE_FALSE(p.is_type(registry.get("Vector2D")));

    // Safe typed access with type checking
    p.view().field("x").as<int>() = 5;
    ConstValueView cpv = p.const_view();

    // try_as returns nullptr if type doesn't match
    const int* x_ptr = cpv.field("x").try_as<int>();
    REQUIRE(x_ptr != nullptr);
    REQUIRE(*x_ptr == 5);

    const double* wrong_ptr = cpv.field("x").try_as<double>();
    REQUIRE(wrong_ptr == nullptr);  // Type mismatch

    // Copy preserves type
    Value p_copy = Value::copy(p);
    REQUIRE(p_copy.same_type_as(p));
    REQUIRE(p_copy.equals(p));
    REQUIRE(p_copy.is_type(point2d));
}

// ============================================================================
// Example 8: Working with Views at Different Levels
// ============================================================================

TEST_CASE("Example 8: Views at Different Nesting Levels", "[value][examples]") {
    TypeRegistry registry;

    // Build nested structure
    auto inner_meta = BundleTypeBuilder()
        .add_field<int>("value")
        .add_field<double>("factor")
        .build("Inner");
    const BundleTypeMeta* inner_type = registry.register_type("Inner", std::move(inner_meta));

    auto outer_meta = BundleTypeBuilder()
        .add_field<int>("id")
        .add_field("data", inner_type)
        .build("Outer");
    const BundleTypeMeta* outer_type = registry.register_type("Outer", std::move(outer_meta));

    // Create owner value (the "owner type")
    Value owner(outer_type);
    REQUIRE(owner.schema() == outer_type);

    // Get view at root level
    ValueView root_view = owner.view();
    REQUIRE(root_view.is_type(outer_type));

    // Navigate to nested level - view preserves type information
    ValueView data_view = root_view.field("data");
    REQUIRE(data_view.is_type(inner_type));

    // Navigate to scalar level
    ValueView value_view = data_view.field("value");
    REQUIRE(value_view.is_scalar());
    REQUIRE(value_view.is_type(scalar_type_meta<int>()));

    // Set values at different levels
    root_view.field("id").as<int>() = 100;
    data_view.field("value").as<int>() = 42;
    data_view.field("factor").as<double>() = 2.5;

    // Views point to same memory
    ConstValueView check1 = owner.const_view().field("data").field("value");
    ConstValueView check2 = data_view.field("value");  // Using existing view

    REQUIRE(check1.data() == check2.data());  // Same memory
    REQUIRE(check1.schema() == check2.schema());  // Same type
    REQUIRE(check1.as<int>() == 42);

    // Store views for later use
    std::vector<ConstValueView> field_views;
    ConstValueView cv = owner.const_view();
    for (size_t i = 0; i < cv.field_count(); ++i) {
        field_views.push_back(cv.field(i));
    }

    REQUIRE(field_views.size() == 2);
    REQUIRE(field_views[0].is_scalar());     // id
    REQUIRE(field_views[1].is_bundle());     // data
}

// ============================================================================
// Example 9: Registry Lookup and Type Construction
// ============================================================================

TEST_CASE("Example 9: Registry Lookup and Type Construction", "[value][examples]") {
    TypeRegistry registry;

    // Register complex types that reference each other
    registry.register_type("Coordinate",
        BundleTypeBuilder()
            .add_field<double>("lat")
            .add_field<double>("lon")
            .build("Coordinate"));

    registry.register_type("Timestamp",
        BundleTypeBuilder()
            .add_field<int64_t>("seconds")
            .add_field<int32_t>("nanos")
            .build("Timestamp"));

    // Use require() to get types and compose them
    registry.register_type("LocationEvent",
        BundleTypeBuilder()
            .add_field("coord", registry.require("Coordinate"))
            .add_field("time", registry.require("Timestamp"))
            .add_field<int>("device_id")
            .build("LocationEvent"));

    // Verify types are registered
    REQUIRE(registry.contains("Coordinate"));
    REQUIRE(registry.contains("Timestamp"));
    REQUIRE(registry.contains("LocationEvent"));

    // Create value from registry lookup
    const TypeMeta* event_type = registry.require("LocationEvent");
    Value event(event_type);

    // Populate using views
    ValueView ev = event.view();
    ev.field("device_id").as<int>() = 12345;

    ValueView coord = ev.field("coord");
    coord.field("lat").as<double>() = 51.5074;
    coord.field("lon").as<double>() = -0.1278;

    ValueView ts = ev.field("time");
    ts.field("seconds").as<int64_t>() = 1702400000;
    ts.field("nanos").as<int32_t>() = 123456789;

    // Verify via const view
    ConstValueView cev = event.const_view();
    REQUIRE(cev.field("device_id").as<int>() == 12345);
    REQUIRE(cev.field("coord").field("lat").as<double>() == 51.5074);
    REQUIRE(cev.field("time").field("seconds").as<int64_t>() == 1702400000);

    // Type checking via registry
    REQUIRE(cev.field("coord").is_type(registry.get("Coordinate")));
    REQUIRE(cev.field("time").is_type(registry.get("Timestamp")));
}
