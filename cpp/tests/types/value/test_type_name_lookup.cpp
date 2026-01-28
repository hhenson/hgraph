/**
 * @file test_type_name_lookup.cpp
 * @brief Unit tests for name-based type lookup in TypeRegistry.
 *
 * Phase 1 tests for:
 * - TypeMeta name field
 * - TypeRegistry::register_scalar<T>(name)
 * - TypeRegistry::get_by_name()
 * - TypeMeta::get("name") static lookup
 * - Built-in type name registration
 * - Python type lookup (from_python_type)
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/type_meta.h>

using namespace hgraph::value;

// ============================================================================
// TypeMeta Name Field Tests
// ============================================================================

TEST_CASE("TypeMeta - name field exists and defaults to nullptr", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::name field");

    auto& registry = TypeRegistry::instance();
    const TypeMeta* int_type = registry.register_scalar<int64_t>();

    CHECK(int_type != nullptr);
    // Name should be nullptr for types registered without explicit name
    // FUTURE: CHECK(int_type->name == nullptr);
}

// ============================================================================
// Name Registration Tests
// ============================================================================

TEST_CASE("TypeRegistry - register_scalar with name stores the name", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - register_scalar(name)");

    auto& registry = TypeRegistry::instance();

    // FUTURE API: const TypeMeta* my_int = registry.register_scalar<int64_t>("int");

    // CHECK(my_int != nullptr);
    // CHECK(my_int->name != nullptr);
    // CHECK(std::string(my_int->name) == "int");
}

TEST_CASE("TypeRegistry - get_by_name finds registered types", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - get_by_name()");

    auto& registry = TypeRegistry::instance();

    // FUTURE: registry.register_scalar<double>("float");
    // FUTURE: const TypeMeta* found = registry.get_by_name("float");

    // CHECK(found != nullptr);
    // CHECK(found->size == sizeof(double));
}

TEST_CASE("TypeRegistry - get_by_name returns nullptr for unknown names", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - get_by_name()");

    auto& registry = TypeRegistry::instance();

    // FUTURE: const TypeMeta* not_found = registry.get_by_name("nonexistent_type_xyz");
    // CHECK(not_found == nullptr);
}

TEST_CASE("TypeRegistry - has_by_name correctly reports registration status", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - has_by_name()");

    auto& registry = TypeRegistry::instance();

    // FUTURE: CHECK(registry.has_by_name("int") == true);
    // FUTURE: CHECK(registry.has_by_name("unknown_type") == false);
}

// ============================================================================
// TypeMeta::get() Static Lookup Tests
// ============================================================================

TEST_CASE("TypeMeta::get - returns correct type for 'int'", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::get()");

    // FUTURE: const TypeMeta* int_type = TypeMeta::get("int");
    // CHECK(int_type != nullptr);
    // CHECK(int_type->kind == TypeKind::Atomic);
    // CHECK(int_type->size == sizeof(int64_t));
}

TEST_CASE("TypeMeta::get - returns correct type for 'float'", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::get()");

    // FUTURE: const TypeMeta* float_type = TypeMeta::get("float");
    // CHECK(float_type != nullptr);
    // CHECK(float_type->size == sizeof(double));
}

TEST_CASE("TypeMeta::get - returns correct type for 'str'", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::get()");

    // FUTURE: const TypeMeta* str_type = TypeMeta::get("str");
    // CHECK(str_type != nullptr);
    // CHECK(str_type->size == sizeof(std::string));
}

TEST_CASE("TypeMeta::get - returns correct type for 'bool'", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::get()");

    // FUTURE: const TypeMeta* bool_type = TypeMeta::get("bool");
    // CHECK(bool_type != nullptr);
    // CHECK(bool_type->size == sizeof(bool));
}

TEST_CASE("TypeMeta::get - returns nullptr for unknown type names", "[value][phase1][type_name]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::get()");

    // FUTURE: const TypeMeta* unknown = TypeMeta::get("unknown");
    // CHECK(unknown == nullptr);
}

// ============================================================================
// Built-in Type Registration Tests
// ============================================================================

TEST_CASE("Built-in types - bool, int, float, str are registered with names", "[value][phase1][builtins]") {
    SKIP("Awaiting Phase 1 implementation - register_builtin_types()");

    // After initialization, these should all be findable by name
    // FUTURE:
    // CHECK(TypeMeta::get("bool") != nullptr);
    // CHECK(TypeMeta::get("int") != nullptr);
    // CHECK(TypeMeta::get("float") != nullptr);
    // CHECK(TypeMeta::get("str") != nullptr);
}

TEST_CASE("Built-in types - date, datetime, timedelta are registered", "[value][phase1][builtins]") {
    SKIP("Awaiting Phase 1 implementation - register_builtin_types()");

    // FUTURE:
    // CHECK(TypeMeta::get("date") != nullptr);
    // CHECK(TypeMeta::get("datetime") != nullptr);
    // CHECK(TypeMeta::get("timedelta") != nullptr);
}

// ============================================================================
// String Interning Tests
// ============================================================================

TEST_CASE("TypeRegistry - name strings are interned (same pointer for same name)", "[value][phase1][interning]") {
    SKIP("Awaiting Phase 1 implementation - string interning");

    auto& registry = TypeRegistry::instance();

    // Register two types with names
    // The registry should intern strings so same names get same pointer
    // FUTURE:
    // const char* name1 = registry.store_name("test_type");
    // const char* name2 = registry.store_name("test_type");
    // CHECK(name1 == name2);  // Same pointer due to interning
}

// ============================================================================
// Python Type Lookup Tests (Requires Python Environment)
// ============================================================================

TEST_CASE("TypeRegistry - from_python_type returns nullptr for unregistered types", "[value][phase1][python]") {
    SKIP("Awaiting Phase 1 implementation - from_python_type()");

    // This test requires Python embedding or mocking
}

TEST_CASE("TypeRegistry - from_python_type finds registered Python type mappings", "[value][phase1][python]") {
    SKIP("Awaiting Phase 1 implementation - from_python_type() and register_python_type()");
}

// ============================================================================
// Template Lookup Tests
// ============================================================================

TEST_CASE("TypeMeta::get<T> - template version returns same as string lookup", "[value][phase1][template]") {
    SKIP("Awaiting Phase 1 implementation - TypeMeta::get<T>()");

    // FUTURE:
    // const TypeMeta* by_name = TypeMeta::get("int");
    // const TypeMeta* by_template = TypeMeta::get<int64_t>();
    // CHECK(by_name == by_template);
}
