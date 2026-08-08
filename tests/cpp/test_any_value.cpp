// Tests for the ``Any`` value kind — a type-erased box holding an embedded
// owning ``Value``. Covers the registry singleton schema, plan/binding
// synthesis, the AnyView / MutableAnyView surface (has_value / get / set /
// clear), heterogeneous contents, and equals / hash / compare / to_string
// delegation to the contained value (with an explicit empty state).

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/value.h>

#include <string>

namespace
{
    using namespace hgraph;

    // Build an empty Any value (its box exists; the contained value is empty).
    Value make_any()
    {
        ValueTypeRef binding = ValuePlanFactory::instance().type_for(TypeRegistry::instance().any());
        REQUIRE(binding != nullptr);
        return Value{binding};
    }

    // Build an Any value holding a copy of `inner`.
    Value make_any(const Value &inner)
    {
        Value any = make_any();
        any.as_any().begin_mutation().set(inner.view());
        return any;
    }
}  // namespace

TEST_CASE("Any: registry.any() is an unconstrained singleton schema")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();

    const ValueTypeMetaData *a = registry.any();
    const ValueTypeMetaData *b = registry.any();
    REQUIRE(a != nullptr);
    CHECK(a == b);  // interned singleton
    CHECK(a->value_kind() == ValueTypeKind::Any);
    CHECK(a->element_type == nullptr);  // unconstrained
    CHECK(a->key_type == nullptr);
    CHECK(a->field_count == 0);
}

TEST_CASE("Any: plan + binding synthesise and the binding is interned")
{
    using namespace hgraph;
    auto       &factory = ValuePlanFactory::instance();
    const auto *schema  = TypeRegistry::instance().any();

    ValueTypeRef binding = factory.type_for(schema);
    REQUIRE(binding != nullptr);
    REQUIRE(binding.plan() != nullptr);
    CHECK(binding.schema() == schema);
    CHECK(factory.plan_for(schema) == binding.plan());
    CHECK(any_type() == binding);  // same interned triple
}

TEST_CASE("Any: an empty box reports no contained value")
{
    using namespace hgraph;
    Value any = make_any();

    REQUIRE(any.has_value());  // the box itself is constructed
    auto view = any.as_any();
    CHECK(any.view().is_any());
    CHECK_FALSE(view.has_value());            // ...but the contained value is empty
    CHECK(view.value_schema() == nullptr);
    CHECK_FALSE(view.get().valid());
    CHECK(any.to_string() == "None");
}

TEST_CASE("Any: storage metrics recurse through the active contained value", "[memory]")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<std::string>("str");

    const std::string text(256, 'a');
    Value any = make_any(Value{text});
    const auto direct = any.as_any().get().dynamic_storage_metrics();
    const auto boxed = any.view().dynamic_storage_metrics();

    CHECK(boxed.live_bytes >= direct.live_bytes);
    CHECK(boxed.reserved_bytes >= direct.reserved_bytes);
    CHECK(boxed.live_bytes >= text.size() + 1);

    any.as_any().begin_mutation().clear();
    CHECK(any.view().dynamic_storage_metrics().live_bytes == 0);
    CHECK(any.view().dynamic_storage_metrics().reserved_bytes == 0);
}

TEST_CASE("Any: set / get a contained value and clear it")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    Value any = make_any();
    any.as_any().begin_mutation().set(Value{std::int32_t{42}});

    auto view = any.as_any();
    REQUIRE(view.has_value());
    REQUIRE(view.value_schema() != nullptr);
    CHECK(view.value_schema()->value_kind() == ValueTypeKind::Atomic);
    CHECK(view.get().checked_as<std::int32_t>() == 42);
    CHECK(any.to_string() == "42");

    any.as_any().begin_mutation().clear();
    CHECK_FALSE(any.as_any().has_value());
    CHECK(any.to_string() == "None");
}

TEST_CASE("Any: holds heterogeneous values (different schemas)")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("str");

    Value any = make_any();

    any.as_any().begin_mutation().set(Value{std::int32_t{7}});
    CHECK(any.as_any().get().checked_as<std::int32_t>() == 7);

    // Re-assign with a different schema entirely.
    any.as_any().begin_mutation().set(Value{std::string{"hello"}});
    REQUIRE(any.as_any().value_schema()->value_kind() == ValueTypeKind::Atomic);
    CHECK(any.as_any().get().checked_as<std::string>() == "hello");
}

TEST_CASE("Any: equals / compare / hash delegate to the contained value")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    Value empty1 = make_any();
    Value empty2 = make_any();
    Value a42    = make_any(Value{std::int32_t{42}});
    Value b42    = make_any(Value{std::int32_t{42}});
    Value a7     = make_any(Value{std::int32_t{7}});

    // Equality: both-empty equal; empty vs set unequal; same content equal.
    CHECK(empty1.equals(empty2));
    CHECK_FALSE(empty1.equals(a42));
    CHECK(a42.equals(b42));
    CHECK_FALSE(a42.equals(a7));

    // Hash agrees for equal Any values.
    CHECK(a42.hash() == b42.hash());

    // Compare: empty < non-empty; ordering follows the contained value.
    CHECK(empty1.compare(a42) == std::partial_ordering::less);
    CHECK(a42.compare(empty1) == std::partial_ordering::greater);
    CHECK(empty1.compare(empty2) == std::partial_ordering::equivalent);
    CHECK(a7.compare(a42) == std::partial_ordering::less);
}

TEST_CASE("Any: copy is a deep, independent copy of the contained value")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");

    Value original = make_any(Value{std::int32_t{5}});
    Value copy     = original;  // value copy

    CHECK(copy.as_any().get().checked_as<std::int32_t>() == 5);

    // Mutating the copy does not affect the original.
    copy.as_any().begin_mutation().set(Value{std::int32_t{99}});
    CHECK(copy.as_any().get().checked_as<std::int32_t>() == 99);
    CHECK(original.as_any().get().checked_as<std::int32_t>() == 5);
}
