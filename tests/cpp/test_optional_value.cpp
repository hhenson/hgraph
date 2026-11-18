// Tests for ``Optional[T]`` (RFC 0050): the value type that holds one
// payload or is unset. The registry and its names, the value's equality,
// ordering, hashing and copying in both states, the optional views, the
// static schema spelling, and the binary and JSON forms.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/metadata/type_names.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/metadata/schema_table.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/visitor.h>

#include <algorithm>
#include <array>
#include <compare>
#include <stdexcept>
#include <string>

namespace
{
    using namespace hgraph;

    /** A scalar that cannot be made without a value. */
    struct NoDefault
    {
        NoDefault() = delete;
        explicit NoDefault(int given) : value(given) {}
        int value;
        bool operator==(const NoDefault &) const = default;
    };

    /** An ``Optional[int]`` that holds ``payload``. */
    [[nodiscard]] Value held(Int payload)
    {
        const auto *schema = scalar_descriptor<Optional<Int>>::value_meta();
        Value       value{ValuePlanFactory::instance().type_for(schema)};
        value.view().as_optional().begin_mutation().emplace(Value{payload}.view());
        return value;
    }

    /** An ``Optional[int]`` in the unset state. */
    [[nodiscard]] Value unset()
    {
        const auto *schema = scalar_descriptor<Optional<Int>>::value_meta();
        return Value{ValuePlanFactory::instance().type_for(schema)};
    }
}  // namespace

TEST_CASE("Optional: the registry interns it by payload and names it")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    const auto *str_meta = scalar_descriptor<Str>::value_meta();

    const auto *optional_int = registry.optional(int_meta);
    REQUIRE(optional_int != nullptr);
    CHECK(optional_int == registry.optional(int_meta));
    CHECK(optional_int != registry.optional(str_meta));

    // A kind of its own, whose element is the payload.
    CHECK(optional_int->value_kind() == ValueTypeKind::Optional);
    CHECK(optional_int->is_optional());
    CHECK(optional_int->element_type == int_meta);
    CHECK(optional_int->name() == std::string_view{"Optional[int]"});

    // It is no Tuple[int], and a Tuple[int] is no Optional.
    const auto *tuple_int = registry.tuple({int_meta});
    CHECK(optional_int != tuple_int);
    CHECK(tuple_int->value_kind() == ValueTypeKind::Tuple);
    CHECK_FALSE(tuple_int->is_optional());

    // What the payload is, the Optional is: an int hashes, so it does.
    CHECK(optional_int->is_hashable() == int_meta->is_hashable());
}

TEST_CASE("Optional: any and another Optional are no payload")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = scalar_descriptor<Int>::value_meta();

    CHECK_THROWS_AS(registry.optional(nullptr), std::invalid_argument);
    CHECK_THROWS_AS(registry.optional(registry.any()), std::invalid_argument);
    CHECK_THROWS_AS(registry.optional(registry.optional(int_meta)), std::invalid_argument);
}

TEST_CASE("Optional: its name reads back to the schema, and its descriptor is its own")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = scalar_descriptor<Int>::value_meta();
    const auto *optional_int = registry.optional(int_meta);

    CHECK(parse_value_type_name("Optional[int]") == optional_int);
    CHECK(parse_value_type_name(optional_int->name()) == optional_int);
    const auto *nested = registry.optional(registry.tuple({int_meta, int_meta}));
    CHECK(parse_value_type_name(nested->name()) == nested);
    CHECK(parse_ts_type_name("TS[Optional[int]]") == registry.ts(optional_int));

    // The kind is part of the canonical descriptor, so the wire tells an
    // Optional[int] from a Tuple[int].
    CHECK(manifest::value_descriptor(optional_int) != manifest::value_descriptor(registry.tuple({int_meta})));
    CHECK(manifest::value_descriptor(optional_int) == manifest::value_descriptor(registry.optional(int_meta)));
}

TEST_CASE("Optional: the static schema marker carries no value")
{
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = scalar_descriptor<Int>::value_meta();

    STATIC_REQUIRE_FALSE(std::is_default_constructible_v<Optional<Int>>);
    REQUIRE(scalar_descriptor<Optional<Int>>::is_concrete());
    CHECK(scalar_descriptor<Optional<Int>>::value_meta() == registry.optional(int_meta));
    CHECK(schema_descriptor<TS<Optional<Int>>>::ts_meta() == registry.ts(registry.optional(int_meta)));
    CHECK(scalar_descriptor<Optional<Tuple<Int, Str>>>::value_meta() ==
          registry.optional(scalar_descriptor<Tuple<Int, Str>>::value_meta()));
}

TEST_CASE("Optional: a new value is unset, and a view holds a payload or returns to unset")
{
    Value value = unset();
    REQUIRE(value.view().is_optional());

    auto view = value.view().as_optional();
    CHECK_FALSE(view.has_value());
    // The payload of an unset Optional is a typed null: bound, not valid.
    CHECK_FALSE(view.value().valid());
    CHECK(view.value().bound());
    CHECK(view.payload_binding().schema() == scalar_descriptor<Int>::value_meta());

    view.begin_mutation().emplace(Value{Int{7}}.view());
    CHECK(view.has_value());
    CHECK(view.value().checked_as<Int>() == 7);

    // Holding another payload replaces the one held.
    view.begin_mutation().emplace(Value{Int{9}}.view());
    CHECK(view.value().checked_as<Int>() == 9);

    view.begin_mutation().reset();
    CHECK_FALSE(view.has_value());
    CHECK_FALSE(view.value().valid());

    // What was held is not seen again when a payload is next held.
    view.begin_mutation().emplace(Value{Int{3}}.view());
    CHECK(view.value().checked_as<Int>() == 3);

    // A payload is a value: an empty one is refused, and reset is the way
    // to the unset state.
    CHECK_THROWS_AS(view.begin_mutation().emplace(ValueView{}), std::logic_error);
}

TEST_CASE("Optional: the views are for an Optional, and the mutable one for mutable storage")
{
    const Value plain{Int{1}};
    CHECK_FALSE(plain.view().is_optional());
    CHECK_FALSE(plain.view().try_as_optional().has_value());
    CHECK_THROWS_AS(plain.view().as_optional(), std::logic_error);
    CHECK_THROWS_AS(plain.view().as_mutable_optional(), std::logic_error);

    // A Tuple[int] is no Optional, and an Optional is no tuple.
    const auto *tuple_int = TypeRegistry::instance().tuple({scalar_descriptor<Int>::value_meta()});
    const Value tuple{ValuePlanFactory::instance().type_for(tuple_int)};
    CHECK_FALSE(tuple.view().is_optional());
    CHECK_FALSE(tuple.view().try_as_optional().has_value());

    const Value value = held(5);
    CHECK_FALSE(value.view().is_tuple());
    CHECK_FALSE(value.view().is_indexed());
    CHECK_FALSE(value.view().try_as_tuple().has_value());
    REQUIRE(value.view().try_as_optional().has_value());
    CHECK(static_cast<bool>(value.view().as_optional()));
    CHECK_FALSE(static_cast<bool>(unset().view().as_optional()));
    // A read-only view opens no mutation.
    CHECK_FALSE(value.view().try_as_mutable_optional().has_value());
    CHECK_THROWS_AS(value.view().as_mutable_optional(), std::logic_error);
}

TEST_CASE("Optional: unset equals unset and orders before a held payload")
{
    const Value none = unset();
    const Value also_none = unset();
    const Value five = held(5);
    const Value also_five = held(5);
    const Value seven = held(7);

    CHECK(none.view().equals(also_none.view()));
    CHECK(five.view().equals(also_five.view()));
    CHECK_FALSE(five.view().equals(seven.view()));
    CHECK_FALSE(none.view().equals(five.view()));
    CHECK_FALSE(five.view().equals(none.view()));

    CHECK(none.view().compare(also_none.view()) == std::partial_ordering::equivalent);
    CHECK(none.view().compare(five.view()) == std::partial_ordering::less);
    CHECK(five.view().compare(none.view()) == std::partial_ordering::greater);
    CHECK(five.view().compare(seven.view()) == std::partial_ordering::less);
    CHECK(five.view().compare(also_five.view()) == std::partial_ordering::equivalent);

    CHECK(none.view().hash() == also_none.view().hash());
    CHECK(five.view().hash() == also_five.view().hash());
    CHECK(none.view().hash() != five.view().hash());
}

TEST_CASE("Optional: a value of it is no value of Tuple[T]")
{
    const auto *tuple_int = TypeRegistry::instance().tuple({scalar_descriptor<Int>::value_meta()});
    BundleBuilder builder{ValuePlanFactory::instance().type_for(tuple_int)};
    builder.set(0, Value{Int{5}});
    const Value tuple = builder.build();
    const Value optional = held(5);

    // The two hold the same payload in the same layout, and are of two
    // types: neither equals the other.
    CHECK_FALSE(optional.view().equals(tuple.view()));
    CHECK_FALSE(tuple.view().equals(optional.view()));
}

TEST_CASE("Optional: copying keeps the state, and assigning takes the source's")
{
    const Value five = held(5);
    const Value none = unset();

    const Value five_copy = five.view().clone();
    CHECK(five_copy.view().equals(five.view()));
    CHECK(five_copy.view().as_optional().value().checked_as<Int>() == 5);
    const Value none_copy = none.view().clone();
    CHECK_FALSE(none_copy.view().as_optional().has_value());

    // Unset over held, and held over unset.
    Value target = held(1);
    target.view().begin_mutation().copy_from(none.view());
    CHECK_FALSE(target.view().as_optional().has_value());
    target.view().begin_mutation().copy_from(five.view());
    CHECK(target.view().as_optional().value().checked_as<Int>() == 5);

    Value moved = held(8);
    Value sink = unset();
    sink = std::move(moved);
    CHECK(sink.view().as_optional().value().checked_as<Int>() == 8);
}

TEST_CASE("Optional: a payload may be a composite")
{
    auto       &registry = TypeRegistry::instance();
    const auto *pair = scalar_descriptor<Tuple<Int, Str>>::value_meta();
    const auto *schema = registry.optional(pair);

    BundleBuilder payload{ValuePlanFactory::instance().type_for(pair)};
    payload.set(0, Value{Int{4}});
    payload.set(1, Value{Str{"four"}});

    Value value{ValuePlanFactory::instance().type_for(schema)};
    value.view().as_optional().begin_mutation().emplace(payload.build().view());
    const auto read = value.view().as_optional().value().as_tuple();
    CHECK(read.at(0).checked_as<Int>() == 4);
    CHECK(read.at(1).checked_as<Str>() == Str{"four"});

    const Value copy = value.view().clone();
    CHECK(copy.view().equals(value.view()));
}

TEST_CASE("Optional: the binary form is a presence tag and then the payload")
{
    const auto *schema = scalar_descriptor<Optional<Int>>::value_meta();

    const Value none = unset();
    const auto  none_bytes = to_binary_string(none.view());
    REQUIRE(none_bytes.size() == 1);
    CHECK(none_bytes[0] == '\0');
    CHECK_FALSE(from_binary_string(schema, none_bytes).view().as_optional().has_value());

    const Value five = held(5);
    const auto  five_bytes = to_binary_string(five.view());
    REQUIRE(five_bytes.size() > 1);
    CHECK(five_bytes[0] == '\1');
    // What follows the tag is the payload's own form.
    CHECK(five_bytes.substr(1) == to_binary_string(Value{Int{5}}.view()));
    const Value back = from_binary_string(schema, five_bytes);
    CHECK(back.view().equals(five.view()));

    // A tag is 0 or 1.
    std::string bad = five_bytes;
    bad[0] = '\2';
    CHECK_THROWS_WITH(from_binary_string(schema, bad),
                      Catch::Matchers::ContainsSubstring("invalid optional presence tag"));
}

TEST_CASE("Optional: the JSON form is null, or the payload's own")
{
    auto       &registry = TypeRegistry::instance();
    const auto *schema = scalar_descriptor<Optional<Int>>::value_meta();

    CHECK(to_json_string(unset().view()) == "null");
    CHECK(to_json_string(held(5).view()) == "5");

    CHECK_FALSE(from_json_string(schema, "null").view().as_optional().has_value());
    const Value five = from_json_string(schema, "5");
    CHECK(five.view().as_optional().value().checked_as<Int>() == 5);

    // Inside a list each element is null or its payload.
    const auto *list = registry.list(schema, 0, true);
    const Value read = from_json_string(list, "[1, null, 3]");
    const auto  elements = read.view().as_list();
    REQUIRE(elements.size() == 3);
    CHECK(elements.at(0).as_optional().value().checked_as<Int>() == 1);
    CHECK_FALSE(elements.at(1).as_optional().has_value());
    CHECK(elements.at(2).as_optional().value().checked_as<Int>() == 3);
    CHECK(to_json_string(read.view()) == "[1, null, 3]");

    // A composite payload is written as that composite is.
    const auto *pair = registry.optional(scalar_descriptor<Tuple<Int, Str>>::value_meta());
    const Value held_pair = from_json_string(pair, "[4, \"four\"]");
    CHECK(held_pair.view().as_optional().has_value());
    CHECK(to_json_string(held_pair.view()) == "[4, \"four\"]");
}

TEST_CASE("Optional: nested in a composite or a map it reads its own JSON null")
{
    auto       &registry = TypeRegistry::instance();
    const auto *optional_int = scalar_descriptor<Optional<Int>>::value_meta();

    // A tuple's field: `[null]` is a field that holds an unset Optional.
    const auto *tuple = registry.tuple({optional_int, optional_int});
    const Value pair = from_json_string(tuple, "[null, 5]");
    const auto  fields = pair.view().as_tuple();
    REQUIRE(fields.at(0).valid());
    CHECK_FALSE(fields.at(0).as_optional().has_value());
    CHECK(fields.at(1).as_optional().value().checked_as<Int>() == 5);
    CHECK(to_json_string(pair.view()) == "[null, 5]");

    // A bundle's field: `null` is the Optional's, and a key left out is a
    // field that is unset.
    const auto *bundle = registry.un_named_bundle({{"first", optional_int}, {"second", optional_int}});
    const Value partial = from_json_string(bundle, "{\"first\": null}");
    const auto  named = partial.view().as_bundle();
    REQUIRE(named.at("first").valid());
    CHECK_FALSE(named.at("first").as_optional().has_value());
    CHECK_FALSE(named.at("second").valid());
    CHECK(to_json_string(partial.view()) == "{\"first\": null}");

    // A map's value: the entry is there, and holds an unset Optional.
    const auto *map = registry.map(scalar_descriptor<Str>::value_meta(), optional_int);
    const Value entries = from_json_string(map, "{\"a\": null, \"b\": 7}");
    const auto  read = entries.view().as_map();
    REQUIRE(read.size() == 2);
    const auto a = read.at(Value{Str{"a"}}.view());
    REQUIRE(a.valid());
    CHECK_FALSE(a.as_optional().has_value());
    CHECK(read.at(Value{Str{"b"}}.view()).as_optional().value().checked_as<Int>() == 7);
}

TEST_CASE("Optional: a visitor reaches it by its own view, or by its catch-all")
{
    const Value five = held(5);
    const Value none = unset();

    // A handler of OptionalView takes it, whatever it holds.
    const auto by_view = [](const ValueView &value) {
        return visit(
            value, [](OptionalView optional) { return optional.has_value() ? 1 : 0; }, [](ValueView) { return -1; });
    };
    CHECK(by_view(five.view()) == 1);
    CHECK(by_view(none.view()) == 0);
    CHECK(by_view(Value{Int{3}}.view()) == -1);

    // With no such handler the catch-all does, and a tuple's does not.
    CHECK(visit(five.view(), [](TupleView) { return 1; }, [](ValueView) { return 2; }) == 2);

    // A visitor that names each kind it was written for, and has no
    // catch-all, still compiles. An Optional is refused when it meets one.
    const auto every_other_kind = [](const ValueView &value) {
        return visit(
            value, [](AtomicView) { return 0; }, [](TupleView) { return 0; }, [](BundleView) { return 0; },
            [](ListView) { return 0; }, [](SetView) { return 0; }, [](MapView) { return 0; },
            [](CyclicBufferView) { return 0; }, [](QueueView) { return 0; });
    };
    CHECK(every_other_kind(Value{Int{3}}.view()) == 0);
    CHECK_THROWS_AS(every_other_kind(five.view()), std::invalid_argument);
}

TEST_CASE("Optional: a schema table writes its kind and its payload, and reads them back")
{
    auto       &registry = TypeRegistry::instance();
    const auto *optional_int = registry.optional(scalar_descriptor<Int>::value_meta());
    const auto *of_tuple = registry.optional(
        registry.tuple({scalar_descriptor<Int>::value_meta(), scalar_descriptor<Str>::value_meta()}));
    const auto *in_list = registry.list(optional_int, 0, false);

    SchemaTableWriter writer;
    const auto        optional_at = writer.value_ref(optional_int);
    const auto        of_tuple_at = writer.value_ref(of_tuple);
    const auto        in_list_at = writer.value_ref(in_list);
    std::string       bytes;
    writer.write(bytes);

    BinaryReader      reader{bytes};
    SchemaTableReader table;
    table.read(reader);
    CHECK(table.value_at(optional_at) == optional_int);
    CHECK(table.value_at(of_tuple_at) == of_tuple);
    CHECK(table.value_at(in_list_at) == in_list);
    CHECK(table.value_at(optional_at)->value_kind() == ValueTypeKind::Optional);
}

TEST_CASE("Optional: it is written as its payload is, or as None")
{
    CHECK(held(5).view().to_string() == "5");
    CHECK(unset().view().to_string() == "None");

    // A tuple of one is written as a tuple; an Optional is not one.
    const auto *tuple_int = TypeRegistry::instance().tuple({scalar_descriptor<Int>::value_meta()});
    BundleBuilder builder{ValuePlanFactory::instance().type_for(tuple_int)};
    builder.set(0, Value{Int{5}});
    CHECK(builder.build().view().to_string() == "(5,)");
}

TEST_CASE("Optional: a payload with no default constructor is refused by name")
{
    // The payload's storage is live while the value holds nothing, so it
    // is default-constructed with the value. A payload that cannot be is
    // refused where the storage is planned, and not where a value is made.
    auto       &registry = TypeRegistry::instance();
    const auto *payload = registry.register_scalar<NoDefault>("tests.optional.NoDefault");
    const auto *schema = registry.optional(payload);
    REQUIRE(schema != nullptr);
    CHECK_THROWS_WITH(ValuePlanFactory::instance().type_for(schema),
                      Catch::Matchers::ContainsSubstring("Optional[tests.optional.NoDefault]") &&
                          Catch::Matchers::ContainsSubstring("default-constructible"));
}

TEST_CASE("Optional: a payload is assigned whole, whatever its own view allows")
{
    // An immutable set opens no mutation of its own. It is assigned as a
    // whole value, into an Optional that held nothing and into one that held
    // another, and survives the binary form.
    auto       &registry = TypeRegistry::instance();
    const auto *set_schema = registry.set(scalar_descriptor<Int>::value_meta());
    const auto *schema = registry.optional(set_schema);

    SetBuilder first{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
    first.insert(Int{1});
    first.insert(Int{2});
    const Value one_two = first.build();
    SetBuilder second{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
    second.insert(Int{9});
    const Value nine = second.build();

    Value value{ValuePlanFactory::instance().type_for(schema)};
    value.view().as_optional().begin_mutation().emplace(one_two.view());
    REQUIRE(value.view().as_optional().has_value());
    CHECK(value.view().as_optional().value().as_set().size() == 2);
    value.view().as_optional().begin_mutation().emplace(nine.view());
    CHECK(value.view().as_optional().value().as_set().size() == 1);

    const Value read = from_binary_string(schema, to_binary_string(value.view()));
    CHECK(read.view().equals(value.view()));

    // A payload of another type is refused, and what was held stays held.
    Value plain = unset();
    CHECK_THROWS_AS(plain.view().as_optional().begin_mutation().emplace(Value{Str{"no"}}.view()),
                    std::invalid_argument);
    CHECK_FALSE(plain.view().as_optional().has_value());
    Value kept = held(5);
    CHECK_THROWS_AS(kept.view().as_optional().begin_mutation().emplace(Value{Str{"no"}}.view()),
                    std::invalid_argument);
    CHECK(kept.view().as_optional().value().checked_as<Int>() == 5);
}

TEST_CASE("Optional: two realizations of one schema compare as one")
{
    // The same schema, realized with its canonical binding and with one
    // made from its payload's binding.
    const auto *schema = scalar_descriptor<Optional<Int>>::value_meta();
    auto       &factory = ValuePlanFactory::instance();
    const auto  canonical = factory.type_for(schema);
    const std::array<ValueTypeRef, 1> payload{factory.type_for(scalar_descriptor<Int>::value_meta())};
    const auto  realized = factory.realized_composite_type_for(schema, payload);
    REQUIRE(realized);
    // Two bindings, so the comparison is the one made across realizations.
    REQUIRE(realized != canonical);

    const auto holding = [](ValueTypeRef binding, std::optional<Int> payload) {
        Value value{binding};
        if (payload.has_value()) { value.view().as_optional().begin_mutation().emplace(Value{*payload}.view()); }
        return value;
    };
    const Value none_a = holding(canonical, std::nullopt);
    const Value none_b = holding(realized, std::nullopt);
    const Value five_a = holding(canonical, 5);
    const Value five_b = holding(realized, 5);
    const Value seven_b = holding(realized, 7);

    CHECK(none_a.view().equals(none_b.view()));
    CHECK(five_a.view().equals(five_b.view()));
    CHECK_FALSE(five_a.view().equals(seven_b.view()));
    CHECK_FALSE(none_a.view().equals(five_b.view()));
    CHECK(none_a.view().compare(none_b.view()) == std::partial_ordering::equivalent);
    CHECK(none_a.view().compare(five_b.view()) == std::partial_ordering::less);
    CHECK(five_a.view().compare(none_b.view()) == std::partial_ordering::greater);
    CHECK(five_a.view().compare(seven_b.view()) == std::partial_ordering::less);
    CHECK(five_a.view().compare(five_b.view()) == std::partial_ordering::equivalent);
}

TEST_CASE("Optional: a manifest encodes whether it holds a payload, and then the payload")
{
    const auto encoded = [](const Value &value) {
        manifest::CanonicalWriter writer;
        manifest::encode_manifest_scalar(writer, value.view());
        return writer.take();
    };
    const auto *schema = scalar_descriptor<Optional<Int>>::value_meta();
    std::string reason;
    REQUIRE(manifest::manifest_scalar_encodable(schema, &reason));

    // Nothing is one byte, and a payload is that byte set and then the
    // payload as it is encoded alone.
    const auto none = encoded(unset());
    const auto five = encoded(held(5));
    const auto plain = encoded(Value{Int{5}});
    REQUIRE(none.size() == 1);
    CHECK(none[0] == std::byte{0});
    REQUIRE(five.size() == plain.size() + 1);
    CHECK(five[0] == std::byte{1});
    CHECK(std::equal(plain.begin(), plain.end(), five.begin() + 1));
    CHECK(encoded(held(7)) != five);
}
