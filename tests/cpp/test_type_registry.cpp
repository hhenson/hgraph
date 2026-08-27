#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/compound_scalar_storage.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace {
struct LabelScalarA {
  int value{};
};

struct LabelScalarB {
  int value{};
};

struct AnonymousLabelScalar {
  int value{};
};
} // namespace

TEST_CASE("TypeRegistry::register_scalar returns canonical metadata") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *a = registry.register_scalar<std::int32_t>("int32");
  const auto *b = registry.register_scalar<std::int32_t>("int32");
  REQUIRE(a == b);
  REQUIRE(a != nullptr);
  REQUIRE(a->value_kind() == ValueTypeKind::Atomic);
}

TEST_CASE("TypeRegistry::register_scalar populates the value_type alias") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *meta = registry.register_scalar<double>("double");
  REQUIRE(registry.value_type("double") == meta);
}

TEST_CASE("TypeRegistry aliases reject conflicting schema bindings") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *standard_int = registry.value_type("int");
  REQUIRE(standard_int == registry.scalar_type<std::int64_t>().schema());

  const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
  REQUIRE(int32_meta != standard_int);
  REQUIRE_THROWS_AS(registry.register_scalar<std::int32_t>("int"),
                    std::invalid_argument);
  REQUIRE_THROWS_AS(registry.register_value_type_alias("int", int32_meta),
                    std::invalid_argument);
  REQUIRE(registry.value_type("int") == standard_int);

  const auto *standard_ts_int = registry.time_series_type("TS[int]");
  REQUIRE(standard_ts_int == registry.ts(standard_int));
  REQUIRE_THROWS_AS(registry.register_time_series_type_alias(
                        "TS[int]", registry.ts(int32_meta)),
                    std::invalid_argument);
  REQUIRE(registry.time_series_type("TS[int]") == standard_ts_int);
}

TEST_CASE("TypeRegistry::register_scalar wires the canonical plan into "
          "ValuePlanFactory") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  auto &factory = ValuePlanFactory::instance();
  const auto *meta = registry.register_scalar<long long>("long long");
  REQUIRE(factory.find(meta) == &MemoryUtils::plan_for<long long>());
}

TEST_CASE("TypeRegistry: different scalar types yield different metadata") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *long_meta = registry.register_scalar<long>("long");
  REQUIRE(int_meta != long_meta);
}

TEST_CASE("TypeRegistry::tuple interns by component identity and is "
          "order-sensitive") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *float_meta = registry.register_scalar<float>("float32");

  const auto *t_if = registry.tuple({int_meta, float_meta});
  const auto *t_if_again = registry.tuple({int_meta, float_meta});
  const auto *t_fi = registry.tuple({float_meta, int_meta});

  REQUIRE(t_if == t_if_again);
  REQUIRE(t_if != t_fi);
  REQUIRE(t_if->value_kind() == ValueTypeKind::Tuple);
  REQUIRE(t_if->field_count == 2);
  REQUIRE(t_if->fields[0].type == int_meta);
  REQUIRE(t_if->fields[1].type == float_meta);
  REQUIRE(t_if->fields[0].name == nullptr);
  REQUIRE(t_if->fields[1].name == nullptr);
}

TEST_CASE("TypeRegistry::bundle interns by structural identity and registers "
          "the alias") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *str_meta = registry.register_scalar<std::string>("string");
  const auto *bundle_meta =
      registry.bundle("TestBundleA", {{"x", int_meta}, {"y", str_meta}});

  REQUIRE(bundle_meta != nullptr);
  REQUIRE(bundle_meta->value_kind() == ValueTypeKind::Bundle);
  REQUIRE(bundle_meta->field_count == 2);
  REQUIRE(std::string(bundle_meta->fields[0].name) == "x");
  REQUIRE(std::string(bundle_meta->fields[1].name) == "y");

  REQUIRE(registry.bundle("TestBundleA", {{"x", int_meta}, {"y", str_meta}}) ==
          bundle_meta);
  REQUIRE(registry.value_type("TestBundleA") == bundle_meta);
}

TEST_CASE("TypeRegistry: un_named_bundle and bundle distinguish structural vs "
          "nominal identity") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *str_meta = registry.register_scalar<std::string>("string");

  const std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields{
      {"x", int_meta}, {"y", str_meta}};

  // Two un_named_bundle calls with the same field list return the same
  // canonical pointer.
  const auto *u1 = registry.un_named_bundle(fields);
  const auto *u2 = registry.un_named_bundle(fields);
  REQUIRE(u1 == u2);
  REQUIRE(u1->is_un_named_bundle());
  REQUIRE_FALSE(u1->is_named_bundle());
  REQUIRE(std::string{u1->name()} == "Bundle{x:int32,y:str}");
  REQUIRE(u1->wrapped_un_named == nullptr);

  // bundle(name, fields) wraps an un_named_bundle; same name + same fields →
  // same pointer.
  const auto *named_a1 = registry.bundle("BundleNamedA", fields);
  const auto *named_a2 = registry.bundle("BundleNamedA", fields);
  REQUIRE(named_a1 == named_a2);
  REQUIRE(named_a1->is_named_bundle());
  REQUIRE_FALSE(named_a1->is_un_named_bundle());
  REQUIRE(named_a1->wrapped_un_named == u1); // wraps the un-named twin
  REQUIRE(std::string(named_a1->name()) == std::string("BundleNamedA"));
  REQUIRE(named_a1->fields == u1->fields); // shares the field array
  REQUIRE(named_a1->field_count == u1->field_count);

  // Different names with the same fields are DISTINCT named schemas (nominal
  // identity).
  const auto *named_b = registry.bundle("BundleNamedB", fields);
  REQUIRE(named_b != named_a1);
  REQUIRE(named_b->wrapped_un_named == u1); // both wrap the same un-named

  // Named ≠ un-named, even though they share fields.
  REQUIRE(named_a1 != u1);

  // bundle(...) requires a non-empty name.
  REQUIRE_THROWS_AS(registry.bundle("", fields), std::invalid_argument);

  // named_bundle() lookup: returns the named meta, or nullptr when the
  // name doesn't exist or doesn't resolve to a named bundle.
  REQUIRE(registry.named_bundle("BundleNamedA") == named_a1);
  REQUIRE(registry.named_bundle("BundleNamedB") == named_b);
  REQUIRE(registry.named_bundle("DoesNotExist") == nullptr);
  // The atomic "int" registered earlier shares the value-name space but is not
  // a named bundle.
  REQUIRE(registry.named_bundle("int") == nullptr);

  // Bundle name namespace is unique: same name + same fields is idempotent,
  // same name + different fields is rejected.
  const std::vector<std::pair<std::string, const ValueTypeMetaData *>>
      different_fields{{"x", int_meta}, {"z", int_meta}};
  REQUIRE_NOTHROW(registry.bundle("BundleNamedA", fields)); // same shape -> OK
  REQUIRE_THROWS_AS(
      registry.bundle("BundleNamedA",
                      different_fields), // different shape -> conflict
      std::invalid_argument);
}

TEST_CASE("TypeRegistry gives named bundles qualified nominal identity") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *left =
      registry.bundle("tests.alpha", "QualifiedThing", {{"value", integer}});
  const auto *right =
      registry.bundle("tests.beta", "QualifiedThing", {{"value", integer}});

  REQUIRE(left != right);
  REQUIRE(std::string{left->bundle_namespace()} == "tests.alpha");
  REQUIRE(std::string{left->bundle_local_name()} == "QualifiedThing");
  REQUIRE(std::string{left->name()} == "tests.alpha::QualifiedThing");
  REQUIRE(registry.named_bundle("tests.alpha", "QualifiedThing") == left);
  REQUIRE(registry.named_bundle("tests.beta", "QualifiedThing") == right);
  REQUIRE(registry.bundle("tests.alpha::QualifiedThing",
                          {{"value", integer}}) == left);
}

TEST_CASE("TypeRegistry records invariant Bundle specializations") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  const auto *integer_box =
      registry.bundle("tests.generic", "Box[int]", {{"value", integer}}, {},
                      false, "__type__", {integer});
  const auto *string_box =
      registry.bundle("tests.generic", "Box[str]", {{"value", text}}, {}, false,
                      "__type__", {text});

  REQUIRE(integer_box != string_box);
  REQUIRE(integer_box->bundle_generic_arguments() ==
          std::vector<const ValueTypeMetaData *>{integer});
  REQUIRE(string_box->bundle_generic_arguments() ==
          std::vector<const ValueTypeMetaData *>{text});
  REQUIRE_THROWS_AS(registry.bundle("tests.generic", "Box[int]",
                                    {{"value", integer}}, {}, false, "__type__",
                                    {text}),
                    std::invalid_argument);
}

TEST_CASE("a storage category takes no part in type resolution") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *text = registry.value_type("str");
  REQUIRE(text != nullptr);

  // Base has a descendant, so a field declared as Base is held behind an owner
  // pointer: its closed union contains the leaf that embeds it and a flat
  // layout would recurse (issue #556).
  const auto *base = registry.bundle("tests.storage_category", "StorageBase",
                                     {{"symbol", text}}, {}, true);
  const auto *middle =
      registry.bundle("tests.storage_category", "StorageMiddle",
                      {{"symbol", text}, {"tenor", text}}, {base});
  const auto *unrelated = registry.bundle(
      "tests.storage_category", "StorageUnrelated", {{"symbol", text}}, {});

  const auto *owned_base = registry.owned(base);
  const auto *owned_middle = registry.owned(middle);
  const auto *shared_base = registry.shared(base);
  REQUIRE(owned_base != nullptr);
  REQUIRE(shared_base != nullptr);
  REQUIRE(owned_base->is_indirect());
  REQUIRE(shared_base->is_indirect());

  // Both categories are stripped, and stripping is idempotent on a plain type.
  CHECK(value_schema_without_storage(owned_base) == base);
  CHECK(value_schema_without_storage(shared_base) == base);
  CHECK(value_schema_without_storage(base) == base);
  CHECK(value_schema_without_storage(nullptr) == nullptr);

  const auto *ts_base = registry.ts(base);
  const auto *ts_middle = registry.ts(middle);
  const auto *ts_unrelated = registry.ts(unrelated);
  const auto *ts_owned_base = registry.ts(owned_base);
  const auto *ts_owned_middle = registry.ts(owned_middle);
  const auto *ts_shared_base = registry.ts(shared_base);

  // Comparison is blind to the category: these ARE the same type, so there is
  // nothing for an alternative to bind or present.
  CHECK(time_series_schema_equivalent(ts_base, ts_owned_base));
  CHECK(time_series_schema_equivalent(ts_owned_base, ts_base));
  CHECK(time_series_schema_equivalent(ts_base, ts_shared_base));
  CHECK(time_series_schema_equivalent(ts_owned_base, ts_shared_base));
  // ... and still tells genuinely different types apart.
  CHECK_FALSE(time_series_schema_equivalent(ts_base, ts_unrelated));
  CHECK_FALSE(time_series_schema_equivalent(ts_base, ts_middle));

  using hgraph::graph_wiring_detail::input_accepts_output_schema;

  // The subtype question is asked of the types, not of the categories.
  CHECK(input_accepts_output_schema(ts_base, ts_owned_middle));
  CHECK(input_accepts_output_schema(ts_base, ts_middle));
  CHECK(input_accepts_output_schema(ts_base, ts_shared_base));
  CHECK_FALSE(input_accepts_output_schema(ts_middle, ts_owned_base));
  CHECK_FALSE(input_accepts_output_schema(ts_unrelated, ts_owned_base));
}

TEST_CASE(
    "TypeRegistry records closed multiple-inheritance bundle hierarchies") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  const auto *order = registry.bundle("tests.orders", "HierarchyOrder",
                                      {{"id", integer}}, {}, true);
  const auto *priced = registry.bundle("tests.orders", "HierarchyPriced",
                                       {{"price", integer}}, {}, true);
  const auto *limit = registry.bundle(
      "tests.orders", "HierarchyLimit",
      {{"id", integer}, {"price", integer}, {"venue", text}}, {order, priced});

  REQUIRE(order->is_abstract_bundle());
  REQUIRE_FALSE(limit->is_abstract_bundle());
  REQUIRE(registry.bundle_is_a(limit, order));
  REQUIRE(registry.bundle_is_a(limit, priced));
  REQUIRE_FALSE(registry.bundle_is_a(order, limit));
  REQUIRE(order->bundle_hierarchy->children ==
          std::vector<const ValueTypeMetaData *>{limit});
  REQUIRE(limit->bundle_hierarchy->parents ==
          std::vector<const ValueTypeMetaData *>{order, priced});

  const auto descendants = registry.bundle_descendants(order);
  REQUIRE(descendants == std::vector<const ValueTypeMetaData *>{limit});
  const auto all_descendants = registry.bundle_descendants(order, true, true);
  REQUIRE(all_descendants ==
          std::vector<const ValueTypeMetaData *>{order, limit});

  REQUIRE_THROWS_AS(registry.bundle("tests.orders", "HierarchyBroken",
                                    {{"id", text}}, {order}),
                    std::invalid_argument);
}

TEST_CASE("self-recursive Bundles store one inline owner pointer and allocate "
          "on demand") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *recursive =
      registry.recursive_bundle("tests.recursion", "RecursiveValue",
                                {{"value", integer}, {"next", nullptr}});
  REQUIRE(recursive->field_count == 2);
  const auto *owned = recursive->fields[1].type;
  REQUIRE(owned != nullptr);
  REQUIRE(owned->is_owned());
  REQUIRE(owned->element_type == recursive);

  const auto owned_type = ValuePlanFactory::instance().type_for(owned);
  REQUIRE(owned_type.checked_plan().layout.size == sizeof(void *));
  REQUIRE(owned_type.checked_plan().layout.alignment == alignof(void *));

  Value empty_root{ValuePlanFactory::instance().type_for(recursive)};
  Value empty_owner{owned_type};
  REQUIRE_FALSE(empty_root.view().equals(empty_owner.view()));

  Value root{ValuePlanFactory::instance().type_for(recursive)};
  auto root_fields = root.as_bundle().begin_mutation();
  root_fields["value"].set(std::int64_t{1});
  auto next_owner = root_fields["next"];
  REQUIRE(next_owner.to_string() == "None");

  auto next_fields = next_owner.as_bundle().begin_mutation();
  next_fields["value"].set(std::int64_t{2});
  REQUIRE(root.as_bundle()["next"].concrete().schema() == recursive);
  REQUIRE(root.as_bundle()["next"]
              .as_bundle()["value"]
              .checked_as<std::int64_t>() == 2);

  Value copy{root};
  copy.as_bundle()
      .begin_mutation()["next"]
      .as_bundle()
      .begin_mutation()["value"]
      .set(std::int64_t{3});
  REQUIRE(root.as_bundle()["next"]
              .as_bundle()["value"]
              .checked_as<std::int64_t>() == 2);
  REQUIRE(copy.as_bundle()["next"]
              .as_bundle()["value"]
              .checked_as<std::int64_t>() == 3);

  const std::string encoded = to_json_string(root.view());
  Value decoded = from_json_string(recursive, encoded);
  REQUIRE(decoded.as_bundle()["next"]
              .as_bundle()["value"]
              .checked_as<std::int64_t>() == 2);
}

TEST_CASE("mutually recursive Bundles resolve owned edges across one "
          "declaration batch") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto schemas = registry.recursive_bundles({
      RecursiveBundleDefinition{
          .bundle_namespace = "tests.recursion",
          .local_name = "MutualLeft",
          .fields =
              {
                  {.name = "value", .type = integer},
                  {.name = "right", .owned_target = 1},
              },
      },
      RecursiveBundleDefinition{
          .bundle_namespace = "tests.recursion",
          .local_name = "MutualRight",
          .fields =
              {
                  {.name = "value", .type = integer},
                  {.name = "left", .owned_target = 0},
              },
      },
  });

  REQUIRE(schemas.size() == 2);
  REQUIRE(schemas[0]->fields[1].type->is_owned());
  REQUIRE(schemas[0]->fields[1].type->element_type == schemas[1]);
  REQUIRE(schemas[1]->fields[1].type->is_owned());
  REQUIRE(schemas[1]->fields[1].type->element_type == schemas[0]);
  REQUIRE(ValuePlanFactory::instance()
              .type_for(schemas[0]->fields[1].type)
              .checked_plan()
              .layout.size == sizeof(void *));

  Value left{ValuePlanFactory::instance().type_for(schemas[0])};
  auto fields = left.as_bundle().begin_mutation();
  fields["value"].set(std::int64_t{1});
  auto right = fields["right"].as_bundle().begin_mutation();
  right["value"].set(std::int64_t{2});
  right["left"].as_bundle().begin_mutation()["value"].set(std::int64_t{3});

  REQUIRE(left.as_bundle()["right"]
              .as_bundle()["value"]
              .checked_as<std::int64_t>() == 2);
  REQUIRE(left.as_bundle()["right"]
              .as_bundle()["left"]
              .as_bundle()["value"]
              .checked_as<std::int64_t>() == 3);
}

TEST_CASE("TypeRealizationSnapshot closes polymorphic Bundle storage without "
          "taxing leaves") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  const auto *base =
      registry.bundle("tests.realization", "SnapshotBase", {{"id", integer}});
  const auto *small = registry.bundle(
      "tests.realization", "SnapshotSmall",
      {{"id", integer}, {"quantity", integer}, {"label", text}}, {base});
  const auto *holder =
      registry.bundle("tests.realization", "SnapshotHolder", {{"item", base}});
  const auto *list_of_base = registry.list(base);
  const auto *fixed_list_of_base = registry.list(base, 2);
  const auto *mutable_list_of_base = registry.mutable_list(base);

  const auto first = TypeRealizationSnapshot::capture(registry);
  REQUIRE(TypeRealizationSnapshot::capture(registry) == first);
  const auto exact_small = ValuePlanFactory::instance().type_for(small);
  const auto realized_small = first->type_for(small);
  const auto realized_base = first->type_for(base);
  Value concrete{exact_small};
  concrete.as_bundle().begin_mutation()["label"].set(std::string{"small"});

  REQUIRE(realized_small == exact_small);
  REQUIRE(realized_base != ValuePlanFactory::instance().type_for(base));
  REQUIRE(realized_base.checked_plan().layout.size >=
          exact_small.checked_plan().layout.size + sizeof(const TypeRecord *));
  REQUIRE(first->alternatives(base) ==
          std::vector<const ValueTypeMetaData *>{base, small});

  const auto exact_holder = ValuePlanFactory::instance().type_for(holder);
  const auto realized_holder = first->type_for(holder);
  REQUIRE(realized_holder != exact_holder);
  REQUIRE(realized_holder.checked_plan().component("item").plan ==
          realized_base.plan());

  const auto realized_list = first->type_for(list_of_base);
  REQUIRE(realized_list != ValuePlanFactory::instance().type_for(list_of_base));
  REQUIRE_THROWS_AS(first->type_for(fixed_list_of_base), std::logic_error);
  const auto realized_mutable_list = first->type_for(mutable_list_of_base);
  REQUIRE(realized_mutable_list !=
          ValuePlanFactory::instance().type_for(mutable_list_of_base));
  Value mutable_list{realized_mutable_list};
  auto mutable_values = mutable_list.as_list().begin_mutation();
  mutable_values.push_back(concrete.view());
  REQUIRE(mutable_values.at(0).concrete().schema() == small);

  Value replacement{exact_small};
  replacement.as_bundle().begin_mutation()["label"].set(
      std::string{"replacement"});
  mutable_values.set(0, replacement.view());
  REQUIRE(mutable_values.at(0)
              .concrete()
              .as_bundle()["label"]
              .checked_as<std::string>() == "replacement");
  {
    TypeRealizationScope scope{first.get()};
    Value list = from_json_string(
        list_of_base,
        R"([{"__type__": "tests.realization::SnapshotSmall", "id": 1, "quantity": 2, "label": "one"}])");
    REQUIRE(list.binding() == realized_list);
    REQUIRE(list.view().as_list().at(0).concrete().schema() == small);
  }

  Value holder_value{realized_holder};
  auto item = holder_value.as_bundle().begin_mutation()["item"];
  realized_base.ops_ref().copy_assign_from(realized_base, item.mutable_data(),
                                           exact_small, concrete.view().data());
  REQUIRE(holder_value.as_bundle()["item"].concrete().schema() == small);

  Value polymorphic{realized_base};
  auto destination = polymorphic.begin_mutation();
  realized_base.ops_ref().copy_assign_from(realized_base,
                                           destination.mutable_data(),
                                           exact_small, concrete.view().data());
  const auto projected = polymorphic.view().concrete();
  REQUIRE(projected.schema() == small);
  REQUIRE(projected.binding() == exact_small);

  const auto *large = registry.bundle(
      "tests.realization", "SnapshotLarge",
      {{"id", integer}, {"description", text}, {"quantity", integer}}, {base});
  REQUIRE(first->alternatives(base) ==
          std::vector<const ValueTypeMetaData *>{base, small});

  const auto second = TypeRealizationSnapshot::capture(registry);
  REQUIRE(second->alternatives(base) ==
          std::vector<const ValueTypeMetaData *>{base, small, large});
  REQUIRE(
      second->type_for(base).checked_plan().layout.size >=
      ValuePlanFactory::instance().type_for(large).checked_plan().layout.size +
          sizeof(const TypeRecord *));

  Value newer{second->type_for(base)};
  auto newer_destination = newer.begin_mutation();
  second->type_for(base).ops_ref().copy_assign_from(
      second->type_for(base), newer_destination.mutable_data(),
      polymorphic.binding(), polymorphic.view().data());
  REQUIRE(newer.view().concrete().schema() == small);

  Value newer_small{second->type_for(base)};
  auto newer_small_destination = newer_small.begin_mutation();
  second->type_for(base).ops_ref().copy_assign_from(
      second->type_for(base), newer_small_destination.mutable_data(),
      exact_small, concrete.view().data());
  Value older_target{realized_base};
  auto older_destination = older_target.begin_mutation();
  auto newer_small_source = newer_small.begin_mutation();
  realized_base.ops_ref().move_assign_from(
      realized_base, older_destination.mutable_data(), newer_small.binding(),
      newer_small_source.mutable_data());
  REQUIRE(older_target.view()
              .concrete()
              .as_bundle()["label"]
              .checked_as<std::string>() == "small");

  Value large_value{ValuePlanFactory::instance().type_for(large)};
  second->type_for(base).ops_ref().copy_assign_from(
      second->type_for(base), newer_destination.mutable_data(),
      large_value.binding(), large_value.view().data());
  REQUIRE(newer.view().concrete().schema() == large);
  REQUIRE_THROWS_AS(first->type_for(base).ops_ref().copy_assign_from(
                        first->type_for(base), destination.mutable_data(),
                        newer.binding(), newer.view().data()),
                    std::invalid_argument);
}

TEST_CASE("graph realization pools wide polymorphic Bundles without escaping "
          "pool ownership") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  const auto *base = registry.bundle("tests.realization.pool", "Base",
                                     {{"id", integer}}, {}, true);
  const auto *small = registry.bundle("tests.realization.pool", "Small",
                                      {{"id", integer}}, {base});
  const auto *large = registry.bundle(
      "tests.realization.pool", "Large",
      {{"id", integer}, {"a", text}, {"b", text}, {"c", text}}, {base});

  const auto inline_snapshot = TypeRealizationSnapshot::capture(registry);
  REQUIRE(inline_snapshot->options().polymorphic_compound_storage ==
          PolymorphicCompoundStoragePolicy::Inline);
  REQUIRE(inline_snapshot->inspect(base).representation ==
          GraphValueRepresentation::InlineUnion);

  const TypeRealizationOptions pooled_options{
      .polymorphic_compound_storage =
          PolymorphicCompoundStoragePolicy::Pooled,
  };
  const auto snapshot =
      TypeRealizationSnapshot::capture(registry, pooled_options);
  REQUIRE(snapshot != inline_snapshot);
  REQUIRE(TypeRealizationSnapshot::capture(registry, pooled_options) ==
          snapshot);
  const auto external = snapshot->type_for(base);
  const auto graph = snapshot->graph_type_for(base);
  REQUIRE(graph != external);
  REQUIRE(graph.checked_plan().layout.size == sizeof(void *));
  REQUIRE(external.checked_plan().layout.size >
          graph.checked_plan().layout.size);
  REQUIRE(value_owning_type(graph) == external);
  const auto inspection = snapshot->inspect(base);
  REQUIRE(inspection.representation == GraphValueRepresentation::PooledUnion);
  REQUIRE(inspection.maximum_leaf_size - inspection.minimum_leaf_size > 32);
  REQUIRE(inspection.graph_size == sizeof(void *));

  Value canonical_large{ValuePlanFactory::instance().type_for(large)};
  auto canonical_fields = canonical_large.as_bundle().begin_mutation();
  canonical_fields["id"].set(Int{7});
  canonical_fields["a"].set(Str{"original"});

  CompoundScalarStorage pools = CompoundScalarStorage::make_default();
  TypeRealizationScope realization_scope{snapshot.get()};
  pools.bind(snapshot->pool_binding());

  Value escaped;
  {
    Value::storage_type first{*graph.record()};
    graph.ops_ref().copy_assign_from(graph, first.data(),
                                     canonical_large.binding(),
                                     canonical_large.view().data());
    Value::storage_type second{first};

    const void *const first_payload =
        graph.ops_ref().concrete_memory(first.data());
    REQUIRE(first_payload == graph.ops_ref().concrete_memory(second.data()));
    REQUIRE(pools.view().owns(first_payload));
    REQUIRE(graph.ops_ref().concrete_type(graph, first.data()).schema() ==
            large);

    // RFC 0029: a realization's pooled types serve exactly one live root
    // graph, so a second pool cannot claim them while this one is bound.
    CompoundScalarStorage other_root = CompoundScalarStorage::make_default();
    REQUIRE_THROWS_AS(other_root.bind(snapshot->pool_binding()),
                      std::logic_error);
    REQUIRE(other_root.view().inspect().live_slot_count == 0);
    REQUIRE(pools.view().owns(first_payload));

    std::vector<Value::storage_type> replicated_keys;
    replicated_keys.reserve(1024);
    for (std::size_t index = 0; index < 1024; ++index) {
      replicated_keys.emplace_back(first);
    }
    const auto replicated_metrics = pools.view().metrics();
    const auto pooled_bytes =
        replicated_metrics.reserved_bytes +
        replicated_keys.size() * graph.checked_plan().layout.size;
    const auto flat_bytes =
        replicated_keys.size() * external.checked_plan().layout.size;
    REQUIRE(pooled_bytes < flat_bytes);
    replicated_keys.clear();

    Value::storage_type moved_source{first};
    Value::storage_type move_survivor{moved_source};
    Value moved_external{external};
    external.ops_ref().move_assign_from(
        external, moved_external.begin_mutation().mutable_data(), graph,
        moved_source.data());
    REQUIRE(graph.ops_ref().concrete_memory(moved_source.data()) !=
            graph.ops_ref().concrete_memory(move_survivor.data()));
    const auto &move_survivor_read = move_survivor;
    REQUIRE(ValueView{graph, move_survivor_read.data()}
                .concrete()
                .as_bundle()["a"]
                .checked_as<Str>() == "original");
    REQUIRE(
        moved_external.view().concrete().as_bundle()["a"].checked_as<Str>() ==
        "original");

    auto second_concrete =
        ValueView{graph, second.data()}.begin_mutation().concrete();
    second_concrete.as_bundle().begin_mutation()["a"].set(Str{"detached"});
    REQUIRE(graph.ops_ref().concrete_memory(first.data()) == first_payload);
    REQUIRE(graph.ops_ref().concrete_memory(second.data()) != first_payload);
    const auto &first_read = first;
    REQUIRE(ValueView{graph, first_read.data()}
                .concrete()
                .as_bundle()["a"]
                .checked_as<Str>() == "original");

    std::vector<Value::storage_type> values;
    values.reserve(128);
    for (std::size_t index = 0; index < 128; ++index) {
      values.emplace_back(*graph.record());
      graph.ops_ref().copy_assign_from(graph, values.back().data(),
                                       canonical_large.binding(),
                                       canonical_large.view().data());
    }
    REQUIRE(graph.ops_ref().concrete_memory(first.data()) == first_payload);
    REQUIRE(pools.view().inspect().slot_capacity >= 128);

    escaped = Value{ValueView{graph, second.data()}};
    REQUIRE(escaped.binding() == external);
  }

  REQUIRE(escaped.view().concrete().schema() == large);
  REQUIRE(escaped.view().concrete().as_bundle()["a"].checked_as<Str>() ==
          "detached");
  const auto after_release = pools.view().inspect();
  REQUIRE(after_release.leaf_pool_count == 2);
  REQUIRE(after_release.live_slot_count == 0);
  REQUIRE(small != nullptr);
}

TEST_CASE("graph realization keeps narrow polymorphic Bundles inline") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *base = registry.bundle("tests.realization.inline", "Base",
                                     {{"id", integer}}, {}, true);
  registry.bundle("tests.realization.inline", "Small", {{"id", integer}},
                  {base});
  registry.bundle("tests.realization.inline", "Medium",
                  {{"id", integer}, {"quantity", integer}}, {base});

  const auto snapshot = TypeRealizationSnapshot::capture(
      registry,
      TypeRealizationOptions{
          .polymorphic_compound_storage =
              PolymorphicCompoundStoragePolicy::Pooled,
      });
  const auto inspection = snapshot->inspect(base);
  REQUIRE(inspection.representation == GraphValueRepresentation::InlineUnion);
  REQUIRE(inspection.maximum_leaf_size - inspection.minimum_leaf_size <= 32);
  REQUIRE(inspection.graph_size > sizeof(void *));
}

namespace {
struct AdmissionRace {
  std::size_t admitted{0};
  std::size_t refused{0};
  std::size_t owners{0};
};

/** Race `contenders` pool owners into one binding; report who got in. */
[[nodiscard]] AdmissionRace
race_admission(hgraph::CompoundScalarStorageBinding &binding,
               std::size_t contenders) {
  using namespace hgraph;
  std::vector<CompoundScalarStorage> pools;
  pools.reserve(contenders);
  for (std::size_t index = 0; index < contenders; ++index) {
    pools.push_back(CompoundScalarStorage::make_default());
  }

  std::atomic<std::size_t> ready{0};
  std::atomic<bool> go{false};
  std::atomic<std::size_t> admitted{0};
  std::atomic<std::size_t> refused{0};
  std::vector<std::thread> threads;
  threads.reserve(contenders);
  for (std::size_t index = 0; index < contenders; ++index) {
    threads.emplace_back([&, index] {
      ready.fetch_add(1, std::memory_order_release);
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      try {
        pools[index].bind(binding);
        admitted.fetch_add(1, std::memory_order_relaxed);
      } catch (const std::logic_error &) {
        refused.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  while (ready.load(std::memory_order_acquire) < contenders) {
    std::this_thread::yield();
  }
  go.store(true, std::memory_order_release);
  for (auto &thread : threads) {
    thread.join();
  }

  AdmissionRace result{admitted.load(), refused.load(), 0};
  if (binding.bound()) {
    // A torn write matches no contender's storage entire.
    const auto bound = binding.storage();
    for (auto &pool : pools) {
      if (pool.view().same_storage(bound)) {
        ++result.owners;
      }
    }
  }
  return result;
}
} // namespace

TEST_CASE("a realization admits exactly one root graph pool concurrently") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  // Registering here moves the bundle-hierarchy generation on, so the cached
  // snapshot this captures is this test's own and its binding starts free.
  registry.bundle("tests.realization.admission", "Base", {{"id", integer}}, {},
                  true);
  const auto snapshot = TypeRealizationSnapshot::capture(
      registry,
      TypeRealizationOptions{
          .polymorphic_compound_storage =
              PolymorphicCompoundStoragePolicy::Pooled,
      });
  REQUIRE_FALSE(snapshot->pool_binding().bound());

  // A cached realization is shared, so two root graphs can be constructed
  // against it at once.  Exactly one may claim its pooled value types.
  constexpr std::size_t contenders = 8;
  {
    const auto race = race_admission(snapshot->pool_binding(), contenders);
    CHECK(race.admitted == 1);
    CHECK(race.refused == contenders - 1);
    CHECK(race.owners == 1);
  }

  // The claim is released with its owner, and the next graph may take it.
  CHECK_FALSE(snapshot->pool_binding().bound());
  CompoundScalarStorage successor = CompoundScalarStorage::make_default();
  REQUIRE_NOTHROW(successor.bind(snapshot->pool_binding()));
  CHECK(snapshot->pool_binding().storage().same_storage(successor.view()));
}

TEST_CASE("concurrent binding admission never admits twice") {
  using namespace hgraph;
  // One race is a weak probe: a check-then-set admission passes it most of the
  // time.  Repeating over a fresh binding each round makes a non-atomic
  // admission fail reliably rather than occasionally.
  constexpr std::size_t rounds = 48;
  constexpr std::size_t contenders = 8;
  std::size_t double_admissions = 0;
  std::size_t torn_views = 0;
  for (std::size_t round = 0; round < rounds; ++round) {
    CompoundScalarStorageBinding binding{};
    const auto race = race_admission(binding, contenders);
    if (race.admitted != 1) {
      ++double_admissions;
    }
    if (race.owners != 1) {
      ++torn_views;
    }
    CHECK(race.admitted + race.refused == contenders);
  }
  CHECK(double_admissions == 0);
  CHECK(torn_views == 0);
}

TEST_CASE("abstract Bundle without a concrete alternative cannot be realized") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);
  const auto *abstract = registry.bundle("tests.realization", "UninhabitedBase",
                                         {{"id", integer}}, {}, true);

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  REQUIRE(snapshot->is_polymorphic(abstract));
  REQUIRE_THROWS_AS(snapshot->type_for(abstract), std::logic_error);
}

TEST_CASE("canonical and realized views have equal logical hashes") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  const auto *base = registry.bundle(
      "tests.realization.hash", "Base", {{"id", integer}}, {}, true);
  const auto *leaf = registry.bundle(
      "tests.realization.hash", "Leaf",
      {{"id", integer}, {"name", text}}, {base});

  Value canonical{ValuePlanFactory::instance().type_for(leaf)};
  auto canonical_fields = canonical.as_bundle().begin_mutation();
  canonical_fields["id"].set(Int{7});
  canonical_fields["name"].set(Str{"seven"});

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  const auto realized_base = snapshot->type_for(base);
  Value realized{realized_base};
  auto destination = realized.begin_mutation();
  realized_base.ops_ref().copy_assign_from(
      realized_base, destination.mutable_data(), canonical.binding(),
      canonical.view().data());

  REQUIRE(canonical.view().equals(realized.view()));
  REQUIRE(realized.view().equals(canonical.view()));
  REQUIRE(canonical.view().hash() == realized.view().hash());
}

TEST_CASE("KeySlotStore supports heterogeneous realized value lookup") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  const auto *base = registry.bundle(
      "tests.realization.key_store", "Base", {{"id", integer}}, {}, true);
  const auto *leaf = registry.bundle(
      "tests.realization.key_store", "Leaf",
      {{"id", integer}, {"name", text}}, {base});

  const auto canonical_binding = ValuePlanFactory::instance().type_for(leaf);
  Value canonical{canonical_binding};
  auto canonical_fields = canonical.as_bundle().begin_mutation();
  canonical_fields["id"].set(Int{7});
  canonical_fields["name"].set(Str{"seven"});

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  const auto realized_binding = snapshot->type_for(base);
  Value realized{realized_binding};
  auto realized_destination = realized.begin_mutation();
  realized_binding.ops_ref().copy_assign_from(
      realized_binding, realized_destination.mutable_data(), canonical.binding(),
      canonical.view().data());

  KeySlotStore realized_store{realized_binding};
  const auto inserted = realized_store.insert(canonical.view());
  REQUIRE(inserted.inserted);
  REQUIRE(inserted.constructed);
  REQUIRE(realized_store.find_slot(realized.view()) == inserted.slot);
  REQUIRE_FALSE(realized_store.insert(realized.view()).inserted);
  REQUIRE(realized_store.size() == 1);
  REQUIRE(ValueView{realized_binding, realized_store.key_memory(inserted.slot)}
              .equals(canonical.view()));

  KeySlotStore canonical_store{canonical_binding};
  const auto reverse_inserted = canonical_store.insert(canonical.view());
  REQUIRE(reverse_inserted.inserted);
  REQUIRE(reverse_inserted.constructed);
  REQUIRE(canonical_store.find_slot(realized.view()) == reverse_inserted.slot);
  REQUIRE_FALSE(canonical_store.insert(realized.view()).inserted);
  REQUIRE(canonical_store.size() == 1);
}

TEST_CASE("closed unions convert alternatives between realization snapshots") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *inner_base = registry.bundle(
      "tests.realization.snapshot_conversion", "InnerBase", {{"id", integer}});
  registry.bundle("tests.realization.snapshot_conversion", "InnerSmall",
                  {{"id", integer}, {"small", integer}}, {inner_base});
  const auto *outer_base = registry.bundle(
      "tests.realization.snapshot_conversion", "OuterBase", {}, {}, true);
  const auto *outer_leaf = registry.bundle(
      "tests.realization.snapshot_conversion", "OuterLeaf",
      {{"inner", inner_base}}, {outer_base});

  const auto first = TypeRealizationSnapshot::capture(registry);
  const auto first_outer = first->type_for(outer_base);
  Value canonical{ValuePlanFactory::instance().type_for(outer_leaf)};
  canonical.as_bundle().begin_mutation()["inner"]
      .as_bundle().begin_mutation()["id"].set(Int{7});
  Value older{first_outer};
  auto older_destination = older.begin_mutation();
  first_outer.ops_ref().copy_assign_from(
      first_outer, older_destination.mutable_data(), canonical.binding(),
      canonical.view().data());

  registry.bundle("tests.realization.snapshot_conversion", "InnerLarge",
                  {{"id", integer}, {"large", integer}, {"extra", integer}},
                  {inner_base});
  const auto second = TypeRealizationSnapshot::capture(registry);
  const auto second_outer = second->type_for(outer_base);
  REQUIRE(first->type_for(outer_leaf) != second->type_for(outer_leaf));

  Value copied{second_outer};
  auto copied_destination = copied.begin_mutation();
  second_outer.ops_ref().copy_assign_from(
      second_outer, copied_destination.mutable_data(), older.binding(),
      older.view().data());
  REQUIRE(copied.view().concrete().schema() == outer_leaf);
  REQUIRE(copied.view().concrete().as_bundle()["inner"]
              .concrete().as_bundle()["id"].checked_as<Int>() == 7);

  Value moved{second_outer};
  auto moved_destination = moved.begin_mutation();
  auto older_source = older.begin_mutation();
  second_outer.ops_ref().move_assign_from(
      second_outer, moved_destination.mutable_data(), older.binding(),
      older_source.mutable_data());
  REQUIRE(moved.view().concrete().schema() == outer_leaf);
  REQUIRE(moved.view().concrete().as_bundle()["inner"]
              .concrete().as_bundle()["id"].checked_as<Int>() == 7);
}

TEST_CASE("realized Base lists preserve mixed derived element types") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  const auto *base = registry.bundle(
      "tests.realization", "MixedListBase", {{"id", integer}}, {}, true);
  const auto *named = registry.bundle(
      "tests.realization", "MixedListNamed",
      {{"id", integer}, {"name", text}}, {base});
  const auto *sized = registry.bundle(
      "tests.realization", "MixedListSized",
      {{"id", integer}, {"size", integer}}, {base});
  const auto *list_schema = registry.mutable_list(base);

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  Value list{snapshot->type_for(list_schema)};
  Value first{ValuePlanFactory::instance().type_for(named)};
  Value second{ValuePlanFactory::instance().type_for(sized)};
  first.as_bundle().begin_mutation()["name"].set(std::string{"first"});
  second.as_bundle().begin_mutation()["size"].set(std::int64_t{2});

  auto values = list.as_list().begin_mutation();
  values.push_back(first.view());
  values.push_back(second.view());

  REQUIRE(values.at(0).concrete().schema() == named);
  REQUIRE(values.at(1).concrete().schema() == sized);
}

TEST_CASE("closed Bundle accepts a canonical alternative with realized fields") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *model = registry.bundle(
      "tests.realization.canonical_source", "Model", {{"id", integer}});
  const auto *concrete_model = registry.bundle(
      "tests.realization.canonical_source", "ConcreteModel",
      {{"id", integer}, {"parameter", integer}}, {model});
  REQUIRE(concrete_model != nullptr);
  const auto *base = registry.bundle(
      "tests.realization.canonical_source", "Base", {{"id", integer}}, {},
      true);
  const auto *derived = registry.bundle(
      "tests.realization.canonical_source", "Derived",
      {{"id", integer}, {"model", model}}, {base});

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  const auto realized_base = snapshot->type_for(base);
  const auto realized_derived = snapshot->exact_type_for(derived);
  const auto canonical_derived = ValuePlanFactory::instance().type_for(derived);
  REQUIRE(realized_derived != canonical_derived);

  Value source{canonical_derived};
  auto source_fields = source.as_bundle().begin_mutation();
  source_fields["id"].set(Int{1});

  Value target{realized_base};
  auto mutable_target = target.begin_mutation();
  realized_base.ops_ref().copy_assign_from(
      realized_base, mutable_target.mutable_data(), canonical_derived,
      source.view().data());

  const auto concrete = target.view().concrete();
  REQUIRE(concrete.schema() == derived);
  REQUIRE(concrete.as_bundle()["id"].checked_as<Int>() == 1);
}

TEST_CASE(
    "closed Bundle realization owns only an indirect recursive union edge") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *instrument = registry.bundle(
      "tests.realization", "RecursiveInstrument", {{"id", integer}});
  const auto *specification =
      registry.bundle("tests.realization", "RecursiveSpecification",
                      {{"underlying", instrument}});
  const auto *series = registry.bundle("tests.realization", "RecursiveSeries",
                                       {{"specification", specification}});
  const auto *future =
      registry.bundle("tests.realization", "RecursiveFuture",
                      {{"id", integer}, {"series", series}}, {instrument});

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  const auto realized_future = snapshot->type_for(future);
  const auto realized_instrument = snapshot->type_for(instrument);
  const auto graph_future = snapshot->graph_type_for(future);
  REQUIRE(realized_instrument !=
          ValuePlanFactory::instance().type_for(instrument));
  REQUIRE(realized_future != ValuePlanFactory::instance().type_for(future));
  REQUIRE(graph_future != ValuePlanFactory::instance().type_for(future));

  Value value{realized_future};
  auto future_fields = value.as_bundle().begin_mutation();
  REQUIRE(future_fields.size() == 2);
  auto series_fields = future_fields["series"].as_bundle().begin_mutation();
  REQUIRE(series_fields.size() == 1);
  auto specification_fields =
      series_fields["specification"].as_bundle().begin_mutation();
  REQUIRE(specification_fields.size() == 1);
  auto underlying = specification_fields["underlying"];
  REQUIRE(underlying.binding().schema()->is_owned());
  REQUIRE(underlying.binding().schema()->element_type == instrument);
  REQUIRE(underlying.binding().checked_plan().layout.size == sizeof(void *));

  TypeRealizationScope scope{snapshot.get()};
  auto underlying_fields = underlying.as_bundle().begin_mutation();
  REQUIRE(underlying_fields.size() == 1);
  underlying_fields["id"].set(std::int64_t{42});
  REQUIRE(underlying.as_bundle()["id"].checked_as<std::int64_t>() == 42);
}

TEST_CASE(
    "indirect recursive Bundle realization is independent of entry order") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *instrument = registry.bundle(
      "tests.realization.entry_order", "Instrument", {{"id", integer}});
  const auto *specification = registry.bundle(
      "tests.realization.entry_order", "Specification",
      {{"underlying", instrument}});
  const auto *series = registry.bundle(
      "tests.realization.entry_order", "Series",
      {{"specification", specification}});
  const auto *future = registry.bundle(
      "tests.realization.entry_order", "Future",
      {{"id", integer}, {"series", series}}, {instrument});

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  REQUIRE_NOTHROW(snapshot->type_for(series));

  const auto realized_future = snapshot->type_for(future);
  Value future_value{realized_future};
  auto future_fields = future_value.as_bundle().begin_mutation();
  REQUIRE(future_fields["series"].binding().schema()->is_owned());
  REQUIRE(future_fields["series"].binding().schema()->element_type == series);
}

TEST_CASE(
    "polymorphic Bundle JSON requires and consumes an external discriminator") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);

  const auto *base =
      registry.bundle("tests.json", "JsonBase", {{"id", integer}}, {}, true);
  const auto *child =
      registry.bundle("tests.json", "JsonChild",
                      {{"id", integer}, {"quantity", integer}}, {base});
  const auto *holder =
      registry.bundle("tests.json", "JsonHolder", {{"item", base}});
  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  TypeRealizationScope scope{snapshot.get()};

  REQUIRE_THROWS_AS(from_json_string(base, R"({"id": 1})"),
                    std::invalid_argument);
  Value value = from_json_string(
      base, R"({"__type__": "tests.json::JsonChild", "id": 1, "quantity": 2})");
  REQUIRE(value.binding() == snapshot->type_for(base));
  REQUIRE(value.view().concrete().schema() == child);

  const std::string encoded = to_json_string(value.view());
  REQUIRE(encoded.find(R"("__type__": "tests.json::JsonChild")") !=
          std::string::npos);
  Value round_trip = from_json_string(base, encoded);
  REQUIRE(round_trip.view().concrete().schema() == child);

  Value nested = from_json_string(
      holder,
      R"({"item": {"__type__": "tests.json::JsonChild", "id": 3, "quantity": 4}})");
  REQUIRE(nested.binding() == snapshot->type_for(holder));
  REQUIRE(nested.as_bundle()["item"].concrete().schema() == child);
  Value nested_round_trip =
      from_json_string(holder, to_json_string(nested.view()));
  REQUIRE(nested_round_trip.as_bundle()["item"].concrete().schema() == child);
}

TEST_CASE(
    "polymorphic Bundle JSON supports configured discriminator values") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *text = registry.value_type("str");
  REQUIRE(integer != nullptr);
  REQUIRE(text != nullptr);

  SECTION("external discriminator") {
    const auto *base = registry.bundle(
        "tests.json.alias", "LegacyBase", {{"id", integer}}, {}, true,
        "kind");
    const auto *child = registry.bundle(
        "tests.json.alias", "LegacyChild",
        {{"id", integer}, {"quantity", integer}}, {base}, false, "kind", {},
        "LSCS");
    const auto snapshot = TypeRealizationSnapshot::capture(registry);
    TypeRealizationScope scope{snapshot.get()};

    REQUIRE(child->bundle_discriminator_value() == "LSCS");
    REQUIRE(child->matches_bundle_discriminator("LSCS"));
    Value value = from_json_string(
        base, R"({"kind": "LSCS", "id": 1, "quantity": 2})");
    REQUIRE(value.view().concrete().schema() == child);
    REQUIRE(to_json_string(value.view()).find(R"("kind": "LSCS")") !=
            std::string::npos);
  }

  SECTION("discriminator stored as a Bundle field") {
    const auto *base = registry.bundle(
        "tests.json.field_alias", "LegacyFieldBase",
        {{"id", integer}, {"kind", text}}, {}, true, "kind");
    const auto *child = registry.bundle(
        "tests.json.field_alias", "LegacyFieldChild",
        {{"id", integer}, {"kind", text}, {"quantity", integer}}, {base},
        false, "kind", {}, "LSCS");
    const auto snapshot = TypeRealizationSnapshot::capture(registry);
    TypeRealizationScope scope{snapshot.get()};

    Value value = from_json_string(
        base, R"({"id": 1, "kind": "LSCS", "quantity": 2})");
    REQUIRE(value.view().concrete().schema() == child);
    const std::string encoded = to_json_string(value.view());
    REQUIRE(encoded.find(R"("kind": "LSCS")") != std::string::npos);
    REQUIRE(encoded.find("kind", encoded.find("kind") + 1) ==
            std::string::npos);
    REQUIRE(from_json_string(base, encoded).view().concrete().schema() ==
            child);
  }
}

TEST_CASE("TypeRegistry assigns exact stable canonical labels to every value "
          "schema family") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *i = registry.register_scalar<LabelScalarA>("LabelInt");
  const auto *s = registry.register_scalar<LabelScalarB>("LabelStr");
  const auto *anonymous = registry.register_scalar<AnonymousLabelScalar>();
  const auto label = [](const ValueTypeMetaData *meta) {
    return std::string{meta->name()};
  };

  REQUIRE(label(i) == "LabelInt");
  REQUIRE_FALSE(anonymous->name().empty());
  REQUIRE(label(anonymous) == typeid(AnonymousLabelScalar).name());
  REQUIRE(label(registry.tuple({i, s})) == "Tuple[LabelInt,LabelStr]");
  REQUIRE(label(registry.un_named_bundle({{"field", i}, {"other", s}})) ==
          "Bundle{field:LabelInt,other:LabelStr}");
  REQUIRE(label(registry.bundle("LabelBundle", {{"field", i}})) ==
          "LabelBundle");
  REQUIRE(label(registry.list(i)) == "List[LabelInt]");
  REQUIRE(label(registry.list(i, 4)) == "List[LabelInt,4]");
  REQUIRE(label(registry.list(i, 0, true)) == "VariadicTuple[LabelInt]");
  REQUIRE(label(registry.nullable_tuple(i)) == "NullableTuple[LabelInt]");
  REQUIRE(label(registry.mutable_list(i)) == "MutableList[LabelInt]");
  REQUIRE(label(registry.set(i)) == "Set[LabelInt]");
  REQUIRE(label(registry.mutable_set(i)) == "MutableSet[LabelInt]");
  REQUIRE(label(registry.map(i, s)) == "Map[LabelInt,LabelStr]");
  REQUIRE(label(registry.mutable_map(i, s)) == "MutableMap[LabelInt,LabelStr]");
  REQUIRE(label(registry.cyclic_buffer(i, 4)) == "CyclicBuffer[LabelInt,4]");
  REQUIRE(label(registry.queue(i)) == "Queue[LabelInt]");
  REQUIRE(label(registry.queue(i, 4)) == "Queue[LabelInt,4]");
  const auto *typed_series = registry.series(i);
  const auto *typed_frame =
      registry.frame(registry.bundle("LabelFrameColumns", {{"field", i}}));
  REQUIRE(label(typed_series) == "series[LabelInt]");
  REQUIRE(label(typed_frame) == "frame[LabelFrameColumns]");
  REQUIRE(registry.is_series(typed_series));
  REQUIRE_FALSE(registry.is_frame(typed_series));
  REQUIRE(registry.is_frame(typed_frame));
  REQUIRE_FALSE(registry.is_series(typed_frame));
  REQUIRE(label(registry.any()) == "Any");
  REQUIRE(label(registry.json()) == "JSON");
  REQUIRE(label(registry.list(nullptr)) == "List[<unresolved>]");

  const char *canonical_label = i->schema_header().label;
  registry.register_value_type_alias("LabelIntAlias", i);
  REQUIRE(registry.value_type("LabelIntAlias") == i);
  REQUIRE(i->schema_header().label == canonical_label);
  REQUIRE(label(i) == "LabelInt");

  const auto *structural = registry.un_named_bundle({{"field", i}});
  const char *structural_label = structural->schema_header().label;
  registry.register_value_type_alias("StructuralAlias", structural);
  REQUIRE(structural->schema_header().label == structural_label);
  REQUIRE(label(structural) == "Bundle{field:LabelInt}");
  REQUIRE(structural->is_un_named_bundle());
  REQUIRE_FALSE(structural->is_named_bundle());
}

TEST_CASE("TypeRegistry serializes concurrent equal schema requests") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *i = registry.register_scalar<LabelScalarA>("ConcurrentInt");
  const auto *s = registry.register_scalar<LabelScalarB>("ConcurrentStr");

  constexpr std::size_t thread_count = 8;
  std::array<const ValueTypeMetaData *, thread_count> tuples{};
  std::array<const ValueTypeMetaData *, thread_count> bundles{};
  std::array<const ValueTypeMetaData *, thread_count> maps{};
  std::array<std::thread, thread_count> threads;
  for (std::size_t index = 0; index < thread_count; ++index) {
    threads[index] = std::thread([&, index] {
      for (int iteration = 0; iteration < 100; ++iteration) {
        tuples[index] = registry.tuple({i, s});
        bundles[index] =
            registry.bundle("ConcurrentBundle", {{"i", i}, {"s", s}});
        maps[index] = registry.map(i, s);
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }

  for (std::size_t index = 1; index < thread_count; ++index) {
    REQUIRE(tuples[index] == tuples[0]);
    REQUIRE(bundles[index] == bundles[0]);
    REQUIRE(maps[index] == maps[0]);
  }
  REQUIRE(std::string{tuples[0]->name()} ==
          "Tuple[ConcurrentInt,ConcurrentStr]");
  REQUIRE(std::string{bundles[0]->name()} == "ConcurrentBundle");
  REQUIRE(std::string{maps[0]->name()} == "Map[ConcurrentInt,ConcurrentStr]");
}

TEST_CASE("TypeRegistry::list distinguishes fixed and dynamic forms") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

  const auto *fixed = registry.list(int_meta, 4, false);
  const auto *dynamic = registry.list(int_meta, 0, false);
  const auto *fixed2 = registry.list(int_meta, 4, false);

  REQUIRE(fixed != dynamic);
  REQUIRE(fixed == fixed2);
  REQUIRE(fixed->is_fixed_size());
  REQUIRE(fixed->fixed_size == 4);
  REQUIRE(!dynamic->is_fixed_size());
}

TEST_CASE("TypeRegistry: set, map, cyclic_buffer and queue intern correctly") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

  const auto *s = registry.set(int_meta);
  REQUIRE(s->value_kind() == ValueTypeKind::Set);
  REQUIRE(s == registry.set(int_meta));
  REQUIRE(s->element_type == int_meta);

  const auto *m = registry.map(int_meta, int_meta);
  REQUIRE(m->value_kind() == ValueTypeKind::Map);
  REQUIRE(m == registry.map(int_meta, int_meta));
  REQUIRE(m->key_type == int_meta);
  REQUIRE(m->element_type == int_meta);

  const auto *cb = registry.cyclic_buffer(int_meta, 8);
  REQUIRE(cb->value_kind() == ValueTypeKind::CyclicBuffer);
  REQUIRE(cb->fixed_size == 8);
  REQUIRE(cb->is_hashable());
  REQUIRE(cb->is_equatable());
  REQUIRE(cb->is_comparable());
  REQUIRE(cb->is_buffer_compatible());
  REQUIRE(cb == registry.cyclic_buffer(int_meta, 8));

  const auto *q = registry.queue(int_meta, 16);
  REQUIRE(q->value_kind() == ValueTypeKind::Queue);
  REQUIRE(q->fixed_size == 16);
  REQUIRE(q->is_hashable());
  REQUIRE(q->is_equatable());
  REQUIRE(q->is_comparable());
  REQUIRE(q->is_buffer_compatible());
  REQUIRE(q == registry.queue(int_meta, 16));
}

TEST_CASE("TypeRegistry::ts / tss / tsd / tsl / tsw intern correctly") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

  const auto *ts_int = registry.ts(int_meta);
  REQUIRE(ts_int->kind == TSTypeKind::TS);
  REQUIRE(ts_int == registry.ts(int_meta));

  const auto *tss = registry.tss(int_meta);
  REQUIRE(tss->kind == TSTypeKind::TSS);
  REQUIRE(tss == registry.tss(int_meta));

  const auto *tsd = registry.tsd(int_meta, ts_int);
  REQUIRE(tsd->kind == TSTypeKind::TSD);
  REQUIRE(tsd->key_type() == int_meta);
  REQUIRE(tsd->element_ts() == ts_int);

  const auto *tsl_fixed = registry.tsl(ts_int, 4);
  REQUIRE(tsl_fixed->kind == TSTypeKind::TSL);
  REQUIRE(tsl_fixed->fixed_size() == 4);
  REQUIRE(tsl_fixed->element_ts() == ts_int);

  const auto *tsl_dynamic = registry.tsl(ts_int, 0);
  REQUIRE(tsl_dynamic != tsl_fixed);
  REQUIRE(tsl_dynamic->fixed_size() == 0);

  const auto *tsw_tick = registry.tsw(int_meta, 10, 5);
  REQUIRE(tsw_tick->kind == TSTypeKind::TSW);
  REQUIRE(!tsw_tick->is_duration_based());
  REQUIRE(tsw_tick->period() == 10);
  REQUIRE(tsw_tick->min_period() == 5);

  const auto *tsw_dur =
      registry.tsw_duration(int_meta, TimeDelta{1000}, TimeDelta{500});
  REQUIRE(tsw_dur->kind == TSTypeKind::TSW);
  REQUIRE(tsw_dur->is_duration_based());
  REQUIRE(tsw_dur->time_range() == TimeDelta{1000});
  REQUIRE(tsw_dur->min_time_range() == TimeDelta{500});
  REQUIRE(tsw_tick != tsw_dur);
}

TEST_CASE("TypeRegistry::tsb stores fields and registers the alias") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *ts_int = registry.ts(int_meta);

  const auto *tsb =
      registry.tsb("TestTSBundleA", {{"a", ts_int}, {"b", ts_int}});
  REQUIRE(tsb->kind == TSTypeKind::TSB);
  REQUIRE(tsb->field_count() == 2);
  REQUIRE(std::string(tsb->fields()[0].name) == "a");
  REQUIRE(std::string(tsb->fields()[1].name) == "b");
  REQUIRE(tsb->fields()[0].type == ts_int);

  REQUIRE(registry.time_series_type("TestTSBundleA") == tsb);
  REQUIRE(registry.tsb("TestTSBundleA", {{"a", ts_int}, {"b", ts_int}}) == tsb);
}

TEST_CASE("TypeRegistry::tsb reuses a qualified CompoundScalar value schema") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  REQUIRE(integer != nullptr);
  const auto *bundle =
      registry.bundle("tests.tsb", "QualifiedValue", {{"value", integer}});
  const auto *tsb =
      registry.tsb(bundle->name(), {{"value", registry.ts(integer)}});

  REQUIRE(tsb->value_schema == bundle);
  REQUIRE(std::string{tsb->name()} == std::string{bundle->name()});
}

TEST_CASE("TypeRegistry::tsb lifts Bundle value fields without inferring "
          "time-series containers") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.value_type("int");
  const auto *string = registry.value_type("str");
  const auto *items = registry.list(integer);
  const auto *child =
      registry.bundle("tests.tsb", "LiftedChild", {{"label", string}});
  const auto *model = registry.bundle(
      "tests.tsb", "LiftedModel",
      {{"number", integer}, {"items", items}, {"child", child}});

  const auto *lifted = registry.tsb(model);

  REQUIRE(lifted == registry.tsb(model));
  REQUIRE(lifted->is_named_tsb());
  REQUIRE(lifted->value_schema == model);
  REQUIRE(lifted->field_count() == 3);
  REQUIRE(lifted->fields()[0].type == registry.ts(integer));
  REQUIRE(lifted->fields()[1].type == registry.ts(items));
  REQUIRE(lifted->fields()[2].type == registry.ts(child));
  REQUIRE_FALSE(lifted->fields()[1].type->kind == TSTypeKind::TSL);
  REQUIRE_FALSE(lifted->fields()[2].type->kind == TSTypeKind::TSB);

  const auto *structural =
      registry.un_named_bundle({{"number", integer}, {"items", items}});
  REQUIRE(registry.tsb(structural)->is_un_named_tsb());
  REQUIRE_THROWS_AS(registry.tsb(integer), std::invalid_argument);
  REQUIRE_THROWS_AS(
      registry.tsb(static_cast<const ValueTypeMetaData *>(nullptr)),
      std::invalid_argument);
}

TEST_CASE("TypeRegistry: un_named_tsb and tsb distinguish structural vs "
          "nominal identity") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *ts_int = registry.ts(int_meta);

  const std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields{
      {"a", ts_int}, {"b", ts_int}};

  // Same field list -> same canonical un-named TSB.
  const auto *u1 = registry.un_named_tsb(fields);
  const auto *u2 = registry.un_named_tsb(fields);
  REQUIRE(u1 == u2);
  REQUIRE(u1->is_un_named_tsb());
  REQUIRE_FALSE(u1->is_named_tsb());
  REQUIRE(u1->schema_header().valid());
  REQUIRE_FALSE(u1->name().empty());
  REQUIRE(u1->wrapped_un_named_tsb() == nullptr);
  // The un-named TSB's value-side bundle is the matching un-named Bundle.
  REQUIRE(u1->value_type != nullptr);
  REQUIRE(u1->value_type->is_un_named_bundle());

  // tsb(name, fields) wraps the un-named TSB.
  const auto *named_x1 = registry.tsb("TSBNamedX", fields);
  const auto *named_x2 = registry.tsb("TSBNamedX", fields);
  REQUIRE(named_x1 == named_x2);
  REQUIRE(named_x1->is_named_tsb());
  REQUIRE(named_x1->wrapped_un_named_tsb() == u1);
  REQUIRE(std::string(named_x1->name()) == std::string("TSBNamedX"));
  // Field array shared with the un-named twin.
  REQUIRE(named_x1->fields() == u1->fields());
  REQUIRE(named_x1->field_count() == u1->field_count());
  // Value-side bundle is the matching named Bundle (nominal identity flows
  // through).
  REQUIRE(named_x1->value_type != nullptr);
  REQUIRE(named_x1->value_type->is_named_bundle());

  // Different names with the same fields are distinct named TSBs.
  const auto *named_y = registry.tsb("TSBNamedY", fields);
  REQUIRE(named_y != named_x1);
  REQUIRE(named_y->wrapped_un_named_tsb() == u1); // share the un-named twin
  REQUIRE(named_y->value_type != named_x1->value_type);

  // Named TSB ≠ un-named TSB.
  REQUIRE(named_x1 != u1);

  // tsb(...) requires a non-empty name.
  REQUIRE_THROWS_AS(registry.tsb("", fields), std::invalid_argument);

  // named_tsb() lookup.
  REQUIRE(registry.named_tsb("TSBNamedX") == named_x1);
  REQUIRE(registry.named_tsb("TSBNamedY") == named_y);
  REQUIRE(registry.named_tsb("DoesNotExist") == nullptr);
  // SIGNAL is registered in the TS name space but isn't a named TSB.
  (void)registry.signal();
  REQUIRE(registry.named_tsb("SIGNAL") == nullptr);

  // TSB name namespace is unique: same name + same fields is idempotent,
  // same name + different fields is rejected.
  const std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
      different_fields{{"a", ts_int}, {"c", ts_int}};
  REQUIRE_NOTHROW(registry.tsb("TSBNamedX", fields)); // same shape -> OK
  REQUIRE_THROWS_AS(
      registry.tsb("TSBNamedX",
                   different_fields), // different shape -> conflict
      std::invalid_argument);
}

TEST_CASE("TypeRegistry::signal returns a single canonical instance") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *s1 = registry.signal();
  const auto *s2 = registry.signal();
  REQUIRE(s1 == s2);
  REQUIRE(s1->kind == TSTypeKind::SIGNAL);
  REQUIRE(registry.time_series_type("SIGNAL") == s1);
}

TEST_CASE("TypeRegistry::ref creates the TimeSeriesReference singleton") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *ts_int = registry.ts(int_meta);

  const auto *r1 = registry.ref(ts_int);
  const auto *r2 = registry.ref(ts_int);
  REQUIRE(r1 == r2);
  REQUIRE(r1->kind == TSTypeKind::REF);
  REQUIRE(r1->referenced_ts() == ts_int);
  REQUIRE(registry.ref(r1) == r1);
}

TEST_CASE("TypeRegistry::contains_ref recurses through composite TS kinds") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *ts_int = registry.ts(int_meta);
  const auto *ref_int = registry.ref(ts_int);

  REQUIRE(!TypeRegistry::contains_ref(ts_int));
  REQUIRE(TypeRegistry::contains_ref(ref_int));

  const auto *bundle_with_ref =
      registry.tsb("TestRefBundleA", {{"r", ref_int}});
  const auto *bundle_without_ref =
      registry.tsb("TestRefBundleB", {{"v", ts_int}});
  REQUIRE(TypeRegistry::contains_ref(bundle_with_ref));
  REQUIRE(!TypeRegistry::contains_ref(bundle_without_ref));

  const auto *list_of_refs = registry.tsl(ref_int);
  REQUIRE(TypeRegistry::contains_ref(list_of_refs));

  const auto *dict_with_ref = registry.tsd(int_meta, ref_int);
  REQUIRE(TypeRegistry::contains_ref(dict_with_ref));

  const auto *nested_list_dict_ref =
      registry.tsl(registry.tsd(int_meta, ref_int));
  REQUIRE(TypeRegistry::contains_ref(nested_list_dict_ref));
}

TEST_CASE(
    "TypeRegistry::dereference unwraps refs and recurses into containers") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *ts_int = registry.ts(int_meta);
  const auto *ref_int = registry.ref(ts_int);

  REQUIRE(registry.dereference(ref_int) == ts_int);
  REQUIRE(registry.dereference(ts_int) == ts_int);

  const auto *list_of_refs = registry.tsl(ref_int);
  const auto *list_deref = registry.dereference(list_of_refs);
  REQUIRE(list_deref != list_of_refs);
  REQUIRE(list_deref->kind == TSTypeKind::TSL);
  REQUIRE(list_deref->element_ts() == ts_int);

  const auto *list_of_ts = registry.tsl(ts_int);
  REQUIRE(registry.dereference(list_of_ts) == list_of_ts);
}

TEST_CASE("TypeRegistry::synthetic_atomic interns by name") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();

  // Trigger ref() to indirectly create the synthetic TimeSeriesReference
  // atomic.
  const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
  const auto *ts_int = registry.ts(int_meta);
  (void)registry.ref(ts_int);

  const auto *synthetic = registry.value_type("TimeSeriesReference");
  REQUIRE(synthetic != nullptr);
  REQUIRE(synthetic->value_kind() == ValueTypeKind::Atomic);
  REQUIRE(synthetic->is_hashable());
  REQUIRE(synthetic->is_equatable());
}

TEST_CASE("TypeRegistry: value_type and time_series_type return null for "
          "unknown names") {
  using namespace hgraph;
  auto &registry = TypeRegistry::instance();
  REQUIRE(registry.value_type("nonexistent_value_type_xyzzy") == nullptr);
  REQUIRE(registry.time_series_type("nonexistent_ts_type_xyzzy") == nullptr);
}
