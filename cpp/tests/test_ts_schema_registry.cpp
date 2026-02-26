#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/type_registry.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

std::string safe_name(const hgraph::value::TypeMeta* meta) {
    if (meta == nullptr || meta->name == nullptr) {
        return {};
    }
    return meta->name;
}

const hgraph::value::TypeMeta* field_at(const hgraph::value::TypeMeta* meta, size_t index) {
    if (meta == nullptr || meta->fields == nullptr || index >= meta->field_count) {
        return nullptr;
    }
    return meta->fields[index].type;
}

}  // namespace

TEST_CASE("TSTypeRegistry caches scalar TS and parallel schemas", "[ts_registry][schema]") {
    using namespace hgraph;
    using namespace hgraph::value;

    const TypeMeta* int_type = scalar_type_meta<int64_t>();
    auto& registry = TSTypeRegistry::instance();

    const TSMeta* ts_int_1 = registry.ts(int_type);
    const TSMeta* ts_int_2 = registry.ts(int_type);

    REQUIRE(ts_int_1 == ts_int_2);
    REQUIRE(ts_int_1->kind == TSKind::TSValue);
    REQUIRE(ts_int_1->value_type == int_type);

    REQUIRE(ts_int_1->time_schema() != nullptr);
    REQUIRE(ts_int_1->observer_schema() != nullptr);
    REQUIRE(ts_int_1->delta_value_schema() == nullptr);
    REQUIRE(ts_int_1->link_schema() != nullptr);
    REQUIRE(ts_int_1->input_link_schema() != nullptr);
    REQUIRE(ts_int_1->active_schema() != nullptr);

    REQUIRE(ts_int_1->link_schema()->kind == TypeKind::Atomic);
    REQUIRE(safe_name(ts_int_1->link_schema()) == "REFLink");
    REQUIRE(ts_int_1->input_link_schema()->kind == TypeKind::Atomic);
    REQUIRE(safe_name(ts_int_1->input_link_schema()) == "LinkTarget");
    REQUIRE(ts_int_1->active_schema()->kind == TypeKind::Atomic);
}

TEST_CASE("TSB link and active schemas preserve container slot layout", "[ts_registry][schema][tsb]") {
    using namespace hgraph;
    using namespace hgraph::value;

    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* double_type = scalar_type_meta<double>();
    const TypeMeta* bool_type = scalar_type_meta<bool>();

    const TSMeta* ts_double = registry.ts(double_type);
    const TSMeta* ts_bool = registry.ts(bool_type);
    const TSMeta* tsb_meta = registry.tsb({{"price", ts_double}, {"ready", ts_bool}}, "QuoteSchemaTest");

    REQUIRE(tsb_meta->kind == TSKind::TSB);
    REQUIRE(tsb_meta->field_count() == 2);
    REQUIRE(std::string(tsb_meta->fields()[0].name) == "price");
    REQUIRE(std::string(tsb_meta->fields()[1].name) == "ready");

    const TypeMeta* out_link = tsb_meta->link_schema();
    REQUIRE(out_link != nullptr);
    REQUIRE(out_link->kind == TypeKind::Tuple);
    REQUIRE(out_link->field_count == 3);
    REQUIRE(safe_name(field_at(out_link, 0)) == "REFLink");
    REQUIRE(safe_name(field_at(out_link, 1)) == "REFLink");
    REQUIRE(safe_name(field_at(out_link, 2)) == "REFLink");

    const TypeMeta* in_link = tsb_meta->input_link_schema();
    REQUIRE(in_link != nullptr);
    REQUIRE(in_link->kind == TypeKind::Tuple);
    REQUIRE(in_link->field_count == 3);
    REQUIRE(safe_name(field_at(in_link, 0)) == "LinkTarget");
    REQUIRE(safe_name(field_at(in_link, 1)) == "LinkTarget");
    REQUIRE(safe_name(field_at(in_link, 2)) == "LinkTarget");

    const TypeMeta* active = tsb_meta->active_schema();
    REQUIRE(active != nullptr);
    REQUIRE(active->kind == TypeKind::Tuple);
    REQUIRE(active->field_count == 3);
    REQUIRE(field_at(active, 0) == scalar_type_meta<bool>());
}

TEST_CASE("REF schemas produce concrete value_type and dereference removes REF", "[ts_registry][schema][ref]") {
    using namespace hgraph;
    using namespace hgraph::value;

    auto& registry = TSTypeRegistry::instance();

    const TypeMeta* int_type = scalar_type_meta<int64_t>();
    const TSMeta* ts_int = registry.ts(int_type);
    const TSMeta* ref_int = registry.ref(ts_int);

    REQUIRE(ref_int->kind == TSKind::REF);
    REQUIRE(ref_int->value_type != nullptr);
    REQUIRE(safe_name(ref_int->value_type) == "TimeSeriesReference");
    REQUIRE(ref_int->element_ts() == ts_int);

    REQUIRE(ref_int->link_schema() != nullptr);
    REQUIRE(ref_int->link_schema()->kind == TypeKind::Tuple);
    REQUIRE(ref_int->link_schema()->field_count == 2);
    REQUIRE(safe_name(field_at(ref_int->link_schema(), 0)) == "REFLink");

    REQUIRE(ref_int->input_link_schema() != nullptr);
    REQUIRE(ref_int->input_link_schema()->kind == TypeKind::Tuple);
    REQUIRE(ref_int->input_link_schema()->field_count == 2);
    REQUIRE(safe_name(field_at(ref_int->input_link_schema(), 0)) == "LinkTarget");

    const TSMeta* bundle_with_ref = registry.tsb(
        {{"value", ts_int}, {"value_ref", ref_int}},
        "BundleWithRefSchemaTest");

    REQUIRE(TSTypeRegistry::contains_ref(bundle_with_ref));

    const TSMeta* deref_bundle = registry.dereference(bundle_with_ref);
    REQUIRE(deref_bundle != nullptr);
    REQUIRE(deref_bundle != bundle_with_ref);
    REQUIRE(deref_bundle->kind == TSKind::TSB);
    REQUIRE(deref_bundle->field_count() == 2);
    REQUIRE(!TSTypeRegistry::contains_ref(deref_bundle));
    REQUIRE(deref_bundle->fields()[1].ts_type == ts_int);
}

TEST_CASE("TSD schemas expose map value schema and nested link/delta structures", "[ts_registry][schema][tsd]") {
    using namespace hgraph;
    using namespace hgraph::value;

    auto& registry = TSTypeRegistry::instance();
    const TypeMeta* int_type = scalar_type_meta<int64_t>();
    const TypeMeta* double_type = scalar_type_meta<double>();
    const TSMeta* ts_double = registry.ts(double_type);
    const TSMeta* tsd_meta = registry.tsd(int_type, ts_double);

    REQUIRE(tsd_meta->kind == TSKind::TSD);
    REQUIRE(tsd_meta->value_type != nullptr);
    REQUIRE(tsd_meta->value_type->kind == TypeKind::Map);
    REQUIRE(tsd_meta->value_type->key_type == int_type);
    REQUIRE(tsd_meta->value_type->element_type == double_type);

    const TypeMeta* out_link = tsd_meta->link_schema();
    REQUIRE(out_link != nullptr);
    REQUIRE(out_link->kind == TypeKind::Tuple);
    REQUIRE(out_link->field_count == 2);
    REQUIRE(safe_name(field_at(out_link, 0)) == "REFLink");
    REQUIRE(field_at(out_link, 1) != nullptr);
    REQUIRE(field_at(out_link, 1)->kind == TypeKind::List);
    REQUIRE(safe_name(field_at(out_link, 1)->element_type) == "REFLink");

    const TypeMeta* in_link = tsd_meta->input_link_schema();
    REQUIRE(in_link != nullptr);
    REQUIRE(in_link->kind == TypeKind::Tuple);
    REQUIRE(in_link->field_count == 2);
    REQUIRE(safe_name(field_at(in_link, 0)) == "LinkTarget");
    REQUIRE(field_at(in_link, 1) != nullptr);
    REQUIRE(field_at(in_link, 1)->kind == TypeKind::List);
    REQUIRE(safe_name(field_at(in_link, 1)->element_type) == "LinkTarget");

    const TypeMeta* delta = tsd_meta->delta_value_schema();
    REQUIRE(delta != nullptr);
    REQUIRE(delta->kind == TypeKind::Tuple);
    REQUIRE(delta->field_count >= 3);
}
