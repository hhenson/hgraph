#include <hgraph/types/value/view_range.h>
#include <hgraph/types/value/type_meta.h>

namespace hgraph::value {

// ============================================================================
// ViewPairRange Factory Methods
// ============================================================================

namespace {

ViewPairRange::Pair bundle_pair_at(const void* data, const TypeMeta* schema, size_t index) {
    const void* field_data = schema->ops().at(data, index, schema);
    const TypeMeta* field_schema = schema->fields[index].type;
    // The name is a stable const char* from the registry string pool.
    // We expose it as a View over that string pointer.
    return {
        View(schema->fields[index].name, TypeMeta::get<std::string>()),
        View(field_data, field_schema)
    };
}

ViewPairRange::Pair tuple_pair_at(const void* data, const TypeMeta* schema, size_t index) {
    const void* elem_data = schema->ops().at(data, index, schema);
    const TypeMeta* elem_schema = schema->fields[index].type;
    return {
        View(&index, TypeMeta::get<size_t>()),
        View(elem_data, elem_schema)
    };
}

ViewPairRange::Pair list_pair_at(const void* data, const TypeMeta* schema, size_t index) {
    const void* elem_data = schema->ops().at(data, index, schema);
    return {
        View(&index, TypeMeta::get<size_t>()),
        View(elem_data, schema->element_type)
    };
}

} // anonymous namespace

ViewPairRange ViewPairRange::bundle_items(const View& view) {
    if (!view.valid() || view.schema()->kind != TypeKind::Bundle) return {};
    const auto* schema = view.schema();
    return {view.data(), schema, schema->field_count, &bundle_pair_at};
}

ViewPairRange ViewPairRange::tuple_items(const View& view) {
    if (!view.valid() || view.schema()->kind != TypeKind::Tuple) return {};
    const auto* schema = view.schema();
    return {view.data(), schema, schema->field_count, &tuple_pair_at};
}

ViewPairRange ViewPairRange::list_items(const View& view) {
    if (!view.valid() || view.schema()->kind != TypeKind::List) return {};
    const auto* schema = view.schema();
    size_t n = schema->ops().size(view.data(), schema);
    return {view.data(), schema, n, &list_pair_at};
}

} // namespace hgraph::value
