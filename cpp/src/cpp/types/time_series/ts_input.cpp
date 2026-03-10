#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/value/type_registry.h>

#include <mutex>
#include <unordered_map>

namespace hgraph
{
    const value::TypeMeta *TSInput::active_schema_from(const TSMeta *schema)
    {
        auto &registry = value::TypeRegistry::instance();
        const value::TypeMeta *bool_meta = value::scalar_type_meta<bool>();
        static std::unordered_map<const TSMeta *, const value::TypeMeta *> cache;
        static std::recursive_mutex cache_mutex;

        if (schema == nullptr) {
            return bool_meta;
        }

        std::lock_guard<std::recursive_mutex> lock(cache_mutex);
        if (auto it = cache.find(schema); it != cache.end()) {
            return it->second;
        }

        const value::TypeMeta *active_schema = bool_meta;
        switch (schema->kind) {
            case TSKind::TSValue:
            case TSKind::TSS:
            case TSKind::TSW:
            case TSKind::REF:
            case TSKind::SIGNAL:
                active_schema = bool_meta;
                break;

            case TSKind::TSB:
                {
                    auto builder = registry.tuple();
                    builder.add_element(bool_meta);
                    for (size_t i = 0; i < schema->field_count(); ++i) {
                        builder.add_element(active_schema_from(schema->fields()[i].ts_type));
                    }
                    active_schema = builder.build();
                    break;
                }

            case TSKind::TSL:
                {
                    const value::TypeMeta *child_active = active_schema_from(schema->element_ts());
                    const value::TypeMeta *child_collection = schema->fixed_size() > 0
                                                                  ? registry.fixed_list(child_active, schema->fixed_size()).build()
                                                                  : registry.list(child_active).build();
                    auto builder = registry.tuple();
                    builder.add_element(bool_meta);
                    builder.add_element(child_collection);
                    active_schema = builder.build();
                    break;
                }

            case TSKind::TSD:
                {
                    const value::TypeMeta *child_active = active_schema_from(schema->element_ts());
                    const value::TypeMeta *child_collection = registry.map(schema->key_type(), child_active).build();
                    auto builder = registry.tuple();
                    builder.add_element(bool_meta);
                    builder.add_element(child_collection);
                    active_schema = builder.build();
                    break;
                }
        }

        cache.emplace(schema, active_schema);
        return active_schema;
    }
}  // namespace hgraph
