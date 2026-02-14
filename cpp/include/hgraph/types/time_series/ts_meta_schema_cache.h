#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/type_meta.h>

#include <mutex>
#include <unordered_map>

namespace hgraph {

/**
 * Parallel schema set derived from a TSMeta.
 */
struct TSMetaSchemaSet {
    const value::TypeMeta* value_schema{nullptr};
    const value::TypeMeta* time_schema{nullptr};
    const value::TypeMeta* observer_schema{nullptr};
    const value::TypeMeta* delta_schema{nullptr};
    const value::TypeMeta* link_schema{nullptr};
    const value::TypeMeta* input_link_schema{nullptr};
    const value::TypeMeta* active_schema{nullptr};
};

/**
 * Cache for generated parallel schemas used by TSValue and TSInput.
 */
class HGRAPH_EXPORT TSMetaSchemaCache {
public:
    static TSMetaSchemaCache& instance();

    const TSMetaSchemaSet& get(const TSMeta* meta);

private:
    TSMetaSchemaCache() = default;

    TSMetaSchemaSet generate(const TSMeta* meta);

    const value::TypeMeta* generate_time_schema_impl(const TSMeta* meta);
    const value::TypeMeta* generate_observer_schema_impl(const TSMeta* meta);
    const value::TypeMeta* generate_link_schema_impl(const TSMeta* meta, bool input_mode);
    const value::TypeMeta* generate_active_schema_impl(const TSMeta* meta);
    const value::TypeMeta* generate_delta_schema_impl(const TSMeta* meta);

    std::mutex mutex_;
    std::unordered_map<const TSMeta*, TSMetaSchemaSet> cache_;
};

}  // namespace hgraph
