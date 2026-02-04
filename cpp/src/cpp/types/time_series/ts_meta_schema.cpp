/**
 * @file ts_meta_schema.cpp
 * @brief Implementation of TSMeta schema generation functions.
 *
 * This file implements the schema generation logic for the four parallel
 * Value structures in TSValue: value_, time_, observer_, and delta_value_.
 */

#include <hgraph/types/time_series/ts_meta_schema.h>
#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/time_series/ts_reference_ops.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/types/value/composite_ops.h>

namespace hgraph {

// ============================================================================
// TSMetaSchemaCache Singleton
// ============================================================================

TSMetaSchemaCache& TSMetaSchemaCache::instance() {
    static TSMetaSchemaCache cache;
    return cache;
}

TSMetaSchemaCache::TSMetaSchemaCache() {
    // Initialize singleton TypeMetas

    // engine_time_t - register as scalar
    engine_time_meta_ = value::scalar_type_meta<engine_time_t>();

    // ObserverList
    auto obs_meta = std::make_unique<value::TypeMeta>();
    obs_meta->size = sizeof(ObserverList);
    obs_meta->alignment = alignof(ObserverList);
    obs_meta->kind = value::TypeKind::Atomic;
    obs_meta->flags = value::TypeFlags::None;
    obs_meta->ops = ObserverListOps::ops();
    obs_meta->name = "ObserverList";
    obs_meta->element_type = nullptr;
    obs_meta->key_type = nullptr;
    obs_meta->fields = nullptr;
    obs_meta->field_count = 0;
    obs_meta->fixed_size = 0;
    observer_list_meta_ = obs_meta.get();
    owned_metas_.push_back(std::move(obs_meta));

    // SetDelta
    auto set_delta_m = std::make_unique<value::TypeMeta>();
    set_delta_m->size = sizeof(SetDelta);
    set_delta_m->alignment = alignof(SetDelta);
    set_delta_m->kind = value::TypeKind::Atomic;
    set_delta_m->flags = value::TypeFlags::None;
    set_delta_m->ops = SetDeltaOps::ops();
    set_delta_m->name = "SetDelta";
    set_delta_m->element_type = nullptr;
    set_delta_m->key_type = nullptr;
    set_delta_m->fields = nullptr;
    set_delta_m->field_count = 0;
    set_delta_m->fixed_size = 0;
    set_delta_meta_ = set_delta_m.get();
    owned_metas_.push_back(std::move(set_delta_m));

    // MapDelta
    auto map_delta_m = std::make_unique<value::TypeMeta>();
    map_delta_m->size = sizeof(MapDelta);
    map_delta_m->alignment = alignof(MapDelta);
    map_delta_m->kind = value::TypeKind::Atomic;
    map_delta_m->flags = value::TypeFlags::None;
    map_delta_m->ops = MapDeltaOps::ops();
    map_delta_m->name = "MapDelta";
    map_delta_m->element_type = nullptr;
    map_delta_m->key_type = nullptr;
    map_delta_m->fields = nullptr;
    map_delta_m->field_count = 0;
    map_delta_m->fixed_size = 0;
    map_delta_meta_ = map_delta_m.get();
    owned_metas_.push_back(std::move(map_delta_m));

    // BundleDeltaNav
    auto bundle_nav_m = std::make_unique<value::TypeMeta>();
    bundle_nav_m->size = sizeof(BundleDeltaNav);
    bundle_nav_m->alignment = alignof(BundleDeltaNav);
    bundle_nav_m->kind = value::TypeKind::Atomic;
    bundle_nav_m->flags = value::TypeFlags::None;
    bundle_nav_m->ops = BundleDeltaNavOps::ops();
    bundle_nav_m->name = "BundleDeltaNav";
    bundle_nav_m->element_type = nullptr;
    bundle_nav_m->key_type = nullptr;
    bundle_nav_m->fields = nullptr;
    bundle_nav_m->field_count = 0;
    bundle_nav_m->fixed_size = 0;
    bundle_delta_nav_meta_ = bundle_nav_m.get();
    owned_metas_.push_back(std::move(bundle_nav_m));

    // ListDeltaNav
    auto list_nav_m = std::make_unique<value::TypeMeta>();
    list_nav_m->size = sizeof(ListDeltaNav);
    list_nav_m->alignment = alignof(ListDeltaNav);
    list_nav_m->kind = value::TypeKind::Atomic;
    list_nav_m->flags = value::TypeFlags::None;
    list_nav_m->ops = ListDeltaNavOps::ops();
    list_nav_m->name = "ListDeltaNav";
    list_nav_m->element_type = nullptr;
    list_nav_m->key_type = nullptr;
    list_nav_m->fields = nullptr;
    list_nav_m->field_count = 0;
    list_nav_m->fixed_size = 0;
    list_delta_nav_meta_ = list_nav_m.get();
    owned_metas_.push_back(std::move(list_nav_m));

    // bool - for link flags (used in TSB per-field links)
    bool_meta_ = value::scalar_type_meta<bool>();

    // TSReference - for REF type values
    ts_reference_meta_ = value::scalar_type_meta<TSReference>();

    // LinkTarget - for direct links (internal use)
    auto link_target_m = std::make_unique<value::TypeMeta>();
    link_target_m->size = sizeof(LinkTarget);
    link_target_m->alignment = alignof(LinkTarget);
    link_target_m->kind = value::TypeKind::Atomic;
    link_target_m->flags = value::TypeFlags::None;
    link_target_m->ops = LinkTargetOps::ops();
    link_target_m->name = "LinkTarget";
    link_target_m->element_type = nullptr;
    link_target_m->key_type = nullptr;
    link_target_m->fields = nullptr;
    link_target_m->field_count = 0;
    link_target_m->fixed_size = 0;
    link_target_meta_ = link_target_m.get();
    owned_metas_.push_back(std::move(link_target_m));

    // REFLink - for inline link storage (supports both simple links and REF→TS)
    auto ref_link_m = std::make_unique<value::TypeMeta>();
    ref_link_m->size = sizeof(REFLink);
    ref_link_m->alignment = alignof(REFLink);
    ref_link_m->kind = value::TypeKind::Atomic;
    ref_link_m->flags = value::TypeFlags::None;
    ref_link_m->ops = REFLinkOps::ops();
    ref_link_m->name = "REFLink";
    ref_link_m->element_type = nullptr;
    ref_link_m->key_type = nullptr;
    ref_link_m->fields = nullptr;
    ref_link_m->field_count = 0;
    ref_link_m->fixed_size = 0;
    ref_link_meta_ = ref_link_m.get();
    owned_metas_.push_back(std::move(ref_link_m));
}

// ============================================================================
// Singleton Type Accessors
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::engine_time_meta() {
    return engine_time_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::observer_list_meta() {
    return observer_list_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::set_delta_meta() {
    return set_delta_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::map_delta_meta() {
    return map_delta_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::bundle_delta_nav_meta() {
    return bundle_delta_nav_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::list_delta_nav_meta() {
    return list_delta_nav_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::bool_meta() {
    return bool_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::link_target_meta() {
    return link_target_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::ref_link_meta() {
    return ref_link_meta_;
}

const value::TypeMeta* TSMetaSchemaCache::ts_reference_meta() {
    return ts_reference_meta_;
}

// ============================================================================
// Schema Access Methods
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::get_time_schema(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    // Check cache
    auto it = time_schema_cache_.find(ts_meta);
    if (it != time_schema_cache_.end()) {
        return it->second;
    }

    // Generate and cache
    const value::TypeMeta* schema = generate_time_schema_impl(ts_meta);
    time_schema_cache_[ts_meta] = schema;
    return schema;
}

const value::TypeMeta* TSMetaSchemaCache::get_observer_schema(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    // Check cache
    auto it = observer_schema_cache_.find(ts_meta);
    if (it != observer_schema_cache_.end()) {
        return it->second;
    }

    // Generate and cache
    const value::TypeMeta* schema = generate_observer_schema_impl(ts_meta);
    observer_schema_cache_[ts_meta] = schema;
    return schema;
}

const value::TypeMeta* TSMetaSchemaCache::get_delta_value_schema(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    // Check cache
    auto it = delta_value_schema_cache_.find(ts_meta);
    if (it != delta_value_schema_cache_.end()) {
        return it->second;
    }

    // Generate and cache
    const value::TypeMeta* schema = generate_delta_value_schema_impl(ts_meta);
    delta_value_schema_cache_[ts_meta] = schema;
    return schema;
}

const value::TypeMeta* TSMetaSchemaCache::get_link_schema(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    // Check cache
    auto it = link_schema_cache_.find(ts_meta);
    if (it != link_schema_cache_.end()) {
        return it->second;
    }

    // Generate and cache
    const value::TypeMeta* schema = generate_link_schema_impl(ts_meta);
    link_schema_cache_[ts_meta] = schema;
    return schema;
}

const value::TypeMeta* TSMetaSchemaCache::get_active_schema(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    // Check cache
    auto it = active_schema_cache_.find(ts_meta);
    if (it != active_schema_cache_.end()) {
        return it->second;
    }

    // Generate and cache
    const value::TypeMeta* schema = generate_active_schema_impl(ts_meta);
    active_schema_cache_[ts_meta] = schema;
    return schema;
}

// ============================================================================
// Time Schema Generation
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::generate_time_schema_impl(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    switch (ts_meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // Atomic time-series types: just engine_time_t
            return engine_time_meta_;

        case TSKind::TSD: {
            // TSD[K,V] -> tuple[engine_time_t, var_list[time_schema(V)]]
            // The var_list grows dynamically with the map
            const value::TypeMeta* child_time = get_time_schema(ts_meta->element_ts);

            auto& registry = value::TypeRegistry::instance();

            // Build var_list for child times (dynamic list)
            const value::TypeMeta* var_list_type = registry.list(child_time).build();

            // Build tuple[engine_time_t, var_list[...]]
            return registry.tuple()
                .element(engine_time_meta_)
                .element(var_list_type)
                .build();
        }

        case TSKind::TSB: {
            // TSB[...] -> tuple[engine_time_t, fixed_list[time_schema(field_i) for each field]]
            // Use a tuple with one element for container time + one for each field's time schema
            auto& registry = value::TypeRegistry::instance();
            auto builder = registry.tuple();

            // First element: container time
            builder.element(engine_time_meta_);

            // Subsequent elements: per-field time schemas
            for (size_t i = 0; i < ts_meta->field_count; ++i) {
                const value::TypeMeta* field_time = get_time_schema(ts_meta->fields[i].ts_type);
                builder.element(field_time);
            }

            return builder.build();
        }

        case TSKind::TSL: {
            // TSL[T] -> tuple[engine_time_t, fixed_list[time_schema(element) x size]]
            const value::TypeMeta* element_time = get_time_schema(ts_meta->element_ts);

            auto& registry = value::TypeRegistry::instance();

            if (ts_meta->fixed_size > 0) {
                // Fixed-size list
                const value::TypeMeta* fixed_list_type =
                    registry.fixed_list(element_time, ts_meta->fixed_size).build();

                return registry.tuple()
                    .element(engine_time_meta_)
                    .element(fixed_list_type)
                    .build();
            } else {
                // Dynamic list (SIZE=0 means dynamic)
                const value::TypeMeta* var_list_type = registry.list(element_time).build();

                return registry.tuple()
                    .element(engine_time_meta_)
                    .element(var_list_type)
                    .build();
            }
        }
    }

    return engine_time_meta_;
}

// ============================================================================
// Observer Schema Generation
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::generate_observer_schema_impl(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    switch (ts_meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // Atomic time-series types: just ObserverList
            return observer_list_meta_;

        case TSKind::TSD: {
            // TSD[K,V] -> tuple[ObserverList, var_list[observer_schema(V)]]
            const value::TypeMeta* child_observer = get_observer_schema(ts_meta->element_ts);

            auto& registry = value::TypeRegistry::instance();

            // Build var_list for child observers (dynamic list)
            const value::TypeMeta* var_list_type = registry.list(child_observer).build();

            // Build tuple[ObserverList, var_list[...]]
            return registry.tuple()
                .element(observer_list_meta_)
                .element(var_list_type)
                .build();
        }

        case TSKind::TSB: {
            // TSB[...] -> tuple[ObserverList, fixed_list[observer_schema(field_i) for each field]]
            auto& registry = value::TypeRegistry::instance();
            auto builder = registry.tuple();

            // First element: container observer list
            builder.element(observer_list_meta_);

            // Subsequent elements: per-field observer schemas
            for (size_t i = 0; i < ts_meta->field_count; ++i) {
                const value::TypeMeta* field_observer = get_observer_schema(ts_meta->fields[i].ts_type);
                builder.element(field_observer);
            }

            return builder.build();
        }

        case TSKind::TSL: {
            // TSL[T] -> tuple[ObserverList, fixed_list[observer_schema(element) x size]]
            const value::TypeMeta* element_observer = get_observer_schema(ts_meta->element_ts);

            auto& registry = value::TypeRegistry::instance();

            if (ts_meta->fixed_size > 0) {
                // Fixed-size list
                const value::TypeMeta* fixed_list_type =
                    registry.fixed_list(element_observer, ts_meta->fixed_size).build();

                return registry.tuple()
                    .element(observer_list_meta_)
                    .element(fixed_list_type)
                    .build();
            } else {
                // Dynamic list
                const value::TypeMeta* var_list_type = registry.list(element_observer).build();

                return registry.tuple()
                    .element(observer_list_meta_)
                    .element(var_list_type)
                    .build();
            }
        }
    }

    return observer_list_meta_;
}

// ============================================================================
// Delta Value Schema Generation
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::generate_delta_value_schema_impl(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    switch (ts_meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // No delta tracking for these types
            return nullptr;

        case TSKind::TSS:
            // TSS -> SetDelta
            return set_delta_meta_;

        case TSKind::TSD:
            // TSD -> MapDelta
            return map_delta_meta_;

        case TSKind::TSB: {
            // TSB -> BundleDeltaNav (if has_delta), else nullptr
            if (!has_delta(ts_meta)) {
                return nullptr;
            }
            return bundle_delta_nav_meta_;
        }

        case TSKind::TSL: {
            // TSL -> ListDeltaNav (if has_delta), else nullptr
            if (!has_delta(ts_meta)) {
                return nullptr;
            }
            return list_delta_nav_meta_;
        }
    }

    return nullptr;
}

// ============================================================================
// Link Schema Generation
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::generate_link_schema_impl(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    switch (ts_meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // Scalar time-series types: REFLink for link storage
            // Enables REF→TS conversion when used as alternatives
            return ref_link_meta_;

        case TSKind::TSD:
            // TSD: REFLink for collection-level link
            // REFLink stores the link target inline and can also handle REF→TS
            // dereferencing when needed. This provides stable addresses for
            // the two-phase removal lifecycle.
            return ref_link_meta_;

        case TSKind::TSL: {
            // TSL: For fixed-size lists (inputs), use fixed_list[REFLink] for per-element binding
            // For dynamic lists, use single REFLink for collection-level link
            if (ts_meta->fixed_size > 0) {
                auto& registry = value::TypeRegistry::instance();
                return registry.fixed_list(ref_link_meta_, ts_meta->fixed_size).build();
            }
            return ref_link_meta_;
        }

        case TSKind::TSB: {
            // TSB[...] -> tuple[REFLink, link_schema(field_0), link_schema(field_1), ...]
            // The first element is the bundle-level REFLink (for binding the whole bundle).
            // Subsequent elements are per-field link schemas, enabling nested link storage
            // for composite fields (TSL, TSD, TSB within the bundle).
            if (ts_meta->field_count == 0) {
                return nullptr;
            }

            auto& registry = value::TypeRegistry::instance();
            auto builder = registry.tuple();

            // First element: bundle-level REFLink
            builder.element(ref_link_meta_);

            // Subsequent elements: per-field link schemas
            for (size_t i = 0; i < ts_meta->field_count; ++i) {
                const value::TypeMeta* field_link = get_link_schema(ts_meta->fields[i].ts_type);
                builder.element(field_link);
            }

            return builder.build();
        }
    }

    return nullptr;
}

// ============================================================================
// Active Schema Generation
// ============================================================================

const value::TypeMeta* TSMetaSchemaCache::generate_active_schema_impl(const TSMeta* ts_meta) {
    if (!ts_meta) return nullptr;

    switch (ts_meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            // Atomic time-series types: just bool for active state
            return bool_meta_;

        case TSKind::TSD: {
            // TSD[K,V] -> tuple[bool, var_list[active_schema(V)]]
            const value::TypeMeta* child_active = get_active_schema(ts_meta->element_ts);

            auto& registry = value::TypeRegistry::instance();

            // Build var_list for child active states (dynamic list)
            const value::TypeMeta* var_list_type = registry.list(child_active).build();

            // Build tuple[bool, var_list[...]]
            return registry.tuple()
                .element(bool_meta_)
                .element(var_list_type)
                .build();
        }

        case TSKind::TSB: {
            // TSB[...] -> tuple[bool, fixed_list[active_schema(field_i) for each field]]
            auto& registry = value::TypeRegistry::instance();
            auto builder = registry.tuple();

            // First element: container active state
            builder.element(bool_meta_);

            // Subsequent elements: per-field active schemas
            for (size_t i = 0; i < ts_meta->field_count; ++i) {
                const value::TypeMeta* field_active = get_active_schema(ts_meta->fields[i].ts_type);
                builder.element(field_active);
            }

            return builder.build();
        }

        case TSKind::TSL: {
            // TSL[T] -> tuple[bool, fixed_list[active_schema(element) x size]]
            const value::TypeMeta* element_active = get_active_schema(ts_meta->element_ts);

            auto& registry = value::TypeRegistry::instance();

            if (ts_meta->fixed_size > 0) {
                // Fixed-size list
                const value::TypeMeta* fixed_list_type =
                    registry.fixed_list(element_active, ts_meta->fixed_size).build();

                return registry.tuple()
                    .element(bool_meta_)
                    .element(fixed_list_type)
                    .build();
            } else {
                // Dynamic list
                const value::TypeMeta* var_list_type = registry.list(element_active).build();

                return registry.tuple()
                    .element(bool_meta_)
                    .element(var_list_type)
                    .build();
            }
        }
    }

    return bool_meta_;
}

} // namespace hgraph
