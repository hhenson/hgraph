#pragma once

/**
 * @file ts_meta_schema.h
 * @brief Schema generation functions for TSMeta parallel Value structures.
 *
 * This file provides functions to generate TypeMeta schemas for the four
 * parallel Value structures that make up a TSValue:
 *
 * 1. value_schema: User-visible data (derived from TSMeta directly)
 * 2. time_schema: Modification timestamps (recursive, mirrors data structure)
 * 3. observer_schema: Observer lists (recursive, mirrors data structure)
 * 4. delta_value_schema: Delta tracking data (only where TSS/TSD exist)
 * 5. link_schema: Link flags for binding support (parallel to value structure)
 *
 * Schema Generation Rules:
 *
 * time_schema:
 *   TS[T], TSS, SIGNAL, TSW, REF -> engine_time_t
 *   TSD[K,V] -> tuple[engine_time_t, var_list[time_schema(V)]]
 *   TSB[...] -> tuple[engine_time_t, fixed_list[time_schema(field_i) for each field]]
 *   TSL[T]   -> tuple[engine_time_t, fixed_list[time_schema(element) x size]]
 *
 * observer_schema:
 *   TS[T], TSS, SIGNAL, TSW, REF -> ObserverList
 *   TSD[K,V] -> tuple[ObserverList, var_list[observer_schema(V)]]
 *   TSB[...] -> tuple[ObserverList, fixed_list[observer_schema(field_i) for each field]]
 *   TSL[T]   -> tuple[ObserverList, fixed_list[observer_schema(element) x size]]
 *
 * delta_value_schema:
 *   TS[T], SIGNAL, TSW, REF -> nullptr (no delta)
 *   TSS[T]   -> SetDelta
 *   TSD[K,V] -> MapDelta
 *   TSB[...] -> BundleDeltaNav (if has_delta), else nullptr
 *   TSL[T]   -> ListDeltaNav (if has_delta), else nullptr
 *
 * link_schema:
 *   TS[T], TSS, SIGNAL, TSW, REF -> nullptr (no link tracking at scalar level)
 *   TSD[K,V] -> bool (collection-level link flag)
 *   TSL[T]   -> bool (collection-level link flag)
 *   TSB[...] -> fixed_list[bool x field_count] (per-field link flags)
 */

#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_type_ops.h>
#include <hgraph/types/time_series/link_target.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/util/date_time.h>

#include <unordered_map>
#include <memory>

namespace hgraph {

// Forward declarations
class TSMetaSchemaCache;

/**
 * @brief Singleton cache for generated schemas.
 *
 * Since schema generation is recursive and schemas should be reused,
 * this cache stores generated schemas for each TSMeta.
 */
class TSMetaSchemaCache {
public:
    /// Get the singleton instance
    static TSMetaSchemaCache& instance();

    // Deleted copy/move
    TSMetaSchemaCache(const TSMetaSchemaCache&) = delete;
    TSMetaSchemaCache& operator=(const TSMetaSchemaCache&) = delete;
    TSMetaSchemaCache(TSMetaSchemaCache&&) = delete;
    TSMetaSchemaCache& operator=(TSMetaSchemaCache&&) = delete;

    // ========== Schema Access ==========

    /**
     * @brief Get the time schema for a TSMeta.
     *
     * Generates and caches the schema on first call.
     *
     * @param ts_meta The time-series metadata
     * @return TypeMeta for the time_ parallel Value
     */
    const value::TypeMeta* get_time_schema(const TSMeta* ts_meta);

    /**
     * @brief Get the observer schema for a TSMeta.
     *
     * @param ts_meta The time-series metadata
     * @return TypeMeta for the observer_ parallel Value
     */
    const value::TypeMeta* get_observer_schema(const TSMeta* ts_meta);

    /**
     * @brief Get the delta value schema for a TSMeta.
     *
     * @param ts_meta The time-series metadata
     * @return TypeMeta for the delta_value_ parallel Value, or nullptr if no delta
     */
    const value::TypeMeta* get_delta_value_schema(const TSMeta* ts_meta);

    /**
     * @brief Get the link schema for a TSMeta.
     *
     * Link schema is used for tracking which positions are bound to external targets.
     * - TSL/TSD: Single bool (collection-level link flag)
     * - TSB: fixed_list[bool] with one entry per field
     * - Scalars: nullptr (no link tracking at scalar level)
     *
     * @param ts_meta The time-series metadata
     * @return TypeMeta for the link_ parallel Value, or nullptr if no links needed
     */
    const value::TypeMeta* get_link_schema(const TSMeta* ts_meta);

    // ========== Singleton Type Accessors ==========

    /**
     * @brief Get the TypeMeta for engine_time_t.
     */
    const value::TypeMeta* engine_time_meta();

    /**
     * @brief Get the TypeMeta for ObserverList.
     */
    const value::TypeMeta* observer_list_meta();

    /**
     * @brief Get the TypeMeta for SetDelta.
     */
    const value::TypeMeta* set_delta_meta();

    /**
     * @brief Get the TypeMeta for MapDelta.
     */
    const value::TypeMeta* map_delta_meta();

    /**
     * @brief Get the TypeMeta for BundleDeltaNav.
     */
    const value::TypeMeta* bundle_delta_nav_meta();

    /**
     * @brief Get the TypeMeta for ListDeltaNav.
     */
    const value::TypeMeta* list_delta_nav_meta();

    /**
     * @brief Get the TypeMeta for bool (used for link flags).
     */
    const value::TypeMeta* bool_meta();

    /**
     * @brief Get the TypeMeta for LinkTarget (used for collection-level links).
     */
    const value::TypeMeta* link_target_meta();

private:
    TSMetaSchemaCache();
    ~TSMetaSchemaCache() = default;

    // ========== Schema Generation (Internal) ==========

    const value::TypeMeta* generate_time_schema_impl(const TSMeta* ts_meta);
    const value::TypeMeta* generate_observer_schema_impl(const TSMeta* ts_meta);
    const value::TypeMeta* generate_delta_value_schema_impl(const TSMeta* ts_meta);
    const value::TypeMeta* generate_link_schema_impl(const TSMeta* ts_meta);

    // ========== Caches ==========

    std::unordered_map<const TSMeta*, const value::TypeMeta*> time_schema_cache_;
    std::unordered_map<const TSMeta*, const value::TypeMeta*> observer_schema_cache_;
    std::unordered_map<const TSMeta*, const value::TypeMeta*> delta_value_schema_cache_;
    std::unordered_map<const TSMeta*, const value::TypeMeta*> link_schema_cache_;

    // ========== Singleton TypeMetas ==========

    const value::TypeMeta* engine_time_meta_{nullptr};
    const value::TypeMeta* observer_list_meta_{nullptr};
    const value::TypeMeta* set_delta_meta_{nullptr};
    const value::TypeMeta* map_delta_meta_{nullptr};
    const value::TypeMeta* bundle_delta_nav_meta_{nullptr};
    const value::TypeMeta* list_delta_nav_meta_{nullptr};
    const value::TypeMeta* bool_meta_{nullptr};
    const value::TypeMeta* link_target_meta_{nullptr};

    // ========== Owned TypeMetas ==========

    std::vector<std::unique_ptr<value::TypeMeta>> owned_metas_;
    std::vector<std::unique_ptr<value::BundleFieldInfo[]>> owned_field_infos_;
};

// ============================================================================
// Free Functions
// ============================================================================

/**
 * @brief Determine if a TS type needs delta tracking.
 *
 * has_delta rules:
 * - TS[T]     -> false
 * - TSS[T]    -> true (add/remove tracking)
 * - TSD[K,V]  -> true (add/remove/update tracking)
 * - TSW[T]    -> false
 * - REF[T]    -> false
 * - SIGNAL    -> false
 * - TSB[...]  -> any(has_delta(field) for field in fields)
 * - TSL[T]    -> has_delta(element)
 *
 * @param ts_meta The time-series metadata
 * @return true if delta tracking is needed
 */
inline bool has_delta(const TSMeta* ts_meta) {
    if (!ts_meta) return false;

    switch (ts_meta->kind) {
        case TSKind::TSValue:
        case TSKind::TSW:
        case TSKind::REF:
        case TSKind::SIGNAL:
            return false;

        case TSKind::TSS:
        case TSKind::TSD:
            return true;

        case TSKind::TSB:
            // Bundle has delta if any field has delta
            for (size_t i = 0; i < ts_meta->field_count; ++i) {
                if (has_delta(ts_meta->fields[i].ts_type)) {
                    return true;
                }
            }
            return false;

        case TSKind::TSL:
            // List has delta if element has delta
            return has_delta(ts_meta->element_ts);
    }

    return false;
}

/**
 * @brief Generate the time schema for a TSMeta.
 *
 * Convenience function that delegates to TSMetaSchemaCache.
 *
 * @param ts_meta The time-series metadata
 * @return TypeMeta for the time_ parallel Value
 */
inline const value::TypeMeta* generate_time_schema(const TSMeta* ts_meta) {
    return TSMetaSchemaCache::instance().get_time_schema(ts_meta);
}

/**
 * @brief Generate the observer schema for a TSMeta.
 *
 * Convenience function that delegates to TSMetaSchemaCache.
 *
 * @param ts_meta The time-series metadata
 * @return TypeMeta for the observer_ parallel Value
 */
inline const value::TypeMeta* generate_observer_schema(const TSMeta* ts_meta) {
    return TSMetaSchemaCache::instance().get_observer_schema(ts_meta);
}

/**
 * @brief Generate the delta value schema for a TSMeta.
 *
 * Convenience function that delegates to TSMetaSchemaCache.
 *
 * @param ts_meta The time-series metadata
 * @return TypeMeta for the delta_value_ parallel Value, or nullptr if no delta
 */
inline const value::TypeMeta* generate_delta_value_schema(const TSMeta* ts_meta) {
    return TSMetaSchemaCache::instance().get_delta_value_schema(ts_meta);
}

/**
 * @brief Generate the link schema for a TSMeta.
 *
 * Convenience function that delegates to TSMetaSchemaCache.
 *
 * Link schema is used for tracking which positions are bound to external targets.
 * - TSL/TSD: Single bool (collection-level link flag)
 * - TSB: fixed_list[bool] with one entry per field
 * - Scalars: nullptr (no link tracking at scalar level)
 *
 * @param ts_meta The time-series metadata
 * @return TypeMeta for the link_ parallel Value, or nullptr if no links needed
 */
inline const value::TypeMeta* generate_link_schema(const TSMeta* ts_meta) {
    return TSMetaSchemaCache::instance().get_link_schema(ts_meta);
}

} // namespace hgraph
