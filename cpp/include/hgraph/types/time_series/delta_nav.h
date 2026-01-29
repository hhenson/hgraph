#pragma once

/**
 * @file delta_nav.h
 * @brief BundleDeltaNav and ListDeltaNav - Delta navigation structures.
 *
 * These structures provide delta navigation for TSB (Time-Series Bundle) and
 * TSL (Time-Series List) when their fields/elements have delta tracking.
 * Unlike SetDelta and MapDelta which implement SlotObserver, these are
 * simple navigation structures that hold child delta pointers.
 *
 * Key design principles:
 * - Hold last_cleared_time for lazy delta clearing coordination
 * - Hold children vector of DeltaVariant for nested navigation
 * - clear() resets children to monostate
 */

#include <hgraph/types/time_series/map_delta.h>  // For DeltaVariant
#include <hgraph/util/date_time.h>

#include <vector>

namespace hgraph {

/**
 * @brief Delta navigation structure for TSB (Time-Series Bundle).
 *
 * BundleDeltaNav provides navigation to child deltas for bundle fields.
 * When a TSB has fields that are TSS, TSD, or nested TSB/TSL types,
 * BundleDeltaNav allows traversing to their delta information.
 *
 * The last_cleared_time tracks when this bundle's delta was last cleared,
 * enabling lazy delta clearing coordination with the owning TSValue.
 */
struct BundleDeltaNav {
    /**
     * @brief Time when this delta was last cleared.
     *
     * Used for lazy delta clearing: if current_time > last_cleared_time,
     * the delta should be cleared before accessing.
     */
    engine_time_t last_cleared_time{MIN_ST};

    /**
     * @brief Child delta pointers for each bundle field.
     *
     * Indexed by field position. Fields without delta tracking have
     * monostate. Fields with delta tracking have the appropriate
     * delta pointer type.
     */
    std::vector<DeltaVariant> children;

    /**
     * @brief Default constructor.
     */
    BundleDeltaNav() = default;

    /**
     * @brief Reset all children to monostate.
     *
     * Called when clearing delta state. Does not reset last_cleared_time
     * (that's managed by the caller based on current engine time).
     */
    void clear() {
        for (auto& child : children) {
            child = std::monostate{};
        }
    }
};

/**
 * @brief Delta navigation structure for TSL (Time-Series List).
 *
 * ListDeltaNav provides navigation to child deltas for list elements.
 * When a TSL has elements that are TSS, TSD, or nested TSB/TSL types,
 * ListDeltaNav allows traversing to their delta information.
 *
 * The last_cleared_time tracks when this list's delta was last cleared,
 * enabling lazy delta clearing coordination with the owning TSValue.
 */
struct ListDeltaNav {
    /**
     * @brief Time when this delta was last cleared.
     *
     * Used for lazy delta clearing: if current_time > last_cleared_time,
     * the delta should be cleared before accessing.
     */
    engine_time_t last_cleared_time{MIN_ST};

    /**
     * @brief Child delta pointers for each list element.
     *
     * Indexed by element position. Elements without delta tracking have
     * monostate. Elements with delta tracking have the appropriate
     * delta pointer type.
     */
    std::vector<DeltaVariant> children;

    /**
     * @brief Default constructor.
     */
    ListDeltaNav() = default;

    /**
     * @brief Reset all children to monostate.
     *
     * Called when clearing delta state. Does not reset last_cleared_time
     * (that's managed by the caller based on current engine time).
     */
    void clear() {
        for (auto& child : children) {
            child = std::monostate{};
        }
    }
};

} // namespace hgraph
