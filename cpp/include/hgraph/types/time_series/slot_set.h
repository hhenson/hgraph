#pragma once

/**
 * @file slot_set.h
 * @brief SlotSet type alias for slot-based delta tracking.
 *
 * Provides a centralized type alias for the set type used to track slot indices
 * in delta structures. This makes it easy to change the underlying container
 * type (e.g., between std::unordered_set and ankerl::unordered_dense::set).
 */

#include <ankerl/unordered_dense.h>

#include <cstddef>

namespace hgraph {

/**
 * @brief Type alias for slot index sets used in delta tracking.
 *
 * Uses ankerl::unordered_dense::set for high-performance O(1) membership
 * queries. Change this typedef to switch the underlying container type
 * for all delta structures.
 */
using SlotSet = ankerl::unordered_dense::set<size_t>;

} // namespace hgraph
