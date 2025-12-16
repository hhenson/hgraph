//
// TSContext - Navigation Context for Time-Series
//
// Provides navigation up the hierarchy to the owning node.
// This is orthogonal to value state - just provides access to graph structure.
//

#ifndef HGRAPH_TS_CONTEXT_H
#define HGRAPH_TS_CONTEXT_H

#include <hgraph/hgraph_base.h>
#include <variant>

namespace hgraph {

// Forward declarations
struct Node;
struct Graph;
struct TimeSeriesType;
struct TimeSeriesOutput;
struct TimeSeriesInput;

/**
 * TSContext - Navigation context for time-series types.
 *
 * Provides the "who owns me" aspect of time-series, orthogonal to value state.
 * An owner can be:
 * - A node (for root-level time-series)
 * - A parent time-series (for nested time-series in TSB/TSL/TSD)
 *
 * This enables navigation up the hierarchy to access:
 * - Owning node
 * - Owning graph
 * - Current engine time
 */
struct TSContext {
    // Owner can be a node (root) or parent time-series (nested)
    // Using variant for efficient storage and clear semantics
    std::variant<node_ptr, TimeSeriesType*> owner;

    // Default constructor - no owner
    TSContext() : owner(static_cast<node_ptr>(nullptr)) {}

    // Construct with node owner
    explicit TSContext(node_ptr node) : owner(node) {}

    // Construct with parent time-series owner
    explicit TSContext(TimeSeriesType* parent) : owner(parent) {}

    // Navigation - get the owning node (traverses up hierarchy)
    [[nodiscard]] node_ptr owning_node() const;

    // Navigation - get the owning graph
    [[nodiscard]] graph_ptr owning_graph() const;

    // Get current engine time from the owning node
    [[nodiscard]] engine_time_t current_time() const;

    // Check if we have an owner
    [[nodiscard]] bool has_owner() const {
        if (std::holds_alternative<node_ptr>(owner)) {
            return std::get<node_ptr>(owner) != nullptr;
        }
        return std::get<TimeSeriesType*>(owner) != nullptr;
    }

    // Check if owner is a node (vs parent time-series)
    [[nodiscard]] bool is_node_owner() const {
        return std::holds_alternative<node_ptr>(owner) && std::get<node_ptr>(owner) != nullptr;
    }

    // Check if owner is a parent time-series
    [[nodiscard]] bool is_parent_owner() const {
        return std::holds_alternative<TimeSeriesType*>(owner) && std::get<TimeSeriesType*>(owner) != nullptr;
    }

    // Re-parent to a new node
    void re_parent(node_ptr new_parent) {
        owner = new_parent;
    }

    // Re-parent to a new parent time-series
    void re_parent(TimeSeriesType* new_parent) {
        owner = new_parent;
    }

    // Reset owner (orphan this context)
    void reset() {
        owner = static_cast<node_ptr>(nullptr);
    }

    // Factory helpers for clear construction intent
    static TSContext from_node(node_ptr n) { return TSContext(n); }
    static TSContext from_parent(TimeSeriesType* p) { return TSContext(p); }
};

} // namespace hgraph

#endif // HGRAPH_TS_CONTEXT_H
