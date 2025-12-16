//
// TSValue - The Shared Time-Series State
//
// This is the core shared state that represents a single time-series value.
// It holds the value, modification time, and subscriber list.
// Outputs own this state; inputs bind to it (share via shared_ptr).
//

#ifndef HGRAPH_TS_VALUE_H
#define HGRAPH_TS_VALUE_H

#include <hgraph/hgraph_base.h>
#include <hgraph/types/notifiable.h>
#include <nanobind/nanobind.h>
#include <unordered_set>
#include <memory>

namespace nb = nanobind;

namespace hgraph {

// Forward declaration
struct TSTypeMeta;

/**
 * TSValue - The single source of truth for a time-series value.
 *
 * This struct holds all state for a scalar time-series:
 * - The current value (as nb::object for type erasure)
 * - When it was last modified
 * - Who should be notified on changes
 * - Type metadata
 *
 * Outputs create and own this; inputs bind to it by sharing the pointer.
 * This enables the "single thing" model where input and output are views.
 */
struct TSValue {
    using ptr = std::shared_ptr<TSValue>;
    using weak_ptr = std::weak_ptr<TSValue>;

    // State
    nb::object value{nb::none()};                    // Current value
    engine_time_t last_modified{MIN_DT};             // When last modified
    std::unordered_set<Notifiable*> subscribers;     // Who to notify on change
    const TSTypeMeta* meta{nullptr};                 // Type metadata

    // Default constructor
    TSValue() = default;

    // Construct with metadata
    explicit TSValue(const TSTypeMeta* type_meta) : meta(type_meta) {}

    // Core state queries
    [[nodiscard]] bool valid() const {
        return !value.is_none();
    }

    [[nodiscard]] bool modified(engine_time_t current_time) const {
        return last_modified == current_time;
    }

    // Mutation - called by output view
    void set_value(const nb::object& v, engine_time_t time) {
        value = v;
        last_modified = time;
        notify_all(time);
    }

    void invalidate(engine_time_t time) {
        value = nb::none();
        last_modified = time;
        notify_all(time);
    }

    // Mark as modified without changing value (used for propagation)
    void mark_modified(engine_time_t time) {
        last_modified = time;
        notify_all(time);
    }

    // Subscriber management
    void subscribe(Notifiable* n) {
        if (n) subscribers.insert(n);
    }

    void unsubscribe(Notifiable* n) {
        if (n) subscribers.erase(n);
    }

    // Clear all state (used for reset/cleanup)
    void clear() {
        value = nb::none();
        last_modified = MIN_DT;
        // Note: don't clear subscribers - they may want to know about clear
    }

private:
    void notify_all(engine_time_t time) {
        for (auto* sub : subscribers) {
            if (sub) sub->notify(time);
        }
    }
};

} // namespace hgraph

#endif // HGRAPH_TS_VALUE_H
