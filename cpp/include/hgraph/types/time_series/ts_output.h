//
// Created by Claude on 16/12/2025.
//
// TSOutput - Value-based time-series output implementation
//
// Design principles:
// - Output owns a TimeSeriesValue (type-erased storage)
// - Time is passed as parameter to mutation methods (not stored in view)
// - Views returned are TimeSeriesValueView with path tracking for REF support
// - Chainable navigation API: output.view().field("price").element(0)
//

#ifndef HGRAPH_TS_OUTPUT_H
#define HGRAPH_TS_OUTPUT_H

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/types/notifiable.h>
#include <hgraph/types/value/time_series_value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/delta_view.h>
#include <string>

namespace hgraph::ts {

// Forward declarations
class TSOutput;
class DeltaView;

// ============================================================================
// TSOutput - Owning time-series output with node parentage
// ============================================================================

/**
 * TSOutput - Value-based time-series output implementation
 *
 * Owns:
 * - TimeSeriesValue (type-erased value storage + modification tracking)
 * - Back-reference to owning node
 * - Type metadata for navigation and construction
 *
 * Provides:
 * - View creation with path tracking (returns TimeSeriesValueView)
 * - Direct value access for simple cases
 * - Observer/subscription support (delegated to TimeSeriesValue)
 *
 * Construction:
 * - Use TimeSeriesTypeMeta::make_output() to create instances
 * - The meta provides the value schema for TimeSeriesValue construction
 */
class TSOutput {
public:
    using ptr = TSOutput*;
    using s_ptr = std::shared_ptr<TSOutput>;

    TSOutput() = default;

    /**
     * Construct from type metadata and owning node
     *
     * The meta provides the value schema used to construct the
     * underlying TimeSeriesValue storage.
     */
    TSOutput(const TimeSeriesTypeMeta* meta, node_ptr owning_node)
        : _meta(meta)
        , _owning_node(owning_node)
        , _value(meta ? meta->value_schema() : nullptr) {}

    // Move only (value owns storage)
    TSOutput(TSOutput&&) noexcept = default;
    TSOutput& operator=(TSOutput&&) noexcept = default;
    TSOutput(const TSOutput&) = delete;
    TSOutput& operator=(const TSOutput&) = delete;

    // === Validity and type information ===
    [[nodiscard]] bool valid() const { return _value.valid(); }
    [[nodiscard]] const TimeSeriesTypeMeta* meta() const { return _meta; }
    [[nodiscard]] const value::TypeMeta* value_schema() const { return _value.schema(); }
    [[nodiscard]] value::TypeKind kind() const { return _value.kind(); }
    [[nodiscard]] TimeSeriesKind ts_kind() const { return _meta ? _meta->ts_kind : TimeSeriesKind::TS; }

    // === Node parentage ===
    [[nodiscard]] node_ptr owning_node() const { return _owning_node; }

    // === View creation with path tracking ===

    /**
     * Create a view into this output
     *
     * The view is initialized with this TSOutput as the path root.
     * Navigation from the view creates new views with extended paths.
     *
     * Example:
     *   auto price_view = output.view().field("price");
     *   // price_view.path_string() == "root[0]"  (if price is field 0)
     */
    [[nodiscard]] value::TimeSeriesValueView view() {
        return {_value.view().value_view(), _value.view().tracker(),
                _value.underlying_observers(), _meta, value::ValuePath(this)};
    }

    // === Direct value access (convenience for simple TS values) ===

    template<typename T>
    void set_value(const T& val, engine_time_t time) {
        _value.set_value(val, time);
    }

    template<typename T>
    [[nodiscard]] const T& as() const {
        return _value.as<T>();
    }

    [[nodiscard]] value::ConstValueView value() const {
        return _value.value();
    }

    // === Modification queries ===
    [[nodiscard]] bool modified_at(engine_time_t time) const {
        return _value.modified_at(time);
    }

    [[nodiscard]] engine_time_t last_modified_time() const {
        return _value.last_modified_time();
    }

    [[nodiscard]] bool has_value() const {
        return _value.has_value();
    }

    void mark_invalid() {
        _value.mark_invalid();
    }

    // === Observer/subscription support ===
    void subscribe(Notifiable* notifiable) {
        _value.subscribe(notifiable);
    }

    void unsubscribe(Notifiable* notifiable) {
        _value.unsubscribe(notifiable);
    }

    [[nodiscard]] bool has_observers() const {
        return _value.has_observers();
    }

    // === Underlying storage access (for advanced use) ===
    [[nodiscard]] value::TimeSeriesValue& underlying() { return _value; }
    [[nodiscard]] const value::TimeSeriesValue& underlying() const { return _value; }

    // === String representation ===
    [[nodiscard]] std::string to_string() const {
        return _value.to_string();
    }

    [[nodiscard]] std::string to_debug_string(engine_time_t time) const {
        std::string result = "TSOutput{";
        if (_meta && _meta->name) {
            result += "type=";
            result += _meta->name;
            result += ", ";
        }
        result += _value.to_debug_string(time);
        result += "}";
        return result;
    }

    // TODO: Register with visitor system
    // VISITOR_SUPPORT()

private:
    const TimeSeriesTypeMeta* _meta{nullptr};
    node_ptr _owning_node{nullptr};
    value::TimeSeriesValue _value;
};

} // namespace hgraph::ts

// ============================================================================
// Inline definitions for value::ValuePath and value::TimeSeriesValueView
// These need to be defined here because they depend on ts::TSOutput
// ============================================================================

namespace hgraph::value {

// ValuePath::owning_node() - get owning node from root TSOutput
inline node_ptr ValuePath::owning_node() const {
    return root ? root->owning_node() : nullptr;
}

// TimeSeriesValueView::ts_kind() - get time-series kind from metadata
inline TimeSeriesKind TimeSeriesValueView::ts_kind() const {
    return _ts_meta ? _ts_meta->ts_kind : TimeSeriesKind::TS;
}

// TimeSeriesValueView::delta_view() - create DeltaView for delta value access
inline ts::DeltaView TimeSeriesValueView::delta_view(engine_time_t time) const {
    if (!valid() || !modified_at(time)) {
        return {};
    }
    // Cast to base class to access const data() method (ValueView::data() hides it)
    auto const_value = ConstValueView(static_cast<const ConstValueView&>(_value_view).data(), _value_view.schema());
    return {const_value, _tracker, _ts_meta, time};
}

// TimeSeriesValueView helper methods for navigation
inline const TimeSeriesTypeMeta* TimeSeriesValueView::field_meta_at(size_t index) const {
    return _ts_meta ? _ts_meta->field_meta(index) : nullptr;
}

inline const TimeSeriesTypeMeta* TimeSeriesValueView::element_meta_at() const {
    return _ts_meta ? _ts_meta->element_meta() : nullptr;
}

inline const TimeSeriesTypeMeta* TimeSeriesValueView::value_meta_at() const {
    return _ts_meta ? _ts_meta->value_meta() : nullptr;
}

} // namespace hgraph::value

#endif // HGRAPH_TS_OUTPUT_H
