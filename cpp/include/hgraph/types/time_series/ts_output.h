//
// Created by Claude on 16/12/2025.
//
// TSOutput - Value-based time-series output implementation
//
// Design principles:
// - Output owns a TSValue (type-erased storage)
// - Time is passed as parameter to mutation methods (not stored in view)
// - Views returned are TSView with path tracking for REF support
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
struct PythonCache;  // Defined in cpp file with nanobind - holds cached Python conversions

// ============================================================================
// TSOutput - Owning time-series output with node parentage
// ============================================================================

/**
 * TSOutput - Value-based time-series output implementation
 *
 * Owns:
 * - TSValue (type-erased value storage + modification tracking)
 * - Back-reference to owning node
 * - Type metadata for navigation and construction
 *
 * Provides:
 * - View creation with path tracking (returns TSView)
 * - Direct value access for simple cases
 * - Observer/subscription support (delegated to TSValue)
 *
 * Construction:
 * - Use TSMeta::make_output() to create instances
 * - The meta provides the value schema for TSValue construction
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
     * underlying TSValue storage.
     */
    TSOutput(const TSMeta* meta, node_ptr owning_node)
        : _meta(meta)
        , _owning_node(owning_node)
        , _value(meta ? meta->value_schema() : nullptr) {}

    // Destructor - defined in cpp where PythonCache is complete
    ~TSOutput();

    // Move only (value owns storage)
    TSOutput(TSOutput&& other) noexcept;
    TSOutput& operator=(TSOutput&& other) noexcept;
    TSOutput(const TSOutput&) = delete;
    TSOutput& operator=(const TSOutput&) = delete;

    // === Validity and type information ===
    [[nodiscard]] bool valid() const { return _value.valid(); }
    [[nodiscard]] const TSMeta* meta() const { return _meta; }
    [[nodiscard]] const value::TypeMeta* value_schema() const { return _value.schema(); }
    [[nodiscard]] value::TypeKind kind() const { return _value.kind(); }
    [[nodiscard]] TSKind ts_kind() const { return _meta ? _meta->ts_kind : TSKind::TS; }

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
    [[nodiscard]] value::TSView view() {
        // Ensure observers exist so TSView can subscribe
        _value.ensure_observers();
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
    [[nodiscard]] value::TSValue& underlying() { return _value; }
    [[nodiscard]] const value::TSValue& underlying() const { return _value; }

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

    // === Delta reset callback registration ===

    /**
     * Register callback to clear delta tracking at end of evaluation tick.
     *
     * For Set and Dict types, the delta (added/removed elements) needs to be
     * cleared after each evaluation tick. This method registers a callback
     * with the evaluation engine to do this cleanup.
     *
     * Should be called once when the output is first modified.
     *
     * Note: This is const because the flag is mutable - registration is a
     * caching mechanism that doesn't affect the logical state.
     */
    void register_delta_reset_callback() const;

    /**
     * Check if delta reset callback is already registered.
     */
    [[nodiscard]] bool delta_reset_registered() const { return _delta_reset_registered; }

    // === Python value/delta cache ===

    /**
     * Get or create the Python cache for this output.
     *
     * The cache stores:
     * - Cached value conversion (valid while cache_time >= last_modified_time)
     * - Cached delta conversion (valid only for current evaluation tick)
     *
     * When delta is first cached, a cleanup callback is registered
     * to clear it at the end of the evaluation tick.
     *
     * @return Pointer to the cache (created on first call)
     */
    PythonCache* python_cache();

    /**
     * Check if a Python cache exists (without creating one).
     */
    [[nodiscard]] bool has_python_cache() const { return _python_cache != nullptr; }

    /**
     * Clear the cached delta value.
     * Called by the after-evaluation callback.
     */
    void clear_cached_delta();

    /**
     * Clear the cached value.
     * Called when the value is updated.
     */
    void clear_cached_value();

    // TODO: Register with visitor system
    // VISITOR_SUPPORT()

private:
    const TSMeta* _meta{nullptr};
    node_ptr _owning_node{nullptr};
    value::TSValue _value;
    mutable bool _delta_reset_registered{false};  // mutable: registration is a caching mechanism
    PythonCache* _python_cache{nullptr};  // Lazily created, owned by this object
};

} // namespace hgraph::ts

// ============================================================================
// Inline definitions for value::ValuePath and value::TSView
// These need to be defined here because they depend on ts::TSOutput
// ============================================================================

namespace hgraph::value {

// ValuePath::owning_node() - get owning node from root TSOutput
inline node_ptr ValuePath::owning_node() const {
    return root ? root->owning_node() : nullptr;
}

// TSView::ts_kind() - get time-series kind from metadata
inline TSKind TSView::ts_kind() const {
    return _ts_meta ? _ts_meta->ts_kind : TSKind::TS;
}

// TSView::delta_view() - create DeltaView for delta value access
inline ts::DeltaView TSView::delta_view(engine_time_t time) const {
    if (!valid() || !modified_at(time)) {
        return {};
    }
    // Cast to base class to access const data() method (ValueView::data() hides it)
    auto const_value = ConstValueView(static_cast<const ConstValueView&>(_value_view).data(), _value_view.schema());
    return {const_value, _tracker, _ts_meta, time};
}

// TSView helper methods for navigation
inline const TSMeta* TSView::field_meta_at(size_t index) const {
    return _ts_meta ? _ts_meta->field_meta(index) : nullptr;
}

inline const TSMeta* TSView::element_meta_at() const {
    return _ts_meta ? _ts_meta->element_meta() : nullptr;
}

inline const TSMeta* TSView::value_meta_at() const {
    return _ts_meta ? _ts_meta->value_meta() : nullptr;
}

} // namespace hgraph::value

#endif // HGRAPH_TS_OUTPUT_H
