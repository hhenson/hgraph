#pragma once

/**
 * @file ts_value.h
 * @brief TSValue - Time-series value with schema and ownership tracking.
 *
 * TSValue extends the base Value type (with caching + modification tracking policies)
 * to add time-series specific metadata:
 * - TSMeta* schema for time-series structure
 * - Node* ownership for notification routing
 * - Output ID for identifying which output of a node
 * - View creation methods for type-safe access
 *
 * This is the core storage type for all time-series data in nodes.
 */

#include <hgraph/types/value/value.h>
#include <hgraph/types/time_series/ts_type_meta.h>

namespace hgraph {

// Forward declarations
struct Node;
struct TSView;
struct TSMutableView;
struct TSBView;

// Output ID constants
constexpr int OUTPUT_MAIN = 0;    ///< Main output of the node
constexpr int ERROR_PATH = -1;    ///< Error output for exceptions
constexpr int STATE_PATH = -2;    ///< Recordable state for checkpointing

/**
 * @brief Time-series value with schema and ownership tracking.
 *
 * TSValue wraps a Value<CombinedPolicy<WithPythonCache, WithModificationTracking>>
 * and adds time-series specific metadata. It provides the core storage for
 * all time-series data in the graph execution engine.
 *
 * Key features:
 * - Uses existing policy infrastructure for caching and modification tracking
 * - Tracks TSMeta schema for time-series type information
 * - Tracks owning Node for notification routing
 * - Provides view creation methods for type-safe access
 *
 * Usage:
 * @code
 * // Create from TSMeta schema
 * const TSMeta* schema = TSTypeRegistry::instance().ts(value::int_type());
 * TSValue ts_int(schema, owning_node);
 *
 * // Access via views
 * TSMutableView out = ts_int.mutable_view();
 * out.set<int64_t>(42, current_time);
 *
 * // For bundle types
 * TSBView bundle = ts_bundle.bundle_view();
 * float price = bundle.field("price").as<float>();
 * @endcode
 */
class TSValue {
public:
    /// The underlying Value type with appropriate policies
    using base_value_type = value::Value<value::CombinedPolicy<
        value::WithPythonCache,
        value::WithModificationTracking>>;

    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid TSValue.
     */
    TSValue() noexcept = default;

    /**
     * @brief Construct from a time-series schema.
     *
     * Creates storage according to the schema's value_schema().
     *
     * @param ts_schema The time-series type metadata
     * @param owner Optional owning node (can be nullptr for standalone values)
     * @param output_id The output identifier (OUTPUT_MAIN, ERROR_PATH, STATE_PATH)
     */
    explicit TSValue(const TSMeta* ts_schema,
                     Node* owner = nullptr,
                     int output_id = OUTPUT_MAIN);

    // ========== Move Semantics ==========

    TSValue(TSValue&& other) noexcept;
    TSValue& operator=(TSValue&& other) noexcept;

    // Copying disabled - use explicit copy methods
    TSValue(const TSValue&) = delete;
    TSValue& operator=(const TSValue&) = delete;

    /**
     * @brief Create a copy of a TSValue.
     */
    [[nodiscard]] static TSValue copy(const TSValue& other);

    // ========== Validity ==========

    /**
     * @brief Check if the TSValue contains valid data.
     */
    [[nodiscard]] bool valid() const noexcept;

    /**
     * @brief Boolean conversion - returns validity.
     */
    explicit operator bool() const noexcept { return valid(); }

    // ========== Schema Access ==========

    /**
     * @brief Get the time-series schema.
     * @return The TSMeta schema, or nullptr if invalid
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return _ts_meta; }

    /**
     * @brief Get the value schema (for data storage).
     * @return The TypeMeta schema from ts_meta->value_schema()
     */
    [[nodiscard]] const value::TypeMeta* value_schema() const noexcept;

    // ========== Ownership ==========

    /**
     * @brief Get the owning node.
     * @return The Node that owns this TSValue, or nullptr
     */
    [[nodiscard]] Node* owning_node() const noexcept { return _owning_node; }

    /**
     * @brief Get the output ID.
     * @return OUTPUT_MAIN, ERROR_PATH, STATE_PATH, or custom ID
     */
    [[nodiscard]] int output_id() const noexcept { return _output_id; }

    // ========== View Creation ==========

    /**
     * @brief Get a read-only view of the data.
     * @return TSView for reading values
     */
    [[nodiscard]] TSView view() const;

    /**
     * @brief Get a mutable view of the data.
     *
     * This invalidates any cached Python object.
     *
     * @return TSMutableView for reading and writing values
     */
    [[nodiscard]] TSMutableView mutable_view();

    /**
     * @brief Get a bundle-specific view (for TSB types).
     *
     * @return TSBView with field navigation methods
     * @throws std::runtime_error if this is not a bundle type
     */
    [[nodiscard]] TSBView bundle_view() const;

    // ========== Direct Value Access ==========

    /**
     * @brief Get the underlying Value.
     */
    [[nodiscard]] base_value_type& value() { return _value; }

    /**
     * @brief Get the underlying Value (const).
     */
    [[nodiscard]] const base_value_type& value() const { return _value; }

    // ========== Python Interop ==========

    /**
     * @brief Convert to Python object (uses caching).
     */
    [[nodiscard]] nb::object to_python() const;

    /**
     * @brief Set from Python object.
     *
     * This invalidates the cache and triggers modification callbacks.
     */
    void from_python(const nb::object& src);

    // ========== Modification Tracking ==========

    /**
     * @brief Register a callback for modifications.
     */
    template<typename Callback>
    void on_modified(Callback&& cb) {
        _value.on_modified(std::forward<Callback>(cb));
    }

private:
    base_value_type _value;              ///< Underlying type-erased storage
    const TSMeta* _ts_meta{nullptr};     ///< Time-series schema
    Node* _owning_node{nullptr};         ///< Owning node (not owned)
    int _output_id{OUTPUT_MAIN};         ///< Output identifier
};

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * @brief Create a TSValue from a time-series schema.
 *
 * Simple factory function for creating TSValue instances.
 *
 * @param ts_schema The time-series type metadata
 * @param owner Optional owning node
 * @param output_id The output identifier (OUTPUT_MAIN, ERROR_PATH, STATE_PATH)
 * @return A new TSValue configured for this time-series type
 */
inline TSValue make_ts_value(const TSMeta* ts_schema,
                             Node* owner = nullptr,
                             int output_id = OUTPUT_MAIN) {
    return TSValue(ts_schema, owner, output_id);
}

}  // namespace hgraph
