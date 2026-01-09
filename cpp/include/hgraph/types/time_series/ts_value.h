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
 * - Hierarchical modification tracking via a shadow Value
 *
 * This is the core storage type for all time-series data in nodes.
 */

#include <hgraph/types/value/value.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_overlay_storage.h>
#include <hgraph/types/time_series/ts_link.h>

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
struct TSValue {
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

    /**
     * @brief Notify that the value was modified at given time.
     * @param time The engine time of modification
     */
    void notify_modified(engine_time_t time);

    /**
     * @brief Get the last modification time.
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    /**
     * @brief Check if modified at specific time.
     * @param time The time to check against
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const;

    /**
     * @brief Check if the time-series value is valid (has been set).
     */
    [[nodiscard]] bool ts_valid() const;

    /**
     * @brief Mark the time-series as invalid (cleared).
     */
    void invalidate_ts();

    // ========== Overlay Access ==========

    /**
     * @brief Get the overlay storage (mutable).
     *
     * The overlay provides hierarchical modification tracking and observer
     * management. It mirrors the TSMeta structure.
     *
     * @return Pointer to the overlay, or nullptr if not initialized
     */
    [[nodiscard]] TSOverlayStorage* overlay() noexcept {
        return _overlay.get();
    }

    /**
     * @brief Get the overlay storage (const).
     */
    [[nodiscard]] const TSOverlayStorage* overlay() const noexcept {
        return _overlay.get();
    }

private:
    base_value_type _value;                         ///< Underlying type-erased storage
    std::unique_ptr<TSOverlayStorage> _overlay;     ///< Hierarchical modification tracking + observers
    const TSMeta* _ts_meta{nullptr};                ///< Time-series schema
    Node* _owning_node{nullptr};                    ///< Owning node (not owned)
    int _output_id{OUTPUT_MAIN};                    ///< Output identifier

    // ========== Link Support (for inputs) ==========

    /**
     * @brief Container for link support data.
     *
     * Separated into its own struct so that TSValues without link support
     * (the majority - all outputs) only pay 8 bytes (nullptr) instead of
     * ~48 bytes (two empty vectors).
     */
    struct LinkSupport {
        /**
         * @brief Per-child link tracking for composite types.
         *
         * Indexed by child position (field index for TSB, element index for TSL).
         * - nullptr = local data (non-peered at this position)
         * - non-null = linked to external output (peered)
         */
        std::vector<std::unique_ptr<TSLink>> child_links;

        /**
         * @brief Per-child TSValue storage for non-peered composite children.
         *
         * When a child position is non-peered but is itself a composite type
         * (TSB or TSL) that may have peered grandchildren, we need nested
         * TSValues with their own link support.
         *
         * - nullptr = child is either peered (use link) or a leaf type
         * - non-null = child is a non-peered composite with nested structure
         */
        std::vector<std::unique_ptr<TSValue>> child_values;
    };

    /**
     * @brief Optional link support data.
     *
     * Only allocated when enable_link_support() is called.
     * nullptr for outputs and TSValues not used as inputs.
     */
    std::unique_ptr<LinkSupport> _link_support;

public:
    // ========== Link Support API ==========

    /**
     * @brief Check if this TSValue has link support enabled.
     *
     * Link support is enabled for TSValues used as inputs to allow
     * binding child positions to external outputs.
     */
    [[nodiscard]] bool has_link_support() const noexcept {
        return _link_support != nullptr;
    }

    /**
     * @brief Enable link support for this TSValue.
     *
     * Allocates link slots for each child position based on schema.
     * Initially all slots are nullptr (non-peered/local).
     * Call this to make a TSValue usable as an input.
     *
     * Only valid for composite types (TSB, TSL). No-op for scalars.
     */
    void enable_link_support();

    /**
     * @brief Check if child at index is linked (peered).
     * @param index Child position (field index for TSB, element index for TSL)
     * @return true if the child is linked to an external output
     */
    [[nodiscard]] bool is_linked(size_t index) const noexcept {
        if (!_link_support) return false;
        return index < _link_support->child_links.size() &&
               _link_support->child_links[index] != nullptr &&
               _link_support->child_links[index]->bound();
    }

    /**
     * @brief Get link at index (mutable).
     * @param index Child position
     * @return TSLink pointer, or nullptr if not linked
     */
    [[nodiscard]] TSLink* link_at(size_t index) noexcept {
        if (!_link_support) return nullptr;
        return index < _link_support->child_links.size() ? _link_support->child_links[index].get() : nullptr;
    }

    /**
     * @brief Get link at index (const).
     * @param index Child position
     * @return TSLink pointer, or nullptr if not linked
     */
    [[nodiscard]] const TSLink* link_at(size_t index) const noexcept {
        if (!_link_support) return nullptr;
        return index < _link_support->child_links.size() ? _link_support->child_links[index].get() : nullptr;
    }

    /**
     * @brief Create a link at child position (makes it peered).
     *
     * If a link already exists at this position, it is rebound.
     *
     * @param index Child position (field index or element index)
     * @param output The TSValue output to link to
     */
    void create_link(size_t index, const TSValue* output);

    /**
     * @brief Remove link at child position (unbind).
     *
     * Active state of the link is preserved.
     *
     * @param index Child position
     */
    void remove_link(size_t index);

    /**
     * @brief Get nested TSValue for non-peered composite child.
     *
     * For non-peered children that are themselves composite types,
     * this returns the nested TSValue that holds their link structure.
     *
     * @param index Child position
     * @return Pointer to child TSValue, or nullptr if linked or leaf
     */
    [[nodiscard]] TSValue* child_value(size_t index) noexcept {
        if (!_link_support) return nullptr;
        return index < _link_support->child_values.size() ? _link_support->child_values[index].get() : nullptr;
    }

    /**
     * @brief Get nested TSValue for non-peered composite child (const).
     */
    [[nodiscard]] const TSValue* child_value(size_t index) const noexcept {
        if (!_link_support) return nullptr;
        return index < _link_support->child_values.size() ? _link_support->child_values[index].get() : nullptr;
    }

    /**
     * @brief Get or create a non-peered child TSValue at index.
     *
     * For non-peered children that are themselves collections,
     * we need nested TSValues with their own link support.
     *
     * @param index Child position
     * @return Pointer to child TSValue (created if needed)
     */
    TSValue* get_or_create_child_value(size_t index);

    /**
     * @brief Make all links active (subscribe to their outputs).
     *
     * Recursively activates any non-linked children that have links.
     */
    void make_links_active();

    /**
     * @brief Make all links passive (unsubscribe from their outputs).
     *
     * Recursively deactivates any non-linked children.
     */
    void make_links_passive();

    /**
     * @brief Get the number of child slots (for link support).
     *
     * Returns 0 if link support is not enabled.
     */
    [[nodiscard]] size_t child_count() const noexcept {
        return _link_support ? _link_support->child_links.size() : 0;
    }
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
