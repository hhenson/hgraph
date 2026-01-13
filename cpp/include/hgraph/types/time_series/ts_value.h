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
#include <hgraph/types/time_series/ts_ref_target_link.h>

#include <unordered_map>
#include <any>

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
     * @brief Destructor.
     */
    ~TSValue() = default;

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

    // ========== REF Output Cache ==========
    /**
     * @brief Cached Python object for REF outputs.
     *
     * When this TSValue is a REF output, stores the TimeSeriesReference
     * Python object so views can auto-dereference to the target.
     * Uses std::any to avoid nanobind dependency in header.
     */
    mutable std::any _ref_cache;

public:
    /**
     * @brief Set the REF output cache value.
     * @param value The Python object to cache (should be nb::object)
     */
    void set_ref_cache(std::any value) const { _ref_cache = std::move(value); }

    /**
     * @brief Get the REF output cache value.
     * @return Reference to the cached any, or empty any if not set
     */
    [[nodiscard]] const std::any& ref_cache() const noexcept { return _ref_cache; }

    /**
     * @brief Check if REF cache has a value.
     */
    [[nodiscard]] bool has_ref_cache() const noexcept { return _ref_cache.has_value(); }

    /**
     * @brief Clear the REF cache.
     */
    void clear_ref_cache() const { _ref_cache.reset(); }

private:

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
         * Uses LinkStorage variant to support both TSLink (non-REF) and
         * TSRefTargetLink (REF->TS) with zero overhead for the common case.
         *
         * - monostate = local data (non-peered at this position)
         * - TSLink = linked to non-REF external output
         * - TSRefTargetLink = linked to REF external output
         */
        std::vector<LinkStorage> child_links;

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

    // ========== Cast Cache (for outputs) ==========

    /**
     * @brief Cache of TSValues converted to different target schemas.
     *
     * When an input needs a different type view of this output
     * (e.g., REF[TS[V]] from TS[V]), the converted value is cached here.
     * Multiple inputs can share the same conversion.
     *
     * KEY: Target TSMeta* - the schema to cast TO
     * VALUE: Converted TSValue with that target schema
     *
     * This allows a single source to have multiple different conversions
     * cached simultaneously:
     *   - TS[V] → REF[TS[V]]  (key: REF[TS[V]] schema)
     *   - TSD[K, V] → TSD[K, REF[V]]  (key: TSD[K, REF[V]] schema)
     *   - Future: other conversion types
     *
     * Casting is RECURSIVE - when casting a composite type, each child
     * is also cast to its corresponding target type, creating a hierarchy
     * of cached conversions.
     */
    std::unique_ptr<std::unordered_map<const TSMeta*, std::unique_ptr<TSValue>>> _cast_cache;

    /**
     * @brief For cast TSValues, points to the source TSValue this was cast from.
     *
     * When a TSValue is created via cast_to(), this pointer is set to the
     * original source TSValue. This allows:
     * - REF casts to synthesize TimeSeriesReference values pointing to source
     * - Navigation back to source data for view access
     * - Recursive cast chains to track their origin
     *
     * nullptr for non-cast TSValues (regular outputs/inputs).
     */
    const TSValue* _cast_source{nullptr};

    /**
     * @brief Index within parent container for cast TSValues.
     *
     * For list/bundle element casts where _cast_source points to the container,
     * this stores the element index for proper source data access.
     * -1 if not applicable (root cast or non-indexed).
     */
    int64_t _cast_index{-1};

    /**
     * @brief Source element TSValue for element wrapper casts.
     *
     * For TS→REF element conversions in lists/bundles, this points to a
     * source element TSValue (with non-REF schema like TS[V]) that provides
     * the actual element data. The TimeSeriesReference is created from this.
     *
     * When _cast_source is a container (list/bundle) and _cast_index >= 0,
     * this provides the element-level TSValue for TimeSeriesReference creation.
     */
    TSValue* _source_element{nullptr};

    // ========== REF Observer Support (for REF outputs) ==========

    /**
     * @brief Observers that should be rebound when this REF output's value changes.
     *
     * When a non-REF input binds to a REF output, Python's observer pattern
     * causes the input to be REBOUND to the target when the REF value is set.
     * This mimics Python's TimeSeriesReferenceOutput._reference_observers.
     *
     * Each entry is (input_ts_value, link_index) - the input TSValue with a
     * link at link_index that should be rebound to the target.
     *
     * Only allocated for REF outputs that have observers registered.
     */
    std::unique_ptr<std::vector<std::pair<TSValue*, size_t>>> _ref_observers;

public:
    // ========== REF Observer API ==========

    /**
     * @brief Register as an observer of this REF output.
     *
     * When this REF output's value changes, the input at link_index will
     * be rebound to the new target.
     *
     * @param input_ts_value The input TSValue containing the link
     * @param link_index The index within input_ts_value where the link is
     */
    void observe_ref(TSValue* input_ts_value, size_t link_index);

    /**
     * @brief Unregister as an observer of this REF output.
     */
    void stop_observing_ref(TSValue* input_ts_value, size_t link_index);

    /**
     * @brief Notify all REF observers to rebind to the target.
     *
     * Called when this REF output's value (TimeSeriesReference) changes.
     * @param target The target TSValue that the TimeSeriesReference points to
     */
    void notify_ref_observers(const TSValue* target);

    /**
     * @brief Notify all REF observers to rebind to an element within a container.
     *
     * Called when this REF output's value references an element in a container
     * (like TSL) that doesn't have its own TSValue.
     * @param container The container TSValue
     * @param elem_index The element index within the container
     */
    void notify_ref_observers_element(const TSValue* container, size_t elem_index);

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
        if (index >= _link_support->child_links.size()) return false;
        return link_storage_bound(_link_support->child_links[index]);
    }

    /**
     * @brief Get TSLink at index (for non-REF bindings).
     * @param index Child position
     * @return TSLink pointer, or nullptr if not linked or is REF binding
     */
    [[nodiscard]] TSLink* link_at(size_t index) noexcept {
        if (!_link_support || index >= _link_support->child_links.size()) return nullptr;
        auto* ptr = std::get_if<std::unique_ptr<TSLink>>(&_link_support->child_links[index]);
        return ptr ? ptr->get() : nullptr;
    }

    /**
     * @brief Get TSLink at index (const, for non-REF bindings).
     * @param index Child position
     * @return TSLink pointer, or nullptr if not linked or is REF binding
     */
    [[nodiscard]] const TSLink* link_at(size_t index) const noexcept {
        if (!_link_support || index >= _link_support->child_links.size()) return nullptr;
        auto* ptr = std::get_if<std::unique_ptr<TSLink>>(&_link_support->child_links[index]);
        return ptr ? ptr->get() : nullptr;
    }

    /**
     * @brief Get TSRefTargetLink at index (for REF->TS bindings).
     * @param index Child position
     * @return TSRefTargetLink pointer, or nullptr if not linked or is non-REF binding
     */
    [[nodiscard]] TSRefTargetLink* ref_link_at(size_t index) noexcept {
        if (!_link_support || index >= _link_support->child_links.size()) return nullptr;
        auto* ptr = std::get_if<std::unique_ptr<TSRefTargetLink>>(&_link_support->child_links[index]);
        return ptr ? ptr->get() : nullptr;
    }

    /**
     * @brief Get TSRefTargetLink at index (const, for REF->TS bindings).
     * @param index Child position
     * @return TSRefTargetLink pointer, or nullptr if not linked or is non-REF binding
     */
    [[nodiscard]] const TSRefTargetLink* ref_link_at(size_t index) const noexcept {
        if (!_link_support || index >= _link_support->child_links.size()) return nullptr;
        auto* ptr = std::get_if<std::unique_ptr<TSRefTargetLink>>(&_link_support->child_links[index]);
        return ptr ? ptr->get() : nullptr;
    }

    /**
     * @brief Get LinkStorage at index (for direct variant access).
     * @param index Child position
     * @return Pointer to LinkStorage, or nullptr if no link support
     */
    [[nodiscard]] LinkStorage* link_storage_at(size_t index) noexcept {
        if (!_link_support || index >= _link_support->child_links.size()) return nullptr;
        return &_link_support->child_links[index];
    }

    /**
     * @brief Get LinkStorage at index (const).
     * @param index Child position
     * @return Pointer to const LinkStorage, or nullptr if no link support
     */
    [[nodiscard]] const LinkStorage* link_storage_at(size_t index) const noexcept {
        if (!_link_support || index >= _link_support->child_links.size()) return nullptr;
        return &_link_support->child_links[index];
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

    // ========== Cast Cache API ==========

    /**
     * @brief Get or create a cast view of this TSValue.
     *
     * If the target schema matches this TSValue's schema, returns this.
     * Otherwise, creates/returns a cached converted TSValue.
     *
     * For composite types (TSB, TSL, TSD), casting is recursive:
     * - Each child element is cast to its corresponding target type
     * - The cast TSValue links to child casts (not copies)
     *
     * This enables TS → REF conversions at any level of the hierarchy:
     * - TS[V] → REF[TS[V]]
     * - TSB[a: TS[V]] → TSB[a: REF[TS[V]]]
     * - TSD[K, V] → TSD[K, REF[V]]
     *
     * @param target_schema The TSMeta schema to cast to
     * @return Pointer to TSValue with target schema (this or cached)
     */
    TSValue* cast_to(const TSMeta* target_schema);

    /**
     * @brief Check if a cast to target schema is already cached.
     */
    [[nodiscard]] bool has_cast(const TSMeta* target_schema) const;

    /**
     * @brief Clear the cast cache (e.g., on invalidation).
     */
    void clear_cast_cache();

    /**
     * @brief Get the source TSValue this was cast from.
     * @return Source TSValue pointer, or nullptr if not a cast value
     */
    [[nodiscard]] const TSValue* cast_source() const noexcept {
        return _cast_source;
    }

    /**
     * @brief Check if this is a cast TSValue.
     */
    [[nodiscard]] bool is_cast() const noexcept {
        return _cast_source != nullptr;
    }

    /**
     * @brief Get the element index for container element casts.
     * @return Element index, or -1 if not an indexed cast
     */
    [[nodiscard]] int64_t cast_index() const noexcept {
        return _cast_index;
    }

    /**
     * @brief Get the source element TSValue for element wrapper casts.
     * @return Source element TSValue, or nullptr if not an element cast
     */
    [[nodiscard]] TSValue* source_element() const noexcept {
        return _source_element;
    }

private:
    // Cast creation helpers - dispatched by type kind
    std::unique_ptr<TSValue> create_cast_value(const TSMeta* target_schema);
    void setup_ts_to_ref_cast(TSValue& cast_value);
    void setup_tsb_cast(TSValue& cast_value, const TSMeta* target_schema);
    void setup_tsl_cast(TSValue& cast_value, const TSMeta* target_schema);
    void setup_tsd_cast(TSValue& cast_value, const TSMeta* target_schema);
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
