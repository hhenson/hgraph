#pragma once

/**
 * @file ts_input_root.h
 * @brief TSInputRoot - Top-level input container with link support.
 *
 * TSInputRoot wraps a TSValue (always a TSB) with link support enabled.
 * It provides the user-facing API for input access and binding.
 *
 * Key features:
 * - Root is always a bundle type (TSB)
 * - Link support enabled for transparent navigation to linked outputs
 * - Field binding methods for connecting to external outputs
 * - Active/passive control for subscription management
 * - State queries that aggregate across all linked children
 *
 * Usage:
 * @code
 * // Create input from schema
 * TSInputRoot input(bundle_meta, owning_node);
 *
 * // Bind fields to outputs
 * input.bind_field("price", price_output.ts_value());
 * input.bind_field("volume", volume_output.ts_value());
 *
 * // Make active to receive notifications
 * input.make_active();
 *
 * // Navigate through input (links are followed transparently)
 * TSView price = input.field("price");
 * float val = price.as<float>();
 * @endcode
 *
 * See: ts_design_docs/TSInput_DESIGN.md
 */

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/ts_link.h>
#include <stdexcept>
#include <string>

namespace hgraph {

/**
 * @brief Top-level input container with link support.
 *
 * Wraps a TSValue (always a TSB) to provide:
 * - Transparent navigation through links
 * - Field binding to external outputs
 * - Active/passive subscription control
 * - Aggregated state queries
 */
struct TSInputRoot {
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid input.
     */
    TSInputRoot() noexcept = default;

    /**
     * @brief Construct from a bundle schema and owning node.
     *
     * Creates a TSValue with link support enabled.
     *
     * @param meta The bundle type metadata (must be TSB)
     * @param node The owning node for notification routing
     */
    TSInputRoot(const TSBTypeMeta* meta, Node* node);

    /**
     * @brief Construct from a generic TSMeta (must be TSB).
     *
     * @param meta The time-series metadata (must be TSB type)
     * @param node The owning node for notification routing
     * @throws std::runtime_error if meta is not a bundle type
     */
    TSInputRoot(const TSMeta* meta, Node* node);

    // Move operations
    TSInputRoot(TSInputRoot&& other) noexcept = default;
    TSInputRoot& operator=(TSInputRoot&& other) noexcept = default;

    // Non-copyable
    TSInputRoot(const TSInputRoot&) = delete;
    TSInputRoot& operator=(const TSInputRoot&) = delete;

    // ========== Validity ==========

    /**
     * @brief Check if the input is valid.
     */
    [[nodiscard]] bool valid() const noexcept;

    /**
     * @brief Boolean conversion - returns validity.
     */
    explicit operator bool() const noexcept { return valid(); }

    // ========== Navigation ==========

    /**
     * @brief Get a view of a field by index.
     *
     * If the field is linked, returns a view into the linked output.
     * Otherwise returns a view into local data.
     *
     * @param index Field index (0-based)
     * @return TSView for the field
     * @throws std::out_of_range if index out of bounds
     */
    [[nodiscard]] TSView field(size_t index) const;

    /**
     * @brief Get a view of a field by name.
     *
     * @param name Field name
     * @return TSView for the field
     * @throws std::runtime_error if field not found
     */
    [[nodiscard]] TSView field(const std::string& name) const;

    /**
     * @brief Element access (alias for field by index).
     *
     * TSB supports element access like a tuple.
     *
     * @param index Field/element index
     * @return TSView for the element
     */
    [[nodiscard]] TSView element(size_t index) const { return field(index); }

    /**
     * @brief Get the number of fields.
     */
    [[nodiscard]] size_t size() const noexcept;

    /**
     * @brief Get the bundle schema.
     */
    [[nodiscard]] const TSBTypeMeta* bundle_meta() const noexcept;

    /**
     * @brief Get a bundle view for the entire input.
     *
     * The returned view has link support enabled for transparent navigation.
     */
    [[nodiscard]] TSBView bundle_view() const;

    // ========== Binding ==========

    /**
     * @brief Bind a field to an external output (make it peered).
     *
     * The field will now return views into the linked output
     * instead of local data.
     *
     * @param index Field index
     * @param output The TSValue output to link to
     */
    void bind_field(size_t index, const TSValue* output);

    /**
     * @brief Bind a field to an external output by name.
     *
     * @param name Field name
     * @param output The TSValue output to link to
     * @throws std::runtime_error if field not found
     */
    void bind_field(const std::string& name, const TSValue* output);

    /**
     * @brief Unbind a field (disconnect from output).
     *
     * The field will now return views into local data.
     * Active state of the link is preserved.
     *
     * @param index Field index
     */
    void unbind_field(size_t index);

    /**
     * @brief Unbind a field by name.
     *
     * @param name Field name
     */
    void unbind_field(const std::string& name);

    /**
     * @brief Check if a field is bound (linked) to an output.
     *
     * @param index Field index
     * @return true if the field is linked to an external output
     */
    [[nodiscard]] bool is_field_bound(size_t index) const noexcept;

    /**
     * @brief Check if a field is bound by name.
     *
     * @param name Field name
     * @return true if the field is linked
     */
    [[nodiscard]] bool is_field_bound(const std::string& name) const noexcept;

    // ========== Active Control ==========

    /**
     * @brief Make all links active (subscribe to outputs).
     *
     * When active, modifications to linked outputs will notify
     * the owning node.
     */
    void make_active();

    /**
     * @brief Make all links passive (unsubscribe from outputs).
     *
     * When passive, modifications to linked outputs are not notified.
     */
    void make_passive();

    /**
     * @brief Check if the input is active.
     *
     * @return true if any link is active
     */
    [[nodiscard]] bool active() const noexcept { return _active; }

    /**
     * @brief Check and trigger startup notifications for all links.
     *
     * For REF bindings (notify_once mode), the owning node needs to be
     * notified on the first tick even if the underlying output wasn't
     * modified. This method checks all links and triggers notifications
     * for those that need startup notification.
     *
     * Should be called after make_active() during node startup.
     *
     * @param start_time The graph start time for notification
     */
    void check_links_startup_notify(engine_time_t start_time);

    // ========== State Queries ==========

    /**
     * @brief Check if any field was modified at the given time.
     *
     * @param time The time to check against
     * @return true if any linked field was modified at time
     */
    [[nodiscard]] bool modified_at(engine_time_t time) const;

    /**
     * @brief Check if all linked fields are valid.
     *
     * @return true if all linked fields have been set
     */
    [[nodiscard]] bool all_valid() const;

    /**
     * @brief Get the last modification time of any field.
     *
     * @return The most recent modification time across all fields
     */
    [[nodiscard]] engine_time_t last_modified_time() const;

    // ========== Direct Access ==========

    /**
     * @brief Get the underlying TSValue.
     *
     * @return Reference to the root TSValue
     */
    [[nodiscard]] TSValue& value() noexcept { return _value; }

    /**
     * @brief Get the underlying TSValue (const).
     */
    [[nodiscard]] const TSValue& value() const noexcept { return _value; }

    /**
     * @brief Get the owning node.
     */
    [[nodiscard]] Node* owning_node() const noexcept { return _node; }

private:
    TSValue _value;           ///< Root bundle with link support
    Node* _node{nullptr};     ///< Owning node for notification routing
    bool _active{false};      ///< Whether links are currently active

    /**
     * @brief Get field index by name.
     * @return Index if found, throws otherwise
     */
    size_t field_index(const std::string& name) const;
};

}  // namespace hgraph
