#pragma once

/**
 * @file ts_output.h
 * @brief TSOutput - Producer of time-series values.
 *
 * TSOutput owns the native time-series value and manages cast alternatives
 * for consumers that need different schemas. Observer management is delegated
 * to TSValue's observer component.
 *
 * @see design/05_TSOUTPUT_TSINPUT.md
 * @see user_guide/05_TSOUTPUT_TSINPUT.md
 */

#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/ts_view.h>
#include <hgraph/types/time_series/short_path.h>
#include <hgraph/types/time_series/ref_link.h>
#include <hgraph/hgraph_forward_declarations.h>

#include <unordered_map>
#include <vector>
#include <memory>

namespace hgraph {

// Forward declarations
class TSOutputView;

/**
 * @brief Producer of time-series values.
 *
 * TSOutput owns the native time-series value and manages cast alternatives
 * for consumers that need different schemas. Each alternative is a TSValue
 * with links back to the native value (or REFLinks for REF→TS conversion).
 *
 * Key responsibilities:
 * - Owns native TSValue storage
 * - Creates alternatives on-demand when view() is called with different schema
 * - Provides TSOutputView for access and mutation
 *
 * Usage:
 * @code
 * // Create output with schema
 * TSOutput output(ts_meta, owning_node);
 *
 * // Get view for mutation
 * TSOutputView view = output.view(current_time);
 * view.set_value(some_value);
 *
 * // Get alternative view (creates on demand)
 * TSOutputView alt_view = output.view(current_time, different_schema);
 * @endcode
 */
class TSOutput {
public:
    // ========== Construction ==========

    /**
     * @brief Construct TSOutput with schema and owning node.
     *
     * @param ts_meta Schema for the native value
     * @param owner The Node that owns this output (for graph context)
     * @param port_index The port index on the owning node
     */
    TSOutput(const TSMeta* ts_meta, node_ptr owner, size_t port_index = 0);

    /**
     * @brief Default constructor - creates invalid TSOutput.
     */
    TSOutput() noexcept = default;

    // Non-copyable, movable
    TSOutput(const TSOutput&) = delete;
    TSOutput& operator=(const TSOutput&) = delete;
    TSOutput(TSOutput&&) noexcept = default;
    TSOutput& operator=(TSOutput&&) noexcept = default;

    ~TSOutput() = default;

    // ========== View Access ==========

    /**
     * @brief Get view for this output at current time using native schema.
     *
     * @param current_time The current engine time
     * @return TSOutputView for access and mutation
     */
    TSOutputView view(engine_time_t current_time);

    /**
     * @brief Get view for this output at current time with specific schema.
     *
     * If schema differs from native, returns view to alternative (created on demand).
     * The alternative contains links back to native (or REFLinks for REF→TS).
     *
     * @param current_time The current engine time
     * @param schema The requested schema
     * @return TSOutputView for the requested schema
     */
    TSOutputView view(engine_time_t current_time, const TSMeta* schema);

    // ========== Accessors ==========

    /**
     * @brief Get the owning node.
     */
    [[nodiscard]] node_ptr owning_node() const noexcept { return owning_node_; }

    /**
     * @brief Get the port index on the owning node.
     */
    [[nodiscard]] size_t port_index() const noexcept { return port_index_; }

    /**
     * @brief Get the native schema.
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return native_value_.meta(); }

    /**
     * @brief Get mutable reference to native value.
     */
    [[nodiscard]] TSValue& native_value() noexcept { return native_value_; }

    /**
     * @brief Get const reference to native value.
     */
    [[nodiscard]] const TSValue& native_value() const noexcept { return native_value_; }

    /**
     * @brief Get the root ShortPath for this output.
     */
    [[nodiscard]] ShortPath root_path() const {
        return ShortPath(owning_node_, PortType::OUTPUT, {port_index_});
    }

    /**
     * @brief Check if valid (has schema).
     */
    [[nodiscard]] bool valid() const noexcept { return native_value_.meta() != nullptr; }

private:
    // ========== Alternative Management ==========

    /**
     * @brief Get or create alternative for given schema.
     *
     * Creates links from alternative to native as appropriate:
     * - Same type: Direct link
     * - REF→TS: REFLink
     * - TS→REF: TSReference value
     *
     * @param schema The target schema
     * @return Reference to the alternative TSValue
     */
    TSValue& get_or_create_alternative(const TSMeta* schema);

    /**
     * @brief Establish links from alternative to native.
     *
     * Walks schemas in parallel, creating appropriate link types
     * based on the position-by-position logic.
     *
     * @param alt The alternative TSValue
     * @param alt_view View into the alternative
     * @param native_view View into the native value
     * @param target_meta Target schema at this position
     * @param native_meta Native schema at this position
     */
    void establish_links_recursive(
        TSValue& alt,
        TSView alt_view,
        TSView native_view,
        const TSMeta* target_meta,
        const TSMeta* native_meta
    );

    // ========== Member Variables ==========

    TSValue native_value_;                                          ///< Native representation
    std::unordered_map<const TSMeta*, TSValue> alternatives_;       ///< Cast/peer representations
    node_ptr owning_node_{nullptr};                                 ///< For graph context
    size_t port_index_{0};                                          ///< Port index on node

    // Note: REFLinks are stored inline in the alternative TSValue's link storage.
    // The link schema uses REFLink at each position, which can function as either
    // a simple link (like LinkTarget) or a full REFLink for REF→TS dereferencing.
    // This provides stable addresses and proper lifecycle management with the
    // two-phase removal pattern (mark dead + unsubscribe, then later destroy).
};

} // namespace hgraph
