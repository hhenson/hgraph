#pragma once

/**
 * @file short_path.h
 * @brief ShortPath - Graph-aware navigation path for time-series.
 *
 * ShortPath enables tracing back from a TSView to its owning node and port,
 * supporting scheduling and subscription operations. This is distinct from
 * value::ViewPath which tracks navigation within a single Value.
 *
 * ShortPath is the foundation for:
 * - Link binding (TSInput -> TSOutput connections)
 * - Observer subscription chains
 * - Delta propagation paths
 */

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <string>
#include <vector>

namespace hgraph {

// Forward declarations
class TSView;
class FQPath;
struct ViewData;

/**
 * @brief Port type for time-series endpoints.
 */
enum class PortType : uint8_t {
    INPUT,   ///< TSInput port
    OUTPUT   ///< TSOutput port
};

/**
 * @brief Graph-aware navigation path for time-series.
 *
 * ShortPath tracks:
 * - The owning Node (for scheduling/subscription)
 * - Whether this is an input or output port
 * - Navigation indices within the time-series structure
 *
 * Unlike value::ViewPath (which uses names and indices for debugging),
 * ShortPath uses only indices for efficient runtime navigation and
 * includes the graph context (Node*, PortType) needed for scheduling.
 *
 * Usage:
 * @code
 * // Get the path from a TSView
 * ShortPath path = view.short_path();
 *
 * // Check ownership
 * Node* owner = path.node();
 * PortType port = path.port_type();
 *
 * // Navigate indices
 * for (size_t idx : path.indices()) {
 *     // Process navigation step
 * }
 *
 * // Resolve to a view at a specific time
 * TSView resolved = path.resolve(current_time);
 * @endcode
 */
class ShortPath {
public:
    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an invalid path.
     */
    ShortPath() noexcept = default;

    /**
     * @brief Construct a root path for a node port.
     *
     * @param node The owning node
     * @param port_type INPUT or OUTPUT
     */
    ShortPath(node_ptr node, PortType port_type) noexcept
        : node_(node), port_type_(port_type) {}

    /**
     * @brief Construct with initial indices.
     *
     * @param node The owning node
     * @param port_type INPUT or OUTPUT
     * @param indices Navigation indices
     */
    ShortPath(node_ptr node, PortType port_type, std::vector<size_t> indices) noexcept
        : node_(node), port_type_(port_type), indices_(std::move(indices)) {}

    // ========== Validity ==========

    /**
     * @brief Check if the path is valid.
     * @return true if the path has a valid node
     */
    [[nodiscard]] bool valid() const noexcept {
        return node_ != nullptr;
    }

    /**
     * @brief Boolean conversion - returns validity.
     */
    explicit operator bool() const noexcept {
        return valid();
    }

    // ========== Accessors ==========

    /**
     * @brief Get the owning node.
     * @return The node pointer, or nullptr if invalid
     */
    [[nodiscard]] node_ptr node() const noexcept {
        return node_;
    }

    /**
     * @brief Get the port type.
     * @return INPUT or OUTPUT
     */
    [[nodiscard]] PortType port_type() const noexcept {
        return port_type_;
    }

    /**
     * @brief Check if this is an input port.
     */
    [[nodiscard]] bool is_input() const noexcept {
        return port_type_ == PortType::INPUT;
    }

    /**
     * @brief Check if this is an output port.
     */
    [[nodiscard]] bool is_output() const noexcept {
        return port_type_ == PortType::OUTPUT;
    }

    /**
     * @brief Get the navigation indices.
     * @return Vector of indices for navigating nested structures
     */
    [[nodiscard]] const std::vector<size_t>& indices() const noexcept {
        return indices_;
    }

    /**
     * @brief Get the path depth (number of navigation steps).
     */
    [[nodiscard]] size_t depth() const noexcept {
        return indices_.size();
    }

    /**
     * @brief Check if this is a root path (no navigation).
     */
    [[nodiscard]] bool is_root() const noexcept {
        return indices_.empty();
    }

    // ========== Navigation ==========

    /**
     * @brief Create a child path by appending an index.
     *
     * @param index The index to append
     * @return A new ShortPath with the index appended
     */
    [[nodiscard]] ShortPath child(size_t index) const {
        ShortPath result(*this);
        result.indices_.push_back(index);
        return result;
    }

    /**
     * @brief Append an index to this path (in-place).
     *
     * @param index The index to append
     */
    void push(size_t index) {
        indices_.push_back(index);
    }

    /**
     * @brief Remove the last index from this path (in-place).
     *
     * @return The removed index
     * @throws std::out_of_range if the path is empty
     */
    size_t pop() {
        if (indices_.empty()) {
            throw std::out_of_range("ShortPath::pop() on empty path");
        }
        size_t result = indices_.back();
        indices_.pop_back();
        return result;
    }

    /**
     * @brief Get the parent path (path without the last index).
     *
     * @return The parent path
     * @throws std::out_of_range if the path is already root
     */
    [[nodiscard]] ShortPath parent() const {
        if (indices_.empty()) {
            throw std::out_of_range("ShortPath::parent() on root path");
        }
        ShortPath result(*this);
        result.indices_.pop_back();
        return result;
    }

    // ========== Resolution ==========

    /**
     * @brief Resolve this path to a TSView at the given time.
     *
     * Navigates from the node's port through the indices to produce
     * a TSView bound to the specified time.
     *
     * @param current_time The time to bind the view to
     * @return The resolved TSView
     * @throws std::runtime_error if resolution fails
     */
    [[nodiscard]] TSView resolve(engine_time_t current_time) const;

    // ========== Conversion ==========

    /**
     * @brief Convert to a fully-qualified path string.
     *
     * Format: "node_id.port[idx1][idx2]..."
     *
     * @return String representation of the path
     */
    [[nodiscard]] std::string to_string() const;

    /**
     * @brief Convert to a fully-qualified path (FQPath).
     *
     * Navigates through the ViewData structure to convert slot indices
     * to semantic path elements:
     * - TSB: slot index -> field name
     * - TSL: slot index -> list index (unchanged)
     * - TSD: slot index -> actual key value (cloned)
     *
     * @param root_vd The root ViewData for this path's port
     * @return FQPath with semantic path elements
     * @throws std::runtime_error if navigation fails
     */
    [[nodiscard]] FQPath to_fq(const ViewData& root_vd) const;

    // ========== Comparison ==========

    /**
     * @brief Check equality with another path.
     */
    [[nodiscard]] bool operator==(const ShortPath& other) const noexcept {
        return node_ == other.node_ &&
               port_type_ == other.port_type_ &&
               indices_ == other.indices_;
    }

    /**
     * @brief Check inequality with another path.
     */
    [[nodiscard]] bool operator!=(const ShortPath& other) const noexcept {
        return !(*this == other);
    }

private:
    node_ptr node_{nullptr};
    PortType port_type_{PortType::OUTPUT};
    std::vector<size_t> indices_;
};

} // namespace hgraph
