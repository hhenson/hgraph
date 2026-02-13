#pragma once

/**
 * @file fq_path.h
 * @brief FQPath - Fully-qualified path for time-series navigation.
 *
 * FQPath is a standalone, serializable path that contains:
 * - Stable node identifier (survives across sessions)
 * - Port type (INPUT/OUTPUT)
 * - Semantic path elements (field names, indices, actual keys)
 *
 * Unlike ShortPath which uses raw slot indices for TSD, FQPath stores
 * the actual key values, enabling proper serialization and debugging.
 *
 * Created on-demand via ShortPath::to_fq() by navigating through ViewData.
 */

#include <hgraph/types/time_series/short_path.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace hgraph {

// Forward declarations
struct ViewData;
struct TSMeta;

/**
 * @brief Single element in a fully-qualified path.
 *
 * PathElement represents one navigation step:
 * - Field name (string) for TSB navigation
 * - Index (size_t) for TSL navigation
 * - Key (Value<>) for TSD navigation - owns the key value
 */
class PathElement {
public:
    using ElementVariant = std::variant<
        std::string,           // Field name (TSB)
        size_t,                // Index (TSL)
        value::Value<>         // Dict key (TSD) - owning
    >;

    // ========== Construction ==========

    PathElement() = default;

    PathElement(const PathElement& other);
    PathElement(PathElement&& other) noexcept = default;
    PathElement& operator=(const PathElement& other);
    PathElement& operator=(PathElement&& other) noexcept = default;

    // ========== Factory Methods ==========

    /**
     * @brief Create a field name element (for TSB navigation).
     */
    static PathElement field(std::string name) {
        PathElement e;
        e.element_ = std::move(name);
        return e;
    }

    /**
     * @brief Create an index element (for TSL navigation).
     */
    static PathElement index(size_t idx) {
        PathElement e;
        e.element_ = idx;
        return e;
    }

    /**
     * @brief Create a key element (for TSD navigation).
     * @param k The key value - will be moved/owned by PathElement
     */
    static PathElement key(value::Value<> k) {
        PathElement e;
        e.element_ = std::move(k);
        return e;
    }

    /**
     * @brief Create a key element by copying from a View.
     * @param key_view View of the key to copy
     */
    static PathElement key_from_view(const value::View& key_view);

    // ========== Queries ==========

    [[nodiscard]] bool is_field() const noexcept {
        return std::holds_alternative<std::string>(element_);
    }

    [[nodiscard]] bool is_index() const noexcept {
        return std::holds_alternative<size_t>(element_);
    }

    [[nodiscard]] bool is_key() const noexcept {
        return std::holds_alternative<value::Value<>>(element_);
    }

    // ========== Accessors ==========

    [[nodiscard]] const std::string& as_field() const {
        return std::get<std::string>(element_);
    }

    [[nodiscard]] size_t as_index() const {
        return std::get<size_t>(element_);
    }

    [[nodiscard]] const value::Value<>& as_key() const {
        return std::get<value::Value<>>(element_);
    }

    [[nodiscard]] value::View as_key_view() const {
        return std::get<value::Value<>>(element_).view();
    }

    // ========== Conversion ==========

    /**
     * @brief Convert to string representation.
     *
     * - Field: "fieldname"
     * - Index: "[0]"
     * - Key: "[key_repr]" where key_repr is the key's string representation
     */
    [[nodiscard]] std::string to_string() const;

    /**
     * @brief Convert to Python object.
     *
     * - Field: str
     * - Index: int
     * - Key: the Python representation of the key
     */
    [[nodiscard]] nb::object to_python() const;

    // ========== Comparison ==========

    [[nodiscard]] bool operator==(const PathElement& other) const;

    [[nodiscard]] bool operator!=(const PathElement& other) const {
        return !(*this == other);
    }

private:
    ElementVariant element_;
};


/**
 * @brief Fully-qualified path for time-series navigation.
 *
 * FQPath is a standalone, serializable path that contains:
 * - Stable node identifier (survives across sessions)
 * - Port type (INPUT/OUTPUT)
 * - Semantic path elements (field names, indices, actual keys)
 *
 * Created on-demand via ShortPath::to_fq() by navigating through ViewData.
 *
 * Unlike ShortPath which uses raw slot indices for TSD, FQPath stores
 * the actual key values, enabling proper serialization and debugging.
 *
 * Usage:
 * @code
 * // Get FQPath from a TSView
 * ShortPath sp = view.short_path();
 * ViewData root = output.root_view_data();
 * FQPath fq = sp.to_fq(root);
 *
 * // Convert to string for debugging
 * std::string str = fq.to_string();  // e.g., "42.out.prices[\"AAPL\"]"
 *
 * // Serialize to Python
 * nb::object py = fq.to_python();
 * @endcode
 */
class FQPath {
public:
    // ========== Construction ==========

    FQPath() noexcept = default;

    FQPath(std::vector<int64_t> node_id, PortType port_type)
        : node_id_(std::move(node_id)), port_type_(port_type) {}

    FQPath(std::vector<int64_t> node_id, PortType port_type, std::vector<PathElement> path)
        : node_id_(std::move(node_id)), port_type_(port_type), path_(std::move(path)) {}

    // ========== Accessors ==========

    /**
     * @brief Get the node identifier.
     *
     * For simple graphs, this is a single element [node_ndx].
     * For nested graphs, this is [graph_id..., node_ndx].
     */
    [[nodiscard]] const std::vector<int64_t>& node_id() const noexcept { return node_id_; }

    [[nodiscard]] PortType port_type() const noexcept { return port_type_; }

    [[nodiscard]] const std::vector<PathElement>& path() const noexcept { return path_; }

    [[nodiscard]] size_t depth() const noexcept { return path_.size(); }

    [[nodiscard]] bool is_root() const noexcept { return path_.empty(); }

    // ========== Modification ==========

    void push(PathElement elem) {
        path_.push_back(std::move(elem));
    }

    void push_field(std::string name) {
        path_.push_back(PathElement::field(std::move(name)));
    }

    void push_index(size_t idx) {
        path_.push_back(PathElement::index(idx));
    }

    void push_key(value::Value<> key) {
        path_.push_back(PathElement::key(std::move(key)));
    }

    // ========== Conversion ==========

    /**
     * @brief Convert to string representation.
     *
     * Format: "[node_id].port.element1.element2..."
     * Example: "[0,42].out.prices[\"AAPL\"]"
     */
    [[nodiscard]] std::string to_string() const;

    /**
     * @brief Convert to Python tuple.
     *
     * Returns: (node_id_list, port_type_str, path_list)
     */
    [[nodiscard]] nb::object to_python() const;

    // ========== Comparison ==========

    [[nodiscard]] bool operator==(const FQPath& other) const;

    [[nodiscard]] bool operator!=(const FQPath& other) const {
        return !(*this == other);
    }

    /**
     * @brief Less-than for use as map key.
     */
    [[nodiscard]] bool operator<(const FQPath& other) const;

private:
    std::vector<int64_t> node_id_;
    PortType port_type_{PortType::OUTPUT};
    std::vector<PathElement> path_;
};

} // namespace hgraph
