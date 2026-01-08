#pragma once

/**
 * @file ts_path.h
 * @brief Path types for time-series view navigation and REF persistence.
 *
 * Two path types are defined:
 *
 * 1. LightweightPath - Ordinal-only paths for efficient internal navigation.
 *    Uses only integer indices (ordinal positions). Valid only within a single
 *    engine cycle as container order may change.
 *
 * 2. StoredPath - Fully serializable paths for persistent references (REF type).
 *    Contains graph_id, node_ndx, output_id, and a vector of Value elements.
 *    Pointer-free and suitable for checkpointing/replay.
 *
 * Reference: ts_design_docs/TSValue_DESIGN.md Appendix E
 */

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/value/value_storage.h>
#include <hgraph/types/value/value_view.h>

namespace hgraph {

// Forward declarations
struct TSMeta;
struct TSValue;
struct Node;

// Output ID constants (same as defined in ts_value.h)
// These are duplicated here to avoid circular includes
constexpr int TS_PATH_OUTPUT_MAIN = 0;    ///< Main output (out)
constexpr int TS_PATH_ERROR_PATH = -1;    ///< Error output (error_output)
constexpr int TS_PATH_STATE_PATH = -2;    ///< Recordable state output (recordable_state)

// ============================================================================
// LightweightPath - Ordinal-only path for internal navigation
// ============================================================================

/**
 * @brief Lightweight path using only ordinal positions.
 *
 * This is the efficient path representation for internal navigation within
 * a single engine cycle. All elements are size_t ordinals:
 * - TSB: field ordinal (0, 1, 2, ...)
 * - TSL: list index (0, 1, 2, ...)
 * - TSD: internal slot index (current cycle only)
 *
 * @note TRANSIENT: LightweightPath is invalidated by structural changes to
 * containers (insert, erase, swap). Slot indices for TSD/TSS/TSL may change
 * when elements are added, removed, or swapped. Use immediately after creation
 * and do not hold across operations that modify container structure.
 *
 * @note NOT suitable for persistence - use StoredPath which stores actual
 * key values rather than slot indices.
 */
struct LightweightPath {
    std::vector<size_t> elements;

    LightweightPath() = default;
    explicit LightweightPath(std::vector<size_t> elems) : elements(std::move(elems)) {}

    /// Check if this is the root path (empty)
    [[nodiscard]] bool is_root() const noexcept { return elements.empty(); }

    /// Get the path depth
    [[nodiscard]] size_t depth() const noexcept { return elements.size(); }

    /// Append an ordinal to create an extended path
    [[nodiscard]] LightweightPath with(size_t ordinal) const {
        LightweightPath result = *this;
        result.elements.push_back(ordinal);
        return result;
    }

    /// Get parent path (removes last element)
    [[nodiscard]] LightweightPath parent() const {
        if (elements.empty()) {
            return *this;
        }
        LightweightPath result;
        result.elements.assign(elements.begin(), elements.end() - 1);
        return result;
    }

    /// String representation for debugging
    [[nodiscard]] std::string to_string() const {
        std::string result;
        for (size_t i = 0; i < elements.size(); ++i) {
            if (i > 0) result += ".";
            result += std::to_string(elements[i]);
        }
        return result.empty() ? "<root>" : result;
    }

    bool operator==(const LightweightPath& other) const {
        return elements == other.elements;
    }

    bool operator!=(const LightweightPath& other) const {
        return elements != other.elements;
    }
};

// ============================================================================
// StoredValue - A single value element in a stored path
// ============================================================================

/**
 * @brief Holds a Value for path navigation in stored paths.
 *
 * This wraps ValueStorage to provide copyable semantics via shared_ptr.
 * Used for all path elements:
 * - TSB: string (field name)
 * - TSL: int64_t (index)
 * - TSD: any Value (map key)
 *
 * The schema at each level determines how to interpret the value.
 */
struct StoredValue {
    std::shared_ptr<value::ValueStorage> storage;
    const value::TypeMeta* schema{nullptr};

    StoredValue() = default;

    /// Create from a ConstValueView (copies the data)
    static StoredValue from_view(value::ConstValueView view) {
        StoredValue holder;
        holder.schema = view.schema();
        holder.storage = std::make_shared<value::ValueStorage>();
        holder.storage->construct(view.schema());
        view.schema()->ops->copy_assign(holder.storage->data(), view.data(), view.schema());
        return holder;
    }

    /// Create from a string (for TSB field names)
    static StoredValue from_string(const std::string& str) {
        auto& registry = value::TypeRegistry::instance();
        const value::TypeMeta* string_schema = registry.get_scalar<std::string>();
        StoredValue holder;
        holder.schema = string_schema;
        holder.storage = std::make_shared<value::ValueStorage>();
        holder.storage->construct(string_schema);
        *static_cast<std::string*>(holder.storage->data()) = str;
        return holder;
    }

    /// Create from an index (for TSL)
    static StoredValue from_index(size_t idx) {
        auto& registry = value::TypeRegistry::instance();
        const value::TypeMeta* int_schema = registry.get_scalar<int64_t>();
        StoredValue holder;
        holder.schema = int_schema;
        holder.storage = std::make_shared<value::ValueStorage>();
        holder.storage->construct(int_schema);
        *static_cast<int64_t*>(holder.storage->data()) = static_cast<int64_t>(idx);
        return holder;
    }

    [[nodiscard]] value::ConstValueView view() const {
        return value::ConstValueView(storage->data(), schema);
    }

    [[nodiscard]] bool valid() const {
        return storage && schema;
    }

    /// String representation
    [[nodiscard]] std::string to_string() const {
        if (!valid()) return "<invalid>";
        return view().to_string();
    }
};

// ============================================================================
// StoredPath - Fully serializable path for persistence
// ============================================================================

/**
 * @brief Fully serializable path for persistent references.
 *
 * StoredPath is completely pointer-free, using IDs instead of pointers:
 * - graph_id: Tuple of ints identifying the graph
 * - node_ndx: Node index within the graph
 * - output_id: Which output (0=main, -1=error, -2=state)
 * - output_schema: Schema of the output (for type checking during expansion)
 * - elements: Value elements for navigation (interpreted by schema)
 *
 * This is used by REF types to store references that:
 * - Survive across engine cycles
 * - Can be serialized/deserialized
 * - Support checkpointing and replay
 */
struct StoredPath {
    std::vector<int64_t> graph_id;     ///< Graph identification (not pointer!)
    size_t node_ndx{0};                ///< Node index within the graph
    int output_id{TS_PATH_OUTPUT_MAIN}; ///< 0=main, -1=error, -2=state
    const TSMeta* output_schema{nullptr}; ///< Schema of the output for type checking
    std::vector<StoredValue> elements;

    StoredPath() = default;

    /// Create a root path for a node output
    StoredPath(std::vector<int64_t> gid, size_t ndx, int out_id, const TSMeta* schema = nullptr)
        : graph_id(std::move(gid)), node_ndx(ndx), output_id(out_id), output_schema(schema) {}

    /// Check if this is the root path (no elements)
    [[nodiscard]] bool is_root() const noexcept { return elements.empty(); }

    /// Get the path depth
    [[nodiscard]] size_t depth() const noexcept { return elements.size(); }

    /// Append an element to create an extended path
    [[nodiscard]] StoredPath with(StoredValue elem) const {
        StoredPath result = *this;
        result.elements.push_back(std::move(elem));
        return result;
    }

    /// String representation
    [[nodiscard]] std::string to_string() const {
        std::string result = "graph[";
        for (size_t i = 0; i < graph_id.size(); ++i) {
            if (i > 0) result += ",";
            result += std::to_string(graph_id[i]);
        }
        result += "].node[" + std::to_string(node_ndx) + "]";

        switch (output_id) {
            case TS_PATH_OUTPUT_MAIN: result += ".out"; break;
            case TS_PATH_ERROR_PATH: result += ".error"; break;
            case TS_PATH_STATE_PATH: result += ".state"; break;
            default: result += ".output[" + std::to_string(output_id) + "]"; break;
        }

        for (const auto& elem : elements) {
            result += "[" + elem.to_string() + "]";
        }

        return result;
    }
};

// ============================================================================
// Path Conversion Functions
// ============================================================================

/**
 * @brief Convert a lightweight path to a stored path.
 *
 * Requires the TSValue root to get graph/node information and schema
 * for converting ordinals to values.
 *
 * @param root The root TSValue
 * @param light The lightweight path to convert
 * @return The stored path
 */
StoredPath to_stored_path(const TSValue* root, const LightweightPath& light);

/**
 * @brief Convert a stored path to a lightweight path.
 *
 * Requires the TSValue root to resolve field names to ordinals
 * and key values to current ordinals.
 *
 * @param root The root TSValue
 * @param stored The stored path to convert
 * @return The lightweight path (valid for current cycle only)
 */
LightweightPath to_lightweight_path(const TSValue* root, const StoredPath& stored);

}  // namespace hgraph
