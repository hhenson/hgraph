#pragma once

/**
 * @file ts_reference.h
 * @brief TSReference - Value-stack representation of time-series references.
 *
 * TSReference is the Value-stack equivalent of Python's TimeSeriesReference.
 * It holds a path to a time-series location rather than a pointer to an output object,
 * enabling efficient storage in TSValue and serialization for Python interop.
 *
 * Three variants:
 * - EMPTY: No reference (unbinds any bound input)
 * - PEERED: Direct binding to a single output via ShortPath
 * - NON_PEERED: Collection of references for composite types (e.g., REF[TSL])
 *
 * Usage:
 * @code
 * // Create references
 * auto empty_ref = TSReference::empty();
 * auto peered_ref = TSReference::peered(short_path);
 * auto non_peered_ref = TSReference::non_peered({ref1, ref2, ref3});
 *
 * // Store in TSValue
 * TSView view = ref_value.ts_view(current_time);
 * view.set_value(peered_ref);
 *
 * // Resolve to actual output
 * if (ref.is_peered()) {
 *     TSView target = ref.resolve(current_time);
 * }
 *
 * // Convert for Python
 * FQReference fq = ref.to_fq();
 * nb::object py_ref = fq.to_python();
 * @endcode
 */

#include <hgraph/types/time_series/short_path.h>
#include <hgraph/hgraph_forward_declarations.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph {

// Forward declarations
class TSView;
struct FQReference;

/**
 * @brief Value-stack representation of a time-series reference.
 *
 * TSReference enables storing references to time-series outputs within the
 * Value storage system. Unlike the legacy TimeSeriesReference (which holds
 * output pointers), TSReference uses ShortPath for efficient path-based
 * resolution.
 *
 * Binding Semantics:
 * - PEERED: Input directly peers with the referenced output. The input
 *   receives notifications when the output changes.
 * - NON_PEERED: Input doesn't peer as a whole; each element in the collection
 *   has its own reference. Used for composite types like REF[TSL].
 * - EMPTY: No binding. Causes any previously bound input to unbind.
 */
class TSReference {
public:
    /**
     * @brief Reference variant kind.
     */
    enum class Kind : uint8_t {
        EMPTY = 0,      ///< No reference - unbinds any bound input
        PEERED = 1,     ///< Direct binding to single output
        NON_PEERED = 2  ///< Collection of references (not directly peered)
    };

    // ========== Construction ==========

    /**
     * @brief Default constructor - creates an EMPTY reference.
     */
    TSReference() noexcept : kind_(Kind::EMPTY) {}

    /**
     * @brief Copy constructor.
     */
    TSReference(const TSReference& other);

    /**
     * @brief Move constructor.
     */
    TSReference(TSReference&& other) noexcept;

    /**
     * @brief Copy assignment.
     */
    TSReference& operator=(const TSReference& other);

    /**
     * @brief Move assignment.
     */
    TSReference& operator=(TSReference&& other) noexcept;

    /**
     * @brief Destructor.
     */
    ~TSReference();

    // ========== Factory Methods ==========

    /**
     * @brief Create an empty reference.
     *
     * An empty reference represents "no binding" and will cause any
     * previously bound input to unbind when applied.
     *
     * @return An EMPTY reference
     */
    [[nodiscard]] static TSReference empty() noexcept {
        return TSReference{};
    }

    /**
     * @brief Create a peered reference from a ShortPath.
     *
     * A peered reference establishes a direct binding between an input
     * and the output at the specified path. The input will receive
     * notifications when the referenced output changes.
     *
     * @param path The path to the output
     * @return A PEERED reference
     */
    [[nodiscard]] static TSReference peered(ShortPath path);

    /**
     * @brief Create a non-peered reference from a collection of references.
     *
     * A non-peered reference is used for composite types (e.g., REF[TSL])
     * where each element has its own individual reference rather than
     * the whole collection being peered to a single output.
     *
     * @param items The collection of references
     * @return A NON_PEERED reference
     */
    [[nodiscard]] static TSReference non_peered(std::vector<TSReference> items);

    // ========== Query Methods ==========

    /**
     * @brief Get the reference kind.
     * @return EMPTY, PEERED, or NON_PEERED
     */
    [[nodiscard]] Kind kind() const noexcept { return kind_; }

    /**
     * @brief Check if this is an empty reference.
     */
    [[nodiscard]] bool is_empty() const noexcept { return kind_ == Kind::EMPTY; }

    /**
     * @brief Check if this is a peered reference.
     */
    [[nodiscard]] bool is_peered() const noexcept { return kind_ == Kind::PEERED; }

    /**
     * @brief Check if this is a non-peered reference.
     */
    [[nodiscard]] bool is_non_peered() const noexcept { return kind_ == Kind::NON_PEERED; }

    /**
     * @brief Check if the reference has an output.
     *
     * Only PEERED references have a direct output binding.
     *
     * @return true if PEERED, false otherwise
     */
    [[nodiscard]] bool has_output() const noexcept { return is_peered(); }

    /**
     * @brief Check if the reference is valid.
     *
     * - EMPTY: Always returns false
     * - PEERED: Returns true if the path can be resolved and the output is valid
     * - NON_PEERED: Returns true if any item is non-empty
     *
     * @param current_time The current engine time for resolution
     * @return true if the reference points to valid data
     */
    [[nodiscard]] bool is_valid(engine_time_t current_time) const;

    // ========== Accessors ==========

    /**
     * @brief Get the path (PEERED only).
     *
     * @return The ShortPath to the referenced output
     * @throws std::runtime_error if not PEERED
     */
    [[nodiscard]] const ShortPath& path() const;

    /**
     * @brief Get the items (NON_PEERED only).
     *
     * @return The collection of references
     * @throws std::runtime_error if not NON_PEERED
     */
    [[nodiscard]] const std::vector<TSReference>& items() const;

    /**
     * @brief Get item at index (NON_PEERED only).
     *
     * @param index The index to access
     * @return The reference at the given index
     * @throws std::runtime_error if not NON_PEERED
     * @throws std::out_of_range if index is out of bounds
     */
    [[nodiscard]] const TSReference& operator[](size_t index) const;

    /**
     * @brief Get the number of items (NON_PEERED only).
     *
     * @return The number of items, or 0 if not NON_PEERED
     */
    [[nodiscard]] size_t size() const noexcept;

    // ========== Resolution ==========

    /**
     * @brief Resolve the reference to a TSView (PEERED only).
     *
     * Navigates from the path's node through the indices to produce
     * a TSView bound to the specified time.
     *
     * @param current_time The time to bind the view to
     * @return The resolved TSView
     * @throws std::runtime_error if not PEERED or resolution fails
     */
    [[nodiscard]] TSView resolve(engine_time_t current_time) const;

    // ========== Conversion ==========

    /**
     * @brief Convert to a fully-qualified reference for Python interop.
     *
     * Creates an FQReference that uses node_id instead of Node* pointer,
     * making it serializable and suitable for Python consumption.
     *
     * @return The FQReference representation
     */
    [[nodiscard]] FQReference to_fq() const;

    /**
     * @brief Create from a fully-qualified reference.
     *
     * Converts an FQReference (from Python) back to a TSReference
     * by resolving node_id to Node*.
     *
     * @param fq The FQReference to convert
     * @param graph The graph for resolving node_ids
     * @return The TSReference
     */
    [[nodiscard]] static TSReference from_fq(const FQReference& fq, Graph* graph);

    // ========== Comparison ==========

    /**
     * @brief Check equality with another reference.
     *
     * Two references are equal if they have the same kind and content:
     * - EMPTY == EMPTY
     * - PEERED: paths are equal
     * - NON_PEERED: items are equal (order matters)
     */
    [[nodiscard]] bool operator==(const TSReference& other) const;

    /**
     * @brief Check inequality.
     */
    [[nodiscard]] bool operator!=(const TSReference& other) const {
        return !(*this == other);
    }

    // ========== String Representation ==========

    /**
     * @brief Convert to string representation for debugging.
     *
     * Format:
     * - EMPTY: "REF[<Empty>]"
     * - PEERED: "REF[node_id.port[indices]]"
     * - NON_PEERED: "REF[item0, item1, ...]"
     *
     * @return String representation
     */
    [[nodiscard]] std::string to_string() const;

private:
    Kind kind_{Kind::EMPTY};

    // Tagged union storage
    // We use a union to avoid allocating for the common EMPTY case
    // and to keep the size small for PEERED (which just stores a ShortPath)
    union Storage {
        char empty_tag;                     // EMPTY: no data needed
        ShortPath peered_path;              // PEERED: path to output
        std::vector<TSReference> non_peered_items;  // NON_PEERED: collection

        Storage() noexcept : empty_tag{} {}
        ~Storage() {}  // Destructor handled by TSReference
    } storage_;

    // Private helpers for lifetime management
    void destroy() noexcept;
    void copy_from(const TSReference& other);
    void move_from(TSReference&& other) noexcept;
};

// ============================================================================
// FQReference - Fully-Qualified Reference for Python Interop
// ============================================================================

/**
 * @brief Fully-qualified reference for Python interoperability.
 *
 * FQReference uses node_id (integer) instead of Node* pointer, making it
 * serializable and suitable for crossing the C++/Python boundary.
 *
 * Conversion flow:
 * @code
 * TSReference (C++ runtime)
 *     │
 *     ├── to_fq() ──► FQReference ──► to_python() ──► Python TimeSeriesReference
 *     │
 *     └── from_fq() ◄── FQReference ◄── from_python() ◄── Python TimeSeriesReference
 * @endcode
 */
struct FQReference {
    using Kind = TSReference::Kind;

    Kind kind{Kind::EMPTY};

    // PEERED data: node identification + navigation path
    int node_id{-1};
    PortType port_type{PortType::OUTPUT};
    std::vector<size_t> indices;  // Navigation indices within the time-series

    // NON_PEERED data: collection of references
    std::vector<FQReference> items;

    // ========== Factory Methods ==========

    /**
     * @brief Create an empty FQReference.
     */
    [[nodiscard]] static FQReference empty() {
        return FQReference{};
    }

    /**
     * @brief Create a peered FQReference.
     */
    [[nodiscard]] static FQReference peered(int node_id, PortType port_type,
                                             std::vector<size_t> indices) {
        FQReference ref;
        ref.kind = Kind::PEERED;
        ref.node_id = node_id;
        ref.port_type = port_type;
        ref.indices = std::move(indices);
        return ref;
    }

    /**
     * @brief Create a non-peered FQReference.
     */
    [[nodiscard]] static FQReference non_peered(std::vector<FQReference> items) {
        FQReference ref;
        ref.kind = Kind::NON_PEERED;
        ref.items = std::move(items);
        return ref;
    }

    // ========== Query Methods ==========

    [[nodiscard]] bool is_empty() const noexcept { return kind == Kind::EMPTY; }
    [[nodiscard]] bool is_peered() const noexcept { return kind == Kind::PEERED; }
    [[nodiscard]] bool is_non_peered() const noexcept { return kind == Kind::NON_PEERED; }
    [[nodiscard]] bool has_output() const noexcept { return is_peered(); }

    /**
     * @brief Check if valid.
     *
     * - EMPTY: false
     * - PEERED: true if node_id >= 0
     * - NON_PEERED: true if any item is non-empty
     */
    [[nodiscard]] bool is_valid() const noexcept {
        switch (kind) {
            case Kind::EMPTY:
                return false;
            case Kind::PEERED:
                return node_id >= 0;
            case Kind::NON_PEERED:
                for (const auto& item : items) {
                    if (!item.is_empty()) return true;
                }
                return false;
        }
        return false;
    }

    // ========== Comparison ==========

    [[nodiscard]] bool operator==(const FQReference& other) const {
        if (kind != other.kind) return false;
        switch (kind) {
            case Kind::EMPTY:
                return true;
            case Kind::PEERED:
                return node_id == other.node_id &&
                       port_type == other.port_type &&
                       indices == other.indices;
            case Kind::NON_PEERED:
                return items == other.items;
        }
        return false;
    }

    [[nodiscard]] bool operator!=(const FQReference& other) const {
        return !(*this == other);
    }

    // ========== String Representation ==========

    [[nodiscard]] std::string to_string() const;
};

} // namespace hgraph
