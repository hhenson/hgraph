//
// Created by Howard Henson on 03/01/2025.
//

#ifndef REF_H
#define REF_H

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/type_meta.h>

#include <variant>
#include <vector>
#include <cstring>

namespace hgraph
{
    // Forward declarations
    struct Node;

    // ============================================================
    // TypeErasedKey - For TSD arbitrary key storage
    // ============================================================

    struct TypeErasedKey {
        std::vector<std::byte> data;
        const value::TypeMeta* type{nullptr};

        TypeErasedKey() = default;

        template<typename T>
        static TypeErasedKey from_value(const T& val, const value::TypeMeta* meta) {
            TypeErasedKey key;
            key.type = meta;
            key.data.resize(sizeof(T));
            std::memcpy(key.data.data(), &val, sizeof(T));
            return key;
        }

        // For string keys (common case)
        static TypeErasedKey from_string(const std::string& str, const value::TypeMeta* meta) {
            TypeErasedKey key;
            key.type = meta;
            key.data.resize(str.size());
            std::memcpy(key.data.data(), str.data(), str.size());
            return key;
        }

        [[nodiscard]] bool operator==(const TypeErasedKey& other) const {
            return type == other.type && data == other.data;
        }

        [[nodiscard]] size_t hash() const {
            size_t h = std::hash<const void*>{}(type);
            for (auto b : data) {
                h ^= std::hash<std::byte>{}(b) + 0x9e3779b9 + (h << 6) + (h >> 2);
            }
            return h;
        }
    };

    // ============================================================
    // PathKey - Variant for path navigation
    // ============================================================

    // size_t for: output index, bundle field index, TSL element index
    // TypeErasedKey for: TSD arbitrary keys
    using PathKey = std::variant<size_t, TypeErasedKey>;

    // ============================================================
    // TimeSeriesReference - Reference to a time-series output
    // ============================================================
    //
    // TimeSeriesReference tracks a reference to a time-series output via
    // a weak_ptr to the owning Node plus a path through the output structure.
    // This allows validity tracking (node lifetime) and safe resolution.
    //
    // Three kinds:
    //   EMPTY   - No reference (default state)
    //   BOUND   - References a specific output via node + path
    //   UNBOUND - Collection of references (for composite types)
    //

    struct HGRAPH_EXPORT TimeSeriesReference
    {
        enum class Kind : uint8_t { EMPTY = 0, BOUND = 1, UNBOUND = 2 };

        // Default constructor creates EMPTY reference
        TimeSeriesReference() noexcept;

        // Copy/Move semantics
        TimeSeriesReference(const TimeSeriesReference &other) = default;
        TimeSeriesReference(TimeSeriesReference &&other) noexcept = default;
        TimeSeriesReference &operator=(const TimeSeriesReference &other) = default;
        TimeSeriesReference &operator=(TimeSeriesReference &&other) noexcept = default;
        ~TimeSeriesReference() = default;

        // Query methods
        [[nodiscard]] Kind kind() const noexcept { return _kind; }
        [[nodiscard]] bool is_empty() const noexcept { return _kind == Kind::EMPTY; }
        [[nodiscard]] bool is_bound() const noexcept { return _kind == Kind::BOUND; }
        [[nodiscard]] bool is_unbound() const noexcept { return _kind == Kind::UNBOUND; }

        // Check if the referenced node is still alive (BOUND only)
        [[nodiscard]] bool valid() const;

        // Legacy compatibility
        [[nodiscard]] bool has_output() const { return is_bound() && valid(); }
        [[nodiscard]] bool is_valid() const { return valid(); }

        // Get the owning node (returns nullptr if expired or not bound)
        [[nodiscard]] std::shared_ptr<Node> node() const;

        // Get the path within the node's output structure
        [[nodiscard]] const std::vector<PathKey>& path() const { return _path; }

        // Resolve to view (returns invalid view if expired or not bound)
        [[nodiscard]] value::TimeSeriesValueView resolve() const;

        // Resolve to raw output pointer (returns nullptr if expired or not bound)
        [[nodiscard]] ts::TSOutput* output_ptr() const;

        // For UNBOUND references - access child references
        [[nodiscard]] const std::vector<TimeSeriesReference> &items() const;
        [[nodiscard]] const TimeSeriesReference              &operator[](size_t ndx) const;

        // Operations
        bool                      operator==(const TimeSeriesReference &other) const;
        [[nodiscard]] std::string to_string() const;

        // Factory methods - use these to construct instances
        static TimeSeriesReference make();  // EMPTY
        static TimeSeriesReference make(std::weak_ptr<Node> node, std::vector<PathKey> path = {});  // BOUND
        static TimeSeriesReference make(std::vector<TimeSeriesReference> items);  // UNBOUND

        // Create from output view - extracts node and path from the view
        static TimeSeriesReference make(const value::TimeSeriesValueView& view);

      private:
        // Private constructors for factory use
        TimeSeriesReference(std::weak_ptr<Node> node, std::vector<PathKey> path, ts::TSOutput* direct_output = nullptr);
        explicit TimeSeriesReference(std::vector<TimeSeriesReference> items);

        Kind _kind{Kind::EMPTY};

        // BOUND state: node reference and path to navigate within output
        std::weak_ptr<Node> _node_ref;
        std::vector<PathKey> _path;

        // Direct output pointer (for dynamically created outputs that aren't reachable via node->output())
        // This is the output that was used to create this reference.
        // May be nullptr if this is a standard node output (navigable via node->output() + path).
        ts::TSOutput* _direct_output{nullptr};

        // UNBOUND state: collection of child references
        std::vector<TimeSeriesReference> _items;
    };

}  // namespace hgraph

#endif  // REF_H
