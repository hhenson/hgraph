#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_level_entry.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/value/path.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace hgraph {

struct ts_ops;
struct LinkTarget;
struct REFLink;
struct TSLinkObserverRegistry;
class PythonValueCacheNode;
struct ViewData;

/**
 * Port side for path ownership.
 */
enum class PortType : uint8_t {
    INPUT = 0,
    OUTPUT = 1,
};

/**
 * Optional view projection mode used by runtime wiring.
 */
enum class ViewProjection : uint8_t {
    NONE = 0,
    TSD_KEY_SET = 1,
};

/**
 * Delta evaluation policy for pre-tick contexts (MIN_DT).
 */
enum class DeltaSemantics : uint8_t {
    Strict = 0,
    AllowPreTickDelta = 1,
};

/**
 * Explicit pre-tick delta policy check.
 *
 * This is intentionally policy-only: callers must opt-in via `delta_semantics`
 * and should not infer behavior from clock pointer presence.
 */
[[nodiscard]] inline bool allows_pre_tick_delta(const DeltaSemantics semantics, engine_time_t current_time) noexcept {
    return current_time == MIN_DT && semantics == DeltaSemantics::AllowPreTickDelta;
}

/**
 * Fully-qualified path element.
 */
struct FQPathElement {
    // Preserve TSD keys as typed value payloads (no stringification).
    using element_type = std::variant<std::string, size_t, value::ValueKeyHolder>;

    element_type element{};

    static FQPathElement field(std::string name) {
        return FQPathElement{std::move(name)};
    }

    static FQPathElement index(size_t index) {
        return FQPathElement{index};
    }

    static FQPathElement key(value::View key_view) {
        return FQPathElement{value::ValueKeyHolder::from_view(key_view)};
    }

    [[nodiscard]] std::string to_string() const;
};

/**
 * Serialized path representation.
 */
struct FQPath {
    uint64_t node_id{0};
    PortType port_type{PortType::OUTPUT};
    std::vector<FQPathElement> path;

    [[nodiscard]] std::string to_string() const;
};

/**
 * Runtime path representation (used for FQ path construction and debugging).
 */
struct ShortPath {
    node_ptr node{nullptr};
    PortType port_type{PortType::OUTPUT};
    std::vector<size_t> indices;

    [[nodiscard]] ShortPath child(size_t index) const;
    [[nodiscard]] FQPath to_fq() const;
    [[nodiscard]] FQPath to_fq(const ViewData& root) const;
    [[nodiscard]] std::string to_string() const;
};

// ---------------------------------------------------------------------------
// PathNode — intrusive ref-counted linked list for cheap TSView copies
// ---------------------------------------------------------------------------

/**
 * Each node in the path chain stores one index level. Root nodes carry
 * the owning node pointer and port type; children cache their depth for
 * O(1) access. Copies are a single ref_count increment.
 */
struct PathNode {
    PathNode* parent{nullptr};
    size_t index{0};          // port_index for root, child index for others
    uint32_t ref_count{1};

    union {
        struct {              // root (parent == nullptr)
            node_ptr node;
            PortType port_type;
            bool     sentinel;   // true when root has no indices (depth 0)
        } root_data;
        struct {              // child (parent != nullptr)
            uint16_t depth;   // distance from root (root sentinel=0: first child=1; root sentinel=false: first child=2)
        } child_data;
    };

    void retain() noexcept { ++ref_count; }
    void release() noexcept {
        if (--ref_count == 0) {
            if (parent) parent->release();
            delete this;
        }
    }

    [[nodiscard]] bool is_root() const noexcept { return parent == nullptr; }

    [[nodiscard]] uint16_t depth() const noexcept {
        if (is_root()) return root_data.sentinel ? 0 : 1;
        return child_data.depth;
    }

    [[nodiscard]] std::vector<size_t> collect_indices() const {
        const uint16_t d = depth();
        if (d == 0) return {};
        std::vector<size_t> result(d);
        size_t i = d;
        for (const PathNode* n = this; n && i > 0; n = n->parent)
            result[--i] = n->index;
        return result;
    }

    [[nodiscard]] const PathNode* root() const noexcept {
        const PathNode* n = this;
        while (n->parent) n = n->parent;
        return n;
    }

    /// Compare two path chains for index equality without allocating vectors.
    [[nodiscard]] static bool paths_equal(const PathNode* a, const PathNode* b) noexcept {
        if (a == b) return true;
        if (a == nullptr || b == nullptr) return a == b;
        if (a->depth() != b->depth()) return false;
        // Walk both chains in parallel from leaf to root.
        while (a && b) {
            if (a->index != b->index) return false;
            a = a->parent;
            b = b->parent;
        }
        return true;
    }

    /// Get the index at a given depth (0-based from root) without allocating.
    /// Requires depth < this->depth().
    [[nodiscard]] size_t index_at(size_t target_depth) const noexcept {
        const uint16_t d = depth();
        // Walk from leaf toward root, stopping at the right level.
        const PathNode* n = this;
        for (size_t i = d; i > target_depth + 1 && n; --i) {
            n = n->parent;
        }
        return n ? n->index : 0;
    }

    [[nodiscard]] node_ptr owner_node() const noexcept { return root()->root_data.node; }
    [[nodiscard]] PortType port_type() const noexcept { return root()->root_data.port_type; }

    static PathNode* make_child(PathNode* parent, size_t child_index) {
        auto* n = new PathNode{};
        n->parent = parent;
        n->index = child_index;
        n->ref_count = 1;
        n->child_data.depth = parent ? static_cast<uint16_t>(parent->depth() + 1) : 1;
        if (parent) parent->retain();
        return n;
    }

    static PathNode* make_root(node_ptr node, PortType pt, size_t port_index, bool sentinel = false) {
        auto* n = new PathNode{};
        n->parent = nullptr;
        n->index = port_index;
        n->ref_count = 1;
        n->root_data.node = node;
        n->root_data.port_type = pt;
        n->root_data.sentinel = sentinel;
        return n;
    }
};

/**
 * RAII handle for PathNode ref counting.
 */
struct PathHandle {
    PathNode* node{nullptr};

    PathHandle() = default;
    explicit PathHandle(PathNode* n) noexcept : node(n) {}
    PathHandle(const PathHandle& o) noexcept : node(o.node) { if (node) node->retain(); }
    PathHandle(PathHandle&& o) noexcept : node(o.node) { o.node = nullptr; }
    PathHandle& operator=(const PathHandle& o) noexcept {
        if (this != &o) { if (node) node->release(); node = o.node; if (node) node->retain(); }
        return *this;
    }
    PathHandle& operator=(PathHandle&& o) noexcept {
        if (this != &o) { if (node) node->release(); node = o.node; o.node = nullptr; }
        return *this;
    }
    ~PathHandle() { if (node) node->release(); }

    explicit operator bool() const noexcept { return node != nullptr; }
    PathNode* operator->() const noexcept { return node; }
    PathNode* get() const noexcept { return node; }
};

// ---------------------------------------------------------------------------
// PathHandle factory helpers — build from ShortPath or indices
// ---------------------------------------------------------------------------

/**
 * Build a PathHandle chain from a ShortPath. Used at endpoint boundaries
 * (TSOutput::view, TSInput::view, TSValue::make_view_data) where a ShortPath
 * is constructed once and converted to the cheap ref-counted representation.
 */
inline PathHandle path_handle_from_short_path(const ShortPath& sp) {
    if (sp.indices.empty()) {
        // Sentinel root: carries node/port_type but depth() == 0, collect_indices() == {}
        return PathHandle(PathNode::make_root(sp.node, sp.port_type, 0, /*sentinel=*/true));
    }
    // First index becomes the root
    PathHandle handle(PathNode::make_root(sp.node, sp.port_type, sp.indices[0]));
    for (size_t i = 1; i < sp.indices.size(); ++i) {
        handle = PathHandle(PathNode::make_child(handle.get(), sp.indices[i]));
    }
    return handle;
}

/**
 * Shared data contract for TS views.
 *
 * This matches the locked full contract: path + five storage pointers + flags + ops/meta.
 */
struct ViewData {
    PathHandle path;
    const engine_time_t* engine_time_ptr{nullptr};

    void* value_data{nullptr};
    void* time_data{nullptr};
    void* observer_data{nullptr};
    void* delta_data{nullptr};
    void* link_data{nullptr};
    TSLevelEntry* level{nullptr};
    TSLevelEntry* root_level{nullptr};  // root of the level tree (for re-navigation)
    uint16_t level_depth{0};            // depth of level pointer relative to TSValue root
    void* python_value_cache_data{nullptr};
    void* python_value_cache_slot{nullptr};
    TSLinkObserverRegistry* link_observer_registry{nullptr};

    bool sampled{false};
    bool uses_link_target{false};
    ViewProjection projection{ViewProjection::NONE};
    DeltaSemantics delta_semantics{DeltaSemantics::Strict};

    const ts_ops* ops{nullptr};
    const TSMeta* meta{nullptr};       // resolved meta for THIS level (not root)
    const TSMeta* root_meta{nullptr};  // root TSMeta (for ancestor navigation)

    [[nodiscard]] bool is_null() const {
        return value_data == nullptr && time_data == nullptr && observer_data == nullptr &&
               delta_data == nullptr && link_data == nullptr && python_value_cache_data == nullptr;
    }

    [[nodiscard]] LinkTarget* as_link_target() const;
    [[nodiscard]] REFLink* as_ref_link() const;

    // --- Path convenience accessors ---

    [[nodiscard]] ShortPath to_short_path() const;

    /// Build an FQPath from the PathNode chain, using root_meta for field name resolution.
    [[nodiscard]] FQPath to_fq_path() const;

    [[nodiscard]] std::vector<size_t> path_indices() const {
        return path ? path->collect_indices() : std::vector<size_t>{};
    }
    [[nodiscard]] size_t path_depth() const noexcept { return path ? path->depth() : 0; }
    [[nodiscard]] size_t last_index() const noexcept { return path ? path->index : 0; }
    [[nodiscard]] node_ptr owner_node() const noexcept { return path ? path->owner_node() : nullptr; }
    [[nodiscard]] PortType port_type() const noexcept { return path ? path->port_type() : PortType::OUTPUT; }

    /// Get path index at a given depth without allocating a vector.
    [[nodiscard]] size_t path_index_at(size_t depth) const noexcept {
        return path ? path->index_at(depth) : 0;
    }

    /// Compare paths of two ViewData without allocating vectors.
    [[nodiscard]] bool paths_equal_to(const ViewData& other) const noexcept {
        return PathNode::paths_equal(path.get(), other.path.get());
    }

    /// Identity check: do two ViewData refer to the same backing storage and path?
    /// Sufficient for determining if two views are the same instance,
    /// since all storage pointers originate from the same TSValue.
    [[nodiscard]] bool same_identity(const ViewData& other) const noexcept {
        return value_data == other.value_data &&
               projection == other.projection &&
               paths_equal_to(other);
    }
};

/**
 * Debug-only consistency checks for discriminator and payload assumptions.
 */
HGRAPH_EXPORT void debug_assert_view_data_consistency(const ViewData& vd);

}  // namespace hgraph
