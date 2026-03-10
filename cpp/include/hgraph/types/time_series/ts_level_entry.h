#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/observer_list.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace hgraph {

struct LinkTarget;
struct REFLink;

/**
 * Discriminator for the per-level delta payload stored in TSLevelEntry::delta.
 */
enum class LevelDeltaKind : uint8_t {
    None = 0,
    TSS = 1,
    TSD = 2,
    TSWTick = 3,
    TSWDuration = 4,
};

/**
 * Per-level metadata entry for a time-series hierarchy.
 *
 * Co-locates time, observer, link, and delta data at each level of the TS tree,
 * replacing four parallel Value trees with direct O(1) access via pointer.
 *
 * Value (data) storage remains in the existing value::Value tree — this struct
 * only covers the non-value aspects (time, observers, links, deltas).
 */
struct TSLevelEntry {
    // --- Time ---
    engine_time_t last_modified_time{MIN_DT};

    // --- Observers ---
    ObserverList observers{};

    // --- Link ---
    // For outputs: LinkTarget* or nullptr
    // For inputs:  REFLink* or nullptr
    // Stored type-erased; callers cast based on uses_link_target context.
    void* link{nullptr};
    bool uses_link_target{false};

    // --- Delta ---
    // Kind-specific delta payload, allocated at init for container kinds.
    // Points to TSSLevelDelta / TSDLevelDelta / TSWTickLevelDelta / TSWDurationLevelDelta.
    // Owned via shared_ptr with type-erased deleter.
    std::shared_ptr<void> delta{};
    LevelDeltaKind delta_kind{LevelDeltaKind::None};

    // --- Children ---
    // For TSB: fixed array of children sized at init (field_count).
    // For TSD/TSL: dynamic list that grows on demand.
    // IMPORTANT: Uses unique_ptr indirection so that growing the vector does NOT
    // invalidate pointers to existing entries. ViewData, LinkTarget, and
    // TimeSeriesReference all store raw TSLevelEntry* that must remain stable
    // across vector reallocation.
    std::vector<std::unique_ptr<TSLevelEntry>> children{};

    // --- Lifecycle ---

    /// Get a child at the given index (returns nullptr if not present).
    TSLevelEntry* child_at(size_t index) {
        if (index < children.size() && children[index]) {
            return children[index].get();
        }
        return nullptr;
    }

    /// Ensure a child exists at the given index, growing if needed.
    TSLevelEntry* ensure_child(size_t index) {
        if (index >= children.size()) {
            children.resize(index + 1);
        }
        if (!children[index]) {
            children[index] = std::make_unique<TSLevelEntry>();
        }
        return children[index].get();
    }

    /// Initialize fixed children (for TSB).
    void init_children(size_t count) {
        children.resize(count);
        for (auto& child : children) {
            if (!child) {
                child = std::make_unique<TSLevelEntry>();
            }
        }
    }

    /// Clear a child slot (for TSD key removal).
    void compact_child(size_t index) {
        if (index < children.size()) {
            children[index] = std::make_unique<TSLevelEntry>();
        }
        // Trim trailing empty entries
        while (!children.empty()) {
            auto& back = children.back();
            if (back && (back->last_modified_time != MIN_DT ||
                !back->observers.observers.empty() ||
                back->link != nullptr ||
                static_cast<bool>(back->delta) ||
                !back->children.empty())) {
                break;
            }
            children.pop_back();
        }
    }

    /// Reset this entry to default state.
    void clear() {
        last_modified_time = MIN_DT;
        observers.observers.clear();
        link = nullptr;
        uses_link_target = false;
        delta.reset();
        delta_kind = LevelDeltaKind::None;
        children.clear();
    }
};

}  // namespace hgraph
