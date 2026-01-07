#pragma once

#include <cstddef>

namespace hgraph::value {

/**
 * @brief Optional container hook callbacks for composition-based extensions.
 *
 * These hooks are intended to let higher-level systems (e.g. TS overlays) attach
 * parallel per-slot data to containers that use an index-based backing store.
 *
 * All callbacks are optional; when unset, they are no-ops.
 */
struct ContainerHooks {
    void* ctx{nullptr};
    void (*on_insert)(void* ctx, size_t index){nullptr};
    void (*on_swap)(void* ctx, size_t index_a, size_t index_b){nullptr};
    void (*on_erase)(void* ctx, size_t index){nullptr};

    void insert(size_t index) const {
        if (on_insert) on_insert(ctx, index);
    }

    void swap(size_t index_a, size_t index_b) const {
        if (on_swap) on_swap(ctx, index_a, index_b);
    }

    void erase(size_t index) const {
        if (on_erase) on_erase(ctx, index);
    }
};

/**
 * @brief Result of map set/upsert when index acquisition is required.
 */
struct MapSetResult {
    size_t index{0};
    bool inserted{false};
};

}  // namespace hgraph::value
