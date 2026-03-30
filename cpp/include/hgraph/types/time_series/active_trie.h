#pragma once

#include <hgraph/hgraph_export.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/time_series/value/view.h>

#include <memory>
#include <unordered_map>
#include <vector>

namespace hgraph {

struct ActiveTrieNode;

/**
 * A pending trie entry created when a TSD key is removed while its subtree
 * contains active state.
 *
 * The entry preserves a copy of the key so that the subtree can be reinstalled
 * at a new slot when the same key reappears in the output.
 */
struct HGRAPH_EXPORT PendingTrieEntry {
    Value key;
    std::unique_ptr<ActiveTrieNode> subtrie;
};

/**
 * A single node in the sparse active-state trie.
 *
 * The trie only contains entries for paths that are active or have active
 * descendants. Nodes that become empty (no locally_active flag and no
 * children) should be pruned by the parent.
 *
 * Children are indexed by slot:
 * - TSB: field index
 * - TSL: element index
 * - TSD: slot index (output-owned when below a TargetLinkState boundary)
 */
struct HGRAPH_EXPORT ActiveTrieNode {
    bool locally_active{false};

    /// Children indexed by slot. Sparse: only active paths have entries.
    std::unordered_map<size_t, std::unique_ptr<ActiveTrieNode>> children;

    /// TSD-only: subtrees evicted when their slot was removed while active.
    /// Tracked by key copy so they can be reinstalled at a new slot.
    std::vector<PendingTrieEntry> pending;

    // -- Query --

    /// Return the child at the given slot, or nullptr if not present.
    [[nodiscard]] ActiveTrieNode *child_at(size_t slot) const noexcept;

    /// Return true if this node or any descendant carries active state.
    [[nodiscard]] bool has_any_active() const noexcept;

    // -- Mutation --

    /// Return the child at the given slot, creating it if absent.
    ActiveTrieNode &ensure_child(size_t slot);

    /// Remove the child at the given slot if it has no active state.
    /// Returns true if the child was pruned.
    bool try_prune_child(size_t slot);

    // -- TSD pending ops --

    /// Move the child at @p slot to pending storage, keyed by @p key.
    /// No-op if the slot has no child or the child has no active state.
    void evict_to_pending(size_t slot, const View &key);

    /// Look for a pending entry matching @p key and, if found, reinstall
    /// it as a child at @p new_slot. Returns the reinstalled node, or
    /// nullptr if no matching pending entry was found.
    ActiveTrieNode *resolve_pending(const View &key, size_t new_slot);

    // -- Deep copy --

    [[nodiscard]] std::unique_ptr<ActiveTrieNode> deep_copy() const;
};

/**
 * Root container for the sparse active-state trie owned by TSInput.
 *
 * A null root means no path is active. The root is created lazily on
 * the first make_active call and pruned when all paths become passive.
 */
struct HGRAPH_EXPORT ActiveTrie {
    std::unique_ptr<ActiveTrieNode> root;

    /// Return the root node, or nullptr if no path is active.
    [[nodiscard]] ActiveTrieNode *root_node() const noexcept;

    /// Return the root node, creating it if absent.
    ActiveTrieNode &ensure_root();

    /// Remove the root if it has no active state. No-op if root is null.
    void try_prune_root();

    /// Deep-copy the entire trie.
    [[nodiscard]] ActiveTrie deep_copy() const;
};

/**
 * Lightweight position within the active trie, carried in TSViewContext
 * during input view navigation.
 *
 * A null node means the current path is not active. A non-null node means
 * this level (or a descendant) has active state.
 */
struct ActiveTriePosition {
    ActiveTrie     *trie{nullptr};
    ActiveTrieNode *node{nullptr};
};

}  // namespace hgraph
