#pragma once

#include <hgraph/hgraph_export.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/time_series/value/view.h>

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace hgraph {

struct BaseState;

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

    /// TSD-only: the key associated with this trie node's slot position.
    /// Set when make_active is called on a TSD child so that eviction
    /// can store the correct key in the pending map even after the value
    /// layer has overwritten the slot with a different key.
    std::unique_ptr<Value> slot_key;

    /// TSD-only: subtrees evicted when their slot was removed while active.
    /// Keyed by owning copy of the removed key for O(1) resolve lookup.
    using PendingMap = std::unordered_map<Value, std::unique_ptr<ActiveTrieNode>>;
    PendingMap pending;

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

    /// Move the child at @p slot to pending storage.
    /// The child's slot_key is used as the pending map key.
    /// No-op if the slot has no child, the child has no slot_key, or
    /// the child has no active state.
    void evict_to_pending(size_t slot);

    /// Look for a pending entry matching @p key and, if found, reinstall
    /// it as a child at @p new_slot and update its slot_key.
    /// Returns the reinstalled node, or nullptr if no match found.
    ActiveTrieNode *resolve_pending(const View &key, size_t new_slot);

    // -- Bulk subscription management --
    // These are used during unbind (unsubscribe the subtree from output-side
    // states before the link is torn down) and rebind/resolve_pending
    // (resubscribe against new target states).
    //
    // TODO: Implement when integrating with TSD structural observers and
    //       TargetLinkState unbind cleanup. On unbind, the trie subtree at
    //       the link boundary must be walked to unsubscribe all locally_active
    //       nodes from the output-side states. On rebind, the subtree must
    //       be walked to resubscribe against the new target's states.

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
 * Records a single link-boundary crossing encountered during input-side
 * navigation.  When the BaseState parent chain is walked to reconstruct
 * the input-side slot path, a crossing tells the walk to jump from the
 * output-side root back to the input-side TargetLinkState.
 */
struct LinkCrossing {
    BaseState *output_root;  ///< Output-side BaseState the TargetLinkState's target points to.
    BaseState *link_state;   ///< The TargetLinkState itself (input-side parent + index).
};

/**
 * Lightweight position within the active trie, carried in TSViewContext
 * during input view navigation.
 *
 * A null `node` means the current path has no active state in the trie
 * yet.  A non-null `node` means this level (or a descendant) has active
 * state.
 *
 * When `make_active` is called at a position whose `node` is null, the
 * path from the current BaseState back to the TSInput root is
 * reconstructed by walking BaseState::parent.  The `link_crossings`
 * vector records TargetLinkState boundaries so the walk can jump from
 * the output-side parent chain back to the input side.  This vector is
 * typically empty (no links) or has 1 entry, and only grows when a new
 * link boundary is crossed — not at every child navigation level.
 */
struct ActiveTriePosition {
    ActiveTrie                    *trie{nullptr};
    ActiveTrieNode                *node{nullptr};
    std::vector<LinkCrossing>      link_crossings;
};

/**
 * Reconstruct the input-side slot path from @p state back to the
 * TSInput root (using link_crossings to bridge TargetLinkState
 * boundaries), then create trie nodes along that path.
 *
 * Returns the trie node at the leaf, or nullptr if pos has no trie.
 */
HGRAPH_EXPORT ActiveTrieNode *ensure_trie_path(ActiveTriePosition &pos, BaseState *state);

}  // namespace hgraph
