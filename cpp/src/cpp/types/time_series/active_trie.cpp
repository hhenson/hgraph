#include <hgraph/types/time_series/active_trie.h>

#include <algorithm>

namespace hgraph
{
    // -- ActiveTrieNode --

    ActiveTrieNode *ActiveTrieNode::child_at(size_t slot) const noexcept
    {
        if (const auto it = children.find(slot); it != children.end()) { return it->second.get(); }
        return nullptr;
    }

    bool ActiveTrieNode::has_any_active() const noexcept
    {
        if (locally_active) { return true; }
        for (const auto &[slot, child] : children) {
            if (child && child->has_any_active()) { return true; }
        }
        for (const auto &entry : pending) {
            if (entry.subtrie && entry.subtrie->has_any_active()) { return true; }
        }
        return false;
    }

    ActiveTrieNode &ActiveTrieNode::ensure_child(size_t slot)
    {
        auto &child = children[slot];
        if (!child) { child = std::make_unique<ActiveTrieNode>(); }
        return *child;
    }

    bool ActiveTrieNode::try_prune_child(size_t slot)
    {
        const auto it = children.find(slot);
        if (it == children.end()) { return false; }
        if (it->second && it->second->has_any_active()) { return false; }
        children.erase(it);
        return true;
    }

    void ActiveTrieNode::evict_to_pending(size_t slot, const View &key)
    {
        const auto it = children.find(slot);
        if (it == children.end() || !it->second) { return; }
        if (!it->second->has_any_active()) { return; }

        pending.push_back(PendingTrieEntry{Value{key}, std::move(it->second)});
        children.erase(it);
    }

    ActiveTrieNode *ActiveTrieNode::resolve_pending(const View &key, size_t new_slot)
    {
        for (auto it = pending.begin(); it != pending.end(); ++it) {
            if (it->key.view() == key) {
                auto &child = children[new_slot];
                child = std::move(it->subtrie);
                ActiveTrieNode *result = child.get();
                pending.erase(it);
                return result;
            }
        }
        return nullptr;
    }

    std::unique_ptr<ActiveTrieNode> ActiveTrieNode::deep_copy() const
    {
        auto copy = std::make_unique<ActiveTrieNode>();
        copy->locally_active = locally_active;

        for (const auto &[slot, child] : children) {
            if (child) { copy->children.emplace(slot, child->deep_copy()); }
        }

        for (const auto &entry : pending) {
            copy->pending.push_back(PendingTrieEntry{
                Value{entry.key.view()},
                entry.subtrie ? entry.subtrie->deep_copy() : nullptr});
        }

        return copy;
    }

    // -- ActiveTrie --

    ActiveTrieNode *ActiveTrie::root_node() const noexcept
    {
        return root.get();
    }

    ActiveTrieNode &ActiveTrie::ensure_root()
    {
        if (!root) { root = std::make_unique<ActiveTrieNode>(); }
        return *root;
    }

    void ActiveTrie::try_prune_root()
    {
        if (root && !root->has_any_active()) { root.reset(); }
    }

    ActiveTrie ActiveTrie::deep_copy() const
    {
        ActiveTrie copy;
        if (root) { copy.root = root->deep_copy(); }
        return copy;
    }

}  // namespace hgraph
