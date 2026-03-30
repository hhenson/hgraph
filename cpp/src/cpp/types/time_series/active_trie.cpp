#include <hgraph/types/time_series/active_trie.h>

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
        for (const auto &[key, subtrie] : pending) {
            if (subtrie && subtrie->has_any_active()) { return true; }
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

    void ActiveTrieNode::evict_to_pending(size_t slot)
    {
        const auto it = children.find(slot);
        if (it == children.end() || !it->second) { return; }
        if (!it->second->has_any_active()) { return; }
        if (!it->second->slot_key) { return; }

        Value key_copy = Value{it->second->slot_key->view()};
        auto subtrie = std::move(it->second);
        children.erase(it);
        pending.emplace(std::move(key_copy), std::move(subtrie));
    }

    ActiveTrieNode *ActiveTrieNode::resolve_pending(const View &key, size_t new_slot)
    {
        const Value lookup_key{key};
        const auto it = pending.find(lookup_key);
        if (it == pending.end()) { return nullptr; }

        auto &child = children[new_slot];
        child = std::move(it->second);
        child->slot_key = std::make_unique<Value>(key);
        ActiveTrieNode *result = child.get();
        pending.erase(it);
        return result;
    }

    std::unique_ptr<ActiveTrieNode> ActiveTrieNode::deep_copy() const
    {
        auto copy = std::make_unique<ActiveTrieNode>();
        copy->locally_active = locally_active;
        if (slot_key) { copy->slot_key = std::make_unique<Value>(slot_key->view()); }

        for (const auto &[slot, child] : children) {
            if (child) { copy->children.emplace(slot, child->deep_copy()); }
        }

        for (const auto &[key, subtrie] : pending) {
            copy->pending.emplace(Value{key.view()}, subtrie ? subtrie->deep_copy() : nullptr);
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
