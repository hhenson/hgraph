#include <hgraph/types/time_series/active_trie.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_input.h>

#include <algorithm>
#include <vector>

namespace hgraph
{
    namespace
    {
        template <typename TNodeFn>
        ActiveTrieNode *trie_path_impl(const ActiveTriePosition &pos, BaseState *state, TNodeFn &&advance)
        {
            if (pos.node != nullptr) { return pos.node; }
            if (pos.trie == nullptr || state == nullptr) { return nullptr; }

            std::vector<size_t> slots;
            BaseState *cur = state;
            auto crossing_it = pos.link_crossings.rbegin();

            while (cur != nullptr) {
                if (pos.boundary_root != nullptr && cur == pos.boundary_root) { break; }

                bool is_root = false;
                BaseState *next = nullptr;

                hgraph::visit(
                    cur->parent,
                    [&](auto *p) {
                        using T = std::remove_pointer_t<decltype(p)>;
                        if constexpr (std::same_as<T, TSInput>) {
                            is_root = true;
                        } else if constexpr (!std::same_as<T, TSOutput> && !std::same_as<T, v2::Node>) {
                            next = static_cast<BaseState *>(p);
                        }
                    },
                    [] {});

                if (is_root) { break; }

                if (crossing_it != pos.link_crossings.rend() &&
                    crossing_it->output_root == cur) {
                    slots.push_back(crossing_it->link_state->index);
                    next = nullptr;
                    hgraph::visit(
                        crossing_it->link_state->parent,
                        [&](auto *p) {
                            using T = std::remove_pointer_t<decltype(p)>;
                            if constexpr (!std::same_as<T, TSInput> && !std::same_as<T, TSOutput> &&
                                          !std::same_as<T, v2::Node>) {
                                next = static_cast<BaseState *>(p);
                            }
                        },
                        [] {});
                    ++crossing_it;
                } else {
                    slots.push_back(cur->index);
                }

                if (next == nullptr) { break; }
                cur = next;
            }

            std::ranges::reverse(slots);
            ActiveTrieNode *node = pos.trie->root_node();
            if (node == nullptr) { return nullptr; }

            for (const size_t slot : slots) {
                node = std::forward<TNodeFn>(advance)(node, slot);
                if (node == nullptr) { return nullptr; }
            }

            return node;
        }
    }

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

        Value key_copy = it->second->slot_key->view().clone();
        auto subtrie = std::move(it->second);
        children.erase(it);
        pending.emplace(std::move(key_copy), std::move(subtrie));
    }

    ActiveTrieNode *ActiveTrieNode::resolve_pending(const View &key, size_t new_slot)
    {
        const Value lookup_key = key.clone();
        const auto it = pending.find(lookup_key);
        if (it == pending.end()) { return nullptr; }

        auto &child = children[new_slot];
        child = std::move(it->second);
        child->slot_key = std::make_unique<Value>(key.clone());
        ActiveTrieNode *result = child.get();
        pending.erase(it);
        return result;
    }

    std::unique_ptr<ActiveTrieNode> ActiveTrieNode::deep_copy() const
    {
        auto copy = std::make_unique<ActiveTrieNode>();
        copy->locally_active = locally_active;
        if (slot_key) { copy->slot_key = std::make_unique<Value>(slot_key->view().clone()); }

        for (const auto &[slot, child] : children) {
            if (child) { copy->children.emplace(slot, child->deep_copy()); }
        }

        for (const auto &[key, subtrie] : pending) {
            copy->pending.emplace(key.view().clone(), subtrie ? subtrie->deep_copy() : nullptr);
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

    // -- ensure_trie_path --

    ActiveTrieNode *ensure_trie_path(ActiveTriePosition &pos, BaseState *state)
    {
        if (pos.node != nullptr) { return pos.node; }
        if (pos.trie == nullptr || state == nullptr) { return nullptr; }
        pos.trie->ensure_root();
        pos.node = trie_path_impl(
            pos,
            state,
            [](ActiveTrieNode *node, size_t slot) -> ActiveTrieNode * {
                return node != nullptr ? &node->ensure_child(slot) : nullptr;
            });
        return pos.node;
    }

    ActiveTrieNode *lookup_trie_path(const ActiveTriePosition &pos, BaseState *state)
    {
        return trie_path_impl(
            pos,
            state,
            [](ActiveTrieNode *node, size_t slot) -> ActiveTrieNode * {
                return node != nullptr ? node->child_at(slot) : nullptr;
            });
    }

}  // namespace hgraph
