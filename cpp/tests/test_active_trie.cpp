#include <catch2/catch_test_macros.hpp>
#include <hgraph/types/time_series/active_trie.h>

#include <string>

using namespace hgraph;

// ---------- ActiveTrieNode basics ----------

TEST_CASE("ActiveTrieNode default state", "[active_trie]")
{
    ActiveTrieNode node;
    CHECK_FALSE(node.locally_active);
    CHECK_FALSE(node.has_any_active());
    CHECK(node.children.empty());
    CHECK(node.pending.empty());
    CHECK(node.child_at(0) == nullptr);
    CHECK(node.child_at(42) == nullptr);
}

TEST_CASE("ActiveTrieNode locally_active makes has_any_active true", "[active_trie]")
{
    ActiveTrieNode node;
    node.locally_active = true;
    CHECK(node.has_any_active());
}

TEST_CASE("ActiveTrieNode ensure_child creates and returns child", "[active_trie]")
{
    ActiveTrieNode node;

    auto &child = node.ensure_child(3);
    CHECK(node.child_at(3) == &child);
    CHECK_FALSE(child.locally_active);
    CHECK_FALSE(child.has_any_active());

    // Second call returns same child.
    auto &same = node.ensure_child(3);
    CHECK(&same == &child);
}

TEST_CASE("ActiveTrieNode has_any_active detects deep active descendants", "[active_trie]")
{
    ActiveTrieNode root;
    CHECK_FALSE(root.has_any_active());

    auto &child = root.ensure_child(0);
    CHECK_FALSE(root.has_any_active());

    auto &grandchild = child.ensure_child(1);
    grandchild.locally_active = true;
    CHECK(root.has_any_active());
}

TEST_CASE("ActiveTrieNode try_prune_child removes empty child", "[active_trie]")
{
    ActiveTrieNode root;
    root.ensure_child(5);
    CHECK(root.child_at(5) != nullptr);

    CHECK(root.try_prune_child(5));
    CHECK(root.child_at(5) == nullptr);
}

TEST_CASE("ActiveTrieNode try_prune_child keeps active child", "[active_trie]")
{
    ActiveTrieNode root;
    root.ensure_child(5).locally_active = true;

    CHECK_FALSE(root.try_prune_child(5));
    CHECK(root.child_at(5) != nullptr);
}

TEST_CASE("ActiveTrieNode try_prune_child returns false for absent slot", "[active_trie]")
{
    ActiveTrieNode root;
    CHECK_FALSE(root.try_prune_child(99));
}

// ---------- Pending (TSD) ----------

TEST_CASE("ActiveTrieNode evict_to_pending moves active subtree to pending", "[active_trie][pending]")
{
    auto &registry = value::TypeRegistry::instance();
    registry.register_type<std::string>("str");

    ActiveTrieNode tsd_node;
    tsd_node.ensure_child(2).locally_active = true;

    Value key{std::string{"alpha"}};
    tsd_node.child_at(2)->slot_key = std::make_unique<Value>(key.view());
    tsd_node.evict_to_pending(2);

    CHECK(tsd_node.child_at(2) == nullptr);
    REQUIRE(tsd_node.pending.size() == 1);
    const auto it = tsd_node.pending.find(key);
    REQUIRE(it != tsd_node.pending.end());
    REQUIRE(it->second != nullptr);
    CHECK(it->second->locally_active);
}

TEST_CASE("ActiveTrieNode evict_to_pending is no-op for inactive child", "[active_trie][pending]")
{
    auto &registry = value::TypeRegistry::instance();
    registry.register_type<std::string>("str");

    ActiveTrieNode tsd_node;
    tsd_node.ensure_child(2);  // exists but inactive

    Value key{std::string{"beta"}};
    tsd_node.child_at(2)->slot_key = std::make_unique<Value>(key.view());
    tsd_node.evict_to_pending(2);

    // Child not evicted (no active state), but it may or may not remain.
    CHECK(tsd_node.pending.empty());
}

TEST_CASE("ActiveTrieNode evict_to_pending is no-op for absent slot", "[active_trie][pending]")
{
    ActiveTrieNode tsd_node;
    tsd_node.evict_to_pending(99);
    CHECK(tsd_node.pending.empty());
}

TEST_CASE("ActiveTrieNode resolve_pending reinstalls subtree at new slot", "[active_trie][pending]")
{
    auto &registry = value::TypeRegistry::instance();
    registry.register_type<std::string>("str");

    ActiveTrieNode tsd_node;
    tsd_node.ensure_child(2).locally_active = true;

    Value key{std::string{"delta"}};
    tsd_node.child_at(2)->slot_key = std::make_unique<Value>(key.view());
    tsd_node.evict_to_pending(2);
    REQUIRE(tsd_node.pending.size() == 1);

    // Resolve at a different slot.
    ActiveTrieNode *resolved = tsd_node.resolve_pending(key.view(), 7);
    REQUIRE(resolved != nullptr);
    CHECK(resolved->locally_active);
    CHECK(tsd_node.child_at(7) == resolved);
    CHECK(tsd_node.pending.empty());
}

TEST_CASE("ActiveTrieNode resolve_pending returns nullptr for unknown key", "[active_trie][pending]")
{
    auto &registry = value::TypeRegistry::instance();
    registry.register_type<std::string>("str");

    ActiveTrieNode tsd_node;
    tsd_node.ensure_child(2).locally_active = true;
    Value key_a{std::string{"epsilon"}};
    tsd_node.child_at(2)->slot_key = std::make_unique<Value>(key_a.view());
    tsd_node.evict_to_pending(2);

    Value key_b{std::string{"zeta"}};
    CHECK(tsd_node.resolve_pending(key_b.view(), 5) == nullptr);
    CHECK(tsd_node.pending.size() == 1);
}

TEST_CASE("ActiveTrieNode has_any_active includes pending subtrees", "[active_trie][pending]")
{
    auto &registry = value::TypeRegistry::instance();
    registry.register_type<std::string>("str");

    ActiveTrieNode tsd_node;
    tsd_node.ensure_child(2).locally_active = true;

    Value key{std::string{"eta"}};
    tsd_node.child_at(2)->slot_key = std::make_unique<Value>(key.view());
    tsd_node.evict_to_pending(2);

    // No slot children active, but pending subtree is active.
    CHECK(tsd_node.has_any_active());
}

// ---------- Deep copy ----------

TEST_CASE("ActiveTrieNode deep_copy preserves structure", "[active_trie]")
{
    auto &registry = value::TypeRegistry::instance();
    registry.register_type<std::string>("str");

    ActiveTrieNode root;
    root.locally_active = true;
    root.ensure_child(0).locally_active = true;
    root.ensure_child(1).ensure_child(3).locally_active = true;

    Value key{std::string{"theta"}};
    root.ensure_child(2).locally_active = true;
    root.child_at(2)->slot_key = std::make_unique<Value>(key.view());
    root.evict_to_pending(2);

    auto copy = root.deep_copy();
    REQUIRE(copy != nullptr);
    CHECK(copy->locally_active);
    REQUIRE(copy->child_at(0) != nullptr);
    CHECK(copy->child_at(0)->locally_active);
    REQUIRE(copy->child_at(1) != nullptr);
    REQUIRE(copy->child_at(1)->child_at(3) != nullptr);
    CHECK(copy->child_at(1)->child_at(3)->locally_active);
    REQUIRE(copy->pending.size() == 1);
    const auto pit = copy->pending.find(key);
    REQUIRE(pit != copy->pending.end());
    CHECK(pit->second->locally_active);

    // Mutation of copy doesn't affect original.
    copy->locally_active = false;
    CHECK(root.locally_active);
}

// ---------- ActiveTrie ----------

TEST_CASE("ActiveTrie default state", "[active_trie]")
{
    ActiveTrie trie;
    CHECK(trie.root_node() == nullptr);
}

TEST_CASE("ActiveTrie ensure_root creates root lazily", "[active_trie]")
{
    ActiveTrie trie;
    auto &root = trie.ensure_root();
    CHECK(trie.root_node() == &root);
    CHECK_FALSE(root.locally_active);
}

TEST_CASE("ActiveTrie try_prune_root removes empty root", "[active_trie]")
{
    ActiveTrie trie;
    trie.ensure_root();
    trie.try_prune_root();
    CHECK(trie.root_node() == nullptr);
}

TEST_CASE("ActiveTrie try_prune_root keeps active root", "[active_trie]")
{
    ActiveTrie trie;
    trie.ensure_root().locally_active = true;
    trie.try_prune_root();
    CHECK(trie.root_node() != nullptr);
}

TEST_CASE("ActiveTrie deep_copy", "[active_trie]")
{
    ActiveTrie trie;
    trie.ensure_root().locally_active = true;
    trie.root_node()->ensure_child(0).locally_active = true;

    ActiveTrie copy = trie.deep_copy();
    REQUIRE(copy.root_node() != nullptr);
    CHECK(copy.root_node()->locally_active);
    REQUIRE(copy.root_node()->child_at(0) != nullptr);
    CHECK(copy.root_node()->child_at(0)->locally_active);

    // Independent.
    copy.root_node()->locally_active = false;
    CHECK(trie.root_node()->locally_active);
}

TEST_CASE("ActiveTrie deep_copy of empty trie", "[active_trie]")
{
    ActiveTrie trie;
    ActiveTrie copy = trie.deep_copy();
    CHECK(copy.root_node() == nullptr);
}
