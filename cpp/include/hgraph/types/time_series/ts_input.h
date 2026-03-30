#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/active_trie.h>
#include <hgraph/types/time_series/ts_input_builder.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_value.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/util/tagged_ptr.h>

namespace hgraph {

/**
 * Owning input-side time-series endpoint.
 *
 * `TSInput` represents the consumer-facing endpoint on a node. It owns
 * the input-local runtime state needed for binding and activation.
 *
 * Active state is tracked via a sparse `ActiveTrie` owned directly by
 * the input. The trie only contains entries for paths that are active
 * or have active descendants, replacing the previous schema-mirrored
 * parallel Value tree.
 *
 * The trie preserves activation intent across dynamic structure changes:
 * when a TSD key is removed, the active subtree is evicted to pending
 * storage (keyed by key copy) and reinstalled when the key reappears.
 */
struct HGRAPH_EXPORT TSInput : TSValue {
    /**
     * Construct an unbound input placeholder.
     *
     * This is intended for delayed builder-driven construction during node
     * instantiation.
     */
    TSInput() noexcept = default;

    /**
     * Construct an input endpoint.
     *
     * Inputs are builder-bound. The supplied builder owns the composite
     * storage layout together with the plan-specialized TS state construction
     * logic for this input shape.
     */
    explicit TSInput(const TSInputBuilder &builder);
    TSInput(const TSInput &other);
    TSInput(TSInput &&other) noexcept;
    TSInput &operator=(const TSInput &other);
    TSInput &operator=(TSInput &&other) noexcept;
    ~TSInput();

    /**
     * Return an input view rooted at this endpoint.
     *
     * Binding is expected to happen through collection navigation on this
     * view, matching the Python wiring model where `__getitem__` selects the
     * slot to bind.
     *
     * @param scheduling_notifier The root scheduling identity for this input
     *        branch, typically the owning Node. When navigating through
     *        TargetLinkState boundaries the identity switches automatically
     *        via BaseState::boundary_notifier(). May be nullptr when no
     *        scheduling is required (e.g. during wiring-only traversal).
     */
    [[nodiscard]] TSInputView view(Notifiable *scheduling_notifier = nullptr);

private:
    friend struct TSInputBuilder;

    [[nodiscard]] const TSInputBuilder &builder() const noexcept { return *m_builder; }
    [[nodiscard]] void *storage_memory() noexcept { return m_storage.ptr(); }
    [[nodiscard]] const void *storage_memory() const noexcept { return m_storage.ptr(); }
    void set_storage(void *memory, TSInputBuilder::MemoryOwnership ownership) noexcept
    {
        m_storage.set(memory, storage_ownership_tag(ownership));
    }
    void clear_storage_handle() noexcept { m_storage.clear(); }
    [[nodiscard]] bool owns_storage() const noexcept
    {
        return m_storage.has_tag(storage_ownership_tag(TSInputBuilder::MemoryOwnership::Owned));
    }

    using StoragePtr = erased_tagged_ptr<alignof(void *), 1>;
    [[nodiscard]] static constexpr typename StoragePtr::storage_type
        storage_ownership_tag(TSInputBuilder::MemoryOwnership ownership) noexcept
    {
        return static_cast<typename StoragePtr::storage_type>(ownership);
    }

    void allocate_and_construct();
    void clear_storage() noexcept;

    const TSInputBuilder *m_builder{nullptr};
    StoragePtr m_storage;
    ActiveTrie m_active_trie;
};

}  // namespace hgraph
