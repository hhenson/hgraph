#ifndef HGRAPH_PERSISTENCE_OBJECT_STORE_H
#define HGRAPH_PERSISTENCE_OBJECT_STORE_H

#include <hgraph/persistence/export.h>
#include <hgraph/persistence/store_location.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph::persistence::store
{
    using ObjectBytes = std::vector<std::byte>;

    /** A persistence backend failed; this is never used to report absence or conflict. */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT ObjectStoreError : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /** One complete object plus the backend's opaque compare/exchange token. */
    struct StoredObject
    {
        ObjectBytes data{};
        std::string version_token{};
    };

    enum class ImmutableWriteStatus
    {
        /** This call created the object. */
        Created,
        /** The key already contained the same bytes. */
        Unchanged,
        /** The key already contained different bytes. */
        Conflict,
    };

    struct ImmutableWriteResult
    {
        ImmutableWriteStatus status{ImmutableWriteStatus::Conflict};
        /** Token of the created or already-present object. */
        std::string version_token{};
    };

    struct ObjectInfo
    {
        std::string   key{};
        std::uint64_t size{};
    };

    /** One deterministic page ordered lexicographically by key. */
    struct ObjectListPage
    {
        std::vector<ObjectInfo> objects{};
        /** Pass this value as ``start_after`` to request the next page. */
        std::optional<std::string> next_start_after{};
    };

    struct CompareExchangeResult
    {
        bool exchanged{false};
        /** New value on success, observed winning value on conflict. */
        std::optional<StoredObject> current{};
    };

    struct ObjectStoreConfig
    {
        Location location{MemoryLocation{}};
    };

    /** Passive operations table for one private object-store representation. */
    struct ObjectStoreOps
    {
        ImmutableWriteResult (*put_immutable)(void *context, std::string_view key,
                                              std::span<const std::byte> data);
        std::optional<StoredObject> (*get)(void *context, std::string_view key);
        ObjectListPage (*list)(void *context, std::string_view prefix,
                               std::optional<std::string_view> start_after, std::size_t limit);
        CompareExchangeResult (*compare_exchange_ref)(
            void *context, std::string_view key, std::optional<std::string_view> expected_version,
            std::span<const std::byte> desired);
        void (*clear)(void *context);
    };

    /**
     * Owning type-erased byte/object store used by durable extension policy.
     *
     * Immutable creation and reference compare/exchange are atomic across
     * independently constructed handles to the same local or S3 namespace.
     * Reads return ``std::nullopt`` only for an absent key; backend failures
     * throw ``ObjectStoreError`` and are never reclassified as absence or a
     * conditional-write conflict. Version tokens are opaque and meaningful
     * only to the store which returned them. The operations table supplied to
     * the constructor is copied into the handle, so downstream strategies may
     * construct it dynamically without imposing a separate lifetime contract.
     */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT ObjectStore final
    {
      public:
        ObjectStore() noexcept;
        ObjectStore(std::shared_ptr<void> context, const ObjectStoreOps &ops);

        ObjectStore(const ObjectStore &) = default;
        ObjectStore &operator=(const ObjectStore &) = default;
        ObjectStore(ObjectStore &&other) noexcept;
        ObjectStore &operator=(ObjectStore &&other) noexcept;
        ~ObjectStore() = default;

        [[nodiscard]] ImmutableWriteResult put_immutable(std::string_view           key,
                                                         std::span<const std::byte> data) const;
        [[nodiscard]] std::optional<StoredObject> get(std::string_view key) const;
        [[nodiscard]] ObjectListPage        list(std::string_view                prefix,
                                                 std::optional<std::string_view> start_after = {},
                                                 std::size_t                     limit = 1000) const;
        [[nodiscard]] CompareExchangeResult compare_exchange_ref(
            std::string_view key, std::optional<std::string_view> expected_version,
            std::span<const std::byte> desired) const;
        void clear() const;

        [[nodiscard]] explicit operator bool() const noexcept;
        void                   reset() noexcept;

      private:
        [[nodiscard]] static const ObjectStoreOps &empty_ops() noexcept;

        std::shared_ptr<void> context_{};
        ObjectStoreOps        ops_{empty_ops()};
    };

    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT ObjectStore make_object_store(ObjectStoreConfig config);
}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_OBJECT_STORE_H
