#include <hgraph/persistence/object_store.h>

#include "impl/object_store_impl.h"

#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph::persistence::store
{
    const ObjectStoreOps &ObjectStore::empty_ops() noexcept
    {
        static const ObjectStoreOps ops{
            [](void *, std::string_view, std::span<const std::byte>) -> ImmutableWriteResult {
                throw std::logic_error("no object store is bound");
            },
            [](void *, std::string_view) -> std::optional<StoredObject> { return std::nullopt; },
            [](void *, std::string_view, std::optional<std::string_view>,
               std::size_t) -> ObjectListPage { return {}; },
            [](void *, std::string_view, std::optional<std::string_view>,
               std::span<const std::byte>) -> CompareExchangeResult {
                throw std::logic_error("no object store is bound");
            },
            [](void *) {},
        };
        return ops;
    }

    ObjectStore::ObjectStore() noexcept = default;

    ObjectStore::ObjectStore(std::shared_ptr<void> context, const ObjectStoreOps &ops)
        : context_(std::move(context)), ops_(&ops)
    {
        if (!context_)
        {
            throw std::invalid_argument("object store context must not be null");
        }
        if (ops.put_immutable == nullptr || ops.get == nullptr || ops.list == nullptr ||
            ops.compare_exchange_ref == nullptr || ops.clear == nullptr)
        {
            throw std::invalid_argument("object store operations must be complete");
        }
    }

    ObjectStore::ObjectStore(ObjectStore &&other) noexcept
        : context_(std::move(other.context_)), ops_(std::exchange(other.ops_, &empty_ops()))
    {
    }

    ObjectStore &ObjectStore::operator=(ObjectStore &&other) noexcept
    {
        if (this != &other)
        {
            context_ = std::move(other.context_);
            ops_ = std::exchange(other.ops_, &empty_ops());
        }
        return *this;
    }

    ImmutableWriteResult ObjectStore::put_immutable(std::string_view           key,
                                                    std::span<const std::byte> data) const
    {
        require_valid_key(key);
        return ops_->put_immutable(context_.get(), key, data);
    }

    std::optional<StoredObject> ObjectStore::get(std::string_view key) const
    {
        require_valid_key(key);
        return ops_->get(context_.get(), key);
    }

    ObjectListPage ObjectStore::list(std::string_view                prefix,
                                     std::optional<std::string_view> start_after,
                                     std::size_t                     limit) const
    {
        if (!prefix.empty())
        {
            const auto candidate =
                prefix.ends_with('/') ? prefix.substr(0, prefix.size() - 1) : prefix;
            if (candidate.empty())
            {
                throw std::invalid_argument("object store list prefix must be relative");
            }
            require_valid_key(candidate);
        }
        if (start_after)
        {
            require_valid_key(*start_after);
        }
        if (limit == 0)
        {
            throw std::invalid_argument("object store list limit must be positive");
        }
        if (limit > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max()))
        {
            throw std::invalid_argument("object store list limit is too large");
        }
        return ops_->list(context_.get(), prefix, start_after, limit);
    }

    CompareExchangeResult ObjectStore::compare_exchange_ref(
        std::string_view key, std::optional<std::string_view> expected_version,
        std::span<const std::byte> desired) const
    {
        require_valid_key(key);
        if (expected_version && expected_version->empty())
        {
            throw std::invalid_argument("an expected object version must not be empty");
        }
        return ops_->compare_exchange_ref(context_.get(), key, expected_version, desired);
    }

    void ObjectStore::clear() const { ops_->clear(context_.get()); }

    ObjectStore::operator bool() const noexcept { return context_ != nullptr; }

    void ObjectStore::reset() noexcept
    {
        context_.reset();
        ops_ = &empty_ops();
    }

    ObjectStore make_object_store(ObjectStoreConfig config)
    {
        return std::visit(
            [](const auto &location) -> ObjectStore {
                using T = std::decay_t<decltype(location)>;
                if constexpr (std::is_same_v<T, MemoryLocation>)
                {
                    return impl::make_memory_object_store();
                }
                else if constexpr (std::is_same_v<T, LocalLocation>)
                {
                    return impl::make_local_object_store(location);
                }
                else
                {
                    return impl::make_s3_object_store(location);
                }
            },
            config.location);
    }
}  // namespace hgraph::persistence::store
