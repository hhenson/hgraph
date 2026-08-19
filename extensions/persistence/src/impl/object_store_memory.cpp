#include "object_store_impl.h"

#include "object_store_common.h"

#include <map>
#include <mutex>

namespace hgraph::persistence::store::impl
{
    namespace
    {
        class MemoryObjectStore
        {
          public:
            [[nodiscard]] ImmutableWriteResult put_immutable(std::string_view           key,
                                                             std::span<const std::byte> data)
            {
                std::scoped_lock lock{mutex_};
                const auto [it, inserted] =
                    objects_.try_emplace(std::string{key}, stored_object(data));
                if (inserted)
                {
                    return {ImmutableWriteStatus::Created, it->second.version_token};
                }
                return {it->second.data.size() == data.size() &&
                                std::ranges::equal(it->second.data, data)
                            ? ImmutableWriteStatus::Unchanged
                            : ImmutableWriteStatus::Conflict,
                        it->second.version_token};
            }

            [[nodiscard]] std::optional<StoredObject> get(std::string_view key)
            {
                std::scoped_lock lock{mutex_};
                const auto       it = objects_.find(std::string{key});
                return it == objects_.end() ? std::nullopt
                                            : std::optional<StoredObject>{it->second};
            }

            [[nodiscard]] ObjectListPage list(std::string_view                prefix,
                                              std::optional<std::string_view> start_after,
                                              std::size_t                     limit)
            {
                std::scoped_lock        lock{mutex_};
                std::vector<ObjectInfo> objects;
                objects.reserve(objects_.size());
                for (const auto &[key, object] : objects_)
                {
                    objects.push_back(ObjectInfo{key, object.data.size()});
                }
                return page_objects(std::move(objects), prefix, start_after, limit);
            }

            [[nodiscard]] CompareExchangeResult compare_exchange_ref(
                std::string_view key, std::optional<std::string_view> expected_version,
                std::span<const std::byte> desired)
            {
                std::scoped_lock lock{mutex_};
                auto             it = objects_.find(std::string{key});
                const bool       matches =
                    expected_version
                        ? it != objects_.end() && it->second.version_token == *expected_version
                        : it == objects_.end();
                if (!matches)
                {
                    return {false, it == objects_.end() ? std::nullopt
                                                        : std::optional<StoredObject>{it->second}};
                }

                StoredObject current = stored_object(desired);
                if (it == objects_.end())
                {
                    objects_.emplace(std::string{key}, current);
                }
                else
                {
                    it->second = current;
                }
                return {true, std::move(current)};
            }

            void clear()
            {
                std::scoped_lock lock{mutex_};
                objects_.clear();
            }

          private:
            std::mutex                          mutex_{};
            std::map<std::string, StoredObject> objects_{};
        };

        [[nodiscard]] const ObjectStoreOps &memory_object_store_ops() noexcept
        {
            static const ObjectStoreOps ops{
                [](void *context, std::string_view key, std::span<const std::byte> data) {
                    return static_cast<MemoryObjectStore *>(context)->put_immutable(key, data);
                },
                [](void *context, std::string_view key) {
                    return static_cast<MemoryObjectStore *>(context)->get(key);
                },
                [](void *context, std::string_view prefix,
                   std::optional<std::string_view> start_after, std::size_t limit) {
                    return static_cast<MemoryObjectStore *>(context)->list(prefix, start_after,
                                                                           limit);
                },
                [](void *context, std::string_view key,
                   std::optional<std::string_view> expected_version,
                   std::span<const std::byte>      desired) {
                    return static_cast<MemoryObjectStore *>(context)->compare_exchange_ref(
                        key, expected_version, desired);
                },
                [](void *context) { static_cast<MemoryObjectStore *>(context)->clear(); },
            };
            return ops;
        }
    }  // namespace

    ObjectStore make_memory_object_store()
    {
        return ObjectStore{std::make_shared<MemoryObjectStore>(), memory_object_store_ops()};
    }
}  // namespace hgraph::persistence::store::impl
