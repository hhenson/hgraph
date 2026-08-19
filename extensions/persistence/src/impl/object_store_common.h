#ifndef HGRAPH_PERSISTENCE_IMPL_OBJECT_STORE_COMMON_H
#define HGRAPH_PERSISTENCE_IMPL_OBJECT_STORE_COMMON_H

#include <hgraph/persistence/object_store.h>
#include <hgraph/util/sha256.h>

#include <algorithm>

namespace hgraph::persistence::store::impl
{
    [[nodiscard]] inline std::string content_token(std::span<const std::byte> data)
    {
        const auto hex = util::sha256_hex(util::sha256(data));
        return std::string{hex.data(), hex.size()};
    }

    [[nodiscard]] inline StoredObject stored_object(std::span<const std::byte> data)
    {
        StoredObject object;
        object.data.assign(data.begin(), data.end());
        object.version_token = content_token(data);
        return object;
    }

    [[nodiscard]] inline ObjectListPage page_objects(std::vector<ObjectInfo>         objects,
                                                     std::string_view                prefix,
                                                     std::optional<std::string_view> start_after,
                                                     std::size_t                     limit)
    {
        std::ranges::sort(objects, {}, &ObjectInfo::key);

        ObjectListPage page;
        auto           it = std::ranges::find_if(objects, [&](const ObjectInfo &object) {
            return object.key.starts_with(prefix) && (!start_after || object.key > *start_after);
        });
        for (; it != objects.end() && page.objects.size() < limit; ++it)
        {
            if (!it->key.starts_with(prefix))
            {
                if (it->key > prefix)
                {
                    break;
                }
                continue;
            }
            page.objects.push_back(*it);
        }

        if (!page.objects.empty())
        {
            const auto &last = page.objects.back().key;
            const bool  more = std::ranges::any_of(objects, [&](const ObjectInfo &object) {
                return object.key.starts_with(prefix) && object.key > last;
            });
            if (more)
            {
                page.next_start_after = last;
            }
        }
        return page;
    }
}  // namespace hgraph::persistence::store::impl

#endif  // HGRAPH_PERSISTENCE_IMPL_OBJECT_STORE_COMMON_H
