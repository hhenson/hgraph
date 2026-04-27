#ifndef HGRAPH_CPP_ROOT_INTERN_TABLE_H
#define HGRAPH_CPP_ROOT_INTERN_TABLE_H

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::v2
{
    /**
     * Thread-safe owner for interned values keyed by structural identity.
     *
     * `InternTable` stores each value once, returns stable pointers/references,
     * and can be reused for metadata-style caches that want pointer identity
     * for equivalent keys.
     */
    template <typename Key, typename Value, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
    class InternTable
    {
      public:
        InternTable() = default;

        InternTable(const InternTable &)            = delete;
        InternTable &operator=(const InternTable &) = delete;
        InternTable(InternTable &&)                 = delete;
        InternTable &operator=(InternTable &&)      = delete;
        ~InternTable()                              = default;

        template <typename Factory> [[nodiscard]] const Value &intern(Key key, Factory &&factory) {
            std::lock_guard<std::mutex> lock(m_mutex);

            if (const auto it = m_cache.find(key); it != m_cache.end()) { return *it->second; }

            auto         value  = std::make_unique<Value>(std::forward<Factory>(factory)());
            const Value *result = value.get();
            m_storage.push_back(std::move(value));
            m_cache.emplace(std::move(key), result);
            return *result;
        }

        template <typename... Args> [[nodiscard]] const Value &emplace(Key key, Args &&...args) {
            return intern(std::move(key), [&]() { return Value{std::forward<Args>(args)...}; });
        }

        [[nodiscard]] const Value *find(const Key &key) const noexcept {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto                  it = m_cache.find(key);
            return it == m_cache.end() ? nullptr : it->second;
        }

      private:
        mutable std::mutex                                        m_mutex;
        std::unordered_map<Key, const Value *, KeyHash, KeyEqual> m_cache{};
        std::vector<std::unique_ptr<Value>>                       m_storage{};
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_INTERN_TABLE_H
