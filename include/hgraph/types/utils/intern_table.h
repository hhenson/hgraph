#ifndef HGRAPH_CPP_ROOT_INTERN_TABLE_H
#define HGRAPH_CPP_ROOT_INTERN_TABLE_H

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph
{
    /**
     * Thread-safe owner for interned values keyed by structural identity.
     *
     * ``InternTable`` stores each value once, returns stable pointers and
     * references, and is the foundation other registries (plans, schemas,
     * builders) wrap to provide typed convenience APIs.
     *
     * Type parameters:
     * - ``Key``       hashable, equality-comparable structural identity.
     * - ``Value``     the interned payload; must be constructible from the
     *                 factory's return value.
     * - ``KeyHash``   hash functor for ``Key`` (defaults to ``std::hash``).
     * - ``KeyEqual``  equality functor for ``Key`` (defaults to
     *                 ``std::equal_to``).
     *
     * Stable addresses are guaranteed for the lifetime of the table because
     * stored values are held by ``std::unique_ptr`` and never relocated.
     * Construction, copy, move, and assignment are deleted to keep that
     * invariant intact.
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

        /**
         * Return the canonical instance for ``key``, constructing it via
         * ``factory()`` if it does not already exist.
         *
         * The factory runs without holding the table's mutex, so two threads
         * may both build candidate values for the same key; only the first
         * inserted value is retained, the rest are destroyed when their
         * ``unique_ptr`` goes out of scope. The returned reference is stable
         * for the lifetime of the table.
         */
        template <typename Factory> [[nodiscard]] const Value &intern(Key key, Factory &&factory) {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (const auto it = m_cache.find(key); it != m_cache.end()) { return *it->second; }
            }

            auto value = std::make_unique<Value>(std::forward<Factory>(factory)());

            std::lock_guard<std::mutex> lock(m_mutex);
            if (const auto it = m_cache.find(key); it != m_cache.end()) { return *it->second; }

            const Value *result = value.get();
            m_storage.push_back(std::move(value));
            m_cache.emplace(std::move(key), result);
            return *result;
        }

        /**
         * Convenience for the common case of constructing ``Value`` from
         * forwarded arguments. Equivalent to
         * ``intern(key, [&]{ return Value{args...}; })``.
         */
        template <typename... Args> [[nodiscard]] const Value &emplace(Key key, Args &&...args) {
            return intern(std::move(key), [&]() { return Value{std::forward<Args>(args)...}; });
        }

        /**
         * Look up an existing entry without inserting. Returns ``nullptr``
         * when the key is not present.
         */
        [[nodiscard]] const Value *find(const Key &key) const noexcept {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto                  it = m_cache.find(key);
            return it == m_cache.end() ? nullptr : it->second;
        }

        /**
         * Drop all interned values. Test-only helper for resetting registry
         * state between unit tests; pointers previously returned by
         * ``intern`` / ``emplace`` / ``find`` become invalid.
         */
        void clear() noexcept {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cache.clear();
            m_storage.clear();
        }

      private:
        mutable std::mutex                                        m_mutex;
        std::unordered_map<Key, const Value *, KeyHash, KeyEqual> m_cache{};
        std::vector<std::unique_ptr<Value>>                       m_storage{};
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_INTERN_TABLE_H
