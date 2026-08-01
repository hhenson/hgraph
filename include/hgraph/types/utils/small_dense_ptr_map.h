#ifndef HGRAPH_TYPES_UTILS_SMALL_DENSE_PTR_MAP_H
#define HGRAPH_TYPES_UTILS_SMALL_DENSE_PTR_MAP_H

#include <hgraph/types/storage_metrics.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    /**
     * Sparse owner map for stable heap objects.
     *
     * Runtime ownership tries are predominantly empty or have only a handful
     * of children.  Those entries stay in a compact linear vector, avoiding an
     * empty hash table and its bucket allocation.  A ninth entry promotes the
     * owner to ``ankerl::unordered_dense::map`` so wider nodes retain constant
     * expected-time lookup.  Promotion and dense-table relocation only move
     * ``unique_ptr`` values; the owned object addresses remain stable.
     */
    template <typename Key,
              typename Pointee,
              typename Deleter = std::default_delete<Pointee>,
              typename Hash = ankerl::unordered_dense::hash<Key>,
              typename KeyEqual = std::equal_to<Key>,
              std::size_t SmallLimit = 8>
    class SmallDensePtrMap
    {
        static_assert(SmallLimit > 0);
        static_assert(std::is_nothrow_copy_constructible_v<Key>);
        static_assert(std::is_nothrow_move_constructible_v<Key>);

        using pointer_type = std::unique_ptr<Pointee, Deleter>;
        using value_type = std::pair<Key, pointer_type>;
        using SmallStorage = std::vector<value_type>;
        using DenseStorage = ankerl::unordered_dense::map<Key, pointer_type, Hash, KeyEqual>;

      public:
        SmallDensePtrMap() noexcept = default;
        SmallDensePtrMap(const SmallDensePtrMap &) = delete;
        SmallDensePtrMap &operator=(const SmallDensePtrMap &) = delete;
        SmallDensePtrMap(SmallDensePtrMap &&) noexcept = default;
        SmallDensePtrMap &operator=(SmallDensePtrMap &&) noexcept = default;
        ~SmallDensePtrMap() = default;

        [[nodiscard]] bool empty() const noexcept { return size() == 0; }

        [[nodiscard]] std::size_t size() const noexcept
        {
            return dense_ != nullptr ? dense_->size() : small_.size();
        }

        [[nodiscard]] bool uses_dense_storage() const noexcept { return dense_ != nullptr; }

        [[nodiscard]] Pointee *find(const Key &key) noexcept
        {
            if (dense_ != nullptr)
            {
                const auto it = dense_->find(key);
                return it != dense_->end() ? it->second.get() : nullptr;
            }
            const auto it = find_small(key);
            return it != small_.end() ? it->second.get() : nullptr;
        }

        [[nodiscard]] Pointee *find(const Key &key) const noexcept
        {
            if (dense_ != nullptr)
            {
                const auto it = dense_->find(key);
                return it != dense_->end() ? it->second.get() : nullptr;
            }
            const auto it = find_small(key);
            return it != small_.end() ? it->second.get() : nullptr;
        }

        [[nodiscard]] bool contains(const Key &key) const noexcept { return find(key) != nullptr; }

        [[nodiscard]] std::pair<Pointee *, bool> insert(Key key, pointer_type value)
        {
            assert(value != nullptr && "SmallDensePtrMap owns non-null pointees");
            if (auto *existing = find(key); existing != nullptr) { return {existing, false}; }

            if (dense_ != nullptr)
            {
                auto [it, inserted] = dense_->emplace(std::move(key), std::move(value));
                return {it->second.get(), inserted};
            }
            if (small_.size() < SmallLimit)
            {
                small_.emplace_back(std::move(key), std::move(value));
                return {small_.back().second.get(), true};
            }
            return {promote_and_insert(std::move(key), std::move(value)), true};
        }

        template <typename Factory>
        [[nodiscard]] Pointee &ensure(const Key &key, Factory &&factory)
        {
            if (auto *existing = find(key); existing != nullptr) { return *existing; }
            auto value = std::invoke(std::forward<Factory>(factory));
            return *insert(Key{key}, std::move(value)).first;
        }

        [[nodiscard]] bool erase(const Key &key) noexcept
        {
            if (dense_ != nullptr) { return dense_->erase(key) != 0; }
            const auto it = find_small(key);
            if (it == small_.end()) { return false; }
            small_.erase(it);
            return true;
        }

        template <typename Function>
        void for_each(Function &&function)
        {
            if (dense_ != nullptr)
            {
                for (auto &[key, value] : *dense_) { std::invoke(function, std::as_const(key), *value); }
                return;
            }
            for (auto &[key, value] : small_) { std::invoke(function, std::as_const(key), *value); }
        }

        template <typename Function>
        void for_each(Function &&function) const
        {
            if (dense_ != nullptr)
            {
                for (const auto &[key, value] : *dense_) { std::invoke(function, key, std::as_const(*value)); }
                return;
            }
            for (const auto &[key, value] : small_) { std::invoke(function, key, std::as_const(*value)); }
        }

        template <typename Predicate>
        [[nodiscard]] bool any_of(Predicate &&predicate) const
        {
            if (dense_ != nullptr)
            {
                for (const auto &[key, value] : *dense_)
                {
                    if (std::invoke(predicate, key, std::as_const(*value))) { return true; }
                }
                return false;
            }
            for (const auto &[key, value] : small_)
            {
                if (std::invoke(predicate, key, std::as_const(*value))) { return true; }
            }
            return false;
        }

        /** Heap bytes owned by the index and entry storage, excluding pointees. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            if (dense_ == nullptr)
            {
                return {
                    .live_bytes = small_.size() * sizeof(value_type),
                    .reserved_bytes = small_.capacity() * sizeof(value_type),
                };
            }

            using bucket_type = typename DenseStorage::bucket_type;
            return {
                .live_bytes = sizeof(DenseStorage) +
                              dense_->size() * (sizeof(value_type) + sizeof(bucket_type)),
                .reserved_bytes = sizeof(DenseStorage) +
                                  dense_->values().capacity() * sizeof(value_type) +
                                  dense_->bucket_count() * sizeof(bucket_type),
            };
        }

      private:
        [[nodiscard]] typename SmallStorage::iterator find_small(const Key &key) noexcept
        {
            return std::find_if(small_.begin(), small_.end(), [&](const value_type &entry) {
                return KeyEqual{}(entry.first, key);
            });
        }

        [[nodiscard]] typename SmallStorage::const_iterator find_small(const Key &key) const noexcept
        {
            return std::find_if(small_.begin(), small_.end(), [&](const value_type &entry) {
                return KeyEqual{}(entry.first, key);
            });
        }

        [[nodiscard]] Pointee *promote_and_insert(Key key, pointer_type value)
        {
            auto dense = std::make_unique<DenseStorage>(small_.size() + 1, Hash{}, KeyEqual{});
            for (auto &entry : small_)
            {
                static_cast<void>(dense->emplace(entry.first, std::move(entry.second)));
            }
            auto [inserted, was_inserted] = dense->emplace(std::move(key), std::move(value));
            static_cast<void>(was_inserted);
            auto *result = inserted->second.get();
            SmallStorage{}.swap(small_);
            dense_ = std::move(dense);
            return result;
        }

        SmallStorage small_{};
        std::unique_ptr<DenseStorage> dense_{};
    };
}  // namespace hgraph::detail

#endif  // HGRAPH_TYPES_UTILS_SMALL_DENSE_PTR_MAP_H
