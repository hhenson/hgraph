#ifndef HGRAPH_TYPES_UTILS_IMPL_OBSERVER_LIST_H
#define HGRAPH_TYPES_UTILS_IMPL_OBSERVER_LIST_H

#include <hgraph/types/storage_metrics.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <vector>

namespace hgraph::detail
{
    /**
     * Spill storage shared by modification and structural observers.
     *
     * Traversal uses the dense pointer vector. Searches scan at most eight
     * entries, then use a pointer-to-position index with expected O(1) lookup.
     * The index is retained until the owning facade collapses to one observer.
     * During traversal, removals erase the index entry but leave a null vector
     * slot; additions always append so the current traversal limit stays valid.
     * No removal, replacement, or compaction allocates.
     */
    template <typename Observer>
    struct ObserverListStorage
    {
        static constexpr std::size_t small_limit = 8;
        static constexpr std::size_t not_found = std::numeric_limits<std::size_t>::max();

        std::vector<Observer *> entries{};
        std::size_t notify_depth{0};
        bool compact_pending{false};

        [[nodiscard]] std::size_t find(const Observer *observer) const noexcept
        {
            if (index_)
            {
                const auto it = index_->find(observer);
                return it == index_->end() ? not_found : it->second;
            }
            const auto it = std::find(entries.begin(), entries.end(), observer);
            return it == entries.end() ? not_found : static_cast<std::size_t>(it - entries.begin());
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            if (index_) { return index_->size(); }
            return static_cast<std::size_t>(std::count_if(entries.begin(), entries.end(),
                                                         [](auto *entry) { return entry != nullptr; }));
        }

        [[nodiscard]] bool add(Observer *observer)
        {
            const auto position = entries.size();
            if (index_ && index_->size() < index_->values().capacity())
            {
                // All index capacity is reserved up front, so only appending
                // to the traversal vector can allocate on this path.
                const auto [it, inserted] = index_->emplace(observer, position);
                if (!inserted) { return false; }
                auto rollback = make_scope_exit([&] { index_->erase(it); });
                entries.push_back(observer);
                rollback.release();
                return true;
            }
            if (find(observer) != not_found) { return false; }
            if (!index_ && position < small_limit)
            {
                entries.push_back(observer);
                return true;
            }

            // Build and grow the index off to the side. Allocation failure
            // must leave both the old index and registrations intact. Doubling
            // capacity keeps registration amortized O(1); copy only live index
            // entries so deferred tombstones do not multiply the growth work.
            auto index = std::make_unique<Index>();
            const auto capacity = index_ ? index_->values().capacity() : small_limit;
            index->reserve(capacity > index->max_size() / 2 ? index->max_size() : capacity * 2);
            if (index_)
            {
                for (const auto &entry : *index_) { index->insert(entry); }
            }
            else
            {
                for (std::size_t i = 0; i < position; ++i)
                {
                    if (entries[i] != nullptr) { index->emplace(entries[i], i); }
                }
            }
            index->emplace(observer, position);
            entries.push_back(observer);
            index_ = std::move(index);
            return true;
        }

        [[nodiscard]] bool remove(Observer *observer) noexcept
        {
            const auto position = find(observer);
            if (position == not_found) { return false; }
            if (index_) { index_->erase(observer); }
            if (notify_depth != 0)
            {
                entries[position] = nullptr;
                compact_pending = true;
            }
            else { swap_pop(position); }
            return true;
        }

        [[nodiscard]] bool replace(std::size_t position, Observer *replacement) noexcept
        {
            if (index_)
            {
                const auto it = index_->find(entries[position]);
                // replace_key repairs buckets in place without growing either
                // allocation, preserving the facade's noexcept move/rebind path.
                if (!index_->replace_key(it, replacement).second) { return false; }
            }
            else if (find(replacement) != not_found) { return false; }
            entries[position] = replacement;
            return true;
        }

        void compact() noexcept
        {
            assert(notify_depth == 0);
            if (!compact_pending) { return; }
            for (std::size_t position = 0; position < entries.size();)
            {
                if (entries[position] != nullptr) { ++position; }
                else { swap_pop(position); }
            }
            compact_pending = false;
        }

        void clear_entries() noexcept
        {
            std::fill(entries.begin(), entries.end(), nullptr);
            if (index_) { index_->clear(); }
            compact_pending = true;
        }

        /** Buffer allocations only; the owning facade accounts for this object. */
        [[nodiscard]] DynamicStorageMetrics buffer_metrics() const noexcept
        {
            DynamicStorageMetrics metrics{
                .live_bytes = entries.size() * sizeof(Observer *),
                .reserved_bytes = entries.capacity() * sizeof(Observer *),
            };
            if (index_)
            {
                using Value = typename Index::value_type;
                using Bucket = typename Index::bucket_type;
                metrics += {
                    .live_bytes = sizeof(Index) + index_->size() * (sizeof(Value) + sizeof(Bucket)),
                    .reserved_bytes = sizeof(Index) + index_->values().capacity() * sizeof(Value) +
                                      index_->bucket_count() * sizeof(Bucket),
                };
            }
            return metrics;
        }

      private:
        using Index = ankerl::unordered_dense::map<const Observer *, std::size_t>;
        std::unique_ptr<Index> index_{};

        void swap_pop(std::size_t position) noexcept
        {
            entries[position] = entries.back();
            entries.pop_back();
            if (index_ && position < entries.size() && entries[position] != nullptr)
            {
                auto it = index_->find(entries[position]);
                assert(it != index_->end());
                it->second = position;
            }
        }
    };
}

#endif
