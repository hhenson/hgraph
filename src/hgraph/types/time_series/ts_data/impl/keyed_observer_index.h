#ifndef HGRAPH_IMPL_KEYED_OBSERVER_INDEX_H
#define HGRAPH_IMPL_KEYED_OBSERVER_INDEX_H

#include <hgraph/types/time_series/ts_data/types.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_hash.h>
#include <unordered_map>

namespace hgraph::detail
{
    /** Owned by the observer-set spill, never by a process-wide side table.
        Shared bucket ownership permits self-unsubscription during dispatch. */
    struct KeyedObserverIndex
    {
        // Count allocator requests, including shared bucket control blocks and
        // the standard library's hash nodes/bucket array, without ABI guesses.
        template <typename T> struct Allocator
        {
            using value_type = T;
            std::size_t *bytes{};
            explicit Allocator(std::size_t *bytes_) noexcept : bytes(bytes_) {}
            template <typename U> Allocator(const Allocator<U> &other) noexcept : bytes(other.bytes) {}
            T *allocate(std::size_t count)
            {
                auto *memory = std::allocator<T>{}.allocate(count);
                *bytes += count * sizeof(T);
                return memory;
            }
            void deallocate(T *memory, std::size_t count) noexcept
            {
                *bytes -= count * sizeof(T);
                std::allocator<T>{}.deallocate(memory, count);
            }
            template <typename U> bool operator==(const Allocator<U> &other) const noexcept { return bytes == other.bytes; }
        };
        std::size_t allocated_bytes{0};
        using Entry = std::pair<const Value, std::shared_ptr<TSDataTracking>>;
        std::unordered_map<Value, std::shared_ptr<TSDataTracking>, ValueHash, ValueEqual, Allocator<Entry>>
            keys{0, ValueHash{}, ValueEqual{}, Allocator<Entry>{&allocated_bytes}};
        ObserverListStorage<KeyedNotifiable> relays;
        std::size_t registrations{0};

        template <typename F> void visit_relays(F &&fn)
        {
            ++relays.notify_depth;
            auto cleanup = make_scope_exit([&] {
                if (--relays.notify_depth == 0) { relays.compact(); }
            });
            const auto limit = relays.entries.size();
            for (std::size_t i = 0; i < limit; ++i)
                if (auto *entry = relays.entries[i]) { fn(*entry); }
        }
        void subscribe(const ValueView &key, Notifiable *observer)
        {
            auto it = keys.find(key);
            const bool inserted = it == keys.end();
            if (inserted)
                it = keys.emplace(Value{key}, std::allocate_shared<TSDataTracking>(Allocator<TSDataTracking>{&allocated_bytes})).first;
            auto rollback = make_scope_exit([&] { if (inserted) { keys.erase(it); } });
            it->second->observers.subscribe(observer);
            ++registrations;
            rollback.release();
        }
        void unsubscribe(const ValueView &key, Notifiable *observer)
        {
            auto it = keys.find(key);
            assert(it != keys.end());
            if (it == keys.end()) { return; }
            it->second->observers.unsubscribe(observer);
            --registrations;
            if (it->second->observers.empty()) { keys.erase(it); }
        }
        void notify(const ValueView &key, DateTime time)
        {
            // Resolve before callbacks: a callback can erase or rehash keys.
            auto it = keys.find(key);
            auto bucket = it == keys.end() ? nullptr : it->second;
            if (bucket) { static_cast<void>(bucket->record_modified(time)); }
            visit_relays([&](KeyedNotifiable &relay) { relay.key_modified(key, time); });
        }
        void notify_all(DateTime time)
        {
            std::vector<std::shared_ptr<TSDataTracking>> snapshot;
            snapshot.reserve(keys.size());
            for (const auto &[key, bucket] : keys) { snapshot.push_back(bucket); }
            for (const auto &bucket : snapshot) { static_cast<void>(bucket->record_modified(time)); }
            visit_relays([&](KeyedNotifiable &relay) { relay.keys_replaced(time); });
        }
        void invalidate(const TSDataTracking *source) noexcept
        {
            auto detached = std::move(keys);
            registrations = 0;
            for (auto &[key, bucket] : detached) { bucket->observers.invalidate(source); }
            visit_relays([&](KeyedNotifiable &relay) { relay.source_invalidated(source); });
            relays.clear_entries();
        }
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            DynamicStorageMetrics result{sizeof(*this) + allocated_bytes, sizeof(*this) + allocated_bytes};
            result += relays.buffer_metrics();
            for (const auto &[key, bucket] : keys)
            {
                result += key.dynamic_storage_metrics();
                result += bucket->observers.dynamic_storage_metrics();
            }
            return result;
        }
        void clear() noexcept
        {
            for (auto &[key, bucket] : keys) { bucket->observers.clear(); }
            keys.clear();
            registrations = 0;
            relays.clear_entries();
        }
    };
}
#endif
