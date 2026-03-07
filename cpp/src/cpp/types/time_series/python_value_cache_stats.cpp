#include <hgraph/types/time_series/python_value_cache_stats.h>

#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
#include <atomic>
#endif

namespace hgraph {

#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
namespace {

struct PythonValueCacheStatsStorage {
    std::atomic<uint64_t> to_python_calls{0};
    std::atomic<uint64_t> eligible_reads{0};
    std::atomic<uint64_t> link_target_bypass_reads{0};
    std::atomic<uint64_t> slot_lookups{0};
    std::atomic<uint64_t> slot_lookup_failures{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> cache_writes{0};
    std::atomic<uint64_t> invalidation_calls{0};
    std::atomic<uint64_t> invalidation_effective{0};
};

PythonValueCacheStatsStorage& python_value_cache_stats_storage() {
    static PythonValueCacheStatsStorage storage;
    return storage;
}

}  // namespace
#endif

bool python_value_cache_stats_enabled() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    return true;
#else
    return false;
#endif
}

PythonValueCacheStats python_value_cache_stats_snapshot() {
    PythonValueCacheStats out{};
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    auto& storage = python_value_cache_stats_storage();
    out.to_python_calls = storage.to_python_calls.load(std::memory_order_relaxed);
    out.eligible_reads = storage.eligible_reads.load(std::memory_order_relaxed);
    out.link_target_bypass_reads = storage.link_target_bypass_reads.load(std::memory_order_relaxed);
    out.slot_lookups = storage.slot_lookups.load(std::memory_order_relaxed);
    out.slot_lookup_failures = storage.slot_lookup_failures.load(std::memory_order_relaxed);
    out.cache_hits = storage.cache_hits.load(std::memory_order_relaxed);
    out.cache_misses = storage.cache_misses.load(std::memory_order_relaxed);
    out.cache_writes = storage.cache_writes.load(std::memory_order_relaxed);
    out.invalidation_calls = storage.invalidation_calls.load(std::memory_order_relaxed);
    out.invalidation_effective = storage.invalidation_effective.load(std::memory_order_relaxed);
#endif
    return out;
}

void reset_python_value_cache_stats() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    auto& storage = python_value_cache_stats_storage();
    storage.to_python_calls.store(0, std::memory_order_relaxed);
    storage.eligible_reads.store(0, std::memory_order_relaxed);
    storage.link_target_bypass_reads.store(0, std::memory_order_relaxed);
    storage.slot_lookups.store(0, std::memory_order_relaxed);
    storage.slot_lookup_failures.store(0, std::memory_order_relaxed);
    storage.cache_hits.store(0, std::memory_order_relaxed);
    storage.cache_misses.store(0, std::memory_order_relaxed);
    storage.cache_writes.store(0, std::memory_order_relaxed);
    storage.invalidation_calls.store(0, std::memory_order_relaxed);
    storage.invalidation_effective.store(0, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_to_python_calls() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().to_python_calls.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_eligible_reads() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().eligible_reads.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_link_target_bypass_reads() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().link_target_bypass_reads.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_slot_lookups() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().slot_lookups.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_slot_lookup_failures() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().slot_lookup_failures.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_cache_hits() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().cache_hits.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_cache_misses() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().cache_misses.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_cache_writes() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().cache_writes.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_invalidation_calls() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().invalidation_calls.fetch_add(1, std::memory_order_relaxed);
#endif
}

void python_value_cache_stats_inc_invalidation_effective() {
#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
    python_value_cache_stats_storage().invalidation_effective.fetch_add(1, std::memory_order_relaxed);
#endif
}

}  // namespace hgraph
