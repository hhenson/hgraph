#pragma once

#include <hgraph/hgraph_base.h>

#include <cstdint>

namespace hgraph {

struct PythonValueCacheStats {
    uint64_t to_python_calls{0};
    uint64_t eligible_reads{0};
    uint64_t link_target_bypass_reads{0};
    uint64_t slot_lookups{0};
    uint64_t slot_lookup_failures{0};
    uint64_t cache_hits{0};
    uint64_t cache_misses{0};
    uint64_t cache_writes{0};
    uint64_t invalidation_calls{0};
    uint64_t invalidation_effective{0};
};

HGRAPH_EXPORT bool python_value_cache_stats_enabled();
HGRAPH_EXPORT PythonValueCacheStats python_value_cache_stats_snapshot();
HGRAPH_EXPORT void reset_python_value_cache_stats();

HGRAPH_EXPORT void python_value_cache_stats_inc_to_python_calls();
HGRAPH_EXPORT void python_value_cache_stats_inc_eligible_reads();
HGRAPH_EXPORT void python_value_cache_stats_inc_link_target_bypass_reads();
HGRAPH_EXPORT void python_value_cache_stats_inc_slot_lookups();
HGRAPH_EXPORT void python_value_cache_stats_inc_slot_lookup_failures();
HGRAPH_EXPORT void python_value_cache_stats_inc_cache_hits();
HGRAPH_EXPORT void python_value_cache_stats_inc_cache_misses();
HGRAPH_EXPORT void python_value_cache_stats_inc_cache_writes();
HGRAPH_EXPORT void python_value_cache_stats_inc_invalidation_calls();
HGRAPH_EXPORT void python_value_cache_stats_inc_invalidation_effective();

#if defined(HGRAPH_ENABLE_PY_CACHE_STATS)
#define HGRAPH_PY_CACHE_STATS_INC_TO_PYTHON_CALLS() ::hgraph::python_value_cache_stats_inc_to_python_calls()
#define HGRAPH_PY_CACHE_STATS_INC_ELIGIBLE_READS() ::hgraph::python_value_cache_stats_inc_eligible_reads()
#define HGRAPH_PY_CACHE_STATS_INC_LINK_TARGET_BYPASS_READS() ::hgraph::python_value_cache_stats_inc_link_target_bypass_reads()
#define HGRAPH_PY_CACHE_STATS_INC_SLOT_LOOKUPS() ::hgraph::python_value_cache_stats_inc_slot_lookups()
#define HGRAPH_PY_CACHE_STATS_INC_SLOT_LOOKUP_FAILURES() ::hgraph::python_value_cache_stats_inc_slot_lookup_failures()
#define HGRAPH_PY_CACHE_STATS_INC_CACHE_HITS() ::hgraph::python_value_cache_stats_inc_cache_hits()
#define HGRAPH_PY_CACHE_STATS_INC_CACHE_MISSES() ::hgraph::python_value_cache_stats_inc_cache_misses()
#define HGRAPH_PY_CACHE_STATS_INC_CACHE_WRITES() ::hgraph::python_value_cache_stats_inc_cache_writes()
#define HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_CALLS() ::hgraph::python_value_cache_stats_inc_invalidation_calls()
#define HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_EFFECTIVE() ::hgraph::python_value_cache_stats_inc_invalidation_effective()
#else
#define HGRAPH_PY_CACHE_STATS_INC_TO_PYTHON_CALLS() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_ELIGIBLE_READS() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_LINK_TARGET_BYPASS_READS() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_SLOT_LOOKUPS() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_SLOT_LOOKUP_FAILURES() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_CACHE_HITS() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_CACHE_MISSES() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_CACHE_WRITES() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_CALLS() ((void)0)
#define HGRAPH_PY_CACHE_STATS_INC_INVALIDATION_EFFECTIVE() ((void)0)
#endif

}  // namespace hgraph
