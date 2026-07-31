"""Stable memory-growth profiles built from the comparative scenarios.

The timing pack answers how quickly one workload completes.  This registry
selects deliberately different scale points so the memory pack can answer
whether resident and native structural memory are fixed, bounded, retained,
or proportional to graph/cardinality growth.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class MemoryProfile:
    """One independently measured process invocation."""

    group: str
    label: str
    scenario: str
    cycle_scale: float
    size_scale: float
    growth_axis: str
    expectation: str


def _series(
    group: str,
    label: str,
    scenario: str,
    axis: str,
    expectation: str,
    points: tuple[tuple[str, float, float], ...],
) -> dict[str, MemoryProfile]:
    return {
        f"{scenario}__{suffix}": MemoryProfile(
            group=group,
            label=f"{label} - {suffix}",
            scenario=scenario,
            cycle_scale=cycle_scale,
            size_scale=size_scale,
            growth_axis=axis,
            expectation=expectation,
        )
        for suffix, cycle_scale, size_scale in points
    }


PROFILES: dict[str, MemoryProfile] = {}

# Static graph size and bounded scalar execution establish the fixed and
# duration-dependent floors before collection/nested-graph storage is added.
PROFILES.update(_series(
    "Static graph", "Wide/deep native graph", "construct_std", "graph size",
    "peak should scale with wired graph size; post-GC growth should remain bounded",
    (("small", 1.0, 0.25), ("medium", 1.0, 0.5), ("large", 1.0, 1.0)),
))
PROFILES.update(_series(
    "Bounded execution", "Native scalar hot loop", "tick_std", "duration",
    "peak and retained memory should be approximately duration independent",
    (("short", 0.1, 1.0), ("medium", 1.0, 1.0), ("long", 4.0, 1.0)),
))
PROFILES.update(_series(
    "Bounded execution", "Python compute chain", "tick_py", "duration",
    "peak and retained memory should be approximately duration independent",
    (("short", 0.1, 1.0), ("medium", 0.5, 1.0), ("long", 1.0, 1.0)),
))

# Value representations exercise heap-owned strings, native compound values,
# the Python bridge, and a fixed-capacity tick window.
PROFILES.update(_series(
    "Value storage", "String arithmetic", "type_str_std", "duration",
    "temporary value memory should remain bounded across cycles",
    (("short", 0.1, 1.0), ("long", 1.0, 1.0)),
))
PROFILES.update(_series(
    "Value storage", "CompoundScalar through Python", "type_cs_py", "duration",
    "bridge storage should remain bounded across cycles",
    (("short", 0.1, 1.0), ("long", 1.0, 1.0)),
))
PROFILES.update(_series(
    "Value storage", "Fixed tick window", "type_tsw_append_evict_std", "duration",
    "the 64-element window should remain bounded as evictions continue",
    (("short", 0.25, 1.0), ("medium", 1.0, 1.0), ("long", 4.0, 1.0)),
))
PROFILES.update(_series(
    "Value storage", "Set add/remove", "tss_add_remove_std", "live cardinality",
    "peak should scale with live set cardinality; duration is fixed",
    (("small", 0.2, 0.25), ("medium", 0.2, 1.0), ("large", 0.2, 4.0)),
))

# Keyed collections distinguish live-cardinality cost, deliberately retained
# slot capacity, bounded churn, and true monotonic growth.
PROFILES.update(_series(
    "Keyed collections", "Dense TSD map/reduce", "tsd_dense_std", "cardinality",
    "peak native storage should scale with simultaneously live keys",
    (("small", 0.1, 0.1), ("medium", 0.1, 0.5), ("large", 0.1, 1.0)),
))
PROFILES.update(_series(
    "Keyed collections", "Sparse retained capacity", "tsd_sparse_large_capacity_std",
    "key capacity", "peak should scale with retained key and child-slot capacity",
    (("small", 0.05, 0.05), ("medium", 0.05, 0.25), ("large", 0.05, 1.0)),
))
PROFILES.update(_series(
    "Keyed collections", "Bounded key churn", "tsd_churn_std", "duration",
    "live keys are bounded; slot reuse should prevent cycle-proportional growth",
    (("short", 0.1, 1.0), ("medium", 0.5, 1.0), ("long", 1.0, 1.0)),
))
PROFILES.update(_series(
    "Keyed collections", "Monotonic key growth", "tsd_capacity_growth_std", "duration",
    "memory should grow with the intentionally increasing key population",
    (("short", 0.1, 1.0), ("medium", 0.5, 1.0), ("long", 1.0, 1.0)),
))
PROFILES.update(_series(
    "Keyed collections", "Clear and repopulate", "tsd_clear_repopulate_std", "duration",
    "capacity may be retained, but repeated clear/repopulate must remain bounded",
    (("short", 0.05, 1.0), ("medium", 0.25, 1.0), ("long", 1.0, 1.0)),
))
PROFILES.update(_series(
    "Keyed collections", "Key reactivation", "tsd_key_reactivation_std", "duration",
    "reused identities should not create cycle-proportional storage growth",
    (("short", 0.1, 1.0), ("medium", 0.5, 1.0), ("long", 1.0, 1.0)),
))

# These profiles exercise the slot stores used by reduce, switch, mesh, and
# dynamic TSL as well as service client multiplicity.
PROFILES.update(_series(
    "Nested graphs", "TSD nested-graph reduce", "reduce_tsd_nested_graph_std",
    "cardinality", "reducer banks should scale with input cardinality",
    (("small", 0.05, 0.1), ("medium", 0.05, 0.5), ("large", 0.05, 1.0)),
))
PROFILES.update(_series(
    "Nested graphs", "Keyed collection switch", "switch_keyed_collection_std",
    "live cardinality", "active and retained branch storage should scale with live keys",
    (("small", 0.1, 0.25), ("medium", 0.1, 1.0), ("large", 0.1, 2.0)),
))
PROFILES.update(_series(
    "Nested graphs", "Dependency mesh", "mesh_std", "live cardinality",
    "mesh instance and dependency storage should scale with live keys",
    (("small", 0.1, 0.25), ("medium", 0.1, 1.0), ("large", 0.1, 2.0)),
))
PROFILES.update(_series(
    "Services", "Multiplexed Python service adaptor", "service_adaptor_py",
    "client count", "graph memory should scale with independently wired clients",
    (("small", 0.05, 0.25), ("medium", 0.05, 1.0), ("large", 0.05, 4.0)),
))
PROFILES.update(_series(
    "hg_cpp dynamic storage", "Dynamic TSL map/reduce", "reduce_dynamic_tsl_std",
    "initial capacity", "native slot storage should scale with list capacity",
    (("small", 0.1, 0.25), ("medium", 0.1, 1.0), ("large", 0.1, 4.0)),
))

