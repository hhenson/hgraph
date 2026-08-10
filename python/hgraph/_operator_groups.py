"""Public operator families backed by separately registered native kernels.

The registry names on the right are implementation dispatch points.  They are
kept available to internal wiring by name, but are presented to Python users as
grouped overrides of the public operator on the left.
"""

OPERATOR_OVERRIDE_GROUPS = {
    "combine": (
        ("Compound-scalar values", "combine_cs"),
        ("Dynamic JSON values", "combine_json"),
        ("Mapping values", "combine_map"),
        ("Keyed time-series dictionaries", "combine_tsd"),
        ("Time-series sets", "combine_tss_from_tsl"),
    ),
    "filter_by": (
        ("Keyed time-series dictionaries", "filter_tsd_by_matches"),
    ),
    "max_": (
        ("Packed time-series lists", "max_ts_list"),
    ),
    "merge": (
        ("Disjoint keyed dictionaries", "merge_tsd_disjoint"),
    ),
    "min_": (
        ("Packed time-series lists", "min_ts_list"),
    ),
}

OPERATOR_OVERRIDE_NAMES = frozenset(
    override_name
    for overrides in OPERATOR_OVERRIDE_GROUPS.values()
    for _, override_name in overrides
)
