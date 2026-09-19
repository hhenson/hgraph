"""The campaign's profiles. Imports nothing, so the verdict on a finished campaign (``merge``)
needs no hgraph: it runs where only the shard reports are."""

PROFILES = {
    # A smoke run for a pull request: every leaf and layer once, shallow, mostly in process.
    "pr": {"max_depth": 2, "per_chain": 1, "length": (5, 7), "keys": 3, "process_share": 0.15},
    # The nightly: every chain to depth 3, several event streams and cut plans each.
    "nightly": {"max_depth": 3, "per_chain": 4, "length": (6, 14), "keys": 4, "process_share": 1.0},
}
