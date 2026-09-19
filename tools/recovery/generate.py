"""Seeded scenario generation."""

from __future__ import annotations

import itertools
import random

import hgraph as hg

from . import graphs
from .model import Scenario
from .profiles import PROFILES

_REMOVE = "REMOVE"          # spelled out in recipes; turned into hg.REMOVE when a day is run


def _events(rng: random.Random, depth: int, length: int, keys: int, removals_until=None):
    """``length`` cycles over a ``depth``-level keyed input. Removals only ever name a key
    that is present, quiet cycles are common, and keys leave and come back.

    ``removals_until`` stops removals from that cycle on. A RECOVER day starts its source
    afresh, outside the component and unrestored, and a source cannot remove a key it never
    held; a snapshot restores that baseline, which is why snapshot scenarios need no limit."""
    removing = [True]

    def tick(state, level):
        if level == 0:
            return rng.randint(1, 9)
        delta = {}
        for key in rng.sample(range(keys), rng.randint(1, keys)):
            present = key in state
            if present and removing[0] and rng.random() < 0.2:
                del state[key]
                delta[key] = _REMOVE
                continue
            child = state.setdefault(key, {})
            value = tick(child, level - 1)
            if value is not None and value != {}:
                delta[key] = value
            elif not present:
                del state[key]
        return delta or None

    state: dict = {}
    events = []
    for cycle in range(length):
        removing[0] = removals_until is None or cycle < removals_until
        events.append(None if rng.random() < 0.25 else tick(state, depth))
    if all(event is None for event in events):
        events[0] = tick(state, depth)
    return tuple(events)


def thaw(event):
    """A recipe's event as hgraph takes it."""
    if event == _REMOVE:
        return hg.REMOVE
    if isinstance(event, dict):
        return {key: thaw(value) for key, value in event.items()}
    return event


def _cuts(rng: random.Random, length: int, plan: str):
    if plan == "every":
        return tuple(range(1, length))
    if plan == "single":
        return (rng.randint(1, length - 1),)
    count = rng.randint(2, max(2, length // 3))
    return tuple(sorted(rng.sample(range(1, length), min(count, length - 1))))


def _chains(max_depth: int):
    layers = tuple(graphs.LAYERS)
    for leaf, entry in graphs.LEAVES.items():
        for depth in range(0, max_depth - entry.depth + 1):
            for stack in itertools.product(layers, repeat=depth):
                yield graphs.SEPARATOR.join((*stack, leaf))




def scenarios(profile: str, seed: int):
    """Every scenario of ``profile``, in a stable order that depends only on ``seed``."""
    settings = PROFILES[profile]
    rng = random.Random(seed)
    for chain in sorted(_chains(settings["max_depth"])):
        entry = graphs.resolve(chain)
        uses_processes = "dmapp" in chain
        for host in ("graph", "spawn"):
            for placement in ("outer", "inner"):
                if (placement == "inner") != (entry.name.split(graphs.SEPARATOR)[-1] == "hosted"):
                    continue        # the inner component IS the "hosted" leaf, and only it
                if (uses_processes or host == "spawn") and rng.random() > settings["process_share"]:
                    continue
                for _ in range(settings["per_chain"]):
                    length = rng.randint(*settings["length"])
                    events = _events(rng, entry.depth, length, settings["keys"])
                    plan = rng.choice(("single", "every", "several"))
                    yield Scenario(chain=chain, placement=placement, host=host, mode="snapshot",
                                   events=events, cuts=_cuts(rng, length, plan), seed=seed,
                                   tags=(plan, f"depth{entry.depth}"))
                # RECOVER brings inputs back, not state: stateless graphs, component outermost,
                # in the main graph (a spawn_ sink is not an output a value can be read from).
                if entry.stateful or placement != "outer" or host != "graph":
                    continue
                for _ in range(settings["per_chain"]):
                    length = rng.randint(*settings["length"])
                    plan = rng.choice(("single", "every", "several"))
                    cuts = _cuts(rng, length, plan)
                    events = _events(rng, entry.depth, length, settings["keys"], removals_until=cuts[0])
                    yield Scenario(chain=chain, placement=placement, host=host, mode="recover",
                                   events=events, cuts=cuts, seed=seed,
                                   tags=(plan, f"depth{entry.depth}"))


def shard(items, index: int, count: int):
    return [item for position, item in enumerate(items) if position % count == index]
