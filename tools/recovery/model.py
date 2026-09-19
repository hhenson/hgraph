"""What a scenario is, and what it is expected to do."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from . import graphs

#: How the recoverable unit is placed.
#:   outer  -- the whole scenario is one component; owners inside it are its members and
#:             their workers are saved whole.
#:   inner  -- the component is the leaf, INSIDE the worker; nothing around it is in any
#:             component, and the owner that hosts it stands in for it.
PLACEMENTS = ("outer", "inner")
#: How a run is broken up.
#:   clean     -- never: the oracle, and a check that the scenario is well formed.
#:   snapshot  -- restart from a component checkpoint at each cut. STATE comes back, so the
#:                restarted run has to match the unbroken one delta for delta.
#:   recover   -- restart in RECOVER mode at each cut: the component's recorded INPUTS are
#:                re-seeded as a tick at the start of the day. No state comes back, so this
#:                is for graphs whose output is a function of their current input, and the
#:                promise is about VALUES: a consumer that starts with the day and folds
#:                what it is sent holds, after every cycle, what the unbroken run held. The
#:                re-seed is an extra tick by design, so the deltas differ.
MODES = ("clean", "snapshot", "recover")
HOSTS = ("graph", "spawn")


@dataclass(frozen=True)
class Scenario:
    chain: str                      # graphs.resolve() name, e.g. "dmapp__map__total"
    placement: str
    host: str                       # "graph": wired in the main graph; "spawn": a spawn_ stage
    mode: str
    events: tuple                   # one entry per cycle; None is a quiet cycle
    cuts: tuple = ()                # cycle indexes a new day starts at
    seed: int = 0
    tags: tuple = field(default_factory=tuple)

    @property
    def layers(self) -> tuple:
        return tuple(self.chain.split(graphs.SEPARATOR)[:-1])

    @property
    def leaf(self) -> str:
        return self.chain.split(graphs.SEPARATOR)[-1]

    @property
    def component(self) -> str:
        return "scenario" if self.placement == "outer" else graphs.hosted.recordable_id

    def to_json(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_json(data: dict) -> "Scenario":
        def freeze(value):
            if isinstance(value, list):
                return tuple(freeze(item) for item in value)
            if isinstance(value, dict):
                return {_key(key): freeze(item) for key, item in value.items()}
            return value

        return Scenario(**{key: freeze(value) if key in ("events", "cuts", "tags") else value
                           for key, value in data.items()})


def _key(key):
    # JSON object keys are strings; the scenarios key on ints.
    try:
        return int(key)
    except (TypeError, ValueError):
        return key


@dataclass(frozen=True)
class Expectation:
    outcome: str                    # "equal" | "refused"
    reason: str = ""                # substring of the refusal, or why it is expected


@dataclass(frozen=True)
class KnownDefect:
    id: str
    summary: str


def _removes(event) -> bool:
    if event == "REMOVE":
        return True
    return isinstance(event, dict) and any(_removes(item) for item in event.values())


#: A FAMILY, not a list of recipes: every scenario the defect can reach, by the relation that
#: makes it reachable. A pin on one recipe would cover that recipe only and leave the next
#: seed to rediscover the defect as a new failure.
TSD_SLOT_ORDER = KnownDefect(
    "tsd-restored-slot-order",
    "Python-value keyed storage: a slot freed before the cut and still free at it, then another "
    "removal after it, leaves a restored TSD iterating its keys in a different order from the "
    "unbroken run. Only an order-sensitive reduction shows it. The same stream passes from C++.")


def known_defect(scenario: Scenario):
    """The known-defect family ``scenario`` belongs to, if any. Membership says a failure is
    EXPECTED to be possible, not that it will happen: most members pass. The campaign
    reports members and failures separately, so a fix shows up as failures dropping to none
    -- at which point the family is deleted, not left to rot."""
    if scenario.mode != "snapshot" or scenario.leaf != "folded":
        return None
    # A removal on EACH side of some cut. The first relation asked for them in the two cycles
    # either side of it, and the next seed walked straight past: removal, an ordinary tick,
    # then the cut and another removal. What the defect needs is a freed slot still free at
    # the cut, which outlives the cycle that freed it. Measured on nightly seed 2: 125 of the
    # 168 ``folded`` snapshot scenarios are members, all 6 failures are among them, 119 members
    # pass, and the 43 outside pass -- so this is a relation, not a blanket over the leaf.
    events = scenario.events
    for cut in scenario.cuts:
        if any(map(_removes, events[:cut])) and any(map(_removes, events[cut:])):
            return TSD_SLOT_ORDER
    return None


def expected(scenario: Scenario) -> Expectation:
    """What SHOULD happen. Every refusal here is a documented limit with a reason; a
    scenario that starts to pass is reported just as loudly as one that starts to fail, so
    lifting a limit cannot go unnoticed."""
    if scenario.mode == "clean":
        return Expectation("equal")
    if scenario.mode == "recover":
        # Generated only for stateless leaves with the component outermost; see generate.py.
        return Expectation("equal")
    if scenario.placement == "outer":
        return Expectation("equal")
    # placement == "inner": the component has to be REACHABLE by an image selected by
    # component -- wired directly in the main graph (an ordinary component), directly in a
    # spawn_ stage, or in the child of a dmap_ in the main graph. A map_ or mesh_ the user
    # wrote is not taken by such an image, and neither is a dmap_ nested inside a worker, so
    # a component below one is not reached and the run is refused as not wired (RFC 0039).
    if scenario.host == "spawn":
        reachable = scenario.layers == ()
    else:
        reachable = scenario.layers in ((), ("dmapi",), ("dmapp",))
    if reachable:
        return Expectation("equal")
    return Expectation("refused", "configured component was not wired")
