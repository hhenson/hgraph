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


#: A FAMILY, not a list of recipes: every scenario the divergence can reach, by the relation
#: that makes it reachable. A pin on one recipe would cover that recipe only and leave the next
#: seed to rediscover it as a new failure.
#:
#: NOT a defect: the consequence of a RULING. "No change means no tick" (2026-07-17; parity
#: matrix, "already accepted for mesh_ over an initially empty key set") means a mesh_ that
#: STARTS over an empty key set emits nothing, while one whose keys are all removed later keeps
#: the valid, empty output it already had. Its output therefore depends on its history, and
#: RECOVER mode -- a fresh start with re-seeded inputs -- cannot reproduce that history: the
#: enclosing key is simply absent. A snapshot restores the output and is unaffected. It is a
#: family here so the campaign neither fails on it nor hides a NEW recover failure.
#: (For the record: map_ in the same position emits an empty map, where the 0.5 reference emits
#: nothing -- the other side of the same ruling. That is a parity question, not this family.)
MESH_EMPTY_INPUT = KnownDefect(
    "mesh-empty-input-no-tick",
    "Accepted, not a defect: by the no-change-means-no-tick ruling a mesh_ started over an EMPTY "
    "key set emits nothing, while one emptied later keeps its valid empty output. RECOVER is a "
    "fresh start, so wherever a mesh_ layer's own input is empty at a cut the enclosing key is "
    "absent afterwards where the unbroken run still holds it, empty.")


def _input_at(events, cut):
    """The keyed input as it stands when the day starting at ``cut`` begins."""
    def fold(value, delta):
        if delta is None:
            return value
        if not isinstance(delta, dict):
            return delta
        merged = dict(value) if isinstance(value, dict) else {}
        for key, item in delta.items():
            if item == "REMOVE":
                merged.pop(key, None)
            else:
                merged[key] = fold(merged.get(key), item)
        return merged

    value = None
    for event in events[:cut]:
        value = fold(value, event)
    return value


def _empty_at_depth(value, depth: int) -> bool:
    if not isinstance(value, dict):
        return False            # never ticked is not "empty": both runs agree on nothing
    if depth == 0:
        return not value
    return any(_empty_at_depth(item, depth - 1) for item in value.values())


def _mesh_over_empty_input(scenario: Scenario) -> bool:
    # Layer k consumes the collection k levels down: the outermost the whole input.
    meshes = [depth for depth, layer in enumerate(scenario.layers) if layer == "mesh"]
    return any(_empty_at_depth(_input_at(scenario.events, cut), depth)
               for cut in scenario.cuts for depth in meshes)


def known_defect(scenario: Scenario):
    """The known-defect family ``scenario`` belongs to, if any. Membership says a failure is
    EXPECTED to be possible, not that it will happen. The campaign reports members and
    failures separately, so a fix shows up as failures dropping to none -- at which point the
    family is deleted, not left to rot. (``tsd-restored-slot-order`` went that way: capacity
    growth and the pending-erase flush did not commute in ``KeySlotStore``.)"""
    if scenario.mode == "recover":
        return MESH_EMPTY_INPUT if _mesh_over_empty_input(scenario) else None
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
