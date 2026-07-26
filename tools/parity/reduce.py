"""Failure-preserving reduction for recipe ticks, values, and expressions."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Iterable

from .catalog import validate_recipe
from .model import Recipe, RecipeError


@dataclass(frozen=True)
class ReductionResult:
    recipe: Recipe
    attempts: int
    accepted: int
    timed_out: bool
    steps: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "attempts": self.attempts,
            "accepted": self.accepted,
            "timed_out": self.timed_out,
            "steps": list(self.steps),
            "recipe": self.recipe.to_dict(),
        }


def _valid_candidate(recipe: Recipe) -> bool:
    try:
        validate_recipe(recipe)
    except RecipeError:
        return False
    return True


def _expression_candidates(expression) -> Iterable[dict]:
    if not isinstance(expression, dict) or "op" not in expression:
        return
    args = expression.get("args", ())
    for argument in args:
        if isinstance(argument, dict):
            yield argument
    for index, argument in enumerate(args):
        for replacement in _expression_candidates(argument):
            new_args = list(args)
            new_args[index] = replacement
            yield {"op": expression["op"], "args": new_args}


def reduce_recipe(
    recipe: Recipe,
    predicate: Callable[[Recipe], bool],
    *,
    time_budget_seconds: float = 120.0,
    hypothesis_examples: int = 80,
) -> ReductionResult:
    """Minimize ``recipe`` while ``predicate(candidate)`` remains true."""

    started = time.monotonic()
    current = recipe
    attempts = 0
    accepted = 0
    steps: list[str] = []

    def expired() -> bool:
        return time.monotonic() - started >= time_budget_seconds

    def try_candidate(candidate: Recipe, step: str) -> bool:
        nonlocal current, attempts, accepted
        if expired() or not _valid_candidate(candidate):
            return False
        attempts += 1
        if predicate(candidate):
            current = candidate
            accepted += 1
            steps.append(step)
            return True
        return False

    # Let Hypothesis shrink the cycle prefix before the structural delta pass.
    if current.tick_count > 1 and not expired():
        try:
            from hypothesis import Phase, find, settings
            from hypothesis.errors import NoSuchExample
            from hypothesis import strategies as st

            base = current

            def prefix_fails(count: int) -> bool:
                candidate = base.replace(
                    inputs={
                        name: ticks[:count] for name, ticks in base.inputs.items()
                    }
                )
                return _valid_candidate(candidate) and predicate(candidate)

            attempts += 1
            try:
                count = find(
                    st.integers(min_value=1, max_value=current.tick_count),
                    prefix_fails,
                    settings=settings(
                        max_examples=hypothesis_examples,
                        database=None,
                        deadline=None,
                        phases=(Phase.generate, Phase.shrink),
                    ),
                )
            except NoSuchExample:
                count = current.tick_count
            if count < current.tick_count:
                current = current.replace(
                    inputs={
                        name: ticks[:count]
                        for name, ticks in current.inputs.items()
                    }
                )
                accepted += 1
                steps.append(f"hypothesis-prefix:{count}")
        except ImportError:
            pass

    # Delta-debug aligned ranges of ticks.
    granularity = 2
    while current.tick_count > 1 and not expired():
        tick_count = current.tick_count
        chunk_size = max(1, tick_count // granularity)
        changed = False
        for start in range(0, tick_count, chunk_size):
            stop = min(tick_count, start + chunk_size)
            if stop - start >= tick_count:
                continue
            candidate = current.replace(
                inputs={
                    name: ticks[:start] + ticks[stop:]
                    for name, ticks in current.inputs.items()
                }
            )
            if try_candidate(candidate, f"remove-ticks:{start}:{stop}"):
                changed = True
                granularity = 2
                break
        if not changed:
            if chunk_size == 1:
                break
            granularity = min(tick_count, granularity * 2)

    # Remove individual input events while retaining the cycle.
    changed = True
    while changed and not expired():
        changed = False
        for name, ticks in current.inputs.items():
            for index, value in enumerate(ticks):
                if value is None:
                    continue
                replacement = list(ticks)
                replacement[index] = None
                candidate_inputs = dict(current.inputs)
                candidate_inputs[name] = tuple(replacement)
                if try_candidate(
                    current.replace(inputs=candidate_inputs),
                    f"clear-event:{name}:{index}",
                ):
                    changed = True
                    break
            if changed:
                break

    # Simplify scalar magnitudes and mapping widths.
    for name in tuple(current.inputs):
        index = 0
        while index < len(current.inputs[name]) and not expired():
            value = current.inputs[name][index]
            candidates: list[object] = []
            if isinstance(value, bool):
                candidates = [False]
            elif isinstance(value, int):
                candidates = [0, 1, -1]
            elif isinstance(value, float):
                candidates = [0.0, 1.0, -1.0]
            elif isinstance(value, dict) and not any(
                str(key).startswith("$") for key in value
            ):
                candidates = [
                    {key: item}
                    for key, item in sorted(value.items())
                    if len(value) > 1
                ]
            for simpler in candidates:
                if simpler == value:
                    continue
                replacement = list(current.inputs[name])
                replacement[index] = simpler
                candidate_inputs = dict(current.inputs)
                candidate_inputs[name] = tuple(replacement)
                if try_candidate(
                    current.replace(inputs=candidate_inputs),
                    f"simplify-value:{name}:{index}",
                ):
                    break
            index += 1

    if current.template == "scalar_expression" and not expired():
        changed = True
        while changed:
            changed = False
            expression = current.parameters["expression"]
            for candidate_expression in _expression_candidates(expression):
                parameters = dict(current.parameters)
                parameters["expression"] = candidate_expression
                candidate = current.replace(parameters=parameters)
                if try_candidate(candidate, "simplify-expression"):
                    changed = True
                    break

    return ReductionResult(
        recipe=current,
        attempts=attempts,
        accepted=accepted,
        timed_out=expired(),
        steps=tuple(steps),
    )
