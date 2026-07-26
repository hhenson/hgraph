"""Versioned, data-only parity recipe model."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = 1
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_-]{2,79}$")
_INPUT_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,63}$")
_MAX_TICKS = 256


class RecipeError(ValueError):
    """A recipe is malformed or outside the bounded parity language."""


@dataclass(frozen=True)
class Recipe:
    schema_version: int
    id: str
    template: str
    inputs: Mapping[str, tuple[Any, ...]]
    parameters: Mapping[str, Any]
    features: tuple[str, ...]
    description: str = ""
    source_issue: str | None = None
    seed: int | None = None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Recipe":
        if not isinstance(raw, Mapping):
            raise RecipeError("recipe must be a JSON object")
        version = raw.get("schema_version")
        if version != SCHEMA_VERSION:
            raise RecipeError(
                f"unsupported schema_version {version!r}; expected {SCHEMA_VERSION}"
            )
        recipe_id = raw.get("id")
        if not isinstance(recipe_id, str) or not _IDENTIFIER.fullmatch(recipe_id):
            raise RecipeError(
                "id must be 3-80 lowercase letters, digits, hyphens, or underscores"
            )
        template = raw.get("template")
        if not isinstance(template, str) or not _IDENTIFIER.fullmatch(template):
            raise RecipeError("template must be a bounded-language identifier")

        raw_inputs = raw.get("inputs")
        if not isinstance(raw_inputs, Mapping) or not raw_inputs:
            raise RecipeError("inputs must be a non-empty object")
        inputs: dict[str, tuple[Any, ...]] = {}
        for name, ticks in raw_inputs.items():
            if not isinstance(name, str) or not _INPUT_NAME.fullmatch(name):
                raise RecipeError(f"invalid input name {name!r}")
            if not isinstance(ticks, list):
                raise RecipeError(f"input {name!r} must contain a JSON tick list")
            if not 1 <= len(ticks) <= _MAX_TICKS:
                raise RecipeError(
                    f"input {name!r} must have between 1 and {_MAX_TICKS} ticks"
                )
            inputs[name] = tuple(ticks)

        parameters = raw.get("parameters", {})
        if not isinstance(parameters, Mapping):
            raise RecipeError("parameters must be an object")
        features = raw.get("features", [])
        if (
            not isinstance(features, list)
            or not all(isinstance(value, str) and value for value in features)
        ):
            raise RecipeError("features must be a list of non-empty strings")
        if len(set(features)) != len(features):
            raise RecipeError("features must not contain duplicates")
        description = raw.get("description", "")
        if not isinstance(description, str):
            raise RecipeError("description must be a string")
        source_issue = raw.get("source_issue")
        if source_issue is not None and not isinstance(source_issue, str):
            raise RecipeError("source_issue must be a string or null")
        seed = raw.get("seed")
        if seed is not None and (not isinstance(seed, int) or isinstance(seed, bool)):
            raise RecipeError("seed must be an integer or null")

        recipe = cls(
            schema_version=version,
            id=recipe_id,
            template=template,
            inputs=inputs,
            parameters=dict(parameters),
            features=tuple(features),
            description=description,
            source_issue=source_issue,
            seed=seed,
        )
        recipe._validate_json_values()
        return recipe

    @classmethod
    def load(cls, path: Path | str) -> "Recipe":
        path = Path(path)
        try:
            raw = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise RecipeError(f"cannot load recipe {path}: {error}") from error
        return cls.from_dict(raw)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "id": self.id,
            "description": self.description,
            "template": self.template,
            "inputs": {name: list(ticks) for name, ticks in self.inputs.items()},
            "parameters": dict(self.parameters),
            "features": list(self.features),
        }
        if self.source_issue is not None:
            result["source_issue"] = self.source_issue
        if self.seed is not None:
            result["seed"] = self.seed
        return result

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()

    @property
    def tick_count(self) -> int:
        return max(len(ticks) for ticks in self.inputs.values())

    def replace(
        self,
        *,
        inputs: Mapping[str, tuple[Any, ...]] | None = None,
        parameters: Mapping[str, Any] | None = None,
        recipe_id: str | None = None,
    ) -> "Recipe":
        raw = self.to_dict()
        if inputs is not None:
            raw["inputs"] = {name: list(ticks) for name, ticks in inputs.items()}
        if parameters is not None:
            raw["parameters"] = dict(parameters)
        if recipe_id is not None:
            raw["id"] = recipe_id
        return Recipe.from_dict(raw)

    def _validate_json_values(self) -> None:
        try:
            json.dumps(
                {"inputs": self.inputs, "parameters": self.parameters},
                allow_nan=False,
            )
        except (TypeError, ValueError) as error:
            raise RecipeError(
                "inputs and parameters must use finite, JSON-serializable values"
            ) from error


def load_corpus(path: Path) -> list[Recipe]:
    recipes = [Recipe.load(recipe_path) for recipe_path in sorted(path.glob("*.json"))]
    ids = [recipe.id for recipe in recipes]
    if len(ids) != len(set(ids)):
        raise RecipeError(f"duplicate recipe id in {path}")
    return recipes
