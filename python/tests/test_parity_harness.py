"""Tests for the model-free hgraph differential parity controller."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.artifact_fingerprint import hg_cpp_source_fingerprint
from tools.parity.campaign import run_campaign
from tools.parity.canonical import canonicalize
from tools.parity.catalog import validate_recipe
from tools.parity.compare import compare_outcomes
from tools.parity.coverage import coverage_report, recipe_features
from tools.parity.environments import ParityEnvironments
from tools.parity.issues import failure_fingerprint, issue_body, publish_failures
from tools.parity.model import Recipe, RecipeError, load_corpus
from tools.parity.reduce import reduce_recipe


CORPUS = Path(__file__).parents[2] / "tools" / "parity" / "corpus"


def _scalar_recipe(ticks=None):
    ticks = ticks or [1, 2, 3, 4, 5, 6, 7, 8]
    return Recipe.from_dict(
        {
            "schema_version": 1,
            "id": "test-scalar-recipe",
            "description": "test",
            "template": "scalar_expression",
            "inputs": {"value": ticks},
            "parameters": {
                "input_types": {"value": "int"},
                "output_type": "int",
                "expression": {"input": "value"},
            },
            "features": ["shape:TS"],
        }
    )


def test_committed_parity_corpus_is_valid_and_unique():
    recipes = load_corpus(CORPUS)
    assert len(recipes) >= 7
    for recipe in recipes:
        validate_recipe(recipe)
    assert len({recipe.fingerprint for recipe in recipes}) == len(recipes)


def test_recipe_rejects_unbounded_or_unsafe_shapes():
    raw = _scalar_recipe().to_dict()
    raw["id"] = "../../escape"
    with pytest.raises(RecipeError, match="id must"):
        Recipe.from_dict(raw)

    raw = _scalar_recipe().to_dict()
    raw["inputs"]["value"] = [1] * 257
    with pytest.raises(RecipeError, match="between 1 and 256"):
        Recipe.from_dict(raw)

    raw = _scalar_recipe().to_dict()
    raw["parameters"]["expression"] = {
        "op": "exec",
        "args": [{"input": "value"}],
    }
    with pytest.raises(RecipeError, match="unsupported expression"):
        validate_recipe(Recipe.from_dict(raw))


def test_recipe_fingerprint_is_independent_of_json_key_order():
    first = _scalar_recipe()
    raw = first.to_dict()
    raw["parameters"] = {
        "expression": {"input": "value"},
        "output_type": "int",
        "input_types": {"value": "int"},
    }
    assert Recipe.from_dict(raw).fingerprint == first.fingerprint


def test_canonicalization_preserves_tick_sensitive_value_semantics():
    class Removed:
        def __init__(self, item):
            self.item = item

        def __repr__(self):
            return f"Removed({self.item!r})"

    class FakeSetDelta:
        @property
        def added(self):
            return {2, 1}

        @property
        def removed(self):
            return {3}

    value = {
        "mapping": {"b": -0.0, "a": float("nan")},
        "tuple": (1, 2),
        "delta": FakeSetDelta(),
        "removed": Removed("old"),
    }
    assert canonicalize(value) == {
        "$map": [
            ["delta", {"$set_delta": {"added": [1, 2], "removed": [3]}}],
            [
                "mapping",
                {
                    "$map": [
                        ["a", {"$float": "nan"}],
                        ["b", {"$float": "-0x0.0p+0"}],
                    ]
                },
            ],
            ["removed", {"$set_removed": "old"}],
            ["tuple", {"$tuple": [1, 2]}],
        ]
    }


def test_comparison_distinguishes_tick_values_and_supports_opt_in_float_tolerance():
    reference = {
        "status": "ok",
        "trace": [None, {"$float": float(1.0).hex()}],
    }
    candidate = {
        "status": "ok",
        "trace": [None, {"$float": float(1.0001).hex()}],
    }
    assert compare_outcomes(reference, candidate) is not None
    assert (
        compare_outcomes(reference, candidate, float_abs_tolerance=0.001)
        is None
    )

    candidate["status"] = "error"
    candidate["phase"] = "runtime"
    candidate["exception"] = {"category": "value"}
    assert compare_outcomes(reference, candidate).classification == "status"


def test_generated_recipes_are_deterministic_typed_and_multi_tick():
    pytest.importorskip("hypothesis")
    from tools.parity.generate import generate_recipes

    first = generate_recipes(12, seed=17)
    second = generate_recipes(12, seed=17)
    assert [recipe.canonical_json() for recipe in first] == [
        recipe.canonical_json() for recipe in second
    ]
    assert first
    assert all(recipe.tick_count >= 8 for recipe in first)
    for recipe in first:
        validate_recipe(recipe)


def test_reducer_shrinks_ticks_values_and_expression_while_preserving_failure():
    recipe = _scalar_recipe([10, 20, 30, 99, 40, 50, 60, 70])
    raw = recipe.to_dict()
    raw["parameters"]["expression"] = {
        "op": "add",
        "args": [{"input": "value"}, {"const": 0}],
    }
    recipe = Recipe.from_dict(raw)

    def failure(candidate):
        values = candidate.inputs["value"]
        return len(values) >= 2 and 99 in values

    reduced = reduce_recipe(recipe, failure, time_budget_seconds=10)
    assert failure(reduced.recipe)
    assert reduced.recipe.tick_count == 2
    assert reduced.accepted > 0
    assert reduced.recipe.parameters["expression"] == {"input": "value"}


def test_campaign_verifies_reduces_and_fingerprints_a_stable_mismatch(monkeypatch, tmp_path):
    recipe = _scalar_recipe()
    reference = {
        "status": "ok",
        "phase": "complete",
        "trace": [1],
        "implementation": {"distribution": "hgraph", "version": "1"},
    }
    candidate = {
        "status": "ok",
        "phase": "complete",
        "trace": [2],
        "implementation": {"distribution": "hg_cpp", "version": "1"},
    }

    class Cache:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, *_args, **_kwargs):
            return reference, False

    monkeypatch.setattr("tools.parity.campaign.ReferenceTraceCache", Cache)
    monkeypatch.setattr(
        "tools.parity.campaign.run_recipe",
        lambda interpreter, *_args, **_kwargs: (
            reference if str(interpreter) == "reference" else candidate
        ),
    )
    environments = ParityEnvironments(
        reference_python=Path("reference"),
        candidate_python=Path("candidate"),
        reference_identity=reference["implementation"],
        candidate_identity=candidate["implementation"],
        candidate_fingerprint="candidate-sha",
    )
    report = run_campaign(
        [recipe],
        environments,
        verify_replays=3,
        reduce_failures=False,
        known_divergences_path=tmp_path / "missing.json",
        cache_path=tmp_path / "cache",
    )
    assert report["summary"]["verified_failures"] == 1
    assert report["summary"]["quarantined"] == 0
    failure = report["verified_failures"][0]
    assert failure["failure_fingerprint"] == failure_fingerprint(failure)


def test_reference_failure_is_quarantined_not_promoted(monkeypatch, tmp_path):
    recipe = _scalar_recipe()
    reference = {
        "status": "crash",
        "phase": "process",
        "process_returncode": -11,
    }
    candidate = {"status": "ok", "phase": "complete", "trace": [1]}

    class Cache:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, *_args, **_kwargs):
            return reference, False

    monkeypatch.setattr("tools.parity.campaign.ReferenceTraceCache", Cache)
    monkeypatch.setattr(
        "tools.parity.campaign.run_recipe",
        lambda interpreter, *_args, **_kwargs: (
            reference if str(interpreter) == "reference" else candidate
        ),
    )
    environments = ParityEnvironments(
        reference_python=Path("reference"),
        candidate_python=Path("candidate"),
        reference_identity={"distribution": "hgraph"},
        candidate_identity={"distribution": "hg_cpp"},
        candidate_fingerprint="candidate-sha",
    )
    report = run_campaign(
        [recipe],
        environments,
        reduce_failures=False,
        known_divergences_path=tmp_path / "missing.json",
        cache_path=tmp_path / "cache",
    )
    assert report["summary"]["verified_failures"] == 0
    assert report["quarantined"][0]["classification"] == "reference-failure"


def test_issue_payload_is_deterministic_and_dry_run_has_no_github_write():
    failure = {
        "minimized_recipe": _scalar_recipe().to_dict(),
        "difference": {
            "classification": "value",
            "path": "$.trace[0]",
            "reference": 1,
            "candidate": 2,
        },
        "reference": {"status": "ok", "trace": [1]},
        "candidate": {"status": "ok", "trace": [2]},
        "reduction": {"attempts": 3, "accepted": 2},
    }
    fingerprint = failure_fingerprint(failure)
    assert fingerprint in issue_body(failure)
    actions = publish_failures(
        [failure], repo="hhenson/hg_cpp", publish=False
    )
    assert actions == [
        {
            "action": "dry-run",
            "title": "[parity] scalar_expression differs from released hgraph",
            "fingerprint": fingerprint,
            "body": issue_body(failure),
        }
    ]


def test_issue_publisher_does_not_deduplicate_distinct_same_template_failures(
    monkeypatch,
):
    failure = {
        "failure_fingerprint": "new-fingerprint",
        "minimized_recipe": _scalar_recipe().to_dict(),
        "difference": {
            "classification": "value",
            "path": "$.trace[1]",
            "reference": 1,
            "candidate": 2,
        },
        "reference": {"status": "ok", "trace": [1]},
        "candidate": {"status": "ok", "trace": [2]},
        "reduction": {"attempts": 0, "accepted": 0},
    }
    existing = {
        "number": 44,
        "state": "OPEN",
        "title": "[parity] scalar_expression differs from released hgraph",
        "body": "<!-- hgraph-parity:other-fingerprint -->",
        "url": "https://github.com/hhenson/hg_cpp/issues/44",
    }
    calls = []

    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: [existing]
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append((arguments, repo, capture))
        return SimpleNamespace(
            stdout="https://github.com/hhenson/hg_cpp/issues/45\n"
        )

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)

    assert publish_failures(
        [failure], repo="hhenson/hg_cpp", publish=True
    ) == [
        {
            "action": "created",
            "url": "https://github.com/hhenson/hg_cpp/issues/45",
            "fingerprint": "new-fingerprint",
        }
    ]
    assert any(arguments[:2] == ["issue", "create"] for arguments, _, _ in calls)


def test_coverage_reports_operator_frontier_and_semantic_pairs():
    recipe = _scalar_recipe()
    report = coverage_report(
        [recipe], operator_inventory=("add_", "uncovered_operator")
    )
    assert "uncovered_operator" in report["missing_operators"]
    assert "template:scalar_expression" in recipe_features(recipe)
    assert report["pair_counts"]


def test_wheel_fingerprint_excludes_parity_tooling(tmp_path):
    (tmp_path / "python" / "hgraph").mkdir(parents=True)
    (tmp_path / "src").mkdir()
    (tmp_path / "tools" / "parity").mkdir(parents=True)
    (tmp_path / "CMakeLists.txt").write_text("project(test)\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
    (tmp_path / "src" / "runtime.cpp").write_text("int runtime;\n")
    before = hg_cpp_source_fingerprint(tmp_path, python_version="3.12")
    (tmp_path / "tools" / "parity" / "recipe.json").write_text(
        json.dumps({"changed": True})
    )
    after = hg_cpp_source_fingerprint(tmp_path, python_version="3.12")
    assert before == after
