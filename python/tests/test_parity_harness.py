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
from tools.parity.cli import CAMPAIGN_PROFILES
from tools.parity.compare import compare_outcomes
from tools.parity.coverage import coverage_report, recipe_features
from tools.parity.environments import ParityEnvironments
from tools.parity.issues import failure_fingerprint, issue_body, publish_failures
from tools.parity.model import Recipe, RecipeError, load_corpus
from tools.parity.reduce import reduce_recipe
from tools.parity.runner import _fallback_operator_names


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
    assert len(recipes) >= 19
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

    Sentinel = type("Sentinel", (), {})
    remove = Sentinel()
    remove.name = "REMOVE"
    value = {
        "mapping": {"b": -0.0, "a": float("nan")},
        "tuple": (1, 2),
        "delta": FakeSetDelta(),
        "removed": Removed("old"),
        "remove": remove,
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
            ["remove", {"$remove": True}],
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


def test_generated_framework_recipes_prioritize_ref_and_non_peered_paths():
    pytest.importorskip("hypothesis")
    from tools.parity.generate import generate_recipes

    recipes = generate_recipes(480, seed=29)
    templates = {recipe.template for recipe in recipes}
    assert {
        "service_reference",
        "service_request_reply",
        "service_subscription",
        "adaptor_loopback",
        "service_adaptor_roundtrip",
        "context_switch",
        "operator_pipeline",
        "tsd_key_set_pipeline",
        "mesh_key_set",
    } <= templates
    assert sum(
        "reference:REF" in recipe.features
        and "binding:non-peered" in recipe.features
        for recipe in recipes
    ) >= len(recipes) * 0.8
    assert any(
        recipe.template == "operator_pipeline"
        and recipe.parameters["format_ref"]
        for recipe in recipes
    )
    # dedup_size=false is the ruled no-change space and is corpus-only now
    # (PR #67 review); generated recipes always dedup.
    assert all(
        recipe.parameters["dedup_size"] is True
        for recipe in recipes
        if recipe.template == "tsd_key_set_pipeline"
    )


def test_campaign_profiles_scale_nightly_breadth_and_tick_depth():
    assert CAMPAIGN_PROFILES["pr"]["examples"] == 48
    assert CAMPAIGN_PROFILES["nightly"]["examples"] == 5000
    assert CAMPAIGN_PROFILES["nightly"]["max_ticks"] == 64
    assert CAMPAIGN_PROFILES["nightly"]["time_budget"] == 3600.0


def test_operator_inventory_fallback_excludes_callable_types_and_helpers():
    operator_type = type("OperatorWiringNodeClass", (), {})
    namespace = SimpleNamespace(
        add_=operator_type(),
        TS=type("TS", (), {}),
        helper=lambda: None,
    )
    assert _fallback_operator_names(namespace) == {"add_"}


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


def test_campaign_classifies_known_family_without_verification(monkeypatch, tmp_path):
    # First-pass sanity check: a mismatch inside a documented deviation's
    # parameter space is a known failure and spends no verification replays
    # or reduction budget.
    recipe = Recipe.from_dict(
        {
            "schema_version": 1,
            "id": "test-tsd-reduce-family",
            "description": "test",
            "template": "tsd_map_reduce",
            "inputs": {"values": [None, None]},
            "parameters": {"increment": 1, "zero": -2},
            "features": ["shape:TSD"],
        }
    )
    reference = {
        "status": "ok",
        "phase": "complete",
        "trace": [-4, None],
        "implementation": {"distribution": "hgraph", "version": "1"},
    }
    candidate = {
        "status": "ok",
        "phase": "complete",
        "trace": [-2, None],
        "implementation": {"distribution": "hg_cpp", "version": "1"},
    }

    class Cache:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, *_args, **_kwargs):
            return reference, False

    def no_verification(*_args, **_kwargs):
        raise AssertionError("family-known mismatch must not be re-verified")

    monkeypatch.setattr("tools.parity.campaign.ReferenceTraceCache", Cache)
    monkeypatch.setattr("tools.parity.campaign._verify_pair", no_verification)
    monkeypatch.setattr(
        "tools.parity.campaign.run_recipe",
        lambda interpreter, *_args, **_kwargs: candidate,
    )
    known_path = tmp_path / "known_divergences.json"
    known_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "divergences": [],
                "families": [
                    {
                            "family": "reduce-non-identity-zero",
                            "template": "tsd_map_reduce",
                            "parameters_not_equal": {"zero": 0},
                            "relation": "trace-value",
                        }
                ],
            }
        )
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
        reduce_failures=True,
        known_divergences_path=known_path,
        cache_path=tmp_path / "cache",
    )
    assert report["summary"]["verified_failures"] == 0
    assert report["summary"]["known_failures"] == 1
    assert report["summary"]["quarantined"] == 0
    known = report["known_failures"][0]
    assert known["reduction"]["attempts"] == 0


def test_family_suppression_covers_only_the_documented_difference(
    monkeypatch, tmp_path
):
    # The reduce family's accepted deviation is a completed-run trace-value
    # difference. A candidate crash inside the same parameter space is NOT
    # the deviation and must continue through verification and publish as a
    # verified failure. Likewise a recipe that omits the parameter runs at
    # the template default (the identity), so it is outside the family.
    families = [
        {
            "family": "reduce-non-identity-zero",
            "template": "tsd_map_reduce",
            "parameters_not_equal": {"zero": 0},
        }
    ]
    known_path = tmp_path / "known_divergences.json"
    known_path.write_text(
        json.dumps(
            {"schema_version": 1, "divergences": [], "families": families}
        )
    )
    reference = {
        "status": "ok",
        "phase": "complete",
        "trace": [-4, None],
        "implementation": {"distribution": "hgraph", "version": "1"},
    }

    def campaign_for(recipe, candidate):
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
            candidate_identity={"distribution": "hg_cpp", "version": "1"},
            candidate_fingerprint="candidate-sha",
        )
        return run_campaign(
            [recipe],
            environments,
            verify_replays=3,
            reduce_failures=False,
            known_divergences_path=known_path,
            cache_path=tmp_path / "cache",
        )

    crashing_candidate = {
        "status": "error",
        "phase": "runtime",
        "exception": {"category": "runtime", "type": "AttributeError"},
        "implementation": {"distribution": "hg_cpp", "version": "1"},
    }
    in_family = Recipe.from_dict(
        {
            "schema_version": 1,
            "id": "test-family-crash",
            "description": "test",
            "template": "tsd_map_reduce",
            "inputs": {"values": [None, None]},
            "parameters": {"increment": 1, "zero": -2},
            "features": ["shape:TSD"],
        }
    )
    report = campaign_for(in_family, crashing_candidate)
    assert report["summary"]["verified_failures"] == 1
    assert report["summary"]["known_failures"] == 0

    value_candidate = {
        "status": "ok",
        "phase": "complete",
        "trace": [-2, None],
        "implementation": {"distribution": "hg_cpp", "version": "1"},
    }
    omitted_zero = Recipe.from_dict(
        {
            "schema_version": 1,
            "id": "test-family-omitted-zero",
            "description": "test",
            "template": "tsd_map_reduce",
            "inputs": {"values": [None, None]},
            "parameters": {"increment": 1},
            "features": ["shape:TSD"],
        }
    )
    report = campaign_for(omitted_zero, value_candidate)
    assert report["summary"]["verified_failures"] == 1
    assert report["summary"]["known_failures"] == 0


def test_generated_tsd_map_reduce_recipes_use_identity_zero():
    # Non-identity zeros are the documented reduce deviation: the generator
    # must not explore that space (differential results there measure the
    # deviation, not candidate defects).
    from tools.parity.generate import generate_recipes

    recipes = generate_recipes(200, seed=7)
    tsd = [r for r in recipes if r.template == "tsd_map_reduce"]
    assert tsd, "expected generated tsd_map_reduce recipes"
    assert all(r.parameters["zero"] == 0 for r in tsd)


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


def test_issue_publisher_deduplicates_same_fingerprint_within_one_run(
    monkeypatch,
):
    failure = {
        "failure_fingerprint": "repeat-fingerprint",
        "minimized_recipe": _scalar_recipe().to_dict(),
        "difference": {
            "classification": "value",
            "path": "$.trace[0]",
            "reference": 1,
            "candidate": 2,
        },
        "reference": {"status": "ok", "trace": [1]},
        "candidate": {"status": "ok", "trace": [2]},
        "reduction": {"attempts": 0, "accepted": 0},
    }
    calls = []

    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: []
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append((arguments, repo, capture))
        return SimpleNamespace(
            stdout="https://github.com/hhenson/hg_cpp/issues/45\n"
        )

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)

    assert publish_failures(
        [failure, dict(failure), dict(failure)],
        repo="hhenson/hg_cpp",
        publish=True,
    ) == [
        {
            "action": "created",
            "url": "https://github.com/hhenson/hg_cpp/issues/45",
            "fingerprint": "repeat-fingerprint",
        },
        {
            "action": "deduplicated",
            "url": "https://github.com/hhenson/hg_cpp/issues/45",
            "fingerprint": "repeat-fingerprint",
        },
        {
            "action": "deduplicated",
            "url": "https://github.com/hhenson/hg_cpp/issues/45",
            "fingerprint": "repeat-fingerprint",
        },
    ]
    assert (
        sum(1 for arguments, _, _ in calls if arguments[:2] == ["issue", "create"])
        == 1
    )


def test_publisher_consults_known_divergences_and_never_reopens(
    monkeypatch, tmp_path
):
    # The consolidation step takes the known-divergence records into account:
    # a failure whose fingerprint (or family) is known is reported as
    # "known-divergence" — it is neither created nor REOPENED, even when a
    # closed issue with the matching marker exists.
    failure = {
        "failure_fingerprint": "ruled-fingerprint",
        "minimized_recipe": _scalar_recipe().to_dict(),
        "difference": {
            "classification": "value",
            "path": "$.trace[0]",
            "reference": 1,
            "candidate": 2,
        },
        "reference": {"status": "ok", "trace": [1]},
        "candidate": {"status": "ok", "trace": [2]},
        "reduction": {"attempts": 0, "accepted": 0},
    }
    known_path = tmp_path / "known_divergences.json"
    known_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "divergences": [{"fingerprint": "ruled-fingerprint"}],
                "families": [],
            }
        )
    )
    closed = {
        "number": 99,
        "state": "CLOSED",
        "title": "[parity] scalar_expression differs from released hgraph",
        "body": "<!-- hgraph-parity:ruled-fingerprint -->",
        "url": "https://github.com/hhenson/hg_cpp/issues/99",
    }
    calls = []

    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: [closed]
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append((arguments, repo, capture))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)

    actions = publish_failures(
        [failure],
        repo="hhenson/hg_cpp",
        publish=True,
        known_divergences_path=known_path,
    )
    assert actions == [
        {"action": "known-divergence", "fingerprint": "ruled-fingerprint"}
    ]
    assert not any(
        arguments[:2] in (["issue", "create"], ["issue", "reopen"])
        for arguments, _, _ in calls
    )

    # Dry-run reports the same classification.
    dry = publish_failures(
        [failure],
        repo="hhenson/hg_cpp",
        publish=False,
        known_divergences_path=known_path,
    )
    assert dry == [
        {"action": "known-divergence", "fingerprint": "ruled-fingerprint"}
    ]


def test_family_gate_requires_the_documented_trace_relation():
    # PR #67 review reproductions (base 0a6df5b7). Parameter membership
    # selects a possible deviation, but the complete traces must also satisfy
    # that family's relation so payload regressions remain differential.
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
    )

    _fingerprints, families = load_known_divergences()
    ok = lambda trace: {"status": "ok", "trace": trace}

    # 1. service_adaptor_roundtrip: ref [2] vs cand [None, 2].
    adaptor_recipe = {
        "template": "service_adaptor_roundtrip",
        "inputs": {"value": [0]},
        "parameters": {"increment": 2},
    }
    difference = compare_outcomes(ok([2]), ok([None, 2]))
    assert difference.to_dict()["classification"] == "length"
    assert is_known_family_failure(
        adaptor_recipe, difference.to_dict(), ok([2]), ok([None, 2]), families
    )
    corrupted = ok([None, 3])
    difference = compare_outcomes(ok([2]), corrupted)
    assert not is_known_family_failure(
        adaptor_recipe,
        difference.to_dict(),
        ok([2]),
        corrupted,
        families,
    )

    # 2. tsd_key_set_pipeline dedup_size=false: the unchanged size field is
    # republished by the reference and omitted by the candidate. The float
    # difference is inside the template tolerance and is not a second defect.
    pipeline_recipe = {
        "template": "tsd_key_set_pipeline",
        "inputs": {"values": [None]},
        "parameters": {"dedup_size": False},
    }
    ref_tick = {
        "$map": [
            ["size", 4],
            ["total", -1],
            ["variance", {"$float": "0x1.caaaaaaaaaaabp+3"}],
        ]
    }
    cand_tick = {
        "$map": [
            ["total", -1],
            ["variance", {"$float": "0x1.caaaaaaaaaaaap+3"}],
        ]
    }
    difference = compare_outcomes(ok([ref_tick]), ok([cand_tick]))
    assert difference.to_dict()["classification"] == "length"
    assert difference.to_dict()["path"].startswith("$.trace")
    assert is_known_family_failure(
        pipeline_recipe,
        difference.to_dict(),
        ok([ref_tick]),
        ok([cand_tick]),
        families,
    )
    missing_payload = ok([{"$map": [["size", 4]]}])
    difference = compare_outcomes(ok([ref_tick]), missing_payload)
    assert not is_known_family_failure(
        pipeline_recipe,
        difference.to_dict(),
        ok([ref_tick]),
        missing_payload,
        families,
    )

    # 3. A different subscription key samples normally; a repeated key is
    # same-cycle in hg_cpp and delayed one cycle in released hgraph. Payload
    # values and every other tick must remain equal.
    subscription_recipe = {
        "template": "service_subscription",
        "inputs": {"symbol": ["rates", None, "long_symbol", "rates"]},
        "parameters": {"multiplier": 10, "path": "live"},
    }
    reference = ok([None, 50, None, None, 50])
    candidate = ok([None, 50, None, 50])
    difference = compare_outcomes(reference, candidate)
    assert is_known_family_failure(
        subscription_recipe,
        difference.to_dict(),
        reference,
        candidate,
        families,
    )
    corrupted = ok([None, 50, None, 51])
    difference = compare_outcomes(reference, corrupted)
    assert not is_known_family_failure(
        subscription_recipe,
        difference.to_dict(),
        reference,
        corrupted,
        families,
    )
    first_subscriptions_only = {
        **subscription_recipe,
        "inputs": {"symbol": ["rates", None, "long_symbol"]},
    }
    assert not is_known_family_failure(
        first_subscriptions_only,
        difference.to_dict(),
        reference,
        corrupted,
        families,
    )

    # 4. The non-identity reduce ruling covers a value difference only. A
    # length difference in the same parameter space remains reportable.
    reduce_recipe = {
        "template": "tsd_map_reduce",
        "inputs": {"values": [None]},
        "parameters": {"zero": 2},
    }
    difference = compare_outcomes(ok([4]), ok([2]))
    assert is_known_family_failure(
        reduce_recipe,
        difference.to_dict(),
        ok([4]),
        ok([2]),
        families,
    )
    difference = compare_outcomes(ok([4]), ok([2, 2]))
    assert not is_known_family_failure(
        reduce_recipe,
        difference.to_dict(),
        ok([4]),
        ok([2, 2]),
        families,
    )

    # A candidate crash in the same parameter space is NOT the deviation.
    crash = {"status": "error", "phase": "runtime"}
    difference = compare_outcomes(ok([2]), crash)
    assert not is_known_family_failure(
        adaptor_recipe, difference.to_dict(), ok([2]), crash, families
    )


def test_publisher_suppresses_stale_resubscription_variants(monkeypatch):
    # Symbols and path deliberately differ from the exact issue-66 corpus
    # fingerprint. The input-aware relation must still prevent a stale report
    # from creating or reopening the documented deviation.
    recipe = {
        "schema_version": 1,
        "id": "stale-resubscription-variant",
        "description": "Stale report with a different repeated symbol.",
        "template": "service_subscription",
        "inputs": {"symbol": ["fx", None, "long_symbol", "fx"]},
        "parameters": {"multiplier": 0, "path": "quotes"},
        "features": [],
    }
    reference = {"status": "ok", "trace": [None, 0, None, None, 0]}
    candidate = {"status": "ok", "trace": [None, 0, None, 0]}
    failure = {
        "minimized_recipe": recipe,
        "difference": compare_outcomes(reference, candidate).to_dict(),
        "reference": reference,
        "candidate": candidate,
        "reduction": {"attempts": 0, "accepted": 0},
    }
    failure["failure_fingerprint"] = failure_fingerprint(failure)
    assert failure["failure_fingerprint"] != (
        "6d05f5590cc2ff1d6ff45fa63f5189771924d30b9df7d754569979a3d2718b0b"
    )

    calls = []
    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: []
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append(arguments)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)
    actions = publish_failures([failure], repo="hhenson/hg_cpp", publish=True)
    assert actions == [
        {
            "action": "known-divergence",
            "fingerprint": failure["failure_fingerprint"],
        }
    ]
    assert not any(
        arguments[:2] in (["issue", "create"], ["issue", "reopen"])
        for arguments in calls
    )


def test_resample_relation_permits_subset_delays(monkeypatch):
    # Issue #71: only the repeats that actually sample an existing value
    # delay; inserting a cycle for EVERY repeat is too rigid. Uses the
    # committed known_divergences.json family records.
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
    )

    _fingerprints, families = load_known_divergences()
    ok = lambda trace: {"status": "ok", "trace": trace}

    def classify(symbols, reference_trace, candidate_trace):
        recipe = {
            "template": "service_subscription",
            "inputs": {"symbol": symbols},
            "parameters": {"multiplier": 7, "path": "live"},
        }
        reference, candidate = ok(reference_trace), ok(candidate_trace)
        difference = compare_outcomes(reference, candidate)
        assert difference is not None
        return is_known_family_failure(
            recipe, difference.to_dict(), reference, candidate, families
        )

    # The representative stale case: fx and long_symbol both repeat, but
    # only the final long_symbol repeat emits the existing value.
    representative_symbols = [
        "rates", None, "fx", None, None, "long_symbol", None, "fx", "long_symbol",
    ]
    representative_reference = [None, 35, None, 14, None, None, 77, None, None, 77]
    representative_candidate = [None, 35, None, 14, None, None, 77, None, 77]
    assert classify(
        representative_symbols,
        representative_reference,
        representative_candidate,
    )

    # More than one emitting repeat aligns.
    assert classify(
        ["fx", None, "fx", None, "fx"],
        [None, 14, None, 14, None, 14],
        [None, 14, 14, None, 14],
    )

    # A first-subscription difference remains reportable (no repeats).
    assert not classify(
        ["rates", None, "fx"],
        [None, 35, None, 14],
        [None, 35, None, 15],
    )

    # Payload corruption at the aligned repeat remains reportable.
    assert not classify(
        representative_symbols,
        representative_reference,
        [None, 35, None, 14, None, None, 77, None, 78],
    )

    # An unrelated missing tick (not at an emitting repeat) remains
    # reportable.
    assert not classify(
        representative_symbols,
        representative_reference,
        [None, 35, None, None, None, 77, None, 77],
    )

    # The representative case flows through the publisher as a known
    # mismatch: no create, no reopen.
    recipe = {
        "schema_version": 1,
        "id": "stale-partial-delay-variant",
        "description": "Stale report where only one of two repeats emits.",
        "template": "service_subscription",
        "inputs": {"symbol": representative_symbols},
        "parameters": {"multiplier": 7, "path": "live"},
        "features": [],
    }
    reference = ok(representative_reference)
    candidate = ok(representative_candidate)
    failure = {
        "minimized_recipe": recipe,
        "difference": compare_outcomes(reference, candidate).to_dict(),
        "reference": reference,
        "candidate": candidate,
        "reduction": {"attempts": 0, "accepted": 0},
    }
    failure["failure_fingerprint"] = failure_fingerprint(failure)

    calls = []
    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: []
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append(arguments)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)
    actions = publish_failures([failure], repo="hhenson/hg_cpp", publish=True)
    assert actions == [
        {
            "action": "known-divergence",
            "fingerprint": failure["failure_fingerprint"],
        }
    ]
    assert not any(
        arguments[:2] in (["issue", "create"], ["issue", "reopen"])
        for arguments in calls
    )


def test_mesh_empty_initial_fingerprint_is_pinned_and_suppressed(monkeypatch):
    # 3. mesh_key_set minimized to one initial empty map: released hgraph
    # ticks an empty map, hg_cpp emits no tick (no-change ruling). The
    # committed fingerprint must equal the one computed from the real
    # comparator outcome so stale reports can never re-file it.
    from tools.parity.known import load_known_divergences

    fingerprints, _families = load_known_divergences()
    recipe = Recipe.load(CORPUS / "mesh-empty-initial-no-tick.json")
    reference = {"status": "ok", "trace": [{"$map": []}]}
    candidate = {"status": "ok", "trace": None}
    difference = compare_outcomes(reference, candidate)
    failure = {
        "minimized_recipe": recipe.to_dict(),
        "difference": difference.to_dict(),
        "reference": reference,
        "candidate": candidate,
        "reduction": {"attempts": 0, "accepted": 0},
    }
    fingerprint = failure_fingerprint(failure)
    assert fingerprint == (
        "7834fe8ecb1c87ca79cd6ef4229a20376622857e8b1b7b56b6a76d02e4613182"
    )
    assert fingerprint in fingerprints

    calls = []
    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: []
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append(arguments)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)
    actions = publish_failures([failure], repo="hhenson/hg_cpp", publish=True)
    assert actions == [
        {"action": "known-divergence", "fingerprint": fingerprint}
    ]
    assert not any(arguments[:2] == ["issue", "create"] for arguments in calls)


def test_generated_key_set_recipes_avoid_ruled_no_tick_space():
    # The generator stays out of the ruled no-change space: dedup_size is
    # always true, the initial map is never empty, and no delta nets to no
    # entries (mesh_key_set inherits the same inputs).
    pytest.importorskip("hypothesis")
    from tools.parity.generate import generate_recipes

    recipes = generate_recipes(480, seed=29)
    keyed = [
        r
        for r in recipes
        if r.template in ("tsd_key_set_pipeline", "mesh_key_set")
    ]
    assert keyed, "expected generated key-set recipes"
    for recipe in keyed:
        if recipe.template == "tsd_key_set_pipeline":
            assert recipe.parameters["dedup_size"] is True
        values = recipe.inputs["values"]
        assert values[0]["$map"], "initial map must be non-empty"
        for tick in values:
            if tick is not None:
                assert tick["$map"], "no delta may net to no entries"


def test_empty_family_parameter_map_matches_the_whole_template():
    from tools.parity.known import (
        is_known_family_failure,
        matches_known_family,
    )

    families = [
        {
            "family": "whole-template",
            "template": "service_adaptor_roundtrip",
            "parameters_not_equal": {},
        }
    ]
    assert matches_known_family(
        {"template": "service_adaptor_roundtrip", "parameters": {"increment": 3}},
        families,
    )
    assert not matches_known_family(
        {"template": "service_subscription", "parameters": {}}, families
    )
    difference = {
        "classification": "value",
        "path": "$.trace[0]",
        "reference": 1,
        "candidate": 2,
    }
    assert not is_known_family_failure(
        {"template": "service_adaptor_roundtrip", "parameters": {}},
        difference,
        {"status": "ok", "trace": [1]},
        {"status": "ok", "trace": [2]},
        [{**families[0], "relation": "not-a-supported-relation"}],
    )


def test_generated_subscription_recipes_never_resubscribe():
    # Re-subscribing a previously computed symbol is the designed same-cycle
    # sampling deviation (issue #66): the generator must not explore it.
    pytest.importorskip("hypothesis")
    from tools.parity.generate import generate_recipes

    recipes = generate_recipes(240, seed=29)
    subs = [r for r in recipes if r.template == "service_subscription"]
    assert subs, "expected generated service_subscription recipes"
    for recipe in subs:
        symbols = [s for s in recipe.inputs["symbol"] if s is not None]
        assert len(symbols) == len(set(symbols))


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


def test_generator_covers_the_2026_07_compat_issue_classes():
    # The nightly generator must keep producing recipes in the spaces where
    # the 2026-07 compatibility issues lived: temporal accessors (#82),
    # collection sizes (#81), lifecycle signature spellings (#79), the
    # recorded-frame surface (PR #92), and postponed annotations (#83).
    from tools.parity.generate import generate_recipes

    templates = set()
    postponed = False
    for recipe in generate_recipes(480, seed=29):
        templates.add(recipe.template)
        postponed = postponed or recipe.parameters.get(
            "postponed_annotations", False)
    assert {"temporal_expression", "collection_size", "lifecycle_state",
            "data_frame_recording"} <= templates
    assert postponed


def test_coverage_corpus_recipes_execute_on_the_candidate():
    from tools.parity.runner import run_recipe

    for name in (
        "coverage-temporal-accessors",
        "coverage-collection-sizes",
        "coverage-lifecycle-spellings",
        "coverage-frame-recording",
        "coverage-postponed-annotations",
    ):
        raw = json.loads(
            (CORPUS / f"{name}.json").read_text(encoding="utf-8"))
        result = run_recipe(raw)
        assert result["status"] == "ok", (name, result)
    # The recorded-frame surface reports column timezone presentation: the
    # naive-UTC user boundary must hold (a tz-aware column is a trace diff).
    raw = json.loads(
        (CORPUS / "coverage-frame-recording.json").read_text(encoding="utf-8"))
    frame = dict(run_recipe(raw)["trace"]["$map"])["frame"]
    columns = dict(frame["$map"])["columns"]
    assert all(dict(column["$map"])["tz"] is None for column in columns)


def test_no_change_elision_relation_bounds_the_ruled_deviation():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
    )

    _fingerprints, families = load_known_divergences()
    ok = lambda trace: {"status": "ok", "trace": trace}
    recipe = {
        "template": "collection_size",
        "inputs": {"ts": [None]},
        "parameters": {"shape": "tss", "operation": "len"},
    }

    def classify(reference, candidate):
        difference = compare_outcomes(ok(reference), ok(candidate))
        assert difference is not None
        return is_known_family_failure(
            recipe, difference.to_dict(), ok(reference), ok(candidate),
            families)

    # The ruled shape: upstream re-ticks the unchanged size, hg_cpp elides.
    assert classify([2, 2, 3], [2, None, 3])
    # A changed value is NOT elidable.
    assert not classify([2, 3, 3], [2, None, 3])
    assert not classify([2, 2, 3], [2, None, 4])
    # An extra candidate tick is reportable.
    assert not classify([2, None, 3], [2, 2, 3])
    # A first-tick difference is reportable (nothing emitted yet).
    assert not classify([2, 3], [None, 3])
    # Length differences remain reportable.
    difference = compare_outcomes(ok([2, 2]), ok([2]))
    assert not is_known_family_failure(
        recipe, difference.to_dict(), ok([2, 2]), ok([2]), families)


def test_new_template_validators_reject_malformed_recipes():
    def rejects(raw, match):
        with pytest.raises(RecipeError, match=match):
            validate_recipe(Recipe.from_dict({
                "schema_version": 1,
                "id": "generated-validator-check",
                "description": "validator check",
                **raw,
            }))

    rejects(
        {
            "template": "temporal_expression",
            "inputs": {"lhs": [None]},
            "parameters": {
                "input_type": "date", "target": "input", "accessor": "hour",
            },
        },
        "accessor",
    )
    rejects(
        {
            "template": "collection_size",
            "inputs": {"a": [1], "b": [2]},
            "parameters": {"shape": "tsl", "operation": "contains"},
        },
        "tsl covers len only",
    )
    rejects(
        {
            "template": "lifecycle_state",
            "inputs": {"value": [1]},
            "parameters": {"start_spelling": "banana"},
        },
        "start_spelling",
    )
    rejects(
        {
            "template": "data_frame_recording",
            "inputs": {"ts": [1]},
            "parameters": {"as_of_offset": 0},
        },
        "as_of_offset",
    )
