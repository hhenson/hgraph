"""Tests for the model-free hgraph differential parity controller."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.artifact_fingerprint import hgraph_source_fingerprint
from tools.parity.campaign import run_campaign
from tools.parity.canonical import canonicalize
from tools.parity.catalog import validate_recipe
from tools.parity.cli import CAMPAIGN_PROFILES, _path
from tools.parity.compare import compare_outcomes
from tools.parity.coverage import coverage_report, recipe_features
from tools.parity.environments import ParityEnvironments, prepare_environments
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


def _scalar_operator_recipe(**parameter_updates):
    parameters = {
        "operation": "div",
        "input_type": "int",
        "scalar_type": "int",
        "scalar_side": "rhs",
        "scalar_value": 0,
        "divide_by_zero": "ZERO",
    }
    parameters.update(parameter_updates)
    if parameters.get("divide_by_zero") is None:
        parameters.pop("divide_by_zero", None)
    return Recipe.from_dict(
        {
            "schema_version": 1,
            "id": "test-scalar-operator-recipe",
            "description": "test",
            "template": "scalar_operator_arguments",
            "inputs": {"value": [1, None, -2, 3]},
            "parameters": parameters,
            "features": ["argument:scalar", "shape:TS"],
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


def test_scalar_operator_arguments_validate_reference_safe_public_overloads():
    validate_recipe(_scalar_operator_recipe())
    validate_recipe(_scalar_operator_recipe(
        operation="pow",
        scalar_value=3,
        divide_by_zero="ERROR",
    ))

    with pytest.raises(RecipeError, match="does not accept divide_by_zero"):
        validate_recipe(_scalar_operator_recipe(
            operation="add",
            divide_by_zero="ZERO",
        ))
    with pytest.raises(RecipeError, match="zero divisor requires"):
        validate_recipe(_scalar_operator_recipe(
            operation="floordiv",
            divide_by_zero="ONE",
        ))
    with pytest.raises(RecipeError, match="integer pow requires non-negative"):
        validate_recipe(_scalar_operator_recipe(
            operation="pow",
            scalar_value=-1,
            divide_by_zero="NONE",
        ))
    with pytest.raises(RecipeError, match="comparison operands must have matching"):
        validate_recipe(_scalar_operator_recipe(
            operation="ge",
            scalar_type="float",
            scalar_value=1.0,
            divide_by_zero=None,
        ))


def test_collection_size_rejects_reference_unsupported_string_is_empty():
    recipe = Recipe.from_dict(
        {
            "schema_version": 1,
            "id": "test-invalid-string-is-empty",
            "template": "collection_size",
            "inputs": {"ts": ["", "value"]},
            "parameters": {
                "shape": "str",
                "operation": "is_empty",
                "normalize_output": True,
            },
            "features": ["shape:TS"],
        }
    )
    with pytest.raises(RecipeError, match=r"is_empty.*TS\[str\]"):
        validate_recipe(recipe)


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

    recipes = generate_recipes(640, seed=29)
    templates = {recipe.template for recipe in recipes}
    assert {
        "service_reference",
        "service_request_reply",
        "service_subscription",
        "service_adaptor_roundtrip",
        "adaptor_loopback",
        "context_switch",
        "operator_pipeline",
        "tsd_key_set_pipeline",
        "mesh_key_set",
    } <= templates
    assert {
        recipe.template
        for recipe in generate_recipes(
            8, seed=31, templates=("service_adaptor_roundtrip",),
        )
    } == {"service_adaptor_roundtrip"}
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


def test_generator_covers_scalar_operator_arguments_and_valid_reference_space():
    pytest.importorskip("hypothesis")
    from tools.parity.generate import generate_recipes

    scalar_arguments = generate_recipes(
        256,
        seed=41,
        templates=("scalar_operator_arguments",),
    )
    assert {
        "add", "sub", "mul", "div", "floordiv", "mod", "pow",
        "eq", "ne", "lt", "le", "gt", "ge",
    } == {recipe.parameters["operation"] for recipe in scalar_arguments}
    assert {"lhs", "rhs"} == {
        recipe.parameters["scalar_side"] for recipe in scalar_arguments
    }
    assert {"ERROR", "NAN", "INF", "NONE", "ZERO", "ONE"} == {
        policy
        for recipe in scalar_arguments
        if (policy := recipe.parameters.get("divide_by_zero")) is not None
    }
    assert any(
        recipe.parameters["operation"] in {"div", "floordiv", "mod"}
        and recipe.parameters["scalar_value"] == 0
        and recipe.parameters["scalar_side"] == "rhs"
        for recipe in scalar_arguments
    )
    assert all(
        "argument:scalar" in recipe.features
        for recipe in scalar_arguments
    )
    assert all(
        recipe.parameters["input_type"] == recipe.parameters["scalar_type"]
        for recipe in scalar_arguments
        if recipe.parameters["operation"] in {
            "eq", "ne", "lt", "le", "gt", "ge",
        }
    )

    collection_sizes = generate_recipes(
        256,
        seed=43,
        templates=("collection_size",),
    )
    assert not any(
        recipe.parameters["shape"] == "str"
        and recipe.parameters["operation"] == "is_empty"
        for recipe in collection_sizes
    )


def test_campaign_profiles_scale_nightly_breadth_and_tick_depth():
    assert CAMPAIGN_PROFILES["pr"]["examples"] == 48
    assert "scalar_operator_arguments" in CAMPAIGN_PROFILES["pr"]["templates"]
    assert CAMPAIGN_PROFILES["nightly"]["examples"] == 5000
    assert CAMPAIGN_PROFILES["nightly"]["max_ticks"] == 64
    assert CAMPAIGN_PROFILES["nightly"]["time_budget"] == 3600.0


def test_external_environment_paths_preserve_virtualenv_symlinks(
    monkeypatch, tmp_path
):
    reference = tmp_path / "reference" / "bin" / "python"
    candidate = tmp_path / "candidate" / "bin" / "python"
    reference.parent.mkdir(parents=True)
    candidate.parent.mkdir(parents=True)
    try:
        reference.symlink_to(sys.executable)
        candidate.symlink_to(sys.executable)
    except OSError:
        pytest.skip("gap: interpreter symlinks are unavailable on this platform")
    identities = {
        reference: {"distribution": "hgraph"},
        candidate: {"distribution": "hgraph"},
    }

    monkeypatch.setattr(
        "tools.parity.environments.environment_identity",
        identities.__getitem__,
    )
    environments = prepare_environments(
        reference_python=reference,
        candidate_python=candidate,
    )

    assert _path(str(reference)) == reference
    assert environments.reference_python == reference
    assert environments.candidate_python == candidate


def test_reference_environment_stays_on_the_python_first_0_5_line(
    monkeypatch, tmp_path
):
    import tools.parity.environments as environments

    commands = []
    python = tmp_path / "reference-python"
    monkeypatch.setattr(environments, "PARITY_ROOT", tmp_path)
    monkeypatch.setattr(environments, "_environment_key", lambda _interpreter: "test")
    monkeypatch.setattr(environments, "_ensure_venv", lambda _path, _interpreter: python)
    monkeypatch.setattr(environments, "_run", commands.append)
    monkeypatch.setattr(
        environments,
        "environment_identity",
        lambda _interpreter: {"distribution": "hgraph", "version": "0.5.41"},
    )

    _python, identity = environments.ensure_reference_environment()

    assert _python == python
    assert identity["version"] == "0.5.41"
    assert commands == [
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--upgrade",
            "hgraph==0.5.41",
        ]
    ]


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
        "implementation": {"distribution": "hgraph", "version": "1"},
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
        "implementation": {"distribution": "hgraph", "version": "1"},
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
            candidate_identity={"distribution": "hgraph", "version": "1"},
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
        "implementation": {"distribution": "hgraph", "version": "1"},
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
        "implementation": {"distribution": "hgraph", "version": "1"},
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


def test_generated_recipes_avoid_remaining_accepted_deviation_spaces():
    # Generated discovery must test the agreed contract rather than consume
    # examples rediscovering accepted deviations. Fixed corpus recipes retain
    # each ruled behavior as a permanent regression.
    from tools.parity.generate import generate_recipes

    def generated(template):
        recipes = generate_recipes(
            80, seed=11, templates=(template,),
        )
        assert recipes, f"expected generated {template} recipes"
        return recipes

    # Non-identity reduce zeros have capacity-history-dependent reference
    # behavior. Every generated reduce uses the explicit identity zero.
    assert all(
        recipe.parameters["zero"] == 0
        for recipe in generated("tsd_map_reduce")
    )

    # Equal derived values are normalized at the graph boundary so an
    # operator's value semantics are tested without the ruled re-tick policy.
    assert all(
        recipe.parameters["normalize_output"] is True
        for recipe in generated("collection_size")
    )

    subscriptions = generated("service_subscription")
    assert all(recipe.parameters["multiplier"] != 0 for recipe in subscriptions)
    assert any(
        len(symbols := [
            tick for tick in recipe.inputs["symbol"] if tick is not None
        ]) != len(set(symbols))
        for recipe in subscriptions
    )

    nested = generated("nested_higher_order")
    for recipe in nested:
        parameters = recipe.parameters
        assert parameters["reduce_output"] is True
        assert parameters["normalize_output"] is True
        assert parameters["inner"] in ("arithmetic", "adaptor")
        if parameters["inner"] == "adaptor":
            assert parameters["wrap_switch"] is False

    assert all(
        recipe.parameters["accessor"] in {
            "year", "month", "day", "hour", "minute", "second",
            "microsecond", "days", "seconds", "microseconds",
        }
        for recipe in generated("temporal_expression")
    )


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
        candidate_identity={"distribution": "hgraph"},
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
        [failure], repo="hhenson/hgraph", publish=False
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
        "url": "https://github.com/hhenson/hgraph/issues/44",
    }
    calls = []

    monkeypatch.setattr(
        "tools.parity.issues._existing_issues", lambda _repo: [existing]
    )

    def fake_gh(arguments, *, repo, capture=False):
        calls.append((arguments, repo, capture))
        return SimpleNamespace(
            stdout="https://github.com/hhenson/hgraph/issues/45\n"
        )

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)

    assert publish_failures(
        [failure], repo="hhenson/hgraph", publish=True
    ) == [
        {
            "action": "created",
            "url": "https://github.com/hhenson/hgraph/issues/45",
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
            stdout="https://github.com/hhenson/hgraph/issues/45\n"
        )

    monkeypatch.setattr("tools.parity.issues._gh", fake_gh)

    assert publish_failures(
        [failure, dict(failure), dict(failure)],
        repo="hhenson/hgraph",
        publish=True,
    ) == [
        {
            "action": "created",
            "url": "https://github.com/hhenson/hgraph/issues/45",
            "fingerprint": "repeat-fingerprint",
        },
        {
            "action": "deduplicated",
            "url": "https://github.com/hhenson/hgraph/issues/45",
            "fingerprint": "repeat-fingerprint",
        },
        {
            "action": "deduplicated",
            "url": "https://github.com/hhenson/hgraph/issues/45",
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
        "url": "https://github.com/hhenson/hgraph/issues/99",
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
        repo="hhenson/hgraph",
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
        repo="hhenson/hgraph",
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

    # tsd_key_set_pipeline dedup_size=false: the unchanged size field is
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

    # The non-identity reduce ruling covers a value difference only. A
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
        reduce_recipe, difference.to_dict(), ok([2]), crash, families
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
    actions = publish_failures([failure], repo="hhenson/hgraph", publish=True)
    assert actions == [
        {"action": "known-divergence", "fingerprint": fingerprint}
    ]
    assert not any(arguments[:2] == ["issue", "create"] for arguments in calls)


def test_issue_175_rfc_14_timing_fingerprint_is_pinned():
    from tools.parity.known import load_known_divergences

    fingerprints, _families = load_known_divergences()
    recipe = Recipe.load(
        CORPUS / "issue-175-map-response-delivery-vs-new-key.json"
    )
    mapped = lambda entries: {"$map": entries}
    reference = {
        "status": "ok",
        "trace": [
            mapped([]),
            None,
            mapped([["k1", 8]]),
            None,
            mapped([["k2", -2]]),
        ],
    }
    candidate = {
        "status": "ok",
        "trace": [
            mapped([]),
            mapped([["k1", 8]]),
            mapped([]),
            mapped([["k2", -2]]),
        ],
    }
    difference = compare_outcomes(reference, candidate)
    failure = {
        "minimized_recipe": recipe.to_dict(),
        "difference": difference.to_dict(),
        "reference": reference,
        "candidate": candidate,
    }
    fingerprint = failure_fingerprint(failure)
    assert fingerprint == (
        "70b8f025dfa3f8d634861689a9bdf0ca1ec3c46b0dccb6d8153d340f57e66fa1"
    )
    assert fingerprint in fingerprints


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


def test_valid_subset_reduce_relation_is_narrowly_bounded():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
        matches_known_family,
    )

    _fingerprints, families = load_known_divergences()
    recipe = {
        "template": "nested_higher_order",
        "parameters": {
            "inner": "subscription",
            "outer": "map",
            "wrap_switch": False,
            "reduce_output": True,
            "increment": 2,
        },
    }
    ok = lambda trace: {"status": "ok", "trace": trace}

    # The valid-subset semantics apply only where an in-flight service
    # round trip opens an invalid window: subscription startup (the original
    # #95 shape) and a switch_ flip to a request-reply branch. A pure
    # arithmetic pipeline evaluates same-cycle under sampled semantics, so
    # its extra ticks stay reportable — the families are scoped to the
    # service-backed inners, and the relation stays extra-emissions-only.
    subset_families = [
        f for f in families
        if f["family"] in ("mapped-subscription-valid-subset-reduce",
                           "switch-flip-valid-subset-reduce")
    ]
    assert len(subset_families) == 2
    assert matches_known_family(recipe, subset_families)
    assert matches_known_family(
        {
            **recipe,
            "parameters": {**recipe["parameters"], "inner": "request_reply"},
        },
        subset_families,
    )
    assert not matches_known_family(
        {
            **recipe,
            "parameters": {**recipe["parameters"], "inner": "arithmetic"},
        },
        subset_families,
    )
    assert not matches_known_family(
        {
            **recipe,
            "parameters": {**recipe["parameters"], "reduce_output": False},
        },
        subset_families,
    )

    def classify(reference, candidate):
        difference = compare_outcomes(ok(reference), ok(candidate))
        assert difference is not None
        return is_known_family_failure(
            recipe, difference.to_dict(), ok(reference), ok(candidate), families
        )

    assert classify([None, None, 26, 14], [None, 9, 26, 14])
    assert not classify([None, None, 26, 14], [None, 9, 25, 14])
    assert not classify([None, None, 26, 14], [None, None, None, 14])
    assert not classify([None, None, 26, 14], [None, 9, 26])
    # CATCH-UP composition (issues #98/#102/#110/#150): upstream later
    # emits exactly the value the candidate published early; the candidate
    # elides the equal re-tick. The value must match exactly, and a missed
    # emission with nothing published stays reportable.
    assert classify([None, 0], [0, None])
    assert not classify([None, 7], [0, None])
    assert not classify([None, 0], [None, None])
    # The limiting case (issues #174/#176): released hgraph emitted NOTHING
    # for the whole run (a null trace) — the candidate's early emission is
    # still the ruled extra. A null CANDIDATE trace stays reportable.
    assert classify(None, [0])
    assert not classify([0], None)


def test_switch_flip_valid_subset_relation_is_windowed():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
    )

    _fingerprints, families = load_known_divergences()
    ok = lambda trace: {"status": "ok", "trace": trace}
    # The issue #99 shape: beta at t0, flip to the request-reply alpha at
    # t1, response lands at t3.
    recipe = {
        "template": "nested_higher_order",
        "inputs": {
            "selector": ["beta", "alpha", None, None],
            "values": [{"k1": -1}, None, None, None],
        },
        "parameters": {
            "inner": "request_reply",
            "outer": "map",
            "wrap_switch": False,
            "reduce_output": True,
            "increment": -5,
        },
    }

    def classify(reference, candidate, with_recipe=None):
        difference = compare_outcomes(ok(reference), ok(candidate))
        assert difference is not None
        return is_known_family_failure(
            with_recipe or recipe, difference.to_dict(), ok(reference),
            ok(candidate), families,
        )

    # The flip-cycle valid-subset emission is inside the window.
    assert classify([3, None, None, -6], [3, 0, None, -6])
    # An extra tick AFTER the pipeline settled (no input tick since the
    # last agreeing reference emission) is NOT covered (PR #165 review).
    assert not classify([3, None, None, -6, None], [3, 0, None, -6, 99])
    # Payload mismatches and missing emissions stay reportable everywhere.
    assert not classify([3, None, None, -6], [3, 0, None, -7])
    assert not classify([3, None, None, -6], [3, None, None, None])
    # CATCH-UP composition: upstream's later emission of exactly the
    # candidate's in-window early value is the elided equal re-tick;
    # a different later value stays reportable.
    assert classify([None, 0], [0, None])
    assert not classify([None, 7], [0, None])

    # A spurious tick with the selector PARKED on the arithmetic branch —
    # the t0 emission closes the only window, so nothing later is covered.
    parked = {
        **recipe,
        "inputs": {
            "selector": ["beta", None, None, None],
            "values": [{"k1": -1}, None, None, None],
        },
    }
    assert not classify([3, None, None, None], [3, None, None, 99],
                        with_recipe=parked)


def test_switch_flip_map_removal_relation_is_narrowly_bounded():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
        matches_known_family,
    )

    _fingerprints, families = load_known_divergences()
    removal_families = [
        family
        for family in families
        if family["family"] == "request-reply-switch-map-removal"
    ]
    assert len(removal_families) == 1
    recipe = {
        "template": "nested_higher_order",
        "inputs": {
            "selector": ["beta", "alpha", None, None],
            "values": [{"k1": 1, "k2": 2}, None, None, None],
        },
        "parameters": {
            "inner": "request_reply",
            "outer": "map",
            "wrap_switch": False,
            "reduce_output": False,
            "increment": 3,
        },
    }
    assert matches_known_family(recipe, removal_families)
    assert not matches_known_family(
        {
            **recipe,
            "parameters": {**recipe["parameters"], "reduce_output": True},
        },
        removal_families,
    )

    mapped = lambda entries: {"$map": entries}
    removed = lambda key: [key, {"$remove": True}]
    initial = mapped([["k1", -1], ["k2", 1]])
    response = mapped([["k1", 4], ["k2", 5]])
    reference = [initial, mapped([]), None, response]
    candidate = [
        initial,
        mapped([removed("k1"), removed("k2")]),
        None,
        response,
    ]
    ok = lambda trace: {"status": "ok", "trace": trace}

    def classify(ref, cand, with_recipe=None):
        difference = compare_outcomes(ok(ref), ok(cand))
        assert difference is not None
        return is_known_family_failure(
            with_recipe or recipe,
            difference.to_dict(),
            ok(ref),
            ok(cand),
            removal_families,
        )

    assert classify(reference, candidate)
    earlier_candidate = [initial, candidate[1], response]
    assert classify(reference, earlier_candidate)
    # Only the single silent feedback cycle immediately after the flip may be
    # removed; multiple-cycle shifts and altered earlier responses are defects.
    assert not classify(
        [initial, mapped([]), None, None, response],
        earlier_candidate,
    )
    assert not classify(
        reference,
        [initial, candidate[1], mapped([["k1", 4], ["k2", 6]])],
    )
    assert not classify(
        [initial, mapped([]), mapped([]), response],
        earlier_candidate,
    )
    # The removal must cover exactly every output live before the flip.
    assert not classify(
        reference,
        [initial, mapped([removed("k1")]), None, response],
    )
    # Non-removal payloads and a changed eventual response remain defects.
    assert not classify(
        reference,
        [initial, mapped([["k1", 99], removed("k2")]), None, response],
    )
    assert not classify(
        reference,
        [initial, candidate[1], None, mapped([["k1", 4], ["k2", 6]])],
    )
    # The released-hgraph side must be its canonical empty-map delta.
    assert not classify(
        [initial, None, None, response],
        candidate,
    )
    # The same removal one cycle before the beta-to-alpha flip is unrelated.
    delayed_flip = {
        **recipe,
        "inputs": {
            "selector": ["beta", None, "alpha", None],
            "values": recipe["inputs"]["values"],
        },
    }
    assert not classify(reference, candidate, with_recipe=delayed_flip)
    # Selecting alpha initially is startup, not a branch flip.
    initial_alpha = {
        **recipe,
        "inputs": {
            "selector": [None, "alpha", None, None],
            "values": recipe["inputs"]["values"],
        },
    }
    assert not classify(reference, candidate, with_recipe=initial_alpha)


def test_request_reply_one_cycle_earlier_relation_is_exact():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
        matches_known_family,
    )

    _fingerprints, families = load_known_divergences()
    timing_families = [
        family
        for family in families
        if family["family"] == "self-coupled-request-reply-one-cycle-earlier"
    ]
    assert len(timing_families) == 1
    recipe = {
        "template": "service_request_reply",
        "inputs": {"value": [5, 7, None, 2]},
        "parameters": {"increment": 3, "path": "requests"},
    }
    assert matches_known_family(recipe, timing_families)
    ok = lambda trace: {"status": "ok", "trace": trace}

    def classify(reference, candidate):
        difference = compare_outcomes(ok(reference), ok(candidate))
        assert difference is not None
        return is_known_family_failure(
            recipe,
            difference.to_dict(),
            ok(reference),
            ok(candidate),
            timing_families,
        )

    reference = [None, None, 8, 10, None, 5]
    candidate = [None, 8, 10, None, 5]
    assert classify(reference, candidate)
    # Payload changes, interior-cycle removal, and multiple-cycle advances are
    # not the RFC 0014 timing difference.
    assert not classify(reference, [None, 8, 11, None, 5])
    assert not classify(reference, [None, 8, 10, 5, None])
    assert not classify(reference, [8, 10, None, 5])
    assert not classify(
        [None, None, 8, None, 10, None, 5],
        [None, None, 8, 10, None, 5],
    )
    # A trace without a response is not accepted merely because it is shorter.
    assert not classify([None, None], [None])


def test_nested_request_reply_one_cycle_earlier_preserves_structural_prefix():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
        matches_known_family,
    )

    _fingerprints, families = load_known_divergences()
    timing_family_name = "nested-mapped-request-reply-one-cycle-earlier"
    timing_families = [
        family
        for family in families
        if family["family"] == timing_family_name
    ]
    assert len(timing_families) == 1
    recipe = {
        "template": "nested_higher_order",
        "inputs": {
            "selector": ["alpha"],
            "values": [{"k1": 8}],
        },
        "parameters": {
            "increment": 0,
            "inner": "request_reply",
            "outer": "map",
            "reduce_output": False,
            "wrap_switch": False,
        },
    }
    assert matches_known_family(recipe, timing_families)
    for name, value in (
        ("inner", "arithmetic"),
        ("outer", "mesh"),
        ("reduce_output", True),
        ("wrap_switch", True),
    ):
        assert not matches_known_family(
            {
                **recipe,
                "parameters": {**recipe["parameters"], name: value},
            },
            timing_families,
        )

    mapped = lambda entries: {"$map": entries}
    empty = mapped([])
    first = mapped([["k1", 8]])
    second = mapped([["k2", -2]])
    ok = lambda trace: {"status": "ok", "trace": trace}

    def classify(reference, candidate, *, with_recipe=recipe):
        difference = compare_outcomes(ok(reference), ok(candidate))
        assert difference is not None
        return is_known_family_failure(
            with_recipe,
            difference.to_dict(),
            ok(reference),
            ok(candidate),
            timing_families,
        )

    # Issue #274: reduction removed the later outer-key collision, leaving
    # exactly RFC 0014's one-cycle response advance after a stable map prefix.
    assert classify([empty, None, first], [empty, first])
    # Payload changes, prefix loss, multiple removed cycles, and trailing
    # silence removal remain reportable.
    assert not classify(
        [empty, None, first],
        [empty, mapped([["k1", 9]])],
    )
    assert not classify([empty, None, first], [first])
    assert not classify([empty, None, None, first], [empty, first])
    assert not classify([empty, first, None], [empty, first])
    assert not classify(
        [empty, first, None, second],
        [empty, first, second],
    )
    assert not classify([empty, None, empty], [empty, empty])
    beta_only = {
        **recipe,
        "inputs": {**recipe["inputs"], "selector": ["beta"]},
    }
    assert matches_known_family(beta_only, timing_families)
    assert not classify(
        [empty, None, first],
        [empty, first],
        with_recipe=beta_only,
    )
    beta_at_removal = {
        **recipe,
        "inputs": {**recipe["inputs"], "selector": ["alpha", "beta"]},
    }
    assert not classify(
        [empty, None, first],
        [empty, first],
        with_recipe=beta_at_removal,
    )
    # The complete issue-175 collision also changes the map delta at the next
    # outer-key event, so its existing exact fingerprint remains necessary.
    assert not classify(
        [empty, None, first, None, second],
        [empty, first, empty, second],
    )


def test_nested_no_change_retick_family_is_elision_only():
    from tools.parity.known import (
        is_known_family_failure,
        load_known_divergences,
    )

    _fingerprints, families = load_known_divergences()
    # Bound THIS family's relation in isolation — on a reduce_output
    # pipeline the widened mapped-valid-subset-reduce family separately
    # admits extra candidate emissions.
    elision_families = [
        f for f in families if f["family"] == "nested-no-change-retick"
    ]
    assert elision_families
    ok = lambda trace: {"status": "ok", "trace": trace}
    recipe = {
        "template": "nested_higher_order",
        "parameters": {
            "inner": "arithmetic",
            "outer": "map",
            "wrap_switch": True,
            "reduce_output": True,
            "increment": -2,
        },
    }

    def classify(reference, candidate):
        difference = compare_outcomes(ok(reference), ok(candidate))
        assert difference is not None
        return is_known_family_failure(
            recipe, difference.to_dict(), ok(reference), ok(candidate),
            elision_families,
        )

    # The off-branch len_*0 re-emits an equal 0 upstream; hg_cpp elides it.
    assert classify([0, 0], [0, None])
    # A FIRST emission the candidate missed is NOT elision.
    assert not classify([None, 0], [None, None])
    # An extra candidate tick is NOT elision.
    assert not classify([None, None], [None, 0])
    # A changed value is NOT elision.
    assert not classify([0, 0], [0, 1])


def test_generated_subscription_recipes_include_resubscriptions():
    # Re-subscribing a previously computed symbol is part of the parity
    # contract and must remain in randomized discovery.
    pytest.importorskip("hypothesis")
    from tools.parity.generate import generate_recipes

    subs = generate_recipes(
        240,
        seed=29,
        templates=("service_subscription",),
    )
    assert any(
        len(symbols := [s for s in recipe.inputs["symbol"] if s is not None])
        != len(set(symbols))
        for recipe in subs
    )


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
    before = hgraph_source_fingerprint(tmp_path, python_version="3.12")
    (tmp_path / "tools" / "parity" / "recipe.json").write_text(
        json.dumps({"changed": True})
    )
    after = hgraph_source_fingerprint(tmp_path, python_version="3.12")
    assert before == after


def test_generator_covers_the_2026_07_compat_issue_classes():
    # The nightly generator must keep producing recipes in the spaces where
    # the 2026-07 compatibility issues lived: temporal accessors (#82),
    # collection sizes (#81), lifecycle signature spellings (#79), the
    # recorded-frame surface (PR #92), and postponed annotations (#83).
    from hypothesis import find, settings

    from tools.parity.generate import (
        generate_recipes,
        recipe_payload_strategy,
    )

    required_templates = {
        "temporal_expression",
        "collection_size",
        "lifecycle_state",
        "data_frame_recording",
        "nested_higher_order",
    }
    generated_templates = {
        recipe.template
        for template in required_templates
        for recipe in generate_recipes(
            1, seed=29, templates=(template,)
        )
    }
    assert generated_templates == required_templates

    postponed = find(
        recipe_payload_strategy(
            min_ticks=8,
            max_ticks=32,
            templates=("scalar_expression",),
        ),
        lambda payload: payload["parameters"].get(
            "postponed_annotations", False
        ),
        settings=settings(database=None, deadline=None),
    )
    assert postponed["parameters"]["postponed_annotations"]


def test_coverage_corpus_recipes_execute_on_the_candidate():
    from tools.parity.runner import run_recipe

    for name in (
        "coverage-scalar-operator-arguments",
        "regression-integer-pow-result-type",
        "coverage-temporal-accessors",
        "coverage-collection-sizes",
        "coverage-lifecycle-spellings",
        "coverage-frame-recording",
        "coverage-postponed-annotations",
        "coverage-nested-adaptor-pipeline",
        "coverage-nested-outer-switch",
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
    # Malformed temporal tick encodings reject at the trusted boundary
    # (PR #93 review): never deferred to a runtime decode failure.
    rejects(
        {
            "template": "temporal_expression",
            "inputs": {"lhs": [{"$date": 123}]},
            "parameters": {
                "input_type": "date", "target": "input", "accessor": "year",
            },
        },
        "iso-string",
    )
    rejects(
        {
            "template": "temporal_expression",
            "inputs": {"lhs": [{"$datetime": "2026-07-27T12:00:00+02:00"}]},
            "parameters": {
                "input_type": "datetime", "target": "input", "accessor": "hour",
            },
        },
        "must be naive",
    )
    rejects(
        {
            "template": "temporal_expression",
            "inputs": {"lhs": [{"$date": "not-a-date"}]},
            "parameters": {
                "input_type": "date", "target": "input", "accessor": "year",
            },
        },
        "not a valid ISO",
    )
    rejects(
        {
            "template": "temporal_expression",
            "inputs": {"lhs": [{"$datetime": "2026-07-27T12:00:00"}]},
            "parameters": {
                "input_type": "date", "target": "input", "accessor": "year",
            },
        },
        "ticks must be",
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
            "template": "collection_size",
            "inputs": {"ts": ["abc"]},
            "parameters": {
                "shape": "str",
                "operation": "len",
                "normalize_output": "yes",
            },
        },
        "normalize_output must be a boolean",
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
            "template": "lifecycle_state",
            "inputs": {"value": [1]},
            "parameters": {
                "start_spelling": "default",
                "state_access": "sequence",
            },
        },
        "state_access",
    )
    rejects(
        {
            "template": "data_frame_recording",
            "inputs": {"ts": [1]},
            "parameters": {"as_of_offset": 0},
        },
        "as_of_offset",
    )
    rejects(
        {
            "template": "nested_higher_order",
            "inputs": {"values": [{"k1": 1}], "selector": ["alpha"]},
            "parameters": {"inner": "adaptor", "outer": "map",
                           "wrap_switch": False, "reduce_output": False},
        },
        "adaptor inner requires reduce_output",
    )
    rejects(
        {
            "template": "nested_higher_order",
            "inputs": {"values": [{"k1": 1}], "selector": ["alpha"]},
            "parameters": {
                "inner": "arithmetic",
                "outer": "map",
                "wrap_switch": False,
                "reduce_output": False,
                "normalize_output": True,
            },
        },
        "normalize_output requires reduce_output",
    )
    rejects(
        {
            "template": "nested_higher_order",
            "inputs": {"values": [{"k1": 1}, {"k1": {"$remove": True}}, {"k1": 2}],
                       "selector": ["alpha", None, None]},
            "parameters": {"inner": "subscription", "outer": "map",
                           "wrap_switch": False, "reduce_output": True},
        },
        "must not re-add removed keys",
    )
