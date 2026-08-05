"""Command-line interface for hgraph differential parity testing."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

from .campaign import render_campaign_markdown, run_campaign
from .diagnostics import (
    compare_diagnostics,
    render_diagnostics_markdown,
    run_probe as run_diagnostics_probe,
)
from .surface import (
    SURFACE_EXTRA_DEPENDENCIES,
    classify_findings,
    compare_surfaces,
    load_known_surface,
    probe_surface,
    render_surface_markdown,
)
from .catalog import catalogue_json, validate_recipe
from .compare import compare_outcomes
from .conformance import (
    compare_upstream_results,
    ensure_upstream_source,
    install_conformance_dependencies,
    load_conformance_manifest,
    prepare_test_workspace,
    profile_selectors,
    require_aligned_conformance_environments,
    render_conformance_markdown,
    run_upstream_suite,
    validate_selectors,
)
from .coverage import coverage_json, render_coverage_markdown
from .environments import PARITY_ROOT, prepare_environments
from .generate import generate_recipes
from .issues import publish_failures
from .model import Recipe, RecipeError, load_corpus
from .process import operator_inventory, run_recipe
from .reduce import reduce_recipe


CORPUS = Path(__file__).with_name("corpus")

CAMPAIGN_PROFILES = {
    "pr": {
        "examples": 48,
        "time_budget": 900.0,
        "reduce": False,
        "min_ticks": 8,
        "max_ticks": 32,
        "templates": (
            "scalar_expression",
            "scalar_operator_arguments",
            "feedback_accumulate",
            "switch_arithmetic",
        ),
    },
    "nightly": {
        "examples": 5000,
        "time_budget": 3600.0,
        "reduce": True,
        "min_ticks": 8,
        "max_ticks": 64,
        "templates": None,
    },
}


def _path(value: str | None) -> Path | None:
    # Preserve a virtual environment's bin/python symlink. Resolving it can
    # select the base interpreter on platforms where uv uses symlinked
    # launchers, causing isolated parity subprocesses to lose the environment.
    return Path(value).absolute() if value else None


def _load_selected_recipes(paths: list[str] | None) -> list[Recipe]:
    if not paths:
        return load_corpus(CORPUS)
    recipes: list[Recipe] = []
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            recipes.extend(load_corpus(path))
        else:
            recipes.append(Recipe.load(path))
    return recipes


def _prepare(args) -> object:
    return prepare_environments(
        reference_python=_path(getattr(args, "reference_python", None)),
        candidate_python=_path(getattr(args, "candidate_python", None)),
        candidate_wheel=_path(getattr(args, "candidate_wheel", None)),
    )


def command_validate(args) -> int:
    recipes = _load_selected_recipes(args.paths)
    for recipe in recipes:
        validate_recipe(recipe)
    print(f"validated {len(recipes)} parity recipe(s)")
    return 0


def command_catalogue(args) -> int:
    print(catalogue_json())
    return 0


def command_setup(args) -> int:
    environments = _prepare(args)
    print(
        json.dumps(
            {
                "reference_python": str(environments.reference_python),
                "candidate_python": str(environments.candidate_python),
                "reference_identity": environments.reference_identity,
                "candidate_identity": environments.candidate_identity,
                "candidate_fingerprint": environments.candidate_fingerprint,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def command_replay(args) -> int:
    recipe = Recipe.load(args.recipe)
    validate_recipe(recipe)
    environments = _prepare(args)
    reference = run_recipe(
        environments.reference_python, recipe, timeout=args.timeout
    )
    candidate = run_recipe(
        environments.candidate_python, recipe, timeout=args.timeout
    )
    difference = compare_outcomes(reference, candidate)
    print(
        json.dumps(
            {
                "recipe": recipe.to_dict(),
                "reference": reference,
                "candidate": candidate,
                "difference": difference.to_dict() if difference else None,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if difference else 0


def command_reduce(args) -> int:
    recipe = Recipe.load(args.recipe)
    validate_recipe(recipe)
    environments = _prepare(args)

    def differs(candidate_recipe: Recipe) -> bool:
        reference = run_recipe(
            environments.reference_python,
            candidate_recipe,
            timeout=args.timeout,
        )
        candidate = run_recipe(
            environments.candidate_python,
            candidate_recipe,
            timeout=args.timeout,
        )
        return (
            reference.get("status") == "ok"
            and compare_outcomes(reference, candidate) is not None
        )

    if not differs(recipe):
        raise RecipeError("recipe does not currently reproduce a difference")
    result = reduce_recipe(
        recipe,
        differs,
        time_budget_seconds=args.time_budget,
    )
    output = json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(output)
        print(args.output)
    else:
        print(output, end="")
    return 0


def command_coverage(args) -> int:
    recipes = _load_selected_recipes(args.paths)
    if args.generated:
        recipes.extend(
            generate_recipes(args.generated, seed=args.seed)
        )
    environments = _prepare(args) if args.inventory else None
    inventory = (
        operator_inventory(environments.reference_python)["operators"]
        if environments is not None
        else ()
    )
    from .coverage import coverage_report

    report = coverage_report(recipes, operator_inventory=inventory)
    print(
        render_coverage_markdown(report)
        if args.format == "markdown"
        else coverage_json(report),
        end="",
    )
    return 0


def command_diagnostics(args) -> int:
    environments = _prepare(args)
    reference = run_diagnostics_probe(environments.reference_python)
    candidate = run_diagnostics_probe(environments.candidate_python)
    report = compare_diagnostics(reference, candidate)
    output_dir = Path(args.output_dir) if args.output_dir else PARITY_ROOT / "results" / "diagnostics"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "diagnostics.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (output_dir / "reference-raw.txt").write_text(json.dumps(reference, indent=2))
    (output_dir / "candidate-raw.txt").write_text(json.dumps(candidate, indent=2))
    (output_dir / "diagnostics.md").write_text(render_diagnostics_markdown(report))
    print(render_diagnostics_markdown(report))
    return 0 if not report["gaps"] or args.exit_zero else 1


def command_surface(args) -> int:
    environments = _prepare(args)
    if args.with_extras:
        # One shared dependency list into BOTH environments: adaptor imports
        # become symmetric by construction, so the deep adaptor surfaces are
        # audited instead of skipped.
        for python in (environments.reference_python, environments.candidate_python):
            subprocess.run(
                ["uv", "pip", "install", "--python", str(python),
                 *SURFACE_EXTRA_DEPENDENCIES],
                check=True,
                capture_output=True,
                text=True,
            )
    reference = probe_surface(environments.reference_python)
    candidate = probe_surface(environments.candidate_python)
    report = compare_surfaces(reference, candidate)
    known_path = Path(args.known) if args.known else Path(__file__).with_name("surface_known.json")
    rules = load_known_surface(known_path) if known_path.exists() else []
    actionable, accepted = classify_findings(report["findings"], rules)
    report["actionable"] = actionable
    report["accepted"] = accepted
    output_dir = Path(args.output_dir) if args.output_dir else PARITY_ROOT / "results" / "surface"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "surface.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (output_dir / "surface.md").write_text(render_surface_markdown(report))
    print(render_surface_markdown(report))
    return 0 if not report["actionable"] or args.exit_zero else 1


def _session_report(result: dict) -> dict:
    return {
        key: value
        for key, value in result.items()
        if key not in {"tests", "collected", "collection_errors", "stdout", "stderr"}
    }


def command_conformance(args) -> int:
    environments = _prepare(args)
    manifest_path = (
        Path(args.manifest)
        if args.manifest
        else Path(__file__).with_name("upstream_conformance.json")
    )
    manifest = load_conformance_manifest(manifest_path)
    selectors = validate_selectors(
        args.paths or profile_selectors(manifest, args.profile)
    )
    extras = SURFACE_EXTRA_DEPENDENCIES if args.with_extras else ()
    install_conformance_dependencies(
        (environments.reference_python, environments.candidate_python),
        extras=extras,
    )
    conformance_environment = require_aligned_conformance_environments(
        environments.reference_python,
        environments.candidate_python,
        extras=extras,
    )
    source = ensure_upstream_source(
        environments.reference_identity,
        source_path=_path(args.upstream_source),
    )
    workspace = prepare_test_workspace(source)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_dir = (
        Path(args.output_dir).absolute()
        if args.output_dir
        else PARITY_ROOT / "results" / f"{stamp}-conformance-{args.profile}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    reference = run_upstream_suite(
        environments.reference_python,
        workspace,
        selectors,
        result_path=output_dir / "reference-result.json",
        timeout_seconds=args.timeout,
    )
    candidate = run_upstream_suite(
        environments.candidate_python,
        workspace,
        selectors,
        result_path=output_dir / "candidate-result.json",
        timeout_seconds=args.timeout,
    )
    report = compare_upstream_results(reference, candidate, manifest)
    report.update(
        profile=args.profile,
        selectors=selectors,
        reference_identity=environments.reference_identity,
        candidate_identity=environments.candidate_identity,
        candidate_fingerprint=environments.candidate_fingerprint,
        conformance_environment=conformance_environment,
        source={
            "repository": source.repository,
            "ref": source.ref,
            "revision": source.revision,
            "version": source.version,
            "declared_version": source.declared_version,
            "test_digest": source.test_digest,
        },
        reference_session=_session_report(reference),
        candidate_session=_session_report(candidate),
    )
    (output_dir / "reference-pytest.txt").write_text(
        reference.get("stdout", "") + reference.get("stderr", "")
    )
    (output_dir / "candidate-pytest.txt").write_text(
        candidate.get("stdout", "") + candidate.get("stderr", "")
    )
    (output_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    markdown = render_conformance_markdown(report)
    (output_dir / "report.md").write_text(markdown)
    print(markdown)
    print(f"report: {output_dir / 'report.json'}")
    incomplete = bool(
        report["review_required"]
        or report["confirmed_gaps"]
        or report["ambiguous_rules"]
        or report["reference_unverified"]
        or reference.get("status") != "complete"
        or candidate.get("status") != "complete"
    )
    return 0 if args.exit_zero or not incomplete else 1


def command_campaign(args) -> int:
    profile_defaults = CAMPAIGN_PROFILES[args.profile]
    examples = (
        args.max_examples
        if args.max_examples is not None
        else profile_defaults["examples"]
    )
    time_budget = (
        args.time_budget
        if args.time_budget is not None
        else profile_defaults["time_budget"]
    )
    reduce_failures = (
        args.reduce
        if args.reduce is not None
        else profile_defaults["reduce"]
    )
    min_ticks = (
        args.min_ticks
        if args.min_ticks is not None
        else profile_defaults["min_ticks"]
    )
    max_ticks = (
        args.max_ticks
        if args.max_ticks is not None
        else profile_defaults["max_ticks"]
    )
    recipes = _load_selected_recipes(args.paths)
    if examples:
        recipes.extend(
            generate_recipes(
                examples,
                seed=args.seed,
                min_ticks=min_ticks,
                max_ticks=max_ticks,
                templates=profile_defaults["templates"],
            )
        )
    unique = {recipe.fingerprint: recipe for recipe in recipes}
    recipes = list(unique.values())
    recipes = [
        recipe
        for index, recipe in enumerate(recipes)
        if index % args.shard_count == args.shard_index
    ]
    environments = _prepare(args)
    inventory = operator_inventory(environments.reference_python)["operators"]
    report = run_campaign(
        recipes,
        environments,
        operator_inventory=inventory,
        timeout_seconds=args.timeout,
        time_budget_seconds=time_budget,
        verify_replays=args.verify_replays,
        reduce_failures=reduce_failures,
        reduction_budget_seconds=args.reduction_budget,
    )
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else PARITY_ROOT
        / "results"
        / f"{stamp}-{args.profile}-s{args.shard_index}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    (output_dir / "report.md").write_text(render_campaign_markdown(report))
    (output_dir / "coverage.json").write_text(
        coverage_json(report["coverage"])
    )
    (output_dir / "coverage.md").write_text(
        render_coverage_markdown(report["coverage"])
    )
    print(render_campaign_markdown(report))
    print(f"report: {output_dir / 'report.json'}")
    has_failure = bool(
        report["verified_failures"] or report["quarantined"]
    )
    return 0 if args.exit_zero or not has_failure else 1


def command_publish(args) -> int:
    failures = []
    for path in args.reports:
        report = json.loads(Path(path).read_text())
        failures.extend(report.get("verified_failures", ()))
    actions = publish_failures(
        failures,
        repo=args.repo,
        publish=args.publish,
    )
    print(json.dumps(actions, indent=2, sort_keys=True))
    return 0


def _environment_arguments(parser) -> None:
    parser.add_argument("--reference-python")
    parser.add_argument("--candidate-python")
    parser.add_argument("--candidate-wheel")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hgraph-parity")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate")
    validate.add_argument("paths", nargs="*")
    validate.set_defaults(func=command_validate)

    catalogue = subparsers.add_parser("catalogue")
    catalogue.set_defaults(func=command_catalogue)

    setup = subparsers.add_parser("setup")
    _environment_arguments(setup)
    setup.set_defaults(func=command_setup)

    replay = subparsers.add_parser("replay")
    replay.add_argument("recipe")
    replay.add_argument("--timeout", type=float, default=30.0)
    _environment_arguments(replay)
    replay.set_defaults(func=command_replay)

    reduce_parser = subparsers.add_parser("reduce")
    reduce_parser.add_argument("recipe")
    reduce_parser.add_argument("--timeout", type=float, default=30.0)
    reduce_parser.add_argument("--time-budget", type=float, default=120.0)
    reduce_parser.add_argument("--output")
    _environment_arguments(reduce_parser)
    reduce_parser.set_defaults(func=command_reduce)

    coverage = subparsers.add_parser("coverage")
    coverage.add_argument("paths", nargs="*")
    coverage.add_argument("--generated", type=int, default=0)
    coverage.add_argument("--seed", type=int, default=20260726)
    coverage.add_argument("--inventory", action="store_true")
    coverage.add_argument(
        "--format", choices=("json", "markdown"), default="markdown"
    )
    _environment_arguments(coverage)
    coverage.set_defaults(func=command_coverage)

    campaign = subparsers.add_parser("campaign")
    campaign.add_argument("paths", nargs="*")
    campaign.add_argument("--profile", choices=("pr", "nightly"), default="pr")
    campaign.add_argument("--seed", type=int, default=20260726)
    campaign.add_argument("--max-examples", type=int)
    campaign.add_argument("--min-ticks", type=int)
    campaign.add_argument("--max-ticks", type=int)
    campaign.add_argument("--timeout", type=float, default=30.0)
    campaign.add_argument("--time-budget", type=float)
    campaign.add_argument("--reduction-budget", type=float, default=120.0)
    campaign.add_argument("--verify-replays", type=int, default=3)
    campaign.add_argument(
        "--reduce", action=argparse.BooleanOptionalAction, default=None
    )
    campaign.add_argument("--shard-index", type=int, default=0)
    campaign.add_argument("--shard-count", type=int, default=1)
    campaign.add_argument("--output-dir")
    campaign.add_argument("--exit-zero", action="store_true")
    _environment_arguments(campaign)
    campaign.set_defaults(func=command_campaign)

    diagnostics = subparsers.add_parser("diagnostics")
    diagnostics.add_argument("--output-dir")
    diagnostics.add_argument("--exit-zero", action="store_true")
    _environment_arguments(diagnostics)
    diagnostics.set_defaults(func=command_diagnostics)

    surface = subparsers.add_parser("surface")
    surface.add_argument("--output-dir")
    surface.add_argument("--exit-zero", action="store_true")
    surface.add_argument("--with-extras", action="store_true")
    surface.add_argument("--known")
    _environment_arguments(surface)
    surface.set_defaults(func=command_surface)

    conformance = subparsers.add_parser("conformance")
    conformance.add_argument("paths", nargs="*")
    conformance.add_argument("--profile", default="core")
    conformance.add_argument("--manifest")
    conformance.add_argument("--upstream-source")
    conformance.add_argument("--timeout", type=float, default=3600.0)
    conformance.add_argument("--output-dir")
    conformance.add_argument("--with-extras", action="store_true")
    conformance.add_argument("--exit-zero", action="store_true")
    _environment_arguments(conformance)
    conformance.set_defaults(func=command_conformance)

    publish = subparsers.add_parser("publish-issues")
    publish.add_argument("reports", nargs="+")
    publish.add_argument("--repo", default="hhenson/hg_cpp")
    publish.add_argument("--publish", action="store_true")
    publish.set_defaults(func=command_publish)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "shard_count", 1) < 1:
        parser.error("--shard-count must be at least one")
    if not 0 <= getattr(args, "shard_index", 0) < getattr(
        args, "shard_count", 1
    ):
        parser.error("--shard-index must be in [0, shard-count)")
    if getattr(args, "verify_replays", 2) < 2:
        parser.error("--verify-replays must be at least two")
    try:
        return args.func(args)
    except (RecipeError, RuntimeError, ValueError) as error:
        parser.exit(2, f"error: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
