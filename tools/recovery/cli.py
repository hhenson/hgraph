"""``python -m tools.recovery``: run a campaign, or replay one scenario from a report."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

# hgraph is imported by the commands that run scenarios, never at load: ``merge`` judges
# shard reports on a machine that has only the reports.
from .profiles import PROFILES

BATCH = 20                      # scenarios per child interpreter
SCENARIO_TIMEOUT = 120.0        # seconds; a hang is a finding, not a reason to lose the night


def _run_batch(args) -> int:
    from .model import Scenario
    from .run import run

    recipes = json.loads(Path(args.recipes).read_text())
    results = []
    for position, recipe in enumerate(recipes):
        result = run(Scenario.from_json(recipe), control=position % args.control_every == 0)
        results.append(result.to_json())
        Path(args.output).write_text(json.dumps(results))        # survive a crash mid-batch
    return 0


def _child(recipes, output: Path, control_every: int, timeout: float):
    recipe_file = output.with_suffix(".recipes.json")
    recipe_file.write_text(json.dumps(recipes))
    command = [sys.executable, "-m", "tools.recovery", "run-batch", str(recipe_file), str(output),
               "--control-every", str(control_every)]
    try:
        completed = subprocess.run(command, timeout=timeout, capture_output=True, text=True)
        failure = None if completed.returncode == 0 else f"exit {completed.returncode}: {completed.stderr[-600:]}"
    except subprocess.TimeoutExpired:
        failure = f"timed out after {timeout:.0f}s"
    done = json.loads(output.read_text()) if output.exists() else []
    return done, failure


def _campaign(args) -> int:
    from . import generate

    started = time.monotonic()
    everything = list(generate.scenarios(args.profile, args.seed))
    mine = generate.shard(everything, args.shard_index, args.shard_count)
    if args.max_scenarios:
        mine = mine[: args.max_scenarios]
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for start in range(0, len(mine), BATCH):
        batch = [scenario.to_json() for scenario in mine[start:start + BATCH]]
        done, failure = _child(batch, output / f"batch-{start:05d}.json", args.control_every,
                               SCENARIO_TIMEOUT * len(batch))
        results.extend(done)
        if failure is None:
            continue
        # The interpreter died or hung part way. Whatever had not reported is run again, one
        # scenario to a process, so the culprit is named and the rest still count.
        for offset, recipe in enumerate(batch[len(done):], start=len(done)):
            single, single_failure = _child([recipe], output / f"single-{start + offset:05d}.json",
                                            1, SCENARIO_TIMEOUT)
            results.extend(single or [{"status": "error", "detail": single_failure or failure,
                                       "seconds": 0.0, "sensitive": None, "scenario": recipe}])
        print(f"  batch at {start}: {failure}", flush=True)

    report = _summarise(results, profile=args.profile, seed=args.seed,
                        shard=[args.shard_index, args.shard_count], generated=len(everything),
                        seconds=round(time.monotonic() - started, 1))
    (output / "report.json").write_text(json.dumps(report, indent=2))
    # One shard is too small a sample to call a family retired or a mode vacuous: that is
    # judged on the totals, by ``merge``. A single unsharded run is its own total.
    problems = _judge(report) if args.shard_count == 1 else list(report["failures"] and ["failures"])
    _print(report, problems)
    return 0 if args.exit_zero or not problems else 1


def _summarise(results, **header) -> dict:
    counts: dict = {}
    modes: dict = {}
    families: dict = {}
    coverage = {"host": {}, "placement": {}, "leaf": {}, "layer": {}, "depth": {}, "cuts": {}}
    for result in results:
        scenario = result["scenario"]
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        mode = modes.setdefault(scenario["mode"], {"run": 0, "pass": 0, "controls": 0, "sensitive": 0})
        mode["run"] += 1
        mode["pass"] += result["status"] == "pass"
        if result.get("sensitive") is not None:
            mode["controls"] += 1
            mode["sensitive"] += bool(result["sensitive"])
        if result.get("family"):
            family = families.setdefault(result["family"], {"members": 0, "known": 0})
            family["members"] += 1
            family["known"] += result["status"] == "known"
        *layers, leaf = scenario["chain"].split("__")
        for kind, value in (("host", scenario["host"]), ("placement", scenario["placement"]), ("leaf", leaf),
                            ("depth", str(len(layers))), ("cuts", (scenario.get("tags") or ["?"])[0]),
                            *(("layer", layer) for layer in sorted(set(layers)))):
            coverage[kind][value] = coverage[kind].get(value, 0) + 1
    return {
        **header, "run": len(results), "counts": counts, "modes": modes, "families": families,
        "coverage": coverage,
        "controls": {"run": sum(mode["controls"] for mode in modes.values()),
                     "sensitive": sum(mode["sensitive"] for mode in modes.values())},
        # "known" is reported, counted and NOT a failure: see model.known_defect.
        "failures": [result for result in results if result["status"] not in ("pass", "known")],
        "known": [result for result in results if result["status"] == "known"],
        "slowest": sorted(({"seconds": result["seconds"], "chain": result["scenario"]["chain"],
                            "host": result["scenario"]["host"]} for result in results),
                          key=lambda item: -item["seconds"])[:10],
    }


def _judge(report) -> list:
    """Why this report is not a pass. Failures first; then the two ways a green campaign lies."""
    problems = []
    if report["failures"]:
        problems.append(f"{len(report['failures'])} scenario(s) failed")
    for name, mode in sorted(report["modes"].items()):
        # A mode whose controls never differ is comparing nothing to nothing.
        if mode["controls"] >= 5 and mode["sensitive"] == 0:
            problems.append(f"VACUOUS: no {name} control differed from the uninterrupted run")
    for name, family in sorted(report["families"].items()):
        # A fix that retires a known defect passes silently unless somebody looks.
        if family["members"] >= 20 and family["known"] == 0:
            problems.append(f"RETIRED: none of the {family['members']} scenarios in known-defect family "
                            f"'{name}' failed; if it is fixed, delete the family from tools/recovery/model.py")
    return problems


def _print(report, problems) -> None:
    shard = report.get("shard")
    where = f" shard {shard[0]}/{shard[1]}" if shard else f" {report.get('shards', 1)} shard(s)"
    print(f"recovery campaign [{report['profile']}] seed {report['seed']}{where}: "
          f"{report['run']} of {report['generated']} scenarios, {report['counts']}, "
          f"controls {report['controls']['sensitive']}/{report['controls']['run']} sensitive, "
          f"{report['seconds']}s")
    for name, mode in sorted(report["modes"].items()):
        print(f"  {name:9} {mode['pass']}/{mode['run']} pass, controls {mode['sensitive']}/{mode['controls']} sensitive")
    for name, family in sorted(report["families"].items()):
        print(f"  known family {name}: {family['known']} failing of {family['members']} members")
    for kind, values in report["coverage"].items():
        print(f"  {kind:9} " + ", ".join(f"{key}={count}" for key, count in sorted(values.items())))
    for failure in report["failures"][:20]:
        scenario = failure["scenario"]
        print(f"  {failure['status'].upper()} {scenario['chain']} [{scenario['mode']}/{scenario['placement']}/"
              f"{scenario['host']}] cuts={scenario['cuts']}: {failure['detail'][:300]}")
    for problem in problems:
        print(f"  ** {problem}")


def _merge(args) -> int:
    """The verdict on a sharded campaign: every shard's results as one report."""
    reports = [json.loads(path.read_text()) for path in sorted(Path(args.directory).glob("**/report.json"))
               if "shard" in json.loads(path.read_text())]
    if not reports:
        print(f"no shard reports under {args.directory}")
        return 1
    expected_shards = reports[0]["shard"][1]
    # A shard report keeps only failures and known results in full; totals are summed, which
    # is all the verdict needs, and the full rows stay beside each shard for a replay.
    merged = {"profile": reports[0]["profile"], "seed": reports[0]["seed"], "shards": len(reports),
              "generated": reports[0]["generated"], "run": 0, "counts": {}, "modes": {}, "families": {},
              "coverage": {}, "controls": {"run": 0, "sensitive": 0}, "failures": [], "known": [],
              "seconds": max(report["seconds"] for report in reports), "slowest": []}

    def add(into, other):
        for key, value in other.items():
            if isinstance(value, dict):
                add(into.setdefault(key, {}), value)
            else:
                into[key] = into.get(key, 0) + value

    for report in reports:
        merged["run"] += report["run"]
        for key in ("counts", "modes", "families", "coverage", "controls"):
            add(merged[key], report[key])
        merged["failures"] += report["failures"]
        merged["known"] += report["known"]
        merged["slowest"] += report["slowest"]
    merged["slowest"] = sorted(merged["slowest"], key=lambda item: -item["seconds"])[:10]
    problems = _judge(merged)
    if len(reports) != expected_shards:
        problems.append(f"{len(reports)} of {expected_shards} shards reported")
    if merged["run"] != merged["generated"] and not args.partial:
        problems.append(f"{merged['run']} of {merged['generated']} scenarios ran")
    merged["problems"] = problems
    Path(args.output).write_text(json.dumps(merged, indent=2))
    _print(merged, problems)
    return 0 if not problems else 1


def _replay(args) -> int:
    from .model import Scenario
    from .run import run

    data = json.loads(Path(args.recipe).read_text())
    recipes = [failure["scenario"] for failure in data["failures"]] if "failures" in data else [data.get("scenario", data)]
    worst = 0
    for recipe in recipes[: args.limit]:
        result = run(Scenario.from_json(recipe), control=True)
        print(json.dumps(result.to_json(), indent=2, default=str))
        worst = max(worst, 0 if result.status == "pass" else 1)
    return worst


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tools.recovery")
    commands = parser.add_subparsers(dest="command", required=True)

    campaign = commands.add_parser("campaign", help="generate and run a seeded set of scenarios")
    campaign.add_argument("--profile", choices=tuple(PROFILES), default="pr")
    campaign.add_argument("--seed", type=int, default=20260919)
    campaign.add_argument("--shard-index", type=int, default=0)
    campaign.add_argument("--shard-count", type=int, default=1)
    campaign.add_argument("--max-scenarios", type=int)
    campaign.add_argument("--control-every", type=int, default=4,
                          help="run the no-recovery control for one scenario in this many")
    campaign.add_argument("--output-dir", default="recovery-results")
    campaign.add_argument("--exit-zero", action="store_true")
    campaign.set_defaults(handler=_campaign)

    replay = commands.add_parser("replay", help="re-run the failures of a report, or one recipe")
    replay.add_argument("recipe")
    replay.add_argument("--limit", type=int, default=5)
    replay.set_defaults(handler=_replay)

    merge = commands.add_parser("merge", help="judge a sharded campaign from its shard reports")
    merge.add_argument("directory")
    merge.add_argument("--output", default="recovery-report.json")
    merge.add_argument("--partial", action="store_true", help="--max-scenarios was used: not every scenario ran")
    merge.set_defaults(handler=_merge)

    batch = commands.add_parser("run-batch")     # internal: one child interpreter
    batch.add_argument("recipes")
    batch.add_argument("output")
    batch.add_argument("--control-every", type=int, default=4)
    batch.set_defaults(handler=_run_batch)

    args = parser.parse_args(argv)
    return args.handler(args)
