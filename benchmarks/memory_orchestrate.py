"""Orchestrate comparative process and C++-first structural memory profiles.

Each RSS sample is a fresh subprocess. Fixed-release results are cached until
the host, hgraph version, profile pack, psutil version, or sample policy
changes. The native GraphDiagnostics pass is intentionally separate from RSS.
"""
import argparse
import copy
import datetime as dt
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent
sys.path.insert(0, str(BENCH_DIR))
sys.path.insert(0, str(REPO_ROOT))

import memory_profiles
import orchestrate as performance
import scenarios

RUNNER = BENCH_DIR / "memory_runner.py"
RESULTS_DIR = BENCH_DIR / "results"
MODES = performance.MODES
DEFAULT_MODES = performance.DEFAULT_MODES
BASELINE_MODES = performance.BASELINE_MODES
MODE_LABELS = {
    **performance.MODE_LABELS,
    "upstream-cpp": "hgraph C++",
}
BASELINE_CACHE_SCHEMA = 1
BASELINE_CACHE = RESULTS_DIR / f"memory-baseline-{performance.ENVIRONMENT_KEY}.json"
PACK_INPUTS = (
    Path(__file__).resolve(),
    BENCH_DIR / "memory_profiles.py",
    BENCH_DIR / "scenarios.py",
    RUNNER,
)
MEMORY_FIELDS = (
    "process_start_rss_mb",
    "process_start_uss_mb",
    "ready_rss_mb",
    "ready_uss_mb",
    "ready_pss_mb",
    "runtime_load_increment_mb",
    "pre_run_rss_mb",
    "pre_run_uss_mb",
    "run_peak_rss_mb",
    "peak_increment_mb",
    "post_run_rss_mb",
    "post_run_uss_mb",
    "post_gc_rss_mb",
    "post_gc_uss_mb",
    "retained_increment_mb",
    "retained_uss_increment_mb",
    "repeat_growth_mb",
    "repeat_uss_growth_mb",
    "seconds",
    "node_runtime_types_growth",
    "graph_programs_growth",
    "graph_runtime_types_growth",
    "executor_runtime_types_growth",
    "type_records_growth",
)


def memory_pack_fingerprint() -> str:
    digest = hashlib.sha256()
    for path in PACK_INPUTS:
        digest.update(path.relative_to(REPO_ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _dependency_version(executable: Path | str, package: str) -> str:
    proc = subprocess.run(
        [str(executable), "-c", f"import {package}; print({package}.__version__)"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return proc.stdout.strip() if proc.returncode == 0 else "missing"


def ensure_psutil(executable: Path | str) -> str:
    version = _dependency_version(executable, "psutil")
    if version != "missing":
        return version
    print(f"[setup] installing psutil into {executable} ...")
    subprocess.run(
        ["uv", "pip", "install", "--python", str(executable), "psutil>=6"],
        check=True,
        cwd=REPO_ROOT,
    )
    version = _dependency_version(executable, "psutil")
    if version == "missing":
        raise RuntimeError(f"psutil installation failed for {executable}")
    return version


def setup_modes(modes: list[str]) -> dict[str, str]:
    if any(mode.startswith("upstream") for mode in modes):
        performance.ensure_upstream_venv()
    if "hg-cpp" in modes:
        performance.ensure_hg_cpp_venv()
    if "release" in modes:
        performance.ensure_release_venv()
    versions = {}
    for mode in modes:
        executable, _ = performance.mode_invocation(mode)
        versions[mode] = ensure_psutil(executable)
    return versions


def baseline_identity(samples: int, interval_ms: float, scale: float,
                      psutil_versions: dict[str, str]) -> dict:
    return {
        "schema": BASELINE_CACHE_SCHEMA,
        "environment": performance.ENVIRONMENT_KEY,
        "cpu": performance._cpu_model(),
        "hgraph_versions": {
            "upstream-py": performance.REFERENCE_HGRAPH_VERSION,
            "upstream-cpp": performance.REFERENCE_HGRAPH_VERSION,
            "release": performance.FIXED_RELEASE_HGRAPH_VERSION,
        },
        "reference_artifact": performance.reference_artifact()["sha256"],
        "fixed_release_artifact": performance.fixed_release_artifact()["sha256"],
        "memory_pack": memory_pack_fingerprint(),
        "samples": samples,
        "sampling_interval_ms": interval_ms,
        "scale": scale,
        "psutil": psutil_versions.get(
            "release", next(iter(psutil_versions.values()))
        ),
    }


def load_baseline_cache(path: Path, identity: dict) -> dict:
    return performance.load_baseline_cache(path, identity)


def save_baseline_cache(path: Path, identity: dict, results: dict) -> None:
    performance.save_baseline_cache(path, identity, results)


def cached_baseline_result(cache: dict, profile: str, mode: str) -> dict | None:
    return performance.cached_baseline_result(cache, profile, mode)


def run_one(mode: str, profile: str, measurement: str, interval_ms: float,
            timeout: int) -> dict:
    executable, extra_env = performance.mode_invocation(mode)
    env = os.environ.copy()
    env.pop("HGRAPH_USE_CPP", None)
    env.update(extra_env)
    cmd = [
        executable,
        str(RUNNER),
        "--profile", profile,
        "--measurement", measurement,
        "--sampling-interval-ms", str(interval_ms),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=BENCH_DIR,
        )
    except subprocess.TimeoutExpired:
        return {"profile": profile, "ok": False,
                "error": f"timeout after {timeout}s"}
    for line in proc.stdout.splitlines():
        if line.startswith("@@RESULT@@"):
            return performance.sanitize_public_artifact(
                json.loads(line[len("@@RESULT@@"):])
            )
    return performance.sanitize_public_artifact({
        "profile": profile,
        "ok": False,
        "error": (
            f"no result line (exit {proc.returncode})\n"
            f"stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-2000:]}"
        ),
    })


def aggregate_samples(samples: list[dict]) -> dict:
    failures = [sample for sample in samples if not sample.get("ok")]
    if failures:
        result = dict(samples[0])
        result.update(
            ok=False,
            error="\n".join(
                f"sample {index + 1}: {sample.get('error', 'unknown failure')}"
                for index, sample in enumerate(samples)
                if not sample.get("ok")
            ),
            samples=samples,
        )
        return result

    result = dict(samples[0])
    for field in MEMORY_FIELDS:
        values = [sample.get(field) for sample in samples]
        values = [value for value in values if value is not None]
        if not values:
            result[field] = None
            continue
        median = statistics.median(values)
        result[field] = median
        result[f"{field}_mad"] = statistics.median(
            abs(value - median) for value in values
        )
        result[f"{field}_min"] = min(values)
        result[f"{field}_max"] = max(values)
    result["sample_count"] = len(samples)
    result["samples"] = samples
    return result


def _cell(value, mad=None) -> str:
    if value is None:
        return "N/A"
    rendered = f"{value:.1f}"
    if mad is not None:
        rendered += f" +/- {mad:.1f}"
    return rendered


def render(results: dict, inspector: dict, samples: int, interval_ms: float,
           metadata: dict[str, str] | None = None,
           inspector_mode: str | None = None) -> str:
    metadata = metadata or performance.benchmark_metadata()
    display_modes = [
        mode for mode in MODES
        if any(mode in per_mode for per_mode in results.values())
    ]
    comparison_mode = (
        "hg-cpp" if "hg-cpp" in display_modes
        else "release" if "release" in display_modes else None
    )
    ratio_modes = [
        mode for mode in display_modes
        if comparison_mode is not None
        and mode != comparison_mode
        and mode in BASELINE_MODES
    ]
    reused = sum(
        bool(value.get("baseline_reused"))
        for per_mode in results.values()
        for value in per_mode.values()
    )
    lines = [
        "# hgraph memory-utilisation matrix",
        "",
        f"- date: {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}",
        f"- host: {platform.platform()} / {platform.processor() or platform.machine()}",
        f"- CPU: {metadata['cpu']}",
        f"- Python: {metadata['python']}",
        *(
            [f"- reference baseline: hgraph "
             f"{performance.REFERENCE_HGRAPH_VERSION} (published wheel)",
             f"- reference wheel: "
             f"{metadata.get('reference_wheel', performance.reference_artifact()['filename'])}",
             f"- reference SHA-256: "
             f"{metadata.get('reference_sha256', performance.reference_artifact()['sha256'])}"]
            if any(mode.startswith("upstream") for mode in display_modes) else []
        ),
        *(
            [f"- fixed release baseline: hgraph "
             f"{performance.FIXED_RELEASE_HGRAPH_VERSION} (published wheel)",
             f"- fixed release wheel: "
             f"{metadata.get('fixed_release_wheel', performance.fixed_release_artifact()['filename'])}",
             f"- fixed release SHA-256: "
             f"{metadata.get('fixed_release_sha256', performance.fixed_release_artifact()['sha256'])}"]
            if "release" in display_modes else []
        ),
        *(
            [f"- current-source revision: {metadata['revision']}",
             f"- current-source fingerprint: {metadata['source_fingerprint']}"]
            if "hg-cpp" in display_modes else []
        ),
        f"- fresh-process samples: {samples}",
        f"- RSS sampling interval: {interval_ms:g} ms",
        "- modes: " + ", ".join(
            performance.report_mode_label(mode) for mode in display_modes
        ),
        f"- reused fixed baseline cells: {reused}",
        "",
        "RSS values are medians in MiB; +/- is median absolute deviation. "
        "Peak delta is measured from the post-import/pre-run process state. "
        "Retained delta is measured after graph teardown and two Python GC passes.",
        "GraphDiagnostics columns are a separate C++-first run and are "
        "native-accounted bytes, not RSS; they are intentionally absent from "
        "reference modes.",
    ]
    lines += ["", "## Process floor", ""]
    lines += [
        "| mode | interpreter + psutil RSS | ready RSS | runtime load delta |",
        "|---|---:|---:|---:|",
    ]
    for mode in display_modes:
        values = [
            value for per_mode in results.values()
            if (value := per_mode.get(mode)) is not None and value.get("ok")
        ]
        if not values:
            continue
        process_floor = statistics.median(
            value["process_start_rss_mb"] for value in values
        )
        ready = statistics.median(value["ready_rss_mb"] for value in values)
        load_delta = statistics.median(
            value["runtime_load_increment_mb"] for value in values
        )
        lines.append(
            f"| {performance.report_mode_label(mode)} | {process_floor:.1f} | "
            f"{ready:.1f} | {load_delta:.1f} |"
        )
    current_group = None
    for profile_id, per_mode in results.items():
        profile = memory_profiles.PROFILES[profile_id]
        if profile.group != current_group:
            current_group = profile.group
            peak_headers = " | ".join(
                f"{MODE_LABELS[mode]} peak delta" for mode in display_modes
            )
            retained_headers = " | ".join(
                f"{MODE_LABELS[mode]} retained" for mode in display_modes
            )
            repeat_headers = (
                " | " + " | ".join(
                    f"{MODE_LABELS[mode]} first-to-last growth"
                    for mode in display_modes
                )
                if current_group == "Process lifetime" else ""
            )
            ratio_header = (
                " | " + " | ".join(
                    f"{MODE_LABELS[comparison_mode]}/{MODE_LABELS[mode]}"
                    for mode in ratio_modes
                )
                if ratio_modes else ""
            )
            numeric_columns = (
                len(display_modes) * (3 if repeat_headers else 2)
                + 2 + len(ratio_modes)
            )
            lines += [
                "", f"## {current_group}", "",
                f"| profile | axis | {peak_headers}{ratio_header} | "
                f"{retained_headers}{repeat_headers} | planned KiB | "
                "peak dynamic KiB |",
                "|---|---|" + "---:|" * numeric_columns,
            ]
        peaks = {
            mode: (
                per_mode[mode].get("peak_increment_mb")
                if per_mode.get(mode, {}).get("ok") else None
            )
            for mode in display_modes
        }
        peak_cells = [
            _cell(peaks[mode], per_mode.get(mode, {}).get("peak_increment_mb_mad"))
            for mode in display_modes
        ]
        retained_cells = [
            _cell(
                per_mode.get(mode, {}).get("retained_increment_mb")
                if per_mode.get(mode, {}).get("ok") else None,
                per_mode.get(mode, {}).get("retained_increment_mb_mad"),
            )
            for mode in display_modes
        ]
        repeat_cells = (
            [
                _cell(
                    per_mode.get(mode, {}).get("repeat_growth_mb")
                    if per_mode.get(mode, {}).get("ok") else None,
                    per_mode.get(mode, {}).get("repeat_growth_mb_mad"),
                )
                for mode in display_modes
            ]
            if current_group == "Process lifetime" else []
        )
        ratio_cells = []
        for ratio_mode in ratio_modes:
            baseline_peak = peaks[ratio_mode]
            candidate_peak = peaks[comparison_mode]
            ratio_cells.append(
                f"{candidate_peak / baseline_peak:.2f}x"
                if baseline_peak is not None and baseline_peak > 0
                and candidate_peak is not None else "N/A"
            )
        structural = inspector.get(profile_id, {})
        planned = structural.get("planned_bytes")
        dynamic = structural.get("peak_dynamic_reserved_bytes")
        lines.append(
            f"| {profile.label} (`{profile_id}`) | {profile.growth_axis} | "
            + " | ".join(
                peak_cells + ratio_cells + retained_cells + repeat_cells
            )
            + " | "
            f"{_cell(planned / 1024 if planned is not None else None)} | "
            f"{_cell(dynamic / 1024 if dynamic is not None else None)} |"
        )

    registry_rows = []
    for profile_id, per_mode in results.items():
        candidate = per_mode.get(comparison_mode, {})
        if candidate.get("node_runtime_types_growth") is None:
            continue
        registry_rows.append((profile_id, candidate))
    if registry_rows:
        lines += [
            "", f"## {MODE_LABELS[comparison_mode]} retained runtime registry growth", "",
            "Counts are final-minus-pre-run cold-path cardinalities. They are "
            "process-lifetime structural records, not live graph instances.", "",
            "| profile | node types | graph programs | graph types | executor types | all type records |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for profile_id, candidate in registry_rows:
            cells = [
                _cell(
                    candidate.get(f"{field}_growth"),
                    candidate.get(f"{field}_growth_mad"),
                )
                for field in (
                    "node_runtime_types",
                    "graph_programs",
                    "graph_runtime_types",
                    "executor_runtime_types",
                    "type_records",
                )
            ]
            lines.append(f"| `{profile_id}` | " + " | ".join(cells) + " |")

    lines += ["", "## Interpretation contract", ""]
    for profile_id in results:
        profile = memory_profiles.PROFILES[profile_id]
        lines.append(f"- `{profile_id}`: {profile.expectation}.")

    failures = []
    for profile_id, per_mode in results.items():
        for mode, value in per_mode.items():
            if not value.get("ok") and not value.get("skipped"):
                failures.append((profile_id, mode, value.get("error", "unknown")))
        structural = inspector.get(profile_id)
        if structural is not None and not structural.get("ok"):
            failures.append((profile_id, "inspector", structural.get("error", "unknown")))
    if failures:
        lines += ["", "## Failures", ""]
        for profile_id, mode, error in failures:
            lines += [f"### {profile_id} / {mode}", "", "```", error[-2500:], "```", ""]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action="append",
                        help="restrict to stable profile ID(s)")
    parser.add_argument("--group", action="append",
                        help="restrict to exact memory profile group")
    parser.add_argument(
        "--mode", action="append", choices=MODES,
        help="default: fixed hgraph "
             f"{performance.FIXED_RELEASE_HGRAPH_VERSION} and current source")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--sampling-interval-ms", type=float, default=5.0)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--baseline-cache", type=performance.result_path_argument,
                        default=BASELINE_CACHE)
    parser.add_argument("--refresh-baseline", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--skip-inspector", action="store_true")
    parser.add_argument("--setup-only", action="store_true")
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be at least 1")
    if args.sampling_interval_ms <= 0:
        parser.error("--sampling-interval-ms must be positive")

    names = list(memory_profiles.PROFILES)
    if args.profile:
        unknown = sorted(set(args.profile) - set(names))
        if unknown:
            parser.error(f"unknown profile(s): {', '.join(unknown)}")
        names = args.profile
    if args.group:
        groups = set(args.group)
        names = [
            name for name in names
            if memory_profiles.PROFILES[name].group in groups
        ]
    if not names:
        parser.error("profile filters selected no workloads")

    modes = args.mode or list(DEFAULT_MODES)
    psutil_versions = setup_modes(modes)
    if args.setup_only:
        return 0

    runnable_modes = {
        mode for mode in modes
        if any(
            mode in scenarios.SCENARIOS[memory_profiles.PROFILES[name].scenario].modes
            for name in names
        )
    }
    if not args.skip_validation:
        for mode in modes:
            if mode in runnable_modes:
                print(f"[validate] {mode} ...", end="", flush=True)
                performance.validate_mode(mode)
                print(" ok")

    selected_baselines = [mode for mode in modes if mode in BASELINE_MODES]
    cache_identity = None
    cache = {}
    if selected_baselines:
        cache_identity = baseline_identity(
            args.samples,
            args.sampling_interval_ms,
            1.0,
            {
                mode: psutil_versions[mode]
                for mode in selected_baselines
            },
        )
        cache = load_baseline_cache(args.baseline_cache, cache_identity)

    metadata = performance.benchmark_metadata()
    results: dict[str, dict[str, dict]] = {}
    cache_changed = False
    for profile_index, name in enumerate(names):
        profile = memory_profiles.PROFILES[name]
        scenario = scenarios.SCENARIOS[profile.scenario]
        results[name] = {}
        active_modes = []
        for mode in modes:
            if mode not in scenario.modes:
                results[name][mode] = {"profile": name, "skipped": True,
                                      "reason": "unsupported runtime"}
                continue
            cached = None
            if mode in BASELINE_MODES and not args.refresh_baseline:
                cached = cached_baseline_result(cache, name, mode)
            if cached is not None:
                results[name][mode] = cached
            else:
                active_modes.append(mode)

        collected = {mode: [] for mode in active_modes}
        for sample_index in range(args.samples):
            if active_modes:
                offset = (profile_index + sample_index) % len(active_modes)
                ordered = active_modes[offset:] + active_modes[:offset]
            else:
                ordered = []
            for mode in ordered:
                print(
                    f"[rss] {name} / {mode} / {sample_index + 1}/{args.samples} ...",
                    end="", flush=True,
                )
                value = run_one(
                    mode, name, "process", args.sampling_interval_ms, args.timeout
                )
                collected[mode].append(value)
                print(
                    f" {value.get('peak_increment_mb', 'FAIL')}"
                    f"{' MiB' if value.get('ok') else ''}"
                )
        for mode, values in collected.items():
            aggregate = aggregate_samples(values)
            aggregate["benchmark_metadata"] = metadata
            results[name][mode] = aggregate
            if mode in BASELINE_MODES and aggregate.get("ok"):
                stored = copy.deepcopy(aggregate)
                stored.pop("benchmark_metadata", None)
                stored.pop("baseline_reused", None)
                cache.setdefault(name, {})[mode] = stored
                cache_changed = True

    if cache_changed:
        save_baseline_cache(args.baseline_cache, cache_identity, cache)
        print(f"[baseline] updated {args.baseline_cache}")

    inspector = {}
    inspector_mode = (
        "hg-cpp" if "hg-cpp" in modes
        else "release" if "release" in modes else None
    )
    if inspector_mode is not None and not args.skip_inspector:
        for name in names:
            scenario = scenarios.SCENARIOS[memory_profiles.PROFILES[name].scenario]
            if inspector_mode not in scenario.modes:
                continue
            print(f"[inspector] {name} ...", end="", flush=True)
            value = run_one(
                inspector_mode, name, "inspector", args.sampling_interval_ms,
                args.timeout
            )
            inspector[name] = value
            print(
                f" {value.get('peak_dynamic_reserved_bytes', 'FAIL')}"
                f"{' bytes' if value.get('ok') else ''}"
            )

    payload = {
        "schema": 1,
        "metadata": metadata,
        "samples": args.samples,
        "sampling_interval_ms": args.sampling_interval_ms,
        "results": results,
        "inspector": inspector,
        "inspector_mode": inspector_mode,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    raw_path = RESULTS_DIR / f"memory-raw-{stamp}.json"
    report_path = RESULTS_DIR / f"memory-matrix-{stamp}.md"
    raw_path.write_text(
        json.dumps(performance.sanitize_public_artifact(payload), indent=2) + "\n"
    )
    report = render(results, inspector, args.samples,
                    args.sampling_interval_ms, metadata, inspector_mode)
    report_path.write_text(report)
    print(f"\n{report}\nwritten: {report_path}\nraw: {raw_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
