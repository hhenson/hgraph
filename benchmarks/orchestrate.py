"""Benchmark orchestrator — compares fixed hgraph releases with the
C++-first candidate and renders the performance matrix.

Modes:
  upstream-cpp  same package with HGRAPH_USE_CPP=true (the old C++ runtime)
  release       published C++-first hgraph 0.8.19 release
  hg-cpp        this repository's package, from the CURRENT interpreter's env
  upstream-py   optional pinned hgraph 0.5 Python runtime reference

Usage (from the repo root, inside the repo's env):
  uv run python benchmarks/orchestrate.py                 # C++ comparison
  uv run python benchmarks/orchestrate.py --scale 0.1     # quick pass
  uv run python benchmarks/orchestrate.py --mode upstream-py  # on demand
  uv run python benchmarks/orchestrate.py --mode release      # fixed 0.8.19
  uv run python benchmarks/orchestrate.py --scenario tick_std --mode hg-cpp
  uv run python benchmarks/orchestrate.py --setup-only    # just build venvs

The upstream venv is created once per Python major/minor, platform, and
architecture at benchmarks/.venv-upstream-X.Y-PLATFORM-ARCH (delete it to
force a package refresh). Successful upstream timings are cached until the
installed hgraph version, scenario pack, host, scales, or sample count changes.
Results land in benchmarks/results/.
"""
import argparse
import copy
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shlex
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath, PureWindowsPath

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent
sys.path.insert(0, str(REPO_ROOT))

from tools.artifact_fingerprint import hgraph_source_fingerprint as _source_fingerprint

ENVIRONMENT_KEY = (
    f"{sys.version_info.major}.{sys.version_info.minor}-"
    f"{sys.platform}-{platform.machine().lower()}"
)
UPSTREAM_VENV = BENCH_DIR / f".venv-upstream-{ENVIRONMENT_KEY}"
HG_CPP_VENV = BENCH_DIR / f".venv-hg-cpp-{ENVIRONMENT_KEY}"
REFERENCE_HGRAPH_VERSION = "0.5.41"
FIXED_RELEASE_HGRAPH_VERSION = "0.8.19"
UPSTREAM_ARTIFACT_FILE = UPSTREAM_VENV / ".artifact-sha256"
RELEASE_VENV = BENCH_DIR / (
    f".venv-release-{FIXED_RELEASE_HGRAPH_VERSION}-{ENVIRONMENT_KEY}"
)
RELEASE_ARTIFACT_FILE = RELEASE_VENV / ".artifact-sha256"
REFERENCE_ARTIFACTS = {
    ("darwin", "arm64"): {
        "filename": "hgraph-0.5.41-cp312-abi3-macosx_15_0_arm64.whl",
        "url": "https://files.pythonhosted.org/packages/b6/1c/a5bbc64ae4b749c91cd02662d4213702ec057d2cb6d240e373b1396cb8f4/hgraph-0.5.41-cp312-abi3-macosx_15_0_arm64.whl",
        "sha256": "872bd8f07fcec148317786be517ba7c73bc8c6023de50a4fe88cc68c0ae1eef1",
    },
    ("linux", "x86_64"): {
        "filename": "hgraph-0.5.41-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
        "url": "https://files.pythonhosted.org/packages/98/e1/efac47f201136234dd356ff19a994e95973cb6341fc293cff1c1f8dbe0f3/hgraph-0.5.41-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
        "sha256": "c24da699910c3eb44019a38a0fb293557ec707b48a8e8ab5b3e5fd8b0be2db7d",
    },
    ("win32", "x86_64"): {
        "filename": "hgraph-0.5.41-cp312-abi3-win_amd64.whl",
        "url": "https://files.pythonhosted.org/packages/31/35/2f743be8a9ee0e236169b202fabd69aeaf61292429138bc30cfaea1f2d04/hgraph-0.5.41-cp312-abi3-win_amd64.whl",
        "sha256": "74deabc55a4e5a93f3d5234ff828d499c51344924fdac303303abe8b80b224f8",
    },
}
FIXED_RELEASE_ARTIFACTS = {
    ("darwin", "arm64"): {
        "filename": "hgraph-0.8.19-cp312-abi3-macosx_15_0_arm64.whl",
        "url": "https://files.pythonhosted.org/packages/65/40/7b795751040baacf773d72e0910524a1b9079064d9d6108ec193e36006e0/hgraph-0.8.19-cp312-abi3-macosx_15_0_arm64.whl",
        "sha256": "e7c4f19920a45ce9da0d4e4c479af2fd258e4f55b0f2de0215e5b105548629d1",
    },
    ("linux", "x86_64"): {
        "filename": "hgraph-0.8.19-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
        "url": "https://files.pythonhosted.org/packages/2b/db/d67d52280c3b090843401c7e0815a7a1aefb15eb3bfda4f734df7fa0d232/hgraph-0.8.19-cp312-abi3-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl",
        "sha256": "3c58610039211b0a9965727c4a940da64664188d20ebad6711a02bc700670637",
    },
    ("win32", "x86_64"): {
        "filename": "hgraph-0.8.19-cp312-abi3-win_amd64.whl",
        "url": "https://files.pythonhosted.org/packages/1d/24/007d221a370178ebd82bfe23dccbcd350eda437595037a88581b34a49fa6/hgraph-0.8.19-cp312-abi3-win_amd64.whl",
        "sha256": "b072b49300aa2bb744372da81e070be13846b8a5ccb03cbbb0be2c2108ffb377",
    },
}
RESULTS_DIR = BENCH_DIR / "results"
RUNNER = BENCH_DIR / "runner.py"
VALIDATOR = BENCH_DIR / "validate.py"
HG_CPP_FINGERPRINT_FILE = HG_CPP_VENV / ".source-fingerprint"

MODES = ("upstream-py", "upstream-cpp", "release", "hg-cpp")
DEFAULT_MODES = ("release", "hg-cpp")
BASELINE_MODES = ("upstream-py", "upstream-cpp", "release")
MODE_LABELS = {
    "upstream-py": "Python",
    "upstream-cpp": "legacy C++",
    "release": f"hgraph {FIXED_RELEASE_HGRAPH_VERSION}",
    "hg-cpp": "current source",
}
BASELINE_CACHE_SCHEMA = 1
BASELINE_CACHE = RESULTS_DIR / f"baseline-{ENVIRONMENT_KEY}.json"
BASELINE_INPUTS = (
    Path(__file__).resolve(),
    BENCH_DIR / "scenarios.py",
    RUNNER,
    VALIDATOR,
)


def _sanitize_local_paths(value: str) -> str:
    """Replace developer-local roots while preserving useful diagnostics."""
    replacements = (
        (str(REPO_ROOT), "<repo>"),
        (str(Path.home()), "<home>"),
    )
    sanitized = value
    for root, marker in replacements:
        if len(root) <= 1:
            continue
        variants = {root, root.replace("\\", "/"), root.replace("/", "\\")}
        for variant in sorted(variants, key=len, reverse=True):
            sanitized = sanitized.replace(variant, marker)
    return sanitized


def _portable_native_module(value: str) -> str:
    sanitized = _sanitize_local_paths(value).replace("\\", "/")
    if sanitized.startswith(("<repo>/", "<home>/")) or not sanitized:
        return sanitized
    path = PureWindowsPath(value) if "\\" in value else PurePosixPath(value)
    return path.name if path.is_absolute() else sanitized


def sanitize_public_artifact(value, field: str | None = None):
    """Remove private filesystem roots from cache and result payloads."""
    if isinstance(value, dict):
        return {
            key: sanitize_public_artifact(item, key)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_public_artifact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_public_artifact(item) for item in value)
    if isinstance(value, str):
        if field == "native_module":
            return _portable_native_module(value)
        return _sanitize_local_paths(value)
    return value


def upstream_python() -> Path:
    return UPSTREAM_VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def hg_cpp_python() -> Path:
    return HG_CPP_VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def release_python() -> Path:
    return RELEASE_VENV / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _platform_artifact(artifacts: dict, version: str) -> dict[str, str]:
    machine = platform.machine().lower()
    if machine in {"amd64", "x64"}:
        machine = "x86_64"
    key = (sys.platform, machine)
    try:
        return artifacts[key]
    except KeyError as exc:
        raise RuntimeError(
            f"hgraph {version} has no pinned benchmark "
            f"wheel for {sys.platform}/{machine}"
        ) from exc


def reference_artifact() -> dict[str, str]:
    return _platform_artifact(REFERENCE_ARTIFACTS, REFERENCE_HGRAPH_VERSION)


def fixed_release_artifact() -> dict[str, str]:
    return _platform_artifact(
        FIXED_RELEASE_ARTIFACTS, FIXED_RELEASE_HGRAPH_VERSION
    )


def hg_cpp_source_fingerprint() -> str:
    return _source_fingerprint(REPO_ROOT)


def benchmark_pack_fingerprint() -> str:
    digest = hashlib.sha256()
    for path in BASELINE_INPUTS:
        digest.update(path.relative_to(REPO_ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _first_line(
    command: list[str], *, nonzero_stderr_pattern: re.Pattern[str] | None = None
) -> str:
    try:
        completed = subprocess.run(
            command, check=False, capture_output=True, text=True, cwd=REPO_ROOT,
        )
    except OSError:
        return "unknown"
    if completed.returncode != 0:
        output = completed.stderr.strip()
    else:
        output = completed.stdout.strip() or completed.stderr.strip()
    first_line = output.splitlines()[0] if output else "unknown"
    if completed.returncode != 0 and (
        nonzero_stderr_pattern is None
        or nonzero_stderr_pattern.fullmatch(first_line) is None
    ):
        return "unknown"
    return first_line


def _compiler_version(command: list[str]) -> str:
    # MSVC identifies itself on stderr and returns 2 for ``cl --version``.
    is_msvc = any(
        PureWindowsPath(part).name.lower() in {"cl", "cl.exe"}
        for part in command[:-1]
    )
    msvc_banner = re.compile(
        r"Microsoft \(R\) C/C\+\+ Optimizing Compiler Version "
        r"\d+(?:\.\d+)+(?: for .+)?"
    )
    return _first_line(
        command, nonzero_stderr_pattern=msvc_banner if is_msvc else None
    )


def _cpu_model() -> str:
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    return line.partition(":")[2].strip()
        except OSError:
            pass
    if sys.platform == "darwin":
        model = _first_line(["sysctl", "-n", "machdep.cpu.brand_string"])
        if model != "unknown":
            return model
    return platform.processor() or platform.machine()


def benchmark_metadata() -> dict[str, str]:
    revision = _first_line(["git", "rev-parse", "--short=12", "HEAD"])
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            check=True, capture_output=True, text=True, cwd=REPO_ROOT,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        status = "unknown"
    if status.strip():
        revision += "+dirty"

    compiler = shlex.split(os.environ.get("CXX", "c++"))
    metadata = {
        "revision": revision,
        "source_fingerprint": hg_cpp_source_fingerprint(),
        "build_type": "Release",
        "compiler": _compiler_version([*compiler, "--version"]),
        "python": platform.python_version(),
        "cpu": _cpu_model(),
    }
    try:
        artifact = fixed_release_artifact()
        reference = reference_artifact()
    except RuntimeError:
        return metadata
    metadata.update(
        fixed_release_wheel=artifact["filename"],
        fixed_release_sha256=artifact["sha256"],
        reference_wheel=reference["filename"],
        reference_sha256=reference["sha256"],
    )
    return metadata


def ensure_upstream_venv() -> None:
    artifact = reference_artifact()
    if upstream_python().exists():
        installed = installed_hgraph_version(upstream_python())
        recorded_sha256 = (
            UPSTREAM_ARTIFACT_FILE.read_text().strip()
            if UPSTREAM_ARTIFACT_FILE.exists() else ""
        )
        if (
            installed == REFERENCE_HGRAPH_VERSION
            and recorded_sha256 == artifact["sha256"]
        ):
            return
        print(
            f"[setup] replacing hgraph {installed} in the upstream venv with "
            f"hgraph=={REFERENCE_HGRAPH_VERSION}..."
        )
    else:
        print(
            f"[setup] creating upstream venv at {UPSTREAM_VENV} "
            f"(pip install hgraph=={REFERENCE_HGRAPH_VERSION})..."
        )
        subprocess.run(
            ["uv", "venv", "--python", sys.executable, str(UPSTREAM_VENV)],
            check=True,
        )
    subprocess.run(
        [
            "uv", "pip", "install", "--python", str(upstream_python()),
            "--reinstall", f"{artifact['url']}#sha256={artifact['sha256']}",
        ],
        check=True,
    )
    installed = installed_hgraph_version(upstream_python())
    if installed != REFERENCE_HGRAPH_VERSION:
        raise RuntimeError(
            f"reference environment contains hgraph {installed}, expected "
            f"{REFERENCE_HGRAPH_VERSION}"
        )
    UPSTREAM_ARTIFACT_FILE.write_text(artifact["sha256"] + "\n")


def upstream_hgraph_version() -> str:
    return installed_hgraph_version(upstream_python())


def installed_hgraph_version(executable: Path | str) -> str:
    version = _first_line([
        str(executable),
        "-c",
        "from importlib.metadata import version; print(version('hgraph'))",
    ])
    if version == "unknown":
        raise RuntimeError(
            "could not determine the upstream hgraph version for the "
            "benchmark baseline"
        )
    return version


def ensure_release_venv() -> None:
    artifact = fixed_release_artifact()
    if release_python().exists():
        installed = installed_hgraph_version(release_python())
        recorded_sha256 = (
            RELEASE_ARTIFACT_FILE.read_text().strip()
            if RELEASE_ARTIFACT_FILE.exists() else ""
        )
        if (
            installed == FIXED_RELEASE_HGRAPH_VERSION
            and recorded_sha256 == artifact["sha256"]
        ):
            return
        print(
            f"[setup] replacing hgraph {installed} in the fixed release venv "
            f"with hgraph=={FIXED_RELEASE_HGRAPH_VERSION}..."
        )
    else:
        print(
            f"[setup] creating fixed release venv at {RELEASE_VENV} "
            f"(pip install hgraph=={FIXED_RELEASE_HGRAPH_VERSION})..."
        )
        subprocess.run(
            ["uv", "venv", "--python", sys.executable, str(RELEASE_VENV)],
            check=True,
        )
    subprocess.run(
        [
            "uv", "pip", "install", "--python", str(release_python()),
            "--reinstall", f"{artifact['url']}#sha256={artifact['sha256']}",
        ],
        check=True,
    )
    installed = installed_hgraph_version(release_python())
    if installed != FIXED_RELEASE_HGRAPH_VERSION:
        raise RuntimeError(
            f"fixed release environment contains hgraph {installed}, expected "
            f"{FIXED_RELEASE_HGRAPH_VERSION}"
        )
    RELEASE_ARTIFACT_FILE.write_text(artifact["sha256"] + "\n")


def mode_hgraph_version(mode: str) -> str:
    executable, _ = mode_invocation(mode)
    return installed_hgraph_version(executable)


def baseline_identity(
    cycle_scale: float,
    size_scale: float,
    samples: int,
    modes: tuple[str, ...] | list[str] = BASELINE_MODES,
) -> dict:
    return {
        "schema": BASELINE_CACHE_SCHEMA,
        "environment": ENVIRONMENT_KEY,
        "cpu": _cpu_model(),
        "hgraph_versions": {
            "upstream-py": REFERENCE_HGRAPH_VERSION,
            "upstream-cpp": REFERENCE_HGRAPH_VERSION,
            "release": FIXED_RELEASE_HGRAPH_VERSION,
        },
        "fixed_release_artifact": fixed_release_artifact()["sha256"],
        "reference_artifact": reference_artifact()["sha256"],
        "benchmark_pack": benchmark_pack_fingerprint(),
        "cycle_scale": cycle_scale,
        "size_scale": size_scale,
        "samples": samples,
    }


def _validated_result_path(path: Path) -> Path:
    """Resolve a benchmark artifact path confined beneath ``RESULTS_DIR``."""
    resolved = path.expanduser().resolve()
    results_root = RESULTS_DIR.resolve()
    try:
        resolved.relative_to(results_root)
    except ValueError as exc:
        raise ValueError(
            f"benchmark result path must be inside {results_root}: {path}"
        ) from exc
    return resolved


def result_path_argument(value: str) -> Path:
    try:
        return _validated_result_path(Path(value))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def load_baseline_cache(path: Path, identity: dict) -> dict:
    path = _validated_result_path(path)
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if payload.get("identity") != identity:
        return {}
    results = payload.get("results")
    return sanitize_public_artifact(results) if isinstance(results, dict) else {}


def save_baseline_cache(path: Path, identity: dict, results: dict) -> None:
    path = _validated_result_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "identity": identity,
        "results": results,
    }
    path.write_text(json.dumps(sanitize_public_artifact(payload), indent=2) + "\n")


def cached_baseline_result(
    cache: dict,
    scenario: str,
    mode: str,
) -> dict | None:
    result = cache.get(scenario, {}).get(mode)
    if not isinstance(result, dict) or not result.get("ok"):
        return None
    reused = copy.deepcopy(result)
    reused.pop("benchmark_metadata", None)
    reused["baseline_reused"] = True
    return reused


def ensure_hg_cpp_venv() -> str:
    fingerprint = hg_cpp_source_fingerprint()
    if (hg_cpp_python().exists() and HG_CPP_FINGERPRINT_FILE.exists() and
            HG_CPP_FINGERPRINT_FILE.read_text().strip() == fingerprint):
        return fingerprint

    if not hg_cpp_python().exists():
        print(f"[setup] creating hg-cpp benchmark venv at {HG_CPP_VENV}...")
        subprocess.run(["uv", "venv", "--python", sys.executable, str(HG_CPP_VENV)], check=True)

    print(f"[setup] building optimized hg-cpp wheel for source {fingerprint[:12]}...")
    with tempfile.TemporaryDirectory(prefix="hg-cpp-benchmark-wheel-") as wheel_dir:
        subprocess.run(
            [
                "uv", "build", "--wheel", "--python", sys.executable,
                "--config-setting", "cmake.build-type=Release", "--out-dir", wheel_dir,
                "--no-build-logs",
            ],
            check=True,
            cwd=REPO_ROOT,
        )
        wheels = list(Path(wheel_dir).glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one hg-cpp wheel, found {len(wheels)}")
        subprocess.run(
            ["uv", "pip", "install", "--python", str(hg_cpp_python()), "--reinstall", str(wheels[0])],
            check=True,
        )

    HG_CPP_FINGERPRINT_FILE.write_text(fingerprint + "\n")
    return fingerprint


def mode_invocation(mode: str):
    """(python_executable, extra_env) for a mode."""
    if mode == "hg-cpp":
        fingerprint = HG_CPP_FINGERPRINT_FILE.read_text().strip()
        return str(hg_cpp_python()), {"HGRAPH_BENCHMARK_SOURCE_FINGERPRINT": fingerprint}
    if mode == "release":
        artifact = fixed_release_artifact()
        return str(release_python()), {
            "HGRAPH_BENCHMARK_FIXED_RELEASE": FIXED_RELEASE_HGRAPH_VERSION,
            "HGRAPH_BENCHMARK_FIXED_RELEASE_SHA256": artifact["sha256"],
        }
    artifact = reference_artifact()
    env = {
        "HGRAPH_BENCHMARK_REFERENCE": REFERENCE_HGRAPH_VERSION,
        "HGRAPH_BENCHMARK_REFERENCE_SHA256": artifact["sha256"],
    }
    if mode == "upstream-cpp":
        env["HGRAPH_USE_CPP"] = "true"
    return str(upstream_python()), env


def run_one(
    mode: str,
    scenario: str,
    cycle_scale: float,
    size_scale: float,
    timeout: int,
):
    exe, extra_env = mode_invocation(mode)
    env = os.environ.copy()
    env.pop("HGRAPH_USE_CPP", None)
    env.update(extra_env)
    cmd = [
        exe, str(RUNNER), "--scenario", scenario,
        "--cycle-scale", str(cycle_scale),
        "--size-scale", str(size_scale),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, env=env,
            cwd=str(BENCH_DIR),
        )
        for line in proc.stdout.splitlines():
            if line.startswith("@@RESULT@@"):
                return sanitize_public_artifact(
                    json.loads(line[len("@@RESULT@@"):])
                )
        return sanitize_public_artifact({
            "scenario": scenario, "ok": False,
            "error": f"no result line (exit {proc.returncode})\n"
                     f"stdout: {proc.stdout[-1500:]}\nstderr: {proc.stderr[-1500:]}",
        })
    except subprocess.TimeoutExpired:
        return {"scenario": scenario, "ok": False, "error": f"timeout after {timeout}s"}


def aggregate_samples(sample_results: list[dict]) -> dict:
    """Combine fresh-process samples without hiding intermittent failures."""
    failures = [sample for sample in sample_results if not sample.get("ok")]
    if failures:
        errors = [
            f"sample {index + 1}: {sample.get('error', 'unknown failure')}"
            for index, sample in enumerate(sample_results)
            if not sample.get("ok")
        ]
        result = dict(sample_results[0])
        result.update(
            ok=False,
            error="\n".join(errors),
            samples=sample_results,
        )
        return result

    seconds = [sample["seconds"] for sample in sample_results]
    median_seconds = statistics.median(seconds)
    mad_seconds = statistics.median(
        abs(value - median_seconds) for value in seconds
    )
    result = dict(sample_results[0])
    result.update(
        seconds=median_seconds,
        seconds_mad=mad_seconds,
        seconds_min=min(seconds),
        seconds_max=max(seconds),
        sample_count=len(seconds),
        samples=sample_results,
        cycles_per_s=(
            round(result["cycles"] / median_seconds) if median_seconds > 0 else None
        ),
        max_rss_mb=max(sample["max_rss_mb"] for sample in sample_results),
    )
    return result


def validate_mode(mode: str) -> None:
    exe, extra_env = mode_invocation(mode)
    env = os.environ.copy()
    env.pop("HGRAPH_USE_CPP", None)
    env.update(extra_env)
    proc = subprocess.run(
        [exe, str(VALIDATOR)], capture_output=True, text=True, env=env, cwd=str(REPO_ROOT),
    )
    if proc.returncode != 0:
        detail = (proc.stdout + "\n" + proc.stderr).strip()[-4000:]
        raise RuntimeError(f"benchmark workload validation failed for {mode}:\n{detail}")


def render(
    results: dict,
    cycle_scale: float,
    size_scale: float,
    samples: int,
    metadata: dict[str, str] | None = None,
) -> str:
    """results: {scenario: {mode: result_dict}} -> markdown matrix."""
    if metadata is None:
        metadata = benchmark_metadata()
    display_modes = [
        mode for mode in MODES
        if any(mode in per_mode for per_mode in results.values())
    ]
    baseline_mode = (
        "upstream-py"
        if "upstream-py" in display_modes
        else "upstream-cpp" if "upstream-cpp" in display_modes
        else "release" if "release" in display_modes else None
    )
    reused_baselines = sum(
        bool(result.get("baseline_reused"))
        for per_mode in results.values()
        for result in per_mode.values()
    )
    mode_summary = ", ".join(
        f"{MODE_LABELS[mode]} (`{mode}`)" for mode in display_modes
    )
    lines = [
        "# hgraph performance matrix",
        "",
        f"- date: {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}",
        f"- host: {platform.platform()} / {platform.processor() or platform.machine()}",
        f"- CPU: {metadata['cpu']}",
        f"- Python: {metadata['python']}",
        *(
            [f"- reference baseline: hgraph {REFERENCE_HGRAPH_VERSION} "
             "(published wheel)",
             f"- reference wheel: {metadata.get('reference_wheel', reference_artifact()['filename'])}",
             f"- reference SHA-256: {metadata.get('reference_sha256', reference_artifact()['sha256'])}"]
            if any(mode.startswith("upstream") for mode in display_modes) else []
        ),
        *(
            [f"- fixed release baseline: hgraph {FIXED_RELEASE_HGRAPH_VERSION} "
             "(published wheel)",
             f"- fixed release wheel: {metadata.get('fixed_release_wheel', fixed_release_artifact()['filename'])}",
             f"- fixed release SHA-256: {metadata.get('fixed_release_sha256', fixed_release_artifact()['sha256'])}"]
            if "release" in display_modes else []
        ),
        *(
            [f"- current-source compiler: {metadata['compiler']}",
             f"- current-source revision: {metadata['revision']}",
             f"- current-source fingerprint: {metadata['source_fingerprint']}",
             f"- current-source build type: {metadata['build_type']}"]
            if "hg-cpp" in display_modes else []
        ),
        f"- cycle scale: {cycle_scale}",
        f"- size scale: {size_scale}",
        f"- fresh-process samples: {samples}",
        f"- modes: {mode_summary}",
        f"- reused fixed baseline cells: {reused_baselines}",
        "",
        "Median seconds per scenario (lower is better); +/- is median absolute "
        "deviation"
        + (
            f" and xN is speed-up vs {MODE_LABELS[baseline_mode]}."
            if baseline_mode else "."
        ),
        "C++-first-only sections are tracked without a 0.5 comparison.",
    ]
    current_group = None
    group_is_cpp_first_only = False
    for name, per_mode in results.items():
        metadata = next(
            (value for value in per_mode.values() if not value.get("skipped")),
            next(iter(per_mode.values())),
        )
        group = metadata.get("group", "Ungrouped")
        label = metadata.get("label", name)
        if group != current_group:
            current_group = group
            supported_modes = set(metadata.get("supported_modes", ()))
            group_is_cpp_first_only = bool(supported_modes) and not (
                supported_modes & {"upstream-py", "upstream-cpp"}
            )
            lines += ["", f"## {group}", ""]
            if group_is_cpp_first_only:
                lines += [
                    "This section is tracked within C++-first hgraph and is not a "
                    "cross-implementation comparison.",
                    "",
                    f"| workload | cycles | {' | '.join(MODE_LABELS[mode] for mode in display_modes)} |",
                    "|" + "---|" * (2 + len(display_modes)),
                ]
            else:
                headers = " | ".join(
                    MODE_LABELS[mode] for mode in display_modes
                )
                lines += [
                    f"| workload | cycles | {headers} |",
                    "|" + "---|" * (2 + len(display_modes)),
                ]
        row_modes = display_modes
        base = per_mode.get(baseline_mode, {}) if baseline_mode else {}
        base_s = base.get("seconds") if base.get("ok") else None
        cycles = next(
            (r.get("cycles") for r in per_mode.values() if r.get("ok")), "-")
        cells = []
        for mode in row_modes:
            r = per_mode.get(mode)
            if r is None or r.get("skipped"):
                cells.append("N/A")
                continue
            if r.get("ok"):
                s = r["seconds"]
                cell = f"{s:.3f}s"
                if r.get("sample_count", 1) > 1:
                    cell += f" +/- {r['seconds_mad']:.3f}s"
                if base_s and mode != baseline_mode:
                    cell += f" (x{base_s / s:.1f})" if s else ""
                cells.append(cell)
            else:
                cells.append("FAIL")
        lines.append(
            f"| {label} (`{name}`) | {cycles} | "
            + " | ".join(cells)
            + " |"
        )
    failures = [
        (name, mode, r["error"])
        for name, per_mode in results.items()
        for mode, r in per_mode.items()
        if not r.get("ok") and not r.get("skipped")
    ]
    if failures:
        lines += ["", "## Failures", ""]
        for name, mode, err in failures:
            lines += [f"### {name} / {mode}", "", "```", err.strip()[-2000:], "```", ""]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=float,
                        help="legacy shorthand setting both cycle and size scale")
    parser.add_argument("--cycle-scale", type=float,
                        help="scale only the number of engine cycles")
    parser.add_argument("--size-scale", type=float,
                        help="scale graph width, collection size, or client count")
    parser.add_argument("--samples", type=int, default=3,
                        help="fresh-process timing samples per workload/mode")
    parser.add_argument("--scenario", action="append",
                        help="restrict to scenario(s); default all")
    parser.add_argument("--suite", action="append", choices=("core", "diagnostic"),
                        help="select suite(s); default core")
    parser.add_argument("--group", action="append",
                        help="restrict to exact report group name")
    parser.add_argument("--mode", action="append", choices=MODES,
                        help="restrict to mode(s); default fixed 0.8.19 and current source")
    parser.add_argument("--timeout", type=int, default=300,
                        help="per-scenario timeout, seconds")
    parser.add_argument("--baseline-cache", type=result_path_argument,
                        default=BASELINE_CACHE,
                        help="fixed-release timing cache (default: platform-specific)")
    parser.add_argument("--refresh-baseline", action="store_true",
                        help="rerun selected fixed-release modes and refresh their cache")
    parser.add_argument("--setup-only", action="store_true")
    parser.add_argument("--skip-validation", action="store_true",
                        help="skip the cross-runtime workload correctness preflight")
    args = parser.parse_args()

    if args.samples < 1:
        parser.error("--samples must be at least 1")
    cycle_scale = args.cycle_scale if args.cycle_scale is not None else args.scale
    size_scale = args.size_scale if args.size_scale is not None else args.scale
    cycle_scale = 1.0 if cycle_scale is None else cycle_scale
    size_scale = 1.0 if size_scale is None else size_scale

    modes = args.mode or list(DEFAULT_MODES)
    if any(m.startswith("upstream") for m in modes):
        ensure_upstream_venv()
    if "release" in modes:
        ensure_release_venv()
    if "hg-cpp" in modes:
        ensure_hg_cpp_venv()
    if args.setup_only:
        return 0

    sys.path.insert(0, str(BENCH_DIR))
    import scenarios as sc
    if args.scenario:
        unknown = sorted(set(args.scenario) - set(sc.SCENARIOS))
        if unknown:
            parser.error(f"unknown scenario(s): {', '.join(unknown)}")
        names = args.scenario
    else:
        default_suites = ("core", "diagnostic") if args.group else ("core",)
        suites = set(args.suite or default_suites)
        groups = set(args.group or ())
        names = [
            name for name, scenario in sc.SCENARIOS.items()
            if scenario.suite in suites and (not groups or scenario.group in groups)
        ]
    if not names:
        parser.error("scenario filters selected no workloads")

    metadata = benchmark_metadata()
    selected_baseline_modes = [
        mode for mode in modes if mode in BASELINE_MODES
    ]
    baseline_cache_identity = None
    baseline_cache = {}
    if selected_baseline_modes:
        baseline_cache_identity = baseline_identity(
            cycle_scale, size_scale, args.samples, selected_baseline_modes
        )
        baseline_cache = load_baseline_cache(
            args.baseline_cache, baseline_cache_identity
        )

    reusable_baselines = {}
    modes_to_run = set()
    for name in names:
        scenario = sc.SCENARIOS[name]
        for mode in modes:
            if mode not in scenario.modes:
                continue
            cached = None
            if mode in BASELINE_MODES and not args.refresh_baseline:
                cached = cached_baseline_result(baseline_cache, name, mode)
            if cached is not None:
                reusable_baselines[(name, mode)] = cached
            else:
                modes_to_run.add(mode)

    if not args.skip_validation:
        for mode in modes:
            if mode not in modes_to_run:
                print(f"[validate] {mode} ... cached baseline")
                continue
            print(f"[validate] {mode} ...", end="", flush=True)
            validate_mode(mode)
            print(" ok")

    results = {}
    baseline_cache_changed = False
    for scenario_index, name in enumerate(names):
        scenario = sc.SCENARIOS[name]
        results[name] = {}
        active_modes = []
        for mode in modes:
            if mode not in scenario.modes:
                results[name][mode] = {
                    "scenario": name,
                    "group": scenario.group,
                    "label": scenario.label,
                    "suite": scenario.suite,
                    "supported_modes": list(scenario.modes),
                    "skipped": True,
                    "reason": "workload is not supported by this runtime",
                }
            elif (name, mode) in reusable_baselines:
                reused = reusable_baselines[(name, mode)]
                reused["benchmark_metadata"] = metadata
                results[name][mode] = reused
            else:
                active_modes.append(mode)

        collected = {mode: [] for mode in active_modes}
        for sample_index in range(args.samples):
            if active_modes:
                offset = (scenario_index + sample_index) % len(active_modes)
                ordered_modes = active_modes[offset:] + active_modes[:offset]
            else:
                ordered_modes = []
            for mode in ordered_modes:
                print(
                    f"[run] {name} / {mode} / sample "
                    f"{sample_index + 1}/{args.samples} ...",
                    end="", flush=True,
                )
                sample = run_one(
                    mode, name, cycle_scale, size_scale, args.timeout
                )
                collected[mode].append(sample)
                print(
                    f" {sample.get('seconds', 'FAIL')}"
                    f"{'s' if sample.get('ok') else ''}"
                )
        for mode, sample_results in collected.items():
            aggregate = aggregate_samples(sample_results)
            aggregate.setdefault("group", scenario.group)
            aggregate.setdefault("label", scenario.label)
            aggregate.setdefault("suite", scenario.suite)
            aggregate.setdefault("supported_modes", list(scenario.modes))
            aggregate["benchmark_metadata"] = metadata
            results[name][mode] = aggregate
            if mode in BASELINE_MODES and aggregate.get("ok"):
                cached = copy.deepcopy(aggregate)
                cached.pop("baseline_reused", None)
                cached.pop("benchmark_metadata", None)
                baseline_cache.setdefault(name, {})[mode] = cached
                baseline_cache_changed = True

    RESULTS_DIR.mkdir(exist_ok=True)
    if baseline_cache_changed:
        save_baseline_cache(
            args.baseline_cache,
            baseline_cache_identity,
            baseline_cache,
        )
        print(f"[baseline] updated {args.baseline_cache}")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    (RESULTS_DIR / f"raw-{stamp}.json").write_text(
        json.dumps(sanitize_public_artifact(results), indent=2)
    )
    report = render(results, cycle_scale, size_scale, args.samples, metadata)
    report_path = RESULTS_DIR / f"matrix-{stamp}.md"
    report_path.write_text(report)
    print(f"\n{report}\nwritten: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
