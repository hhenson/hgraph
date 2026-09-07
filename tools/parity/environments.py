"""Content-addressed isolated environments for differential execution."""

from __future__ import annotations

import hashlib
import os
import platform
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from tools.artifact_fingerprint import hgraph_source_fingerprint

from .process import environment_identity


REPO_ROOT = Path(__file__).resolve().parents[2]
PARITY_ROOT = REPO_ROOT / ".parity"
REFERENCE_HGRAPH_VERSION = "0.5.41"


def _python_in(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _run(command: list[str], *, cwd: Path = REPO_ROOT, env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(command, cwd=cwd, env=env)
    if completed.returncode:
        raise RuntimeError(
            f"parity environment command failed ({completed.returncode}): "
            f"{' '.join(command)}"
        )


def _environment_key(interpreter: Path | str) -> str:
    completed = subprocess.run(
        [
            str(interpreter),
            "-c",
            "import platform,sys;"
            "print(f'{sys.version_info.major}.{sys.version_info.minor}-"
            "{sys.platform}-{platform.machine().lower()}')",
        ],
        capture_output=True,
        check=True,
        text=True,
    )
    return completed.stdout.strip()


def _ensure_venv(path: Path, interpreter: Path | str) -> Path:
    python = _python_in(path)
    if not python.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        command = ["uv", "venv"]
        # Cached virtual environments can retain an interpreter symlink whose
        # hosted-toolcache patch version has been retired. The managed venv
        # directory then exists even though its Python executable does not.
        if path.exists() or path.is_symlink():
            command.extend(["--clear", "--force"])
        command.extend(["--python", str(interpreter), str(path)])
        _run(command)
    return python


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ParityEnvironments:
    reference_python: Path
    candidate_python: Path
    reference_identity: dict
    candidate_identity: dict
    candidate_fingerprint: str


def ensure_reference_environment(
    *,
    interpreter: Path | str = sys.executable,
) -> tuple[Path, dict]:
    key = _environment_key(interpreter)
    venv = PARITY_ROOT / "envs" / f"reference-{key}"
    python = _ensure_venv(venv, interpreter)
    # The Python-first 0.5 maintenance line remains the compatibility oracle.
    # Pin it explicitly so publishing hgraph 0.8 cannot silently turn the
    # differential campaign into a comparison of the candidate with itself.
    _run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--upgrade",
            f"hgraph=={REFERENCE_HGRAPH_VERSION}",
        ]
    )
    return python, environment_identity(python)


def _built_candidate_wheel(source_fingerprint: str) -> Path:
    wheel_dir = PARITY_ROOT / "wheels" / source_fingerprint
    wheels = sorted(wheel_dir.glob("*.whl"))
    if len(wheels) == 1:
        return wheels[0]
    wheel_dir.mkdir(parents=True, exist_ok=True)
    _run(
        [
            "uv",
            "build",
            "--wheel",
            "--python",
            "3.12",
            "--config-setting",
            "cmake.build-type=Release",
            "--out-dir",
            str(wheel_dir),
            "--no-build-logs",
        ]
    )
    wheels = sorted(wheel_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(
            f"expected one candidate wheel in {wheel_dir}, found {len(wheels)}"
        )
    return wheels[0]


#: The first-party extension distributions that may ride a candidate
#: environment beside the core wheel (the workspace's native members).
FIRST_PARTY_EXTENSIONS = frozenset({
    "hgraph-analytics",
    "hgraph-fabric",
    "hgraph-kafka",
    "hgraph-persistence",
    "hgraph-web",
})


def _distribution_name(filename: str) -> str:
    """The normalized distribution name of a wheel or ``.dist-info`` filename."""
    return filename.split("-", 1)[0].replace("_", "-").lower()


def _venv_site_packages(python: Path) -> list[Path]:
    """The site-packages directories of the venv ``python`` runs."""
    # The venv's bin/python is a symlink to the base interpreter: resolving it
    # would inspect the base installation's site-packages, not the venv's.
    root = Path(python).absolute().parent.parent
    sites = [root / "Lib" / "site-packages", *sorted(root.glob("lib/python*/site-packages"))]
    return [site for site in sites if site.is_dir()]


def _installed_first_party_extensions(python: Path) -> list[str]:
    """First-party extension distributions installed in the venv ``python`` runs."""
    found = set()
    for site in _venv_site_packages(python):
        for info in site.glob("*.dist-info"):
            name = _distribution_name(info.name)
            if name in FIRST_PARTY_EXTENSIONS:
                found.add(name)
    return sorted(found)


#: The extensions setup builds against the candidate core when the caller
#: supplies no wheel for them: hgraph-persistence serves the frame-recording
#: recipes (RFC 0025). The nightly supplies its own via --candidate-extra-wheel.
BUILT_EXTENSIONS = ("persistence",)

#: The build backend an extension needs beside the core SDK (the nightly's pins).
EXTENSION_BUILD_TOOLS = ("scikit-build-core==1.0.3", "nanobind==2.13.0", "ninja==1.13.0")


def extension_source_fingerprint(name: str) -> str:
    """Hash the inputs that can change the ``extensions/<name>`` wheel."""
    root = REPO_ROOT / "extensions" / name
    digest = hashlib.sha256()
    files: list[Path] = []
    for entry in ("CMakeLists.txt", "pyproject.toml", "cmake", "include", "src", "python"):
        path = root / entry
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(
                child for child in path.rglob("*")
                if child.is_file() and "__pycache__" not in child.parts
            )
    for path in sorted(set(files)):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _built_extension_wheel(name: str, core_fingerprint: str, python: Path) -> Path:
    """Build ``extensions/<name>`` against the candidate core installed in the
    venv ``python`` runs; cached by the core and extension source fingerprints.

    The same shape as the nightly: the core SDK is the installed wheel's
    site-packages (``lib/cmake/hgraph`` lives there), the build runs without
    isolation in that environment so the extension resolves the candidate
    core, not a released one, and links pyarrow's Arrow from the same
    environment it will run in.
    """
    root = REPO_ROOT / "extensions" / name
    fingerprint = hashlib.sha256(
        (core_fingerprint + extension_source_fingerprint(name)).encode()
    ).hexdigest()
    wheel_dir = PARITY_ROOT / "wheels" / fingerprint
    wheels = sorted(wheel_dir.glob(f"hgraph_{name}-*.whl"))
    if len(wheels) == 1:
        return wheels[0]
    wheel_dir.mkdir(parents=True, exist_ok=True)
    _run(["uv", "pip", "install", "--python", str(python), *EXTENSION_BUILD_TOOLS])
    sites = _venv_site_packages(python)
    if not sites:
        raise RuntimeError(f"no site-packages found for the candidate environment {python}")
    env = {**os.environ, "CMAKE_PREFIX_PATH": str(sites[0]), "CMAKE_GENERATOR": "Ninja"}
    _run(
        [
            "uv",
            "build",
            "--wheel",
            "--no-build-isolation",
            "--python",
            str(python),
            "--config-setting",
            "cmake.build-type=Release",
            "--out-dir",
            str(wheel_dir),
            "--no-build-logs",
            str(root),
        ],
        env=env,
    )
    wheels = sorted(wheel_dir.glob(f"hgraph_{name}-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(
            f"expected one hgraph-{name} wheel in {wheel_dir}, found {len(wheels)}"
        )
    return wheels[0]


def ensure_candidate_environment(
    *,
    interpreter: Path | str = sys.executable,
    candidate_wheel: Path | None = None,
    candidate_extra_wheels: tuple[Path, ...] = (),
    build_extensions: bool = True,
) -> tuple[Path, dict, str]:
    key = _environment_key(interpreter)
    venv = PARITY_ROOT / "envs" / f"candidate-{key}"
    python = _ensure_venv(venv, interpreter)
    if candidate_wheel is None:
        fingerprint = hgraph_source_fingerprint(
            REPO_ROOT, python_version="3.12"
        )
        candidate_wheel = _built_candidate_wheel(fingerprint)
    else:
        candidate_wheel = candidate_wheel.resolve()
        fingerprint = _sha256(candidate_wheel)
    # First-party extensions (hgraph-persistence serves the durable
    # record/replay scenarios, RFC 0025) install beside the core wheel and
    # participate in the environment fingerprint.
    extra_wheels = tuple(path.resolve() for path in candidate_extra_wheels)
    for wheel in extra_wheels:
        fingerprint = hashlib.sha256(
            (fingerprint + _sha256(wheel)).encode()
        ).hexdigest()
    core_fingerprint = fingerprint
    supplied = {_distribution_name(wheel.name) for wheel in extra_wheels}
    # The extensions this setup builds itself: every built extension not
    # supplied as a wheel, fingerprinted by its own source too.
    to_build = tuple(
        name for name in BUILT_EXTENSIONS
        if build_extensions and f"hgraph-{name}" not in supplied
    )
    for name in to_build:
        fingerprint = hashlib.sha256(
            (fingerprint + extension_source_fingerprint(name)).encode()
        ).hexdigest()
    marker = venv / ".wheel-fingerprint"
    installed = marker.read_text().strip() if marker.exists() else ""
    if installed != fingerprint:
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "--reinstall",
                str(candidate_wheel),
            ]
        )
        # Built against the core just installed (its SDK is in site-packages).
        built_wheels = tuple(
            _built_extension_wheel(name, core_fingerprint, python) for name in to_build
        )
        supplied |= {f"hgraph-{name}" for name in to_build}
        # An extension wheel an earlier setup installed was built against that
        # setup's core; beside a rebuilt core its native library references
        # symbols the new core may no longer export (a stale hgraph-persistence
        # made every data-frame recipe fail to import locally, 2026-09-07).
        # Drop every first-party extension this setup does not supply or build.
        stale = [
            name for name in _installed_first_party_extensions(python)
            if name not in supplied
        ]
        if stale:
            _run(["uv", "pip", "uninstall", "--python", str(python), *stale])
        extra_wheels = (*extra_wheels, *built_wheels)
        if extra_wheels:
            # --no-deps: an unreleased candidate carries version 0.0.0, which
            # can never satisfy the extension's released hgraph requirement
            # (the same install shape as the wheel-test workflow).
            _run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(python),
                    "--reinstall",
                    "--no-deps",
                    *[str(wheel) for wheel in extra_wheels],
                ]
            )
        marker.write_text(fingerprint + "\n")
    return python, environment_identity(python), fingerprint


def prepare_environments(
    *,
    interpreter: Path | str = sys.executable,
    reference_python: Path | None = None,
    candidate_python: Path | None = None,
    candidate_wheel: Path | None = None,
    candidate_extra_wheels: tuple[Path, ...] = (),
    build_extensions: bool = True,
) -> ParityEnvironments:
    if reference_python is None:
        reference_python, reference_identity = ensure_reference_environment(
            interpreter=interpreter
        )
    else:
        reference_python = reference_python.absolute()
        reference_identity = environment_identity(reference_python)
    if candidate_python is None:
        candidate_python, candidate_identity, fingerprint = (
            ensure_candidate_environment(
                interpreter=interpreter,
                candidate_wheel=candidate_wheel,
                candidate_extra_wheels=candidate_extra_wheels,
                build_extensions=build_extensions,
            )
        )
    else:
        if candidate_wheel is not None or candidate_extra_wheels:
            raise ValueError(
                "candidate wheels cannot be combined with candidate_python"
            )
        candidate_python = candidate_python.absolute()
        candidate_identity = environment_identity(candidate_python)
        fingerprint = "external-" + hashlib.sha256(
            str(candidate_python).encode()
        ).hexdigest()
    return ParityEnvironments(
        reference_python=reference_python,
        candidate_python=candidate_python,
        reference_identity=reference_identity,
        candidate_identity=candidate_identity,
        candidate_fingerprint=fingerprint,
    )
