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


def _run(command: list[str], *, cwd: Path = REPO_ROOT) -> None:
    completed = subprocess.run(command, cwd=cwd)
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


def ensure_candidate_environment(
    *,
    interpreter: Path | str = sys.executable,
    candidate_wheel: Path | None = None,
    candidate_extra_wheels: tuple[Path, ...] = (),
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
