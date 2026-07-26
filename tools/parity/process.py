"""Subprocess isolation, result parsing, and reference trace caching."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from .model import Recipe
from .runner import RESULT_MARKER


RUNNER = Path(__file__).with_name("runner.py")


def _harness_fingerprint() -> str:
    digest = hashlib.sha256()
    for name in ("canonical.py", "catalog.py", "model.py", "runner.py"):
        path = RUNNER.with_name(name)
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


HARNESS_FINGERPRINT = _harness_fingerprint()


def _sanitized_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in (
        "HGRAPH_USE_CPP",
        "PYTHONPATH",
        "PYTHONHOME",
        "VIRTUAL_ENV",
    ):
        environment.pop(name, None)
    environment.update(PYTHONHASHSEED="0", TZ="UTC")
    return environment


def _parse_result(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith(RESULT_MARKER):
            try:
                value = json.loads(line[len(RESULT_MARKER) :])
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, dict) else None
    return None


def _invoke(
    interpreter: Path | str,
    arguments: list[str],
    *,
    input_text: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    command = [str(interpreter), "-I", str(RUNNER), *arguments]
    try:
        completed = subprocess.run(
            command,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=RUNNER.parents[2],
            env=_sanitized_environment(),
        )
    except subprocess.TimeoutExpired as error:
        return {
            "status": "timeout",
            "phase": "process",
            "timeout_seconds": timeout,
            "diagnostic": (error.stderr or "")[-4000:],
        }
    except OSError as error:
        return {
            "status": "infrastructure-error",
            "phase": "process",
            "diagnostic": str(error),
        }

    parsed = _parse_result(completed.stdout)
    if parsed is not None:
        parsed.setdefault("process_returncode", completed.returncode)
        if completed.stderr:
            parsed.setdefault("process_stderr", completed.stderr[-4000:])
        return parsed
    return {
        "status": "crash" if completed.returncode else "harness-error",
        "phase": "process",
        "process_returncode": completed.returncode,
        "diagnostic": (
            f"stdout:\n{completed.stdout[-4000:]}\n"
            f"stderr:\n{completed.stderr[-4000:]}"
        ),
    }


def environment_identity(
    interpreter: Path | str, *, timeout: float = 30.0
) -> dict[str, Any]:
    result = _invoke(interpreter, ["--identity"], timeout=timeout)
    if result.get("status") != "ok":
        raise RuntimeError(f"cannot inspect parity environment: {result}")
    return result["identity"]


def operator_inventory(
    interpreter: Path | str, *, timeout: float = 30.0
) -> dict[str, Any]:
    result = _invoke(interpreter, ["--inventory"], timeout=timeout)
    if result.get("status") != "ok":
        raise RuntimeError(f"cannot inspect operator inventory: {result}")
    return result


def run_recipe(
    interpreter: Path | str,
    recipe: Recipe,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    return _invoke(
        interpreter,
        [],
        input_text=recipe.canonical_json(),
        timeout=timeout,
    )


class ReferenceTraceCache:
    def __init__(self, path: Path, identity: dict[str, Any]):
        self.path = path
        self.identity = identity

    def _key(self, recipe: Recipe) -> str:
        payload = json.dumps(
            {
                "recipe": recipe.fingerprint,
                "identity": self.identity,
                "runner_schema": 1,
                "harness": HARNESS_FINGERPRINT,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def get(self, recipe: Recipe) -> dict[str, Any] | None:
        path = self.path / f"{self._key(recipe)}.json"
        try:
            value = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def put(self, recipe: Recipe, result: dict[str, Any]) -> None:
        if result.get("status") != "ok":
            return
        self.path.mkdir(parents=True, exist_ok=True)
        path = self.path / f"{self._key(recipe)}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(result, sort_keys=True))
        temporary.replace(path)

    def run(
        self,
        interpreter: Path | str,
        recipe: Recipe,
        *,
        timeout: float = 30.0,
        bypass: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        if not bypass and (cached := self.get(recipe)) is not None:
            return cached, True
        result = run_recipe(interpreter, recipe, timeout=timeout)
        self.put(recipe, result)
        return result, False
