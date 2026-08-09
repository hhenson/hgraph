"""Fresh-process executor for one parity recipe.

Invoke this file with ``python -I`` so the selected environment supplies the
only importable hgraph package.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import sys
import traceback
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from tools.parity.canonical import CANONICAL_SCHEMA_VERSION, canonicalize
from tools.parity.catalog import execute_recipe
from tools.parity.model import Recipe


RESULT_MARKER = "@@PARITY_RESULT@@"


def _distribution_identity() -> dict[str, str]:
    for distribution in ("hgraph", "hg_cpp"):
        try:
            version = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            continue
        return {"distribution": distribution, "version": version}
    return {"distribution": "unknown", "version": "unknown"}


def identity() -> dict:
    result = _distribution_identity()
    result.update(
        python=platform.python_version(),
        python_implementation=platform.python_implementation(),
        platform=platform.platform(),
        machine=platform.machine().lower(),
    )
    return result


def _exception_category(error: BaseException) -> str:
    name = type(error).__name__
    if "Wiring" in name or "Resolution" in name:
        return "wiring"
    if isinstance(error, TypeError):
        return "type"
    if isinstance(error, ValueError):
        return "value"
    if isinstance(error, KeyError):
        return "key"
    if isinstance(error, ZeroDivisionError):
        return "division-by-zero"
    if isinstance(error, AssertionError):
        return "assertion"
    return "runtime"


def _is_public_operator(value) -> bool:
    return any(
        cls.__name__ == "OperatorWiringNodeClass"
        for cls in type(value).__mro__
    )


def _fallback_operator_names(hg) -> set[str]:
    return {
        name
        for name in dir(hg)
        if not name.startswith("_")
        and _is_public_operator(getattr(hg, name, None))
    }


def _operator_inventory(hg) -> list[str]:
    names: set[str] = set()
    try:
        import _hgraph

        names.update(str(name) for name in _hgraph.operator_names())
    except (ImportError, AttributeError):
        pass
    try:
        import hgraph._operators as operators

        names.update(str(name) for name in getattr(operators, "__all__", ()))
    except ImportError:
        pass
    if not names:
        names.update(_fallback_operator_names(hg))
    return sorted(names)


def run_recipe(raw: dict) -> dict:
    recipe = Recipe.from_dict(raw)
    result = {
        "schema_version": 1,
        "canonical_schema_version": CANONICAL_SCHEMA_VERSION,
        "recipe_id": recipe.id,
        "recipe_fingerprint": recipe.fingerprint,
        "implementation": identity(),
    }
    try:
        import hgraph as hg
    except BaseException as error:
        result.update(
            status="error",
            phase="import",
            exception={
                "category": _exception_category(error),
                "type": type(error).__name__,
            },
            diagnostic=traceback.format_exc(limit=20),
        )
        return result

    try:
        value = execute_recipe(hg, recipe)
    except BaseException as error:
        category = _exception_category(error)
        result.update(
            status="error",
            phase="wiring" if category == "wiring" else "runtime",
            exception={"category": category, "type": type(error).__name__},
            diagnostic=traceback.format_exc(limit=30),
        )
        return result
    result.update(status="ok", phase="complete", trace=canonicalize(value))
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", action="store_true")
    parser.add_argument("--inventory", action="store_true")
    args = parser.parse_args()
    if args.identity:
        print(RESULT_MARKER + json.dumps({"status": "ok", "identity": identity()}))
        return 0
    if args.inventory:
        import hgraph as hg

        print(
            RESULT_MARKER
            + json.dumps(
                {
                    "status": "ok",
                    "identity": identity(),
                    "operators": _operator_inventory(hg),
                }
            )
        )
        return 0

    try:
        raw = json.loads(sys.stdin.read())
        result = run_recipe(raw)
    except BaseException as error:
        result = {
            "schema_version": 1,
            "status": "error",
            "phase": "harness",
            "exception": {
                "category": _exception_category(error),
                "type": type(error).__name__,
            },
            "diagnostic": traceback.format_exc(limit=30),
        }
    print(RESULT_MARKER + json.dumps(result, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
