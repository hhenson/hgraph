"""Deterministic issue payloads and trusted GitHub publishing."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable


FINGERPRINT_PREFIX = "hgraph-parity:"
ORIGIN_PREFIX = "hgraph-parity-origin:"


def _value_shape(value: Any, *, data_mapping: bool = False) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return "<int>"
    if isinstance(value, float):
        return "<float>"
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [
            _value_shape(item, data_mapping=data_mapping) for item in value
        ]
    if isinstance(value, dict):
        if data_mapping and not any(str(key).startswith("$") for key in value):
            return {
                "$data-map": sorted(
                    (
                        _value_shape(item, data_mapping=True)
                        for item in value.values()
                    ),
                    key=lambda item: json.dumps(item, sort_keys=True),
                )
            }
        return {
            key: _value_shape(item, data_mapping=data_mapping)
            for key, item in sorted(value.items())
        }
    return f"<{type(value).__name__}>"


def failure_fingerprint(failure: dict[str, Any]) -> str:
    recipe = failure["minimized_recipe"]
    difference = failure["difference"]
    payload = {
        "template": recipe["template"],
        "inputs": {
            name: _value_shape(ticks, data_mapping=True)
            for name, ticks in sorted(recipe["inputs"].items())
        },
        "parameters": _value_shape(recipe.get("parameters", {})),
        "difference": {
            "classification": difference["classification"],
            "path": re.sub(r"\[\d+\]", "[]", difference["path"]),
            "reference_shape": _value_shape(difference.get("reference")),
            "candidate_shape": _value_shape(difference.get("candidate")),
        },
        "reference_status": failure.get("reference", {}).get("status"),
        "candidate_status": failure.get("candidate", {}).get("status"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def failure_origin(failure: dict[str, Any]) -> str | None:
    """The generated case a failure was reduced from: its recipe ``id``.

    Eight shards reducing one seeded failure each minimise it to a slightly
    different shape, and every shape mints a fresh fingerprint; on 2026-08-27
    that filed 35 issues (#570-#604) for one defect. The origin is stable
    across those variants, so the publisher files one issue per origin and
    folds the variants into it. The key is the *original* (pre-reduction)
    recipe's id, which hashes that case's payload: one campaign seed
    produces thousands of cases, so the seed alone would fold unrelated
    failures of one template into the first issue filed. A corpus recipe is
    not generated and keeps fingerprint identity only.
    """
    original = failure.get("original_recipe") or failure.get("minimized_recipe") or {}
    if original.get("seed") is None:
        return None
    case_id = original.get("id")
    return case_id if isinstance(case_id, str) and case_id else None


def issue_title(failure: dict[str, Any]) -> str:
    recipe = failure["minimized_recipe"]
    return f"[parity] {recipe['template']} differs from released hgraph"


def issue_body(failure: dict[str, Any]) -> str:
    fingerprint = failure.get("failure_fingerprint") or failure_fingerprint(failure)
    recipe = failure["minimized_recipe"]
    reference = failure["reference"]
    candidate = failure["candidate"]
    reduction = failure.get("reduction", {})
    source_issue = recipe.get("source_issue")
    provenance = (
        f"\nRelated seed: {source_issue}\n" if source_issue else ""
    )
    origin = failure_origin(failure)
    origin_marker = f"\n<!-- {ORIGIN_PREFIX}{origin} -->" if origin else ""
    return f"""<!-- {FINGERPRINT_PREFIX}{fingerprint} -->{origin_marker}
## Differential result

A deterministic graph recipe passes with the maintained Python-first hgraph
0.5 reference and differs under C++-first hgraph. The case reproduced three times in fresh processes
and was reduced before this issue was created.
{provenance}
- Difference: `{failure['difference']['classification']}` at `{failure['difference']['path']}`
- Reference: `{reference.get('implementation', {})}`
- Candidate: `{candidate.get('implementation', {})}`
- Original seed: `{recipe.get('seed')}`
- Origin case: `{origin or recipe.get('id')}`
- Reduction: {reduction.get('accepted', 0)} accepted changes from {reduction.get('attempts', 0)} attempts
- Failure fingerprint: `{fingerprint}`

## Minimized recipe

```json
{json.dumps(recipe, indent=2, sort_keys=True)}
```

## Expected reference trace

```json
{json.dumps(reference, indent=2, sort_keys=True)}
```

## Actual C++-first hgraph trace

```json
{json.dumps(candidate, indent=2, sort_keys=True)}
```

## Acceptance criteria

- The minimized recipe produces the released hgraph trace.
- The fix adds public Python wiring regression coverage.
- The same behavior receives equivalent native C++ `eval_node` coverage.
"""


def _gh(
    arguments: list[str],
    *,
    repo: str,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["gh", *arguments, "--repo", repo],
        check=True,
        capture_output=capture,
        text=True,
    )


def _existing_issues(repo: str) -> list[dict[str, Any]]:
    completed = _gh(
        [
            "issue",
            "list",
            "--state",
            "all",
            "--limit",
            "1000",
            "--json",
            "number,state,title,body,url",
        ],
        repo=repo,
        capture=True,
    )
    return json.loads(completed.stdout)


def publish_failures(
    failures: Iterable[dict[str, Any]],
    *,
    repo: str,
    publish: bool,
    known_divergences_path: Path | None = None,
) -> list[dict[str, Any]]:
    from .known import is_known_family_failure, load_known_divergences

    failures = list(failures)
    known_fingerprints, known_families = load_known_divergences(
        known_divergences_path
    )

    def known_action(failure: dict[str, Any]) -> dict[str, Any] | None:
        # The publisher re-checks the known-divergence records so a stale or
        # concurrently produced report can never file — or reopen — an issue
        # for a documented deviation.
        fingerprint = failure.get("failure_fingerprint") or failure_fingerprint(
            failure
        )
        if fingerprint not in known_fingerprints and not is_known_family_failure(
            failure.get("minimized_recipe") or {},
            failure.get("difference") or {},
            failure.get("reference") or {},
            failure.get("candidate") or {},
            known_families,
        ):
            return None
        return {"action": "known-divergence", "fingerprint": fingerprint}
    if not publish:
        return [
            known_action(failure)
            or {
                "action": "dry-run",
                "title": issue_title(failure),
                "fingerprint": failure.get("failure_fingerprint")
                or failure_fingerprint(failure),
                "body": issue_body(failure),
            }
            for failure in failures
        ]

    _gh(
        [
            "label",
            "create",
            "parity",
            "--color",
            "5319E7",
            "--description",
            "Verified behavioral difference from released hgraph",
            "--force",
        ],
        repo=repo,
    )
    existing = _existing_issues(repo)
    actions: list[dict[str, Any]] = []
    handled_this_run: dict[str, str] = {}
    handled_origins: dict[str, str] = {}
    for failure in failures:
        fingerprint = failure.get("failure_fingerprint") or failure_fingerprint(
            failure
        )
        marker = f"<!-- {FINGERPRINT_PREFIX}{fingerprint} -->"
        origin = failure_origin(failure)
        origin_marker = f"<!-- {ORIGIN_PREFIX}{origin} -->" if origin else None
        title = issue_title(failure)
        known = known_action(failure)
        if known is not None:
            actions.append(known)
            continue
        # One issue per fingerprint per publish, and one per generated origin
        # (the original case's recipe id): every minimized variant of one
        # generated failure folds into the first issue filed for it.
        folded = handled_this_run.get(fingerprint) or (
            handled_origins.get(origin) if origin else None
        )
        if folded is not None:
            actions.append(
                {
                    "action": "deduplicated",
                    "url": folded,
                    "fingerprint": fingerprint,
                }
            )
            continue
        match = next(
            (
                issue
                for issue in existing
                if marker in (issue.get("body") or "")
                or (origin_marker is not None and origin_marker in (issue.get("body") or ""))
            ),
            None,
        )
        if match is not None:
            if match["state"].upper() == "CLOSED":
                _gh(
                    ["issue", "reopen", str(match["number"])],
                    repo=repo,
                )
                action = "reopened"
            else:
                action = "deduplicated"
            actions.append(
                {
                    "action": action,
                    "number": match["number"],
                    "url": match["url"],
                    "fingerprint": fingerprint,
                }
            )
            handled_this_run[fingerprint] = match["url"]
            if origin:
                handled_origins[origin] = match["url"]
            continue

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".md", encoding="utf-8", delete=False
        ) as body_file:
            body_file.write(issue_body(failure))
            body_path = Path(body_file.name)
        try:
            completed = _gh(
                [
                    "issue",
                    "create",
                    "--title",
                    title,
                    "--body-file",
                    str(body_path),
                    "--label",
                    "bug",
                    "--label",
                    "parity",
                ],
                repo=repo,
                capture=True,
            )
        finally:
            body_path.unlink(missing_ok=True)
        url = completed.stdout.strip()
        actions.append(
            {
                "action": "created",
                "url": url,
                "fingerprint": fingerprint,
            }
        )
        handled_this_run[fingerprint] = url
        if origin:
            handled_origins[origin] = url
    return actions
