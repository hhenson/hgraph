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
    return f"""<!-- {FINGERPRINT_PREFIX}{fingerprint} -->
## Differential result

A deterministic graph recipe passes with the released Python hgraph reference
and differs under hg_cpp. The case reproduced three times in fresh processes
and was reduced before this issue was created.
{provenance}
- Difference: `{failure['difference']['classification']}` at `{failure['difference']['path']}`
- Reference: `{reference.get('implementation', {})}`
- Candidate: `{candidate.get('implementation', {})}`
- Original seed: `{recipe.get('seed')}`
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

## Actual hg_cpp trace

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
) -> list[dict[str, Any]]:
    failures = list(failures)
    if not publish:
        return [
            {
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
    for failure in failures:
        fingerprint = failure.get("failure_fingerprint") or failure_fingerprint(
            failure
        )
        marker = f"<!-- {FINGERPRINT_PREFIX}{fingerprint} -->"
        title = issue_title(failure)
        match = next(
            (
                issue
                for issue in existing
                if marker in (issue.get("body") or "")
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
        actions.append(
            {
                "action": "created",
                "url": completed.stdout.strip(),
                "fingerprint": fingerprint,
            }
        )
    return actions
