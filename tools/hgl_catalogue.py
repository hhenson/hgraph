#!/usr/bin/env python3
"""Maintain the source-backed HGL implementation catalogue.

The normal command and --check use only Python's standard library. Registry
refresh additionally needs an installed hgraph wheel from the current sources.
Source sites and expanded registry signatures are deliberately separate records.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
CATALOGUE = ROOT / "language/stdlib/catalogue"


def without_comments(text: str) -> str:
    # Preserve string literals and line numbers, including templates containing
    # quoted registry names. Matching an apparent registration in a comment is
    # not evidence that it participates in the build.
    return re.sub(
        r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|//[^\n]*|/\*[\s\S]*?\*/',
        lambda m: re.sub(r"[^\n]", " ", m[0]) if m[0].startswith(("//", "/*")) else m[0],
        text,
    )


def template_arguments(text: str, start: int) -> tuple[list[str], int]:
    """Read a balanced C++ template argument list, including nested commas."""
    depth = 1
    begin = start
    args = []
    quote = None
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ('"', "'"):
            quote = char
        elif char in "<([{":
            depth += 1
        elif char in ">)]}":
            depth -= 1
            if depth == 0:
                args.append(" ".join(text[begin:index].split()))
                return args, index + 1
        elif char == "," and depth == 1:
            args.append(" ".join(text[begin:index].split()))
            begin = index + 1
    raise ValueError(f"unterminated template argument list at {start}")


def source_inventory(root: Path = ROOT) -> dict:
    patterns = ("include/hgraph/lib/**/*.h", "src/hgraph/lib/**/*.cpp",
                "extensions/*/include/**/*.h", "extensions/*/src/**/*.cpp")
    paths = sorted({p for pattern in patterns for p in root.glob(pattern)})
    contracts = []
    registrations = []
    for path in paths:
        relative = path.relative_to(root).as_posix()
        raw = path.read_text(encoding="utf-8")
        text = without_comments(raw)
        scope = relative.split("/")[1] if relative.startswith("extensions/") else "core"
        for match in re.finditer(r'\bstruct\s+(\w+)\s*:\s*(?:public\s+)?(?:\w+::)*Operator\s*<\s*"([^"\n]+)"', text):
            contracts.append(dict(name=match[2], marker=match[1], scope=scope,
                                  source=relative, line=text.count("\n", 0, match.start()) + 1))
        for match in re.finditer(r"\b(register_overload|register_graph_overload)\s*<", text):
            args, _ = template_arguments(text, match.end())
            if len(args) != 2:
                raise ValueError(f"unexpected registration shape in {relative}: {args}")
            registrations.append(dict(operator=args[0], implementation=args[1], scope=scope,
                                      kind="graph" if "graph" in match[1] else "node",
                                      source=relative, line=text.count("\n", 0, match.start()) + 1))
    # Fingerprint what was extracted, not the bytes it came from. An unrelated
    # implementation or comment edit must not invalidate the catalogue, while a
    # new declaration or registration site must. Line numbers stay out of the
    # identity: they move when comments above a declaration do, and the
    # catalogue regenerates them without a wheel.
    identity = dict(
        contracts=sorted((x["name"], x["marker"], x["scope"], x["source"]) for x in contracts),
        registration_sites=sorted((x["operator"], x["implementation"], x["scope"], x["kind"], x["source"])
                                  for x in registrations),
    )
    return dict(source_fingerprint=hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
                contracts=contracts, registration_sites=registrations)


def registry_inventory() -> dict:
    import hgraph  # noqa: F401 -- initializes the public core registry
    import _hgraph

    operators = {}
    for name in sorted(_hgraph.operator_names()):
        # Wheel-specific bridge and harness registrations are not core-library
        # authoring candidates. Keep named internal library operators.
        if name.startswith(("__py", "__harness")) or "." in name:
            continue
        candidates = []
        for raw in _hgraph.operator_overload_signatures(name):
            if len(raw) == 7:
                params, variadic, positional, kwargs, kwargs_type, output, output_type = raw
                positional_cardinality = keyword_cardinality = (0, None)
            else:
                (params, variadic, positional, positional_cardinality, kwargs,
                 keyword_cardinality, kwargs_type, output, output_type) = raw
            candidate = dict(
                parameters=[dict(name=n, kind="type" if carrier is not None else "temporal" if ts else "scalar",
                                 type=t, default=bool(default), carrier=carrier)
                            for n, ts, t, default, carrier in params],
                variadic=bool(variadic), positional_count=positional,
                positional_cardinality=list(positional_cardinality), kwargs=bool(kwargs),
                keyword_cardinality=list(keyword_cardinality), kwargs_type=kwargs_type,
                output=output_type if output else None,
            )
            candidate["id"] = name + ":" + hashlib.sha256(
                json.dumps(candidate, sort_keys=True).encode()).hexdigest()[:12]
            candidates.append(candidate)
        operators[name] = sorted(candidates, key=lambda x: x["id"])
    return dict(source_fingerprint=source_inventory()["source_fingerprint"], operators=operators)


def hgl_inventory(root: Path = ROOT) -> list[dict]:
    result = []
    for path in sorted((root / "language/stdlib/hgl/hgraph").rglob("*.hgl")):
        text = re.sub(r"(?m)^[ \t]*#.*$", "", path.read_text(encoding="utf-8"))
        module = re.search(r"(?m)^module\s+([\w.]+)", text)
        if module is None:
            raise ValueError(f"missing module declaration: {path}")
        for match in re.finditer(r"(?m)^(operator|impl fn|native fn)\s+(\w+)", text):
            following = re.search(r"(?m)^(?:operator |impl fn |native fn |instantiate |test\b)", text[match.end():])
            end = match.end() + following.start() if following else len(text)
            body = text[match.start():end].strip()
            kind = {"operator": "contract", "impl fn": "implementation", "native fn": "native-value"}[match[1]]
            form = None
            if kind == "implementation":
                form = "native-delegation" if re.search(r"=>\s*core::", body) else "hgl-runtime" if "when" in body else "hgl-composition"
            result.append(dict(module=module[1], name=match[2], kind=kind, form=form,
                               source=path.relative_to(root).as_posix(),
                               line=text.count("\n", 0, match.start()) + 1,
                               declaration=body.split("{", 1)[0].split("=>", 1)[0].strip()))
    return result


def hgl_materializations(root: Path = ROOT) -> list[dict]:
    result = []
    for path in sorted((root / "language/stdlib/hgl/hgraph").rglob("*.hgl")):
        text = re.sub(r"(?m)^[ \t]*#.*$", "", path.read_text(encoding="utf-8"))
        for match in re.finditer(r"(?m)^instantiate[^\n]*(?:\n[ \t]+[^\n]+)*", text):
            result.append(dict(source=path.relative_to(root).as_posix(),
                               line=text.count("\n", 0, match.start()) + 1,
                               declaration=" ".join(match[0].split())))
    return result


def build_catalogue() -> dict:
    source = source_inventory()
    registry = json.loads((CATALOGUE / "registry.json").read_text(encoding="utf-8"))
    policy = json.loads((CATALOGUE / "status.json").read_text(encoding="utf-8"))
    if registry["source_fingerprint"] != source["source_fingerprint"]:
        raise ValueError("native declarations or registration sites changed; "
                         "rebuild the wheel and run --refresh-registry")
    hgl = hgl_inventory()
    names = sorted(set(registry["operators"]) | {x["name"] for x in source["contracts"] if x["scope"] == "core"})
    entries = []
    for name in names:
        contracts = [x for x in source["contracts"] if x["scope"] == "core" and x["name"] == name]
        markers = {x["marker"] for x in contracts}
        sites = [x for x in source["registration_sites"] if x["scope"] == "core" and x["operator"].split("::")[-1] in markers]
        accepted_names = policy["operators"].get(name, {}).get("hgl_names", [name])
        accepted = [x for x in hgl if x["name"] in accepted_names and x["kind"] != "native-value"]
        review = policy["operators"].get(name, {"status": "unreviewed", "reason": "Candidate-domain review required."})
        if review["status"] not in {"implemented", "implemented-slice", "blocked", "native-provider", "unreviewed"}:
            raise ValueError(f"invalid review status for {name}")
        if any(key not in policy["blockers"] for key in review.get("blockers", [])):
            raise ValueError(f"unknown blocker for {name}")
        if review["status"].startswith("implemented") and not any(
                x["kind"] == "implementation" and x["form"] != "native-delegation" for x in accepted):
            raise ValueError(f"{name} has no HGL implementation evidence")
        for path in review.get("evidence", []):
            if not (ROOT / path).is_file():
                raise ValueError(f"missing evidence for {name}: {path}")
        entries.append(dict(name=name, review=review, declarations=contracts, registration_sites=sites,
                            registry_candidates=registry["operators"].get(name, []), hgl=accepted,
                            core_cutover="deferred"))
    unknown = set(policy["operators"]) - set(names)
    if unknown:
        raise ValueError(f"stale catalogue status names: {sorted(unknown)}")
    return dict(format_version=1, scope="core library with separate extension source inventory",
                source_fingerprint=source["source_fingerprint"], blockers=policy["blockers"],
                operators=entries, other_surfaces=policy["other_surfaces"],
                hgl_native_substrate=[x for x in hgl if x["kind"] == "native-value"],
                hgl_materializations=hgl_materializations(),
                all_registration_sites=source["registration_sites"],
                extension_contracts=[x for x in source["contracts"] if x["scope"] != "core"])


def render(data: dict) -> str:
    counts = Counter(x["review"]["status"] for x in data["operators"])
    lines = ["# HGL implementation catalogue", "", "Generated by `python tools/hgl_catalogue.py`; edit `status.json` to record a review.", "",
             "Compiled HGL bodies and bindings to HGL-exposed native value functions count as completed authoring work. A temporal-node delegation stays pending. Core replacement is deferred.",
             "Implementation form and remaining overload domains are recorded separately; an implemented slice is not a claim of complete family parity.", "",
             f"**{len(data['operators'])} core operator identities**, " + ", ".join(f"{n} {s}" for s, n in sorted(counts.items())) + ".", "",
             "The [machine-readable catalogue](catalogue.json) joins declarations, registration sites, expanded registry signatures, HGL sources, and review decisions.",
             "Source registration templates are included without pretending that each site is one concrete candidate. Optional extension declarations and registrations have separate records.", "",
             "## Priority order", "",
             "1. Native error/ownership bindings and lifecycle/startup/activation (B1, B2).",
             "2. Open scalar/schema and structural/reference parity (B3, B4).",
             "3. Domain providers and higher-order lifecycle (B5, B6).", "",
             "Implemented means the recorded domain is authored and tested; it does not promise production API replacement or every generic specialization.", "",
             "## Operators", "", "| Operator | Review status | HGL forms | Remaining work / evidence |", "| --- | --- | --- | --- |"]
    for entry in data["operators"]:
        implementations = [x for x in entry["hgl"] if x["kind"] == "implementation"]
        forms = sorted({x["form"] for x in implementations}) or (["contract only"] if entry["hgl"] else [])
        reason = entry["review"]["reason"].replace("|", "\\|")
        reason += " " + ", ".join(f"[{key}](#{key.lower()})" for key in entry["review"].get("blockers", []))
        lines.append(f"| `{entry['name']}` | {entry['review']['status']} | {', '.join(forms) or '—'} | {reason} |")
    lines += ["", "## Blocker definitions", ""]
    for key, item in data["blockers"].items():
        lines += [f"### {key}", "", f"Priority {item['priority']}. " + item["summary"], "",
                  "Evidence: " + ", ".join(f"[{Path(p).name}](../../../{p})" for p in item["evidence"]), ""]
    lines += ["## Other library surfaces", "", "| Surface | Disposition | Reason |", "| --- | --- | --- |"]
    for item in data["other_surfaces"]:
        lines.append(f"| `{item['name']}` | {item['status']} | {item['reason']} |")
    lines += ["", "## Maintenance", "", "```sh", "python tools/hgl_catalogue.py --check",
              "# After changing native library registrations, use a freshly built wheel:",
              "python tools/hgl_catalogue.py --refresh-registry", "```", "",
              "See [the design record](../../docs/design/migration-catalogue.md). PR #801 is reference material only.", ""]
    return "\n".join(lines)


def comparable(text: str) -> str:
    """Catalogue content without the recorded source positions.

    --check must fail when the inventory changes, not when an unrelated edit
    moves a declaration down its file. A pull request is validated on its merge
    commit, which carries the base branch's sources, so a shifted line would
    otherwise fail this ratchet on work that never touched the catalogue. The
    positions stay in the written file; regenerating refreshes them.
    """

    def strip(node):
        if isinstance(node, dict):
            return {key: strip(value) for key, value in node.items() if key != "line"}
        if isinstance(node, list):
            return [strip(item) for item in node]
        return node

    return json.dumps(strip(json.loads(text)), sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--refresh-registry", action="store_true")
    args = parser.parse_args()
    if args.check and args.refresh_registry:
        parser.error("--check and --refresh-registry are mutually exclusive")
    if args.refresh_registry:
        (CATALOGUE / "registry.json").write_text(json.dumps(registry_inventory(), indent=2) + "\n",
                                                 encoding="utf-8", newline="\n")
    data = build_catalogue()
    outputs = {"catalogue.json": json.dumps(data, indent=2) + "\n", "README.md": render(data)}
    for name, content in outputs.items():
        path = CATALOGUE / name
        if args.check:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if name.endswith(".json") and current is not None:
                stale = comparable(current) != comparable(content)
            else:
                stale = current != content
            if stale:
                raise SystemExit(f"{path.relative_to(ROOT)} is stale; run python tools/hgl_catalogue.py")
        else:
            path.write_text(content, encoding="utf-8", newline="\n")
    print(f"HGL catalogue: {len(data['operators'])} core operator identities")


if __name__ == "__main__":
    main()
