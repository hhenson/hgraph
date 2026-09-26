"""Record the binding the HGL front end makes for each call in front_end.hgl.

    python observe_hgl.py --hgl <path to hgl> --revision <hgraph revision>

Reads the call substitutions from ``hgl check --dump-hir`` and renders each
bound type in HGL spelling. Writes ``observed_hgl.json``; never reads an
expectation.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
TYPE = re.compile(r"^\s*t(\d+) (\w+)(.*)$")
CALL = re.compile(r"operation=exact-function:\S+\.pass substitutions=\[s\d+=t(\d+)\]")


def _render(types: dict[int, tuple[str, str]], index: int) -> str:
    kind, rest = types[index]
    children = [int(c) for c in re.findall(r"t(\d+)", (re.search(r"children=\[([^\]]*)\]", rest) or [None, ""])[1])]
    if kind == "scalar":
        return rest.split()[0]
    if kind in ("ref", "list"):
        return f"{kind}<{_render(types, children[0])}>"
    return f"{kind}?"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hgl", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    dump = subprocess.run(
        [args.hgl, "check", str(HERE / "front_end.hgl"), "--dump-hir"], capture_output=True, text=True, check=True
    ).stdout
    types = {int(m.group(1)): (m.group(2), m.group(3)) for m in map(TYPE.match, dump.splitlines()) if m}
    bound = [_render(types, int(m.group(1))) for m in CALL.finditer(dump)]
    observed = {"revision": args.revision, "front_end": "hgl", "through_ref": bound[0], "through_list": bound[1]}
    (HERE / "observed_hgl.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
    print(json.dumps(observed, sort_keys=True))


if __name__ == "__main__":
    main()
