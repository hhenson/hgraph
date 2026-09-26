"""Record what the HGL front end decides: the binding it makes for each call
in front_end.hgl, and whether it accepts the implementations in
the contract_*.hgl modules.

    python observe_hgl.py --hgl <path to hgl> --revision <hgraph revision>

Reads the call substitutions from ``hgl check --dump-hir`` and renders each
bound type in HGL spelling. Writes ``observed_hgl.json``; never reads an
expectation.
"""

from __future__ import annotations

import argparse
import json
import os
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


def _compiler(path: str) -> Path:
    """The HGL compiler named on the command line: an executable file called hgl."""
    compiler = Path(path).resolve(strict=True)
    if compiler.name not in ("hgl", "hgl.exe") or not compiler.is_file() or not os.access(compiler, os.X_OK):
        raise SystemExit(f"--hgl must name the hgl compiler executable, got {path!r}")
    return compiler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hgl", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    hgl = _compiler(args.hgl)
    dump = subprocess.run(
        [str(hgl), "check", str(HERE / "front_end.hgl"), "--dump-hir"], capture_output=True, text=True, check=True
    ).stdout
    types = {int(m.group(1)): (m.group(2), m.group(3)) for m in map(TYPE.match, dump.splitlines()) if m}
    bound = [_render(types, int(m.group(1))) for m in CALL.finditer(dump)]
    observed = {"revision": args.revision, "front_end": "hgl", "through_ref": bound[0], "through_list": bound[1]}
    for field, module in (("superset_implementation", "contract_superset.hgl"),
                          ("required_extra_implementation", "contract_required_extra.hgl"),
                          ("extra_argument_call", "contract_extra_argument.hgl"),
                          ("widening_implementation", "contract_widening.hgl")):
        checked = subprocess.run([str(hgl), "check", str(HERE / module)], capture_output=True, text=True)
        observed[field] = "accepted" if checked.returncode == 0 else "rejected"
    (HERE / "observed_hgl.json").write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
    print(json.dumps(observed, sort_keys=True))


if __name__ == "__main__":
    main()
