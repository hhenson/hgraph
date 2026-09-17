"""Bootstrap only; the native worker owns transport, execution and teardown."""
import argparse
import importlib
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", required=True)
    for name in ("recipe", "read", "write", "start", "end"):
        parser.add_argument(f"--hgraph-worker-{name}", required=True)
    args = parser.parse_args()
    paths = json.loads(args.paths)
    if not isinstance(paths, list) or not all(isinstance(path, str) for path in paths):
        raise ValueError("worker import paths must be strings")
    sys.path[:] = paths
    recipe = json.loads(args.hgraph_worker_recipe)
    func = importlib.import_module(recipe["module"])
    for part in recipe["qualname"].split("."):
        func = getattr(func, part)
    from ._wiring._graph import _as_wired
    from ._distributed import _load_type
    from ._types import _value_type
    import _hgraph
    _hgraph.serve_distributed_worker(
        _as_wired(func), _value_type(_load_type(recipe["key"])),
        _value_type(_load_type(recipe["value"])), recipe["result"],
        int(args.hgraph_worker_read), int(args.hgraph_worker_write),
        int(args.hgraph_worker_start), int(args.hgraph_worker_end))


if __name__ == "__main__":
    main()
