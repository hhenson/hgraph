"""Import bootstrap for process-hosted spawn_; all execution is native-owned."""
import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    for name in ("recipe", "read", "write", "start", "end"):
        parser.add_argument(f"--hgraph-worker-{name}", required=True)
    args = parser.parse_args()
    if args.hgraph_worker_recipe != "@hgraph-spawn:1:python":
        raise ValueError("unknown spawn worker bootstrap protocol")
    import _hgraph
    channel = _hgraph._DistributedWorkerChannel(
        int(args.hgraph_worker_read), int(args.hgraph_worker_write))
    try:
        recipe = json.loads(channel.receive_bootstrap())
        paths = recipe["paths"]
        if not isinstance(paths, list) or not all(isinstance(path, str) for path in paths):
            raise ValueError("spawn worker import paths must be strings")
        sys.path[:] = paths
        from ._distributed import _load_callable, _unpack_config
        from ._wiring._graph import _as_wired, _wrap_graph_fn
        function = _load_callable(recipe)
        wired = (_wrap_graph_fn(function, input_names=recipe["input_names"],
                    scalar_bindings={name: _unpack_config(value) for name, value in recipe["scalars"].items()})
                 if "input_names" in recipe else _as_wired(function))
        _hgraph._serve_spawn_worker(wired, recipe, channel,
            int(args.hgraph_worker_start), int(args.hgraph_worker_end))
    except Exception as error:
        channel.send_spawn_error(f"spawn worker bootstrap: {error}")


if __name__ == "__main__":
    main()
