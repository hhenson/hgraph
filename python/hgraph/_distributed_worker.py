"""Bootstrap only; the native worker owns transport, execution and teardown."""
import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    for name in ("recipe", "read", "write", "start", "end"):
        parser.add_argument(f"--hgraph-worker-{name}", required=True)
    # The caller's channel bounds. Optional so a launch without them still
    # serves on the library defaults, which is what the native flags mean too.
    for name in ("max-frame", "max-work", "max-depth"):
        parser.add_argument(f"--hgraph-worker-{name}", default=None)
    args = parser.parse_args()
    if args.hgraph_worker_recipe != "@hgraph-channel-bootstrap:1":
        raise ValueError("unknown Python worker bootstrap protocol")
    import _hgraph
    frame_default, work_default, depth_default = _hgraph.distributed_transport_defaults()
    max_frame = int(args.hgraph_worker_max_frame or frame_default)
    max_work = int(args.hgraph_worker_max_work or work_default)
    max_depth = int(args.hgraph_worker_max_depth or depth_default)
    channel = _hgraph._DistributedWorkerChannel(
        int(args.hgraph_worker_read), int(args.hgraph_worker_write), max_frame)
    recipe = json.loads(channel.receive_bootstrap())
    paths = recipe["paths"]
    if not isinstance(paths, list) or not all(isinstance(path, str) for path in paths):
        raise ValueError("worker import paths must be strings")
    sys.path[:] = paths
    from ._wiring._graph import _as_wired, _wrap_graph_fn
    from ._distributed import _load_callable, _unpack_config
    func = _load_callable(recipe)
    if "input_names" in recipe:
        wired = _wrap_graph_fn(func, input_names=recipe["input_names"],
            scalar_bindings={name: _unpack_config(value) for name, value in recipe["scalars"].items()})
    else:
        wired = _as_wired(func)
    _hgraph.serve_distributed_worker(wired, recipe, channel,
        int(args.hgraph_worker_start), int(args.hgraph_worker_end), max_work, max_depth)


if __name__ == "__main__":
    main()
