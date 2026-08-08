# hgraph

A clean-slate, **C++-first** implementation of the
[hgraph](https://github.com/hhenson/hgraph) functional-reactive time-series
runtime. The C++ runtime is the source of truth. Python provides wiring
compatibility and supports Python-authored nodes running inside that runtime.

## Python package

The C++-backed runtime is published under the `hgraph` distribution name:

```sh
python -m pip install hgraph
```

The distribution exposes the `hgraph` import package and the native `_hgraph`
extension. Version 0.8.0 replaces the Python-first runtime maintained on the
`release/0.5` branch. One wheel per supported platform covers CPython 3.12 and
later through the CPython stable ABI. The supported Python and platform policy is recorded in
`docs/source/developer_guide/release_readiness.rst`.

## Build & test

```sh
cmake -S . -B build                 # configure (fmt + Catch2 fetched if absent)
cmake --build build -j              # build hgraph_core + tests
ctest --test-dir build --output-on-failure
```

Requires a C++23 compiler and CMake >= 3.25. Python/nanobind are **not** needed
for the default build (bindings are opt-in via `-DHGRAPH_BUILD_PYTHON_BINDINGS=ON`).

## First-party extensions

First-party extensions are co-developed in `extensions/` but remain separate
native and Python distributions. Kafka is built in-tree for development with
`-DHGRAPH_BUILD_KAFKA_EXTENSION=ON`, or independently from
`extensions/kafka/` against an installed hgraph SDK. Its wheel is selected
from the `uv` workspace after making that matching SDK discoverable:

```sh
CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
  uv build --wheel --package hgraph-kafka --python 3.12
```

The core build does not enable the extension by default and does not acquire a
Kafka or librdkafka dependency.

## Documentation

Sphinx docs live under `docs/source` (`uv sync --extra docs`, then
`uv run sphinx-build -W -b html docs/source docs/_build/html`):

- **User guide** — start at `docs/source/user_guide/quick_start.rst`, then the
  authoring/testing guides for nodes, graphs, and the `eval_node` harness.
- **Developer guide** — the authoritative design records
  (`docs/source/developer_guide/`): architecture, data structures,
  wiring, nested graphs, mesh, services, error handling, operators, roadmap.

## Contributing / AI sessions

- [`AGENTS.md`](AGENTS.md) — canonical project direction: goals, build
  philosophy, source layout, dependency policy, git hygiene.
- [`CLAUDE.md`](CLAUDE.md) — the operational working guide: the enforced
  design-first workflow (docs change in the same commit as code), guardrails,
  architecture map, and current state.
- The read-only `ext/main` tree tracks the maintained Python-first
  `release/0.5` reference.
