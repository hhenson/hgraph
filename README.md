# hgraph

HGraph is a functional-reactive time-series engine with a Python-first user
experience and a native C++ runtime. Most users author and test graphs with the
`hgraph` Python package; library authors can use the C++ API directly when they
need native integration or maximum performance. Both paths wire and execute
through the same runtime.

## Python package

The C++-backed runtime is published under the `hgraph` distribution name:

```sh
python -m pip install hgraph
```

The distribution exposes the supported `hgraph` authoring package and its
private native `_hgraph` extension. The 0.8 line replaces the Python runtime
maintained on the `release/0.5` branch while retaining Python as the primary
public API. One wheel per supported platform covers CPython 3.12 and later
through the CPython stable ABI. The supported Python and platform policy is
recorded in `docs/source/developer_guide/release_readiness.rst`.

Start with [`docs/source/getting_started.rst`](docs/source/getting_started.rst)
and use [`docs/source/reference/`](docs/source/reference/) for the supported
Python types, decorators, operators and modules.

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

- **Getting started** — `docs/source/getting_started.rst`: install the wheel and
  run a first graph in Python.
- **User guide** — `docs/source/user_guide/`: the concepts the runtime
  implements and the primary Python authoring track (`python/`: quick start,
  common tasks, tutorial, programming model). Native C++ authoring is an
  advanced section for library authors.
- **Python API reference** — `docs/source/reference/`: curated reference pages
  plus a generated inventory of wildcard exports, lazy operators and public
  submodules.
- **Specification** — `docs/source/specification/`: a language-neutral
  definition of HGraph semantics.
- **Developer guide** — the authoritative design records
  (`docs/source/developer_guide/`): architecture, data structures,
  wiring, nested graphs, mesh, services, error handling, operators, roadmap.

The narrative documentation's Python examples are executable. They are checked
against a real runtime by `sphinx-build -b doctest`, which needs an importable
`hgraph`; CI runs both that and the warning-free HTML build.

## Contributing / AI sessions

- [`AGENTS.md`](AGENTS.md) — canonical project direction: goals, build
  philosophy, source layout, dependency policy, git hygiene.
- [`CLAUDE.md`](CLAUDE.md) — the operational working guide: the enforced
  design-first workflow (docs change in the same commit as code), guardrails,
  architecture map, and current state.
- [`plugins/hgraph-development/`](plugins/hgraph-development/) — installable
  hgraph development skills for downstream Codex and Claude projects.
