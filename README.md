# hgraph
A functional reactive programming engine with a Python front-end.

This provides a DSL and runtime to support the computation of results over time, featuring
a graph based directed acyclic dependency graph and the concept of time-series properties.
The language is function-based, and promotes composition to extend behaviour.

Here is a simple example:

```python
from hgraph import graph, evaluate_graph, GraphConfiguration, const, debug_print

@graph
def main():
    a = const(1)
    c = a + 2
    debug_print("a + 2", c)

evaluate_graph(main, GraphConfiguration())
```
Results in:
```
[1970-01-01 00:00:00.000385][1970-01-01 00:00:00.000001] a + 2: 3
```

See [this](https://hgraph.readthedocs.io/en/latest/) for more information.

## Development

The project is currently configured to make use of [uv](https://github.com/astral-sh/uv) for dependency management. 
Take a look at the website to see how best to install the tool.

Here are some useful commands:

First, create a virtual environment in the project directory:

```bash
uv venv
```

Then use the following command to install the project and its dependencies:

```bash
# Install the project with all dependencies
uv pip install -e .

# Install with optional dependencies
uv pip install -e ".[docs,web,notebook]"

# Install with all optional dependencies
uv pip install -e ".[docs,web,notebook,test]"
```

PyCharm can make use of the virtual environment created by uv to ``setup`` the project.

### Run Tests

```bash
# No Coverage
python -m pytest
```

```bash
# Generate Coverage Report
python -m pytest --cov=hgraph --cov-report=xml
```


## Indexing with Context7 MCP

This repository includes a baseline configuration for Context7 MCP to improve code search and retrieval quality.

- See docs/context7_indexing.md for guidance.
- The root-level context7.yaml config sets sensible include/exclude rules, priorities, and summarization hints.


## Packaging, CI, and Releases

This project uses scikit-build-core and cibuildwheel to build and publish cross-platform wheels, with uv as the package/venv manager.

- Local build (PEP 517):
  ```bash
  # Build a wheel into dist/
  uv build -v
  # Optionally build sdist too
  uv build -v --sdist
  ```

- Optional Conan (developer builds only):
  ```bash
  # Conan is not required for packaging but may be used in local builds
  uv tool install conan
  # Example of enabling Conan provider for a local build
  CMAKE_ARGS="-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=conan_provider.cmake" uv build -v
  ```

- macOS notes:
  - Wheels target Apple Silicon (arm64) and require a macOS deployment target high enough for libc++ floating-point `to_chars` used by `std::format`.
  - The build system enforces `MACOSX_DEPLOYMENT_TARGET=15.0` (arm64) during Release Wheels and sets a default in CMake if unset.
  - You can override locally via:
    ```bash
    CMAKE_ARGS="-DCMAKE_OSX_DEPLOYMENT_TARGET=15.0 -DCMAKE_OSX_ARCHITECTURES=arm64" uv build -v
    ```

- Windows notes:
  - Build uses MSVC (Visual Studio 2022, x64). No MinGW.
  - If stack traces via backward-cpp are enabled, Windows links `Dbghelp` and `Imagehlp` automatically.

- Linux notes:
  - Release wheels are built inside manylinux containers; optional `backward-cpp` integrations are disabled there for portability.

### CI summary
- `.github/workflows/ci.yml` builds wheels on Linux/macOS/Windows for smoke validation, runs tests on Linux only (fast path), and repairs Linux wheels with auditwheel.
- `.github/workflows/release-wheels.yml` builds publish-grade wheels with cibuildwheel (Linux manylinux, macOS arm64, Windows AMD64), builds an sdist, validates with twine, and publishes to PyPI via Trusted Publishing.

### Releasing (tag-driven)
Releases are automated via GitHub Actions with PyPI Trusted Publishing.

Prerequisites:
- Enable Trusted Publishing for this GitHub repository in the PyPI project settings (Project → Settings → Trusted Publishers). No API token is needed once OIDC is configured.
- Ensure `pyproject.toml` `[project].version` matches the tag you plan to push (e.g., `0.4.112`).

Steps:
```bash
# Update version in pyproject.toml to match your release
# Then tag and push the tag to trigger the release workflow

git tag -a v_0.4.112 -m "release 0.4.112"
git push origin v_0.4.112
```

The workflow will:
- Build wheels for Linux (manylinux), macOS (arm64, macOS 15.0 target), and Windows (AMD64, MSVC)
- Build an sdist
- Validate with `twine check`
- Publish all distributions to PyPI (skips existing files)

Artifacts are uploaded for inspection as part of the workflow run.

### CLion configuration tips
- Use the project venv Python for nanobind discovery:
  - CMake option: `-DPython_EXECUTABLE=${PROJECT_ROOT}/.venv/bin/python`
- If you need Conan, use the preamble to ensure Python/nanobind are discovered first, then the Conan provider:
  - CMake option: `-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES=${PROJECT_ROOT}/conan_preamble.cmake;${PROJECT_ROOT}/conan_provider.cmake`
- Select the `_hgraph` target to build the extension.

### Troubleshooting
- Nanobind not found / Python ordering in CMake:
  - Pass `-DPython_EXECUTABLE=...` (or `-DPython3_EXECUTABLE=...`) and, if using Conan, use the `conan_preamble.cmake` before `conan_provider.cmake`.
- I also find it helpful to perform an uv sync with at least --all-groups
  before setting up the cmake.
