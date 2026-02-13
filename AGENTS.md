# Repository Guidelines

## Project Structure & Module Organization
- `hgraph/`: Python package and DSL wiring; primary runtime APIs.
- `cpp/`: C++ core runtime (`cpp/src`, `cpp/include`), built into the `_hgraph` extension.
- `hgraph_unit_tests/`: pytest test suite mirroring source layout.
- `docs/` and `docs_md/`: Sphinx and Markdown documentation sources.
- `examples/`: runnable examples and notebooks.
- `cmake/`, `CMakeLists.txt`, `pyproject.toml`: build and packaging configuration.

## Build, Test, and Development Commands
- `uv venv`: create a project-local virtual environment.
- `uv sync --all-extras --all-groups`: install all dependencies into the venv (preferred).
- `cmake --build cmake-build-debug`: rebuild the C++ extension from an existing CMake build dir.
- `uv run pytest hgraph_unit_tests`: run tests (set `HGRAPH_USE_CPP` explicitly to select runtime).
- `HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests`: run tests against the Python implementation.
- `HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests`: explicitly force the C++ runtime (recommended for C++ validation).
- `uv run pytest hgraph_unit_tests/_operators/test_const.py -v`: quick smoke check.
- `uv build -v`: build Python wheels via scikit-build-core.

## Fast Iteration: C++ Extension Symlink (Optional)
- `uv venv --python 3.12`: create the venv with the project’s target Python.
- `uv sync --all-extras --all-groups`: install dependencies (includes `hgraph`).
- `cmake --build cmake-build-debug`: build the C++ extension once.
- `rm .venv/lib/python3.12/site-packages/hgraph/_hgraph.cpython-312-darwin.so`: remove the installed extension.
- `ln -s \`pwd\`/cmake-build-debug/cpp/src/cpp/_hgraph.cpython-312-darwin.so \`pwd\`/.venv/lib/python3.12/site-packages/hgraph/_hgraph.cpython-312-darwin.so`: link the build artifact for rapid rebuilds.
- Adjust the extension filename for your OS/Python (e.g., `*.so` on Linux, `*.pyd` on Windows).
- Rebuild with CMake and rerun tests without reinstalling the package.

## Coding Style & Naming Conventions
- Python: 4-space indentation; format with Black (`line-length = 120`, `py312`, see `pyproject.toml`).
- Tests: function-based pytest style (no `unittest` classes); files and functions use `test_*` naming.
- Keep Python and C++ behavior aligned; Python implementation is the reference when comparing semantics.

## Testing Guidelines
- Framework: pytest with `testpaths = ["hgraph_unit_tests", "docs"]`.
- Coverage: `fail_under = 70` in coverage config.
- Test structure mirrors source (e.g., `hgraph/_operators/` -> `hgraph_unit_tests/_operators/`).

## Commit & Pull Request Guidelines
- Commit messages in recent history are short, lowercase phrases; keep them concise and descriptive.
- PRs should include a brief summary, test command/output, and linked issues if applicable.
- Note any changes that affect Python/C++ parity or feature flags.

## Configuration Notes
- Feature flags live in `hgraph_features.yaml` (`use_cpp: true` is the default for development).
- You can override with `HGRAPH_USE_CPP=0` to compare Python vs C++ behavior during tests.
