# Repository Guidelines

## Project Structure & Source of Truth
`hgraph/` is the Python package (wiring, operators, runtime, types, adaptors). `hgraph_unit_tests/` is the main pytest suite and should mirror source layout. C++ runtime code is in `cpp/include/hgraph/` and `cpp/src/cpp/`, with C++-only tests in `cpp/tests/`. Docs live in `docs/source/`, and runnable samples are in `examples/`.

During the C++ port, Python behavior is the reference implementation. If Python and C++ differ, align C++ to Python unless requirements explicitly state otherwise.

## Environment & Feature Switches
Use Python 3.12 with `uv`:
- `uv venv --python 3.12`
- `uv sync --all-extras --all-groups`

At session start, verify `hgraph_features.yaml` keeps:
- `features.use_cpp: true`

Use runtime overrides for comparisons:
- `HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests` (C++ path by default)
- `HGRAPH_USE_CPP=0 uv run pytest hgraph_unit_tests` (Python path)

## Build, Test, and Validation Commands
- `uv build -v`: build wheel via `scikit-build-core`.
- `uv run pytest hgraph_unit_tests`: run core tests.
- `uv run pytest hgraph_unit_tests docs --cov=hgraph --cov-report=xml`: full test/coverage pass.
- `cmake -S cpp/tests -B cpp/tests/build && cmake --build cpp/tests/build && ctest --test-dir cpp/tests/build`: run standalone Catch2 tests.
- `make -C docs html`: build Sphinx docs.

For C++ debugging, a symlinked `_hgraph` binary in `.venv` is acceptable for faster rebuild cycles, but ensure tests execute against the intended binary.

## Coding Style & Test Placement
Python: 4-space indentation, `black` formatting (line length 120), `snake_case` modules, `test_*.py` files, and pytest function-based tests (prefer `@pytest.mark.parametrize` over repetitive cases).  
C++: C++23, `.clang-format` (4-space indent, 132-column limit), headers in `cpp/include/hgraph/`, implementations in `cpp/src/cpp/`.

Place tests near matching domains:
- `hgraph/_operators/` -> `hgraph_unit_tests/_operators/`
- `cpp/include/hgraph/types/value/` -> `hgraph_unit_tests/_types/_value/`
- `cpp/include/hgraph/types/time_series/` -> `hgraph_unit_tests/_types/_time_series/`

## Commit & Pull Request Guidelines
Use short imperative commit subjects (for example, `Fix reduce node checks`). Keep each commit focused on one logical change. PRs should include problem statement, approach, linked issues, and test evidence (including any Python-vs-C++ comparison runs). Do not commit local build artifacts or scratch files (`.venv/`, `build/`, `cmake-build-*`, `dist/`, ad hoc reports).
