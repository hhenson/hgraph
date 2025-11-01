# hgraph C++ Integration Plan and Current Status

This document tracks the staged migration/integration of the C++ runtime/type system (ported in `hg_cpp`) into this repository under `cpp/` and the subsequent packaging via scikit-build-core.

Last updated: 2025-10-31 15:23 local

## Goals
- Keep Python and C++ code separated (`python/` and `cpp/`), starting with `cpp/`.
- Build the C++ nanobind module `_hgraph` inside this repo using the existing CLion CMake profile.
- Stage 2: package the extension in the Python wheel via scikit-build-core so `import hgraph._hgraph` works.
- Avoid disrupting current Python packaging until Stage 2 is ready.

## Stage 1 — Integrate C++ sources and build in CLion

Status: Completed

What’s been done:
- Copied trees:
  - `cpp/include/` ← from `/Users/hhenson/CLionProjects/hg_cpp/include`
  - `cpp/src/` ← from `/Users/hhenson/CLionProjects/hg_cpp/src`
- Added CMake wiring in this repo:
  - Root: `CMakeLists.txt` (minimal — adds `cpp/` subdir)
  - `cpp/CMakeLists.txt` (adapted from `hg_cpp/CMakeLists.txt`)
    - Sets C++23, flags, macOS path hints
    - Finds Python 3.12 and nanobind via `python -m nanobind --cmake_dir`
    - Uses FetchContent for `fmt` and `backward-cpp` if not found via `find_package`
    - Generates `version.h` into `${CMAKE_CURRENT_BINARY_DIR}/generated/version.h`
    - Exposes `HGRAPH_INCLUDES` to subdirectory
  - `cpp/src/cpp/CMakeLists.txt` (copied; defines module `_hgraph` using `nanobind_add_module` and links `Threads`, `fmt`, `Backward`)

Pending/Blocked:
- Configure step failed due to CLion Debug profile referencing a missing vcpkg toolchain:
  - Missing: `/Users/hhenson/.vcpkg-clion/vcpkg/scripts/buildsystems/vcpkg.cmake`
  - Action: Remove toolchain in CLion profile, or point to a valid one.
- Need to pass a Python interpreter with `nanobind` installed via `-DPython_EXECUTABLE=...` during CMake configure to avoid CI/env ambiguity.

Recommended immediate actions:
1) Fix CLion profile (Debug):
   - Settings → Build, Execution, Deployment → CMake → Profiles → Debug
   - Clear the Toolchain File field (remove vcpkg), or supply a valid path if you intend to use vcpkg.
2) Ensure `nanobind` is installed in the interpreter you want to use and set:
   - CMake options: `-DPython_EXECUTABLE=/path/to/venv/bin/python`
   - Example: `/Users/hhenson/CLionProjects/hgraph/.venv/bin/python`
3) Reconfigure in CLion, then build target: `_hgraph`.

Notes:
- We did not bring over standalone C++ tests initially per your guidance (we will rely on `hgraph_unit_tests`).
- If in future we add `cpp/tests/cpp`, we will add `FetchContent` for Catch2 to make tests self-contained.

## Stage 2 — Packaging via scikit-build-core (Option B)

Status: Not started (awaiting Stage 1 success)

Planned steps:
1) Update `pyproject.toml` build backend to `scikit-build-core` while preserving all current project metadata and optional dependencies.
2) Configure scikit-build to build the nanobind module from `cpp/` and install it into the Python package namespace as `hgraph._hgraph`.
3) Validate local editable install and wheel build:
   - `pip install -U pip build
   - `pipx run build` or `pip wheel .`
   - `python -c "import hgraph, hgraph._hgraph; print('ok')"`
4) Ensure the wheel includes the extension for macOS/Linux (and optionally Windows if/when supported).

Key considerations for Stage 2:
- We will avoid relying on the “active venv” in CI by passing `-DPython_EXECUTABLE` explicitly in the build invocation.
- Nanobind discovery is driven by the provided interpreter (`python -m nanobind --cmake_dir`).
- The C++ target is currently named `_hgraph`. If we want flexibility, we can add a cache var (e.g., `HGRAPH_PYMOD_NAME`) in CMake to override.

## Stage 3 — Optional repo layout normalization

Status: Deferred

Planned changes:
- Create `python/` and move the current Python code under `python/src/hgraph`.
- Update `pyproject.toml` accordingly (e.g., `packages = ["python/src/hgraph"]`).
- Adjust docs/CI scripts referencing paths.

Rationale:
- Clear separation of concerns and easier CI/build scripts.
- This is optional and can be done after the C++ build is stable.

## Stage 4 — Tests and CI

Status: Planned

C++ Tests:
- Optional: add `cpp/tests/cpp` back and use FetchContent for Catch2 if useful for engine-level tests.

Python Tests:
- Continue using `hgraph_unit_tests`.
- Add a smoke test to import `hgraph._hgraph` to catch packaging regressions when Stage 2 is in place.

CI:
- Add a matrix job to configure and build the C++ module on macOS and Linux via CMake.
- For wheel builds, use scikit-build-core; pass `-DPython_EXECUTABLE` explicitly.
- Cache FetchContent dependencies where practical.

## Files added/modified in this repo

Added:
- `CMakeLists.txt` (root; minimal — adds `cpp/`)
- `cpp/CMakeLists.txt` (adapted from `hg_cpp/CMakeLists.txt`)

Copied:
- `cpp/include/**`
- `cpp/src/**`

Pre-existing (unchanged):
- `pyproject.toml` (Hatch; will change in Stage 2)
- `conan_provider.cmake` (not currently used by the new wiring; we rely on FetchContent/system packages for Stage 1)

## Known issues / Open items
- CLion Debug profile references a missing vcpkg toolchain — needs user action to clear or fix.
- Python interpreter selection for nanobind must be explicit in CI (and recommended locally) to avoid discovery flakiness.
- If we later enable `cpp/tests/cpp`, we’ll add Catch2 via FetchContent in their `CMakeLists.txt` for portability.

## Quick commands (manual configure/build)
If running CMake manually (outside CLion), using Ninja and a specific Python:

```
cmake -S . -B cmake-build-debug -G Ninja \
  -DPython_EXECUTABLE=/Users/hhenson/CLionProjects/hgraph/.venv/bin/python

cmake --build cmake-build-debug --target _hgraph -j 6
```

If Ninja is not the generator in your CLion profile, omit `-G Ninja` or match the profile’s generator.

---
Maintainer notes:
- All CMake paths in `cpp/CMakeLists.txt` use `${CMAKE_CURRENT_SOURCE_DIR}`/`${CMAKE_CURRENT_BINARY_DIR}` so the subtree is relocatable inside this repo.
- `version.h` is generated into the binary dir `generated/`; headers/sources include paths should reflect that as needed.
