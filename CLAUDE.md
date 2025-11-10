# AI Helper

**Last Updated:** 2025-10-16
**Purpose:** Quick-start reference for initializing debugging/development sessions on the hgraph project.

---

## Project Overview

### What is hgraph?
An event-based processor organized around the concept of a forward propagation graph. It implements functional
reactive programming concepts using a Python DSL. The user is expected to make use of the defined Python API to
construct the event processing logic. Events are wrapped up in the concept of a time-series. A time-series represents
a sequence of events that occur at a specific time. The value property of a time-series is the cumulation of the events
applied on the time-series, the delta_value is effectively the event that is currently being procesed.

The core of the hgraph library is implemented in C++. This covers the types, schedulers and other runtime elements,
whilst the wiring, type-meta-data system and many functions are implemented in Python.

### Key Principles

1. The code is only recently being ported to C++, that has left the reference python implementation in the python portion
of the logic. For now the logical behavior of the C++ and Python implementation need to be kept the same. When an issue
is identified, for now the python implementation is considered authoritative.
2. The C++ runtime is enabled via the feature flag found in hgraph_features.yaml (use_cpp), set to true to use cpp.
   Altenatively, it is possible to set the environment variable HGRAPH_USE_CPP to true.

----

## First Time Setup

When starting a session, I find it useful to link the CMake produced artifact (_hgraph lib) to the instance in the
python virtual environment. This reduces compile test times as we can just run cmake to incrementall re-build the c++
code after a modification.

```bash
# 1. Create virtual environment with uv
uv venv --python 3.12

# 2. Install Python dependencies (including hgraph)
uv sync --all-extras --all-groups

# 3. Build C++ library with CMake (assuming a debug build)
cmake --build cmake-build-debug

# 4. Remove the installed library
rm .venv/lib/python3.12/site-packages/hgraph/_hgraph.cpython-312-darwin.so

# 5. Create symlink for fast iteration (recommended)
ln -s `pwd`/cmake-build-debug/cpp/src/cpp/_hgraph.cpython-312-darwin.so `pwd`/.venv/lib/python3.12/site-packages/hgraph/_hgraph.cpython-312-darwin.so

# 5. Verify installation
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests/_operators/test_const.py -v
```

## Feature Switches

The project has a concept of a feature switch to be able to turn on and off features. These can be defined in a number
of ways, but the most useful to an agent is the environment variable.

The current key feature is the use of C++ for the core runtime. This is controlled by the env var ``HGRAPH_USE_CPP=1``.
When set, the code will use the C++ implementation; when it is off, the python implementation is used.
This is important to check when validating re-factoring as it is easy to forget to set the env var and not test the change.

## Testing

The tests are currently all in python and can be found in ``hgraph_unit_tests``.

```bash
HGRAPH_USE_CPP=1 uv run pytest hgraph_unit_tests
```

## Project Structure

The project model uses ``uv`` to manage the project. This uses the ``pyproject.toml`` file to define the project structure.
The C++ build is managed by CMake, and the integration between uv and CMake is the ``scikit-build-core`` builder.
We use pytest for running the python unit tests.

The project is index using the ``Context7`` mcp. Use this mcp for project-specific API and documentation, 
it indexes most open source projects.

## Debugging / Development

Techniques to apply:

1. Tracing: The python code is generally speaking correct, and does make the unit tests pass. When there is a bug in 
            the c++ code, putting trace code into the Python implementation and comparing it to the equivalent C++ code
            can help identify the differences in behavior.
2. Validation: When making changes to the code, always compile the changes to make sure the code works, then run the
               unit tests to ensure all non-xfail / skipped tests are still working (and remember to set the env var
               ``HGRAPH_USE_CPP=1``). You can't report success if there are failing tests.
3. Checks: Always make sure the env is set up correctly, this can be a quick spot check that the symlink for the
           .so is in the right place and linked. Otherwise, it results in checks against an incorrect version of 
           the code.

