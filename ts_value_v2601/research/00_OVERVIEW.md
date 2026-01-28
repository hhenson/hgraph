# Current Time-Series API Behavior (Python + C++ Proxy)

This folder documents the **current, observable behavior** of time-series types in two layers, **scoped to the API\n+surface exposed by the C++ Python wrappers**:

1. **Python implementation** (`hgraph/_impl/_types/*`) - the authoritative runtime behavior today.
2. **C++ proxy wrappers** (`cpp/src/cpp/api/python/*`) - the view-based wrappers exposed to Python.

Scope:
- Focused on **time-series behavior** for methods that exist in both Python and the C++ wrappers\n+  (`cpp/src/cpp/api/python/*`).\n+- Does **not** describe Python-only APIs that are not exposed via the wrappers.\n+- Does **not** describe implementation details beyond what is required to specify behavior.

References used:
- Python runtime: `hgraph/_impl/_types/_input.py`, `_output.py`, `_ts.py`, `_tsb.py`, `_tsl.py`, `_tsd.py`, `_tss.py`, `_tsw.py`, `_signal.py`, `_ref.py`.
- Type contracts: `hgraph/_types/_time_series_types.py`, `_ts_type.py`, `_tsd_type.py`, `_tss_type.py`, `_ref_type.py`.
- C++ proxy wrappers: `cpp/src/cpp/api/python/py_time_series.cpp`, `py_ts.cpp`, `py_tsb.cpp`, `py_tsl.cpp`, `py_tsd.cpp`, `py_tss.cpp`, `py_tsw.cpp`, `py_ref.cpp`, `py_signal.cpp`.

Structure:
- `01_BASE_TIME_SERIES.md` - shared properties, binding, active/passive, notifications.
- `02_TS_SCALAR.md` - TS/TS_OUT behavior.
- `03_TSB.md` - bundle behavior.
- `04_TSL.md` - list behavior.
- `05_TSD.md` - dict behavior.
- `06_TSS.md` - set behavior.
- `07_TSW.md` - window behavior.
- `08_REF.md` - reference behavior.
- `09_SIGNAL.md` - signal behavior.

Each document has two sections:
- **Python behavior (current)** - what the runtime does today.
- **C++ proxy behavior (current)** - what view-based wrappers expose (and any missing pieces).
