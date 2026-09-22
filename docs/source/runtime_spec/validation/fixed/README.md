# Fixed-collection comparisons

Status: 36 scenarios recorded on 2026-09-22; acceptance is pending three
semantic decisions. No runtime implementation has changed.

Each scenario ran three times in separate Python and C++ processes. All replay
fingerprints are stable. Python is released hgraph 0.5.41; C++ uses the installed
SDK with Python observer nodes. Binary hashes and source context are recorded
in [observed.json](observed.json). A runtime failure is preserved, including any
observed prefix; missing later observations are not agreement.

Of 8,966 asserted observations, 8,621 agree on all three sides and 333 agree
with reasoning and one runtime. Eight have three different results; four
remain unvalidated after Python's bundle-invalidation exception. Counts are
per observation, not per accepted scenario. A case with unresolved fields is
not accepted by combining its individually matching fields.

## Accepted variations

| Contract | Agreement | Variation |
|---|---|---|
| TSB value contains valid fields only, including nested bundles | Reasoning + Python | C++ includes invalid fields with nil values |
| Invalidating an owned child ticks its owned parent, with no invalid child in the delta | Reasoning + C++ | Python leaves the parent's previous time and no delta |
| An assembled input derives time and modification from current children | Revised reasoning + Python | C++ records an invalidation time on invalid children and their parent |
| Sampling a fixed collection reports its valid children's sampled deltas | Reasoning + Python | C++ bridge reports empty deltas at some sampled collection levels |
| An empty REF leaves a fixed input unbound with time `never` | Reasoning + Python | C++ may report peered and retain previous observed times |
| Publishing an equal REF value ticks the REF but does not resample its target | Reasoning + Python | C++ suppresses the equal REF publication in this authoring path |
| Whole TSB invalidation completes | Reasoning + C++ for completion | Python raises `AttributeError: ... has no attribute '_ts_value'`; no later Python observations exist |

Equal returned REF values exercise the compute-result publication path. The
C++ suppression there is recorded separately from target sampling; these
graphs do not isolate whether suppression occurs before native publication.

Whole versus assembled nesting, immediate-child `all_valid`, heterogeneous
fields, passive polling, TSD membership around fixed collections and all four
switch/map timer traces match their reasoned results, subject to the value
format variations above. Fixed collections containing TSDs also preserve their
positions while dictionary keys come and go.

The assembled-time correction is explicit in
[reasoning_corrections.json](reasoning_corrections.json): notification alone
is not a tick. Both implementations agree when one child reference becomes
empty and the remaining child is idle. Separate `latest_invalid` scenarios
confirm the consequence: Python reveals the older sibling time; C++ varies.
Initial expectations remain unchanged in [reasoned.json](reasoned.json).

## Decisions still required

| Case | Reasoned result | Python | C++ |
|---|---|---|---|
| Whole TSL invalidation at t7 | Invalid, unmodified, time `never` | Children invalid; root still valid at t6 | Invalid, modified, time t7 |
| Whole A changes to child bindings A.left + B.right at t4 | Preserve unchanged left binding; delta contains right only | Resamples both children | Preserves left child but parent delta is empty |
| Nested rebind from a valid child to an invalid target | Invalid child has time `never`, is unmodified and contributes no delta | Reports binding time t4; may include empty child delta | Retains old child time t1 |

The first decision also governs TSB invalidation. Python's exception leaves
four assertions without agreement, including two later value observations.
Those missing results remain unvalidated; another case is not substituted for
the missing part of this trace. The eight three-way differences are grouped
by the three decisions above. No user ruling has been recorded yet.

## Evidence and replay

- [reasoned.json](reasoned.json): literal expectations written before each
  scenario's first execution; formatting helpers expand listed states only.
- [expand.py](expand.py): derives assertions from those expectations, never
  from observations. Whole value/delta comparisons and child-key checks detect
  extra fields as well as missing ones. It can print the full assertion list.
- [observed.json](observed.json), [assessment.json](assessment.json): raw
  logical states, comparison counts and every non-unanimous observation. `python check.py`
  regenerates the assessment and currently exits 1 for unresolved evidence.
- [decisions.json](decisions.json): reserved for explicit user rulings.
- [adapter.patch](adapter.patch): bounded public-API catalogue additions;
  recipes cannot supply executable code. Input and output observation do not
  implement a second runtime model.

Use an isolated hgraph checkout at `15e7bf41b2b17145f6e3f2742f08ec2826b30a71`.
The adapter is standalone; do not apply the earlier dynamic-case adapter first.

```sh
git apply /path/to/fixed/adapter.patch
python -m tools.parity validate /path/to/fixed/recipes
python /path/to/fixed/replay.py --harness /path/to/hgraph-checkout \
  --reference-python /path/to/python-hgraph/bin/python \
  --candidate-python /path/to/cpp-hgraph/bin/python \
  --raw-results /path/to/private-replay-results
python /path/to/fixed/check.py
python -m unittest discover -s /path/to/fixed -p 'test_*.py'
```

The shared adapter uses REF inputs to obtain opaque reference values. Whole
and child references are formed by public wiring, including an explicitly
empty scalar REF. A never-valid scalar output is a different target. Discovery
runs that used unsupported reference-constructor arguments or reused a bundle
schema name are excluded; the recorded final adapter replays from a clean base.
Logical reads guard invalid values and idle deltas; empty modified deltas and
all timestamps remain visible. Maps change only their JSON key representation.

The supplementary [native probe](native_probe.cpp) bypasses the Python bridge.
Its [three identical runs](native_observed.txt) show a full native sampled
parent value but unsampled child times. They also confirm the C++ invalidation
time discrepancy. These endpoint observations are distinct from graph-level
recursive sampling and are not counted as another voting implementation.

```sh
cmake -S /path/to/fixed -B /tmp/fixed-native-build \
  -Dhgraph_DIR=/path/to/sdk/lib/cmake/hgraph \
  -DPython_EXECUTABLE=/path/to/cpp-hgraph/bin/python
cmake --build /tmp/fixed-native-build --parallel 2
/tmp/fixed-native-build/runtime_contract_probe
```
