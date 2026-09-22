# Fixed-collection comparisons

Status: 44 scenarios recorded on 2026-09-22; user rulings recorded the same
day. The contract decisions are settled; runtime implementation is unchanged.

Each scenario ran three times in separate Python and C++ processes. All replay
fingerprints are stable. Python reports hgraph 0.5.41; source and distribution
artifact hashes identify the tested installation. C++ uses the installed SDK
with Python observer nodes. [observed.json](observed.json) records binary
hashes and source context. Original expectations and measurements are retained;
[decisions.json](decisions.json) records the rulings and changed expectations.

Of 12,142 observations, 11,375 match both runtimes, 439 match one, and ten are
accepted by explicit ruling despite neither runtime matching. Python's bundle-
invalidation exceptions leave 318 observations unvalidated. Acceptance is per
observation; these counts do not establish whole-case conformance.

## Accepted contracts and variations

| Contract | Support | Variation |
|---|---|---|
| A valid TSB value preserves every declared field; invalid children occupy nil fields, recursively (TS-24) | User ruling + C++ | Python omits invalid fields |
| Invalidating an owned child ticks its owned parent, with no invalid child in its delta (TS-7) | Reasoning + C++ | Python retains the parent's previous time and no delta |
| A still-valid assembled structure retains child-change time; a wholly invalid one resets to `never` (TS-3, TS-26) | User ruling + C++ while valid; Python on whole invalidity | Python loses the change observation; C++ fails to clear it when wholly invalid |
| Sampling a fixed collection reports its valid children's sampled deltas (TS-14) | Reasoning + Python | C++ reports empty deltas at some sampled collection levels |
| An empty REF leaves a fixed input unbound with time `never` | Reasoning + Python | C++ may retain peering and previous times |
| An equal REF designation causes no additional tick or sampling (TS-16) | User ruling + C++ authoring path | Python publishes the equal REF again |
| Whole TSL/TSB invalidation resets every level: invalid, unmodified, time `never`, nil value/delta (TS-26) | User ruling | Python TSL retains root validity/time; C++ records an invalidation tick; Python TSB raises |
| Whole A → A.left + B.right preserves left and samples right; parent delta contains right only (TS-25) | User ruling | Python resamples both; C++ preserves left but omits the parent delta |
| Rebinding to an invalid nested target clears old sample state; no time or sampled delta (TS-14) | User ruling | Python records binding time/empty deltas; C++ retains old binding times |

A TSB is a structure, not a sparse map; its delta remains sparse. A REF carries
routing identity, so repeating it adds no information. Equal ordinary values
may still be signals. The REF comparison measures compute-result publication;
it does not isolate where C++ suppresses the duplicate.

Non-peered collections remove a redundant assembly node and keep local state.
Caching time and modification is permitted; reads need not scan children.
One consumer usually favours this arrangement; several may favour sharing a
node and output. This is a cost choice, not a measured performance claim.
Whole invalidation and rebinding to invalid targets still reset cached state.

## Invalidation boundary

Eight `*_invalidate` cases cover every two-level TSL/TSB shape, owned and
assembled. After all four leaves are valid, t7 invalidates the owned root or
all independently bound leaves; t8 is idle. Both cycles assert reset state at
the root, both children and all four grandchildren. The existing TS-26 ruling
supplies the expectation; no runtime result was used to generate it.
Python stops at t7 in the three owned shapes containing TSB. Their 264 missing
observations join the original 54; C++'s later observations do not fill them.

In `*_assembled`, left becomes invalid at t4 while right stays valid. The
invalid child view and parent read modified at t4; the parent delta is empty.
At t5 right also becomes invalid: the whole structure and all local child
observations reset to unmodified/time `never`. Revalidating left at t6 must
not restore right's old cached time. In `*_latest_invalid`, the parent stays
valid and retains t2 through the idle t3. These are observable rules; caching
is how an implementation may maintain them.

The earlier [reasoning corrections](reasoning_corrections.json) remain as
history; user rulings take precedence. Missing Python observations after the
exception remain missing, even where C++ matches the chosen contract.

## Evidence and replay

- [reasoned.json](reasoned.json): literal expectations written before each
  scenario's first execution; formatting helpers expand listed states only.
- [expand.py](expand.py): derives assertions from those expectations, never
  from observations. Whole value/delta comparisons and child-key checks detect
  extra fields as well as missing ones. It can print the full assertion list.
- [observed.json](observed.json), [assessment.json](assessment.json): raw
  logical states, comparison counts and every non-unanimous observation. `python check.py`
  regenerates the assessment and exits 1 for the 318 missing observations.
- [decisions.json](decisions.json): explicit user rulings, rule IDs and prior/new
  expectations. The checker retains the raw comparison and cannot use a ruling
  to fill absent evidence.
- [adapter.patch](adapter.patch): bounded public-API catalogue additions;
  recipes cannot supply executable code. Input and output observation do not
  implement a second runtime model.

Use an isolated hgraph checkout at `15e7bf41b2b17145f6e3f2742f08ec2826b30a71`.
The adapter is standalone; do not apply the earlier dynamic-case adapter first.
Replay verifies the required HEAD and every tracked or non-ignored file against
base plus adapter, including contents and file modes. It then runs an isolated
copy of that exact tree; ignored files in the supplied checkout cannot enter
its imports. The resulting tree and file-manifest identities are recorded.

Both interpreters hash the actual hgraph package sources, including editable
sources, and installed distribution artifacts. The candidate also hashes its
native binaries. Version or Git HEAD alone does not identify either runtime.
Bytecode caches are excluded. Both identities are captured before and after
replay; a change rejects the run before replacing the published evidence.
Publication atomically replaces the file after a complete temporary write.

Interpreter and harness paths are trusted local configuration: they select code
to execute, never recipe data. Paths are made absolute without resolving venv
interpreter symlinks. The launcher uses argument lists with no shell; the Git
probe uses a working directory rather than path arguments.

Native provenance hashes the extension and installed hgraph libraries beside
it or in `lib`, `bin` and `hgraph.libs`: macOS dylibs, Linux shared objects
(including versioned names) and Windows DLLs. These are artifact hashes, not a
loaded-library audit.

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

A graph replay omits prior native-probe provenance; rebuild and rerun the
supplement for a new candidate.
When present, its source and output hashes and three-run stability must verify.

The supplementary [native probe](native_probe.cpp) bypasses the Python bridge.
Its [three identical runs](native_observed.txt) show a full native sampled
parent value but unsampled child times, plus the C++ invalidation-time result. These endpoint observations are distinct from graph-level
recursive sampling and are not counted as another voting implementation.

```sh
cmake -S /path/to/fixed -B /tmp/fixed-native-build \
  -Dhgraph_DIR=/path/to/sdk/lib/cmake/hgraph \
  -DPython_EXECUTABLE=/path/to/cpp-hgraph/bin/python
cmake --build /tmp/fixed-native-build --parallel 2
/tmp/fixed-native-build/runtime_contract_probe
```
