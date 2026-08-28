# Comparative benchmark pack

The default performance comparison covers the fixed published hgraph 0.8.19
release and the C++-first candidate built from current source. The Python-first
hgraph 0.5.41 Python and legacy-C++ runtimes remain available for reconstructing
the initial release baseline. Historical mode names are retained so existing
result files remain readable:

The fixed release pin moved from 0.8.1 to 0.8.19 on 2026-08-27. Result files
written before that date name 0.8.1 in their headers and remain valid records
of that comparison; they are not directly comparable with 0.8.19 cells. The
committed 0.8.1-versus-0.5.41 release baseline in
`results/baseline-summary-20260809.md` is a historical artifact and is not
reproducible with the current pin.

| mode | implementation |
|---|---|
| `upstream-py` | pinned `hgraph==0.5.41`, Python runtime |
| `upstream-cpp` | the same package with `HGRAPH_USE_CPP=true` (the old C++ runtime) |
| `release` | pinned published `hgraph==0.8.19` wheel; the fixed forward baseline |
| `hg-cpp` | an optimized C++-first `hgraph` wheel built from current source |

Comparative scenarios are written **once**, in standard Python hgraph syntax
(`benchmarks/scenarios.py`), and run unchanged on the applicable implementations.
The scenario registry gives every workload a stable command-line ID, a readable
label, a report group, a suite, and the set of runtimes that support it.
`*_std` scenarios are mostly graph/standard-operator workloads; `*_py`
scenarios deliberately put work in Python-authored nodes.

The **core** suite covers graph construction, scheduler hot loops, scalar and
compound values, dense/sparse/churning TSDs, switch and mesh nested graphs,
services, and adaptors. The **diagnostic** suite decomposes the hot paths and
adds fan-in/fan-out/conflation, Python boundary costs, TSB/TSS/TSW behavior,
large retained TSD capacity, capacity growth, clear/repopulate, multi-input
membership, explicit key sets, reducer implementation shapes, and multi-path
services.

Dynamic TSL is a C++-first feature with no valid 0.5 comparison. Its
diagnostic workload is therefore restricted to 0.8.19 and current source and
appears in a separate, explicitly non-comparative report section. Low-level native timings,
allocation counts, and additional dynamic TSL/TSW operations remain in the
`type_erasure_perf` C++ benchmark.

Terminal outputs use the implementation's native `null_sink`, keeping sink
overhead out of the Python node boundary in every mode.

## Running

```sh
# from the repo root, in the C++-first hgraph environment:
uv run python benchmarks/orchestrate.py                 # 0.8.19 vs current source
uv run python benchmarks/orchestrate.py --scale 0.1     # quick legacy shorthand
uv run python benchmarks/orchestrate.py \
  --suite core --suite diagnostic                       # all workloads
uv run python benchmarks/orchestrate.py \
  --cycle-scale 2 --size-scale 0.5                      # independent axes
uv run python benchmarks/orchestrate.py \
  --group "TSD - key lifecycle" --samples 5
uv run python benchmarks/orchestrate.py --scenario tick_std --mode hg-cpp
uv run python benchmarks/orchestrate.py \
  --mode upstream-py --mode upstream-cpp --mode release # reconstruct baseline
uv run python benchmarks/orchestrate.py \
  --refresh-baseline                                    # rerun fixed releases
uv run python benchmarks/runner.py --list               # readable inventory
```

`--cycle-scale` changes the number of engine cycles without changing graph or
collection size. `--size-scale` changes graph width/depth, TSD cardinality, or
service client count without changing cycle count. `--scale` sets both and is
kept for compatibility with older commands. Explicit `--scenario` filters
override suite/group selection.

The first run for each Python/platform/architecture combination creates
`benchmarks/.venv-upstream-X.Y-PLATFORM-ARCH` (installs `hgraph==0.5.41`) and
`benchmarks/.venv-release-0.8.19-X.Y-PLATFORM-ARCH` (installs the published
`hgraph==0.8.19` wheel). Current-source comparisons additionally create
`benchmarks/.venv-hg-cpp-X.Y-PLATFORM-ARCH`. This prevents a repository shared
between macOS and a Linux VM from reusing an incompatible virtual environment.
The two released environments use platform-specific wheel URLs and SHA-256
digests pinned in `orchestrate.py`; their reports and raw samples record those
artifact identities.
The latter contains a Release wheel built from the current source and is rebuilt
whenever native, binding, or packaged Python source changes. The raw result
records both the source fingerprint and a sanitized loaded-native-module path
so a result cannot be mistaken for a stale editable build. The recorded
compiler is read from the built extension itself where the object format
carries it (ELF ``.comment``), falling back to probing ``CXX``/``c++``:
those aliases can name a different toolchain than CMake selected, and a
misreported compiler makes a build difference look like a code change.
The ``+dirty`` suffix ignores ``benchmarks/results/``, so a campaign's own
matrices do not mark the next run in the same checkout as dirty. Paths within the
checkout use the portable ``<repo>/...`` form; external module locations are
reduced to their filename. Committed artifacts retain platform, CPU, compiler,
and package identities for reproducibility but exclude hostnames, machine login
names, addresses, credentials, and absolute home or workspace
paths. All modes use the same interpreter version. Delete the upstream
directory to refresh its published package. Results (markdown matrix + raw
JSON) are written to `benchmarks/results/`.

Successful fixed-release timings are cached in a platform-specific
`benchmarks/results/baseline-*.json` file. The cache identity includes the
installed hgraph versions, Python/platform/architecture, CPU model, benchmark
scenario-pack fingerprint, scale factors, and sample count. A changed hgraph
version or scenario pack therefore reruns the baseline automatically; normal
candidate iterations reuse the 0.8.19 cells. Use `--refresh-baseline` for a deliberate rerun, or
`--baseline-cache benchmarks/results/NAME.json` to keep a separate controlled
baseline. Cache paths are restricted to `benchmarks/results/`. Cache files are
local measurement artifacts and are ignored by Git.

Each timing sample runs in a fresh subprocess. The orchestrator rotates mode
order deterministically between samples, reports the median and median absolute
deviation, and preserves every individual result in the raw JSON. A failure in
any sample makes the aggregate cell fail rather than being hidden by successful
samples. Unsupported runtimes show as `N/A`, not `FAIL`.

Before timing, the orchestrator runs the small `eval_node` workload guards in
every selected mode. These verify emitted values, service callback counts,
dense and sparse updates, key churn, and mesh dependencies. Use
`--skip-validation` only for repeated local timing after a successful
preflight.

**Timings are not CI gates.** The workload guards run in the normal Python
test suite, while timing exists for occasional controlled runs, not pass/fail
gating. Keep scenario definitions stable between runs you intend to compare.
Use the independent scale controls rather than editing scenario defaults.

The timed interval is the complete `run_graph` call, so it includes graph
construction, startup, steady-state execution, and teardown. Dedicated graph
construction scenarios expose fixed setup costs; use longer cycle scales when
the objective is steady-state throughput. The C++ microbenchmark pack is the
appropriate tool for operation-level timing and allocation counts.

## Memory-utilisation campaign

The memory campaign is deliberately separate from the timing interval while
reusing the same stable scenario implementations. Its profile registry
(`memory_profiles.py`) selects scale series for static graph size, bounded
duration, value representations, collection cardinality, retained capacity,
key churn, monotonic growth, repeated graph lifecycles in a long-lived process,
nested graphs, mesh, and services.

```sh
# fixed hgraph 0.8.19 and current source, plus the current structural pass
uv run python benchmarks/memory_orchestrate.py

# compare the released lines: 0.8.19 against 0.5.41
uv run python benchmarks/memory_orchestrate.py \
  --mode upstream-py --mode upstream-cpp --mode release

# focused iteration
uv run python benchmarks/memory_orchestrate.py \
  --group "Keyed collections" --samples 5
uv run python benchmarks/memory_orchestrate.py \
  --profile tsd_churn_std__long --mode hg-cpp

# setup benchmark environments without measuring
uv run python benchmarks/memory_orchestrate.py --setup-only

# force a new released-runtime memory baseline
uv run python benchmarks/memory_orchestrate.py --refresh-baseline
```

Each process sample starts a new interpreter. After importing and building the
scenario callable, a background sampler records RSS during `run_graph` (5 ms by
default). The result records pre-run memory, peak RSS, post-run memory, and
memory retained after graph teardown plus two Python GC passes. USS and PSS are
also recorded where the operating system exposes them. Multiple samples are
aggregated with a median and median absolute deviation; every original sample
remains in the raw JSON.

Profiles with ``repetitions > 1`` reuse the same graph callable inside one
interpreter and record post-GC RSS/USS after every execution. Their report rows
include first-to-last retained growth, exposing process-lifetime registry or
cache slopes that independent process samples intentionally hide.
The C++-first process pass also records cold-path node, graph, executor, and common
type-record cardinalities before the first run and after each teardown. The
``construct_std__novel_ten`` profile deliberately changes the graph shape on
each repetition, providing an intentional-growth control for the otherwise
identical repeated-wiring profiles.

RSS includes Python, native libraries, allocator fragmentation, and runtime
caches. It is the whole-process ground truth, but it cannot explain ownership.
For C++-first hgraph, a second fresh process attaches native `GraphDiagnostics` and
records planned graph bytes, peak dynamic live/reserved bytes, nested graph
capacity, and the largest dynamic owners. This pass is kept separate because
the collector owns one record per observed graph/node and would otherwise
inflate RSS.

The default matrix reports peak and retained memory for the fixed 0.8.19 release
and current source. When the 0.5 modes are selected to reconstruct the initial
baseline, it reports explicit 0.8.19/Python and 0.8.19/legacy-C++ peak-memory
ratios. Any mode can be selected independently with ``--mode``.

Successful fixed-release measurements are cached in the platform-specific
`memory-baseline-*.json`. The cache identity includes the CPU, hgraph and
psutil versions, Python/platform/architecture, complete profile/scenario pack,
sampling policy, and sample count. Normal current-source iterations therefore
rerun only the current source. Raw data and markdown matrices use `memory-raw-*` and
`memory-matrix-*` names in `benchmarks/results/`.

Memory figures are diagnostic baselines, not hard CI thresholds. Resident
memory is page- and allocator-granular, so compare controlled runs on the same
host and use a scale series rather than a single small delta. The static audit
and interpretation guidance are in the developer guide's
``Memory utilisation and accounting`` page.
