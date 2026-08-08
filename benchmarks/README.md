# Comparative benchmark pack

The default performance comparison covers the Python-first hgraph 0.5.41 C++
runtime and the C++-first candidate. The 0.5 pure-Python runtime remains
available as an on-demand reference. Historical mode names are retained so
existing result files remain comparable:

| mode | implementation |
|---|---|
| `upstream-py` | pinned `hgraph==0.5.41`, Python runtime |
| `upstream-cpp` | the same package with `HGRAPH_USE_CPP=true` (the old C++ runtime) |
| `hg-cpp` | an optimized C++-first `hgraph` wheel built from current source |

Comparative scenarios are written **once**, in standard Python hgraph syntax
(`benchmarks/scenarios.py`), and run unchanged on all three implementations.
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
diagnostic workload is therefore restricted to the candidate and appears in a
separate, explicitly non-comparative report section. Low-level native timings,
allocation counts, and additional dynamic TSL/TSW operations remain in the
`type_erasure_perf` C++ benchmark.

Terminal outputs use the implementation's native `null_sink`, keeping sink
overhead out of the Python node boundary in every mode.

## Running

```sh
# from the repo root, in the C++-first hgraph environment:
uv run python benchmarks/orchestrate.py                 # 0.5 C++ vs candidate
uv run python benchmarks/orchestrate.py --scale 0.1     # quick legacy shorthand
uv run python benchmarks/orchestrate.py \
  --suite core --suite diagnostic                       # all workloads
uv run python benchmarks/orchestrate.py \
  --cycle-scale 2 --size-scale 0.5                      # independent axes
uv run python benchmarks/orchestrate.py \
  --group "TSD - key lifecycle" --samples 5
uv run python benchmarks/orchestrate.py --scenario tick_std --mode hg-cpp
uv run python benchmarks/orchestrate.py \
  --mode upstream-py --mode upstream-cpp --mode hg-cpp  # Python on demand
uv run python benchmarks/orchestrate.py \
  --refresh-baseline                                    # rerun legacy C++
uv run python benchmarks/runner.py --list               # readable inventory
```

`--cycle-scale` changes the number of engine cycles without changing graph or
collection size. `--size-scale` changes graph width/depth, TSD cardinality, or
service client count without changing cycle count. `--scale` sets both and is
kept for compatibility with older commands. Explicit `--scenario` filters
override suite/group selection.

The first run for each Python/platform/architecture combination creates
`benchmarks/.venv-upstream-X.Y-PLATFORM-ARCH` (installs `hgraph==0.5.41`) and
`benchmarks/.venv-hg-cpp-X.Y-PLATFORM-ARCH`. This prevents a repository shared
between macOS and a Linux VM from reusing an incompatible virtual environment.
The latter contains a Release wheel built from the current source and is rebuilt
whenever native, binding, or packaged Python source changes. The raw result
records both the source fingerprint and loaded native-module path so a result
cannot be mistaken for a stale editable build. All modes use the same
interpreter version. Delete the upstream directory to refresh its published
package. Results (markdown matrix + raw JSON) are written to
`benchmarks/results/`.

Successful upstream timings are cached in a platform-specific
`benchmarks/results/baseline-*.json` file. The cache identity includes the
installed hgraph version, Python/platform/architecture, CPU model, benchmark
scenario-pack fingerprint, scale factors, and sample count. A changed hgraph
version or scenario pack therefore reruns the baseline automatically; normal
candidate iterations reuse it. Use `--refresh-baseline` for a deliberate rerun, or
`--baseline-cache PATH` to keep a separate controlled baseline. Cache files are
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
# current Python, hgraph C++, and hg_cpp, plus the native structural pass
uv run python benchmarks/memory_orchestrate.py

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
The hg_cpp process pass also records cold-path node, graph, executor, and common
type-record cardinalities before the first run and after each teardown. The
``construct_std__novel_ten`` profile deliberately changes the graph shape on
each repetition, providing an intentional-growth control for the otherwise
identical repeated-wiring profiles.

RSS includes Python, native libraries, allocator fragmentation, and runtime
caches. It is the whole-process ground truth, but it cannot explain ownership.
For hg_cpp, a second fresh process attaches native `GraphDiagnostics` and
records planned graph bytes, peak dynamic live/reserved bytes, nested graph
capacity, and the largest dynamic owners. This pass is kept separate because
the collector owns one record per observed graph/node and would otherwise
inflate RSS.

The default matrix reports peak and retained memory for current Python,
hgraph C++, and hg_cpp. It includes explicit ``hg_cpp/Python`` and
``hg_cpp/hgraph-C++`` peak-memory ratios so improvements can be judged against
both reference runtimes. Either reference can still be selected independently
with ``--mode``.

Successful Python and hgraph-C++ measurements are cached in the platform-specific
`memory-baseline-*.json`. The cache identity includes the CPU, hgraph and
psutil versions, Python/platform/architecture, complete profile/scenario pack,
sampling policy, and sample count. Normal hg_cpp iterations therefore rerun
only hg_cpp. Raw data and markdown matrices use `memory-raw-*` and
`memory-matrix-*` names in `benchmarks/results/`.

Memory figures are diagnostic baselines, not hard CI thresholds. Resident
memory is page- and allocator-granular, so compare controlled runs on the same
host and use a scale series rather than a single small delta. The static audit
and interpretation guidance are in the developer guide's
``Memory utilisation and accounting`` page.
