# Linux generator cost investigation

The remaining generator-only slowdown is predominantly sensitive to native runtime code placement. It does not require extra graph/node evaluations or a change to Python generator bridge code. Relinking the candidate's existing compiled inputs with a 384-byte shift of runtime function addresses reduces the original benchmark's 5–7% gap to about 1%. Independent checks on another CPU and with process address randomization disabled confirm the effect.

This is a diagnosis, not a shipped optimization: the PR retains the normal linker layout and the measured Linux regression. Arbitrary linker padding is not a portable performance fix. Hardware counters were unavailable under the validation host's permissions, so the specific instruction-cache, fetch or branch-prediction mechanism is **not established**.

## Controls and scope

Baseline: `7e45748c966e8cf6c3cb2fbf57adfbc259302040`. Candidate: `64c84e118d037d75cac2af2146d255fc5d63209b`, including the empty-removal scan guard and latest main integration. The runtime code is unchanged from `5deb2bc88`.

Measurements use native Linux x86_64 on an AMD Ryzen Threadripper 9980X, Python 3.14.4 in both environments. Each timing sample starts a fresh process, pinned to one allowed CPU. Variant order is seeded and shuffled within each repetition. All samples and section hashes are in [generator-analysis.json](generator-analysis.json); sample arrays are in repetition order. Ratios are median elapsed time divided by the matching baseline median; lower is better. No observer or profiler runs during timing.

The existing `python_generator_boundary` scenario connects `_int_pulse(cycles)` to `null_sink`. It has no map, dictionary, collection lookup or collection reconciliation. The runner includes graph construction, execution and teardown, while process startup/imports precede its timer. Cycle scales 1, 10 and 100 mean 20,000, 200,000 and 2,000,000 cycles respectively:

```sh
python benchmarks/runner.py --scenario python_generator_boundary --cycle-scale 100
```

The Python bridge, wiring and standard-operator libraries have byte-identical ELF `.text` sections between baseline and candidate. Only runtime `.text` differs, growing by 3,328 bytes. This excludes a changed bridge instruction sequence as the explanation, but identical code can still be affected by the addresses of code it calls.

Separate untimed runs with lifecycle observers count exactly 2,000 graph evaluations, 2,000 generator evaluations, 2,000 sink evaluations and one start per node in both versions. These establish equal evaluation counts, not equal hardware instruction counts.

## Runtime-library swaps

Fifteen samples per variant, two million cycles each. Substitute only `libhgraph_runtime.so` via a subprocess-local `LD_LIBRARY_PATH`; leave the interpreter, bridge and other libraries in the named environment unchanged.

| Environment / runtime | Median seconds | Ratio |
|---|---:|---:|
| `baseline` | 0.394271 | 1.000× |
| `candidate` | 0.404579 | 1.026× |
| `candidate_with_baseline_runtime` | 0.395479 | 1.003× |
| `baseline_with_candidate_runtime` | 0.403605 | 1.024× |

The slowdown follows the candidate runtime across the two Python environments. This series shows a 2–3% difference; later controlled series show 5–7%. The magnitude varies with execution conditions, so it is not a universal fixed surcharge per generator tick.

## Controlled runtime relinks

Reuse the candidate's runtime object files and original Ninja link command. Redirect the output/dependency file to a diagnostic directory, resolve the original Arrow dependency to the installed matching library, and bound LTO workers to 16. The unpadded control reproduces the installed candidate `.text` SHA-256 exactly. Then add only this linker-script option, varying `N`:

```text
SECTIONS { .text.layout_pad : { BYTE(0x90); . += N-1; } } INSERT BEFORE .text;
```

The padding is never executed. All 13,695 inspected function symbols retain their sizes and their relative arrangement; their addresses move uniformly by the recorded shift. Requested shifts of 16, 32 and 64 bytes all produce a 64-byte shift because of text alignment. Larger tested shifts match their requested size. The source and compiled link inputs are unchanged; relocated machine-code operands need not be byte-identical. Other ELF sections may also move; this is a runtime-layout experiment, not isolation of a particular cache structure. This is not a claim that every byte in the resulting ELF is unchanged.

An exploratory warmed-up diagnostic used 100,000 warmup cycles followed by two million timed cycles and nine samples per layout. Baseline and candidate medians were 213.19 and 226.43 ns/cycle; the unpadded relink was 226.56. A 256-byte shift reduced the candidate to 216.63 ns/cycle, whereas a 4,096-byte shift left it at 225.02. That diagnostic loads `MIN_TD` through the module inside its generator and times within the generator; its absolute times are not directly comparable to the original scenario below. All tried layouts are retained in the JSON.

The decisive confirmation uses the **unmodified original benchmark**, the same candidate Python environment for every variant, and baseline/candidate/shifted runtimes. Nine samples per cell, randomized with seed `511 + repetition`:

| Cycles | Baseline ns/cycle | Candidate | Shift 192 | Shift 256 | Shift 384 | Shift 512 | Shift 1,024 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 20,000 | 227.65 | 1.052× | 1.053× | 1.036× | 1.009× | 1.016× | 1.032× |
| 200,000 | 193.19 | 1.066× | 1.062× | 1.041× | 1.010× | 1.017× | 1.034× |
| 2,000,000 | 188.91 | 1.070× | 1.066× | 1.044× | 1.012× | 1.022× | 1.050× |

The 384-byte variant was selected from this exploration, so it was tested again in independent runs rather than treating the best exploratory result as sufficient evidence. Eleven fresh-process samples per cell, two million cycles, seed `733 + repetition`; no host-wide ASLR setting changed:

| Independent condition | Baseline ns/cycle | Candidate | Shift 384 |
|---|---:|---:|---:|
| Second CPU, normal ASLR | 191.17 | 1.056× | 1.001× |
| First CPU, process ASLR disabled | 189.14 | 1.067× | 1.012× |

The candidate gap falls from 5.6% to 0.1% on the second CPU and from 6.7% to 1.2% with ASLR disabled for the test process. The effect therefore survives both checks. It remains specific to this build, workload and CPU family; no arbitrary padding is added to the build configuration.

## Profiles and interpretation

Native `py-spy` sampling at 499 Hz for ten seconds collected 4,989 samples per version. Both show the same generator/scheduler path: Python generator advancement, Python execution-phase entry, scalar conversion, graph/node evaluation and scheduling. Mutex lock/unlock together account for roughly 14–15% of self samples; generator advancement is 7.58% versus 7.80%, and the Python phase runner 4.67% versus 3.73%. These small differences do not localize a few nanoseconds to a particular function, and sampling is not hardware cycle accounting.

Several sampled native functions have unchanged symbol sizes but different baseline/candidate addresses: `GraphView::evaluate` shifts by 64 bytes, `NodeView::prepared_output` by 1,920, `TSInputView::valid` and `TSDataTracking::record_modified` by 1,984, and `TSInputTargetLinkState::notify` by 2,496. Function size alone does not prove instruction equivalence. The causal evidence is that changing placement with the same compiled inputs removes most of the measured gap, while evaluation counts and the generator path remain unchanged.

The supported conclusion is a code-placement-sensitive tight execution loop, not a new collection-size-dependent charge. A precise microarchitectural diagnosis would require hardware-counter access and additional targeted experiments. The remaining 0–1% in the best diagnostic layout is not attributed to a particular cause. This result does not establish that the unmodified candidate is performance-neutral.

## Validation and disposition

This follow-up changes only reports and benchmark data. Checked sample counts, medians, ratios, section identities, symbol-size/shift consistency and report links. No experimental relink is packaged or proposed. The previously completed native, Python, Windows and sanitizer validation of the unchanged shipping runtime remains recorded in [cost-analysis.md](cost-analysis.md#validation); those runtime suites were not rerun for this documentation-only follow-up.
