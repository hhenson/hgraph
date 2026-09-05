# hgraph performance matrix

- date: 2026-09-05T04:03:34+00:00
- host: macOS-26.6.2-arm64-arm-64bit / arm
- CPU: Apple M4 Max
- Python: 3.12.10
- fixed release baseline: hgraph 0.8.19 (published wheel)
- fixed release wheel: hgraph-0.8.19-cp312-abi3-macosx_15_0_arm64.whl
- fixed release SHA-256: e7c4f19920a45ce9da0d4e4c479af2fd258e4f55b0f2de0215e5b105548629d1
- current-source compiler: Apple clang version 21.0.0 (clang-2100.1.1.101)
- current-source revision: d8ef83bc21df
- current-source fingerprint: 72062e0a56b95d354788671d94fcf02f187763a7b003727601bb12bd0c0267ff
- current-source build type: Release
- cycle scale: 1.0
- size scale: 1.0
- fresh-process samples: 5
- modes: hgraph 0.8.19 (`release`), current hgraph
- reused fixed baseline cells: 0

Median seconds per scenario (lower is better); +/- is median absolute deviation and xN is speed-up vs hgraph 0.8.19.
C++-first-only sections are tracked without a 0.5 comparison.

## Graph construction

This section is tracked within C++-first hgraph and is not a cross-implementation comparison.

| workload | cycles | hgraph 0.8.19 | current hgraph |
|---|---|---|---|
| dispatch with many registered cases (`construct_dispatch_cases`) | 1 | 0.274s +/- 0.001s | 0.099s +/- 0.000s (x2.8) |
| Operator with many overloads at many call sites (`construct_overloads`) | 1 | 0.016s +/- 0.000s | 0.011s +/- 0.000s (x1.4) |
