# Const and debug compiler bootstrap

The first complete compiler case connects two HGL nodes. `const_` has a fixed
`i64` argument, schedules itself at start, and publishes once in `when scheduled()`.
`debug_print` consumes a valid tick and calls the native value helper `print_i64`.
The graph connects `const_(42)` to `debug_print`. `const` is a reserved word, so
the authored source uses `const_`.

Only integer formatting and console output are native. Source scheduling,
admission, publication and composition stay in HGL. Target parts complete the
same native declaration; the helper returns no value and creates no node.
`hgraph` supplies the C++ part and provider; `hgl` supplies its own Rust pair.
Only the shared HGL source is mirrored between them.
This is an i64 bootstrap, not the full generic, labelled standard-library API.

## Expected observations

| Case | Expected |
| --- | --- |
| Start graph | One value tick, 42, at the starting evaluation time; print `42` once |
| Continue without input or alarm | No further tick or print |
| Fresh second execution | Print `42` once again; no shared node state |
| Source configured with -7 | One tick carrying -7 |
| Sink receives absent, 42, absent, 42, -7 | Print 42, 42, -7; absent cells do nothing |
| Construct/check/emit without running | No printing |
| Source with bare `when` | No modified inputs, so no publication even if scheduled |
| Missing implementation or wrong native signature | Reject before native execution |
| Temporal input passed as fixed source configuration | Type error |

Validate the source and generated C++ on hgraph first. Then compile the same
HGL with the Rust compiler and run emitted nodes and graph construction on the
Rust engine. Compare ticks and effects, not generated names or output handles.
Python and C++ operator comparisons cover the source and sink behaviour;
reference debug-print prefixes are excluded from the integer-payload comparison.
The bootstrap itself prints only the integer and a newline.
