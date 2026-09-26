# Wiring validation

Status: recorded 2026-09-26. The expectations follow [Wiring](../../wiring.md)
and are derived in [wiring cases](../../cases_wiring.md). The runtimes are
unchanged by this record; the HGL correction it calls for lands separately
and cites it.

Fifteen cases ran three times each, each run in a fresh process, on Python
hgraph 0.5.41 and on the C++ runtime's Python surface, on macOS arm64 with
Python 3.14.7. Every repeat agreed. The C++ runtime was first observed at
`main` @ `dea948136`, then again at this branch's `96d364e87` when the two
failure cases were added; no earlier observation changed. After this branch
was rebased onto `main` @ `640df576e` it was observed a third time, at
`76b8e22e8`, and two C++ observations changed (see "Observations after the
rebase"). The bundle_field_order case was added and every case observed
again at `719a38cc2` (the same runtime code as `76b8e22e8`); nothing else
changed. The HGL front end was observed on the same generic calls at
`dea948136`.

| Result | Observations |
|---|---:|
| Reasoning matches both runtimes | 40 |
| Reasoning matches C++ only; Python varies | 1 |
| Reasoning matches Python only; C++ varies | 4 |
| Reasoning matches neither runtime; the owner's ruling decides | 4 |
| HGL front end matches | 1 |
| HGL front end varies (WIR-14, WIR-22) | 5 |

The bundle-identity and operator-contract cases were added on 2026-09-26
with the owner's rulings that became WIR-15 and WIR-21 to WIR-24; their
expectations were written from those rulings before they ran.

The bundle_field_order case was added on 2026-09-26 with the owner's
rulings that fields pair by name in any order, and that WIR-15 holds at a
service boundary too; its expectations were written before it ran. Python
0.5.41 meets all four, including the values (`a * 10 + b` is `12` through a
reordered bundle).

A candidate wider than its operator was first recorded without an
expectation (point to settle 6). The owner then ruled that a candidate
cannot widen its operator, and that every operator accepts extra arguments
as if it ended with `*args, **kwargs` (WIR-22 to WIR-24); the widening and
extra-argument observations are asserted from that ruling. Both runtimes
agree with each other against it on widening, so per
[Conformance](../../conformance.md) the ruling decides: both vary (WV-6).

The failure_report and caught_failure cases were added on 2026-09-26 from
the owner's rulings on WIR-4: a failure fails the graph, even when the
graph's code catches it, and the error says where and why. Their
expectations were written from those rulings before they ran.

Recorded, not asserted: the printed names of the bundles used as sources.
They differ between the runtimes and no rule states them.

## Method

[reasoned.json](reasoned.json) states each expected observation and the rules
it comes from, written before either runtime ran. [cases.py](cases.py) wires
and runs one case using only public wiring API; the same file runs on both
runtimes. [observe.py](observe.py) runs every case in fresh processes and
writes [observed.json](observed.json); [observe_hgl.py](observe_hgl.py) reads
the bindings the HGL compiler records for [front_end.hgl](front_end.hgl)
(`hgl check --dump-hir`) into [observed_hgl.json](observed_hgl.json).
[check.py](check.py) regenerates [assessment.json](assessment.json) and never
reads an expectation from an observation. [decisions.json](decisions.json)
holds the owner's rulings.

To replay:

```sh
python observe.py --reference <python 0.5.41> --candidate <python with this checkout> \
    --candidate-revision <revision>
python observe_hgl.py --hgl <build>/language/hgl --revision <revision>
python check.py
```

Two harness changes were made after a first run, neither to an expectation's
rule:

- The type-only cases add a sink to each port: Python 0.5.41 refuses to run
  a graph with no sink.
- The projection case first took its bundle from `dereference(if_(...))`.
  In Python 0.5.41 that `dereference` publishes plain fields, not reference
  fields, and fails during evaluation ("dictionary changed size during
  iteration"). The case now uses a compute node whose declared output has a
  reference field; its expectations were derived from the rules for that
  source.

## Variations

| ID | Case and observation | Accepted expectation | Variation |
|---|---|---|---|
| WV-1 | projection, `getattr_(bundle, "routed")`; WIR-5, WIR-13 | R + C++: the reference field, `REF[TS[int]]` | Python 0.5.41 has no `getattr_` candidate for a bundle; wiring fails. Its `bundle.routed` is a Python-side projection that never calls the operator, and matches. The C++ candidate is a superset |
| WV-3 | bundle_identity, `_same(Foo, {a})`; WIR-15, WIR-17 | R + Python: wires, one bundle is unnamed | C++ fails. On the C++ surface Python's unnamed schema carries a generated name (the port's type prints as `...UnNamedTimeSeriesSchema_<hash>`), and a variable already bound compares the type's identity. No longer varies at `76b8e22e8` (below) |
| WV-4 | bundle_identity, `_takes_foo(Bar)`, and from `76b8e22e8` also `_same(Foo, Bar)`; bundle_field_order, `_takes_pair(Riap)`; WIR-15 | R + Python: fails, both named with different names | C++ wires: its bundle comparison (`time_series_schema_equivalent`) looks only at fields, never at the names |
| WV-5 | HGL front end, an implementation with a `const scale` parameter its operator does not declare, with a default and without one; WIR-22 | R + both runtimes (their candidates may add parameters): accepted | HGL rejects both: "implementation parameter count does not match its operator contract" |
| WV-6 | operator_contract, a candidate accepting `TIME_SERIES_TYPE` for an operator declaring `TS[int]`; WIR-23, WIR-24 | Owner ruling: rejected when registered, so a `TS[float]` call fails | Both runtimes register it and select it for `TS[float]`: neither checks a candidate against its operator. HGL rejects it |
| WV-7 | HGL front end, a call passing `scale=5.0`, which the operator does not declare; WIR-22 | R + both runtimes: accepted, the argument goes to the candidates | HGL binds a call's arguments against the operator's signature and rejects the extra one |
| WV-8 | failure_report, the graph path; WIR-4 | Owner ruling: the error names the path of graph calls that led to the failed call, `failing_outer` then `_failing_inner` | Neither runtime names it. Both name the call, the argument's type and each candidate's reason. The C++ `Wiring` keeps the path (`current_wiring_path`) but passes it only to wiring observers |
| WV-9 | failure_report, the operator's name; WIR-4 | R + Python: the error names `_only_int` | C++ names the operator by its registry name, `__pyop____main__._only_int_1` |
| WV-10 | caught_failure; WIR-4 | Owner ruling: the graph fails to wire | Both runtimes let the graph's code catch the error and wire its fallback; the node the failed attempt added stays in the graph and runs. The Python port's own wiring layer relies on catching in three places: `hgraph.arrow`'s argument-shape retries, port attribute sugar (`port.year`) and `convert`'s target handlers |
| WV-2 | HGL front end, `pass(value: ref<f64>)` and `pass(values: list<ref<f64>, 2>)`; WIR-14, WIR-7 | R + both runtimes: `T` binds `f64` and `list<f64, 2>` | HGL binds `ref<f64>` and `list<ref<f64>, 2>`: its generic inference (`GenericSubstitution::unify`) binds a variable to the argument as supplied. The emitted C++ still wires correctly, because the runtime resolves the emitted generic call, but HGL's own checker works from a different type than the graph it builds |

WV-2 is corrected in the HGL compiler, citing WIR-14; the correction's tests
replay `front_end.hgl`.

## Observations after the rebase

Rebasing onto `main` brought in #1651, which compares a type variable bound
twice by value equivalence (`time_series_value_equivalent`: references
removed, then `time_series_schema_equivalent`) instead of by identity. At
`76b8e22e8` two C++ observations of bundle_identity changed; every other
observation is as before. The earlier set is kept in
[observed_96d364e87.json](observed_96d364e87.json).

| Observation | At `96d364e87` | At `76b8e22e8` | Assessment |
|---|---|---|---|
| `_same(Foo, {a})` (named_and_unnamed_repeat) | fails (WV-3) | wires | Now meets the expectation: WV-3 no longer varies |
| `_same(Foo, Bar)` (two_names_repeat) | fails | wires | Now varies. WIR-15 says two named bundles with different names do not match; the comparison ignores names, as in WV-4, so it is recorded under WV-4 |

The result counts above are unchanged: one observation moved from "C++
varies" to "both", the other the other way. The correction for WV-4
(#1655) makes the bundle comparison count names when both bundles are
named, which covers both observations.

## C++ correction for WIR-15

WV-3 and WV-4 are corrected in the C++ runtime. (WV-3 had already stopped
varying after the rebase onto `main`, through #1651; the bridge change below
removes its cause, the generated name.)

- `time_series_schema_equivalent` compares two bundles' names when both are
  named (WV-4: `_takes_foo(Bar)`, and `_same(Foo, Bar)` since the rebase).
- A variable already bound compares a supplied type as types are compared,
  not by identity, in the runtime matcher (`ts_pattern_match`,
  `prebound_as_supplied`) and the static unifier (`ts_unifier<TsVar>`); the
  first binding stays.
- The Python bridge registers `ts_schema(...)` as an unnamed native bundle
  (`un_named_tsb`) instead of under a generated name (WV-3).

Regression coverage: `tests/cpp/test_wiring_contract.cpp`
("WIRE-BUNDLE-IDENTITY") and `python/tests/test_wiring_contract.py`, which
now holds all six observations. Three reference-alternative tests in
`tests/cpp/test_time_series_reference.cpp` bound two differently named
bundles with the same fields; they now request an unnamed bundle, which
WIR-15 lets match the named source. The archived observations are kept.

## The C++ correction behind WIR-7

Before `main` @ `dea948136` (PR #1650, issue #847) the C++ matcher removed
only a top-level reference: `generic_depth` bound `TSD[str, REF[TS[int]]]` and
showed each element as a reference. That observation is not re-measured
here; the case records the corrected behaviour, which Python 0.5.41 shares.

## Limits

These cases observe type resolution and selection through the public Python
surface; `tests/cpp/test_wiring_contract.cpp` holds the same expectations
through native C++ wiring, where the stated resolution is C++'s explicit
output schema and is covered in `tests/cpp/test_operators.cpp` instead. A structural requested output (a variable nested in a requested
type) and the static C++ unifier are covered by native tests
(`tests/cpp/test_operators.cpp`), not by this record. Parts 1 and 3's
remaining rules rest on source evidence; see the chapter's Evidence table.
