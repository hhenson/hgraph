# Wiring validation

Status: recorded 2026-09-26. The expectations follow [Wiring](../../wiring.md)
and are derived in [wiring cases](../../cases_wiring.md). The runtimes are
unchanged by this record; the HGL correction it calls for lands separately
and cites it.

Twelve cases ran three times each, each run in a fresh process, on Python
hgraph 0.5.41 and on the C++ runtime's Python surface at `main` @
`dea948136`, on macOS arm64 with Python 3.14.7. Every repeat agreed. The HGL
front end was observed on the same generic calls at the same revision.

| Result | Observations |
|---|---:|
| Reasoning matches both runtimes | 31 |
| Reasoning matches C++ only; Python varies | 1 |
| Reasoning matches Python only; C++ varies | 2 |
| Reasoning matches neither runtime | 0 |
| HGL front end varies (WIR-14, WIR-22) | 3 |

The bundle-identity and operator-contract cases were added on 2026-09-26
with the owner's rulings that became WIR-15 and WIR-21 to WIR-24; their
expectations were written from those rulings before they ran.

Recorded, not asserted: a candidate wider than its operator (point to settle
6). Both runtimes register it and select it for a `TS[float]` call; HGL
rejects it.

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
| WV-3 | bundle_identity, `_same(Foo, {a})`; WIR-15, WIR-17 | R + Python: wires, one bundle is unnamed | C++ fails. On the C++ surface Python's unnamed schema carries a generated name (the port's type prints as `...UnNamedTimeSeriesSchema_<hash>`), and a variable already bound compares the type's identity |
| WV-4 | bundle_identity, `_takes_foo(Bar)`; WIR-15 | R + Python: fails, both named with different names | C++ wires: its bundle comparison (`time_series_schema_equivalent`) looks only at fields, never at the names |
| WV-5 | HGL front end, an implementation with a `const scale` parameter its operator does not declare; WIR-22 | R + both runtimes (their candidates may add parameters): accepted | HGL rejects it: "implementation parameter count does not match its operator contract" |
| WV-2 | HGL front end, `pass(value: ref<f64>)` and `pass(values: list<ref<f64>, 2>)`; WIR-14, WIR-7 | R + both runtimes: `T` binds `f64` and `list<f64>` | HGL binds `ref<f64>` and `list<ref<f64>>`: its generic inference (`GenericSubstitution::unify`) binds a variable to the argument as supplied. The emitted C++ still wires correctly, because the runtime resolves the emitted generic call, but HGL's own checker works from a different type than the graph it builds |

WV-2 is corrected in the HGL compiler, citing WIR-14; the correction's tests
replay `front_end.hgl`.

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
