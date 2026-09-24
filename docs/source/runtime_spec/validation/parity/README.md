# Parity issue validation

Status: recorded 2026-09-24. The expectations follow [Operator contracts](../../operators.md)
and the core chapters. The C++ runtime is unchanged by this record; the
corrections it calls for land separately and cite it.

On 2026-09-24 there were 613 open `[parity]` issues. 528 came from one nightly
run whose candidate wheel could not be imported, so those recipes were never
compared. Every issue's minimized recipe was replayed against `main` @
`a2d0f7dec`, in fresh Python 0.5.41 and C++ environments, on Linux x86_64 and
macOS arm64. The two platforms agreed on every recipe.

| Outcome | Issues |
|---|---:|
| Matched the reference: import artefact, closed | 507 |
| Matched the reference: already fixed (#939, #941), closed | 15 |
| Already inside an accepted family (#862, #926, #1385), closed | 3 |
| Real divergence, recorded here | 84 |
| Hand-written, triaged on their own issues (#818, #821, #833, #835) | 4 |

Issue #1062's body stores U+0087 as the text `\^G`; its recipe was
reconstructed from the reference trace and replayed on macOS only.

## Method

For each of the 84, [reasoned.json](reasoned.json) states the expected
observation and the rules it comes from. The issues had already been observed,
so the derivations were written knowing that the runtimes differ; each
was written from the rules, then scored against both runtimes unchanged. The
TSD set operators have one [executable model](tsd_set_model.py) of their rules,
not 56 separate expectations. [check.py](check.py) regenerates
[assessment.json](assessment.json) from [observed.json](observed.json); it
never reads an expectation from an observation.

Per [Conformance](../../conformance.md): reasoning that matches the reference
alone means the C++ runtime is corrected and the issue closes when the trace
matches. Reasoning that matches the C++ runtime alone means the deviation is
accepted and bounded in `tools/parity/known_divergences.json`. Where neither
matches, or no rule decides, the question goes to the owner.

## Assessment

| Family | Issues | Rules | Verdict | Decision |
|---|---|---|---|---|
| TSD set operators | 56 (see assessment) | OP-4–OP-7 | 51 reference, 1 C++ (#978), 4 neither | Correct C++. #978 and the four hinge on the first empty result (point to settle 1), raised as a question |
| Three-operand TSS fold | #1428, #1478 | OP-7 | C++ | Accepted (already in `parity_matrix.rst`; now a family) |
| Key-set reader tick | #1139, #1140, #1160, #1200, #1391, #1393 | OP-1–OP-3 | C++ | Accepted: Python's `is_empty` ticks the key set it reads |
| Aggregate over an invalid list | #1181, #1246, #1355, #1494, #1476, #1538 | OP-1, OP-2 | reference | Correct C++ |
| Strict formatting | #1122, #1339, #1564, #1613 | OP-8 | reference | Correct C++ |
| Nested delta with an invalid child | #963, #964, #965 | TSD delta row | reference | Correct C++ |
| Empty recording | #1315 | OP-11 | reference | Correct C++ |
| `repr` escaping | #960, #1062 | OP-9 | reference | Correct C++ |
| `ln` domain | #1116 | OP-10 | C++ | Accepted (already in `parity_matrix.rst`; now a family) |
| Map text order | #1082, #1083, #1086 | VAL-8 | open | Question for the owner (point to settle 2) |

### Why each accepted family is accepted

- **Key-set reader tick.** Python 0.5.41's `is_empty` over a TSS returns a
  reference to a child output that it creates on the set, with the set as its
  owning output. Initialising that child marks the set modified, so the first
  read of `is_empty` makes a never-ticked key set valid and ticking. Its `len_`
  then publishes `size = 0`. A reader changing its producer breaks NOD-5,
  NOD-21 and TS-21. The C++ runtime publishes no `size`. The other fields at
  that tick (`total`, `average`, `minimum`, `maximum`) are observations
  where the two runtimes agree only because of this Python side effect.
  Correcting the C++ aggregates (OP-2) removes them from the C++ trace too.
  The family relation admits exactly the empty-set aggregates the Python
  side effect produces.
- **Three-operand folds.** OP-7. Python has no zero for an intersection or
  symmetric-difference fold, so three TSS operands fail at wiring (#1428,
  #1478). Three TSD operands to symmetric difference fold through `nothing`
  and never publish. The C++ answer to #978 matched the fold only because
  its binary symmetric difference read a never-ticked operand as empty
  (against OP-6). Once it is gated, the intermediate `a ^ b` of #978 is an
  empty first result, so the fold publishes only if point to settle 1 is
  ruled yes. The family's TSD clause is kept for that ruling.
- **`ln`.** OP-10, already accepted in `parity_matrix.rst`. It was pinned by
  one fingerprint and the generator reached it by another recipe.

### Neither runtime

In #983, #984, #1004 and #1008 the reasoning publishes the first empty
symmetric difference to validate the output; neither runtime does. Every other
observation in those four matches Python, and C++ matches it too once
corrected. So the parity issues close, and the rule remains a question
(point to settle 1). #978 depends on the same rule one level down: without
it, a three-operand symmetric difference whose first two operands cancel
never publishes in either runtime, although its value is well defined.

## Observations outside the issues

Probing the aggregates found the C++ runtime publishing `sum_` and `mean` over a
never-valid TSS as well as a list, where Python and the C++ `min_`, `max_` and
`len_` publish nothing. The same correction covers them (OP-2). Both runtimes
publish `is_empty` of a never-ticked set as `True`, against OP-1; nothing here
depends on it, and it stays as it is (point to settle 3).
