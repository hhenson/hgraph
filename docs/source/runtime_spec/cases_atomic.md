# Atomic publication cases

Status: proposed conformance cases for TS-1, TS-2, TS-3, TS-21 and TS-22;
adapted from #933/#934. No runtime execution is claimed.

## ATOMIC-RETENTION

One live owned `TS<i64>` output, initially invalid, has at most one admitted
publication per cycle. No binding, invalidation, concurrent access, allocation
failure or same-cycle batching is covered. `begin(t)` advances the observation
time; it does not physically clear the stored value or timestamp. `publish(v)`
sets the payload and last-modified time together before observation. `observe`
changes nothing and consumes nothing. These actions must finish.

Each row is observed immediately after the action. `—` denotes no value,
no current-cycle delta, or `never`, according to the column; it is not zero.

| Action | Value | Valid | Modified | Last modified | Delta |
|---|---|---|---|---|---|
| begin(10) | — | false | false | never | — |
| publish(7) | 7 | true | true | 10 | 7 |
| observe again | 7 | true | true | 10 | 7 |
| begin(20) | 7 | true | false | 10 | — |
| begin(30) | 7 | true | false | 10 | — |
| publish(7) | 7 | true | true | 30 | 7 |
| begin(40) | 7 | true | false | 30 | — |
| publish(9) | 9 | true | true | 40 | 9 |

Clearing the value on begin(20), consuming the delta on the first read, or
suppressing the **admitted** publication at 30 is forbidden. This says nothing
about an arbitrary setter's admission policy; the driver must establish that
boundary, as described in [Conformance](conformance.md).

## ATOMIC-ZERO

With a fresh output, begin at offset 0, publish payload 0, then begin at
offset 1. The observations are respectively:

| Observation | Value | Valid | Modified | Last modified | Delta |
|---|---|---|---|---|---|
| before publication | — | false | false | never | — |
| after publication | 0 | true | true | 0 | 0 |
| next cycle | 0 | true | false | 0 | — |

Zero is both a legitimate payload and a legitimate offset from run start.
It must not be confused with `never`. The [layout example](layout_example.md)
uses its own private stamp encoding; that is not the engine's epoch.

## Invalidation is a separate case

TS-7 adds a transition excluded above: resetting the stamp to `never` makes
the output invalid and unmodified but notifies watchers. A parent collection
is stamped as changed. An adapter must record notification and node admission
separately: a scheduled node whose required input is now invalid may not run.
Sampled inputs and keyed withdrawal need their own observations; they cannot
be inferred from this owned-output table.
