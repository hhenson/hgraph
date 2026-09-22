# Atomic cases

Status: proposed. Covers TS-1, TS-2 and TS-3.

## ATOMIC-RETENTION

One owned `TS<i64>`, initially invalid; at most one admitted publication per
cycle. Binding, invalidation, concurrency, allocation failure and batching
are excluded. Begin advances time, retaining value and stamp. Publish sets
both before observation. Observe changes nothing.

Each row is read after its action. `—` means absent, not zero.

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

Idle cycles keep the value. Reads do not consume the delta. The equal
publication at 30 still ticks. [Conformance](conformance.md) distinguishes
publication from a setter that may suppress equal results.

## ATOMIC-ZERO

Start fresh at offset 0, publish 0, then begin at offset 1:

| Observation | Value | Valid | Modified | Last modified | Delta |
|---|---|---|---|---|---|
| before publication | — | false | false | never | — |
| after publication | 0 | true | true | 0 | 0 |
| next cycle | 0 | true | false | 0 | — |

Neither zero is `never`. The [layout](layout_example.md) has its own encoding.

## Invalidation

TS-7, outside these cases, resets the stamp to `never`: invalid, unmodified,
yet notifying. The parent is stamped as changed. Record notification and
admission separately; an invalid required input can prevent evaluation.
Sampled inputs and keyed withdrawal need separate cases.
