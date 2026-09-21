# Nested graph cases

Status: validated on Python and C++ hgraph; see [results](validation.md).

## KEYED-ROUTE-TIMER

Rules: TS-14, TS-16, TS-17, TS-19; GRF-15, GRF-16, GRF-18, GRF-21–GRF-23;
NOD-11–NOD-13. Only the observations below are covered.

A dictionary maps child keys to price keys. Each member owns one child graph.
The child selects a price through a reference, adds each input tick to a
running total initially zero, and publishes the total. Each input tick replaces
its tagged timer with a deadline two cycles later. The timer publishes the
negative total without changing it. If input and timer coincide, one eval
processes the input and replaces the timer.

The price dictionary is passed whole to each child; its keys do not create
children. This uses `pass_through`, not `no_key`. There is no deduplication.
State lasts for that child instance. Removing its member stops it; a later
insertion creates a new total and scheduler.

| t | Child-key changes | Price changes | Output delta |
|---|---|---|---|
| 0 | X selects A | A:2, B:10 | X:2 |
| 1 | — | A:3 | X:5 |
| 2 | X selects B | — | X:15 |
| 3 | — | A:4 | — |
| 4 | — | — | X:-15 |
| 5 | remove X | — | remove X |
| 6 | — | A:5 | — |
| 7 | X selects A | — | X:5 |
| 8 | — | — | — |
| 9 | — | — | X:-5 |
| 10 | — | — | — |

At 2, B's old value is sampled. At 3, A cannot wake the child now following B.
At 4 and 9, only a child deadline causes work. Start/stop events are
`start, stop, start, stop`; the final stop is run shutdown. Repeat with an
outer `switch(True)` owning the map: the complete trace is unchanged.

The fixture uses native/Python `STATE` to observe instance lifetime. This does
not validate recording, recovery or HGL's state/cache lowering.

## DEADLINES-AND-REMOVAL

Rules: GRF-16, GRF-18, GRF-22; NOD-11–NOD-13. A child publishes an incoming
integer and schedules a tagged timer that many cycles ahead. A timer publishes
-1. Input takes precedence if both causes coincide.

The [input and expected-output arrays](validation/reasoned.json) fix four cases:

- `timer`: X=2 at 0; fire at 2; remove at 3; X=3 at 5; fire at 8.
- `cancel_timer`: X=4 at 0; remove at 2; no tick at its abandoned deadline 4;
  X=2 at 5; fire at 7.
- `nested_timer`: the first case inside an outer switch; identical ticks.
- `parallel_children`: X=3,Y=2 at 0; Y=4 at 2 replaces Y's due timer;
  X fires at 3; remove X at 4; X=2 at 5; Y fires at 6, X at 7;
  remove both at 8. No extra tick or sibling interference.

Idle positions remain no-tick, including trailing positions before the
exclusive run end. These cases validate deadline propagation and cancellation;
they do not establish memory bounds or absence of physical allocations.

## CHILD-FAILURE

Rules: GRF-18; NOD-11 and NOD-20. A mapped child starts, receives 1, then -1.
It raises on -1 without capturing the error. Expected events:
`start, eval(1), eval(-1), stop`; the failure leaves the run.

In a separate case its start raises. Expected events: `start`, failure.
No eval and no stop run for that child. These single-child cases do not prove
rollback order among several partially started siblings or nested error capture.
