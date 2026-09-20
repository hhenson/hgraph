# Activation and lifetime cases

Status: proposed. The pair owner is a physical example, not the graph lifecycle.

## ORDERED-ACTIVATION — GRF-15, GRF-16 and NOD-2

Declare ranks `[A, B, sum, sink]`. A publishes 2 and B publishes 3 in one
cycle. Sum's inputs are active and required valid. It evaluates once after
both sources, publishes 5, and the sink observes 5 once.

Leave B invalid: A schedules sum, but admission fails. Make B passive and
valid: B alone does not schedule sum; A can wake it to read both values.
Absent policies use defaults; explicit empty selections select no inputs.

## PAIR-NORMAL

Left and right share the [32-byte region](layout_example.md); controller and
link live outside it. Allocation succeeds; hooks, detach and destruction
finish without throwing. One synchronous read borrow, ending explicitly,
permits no mutation or callback retention. One consumer link is admitted.

State is `(phase, allocated, live cells, borrowed, linked)`, initially
`(fresh, false, 0, false, false)`. Read after each action. Unmentioned state
stays; “none” forbids events. Error names are local to this example.

| Action | Resulting state | Ordered events / error |
|---|---|---|
| construct | ready, true, 2, false, false | allocate; construct left; construct right |
| start | running, true, 2, false, false | start left; start right |
| attach | running, true, 2, false, true | attach left |
| borrow | running, true, 2, true, true | none |
| release while borrowed | unchanged | BorrowLive; no events |
| stop while borrowed | unchanged | BorrowLive; no events |
| end borrow | running, true, 2, false, true | none |
| release while running | unchanged | WrongPhase; no events |
| stop | stopped, true, 2, false, false | quiesce; stop right; stop left; detach |
| release | released, false, 0, false, false | destroy right; destroy left; release region |
| observe again | unchanged | none |

Quiesce prevents new delivery; stop hooks still have live cells. Detach precedes
destruction. BorrowLive takes precedence over WrongPhase on release.

## PAIR-CONSTRUCTION-FAILURE

Fail at entry to right's constructor, before any right subobject is live.
From fresh: `allocate; construct left; fail right; destroy left; release region`.
Return ConstructionFailed and `(released, false, 0, false, false)`. Observe
changes nothing. Never destroy right, call stop or release twice.

## PAIR-NEVER-STARTED

From fresh: `allocate; construct left; construct right; destroy right;
destroy left; release region`. No stop hook runs.

Allocation failure, partial right construction, failing hooks, concurrent
delivery and retained references remain outside these cases. GRF-9 and GRF-18
own the actual graph's construction and failed-start rules.
