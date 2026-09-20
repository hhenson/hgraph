# Activation and lifetime cases

Status: proposed cases extracted from #796/#934; the pair owner is a bounded
physical example, not a replacement graph lifecycle.

## ORDERED-ACTIVATION — GRF-15, GRF-16 and NOD-2

Declare ranks explicitly: source A, source B, sum, sink. In one cycle A
publishes 2 and B publishes 3. Both inputs of sum are active and required
valid. Sum evaluates once after both sources and publishes 5; the sink observes
5 once. The order is `[A, B, sum, sink]` because this graph description declares
it, not because an unspecified resolver must arbitrarily rank A before B.

In a second instance leave B invalid. A's notification schedules sum, but sum
is not admitted and the sink sees no result. In a third, make B passive and
initially valid: B's notification alone does not schedule sum, but an A tick
can evaluate it with B's current value. Absent policies use the declared
defaults; an explicit empty active or valid selection selects no inputs for
that policy. Neither a missing trigger nor an empty policy invents a tick.

## PAIR-NORMAL

Two cells, left and right, share one owned region. The controller and consumer
link are outside the [32-byte example region](layout_example.md). Allocation
succeeds; start, stop, detach and destruction finish without throwing. Only
one synchronous read borrow and one consumer link are admitted. A borrow
permits no mutation or callback retention and ends explicitly.

State is `(phase, allocated, live cells, borrowed, linked)`, initially
`(fresh, false, 0, false, false)`. Each row observes the completed action.
An empty event list forbids side effects; unspecified state is retained.

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

Quiesce disables new delivery before stop hooks, which still have live cells.
Detach removes the link before destruction. No subsequent notification may
reach dead storage. BorrowLive takes precedence over WrongPhase on release.
The error names belong to this example; they are not promised C++ exceptions.

## PAIR-CONSTRUCTION-FAILURE

Inject a failure at entry to right's constructor, before any right subobject
is live. Starting fresh, the exact events are `allocate; construct left;
fail right; destroy left; release region`. Return ConstructionFailed with
state `(released, false, 0, false, false)`. Observation changes nothing.
Never destroy right, call either stop, or release the region twice. Unlike
BorrowLive, this failure has cleanup effects and changes the initial state.

## PAIR-NEVER-STARTED

From fresh, construct both cells, then release without start. Events are
`allocate; construct left; construct right; destroy right; destroy left;
release region`. No semantic stop hook runs for an unstarted object.

These three cases preserve the source experiment's failure and borrow
boundaries. Allocation failure, partial right subobjects, failing start/stop,
concurrent delivery and retained references require further contracts. In the
actual graph, GRF-9 and GRF-18 govern partial instantiation and failed start;
the pair's nonthrowing start assumption is not a restriction on graph nodes.
