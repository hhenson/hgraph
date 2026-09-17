# ADR 0010: clock and scheduler capabilities, scheduled handlers, and input activity

Status: accepted. Implemented for `inject clock`, `inject scheduler`, the
`scheduled()` handler selector, scheduler-driven sources, and the
`passivate`/`activate` input-activity statements.

## Context

The language agreed the injectable names `clock` and `scheduler` with no
surface behind them, and the migration catalogue holds thirty-six core
operators under blocker B2 (lifecycle and activation). Three things were
missing, and they are what the native stream nodes actually use:

- an evaluation clock (`EvaluationClockView`: evaluation time, wall-clock
  time, next cycle);
- the node scheduler (`NodeScheduler`: schedule after a delay or at a time,
  on the engine or the wall clock; query the next alarm; know whether the
  current evaluation is the alarm firing); and
- input activity: a node that has seen enough ticks stops listening to an
  input (`make_passive`) and may listen again later (`make_active`).

The language model also left two lifecycle questions open: the meaning of a
bare handler in a runtime function with no temporal parameters, and a source
form for an explicitly empty activation set (requirement MIG-014, library
blocker LIB-002). Both are the scheduler case.

## Decision

1. **`inject clock`** binds `hgraph::EvaluationClockView` as `clock` in every
   hook. `clock.evaluation_time()`, `clock.now()` (wall clock) and
   `clock.next_cycle_evaluation_time()` return `datetime`.

2. **`inject scheduler`** binds `hgraph::NodeScheduler` as `scheduler` in
   every hook. `scheduler.schedule(delay)` and
   `scheduler.schedule(delay, on_wall_clock)` take a `duration`;
   `scheduler.schedule_at(time)` and `scheduler.schedule_at(time, on_wall_clock)`
   take a `datetime`; `scheduler.is_scheduled()` returns `bool`;
   `scheduler.next_scheduled_time()` returns `datetime`. Wall-clock alarms
   follow hgraph's rule: only a real-time executor accepts them, and a due
   alarm fires on the next evaluatable cycle. Tags are not exposed.

3. **`scheduled()` is a handler selector.** It is valid only in a
   function-level `when` condition of a function that injects `scheduler`,
   and is true when the current evaluation is the node's alarm firing
   (`is_scheduled_now`). A handler with `scheduled()` at top level receives
   no implicit `modified()`; its implicit `valid()` is unchanged. Such a
   handler adds no input to the node's activation set. When no handler names
   an input, the set is explicitly empty: every input is passive and the
   node evaluates only when scheduled. This is the agreed spelling of the
   empty activation set; `modified()` keeps its complete-list meaning.

4. **A runtime function may have no temporal parameters when it injects
   `scheduler`.** It is a source: `start { scheduler.schedule(0s) }` asks
   for evaluation in the starting cycle, which is how a static node's
   `schedule_on_start` is spelled. The implicit `valid()` of such a
   function is vacuously true and its implicit `modified()` never holds.

5. **`passivate(input)` and `activate(input)`** are runtime statements
   whose argument is a direct temporal parameter of the function. They
   lower to the input view's `make_passive()` / `make_active()`. A passive
   input still holds its value and validity; it just no longer activates the
   node. The node's static activation policy is unchanged; activity is a
   per-evaluation effect, exactly as in a hand-written node.

## Consequences

- `schedule`, `freeze`, `take` and `until_true` become authorable as
  parallel identities; `throttle`, `batch`, `gate`, `lag` and `window` still
  need the buffered-delta and queue state contracts (MIG-005) and remain B2.
- The language model's open questions on bare handlers without temporal
  parameters and on the explicit empty activation set are closed by
  decisions 3 and 4. LIB-002 (collection startup) can now be spelled with
  `start { scheduler.schedule(0s) }` and `when scheduled()`; closing it is
  library work, not language work.
- `scheduled`, `passivate` and `activate` are intrinsic names and cannot be
  used as identifiers.
- Output access inside `start` and `stop` remains rejected; scheduling from
  `start` is admitted because the scheduler is a capability, not the output.
