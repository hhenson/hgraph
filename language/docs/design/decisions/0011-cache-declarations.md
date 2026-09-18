# ADR 0011: `cache` declarations

Status: accepted. Implemented for one scalar `cache` declaration per runtime
function, lowered to the native `State<T>` selector; the limits below are the
native contract's, not the language's.

## Context

[ADR 0008](0008-temporal-contracts-and-target-mappings.md) agreed the concept:
`state` is semantic history and is recorded and restored; `cache<T>` is
node-local data that can be rebuilt without missing history, is excluded from
record/replay, and maps to the native `State<T>` slot rather than
`RecordableState<TSchema>`. It deliberately left the declaration syntax open.

The migration catalogue then held `schedule` native-only for exactly this
reason: its tick counter lives in a non-recordable `State<Int>`, and HGL
`state` would have recorded it, changing behaviour after a restore. The same
gap (MIG-005) is the first of the contracts behind `throttle`, `batch`,
`gate`, `lag` and `window`.

## Decision

1. **Syntax.** `cache name[: T] = init` is a function-level declaration of a
   runtime function, placed like `state`, before the executable blocks.
   `cache` is a reserved word.

2. **Semantics.** A cache is read and written exactly like state inside
   hooks. Its initializer runs on **every** start, restored or not: a cache
   holds nothing the engine promises to bring back. Its type follows state's
   typing rules; this slice admits the scalar types state admits.

3. **Lowering.** One `cache` declaration lowers to `hgraph::State<T>` bound
   as `hgl_cache`; reads are `hgl_cache.get()`, writes `hgl_cache.set(...)`,
   and `start` seeds it unconditionally.

4. **Native limits, reported as such.** hgraph's static node has one
   `State<T>` slot and rejects it beside `RecordableState`. A second `cache`
   declaration, or `cache` together with `state` in one function, is a
   diagnostic that names the native contract. Lifting either needs backend
   work: a bundle or opaque value schema for several cache fields, and the
   static-node change ADR 0008 already records as the agreed direction.

## Consequences

- `schedule` is authorable with native parity: `cache ticks: i64 = 0`.
- A cache is function-level data, not a local: the unread-local rule does
  not apply, and a cache the body never reads is still seeded on start. The
  emitter names the `hgl_cache` selector only in hooks that use it, as for
  every other name.
- What MIG-005 still lacks after this slice: several cache fields, a cache
  beside recordable state, non-scalar caches (queues, windows, indexes), and
  generic recordable state without a default. Those are separate decisions;
  a cache must not be used to hide semantic history from record/replay.
