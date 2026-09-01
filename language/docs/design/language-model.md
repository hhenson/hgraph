# Language model

Status: initial design; syntax is provisional

## Design principles

The language should feel familiar to Python, Rust, and Swift users without
copying any one language. Braces make phase bodies and generated source ranges
unambiguous. Type arguments use angle brackets. Named arguments are part of the
ordinary call syntax.

The bespoke part is semantic rather than decorative: `graph`, `node`, `emit`,
time-series types, wiring scalars, and imported operators have meanings that
ordinary general-purpose functions do not.

## Illustrative program

```text
module prices

use hgraph.analytics::{rolling_mean}

node midpoint(bid: ts<f64>, ask: ts<f64>) -> ts<f64> {
    when bid.valid && ask.valid {
        emit (bid.value + ask.value) / 2.0
    }
}

graph smooth(
    bid: ts<f64>,
    ask: ts<f64>;
    window: i64 = 20
) -> ts<f64> {
    let mid = midpoint(bid, ask)
    return rolling_mean(mid, period: window)
}
```

The semicolon in a graph signature separates time-series inputs from
wiring-time scalar parameters. This is provisional syntax for an important
semantic distinction; the distinction will remain even if its spelling
changes.

## Declarations

The first language slice has four declaration forms:

- `module` names a compilation and import unit.
- `use` imports public declarations from a language module descriptor.
- `node` declares a typed per-tick compute primitive.
- `graph` declares wiring-time composition.

User-defined operators and overload families are deferred. Programs may call
operators imported from hgraph and extension modules in the first slice.

Top-level executable configuration will be a graph whose sources and sinks are
imported native facilities or testing facilities. The language does not define
a push adaptor declaration.

## Initial type vocabulary

The first slice includes:

- scalar `bool`, `i64`, `f64`, and `str`;
- `ts<T>` for an atomic time-series value;
- `signal` for tick-only input observation;
- wiring-time scalar parameters, represented by ordinary scalar types in the
  scalar portion of a graph or node call.

Temporal scalars, enums, records, containers, `tss`, `tsl`, `tsd`, `tsb`,
`tsw`, and explicit `ref` types follow after the atomic vertical slice. Their
semantics must map to existing hgraph schemas rather than define parallel
representations.

Numeric conversions and scalar-to-time-series constant lifting must match the
hgraph operator and wiring contracts. The language must not add implicit
conversions that cause it to choose a different operator overload from native
hgraph wiring.

## Graph semantics

A graph body executes once during wiring and returns one or more port handles.
Its local bindings therefore hold ports or wiring scalars, never current
time-series values.

A graph may:

- call nodes, graphs, and imported operators;
- bind and return ports;
- inspect resolved types and wiring scalar values;
- use ordinary conditionals over wiring-time facts to choose fixed topology;
- call explicit hgraph runtime control-flow operations when a time-series value
  must select behavior after wiring.

A graph may not:

- read `.value`, `.delta`, `.valid`, or `.modified` from a port;
- retain evaluation state;
- perform tick-time side effects;
- treat a time-series boolean as an ordinary wiring conditional.

These are compile-time phase errors, not generated C++ errors.

## Node semantics

A node body executes when scheduled by hgraph. Its inputs are typed views of
live time-series endpoints. Atomic input views initially expose:

- `.value` for the current value;
- `.valid` for whether a value exists;
- `.modified` for whether the endpoint ticked in the current cycle.

`emit expression` writes and ticks the declared output. Reaching the end of a
compute-node evaluation without `emit` produces no output tick. An output is
not implicitly repeated.

The first slice supports pure stateless compute nodes only. A later stateful
slice will distinguish:

- semantic state that affects future outputs and must lower to hgraph
  recordable state;
- explicitly ephemeral implementation cache that does not participate in
  record/replay.

The language will not silently use an opaque cache for semantic loopback state.
Lifecycle blocks, when introduced, will lower to typed `start` and `stop` hooks
and will expose only capabilities admitted by the language.

## Operators and calls

An operator name is a function-like contract, not an implementation. Calls to
imported operators are resolved using the hgraph candidate registry and its
type-pattern rules. A selected graph overload remains wiring-time composition;
a selected node overload remains a runtime primitive.

Named arguments are preserved through lowering. Wiring-time policy values
select an overload or immutable plan before evaluation. A fixed policy does not
become a string or enum branch in generated per-tick code.

## Control flow

Graph and node conditionals have different input domains:

- A graph `if` accepts a compile-time or wiring-time scalar boolean and selects
  fixed topology.
- A node `if` accepts an evaluation-time scalar boolean and selects work for
  the current evaluation.
- A time-series boolean in a graph requires an imported runtime switching,
  routing, mapping, or mesh operation.

Loops are deferred. When introduced, graph loops will require statically
bounded wiring-time iterables. Node loops will operate only over supported
bounded scalar or time-series collection views. Unbounded allocation and
history retention are outside the default execution model.

## Restricted capabilities

Language source cannot:

- include native headers or name arbitrary C++ symbols;
- open files, sockets, or processes directly;
- create threads, callbacks, mutexes, or push-source senders;
- allocate arbitrary native objects with unconstrained lifetime;
- register scalar types or native services;
- bypass hgraph wiring, scheduling, state, or lifecycle contracts.

Those capabilities live in C++ packages and are surfaced through reviewed
module descriptors at graph-safe call boundaries.

## Diagnostics

Diagnostics should identify the semantic phase as well as the source error.
Examples include:

- "time-series values are unavailable while wiring graph `smooth`";
- "operator `add` has no overload for `ts<str>` and `ts<i64>`";
- "runtime resource access is not available in a language node";
- "state affecting later ticks must be recordable".

The compiler should report imported candidate rejection reasons from hgraph
rather than replace them with a generic no-match message.
