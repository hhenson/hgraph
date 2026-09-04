# User Guide

This guide describes the emerging source language from an author's point of
view: how functions and types look, which values change over time, and how
source calls reach hgraph.

> **Design preview:** the current `hgl` command checks these examples and
> runs composition functions directly and, for file-based `hgl test` and
> `hgl run`, compiles, caches, and loads the documented scalar runtime-node
> subset on Unix. The REPL uses the same native route and transactionally
> replaces the session image when a runtime declaration is accepted.
> The remaining limits are listed in
> [Testing and running](testing-and-running.md#first-pass-limits). The
> documents record syntax agreed during design discussion, not a source
> compatibility promise.

## Read in this order

1. [Language tour](language-tour.md) introduces a complete module.
2. [Functions](functions.md) covers composition and runtime functions,
   public exports, generic constraints and substitution, operator contracts,
   state, injectables, lifecycle, activation, and output.
3. [Types and expressions](types-and-expressions.md) defines recursive temporal
   types, nominal and generic structs, abstract data families and final
   concrete values, generic construction, sparse deltas, optional fields,
   rolling windows, the `atomic<T>` boundary, metadata, and runtime collection
   traversal.
4. [Modules and tools](modules-and-tools.md) covers public declarations,
   implementation discovery, compiled module lifecycle, native modules,
   `check`, `test`, `run`, `build`, and the REPL.
5. [Testing and running](testing-and-running.md) covers `test` declarations,
   `eval` with dense and timed sequences, running an entry from the command
   line or a configuration file, and the REPL.

Source files are collected under [`language/examples`](../../examples). The
frontend checks every example; the scripted backends run the supported subset described in
[Testing and running](testing-and-running.md#first-pass-limits).

## Current language shape

The source language uses `fn` for every implementation and does not ask authors
to declare a `graph` or `node`. A bodyless `operator` declares a nominal,
generic callable contract whose implementations are supplied by compatible
`impl fn` definitions. Operators are public by definition; an ordinary
exact function is module-internal unless declared `export fn`.

```hgl
fn midpoint(
    tob: atomic<tuple<f64, f64>>
) -> f64 =>
    (tob[0] + tob[1]) / 2.0
```

Parameters are temporal by default. `const` marks a wiring-time value:

```hgl
export fn smooth(
    tob: atomic<tuple<f64, f64>>,
    const window: i64
) -> f64 {
    rolling_mean(midpoint(tob), window)
}
```

Within a body, `let` introduces an immutable local and `var` introduces a
mutable local. Runtime `var` values last only for the current block execution;
persistent values use `state`.

The current design classifies a function from the constructs used in its body:

- an ordinary expression body describes wiring composition;
- `state`, `inject`, `start`, `when`, `stop`, or runtime collection traversal
  makes the complete function a runtime implementation compiled as one node.

Runtime functions use ordinary `return` to produce an output tick. They may
request direct output access alongside other runtime capabilities:

```hgl
fn running_total(value: f64) -> f64 {
    inject out

    when modified(value) && valid(value) {
        if valid(out) {
            out += value
        } else {
            out = value
        }
    }
}
```

This lets the same `fn` declaration either compose existing operations or
fuse per-tick work into one generated node without adding `graph` and `node`
declaration keywords.

## Native boundary

Language functions call typed contracts supplied by hgraph and extension
modules. A selected native implementation may internally be a graph, compute
node, sink, source, or service. That implementation category is not part of
ordinary call syntax.

External threads, callbacks, queues, I/O, resource ownership, and push
adaptors remain C++ extension responsibilities.
