# User Guide

This guide describes the emerging source language from an author's point of
view: how functions and types look, which values change over time, and how
source calls reach hgraph.

> **Design preview:** the current `hgl` command does not parse these examples.
> The documents record syntax agreed during design discussion, not a source
> compatibility promise.

## Read in this order

1. [Language tour](language-tour.md) introduces a complete module.
2. [Functions](functions.md) covers composition and runtime functions,
   parameters, state, injectables, lifecycle, activation, and output.
3. [Types and expressions](types-and-expressions.md) defines recursive temporal
   types, the `atomic<T>` boundary, metadata, and runtime collection traversal.
4. [Modules and tools](modules-and-tools.md) covers native modules, `check`,
   `run`, `build`, and the REPL.

Source files are collected under [`language/examples`](../../examples). Until
the frontend lands, they are checked documentation rather than executable
programs.

## Current language shape

The source language presents one callable abstraction: `fn`. It does not ask
authors to declare a `graph` or `node`.

```hgl
fn midpoint(
    tob: atomic<tuple<f64, f64>>
) -> f64 =>
    (tob[0] + tob[1]) / 2.0
```

Parameters are temporal by default. `const` marks a wiring-time value:

```hgl
fn smooth(
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
        out = (out if valid(out) else 0.0) + value
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
