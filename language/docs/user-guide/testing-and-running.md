# Testing and running

Status: proposed (2026-09-03). The specification is
[Tests and the evaluation harness](../developer-guide/syntax-and-semantics.md#tests-and-the-evaluation-harness)
and [Running a module](../developer-guide/syntax-and-semantics.md#running-a-module).

A module carries its own tests, and a module is run from outside its source.
This page shows both from the author's side.

## Tests live beside the code

A `test` declaration is a named block at module scope. It sees everything
its module declares, exported or not, so a private helper can be tested
without exporting it:

```hgl
module examples.midpoint

fn midpoint(tob: tuple<f64, f64>) -> f64 =>
    (tob[0] + tob[1]) / 2.0

test midpoint_ticks {
    assert eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)]) == [1.5, 2.5]
}
```

`hgl test path/to/program.hgl` runs every test in the module and reports
each failing assertion with the cycle or time at which the expected and
observed outputs first differ. Tests are never part of a built artifact.

`assert` takes any wiring-time `bool` expression. `eval` drives a function
through hgraph's replay and record harness: its first argument is the
function or operator, the rest bind to that callee's parameters exactly as a
call would, positionally or by name.

## Dense sequences

A temporal parameter receives a *sequence*: one element per engine cycle,
starting at the first cycle of the run. `_` means the input does not tick in
that cycle. The result of `eval` is the output's sequence, with `_` where
the output did not tick:

```hgl
test midpoint_waits_for_both_sides {
    assert eval(midpoint, tob: [(1.0, _), (_, 3.0), (2.0, _)])
        == [_, 2.0, 2.5]
}
```

Cycle 0 ticks only the bid, so the bundle is not yet valid and `midpoint`
does not tick. Cycle 1 supplies the ask, the bundle becomes valid, and the
output is `2.0`. Cycle 2 updates the bid alone; the ask keeps its last value.

An element has the shape of the parameter's type: a scalar for `f64`, a
tuple for `tuple<f64, f64>` with `_` in a position whose field does not
tick. This `midpoint` takes a structural tuple, which is why its fields tick
independently; the `atomic<tuple<f64, f64>>` version in the
[language tour](language-tour.md) takes whole tuples, and `_` then stands
for the whole element. The observed sequence runs through the later of the
last input cycle and the last output tick, and `==` requires the same length
and equal elements.

A `const` parameter receives a constant, not a sequence:

```hgl
fn scale(value: f64, const factor: f64) -> f64 => value * factor

test scale_applies_factor {
    assert eval(scale, value: [1.0, _, 2.0], factor: 3.0) == [3.0, _, 6.0]
}
```

A function without an output can still be evaluated as a statement, which
runs it to completion.

## Timed sequences

When the timing matters, key each element by a time. A `duration` key is an
offset from the start of the run; a `datetime` key is an absolute instant and
fixes the run's start:

```hgl
fn recent_mean(price: rolling<f64, 5m>) -> f64 => mean(price)

test recent_mean_spans_five_minutes {
    assert eval(recent_mean, price: [0s: 1.0, 2m: 3.0, 5m: 5.0, 9m: 7.0])
        == [5m: 3.0, 9m: 5.0]
}
```

A timed expected sequence lists exactly the ticks the output produced, at
their times; there is no `_` because a silent instant is simply absent.
Within one `eval`, every sequence is either dense or timed.

## Running a module

A module has no `main`. Any `export fn` with no temporal parameters is an
*entry*; its `const` parameters are bound from the run configuration and its
output, if any, is the run's output:

```hgl
module examples.app

use acme.market_data::{subscribe_quotes}
use examples.prices::{smooth}

export fn main_graph(const symbols: list<str>, const window: i64) -> f64 =>
    smooth(subscribe_quotes(symbols), window)
```

Run it from the command line:

```text
hgl run examples/app.hgl --mode sim --start 2026-09-03T08:00:00Z \
    --end 1d --set window=20 --set 'symbols=["AAPL", "MSFT"]'
```

or from a configuration file, `hgl run examples/app.hgl --config run.toml`:

```toml
[run]
entry = "main_graph"
mode = "realtime"
start = 2026-09-03T08:00:00Z
end = "1d"

[run.params]
window = 20
symbols = ["AAPL", "MSFT"]
```

Command-line options override the file. A module with exactly one entry
needs no `--entry`. `--set` values are HGL constant expressions checked
against the parameter's declared type; TOML values bind by their type
(integer to `i64`, float to `f64`, string to `str` or, for a temporal
parameter, the HGL literal spelling such as `"1d"`, offset date-time to
`datetime`, local date to `date`, array to `list`). The mode, start, and end
default to hgraph's own run defaults: a simulation starts at the engine
origin and ends when nothing remains scheduled; a real-time run starts now.
Each tick of the entry's output is printed as a `time value` line.

The same module therefore runs as a backtest and as a live process with
nothing changed in the source. That is why the language has neither a
`main` nor a `run(fn, config)` form.

## The REPL

`hgl repl` accepts declarations and test bodies interactively. A `test`
block entered at the prompt runs immediately; a bare `eval(...)` at the
prompt prints the observed sequence, which is the quickest way to see what
a function does:

```text
hgl> fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0
hgl> eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)])
[1.5, 2.5]
hgl> let half = midpoint
hgl> test again { assert eval(half, tob: [(2.0, 4.0)]) == [3.0] }
again ... ok
1 test, 0 failed
```

The REPL rebuilds the session after each accepted input, so what it shows is
what the same source does under `hgl test` and `hgl run`. A declaration
that does not check is reported and dropped; the session keeps its last
valid state. `let` and `var` bindings entered at the prompt persist.
Input continues on the next line while a bracket is open; `:list` shows the
session, `:help` the commands, and `:quit` leaves.

On a terminal the prompt is an editable line: the arrow keys move and recall
earlier inputs, history persists across sessions in `~/.hgl_history` (or the
file named by `HGL_HISTORY`), and tab completes the `:` commands, the
declaration keywords, the kernel modules, and every name the session has
declared or bound. Piped input (`hgl repl < session.hgl`) reads plain lines,
so scripts behave as before; `HGL_NO_LINE_EDITING=1` forces that mode on a
terminal too.

## First-pass limits

The current `hgl` runs everything on this page that is written with
`atomic` tuples and dense sequences. Until the next steps land:

- `eval` drives scalar and `atomic` parameters; a structural tuple, list,
  set, map, or rolling parameter is reported as unsupported;
- timed sequences are reported as unsupported; write one value per cycle;
- `eval` takes a module `fn`; wrap an operator in a `fn` to evaluate it;
- `hgl run` takes its configuration from the command line only; the
  `--config` file is not read yet;
- a program with a runtime function, an `impl fn`, or a generic function
  checks, but running it is reported as unsupported;
- complete scalar struct values, type-only generic struct specializations,
  `atomic<S>` harness values, and simple field-wise temporal struct
  construction run; generic constructor inference, `const` generic struct
  identity, multiple inheritance, temporal structured deltas, and explicit
  optional-field clearing are reported as unsupported;
- types in diagnostics are printed with hgraph's names (`float`,
  `TS[float]`, `Tuple[float,float]`).

## What runs where

Programs made only of composition functions, which includes every example
on this page, are wired straight onto the hgraph runtime in process by
`hgl test`, `hgl run`, and the REPL; no native toolchain is involved. A
program that contains a runtime function (`state`, `inject`, `when`, ...)
needs generated C++. `hgl emit-cpp` supplies the supported scalar runtime-node
subset for ahead-of-time packages; teaching these three scripted commands to
build and load that artifact is the next implementation slice. The
[Architecture](../design/architecture.md#two-backends-one-wiring) record
describes the split; both backends must produce the same ticks.
