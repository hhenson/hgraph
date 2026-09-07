# Testing and running

Status: `test`, `assert`, and `eval` over dense sequences, `hgl run` from the
command line, and the REPL are implemented; every example on this page runs
as written unless it is labelled provisional. Timed sequences and the TOML
run configuration are provisional: their agreed form is shown in labelled
snippets that the current `hgl` rejects. The specification is
[Tests and the evaluation harness](../developer-guide/syntax-and-semantics.md#tests-and-the-evaluation-harness)
and [Running a module](../developer-guide/syntax-and-semantics.md#running-a-module);
the [roadmap status matrix](../design/roadmap.md#feature-status-matrix-2026-09-07)
records the status of each form.

A module carries its own tests, and a module is run from outside its source.
This page shows both from the author's side.

## Tests live beside the code

A `test` declaration is a named block at module scope. It sees everything
its module declares, exported or not, so a private helper can be tested
without exporting it:

```hgl
module examples.midpoint

fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 =>
    (tob[0] + tob[1]) / 2.0

test midpoint_ticks {
    assert eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)]) == [1.5, 2.5]
}
```

`hgl test path/to/program.hgl` runs every test in the module and reports
each failing assertion with the cycle at which the expected and observed
outputs first differ:

```text
midpoint_ticks ... ok
1 test, 0 failed
```

Test names after the file select a subset. Tests are never part of a built
artifact.

`assert` takes any wiring-time `bool` expression. `eval` drives a function
through hgraph's replay and record harness: its first argument is a module
`fn`, the rest bind to that function's parameters exactly as a call would,
positionally or by name. To evaluate an operator, wrap it in a `fn`.

## Dense sequences

A temporal parameter receives a *sequence*: one element per engine cycle,
starting at the first cycle of the run. `_` means the input does not tick in
that cycle. The result of `eval` is the output's sequence, with `_` where
the output did not tick:

```hgl
test midpoint_skips_silent_cycles {
    assert eval(midpoint, tob: [(1.0, 3.0), _, (2.0, 4.0)]) == [2.0, _, 3.0]
}
```

Cycle 1 has no input, so `midpoint` does not tick and the expected sequence
says `_` there. An element has the shape of the parameter's value type: a
scalar for `f64`, a tuple for `atomic<tuple<f64, f64>>`. Because the tuple
is `atomic`, it arrives whole and `_` stands for the whole element. The
observed sequence runs through the later of the last input cycle and the
last output tick, and `==` requires the same length and equal elements.

A `const` parameter receives a constant, not a sequence:

```hgl
fn scale(value: f64, const factor: f64) -> f64 => value * factor

test scale_applies_factor {
    assert eval(scale, value: [1.0, _, 2.0], factor: 3.0) == [3.0, _, 6.0]
}
```

A function without an output can still be evaluated as a statement, which
runs it to completion.

> **Provisional: structural tuple elements.** For a structural
> `tuple<f64, f64>` parameter the agreed element shape is a tuple literal
> whose positions are values or `_` for a field that does not tick, so a
> bid-only cycle would be written `(1.0, _)`:
>
> ```hgl
> fn midpoint(tob: tuple<f64, f64>) -> f64 =>          // not runnable today
>     (tob[0] + tob[1]) / 2.0
>
> test midpoint_waits_for_both_sides {
>     assert eval(midpoint, tob: [(1.0, _), (_, 3.0), (2.0, _)])
>         == [_, 2.0, 2.5]
> }
> ```
>
> The current `hgl test` rejects it (`a structural tuple has no time-series
> schema; use atomic<tuple<...>> for one value`): `eval` drives scalar and
> `atomic` parameters only.

## Timed sequences

> **Provisional.** Timed sequences parse, but the harness does not run them
> (`timed sequences are not supported by the first pass; write one value per
> cycle`), and `eval` does not yet accept a `rolling` parameter. The agreed
> form is recorded here so that dense tests are not written in a shape that
> will change.

When the timing matters, key each element by a time. A `duration` key is an
offset from the start of the run; a `datetime` key is an absolute instant and
fixes the run's start:

```hgl
use hgraph.std::{mean}

fn recent_mean(price: rolling<f64, 5m>) -> f64 => mean(price)   // not runnable today

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
*entry*; its `const` parameters are bound from the command line and its
output, if any, is the run's output:

```hgl
module examples.app

use hgraph.std::{schedule}

export fn heartbeat(const every: duration = 1s) -> datetime =>
    last_modified(schedule(every))
```

Run it from the command line; each tick of the entry's output is printed as
a `time value` line:

```text
hgl run examples/app.hgl --mode sim --start 2026-09-03T08:00:00Z \
    --end 3s --set every=1500ms
2026-09-03T08:00:01.5Z 2026-09-03 08:00:01.500000
```

A module with exactly one entry needs no `--entry`. `--set` values are HGL
constant expressions checked against the parameter's declared type
(`--set every=3` is `type: 'every' expects timedelta, got int`); a parameter
with a default may be left unset. `--start` takes a datetime with or without
its `@`, and `--end` a datetime or a duration after the start. The mode,
start, and end default to hgraph's own run defaults: a simulation starts at
the engine origin and ends when nothing remains scheduled; a real-time run
starts now.

> **Provisional: configuration file.** The agreed `--config run.toml` form
> mirrors the command line, with command-line options overriding the file.
> The current `hgl run` does not read it, so this is not runnable today:
>
> ```toml
> [run]
> entry = "heartbeat"
> mode = "realtime"
> start = 2026-09-03T08:00:00Z
> end = "1d"
>
> [run.params]
> every = "1500ms"
> ```
>
> TOML values would bind by their type: integer to `i64`, float to `f64`,
> string to `str` or, for a temporal parameter, the HGL literal spelling such
> as `"1d"`, offset date-time to `datetime`, local date to `date`, array to
> `list`.

The same module therefore runs as a backtest and as a live process with
nothing changed in the source. That is why the language has neither a
`main` nor a `run(fn, config)` form.

## The REPL

`hgl repl` accepts declarations and test bodies interactively. A `test`
block entered at the prompt runs immediately; a bare `eval(...)` at the
prompt prints the observed sequence, which is the quickest way to see what
a function does:

```text
hgl> let x = 1 + 2
hgl> x
3
hgl> fn midpoint(tob: atomic<tuple<f64, f64>>) -> f64 => (tob[0] + tob[1]) / 2.0
hgl> eval(midpoint, tob: [(1.0, 2.0), (2.0, 3.0)])
[1.5, 2.5]
hgl> test again { assert eval(midpoint, tob: [(2.0, 4.0)]) == [3.0] }
again ... ok
1 test, 0 failed
```

The REPL rebuilds the session after each accepted input, so what it shows is
what the same source does under `hgl test` and `hgl run`. A declaration
that does not check is reported and dropped; the session keeps its last
valid state. `let` and `var` bindings entered at the prompt persist; binding
a function to a name (`let half = midpoint`) is accepted but the name cannot
then be passed to `eval`, which requires an exact HGL function.
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

The current `hgl` runs every example on this page that is not labelled
provisional. The limits, each reported by name:

- `eval` drives scalar and `atomic` parameters; a structural tuple, list,
  set, map, or rolling parameter is reported as unsupported;
- timed sequences are reported as unsupported; write one value per cycle;
- `eval` takes a module `fn`; wrap an operator in a `fn` to evaluate it;
- `hgl run` takes its configuration from the command line only; the
  `--config` file is not read;
- file-based `test` and `run`, and the REPL, compile supported runtime
  functions and generic `impl fn` candidates on Unix only; an unresolved
  generic composition call is still outside the direct-wiring path, while the
  generated backend supports the concrete generic operator and window forms
  used by the examples;
- complete scalar struct values, type-only generic struct specializations,
  `atomic<S>` harness values, and simple field-wise temporal struct
  construction run; generic constructor inference, `const` generic struct
  identity, multiple inheritance, temporal structured deltas, and explicit
  optional-field clearing are reported as unsupported;
- a `for` statement inside a `test` body is reported as unavailable;
- types in diagnostics are printed with hgraph's names (`float`,
  `TS[float]`, `Tuple[float,float]`).

The [roadmap status matrix](../design/roadmap.md#feature-status-matrix-2026-09-07)
is the complete list.

## What runs where

Programs made only of composition functions, which includes every runnable
example on this page, are wired straight onto the hgraph runtime in process
by `hgl test`, `hgl run`, and the REPL; no native toolchain is involved. For
a file containing a runtime function (`state`, `inject`, `when`, ...) or an
`impl fn`, `hgl test` and `hgl run` emit the complete module, compile a
content-addressed native image or reuse a complete cached image, load its
candidates into the same hgraph registry, and then use the ordinary harness.
A failed native build reports and retains its artifact directory; incomplete or
digest-mismatched cache entries are never loaded. The REPL uses the same image
format and cache. It compiles a complete candidate session before removing the
old provider, restores that provider if activating the replacement fails, and
keeps native images mapped for process lifetime so removed callbacks cannot
dangle. The
[Architecture](../design/architecture.md#two-backends-one-wiring) record
describes the split; both backends must produce the same ticks.
