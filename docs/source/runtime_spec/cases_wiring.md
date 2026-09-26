# Wiring cases

Status: validated expectations with recorded variations; see
[results](validation/wiring/README.md).

Each case wires a small graph and observes what wiring decided: the type of
a port (written without spaces), which candidate a call selected, whether a
call failed, and, where the decision shows at run time, the values a node
sees. Values are per cycle from `MIN_ST`; `—` means no publication.

`_identity(ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE` is a generic node that
publishes its input. `_to_ref(ts: REF[TS[int]]) -> REF[TS[int]]` publishes a
reference to its input. `tsl_to_tsd` publishes a map with a reference per
key, the shape a `map_` output has.

## WIRE-GENERIC-DEPTH — WIR-7, WIR-8

A list of three ints, `(1, 2, 3)` then `{1: 30}`, becomes a map with keys
`a`, `b`, `c` through `tsl_to_tsd`. A generic node shows the map's values.

| Observation | Expected |
|---|---|
| The map's type | `TSD[str,REF[TS[int]]]` |
| `_identity(map)`'s type: what the variable binds | `TSD[str,TS[int]]` |
| What the generic node sees at 0 | `a=1,b=2,c=3` |
| At 1 | `a=1,b=30,c=3` |

The variable binds the map with its element references removed. The input
follows each reference, so it sees values, and a value tick behind a
reference (`b` at 1) makes it modified. This is issue #847.

## WIRE-GENERIC-TOP — WIR-7, WIR-8

`_to_ref(value)` with value `1` then `2`.

| Observation | Expected |
|---|---|
| The source's type | `REF[TS[int]]` |
| `_identity(source)`'s type | `TS[int]` |
| What a generic node sees at 0, 1 | `1`, `2` |

## WIRE-REF-PATTERN — WIR-10

A node declares `ts: REF[TIME_SERIES_TYPE]`.

| Observation | Expected |
|---|---|
| Its output `REF[TIME_SERIES_TYPE]`, given the map above | `REF[TSD[str,TS[int]]]` |
| Given a plain `TS[int]`, does it receive a reference? | yes |

The variable binds beneath the declared reference; below it, references are
removed. Given a value output, the declared reference input receives a
reference to it.

## WIRE-STATED — WIR-11

`_identity` with its variable stated before matching.

| Observation | Expected |
|---|---|
| Stated `TSD[str, REF[TS[int]]]`, given the map above | `TSD[str,REF[TS[int]]]` |
| Stated `REF[TS[int]]`, given `_to_ref(value)` | `REF[TS[int]]` |

## WIRE-REQUESTED — WIR-12

`nothing` produces an output of the requested type and never ticks. Its
output pattern is a bare variable.

| Observation | Expected |
|---|---|
| `nothing[TSD[str, REF[TS[int]]]]` | `TSD[str,REF[TS[int]]]` |
| `nothing[REF[TS[int]]]` | `REF[TS[int]]` |
| `nothing[TSD[str, TS[int]]]` | `TSD[str,TS[int]]` |

## WIRE-PROJECTION — WIR-5, WIR-13

A node's declared output is a bundle with fields `routed: REF[TS[int]]` and
`plain: TS[int]`. The graph returns `routed` as a `TS[int]`; the input
publishes `1` then `2`.

| Observation | Expected |
|---|---|
| `bundle["routed"]` | `REF[TS[int]]` |
| `bundle.routed` | `REF[TS[int]]` |
| `getattr_(bundle, "routed")` | `REF[TS[int]]` |
| `bundle["plain"]` | `TS[int]` |
| The graph's output at 0, 1 | `1`, `2` |

Each spelling selects a field known at wiring: a projection, with no node
and no variable. The graph's `TS[int]` output follows the projected
reference.

## WIRE-PROJECTION-THROUGH-REF — WIR-5

`if_(condition, value)` publishes a reference to a bundle of references.
`condition` is true then false; `value` is `1` then `2`.

| Observation | Expected |
|---|---|
| `if_(...)["true"]` | `REF[TS[int]]` |
| The graph's `TS[int]` output at 0, 1 | `1`, `—` |

The field beneath a reference is known only at run time, so a node
publishes a reference to it. When the condition turns false the field's
reference becomes empty: the follower unbinds, which does not tick (TS-15,
TS-17).

## WIRE-SPECIFICITY — WIR-16, WIR-18

An operator `_pick` has three candidates: `TS[int]` returning `"int"`,
`TIME_SERIES_TYPE` returning `"generic"`, and `TSL[TIME_SERIES_TYPE, SIZE]`
returning `"tsl-generic"`.

| Call | Expected |
|---|---|
| `_pick(TS[int])` | `int` |
| `_pick(TS[float])` | `generic` |
| `_pick(TSL[TS[int], Size[2]])` | `tsl-generic` |
| `_pick(REF[TS[int]])` | `int` |

A concrete candidate beats a variable, a variable inside a list beats a bare
one, and a reference adds no specificity.

## WIRE-FAILURES — WIR-4, WIR-16

| Call | Expected |
|---|---|
| An operator with two `TS[int]` candidates, called with `TS[int]` | fails (ambiguous) |
| An operator with only a `TS[int]` candidate, called with `TS[str]` | fails (no candidate) |

A graph `failing_outer` calls a graph `_failing_inner`, which calls that
operator, `_only_int`, with `TS[str]`. The graph fails to wire, and the
error says:

| Observation | Expected |
|---|---|
| The operator, as declared (`_only_int`) | named |
| The argument's type, `TS[str]` | named |
| The candidate's parameter type, `TS[int]` | named |
| The graph path, `failing_outer` then `_failing_inner` | named |

A graph `g` calls a graph `attempt`, which adds a node and then makes that
failing call. `g` catches the error and returns its input instead.

| Observation | Expected |
|---|---|
| Wiring `g` | fails: a caught failure still fails the graph |

## WIRE-REPEATED — WIR-7, WIR-17

A node `_same(a: TIME_SERIES_TYPE, b: TIME_SERIES_TYPE)`.

| Call | Expected |
|---|---|
| `_same(REF[TS[int]], TS[int])` | wires; the variable binds `TS[int]` once |
| `_same(TS[int], TS[float])` | fails |

## WIRE-BUNDLE-IDENTITY — WIR-15, WIR-17

`Foo` and `Bar` are named bundles with the same single field `a: TS[int]`;
`{a: TS[int]}` is an unnamed bundle with that field. `_same(a: T, b: T)` is
the repeated-variable node above; `_takes_foo` and `_takes_unnamed` declare
a `Foo` and an unnamed input.

| Call | Expected |
|---|---|
| `_same(Foo, {a})` | wires: one is unnamed, so fields decide |
| `_same(Foo, Foo)` | wires |
| `_same(Foo, Bar)` | fails: both named, different names |
| `_takes_foo({a})` | wires |
| `_takes_unnamed(Foo)` | wires |
| `_takes_foo(Bar)` | fails |

## WIRE-OPERATOR-CONTRACT — WIR-21 to WIR-24

Four operators and their candidates. `_declares_generic(ts: T)` has one
candidate, `(ts: TS[int], scale: int = 2)`, with a parameter the operator
does not declare. `_refinable(ts: T)` has one candidate accepting only
`TS[int]`. `_needs_extra(ts: T)` has a fallback candidate `(ts: T)` and one
registered here, `(ts: TS[int], scale: int)`, whose extra parameter has no
default. `_declares_int(ts: TS[int])` gets a candidate accepting any
`TIME_SERIES_TYPE`.

| Call | Expected |
|---|---|
| `_declares_generic(TS[int])` | `extra 2`: the extra parameter's default (WIR-22) |
| `_declares_generic(TS[int], scale=5)` | `extra 5`: the call supplies it (WIR-22) |
| `_refinable(TS[int])` | `refined`: a narrower candidate is selected (WIR-23) |
| Registering `(ts: TS[int], scale: int)` | registered: no default is needed (WIR-22) |
| `_needs_extra(TS[int])` | `fallback`: the scaled candidate requires `scale` and does not match |
| `_needs_extra(TS[int], scale=3)` | `scaled 3`: both match; `TS[int]` is more specific |
| Registering the wider candidate for `_declares_int` | rejected (WIR-23, WIR-24) |
| `_declares_int(TS[float])` | fails: no candidate remains |

The HGL modules [contract_superset.hgl](validation/wiring/contract_superset.hgl)
and [contract_required_extra.hgl](validation/wiring/contract_required_extra.hgl)
declare an implementation with a `const scale` parameter its operator does
not declare, with and without a default; HGL must accept both (WIR-22).
[contract_extra_argument.hgl](validation/wiring/contract_extra_argument.hgl)
passes `scale=5.0` through the operator's call; HGL must accept it.
[contract_widening.hgl](validation/wiring/contract_widening.hgl) is a wider
implementation; HGL must reject it (WIR-23, WIR-24).

## WIRE-FRONT-END — WIR-14, WIR-7

The HGL module [front_end.hgl](validation/wiring/front_end.hgl) calls a
generic `pass<T>(value: T) -> T` with the argument types above. HGL must
bind as the runtime does.

| Call | Expected binding of `T` |
|---|---|
| `pass(value)`, `value: ref<f64>` | `f64` |
| `pass(values)`, `values: list<ref<f64>, 2>` | `list<f64, 2>` |
