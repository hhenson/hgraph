# Imported values, reference types, and SIGNAL inputs

Status: agreed source semantics, 2026-09-05; compiler implementation is outside
this change. The collection-reference mapping noted below still needs
clarification. This record introduces no native declaration syntax.

## Imported types are atomic values

Imported C++ and Python types are not time-series types. They participate as
scalar value types, like `i64`, `f64`, and `str`:

- An ordinary temporal parameter carries complete imported values on one
  atomic endpoint.
- A `const` parameter carries a wiring-time scalar value.
- An explicit `atomic` annotation is not required.
- An imported type remains an atomic leaf inside an HGL list, map, or struct.
- Native fields do not cause recursive temporalization or acquire independent
  tick histories merely because the type has been imported.

HGL-declared structs retain their existing structural temporalization. An
imported structured object does not acquire that behavior. Importing a type
preserves its native value identity and supported operations; it does not
import a time-series shape. The declaration mechanism for exposing those types
and operations remains to be discussed.

## Reference spelling and type compatibility

The agreed spelling is `ref<T>`, wrapping an existing HGL type. The user or
component designer specifies REF to declare what the component intends to
access or pass through. Use it only where the component intends to forward a
time series without observing or interacting with its values. It is not a
default annotation for every connection or a general inferred rewrite of
user-declared types.

For compiler-generated conditional branch callables, the compiler specifies
that component's access intent: a branch which only forwards an enclosing
variable's incoming binding takes it by reference. An ordinary temporal input
is sufficient when the branch is known to process it. This narrowly scoped
capture rule leaves the author's variable declaration unchanged; see
[Forwarding an existing binding](control-flow.md#forwarding-an-existing-binding).

For example:

```text
ref<map<i64, str>> -> REF[TSD[int, TS[str]]]
```

Reference wrappers do not participate in underlying type compatibility:

```text
ref<map<i64, str>> ~ map<i64, str>
```

The same compatibility principle applies within a type's structure. The
underlying types must still match. Compatibility does not erase the reference
from its endpoint representation or grant a node access through it.

Type formation is checked separately. A map key cannot be a reference:

```text
map<ref<i64>, str> -> error
```

Ignoring REF for compatibility does not make an invalid type formation legal.

## Node access and ticks

Inside node evaluation, including a `when` handler, a reference is an opaque
value. Code cannot inspect the values of the schema below the reference:
field access, collection indexing, and traversal through that reference are
not value-reading operations available to the node.

The reference ticks only when its binding changes. A value tick on the
referenced endpoint does not itself tick the reference. A binding change is
observable even when the old and new targets currently hold equal values.

REF-transparent type compatibility and opaque node access are distinct rules.
An ordinary non-reference input can consume a reference producer through
normal hgraph binding adaptation. A reference-typed node input retains the
opaque reference view; it does not implicitly dereference itself during
evaluation.

## Selecting and forwarding a reference

The routing example from the discussion illustrates the intended use. This
is a signature/body fragment; its enclosing generic declaration is omitted:

```text
route(r: i64, l: list<ref<T>, S>) -> ref<T> {
    when modified(r) {
        return l[r]
    }
}
```

The node observes the index `r`. The list itself is accessible, and indexing
it obtains one opaque `ref<T>` element. This does not cross that element's
reference boundary or read a value of `T`. The node returns the selected
reference, establishing the connection through which consumers observe the
selected time series.

After that connection is established, value ticks from the selected target
do not require this routing node to evaluate, copy the value, or emit it
again. Reference inputs observe binding changes, rather than value ticks
behind those bindings. The example's explicit handler condition remains
`modified(r)`; reference tick semantics do not silently add another handler
condition.

The position of the boundary determines which operations are available:

| Node input | Permitted access in evaluation |
| --- | --- |
| `list<ref<T>, S>` | Access the list and select an opaque reference element. |
| `ref<list<T, S>>` | Handle the opaque reference; do not index the list beneath it. |

An ordinary list of temporal values instead requires a value-forwarding node
to observe and copy each selected value tick that it forwards. REF expresses
that this component is responsible for selecting the connection, without
processing the values subsequently carried by it.

## Wiring-time access

During wiring, code may access elements beneath a reference layer. Such access
causes dereferencing to obtain the element of interest. The result of wiring
is a connection to that element, not a read of its current runtime value.

This permits graph composition to select fields or collection elements through
reference-backed sources while preserving the node-level access restriction.
Reference binding and adaptation belong to the existing hgraph runtime.

## SIGNAL inputs

SIGNAL is an input-only observation contract. It accepts a time-series input
without exposing the input's value or structure. Its only observation
operations are:

- `modified`: whether the input was modified in the current evaluation cycle;
- `valid`: whether the input is valid;
- `last_modified`: the input's last modification time.

Value access is unavailable, including any value or delta payload that an
underlying native representation may technically expose. The intended source
contract does not permit arithmetic, Boolean value tests, field access,
indexing, or traversal of the connected data through a SIGNAL input.

There is no SIGNAL output in the language contract. There is consequently no
signal-emission operation or literal to design. Native implementation details
must not broaden this source contract.

SIGNAL is primarily meaningful inside a node, where those observation
operations can control evaluation. Graph functions may also accept SIGNAL
inputs and pass them to components with compatible inputs. This does not make
the graph body execute on ticks or expose a runtime value during wiring.

The semantics above are agreed; the HGL type spelling remains to be confirmed.

## Collection-reference mapping to clarify

The second mapping supplied during the discussion was:

```text
map<i64, ref<str>> -> REF[TSD[int, REF[TS[str]]]]
```

This includes an outer REF without an explicit outer `ref` in the source.
Whether that outer REF is intentional, and the rule that would add it, are
awaiting clarification. Do not infer a general reference-propagation rule or
silently remove the outer REF from this example.

## Scope of this agreement

This record does not settle SIGNAL spelling, native declaration syntax,
reference construction or mutation operations, or additional restrictions on
reference placement. Those remain separate discussion items. No changes to
the parser, compiler backends, native runtime, or executable examples accompany
this record.
