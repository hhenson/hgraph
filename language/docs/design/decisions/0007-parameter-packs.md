# ADR 0007: explicit parameter-pack shapes

Status: accepted. Implemented for signatures, calls, composition and runtime
traversal, module descriptors, generated C++ operator contracts, native
runtime-node aggregate inputs, inclusive cardinality constraints, and the
`len`/`keys`/`types`/`type_at` constraint intrinsics, quantified `each`
constraints, and runtime schema views.

## Context

Hgraph operators need three materially different variadic shapes. Treating all
of them as one anonymous bundle would either erase useful type relationships or
leak the generated `_1`, `_2`, ... field names used by native packed inputs
representations into HGL source.

## Decision

HGL spells the three shapes explicitly:

```hgl
# Repeated values with one shared type. Positional only; native shape is TSL.
operator homogeneous<T>(values: ...T) -> T

# Positional values whose types may differ. HGL exposes a structural tuple.
operator positional<...Ts>(values: ...Ts) -> i64

# Named values whose names and types may differ. HGL exposes a bundle.
operator keyword<...Fields>(values: ...{Fields}) -> i64
```

The declaration, not the arguments at one call site, determines the shape.
Supplying equal types to a heterogeneous pack does not turn it into a
homogeneous pack.

A homogeneous positional pack unifies every captured value with the same `T`.
A heterogeneous pack checks each member independently and does not require a
common type. A type-pack name such as `Ts` or `Fields` is not a singular type:
it may only appear through its corresponding pack parameter and pack
reflection.

Fixed parameters are bound first. Remaining positional arguments enter the
positional pack; unmatched named arguments enter the named pack with their
source names intact. A call cannot repeat a fixed name, and a positional
argument cannot follow a named argument. Packs have no default and may be
empty. An empty homogeneous pack must obtain `T` from another position or the
call is not resolvable.

Candidate ordering remains owned by hgraph's operator resolver. The intended
specificity is fixed arity before a homogeneous pack before a heterogeneous
positional pack. HGL emits `In`/`VarIn`/`VarKwIn` selectors and does not
reimplement native overload ranking.

## Source views

The positional heterogeneous view behaves as `tuple<...>` even if generated
C++ uses a private bundle representation:

```hgl
for value in elements(values) { ... }
for index, value in items(values) { ... } // zero-based i64 index
```

The named heterogeneous view preserves names:

```hgl
for name in keys(values) { ... }
for value in values(values) { ... }
for name, value in items(values) { ... }
```

Private `_1`, `_2`, ... names are never HGL keys and must not appear in source
diagnostics, descriptors, or documentation. Composition traversal expands at
wiring time and passes erased ports to the ordinary hgraph resolver, so each
heterogeneous member retains its concrete schema.

## Runtime-node lowering

Runtime packs use hgraph's existing structural node inputs; HGL does not define
a second aggregate ABI:

| HGL parameter | Generated C++ node input | HGL source view |
| --- | --- | --- |
| `values: ...T` | `In<"values", Args<S>>` where `S` is the temporal schema for `T` | fixed homogeneous list |
| `values: ...Ts` | `In<"values", Kwargs<...>>` with private `_1`, `_2`, ... fields | heterogeneous tuple |
| `values: ...{Fields}` | `In<"values", Kwargs<...>>` with the supplied field names | heterogeneous bundle |

`Args` is resolved by hgraph as a fixed `TSL` and `Kwargs` as an unnamed TSB.
The existing `TSLInputView` and `TSBInputView` own runtime traversal, child
validity and modification metadata, and evaluation-local borrowing. The HGL
compiler is responsible only for lowering to those selectors and preserving the
source abstraction. In particular, positional `items(values)` returns a
zero-based `i64` index even though its private C++ field is numbered from `_1`.

A runtime function currently accepts one aggregate pack input. A composition
function may combine positional and named packs, but spelling both on the same
runtime function is diagnosed before C++ emission until the native static-node
ABI can bind two independent aggregate inputs.

An empty runtime pack whose schema is otherwise resolved must be constructed
with that resolved aggregate schema; it cannot infer its schema from children.
This requires the core runtime to represent a fixed-empty `TSL` separately from
an unbounded `TSL`: the unbounded extent uses hgraph's original `-1` sentinel,
leaving zero as an ordinary fixed extent. Runtime-node pack lowering is
available because the core contract preserves this distinction.

## Cardinality

A pack without a cardinality accepts zero or more arguments. A regex-like
suffix constrains its inclusive cardinality:

```hgl
operator exactly_two<T>(values: ...T{2}) -> T
operator at_least_two<T>(values: ...T{2:*}) -> T
operator bounded<...Ts>(values: ...Ts{2:8}) -> i64
operator named<...Fields>(values: ...{Fields}{1:*}) -> i64
```

`{n}` means exactly `n`, `{n:*}` means at least `n`, and `{n:m}` means from
`n` through `m`. Omitting the suffix is `{0:*}`. Cardinality rejects a candidate
during call normalization; it does not establish a second overload-ranking
algorithm.

## Pack reflection

Pack generics are compile-time structures. A positional type pack exposes its
ordered member types; a named field pack exposes ordered name/type pairs:

```hgl
len(Ts)
types(Ts)
type_at(Ts, 0)

len(Fields)
keys(Fields)
types(Fields)
type_at(Fields, "price")
```

This supports constraints such as:

```hgl
requires "price" in keys(Fields)
      && type_at(Fields, "price") in {i64, f64}

requires each T in types(Ts) {
    format_value(T) -> str
}
```

The `each` block is a compile-time conjunction. Its body must hold for every
member type and is vacuously true for an empty pack. The binding after `each`
is local to the block, and a forwarded generic function may satisfy the
constraint with an alpha-equivalent `each` premise. At runtime the value
parameter retains the ordinary collection vocabulary: `elements`/`items` for positional packs and
`keys`/`values`/`items` for named packs. A native operation that genuinely
needs runtime type metadata may consume `schemas(values)`; `types(...)` remains
compile-time reflection:

```hgl
native fn known(value: schema) -> bool {
    cpp(const hgraph::TSValueTypeMetaData *value) {
        return value != nullptr;
    }
}

for index, value_schema in items(schemas(values)) {
    if known(value_schema) { ... }
}
```

`schema` is not a materialized HGL value. It is a native-parameter-only,
immutable borrowed handle which lowers directly to each existing C++ child
endpoint's `TSValueTypeMetaData`. The view preserves the pack's tuple or bundle
shape: positional views support `elements` and `items`; named views support
`keys`, `values`, and `items`. Schema views do not accept value predicates.
Neither a view nor one of its handles may be stored, returned, captured, used
as state or output, or retained beyond the evaluation.

The four reflection intrinsics above are implemented for concrete calls,
forwarded packs, and positive equality inference such as `N == len(Ts)`.
Quantified `each` constraints are implemented for concrete and forwarded
packs. At runtime `schemas(values)` is a compiler-only borrowed view. Generated
C++ iterates the already-bound `TSLInputView` or `TSBInputView` and passes each
child's `.schema()` pointer directly. It creates no schema collection, copies
no metadata, and performs no registry lookup.
