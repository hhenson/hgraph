# Native surface completion

Status: accepted names and direction; implemented coverage and remaining work
are distinguished below. The [module inventory](../../stdlib/hgl/hgraph/README.md)
records the source-native bindings under the single `hgraph.native` identity.

## Values are values

Type erasure is an implementation detail, not a separate HGL value category.
Reading a value uses ordinary HGL expressions and operations. A delta is obtained
with the existing name `delta_value`, regardless of whether its type is named,
generic, or known only through runtime metadata. Do not introduce `value_equals`,
erased-value accessors, or different spelling based on the C++ representation.

This decision does not claim that the compiler already lowers every value and
delta shape. Missing transport, borrowing, and operator implementations are
compiler/library work behind the same language surface.

## Membership and access

| Function | Contract | Implementation in this slice |
| --- | --- | --- |
| `key_set(value)` | The live set of map keys | Existing composition path plus runtime map-input projection |
| `modified(key_set(value))` | Keys were added or removed, not merely child values changed | Runtime membership-delta query; structural activation for a membership-only `when` |
| `contains(collection, key)` | Membership, independent of a child's validity | Runtime sets/maps/key-set projections; strings also support substring membership |
| `at(value, key_or_index)` | Strict access; missing key or out-of-range index is an error | Runtime map/list children and tick-window samples |
| `get(value, key_or_index, default=null)` | Safe lookup with a caller-supplied fallback, defaulting to `null` | Accepted; not implemented yet |

The `default=null` notation above describes the parameter default. HGL named
call arguments retain their existing colon syntax: `get(value, key, default: 0)`.
The new collection access intrinsics currently take positional arguments.

```hgl
fn membership_changed(value: map<i64, f64>) -> bool {
    when modified(key_set(value)) { return true }
}

fn read_price(value: map<i64, f64>, key: i64) -> f64 {
    when { return at(value, key) }
}

fn key_changes(value: map<i64, f64>) -> i64 {
    when {
        let ks = key_set(value)
        var count = 0
        for key in elements(ks, added) { count += 1 }
        for key in elements(ks, removed) { count += 1 }
        return count
    }
}
```

`key_set` is borrowed within the evaluation; it does not copy keys into a new
container. Its added/removed ranges use the input's current projected delta,
not stale storage changes from an earlier evaluation. A local projection must
use `let`, not mutable `var`. The projection must not escape the evaluation.
An ordinary child-only tick is rejected by the membership timestamp without
scanning keys. A sampled reference rebind uses the input's projected key ranges.
`last_modified` on a runtime key-set projection is deliberately rejected until
the compiler can retain membership history across rebinds. A composition-level
`key_set` endpoint already supplies persistent tracking.

The raw C++ `TSDInputView::structure_modified()` also reports child-only delta
epochs. It is not the implementation of the HGL membership predicate. Raw C++
View methods and their spelling remain unchanged.

Open detail for `get`: whether a present but invalid child returns the fallback
or remains distinct from an absent key/index. The proposed rule is absent-only,
preserving removal versus invalidation; this detail still needs agreement.
The compiler also needs a general nullable-expression/result path: its current
`null` lowering is limited to optional struct fields and sparse deltas. Neither
an invented zero value nor an implicit no-output tick implements nullable lookup.

## Windows

The accepted initial accessors are `at(window, index)`, `time_at(window, index)`,
`front(window)`, `back(window)`, and `removed_value(window)`. They now lower for
tick-count windows and are tested through ring-buffer wraparound. Indices are
zero-based in oldest-to-newest logical order. Strict bounds errors propagate
through the generated node; they are not hidden inside a `noexcept` wrapper.
List `front` and `back` use the same strict bounds policy.
Strict list/map value access also rejects a present but invalid child instead
of reading its retained storage. `valid(at(value, key))` can inspect the child
without reading its payload, but the key/index must still exist.

```hgl
use hgraph.native as native

fn evicted(value: rolling<i64, 3, 1>) -> i64 {
    when {
        if native::has_removed_value(value) { return removed_value(value) }
    }
}
```

`removed_value` is the evaluation's evicted sample. Guard it with
`has_removed_value`; it is not an accessor for an arbitrary historical sample.
Existing native window metadata includes `capacity`, `period`, `min_period`,
`is_full`, `has_removed_value`, and `first_modified`.

## Implementation boundaries and next work

The newly implemented collection accessors are compiler intrinsics in runtime
bodies, not additional descriptor-native overloads. They use the existing C++
typed input APIs. Only `key_set` also has composition lowering in this slice.
The source-native ABI still needs standalone generic value arguments,
dependent/borrowed results, and exception/effect metadata before all these
functions can move behind ordinary imported native declarations.

Further work, using the same ordinary value operations:

- General nullable `get`, including its invalid-child policy.
- Uniform current/delta-value expression lowering for atomic collections,
  tuples, structs and generic values; no representation-specific accessors.
- Atomic collection lookup and traversal, which must use scalar collection
  views rather than pretending they are TSL/TSS/TSD endpoints.
- Native bundle/reference patterns and duration-window const generics.
- Numeric, text and temporal value operations beyond existing arithmetic and
  predicates; preserve existing C++ semantics and flag genuinely undecided
  names or policies separately.

Storage pointers, slot indices, notifiers, binding mutation and ownership hooks
are not proposed HGL functions. Output mutations retain the already accepted
HGL surface and are not renamed as part of this work.
