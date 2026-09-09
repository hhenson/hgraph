# Core HGL modules

Status: compiled native substrate; compiled HGL operator integration prototype

This folder contains two different layers. [`native.hgl`](native.hgl) is a thin
C++ value/view substrate. [`standard.hgl`](standard.hgl) is ordinary HGL that
defines, materializes, and registers the first higher-level operator families.

## Native substrate

[`native.hgl`](native.hgl) defines the `hgraph.native` module. It is built as
the `hgl::core_native` CMake target and installs its generated C++ library,
header, module descriptor, and HGL source with the opt-in language SDK.

This is a deliberately thin substrate for HGL library authors. It exposes
current-value or live-view calculations; it does not own graph scheduling,
state, lifecycle, or output mutation. Those remain in ordinary HGL functions.
The module names each owning hgraph view header explicitly with `cpp include`,
so its source-native signatures do not depend on incidental umbrella includes.

## Implemented surface

The first slice provides `len(value)` and `is_empty(value)` for:

- `str`, passed as its current `hgraph::Str` value;
- fixed `list<T, size>` and dynamic `list<T, unbounded>`, passed as a
  `hgraph::TSLInputView`;
- `set<T>`, passed as a `hgraph::TSSInputView`;
- `map<K, V>`, passed as a `hgraph::TSDInputView`;
- tick-count `rolling<T, max_size, min_size>`, passed as a
  `hgraph::TSWInputView`.

All casts and calls in the source are real C++ and are compiled with the same
warnings as the rest of the language build. The runtime tests exercise every
listed hgraph view, including list growth/truncation and window growth.

An HGL module imports the descriptor by linking its generated target to
`hgl::core_native`:

```cmake
hgl_add_module(my_hgl_library STATIC
    HGL my_library.hgl
    LINK_LIBRARIES hgl::core_native)
```

```hgl
use hgraph.native as native

fn list_size<T, const size: i64>(value: list<T, size>) -> i64 {
    when {
        return native::len(value)
    }
}
```

See the compiled consumer
[`core-native-library.hgl`](../examples/core-native-library.hgl).

## First HGL-authored operators

[`standard.hgl`](standard.hgl) defines `hgraph.std.len_` and
`hgraph.std.is_empty`. It is compiled with `hgl_add_module()` as
`hgl::standard_library`, installed with its generated header and descriptor,
and runtime-tested through the public operator registry. The implementation is
HGL; its only native calls are the current-value/live-view projections from
`hgraph.native`.

The source is deliberately compact:

```hgl
impl fn len_<T, const size: i64>(value: list<T, size>) -> i64 {
    inject out

    when {
        let current = native::len(value)
        if valid(out) {
            if out != current {
                out = current
            }
        } else {
            out = current
        }
    }
}

instantiate len_<_, _>, len_<_>
```

`when {}` means any input modification activates the handler and all inputs must
be valid. `inject out` lets the implementation compare with the previous result
and avoid an unchanged tick. One partially materialized list implementation
matches fixed and unbounded extents; the retained `size` selects the concrete
hgraph schema without being read by the body. Separate retained materializations
cover the set and map candidates.

This is the first compiler/standard-library integration slice, not yet a
replacement for the production C++ operators:

- generated contracts currently have the module-qualified identities
  `hgraph.std.len_` and `hgraph.std.is_empty`; emitting an implementation of the
  existing imported public contracts is still blocked;
- HGL has no contract for `schedule_on_start` or observing a bound collection
  before it first becomes valid, so the production first-tick behavior of TSL,
  TSS, and TSD is not yet expressible;
- a retained rolling extent lowers to an any-window pattern and cannot
  materialize the concrete node input schema, so rolling is intentionally
  absent from this first module;
- TSB length/emptiness is schema metadata and needs a graph-level metadata
  operation rather than the live collection-view primitive used here.

These are tracked as `HGL-LIB-001` through `HGL-LIB-004` beside the HGL source.
The existing C++ registrations remain authoritative until identity and behavior
parity are complete.

## Honest boundaries

Descriptor ABI v1 currently accepts native collection extents only through
`const ...: i64` generics. Duration-based rolling windows therefore cannot yet
join this generic overload family. Nominal bundle/struct views and `ref` views
are also outside the current native input-view envelope. They are omitted from
the module rather than represented by declarations that cannot be imported or
run.
