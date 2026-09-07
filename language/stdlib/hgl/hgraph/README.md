# Core native HGL module

Status: compiled, tested, and installed C++ implementation

[`native.hgl`](native.hgl) defines the `hgraph.native` module. It is built as
the `hgl::core_native` CMake target and installs its generated C++ library,
header, module descriptor, and HGL source with the opt-in language SDK.

This is a deliberately thin substrate for HGL library authors. It exposes
current-value or live-view calculations; it does not own graph scheduling,
state, lifecycle, or output mutation. Those remain in ordinary HGL functions.

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

## Honest boundaries

Descriptor ABI v1 currently accepts native collection extents only through
`const ...: i64` generics. Duration-based rolling windows therefore cannot yet
join this generic overload family. Nominal bundle/struct views and `ref` views
are also outside the current native input-view envelope. They are omitted from
the module rather than represented by declarations that cannot be imported or
run.
