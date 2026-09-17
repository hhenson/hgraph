# Core HGL modules

Status: compiled native substrate and parallel HGL implementations

This folder contains two different layers. [`native.hgl`](native.hgl) anchors a thin
C++ value/view substrate. [`standard.hgl`](standard.hgl) is ordinary HGL that
defines, materializes, and registers the first higher-level operator families.

Repository acceptance is visible in the filename. A standard-library design
remains `.hgl.proposed` and is never passed to the compiler. Once its syntax,
semantics, and behavior are accepted, it becomes `.hgl`, is added to the
compiled module list, and receives a generated-C++ snapshot under
[`language/generated/cpp/hgraph`](../../../generated/cpp/hgraph). The snapshot
is validation evidence for accepted source; it is not an alternative source of
truth.

## Native substrate

[`native.hgl`](native.hgl) defines the `hgraph.native` module. It is built as
the `hgl::core_native` CMake target and installs its generated C++ library,
header, module descriptor, and all HGL source parts with the opt-in language SDK.

The parts are one module, not independently importable submodules:

| Source | Part | Responsibility |
| --- | --- | --- |
| [`native.hgl`](native.hgl) | `endpoint` | Common payload-erased input queries |
| [`native/sequences.hgl`](native/sequences.hgl) | `sequences` | Fixed and unbounded TSL |
| [`native/sets_maps.hgl`](native/sets_maps.hgl) | `sets_maps` | TSS and TSD |
| [`native/windows.hgl`](native/windows.hgl) | `windows` | Tick-window queries |
| [`native/scalar_values.hgl`](native/scalar_values.hgl) | `scalar_values` | String queries, numeric/Boolean projections and conversions |
| [`native/temporal_values.hgl`](native/temporal_values.hgl) | `temporal_values` | Calendar, clock, duration and epoch value projections |

CMake explicitly passes the complete list through `PARTS`; compiling just the
anchor does not discover its siblings. All declarations remain accessible via
`use hgraph.native as native`, with one descriptor, library, and generated
header/source pair. This is a source-organisation split, not C++ translation-unit
sharding. Parts for bundles/references and compound atomic values will be added
when their native signature contracts exist; there are no empty placeholder
modules. The installed-SDK test recompiles these installed source parts as well
as linking the prebuilt library.

This is a deliberately thin substrate for HGL library authors. It exposes
current-value or live-view calculations; it does not own graph scheduling,
state, lifecycle, or output mutation. Those remain in ordinary HGL functions.
The module names each owning hgraph view header explicitly with `cpp include`,
so its source-native signatures do not depend on incidental umbrella includes.

## Implemented surface

The first typed-view slice provides `len(value)` and `is_empty(value)` for:

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

The erased-view slice provides `valid`, `all_valid`, `modified`, `last_modified`,
`bound`, and `active` once each with a `signal` parameter. The compiler passes the
common `TSInputView`, so those declarations cover atomic values, nominal
bundles, fixed and unbounded lists, sets, maps, tick and duration windows,
references, and signals without a type-kind switch. Runtime tests bind the
generated node to every listed standard time-series shape; the public
native-package authoring API and descriptor reader also validate this
input-view pattern. Value operations keep ordinary HGL spelling, independent
of their C++ representation; there is no separate erased-value API.

The additional query bindings are:

| Function | Input | Meaning |
| --- | --- | --- |
| `bound(value)` | `signal` | Whether the input is bound; it need not hold a valid value yet |
| `active(value)` | `signal` | Current input subscription status; does not activate it |
| `capacity(value)` | Tick window | Retained-sample capacity |
| `period(value)` | Tick window | Configured maximum sample count |
| `min_period(value)` | Tick window | Configured minimum sample count for `all_valid` readiness |
| `is_full(value)` | Tick window | The retained window is full |
| `has_removed_value(value)` | Tick window | A value was evicted in this evaluation, not merely at some earlier tick |
| `first_modified(value)` | Tick window | Timestamp of the oldest retained sample, not the first-ever modification |
| `contains(value, needle)` | `str`, `str` | Substring membership |
| `starts_with(value, prefix)` | `str`, `str` | Prefix match |
| `ends_with(value, suffix)` | `str`, `str` | Suffix match |

These are thin C++ view/value projections, not new runtime semantics. Query
names follow the existing view methods except `is_full` follows `is_empty`,
and `first_modified` follows `last_modified`. Tick-window queries are constant
time and require a bound window. `first_modified` forwards `MIN_DT` if the bound
window is empty; normal `when {}` guards wait for validity. String queries use
`hgraph::Str` byte semantics, including embedded NUL; `len` counts bytes, not
Unicode code points. An empty needle/prefix/suffix matches every string.
String searches may scan the input but neither allocate nor retain arguments.
For tick windows the current C++ `valid` becomes true on the first sample;
`all_valid` becomes true at `min_period`. These bindings preserve that distinction.

The compiled [consumer examples](../examples/core-native-library.hgl) include
clock-driven sampling before validity, passive input inspection, window
growth/eviction, and string queries. No Python runtime wrapper
or duplicate system node is introduced by this module.

The compiler additionally implements runtime `key_set`, membership-only
`modified(key_set(value))`, `contains` for sets/maps/key sets, strict `at` for
maps/lists/tick windows, `front`/`back` for lists/tick windows, and window
`time_at`/`removed_value`. These are bare HGL intrinsics, not extra native-module
declarations. Their generated consumers and runtime tests cover key additions,
child-only updates, removals, strict bounds and window wraparound. See the
[accepted surface and remaining work](../../../docs/design/native-surface-proposal.md),
including the accepted but still unimplemented nullable `get` contract.

## Tests beside each native part

Every native source part ends with an unnamed HGL `test { ... }` context
containing named test cases and private helpers. Helpers are shared across
parts of the same module, but are unavailable to importing modules and to
production code. These are the readable library-level behavior checks; C++ runtime
tests remain additional compiler/ABI and edge-case coverage, not a replacement.
CTest assembles the same explicit part inventory as the production build:

```sh
ctest --preset cpp -R '^hgraph_language_test_core_native_parts$' --output-on-failure
```

The assembled module runs every native part’s named tests. They cover string predicates,
endpoint metadata, dynamic-list growth/truncation, set/map insertion and
removal, and window configuration, population, eviction and timestamps.
Structural inputs are constructed by ordinary HGL functions from scalar
sequences; window tests call the production `to_window` operator.

Remaining harness gaps are explicit:

- Fixed TSL replay is not supported by HGL `eval`; temporal list literals and
  homogeneous-pack-to-native-view calls cannot yet provide an alternative.
  Fixed-list `len`/`is_empty` remain covered by C++ generated-runtime tests.
- Direct structural replay and expected exception assertions still need
  harness support. Existing C++ tests retain pre-validity/passive inspection,
  invalid-child, reference-rebind and strict lookup-failure coverage.

Production compilation excludes test assertions, context helpers, helper
registrations, and native dependencies used only by those helpers. A native
dependency shared with production remains present. `hgl test` includes the
test context; its helpers are still absent from the public HGL descriptor.
See [test contexts](../../../docs/user-guide/testing-and-running.md#test-only-helpers-and-module-parts).

Local scalar `const fn` helpers can be tested using automatic lifting;
`eval(const(helper), ...)` explicitly selects the value version when a temporal
function has the same name. Existing `native fn` bindings are unchanged and
are not automatically migrated to value functions. See
[value functions and lifting](../../../docs/user-guide/value-functions.md).

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

## Accepted HGL operator surface

[`standard.hgl`](standard.hgl) defines `hgraph.std.len_` and
`hgraph.std.is_empty`. It is compiled with `hgl_add_module()` as
`hgl::standard_library`, installed with its generated header and descriptor,
and runtime-tested through the public operator registry. The implementation is
HGL; its only native calls are the current-value/live-view projections from
`hgraph.native`.

[`control.hgl`](control.hgl) adds the accepted homogeneous variadic contracts
for `merge`, `race`, `all_`, and `any_`. These declarations now compile to real
`VarIn` operator contracts and descriptors. Their bodies remain pending because startup results and complete reference/
reselection semantics are still missing.

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

- generated contracts use module-qualified parallel identities; production
  publication/cutover is deferred;
- HGL has no contract for `schedule_on_start`. Native `bound` queries can now
  observe a connected collection before its first value, but startup output
  policies still require an admitted lifecycle contract;
- a retained rolling extent lowers to an any-window pattern and cannot
  materialize the concrete node input schema, so rolling is intentionally
  absent from this first module;
- TSB length/emptiness is schema metadata and needs a graph-level metadata
  operation rather than the live collection-view primitive used here.

These are tracked as `HGL-LIB-001` through `HGL-LIB-004` beside the HGL source.
The existing C++ registrations remain authoritative until identity and behavior
parity are complete.

## Honest boundaries

Descriptor ABI v2 currently accepts typed native collection extents only
through `const ...: i64` generics. Duration-based rolling windows therefore
cannot yet join the typed `len` / `is_empty` overload families. Nominal
bundle/struct and `ref` patterns likewise have no typed input-view declaration.
All of these can use the common `signal` input-view operations because those
operations neither expose nor specialize on the payload schema.

The complete gap table for current/delta values, references, hashing,
ordering, formatting, metadata, output mutation, and iterators is maintained in
the [native-interface design](../../../docs/design/native-interface.md#exact-native-value-and-view-functions).

The [native surface completion record](../../../docs/design/native-surface-proposal.md)
separates implemented operations, accepted compiler/ABI work, and behavior
that still needs agreement.

## Migration catalogue and implementation parts

The [catalogue](../../catalogue/README.md) records completed domains, native
source signatures, test evidence and outstanding capabilities. Native value
bindings count; delegation to an existing temporal operator stays pending.

- `operators.hgl`: HGL arithmetic/comparison/Boolean bodies and exposed native
  value projections; binary extrema, mean, string membership and `substr`;
  checked `pow_`, `lshift_` and `rshift_` through `throws` native bindings
  of the reviewed lifted kernels (a raise ends the evaluation, ADR 0009).
- `standard.hgl`: collection queries, membership/index search, map accumulation, keyed
  construction/removal and named scalar
  conversions (`to_int`, `to_float`, `to_bool`, `to_date`, `to_datetime`).
- `stream.hgl`: sample/drop/filter/dedup, running sum/mean/extrema and internal tick counting;
  `take`, `freeze` and `until_true` through `passivate` (ADR 0010).
  `schedule` remains blocked on non-recordable counter storage and start validation.
- `control.hgl`: if_true, scalar null_sink and pass_through bodies; pending
  variadic control contracts remain declarations.
- `temporal.hgl`: Date/DateTime/Time/Duration fields and modification metadata.

Overload-specific aliases retain the available algorithms without requiring
production overload consolidation: `eq_epsilon`, `dedup_float`, `sum_reset`,
and the named conversion targets. `dedup_float` requires an explicit temporal
tolerance because HGL temporal defaults are not admitted; unary `dedup(f64)`
preserves the native default. The catalogue lists these aliases together with
the corresponding native identity. These modules do not replace core nodes.

Generated C++ parity tests exercise sparse/repeated ticks, reset-before-value,
float tolerance, NaNs/signed zero, negative durations, collection pre-validity
and partial lists. Native-part and imported-module scripted tests exercise HGL
source helpers. The installed-SDK consumer rebuilds native and standard parts.
