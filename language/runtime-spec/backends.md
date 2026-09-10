# Prototype backend mappings

Status: exploratory; no generator implements these mappings

The `.hgspec` model is semantic. A backend owns its representation, but must
preserve canonical identities, operation availability, ownership, lifecycle,
and conformance traces.

| Runtime-spec construct | C | C++ | Rust | Swift |
| --- | --- | --- | --- | --- |
| `record` | generated struct | aggregate descriptor | struct | struct |
| `choice` | tag plus union | descriptor tag plus payload | enum | enum |
| `interned schema` | registry index/handle | stable metadata pointer | interned `Arc`/index | registry token |
| `ops` | function-pointer table | passive ops table | trait-object vtable or generated table | protocol witness/internal table |
| `owned<T>` | explicit destroyable handle | RAII owner | owning value | owning value/box |
| `borrowed<T, 'a>` | documented scoped view | non-owning view | checked borrow | scoped non-owning façade |
| `inout<T, 'a>` | exclusive mutable scoped handle | mutable non-owning view | checked `&mut` borrow | `inout`/scoped mutable façade |
| `shared<T>` | retained immutable handle | shared arena handle | `Arc<T>`-like handle | immutable retained box |
| `fallible` | status plus out value | result/exception boundary chosen by profile | `Result<T, E>` | `throws` or result value |
| `law` / `scenario` | generated test functions | Catch2 fixtures | property/unit tests | XCTest fixtures |

## C ABI profile

C is the interoperability baseline, not necessarily the implementation model.
A C ABI profile would constrain:

- fixed-width discriminants and descriptor versions;
- opaque owner and borrowed-view handles;
- function tables with explicit contexts;
- status values rather than cross-boundary exceptions;
- provider init/deinit/query entry points;
- byte/string/span views with caller-visible lifetime rules.

The ABI profile must not leak backend container layouts or make all internal
calls pay an FFI cost. C++, Rust, and Swift backends may use native façades
inside one package and expose the C profile only at module boundaries.

## Required spike

The first generator experiment should implement `ValueKind`, `ValueShape`,
`ValueType`, and the read-only portion of `ValueOps` for C++ and Rust. It should
then run the same canonicality and nominal-identity conformance vectors. That
slice is large enough to test the model without committing to node execution or
storage mutation.
