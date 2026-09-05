# hgraph language

This directory hosts the experimental hgraph language toolchain. It is a
parallel project which consumes the public hgraph C++ SDK; hgraph core does not
depend on it. The language is intended for user-authored typed functions over
hgraph, not for implementing transports, threads, callbacks, or arbitrary
native extensions.

Two backends share one frontend: the direct-wiring backend wires composition
programs onto the hgraph runtime in process, and the C++ backend writes the
documented composition and runtime forms as public, formatted hgraph C++.
`hgl test` and `hgl run` compile and load that generated C++ when a file
contains runtime functions or source-defined implementations; `hgl emit-cpp`
and `hgl_add_module()` expose the same route to package builds. The shared
subset builds the same graph; the parity tests hold the two paths to it.

The project is an intentionally changeable prototype in its first executable
slice. `hgl check` lexes, parses, and resolves a module and reports diagnostics
(`--dump-tokens`, `--dump-ast`, `--dump-hir`, and `--dump-hgraph-ir` show its
successive views). The hgraph-IR dump now owns callable and test bodies as well
as their interfaces; backends still use their temporary resolved-AST adapters.
That frontend now
models nominal and generic structs, abstract-only inheritance, defaults and
optional fields, `requires` constraints, and sparse `delta<S>` construction.
`hgl test`, `hgl run`, and `hgl repl` additionally execute supported generated
runtime nodes through a content-addressed native image on Unix; the REPL
transactionally replaces that image as declarations join the session.
Composition-only sessions retain the direct-wiring path. Both paths include scalar
struct construction, type-generic Bundle specializations, `atomic<S>` values,
and field-wise temporal struct composition. On a terminal the REPL has line
editing, history (`~/.hgl_history`) and tab completion. `hgl emit-cpp` writes a
module as `<name>.h` / `<name>.cpp` in the module's namespace, registering
composition functions as graph overloads and runtime functions as node
overloads. Every checked-in example now reaches generated C++: nominal and
generic structs, sparse deltas, generic operators, fixed and duration windows,
concise `map` functions, collection traversal and predicates, logger injection,
scalar recordable state, prior and keyed output access, and lifecycle blocks.
Source operators become transparent aliases of `hgraph::Operator` contracts,
not generated subclasses. `hgl_add_module()`
builds such modules — together with hand-written C++ — into a library and,
optionally, a Python extension module with generated wrappers. Every file under
`examples/` is a CTest check case, `midpoint.hgl` runs its test, and the codegen
fixtures compile and execute generated graph and runtime-node modules.

Portable runtime-module loading, multi-registry module transactions,
typed `const` arguments in native generic Bundle identity, multiple-parent
field order, explicit optional-field clearing,
general runtime calls and non-scalar state, timed harness sequences, and TOML
run configuration remain staged work
([roadmap](docs/design/roadmap.md)).

## Build

For an opt-in repository build:

```sh
cmake --preset cpp --fresh -DHGRAPH_BUILD_LANGUAGE=ON
cmake --build --preset cpp --target hgl
ctest --preset cpp -R hgraph_language
```

`HGL_ENABLE_LINE_EDITING=OFF` drops the REPL's line editor (isocline, MIT,
fetched at configure time) and its network fetch; the REPL then reads plain
lines. `clang-format` is required because formatted C++ is part of every
`emit-cpp`, scripted, and AOT generation path. Set
`HGL_CLANG_FORMAT_EXECUTABLE` while configuring to select it and
`HGL_CLANG_FORMAT` while running `hgl` to override it. Generated code uses the
repository's `.clang-format` policy, embedded in `hgl` so output does not vary
with the caller's working directory. A build with no network keeps the editor
by handing CMake an
unpacked isocline v1.1.0 source tree:
`-DFETCHCONTENT_SOURCE_DIR_ISOCLINE=/path/to/isocline
-DFETCHCONTENT_FULLY_DISCONNECTED=ON` (this is what the Homebrew formula in
`packaging/homebrew/` does). CI builds the toolchain on Linux and macOS
(`.github/workflows/language.yml`); `.github/workflows/packaging.yml`
builds it the way the package channels do.

`hgl --version` prints `hgl <release> (hgraph api <api>)`: the release
version is hgraph's (`HGRAPH_RELEASE_VERSION`, derived from git tags when
unset; see `build_system.rst`), and the tool has no version of its own.

For an independent build against an installed hgraph SDK:

```sh
cmake -S language -B build-language \
  -DCMAKE_PREFIX_PATH=/path/to/hgraph-sdk
cmake --build build-language
ctest --test-dir build-language
```

When the SDK is the installed `hgraph` wheel, pass its `site-packages`
directory as the prefix and `-DPython_EXECUTABLE=<that interpreter>`;
`hgraphConfig.cmake` needs the interpreter to locate nanobind. Catch2 is
found or fetched for the frontend tests unless the language is built as
part of the repository, where it reuses the repository's copy.

## Documentation

- [User Guide](docs/user-guide/README.md)
- [Developer Guide](docs/developer-guide/README.md)

The guides develop the first syntax and examples from both sides of the
contract: what an author writes and observes, and how the compiler classifies
and preserves those semantics through hgraph's public C++ APIs.

### Design records

- [Architecture](docs/design/architecture.md)
- [Language model](docs/design/language-model.md)
- [Modules and native extensions](docs/design/modules.md)
- [Roadmap](docs/design/roadmap.md)
- [Distribution and deployment](docs/design/distribution.md)

Syntax remains provisional while the prototype evolves; compatibility is not
yet a release constraint. Implemented forms are kept under grammar, semantic,
and direct-wiring tests.
