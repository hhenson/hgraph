# hgraph language

This directory hosts the experimental hgraph language toolchain. It is a
parallel project which consumes the public hgraph C++ SDK; hgraph core does not
depend on it. The language is intended for user-authored typed functions over
hgraph, not for implementing transports, threads, callbacks, or arbitrary
native extensions.

Two backends share one frontend: the direct-wiring backend wires composition
programs onto the hgraph runtime in process (`hgl test`, `hgl run`, `hgl repl`),
and the C++ backend (`hgl emit-cpp`) writes the same programs as public hgraph
C++ authoring code that a package builds with the `hgl_add_module()` CMake
function. Both build the same graph; the parity tests hold them to it.

The project is an intentionally changeable prototype in its first executable
slice. `hgl check` lexes, parses, and resolves a module and reports diagnostics
(`--dump-tokens` and `--dump-ast` show the frontend's view). That frontend now
models nominal and generic structs, abstract-only inheritance, defaults and
optional fields, `requires` constraints, and sparse `delta<S>` construction.
`hgl test`, `hgl run`, and `hgl repl` wire composition-only programs straight
onto the hgraph runtime through the direct-wiring backend, including scalar
struct construction, type-generic Bundle specializations, `atomic<S>` values,
and field-wise temporal struct composition. On a terminal the REPL has line
editing, history (`~/.hgl_history`) and tab completion. `hgl emit-cpp` writes a
composition-only module as `<name>.h` / `<name>.cpp` in the module's namespace,
registering its exported functions as hgraph operators, and `hgl_add_module()`
builds such modules — together with hand-written C++ — into a library and,
optionally, a Python extension module with generated wrappers. Every file under
`examples/` is a CTest check case, `midpoint.hgl` runs its test, and the
`tests/codegen/parity.hgl` module is built through both backends.

Constructor inference, typed `const` arguments in native generic Bundle
identity, multiple-parent field order, explicit optional-field clearing,
runtime-function lowering (in both backends), structs and generics in generated
C++, timed harness sequences, and TOML run configuration remain staged work
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
lines. CI builds the toolchain on Linux and macOS
(`.github/workflows/language.yml`).

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
