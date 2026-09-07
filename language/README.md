# hgraph language

This directory hosts the experimental hgraph language toolchain. It is a
parallel project which consumes the public hgraph C++ SDK; hgraph core does not
depend on it. The language is intended for user-authored typed functions over
hgraph, not for implementing transports, threads, callbacks, or arbitrary
native extensions.

Two backends share one frontend: the direct-wiring backend wires composition
programs onto the hgraph runtime in process, and the C++ backend writes the
documented composition and runtime forms as public, formatted hgraph C++.
Small evaluation-time helpers may be declared as a top-level `native fn` with a
real C++ parameter projection and body; they emit as plain formatted C++
functions and remain outside graph/node bodies.
`hgl test` and `hgl run` compile and load that generated C++ when a file
contains runtime functions or source-defined implementations; `hgl emit-cpp`
and `hgl_add_module()` expose the same route to package builds. The shared
subset builds the same graph; the parity tests hold the two paths to it.

The project is an intentionally changeable prototype in its first executable
slice. `hgl check` lexes, parses, resolves, and type-checks an HGL module
(`--dump-tokens`, `--dump-ast`, `--dump-hir`, and `--dump-hgraph-ir` show its
successive views) and validates one generated `.hgl-module.json` descriptor
without loading native code. `hgl test`, `hgl run`, and `hgl repl` wire
composition programs directly onto the runtime and, on Unix, compile and load
supported runtime functions and `impl fn` candidates through a
content-addressed native image; `hgl emit-cpp` writes a module as
`<name>.h` / `<name>.cpp` in the module's namespace and `hgl_add_module()`
builds it, with hand-written C++, into a library and optionally a Python
extension module. Every file under `examples/` is a CTest check case and the
codegen fixtures compile and execute generated graph and runtime-node
modules. What each language surface supports today, its fail-closed
boundary, and its named blockers are recorded once, in the
[roadmap status matrix](docs/design/roadmap.md#feature-status-matrix-2026-09-07).

## Build

For an opt-in repository build:

```sh
cmake --preset cpp --fresh -DHGRAPH_BUILD_LANGUAGE=ON
cmake --build --preset cpp --target hgl
ctest --preset cpp -R hgraph_language
```

`HGL_ENABLE_LINE_EDITING=OFF` drops the REPL's line editor (isocline, MIT)
and its fetch; the REPL then reads plain lines. The declarative parser uses
lexy in every language build. `clang-format` is required because formatted C++
is part of every `emit-cpp`, scripted, and AOT generation path. Set
`HGL_CLANG_FORMAT_EXECUTABLE` while configuring to select it and
`HGL_CLANG_FORMAT` while running `hgl` to override it. Generated code uses the
repository's `.clang-format` policy, embedded in `hgl` so output does not vary
with the caller's working directory. A build with no network hands CMake
unpacked lexy v2025.05.0 and isocline v1.1.0 source trees:
`-DFETCHCONTENT_SOURCE_DIR_LEXY=/path/to/lexy
-DFETCHCONTENT_SOURCE_DIR_ISOCLINE=/path/to/isocline
-DFETCHCONTENT_FULLY_DISCONNECTED=ON` (this is what the Homebrew formula in
`packaging/homebrew/` and the language-enabled Conan recipe do). CI builds the
toolchain on Linux and macOS
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
