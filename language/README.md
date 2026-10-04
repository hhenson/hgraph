# hgraph language

The shared [runtime behaviour specification](https://github.com/hhenson/hgraph_spec/blob/main/runtime/overview.md) records concepts,
numbered rules, conformance cases and implementation evidence. HGL source
syntax remains specified in this language documentation.

HGL is a temporal programming language for expressing computations over values
that evolve through time. This directory hosts its experimental toolchain, a
parallel project which consumes the public hgraph C++ SDK; hgraph core does not
depend on it. The language combines temporal computations with value-level
work and explicit state; transports, threads, callbacks, and arbitrary native
extensions remain native responsibilities.

Local `const fn` functions include generic ordinary struct/list helpers and
checked publication-delta retention. Default temporal lifting and scalar
reconstructible caches are implemented. Value-function packs, broader cache
contracts, and portable native target mappings remain separate work. See the
[status matrix](docs/design/roadmap.md#feature-status-matrix-2026-09-07) and
[ADR 0008](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/design/decisions/0008-temporal-contracts-and-target-mappings.md).

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
extension module. A module may be split into explicit `part` files while
retaining one scope, descriptor, and provider identity; pass them with
repeatable `--part` options or CMake `PARTS`. Every file under `examples/` is a CTest check case and the
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

- [User Guide](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/user-guide/README.md)
- [Developer Guide](docs/developer-guide/README.md)

The guides develop the first syntax and examples from both sides of the
contract: what an author writes and observes, and how the compiler classifies
and preserves those semantics through hgraph's public C++ APIs.

### Design records

- [Architecture](docs/design/architecture.md)
- [Language model](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/design/language-model.md)
- [Temporal contracts and target mappings](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/design/decisions/0008-temporal-contracts-and-target-mappings.md)
- [Modules and native extensions](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/design/modules.md)
- [Roadmap](docs/design/roadmap.md)
- [Distribution and deployment](docs/design/distribution.md)

Syntax remains provisional while the prototype evolves; compatibility is not
yet a release constraint. Implemented forms are kept under grammar, semantic,
and direct-wiring tests.

The first compiled library modules live under
[`stdlib/hgl/hgraph`](stdlib/hgl/hgraph). `native.hgl` is the deliberately thin
C++ value/view substrate published as `hgl::core_native`; `standard.hgl` is the
first HGL-authored operator module, published as `hgl::standard_library`.
Its `len_` and `is_empty` families are real generated and runtime-tested code,
with the remaining production-identity and first-tick parity gaps recorded next
to the source.

The shared HGL library in `external/hgraph_std` supplies `replay`, `record`,
and generic `pass_through`. `eval` composes those operators internally and
returns independently retained `delta<T>` values. The same HGL tests exercise
the supported scalar and structural publication types in both compilers.

## Shared compiler conformance gate

`tools/hgl_compiler_parity.py` discovers every shared library module, source
part and named HGL test under `external/hgraph_std/hgl/hgraph`. Each compiler
must execute the shared assertions and report every expected test. C++ reports
once per test; Rust reports once per assertion or outputless eval, whose counts
are inventoried from those same source files. Matching failures, empty output and silently omitted test parts fail the
gate. No compiler's observed behavior supplies another compiler's expectations.

After rebuilding `hgl_stdlib_test_driver`, run:

```sh
python tools/hgl_compiler_parity.py --backend cpp --build build-language \
  --output build-language/compiler-conformance
python tools/hgl_compiler_parity.py --backend both --build build-language \
  --rust-root <rust-checkout> --output build-language/compiler-parity
```

Use a new output directory per run. `--list` prints the discovered test inventory.
The Rust adapter uses that checkout's existing `tools/test_hgl.py` with the
same explicit shared sources as C++, rather than the Rust checkout's own pins.
The public language workflow requires the C++ gate on every change; local
`--backend both` runs independently check Rust when its checkout is available.
It never downloads or discloses that checkout. The gate does not rebuild either
implementation; rebuild first and keep sources unchanged during the run.

The JSON report records compiler/provider inputs, Git revisions and hashes of
actual working files, including staged specification changes. It distinguishes
source revision from binary identity; an already-built C++ binary is not proof
that every local edit was compiled. Per-module logs remain local beside the
report. Source changes during measurement invalidate the result. The existing
shared audit tools in `external/hgraph_spec_audit/compiler/stdlib_eval` retain
their separate clean-build evidence workflow and archived measurement scope;
this gate neither rewrites that evidence nor claims runtime/Python parity.
