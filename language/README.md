# hgraph language

This directory hosts the experimental hgraph language toolchain. It is a
parallel project which consumes the public hgraph C++ SDK; hgraph core does not
depend on it. The language is intended for user-authored typed functions over
hgraph, not for implementing transports, threads, callbacks, or arbitrary
native extensions.

The first backend will transpile typed language programs to public hgraph C++
authoring APIs. Scripted execution, the REPL, and production builds will share
that frontend and backend so they cannot acquire separate runtime semantics.

The project is currently a design and build scaffold. The `hgl` executable
only reports help and version information; it does not parse language source
yet.

## Build

For an opt-in repository build:

```sh
cmake --preset cpp --fresh -DHGRAPH_BUILD_LANGUAGE=ON
cmake --build --preset cpp --target hgl
ctest --preset cpp -R hgraph_language
```

For an independent build against an installed hgraph SDK:

```sh
cmake -S language -B build-language \
  -DCMAKE_PREFIX_PATH=/path/to/hgraph-sdk
cmake --build build-language
ctest --test-dir build-language
```

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

Syntax shown in the design records and examples is provisional until the
grammar and semantic tests land.
