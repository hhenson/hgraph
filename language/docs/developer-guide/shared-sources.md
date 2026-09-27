# Shared sources

[Specification and examples](https://github.com/hhenson/hgraph_spec),
[validation code and results](https://github.com/hhenson/hgraph_spec_audit), and
[portable HGL standard library](https://github.com/hhenson/hgraph_std) have
separate owners. Compiler/runtime internals and target providers stay here.

Initialize the pinned packages after cloning or changing dependency pins:

```sh
python3 tools/shared_artifacts.py
python3 tools/shared_artifacts.py --check
```

Builds and tests read the submodules in `external/` directly. No compatibility
copies are created. Setup removes unchanged copies left by earlier versions;
it refuses to remove edited copies. Move those edits into their owning package
before retrying. Use `--offline` when dependencies are already initialized.
Commit updated submodule pins with the corresponding implementation change.

The standard library separates operator declarations and properties from
`impl/` bodies and `tests/` parts. Production builds assemble declarations with
implementations; validation additionally supplies the test parts.

From a clean commit, export the checkout and its pinned dependencies:

```sh
python3 tools/source_archive.py
```

The fixed output `dist/hgraph-source.tar.gz` builds without Git or submodule
initialization. GitHub-generated tag archives omit dependencies. Core runtime
builds without language tools or conformance tests do not need shared sources.

## Declaration formatter

`hgl fmt file.hgl` previews declaration layout on stdout. `--write` replaces
that file; `--check` writes nothing and returns 1 when formatting is needed.
The modes are exclusive. Syntax errors return 1 without rewriting; usage or
file errors return 2. Formatting needs no import resolution or native provider.

The source-range formatter preserves comments, literals and body formatting.
It separates definitions and indents attached `requires`/`properties` clauses.
The rules belong to [the shared specification](https://github.com/hhenson/hgraph_spec/blob/main/language/docs/design/formatting.md).
Expression spacing and line wrapping are outside this first slice. File writes
replace a completed sibling temporary file and preserve permission bits;
symbolic-link writes are rejected.
