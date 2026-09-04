# hhenson/homebrew-hgraph

Homebrew tap for [hgraph](https://github.com/hhenson/hgraph): the native
runtime SDK and the `hgl` language toolchain.

```sh
brew install hhenson/hgraph/hgraph   # or: brew install hhenson/hgraph/hgl
hgl --version
```

The formula installs `bin/hgl`, the SDK headers and shared libraries, and
the CMake packages (`lib/cmake/hgraph`, `lib/cmake/hgl`) that
`find_package(hgraph)` and `hgl_add_module()` consume. Arrow, fmt, spdlog,
simdjson and date come from homebrew-core; runtime (`state`/`when`)
functions compile at run time with the Xcode command-line tools on macOS
and the `gcc` formula on Linux.

This repository mirrors `packaging/homebrew/` in the hgraph repository,
where the formula is maintained (RFC 0032); pull requests here should be
limited to bottle blocks and version bumps that `brew bump-formula-pr`
produces. `brew test-bot` builds bottles on pull requests and `brew
pr-pull` publishes them when a maintainer applies the `pr-pull` label.
