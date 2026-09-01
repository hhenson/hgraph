# hgraph-web

C++-first HTTP/S and WebSocket transports for hgraph, implementing the contract
in hgraph RFC 0024. The extension covers all four transports — HTTP(S) server,
HTTP(S) client, WebSocket server, and WebSocket client — and exposes the same
service model to native C++ and Python graphs.

One path-bound, multi-interface `service_impl` owns the sockets, TLS contexts,
and worker threads for a configuration. Serving a route and connecting a
WebSocket are subscription services keyed by the immutable route or connection
key; responding, sending frames, and calling a remote endpoint are
request/reply services; events and statistics are reference services. Graph
output reaches the transport through the implementation's sink inputs, and
requests, frames, delivery reports, events, and stats re-enter the root graph
through bounded push sources. Listeners and clients are created on graph start
and stopped with the graph; there is no user-held server or client object.

Routing lives in the transport rather than the graph. The route key set arrives
as `TSS` deltas, is compiled into a native route table, and requests are
delivered already keyed by route. Duplicate and ordered headers, multi-value
query parameters, and binary bodies are preserved end to end.

The server speaks HTTP/1.1 and WebSocket over Asio/Beast; the client speaks
HTTP/1.1, HTTP/2, and WebSocket over libcurl. The server ships the complete
HTTP/2 seam — the ALPN callback, protocol dispatch, and configuration
placeholders — and rejects `h2` in server ALPN until the nghttp2 session lands
in a v1.x release. TLS termination, mTLS, SNI, and ALPN use OpenSSL on every
platform.

The core `hgraph` wheel owns a guarded compatibility shim at
`hgraph.adaptors.web` which delegates to this extension when it is installed.
The released `hgraph.adaptors.tornado` package is unchanged; whether it is
later re-pointed or deprecated is a separate migration decision. The extension
wheel installs only `hgraph_web`; it never contributes files to the core
`hgraph` package.

## Status

The HTTP/1.1, HTTP/2 client, and WebSocket service implementations, schemas,
service interfaces, endpoint decorators, and codec helpers are available.
Runnable HTTP server/client and WebSocket loopback examples are in
[`python/examples`](python/examples/README.md).

## Build and test

This is a first-party extension in the hgraph monorepo. It remains a separate
CMake package and Python distribution: the top-level core package does not link
curl, Boost, nghttp2, or OpenSSL, and does not install these modules.

For an in-tree native development build from the repository root:

```sh
cmake -S . -B build-web \
  -DHGRAPH_BUILD_WEB_EXTENSION=ON \
  -DBUILD_TESTING=ON
cmake --build build-web --parallel
ctest --test-dir build-web --output-on-failure
```

The extension can still be configured independently against an installed
hgraph SDK:

```sh
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/hgraph/install
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Each third-party dependency arrives with the translation unit that links it,
resolved with `find_package(... CONFIG QUIET)` first and falling back to a
pinned static `FetchContent` build. The skeleton links only `hgraph::core`.

Build its separately deployable ABI3 wheel from the repository root after
making the matching hgraph SDK discoverable through `CMAKE_PREFIX_PATH`:

```sh
CMAKE_PREFIX_PATH=/path/to/hgraph/sdk \
  uv build --wheel --package hgraph-web --python 3.12
```

The deterministic suite runs against the extension's fake transport, which
exercises routing, pacing, reservation, and lifecycle without sockets. A live
loopback subset acts as the transport oracle.

Wheel builds require the SDK installed by a stable-ABI hgraph wheel. The
extension rejects an SDK that links `Python::Python`, because that would pin
the nominal ABI3 module to the build interpreter.
