# Changelog

## Unreleased

- Initial extension skeleton (RFC 0024).
- Service tier: bounded bridge with per-channel conflating wakes and
  watermarks, drain nodes, the eleven service interfaces wired through
  `impl_input`/`impl_output`, wiring sugar, and the socketless
  `FakeWebServer`/`FakeWebClient` transports (RFC 0024, implementation
  plan step 2).
- Python surface: the `hgraph_web` schemas/services/wiring helpers, the
  `@http_endpoint`/`@ws_endpoint` decorators with JSON/text codec helpers
  and route/server authenticators, and `hgraph_web.compat` — the released
  tornado adaptor API re-implemented on the native transports with its
  legacy behaviors reproduced only there (parity notes in the module).
- Live transports: the Asio/Beast server (h1.1 + WebSocket, TLS with the
  HTTP/2 ALPN seam, route trie, port-sharing listener registry, request
  timeouts, watermark read-pausing, strict stop ordering) and the libcurl
  client (h1 + h2, WebSocket over CONNECT_ONLY, response reservations,
  pre-warmed value bindings), `register_server`/`register_client`, and
  the loopback test tiers — an independent Beast oracle against the
  server, and the extension's own client against its own server
  (RFC 0024, implementation plan steps 3-4).
