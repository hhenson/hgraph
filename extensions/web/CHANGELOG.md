# Changelog

## Unreleased

- Initial extension skeleton (RFC 0024).
- Service tier: one standard bounded burst push source per independently
  ordered logical channel, avoiding cross-channel head-of-line blocking while
  distributing distinct TSD keys together and preserving same-key/scalar FIFO;
  graph-side projections and command
  sinks; domain byte/reservation watermarks without a second value queue; the
  eleven service interfaces wired through
  `impl_input`/`impl_output`, wiring sugar, and the socketless
  `FakeWebServer`/`FakeWebClient` transports. Graph-owned subscription
  generations prevent queued HTTP or WebSocket ingress from crossing route/key
  removal and re-addition boundaries (RFC 0024/0027).
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
