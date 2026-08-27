# Changelog

## Unreleased

- Retain the private request, response, WebSocket, delivery, and event
  transport envelopes as immutable `Shared<T>` allocations across the
  push-source and graph-buffering path. Public web service schemas remain
  unchanged and receive one concrete projection at publication.
- Keep server timer handles immutable during shutdown so graph teardown cannot
  race an Asio timer callback copying the same handle.
- Initial extension skeleton (RFC 0024).
- Service tier: one standard bounded burst push source per independently
  ordered logical channel, avoiding cross-channel head-of-line blocking while
  distributing distinct TSD keys together and preserving same-key/scalar FIFO;
  graph-side stateless grouping followed by standard `collect`/mapped `emit`,
  standard scalar `emit`, and command sinks; domain byte/reservation watermarks
  released at graph handoff without a second value queue; the
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
  Compatibility requests are keyed before route streams merge, preserving
  concurrent requests matched by distinct routes in one burst cycle.
- Live transports: the Asio/Beast server (h1.1 + WebSocket, TLS with the
  HTTP/2 ALPN seam, route trie, port-sharing listener registry, request
  timeouts, watermark read-pausing, strict stop ordering) and the libcurl
  client (h1 + h2, WebSocket over CONNECT_ONLY, response reservations,
  pre-warmed value bindings), `register_server`/`register_client`, and
  the loopback test tiers — an independent Beast oracle against the
  server, and the extension's own client against its own server
  (RFC 0024, implementation plan steps 3-4).
