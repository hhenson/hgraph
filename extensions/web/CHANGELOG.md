# Changelog

## Unreleased

- Initial extension skeleton (RFC 0024).
- Service tier: bounded bridge with per-channel conflating wakes and
  watermarks, drain nodes, the eleven service interfaces wired through
  `impl_input`/`impl_output`, wiring sugar, and the socketless
  `FakeWebServer`/`FakeWebClient` transports (RFC 0024, implementation
  plan step 2).
