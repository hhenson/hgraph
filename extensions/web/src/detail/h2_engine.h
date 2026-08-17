#ifndef HGRAPH_WEB_DETAIL_H2_ENGINE_H
#define HGRAPH_WEB_DETAIL_H2_ENGINE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::web::detail {

// The HTTP/2 protocol engine (RFC 0024, HTTP/2 activation plan).
//
// A pure bytes-in/actions-out wrapper over an nghttp2 server session:
// the owning driver (asio_server.cpp) feeds TLS-decrypted bytes in via
// receive(), drains frames to write via next_output(), and receives
// protocol events through H2Host.  nghttp2 itself is confined to
// nghttp2_session.cpp; this header carries no third-party types, and the
// driver carries no HTTP/2 framing knowledge.
//
// Flow control IS the ingress admission contract: the engine never
// releases connection or stream window for request DATA by itself — the
// host calls consume() once reservation-accounted on the bridge, or discard()
// once the bytes are deliberately abandoned, so an unadmitted stream stalls
// at the sender exactly like an unread h1 socket (RFC 0024, flow control).

using H2Headers = std::vector<std::pair<std::string, std::string>>;

struct H2Settings {
  std::size_t max_concurrent_streams{100};
  std::size_t initial_window_bytes{1024 * 1024};
  std::size_t max_header_bytes{64 * 1024};
};

/** Stream-level error codes the driver may reset with (RFC 9113 §7);
 * mirrored here so the driver does not include nghttp2 headers. */
enum class H2StreamError : std::uint32_t {
  NoError = 0x0,
  InternalError = 0x2,
  RefusedStream = 0x7,
  Cancel = 0x8,
  EnhanceYourCalm = 0xb,
};

/** Engine-to-driver events.  All calls arrive synchronously from inside
 * receive() on the driver's strand. */
class H2Host {
public:
  virtual ~H2Host() = default;

  /** A request's header block is complete.  ``end_stream`` true means no
   * body follows.  The method is the raw ``:method`` token; the driver
   * owns the mapping onto HttpMethod and the route table. */
  virtual void on_request_headers(std::int32_t stream_id, std::string method,
                                  std::string target, H2Headers headers,
                                  bool end_stream) = 0;

  /** A request DATA chunk.  The engine has NOT released flow-control
   * window for these bytes; the driver must account them and then call
   * consume(). */
  virtual void on_request_data(std::int32_t stream_id, std::string_view data,
                               bool end_stream) = 0;

  /** A trailing HEADERS block (request trailers).  Ends the request when
   * ``end_stream`` is set — a gRPC-style request always finishes this way
   * (review P1: trailers must not be dropped, nor END_STREAM lost). */
  virtual void on_request_trailers(std::int32_t stream_id, H2Headers trailers,
                                   bool end_stream) = 0;

  /** The peer reset the stream (per-stream cancellation). */
  virtual void on_stream_reset(std::int32_t stream_id,
                               std::uint32_t error_code) = 0;

  /** The stream is fully closed (both directions); per-stream state can be
   * dropped.  Follows on_stream_reset when the close was a reset. */
  virtual void on_stream_closed(std::int32_t stream_id) = 0;

  /** The peer sent GOAWAY: finish in-flight streams, start nothing new. */
  virtual void on_goaway_received() = 0;
};

class H2Engine {
public:
  H2Engine(H2Host &host, const H2Settings &settings);
  ~H2Engine();
  H2Engine(const H2Engine &) = delete;
  H2Engine &operator=(const H2Engine &) = delete;

  /** Feed peer bytes.  Returns false on a fatal session error — the
   * driver should flush any remaining output (a GOAWAY) and close. */
  [[nodiscard]] bool receive(std::string_view bytes);

  /** The next span of bytes the session wants on the wire, valid until
   * the next engine call; empty when nothing is pending.  The driver
   * loops this into its write pump. */
  [[nodiscard]] std::string_view next_output();

  /** Release flow-control window for request bytes the driver has
   * accounted (window updates are withheld until admission; RFC 0024). */
  void consume(std::int32_t stream_id, std::size_t bytes);

  /** Release bytes that will never be retained because their request is
   * being discarded.  Stream credit is consumed normally; connection credit
   * is restored immediately so many reset streams cannot cumulatively wedge
   * unrelated work before nghttp2's batching threshold is reached. */
  void discard(std::int32_t stream_id, std::size_t bytes);

  /** Submit a complete response on the stream.  Headers/trailers use the
   * wire names; the engine adds ``:status``. */
  [[nodiscard]] bool submit_response(std::int32_t stream_id, int status,
                                     const H2Headers &headers,
                                     std::string body,
                                     const H2Headers &trailers);

  /** Reset one stream (admission reject, slow consumer, cancel). */
  void reset_stream(std::int32_t stream_id, H2StreamError error);

  /** Graceful shutdown: GOAWAY carrying the last processed stream id;
   * in-flight streams may still complete. */
  void submit_goaway();

  /** True while the session has (or may produce) bytes to write. */
  [[nodiscard]] bool wants_write() const;

  /** True once the session is over (GOAWAY completed or fatal error) and
   * the connection should close after the final flush. */
  [[nodiscard]] bool finished() const;

  // Public so the nghttp2 C callbacks (file-local in nghttp2_session.cpp)
  // can cast their user_data back; opaque everywhere else.
  struct Impl;

private:
  std::unique_ptr<Impl> impl_;
};

} // namespace hgraph::web::detail

#endif // HGRAPH_WEB_DETAIL_H2_ENGINE_H
