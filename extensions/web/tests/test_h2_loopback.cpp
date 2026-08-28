// The HTTP/2 loopback oracle (RFC 0024, HTTP/2 activation plan): the
// extension's curl client — pinned H2Only — calls the extension's own
// server over TLS with ALPN ["h2", "http/1.1"] on 127.0.0.1.  A completed
// call therefore proves the ALPN dispatch, the nghttp2 session, header and
// trailer round-trips, and the reservation-mapped flow control end to end;
// the second call streams a body larger than the h2 stream window to
// exercise incremental admission.

#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/util/scope.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#if defined(_WIN32)
// Use nghttp2's portable signed-size alias instead of requiring the legacy
// POSIX ssize_t name in the Windows SDK namespace.
#define NGHTTP2_NO_SSIZE_T
#endif
#include <nghttp2/nghttp2.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using namespace hgraph::testing;
using namespace std::chrono_literals;

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

// A 100-year self-signed certificate for 127.0.0.1/localhost, generated
// once for this suite; the client disables verification, so nothing
// depends on its trust chain.
constexpr const char *kTestCertPem = R"pem(-----BEGIN CERTIFICATE-----
MIIDJzCCAg+gAwIBAgIUcNava/ilBlFAAby854h6n3WklcYwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJMTI3LjAuMC4xMCAXDTI2MDgxNzEwNTEyM1oYDzIxMjYw
NzI0MTA1MTIzWjAUMRIwEAYDVQQDDAkxMjcuMC4wLjEwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCR/ul5caZPxNee1TPZRkgzwPgHpsJAMchsAmRnkRXm
YGnzIvP7VZHFODj5nud2j/GCqBAxSCCbkLPfobnndehiCx40XEgpePsEDI7wnH6g
bO51I2ENAXodKE0y9Lq8WtEFyzKFbD73HUoNX8xv+KQjDxoQUpvgnXLBo0U/WJJJ
aUO1n1pAkEOMrNIj1Oda0JL6El+TbHJznRcfNh/RSKMDDYXrVXzFXt02I6dYvMKO
FVwTrBC9/bKr2WAvUNU2ugzYueVAkm24buMawgqxMbFSokJmVVj6LQz5SVheUZjs
4Rg1ASSvDs9FYAyFcAQ5X5ps4SZJa6lWhnobupq7mXj7AgMBAAGjbzBtMB0GA1Ud
DgQWBBSxRx4oz39xyLrEtzTDhqwQCaaE/TAfBgNVHSMEGDAWgBSxRx4oz39xyLrE
tzTDhqwQCaaE/TAPBgNVHRMBAf8EBTADAQH/MBoGA1UdEQQTMBGHBH8AAAGCCWxv
Y2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAf4R9Aekow3u747bg7c33t9rI0/7w
RXGDOwb8JifIE420J6miDiFoWHwXmb1/pu+Ti+VsDYUsj6nkTXLYnO9tdDp71G6T
wHa7e7bDA0fBfWvDGJMq+9SK76ZOf6gD26JEN86ejbY/YKymzRF5+Q/Njj+WjfGt
IozucryoCL/TPpQW4W846THj6Jb6ePhfb8J65vxhU5e05Nn0Zv3KmJIg854kKFzE
9DmMw+yRBc7+sgkidrC3Z43CJ2UqStxTeh7ObnTayPUUou6EEbZrAYfGW3XYI1xe
/uMQ3iaGE08aFv7q8mjRN73rcvvqabBfqDcHGf93rUbCyh8HrPC6eLgSKQ==
-----END CERTIFICATE-----
)pem";

constexpr const char *kTestKeyPem = R"pem(-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCR/ul5caZPxNee
1TPZRkgzwPgHpsJAMchsAmRnkRXmYGnzIvP7VZHFODj5nud2j/GCqBAxSCCbkLPf
obnndehiCx40XEgpePsEDI7wnH6gbO51I2ENAXodKE0y9Lq8WtEFyzKFbD73HUoN
X8xv+KQjDxoQUpvgnXLBo0U/WJJJaUO1n1pAkEOMrNIj1Oda0JL6El+TbHJznRcf
Nh/RSKMDDYXrVXzFXt02I6dYvMKOFVwTrBC9/bKr2WAvUNU2ugzYueVAkm24buMa
wgqxMbFSokJmVVj6LQz5SVheUZjs4Rg1ASSvDs9FYAyFcAQ5X5ps4SZJa6lWhnob
upq7mXj7AgMBAAECggEAAL/wjZy+gSQrN3GR20vlq8LnXur0jjqTOLBhQY6BJ+vB
s824Hce/4rrm0d71vhTEKci2cArjFZUsOfninTm5OQbgUsz7dPTvMq31bl3FPvlV
krPUgswWguPjmEWynDTShlDb7jHy812ZxlLO/asE7IHV2O5YedrtrXGALV+JtMmI
uDviZONYad/xzVUYwdft77koeKjZGnJQJitTr86AMKOpKixwtSOo/8fMXBjC4dTT
GWgfcxXorTDEUiiX3GvPgSEqRBxAkZ2epe1hdmO5rhZcry11tetupOq7tsHroASp
3zhwPtd17te/vSeMoUvrwZcn0cBFP+wKDfKgOC4E7QKBgQDCxhJyXPnlGbJBCk6t
OVJA6moRRjH62CbsC2WPjiL0z7P2NBezCuYqOUPXXsh+2TrHKfwQuj9Mv8HTonJB
ZzyZVnhwiPchM/CC1hK1ps6eHbOeu9jrbXlmaDYVPUAHhRJzA+aBoo3RCB7eVUd4
6qnUyGszXTP63daWnYQu8MudvQKBgQC/444NAMPnUgZmPlbfoxAYq5BTNPmjxzXS
rmQu2hBpNQKLDMG+sIgpSMlK8ojn9Ko1Td61y/8Ub0qndPmkADeBWNMxxpoy/Cxv
EI9x0JBN8UfoPnG2TllQ9ZiM6qH4/4jaXxahq2SkJ1ZJ+TmehCsYiqQv8eeP5m2H
kaySEpPRFwKBgQCVQIrqL+0ebe52gJuBiidJr1fQHOY3vmM1BhaxRs3qoy7YP1rZ
zERLns4pv2wMKBIuhDGv78iJ23d/4T+EdsOtDOIF+i7FtrNazwhPQp+Z8lCuFmxH
HACnRLwM0n66RHK6yAZe2F2sDHj7DoZSViAF+f6LwaQPXOcPS2z7O3IMUQKBgCA7
2IPkqgP0qnCIbk149d4/C6p+jqTtdOQkOV4JcZJKvlefV/hxbR4KRQ4a+daFKgZ0
Q0Ikt3+2RkMlCj57bteClU+aPhLse4ZYsM/8qhD9xAeGXdGzDZvk9bBORdEvE80j
Bgk4YlqU5RDeFcjECP1BZN1M9Ioeui140hVjm4MXAoGAeVrFpOGVToOedj3SG+4k
UP85yV64d10flGkxbCCaEEV8gOjoG3YWh+r0oeIGVFUOi9TzbwI9xYJb154/+dOi
ljQAIYegDzbgnKbPvtbj35Dy07fljW3WYT3fzH70nB3YieivDx2JXqp+DZ8AAnSc
8n8PFd6yOYtIC1rQQ0aqWag=
-----END PRIVATE KEY-----
)pem";

constexpr std::size_t kBigBodyBytes = 2 * 1024 * 1024;

struct RawH2Stream {
  int status{};
  std::string body{};
  bool closed{};
  std::uint32_t error_code{};
};

/** A deliberately small synchronous nghttp2/TLS client used only for the
 * discard regression below.  It can keep one request open while another
 * sends DATA plus trailers, which the public HTTP client call shape cannot
 * express. */
class RawH2Client {
public:
  struct RequestBody {
    std::string payload{};
    std::size_t offset{};
    std::vector<std::pair<std::string, std::string>> trailers{};
    bool trailers_submitted{};
  };

  explicit RawH2Client(int port)
      : context_{asio::ssl::context::tls_client}, stream_{io_, context_} {
    context_.set_verify_mode(asio::ssl::verify_none);
    const std::array<unsigned char, 3> alpn{{2, 'h', '2'}};
    require(SSL_set_alpn_protos(stream_.native_handle(), alpn.data(),
                                static_cast<unsigned int>(alpn.size())) == 0,
            "failed to configure raw-client ALPN");
    tcp::resolver resolver{io_};
    asio::connect(stream_.next_layer(),
                  resolver.resolve("127.0.0.1", std::to_string(port)));
    stream_.handshake(asio::ssl::stream_base::client);
    const unsigned char *selected = nullptr;
    unsigned int selected_length = 0;
    SSL_get0_alpn_selected(stream_.native_handle(), &selected,
                           &selected_length);
    require(selected_length == 2 && std::memcmp(selected, "h2", 2) == 0,
            "raw client did not negotiate h2");

    nghttp2_session_callbacks *callbacks = nullptr;
    require(nghttp2_session_callbacks_new(&callbacks) == 0,
            "raw client callback allocation failed");
    nghttp2_session_callbacks_set_on_header_callback(
        callbacks,
        [](nghttp2_session *, const nghttp2_frame *frame, const uint8_t *name,
           std::size_t name_length, const uint8_t *value,
           std::size_t value_length, uint8_t, void *user_data) -> int {
          auto &stream = static_cast<RawH2Client *>(user_data)
                             ->streams_[frame->hd.stream_id];
          const std::string_view header_name{
              reinterpret_cast<const char *>(name), name_length};
          if (header_name == ":status") {
            stream.status = std::stoi(std::string{
                reinterpret_cast<const char *>(value), value_length});
          }
          return 0;
        });
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks,
        [](nghttp2_session *, uint8_t, std::int32_t stream_id,
           const uint8_t *data, std::size_t length, void *user_data) -> int {
          static_cast<RawH2Client *>(user_data)
              ->streams_[stream_id]
              .body.append(reinterpret_cast<const char *>(data), length);
          return 0;
        });
    nghttp2_session_callbacks_set_on_stream_close_callback(
        callbacks,
        [](nghttp2_session *, std::int32_t stream_id, std::uint32_t error_code,
           void *user_data) -> int {
          auto &stream =
              static_cast<RawH2Client *>(user_data)->streams_[stream_id];
          stream.closed = true;
          stream.error_code = error_code;
          return 0;
        });
    nghttp2_session_callbacks_set_on_frame_recv_callback(
        callbacks,
        [](nghttp2_session *, const nghttp2_frame *frame,
           void *user_data) -> int {
          if (frame->hd.type == NGHTTP2_SETTINGS &&
              (frame->hd.flags & NGHTTP2_FLAG_ACK) == 0) {
            static_cast<RawH2Client *>(user_data)->settings_seen_ = true;
          }
          return 0;
        });
    require(nghttp2_session_client_new(&session_, callbacks, this) == 0,
            "raw client session allocation failed");
    nghttp2_session_callbacks_del(callbacks);
    require(nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, nullptr, 0) ==
                0,
            "raw client SETTINGS submission failed");
    stream_.next_layer().non_blocking(true);
  }

  ~RawH2Client() {
    if (session_ != nullptr) {
      nghttp2_session_del(session_);
    }
  }

  RawH2Client(const RawH2Client &) = delete;
  RawH2Client &operator=(const RawH2Client &) = delete;

  void exchange_settings() {
    pump_until([this] { return settings_seen_; },
               "the server SETTINGS did not arrive");
    flush_output(); // the automatically generated SETTINGS ACK
  }

  [[nodiscard]] std::int32_t submit_open_request(std::string_view path) {
    auto headers = request_headers("POST", path);
    const std::int32_t stream_id =
        nghttp2_submit_headers(session_, NGHTTP2_FLAG_END_HEADERS, -1, nullptr,
                               headers.data(), headers.size(), nullptr);
    require(stream_id > 0, "open request submission failed");
    streams_.try_emplace(stream_id);
    return stream_id;
  }

  [[nodiscard]] std::int32_t submit_request(std::string_view path,
                                            RequestBody &body) {
    auto headers = request_headers("POST", path);
    nghttp2_data_provider2 provider{};
    provider.source.ptr = &body;
    provider.read_callback =
        [](nghttp2_session *session, std::int32_t stream_id, uint8_t *buffer,
           std::size_t length, std::uint32_t *data_flags,
           nghttp2_data_source *source, void *) -> nghttp2_ssize {
      auto &request = *static_cast<RequestBody *>(source->ptr);
      const std::size_t remaining = request.payload.size() - request.offset;
      const std::size_t take = std::min(remaining, length);
      std::memcpy(buffer, request.payload.data() + request.offset, take);
      request.offset += take;
      if (request.offset == request.payload.size()) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        if (!request.trailers.empty() && !request.trailers_submitted) {
          *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
          request.trailers_submitted = true;
          std::vector<nghttp2_nv> trailers;
          trailers.reserve(request.trailers.size());
          for (const auto &[name, value] : request.trailers) {
            trailers.push_back(make_nv(name, value));
          }
          if (nghttp2_submit_trailer(session, stream_id, trailers.data(),
                                     trailers.size()) != 0) {
            return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
          }
        }
      }
      return static_cast<nghttp2_ssize>(take);
    };
    const std::int32_t stream_id = nghttp2_submit_request2(
        session_, nullptr, headers.data(), headers.size(), &provider, nullptr);
    require(stream_id > 0, "raw request submission failed");
    streams_.try_emplace(stream_id);
    return stream_id;
  }

  void reset(std::int32_t stream_id) {
    require(nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id,
                                      NGHTTP2_CANCEL) == 0,
            "raw client reset submission failed");
  }

  [[nodiscard]] const RawH2Stream &stream(std::int32_t stream_id) const {
    return streams_.at(stream_id);
  }

  [[nodiscard]] std::int32_t connection_window() const {
    return nghttp2_session_get_remote_window_size(session_);
  }

  template <typename Predicate>
  void pump_until(Predicate predicate, std::string_view failure) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
      flush_output();
      bool progressed = false;
      while (read_input()) {
        progressed = true;
      }
      if (predicate()) {
        flush_output();
        return;
      }
      if (!progressed) {
        std::this_thread::sleep_for(1ms);
      }
    }
    throw std::runtime_error(std::string{failure});
  }

private:
  [[nodiscard]] static nghttp2_nv make_nv(std::string_view name,
                                          std::string_view value) {
    return nghttp2_nv{
        reinterpret_cast<uint8_t *>(const_cast<char *>(name.data())),
        reinterpret_cast<uint8_t *>(const_cast<char *>(value.data())),
        name.size(), value.size(), NGHTTP2_NV_FLAG_NONE};
  }

  [[nodiscard]] static std::array<nghttp2_nv, 4>
  request_headers(std::string_view method, std::string_view path) {
    return {make_nv(":method", method), make_nv(":scheme", "https"),
            make_nv(":authority", "127.0.0.1"), make_nv(":path", path)};
  }

  void flush_output() {
    while (true) {
      const uint8_t *bytes = nullptr;
      const nghttp2_ssize length = nghttp2_session_mem_send2(session_, &bytes);
      require(length >= 0, "raw client failed to encode output");
      if (length == 0) {
        return;
      }
      stream_.next_layer().non_blocking(false);
      boost::system::error_code ec;
      asio::write(stream_,
                  asio::buffer(bytes, static_cast<std::size_t>(length)), ec);
      stream_.next_layer().non_blocking(true);
      require(!ec, "raw client TLS write failed: " + ec.message());
    }
  }

  [[nodiscard]] bool read_input() {
    boost::system::error_code ec;
    const std::size_t available = stream_.next_layer().available(ec);
    require(!ec, "raw client socket query failed: " + ec.message());
    if (available == 0 && SSL_pending(stream_.native_handle()) == 0) {
      return false;
    }
    std::array<char, 16 * 1024> buffer{};
    const std::size_t received = stream_.read_some(asio::buffer(buffer), ec);
    if (ec == asio::error::would_block || ec == asio::error::try_again) {
      return false;
    }
    require(!ec, "raw client TLS read failed: " + ec.message());
    const nghttp2_ssize processed = nghttp2_session_mem_recv2(
        session_, reinterpret_cast<const uint8_t *>(buffer.data()), received);
    require(processed >= 0 && static_cast<std::size_t>(processed) == received,
            "raw client rejected server HTTP/2 bytes");
    return received != 0;
  }

  asio::io_context io_{};
  asio::ssl::context context_;
  asio::ssl::stream<tcp::socket> stream_;
  nghttp2_session *session_{};
  std::map<std::int32_t, RawH2Stream> streams_{};
  bool settings_seen_{};
};

void test_rejected_stream_restores_connection_window(int port) {
  RawH2Client client{port};
  client.exchange_settings();

  // Stream 1 owns the only ingress record without completing.  Stream 3 is
  // therefore backpressured: its DATA remains unconsumed when a large trailer
  // block crosses the one-window metadata bound and forces a reset.
  const std::int32_t holding =
      client.submit_open_request("/h2-discard-probe");
  // More than one default 65,535-byte connection window in aggregate: a
  // driver that drops each stream's 3000-byte credit eventually wedges,
  // while the correct consume path periodically emits WINDOW_UPDATE.
  for (int attempt = 0; attempt != 24; ++attempt) {
    RawH2Client::RequestBody rejected_body{
        std::string(3000, 'r'),
        0,
        {{"x-overflow", std::string(2000, 't')}},
        false};
    const std::int32_t rejected =
        client.submit_request("/h2-discard-probe", rejected_body);
    client.pump_until([&] { return client.stream(rejected).closed; },
                      "a trailer-overflow stream was not reset");
    require(client.stream(rejected).error_code == NGHTTP2_ENHANCE_YOUR_CALM,
            "a trailer-overflow stream used the wrong reset code");
  }

  client.reset(holding);
  client.pump_until([&] { return client.stream(holding).closed; },
                    "the holding stream did not reset");

  // The rejected streams consumed more than the whole connection window.
  // This complete request can reach dispatch only if every discard returned
  // its credit; the deliberately unanswered route then produces the timeout
  // 503 that proves dispatch completed.
  RawH2Client::RequestBody recovery{std::string(3000, 'g')};
  const std::int32_t recovered =
      client.submit_request("/h2-discard-probe", recovery);
  try {
    client.pump_until([&] { return client.stream(recovered).closed; },
                      "the connection window was not restored after discard");
  } catch (const std::runtime_error &) {
    throw std::runtime_error(
        "the recovery stream stalled (sent=" + std::to_string(recovery.offset) +
        ", window=" + std::to_string(client.connection_window()) +
        ", status=" + std::to_string(client.stream(recovered).status) +
        ", body='" + client.stream(recovered).body + "')");
  }
  require(client.stream(recovered).error_code == NGHTTP2_NO_ERROR,
          "the recovery stream did not close cleanly");
  require(client.stream(recovered).status == 503,
          "the recovery request did not reach the dispatch timeout");
}

inline std::atomic<bool> get_triggered{false};
inline std::atomic<bool> post_triggered{false};
inline std::atomic<bool> status_triggered{false};
inline std::atomic<bool> get_response_seen{false};
inline std::atomic<bool> post_response_seen{false};
inline std::atomic<bool> status_response_seen{false};
inline std::atomic<bool> failure_seen{false};
inline std::atomic<bool> raw_rejection_complete{false};
inline std::atomic<Int> bound_port{0};
inline Value observed_get_response{};
inline Value observed_post_response{};
inline Value observed_status_response{};
inline Value observed_failure{};
inline Value observed_server_peer{};

void release_test_state() {
  observed_get_response = Value{};
  observed_post_response = Value{};
  observed_status_response = Value{};
  observed_failure = Value{};
  observed_server_peer = Value{};
}

void maybe_stop(NodeView &node) {
  if ((raw_rejection_complete.load() && get_response_seen.load() &&
       post_response_seen.load() && status_response_seen.load()) ||
      failure_seen.load()) {
    node.graph().executor().request_stop();
  }
}

struct H2GetTrigger {
  static constexpr auto name = "web_h2_loopback_get_trigger";

  static void eval(In<"stats", TS<WebServerStats>, InputValidity::Unchecked>
                       stats,
                   Out<TS<HttpClientRequest>> out) {
    if (!stats.valid() || !stats.modified() || get_triggered.load()) {
      return;
    }
    const Int port = stats.base()
                         .value()
                         .as_bundle()
                         .at("listening_port")
                         .checked_as<Int>();
    if (port == 0) {
      return;
    }
    bound_port.store(port);
    if (!raw_rejection_complete.load()) {
      return;
    }
    get_triggered.store(true);
    const Value request = make_client_request(
        HttpMethod::Get,
        Str{"https://127.0.0.1:" + std::to_string(port) + "/h2/answer"});
    out.apply(request.view());
  }
};

struct H2GetCapture {
  static constexpr auto name = "web_h2_loopback_get_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result,
                   Out<TS<HttpClientRequest>> out) {
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      failure_seen.store(true);
      maybe_stop(node);
      return;
    }
    auto response = result.template field<"response">();
    if (!response.valid() || !response.modified()) {
      return;
    }
    observed_get_response = response.base().value().clone();
    get_response_seen.store(true);
    if (!post_triggered.exchange(true)) {
      // Chain the streaming call: a body larger than the h2 stream window
      // exercises the consume-gated flow control end to end.
      const Value request = make_client_request(
          HttpMethod::Post,
          Str{"https://127.0.0.1:" + std::to_string(bound_port.load()) +
              "/h2-ingest"},
          {}, Bytes{std::string(kBigBodyBytes, 'z')});
      out.apply(request.view());
    }
    maybe_stop(node);
  }
};

struct H2PostCapture {
  static constexpr auto name = "web_h2_loopback_post_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result,
                   Out<TS<HttpClientRequest>> out) {
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      failure_seen.store(true);
      maybe_stop(node);
      return;
    }
    auto response = result.template field<"response">();
    if (!response.valid() || !response.modified()) {
      return;
    }
    observed_post_response = response.base().value().clone();
    post_response_seen.store(true);
    if (!status_triggered.exchange(true)) {
      // A gRPC-shaped exchange: empty body, terminal status in trailers.
      const Value request = make_client_request(
          HttpMethod::Get,
          Str{"https://127.0.0.1:" + std::to_string(bound_port.load()) +
              "/h2-status"});
      out.apply(request.view());
    }
    maybe_stop(node);
  }
};

struct H2StatusCapture {
  static constexpr auto name = "web_h2_loopback_status_capture";

  static void eval(NodeView node,
                   In<"result", HttpCallResult, InputValidity::Unchecked>
                       result) {
    auto failure = result.template field<"failure">();
    if (failure.valid() && failure.modified()) {
      observed_failure = failure.base().value().clone();
      failure_seen.store(true);
      maybe_stop(node);
      return;
    }
    auto response = result.template field<"response">();
    if (response.valid() && response.modified()) {
      observed_status_response = response.base().value().clone();
      status_response_seen.store(true);
    }
    maybe_stop(node);
  }
};

struct H2StatusResponse {
  static constexpr auto name = "web_h2_loopback_status_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const Value response = make_response(
          Int{200}, {}, Bytes{},
          {{Str{"grpc-like-status"}, Str{"0"}}});
      out.apply(response.view());
    }
  }
};

struct H2AnswerId {
  static constexpr auto name = "web_h2_loopback_answer_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const auto fields = request.base().value().as_bundle();
      observed_server_peer = fields.at("peer").clone();
      out.apply(fields.at("request_id"));
    }
  }
};

struct H2AnswerResponse {
  static constexpr auto name = "web_h2_loopback_answer_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const auto name_param = request.base()
                                  .value()
                                  .as_bundle()
                                  .at("request")
                                  .as_bundle()
                                  .at("path_params");
      Str captured{};
      for (const auto param : name_param.as_list()) {
        captured = param.as_bundle().at("value").checked_as<Str>();
      }
      const Value response = make_response(
          Int{200}, {{Str{"x-served-by"}, Str{"h2-loopback"}}},
          Bytes{"hi " + std::string{captured}},
          {{Str{"x-trail"}, Str{"h2-checksum"}}});
      out.apply(response.view());
    }
  }
};

struct H2IngestId {
  static constexpr auto name = "web_h2_loopback_ingest_id";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<Int>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      out.apply(request.base().value().as_bundle().at("request_id"));
    }
  }
};

struct H2IngestResponse {
  static constexpr auto name = "web_h2_loopback_ingest_response";

  static void eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>
                       routed,
                   Out<TS<HttpResponse>> out) {
    auto request = routed.template field<"request">();
    if (request.valid() && request.modified()) {
      const auto body = request.base()
                            .value()
                            .as_bundle()
                            .at("request")
                            .as_bundle()
                            .at("body")
                            .checked_as<Bytes>();
      const Value response = make_response(
          Int{200}, {}, Bytes{std::to_string(body.data.size())});
      out.apply(response.view());
    }
  }
};

struct H2DiscardProbeSink {
  static constexpr auto name = "web_h2_discard_probe_sink";

  static void
  eval(In<"routed", WebRouteOutput, InputValidity::Unchecked>) {}
};

struct H2LoopbackGraph {
  static constexpr auto name = "web_h2_loopback_test_graph";

  static void compose(Wiring &w) {
    const auto server_path = service::path("web-h2-server");
    const auto client_path = service::path("web-h2-client");
    register_server(w, server_path,
                    server_config()
                        .port(0)
                        .request_timeout(500ms)
                        .ingress_limits(1, 64 * 1024 * 1024)
                        .h2_initial_window_bytes(4096)
                        .stats_interval(50ms)
                        .tls(tls_server()
                                 .cert_pem(Str{kTestCertPem})
                                 .key_pem(Str{kTestKeyPem})
                                 .alpn({Str{"h2"}, Str{"http/1.1"}})
                                 .build())
                        .build());
    register_client(w, client_path,
                    client_config()
                        .http_version_policy(WebHttpVersionPolicy::H2Only)
                        .tls(tls_client()
                                 .verify_peer(false)
                                 .verify_host(false)
                                 .build())
                        .build());

    auto stats = server_stats(w, server_path);

    auto answer_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/h2/{name}"));
    auto answered = serve(w, server_path, answer_route);
    auto answer_id = wire<H2AnswerId>(w, answered).as<TS<Int>>();
    auto answer_response =
        wire<H2AnswerResponse>(w, answered).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, answer_id, answer_response)));

    auto ingest_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Post, "/h2-ingest"));
    auto ingested = serve(w, server_path, ingest_route);
    auto ingest_id = wire<H2IngestId>(w, ingested).as<TS<Int>>();
    auto ingest_response =
        wire<H2IngestResponse>(w, ingested).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, ingest_id, ingest_response)));

    auto discard_probe_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Post, "/h2-discard-probe"));
    static_cast<void>(wire<H2DiscardProbeSink>(
        w, serve(w, server_path, discard_probe_route)));

    auto status_route = wire<stdlib::const_, TS<WebRoute>>(
        w, make_route(HttpMethod::Get, "/h2-status"));
    auto status_served = serve(w, server_path, status_route);
    auto status_id = wire<H2IngestId>(w, status_served).as<TS<Int>>();
    auto status_response =
        wire<H2StatusResponse>(w, status_served).as<TS<HttpResponse>>();
    static_cast<void>(respond(
        w, server_path, respond_request(w, status_id, status_response)));

    auto get_request =
        wire<H2GetTrigger>(w, stats).as<TS<HttpClientRequest>>();
    auto get_result =
        http_request(w, client_path, http_client_call(w, get_request));
    auto post_request =
        wire<H2GetCapture>(w, get_result).as<TS<HttpClientRequest>>();
    auto post_result =
        http_request(w, client_path, http_client_call(w, post_request));
    auto status_request =
        wire<H2PostCapture>(w, post_result).as<TS<HttpClientRequest>>();
    auto status_result =
        http_request(w, client_path, http_client_call(w, status_request));
    static_cast<void>(wire<H2StatusCapture>(w, status_result));
  }
};

} // namespace

int main() {
  try {
    hgraph::stdlib::register_standard_operators();
    const auto release_state = hgraph::make_scope_exit(release_test_state);
    register_web_types();

    const DateTime start = wall_now();
    GraphExecutorBuilder executor_builder;
    executor_builder
        .graph_builder(build_graph<H2LoopbackGraph>(
            WiringOptions{.is_realtime = true}))
        .mode(GraphExecutorMode::RealTime)
        .start_time(start)
        .end_time(start + TimeDelta{30'000'000});
    auto executor = executor_builder.make_executor();
    auto view = executor.view();
    {
      AsyncGraphExecutorRun runner{view};
      try {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (bound_port.load() == 0 &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::sleep_for(10ms);
        }
        require(bound_port.load() != 0,
                "the h2 server did not report its listening port");
        test_rejected_stream_restores_connection_window(
            static_cast<int>(bound_port.load()));
        raw_rejection_complete.store(true);
        runner.join();
      } catch (...) {
        view.request_stop();
        runner.join();
        throw;
      }
    }

    require(!failure_seen.load(),
            std::string{"a call took the transport-failure arm"} +
                (observed_failure.has_value()
                     ? ": " + std::string{observed_failure.view()
                                              .as_bundle()
                                              .at("message")
                                              .checked_as<Str>()}
                     : ""));
    require(get_response_seen.load(), "the h2 GET did not complete");
    const auto response = observed_get_response.view().as_bundle();
    require(response.at("status").checked_as<Int>() == Int{200},
            "the h2 GET status was not 200");
    require(response.at("body").checked_as<Bytes>().data == "hi answer",
            "the h2 GET body (path capture) did not round-trip");
    bool served_by = false;
    for (const auto entry : response.at("headers").as_list()) {
      const auto pair = entry.as_bundle();
      served_by |= pair.at("name").checked_as<Str>() == Str{"x-served-by"} &&
                   pair.at("value").checked_as<Str>() == Str{"h2-loopback"};
    }
    require(served_by, "the h2 response header did not round-trip");
    // Trailers arrive on the TRAILERS field, distinct from headers — the
    // separation a gRPC-style terminal status depends on (review P1).
    bool trailer = false;
    const auto trailer_entries = response.at("trailers");
    require(trailer_entries.data() != nullptr,
            "the h2 response trailers field was not populated");
    for (const auto entry : trailer_entries.as_list()) {
      const auto pair = entry.as_bundle();
      trailer |= pair.at("name").checked_as<Str>() == Str{"x-trail"} &&
                 pair.at("value").checked_as<Str>() == Str{"h2-checksum"};
    }
    require(trailer, "the h2 response trailer did not arrive as a trailer");

    const auto peer = observed_server_peer.view().as_bundle();
    require(peer.at("negotiated_protocol").checked_as<Str>() == Str{"h2"},
            "the server-side peer did not record the h2 negotiation");
    require(peer.at("tls").checked_as<Bool>(),
            "the server-side peer did not record TLS");

    require(post_response_seen.load(), "the h2 POST did not complete");
    const auto post = observed_post_response.view().as_bundle();
    require(post.at("status").checked_as<Int>() == Int{200},
            "the h2 POST status was not 200");
    require(post.at("body").checked_as<Bytes>().data ==
                std::to_string(kBigBodyBytes),
            "the h2 POST body was not delivered intact");

    // The gRPC-shaped exchange: an EMPTY body must not fold the trailer
    // into the headers (review P1 — origin-based separation).
    require(status_response_seen.load(),
            "the trailers-only exchange did not complete");
    const auto status = observed_status_response.view().as_bundle();
    require(status.at("body").checked_as<Bytes>().data.empty(),
            "the trailers-only response grew a body");
    bool status_trailer = false;
    const auto status_trailers = status.at("trailers");
    require(status_trailers.data() != nullptr,
            "the trailers-only response has no trailers field");
    for (const auto entry : status_trailers.as_list()) {
      const auto pair = entry.as_bundle();
      status_trailer |=
          pair.at("name").checked_as<Str>() == Str{"grpc-like-status"} &&
          pair.at("value").checked_as<Str>() == Str{"0"};
    }
    require(status_trailer,
            "the empty-body trailer did not arrive as a trailer");
    for (const auto entry : status.at("headers").as_list()) {
      const auto pair = entry.as_bundle();
      require(pair.at("name").checked_as<Str>() != Str{"grpc-like-status"},
              "the empty-body trailer leaked into the headers");
    }

    release_test_state();
  } catch (const std::exception &error) {
    release_test_state();
    std::cerr << "hgraph_web_h2_loopback_tests failed: " << error.what()
              << "\n";
    return 1;
  }
  std::cout << "hgraph_web_h2_loopback_tests passed\n";
  return 0;
}
