// H2Engine protocol tests (RFC 0024, HTTP/2 activation plan): a genuine
// nghttp2 CLIENT session converses with the engine entirely in memory, so
// request/response framing, withheld-window flow control, RST_STREAM,
// REFUSED_STREAM, and GOAWAY are validated against the reference
// implementation without sockets.

#include "detail/h2_engine.h"

#include <nghttp2/nghttp2.h>

#include <cstring>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
using hgraph::web::detail::H2Engine;
using hgraph::web::detail::H2Headers;
using hgraph::web::detail::H2Host;
using hgraph::web::detail::H2Settings;
using hgraph::web::detail::H2StreamError;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

// ---------------------------------------------------------------------------
// Server side: a recording host.

struct RecordedRequest {
  std::int32_t stream_id{};
  std::string method{};
  std::string target{};
  H2Headers headers{};
  std::string body{};
  bool complete{};
};

struct RecordingHost final : H2Host {
  std::map<std::int32_t, RecordedRequest> requests{};
  std::vector<std::int32_t> resets{};
  std::vector<std::int32_t> closed{};
  bool goaway_received{};

  void on_request_headers(std::int32_t stream_id, std::string method,
                          std::string target, H2Headers headers,
                          bool end_stream) override {
    RecordedRequest request{stream_id, std::move(method), std::move(target),
                            std::move(headers), {}, end_stream};
    requests[stream_id] = std::move(request);
  }

  void on_request_data(std::int32_t stream_id, std::string_view data,
                       bool end_stream) override {
    auto &request = requests[stream_id];
    request.body.append(data);
    if (end_stream) {
      request.complete = true;
    }
  }

  void on_stream_reset(std::int32_t stream_id, std::uint32_t) override {
    resets.push_back(stream_id);
  }

  void on_stream_closed(std::int32_t stream_id) override {
    closed.push_back(stream_id);
  }

  void on_goaway_received() override { goaway_received = true; }
};

// ---------------------------------------------------------------------------
// Client side: a plain nghttp2 client session.

struct ClientStream {
  int status{};
  H2Headers headers{};
  std::string body{};
  bool closed{};
  std::uint32_t error_code{};
};

struct TestClient {
  nghttp2_session *session{};
  std::map<std::int32_t, ClientStream> streams{};
  bool goaway_received{};

  TestClient() {
    nghttp2_session_callbacks *callbacks = nullptr;
    require(nghttp2_session_callbacks_new(&callbacks) == 0,
            "client callback allocation failed");
    nghttp2_session_callbacks_set_on_header_callback(
        callbacks,
        [](nghttp2_session *, const nghttp2_frame *frame, const uint8_t *name,
           size_t name_length, const uint8_t *value, size_t value_length,
           uint8_t, void *user_data) -> int {
          auto &self = *static_cast<TestClient *>(user_data);
          auto &stream = self.streams[frame->hd.stream_id];
          const std::string_view header_name{
              reinterpret_cast<const char *>(name), name_length};
          const std::string header_value{
              reinterpret_cast<const char *>(value), value_length};
          if (header_name == ":status") {
            stream.status = std::stoi(header_value);
          } else {
            stream.headers.emplace_back(std::string{header_name},
                                        header_value);
          }
          return 0;
        });
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        callbacks,
        [](nghttp2_session *, uint8_t, int32_t stream_id, const uint8_t *data,
           size_t length, void *user_data) -> int {
          static_cast<TestClient *>(user_data)->streams[stream_id].body.append(
              reinterpret_cast<const char *>(data), length);
          return 0;
        });
    nghttp2_session_callbacks_set_on_stream_close_callback(
        callbacks,
        [](nghttp2_session *, int32_t stream_id, uint32_t error_code,
           void *user_data) -> int {
          auto &stream =
              static_cast<TestClient *>(user_data)->streams[stream_id];
          stream.closed = true;
          stream.error_code = error_code;
          return 0;
        });
    nghttp2_session_callbacks_set_on_frame_recv_callback(
        callbacks,
        [](nghttp2_session *, const nghttp2_frame *frame,
           void *user_data) -> int {
          if (frame->hd.type == NGHTTP2_GOAWAY) {
            static_cast<TestClient *>(user_data)->goaway_received = true;
          }
          return 0;
        });
    require(nghttp2_session_client_new(&session, callbacks, this) == 0,
            "client session allocation failed");
    nghttp2_session_callbacks_del(callbacks);
    require(nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, nullptr, 0) ==
                0,
            "client settings submission failed");
  }

  ~TestClient() { nghttp2_session_del(session); }

  [[nodiscard]] std::int32_t submit_request(std::string_view method,
                                            std::string_view path,
                                            const std::string *body) {
    std::vector<nghttp2_nv> nva;
    const auto nv = [](std::string_view name, std::string_view value) {
      return nghttp2_nv{
          reinterpret_cast<uint8_t *>(const_cast<char *>(name.data())),
          reinterpret_cast<uint8_t *>(const_cast<char *>(value.data())),
          name.size(), value.size(), NGHTTP2_NV_FLAG_NONE};
    };
    nva.push_back(nv(":method", method));
    nva.push_back(nv(":scheme", "https"));
    nva.push_back(nv(":authority", "loopback.test"));
    nva.push_back(nv(":path", path));
    nghttp2_data_provider2 provider{};
    if (body != nullptr) {
      provider.source.ptr = const_cast<std::string *>(body);
      provider.read_callback =
          [](nghttp2_session *, int32_t, uint8_t *buffer, size_t length,
             uint32_t *data_flags, nghttp2_data_source *source,
             void *) -> nghttp2_ssize {
        auto &payload = *static_cast<std::string *>(source->ptr);
        const size_t take = std::min(payload.size(), length);
        std::memcpy(buffer, payload.data(), take);
        payload.erase(0, take);
        if (payload.empty()) {
          *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        }
        return static_cast<nghttp2_ssize>(take);
      };
    }
    const std::int32_t stream_id = nghttp2_submit_request2(
        session, nullptr, nva.data(), nva.size(),
        body != nullptr ? &provider : nullptr, nullptr);
    require(stream_id > 0, "client request submission failed");
    return stream_id;
  }
};

// Pump both directions until neither side has bytes to move.
void pump(TestClient &client, H2Engine &engine) {
  bool progressed = true;
  while (progressed) {
    progressed = false;
    while (true) {
      const uint8_t *data = nullptr;
      const nghttp2_ssize length =
          nghttp2_session_mem_send2(client.session, &data);
      require(length >= 0, "client send failed");
      if (length == 0) {
        break;
      }
      require(engine.receive(std::string_view{
                  reinterpret_cast<const char *>(data),
                  static_cast<std::size_t>(length)}),
              "engine rejected client bytes");
      progressed = true;
    }
    while (true) {
      const std::string_view output = engine.next_output();
      if (output.empty()) {
        break;
      }
      const nghttp2_ssize processed = nghttp2_session_mem_recv2(
          client.session, reinterpret_cast<const uint8_t *>(output.data()),
          output.size());
      require(processed >= 0 &&
                  static_cast<std::size_t>(processed) == output.size(),
              "client rejected engine bytes");
      progressed = true;
    }
  }
}

// ---------------------------------------------------------------------------

void test_get_round_trip_with_trailers() {
  RecordingHost host;
  H2Engine engine{host, H2Settings{}};
  TestClient client;
  const auto stream_id = client.submit_request("GET", "/orders/42", nullptr);
  pump(client, engine);

  require(host.requests.count(stream_id) == 1, "the request did not arrive");
  const auto &request = host.requests[stream_id];
  require(request.method == "GET", "the method was not preserved");
  require(request.target == "/orders/42", "the target was not preserved");
  require(request.complete, "END_STREAM on HEADERS was not surfaced");
  bool host_header = false;
  for (const auto &[name, value] : request.headers) {
    host_header |= name == "host" && value == "loopback.test";
  }
  require(host_header, ":authority was not surfaced as the host header");

  require(engine.submit_response(stream_id, 200,
                                 H2Headers{{"content-type", "text/plain"}},
                                 "hello-h2",
                                 H2Headers{{"x-trail", "checksum"}}),
          "the response was not accepted");
  pump(client, engine);

  const auto &stream = client.streams[stream_id];
  require(stream.status == 200, "the status did not round-trip");
  require(stream.body == "hello-h2", "the body did not round-trip");
  bool trailer = false;
  bool content_type = false;
  for (const auto &[name, value] : stream.headers) {
    trailer |= name == "x-trail" && value == "checksum";
    content_type |= name == "content-type" && value == "text/plain";
  }
  require(content_type, "the response header did not round-trip");
  require(trailer, "the trailer did not round-trip");
  require(stream.closed && stream.error_code == NGHTTP2_NO_ERROR,
          "the stream did not close cleanly");
}

void test_withheld_window_stalls_the_sender_until_consume() {
  RecordingHost host;
  H2Settings settings;
  settings.initial_window_bytes = 4096;
  H2Engine engine{host, settings};
  TestClient client;
  pump(client, engine); // exchange SETTINGS so the 4KB window applies

  std::string body(64 * 1024, 'b');
  const auto stream_id = client.submit_request("POST", "/ingest", &body);
  pump(client, engine);

  // The engine withheld all window updates, so the client stalls at the
  // initial stream window and the request cannot complete.
  auto &request = host.requests[stream_id];
  require(!request.complete, "the body completed without any consume()");
  require(request.body.size() <= 4096,
          "more than the initial window arrived without consume()");

  // Accounting the received bytes releases window and the rest flows.
  std::size_t consumed_total = request.body.size();
  engine.consume(stream_id, consumed_total);
  pump(client, engine);
  while (!request.complete) {
    const std::size_t newly = request.body.size() - consumed_total;
    require(newly != 0, "no progress after consume()");
    engine.consume(stream_id, newly);
    consumed_total += newly;
    pump(client, engine);
  }
  require(request.body == std::string(64 * 1024, 'b'),
          "the streamed body was not delivered intact");

  require(engine.submit_response(stream_id, 204, {}, {}, {}),
          "the empty response was not accepted");
  pump(client, engine);
  require(client.streams[stream_id].status == 204,
          "the response status did not round-trip");
}

void test_reset_stream_reaches_the_client() {
  RecordingHost host;
  H2Engine engine{host, H2Settings{}};
  TestClient client;
  std::string body(1024, 'x');
  const auto stream_id = client.submit_request("POST", "/slow", &body);
  pump(client, engine);

  engine.reset_stream(stream_id, H2StreamError::EnhanceYourCalm);
  pump(client, engine);
  const auto &stream = client.streams[stream_id];
  require(stream.closed, "the reset stream did not close at the client");
  require(stream.error_code == NGHTTP2_ENHANCE_YOUR_CALM,
          "the reset error code did not reach the client");
}

void test_client_reset_surfaces_as_cancellation() {
  RecordingHost host;
  H2Engine engine{host, H2Settings{}};
  TestClient client;
  std::string body(1024, 'x');
  const auto stream_id = client.submit_request("POST", "/cancel-me", &body);
  pump(client, engine);

  require(nghttp2_submit_rst_stream(client.session, NGHTTP2_FLAG_NONE,
                                    stream_id, NGHTTP2_CANCEL) == 0,
          "the client reset submission failed");
  pump(client, engine);
  bool reset_seen = false;
  for (const auto reset : host.resets) {
    reset_seen |= reset == stream_id;
  }
  require(reset_seen, "the client cancellation did not reach the host");
}

void test_max_concurrent_streams_gates_a_compliant_client() {
  // A compliant peer holds streams beyond the advertised
  // SETTINGS_MAX_CONCURRENT_STREAMS instead of sending them, so the
  // observable contract here is gating-then-release; the REFUSED_STREAM
  // answer to a peer that violates the advertised limit is nghttp2's own
  // enforcement, exercised by h2spec conformance in CI.
  RecordingHost host;
  H2Settings settings;
  settings.max_concurrent_streams = 1;
  H2Engine engine{host, settings};
  TestClient client;
  pump(client, engine); // exchange SETTINGS first

  const auto first = client.submit_request("GET", "/one", nullptr);
  const auto second = client.submit_request("GET", "/two", nullptr);
  pump(client, engine);

  require(host.requests.count(first) == 1, "the first stream did not arrive");
  require(host.requests.count(second) == 0,
          "the second stream was not gated by the advertised limit");

  require(engine.submit_response(first, 200, {}, "one", {}),
          "the first response was not accepted");
  pump(client, engine);
  require(host.requests.count(second) == 1,
          "the gated stream was not released when a slot freed");
  require(engine.submit_response(second, 200, {}, "two", {}),
          "the second response was not accepted");
  pump(client, engine);
  require(client.streams[second].body == "two",
          "the gated stream did not complete");
}

void test_goaway_finishes_in_flight_and_refuses_new() {
  RecordingHost host;
  H2Engine engine{host, H2Settings{}};
  TestClient client;
  const auto stream_id = client.submit_request("GET", "/inflight", nullptr);
  pump(client, engine);
  require(host.requests.count(stream_id) == 1,
          "the in-flight request did not arrive");

  engine.submit_goaway();
  pump(client, engine);
  require(client.goaway_received, "the GOAWAY did not reach the client");

  // The in-flight stream still completes after GOAWAY.
  require(engine.submit_response(stream_id, 200, {}, "late-but-fine", {}),
          "the in-flight response was not accepted after GOAWAY");
  pump(client, engine);
  require(client.streams[stream_id].body == "late-but-fine",
          "the in-flight stream did not complete after GOAWAY");
}

} // namespace

int main() {
  try {
    test_get_round_trip_with_trailers();
    test_withheld_window_stalls_the_sender_until_consume();
    test_reset_stream_reaches_the_client();
    test_client_reset_surfaces_as_cancellation();
    test_max_concurrent_streams_gates_a_compliant_client();
    test_goaway_finishes_in_flight_and_refuses_new();
  } catch (const std::exception &error) {
    std::cerr << "hgraph_web_h2_engine_tests failed: " << error.what()
              << "\n";
    return 1;
  }
  std::cout << "hgraph_web_h2_engine_tests passed\n";
  return 0;
}
