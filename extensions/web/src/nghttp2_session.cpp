// The HTTP/2 protocol engine: the only translation unit that includes
// nghttp2 (RFC 0024, HTTP/2 activation plan).  H2Engine wraps a server
// session in the pull model — nghttp2_session_mem_recv2 in, mem_send2 out —
// with automatic window updates disabled so request-DATA flow control is
// released only through consume(), i.e. only once the driver has
// reservation-accounted the bytes (RFC 0024, flow control).

#include "detail/h2_engine.h"

// MSVC has no ssize_t; the v1 nghttp2 API is hidden and only the *2
// entry points (which this code uses exclusively) remain.
#ifdef _WIN32
#define NGHTTP2_NO_SSIZE_T
#endif

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace hgraph::web::detail {
namespace {

[[nodiscard]] nghttp2_nv make_nv(std::string_view name,
                                 std::string_view value) noexcept {
  // nghttp2 copies name and value at submit time (no NO_COPY flags).
  return nghttp2_nv{
      reinterpret_cast<uint8_t *>(const_cast<char *>(name.data())),
      reinterpret_cast<uint8_t *>(const_cast<char *>(value.data())),
      name.size(), value.size(), NGHTTP2_NV_FLAG_NONE};
}

struct StreamState {
  std::string method{};
  std::string target{};
  std::string authority{};
  H2Headers headers{};
  std::size_t header_bytes{};
  bool host_seen{};
};

struct ResponseState {
  std::string status{};
  std::string body{};
  std::size_t offset{};
  std::unique_ptr<std::ifstream> file{};
  H2Headers trailers{};
};

} // namespace

struct H2Engine::Impl {
  H2Host &host;
  H2Settings settings;
  nghttp2_session *session{};
  std::map<std::int32_t, StreamState> requests{};
  std::map<std::int32_t, StreamState> trailer_blocks{};
  std::map<std::int32_t, ResponseState> responses{};
  bool fatal{};

  Impl(H2Host &host_ref, const H2Settings &config)
      : host{host_ref}, settings{config} {}

  ~Impl() {
    if (session != nullptr) {
      nghttp2_session_del(session);
    }
  }
};

namespace {

[[nodiscard]] H2Engine::Impl *impl_of(void *user_data) noexcept {
  return static_cast<H2Engine::Impl *>(user_data);
}

int on_begin_headers(nghttp2_session *, const nghttp2_frame *frame,
                     void *user_data) {
  if (frame->hd.type != NGHTTP2_HEADERS) {
    return 0;
  }
  if (frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
    impl_of(user_data)->requests.emplace(frame->hd.stream_id, StreamState{});
  } else if (frame->headers.cat == NGHTTP2_HCAT_HEADERS) {
    // A trailing HEADERS block: request trailers.
    impl_of(user_data)->trailer_blocks.emplace(frame->hd.stream_id,
                                               StreamState{});
  }
  return 0;
}

int on_header(nghttp2_session *, const nghttp2_frame *frame,
              const uint8_t *name, size_t name_length, const uint8_t *value,
              size_t value_length, uint8_t, void *user_data) {
  auto *impl = impl_of(user_data);
  auto found = impl->requests.find(frame->hd.stream_id);
  const bool trailer = found == impl->requests.end();
  if (trailer) {
    found = impl->trailer_blocks.find(frame->hd.stream_id);
    if (found == impl->trailer_blocks.end()) {
      return 0;
    }
  }
  StreamState &stream = found->second;
  stream.header_bytes += name_length + value_length;
  if (stream.header_bytes > impl->settings.max_header_bytes) {
    // Oversized header block: reset just this stream (RFC 0024 maps the
    // h1 header limit onto h2 per stream).
    if (trailer) {
      impl->trailer_blocks.erase(found);
    } else {
      impl->requests.erase(found);
    }
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  }
  const std::string_view header_name{reinterpret_cast<const char *>(name),
                                     name_length};
  const std::string_view header_value{reinterpret_cast<const char *>(value),
                                      value_length};
  if (header_name == ":method") {
    stream.method.assign(header_value);
  } else if (header_name == ":path") {
    stream.target.assign(header_value);
  } else if (header_name == ":authority") {
    stream.authority.assign(header_value);
  } else if (header_name == ":scheme") {
    // The scheme is implied by the listener's TLS configuration.
  } else {
    if (header_name == "host") {
      stream.host_seen = true;
    }
    stream.headers.emplace_back(std::string{header_name},
                                std::string{header_value});
  }
  return 0;
}

int on_frame_recv(nghttp2_session *, const nghttp2_frame *frame,
                  void *user_data) {
  auto *impl = impl_of(user_data);
  switch (frame->hd.type) {
  case NGHTTP2_HEADERS: {
    if (frame->headers.cat == NGHTTP2_HCAT_HEADERS) {
      const auto trailer_found = impl->trailer_blocks.find(frame->hd.stream_id);
      if (trailer_found == impl->trailer_blocks.end()) {
        break;
      }
      H2Headers trailers = std::move(trailer_found->second.headers);
      impl->trailer_blocks.erase(trailer_found);
      impl->host.on_request_trailers(
          frame->hd.stream_id, std::move(trailers),
          (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0);
      break;
    }
    if (frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
      break;
    }
    const auto found = impl->requests.find(frame->hd.stream_id);
    if (found == impl->requests.end()) {
      break;
    }
    StreamState stream = std::move(found->second);
    impl->requests.erase(found);
    if (!stream.host_seen && !stream.authority.empty()) {
      // ``:authority`` carries what h1 sent as Host (RFC 9113 §8.3.1);
      // surfacing it keeps the graph-visible header list h1-shaped.
      stream.headers.emplace_back("host", stream.authority);
    }
    impl->host.on_request_headers(
        frame->hd.stream_id, std::move(stream.method),
        std::move(stream.target), std::move(stream.headers),
        (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0);
    break;
  }
  case NGHTTP2_DATA:
    if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0) {
      impl->host.on_request_data(frame->hd.stream_id, std::string_view{},
                                 true);
    }
    break;
  case NGHTTP2_GOAWAY:
    impl->host.on_goaway_received();
    break;
  default:
    break;
  }
  return 0;
}

int on_data_chunk_recv(nghttp2_session *, uint8_t, int32_t stream_id,
                       const uint8_t *data, size_t length, void *user_data) {
  impl_of(user_data)->host.on_request_data(
      stream_id,
      std::string_view{reinterpret_cast<const char *>(data), length}, false);
  return 0;
}

int on_stream_close(nghttp2_session *, int32_t stream_id, uint32_t error_code,
                    void *user_data) {
  auto *impl = impl_of(user_data);
  impl->requests.erase(stream_id);
  impl->trailer_blocks.erase(stream_id);
  impl->responses.erase(stream_id);
  if (error_code != NGHTTP2_NO_ERROR) {
    impl->host.on_stream_reset(stream_id, error_code);
  }
  impl->host.on_stream_closed(stream_id);
  return 0;
}

nghttp2_ssize read_response_body(nghttp2_session *session, int32_t stream_id,
                                 uint8_t *buffer, size_t length,
                                 uint32_t *data_flags, nghttp2_data_source *,
                                 void *user_data) {
  auto *impl = impl_of(user_data);
  const auto found = impl->responses.find(stream_id);
  if (found == impl->responses.end()) {
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  }
  ResponseState &response = found->second;
  std::size_t take = 0;
  bool eof = false;
  if (response.file) {
    response.file->read(reinterpret_cast<char *>(buffer),
                        static_cast<std::streamsize>(length));
    take = static_cast<std::size_t>(response.file->gcount());
    eof = response.file->eof();
    if (!eof && !response.file->good()) {
      return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    }
  } else {
    const std::size_t remaining = response.body.size() - response.offset;
    take = std::min(remaining, length);
    std::memcpy(buffer, response.body.data() + response.offset, take);
    response.offset += take;
    eof = response.offset == response.body.size();
  }
  if (eof) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    if (!response.trailers.empty()) {
      *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      std::vector<nghttp2_nv> nva;
      nva.reserve(response.trailers.size());
      for (const auto &[name, value] : response.trailers) {
        nva.push_back(make_nv(name, value));
      }
      if (nghttp2_submit_trailer(session, stream_id, nva.data(),
                                 nva.size()) != 0) {
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
      }
    }
  }
  return static_cast<nghttp2_ssize>(take);
}

} // namespace

H2Engine::H2Engine(H2Host &host, const H2Settings &settings)
    : impl_{std::make_unique<Impl>(host, settings)} {
  nghttp2_session_callbacks *callbacks = nullptr;
  if (nghttp2_session_callbacks_new(&callbacks) != 0) {
    throw std::runtime_error("nghttp2 callbacks allocation failed");
  }
  nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                          on_begin_headers);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, on_header);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                       on_frame_recv);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
      callbacks, on_data_chunk_recv);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks,
                                                         on_stream_close);

  nghttp2_option *option = nullptr;
  if (nghttp2_option_new(&option) != 0) {
    nghttp2_session_callbacks_del(callbacks);
    throw std::runtime_error("nghttp2 option allocation failed");
  }
  // Window updates are the admission lever: released only via consume()
  // once the driver has reserved the bytes (RFC 0024, flow control).
  nghttp2_option_set_no_auto_window_update(option, 1);

  const int rc = nghttp2_session_server_new2(&impl_->session, callbacks,
                                             impl_.get(), option);
  nghttp2_option_del(option);
  nghttp2_session_callbacks_del(callbacks);
  if (rc != 0) {
    throw std::runtime_error("nghttp2 server session allocation failed");
  }

  const std::array<nghttp2_settings_entry, 3> entries{{
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS,
       static_cast<uint32_t>(settings.max_concurrent_streams)},
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,
       static_cast<uint32_t>(settings.initial_window_bytes)},
      {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE,
       static_cast<uint32_t>(settings.max_header_bytes)},
  }};
  if (nghttp2_submit_settings(impl_->session, NGHTTP2_FLAG_NONE,
                              entries.data(), entries.size()) != 0) {
    throw std::runtime_error("nghttp2 settings submission failed");
  }
  // The connection-level window follows the configured stream window so a
  // single stream can use the whole configured budget.
  static_cast<void>(nghttp2_session_set_local_window_size(
      impl_->session, NGHTTP2_FLAG_NONE, 0,
      static_cast<int32_t>(settings.initial_window_bytes)));
}

H2Engine::~H2Engine() = default;

bool H2Engine::receive(std::string_view bytes) {
  if (impl_->fatal) {
    return false;
  }
  const nghttp2_ssize processed = nghttp2_session_mem_recv2(
      impl_->session, reinterpret_cast<const uint8_t *>(bytes.data()),
      bytes.size());
  if (processed < 0 ||
      static_cast<std::size_t>(processed) != bytes.size()) {
    impl_->fatal = true;
    return false;
  }
  return true;
}

std::string_view H2Engine::next_output() {
  const uint8_t *data = nullptr;
  const nghttp2_ssize length =
      nghttp2_session_mem_send2(impl_->session, &data);
  if (length <= 0) {
    if (length < 0) {
      impl_->fatal = true;
    }
    return {};
  }
  return std::string_view{reinterpret_cast<const char *>(data),
                          static_cast<std::size_t>(length)};
}

void H2Engine::consume(std::int32_t stream_id, std::size_t bytes) {
  static_cast<void>(
      nghttp2_session_consume(impl_->session, stream_id, bytes));
}

void H2Engine::discard(std::int32_t stream_id, std::size_t bytes) {
  // A reset may remove the stream-level WINDOW_UPDATE queued by consume().
  // Split the two levels: stream credit may follow nghttp2's normal batching,
  // while connection credit is returned explicitly and cannot disappear with
  // the stream (review P1).
  static_cast<void>(
      nghttp2_session_consume_stream(impl_->session, stream_id, bytes));
  static_cast<void>(nghttp2_submit_window_update(
      impl_->session, NGHTTP2_FLAG_NONE, 0, static_cast<std::int32_t>(bytes)));
}

bool H2Engine::submit_response(std::int32_t stream_id, int status,
                               const H2Headers &headers, std::string body,
                               const H2Headers &trailers) {
  auto [slot, inserted] = impl_->responses.emplace(
      stream_id,
      ResponseState{
          std::to_string(status),
          std::move(body),
          0,
          nullptr,
          trailers});
  if (!inserted) {
    return false;
  }
  std::vector<nghttp2_nv> nva;
  nva.reserve(headers.size() + 1);
  nva.push_back(make_nv(":status", slot->second.status));
  for (const auto &[name, value] : headers) {
    nva.push_back(make_nv(name, value));
  }
  nghttp2_data_provider2 provider{};
  provider.read_callback = read_response_body;
  const bool with_body =
      !slot->second.body.empty() || !slot->second.trailers.empty();
  const int rc = nghttp2_submit_response2(impl_->session, stream_id,
                                          nva.data(), nva.size(),
                                          with_body ? &provider : nullptr);
  if (rc != 0) {
    impl_->responses.erase(stream_id);
    return false;
  }
  return true;
}

bool H2Engine::submit_file_response(std::int32_t stream_id, int status,
                                    const H2Headers &headers,
                                    std::string path,
                                    const H2Headers &trailers) {
  auto file = std::make_unique<std::ifstream>(path, std::ios::binary);
  if (!*file) {
    return false;
  }
  auto [slot, inserted] = impl_->responses.emplace(
      stream_id,
      ResponseState{
          std::to_string(status),
          "",
          0,
          std::move(file),
          trailers});
  if (!inserted) {
    return false;
  }
  std::vector<nghttp2_nv> nva;
  nva.reserve(headers.size() + 1);
  nva.push_back(make_nv(":status", slot->second.status));
  for (const auto &[name, value] : headers) {
    nva.push_back(make_nv(name, value));
  }
  nghttp2_data_provider2 provider{};
  provider.read_callback = read_response_body;
  const int rc = nghttp2_submit_response2(impl_->session, stream_id,
                                          nva.data(), nva.size(), &provider);
  if (rc != 0) {
    impl_->responses.erase(stream_id);
    return false;
  }
  return true;
}

void H2Engine::reset_stream(std::int32_t stream_id, H2StreamError error) {
  static_cast<void>(nghttp2_submit_rst_stream(
      impl_->session, NGHTTP2_FLAG_NONE, stream_id,
      static_cast<uint32_t>(error)));
}

void H2Engine::submit_goaway() {
  static_cast<void>(nghttp2_submit_goaway(
      impl_->session, NGHTTP2_FLAG_NONE,
      nghttp2_session_get_last_proc_stream_id(impl_->session),
      NGHTTP2_NO_ERROR, nullptr, 0));
}

bool H2Engine::wants_write() const {
  return !impl_->fatal && nghttp2_session_want_write(impl_->session) != 0;
}

bool H2Engine::finished() const {
  return impl_->fatal ||
         (nghttp2_session_want_read(impl_->session) == 0 &&
          nghttp2_session_want_write(impl_->session) == 0);
}

} // namespace hgraph::web::detail
