// The server transport translation unit: the ONLY file that includes
// Boost.Asio/Beast and server-side OpenSSL (RFC 0024, packaging).  It owns
// listener sockets, TLS contexts, connection strands, the compiled route
// tables, and the strict stop ordering.  The HTTP/2 seam is the ALPN
// protocol dispatch below: the h1 branch is the only one wired until
// nghttp2_session.cpp activates (config validation rejects advertising h2).

#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include "detail/h2_engine.h"
#include "detail/route_table.h"
#include "detail/service_transport.h"
#include "detail/web_bindings.h"
#include "detail/stream_model.h"

#include <hgraph/util/scope.h>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <openssl/ssl.h>
#include <openssl/x509.h>


#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <set>
#include <string_view>
#include <thread>
#include <tuple>
#include <typeindex>
#include <utility>
#include <variant>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = asio::ip::tcp;

namespace hgraph::web::detail {
namespace {
// All transport-side value construction goes through WebBindings
// (detail/web_bindings.h): resolved once at start so io threads never touch
// the type registry.

using NamedPairs = WebBindings::NamedPairs;

// ---------------------------------------------------------------------------
// Configuration

struct TlsServerSettings {
  bool enabled{};
  Str cert_path{};
  Str cert_pem{};
  Str key_path{};
  Str key_pem{};
  Str key_password{};
  Str ca_path{};
  Str ca_pem{};
  WebClientVerify client_verify{WebClientVerify::None};
  std::vector<Str> alpn{};
  WebTlsVersion min_version{WebTlsVersion::Tls1_2};
  Value raw{};
};

struct StaticFileConfig {
  Str url{};
  Str file{};
  Str content_type{};
  Str cache_control{};
};

struct StaticDirectoryConfig {
  Str url_prefix{};
  Str directory{};
  Str cache_control{};
};

struct ServerRuntimeConfig {
  // The whole WebServerConfig value: sharing a port requires the FULL
  // configuration to match, not just TLS — the first attachee's listener
  // settings would otherwise silently govern later attachees (review P1).
  Value config_identity{};
  Str bind_address{};
  std::uint16_t port{};
  TlsServerSettings tls{};
  std::size_t io_threads{1};
  std::size_t max_connections{};
  std::size_t max_header_bytes{};
  std::size_t max_body_bytes{};
  std::chrono::milliseconds request_timeout{};
  std::chrono::milliseconds idle_timeout{};
  std::chrono::milliseconds keep_alive_timeout{};
  bool bind_deferred{};
  std::vector<StaticFileConfig> static_files{};
  std::vector<StaticDirectoryConfig> static_directories{};
  OutputLimits ingress{};
  OutputLimits ws_ingress{};
  Int watermark_high_pct{};
  Int watermark_low_pct{};
  WebInboundOverflow inbound_overflow{WebInboundOverflow::Backpressure};
  std::size_t outbound_message_limit{};
  std::size_t outbound_byte_limit{};
  WebSlowConsumerPolicy slow_consumer_policy{WebSlowConsumerPolicy::Close};
  WebFailurePolicy failure_policy{WebFailurePolicy::Report};
  std::chrono::milliseconds shutdown_drain_timeout{};
  std::size_t ws_max_frame_bytes{};
  std::size_t ws_max_message_bytes{};
  std::chrono::milliseconds ping_interval{};
  std::chrono::milliseconds pong_timeout{};
  std::chrono::milliseconds stats_interval{};
  Int h2_max_concurrent_streams{};
  Int h2_initial_window_bytes{};
};

[[nodiscard]] std::size_t positive_size(const ValueView &field,
                                        std::string_view name) {
  const Int value = field.checked_as<Int>();
  if (value <= 0) {
    throw std::invalid_argument("Web server " + std::string{name} +
                                " must be positive");
  }
  return static_cast<std::size_t>(value);
}

[[nodiscard]] std::chrono::milliseconds
milliseconds_field(const ValueView &field, std::string_view name) {
  const Int value = field.checked_as<Int>();
  if (value < 0) {
    throw std::invalid_argument("Web server " + std::string{name} +
                                " cannot be negative");
  }
  return std::chrono::milliseconds{value};
}

void validate_literal_static_path(std::string_view path,
                                  std::string_view what) {
  if (path.empty() || !path.starts_with('/')) {
    throw std::invalid_argument("Web " + std::string{what} +
                                " must start with '/'");
  }
  std::size_t start = 1;
  while (true) {
    const auto slash = path.find('/', start);
    const auto segment =
        path.substr(start, slash == std::string_view::npos
                               ? std::string_view::npos
                               : slash - start);
    if (!segment.empty() &&
        (segment.front() == '*' ||
         segment.find('{') != std::string_view::npos ||
         segment.find('}') != std::string_view::npos)) {
      throw std::invalid_argument(
          "Web " + std::string{what} + " must be exact literal paths");
    }
    if (slash == std::string_view::npos) {
      return;
    }
    start = slash + 1;
  }
}

[[nodiscard]] Str normalize_static_directory_prefix(Str prefix) {
  validate_literal_static_path(prefix, "static directory URL prefixes");
  while (prefix.size() > 1 && prefix.back() == '/') {
    prefix.pop_back();
  }
  return prefix;
}

[[nodiscard]] ServerRuntimeConfig parse_server_config(const Value &value) {
  if (value.schema() != scalar_descriptor<WebServerConfig>::value_meta()) {
    throw std::invalid_argument("Web server requires WebServerConfig");
  }
  const auto root = value.view().as_bundle();

  ServerRuntimeConfig result;
  result.config_identity = value.clone();
  result.bind_address = root.at("bind_address").checked_as<Str>();
  const Int port = root.at("port").checked_as<Int>();
  if (port < 0 || port > 65'535) {
    throw std::invalid_argument("Web server port must be 0..65535");
  }
  result.port = static_cast<std::uint16_t>(port);
  result.io_threads = positive_size(root.at("io_threads"), "io_threads");
  result.max_connections =
      positive_size(root.at("max_connections"), "max_connections");
  result.max_header_bytes =
      positive_size(root.at("max_header_bytes"), "max_header_bytes");
  result.max_body_bytes =
      positive_size(root.at("max_body_bytes"), "max_body_bytes");
  result.request_timeout =
      milliseconds_field(root.at("request_timeout_ms"), "request timeout");
  result.idle_timeout =
      milliseconds_field(root.at("idle_timeout_ms"), "idle timeout");
  result.keep_alive_timeout = milliseconds_field(
      root.at("keep_alive_timeout_ms"), "keep-alive timeout");
  result.bind_deferred = root.at("bind_deferred").checked_as<Bool>();
  for (const auto file : root.at("static_files").as_list()) {
    const auto fields = file.as_bundle();
    StaticFileConfig config{
        fields.at("url").checked_as<Str>(),
        fields.at("file").checked_as<Str>(),
        fields.at("content_type").checked_as<Str>(),
        fields.at("cache_control").checked_as<Str>(),
    };
    validate_literal_static_path(config.url, "static file URLs");
    if (config.file.empty()) {
      throw std::invalid_argument(
          "Web static files require a filesystem path");
    }
    result.static_files.push_back(std::move(config));
  }
  for (const auto directory : root.at("static_directories").as_list()) {
    const auto fields = directory.as_bundle();
    StaticDirectoryConfig config{
        normalize_static_directory_prefix(
            fields.at("url_prefix").checked_as<Str>()),
        fields.at("directory").checked_as<Str>(),
        fields.at("cache_control").checked_as<Str>(),
    };
    if (config.directory.empty()) {
      throw std::invalid_argument(
          "Web static directories require a filesystem path");
    }
    result.static_directories.push_back(std::move(config));
  }
  result.ingress = OutputLimits{
      positive_size(root.at("ingress_record_limit"), "ingress record limit"),
      positive_size(root.at("ingress_byte_limit"), "ingress byte limit"),
  };
  result.ws_ingress = OutputLimits{
      positive_size(root.at("ws_ingress_record_limit"),
                    "WS ingress record limit"),
      positive_size(root.at("ws_ingress_byte_limit"), "WS ingress byte limit"),
  };
  result.watermark_high_pct = root.at("watermark_high_pct").checked_as<Int>();
  result.watermark_low_pct = root.at("watermark_low_pct").checked_as<Int>();
  result.inbound_overflow =
      root.at("inbound_overflow").checked_as<WebInboundOverflow>();
  result.outbound_message_limit = positive_size(
      root.at("outbound_message_limit"), "outbound message limit");
  result.outbound_byte_limit =
      positive_size(root.at("outbound_byte_limit"), "outbound byte limit");
  result.slow_consumer_policy =
      root.at("slow_consumer_policy").checked_as<WebSlowConsumerPolicy>();
  result.failure_policy =
      root.at("failure_policy").checked_as<WebFailurePolicy>();
  result.shutdown_drain_timeout = milliseconds_field(
      root.at("shutdown_drain_timeout_ms"), "shutdown drain timeout");
  result.ws_max_frame_bytes =
      positive_size(root.at("ws_max_frame_bytes"), "WS frame limit");
  result.ws_max_message_bytes =
      positive_size(root.at("ws_max_message_bytes"), "WS message limit");
  result.ping_interval =
      milliseconds_field(root.at("ping_interval_ms"), "ping interval");
  result.pong_timeout =
      milliseconds_field(root.at("pong_timeout_ms"), "pong timeout");
  result.stats_interval =
      milliseconds_field(root.at("stats_interval_ms"), "stats interval");
  result.h2_max_concurrent_streams = static_cast<Int>(positive_size(
      root.at("h2_max_concurrent_streams"), "h2_max_concurrent_streams"));
  result.h2_initial_window_bytes = static_cast<Int>(positive_size(
      root.at("h2_initial_window_bytes"), "h2_initial_window_bytes"));
  if (result.h2_initial_window_bytes > 2'147'483'647 ||
      result.h2_max_concurrent_streams > 2'147'483'647) {
    throw std::invalid_argument(
        "Web server h2 settings must be at most 2^31-1");
  }

  // A single maximal payload must always fit an EMPTY ingress channel:
  // under Backpressure a message that can never fit would otherwise be
  // parked forever with no pauser left to resume it (review P1).  The
  // constants mirror the transport's retained-byte estimates.
  if (result.ingress.bytes <
      result.max_body_bytes + 6 * result.max_header_bytes + 1024) {
    // The implementation's own worst-case weight: initial headers at 2x
    // plus a 4x-weighted target (both inside one max_header_bytes block,
    // worst case 4x) plus trailers at 2x (review P1).
    throw std::invalid_argument(
        "Web server ingress_byte_limit must cover one maximal request "
        "(max_body_bytes + 6*max_header_bytes + 1024)");
  }
  if (result.ws_ingress.bytes < result.ws_max_message_bytes + 256) {
    throw std::invalid_argument(
        "Web server ws_ingress_byte_limit must cover one maximal message "
        "(ws_max_message_bytes + 256)");
  }

  const auto tls = root.at("tls");
  if (tls.data() != nullptr) {
    const auto fields = tls.as_bundle();
    const auto text = [&](std::string_view name) {
      const auto field = fields.at(name);
      return field.data() != nullptr ? field.checked_as<Str>() : Str{};
    };
    result.tls.enabled = true;
    result.tls.cert_path = text("cert_path");
    result.tls.cert_pem = text("cert_pem");
    result.tls.key_path = text("key_path");
    result.tls.key_pem = text("key_pem");
    result.tls.key_password = text("key_password");
    result.tls.ca_path = text("ca_path");
    result.tls.ca_pem = text("ca_pem");
    result.tls.client_verify =
        fields.at("client_verify").checked_as<WebClientVerify>();
    result.tls.min_version =
        fields.at("min_version").checked_as<WebTlsVersion>();
    for (const auto protocol : fields.at("alpn").as_list()) {
      result.tls.alpn.push_back(protocol.checked_as<Str>());
    }
  }
  return result;
}

struct ServerConfigHandle {
  std::shared_ptr<const ServerRuntimeConfig> value{};
};

[[nodiscard]] std::optional<HttpMethod> method_from(http::verb verb) noexcept {
  switch (verb) {
  case http::verb::get:
    return HttpMethod::Get;
  case http::verb::head:
    return HttpMethod::Head;
  case http::verb::post:
    return HttpMethod::Post;
  case http::verb::put:
    return HttpMethod::Put;
  case http::verb::delete_:
    return HttpMethod::Delete;
  case http::verb::patch:
    return HttpMethod::Patch;
  case http::verb::options:
    return HttpMethod::Options;
  case http::verb::trace:
    return HttpMethod::Trace;
  default:
    return std::nullopt;
  }
}

/** The h2 ``:method`` token (always uppercase on the wire). */
[[nodiscard]] std::optional<HttpMethod>
method_from_token(std::string_view token) noexcept {
  if (token == "GET") { return HttpMethod::Get; }
  if (token == "HEAD") { return HttpMethod::Head; }
  if (token == "POST") { return HttpMethod::Post; }
  if (token == "PUT") { return HttpMethod::Put; }
  if (token == "DELETE") { return HttpMethod::Delete; }
  if (token == "PATCH") { return HttpMethod::Patch; }
  if (token == "OPTIONS") { return HttpMethod::Options; }
  if (token == "TRACE") { return HttpMethod::Trace; }
  return std::nullopt;
}

// Query strings stay raw: decoding is a codec-tier decision (RFC 0024).
[[nodiscard]] NamedPairs parse_query(std::string_view query) {
  NamedPairs result;
  while (!query.empty()) {
    const auto amp = query.find('&');
    const auto item = query.substr(0, amp);
    if (!item.empty()) {
      const auto eq = item.find('=');
      if (eq == std::string_view::npos) {
        result.emplace_back(std::string{item}, std::string{});
      } else {
        result.emplace_back(std::string{item.substr(0, eq)},
                            std::string{item.substr(eq + 1)});
      }
    }
    if (amp == std::string_view::npos) {
      break;
    }
    query.remove_prefix(amp + 1);
  }
  return result;
}

class WebServerRuntime;

struct CompiledRoutes {
  RouteTable table{RouteTable::build({})};
  std::vector<Value> routes{};
  std::vector<DateTime> generations{};
};

struct CompiledStaticFiles {
  RouteTable table{RouteTable::build({})};
  std::vector<StaticFileConfig> files{};
  std::vector<StaticDirectoryConfig> directories{};
};

struct MatchedRoute {
  std::shared_ptr<WebServerRuntime> runtime{};
  // The route is SHARED, not cloned: the snapshot pin keeps the pointed-at
  // value alive across concurrent route swaps, so a backpressured request
  // holds O(1) route memory instead of an owning copy per request
  // (review P1).  The single owning copy is the envelope clone at push,
  // which the retained estimate accounts via route_weight().
  std::shared_ptr<const CompiledRoutes> snapshot{};
  const Value *route{};
  DateTime generation{MIN_ST};
  // Capture NAMES are views into the snapshot's route table; values are
  // owned (request-derived).  Materialized into owning pairs only after
  // admission (review P1).
  std::vector<std::pair<std::string_view, std::string>> params{};
};

struct MatchedStaticFile {
  const StaticFileConfig *file{};
  const StaticDirectoryConfig *directory{};
  std::string path{};
};

[[nodiscard]] WebBindings::NamedPairs
materialize_params(
    const std::vector<std::pair<std::string_view, std::string>> &params) {
  WebBindings::NamedPairs owned;
  owned.reserve(params.size());
  for (const auto &[name, value] : params) {
    owned.emplace_back(std::string{name}, value);
  }
  return owned;
}

/** The bytes the envelope's owning route copy retains (pattern + fixed
 * fields); route patterns are server-defined but can be long relative to
 * tiny requests, so they are accounted, not assumed (review P1). */
[[nodiscard]] std::size_t route_weight(const Value &route) {
  return route.view().as_bundle().at("pattern").checked_as<Str>().size() + 64;
}

[[nodiscard]] CompiledStaticFiles
compile_static_files(const std::vector<StaticFileConfig> &files,
                     const std::vector<StaticDirectoryConfig> &directories) {
  CompiledStaticFiles compiled;
  std::vector<RouteTable::Entry> entries;
  entries.reserve(files.size());
  compiled.files = files;
  for (const StaticFileConfig &file : compiled.files) {
    entries.push_back(RouteTable::Entry{HttpMethod::Get, std::string{file.url}});
  }
  compiled.directories = directories;
  std::sort(compiled.directories.begin(), compiled.directories.end(),
            [](const StaticDirectoryConfig &lhs,
               const StaticDirectoryConfig &rhs) {
              return lhs.url_prefix.size() > rhs.url_prefix.size();
            });
  compiled.table = RouteTable::build(std::move(entries));
  return compiled;
}

struct StaticFileTarget {
  std::string file{};
  std::string content_type{};
  std::string cache_control{};
};

[[nodiscard]] std::string
infer_content_type(const StaticFileTarget &file) {
  if (!file.content_type.empty()) {
    return file.content_type;
  }
  const auto extension = std::filesystem::path{std::string{file.file}}
                             .extension()
                             .string();
  std::string lowered;
  lowered.reserve(extension.size());
  for (const unsigned char character : extension) {
    lowered.push_back(
        static_cast<char>(std::tolower(character)));
  }
  if (lowered == ".ico") return "image/x-icon";
  if (lowered == ".svg") return "image/svg+xml";
  if (lowered == ".html" || lowered == ".htm") return "text/html; charset=utf-8";
  if (lowered == ".css") return "text/css; charset=utf-8";
  if (lowered == ".js" || lowered == ".mjs") return "text/javascript; charset=utf-8";
  if (lowered == ".json") return "application/json";
  if (lowered == ".txt") return "text/plain; charset=utf-8";
  if (lowered == ".png") return "image/png";
  if (lowered == ".jpg" || lowered == ".jpeg") return "image/jpeg";
  if (lowered == ".gif") return "image/gif";
  if (lowered == ".webp") return "image/webp";
  if (lowered == ".wasm") return "application/wasm";
  return "application/octet-stream";
}

struct ResolvedStaticFile {
  http::status status{http::status::ok};
  std::string file{};
  std::size_t size{};
  std::string content_type{"text/plain"};
  std::string cache_control{};
  std::string body{};
};

[[nodiscard]] ResolvedStaticFile
static_text_response(http::status status, std::string body) {
  return ResolvedStaticFile{status, "", 0, "text/plain", "", std::move(body)};
}

[[nodiscard]] ResolvedStaticFile
resolve_static_file(const StaticFileTarget &file) {
  namespace fs = std::filesystem;

  const fs::path path{std::string{file.file}};
  std::error_code ec;
  const auto status = fs::status(path, ec);
  if (ec || !fs::exists(status) || !fs::is_regular_file(status)) {
    return static_text_response(http::status::not_found, "no such file");
  }
  const auto size = fs::file_size(path, ec);
  if (ec) {
    return static_text_response(http::status::internal_server_error,
                                "static file error");
  }
  return {http::status::ok, path.string(), static_cast<std::size_t>(size),
          infer_content_type(file), std::string{file.cache_control}, ""};
}

[[nodiscard]] std::optional<std::string_view>
static_directory_relative_path(std::string_view prefix,
                               std::string_view path) {
  if (prefix == "/") {
    if (path.size() <= 1 || !path.starts_with('/')) {
      return std::nullopt;
    }
    return path.substr(1);
  }
  if (path == prefix || !path.starts_with(prefix) ||
      path.size() <= prefix.size() || path[prefix.size()] != '/') {
    return std::nullopt;
  }
  return path.substr(prefix.size() + 1);
}

[[nodiscard]] bool path_within_root(const std::filesystem::path &root,
                                    const std::filesystem::path &path) {
  auto root_it = root.begin();
  auto path_it = path.begin();
  for (; root_it != root.end(); ++root_it, ++path_it) {
    if (path_it == path.end() || *root_it != *path_it) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] ResolvedStaticFile
load_static_directory_file(const StaticDirectoryConfig &directory,
                           std::string_view request_path) {
  namespace fs = std::filesystem;

  const auto relative = static_directory_relative_path(directory.url_prefix,
                                                       request_path);
  if (!relative.has_value() || relative->empty()) {
    return static_text_response(http::status::not_found, "no such file");
  }

  const fs::path relative_path{std::string{*relative}};
  if (relative_path.is_absolute()) {
    return static_text_response(http::status::not_found, "no such file");
  }
  for (const auto &part : relative_path) {
    if (part == "." || part == "..") {
      return static_text_response(http::status::not_found, "no such file");
    }
  }

  const fs::path root{std::string{directory.directory}};
  std::error_code ec;
  const auto root_status = fs::status(root, ec);
  if (ec || !fs::exists(root_status) || !fs::is_directory(root_status)) {
    return static_text_response(http::status::not_found, "no such file");
  }

  const fs::path target = root / relative_path;
  const auto target_status = fs::status(target, ec);
  if (ec || !fs::exists(target_status) || !fs::is_regular_file(target_status)) {
    return static_text_response(http::status::not_found, "no such file");
  }

  const auto canonical_root = fs::weakly_canonical(root, ec);
  if (ec) {
    return static_text_response(http::status::internal_server_error,
                                "static file error");
  }
  const auto canonical_target = fs::weakly_canonical(target, ec);
  if (ec || !path_within_root(canonical_root, canonical_target)) {
    return static_text_response(http::status::not_found, "no such file");
  }

  return resolve_static_file(
      StaticFileTarget{canonical_target.string(), "",
                       std::string{directory.cache_control}});
}

// ---------------------------------------------------------------------------
// Listener: owns the io_context pool and acceptor for one (address, port).
// Several server runtimes may attach (RFC 0024, routing/port sharing); the
// first to start binds, later starts attach after a full-config identity
// check, and the last detach closes the listener.

class PendingTarget;

class WebListener : public std::enable_shared_from_this<WebListener> {
public:
  WebListener(std::string address, std::uint16_t port, Value config_identity,
              std::size_t io_threads, CompiledStaticFiles static_files)
      : address_{std::move(address)}, port_{port},
        config_identity_{std::move(config_identity)}, io_threads_{io_threads},
        acceptor_{io_context_}, static_files_{std::move(static_files)} {}

  ~WebListener() { stop_io(); }

  [[nodiscard]] asio::io_context &io_context() noexcept { return io_context_; }

  [[nodiscard]] std::uint16_t bound_port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
  }

  [[nodiscard]] const Value &config_identity() const noexcept {
    return config_identity_;
  }

  void attach(std::shared_ptr<WebServerRuntime> runtime) {
    std::lock_guard lock{mutex_};
    runtimes_.push_back(std::move(runtime));
  }

  /** Every live connection registers here so a stopping runtime can
   * retire ITS work everywhere — streams still blocked before dispatch
   * included — and so the LAST detaching runtime can shut every
   * connection down, idle ones included (review P1). */
  void register_connection(std::weak_ptr<PendingTarget> connection) {
    std::lock_guard lock{mutex_};
    std::erase_if(live_connections_,
                  [](const auto &weak) { return weak.expired(); });
    live_connections_.push_back(std::move(connection));
  }

  void retire_runtime_connections(const WebServerRuntime *runtime,
                                  std::shared_ptr<const void> barrier);
  void shutdown_connections();

  /** @return true when this was the last attached runtime. */
  bool detach(const WebServerRuntime *runtime) {
    std::lock_guard lock{mutex_};
    std::erase_if(runtimes_, [runtime](const auto &attached) {
      return attached.get() == runtime;
    });
    return runtimes_.empty();
  }

  void ensure_listening();
  void start_io();
  void stop_accepting();
  void stop_io();

  [[nodiscard]] std::optional<MatchedRoute>
  match(HttpMethod method, std::string_view path, bool upgrade);

  [[nodiscard]] std::optional<MatchedStaticFile>
  match_static(HttpMethod method, std::string_view path) const;

  /** Cross-attachee duplicate detection (RFC 0024, port sharing): an
   * identical (method, pattern) served by two runtimes would otherwise be
   * resolved by attach order, so it is rejected at route-apply time. */
  void check_route_conflicts(const WebServerRuntime *applying, bool upgrade,
                             const std::vector<Value> &added);

  void connection_opened() noexcept { ++open_connections_; }
  void connection_closed() noexcept { --open_connections_; }
  [[nodiscard]] std::size_t open_connections() const noexcept {
    return open_connections_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] bool at_connection_limit(std::size_t limit) const noexcept {
    return open_connections_.load(std::memory_order_relaxed) >= limit;
  }

  // Read pausing is tiered (HTTP requests vs WS ingress) and token-counted:
  // several attached runtimes may pause independently, and reads resume only
  // when EVERY pauser on that tier has dropped below its low watermark —
  // last-writer-wins would reopen the memory-growth path (review P1).
  enum class ReadTier : std::size_t { Http = 0, Ws = 1, Count };
  void set_reads_paused(ReadTier tier, const void *token, bool paused);
  [[nodiscard]] bool reads_paused(ReadTier tier) const noexcept {
    return pause_counts_[static_cast<std::size_t>(tier)].load(
               std::memory_order_acquire) != 0;
  }
  void park_for_resume(ReadTier tier, std::function<void()> resume);

  [[nodiscard]] std::shared_ptr<WebServerRuntime> owner() {
    std::lock_guard lock{mutex_};
    return runtimes_.empty() ? nullptr : runtimes_.front();
  }

private:
  void accept_next();

  friend class ListenerRegistry;

  std::string address_{};
  std::uint16_t port_{};
  Value config_identity_{};
  std::size_t io_threads_{1};
  asio::io_context io_context_{};
  tcp::acceptor acceptor_;
  std::optional<asio::executor_work_guard<asio::io_context::executor_type>>
      work_{};
  std::vector<std::thread> threads_{};
  std::atomic<std::uint16_t> bound_port_{0};
  std::atomic<bool> accepting_{false};
  std::atomic<bool> listening_{false};
  std::atomic<std::size_t> open_connections_{0};
  std::array<std::atomic<std::size_t>, 2> pause_counts_{};
  std::mutex mutex_{};
  // Owning (P1): a connection's asynchronous callbacks may outlive one
  // attachee's stop when the peer stalls its close handshake, so runtimes
  // stay alive until every reference (match results, WS connections, queued
  // frames) has drained.  detach() drops this reference at stop, breaking
  // the runtime->listener->runtime cycle.
  std::vector<std::shared_ptr<WebServerRuntime>> runtimes_{};
  std::vector<std::weak_ptr<PendingTarget>> live_connections_{};
  std::array<std::set<const void *>, 2> pause_tokens_{};
  std::array<std::vector<std::function<void()>>, 2> parked_{};
  CompiledStaticFiles static_files_{};
};

class ListenerRegistry {
public:
  static ListenerRegistry &instance() {
    static ListenerRegistry registry;
    return registry;
  }

  [[nodiscard]] std::shared_ptr<WebListener>
  acquire(const std::string &address, std::uint16_t port, const Value &config,
          std::size_t io_threads, std::shared_ptr<WebServerRuntime> runtime,
          const std::vector<StaticFileConfig> &static_files,
          const std::vector<StaticDirectoryConfig> &static_directories) {
    std::lock_guard lock{mutex_};
    const auto key = std::make_pair(address, port);
    if (port != 0) {
      if (const auto found = listeners_.find(key); found != listeners_.end()) {
        if (auto existing = found->second.lock()) {
          // The first attachee's listener settings (io threads, limits,
          // timeouts, TLS) govern the shared socket, so every later
          // attachee must present the identical configuration.
          if (!existing->config_identity().view().equals(config.view())) {
            throw std::invalid_argument(
                "Web servers sharing " + address + ":" +
                std::to_string(port) +
                " must use an identical WebServerConfig");
          }
          existing->attach(std::move(runtime));
          return existing;
        }
      }
    }
    auto listener = std::make_shared<WebListener>(
        address, port, config.clone(), io_threads,
        compile_static_files(static_files, static_directories));
    listener->attach(std::move(runtime));
    if (port != 0) {
      listeners_[key] = listener;
    }
    return listener;
  }

  /** Detach; when this was the last attachee, accepting stops here and the
   * caller runs the drain before stopping the io pool itself — stopping io
   * first would strand the posted shutdown work (RFC 0024, lifecycle). */
  [[nodiscard]] bool release(const std::shared_ptr<WebListener> &listener,
                             WebServerRuntime *runtime) {
    bool last = false;
    {
      std::lock_guard lock{mutex_};
      last = listener->detach(runtime);
      if (last && listener->port_ != 0) {
        listeners_.erase(std::make_pair(listener->address_, listener->port_));
      }
    }
    if (last) {
      listener->stop_accepting();
    }
    return last;
  }

private:
  std::mutex mutex_{};
  std::map<std::pair<std::string, std::uint16_t>,
           std::weak_ptr<WebListener>>
      listeners_{};
};

// ---------------------------------------------------------------------------
// Runtime

// Lock-free published-snapshot access for io threads. Prefer the C++20
// specialization where the standard library provides it. Older Apple libc++
// releases expose only the free shared_ptr atomics, which remain the fallback.
#if defined(__cpp_lib_atomic_shared_ptr) && __cpp_lib_atomic_shared_ptr >= 201711L
using PublishedRoutes = std::atomic<std::shared_ptr<const CompiledRoutes>>;

[[nodiscard]] inline std::shared_ptr<const CompiledRoutes>
atomic_load_routes(const PublishedRoutes *slot) {
  return slot->load(std::memory_order_acquire);
}

inline void atomic_store_routes(PublishedRoutes *slot,
                                std::shared_ptr<const CompiledRoutes> value) {
  slot->store(std::move(value), std::memory_order_release);
}
#else
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
using PublishedRoutes = std::shared_ptr<const CompiledRoutes>;

[[nodiscard]] inline std::shared_ptr<const CompiledRoutes>
atomic_load_routes(const PublishedRoutes *slot) {
  return std::atomic_load(slot);
}

inline void atomic_store_routes(PublishedRoutes *slot,
                                std::shared_ptr<const CompiledRoutes> value) {
  std::atomic_store(slot, std::move(value));
}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#endif

class ServerConnection;
class WebServerRuntime;

// Request and connection ids are unique across every runtime in the
// process: one h2 connection on a shared listener can carry streams from
// several runtimes in a single map, so per-runtime counters could collide
// (review P1).
inline std::atomic<Int> process_request_ids{0};

/** The respond surface a pending request routes back to: the h1
 * connection or the h2 stream driver that owns the transport side of the
 * request (RFC 0024, HTTP/2 activation plan). */
class PendingTarget {
public:
  virtual ~PendingTarget() = default;
  virtual void deliver_response(Int request_id, Value response, Int client_id,
                                std::shared_ptr<WebServerRuntime> runtime) = 0;
  virtual void answer_timeout(Int request_id) = 0;
  /** One runtime is stopping: retire only ITS work.  A shared h2
   * connection keeps serving the other attachees' streams (review P1).
   * The barrier is held through every handler the retirement schedules,
   * so the stopping runtime can await ITS completion — without waiting
   * on other attachees' connections — before releasing its admission
   * budget. */
  virtual void retire_runtime(const WebServerRuntime *runtime,
                              std::shared_ptr<const void> barrier) = 0;
  /** The listener itself is shutting down (last attachee): close the
   * whole connection — GOAWAY for h2, close for h1 — idle or not. */
  virtual void connection_shutdown() = 0;
};

class WebServerRuntime
    : public std::enable_shared_from_this<WebServerRuntime> {
public:
  WebServerRuntime(std::shared_ptr<const ServerRuntimeConfig> config, Str path,
                   ServerAdmissionHandle admission,
                   WebTransportBindingsHandle transport_bindings,
                   ServerTransportOutput::Senders senders)
      : config_{std::move(config)}, path_{std::move(path)},
        admission_{std::move(admission)},
        output_{std::move(senders), admission_,
                std::move(transport_bindings)} {}

  ~WebServerRuntime() { stop(); }

  void start();
  void stop() noexcept;

  void apply_http_routes(std::vector<SubscriptionBinding> added,
                         std::vector<Value> removed);
  void apply_ws_routes(std::vector<SubscriptionBinding> added,
                       std::vector<Value> removed);
  void respond(Int client_id, Int request_id, Value response);
  void ws_send(Int client_id, Int connection_id, Value frame);

  // --- transport-side entry points (io threads) ---
  [[nodiscard]] const ServerRuntimeConfig &config() const noexcept {
    return *config_;
  }
  [[nodiscard]] std::shared_ptr<const ServerRuntimeConfig>
  config_ptr() const noexcept {
    return config_;
  }
  [[nodiscard]] const WebBindings &bindings() const noexcept {
    return bindings_;
  }
  [[nodiscard]] const Str &path() const noexcept { return path_; }
  [[nodiscard]] asio::ssl::context *tls_context() noexcept {
    return tls_context_ ? tls_context_.get() : nullptr;
  }
  [[nodiscard]] std::shared_ptr<const CompiledRoutes> http_routes() const {
    return atomic_load_routes(&http_routes_);
  }
  [[nodiscard]] std::shared_ptr<const CompiledRoutes> ws_routes() const {
    return atomic_load_routes(&ws_routes_);
  }

  [[nodiscard]] Int register_pending(const std::shared_ptr<PendingTarget> &target);
  void unregister_pending(Int request_id) noexcept;
  [[nodiscard]] Int register_ws_connection(
      const std::shared_ptr<ServerConnection> &connection);
  void unregister_ws_connection(Int connection_id) noexcept;

  [[nodiscard]] bool push_request_reserved(Value route, DateTime generation,
                                           Value request,
                                           std::size_t retained_bytes,
                                           std::size_t reserved_bytes);
  [[nodiscard]] bool push_ws_event(Value route, DateTime generation, Value event,
                                   std::size_t retained_bytes);
  [[nodiscard]] bool push_ws_event_reserved(Value route, DateTime generation,
                                            Value event,
                                            std::size_t retained_bytes,
                                            std::size_t reserved_bytes);
  [[nodiscard]] bool push_ws_frame_reserved(Value route, DateTime generation,
                                            Value inbound_frame,
                                            std::size_t retained_bytes,
                                            std::size_t reserved_bytes);
  [[nodiscard]] Value delivery_report(Int request_id, WebDeliveryStatus status,
                                      Int error_code = 0,
                                      const Str &message = {});
  void report(std::size_t channel, Int client_id, Value report_value);
  void emit_event(WebSeverity severity, Str component, Str category,
                  Str message, Int error_code = 0, bool retriable = false,
                  bool fatal = false, Int connection_id = 0);
  void count_drop() noexcept { ++dropped_; }

  [[nodiscard]] std::shared_ptr<WebListener> listener() const noexcept {
    return listener_;
  }

  // Ingress admission (review P1): a connection reserves its projected
  // retained bytes in the admission budget BEFORE reading the request body or arming
  // a WebSocket message read, so concurrent connections cannot each hold a
  // full payload while the graph transport is full — the excess stays unread
  // in the kernel (RFC 0024, flow control). The budget reservation covers the
  // record and byte limits under one lock, and the standard push-source queue
  // has matching aggregate capacity, so an admitted payload needs no retry or
  // second queue.
  [[nodiscard]] bool reserve_request(std::size_t bytes) {
    return admission_.value->reserve(index(ServerChannel::Request), bytes);
  }
  [[nodiscard]] bool grow_request_reservation(std::size_t bytes) {
    return admission_.value->grow_reservation(index(ServerChannel::Request),
                                              bytes);
  }
  [[nodiscard]] Int allocate_connection_id() noexcept {
    return ++process_request_ids;
  }
  void release_request_reservation(std::size_t bytes) noexcept {
    admission_.value->release_reservation(index(ServerChannel::Request),
                                          bytes);
  }
  [[nodiscard]] bool reserve_ws_ingress(std::size_t bytes) {
    return admission_.value->reserve(index(ServerChannel::WsIngress), bytes);
  }
  [[nodiscard]] bool grow_ws_reservation(std::size_t bytes) {
    return admission_.value->grow_reservation(index(ServerChannel::WsIngress),
                                              bytes);
  }
  void release_ws_reservation(std::size_t bytes) noexcept {
    admission_.value->release_reservation(index(ServerChannel::WsIngress),
                                          bytes);
  }

private:
  void sweep_expired_requests();
  void arm_sweep_timer();
  void arm_stats_timer();
  void emit_stats_once();
  void rebuild(PublishedRoutes &slot,
               std::vector<std::tuple<HttpMethod, std::string, Value, DateTime>>
                   &master,
               std::vector<SubscriptionBinding> added,
               std::vector<Value> removed);

  friend class ServerConnection;

  std::shared_ptr<const ServerRuntimeConfig> config_{};
  WebBindings bindings_{};
  Str path_{};
  ServerAdmissionHandle admission_{};
  ServerTransportOutput output_;
  bool started_{};
  std::shared_ptr<WebListener> listener_{};
  std::unique_ptr<asio::ssl::context> tls_context_{};
  // The wire-format ALPN preference list the select callback reads; owned
  // here so its address outlives every TLS handshake on this context.
  std::vector<unsigned char> alpn_wire_{};
  PublishedRoutes http_routes_{
      std::make_shared<CompiledRoutes>()};
  PublishedRoutes ws_routes_{
      std::make_shared<CompiledRoutes>()};
  std::mutex routes_mutex_{};
  std::vector<std::tuple<HttpMethod, std::string, Value, DateTime>>
      http_master_{};
  std::vector<std::tuple<HttpMethod, std::string, Value, DateTime>> ws_master_{};

  std::mutex pending_mutex_{};
  struct Pending {
    // Owning: between dispatch and the graph's answer no async operation
    // holds the transport target, so the pending registry keeps it alive
    // until it is answered, timed out, or shut down.
    std::shared_ptr<PendingTarget> target{};
    std::chrono::steady_clock::time_point deadline{};
  };
  std::map<Int, Pending> pending_{};
  std::map<Int, std::weak_ptr<ServerConnection>> ws_connections_{};
  std::shared_ptr<asio::steady_timer> sweep_timer_{};
  std::shared_ptr<asio::steady_timer> stats_timer_{};
  std::atomic<Int> sequence_{0};
  std::atomic<Int> dropped_{0};
  std::atomic<std::size_t> in_flight_ingress_{0};
  std::atomic<bool> stopping_{false};
};

// ---------------------------------------------------------------------------
// Connection: one strand per socket; plain h1 and WebSocket.  The eval
// thread never touches a socket — respond/ws_send post owned data onto the
// strand (RFC 0024, threading).

using ServerTlsStream = asio::ssl::stream<beast::tcp_stream>;

class ServerConnection : public std::enable_shared_from_this<ServerConnection>,
                         public PendingTarget {
public:
  ServerConnection(std::shared_ptr<WebListener> listener,
                   std::shared_ptr<const ServerRuntimeConfig> config,
                   tcp::socket socket, asio::ssl::context *tls)
      : listener_{std::move(listener)}, config_{std::move(config)},
        strand_{asio::make_strand(listener_->io_context())} {
    if (tls != nullptr) {
      tls_stream_.emplace(std::move(socket), *tls);
    } else {
      plain_stream_.emplace(std::move(socket));
    }
    listener_->connection_opened();
  }

  ~ServerConnection() {
    release_admission();
    release_ws_reservation_held();
    release_ws_terminal_reservation();
    listener_->connection_closed();
  }

  void run() {
    listener_->register_connection(
        std::weak_ptr<PendingTarget>{shared_from_this()});
    asio::post(strand_, [self = shared_from_this()] { self->handshake(); });
  }

  void deliver_response(Int request_id, Value response, Int client_id,
                        std::shared_ptr<WebServerRuntime> runtime) override {
    asio::post(strand_, [self = shared_from_this(), request_id,
                         response = std::move(response), client_id, runtime] {
      self->write_response(request_id, response, client_id, runtime);
    });
  }

  void deliver_ws_frame(Value frame, Int client_id,
                        std::shared_ptr<WebServerRuntime> runtime) {
    asio::post(strand_, [self = shared_from_this(), frame = std::move(frame),
                         client_id, runtime] {
      self->queue_ws_frame(frame, client_id, runtime);
    });
  }

  void answer_timeout(Int request_id) override {
    asio::post(strand_, [self = shared_from_this(), request_id] {
      if (self->pending_request_id_ == request_id && !self->writing_) {
        self->send_simple_response(http::status::service_unavailable,
                                   "the graph did not answer in time", false);
        self->pending_request_id_ = -1;
      }
    });
  }

  void retire_runtime(const WebServerRuntime *runtime,
                      std::shared_ptr<const void> barrier) override {
    asio::post(strand_, [self = shared_from_this(), runtime, barrier] {
      // Close only when the stopping runtime owns this connection's
      // CURRENT activity — from route match (admission wait, body read,
      // WS handshake) through the answered response; an idle keep-alive
      // or a connection serving another attachee keeps serving.  The
      // shutdown runs INLINE on this same handler: a second post would
      // let a queued completion return the connection to idle (or to
      // another runtime's next request) between check and close
      // (review P1).
      if (self->serving_runtime_ == runtime) {
        // The barrier must survive the ASYNC tail — the 503 write, the
        // WS close handshake, an already-running response — all of which
        // terminate in close(); holding it as a member until then lets
        // the stopping runtime await true completion (review P1).
        self->retire_barrier_ = std::move(barrier);
        self->shutdown_now();
      }
    });
  }

  void connection_shutdown() override {
    asio::post(strand_,
               [self = shared_from_this()] { self->shutdown_now(); });
  }

  // Must run on the strand.
  void shutdown_now() {
    shutting_down_ = true;
    if (ws_) {
      ws_close(websocket::close_code{1001}, "going away",
               WsConnectionState::Closed);
    } else if (pending_request_id_ >= 0 && !writing_) {
      send_simple_response(http::status::service_unavailable,
                           "server shutting down", false);
      pending_request_id_ = -1;
    } else if (!writing_) {
      close();
    }
  }

  void resume_reading() {
    asio::post(strand_, [self = shared_from_this()] {
      if (self->read_parked_) {
        self->read_parked_ = false;
        self->read_next();
      }
    });
  }

private:
  using PlainStream = beast::tcp_stream;
  using TlsStream = ServerTlsStream;
  using PlainWs = websocket::stream<beast::tcp_stream>;
  using TlsWs = websocket::stream<TlsStream>;

  void handshake() {
    if (tls_stream_.has_value()) {
      beast::get_lowest_layer(*tls_stream_)
          .expires_after(config_->idle_timeout);
      tls_stream_->async_handshake(
          asio::ssl::stream_base::server,
          asio::bind_executor(
              strand_, [self = shared_from_this()](beast::error_code ec) {
                if (ec) {
                  self->close();
                  return;
                }
                // The ALPN seam (RFC 0024, activation plan): "h2"
                // hands the socket to the HTTP/2 driver; everything
                // else stays on the h1 path.
                if (self->negotiated_h2()) {
                  self->start_h2();
                  return;
                }
                self->read_next();
              }));
    } else {
      read_next();
    }
  }


  [[nodiscard]] bool negotiated_h2() const {
    const unsigned char *protocol = nullptr;
    unsigned int length = 0;
    SSL_get0_alpn_selected(
        const_cast<TlsStream &>(*tls_stream_).native_handle(), &protocol,
        &length);
    return length == 2 && std::memcmp(protocol, "h2", 2) == 0;
  }
  void start_h2();

  void read_next();
  void on_headers();
  void respond_after_headers(http::status status, std::string_view body);
  void serve_static_after_headers(const MatchedStaticFile &file,
                                  bool suppress_body);
  void admit_and_read_body(MatchedRoute matched);
  void read_body(MatchedRoute matched);
  void release_admission() noexcept;
  void dispatch_http(MatchedRoute matched, bool keep_alive);
  void accept_ws(MatchedRoute matched);
  void ws_read_next();
  void ws_read_continue();
  void ws_account_chunk();
  void ws_continue_message();
  void deliver_ws_ingress(Value inbound, std::size_t bytes);
  void release_ws_reservation_held() noexcept;
  void write_response(Int request_id, const Value &response, Int client_id,
                      const std::shared_ptr<WebServerRuntime> &runtime);
  void finish_response(beast::error_code ec, Int client_id, Int request_id,
                       const std::shared_ptr<WebServerRuntime> &runtime,
                       bool keep_alive);
  void queue_ws_frame(const Value &frame, Int client_id,
                      const std::shared_ptr<WebServerRuntime> &runtime);
  void ws_write_next();
  void ws_send_fragment();
  void ws_close(websocket::close_code code, beast::string_view reason,
                WsConnectionState terminal_state);
  void finish_ws(WsConnectionState state, Int close_code, Str close_reason);
  void release_ws_terminal_reservation() noexcept;
  void send_buffer_response(http::status status, std::string body,
                            bool keep_alive, std::string_view content_type,
                            std::string_view cache_control,
                            std::optional<std::size_t> content_length = std::nullopt);
  void send_file_response(http::status status, std::string path,
                          std::size_t content_length, bool keep_alive,
                          std::string_view content_type,
                          std::string_view cache_control);
  void send_simple_response(http::status status, std::string_view body,
                            bool keep_alive);
  void close();
  void close_socket_only();
  [[nodiscard]] Value build_server_request(const MatchedRoute &matched,
                                           Int request_id,
                                           std::size_t &retained_bytes);
  [[nodiscard]] Value peer_value(const WebBindings &b);

  std::shared_ptr<WebListener> listener_;
  // Captured at accept: the owning runtime may detach from the listener
  // while this connection's handlers are still draining, so socket-level
  // settings are never fetched through the (possibly empty) attach list.
  std::shared_ptr<const ServerRuntimeConfig> config_;
  asio::strand<asio::io_context::executor_type> strand_;
  std::optional<PlainStream> plain_stream_{};
  std::optional<TlsStream> tls_stream_{};
  std::optional<PlainWs> plain_ws_{};
  std::optional<TlsWs> tls_ws_{};
  beast::flat_buffer buffer_{};
  std::optional<http::request_parser<http::string_body>> parser_{};
  http::request<http::string_body> request_{};
  bool ws_{};
  bool writing_{};
  bool shutting_down_{};
  bool read_parked_{};
  bool request_keep_alive_{};
  bool pending_is_head_{};
  std::string decoded_path_{};
  std::size_t admitted_bytes_{};
  std::shared_ptr<WebServerRuntime> admitted_runtime_{};
  // The runtime this connection is working FOR, tracked from route match
  // (admission wait, body read, WS handshake) through completion, so a
  // stopping runtime can retire pre-dispatch work too (review P1).
  const WebServerRuntime *serving_runtime_{};
  std::shared_ptr<const void> retire_barrier_{};
  std::size_t ws_reserved_bytes_{};
  std::size_t ws_unaccounted_bytes_{};
  // Capacity for THIS connection's terminal Closed/Failed event, reserved
  // at accept and held for the socket's lifetime (review P1).
  std::size_t ws_close_reserved_{};
  bool ws_close_started_{};
  bool ws_terminal_emitted_{};
  // Message bytes accumulate here — storage the reservation accounts —
  // while ws_read_buffer_ stays chunk-sized: Beast's flat_buffer keeps its
  // allocated capacity after consume(), so assembling messages inside it
  // would let every idle connection retain its largest-ever message
  // outside admission accounting (review P1).
  std::string ws_message_{};
  Int pending_request_id_{-1};
  Int ws_connection_id_{-1};
  std::shared_ptr<WebServerRuntime> ws_runtime_{};
  // Shared route for the WebSocket lifetime: the snapshot pin replaces a
  // per-connection owning clone (review P1); envelope clones are per-push
  // and accounted via ws_route_weight_.
  std::shared_ptr<const CompiledRoutes> ws_route_snapshot_{};
  const Value *ws_route_{};
  DateTime ws_generation_{MIN_ST};
  std::size_t ws_route_weight_{};
  std::optional<http::response<http::string_body>> outgoing_{};
  std::optional<http::response<http::file_body>> outgoing_file_{};
  std::optional<http::response<http::empty_body>> chunk_head_{};
  std::optional<http::response_serializer<http::empty_body>> chunk_serializer_{};
  std::optional<http::response_serializer<http::file_body>>
      outgoing_file_serializer_{};
  std::string chunk_body_{};
  http::fields chunk_trailers_{};
  struct QueuedWsFrame {
    Value frame{};
    Int client_id{};
    std::shared_ptr<WebServerRuntime> runtime{};
    std::size_t bytes{};
  };
  std::deque<QueuedWsFrame> ws_outbound_{};
  std::size_t ws_outbound_bytes_{};
  std::string ws_send_buffer_{};
  std::size_t ws_send_offset_{};
  beast::flat_buffer ws_read_buffer_{};
};

// ---------------------------------------------------------------------------
// HTTP/2 connection driver (RFC 0024, HTTP/2 activation plan).
//
// Owns the TLS socket after ALPN selects "h2" and pumps bytes through the
// pure H2Engine; every protocol event arrives via H2Host on this strand.
// Requests flow through the SAME admission contract as h1: header admission
// reserves on the matched runtime before any DATA window is released, DATA
// chunks grow the reservation before the engine may consume them, and the
// reserved send transfers everything to the standard push-source value.

class H2Driver final : public std::enable_shared_from_this<H2Driver>,
                       public PendingTarget,
                       private H2Host {
public:
  H2Driver(std::shared_ptr<WebListener> listener,
           std::shared_ptr<const ServerRuntimeConfig> config,
           ServerTlsStream stream)
      : listener_{std::move(listener)}, config_{std::move(config)},
        strand_{asio::make_strand(listener_->io_context())},
        stream_{std::move(stream)},
        engine_{*this,
                H2Settings{
                    static_cast<std::size_t>(
                        config_->h2_max_concurrent_streams),
                    static_cast<std::size_t>(config_->h2_initial_window_bytes),
                    config_->max_header_bytes,
                }} {
    listener_->connection_opened();
  }

  ~H2Driver() override { listener_->connection_closed(); }

  void run() {
    listener_->register_connection(
        std::weak_ptr<PendingTarget>{shared_from_this()});
    asio::post(strand_, [self = shared_from_this()] {
      self->pump_writes(); // the server SETTINGS/window preface
      self->read_next();
    });
  }

  // --- PendingTarget ---
  void deliver_response(Int request_id, Value response, Int client_id,
                        std::shared_ptr<WebServerRuntime> runtime) override {
    asio::post(strand_, [self = shared_from_this(), request_id,
                         response = std::move(response), client_id,
                         runtime = std::move(runtime)] {
      self->write_stream_response(request_id, response, client_id, runtime);
    });
  }

  void answer_timeout(Int request_id) override {
    asio::post(strand_, [self = shared_from_this(), request_id] {
      self->finish_stream_transport(request_id, 503,
                                    "the graph did not answer in time");
    });
  }

  void retire_runtime(const WebServerRuntime *runtime,
                      std::shared_ptr<const void> barrier) override {
    asio::post(strand_, [self = shared_from_this(), runtime, barrier] {
      if (self->closed_) {
        return;
      }
      // Stamp FIRST, respond after: a small 503 or RST can reach
      // on_stream_closed inside the very pump that sends it, erasing the
      // stream — stamping afterwards would let the retirement token
      // release while that socket write is still outstanding (review P1).
      // Delivery reports already queued for the retiring runtime release
      // the barrier only when their carrying write completes.
      for (auto &report : self->pending_flush_reports_) {
        if (report.runtime.get() == runtime) {
          report.barrier = barrier;
        }
      }
      std::vector<std::int32_t> retiring;
      for (auto &[stream_id, stream] : self->streams_) {
        if (stream.matched.runtime.get() == runtime) {
          stream.retire_barrier = barrier;
          if (!stream.discarding) {
            retiring.push_back(stream_id);
          }
        }
      }
      // Only the stopping runtime's streams retire; a shared listener's
      // other attachees keep this connection serving.
      for (const std::int32_t stream_id : retiring) {
        self->respond_transport(stream_id, 503, "server shutting down");
      }
      if (!retiring.empty() && !self->closed_) {
        self->pump_writes();
      }
    });
  }

  void connection_shutdown() override {
    asio::post(strand_, [self = shared_from_this()] {
      if (self->shutting_down_) {
        return;
      }
      self->shutting_down_ = true;
      // GOAWAY carries the last processed stream id; unanswered streams
      // get the transport 503 and in-flight writes drain (RFC 0024,
      // lifecycle; h2 acceptance criteria).
      std::vector<Int> unanswered;
      for (const auto &[request_id, stream_id] : self->request_to_stream_) {
        unanswered.push_back(request_id);
      }
      for (const Int request_id : unanswered) {
        self->finish_stream_transport(request_id, 503,
                                      "server shutting down");
      }
      self->engine_.submit_goaway();
      self->pump_writes();
    });
  }

private:
  struct Stream {
    MatchedRoute matched{};
    HttpMethod method{HttpMethod::Get};
    bool head_fallback{};
    std::string target{};
    std::string decoded_path{};
    H2Headers headers{};
    std::size_t header_bytes{};
    H2Headers trailers{};
    std::size_t trailer_bytes{};
    std::size_t trailer_unaccounted{};
    std::string body{};
    std::size_t reserved{};
    std::size_t unaccounted{};
    bool admitted{};
    bool admission_in_flight{};
    // True while this stream's header block is counted in the
    // connection's unaccounted bound; cleared exactly once, on admission
    // (into the reservation) or on release (review P1).
    bool headers_counted{};
    // A retirement barrier rides the stream to its terminal event —
    // close, reset, or write failure — so a flow-control-blocked
    // response cannot let stop() clear its admission budget early (review P1).
    std::shared_ptr<const void> retire_barrier{};
    bool end_stream_seen{};
    bool discarding{};
    Int request_id{-1};
    // Delivery is reported from the write pump once the response bytes
    // have actually left the connection (review P1).
    std::size_t response_bytes{};
    bool response_submitted{};
    std::uint32_t close_error{};
    Int report_client_id{-1};
    Int report_request_id{-1};
    std::shared_ptr<WebServerRuntime> report_runtime{};
  };

  // --- H2Host (all calls arrive inside engine_.receive on the strand) ---
  void on_request_headers(std::int32_t stream_id, std::string method,
                          std::string target, H2Headers headers,
                          bool end_stream) override;
  void on_request_data(std::int32_t stream_id, std::string_view data,
                       bool end_stream) override;
  void on_request_trailers(std::int32_t stream_id, H2Headers trailers,
                           bool end_stream) override;
  void on_stream_reset(std::int32_t stream_id,
                       std::uint32_t error_code) override;
  void on_stream_closed(std::int32_t stream_id) override;
  void on_goaway_received() override { shutting_down_ = true; }

  void read_next();
  void resume_stalled_read();
  void pump_writes();
  void close();
  void admit_stream(std::int32_t stream_id);
  void account_stream_data(std::int32_t stream_id);
  void maybe_dispatch(std::int32_t stream_id);
  void respond_transport(std::int32_t stream_id, int status,
                         std::string_view body);
  void respond_static(std::int32_t stream_id, const MatchedStaticFile &file,
                      bool suppress_body);
  void write_stream_response(Int request_id, const Value &response,
                             Int client_id,
                             const std::shared_ptr<WebServerRuntime> &runtime);
  void finish_stream_transport(Int request_id, int status,
                               std::string_view body);
  void release_stream(std::int32_t stream_id, Stream &stream);
  void discard_stream_input(std::int32_t stream_id, Stream &stream);
  [[nodiscard]] Value peer_value(const WebBindings &bindings);

  std::shared_ptr<WebListener> listener_;
  std::shared_ptr<const ServerRuntimeConfig> config_;
  asio::strand<asio::io_context::executor_type> strand_;
  ServerTlsStream stream_;
  H2Engine engine_;
  std::map<std::int32_t, Stream> streams_{};
  std::map<Int, std::int32_t> request_to_stream_{};
  std::array<char, 16 * 1024> read_buffer_{};
  std::string write_buffer_{};
  std::size_t outstanding_response_bytes_{};
  std::size_t outstanding_response_messages_{};
  struct PendingReport {
    std::shared_ptr<WebServerRuntime> runtime{};
    Int client_id{};
    Int request_id{};
    bool clean{};
    // Barrier-only sentinel entries carry no runtime; they exist to hold
    // a retirement barrier until the write that flushes them completes.
    std::shared_ptr<const void> barrier{};
  };
  void queue_report(std::shared_ptr<WebServerRuntime> runtime, Int client_id,
                    Int request_id, bool clean) {
    pending_flush_reports_.push_back(
        PendingReport{std::move(runtime), client_id, request_id, clean});
  }
  void flush_reports(std::size_t count, bool written);
  void shutdown_send();

  std::vector<PendingReport> pending_flush_reports_{};
  // Bytes buffered ahead of admission across ALL streams; the connection
  // read loop stalls at one window's worth so backpressured streams
  // cannot accumulate stream-window x stream-count outside the admission
  // budget (review P1).
  std::size_t unaccounted_total_{};
  bool read_stalled_{};
  Int connection_id_{-1};
  bool writing_{};
  bool shutting_down_{};
  bool sent_fin_{};
  bool closed_{};
};

// ---------------------------------------------------------------------------
// Listener implementation

void WebListener::ensure_listening() {
  if (listening_.exchange(true)) {
    return;
  }
  tcp::endpoint endpoint{asio::ip::make_address(address_), port_};
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(asio::socket_base::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen(asio::socket_base::max_listen_connections);
  bound_port_.store(acceptor_.local_endpoint().port(),
                    std::memory_order_release);
  accepting_.store(true, std::memory_order_release);
  accept_next();
}

void WebListener::start_io() {
  if (!threads_.empty()) {
    return;
  }
  work_.emplace(asio::make_work_guard(io_context_));
  threads_.reserve(io_threads_);
  for (std::size_t index = 0; index != io_threads_; ++index) {
    threads_.emplace_back([this] { io_context_.run(); });
  }
}

void WebListener::stop_accepting() {
  accepting_.store(false, std::memory_order_release);
  asio::post(io_context_, [self = shared_from_this()] {
    beast::error_code ec;
    static_cast<void>(self->acceptor_.cancel(ec));
    static_cast<void>(self->acceptor_.close(ec));
  });
}

void WebListener::stop_io() {
  work_.reset();
  io_context_.stop();
  for (auto &thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  threads_.clear();
}

void WebListener::accept_next() {
  if (!accepting_.load(std::memory_order_acquire)) {
    return;
  }
  acceptor_.async_accept([self = shared_from_this()](beast::error_code ec,
                                                     tcp::socket socket) {
    if (!ec && self->accepting_.load(std::memory_order_acquire)) {
      std::shared_ptr<WebServerRuntime> owner;
      {
        std::lock_guard lock{self->mutex_};
        owner = self->runtimes_.empty() ? nullptr : self->runtimes_.front();
      }
      if (owner != nullptr &&
          !self->at_connection_limit(owner->config().max_connections)) {
        auto connection = std::make_shared<ServerConnection>(
            self, owner->config_ptr(), std::move(socket),
            owner->tls_context());
        connection->run();
      }
    }
    self->accept_next();
  });
}

std::optional<MatchedRoute> WebListener::match(HttpMethod method,
                                               std::string_view path,
                                               bool upgrade) {
  std::vector<std::shared_ptr<WebServerRuntime>> runtimes;
  {
    std::lock_guard lock{mutex_};
    runtimes = runtimes_;
  }
  for (const std::shared_ptr<WebServerRuntime> &runtime : runtimes) {
    const auto snapshot = upgrade ? runtime->ws_routes() : runtime->http_routes();
    const auto matched = snapshot->table.match(method, path);
    if (matched.matched) {
      return MatchedRoute{runtime, snapshot,
                          &snapshot->routes[matched.entry_index],
                          snapshot->generations[matched.entry_index],
                          matched.params};
    }
  }
  return std::nullopt;
}

std::optional<MatchedStaticFile>
WebListener::match_static(HttpMethod method, std::string_view path) const {
  if (method != HttpMethod::Get && method != HttpMethod::Head) {
    return std::nullopt;
  }
  // ``path`` arrives decoded from the header boundary, and the directory
  // branch below already treats it that way; decoding it again here rejected
  // any mount whose decoded name contains a literal '%'.
  const auto matched = static_files_.table.match_decoded(HttpMethod::Get, path);
  if (!matched.matched) {
    for (const StaticDirectoryConfig &directory : static_files_.directories) {
      if (const auto relative =
              static_directory_relative_path(directory.url_prefix, path);
          relative.has_value() && !relative->empty()) {
        return MatchedStaticFile{nullptr, &directory, std::string{path}};
      }
    }
    return std::nullopt;
  }
  return MatchedStaticFile{&static_files_.files[matched.entry_index], nullptr,
                           std::string{path}};
}

void WebListener::retire_runtime_connections(
    const WebServerRuntime *runtime, std::shared_ptr<const void> barrier) {
  std::vector<std::weak_ptr<PendingTarget>> live;
  {
    std::lock_guard lock{mutex_};
    live = live_connections_;
  }
  for (const auto &weak : live) {
    if (const auto connection = weak.lock()) {
      connection->retire_runtime(runtime, barrier);
    }
  }
}

void WebListener::shutdown_connections() {
  std::vector<std::weak_ptr<PendingTarget>> live;
  {
    std::lock_guard lock{mutex_};
    live.swap(live_connections_);
  }
  for (const auto &weak : live) {
    if (const auto connection = weak.lock()) {
      connection->connection_shutdown();
    }
  }
}

void WebListener::check_route_conflicts(const WebServerRuntime *applying,
                                        bool upgrade,
                                        const std::vector<Value> &added) {
  if (added.empty()) {
    return;
  }
  if (!upgrade) {
    for (const Value &route : added) {
      const auto fields = route.view().as_bundle();
      if (fields.at("method").checked_as<HttpMethod>() != HttpMethod::Get) {
        continue;
      }
      const auto pattern = fields.at("pattern").checked_as<Str>();
      for (const StaticFileConfig &file : static_files_.files) {
        if (file.url == pattern) {
          throw std::invalid_argument(
              "Web route " + std::string{enum_name(HttpMethod::Get)} + " " +
              std::string{pattern} + " from service path '" +
              std::string{applying->path()} +
              "' conflicts with a static file mount on this port");
        }
      }
      for (const StaticDirectoryConfig &directory : static_files_.directories) {
        if (static_directory_relative_path(directory.url_prefix, pattern)
                .has_value()) {
          throw std::invalid_argument(
              "Web route " + std::string{enum_name(HttpMethod::Get)} + " " +
              std::string{pattern} + " from service path '" +
              std::string{applying->path()} +
              "' conflicts with a static directory mount on this port");
        }
      }
    }
  }
  std::vector<std::shared_ptr<WebServerRuntime>> runtimes;
  {
    std::lock_guard lock{mutex_};
    runtimes = runtimes_;
  }
  for (const std::shared_ptr<WebServerRuntime> &other : runtimes) {
    if (other.get() == applying) {
      continue;
    }
    const auto snapshot = upgrade ? other->ws_routes() : other->http_routes();
    for (const Value &route : added) {
      const auto fields = route.view().as_bundle();
      const auto method = fields.at("method").checked_as<HttpMethod>();
      const auto pattern = fields.at("pattern").checked_as<Str>();
      for (const Value &existing : snapshot->routes) {
        const auto existing_fields = existing.view().as_bundle();
        if (existing_fields.at("method").checked_as<HttpMethod>() == method &&
            existing_fields.at("pattern").checked_as<Str>() == pattern) {
          throw std::invalid_argument(
              "Web route " + std::string{enum_name(method)} + " " +
              std::string{pattern} + " from service path '" +
              std::string{applying->path()} +
              "' is already served on this port by service path '" +
              std::string{other->path()} + "'");
        }
      }
    }
  }
}

void WebListener::set_reads_paused(ReadTier tier, const void *token,
                                   bool paused) {
  const auto index = static_cast<std::size_t>(tier);
  std::vector<std::function<void()>> parked;
  {
    std::lock_guard lock{mutex_};
    if (paused) {
      // Only attached runtimes may pause: a watermark callback racing a
      // stop() could otherwise re-add the departing runtime's token after
      // its withdrawal, leaving the shared listener paused forever once
      // admission stop discards the callback (review P1). detach() and
      // this check share mutex_, so the window is closed.
      const bool attached =
          std::any_of(runtimes_.begin(), runtimes_.end(),
                      [token](const auto &runtime) {
                        return static_cast<const void *>(runtime.get()) ==
                               token;
                      });
      if (!attached) {
        return;
      }
      pause_tokens_[index].insert(token);
    } else {
      pause_tokens_[index].erase(token);
    }
    pause_counts_[index].store(pause_tokens_[index].size(),
                               std::memory_order_release);
    if (!pause_tokens_[index].empty()) {
      return;
    }
    parked.swap(parked_[index]);
  }
  for (auto &resume : parked) {
    resume();
  }
}

void WebListener::park_for_resume(ReadTier tier, std::function<void()> resume) {
  const auto index = static_cast<std::size_t>(tier);
  bool run_now = false;
  {
    std::lock_guard lock{mutex_};
    if (!pause_tokens_[index].empty()) {
      parked_[index].push_back(std::move(resume));
    } else {
      run_now = true;
    }
  }
  if (run_now) {
    resume();
  }
}

// ---------------------------------------------------------------------------
// Runtime implementation

void WebServerRuntime::start() {
  register_web_types();
  register_internal_types();
  bindings_.resolve_all();

  if (config_->tls.enabled) {
    tls_context_ = std::make_unique<asio::ssl::context>(
        asio::ssl::context::tls_server);
    auto &context = *tls_context_;
    context.set_options(asio::ssl::context::default_workarounds |
                        asio::ssl::context::no_sslv2 |
                        asio::ssl::context::no_sslv3 |
                        asio::ssl::context::no_tlsv1 |
                        asio::ssl::context::no_tlsv1_1);
    if (config_->tls.min_version == WebTlsVersion::Tls1_3) {
      SSL_CTX_set_min_proto_version(context.native_handle(), TLS1_3_VERSION);
    }
    if (!config_->tls.key_password.empty()) {
      context.set_password_callback(
          [password = config_->tls.key_password](std::size_t,
                                                asio::ssl::context::password_purpose) {
            return password;
          });
    }
    if (!config_->tls.cert_path.empty()) {
      context.use_certificate_chain_file(config_->tls.cert_path);
    } else {
      context.use_certificate_chain(
          asio::buffer(config_->tls.cert_pem.data(), config_->tls.cert_pem.size()));
    }
    if (!config_->tls.key_path.empty()) {
      context.use_private_key_file(config_->tls.key_path,
                                   asio::ssl::context::pem);
    } else {
      context.use_private_key(
          asio::buffer(config_->tls.key_pem.data(), config_->tls.key_pem.size()),
          asio::ssl::context::pem);
    }
    if (!config_->tls.ca_path.empty()) {
      context.load_verify_file(config_->tls.ca_path);
    } else if (!config_->tls.ca_pem.empty()) {
      context.add_certificate_authority(
          asio::buffer(config_->tls.ca_pem.data(), config_->tls.ca_pem.size()));
    }
    switch (config_->tls.client_verify) {
    case WebClientVerify::None:
      context.set_verify_mode(asio::ssl::verify_none);
      break;
    case WebClientVerify::Optional:
      context.set_verify_mode(asio::ssl::verify_peer);
      break;
    case WebClientVerify::Required:
      context.set_verify_mode(asio::ssl::verify_peer |
                              asio::ssl::verify_fail_if_no_peer_cert);
      break;
    }
    // The ALPN callback selects from the CONFIGURED list, in configured
    // preference order; "h2" hands the connection to the HTTP/2 driver
    // after the handshake (RFC 0024, activation plan).
    alpn_wire_.clear();
    const std::vector<Str> protocols =
        config_->tls.alpn.empty() ? std::vector<Str>{Str{"http/1.1"}}
                                  : config_->tls.alpn;
    for (const Str &protocol : protocols) {
      alpn_wire_.push_back(static_cast<unsigned char>(protocol.size()));
      alpn_wire_.insert(alpn_wire_.end(), protocol.begin(), protocol.end());
    }
    SSL_CTX_set_alpn_select_cb(
        context.native_handle(),
        [](SSL *, const unsigned char **out, unsigned char *out_length,
           const unsigned char *in, unsigned int in_length,
           void *wire) noexcept -> int {
          const auto *preference =
              static_cast<const std::vector<unsigned char> *>(wire);
          unsigned char *selected = nullptr;
          if (SSL_select_next_proto(
                  &selected, out_length, preference->data(),
                  static_cast<unsigned int>(preference->size()), in,
                  in_length) == OPENSSL_NPN_NEGOTIATED) {
            *out = selected;
            return SSL_TLSEXT_ERR_OK;
          }
          return SSL_TLSEXT_ERR_NOACK;
        },
        &alpn_wire_);
  }

  listener_ = ListenerRegistry::instance().acquire(
      std::string{config_->bind_address}, config_->port,
      config_->config_identity, config_->io_threads, shared_from_this(),
      config_->static_files, config_->static_directories);
  try {
    if (!config_->bind_deferred) {
      listener_->ensure_listening();
    }
    listener_->start_io();
  } catch (...) {
    if (ListenerRegistry::instance().release(listener_, this)) {
      listener_->stop_io();
    }
    listener_.reset();
    throw;
  }
  started_ = true;

  // Watermarks pause socket reads while the graph catches up (RFC 0024,
  // flow control).  The callback runs on whichever thread crossed the
  // watermark; it only flips an atomic and posts resumes.
  const auto high = OutputLimits{
      config_->ingress.records * static_cast<std::size_t>(config_->watermark_high_pct) / 100,
      config_->ingress.bytes * static_cast<std::size_t>(config_->watermark_high_pct) / 100,
  };
  const auto low = OutputLimits{
      config_->ingress.records * static_cast<std::size_t>(config_->watermark_low_pct) / 100,
      config_->ingress.bytes * static_cast<std::size_t>(config_->watermark_low_pct) / 100,
  };
  admission_.value->set_watermark(
      index(ServerChannel::Request),
      WatermarkConfig{high, low,
                      [listener = listener_, token = this](bool paused) {
                        listener->set_reads_paused(
                            WebListener::ReadTier::Http, token, paused);
                      }});
  const auto ws_high = OutputLimits{
      config_->ws_ingress.records *
          static_cast<std::size_t>(config_->watermark_high_pct) / 100,
      config_->ws_ingress.bytes *
          static_cast<std::size_t>(config_->watermark_high_pct) / 100,
  };
  const auto ws_low = OutputLimits{
      config_->ws_ingress.records *
          static_cast<std::size_t>(config_->watermark_low_pct) / 100,
      config_->ws_ingress.bytes *
          static_cast<std::size_t>(config_->watermark_low_pct) / 100,
  };
  admission_.value->set_watermark(
      index(ServerChannel::WsIngress),
      WatermarkConfig{ws_high, ws_low,
                      [listener = listener_, token = this](bool paused) {
                        listener->set_reads_paused(WebListener::ReadTier::Ws,
                                                   token, paused);
                      }});

  // Timers run on their own strand and their handlers own the runtime, so a
  // non-last attachee's stop cannot race an expiring handler against the
  // runtime's release (review P1; Boost documents shared timer objects as
  // unsafe across threads).
  sweep_timer_ = std::make_shared<asio::steady_timer>(
      asio::make_strand(listener_->io_context()));
  arm_sweep_timer();
  if (config_->stats_interval.count() > 0) {
    stats_timer_ = std::make_shared<asio::steady_timer>(
        asio::make_strand(listener_->io_context()));
    arm_stats_timer();
  }
  if (!config_->bind_deferred) {
    emit_event(WebSeverity::Info, Str{"server"}, Str{"listening"},
               Str{"listening on port " +
                   std::to_string(listener_->bound_port())});
  }
}

void WebServerRuntime::stop() noexcept {
  if (!started_) {
    return;
  }
  started_ = false;
  stopping_.store(true, std::memory_order_release);
  try {
    if (listener_) {
      // Cancel the runtime-owned timers while the listener's io pool is
      // still running.  Their wait handlers capture this runtime; stopping
      // the io_context first would strand those handlers and form the cycle
      // runtime -> listener -> io_context -> handler -> runtime.
      // Keep the member handles immutable after start. Timer callbacks run
      // on the Asio strand and may copy these handles while graph teardown is
      // cancelling them; exchanging the members here would race those reads.
      // The callbacks release their runtime captures after cancellation, so
      // retaining the member handles until destruction does not form a cycle.
      auto sweep_timer = sweep_timer_;
      auto stats_timer = stats_timer_;
      const auto cancel_timer = [](const auto &timer) {
        if (timer) {
          asio::post(timer->get_executor(), [timer] {
            static_cast<void>(timer->cancel());
          });
        }
      };
      cancel_timer(sweep_timer);
      cancel_timer(stats_timer);

      // (1) stop intake, (2) 503 pending + drain, (3) WS Close(1001),
      // (4) cancel + join IO, (5) admission budget last (RFC 0024, lifecycle).
      const bool last =
          ListenerRegistry::instance().release(listener_, this);

      // Withdraw this runtime's pause tokens AFTER detaching: from here the
      // listener refuses new pauses from this runtime, so a watermark
      // callback racing this stop cannot re-add the token that
      // admission stop below would orphan (review P1).
      listener_->set_reads_paused(WebListener::ReadTier::Http, this, false);
      listener_->set_reads_paused(WebListener::ReadTier::Ws, this, false);

      {
        // The pending and WS registries are bookkeeping only from here;
        // retirement itself reaches EVERY live connection through the
        // listener, so streams still blocked before dispatch retire too
        // (review P1).
        std::lock_guard lock{pending_mutex_};
        pending_.clear();
        ws_connections_.clear();
      }
      // Retirement completion barrier: the barrier is captured by every
      // handler the retirement schedules, so use_count falls back to one
      // exactly when THIS runtime's 503s and releases have run — no wait
      // on other attachees' live connections (review P1).
      const std::shared_ptr<const void> barrier =
          std::make_shared<const int>(0);
      listener_->retire_runtime_connections(this, barrier);
      {
        const auto retire_deadline = std::chrono::steady_clock::now() +
                                     config_->shutdown_drain_timeout;
        while (std::chrono::steady_clock::now() < retire_deadline &&
               barrier.use_count() > 1) {
          std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
      }
      if (last) {
        // The listener is going away: every live connection — idle ones
        // included — gets its orderly shutdown (GOAWAY on h2), and only
        // then is the connection drain awaited.  A non-last stop must NOT
        // wait on listener-wide connections that stay open serving the
        // other attachees (review P1); its 503s flush on the live io pool.
        listener_->shutdown_connections();
        const auto deadline = std::chrono::steady_clock::now() +
                              config_->shutdown_drain_timeout;
        while (std::chrono::steady_clock::now() < deadline &&
               (listener_->open_connections() != 0 ||
                // One reference is retained by the runtime and one by this
                // stop frame. Any additional reference belongs to a posted
                // cancellation or wait handler that must drain before the IO
                // pool is stopped.
                (sweep_timer && sweep_timer.use_count() > 2) ||
                (stats_timer && stats_timer.use_count() > 2))) {
          std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
        listener_->stop_io();
      }
      listener_.reset();
    }
  } catch (...) {
  }
}

void WebServerRuntime::rebuild(
    PublishedRoutes &slot,
    std::vector<std::tuple<HttpMethod, std::string, Value, DateTime>> &master,
    std::vector<SubscriptionBinding> added, std::vector<Value> removed) {
  std::lock_guard lock{routes_mutex_};
  for (const Value &route : removed) {
    std::erase_if(master, [&](const auto &entry) {
      return std::get<2>(entry).view().equals(route.view());
    });
  }
  for (SubscriptionBinding &binding : added) {
    const auto fields = binding.key.view().as_bundle();
    master.emplace_back(fields.at("method").checked_as<HttpMethod>(),
                        std::string{fields.at("pattern").checked_as<Str>()},
                        std::move(binding.key), binding.generation);
  }
  auto compiled = std::make_shared<CompiledRoutes>();
  std::vector<RouteTable::Entry> entries;
  entries.reserve(master.size());
  for (const auto &[method, pattern, route, generation] : master) {
    entries.push_back(RouteTable::Entry{method, pattern});
    compiled->routes.push_back(route.clone());
    compiled->generations.push_back(generation);
  }
  compiled->table = RouteTable::build(std::move(entries));
  atomic_store_routes(&slot, compiled);
}

void WebServerRuntime::apply_http_routes(std::vector<SubscriptionBinding> added,
                                         std::vector<Value> removed) {
  if (listener_) {
    std::vector<Value> routes;
    routes.reserve(added.size());
    for (const auto &binding : added) {
      routes.push_back(binding.key.clone());
    }
    listener_->check_route_conflicts(this, false, routes);
  }
  for (const SubscriptionBinding &binding : added) {
    const Value &route = binding.key;
    // Route-aware floor: a maximal request on this route (its envelope
    // route copy and capture names included) must fit an empty ingress
    // channel, or Backpressure could park it forever (review P1).
    if (config_->ingress.bytes <
        config_->max_body_bytes + 6 * config_->max_header_bytes + 1024 +
            2 * route_weight(route)) {
      throw std::invalid_argument(
          "Web ingress_byte_limit cannot admit a maximal request on route "
          "'" +
          std::string{
              route.view().as_bundle().at("pattern").checked_as<Str>()} +
          "'");
    }
  }
  rebuild(http_routes_, http_master_, std::move(added), std::move(removed));
  if (config_->bind_deferred && listener_) {
    listener_->ensure_listening();
    emit_event(WebSeverity::Info, Str{"server"}, Str{"listening"},
               Str{"listening on port " +
                   std::to_string(listener_->bound_port())});
  }
}

void WebServerRuntime::apply_ws_routes(std::vector<SubscriptionBinding> added,
                                       std::vector<Value> removed) {
  if (listener_) {
    std::vector<Value> routes;
    routes.reserve(added.size());
    for (const auto &binding : added) {
      routes.push_back(binding.key.clone());
    }
    listener_->check_route_conflicts(this, true, routes);
  }
  for (const SubscriptionBinding &binding : added) {
    const Value &route = binding.key;
    // Route-aware floor: BOTH the maximal message and the largest
    // lifecycle record (the Open event carries the upgrade request's
    // metadata and capture names) must fit — the payload lane for the
    // former, and the guaranteed control lane for the latter (review P1).
    const std::size_t weight = route_weight(route);
    const std::size_t open_event_worst =
        3 * config_->max_header_bytes + 2 * weight + 1024;
    if (config_->ws_ingress.bytes <
            config_->ws_max_message_bytes + 256 + weight ||
        config_->ws_ingress.bytes < open_event_worst ||
        kControlLaneBytes < open_event_worst) {
      throw std::invalid_argument(
          "Web ws_ingress_byte_limit cannot admit route '" +
          std::string{
              route.view().as_bundle().at("pattern").checked_as<Str>()} +
          "': a maximal message and its largest lifecycle record must both "
          "fit (control lane included)");
    }
  }
  rebuild(ws_routes_, ws_master_, std::move(added), std::move(removed));
  if (config_->bind_deferred && listener_) {
    listener_->ensure_listening();
  }
}

Int WebServerRuntime::register_pending(
    const std::shared_ptr<PendingTarget> &target) {
  const Int request_id = ++process_request_ids;
  std::lock_guard lock{pending_mutex_};
  pending_[request_id] =
      Pending{target, std::chrono::steady_clock::now() +
                          config_->request_timeout};
  return request_id;
}

void WebServerRuntime::unregister_pending(Int request_id) noexcept {
  std::lock_guard lock{pending_mutex_};
  pending_.erase(request_id);
}

Int WebServerRuntime::register_ws_connection(
    const std::shared_ptr<ServerConnection> &connection) {
  const Int connection_id = ++process_request_ids;
  std::lock_guard lock{pending_mutex_};
  ws_connections_[connection_id] = connection;
  return connection_id;
}

void WebServerRuntime::unregister_ws_connection(Int connection_id) noexcept {
  std::lock_guard lock{pending_mutex_};
  ws_connections_.erase(connection_id);
}

bool WebServerRuntime::push_request_reserved(Value route, DateTime generation,
                                             Value request,
                                             std::size_t retained_bytes,
                                             std::size_t reserved_bytes) {
  if (retained_bytes > reserved_bytes) {
    // Admission projects the exact derived sizes, so this cannot happen
    // by construction; if it ever does, surface it instead of letting a
    // silent clamp mask the under-accounting (review P2).
    emit_event(WebSeverity::Warning, Str{"server"}, Str{"accounting"},
               Str{"request retained estimate exceeded its reservation"});
    retained_bytes = reserved_bytes;
  }
  return output_.send_reserved(
      WebTransportEventKind::ServerRequest, "request",
      build_on(bindings_.request_envelope,
               {
                   {"route", std::move(route)},
                   {"request", std::move(request)},
                   {"generation", Value{generation}},
               }),
      index(ServerChannel::Request), retained_bytes, reserved_bytes);
}

bool WebServerRuntime::push_ws_event_reserved(Value route,
                                              DateTime generation, Value event,
                                              std::size_t retained_bytes,
                                              std::size_t reserved_bytes) {
  // Terminal lifecycle events ride capacity reserved at accept, so a full
  // channel can never lose a Closed/Failed event (review P1).
  return output_.send_reserved(
      WebTransportEventKind::ServerWsIngress, "server_ws",
      build_on(bindings_.ws_ingress_envelope,
               {
                   {"route", std::move(route)},
                   {"event", std::move(event)},
                   {"generation", Value{generation}},
               }),
      index(ServerChannel::WsIngress),
      std::min(retained_bytes, reserved_bytes), reserved_bytes);
}

bool WebServerRuntime::push_ws_event(Value route, DateTime generation,
                                     Value event,
                                     std::size_t retained_bytes) {
  // Connection lifecycle is graph data and must never be lost (review P1):
  // an event the payload lane cannot take goes through the control lane,
  // which is sized for exactly this.
  const Value envelope = build_on(bindings_.ws_ingress_envelope,
                                  {
                                      {"route", std::move(route)},
                                      {"event", std::move(event)},
                                      {"generation", Value{generation}},
                                  });
  if (output_.send(WebTransportEventKind::ServerWsIngress, "server_ws",
                   envelope.clone(), index(ServerChannel::WsIngress),
                   retained_bytes)) {
    return true;
  }
  return output_.send(WebTransportEventKind::ServerWsIngress, "server_ws",
                      envelope.clone(), index(ServerChannel::WsIngress),
                      retained_bytes, true);
}

[[nodiscard]] bool
WebServerRuntime::push_ws_frame_reserved(Value route, DateTime generation,
                                         Value inbound_frame,
                                         std::size_t retained_bytes,
                                         std::size_t reserved_bytes) {
  return output_.send_reserved(
      WebTransportEventKind::ServerWsIngress, "server_ws",
      build_on(bindings_.ws_ingress_envelope,
               {
                   {"route", std::move(route)},
                   {"frame", std::move(inbound_frame)},
                   {"generation", Value{generation}},
               }),
      index(ServerChannel::WsIngress),
      std::min(retained_bytes, reserved_bytes), reserved_bytes);
}

void WebServerRuntime::report(std::size_t channel, Int client_id,
                              Value report_value) {
  const std::size_t retained = 512;
  const Value envelope =
      build_on(bindings_.delivery_envelope,
               {
                   {"request_id", bindings_.number(client_id)},
                   {"report", std::move(report_value)},
               });
  const auto kind = channel == index(ServerChannel::RespondDelivery)
                        ? WebTransportEventKind::ServerRespondDelivery
                        : WebTransportEventKind::ServerWsSendDelivery;
  if (!output_.send(kind, "delivery", envelope.clone(), channel, retained)) {
    if (!output_.send(kind, "delivery", envelope.clone(), channel, retained,
                      true)) {
      count_drop();
    }
  }
}

Value WebServerRuntime::delivery_report(Int request_id, WebDeliveryStatus status,
                                        Int error_code, const Str &message) {
  const auto &b = bindings_;
  return build_on(b.delivery_report,
                  {
                      {"request_id", b.number(request_id)},
                      {"sequence", b.number(++sequence_)},
                      {"status", b.enum_value(status)},
                      {"error_code", b.number(error_code)},
                      {"retriable", b.flag(false)},
                      {"fatal", b.flag(false)},
                      {"message", b.string(message)},
                  });
}

void WebServerRuntime::emit_event(WebSeverity severity, Str component,
                                  Str category, Str message, Int error_code,
                                  bool retriable, bool fatal,
                                  Int connection_id) {
  const bool stop_graph =
      fatal && config_->failure_policy == WebFailurePolicy::StopGraph;
  const auto &b = bindings_;
  const std::size_t retained = message.size() + 512;
  Value event = build_on(b.web_event,
                         {
                             {"severity", b.enum_value(severity)},
                             {"component", b.string(component)},
                             {"category", b.string(category)},
                             {"error_code", b.number(error_code)},
                             {"retriable", b.flag(retriable)},
                             {"fatal", b.flag(fatal)},
                             {"service_path", b.string(path_)},
                             {"connection_id", b.number(connection_id)},
                             {"message", b.string(message)},
                         });
  const Value envelope = build_on(b.event_envelope,
                                  {
                                      {"event", std::move(event)},
                                      {"stop_graph", b.flag(stop_graph)},
                                  });
  if (!output_.send(WebTransportEventKind::ServerEvent, "event",
                    envelope.clone(), index(ServerChannel::Event), retained)) {
    if (!output_.send(WebTransportEventKind::ServerEvent, "event",
                      envelope.clone(), index(ServerChannel::Event), retained,
                      true)) {
      count_drop();
    }
  }
}

void WebServerRuntime::sweep_expired_requests() {
  if (stopping_.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<std::pair<Int, std::shared_ptr<PendingTarget>>> expired;
  {
    std::lock_guard lock{pending_mutex_};
    const auto now = std::chrono::steady_clock::now();
    for (auto item = pending_.begin(); item != pending_.end();) {
      if (item->second.deadline <= now) {
        expired.emplace_back(item->first, std::move(item->second.target));
        item = pending_.erase(item);
      } else {
        ++item;
      }
    }
  }
  for (auto &[request_id, target] : expired) {
    target->answer_timeout(request_id);
  }
  arm_sweep_timer();
}

void WebServerRuntime::arm_sweep_timer() {
  auto timer = sweep_timer_;
  if (!timer || stopping_.load(std::memory_order_acquire)) {
    return;
  }
  timer->expires_after(std::chrono::milliseconds{250});
  timer->async_wait(
      [self = shared_from_this(), timer](beast::error_code ec) {
        if (!ec && !self->stopping_.load(std::memory_order_acquire)) {
          self->sweep_expired_requests();
        }
      });
}

void WebServerRuntime::arm_stats_timer() {
  auto timer = stats_timer_;
  if (!timer || stopping_.load(std::memory_order_acquire)) {
    return;
  }
  timer->expires_after(config_->stats_interval);
  timer->async_wait([self = shared_from_this(),
                     timer](beast::error_code ec) {
    if (ec || self->stopping_.load(std::memory_order_acquire)) {
      return;
    }
    self->emit_stats_once();
    self->arm_stats_timer();
  });
}

void WebServerRuntime::emit_stats_once() {
  std::size_t pending_count = 0;
  std::size_t ws_count = 0;
  {
    std::lock_guard lock{pending_mutex_};
    pending_count = pending_.size();
    ws_count = ws_connections_.size();
  }
  const auto &b = bindings_;
  if (!output_.send(
          WebTransportEventKind::ServerStats, "server_stats",
          build_on(b.server_stats,
                   {
                       {"listening_port",
                        b.number(Int{listener_ ? listener_->bound_port() : 0})},
                       {"connection_count",
                        b.number(Int(listener_ ? listener_->open_connections()
                                               : 0))},
                       {"ws_connection_count", b.number(Int(ws_count))},
                       {"pending_request_count", b.number(Int(pending_count))},
                       {"ingress_record_count",
                        b.number(Int(admission_.value->payload_pending(
                            index(ServerChannel::Request))))},
                       {"ingress_byte_count",
                        b.number(Int(admission_.value->payload_retained_bytes(
                            index(ServerChannel::Request))))},
                       {"outbound_byte_count", b.number(Int{0})},
                       {"dropped_count", b.number(dropped_.load())},
                   }),
          index(ServerChannel::Stats), 256)) {
    // Statistics are self-superseding; the next admitted sample contains the
    // current counters, including this refusal.
    count_drop();
  }
}

void WebServerRuntime::respond(Int client_id, Int request_id, Value response) {
  if (stopping_.load(std::memory_order_acquire)) {
    report(index(ServerChannel::RespondDelivery), client_id,
           delivery_report(request_id, WebDeliveryStatus::EnqueueRejected, 0,
                           Str{"web server is not serving"}));
    return;
  }
  std::shared_ptr<PendingTarget> target;
  {
    std::lock_guard lock{pending_mutex_};
    const auto found = pending_.find(request_id);
    if (found != pending_.end()) {
      target = std::move(found->second.target);
      pending_.erase(found);
    }
  }
  if (!target) {
    // Responding to an unknown or already-answered id is a reported error,
    // never silence (RFC 0024).
    report(index(ServerChannel::RespondDelivery), client_id,
           delivery_report(request_id, WebDeliveryStatus::PermanentFailure, 0,
                           Str{"unknown or already-answered request id"}));
    return;
  }
  target->deliver_response(request_id, std::move(response), client_id,
                           shared_from_this());
}

void WebServerRuntime::ws_send(Int client_id, Int connection_id, Value frame) {
  if (stopping_.load(std::memory_order_acquire)) {
    report(index(ServerChannel::WsSendDelivery), client_id,
           delivery_report(connection_id, WebDeliveryStatus::EnqueueRejected,
                           0, Str{"web server is not serving"}));
    return;
  }
  std::shared_ptr<ServerConnection> connection;
  {
    std::lock_guard lock{pending_mutex_};
    const auto found = ws_connections_.find(connection_id);
    if (found != ws_connections_.end()) {
      connection = found->second.lock();
    }
  }
  if (!connection) {
    report(index(ServerChannel::WsSendDelivery), client_id,
           delivery_report(connection_id, WebDeliveryStatus::PermanentFailure,
                           0, Str{"WebSocket is not connected"}));
    return;
  }
  connection->deliver_ws_frame(std::move(frame), client_id,
                               shared_from_this());
}

// ---------------------------------------------------------------------------
// Connection implementation

void ServerConnection::read_next() {
  if (shutting_down_) {
    close();
    return;
  }
  serving_runtime_ = nullptr;  // a fresh request cycle owns nothing yet
  if (listener_->reads_paused(WebListener::ReadTier::Http)) {
    read_parked_ = true;
    listener_->park_for_resume(
        WebListener::ReadTier::Http,
        [self = shared_from_this()] { self->resume_reading(); });
    return;
  }
  const auto &config = *config_;
  parser_.emplace();
  parser_->header_limit(static_cast<std::uint32_t>(config.max_header_bytes));
  parser_->body_limit(config.max_body_bytes);
  auto &stream_timeout = tls_stream_.has_value()
                             ? beast::get_lowest_layer(*tls_stream_)
                             : *plain_stream_;
  stream_timeout.expires_after(config.idle_timeout);
  // Headers first: route admission happens BEFORE the body is read, so a
  // request the admission budget cannot hold stays unread in the kernel instead of
  // being retained per-connection (review P1; RFC 0024, flow control).
  const auto on_headers = asio::bind_executor(
      strand_, [self = shared_from_this()](beast::error_code ec, std::size_t) {
        if (ec) {
          self->close();
          return;
        }
        self->on_headers();
      });
  if (tls_stream_.has_value()) {
    http::async_read_header(*tls_stream_, buffer_, *parser_, on_headers);
  } else {
    http::async_read_header(*plain_stream_, buffer_, *parser_, on_headers);
  }
}

void ServerConnection::on_headers() {
  const auto &header = parser_->get();
  const auto method = method_from(header.method());
  if (!method.has_value()) {
    send_simple_response(http::status::not_implemented,
                         "unsupported method", false);
    return;
  }
  const std::string_view target{header.target().data(),
                                header.target().size()};
  const auto query_start = target.find('?');
  const std::string_view path = target.substr(0, query_start);
  // Malformed percent-encoding is a client error, not an unmatched route
  // (RFC 0024, routing); decode once here so the graph-visible path and the
  // matcher agree on the same bytes.
  auto decoded_path = RouteTable::decode_path(path);
  if (!decoded_path.has_value()) {
    respond_after_headers(http::status::bad_request,
                          "malformed percent-encoding");
    return;
  }
  decoded_path_ = std::move(*decoded_path);

  if (websocket::is_upgrade(header)) {
    // Upgrade requests carry no body, so the message is already complete.
    request_ = parser_->release();
    auto matched = listener_->match(*method, path, true);
    if (!matched.has_value()) {
      send_simple_response(http::status::not_found, "no such route", false);
      return;
    }
    accept_ws(std::move(*matched));
    return;
  }

  if (*method == HttpMethod::Get) {
    if (const auto static_file =
            listener_->match_static(*method, decoded_path_)) {
      serve_static_after_headers(*static_file, false);
      return;
    }
    auto matched = listener_->match(*method, path, false);
    if (!matched.has_value()) {
      respond_after_headers(http::status::not_found, "no such route");
      return;
    }
    admit_and_read_body(std::move(*matched));
    return;
  }

  if (*method == HttpMethod::Head) {
    auto matched = listener_->match(HttpMethod::Head, path, false);
    if (matched.has_value()) {
      admit_and_read_body(std::move(*matched));
      return;
    }
    if (const auto static_file =
            listener_->match_static(HttpMethod::Head, decoded_path_)) {
      serve_static_after_headers(*static_file, true);
      return;
    }
    // Standard HTTP: HEAD is GET without the body, so a GET route serves it
    // (the transport suppresses the body on the way out).
    matched = listener_->match(HttpMethod::Get, path, false);
    if (!matched.has_value()) {
      respond_after_headers(http::status::not_found, "no such route");
      return;
    }
    admit_and_read_body(std::move(*matched));
    return;
  }

  auto matched = listener_->match(*method, path, false);
  if (!matched.has_value()) {
    respond_after_headers(http::status::not_found, "no such route");
    return;
  }
  admit_and_read_body(std::move(*matched));
}

void ServerConnection::respond_after_headers(http::status status,
                                             std::string_view body) {
  if (parser_->is_done()) {
    // No body follows: framing is intact, keep-alive can be honoured.
    request_ = parser_->release();
    send_simple_response(status, body, request_.keep_alive());
    return;
  }
  // An unread body follows; closing after the response keeps the framing
  // sound without reading bytes the graph will never see.
  send_simple_response(status, body, false);
}

void ServerConnection::serve_static_after_headers(const MatchedStaticFile &file,
                                                  bool suppress_body) {
  ResolvedStaticFile resolved =
      file.file != nullptr
          ? resolve_static_file(
                StaticFileTarget{std::string{file.file->file},
                                 std::string{file.file->content_type},
                                 std::string{file.file->cache_control}})
          : load_static_directory_file(*file.directory, file.path);
  const bool keep_alive = parser_->is_done() && !shutting_down_;
  const std::size_t content_length =
      resolved.file.empty() ? resolved.body.size() : resolved.size;
  if (parser_->is_done()) {
    request_ = parser_->release();
  }
  if (!suppress_body && !resolved.file.empty() &&
      resolved.status == http::status::ok) {
    send_file_response(resolved.status, std::move(resolved.file),
                       content_length, keep_alive, resolved.content_type,
                       resolved.cache_control);
    return;
  }
  send_buffer_response(resolved.status,
                       suppress_body ? std::string{} : std::move(resolved.body),
                       keep_alive, resolved.content_type,
                       resolved.cache_control,
                       suppress_body
                           ? std::optional<std::size_t>{content_length}
                           : std::nullopt);
}

void ServerConnection::admit_and_read_body(MatchedRoute matched) {
  if (shutting_down_) {
    close();
    return;
  }
  const auto &header = parser_->get();
  std::size_t header_bytes = 0;
  for (const auto &field : header) {
    header_bytes += field.name_string().size() + field.value().size();
  }
  // Projection mirrors build_server_request's retained estimate.  A
  // header-complete message (a bodyless GET/HEAD) projects zero body
  // bytes — treating it as potentially chunked would reserve the whole
  // body limit per request and starve concurrent admission (review P1);
  // only genuinely chunked framing falls back to the body limit.
  const auto content_length = parser_->content_length();
  const std::size_t projected_body =
      parser_->is_done() ? 0
      : content_length.has_value()
          ? static_cast<std::size_t>(*content_length)
          : config_->max_body_bytes;
  // The derived sizes are already known at admission — decoded path,
  // query slice, and the MATCHED parameter names and values (route
  // patterns can carry capture names with no length relation to the
  // target) — so the projection uses them exactly (review P2).
  const std::string_view admission_target{header.target().data(),
                                          header.target().size()};
  const auto admission_query = admission_target.find('?');
  const std::size_t query_size =
      admission_query == std::string_view::npos
          ? 0
          : admission_target.size() - admission_query - 1;
  std::size_t params_bytes = 0;
  for (const auto &[name, value] : matched.params) {
    params_bytes += name.size() + value.size();
  }
  const std::size_t projected = projected_body + header_bytes +
                                admission_target.size() +
                                decoded_path_.size() + query_size +
                                params_bytes + route_weight(*matched.route) +
                                512;
  const std::shared_ptr<WebServerRuntime> runtime = matched.runtime;
  serving_runtime_ = runtime.get();
  // Absolute oversize rejection: a request that cannot fit even an EMPTY
  // channel must never enter the Backpressure retry loop (review P1).
  if (projected > runtime->config().ingress.bytes) {
    send_simple_response(http::status::payload_too_large,
                         "request cannot fit the ingress limit", false);
    return;
  }
  if (runtime->reserve_request(projected)) {
    admitted_runtime_ = runtime;
    admitted_bytes_ = projected;
    read_body(std::move(matched));
    return;
  }
  if (runtime->config().inbound_overflow == WebInboundOverflow::Reject) {
    // Over the limit the transport answers itself; the body stays unread,
    // so the connection closes after the response to keep framing sound.
    send_simple_response(http::status::service_unavailable,
                         "server is at capacity", false);
    return;
  }
  // Backpressure: the body stays in the kernel; retry admission when the
  // HTTP tier resumes, or on a timer when no pauser is registered (the
  // admission window can be exhausted before a watermark fires).
  auto held = std::make_shared<MatchedRoute>(std::move(matched));
  if (listener_->reads_paused(WebListener::ReadTier::Http)) {
    listener_->park_for_resume(
        WebListener::ReadTier::Http, [self = shared_from_this(), held] {
          asio::post(self->strand_, [self, held] {
            self->admit_and_read_body(std::move(*held));
          });
        });
    return;
  }
  auto retry = std::make_shared<asio::steady_timer>(listener_->io_context());
  retry->expires_after(std::chrono::milliseconds{50});
  retry->async_wait(asio::bind_executor(
      strand_,
      [self = shared_from_this(), held, retry](beast::error_code ec) {
        if (!ec && !self->shutting_down_) {
          self->admit_and_read_body(std::move(*held));
        }
      }));
}

void ServerConnection::read_body(MatchedRoute matched) {
  auto &stream_timeout = tls_stream_.has_value()
                             ? beast::get_lowest_layer(*tls_stream_)
                             : *plain_stream_;
  stream_timeout.expires_after(config_->idle_timeout);
  auto held = std::make_shared<MatchedRoute>(std::move(matched));
  const auto on_read = asio::bind_executor(
      strand_,
      [self = shared_from_this(), held](beast::error_code ec, std::size_t) {
        if (ec) {
          self->release_admission();
          self->close();
          return;
        }
        self->request_ = self->parser_->release();
        self->dispatch_http(std::move(*held), self->request_.keep_alive());
      });
  if (tls_stream_.has_value()) {
    http::async_read(*tls_stream_, buffer_, *parser_, on_read);
  } else {
    http::async_read(*plain_stream_, buffer_, *parser_, on_read);
  }
}

void ServerConnection::release_admission() noexcept {
  if (admitted_runtime_) {
    admitted_runtime_->release_request_reservation(admitted_bytes_);
    admitted_runtime_.reset();
    admitted_bytes_ = 0;
  }
}

Value ServerConnection::peer_value(const WebBindings &b) {
  beast::error_code ec;
  auto &socket = tls_stream_.has_value()
                     ? beast::get_lowest_layer(*tls_stream_).socket()
                     : plain_stream_->socket();
  const auto remote = socket.remote_endpoint(ec);
  const auto local = socket.local_endpoint(ec);
  Str negotiated{};
  Str sni{};
  Str subject{};
  const bool tls = tls_stream_.has_value();
  if (tls) {
    SSL *ssl = tls_stream_->native_handle();
    const unsigned char *alpn = nullptr;
    unsigned int alpn_length = 0;
    SSL_get0_alpn_selected(ssl, &alpn, &alpn_length);
    if (alpn != nullptr && alpn_length != 0) {
      negotiated = Str{reinterpret_cast<const char *>(alpn), alpn_length};
    }
    if (const char *name =
            SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name)) {
      sni = Str{name};
    }
    if (X509 *cert = SSL_get_peer_certificate(ssl)) {
      char buffer[512];
      X509_NAME_oneline(X509_get_subject_name(cert), buffer, sizeof(buffer));
      subject = Str{buffer};
      X509_free(cert);
    }
  }
  return build_on(
      b.peer,
      {
          {"remote_address",
           b.string(Str{ec ? std::string{} : remote.address().to_string()})},
          {"remote_port", b.number(Int{ec ? 0 : remote.port()})},
          {"local_port", b.number(Int{ec ? 0 : local.port()})},
          {"tls", b.flag(Bool{tls})},
          {"negotiated_protocol", b.string(negotiated)},
          {"sni", b.string(sni)},
          {"client_cert_subject", b.string(subject)},
      });
}

Value ServerConnection::build_server_request(const MatchedRoute &matched,
                                             Int request_id,
                                             std::size_t &retained_bytes) {
  const auto &b = matched.runtime->bindings();
  const auto method = *method_from(request_.method());
  const std::string_view target{request_.target().data(),
                                request_.target().size()};
  const auto query_start = target.find('?');
  const std::string_view query =
      query_start == std::string_view::npos ? std::string_view{}
                                            : target.substr(query_start + 1);

  WebBindings::NamedPairs headers;
  std::size_t header_bytes = 0;
  for (const auto &field : request_) {
    headers.emplace_back(std::string{field.name_string()},
                         std::string{field.value()});
    header_bytes += headers.back().first.size() + headers.back().second.size();
  }
  // The admission budget accounts for real payload memory, so byte limits and
  // watermarks bound what a peer can make the graph retain (review P1).
  // Moving the body out empties the Beast message, so once the value is
  // pushed the connection itself no longer retains the payload.
  Bytes body{std::move(request_.body())};
  // EXACT derived sizes: the Beast message is released right after the
  // push, so the graph value's copies are single-counted (review P1).
  std::size_t params_bytes = 0;
  for (const auto &[name, value] : matched.params) {
    params_bytes += name.size() + value.size();
  }
  retained_bytes = body.data.size() + header_bytes + target.size() +
                   decoded_path_.size() + query.size() + params_bytes +
                   route_weight(*matched.route) + 512;

  Value request = build_on(
      b.http_request,
      {
          {"method", b.enum_value(method)},
          {"target", b.string(Str{std::string{target}})},
          {"path", b.string(Str{decoded_path_})},
          {"query", b.params(parse_query(query))},
          {"path_params", b.params(materialize_params(matched.params))},
          {"headers", b.headers(headers)},
          {"body", b.bytes(body)},
          {"trailers", b.headers({})},
      });
  return build_on(b.server_request,
                  {
                      {"request_id", b.number(request_id)},
                      {"connection_id", b.number(Int{0})},
                      {"stream_id", b.number(Int{0})},
                      {"request", std::move(request)},
                      {"peer", peer_value(b)},
                  });
}

void ServerConnection::dispatch_http(MatchedRoute matched, bool keep_alive) {
  const std::shared_ptr<WebServerRuntime> runtime = matched.runtime;
  const Int request_id = runtime->register_pending(shared_from_this());
  std::size_t retained_bytes = 0;
  Value server_request =
      build_server_request(matched, request_id, retained_bytes);
  // The reserved send consumes the admission taken before the body read;
  // capacity was decided there, so no retry can retain a body outside the
  // standard push-source queue (review P1). It fails only once graph teardown
  // stops admission.
  const bool pushed = runtime->push_request_reserved(
      matched.route->clone(), matched.generation, std::move(server_request),
      retained_bytes, admitted_bytes_);
  admitted_runtime_.reset();
  admitted_bytes_ = 0;
  if (!pushed) {
    runtime->unregister_pending(request_id);
    send_simple_response(http::status::service_unavailable,
                         "server shutting down", false);
    return;
  }
  pending_request_id_ = request_id;
  request_keep_alive_ = keep_alive;
  pending_is_head_ = request_.method() == http::verb::head;
  // The graph value owns its copies; drop the transport's source message
  // so the retained estimate single-counts everything (review P1).
  request_ = http::request<http::string_body>{};
  decoded_path_ = std::string{};
}

void ServerConnection::write_response(
    Int request_id, const Value &response, Int client_id,
    const std::shared_ptr<WebServerRuntime> &runtime) {
  if (pending_request_id_ != request_id || writing_) {
    runtime->report(index(ServerChannel::RespondDelivery), client_id,
                    runtime->delivery_report(
                        request_id, WebDeliveryStatus::PermanentFailure, 0,
                        Str{"the request was already answered"}));
    return;
  }
  pending_request_id_ = -1;
  const auto fields = response.view().as_bundle();
  const bool keep_alive = request_keep_alive_ && !shutting_down_;
  const auto status =
      static_cast<unsigned>(fields.at("status").checked_as<Int>());
  std::string body;
  const auto body_field = fields.at("body");
  if (body_field.data() != nullptr) {
    body = std::string{body_field.checked_as<Bytes>().data};
  }
  chunk_trailers_.clear();
  const auto trailers = fields.at("trailers");
  const bool suppress_body = pending_is_head_;
  if (!suppress_body && trailers.data() != nullptr) {
    for (const auto trailer : trailers.as_list()) {
      const auto pair = trailer.as_bundle();
      chunk_trailers_.insert(std::string{pair.at("name").checked_as<Str>()},
                             std::string{pair.at("value").checked_as<Str>()});
    }
  }
  writing_ = true;

  if (chunk_trailers_.begin() != chunk_trailers_.end()) {
    // Trailer-bearing responses use chunked transfer (the only way h1.1
    // carries trailers); the header, one body chunk, and the trailer part
    // write in sequence on the strand (RFC 0024, value contract).
    chunk_head_.emplace();
    chunk_head_->version(11);
    chunk_head_->result(status);
    const auto headers = fields.at("headers");
    if (headers.data() != nullptr) {
      for (const auto header : headers.as_list()) {
        const auto pair = header.as_bundle();
        chunk_head_->insert(std::string{pair.at("name").checked_as<Str>()},
                            std::string{pair.at("value").checked_as<Str>()});
      }
    }
    std::string trailer_names;
    for (const auto &trailer : chunk_trailers_) {
      if (!trailer_names.empty()) {
        trailer_names += ", ";
      }
      trailer_names += std::string{trailer.name_string()};
    }
    chunk_head_->set(http::field::trailer, trailer_names);
    chunk_head_->keep_alive(keep_alive);
    chunk_head_->chunked(true);
    chunk_body_ = std::move(body);
    chunk_serializer_.emplace(*chunk_head_);
    const auto on_header = asio::bind_executor(
        strand_, [self = shared_from_this(), client_id, request_id, runtime,
                  keep_alive](beast::error_code ec, std::size_t) {
          if (ec) {
            self->finish_response(ec, client_id, request_id, runtime,
                                  keep_alive);
            return;
          }
          const auto on_body = asio::bind_executor(
              self->strand_,
              [self, client_id, request_id, runtime,
               keep_alive](beast::error_code body_ec, std::size_t) {
                if (body_ec) {
                  self->finish_response(body_ec, client_id, request_id,
                                        runtime, keep_alive);
                  return;
                }
                const auto on_last = asio::bind_executor(
                    self->strand_,
                    [self, client_id, request_id, runtime,
                     keep_alive](beast::error_code last_ec, std::size_t) {
                      self->finish_response(last_ec, client_id, request_id,
                                            runtime, keep_alive);
                    });
                if (self->tls_stream_.has_value()) {
                  asio::async_write(*self->tls_stream_,
                                    http::make_chunk_last(self->chunk_trailers_),
                                    on_last);
                } else {
                  asio::async_write(*self->plain_stream_,
                                    http::make_chunk_last(self->chunk_trailers_),
                                    on_last);
                }
              });
          if (self->chunk_body_.empty()) {
            asio::post(self->strand_, [on_body] {
              on_body.get()(beast::error_code{}, 0);
            });
            return;
          }
          if (self->tls_stream_.has_value()) {
            asio::async_write(*self->tls_stream_,
                              http::make_chunk(asio::buffer(self->chunk_body_)),
                              on_body);
          } else {
            asio::async_write(*self->plain_stream_,
                              http::make_chunk(asio::buffer(self->chunk_body_)),
                              on_body);
          }
        });
    if (tls_stream_.has_value()) {
      http::async_write_header(*tls_stream_, *chunk_serializer_, on_header);
    } else {
      http::async_write_header(*plain_stream_, *chunk_serializer_, on_header);
    }
    return;
  }

  outgoing_.emplace();
  outgoing_->version(11);
  outgoing_->result(status);
  const auto headers = fields.at("headers");
  if (headers.data() != nullptr) {
    for (const auto header : headers.as_list()) {
      const auto pair = header.as_bundle();
      outgoing_->insert(std::string{pair.at("name").checked_as<Str>()},
                        std::string{pair.at("value").checked_as<Str>()});
    }
  }
  const std::size_t body_size = body.size();
  if (!suppress_body) {
    outgoing_->body() = std::move(body);
  }
  outgoing_->keep_alive(keep_alive);
  outgoing_->prepare_payload();
  if (suppress_body) {
    // A HEAD response advertises the entity's Content-Length but carries no
    // body (RFC 0024 / RFC 9110).
    outgoing_->content_length(body_size);
  }
  const auto on_write = asio::bind_executor(
      strand_, [self = shared_from_this(), client_id, request_id, runtime,
                keep_alive](beast::error_code ec, std::size_t) {
        self->finish_response(ec, client_id, request_id, runtime, keep_alive);
      });
  if (tls_stream_.has_value()) {
    http::async_write(*tls_stream_, *outgoing_, on_write);
  } else {
    http::async_write(*plain_stream_, *outgoing_, on_write);
  }
}

void ServerConnection::finish_response(
    beast::error_code ec, Int client_id, Int request_id,
    const std::shared_ptr<WebServerRuntime> &runtime, bool keep_alive) {
  writing_ = false;
  outgoing_.reset();
  outgoing_file_serializer_.reset();
  outgoing_file_.reset();
  chunk_serializer_.reset();
  chunk_head_.reset();
  chunk_body_.clear();
  chunk_trailers_.clear();
  runtime->report(index(ServerChannel::RespondDelivery), client_id,
                  runtime->delivery_report(
                      request_id,
                      ec ? WebDeliveryStatus::PermanentFailure
                         : WebDeliveryStatus::Delivered,
                      ec ? ec.value() : 0, ec ? Str{ec.message()} : Str{}));
  if (ec || !keep_alive) {
    close();
  } else {
    read_next();
  }
}

void ServerConnection::send_simple_response(http::status status,
                                            std::string_view body,
                                            bool keep_alive) {
  send_buffer_response(status, std::string{body}, keep_alive, "text/plain", "",
                       std::nullopt);
}

void ServerConnection::send_buffer_response(http::status status, std::string body,
                                            bool keep_alive,
                                            std::string_view content_type,
                                            std::string_view cache_control,
                                            std::optional<std::size_t> content_length) {
  if (writing_) {
    return;
  }
  outgoing_.emplace();
  outgoing_->version(11);
  outgoing_->result(status);
  outgoing_->set(http::field::content_type, content_type);
  if (!cache_control.empty()) {
    outgoing_->set(http::field::cache_control, cache_control);
  }
  outgoing_->body() = std::move(body);
  outgoing_->keep_alive(keep_alive && !shutting_down_);
  outgoing_->prepare_payload();
  if (content_length.has_value()) {
    outgoing_->content_length(*content_length);
  }
  writing_ = true;
  const bool continue_reading = keep_alive && !shutting_down_;
  const auto on_write = asio::bind_executor(
      strand_,
      [self = shared_from_this(), continue_reading](beast::error_code ec,
                                                    std::size_t) {
        self->writing_ = false;
        self->outgoing_.reset();
        self->outgoing_file_serializer_.reset();
        self->outgoing_file_.reset();
        if (ec || !continue_reading) {
          self->close();
        } else {
          self->read_next();
        }
      });
  if (tls_stream_.has_value()) {
    http::async_write(*tls_stream_, *outgoing_, on_write);
  } else {
    http::async_write(*plain_stream_, *outgoing_, on_write);
  }
}

void ServerConnection::send_file_response(http::status status, std::string path,
                                          std::size_t content_length,
                                          bool keep_alive,
                                          std::string_view content_type,
                                          std::string_view cache_control) {
  if (writing_) {
    return;
  }
  beast::error_code ec;
  outgoing_file_.emplace();
  outgoing_file_->version(11);
  outgoing_file_->result(status);
  outgoing_file_->set(http::field::content_type, content_type);
  if (!cache_control.empty()) {
    outgoing_file_->set(http::field::cache_control, cache_control);
  }
  outgoing_file_->body().open(path.c_str(), beast::file_mode::scan, ec);
  if (ec) {
    outgoing_file_.reset();
    send_simple_response(http::status::internal_server_error,
                         "static file error", keep_alive);
    return;
  }
  outgoing_file_->content_length(content_length);
  outgoing_file_->keep_alive(keep_alive && !shutting_down_);
  outgoing_file_serializer_.emplace(*outgoing_file_);
  writing_ = true;
  const bool continue_reading = keep_alive && !shutting_down_;
  const auto on_write = asio::bind_executor(
      strand_,
      [self = shared_from_this(), continue_reading](beast::error_code ec,
                                                    std::size_t) {
        self->writing_ = false;
        self->outgoing_file_serializer_.reset();
        self->outgoing_file_.reset();
        self->outgoing_.reset();
        if (ec || !continue_reading) {
          self->close();
        } else {
          self->read_next();
        }
      });
  if (tls_stream_.has_value()) {
    http::async_write(*tls_stream_, *outgoing_file_serializer_, on_write);
  } else {
    http::async_write(*plain_stream_, *outgoing_file_serializer_, on_write);
  }
}

void ServerConnection::accept_ws(MatchedRoute matched) {
  const std::shared_ptr<WebServerRuntime> runtime = matched.runtime;
  serving_runtime_ = runtime.get();
  ws_runtime_ = runtime;
  ws_route_snapshot_ = matched.snapshot;
  ws_route_ = matched.route;
  ws_generation_ = matched.generation;
  ws_route_weight_ = route_weight(*ws_route_);
  ws_connection_id_ = runtime->register_ws_connection(shared_from_this());

  const auto &config = runtime->config();
  websocket::stream_base::timeout timeout{};
  timeout.handshake_timeout = config.request_timeout.count() > 0
                                  ? websocket::stream_base::duration{
                                        config.request_timeout}
                                  : websocket::stream_base::none();
  // Beast's built-in keep-alive pings implement the configured ping/pong
  // policy; a zero interval disables idle enforcement.
  timeout.idle_timeout =
      config.ping_interval.count() > 0
          ? websocket::stream_base::duration{config.ping_interval +
                                             config.pong_timeout}
          : websocket::stream_base::none();
  timeout.keep_alive_pings = config.ping_interval.count() > 0;

  std::size_t request_bytes = 0;
  const Value server_request =
      build_server_request(matched, ws_connection_id_, request_bytes);

  const auto on_accept = asio::bind_executor(
      strand_, [self = shared_from_this(), runtime, request_bytes,
                server_request = server_request.clone()](beast::error_code ec) {
        if (ec) {
          runtime->unregister_ws_connection(self->ws_connection_id_);
          self->close();
          return;
        }
        self->ws_ = true;
        const auto &b = runtime->bindings();
        // Reserve the terminal event BEFORE publishing Open.  Once the graph
        // can observe the connection it must also be guaranteed to observe
        // exactly one Closed/Failed event, even when Open itself fills the
        // payload lane (review P1).
        const std::size_t close_weight = self->ws_route_weight_ + 128 + 512;
        if (!runtime->reserve_ws_ingress(close_weight)) {
          runtime->count_drop();
          runtime->emit_event(
              WebSeverity::Warning, Str{"server"}, Str{"ws_ingress"},
              Str{"terminal-event capacity unavailable; closing"}, 0, true,
              false, self->ws_connection_id_);
          runtime->unregister_ws_connection(self->ws_connection_id_);
          self->close();
          return;
        }
        self->ws_close_reserved_ = close_weight;
        const bool open_delivered = runtime->push_ws_event(
            self->ws_route_->clone(), self->ws_generation_,
            build_on(b.ws_event,
                     {
                         {"connection_id", b.number(self->ws_connection_id_)},
                         {"state", b.enum_value(WsConnectionState::Open)},
                         {"request", server_request.clone()},
                     }),
            // request_bytes (build_server_request) already includes the
            // route weight — adding it again could overcharge a genuinely
            // fitting event off the control lane (review P1).
            request_bytes + 512);
        if (!open_delivered) {
          // A connection whose Open event the graph never saw must not
          // proceed: the graph could neither serve nor close it
          // (review P1).
          runtime->count_drop();
          runtime->emit_event(WebSeverity::Warning, Str{"server"},
                              Str{"ws_ingress"},
                              Str{"WebSocket Open event could not be "
                                  "delivered; closing the connection"},
                              0, true, false, self->ws_connection_id_);
          runtime->unregister_ws_connection(self->ws_connection_id_);
          self->close();
          return;
        }
        // Beast no longer needs the upgrade request; releasing it keeps
        // the single-counted accounting honest for the WebSocket's whole
        // lifetime (review P1).
        self->request_ = http::request<http::string_body>{};
        self->decoded_path_ = std::string{};
        self->ws_read_continue();
      });
  if (tls_stream_.has_value()) {
    tls_ws_.emplace(std::move(*tls_stream_));
    tls_stream_.reset();
    tls_ws_->set_option(timeout);
    tls_ws_->read_message_max(config.ws_max_message_bytes);
    tls_ws_->async_accept(request_, on_accept);
  } else {
    plain_ws_.emplace(std::move(*plain_stream_));
    plain_stream_.reset();
    plain_ws_->set_option(timeout);
    plain_ws_->read_message_max(config.ws_max_message_bytes);
    plain_ws_->async_accept(request_, on_accept);
  }
}

void ServerConnection::ws_read_next() {
  const std::size_t chunk =
      std::min<std::size_t>(64 * 1024, config_->ws_max_message_bytes);
  const auto on_read = asio::bind_executor(
      strand_,
      [self = shared_from_this()](beast::error_code ec, std::size_t received) {
        if (ec) {
          const bool orderly = ec == websocket::error::closed;
          Int close_code = 1005;
          Str close_reason{};
          if (orderly) {
            const auto &reason = self->plain_ws_.has_value()
                                     ? self->plain_ws_->reason()
                                     : self->tls_ws_->reason();
            close_code = static_cast<Int>(reason.code);
            close_reason = Str{std::string{reason.reason.data(),
                                           reason.reason.size()}};
          } else {
            close_code = 1006;
            close_reason = Str{ec.message()};
          }
          self->finish_ws(orderly ? WsConnectionState::Closed
                                  : WsConnectionState::Failed,
                          close_code, std::move(close_reason));
          return;
        }
        static_cast<void>(received);
        const auto data = self->ws_read_buffer_.data();
        self->ws_unaccounted_bytes_ += data.size();
        self->ws_message_.append(static_cast<const char *>(data.data()),
                                 data.size());
        self->ws_read_buffer_.consume(self->ws_read_buffer_.size());
        self->ws_account_chunk();
      });
  if (plain_ws_.has_value()) {
    plain_ws_->async_read_some(ws_read_buffer_, chunk, on_read);
  } else {
    tls_ws_->async_read_some(ws_read_buffer_, chunk, on_read);
  }
}

void ServerConnection::ws_continue_message() {
  const bool done = plain_ws_.has_value() ? plain_ws_->is_message_done()
                                          : tls_ws_->is_message_done();
  if (!done) {
    ws_read_next();
    return;
  }
  const std::shared_ptr<WebServerRuntime> runtime = ws_runtime_;
  const bool text = plain_ws_.has_value() ? plain_ws_->got_text()
                                          : tls_ws_->got_text();
  std::string payload{std::move(ws_message_)};
  ws_message_ = std::string{};
  const auto &b = runtime->bindings();
  const std::size_t frame_bytes = payload.size() + 256 + ws_route_weight_;
  Value frame =
      text ? build_on(b.ws_frame,
                      {
                          {"kind", b.enum_value(WsFrameKind::Text)},
                          {"text", b.string(Str{std::move(payload)})},
                      })
           : build_on(b.ws_frame,
                      {
                          {"kind", b.enum_value(WsFrameKind::Binary)},
                          {"data", b.bytes(Bytes{std::move(payload)})},
                      });
  Value inbound =
      build_on(b.ws_inbound_frame,
               {
                   {"connection_id", b.number(ws_connection_id_)},
                   {"frame", std::move(frame)},
               });
  deliver_ws_ingress(std::move(inbound), frame_bytes);
}

void ServerConnection::deliver_ws_ingress(Value inbound, std::size_t bytes) {
  // Capacity was reserved before the read was armed (review P1), so the
  // A reserved send cannot fail for lack of space; false means graph teardown
  // has stopped admission and the connection is being torn down.
  const std::shared_ptr<WebServerRuntime> runtime = ws_runtime_;
  const std::size_t reserved = ws_reserved_bytes_;
  ws_reserved_bytes_ = 0;
  if (runtime->push_ws_frame_reserved(ws_route_->clone(), ws_generation_,
                                      std::move(inbound), bytes, reserved)) {
    ws_read_continue();
  }
}

void ServerConnection::ws_read_continue() {
  if (shutting_down_) {
    return;
  }
  ws_read_next();
}

// Chunked accounting (review P1): capacity is reserved incrementally as
// data arrives, not up-front for a maximal message — an idle connection
// holds NO standing reservation, so healthy-but-quiet WebSockets cannot
// monopolize the ingress limit.  The only unaccounted memory is one
// in-flight chunk per connection, the same order as the kernel socket
// buffer that precedes it.
void ServerConnection::ws_account_chunk() {
  const std::shared_ptr<WebServerRuntime> runtime = ws_runtime_;
  const std::size_t pending = ws_unaccounted_bytes_;
  // The first chunk of a message takes the record slot and the envelope
  // overhead; later chunks grow the same reservation by bytes only.
  const bool first = ws_reserved_bytes_ == 0;
  const std::size_t wanted =
      first ? pending + 256 + ws_route_weight_ : pending;
  // Absolute oversize rejection: a record that cannot fit even an EMPTY
  // channel (route weight included) must never enter the Backpressure
  // retry loop — it would stall forever (review P1).
  if (ws_reserved_bytes_ + wanted >
      static_cast<std::size_t>(runtime->config().ws_ingress.bytes)) {
    runtime->count_drop();
    runtime->emit_event(WebSeverity::Warning, Str{"server"}, Str{"ws_ingress"},
                        Str{"message cannot fit the WS ingress limit"}, 0,
                        false, false, ws_connection_id_);
    ws_close(websocket::close_code{1009}, "message too big to admit",
             WsConnectionState::Failed);
    return;
  }
  const bool accounted = first ? runtime->reserve_ws_ingress(wanted)
                               : runtime->grow_ws_reservation(wanted);
  if (accounted) {
    ws_reserved_bytes_ += wanted;
    ws_unaccounted_bytes_ = 0;
    ws_continue_message();
    return;
  }
  if (runtime->config().inbound_overflow == WebInboundOverflow::Reject) {
    runtime->count_drop();
    runtime->emit_event(WebSeverity::Warning, Str{"server"}, Str{"ws_ingress"},
                        Str{"WebSocket ingress rejected at the hard limit"}, 0,
                        true, false, ws_connection_id_);
    ws_close(websocket::close_code{1013}, "server is at capacity",
             WsConnectionState::Failed);
    return;
  }
  // Backpressure: hold what is already read and retry the accounting; no
  // further read is armed, so TCP backpressure propagates.  Wake on the
  // watermark resume when a pauser is registered; otherwise poll —
  // reservations occupy capacity without engaging the payload watermark.
  if (listener_->reads_paused(WebListener::ReadTier::Ws)) {
    listener_->park_for_resume(
        WebListener::ReadTier::Ws, [self = shared_from_this()] {
          asio::post(self->strand_, [self] { self->ws_account_chunk(); });
        });
    return;
  }
  auto retry = std::make_shared<asio::steady_timer>(listener_->io_context());
  retry->expires_after(std::chrono::milliseconds{50});
  retry->async_wait(asio::bind_executor(
      strand_, [self = shared_from_this(), retry](beast::error_code ec) {
        if (!ec && !self->shutting_down_) {
          self->ws_account_chunk();
        }
      }));
}

void ServerConnection::release_ws_reservation_held() noexcept {
  if (ws_reserved_bytes_ != 0 && ws_runtime_) {
    ws_runtime_->release_ws_reservation(ws_reserved_bytes_);
    ws_reserved_bytes_ = 0;
  }
  ws_unaccounted_bytes_ = 0;
  ws_message_ = std::string{};
}

void ServerConnection::queue_ws_frame(
    const Value &frame, Int client_id,
    const std::shared_ptr<WebServerRuntime> &runtime) {
  if (!ws_) {
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    runtime->delivery_report(
                        ws_connection_id_, WebDeliveryStatus::PermanentFailure,
                        0, Str{"WebSocket is not connected"}));
    return;
  }
  const auto fields = frame.view().as_bundle();
  const auto kind = fields.at("kind").checked_as<WsFrameKind>();
  if (kind == WsFrameKind::Close) {
    const auto code = fields.at("close_code");
    const auto reason = fields.at("close_reason");
    ws_close(websocket::close_code{static_cast<std::uint16_t>(
                 code.data() != nullptr ? code.checked_as<Int>() : 1000)},
             reason.data() != nullptr
                 ? std::string{reason.checked_as<Str>()}
                 : std::string{},
             WsConnectionState::Closed);
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    runtime->delivery_report(ws_connection_id_,
                                             WebDeliveryStatus::Delivered));
    return;
  }
  if (kind == WsFrameKind::Ping || kind == WsFrameKind::Pong) {
    // Beast's keep-alive pings own the control-frame policy; graph-initiated
    // pings are not part of the v1 surface (RFC 0024).
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    runtime->delivery_report(
                        ws_connection_id_, WebDeliveryStatus::PermanentFailure,
                        0, Str{"control frames are transport-managed"}));
    return;
  }

  const auto &config = runtime->config();
  const auto text_field = fields.at("text");
  const auto data_field = fields.at("data");
  const std::size_t bytes =
      kind == WsFrameKind::Text
          ? (text_field.data() != nullptr
                 ? std::string_view{text_field.checked_as<Str>()}.size()
                 : 0)
          : (data_field.data() != nullptr
                 ? data_field.checked_as<Bytes>().data.size()
                 : 0);
  if (ws_outbound_.size() >= config.outbound_message_limit ||
      ws_outbound_bytes_ + bytes > config.outbound_byte_limit) {
    // The explicit slow-consumer policy (RFC 0024, flow control): Close
    // (1013) or DropNewest; never an unbounded queue.
    if (config.slow_consumer_policy == WebSlowConsumerPolicy::Close) {
      for (const auto &queued : ws_outbound_) {
        queued.runtime->report(
            index(ServerChannel::WsSendDelivery), queued.client_id,
            queued.runtime->delivery_report(ws_connection_id_,
                                            WebDeliveryStatus::Dropped, 0,
                                            Str{"slow consumer"}));
      }
      ws_outbound_.clear();
      ws_outbound_bytes_ = 0;
      ws_close(websocket::close_code{1013}, "slow consumer",
               WsConnectionState::Failed);
    }
    runtime->report(index(ServerChannel::WsSendDelivery), client_id,
                    runtime->delivery_report(ws_connection_id_,
                                             WebDeliveryStatus::Dropped, 0,
                                             Str{"slow consumer"}));
    runtime->count_drop();
    return;
  }
  ws_outbound_.push_back(QueuedWsFrame{frame.clone(), client_id, runtime, bytes});
  ws_outbound_bytes_ += bytes;
  if (!writing_) {
    ws_write_next();
  }
}

void ServerConnection::ws_write_next() {
  if (ws_outbound_.empty()) {
    writing_ = false;
    return;
  }
  writing_ = true;
  const QueuedWsFrame &next = ws_outbound_.front();
  const auto fields = next.frame.view().as_bundle();
  const auto kind = fields.at("kind").checked_as<WsFrameKind>();
  const bool text = kind == WsFrameKind::Text;
  const auto text_field = fields.at("text");
  const auto data_field = fields.at("data");
  const std::string payload =
      text ? (text_field.data() != nullptr
                  ? std::string{text_field.checked_as<Str>()}
                  : std::string{})
           : (data_field.data() != nullptr
                  ? std::string{data_field.checked_as<Bytes>().data}
                  : std::string{});
  ws_send_buffer_ = std::move(payload);
  ws_send_offset_ = 0;
  if (plain_ws_.has_value()) {
    plain_ws_->text(text);
  } else {
    tls_ws_->text(text);
  }
  ws_send_fragment();
}

void ServerConnection::ws_send_fragment() {
  // Outbound messages fragment at the advertised frame cap (RFC 0024,
  // configuration): Beast tracks continuation state across write_some calls.
  const std::size_t cap = std::max<std::size_t>(config_->ws_max_frame_bytes, 1);
  const std::size_t remaining = ws_send_buffer_.size() - ws_send_offset_;
  const std::size_t length = std::min(cap, remaining);
  const bool fin = ws_send_offset_ + length == ws_send_buffer_.size();
  const auto on_write = asio::bind_executor(
      strand_, [self = shared_from_this(), fin,
                length](beast::error_code ec, std::size_t) {
        if (!ec && !fin) {
          self->ws_send_offset_ += length;
          self->ws_send_fragment();
          return;
        }
        QueuedWsFrame sent = std::move(self->ws_outbound_.front());
        self->ws_outbound_.pop_front();
        self->ws_outbound_bytes_ -= sent.bytes;
        sent.runtime->report(
            index(ServerChannel::WsSendDelivery), sent.client_id,
            sent.runtime->delivery_report(
                self->ws_connection_id_,
                ec ? WebDeliveryStatus::PermanentFailure
                   : WebDeliveryStatus::Delivered,
                ec ? ec.value() : 0, ec ? Str{ec.message()} : Str{}));
        if (ec) {
          self->writing_ = false;
          self->finish_ws(WsConnectionState::Failed, 1006, Str{ec.message()});
          return;
        }
        self->ws_write_next();
      });
  const auto fragment = asio::buffer(ws_send_buffer_.data() + ws_send_offset_,
                                     length);
  if (plain_ws_.has_value()) {
    plain_ws_->async_write_some(fin, fragment, on_write);
  } else {
    tls_ws_->async_write_some(fin, fragment, on_write);
  }
}

void ServerConnection::ws_close(websocket::close_code code,
                                beast::string_view reason,
                                WsConnectionState terminal_state) {
  if (ws_close_started_ || ws_terminal_emitted_) {
    return;
  }
  ws_close_started_ = true;
  const Int close_code = static_cast<Int>(code);
  Str close_reason{reason};
  const auto on_close = asio::bind_executor(
      strand_, [self = shared_from_this(), terminal_state, close_code,
                close_reason](beast::error_code ec) mutable {
        if (ec) {
          self->finish_ws(WsConnectionState::Failed, 1006, Str{ec.message()});
        } else {
          self->finish_ws(terminal_state, close_code, std::move(close_reason));
        }
      });
  websocket::close_reason close{code};
  close.reason = reason;
  if (plain_ws_.has_value()) {
    plain_ws_->async_close(close, on_close);
  } else if (tls_ws_.has_value()) {
    tls_ws_->async_close(close, on_close);
  } else {
    finish_ws(WsConnectionState::Failed, 1006,
              Str{"WebSocket transport is unavailable"});
  }
}

void ServerConnection::finish_ws(WsConnectionState state, Int close_code,
                                 Str close_reason) {
  if (ws_terminal_emitted_) {
    close();
    return;
  }
  ws_terminal_emitted_ = true;
  release_ws_reservation_held();

  const std::shared_ptr<WebServerRuntime> runtime = ws_runtime_;
  if (runtime) {
    runtime->unregister_ws_connection(ws_connection_id_);
  }
  if (runtime && ws_route_ != nullptr) {
    const auto &b = runtime->bindings();
    Value terminal =
        build_on(b.ws_event, {
                                 {"connection_id", b.number(ws_connection_id_)},
                                 {"state", b.enum_value(state)},
                                 {"close_code", b.number(close_code)},
                                 {"close_reason", b.string(close_reason)},
                             });
    const std::size_t terminal_bytes =
        close_reason.size() + ws_route_weight_ + 512;
    if (ws_close_reserved_ != 0) {
      const std::size_t reserved = ws_close_reserved_;
      ws_close_reserved_ = 0;
      static_cast<void>(runtime->push_ws_event_reserved(
          ws_route_->clone(), ws_generation_, std::move(terminal),
          terminal_bytes, reserved));
    } else {
      // Only reachable after transport teardown or for a pre-reservation
      // connection; retain the best-effort fallback for orderly cleanup.
      static_cast<void>(runtime->push_ws_event(
          ws_route_->clone(), ws_generation_, std::move(terminal),
          terminal_bytes));
    }
  }
  close();
}

void ServerConnection::release_ws_terminal_reservation() noexcept {
  if (ws_close_reserved_ != 0 && ws_runtime_) {
    ws_runtime_->release_ws_reservation(ws_close_reserved_);
    ws_close_reserved_ = 0;
  }
}

void ServerConnection::close() {
  release_admission();
  release_ws_reservation_held();
  // Pre-Open failures and forced teardown have no graph-visible lifecycle
  // to terminate; return any capacity that was never converted into an
  // event.  Every post-Open transport path calls finish_ws first.
  release_ws_terminal_reservation();
  close_socket_only();
  retire_barrier_.reset();
}

void ServerConnection::close_socket_only() {
  beast::error_code ec;
  if (plain_ws_.has_value()) {
    static_cast<void>(
        beast::get_lowest_layer(*plain_ws_).socket().close(ec));
  } else if (tls_ws_.has_value()) {
    static_cast<void>(
        beast::get_lowest_layer(*tls_ws_).socket().close(ec));
  } else if (tls_stream_.has_value()) {
    static_cast<void>(beast::get_lowest_layer(*tls_stream_).socket().close(ec));
  } else if (plain_stream_.has_value()) {
    static_cast<void>(plain_stream_->socket().close(ec));
  }
}

// ---------------------------------------------------------------------------
// H2Driver implementation

void ServerConnection::start_h2() {
  try {
    auto driver = std::make_shared<H2Driver>(listener_, config_,
                                             std::move(*tls_stream_));
    tls_stream_.reset();
    driver->run();
  } catch (const std::exception &) {
    // An engine construction failure must never escape the handshake
    // handler into io_context::run() (review P2).
    close();
  }
}

void H2Driver::read_next() {
  if (closed_) {
    return;
  }
  if (unaccounted_total_ >=
      static_cast<std::size_t>(config_->h2_initial_window_bytes)) {
    // Enough unadmitted bytes are already buffered: stop reading the
    // connection so TCP backpressure applies until accounting drains.
    read_stalled_ = true;
    return;
  }
  beast::get_lowest_layer(stream_).expires_after(config_->idle_timeout);
  stream_.async_read_some(
      asio::buffer(read_buffer_),
      asio::bind_executor(
          strand_, [self = shared_from_this()](beast::error_code ec,
                                               std::size_t received) {
            if (ec) {
              self->close();
              return;
            }
            if (!self->engine_.receive(
                    std::string_view{self->read_buffer_.data(), received})) {
              // Fatal protocol error: flush the GOAWAY and close.
              self->pump_writes();
              return;
            }
            self->pump_writes();
            self->read_next();
          }));
}

void H2Driver::pump_writes() {
  if (closed_ || writing_) {
    return;
  }
  write_buffer_.clear();
  while (write_buffer_.size() < 64 * 1024) {
    const std::string_view output = engine_.next_output();
    if (output.empty()) {
      break;
    }
    write_buffer_.append(output);
  }
  if (write_buffer_.empty()) {
    // Everything queued so far is on the wire; reports collected while
    // frames were generated are now deliverable.
    flush_reports(pending_flush_reports_.size(), true);
    if (engine_.finished()) {
      // An orderly session end closes with a FIN, never a hard close: the
      // read loop keeps draining to EOF so late peer bytes cannot turn
      // the close into a TCP RST (review; h2spec 7/1 on slow runners).
      shutdown_send();
    }
    return;
  }
  writing_ = true;
  const std::size_t report_batch = pending_flush_reports_.size();
  asio::async_write(
      stream_, asio::buffer(write_buffer_),
      asio::bind_executor(
          strand_,
          [self = shared_from_this(), report_batch](beast::error_code ec,
                                                    std::size_t) {
            self->writing_ = false;
            if (ec) {
              self->close();
              return;
            }
            self->flush_reports(report_batch, true);
            self->pump_writes();
          }));
}

void H2Driver::flush_reports(std::size_t count, bool written) {
  count = std::min(count, pending_flush_reports_.size());
  for (std::size_t position = 0; position != count; ++position) {
    const PendingReport &report = pending_flush_reports_[position];
    if (!report.runtime) {
      continue; // a barrier-only sentinel: released on erase below
    }
    if (!written) {
      report.runtime->report(
          index(ServerChannel::RespondDelivery), report.client_id,
          report.runtime->delivery_report(
              report.request_id, WebDeliveryStatus::PermanentFailure, 0,
              Str{"the connection closed before the response was written"}));
    } else if (report.clean) {
      report.runtime->report(index(ServerChannel::RespondDelivery), report.client_id,
                             report.runtime->delivery_report(
                                 report.request_id,
                                 WebDeliveryStatus::Delivered));
    } else {
      report.runtime->report(
          index(ServerChannel::RespondDelivery), report.client_id,
          report.runtime->delivery_report(
              report.request_id, WebDeliveryStatus::Dropped, 0,
              Str{"the stream was reset before delivery"}));
    }
  }
  pending_flush_reports_.erase(pending_flush_reports_.begin(),
                               pending_flush_reports_.begin() +
                                   static_cast<std::ptrdiff_t>(count));
}

void H2Driver::shutdown_send() {
  if (closed_ || sent_fin_) {
    return;
  }
  sent_fin_ = true;
  beast::error_code ec;
  static_cast<void>(beast::get_lowest_layer(stream_).socket().shutdown(
      asio::ip::tcp::socket::shutdown_send, ec));
}

void H2Driver::close() {
  if (closed_) {
    return;
  }
  // Answers whose streams never closed report as write failures, then
  // everything already queued flushes as failed too (review P1).
  for (auto &[stream_id, stream] : streams_) {
    if (stream.response_submitted && stream.report_runtime) {
      queue_report(std::move(stream.report_runtime), stream.report_client_id,
                   stream.report_request_id, false);
    }
    release_stream(stream_id, stream);
  }
  flush_reports(pending_flush_reports_.size(), false);
  closed_ = true;
  streams_.clear();
  request_to_stream_.clear();
  beast::error_code ec;
  static_cast<void>(beast::get_lowest_layer(stream_).socket().close(ec));
}

void H2Driver::discard_stream_input(std::int32_t stream_id, Stream &stream) {
  // DATA is flow-controlled even when the request is rejected or reset.
  // Returning every byte the application stops retaining is what keeps a
  // discarded stream from permanently shrinking the shared connection
  // window (review P1).
  if (stream.unaccounted != 0) {
    engine_.discard(stream_id, stream.unaccounted);
  }

  // Subtract exactly what this stream contributed: header bytes only while
  // their flag is set (early-error streams refused before counting contribute
  // nothing).  Trailer metadata is retained but not flow-controlled.
  const std::size_t header_part =
      stream.headers_counted ? stream.header_bytes + stream.target.size() : 0;
  const std::size_t buffered =
      stream.unaccounted + stream.trailer_unaccounted + header_part;
  if (buffered != 0) {
    unaccounted_total_ -= std::min(unaccounted_total_, buffered);
  }

  stream.unaccounted = 0;
  stream.trailer_unaccounted = 0;
  stream.headers_counted = false;
  stream.header_bytes = 0;
  stream.trailer_bytes = 0;
  stream.body = std::string{};
  stream.headers = H2Headers{};
  stream.trailers = H2Headers{};
  stream.target = std::string{};
  stream.decoded_path = std::string{};
  stream.matched.params = {};
  stream.matched.route = nullptr;
  stream.matched.snapshot.reset();
  resume_stalled_read();
}

void H2Driver::release_stream(std::int32_t stream_id, Stream &stream) {
  if (stream.request_id >= 0 && stream.matched.runtime) {
    stream.matched.runtime->unregister_pending(stream.request_id);
    request_to_stream_.erase(stream.request_id);
    stream.request_id = -1;
  }
  discard_stream_input(stream_id, stream);
  if (stream.reserved != 0 && stream.matched.runtime) {
    stream.matched.runtime->release_request_reservation(stream.reserved);
    stream.reserved = 0;
  }
  stream.matched.runtime.reset();
  stream.admitted = false;
  stream.admission_in_flight = false;
}

void H2Driver::on_request_headers(std::int32_t stream_id, std::string method,
                                  std::string target, H2Headers headers,
                                  bool end_stream) {
  if (shutting_down_) {
    engine_.reset_stream(stream_id, H2StreamError::RefusedStream);
    return;
  }
  Stream stream;
  stream.target = std::move(target);
  stream.headers = std::move(headers);
  stream.end_stream_seen = end_stream;
  for (const auto &[name, value] : stream.headers) {
    stream.header_bytes += name.size() + value.size();
  }
  // The connection metadata bound is HARD inside a single receive(): one
  // socket read can decode many HPACK-compressed blocks, so a block that
  // would cross the bound is refused before it is retained, and EVERY
  // retained stream — early-error paths included — is accounted the same
  // way (review P1).
  const std::size_t header_part = stream.header_bytes + stream.target.size();
  if (unaccounted_total_ + header_part >
      static_cast<std::size_t>(config_->h2_initial_window_bytes)) {
    engine_.reset_stream(stream_id, H2StreamError::RefusedStream);
    return;
  }
  unaccounted_total_ += header_part;
  stream.headers_counted = true;
  const auto method_value = method_from_token(method);
  if (!method_value.has_value()) {
    streams_.emplace(stream_id, std::move(stream));
    respond_transport(stream_id, 501, "unsupported method");
    return;
  }
  stream.method = *method_value;

  const std::string_view full_target{stream.target};
  const auto query_start = full_target.find('?');
  const std::string_view path = full_target.substr(0, query_start);
  auto decoded_path = RouteTable::decode_path(path);
  if (!decoded_path.has_value()) {
    streams_.emplace(stream_id, std::move(stream));
    respond_transport(stream_id, 400, "malformed percent-encoding");
    return;
  }
  stream.decoded_path = std::move(*decoded_path);

  if (stream.method == HttpMethod::Get) {
    if (const auto static_file =
            listener_->match_static(HttpMethod::Get, stream.decoded_path)) {
      streams_.emplace(stream_id, std::move(stream));
      respond_static(stream_id, *static_file, false);
      return;
    }
    auto matched = listener_->match(HttpMethod::Get, path, false);
    if (!matched.has_value()) {
      streams_.emplace(stream_id, std::move(stream));
      respond_transport(stream_id, 404, "no such route");
      return;
    }
    stream.matched = std::move(*matched);
    streams_.emplace(stream_id, std::move(stream));
    admit_stream(stream_id);
    return;
  }

  if (stream.method == HttpMethod::Head) {
    auto matched = listener_->match(HttpMethod::Head, path, false);
    if (matched.has_value()) {
      stream.matched = std::move(*matched);
      streams_.emplace(stream_id, std::move(stream));
      admit_stream(stream_id);
      return;
    }
    if (const auto static_file =
            listener_->match_static(HttpMethod::Head, stream.decoded_path)) {
      streams_.emplace(stream_id, std::move(stream));
      respond_static(stream_id, *static_file, true);
      return;
    }
    // Standard HTTP: HEAD is GET without the body (transport suppresses
    // the body on the way out), exactly like the h1 path.
    matched = listener_->match(HttpMethod::Get, path, false);
    stream.head_fallback = matched.has_value();
    if (!matched.has_value()) {
      streams_.emplace(stream_id, std::move(stream));
      respond_transport(stream_id, 404, "no such route");
      return;
    }
    stream.matched = std::move(*matched);
    streams_.emplace(stream_id, std::move(stream));
    admit_stream(stream_id);
    return;
  }

  auto matched = listener_->match(stream.method, path, false);
  if (!matched.has_value()) {
    streams_.emplace(stream_id, std::move(stream));
    respond_transport(stream_id, 404, "no such route");
    return;
  }
  stream.matched = std::move(*matched);
  streams_.emplace(stream_id, std::move(stream));
  admit_stream(stream_id);
}

void H2Driver::admit_stream(std::int32_t stream_id) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end() || closed_) {
    return;
  }
  Stream &stream = found->second;
  if (stream.discarding) {
    // Already answered by the transport (a retiring runtime's 503): the
    // admission retry loop ends here instead of spinning against a
    // stopped admission budget (review P1).
    return;
  }
  stream.admission_in_flight = false;
  const std::shared_ptr<WebServerRuntime> runtime = stream.matched.runtime;
  // Header admission mirrors h1: the header block plus envelope overhead
  // reserves before any DATA window is released; the body then grows the
  // same reservation chunk-by-chunk (RFC 0024, flow control).
  // The derived sizes are already known at admission — decoded path,
  // query slice, and the MATCHED parameter names and values (route
  // patterns can carry capture names with no length relation to the
  // target) — so the projection uses them exactly instead of a
  // target-multiplier bound (review P2).
  const std::string_view target_view{stream.target};
  const auto admission_query = target_view.find('?');
  const std::size_t query_size =
      admission_query == std::string_view::npos
          ? 0
          : target_view.size() - admission_query - 1;
  std::size_t params_bytes = 0;
  for (const auto &[name, value] : stream.matched.params) {
    params_bytes += name.size() + value.size();
  }
  const std::size_t projected = stream.header_bytes + stream.target.size() +
                                stream.decoded_path.size() + query_size +
                                params_bytes +
                                route_weight(*stream.matched.route) + 512;
  // Absolute oversize rejection: never park what can never fit
  // (review P1).
  if (projected > runtime->config().ingress.bytes) {
    respond_transport(stream_id, 413, "request cannot fit the ingress limit");
    return;
  }
  if (runtime->reserve_request(projected)) {
    stream.admitted = true;
    stream.reserved = projected;
    if (stream.headers_counted) {
      stream.headers_counted = false;
      unaccounted_total_ -= std::min(
          unaccounted_total_, stream.header_bytes + stream.target.size());
      resume_stalled_read();
    }
    account_stream_data(stream_id);
    return;
  }
  if (runtime->config().inbound_overflow == WebInboundOverflow::Reject) {
    respond_transport(stream_id, 503, "server is at capacity");
    return;
  }
  // Backpressure: no window is released, so the sender stalls; retry on
  // the watermark resume or a timer, exactly like the h1 admission wait.
  stream.admission_in_flight = true;
  if (listener_->reads_paused(WebListener::ReadTier::Http)) {
    listener_->park_for_resume(
        WebListener::ReadTier::Http, [self = shared_from_this(), stream_id] {
          asio::post(self->strand_,
                     [self, stream_id] { self->admit_stream(stream_id); });
        });
    return;
  }
  auto retry = std::make_shared<asio::steady_timer>(listener_->io_context());
  retry->expires_after(std::chrono::milliseconds{50});
  retry->async_wait(asio::bind_executor(
      strand_,
      [self = shared_from_this(), stream_id, retry](beast::error_code ec) {
        if (!ec && !self->closed_) {
          self->admit_stream(stream_id);
        }
      }));
}

void H2Driver::on_request_data(std::int32_t stream_id, std::string_view data,
                               bool end_stream) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end()) {
    // A stream answered by the transport (404/503/...): release the
    // window immediately so a still-sending peer cannot wedge the
    // connection; the bytes are never retained.
    engine_.discard(stream_id, data.size());
    return;
  }
  Stream &stream = found->second;
  if (stream.discarding) {
    engine_.discard(stream_id, data.size());
    return;
  }
  if (stream.body.size() + data.size() >
      stream.matched.runtime->config().max_body_bytes) {
    // The per-request cap, the h2 mirror of Beast's body_limit.  The
    // just-arrived chunk was never buffered, so its window is released
    // here; respond_transport restores the rest and discards uniformly
    // (review P1).
    engine_.discard(stream_id, data.size());
    respond_transport(stream_id, 413, "body exceeds max_body_bytes");
    return;
  }
  stream.body.append(data);
  stream.unaccounted += data.size();
  unaccounted_total_ += data.size();
  if (end_stream) {
    stream.end_stream_seen = true;
  }
  if (stream.admitted) {
    account_stream_data(stream_id);
  }
}

void H2Driver::on_request_trailers(std::int32_t stream_id,
                                   H2Headers trailers, bool end_stream) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end()) {
    return;
  }
  Stream &stream = found->second;
  if (stream.discarding) {
    return;
  }
  std::size_t bytes = 0;
  for (const auto &[name, value] : trailers) {
    bytes += name.size() + value.size();
  }
  // The hard per-receive metadata bound applies to trailer blocks exactly
  // as to initial headers: an over-bound block kills the stream before it
  // is retained (review P1).  Unlike a refused-at-open stream, this one
  // was partially consumed, so REFUSED_STREAM's safe-retry promise would
  // be wrong.
  if (unaccounted_total_ + bytes >
      static_cast<std::size_t>(config_->h2_initial_window_bytes)) {
    release_stream(stream_id, stream);
    stream.discarding = true;
    engine_.reset_stream(stream_id, H2StreamError::EnhanceYourCalm);
    return;
  }
  stream.trailers = std::move(trailers);
  stream.trailer_bytes += bytes;
  stream.trailer_unaccounted += bytes;
  unaccounted_total_ += bytes;
  if (end_stream) {
    stream.end_stream_seen = true;
  }
  if (stream.admitted) {
    account_stream_data(stream_id);
  }
}

void H2Driver::resume_stalled_read() {
  if (read_stalled_ &&
      unaccounted_total_ <
          static_cast<std::size_t>(config_->h2_initial_window_bytes)) {
    read_stalled_ = false;
    read_next();
  }
}

void H2Driver::account_stream_data(std::int32_t stream_id) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end() || closed_) {
    return;
  }
  Stream &stream = found->second;
  if (stream.discarding) {
    return;
  }
  if (stream.unaccounted != 0 || stream.trailer_unaccounted != 0) {
    // Trailer bytes grow the reservation but never consume flow-control
    // window — HEADERS frames are not flow-controlled (RFC 9113 §6.9).
    const std::size_t data_pending = stream.unaccounted;
    // Raw buffered bytes and the reservation growth are the same amount
    // now that transport copies are released at dispatch: the graph copy
    // is the only survivor, single-counted (review P1).
    const std::size_t pending = data_pending + stream.trailer_unaccounted;
    if (stream.reserved + pending >
        stream.matched.runtime->config().ingress.bytes) {
      // Incremental growth that can NEVER fit: respond_transport restores
      // the held window and discards the buffers uniformly (review P1).
      respond_transport(stream_id, 413,
                        "request cannot fit the ingress limit");
      return;
    }
    if (stream.matched.runtime->grow_request_reservation(pending)) {
      stream.reserved += pending;
      stream.unaccounted = 0;
      stream.trailer_unaccounted = 0;
      unaccounted_total_ -= std::min(unaccounted_total_, pending);
      if (data_pending != 0) {
        engine_.consume(stream_id, data_pending);
        pump_writes(); // the WINDOW_UPDATE this consume released
      }
      resume_stalled_read();
    } else if (stream.matched.runtime->config().inbound_overflow ==
               WebInboundOverflow::Reject) {
      respond_transport(stream_id, 503, "server is at capacity");
      return;
    } else {
      // Backpressure: the unaccounted bytes stay window-unreleased, so
      // the sender stalls; retry on the watermark resume or a timer.
      if (listener_->reads_paused(WebListener::ReadTier::Http)) {
        listener_->park_for_resume(
            WebListener::ReadTier::Http,
            [self = shared_from_this(), stream_id] {
              asio::post(self->strand_, [self, stream_id] {
                self->account_stream_data(stream_id);
              });
            });
        return;
      }
      auto retry =
          std::make_shared<asio::steady_timer>(listener_->io_context());
      retry->expires_after(std::chrono::milliseconds{50});
      retry->async_wait(asio::bind_executor(
          strand_, [self = shared_from_this(), stream_id,
                    retry](beast::error_code ec) {
            if (!ec && !self->closed_) {
              self->account_stream_data(stream_id);
            }
          }));
      return;
    }
  }
  maybe_dispatch(stream_id);
}

void H2Driver::maybe_dispatch(std::int32_t stream_id) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end()) {
    return;
  }
  Stream &stream = found->second;
  if (!stream.admitted || !stream.end_stream_seen ||
      stream.unaccounted != 0 || stream.request_id >= 0) {
    return;
  }
  const std::shared_ptr<WebServerRuntime> runtime = stream.matched.runtime;
  const auto &b = runtime->bindings();
  if (connection_id_ < 0) {
    connection_id_ = runtime->allocate_connection_id();
  }

  const std::string_view full_target{stream.target};
  const auto query_start = full_target.find('?');
  const std::string_view query =
      query_start == std::string_view::npos
          ? std::string_view{}
          : full_target.substr(query_start + 1);
  WebBindings::NamedPairs headers{stream.headers.begin(),
                                  stream.headers.end()};
  // The EXACT derived sizes, not multipliers: every owned copy in the
  // graph value is measurable here, and the transport copies are released
  // right after the value is built, so single-counting bounds retained
  // memory (review P1).
  std::size_t params_bytes = 0;
  for (const auto &[name, value] : stream.matched.params) {
    params_bytes += name.size() + value.size();
  }
  const std::size_t retained =
      stream.body.size() + stream.header_bytes + stream.trailer_bytes +
      stream.target.size() + stream.decoded_path.size() + query.size() +
      params_bytes + route_weight(*stream.matched.route) + 512;
  Value request = build_on(
      b.http_request,
      {
          {"method", b.enum_value(stream.head_fallback ? HttpMethod::Head
                                                       : stream.method)},
          {"target", b.string(Str{stream.target})},
          {"path", b.string(Str{stream.decoded_path})},
          {"query", b.params(parse_query(query))},
          {"path_params", b.params(materialize_params(stream.matched.params))},
          {"headers", b.headers(headers)},
          {"body", b.bytes(Bytes{std::move(stream.body)})},
          {"trailers",
           b.headers(WebBindings::NamedPairs{stream.trailers.begin(),
                                             stream.trailers.end()})},
      });
  stream.body = std::string{};
  // The graph value owns its copies now; drop the transport's so the
  // retained estimate above single-counts everything still alive.
  stream.headers = H2Headers{};
  stream.trailers = H2Headers{};
  stream.decoded_path = std::string{};
  stream.matched.params.clear();
  stream.matched.params.shrink_to_fit();
  const Int request_id = runtime->register_pending(shared_from_this());
  Value server_request =
      build_on(b.server_request,
               {
                   {"request_id", b.number(request_id)},
                   {"connection_id", b.number(connection_id_)},
                   {"stream_id", b.number(Int{stream_id})},
                   {"request", std::move(request)},
                   {"peer", peer_value(b)},
               });
  const std::size_t reserved = stream.reserved;
  stream.reserved = 0;
  const bool pushed = runtime->push_request_reserved(
      stream.matched.route->clone(), stream.matched.generation,
      std::move(server_request), retained, reserved);
  if (!pushed) {
    runtime->unregister_pending(request_id);
    respond_transport(stream_id, 503, "server shutting down");
    return;
  }
  stream.target = std::string{};
  stream.matched.route = nullptr;
  stream.matched.snapshot.reset();
  stream.request_id = request_id;
  request_to_stream_[request_id] = stream_id;
}

void H2Driver::respond_transport(std::int32_t stream_id, int status,
                                 std::string_view body) {
  const auto found = streams_.find(stream_id);
  if (found != streams_.end()) {
    Stream &stream = found->second;
    // One release path restores unconsumed DATA credit and drops every
    // retained request buffer before giving the admission reservation back.
    // Trailer-overflow resets, peer resets, transport answers, and shutdown
    // all share the same invariant (review P1).
    release_stream(stream_id, stream);
    stream.discarding = true;
  }
  // Transport answers obey the same outbound budgets as graph responses:
  // a slow peer accumulating error responses is reset instead of growing
  // buffered metadata past the configured bounds (review P1).
  const std::size_t weight = body.size() + 256;
  if (outstanding_response_bytes_ + weight >
          static_cast<std::size_t>(config_->outbound_byte_limit) ||
      outstanding_response_messages_ >=
          static_cast<std::size_t>(config_->outbound_message_limit)) {
    engine_.reset_stream(stream_id, H2StreamError::EnhanceYourCalm);
    pump_writes();
    return;
  }
  if (engine_.submit_response(stream_id, status,
                              H2Headers{{"content-type", "text/plain"}},
                              std::string{body}, {}) &&
      found != streams_.end()) {
    outstanding_response_bytes_ += weight;
    ++outstanding_response_messages_;
    found->second.response_bytes += weight;
    found->second.response_submitted = true;
  }
  pump_writes();
}

void H2Driver::respond_static(std::int32_t stream_id,
                              const MatchedStaticFile &file,
                              bool suppress_body) {
  ResolvedStaticFile resolved =
      file.file != nullptr
          ? resolve_static_file(
                StaticFileTarget{std::string{file.file->file},
                                 std::string{file.file->content_type},
                                 std::string{file.file->cache_control}})
          : load_static_directory_file(*file.directory, file.path);
  const std::size_t content_length =
      resolved.file.empty() ? resolved.body.size() : resolved.size;
  const auto found = streams_.find(stream_id);
  if (found != streams_.end()) {
    Stream &stream = found->second;
    release_stream(stream_id, stream);
    stream.discarding = true;
  }

  std::string body =
      suppress_body || !resolved.file.empty() ? std::string{}
                                              : std::move(resolved.body);
  H2Headers headers{{"content-type", resolved.content_type}};
  if (resolved.status == http::status::ok || suppress_body) {
    headers.emplace_back("content-length", std::to_string(content_length));
  }
  if (!resolved.cache_control.empty()) {
    headers.emplace_back("cache-control", resolved.cache_control);
  }

  const std::size_t buffered_body =
      resolved.file.empty() ? body.size() : std::min<std::size_t>(64 * 1024, content_length);
  std::size_t weight = buffered_body + 256;
  for (const auto &[name, value] : headers) {
    weight += name.size() + value.size();
  }
  if (outstanding_response_bytes_ + weight >
          static_cast<std::size_t>(config_->outbound_byte_limit) ||
      outstanding_response_messages_ >=
          static_cast<std::size_t>(config_->outbound_message_limit)) {
    engine_.reset_stream(stream_id, H2StreamError::EnhanceYourCalm);
    pump_writes();
    return;
  }

  const bool submitted =
      !suppress_body && !resolved.file.empty() &&
              resolved.status == http::status::ok
          ? engine_.submit_file_response(stream_id, static_cast<int>(resolved.status),
                                         headers, std::move(resolved.file), {})
          : engine_.submit_response(stream_id, static_cast<int>(resolved.status),
                                    headers, std::move(body), {});
  if (submitted) {
    if (found != streams_.end()) {
      outstanding_response_bytes_ += weight;
      ++outstanding_response_messages_;
      found->second.response_bytes += weight;
      found->second.response_submitted = true;
    }
  } else {
    // The submit can fail after the metadata resolved -- a configured file
    // that has since been removed or become unreadable is the ordinary case.
    // Something must still finish the stream: returning here would leave the
    // peer waiting on it forever, where the HTTP/1 path answers 500.
    static constexpr std::string_view kStaticFileError{"static file error"};
    const bool answered = engine_.submit_response(
        stream_id, static_cast<int>(http::status::internal_server_error),
        H2Headers{{"content-type", "text/plain"}},
        std::string{kStaticFileError}, {});
    if (answered) {
      if (found != streams_.end()) {
        const std::size_t error_weight = kStaticFileError.size() + 256;
        outstanding_response_bytes_ += error_weight;
        ++outstanding_response_messages_;
        found->second.response_bytes += error_weight;
        found->second.response_submitted = true;
      }
    } else {
      // Even the fallback was refused; reset rather than leak the stream.
      engine_.reset_stream(stream_id, H2StreamError::InternalError);
    }
  }
  pump_writes();
}

void H2Driver::write_stream_response(
    Int request_id, const Value &response, Int client_id,
    const std::shared_ptr<WebServerRuntime> &runtime) {
  const auto found = request_to_stream_.find(request_id);
  if (found == request_to_stream_.end()) {
    // The peer reset the stream before the graph answered: the late
    // answer is Dropped, never written to a dead stream (h2 acceptance
    // criteria).
    runtime->report(index(ServerChannel::RespondDelivery), client_id,
                    runtime->delivery_report(
                        request_id, WebDeliveryStatus::Dropped, 0,
                        Str{"the stream was cancelled by the peer"}));
    return;
  }
  const std::int32_t stream_id = found->second;
  request_to_stream_.erase(found);
  const auto stream_found = streams_.find(stream_id);
  // HTTP HEAD semantics apply to every HEAD request, explicit route or
  // GET fallback alike (review P2).
  const bool head = stream_found != streams_.end() &&
                    stream_found->second.method == HttpMethod::Head;
  if (stream_found != streams_.end()) {
    stream_found->second.request_id = -1;
  }

  const auto fields = response.view().as_bundle();
  const auto status =
      static_cast<int>(fields.at("status").checked_as<Int>());
  std::string body;
  const auto body_field = fields.at("body");
  if (!head && body_field.data() != nullptr) {
    body = std::string{body_field.checked_as<Bytes>().data};
  }
  H2Headers headers;
  const auto header_fields = fields.at("headers");
  if (header_fields.data() != nullptr) {
    for (const auto header : header_fields.as_list()) {
      const auto pair = header.as_bundle();
      headers.emplace_back(std::string{pair.at("name").checked_as<Str>()},
                           std::string{pair.at("value").checked_as<Str>()});
    }
  }
  H2Headers trailers;
  const auto trailer_fields = fields.at("trailers");
  if (!head && trailer_fields.data() != nullptr) {
    for (const auto trailer : trailer_fields.as_list()) {
      const auto pair = trailer.as_bundle();
      trailers.emplace_back(std::string{pair.at("name").checked_as<Str>()},
                            std::string{pair.at("value").checked_as<Str>()});
    }
  }

  // Per-stream slow-consumer policy: a response that would push the
  // buffered outbound share past the configured byte OR message limits
  // resets JUST this stream.  The weight covers headers and trailers, so
  // metadata-heavy or empty-body responses cannot bypass the bounds
  // (review P1).
  std::size_t response_weight = body.size() + 256;
  for (const auto &[name, value] : headers) {
    response_weight += name.size() + value.size();
  }
  for (const auto &[name, value] : trailers) {
    response_weight += name.size() + value.size();
  }
  if (outstanding_response_bytes_ + response_weight >
          static_cast<std::size_t>(config_->outbound_byte_limit) ||
      outstanding_response_messages_ >=
          static_cast<std::size_t>(config_->outbound_message_limit)) {
    engine_.reset_stream(stream_id, H2StreamError::EnhanceYourCalm);
    pump_writes();
    runtime->report(index(ServerChannel::RespondDelivery), client_id,
                    runtime->delivery_report(
                        request_id, WebDeliveryStatus::Dropped, 0,
                        Str{"slow consumer: the stream was reset"}));
    return;
  }

  if (engine_.submit_response(stream_id, status, headers, std::move(body),
                              trailers)) {
    outstanding_response_bytes_ += response_weight;
    ++outstanding_response_messages_;
    if (stream_found != streams_.end()) {
      // Delivery is reported from the write pump once the stream closes
      // and its bytes have left the connection — never at enqueue
      // (review P1; the RFC separates acceptance from delivery).
      Stream &stream = stream_found->second;
      stream.response_bytes += response_weight;
      stream.response_submitted = true;
      stream.report_client_id = client_id;
      stream.report_request_id = request_id;
      stream.report_runtime = runtime;
    }
  } else {
    runtime->report(index(ServerChannel::RespondDelivery), client_id,
                    runtime->delivery_report(
                        request_id, WebDeliveryStatus::PermanentFailure, 0,
                        Str{"the stream could not accept the response"}));
  }
  pump_writes();
}

void H2Driver::finish_stream_transport(Int request_id, int status,
                                       std::string_view body) {
  const auto found = request_to_stream_.find(request_id);
  if (found == request_to_stream_.end()) {
    return;
  }
  const std::int32_t stream_id = found->second;
  request_to_stream_.erase(found);
  const auto stream_found = streams_.find(stream_id);
  if (stream_found != streams_.end()) {
    stream_found->second.request_id = -1;
  }
  respond_transport(stream_id, status, body);
}

void H2Driver::on_stream_reset(std::int32_t stream_id,
                               std::uint32_t error_code) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end()) {
    return;
  }
  // Per-stream cancellation: exactly this request is retired; a late
  // graph answer will be reported Dropped (h2 acceptance criteria).
  found->second.close_error = error_code;
  release_stream(stream_id, found->second);
}

void H2Driver::on_stream_closed(std::int32_t stream_id) {
  const auto found = streams_.find(stream_id);
  if (found == streams_.end()) {
    return;
  }
  Stream &stream = found->second;
  // The buffered-outbound budgets free as streams retire (review P1).
  outstanding_response_bytes_ -=
      std::min(outstanding_response_bytes_, stream.response_bytes);
  if (stream.response_submitted && outstanding_response_messages_ != 0) {
    --outstanding_response_messages_;
  }
  if (stream.response_submitted && stream.report_runtime) {
    queue_report(std::move(stream.report_runtime), stream.report_client_id,
                 stream.report_request_id, stream.close_error == 0);
    pending_flush_reports_.back().barrier = std::move(stream.retire_barrier);
  } else if (stream.retire_barrier) {
    // No report to carry it: a barrier-only entry releases once the
    // frames that closed this stream have been written.
    pending_flush_reports_.push_back(
        PendingReport{nullptr, 0, 0, true, std::move(stream.retire_barrier)});
  }
  release_stream(stream_id, stream);
  streams_.erase(found);
}

Value H2Driver::peer_value(const WebBindings &b) {
  beast::error_code ec;
  auto &socket = beast::get_lowest_layer(stream_).socket();
  const auto remote = socket.remote_endpoint(ec);
  const auto local = socket.local_endpoint(ec);
  Str negotiated{"h2"};
  Str sni{};
  Str subject{};
  SSL *ssl = stream_.native_handle();
  if (const char *name = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name)) {
    sni = Str{name};
  }
  if (X509 *cert = SSL_get_peer_certificate(ssl)) {
    char buffer[512];
    X509_NAME_oneline(X509_get_subject_name(cert), buffer, sizeof(buffer));
    subject = Str{buffer};
    X509_free(cert);
  }
  return build_on(
      b.peer,
      {
          {"remote_address",
           b.string(Str{ec ? std::string{} : remote.address().to_string()})},
          {"remote_port", b.number(Int{ec ? 0 : remote.port()})},
          {"local_port", b.number(Int{ec ? 0 : local.port()})},
          {"tls", b.flag(Bool{true})},
          {"negotiated_protocol", b.string(negotiated)},
          {"sni", b.string(sni)},
          {"client_cert_subject", b.string(subject)},
      });
}
} // namespace

// ---------------------------------------------------------------------------
// Graph-scoped runtime resource + service implementation

namespace {
class WebServerRuntimeResource {
public:
  void install(std::shared_ptr<WebServerRuntime> runtime) {
    std::lock_guard lock{mutex_};
    if (runtime_) {
      throw std::logic_error("Web server runtime was installed twice");
    }
    runtime_ = std::move(runtime);
  }

  [[nodiscard]] std::shared_ptr<WebServerRuntime> get() const {
    std::lock_guard lock{mutex_};
    return runtime_;
  }

  [[nodiscard]] std::shared_ptr<WebServerRuntime> take() noexcept {
    std::lock_guard lock{mutex_};
    return std::exchange(runtime_, {});
  }

private:
  mutable std::mutex mutex_{};
  std::shared_ptr<WebServerRuntime> runtime_{};
};

struct WebServerRuntimeHandle {
  std::shared_ptr<WebServerRuntimeResource> value{};

  friend bool operator==(const WebServerRuntimeHandle &,
                         const WebServerRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const WebServerRuntimeHandle &lhs,
              const WebServerRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const WebServerRuntimeHandle &value) {
  return stream << "WebServerRuntimeHandle(" << value.value.get() << ')';
}
} // namespace
} // namespace hgraph::web::detail

namespace std {
template <> struct hash<hgraph::web::detail::WebServerRuntimeHandle> {
  size_t operator()(const hgraph::web::detail::WebServerRuntimeHandle &value)
      const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::detail::WebServerRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::WebServerRuntimeHandle"};
};

} // namespace hgraph::static_schema_detail

namespace hgraph::web {
namespace {
namespace wd = ::hgraph::web::detail;

[[nodiscard]] std::shared_ptr<wd::WebServerRuntime>
live_server_runtime(Scalar<"runtime", wd::WebServerRuntimeHandle> runtime) {
  if (!runtime.value().value) {
    throw std::logic_error("Web server runtime resource is not configured");
  }
  auto value = runtime.value().value->get();
  if (!value) {
    throw std::logic_error("Web server command evaluated before runtime start");
  }
  return value;
}

/** Applies route deltas to the transport's immutable routing snapshot.
 *  Cost is O(A + R) plus route-table rebuild for additions A/removals R. */
struct WebServerHttpRouteSink {
  static constexpr auto name = "web_server_http_routes";

  static void eval(
      In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
      In<"generations", TSD<WebRoute, TS<DateTime>>,
         InputValidity::Unchecked> generations,
      Scalar<"runtime", wd::WebServerRuntimeHandle> runtime) {
    if (!routes.modified()) {
      return;
    }
    const auto &erased = static_cast<const TSSInputView &>(routes);
    std::vector<Value> removed;
    for (const auto route : erased.removed()) {
      removed.push_back(route.clone());
    }
    const auto &generation_values =
        static_cast<const TSDInputView &>(generations);
    std::vector<wd::SubscriptionBinding> added;
    for (const auto route : erased.added()) {
      const auto generation = generation_values.at(route);
      if (!generation.valid()) {
        throw std::logic_error("Web HTTP route generation is unavailable");
      }
      added.push_back(wd::SubscriptionBinding{
          route.clone(), generation.value().checked_as<DateTime>()});
    }
    live_server_runtime(runtime)->apply_http_routes(std::move(added),
                                                    std::move(removed));
  }
};

/** Applies WebSocket route deltas to the transport routing snapshot.
 *  Cost is O(A + R) plus route-table rebuild. */
struct WebServerWsRouteSink {
  static constexpr auto name = "web_server_ws_routes";

  static void eval(
      In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
      In<"generations", TSD<WebRoute, TS<DateTime>>,
         InputValidity::Unchecked> generations,
      Scalar<"runtime", wd::WebServerRuntimeHandle> runtime) {
    if (!routes.modified()) {
      return;
    }
    const auto &erased = static_cast<const TSSInputView &>(routes);
    std::vector<Value> removed;
    for (const auto route : erased.removed()) {
      removed.push_back(route.clone());
    }
    const auto &generation_values =
        static_cast<const TSDInputView &>(generations);
    std::vector<wd::SubscriptionBinding> added;
    for (const auto route : erased.added()) {
      const auto generation = generation_values.at(route);
      if (!generation.valid()) {
        throw std::logic_error("WebSocket route generation is unavailable");
      }
      added.push_back(wd::SubscriptionBinding{
          route.clone(), generation.value().checked_as<DateTime>()});
    }
    live_server_runtime(runtime)->apply_ws_routes(std::move(added),
                                                  std::move(removed));
  }
};

/** Dispatches graph-produced HTTP responses to their owning connection.
 *  Cost is O(M) for M modified response requests. */
struct WebServerRespondSink {
  static constexpr auto name = "web_server_respond";

  static void eval(
      In<"responses", TSD<Int, HttpRespondRequest>, InputValidity::Unchecked>
          responses,
      Scalar<"runtime", wd::WebServerRuntimeHandle> runtime) {
    if (!responses.modified()) {
      return;
    }
    auto task = live_server_runtime(runtime);
    for (const auto &[client_id, request] : responses.modified_items()) {
      auto response = request.template field<"response">();
      auto request_id = request.template field<"request_id">();
      if (!response.modified() || !response.valid()) {
        continue;
      }
      if (!request_id.valid()) {
        throw std::invalid_argument("Web respond requires a valid request id");
      }
      task->respond(client_id.template checked_as<Int>(), request_id.value(),
                    response.base().value().clone());
    }
  }
};

/** Dispatches graph-produced WebSocket frames. Cost is O(M) for M modified
 *  sends. */
struct WebServerWsSendSink {
  static constexpr auto name = "web_server_ws_send";

  static void eval(
      In<"ws_sends", TSD<Int, WsSendRequest>, InputValidity::Unchecked>
          ws_sends,
      Scalar<"runtime", wd::WebServerRuntimeHandle> runtime) {
    if (ws_sends.modified()) {
      auto task = live_server_runtime(runtime);
      for (const auto &[client_id, request] : ws_sends.modified_items()) {
        auto frame = request.template field<"frame">();
        auto connection_id = request.template field<"connection_id">();
        if (!frame.modified() || !frame.valid()) {
          continue;
        }
        if (!connection_id.valid()) {
          throw std::invalid_argument(
              "Web WS send requires a valid connection id");
        }
        task->ws_send(client_id.template checked_as<Int>(),
                      connection_id.value(), frame.base().value().clone());
      }
    }
  }
};

struct WebServerTransportTag {};

[[nodiscard]] wd::ServerTransportPorts wire_server_transport(
    Wiring &w, wd::ServerConfigHandle config, Str path,
    wd::WebServerRuntimeHandle runtime, wd::ServerAdmissionHandle admission,
    wd::WebTransportBindingsHandle bindings) {
  return wd::wire_transport_sources<WebServerTransportTag>(
      w, admission,
      [config = std::move(config), path = std::move(path), runtime, admission,
       bindings](wd::ServerTransportOutput::Senders senders,
                 const NodeView &, DateTime) {
        admission.value->start();
        auto admission_rollback =
            make_scope_exit<true>([&] { admission.value->stop(); });
        auto task = std::make_shared<wd::WebServerRuntime>(
            config.value, path, admission, bindings, std::move(senders));
        task->start();
        auto task_rollback = make_scope_exit<true>([&] { task->stop(); });
        runtime.value->install(task);
        task_rollback.release();
        admission_rollback.release();
      },
      [runtime, admission](const NodeView &) {
        if (auto task = runtime.value->take()) {
          task->stop();
        }
        admission.value->stop();
      });
}

struct WebServerImpl {
  static constexpr auto name = "web_server_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"path", Str> path) {
    if (!w.is_realtime()) {
      throw std::invalid_argument(
          "the live web server requires a real-time graph");
    }
    register_web_types();
    wd::register_internal_types();
    // Fail every structural error at wiring time (RFC 0024, configuration).
    wd::ServerConfigHandle runtime_config{
        std::make_shared<const wd::ServerRuntimeConfig>(
            wd::parse_server_config(config.value()))};

    const auto binding = service::path(path.value());
    auto http_routes = service::impl_input<HttpServeService>(w, binding);
    auto ws_routes = service::impl_input<WsServeService>(w, binding);
    auto responses = service::impl_input<HttpRespondService>(w, binding);
    auto ws_sends = service::impl_input<WsSendService>(w, binding);

    wd::WebServerRuntimeHandle runtime{
        std::make_shared<wd::WebServerRuntimeResource>()};
    auto admission = wd::make_server_admission(config.value());
    auto transport_bindings = wd::make_transport_bindings();
    auto transport = wire_server_transport(w, runtime_config, path.value(),
                                           runtime, admission,
                                           transport_bindings);
    auto http_generations =
        wire<wd::SubscriptionGenerationNode<WebRoute>>(w, http_routes)
            .template as<TSD<WebRoute, TS<DateTime>>>();
    auto ws_generations =
        wire<wd::SubscriptionGenerationNode<WebRoute>>(w, ws_routes)
            .template as<TSD<WebRoute, TS<DateTime>>>();
    static_cast<void>(wire<WebServerHttpRouteSink>(
        w, http_routes, http_generations, runtime));
    static_cast<void>(wire<WebServerWsRouteSink>(w, ws_routes, ws_generations,
                                                 runtime));
    static_cast<void>(wire<WebServerRespondSink>(w, responses, runtime));
    static_cast<void>(wire<WebServerWsSendSink>(w, ws_sends, runtime));
    auto outputs = wd::wire_server_outputs(
        w, transport, http_routes, ws_routes, http_generations, ws_generations,
        admission, transport_bindings);

    service::impl_output<HttpServeService>(w, binding, outputs.requests);
    service::impl_output<WsServeService>(w, binding, outputs.ws);
    service::impl_output<HttpRespondService>(w, binding,
                                             outputs.respond_reports);
    service::impl_output<WsSendService>(w, binding, outputs.ws_send_reports);
    service::impl_output<WebServerEventService>(w, binding, outputs.events);
    service::impl_output<WebServerStatsService>(w, binding, outputs.stats);
  }
};
} // namespace

void register_server(Wiring &w, service::ServicePath path,
                     Value server_config) {
  service::register_services<WebServerImpl, HttpServeService,
                             HttpRespondService, WsServeService, WsSendService,
                             WebServerEventService, WebServerStatsService>(
      w, std::move(path), std::move(server_config));
}
} // namespace hgraph::web
