#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <hgraph/python/native_scalar_registration.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

namespace nb = nanobind;

NB_MODULE(_hgraph_web, module) {
  using namespace hgraph;
  using namespace hgraph::web;

  auto http_method = nb::enum_<HttpMethod>(module, "HttpMethod")
                         .value("GET", HttpMethod::Get)
                         .value("HEAD", HttpMethod::Head)
                         .value("POST", HttpMethod::Post)
                         .value("PUT", HttpMethod::Put)
                         .value("DELETE", HttpMethod::Delete)
                         .value("PATCH", HttpMethod::Patch)
                         .value("OPTIONS", HttpMethod::Options)
                         .value("TRACE", HttpMethod::Trace);
  python_bridge::register_native_scalar_type<HttpMethod>(http_method);

  auto route_state = nb::enum_<WebRouteState>(module, "WebRouteState")
                         .value("STARTING", WebRouteState::Starting)
                         .value("SERVING", WebRouteState::Serving)
                         .value("REMOVED", WebRouteState::Removed)
                         .value("FAILED", WebRouteState::Failed);
  python_bridge::register_native_scalar_type<WebRouteState>(route_state);

  auto frame_kind = nb::enum_<WsFrameKind>(module, "WsFrameKind")
                        .value("TEXT", WsFrameKind::Text)
                        .value("BINARY", WsFrameKind::Binary)
                        .value("PING", WsFrameKind::Ping)
                        .value("PONG", WsFrameKind::Pong)
                        .value("CLOSE", WsFrameKind::Close);
  python_bridge::register_native_scalar_type<WsFrameKind>(frame_kind);

  auto connection_state =
      nb::enum_<WsConnectionState>(module, "WsConnectionState")
          .value("OPEN", WsConnectionState::Open)
          .value("CLOSING", WsConnectionState::Closing)
          .value("CLOSED", WsConnectionState::Closed)
          .value("FAILED", WsConnectionState::Failed);
  python_bridge::register_native_scalar_type<WsConnectionState>(
      connection_state);

  auto delivery_status =
      nb::enum_<WebDeliveryStatus>(module, "WebDeliveryStatus")
          .value("ENQUEUE_REJECTED", WebDeliveryStatus::EnqueueRejected)
          .value("DROPPED", WebDeliveryStatus::Dropped)
          .value("DELIVERED", WebDeliveryStatus::Delivered)
          .value("RETRIABLE_FAILURE", WebDeliveryStatus::RetriableFailure)
          .value("PERMANENT_FAILURE", WebDeliveryStatus::PermanentFailure);
  python_bridge::register_native_scalar_type<WebDeliveryStatus>(
      delivery_status);

  auto severity = nb::enum_<WebSeverity>(module, "WebSeverity")
                      .value("DEBUG", WebSeverity::Debug)
                      .value("INFO", WebSeverity::Info)
                      .value("WARNING", WebSeverity::Warning)
                      .value("ERROR", WebSeverity::Error)
                      .value("FATAL", WebSeverity::Fatal);
  python_bridge::register_native_scalar_type<WebSeverity>(severity);

  auto inbound_overflow =
      nb::enum_<WebInboundOverflow>(module, "WebInboundOverflow")
          .value("BACKPRESSURE", WebInboundOverflow::Backpressure)
          .value("REJECT", WebInboundOverflow::Reject);
  python_bridge::register_native_scalar_type<WebInboundOverflow>(
      inbound_overflow);

  auto slow_consumer =
      nb::enum_<WebSlowConsumerPolicy>(module, "WebSlowConsumerPolicy")
          .value("CLOSE", WebSlowConsumerPolicy::Close)
          .value("DROP_NEWEST", WebSlowConsumerPolicy::DropNewest);
  python_bridge::register_native_scalar_type<WebSlowConsumerPolicy>(
      slow_consumer);

  auto overflow_action =
      nb::enum_<WebOverflowAction>(module, "WebOverflowAction")
          .value("FAIL", WebOverflowAction::Fail)
          .value("DROP", WebOverflowAction::Drop)
          .value("STAGE", WebOverflowAction::Stage);
  python_bridge::register_native_scalar_type<WebOverflowAction>(
      overflow_action);

  auto failure_policy =
      nb::enum_<WebFailurePolicy>(module, "WebFailurePolicy")
          .value("REPORT", WebFailurePolicy::Report)
          .value("STOP_GRAPH", WebFailurePolicy::StopGraph);
  python_bridge::register_native_scalar_type<WebFailurePolicy>(failure_policy);

  auto client_verify = nb::enum_<WebClientVerify>(module, "WebClientVerify")
                           .value("NONE", WebClientVerify::None)
                           .value("OPTIONAL", WebClientVerify::Optional)
                           .value("REQUIRED", WebClientVerify::Required);
  python_bridge::register_native_scalar_type<WebClientVerify>(client_verify);

  auto tls_version = nb::enum_<WebTlsVersion>(module, "WebTlsVersion")
                         .value("TLS1_2", WebTlsVersion::Tls1_2)
                         .value("TLS1_3", WebTlsVersion::Tls1_3);
  python_bridge::register_native_scalar_type<WebTlsVersion>(tls_version);

  auto version_policy =
      nb::enum_<WebHttpVersionPolicy>(module, "WebHttpVersionPolicy")
          .value("AUTO", WebHttpVersionPolicy::Auto)
          .value("H1_ONLY", WebHttpVersionPolicy::H1Only)
          .value("H2_ONLY", WebHttpVersionPolicy::H2Only);
  python_bridge::register_native_scalar_type<WebHttpVersionPolicy>(
      version_policy);

  register_web_types();
}
