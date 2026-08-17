#ifndef HGRAPH_WEB_DETAIL_WEB_BINDINGS_H
#define HGRAPH_WEB_DETAIL_WEB_BINDINGS_H

#include <hgraph/web/types.h>

#include "stream_model.h"

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::web::detail {

[[nodiscard]] inline ValueTypeRef
resolve_binding(const ValueTypeMetaData *meta) {
  auto binding = ValuePlanFactory::instance().type_for(meta);
  if (!binding) {
    throw std::logic_error("Web schema did not resolve");
  }
  return binding;
}

template <typename Schema> [[nodiscard]] ValueTypeRef resolve_schema() {
  return resolve_binding(scalar_descriptor<Schema>::value_meta());
}

/**
 * Every binding a transport thread builds Values from, resolved once on the
 * graph thread at start.
 *
 * Pre-warming alone is not enough: `scalar_descriptor<T>::value_meta()` and
 * `Value(T)` take the counted type-system mutex on every invocation, so an
 * I/O thread constructing per-request values would contend on the registry.
 * Holding the resolved `ValueTypeRef`s and building through
 * `Value(binding, source)` / `BundleBuilder(binding)` keeps transport
 * threads off the registry entirely (CLAUDE.md single-threaded-evaluation
 * conventions).
 */
struct WebBindings {
  ValueTypeRef integer{};
  ValueTypeRef boolean{};
  ValueTypeRef text{};
  ValueTypeRef binary{};
  ValueTypeRef http_method{};
  ValueTypeRef route_state{};
  ValueTypeRef frame_kind{};
  ValueTypeRef connection_state{};
  ValueTypeRef delivery_status{};
  ValueTypeRef severity{};

  ValueTypeRef header{};
  ValueTypeRef header_tuple{};
  ValueTypeRef param{};
  ValueTypeRef param_tuple{};
  ValueTypeRef peer{};
  ValueTypeRef http_request{};
  ValueTypeRef server_request{};
  ValueTypeRef http_response{};
  ValueTypeRef ws_frame{};
  ValueTypeRef ws_inbound_frame{};
  ValueTypeRef ws_event{};
  ValueTypeRef delivery_report{};
  ValueTypeRef web_event{};
  ValueTypeRef server_stats{};

  ValueTypeRef request_envelope{};
  ValueTypeRef ws_ingress_envelope{};
  ValueTypeRef delivery_envelope{};
  ValueTypeRef event_envelope{};

  void resolve_all() {
    integer = resolve_schema<Int>();
    boolean = resolve_schema<Bool>();
    text = resolve_schema<Str>();
    binary = resolve_schema<Bytes>();
    http_method = resolve_schema<HttpMethod>();
    route_state = resolve_schema<WebRouteState>();
    frame_kind = resolve_schema<WsFrameKind>();
    connection_state = resolve_schema<WsConnectionState>();
    delivery_status = resolve_schema<WebDeliveryStatus>();
    severity = resolve_schema<WebSeverity>();

    header = resolve_schema<WebHeader>();
    header_tuple = resolve_schema<HomogeneousTuple<WebHeader>>();
    param = resolve_schema<WebParam>();
    param_tuple = resolve_schema<HomogeneousTuple<WebParam>>();
    peer = resolve_schema<WebPeer>();
    http_request = resolve_schema<HttpRequest>();
    server_request = resolve_schema<HttpServerRequest>();
    http_response = resolve_schema<HttpResponse>();
    ws_frame = resolve_schema<WsFrame>();
    ws_inbound_frame = resolve_schema<WsInboundFrame>();
    ws_event = resolve_schema<WsEvent>();
    delivery_report = resolve_schema<WebDeliveryReport>();
    web_event = resolve_schema<WebEvent>();
    server_stats = resolve_schema<WebServerStats>();

    request_envelope = resolve_schema<WebRequestEnvelope>();
    ws_ingress_envelope = resolve_schema<WsIngressEnvelope>();
    delivery_envelope = resolve_schema<WebDeliveryEnvelope>();
    event_envelope = resolve_schema<WebEventEnvelope>();
  }

  [[nodiscard]] static Value scalar(const ValueTypeRef &binding,
                                    const void *source) {
    return Value{binding, source};
  }

  [[nodiscard]] Value number(Int value) const { return scalar(integer, &value); }
  [[nodiscard]] Value flag(Bool value) const { return scalar(boolean, &value); }
  [[nodiscard]] Value string(const Str &value) const {
    return scalar(text, &value);
  }
  [[nodiscard]] Value bytes(const Bytes &value) const {
    return scalar(binary, &value);
  }
  template <typename E> [[nodiscard]] Value enum_value(E value) const;

  using NamedPairs = std::vector<std::pair<std::string, std::string>>;

  /** Duplicate names and arrival order survive (RFC 0024, value contract). */
  [[nodiscard]] Value name_values(const ValueTypeRef &element,
                                  const ValueTypeRef &tuple,
                                  const NamedPairs &values) const {
    ListBuilder list{element};
    for (const auto &[name, value] : values) {
      BundleBuilder entry{element};
      entry.set("name", string(Str{name}));
      entry.set("value", string(Str{value}));
      Value item = entry.build();
      list.push_back_copy(item.view().data());
    }
    ListStorage storage = list.build_storage();
    return Value{tuple, &storage};
  }

  [[nodiscard]] Value headers(const NamedPairs &values) const {
    return name_values(header, header_tuple, values);
  }
  [[nodiscard]] Value params(const NamedPairs &values) const {
    return name_values(param, param_tuple, values);
  }
};

template <> inline Value WebBindings::enum_value(HttpMethod value) const {
  return scalar(http_method, &value);
}
template <> inline Value WebBindings::enum_value(WebRouteState value) const {
  return scalar(route_state, &value);
}
template <> inline Value WebBindings::enum_value(WsFrameKind value) const {
  return scalar(frame_kind, &value);
}
template <>
inline Value WebBindings::enum_value(WsConnectionState value) const {
  return scalar(connection_state, &value);
}
template <>
inline Value WebBindings::enum_value(WebDeliveryStatus value) const {
  return scalar(delivery_status, &value);
}
template <> inline Value WebBindings::enum_value(WebSeverity value) const {
  return scalar(severity, &value);
}

/** Assemble a bundle from the fields that are set, on a resolved binding. */
[[nodiscard]] inline Value
build_on(const ValueTypeRef &binding,
         std::vector<std::pair<std::string_view, Value>> fields) {
  BundleBuilder builder{binding};
  for (auto &[name, field] : fields) {
    if (field.has_value()) {
      builder.set(name, std::move(field));
    }
  }
  return builder.build();
}

} // namespace hgraph::web::detail

#endif // HGRAPH_WEB_DETAIL_WEB_BINDINGS_H
