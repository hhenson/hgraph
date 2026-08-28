// Domain-accounting regression tests for the web adaptor's standard
// push-source transport (RFC 0024 / RFC 0027). The accounting object owns no
// Values: the core push source is the sole cross-thread queue.

#include "detail/service_transport.h"

#include <hgraph/web/types.h>

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/shared_value_pool.h>
#include <hgraph/types/value/value_builder.h>

#include <array>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using hgraph::web::detail::AdmissionBudget;
using hgraph::web::detail::AdmissionHandle;
using hgraph::web::detail::OutputLimits;
using hgraph::web::detail::TransportOutput;
using hgraph::web::detail::WatermarkConfig;
using hgraph::web::detail::WebEventEnvelope;
using hgraph::web::detail::WebTransportEventKind;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

using TestBudget = AdmissionBudget<1>;

[[nodiscard]] std::shared_ptr<TestBudget> small_budget() {
  return std::make_shared<TestBudget>(
      std::array<OutputLimits, 1>{OutputLimits{4, 4096}});
}

[[nodiscard]] Value empty_event_envelope() {
  return BundleBuilder{ValuePlanFactory::instance().type_for(
                           scalar_descriptor<WebEventEnvelope>::value_meta())}
      .build();
}

template <typename Envelope> [[nodiscard]] Value empty_envelope() {
  return BundleBuilder{ValuePlanFactory::instance().type_for(
                           scalar_descriptor<Envelope>::value_meta())}
      .build();
}

void test_transport_payloads_retain_one_shared_envelope() {
  const auto bindings = hgraph::web::detail::make_transport_bindings();
  const auto live_before = shared_value_pool_metrics().live_values;

  const auto check = [&](WebTransportEventKind kind, std::string_view field,
                         Value payload, const ValueTypeMetaData *schema) {
    Value event = hgraph::web::detail::transport_event(
        *bindings.value, kind, field, std::move(payload), 0, 128, false);
    const auto retained = event.view().as_bundle().at(field);
    require(retained.schema()->is_shared(),
            "a web transport payload envelope was not shared");
    require(retained.concrete().schema() == schema,
            "a web transport shared envelope changed its concrete schema");
    const void *stable = retained.concrete().data();
    require(stable != nullptr, "a web transport shared envelope was empty");
    require(shared_value_pool_metrics().live_values == live_before + 1,
            "web transport materialised more than one shared envelope");

    Value copied_event = event.clone();
    const auto copied = copied_event.view().as_bundle().at(field);
    require(copied.concrete().data() == stable,
            "a web transport copy did not retain the shared envelope slot");
    require(shared_value_pool_metrics().live_values == live_before + 1,
            "a web transport copy duplicated the shared envelope payload");
  };

  check(WebTransportEventKind::ServerRequest, "request",
        empty_envelope<hgraph::web::detail::WebRequestEnvelope>(),
        scalar_descriptor<hgraph::web::detail::WebRequestEnvelope>::value_meta());
  require(shared_value_pool_metrics().live_values == live_before,
          "the shared request envelope outlived its transport values");
  check(WebTransportEventKind::ServerWsIngress, "server_ws",
        empty_envelope<hgraph::web::detail::WsIngressEnvelope>(),
        scalar_descriptor<hgraph::web::detail::WsIngressEnvelope>::value_meta());
  require(shared_value_pool_metrics().live_values == live_before,
          "the shared server WS envelope outlived its transport values");
  check(WebTransportEventKind::ClientWsIngress, "client_ws",
        empty_envelope<hgraph::web::detail::WsClientEnvelope>(),
        scalar_descriptor<hgraph::web::detail::WsClientEnvelope>::value_meta());
  require(shared_value_pool_metrics().live_values == live_before,
          "the shared client WS envelope outlived its transport values");
  check(WebTransportEventKind::ClientResponse, "response",
        empty_envelope<hgraph::web::detail::WebResponseEnvelope>(),
        scalar_descriptor<hgraph::web::detail::WebResponseEnvelope>::value_meta());
  require(shared_value_pool_metrics().live_values == live_before,
          "the shared response envelope outlived its transport values");
  check(WebTransportEventKind::ClientSendDelivery, "delivery",
        empty_envelope<hgraph::web::detail::WebDeliveryEnvelope>(),
        scalar_descriptor<hgraph::web::detail::WebDeliveryEnvelope>::value_meta());
  require(shared_value_pool_metrics().live_values == live_before,
          "the shared delivery envelope outlived its transport values");
  check(WebTransportEventKind::ClientEvent, "event",
        empty_envelope<hgraph::web::detail::WebEventEnvelope>(),
        scalar_descriptor<hgraph::web::detail::WebEventEnvelope>::value_meta());
  require(shared_value_pool_metrics().live_values == live_before,
          "the shared event envelope outlived its transport values");
}

void test_late_reserved_completion_is_rejected_not_thrown() {
  auto budget = small_budget();
  budget->start();
  require(budget->reserve(0, 1024), "test reservation was rejected");
  budget->stop();

  bool rejected = false;
  try {
    rejected = !budget->admit_reserved(0, 100, 1024);
  } catch (const std::logic_error &) {
    require(false, "a late reserved completion after stop threw instead of "
                   "being rejected");
  }
  require(rejected, "a late reserved completion after stop was accepted");
}

void test_reservation_calls_after_stop_are_inert() {
  auto budget = small_budget();
  budget->start();
  budget->stop();
  require(!budget->reserve(0, 512),
          "reserve succeeded on a stopped transport budget");
  require(!budget->grow_reservation(0, 512),
          "grow_reservation succeeded on a stopped transport budget");
  budget->release_reservation(0, 1024);
  require(budget->payload_retained_bytes(0) == 0,
          "a stopped transport budget accumulated accounting");
}

void test_stopped_sender_refusal_rolls_back_admission() {
  auto budget = small_budget();
  budget->start();
  auto bindings = hgraph::web::detail::make_transport_bindings();
  TransportOutput<TestBudget> output{
      TransportOutput<TestBudget>::Senders{PushSourceSender{}},
      AdmissionHandle<TestBudget>{budget}, bindings};

  require(!output.send(WebTransportEventKind::ServerEvent, "event",
                       empty_event_envelope(), 0, 128),
          "a stopped sender accepted web transport output");
  require(budget->payload_pending(0) == 0,
          "sender refusal leaked record admission");
  require(budget->payload_retained_bytes(0) == 0,
          "sender refusal leaked byte admission");
  budget->stop();
}

void test_watermarks_follow_graph_dequeue_accounting() {
  auto budget = small_budget();
  std::vector<bool> transitions;
  budget->set_watermark(
      0, WatermarkConfig{OutputLimits{2, 1024}, OutputLimits{1, 512},
                         [&transitions](bool paused) {
                           transitions.push_back(paused);
                         }});
  budget->start();

  require(budget->admit(0, 400, false), "first admission was rejected");
  require(budget->admit(0, 400, false), "second admission was rejected");
  require(transitions == std::vector<bool>{true},
          "high watermark did not pause exactly once");

  // In the adaptor this release is performed by a graph sink consuming the
  // standard push-source output. Capacity therefore means queued work, not
  // protocol completion or acknowledgement.
  budget->release(0, 400, false);
  require(budget->payload_pending(0) == 1,
          "one graph dequeue released more than one admitted record");
  require(transitions == std::vector<bool>{true, false},
          "low watermark did not resume after graph dequeue");
  budget->release(0, 400, false);
  require(budget->payload_pending(0) == 0,
          "graph dequeue accounting did not return to zero");
  budget->stop();
}

} // namespace

int main() {
  try {
    register_web_types();
    hgraph::web::detail::register_internal_types();
    test_transport_payloads_retain_one_shared_envelope();
    test_late_reserved_completion_is_rejected_not_thrown();
    test_reservation_calls_after_stop_are_inert();
    test_stopped_sender_refusal_rolls_back_admission();
    test_watermarks_follow_graph_dequeue_accounting();
  } catch (const std::exception &error) {
    std::cerr << "hgraph_web_transport_tests failed: " << error.what() << "\n";
    return 1;
  }
  std::cout << "hgraph_web_transport_tests passed\n";
  return 0;
}
