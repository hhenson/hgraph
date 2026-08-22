// Bridge-level regression tests for the reservation protocol's teardown
// contract (RFC 0024, flow control; review P1): a stopped bridge has
// discarded every reservation, so a late completion — a body or WebSocket
// read finishing on a shared listener after one attachee stopped — must be
// rejected, never treated as a broken invariant that would throw across the
// uncaught io_context::run() boundary.

#include "detail/service_bridge.h"

#include <hgraph/web/types.h>

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/static_schema.h>

#include <array>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {
using namespace hgraph;
using namespace hgraph::web;
using hgraph::web::detail::BoundedBridge;
using hgraph::web::detail::OutputLimits;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

// A never-started bridge is the same observable state stop() leaves
// behind: not accepting, zero reserved records and bytes.  stop() is also
// invoked so the tests walk the actual teardown entry point.
[[nodiscard]] std::array<OutputLimits, 1> small_limits() {
  return std::array<OutputLimits, 1>{OutputLimits{4, 4096}};
}

void test_late_push_reserved_is_rejected_not_thrown() {
  BoundedBridge<1> bridge{small_limits()};
  bridge.stop();
  bool rejected = false;
  try {
    rejected = !bridge.push_reserved(0, Value{Int{1}}, 100, 1024);
  } catch (const std::logic_error &) {
    require(false, "a late reserved completion after stop threw instead of "
                   "being rejected");
  }
  require(rejected, "a late reserved completion after stop was accepted");
}

void test_reservation_calls_after_stop_are_inert() {
  BoundedBridge<1> bridge{small_limits()};
  bridge.stop();
  require(!bridge.reserve(0, 512),
          "reserve succeeded on a stopped bridge");
  require(!bridge.grow_reservation(0, 512),
          "grow_reservation succeeded on a stopped bridge");
  // Releasing a reservation stop() already discarded must be a no-op.
  bridge.release_reservation(0, 1024);
  require(bridge.payload_retained_bytes(0) == 0,
          "a stopped bridge accumulated accounting");
}

void test_wake_delivery_handles_graph_teardown() {
  const auto *ts_int = ts_type<TS<Int>>();
  PushSourceSender sender;
  GraphBuilder builder;
  builder.add_node(make_push_source_node(
      *ts_int, make_push_source_conflating_policy(*ts_int),
      [&sender](PushSourceSender started) { sender = std::move(started); }));

  const DateTime start = hgraph::testing::wall_now();
  GraphExecutorBuilder executor_builder;
  executor_builder.graph_builder(std::move(builder))
      .mode(GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + TimeDelta{1'000'000});
  auto executor = executor_builder.make_executor();
  auto graph = executor.view().graph();
  graph.start(start);

  BoundedBridge<1> bridge{small_limits()};
  bridge.attach(0, sender);
  bridge.start();
  graph.stop(start);

  // Transport callbacks can enqueue after graph teardown has closed the
  // retained sender but before bridge teardown disables admission. The first
  // push exercises the retained stopped sender; the second needs no new
  // notification because a wake is already outstanding.
  require(bridge.push(0, Value{Int{1}}, sizeof(Int)),
          "a bridge push racing graph teardown was rejected");
  require(bridge.push(0, Value{Int{2}}, sizeof(Int)),
          "a bridge push with an outstanding wake was rejected");
  require(bridge.pending(0) == 2,
          "teardown-racing bridge values were not retained");
  bridge.stop();
}

} // namespace

int main() {
  try {
    register_web_types();
    test_late_push_reserved_is_rejected_not_thrown();
    test_reservation_calls_after_stop_are_inert();
    test_wake_delivery_handles_graph_teardown();
  } catch (const std::exception &error) {
    std::cerr << "hgraph_web_bridge_tests failed: " << error.what() << "\n";
    return 1;
  }
  std::cout << "hgraph_web_bridge_tests passed\n";
  return 0;
}
