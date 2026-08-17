// Bridge-level regression tests for the reservation protocol's teardown
// contract (RFC 0024, flow control; review P1): a stopped bridge has
// discarded every reservation, so a late completion — a body or WebSocket
// read finishing on a shared listener after one attachee stopped — must be
// rejected, never treated as a broken invariant that would throw across the
// uncaught io_context::run() boundary.

#include "detail/service_bridge.h"

#include <hgraph/web/types.h>

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

} // namespace

int main() {
  try {
    register_web_types();
    test_late_push_reserved_is_rejected_not_thrown();
    test_reservation_calls_after_stop_are_inert();
  } catch (const std::exception &error) {
    std::cerr << "hgraph_web_bridge_tests failed: " << error.what() << "\n";
    return 1;
  }
  std::cout << "hgraph_web_bridge_tests passed\n";
  return 0;
}
