#include <hgraph/web/value_builders.h>

#include "detail/service_transport.h"

#include <hgraph/types/value/shared_value_pool.h>
#include <hgraph/util/environment.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#if defined(_MSC_VER)
#include <malloc.h>
#endif

namespace {
std::atomic<bool> g_count_allocations{false};
std::atomic<std::size_t> g_allocations{0};
std::atomic<std::size_t> g_allocated_bytes{0};
volatile std::uint64_t g_retained_checksum{0};

void record_allocation(std::size_t size) noexcept {
  if (g_count_allocations.load(std::memory_order_relaxed)) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    g_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
  }
}

[[nodiscard]] void *allocate_unaligned(std::size_t size) {
  if (void *memory = std::malloc(std::max<std::size_t>(size, 1))) {
    return memory;
  }
  throw std::bad_alloc{};
}

[[nodiscard]] void *allocate_aligned(std::size_t size, std::size_t alignment) {
  const auto actual_size = std::max<std::size_t>(size, 1);
#if defined(_MSC_VER)
  if (void *memory = _aligned_malloc(actual_size, alignment)) {
    return memory;
  }
#else
  void *memory{};
  if (posix_memalign(&memory, alignment, actual_size) == 0) {
    return memory;
  }
#endif
  throw std::bad_alloc{};
}

void free_aligned(void *memory) noexcept {
#if defined(_MSC_VER)
  _aligned_free(memory);
#else
  std::free(memory);
#endif
}

struct AllocationScope {
  AllocationScope() {
    g_allocations.store(0, std::memory_order_relaxed);
    g_allocated_bytes.store(0, std::memory_order_relaxed);
    g_count_allocations.store(true, std::memory_order_relaxed);
  }

  ~AllocationScope() {
    g_count_allocations.store(false, std::memory_order_relaxed);
  }
};
} // namespace

void *operator new(std::size_t size) {
  record_allocation(size);
  return allocate_unaligned(size);
}

void *operator new[](std::size_t size) {
  record_allocation(size);
  return allocate_unaligned(size);
}

void *operator new(std::size_t size, std::align_val_t alignment) {
  record_allocation(size);
  return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void *operator new[](std::size_t size, std::align_val_t alignment) {
  record_allocation(size);
  return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void *operator new(std::size_t size, const std::nothrow_t &) noexcept {
  try {
    return ::operator new(size);
  } catch (...) {
    return nullptr;
  }
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept {
  try {
    return ::operator new[](size);
  } catch (...) {
    return nullptr;
  }
}

void *operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t &) noexcept {
  try {
    return ::operator new(size, alignment);
  } catch (...) {
    return nullptr;
  }
}

void *operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t &) noexcept {
  try {
    return ::operator new[](size, alignment);
  } catch (...) {
    return nullptr;
  }
}

void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept {
  std::free(memory);
}
void operator delete(void *memory, const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete[](void *memory, const std::nothrow_t &) noexcept {
  std::free(memory);
}
void operator delete(void *memory, std::align_val_t) noexcept {
  free_aligned(memory);
}
void operator delete[](void *memory, std::align_val_t) noexcept {
  free_aligned(memory);
}
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept {
  free_aligned(memory);
}
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept {
  free_aligned(memory);
}
void operator delete(void *memory, std::align_val_t,
                     const std::nothrow_t &) noexcept {
  free_aligned(memory);
}
void operator delete[](void *memory, std::align_val_t,
                       const std::nothrow_t &) noexcept {
  free_aligned(memory);
}

namespace {
using namespace hgraph;
using namespace hgraph::web;
namespace wd = hgraph::web::detail;

// RFC 0028's before case, retained in this benchmark only. Keep every field
// identical to WebTransportEvent except the six payload envelopes.
using PlainWebTransportEvent =
    Bundle<"hgraph.web.benchmark::PlainWebTransportEvent",
           Field<"kind", wd::WebTransportEventKind>,
           Field<"request", wd::WebRequestEnvelope>,
           Field<"server_ws", wd::WsIngressEnvelope>,
           Field<"client_ws", wd::WsClientEnvelope>,
           Field<"response", wd::WebResponseEnvelope>,
           Field<"delivery", wd::WebDeliveryEnvelope>,
           Field<"event", wd::WebEventEnvelope>,
           Field<"server_stats", WebServerStats>,
           Field<"client_stats", WebClientStats>, Field<"channel", Int>,
           Field<"retained_bytes", Int>, Field<"control", Bool>>;
using PlainWebTransportEventBatch = HomogeneousTuple<PlainWebTransportEvent>;

struct Variant {
  std::string_view name;
  ValueTypeRef event;
  ValueTypeRef batch;
  const ValueTypeMetaData *batch_schema;
  bool shared_envelope;
};

struct RetainedMeasurement {
  DynamicStorageMetrics values{};
  std::size_t shared_pool_reserved_bytes{};
};

struct Fixture {
  Variant plain;
  Variant shared;
  ValueTypeRef public_request;
  Value request_envelope;

  explicit Fixture(std::size_t payload_bytes) {
    register_web_types();
    wd::register_internal_types();
    auto &factory = ValuePlanFactory::instance();
    plain = Variant{
        .name = "plain",
        .event = factory.type_for(
            scalar_descriptor<PlainWebTransportEvent>::value_meta()),
        .batch = factory.type_for(
            scalar_descriptor<PlainWebTransportEventBatch>::value_meta()),
        .batch_schema =
            scalar_descriptor<PlainWebTransportEventBatch>::value_meta(),
        .shared_envelope = false,
    };
    shared = Variant{
        .name = "shared",
        .event = factory.type_for(
            scalar_descriptor<wd::WebTransportEvent>::value_meta()),
        .batch = factory.type_for(
            scalar_descriptor<wd::WebTransportEventBatch>::value_meta()),
        .batch_schema =
            scalar_descriptor<wd::WebTransportEventBatch>::value_meta(),
        .shared_envelope = true,
    };
    public_request =
        factory.type_for(scalar_descriptor<HttpServerRequest>::value_meta());

    Value route = make_route(HttpMethod::Post, Str{"/benchmark"});
    Value request =
        make_request(HttpMethod::Post, Str{"/benchmark"}, Str{"/benchmark"}, {},
                     {}, {}, Bytes{std::string(payload_bytes, 'x')});
    BundleBuilder server_request{public_request};
    server_request.set("request_id", Value{Int{1}});
    server_request.set("connection_id", Value{Int{2}});
    server_request.set("stream_id", Value{Int{3}});
    server_request.set("request", std::move(request));

    BundleBuilder envelope{factory.type_for(
        scalar_descriptor<wd::WebRequestEnvelope>::value_meta())};
    envelope.set("route", std::move(route));
    envelope.set("request", server_request.build());
    envelope.set("generation", Value{DateTime{std::chrono::seconds{1}}});
    request_envelope = envelope.build();
  }

  [[nodiscard]] Value event(const Variant &variant) const {
    BundleBuilder builder{variant.event};
    builder.set("kind", Value{wd::WebTransportEventKind::ServerRequest});
    builder.set("request", request_envelope.clone());
    builder.set("channel", Value{Int{0}});
    builder.set("retained_bytes", Value{Int{65'536}});
    builder.set("control", Value{Bool{false}});
    return builder.build();
  }

  [[nodiscard]] std::uint64_t pipeline(const Variant &variant) const {
    ListBuilder burst{variant.event, *variant.batch_schema};
    burst.push_back(event(variant));
    Value sender_batch = burst.build();

    Value observed_batch{variant.batch, sender_batch.view()};
    Value pending = observed_batch.view().as_list().at(0).clone();
    Value keyed_child = pending.clone();
    const auto envelope =
        keyed_child.view().as_bundle().at("request").concrete().as_bundle();
    Value public_output{public_request, envelope.at("request")};
    const auto request =
        public_output.view().as_bundle().at("request").as_bundle();
    return static_cast<std::uint64_t>(
               request.at("body").checked_as<Bytes>().data.size()) +
           static_cast<std::uint64_t>(public_output.view()
                                          .as_bundle()
                                          .at("request_id")
                                          .checked_as<Int>());
  }

  [[nodiscard]] RetainedMeasurement
  retained_metrics(const Variant &variant, std::size_t records) const {
    const DynamicStorageMetrics envelope_payload =
        request_envelope.view().dynamic_storage_metrics();
    std::vector<Value> retained;
    retained.reserve(records * 3);
    RetainedMeasurement result{};
    for (std::size_t index = 0; index != records; ++index) {
      ListBuilder burst{variant.event, *variant.batch_schema};
      burst.push_back(event(variant));
      Value sender_batch = burst.build();
      retained.emplace_back(variant.batch, sender_batch.view());
      Value pending = retained.back().view().as_list().at(0).clone();
      retained.push_back(pending.clone());
      const auto envelope = retained.back()
                                .view()
                                .as_bundle()
                                .at("request")
                                .concrete()
                                .as_bundle();
      retained.emplace_back(public_request, envelope.at("request"));

      for (auto holder = retained.end() - 3; holder != retained.end();
           ++holder) {
        result.values += holder->view().dynamic_storage_metrics();
      }
      // Shared handles report zero for their payload. Attribute its one
      // logical owner per admitted request so the comparison does not hide
      // the immutable envelopes behind per-handle zero accounting.
      if (variant.shared_envelope) {
        result.values += envelope_payload;
      }
    }
    result.shared_pool_reserved_bytes =
        variant.shared_envelope ? shared_value_pool_metrics().reserved_bytes
                                : 0;
    return result;
  }
};

[[nodiscard]] std::size_t env_size(const char *name, std::size_t fallback) {
  const auto value = hgraph::environment_variable(name);
  if (!value || value->empty()) {
    return fallback;
  }
  char *end{};
  const char *begin = value->c_str();
  const auto parsed = std::strtoull(begin, &end, 10);
  if (end == begin || *end != '\0' || parsed == 0 ||
      parsed > std::numeric_limits<std::size_t>::max()) {
    throw std::invalid_argument(std::string{name} +
                                " must be a positive integer");
  }
  return static_cast<std::size_t>(parsed);
}

[[nodiscard]] double percentile(std::vector<double> values, double quantile) {
  std::ranges::sort(values);
  const double position = quantile * static_cast<double>(values.size() - 1);
  const auto lower = static_cast<std::size_t>(position);
  const auto upper = std::min(lower + 1, values.size() - 1);
  const double fraction = position - static_cast<double>(lower);
  return values[lower] + (values[upper] - values[lower]) * fraction;
}

void run(const Fixture &fixture, const Variant &variant, std::size_t iterations,
         std::size_t payload_bytes, std::size_t retained_records) {
  for (std::size_t warmup = 0; warmup < 1'000; ++warmup) {
    g_retained_checksum = fixture.pipeline(variant);
  }

  std::vector<double> latencies;
  latencies.reserve(iterations);
  std::uint64_t checksum{};
  std::size_t allocations{};
  std::size_t allocated_bytes{};
  {
    AllocationScope scope;
    for (std::size_t index = 0; index < iterations; ++index) {
      const auto start = std::chrono::steady_clock::now();
      checksum += fixture.pipeline(variant);
      latencies.push_back(std::chrono::duration<double, std::nano>(
                              std::chrono::steady_clock::now() - start)
                              .count());
    }
    allocations = g_allocations.load(std::memory_order_relaxed);
    allocated_bytes = g_allocated_bytes.load(std::memory_order_relaxed);
  }
  g_retained_checksum = checksum;

  const auto retained = fixture.retained_metrics(variant, retained_records);
  std::cout
      << "benchmark"
      << " name=web_server_request_transport_" << variant.name
      << " iterations=" << iterations << " payload_bytes=" << payload_bytes
      << " retained_records=" << retained_records
      << " p50_ns_per_request=" << percentile(latencies, 0.50)
      << " p99_ns_per_request=" << percentile(latencies, 0.99)
      << " allocations_per_request="
      << static_cast<double>(allocations) / static_cast<double>(iterations)
      << " allocated_bytes_per_request="
      << static_cast<double>(allocated_bytes) / static_cast<double>(iterations)
      << " retained_live_bytes=" << retained.values.live_bytes
      << " retained_reserved_bytes=" << retained.values.reserved_bytes
      << " shared_pool_reserved_bytes=" << retained.shared_pool_reserved_bytes
      << " tracked_retained_bytes="
      << retained.values.reserved_bytes + retained.shared_pool_reserved_bytes
      << " checksum=" << checksum << '\n';
}
} // namespace

int main() {
  try {
    const std::size_t iterations =
        env_size("HGRAPH_WEB_TRANSPORT_PERF_ITERATIONS", 20'000);
    const std::size_t payload_bytes =
        env_size("HGRAPH_WEB_TRANSPORT_PERF_PAYLOAD_BYTES", 65'536);
    const std::size_t retained_records =
        env_size("HGRAPH_WEB_TRANSPORT_PERF_RETAINED_RECORDS", 64);
    const Fixture fixture{payload_bytes};
    run(fixture, fixture.plain, iterations, payload_bytes, retained_records);
    run(fixture, fixture.shared, iterations, payload_bytes, retained_records);
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "Web transport benchmark failed: " << error.what() << '\n';
    return 1;
  }
}
