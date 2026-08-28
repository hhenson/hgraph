#include <hgraph/kafka/value_builders.h>

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
#include <optional>
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
using namespace hgraph::kafka;

// RFC 0028's before case, retained in this benchmark only. Keep every field
// identical to KafkaTransportEvent except the record representation.
using PlainKafkaTransportEvent =
    Bundle<"hgraph.kafka.benchmark::PlainKafkaTransportEvent",
           Field<"kind", kafka::detail::KafkaTransportEventKind>,
           Field<"subscription_key", KafkaSubscriptionKey>,
           Field<"record", KafkaRecord>, Field<"cursor", KafkaCursor>,
           Field<"state", KafkaSubscriptionState>,
           Field<"evaluation_time", DateTime>, Field<"removed", Bool>,
           Field<"recovery", Bool>, Field<"request_id", Int>,
           Field<"report", KafkaDeliveryReport>, Field<"event", KafkaEvent>,
           Field<"stop_graph", Bool>>;
using PlainKafkaTransportEventBatch =
    HomogeneousTuple<PlainKafkaTransportEvent>;

struct Variant {
  std::string_view name;
  ValueTypeRef event;
  ValueTypeRef batch;
  const ValueTypeMetaData *batch_schema;
  bool shared_record;
};

struct Fixture {
  Variant plain;
  Variant shared;
  ValueTypeRef public_record;
  Value subscription_key;
  Value cursor;
  Value record;

  explicit Fixture(std::size_t payload_bytes) {
    register_kafka_types();
    auto &factory = ValuePlanFactory::instance();
    const auto bindings = kafka::detail::make_transport_bindings();
    plain = Variant{
        .name = "plain",
        .event = factory.type_for(
            scalar_descriptor<PlainKafkaTransportEvent>::value_meta()),
        .batch = factory.type_for(
            scalar_descriptor<PlainKafkaTransportEventBatch>::value_meta()),
        .batch_schema =
            scalar_descriptor<PlainKafkaTransportEventBatch>::value_meta(),
        .shared_record = false,
    };
    shared = Variant{
        .name = "shared",
        .event = bindings.value->event,
        .batch = bindings.value->batch,
        .batch_schema = scalar_descriptor<
            kafka::detail::KafkaTransportEventBatch>::value_meta(),
        .shared_record = true,
    };
    public_record =
        factory.type_for(scalar_descriptor<KafkaRecord>::value_meta());
    subscription_key = make_subscription_key(
        {Str{"benchmark-records"}}, Str{"benchmark-records"}, Str{"earliest"},
        Str{"unbounded"}, KafkaCommitMode::Explicit, Str{"benchmark-records"});
    cursor = make_cursor(Str{"benchmark-records"}, Int{1},
                         Str{"benchmark-records"}, Int{0}, Int{2});
    record = make_record(Str{"benchmark-records"}, Int{0}, Int{1},
                         Bytes{std::string(payload_bytes, 'x')},
                         Bytes{std::string(64, 'k')});
  }

  [[nodiscard]] Value event(const Variant &variant) const {
    BundleBuilder builder{variant.event};
    builder.set("kind",
                Value{kafka::detail::KafkaTransportEventKind::Subscription});
    builder.set("subscription_key", subscription_key.clone());
    builder.set("record", record.clone());
    builder.set("cursor", cursor.clone());
    builder.set("state", Value{KafkaSubscriptionState::Live});
    return builder.build();
  }

  [[nodiscard]] std::uint64_t pipeline(const Variant &variant) const {
    ListBuilder burst{variant.event, *variant.batch_schema};
    burst.push_back(event(variant));
    Value sender_batch = burst.build();

    // These are the retained representations on the production path: push
    // output, emitter pending queue, keyed transport child, and public output.
    Value observed_batch{variant.batch, sender_batch.view()};
    Value pending = observed_batch.view().as_list().at(0).clone();
    Value keyed_child = pending.clone();
    const auto retained_record =
        keyed_child.view().as_bundle().at("record").concrete();
    Value public_output{public_record, retained_record};
    const auto fields = public_output.view().as_bundle();
    return static_cast<std::uint64_t>(
               fields.at("value").checked_as<Bytes>().data.size()) +
           static_cast<std::uint64_t>(fields.at("offset").checked_as<Int>());
  }

  [[nodiscard]] DynamicStorageMetrics
  retained_metrics(const Variant &variant) const {
    const DynamicStorageMetrics record_payload =
        record.view().dynamic_storage_metrics();
    Value initial_event = event(variant);
    ListBuilder burst{variant.event, *variant.batch_schema};
    burst.push_back(std::move(initial_event));
    Value sender_batch = burst.build();
    Value observed_batch{variant.batch, sender_batch.view()};
    sender_batch.reset();
    Value pending = observed_batch.view().as_list().at(0).clone();
    Value keyed_child = pending.clone();
    pending.reset();
    Value public_output{public_record,
                        keyed_child.view().as_bundle().at("record").concrete()};

    // At projection time the push output and keyed child remain live while
    // the public TS<KafkaRecord> output is populated. This is the path's peak
    // retained-record state: three payload copies before, one shared payload
    // plus one public copy after.
    DynamicStorageMetrics result{};
    for (const Value *holder :
         {&observed_batch, &keyed_child, &public_output}) {
      result += holder->view().dynamic_storage_metrics();
    }
    // Shared handles intentionally report zero for their payload. Add its one
    // logical owner here so retained bytes compare like-for-like without
    // multiplying the shared allocation by the number of handles.
    if (variant.shared_record) {
      result += record_payload;
    }
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
         std::size_t payload_bytes) {
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

  const auto retained = fixture.retained_metrics(variant);
  const auto pool = shared_value_pool_metrics();
  const auto shared_pool_reserved =
      variant.shared_record ? pool.reserved_bytes : 0;
  std::cout << "benchmark"
            << " name=kafka_subscription_transport_" << variant.name
            << " iterations=" << iterations
            << " payload_bytes=" << payload_bytes
            << " p50_ns_per_record=" << percentile(latencies, 0.50)
            << " p99_ns_per_record=" << percentile(latencies, 0.99)
            << " allocations_per_record="
            << static_cast<double>(allocations) /
                   static_cast<double>(iterations)
            << " allocated_bytes_per_record="
            << static_cast<double>(allocated_bytes) /
                   static_cast<double>(iterations)
            << " retained_live_bytes=" << retained.live_bytes
            << " retained_reserved_bytes=" << retained.reserved_bytes
            << " shared_pool_reserved_bytes=" << shared_pool_reserved
            << " tracked_retained_bytes="
            << retained.reserved_bytes + shared_pool_reserved
            << " checksum=" << checksum << '\n';
}
} // namespace

int main() {
  try {
    const std::size_t iterations =
        env_size("HGRAPH_KAFKA_TRANSPORT_PERF_ITERATIONS", 20'000);
    const std::size_t payload_bytes =
        env_size("HGRAPH_KAFKA_TRANSPORT_PERF_PAYLOAD_BYTES", 65'536);
    const Fixture fixture{payload_bytes};
    run(fixture, fixture.plain, iterations, payload_bytes);
    run(fixture, fixture.shared, iterations, payload_bytes);
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "Kafka transport benchmark failed: " << error.what() << '\n';
    return 1;
  }
}
