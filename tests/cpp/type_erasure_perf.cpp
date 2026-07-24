#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/runtime/mesh_node.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(_MSC_VER)
#include <malloc.h>
#endif

namespace
{
    std::atomic<bool> g_count_allocations{false};
    std::atomic<std::size_t> g_allocations{0};
    std::atomic<std::size_t> g_allocated_bytes{0};
    std::atomic<const hgraph::ValueView *> g_atomic_value_view_input{nullptr};
    volatile std::uint64_t g_retained_value{0};
    std::uint64_t g_graph_observation{0};

    void record_allocation(std::size_t size) noexcept
    {
        if (g_count_allocations.load(std::memory_order_relaxed))
        {
            g_allocations.fetch_add(1, std::memory_order_relaxed);
            g_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] void *allocate_unaligned(std::size_t size)
    {
        const std::size_t actual_size = std::max<std::size_t>(size, 1);
        if (void *memory = std::malloc(actual_size))
        {
            return memory;
        }
        throw std::bad_alloc{};
    }

    [[nodiscard]] void *allocate_aligned(std::size_t size, std::size_t alignment)
    {
        const std::size_t actual_size = std::max<std::size_t>(size, 1);
#if defined(_MSC_VER)
        if (void *memory = _aligned_malloc(actual_size, alignment))
        {
            return memory;
        }
#else
        void *memory = nullptr;
        if (posix_memalign(&memory, alignment, actual_size) == 0)
        {
            return memory;
        }
#endif
        throw std::bad_alloc{};
    }

    void free_aligned(void *memory) noexcept
    {
#if defined(_MSC_VER)
        _aligned_free(memory);
#else
        std::free(memory);
#endif
    }

    struct AllocationScope
    {
        AllocationScope()
        {
            g_allocations.store(0, std::memory_order_relaxed);
            g_allocated_bytes.store(0, std::memory_order_relaxed);
            g_count_allocations.store(true, std::memory_order_relaxed);
        }

        ~AllocationScope()
        {
            g_count_allocations.store(false, std::memory_order_relaxed);
        }
    };

    struct Sample
    {
        double elapsed_ns{0.0};
        std::size_t allocations{0};
        std::size_t allocated_bytes{0};
        std::uint64_t checksum{0};
    };

    [[nodiscard]] std::size_t env_size(const char *name, std::size_t fallback, std::size_t minimum = 1)
    {
        const char *value = std::getenv(name);
        if (value == nullptr)
        {
            return fallback;
        }

        char *end = nullptr;
        const auto parsed = std::strtoull(value, &end, 10);
        if (end == value || *end != '\0' || parsed > std::numeric_limits<std::size_t>::max())
        {
            throw std::invalid_argument(std::string{name} + " must be a positive integer");
        }
        return std::max<std::size_t>(static_cast<std::size_t>(parsed), minimum);
    }

    template <typename T> [[nodiscard]] T median(std::vector<T> values)
    {
        std::ranges::sort(values);
        return values[values.size() / 2];
    }

    [[nodiscard]] double percentile(std::vector<double> values, double quantile)
    {
        std::ranges::sort(values);
        const double position = quantile * static_cast<double>(values.size() - 1);
        const auto lower = static_cast<std::size_t>(position);
        const auto upper = std::min(lower + 1, values.size() - 1);
        const double fraction = position - static_cast<double>(lower);
        return values[lower] + (values[upper] - values[lower]) * fraction;
    }

    [[nodiscard]] double median_absolute_deviation(const std::vector<double> &values)
    {
        const double center = median(values);
        std::vector<double> deviations;
        deviations.reserve(values.size());
        for (const double value : values)
        {
            deviations.push_back(std::abs(value - center));
        }
        return median(std::move(deviations));
    }

    [[nodiscard]] bool benchmark_selected(std::string_view name) noexcept
    {
        const char *filter = std::getenv("HGRAPH_TYPE_ERASURE_PERF_FILTER");
        return filter == nullptr || *filter == '\0' || name.contains(filter);
    }

    void retain(std::uint64_t value) noexcept
    {
        g_retained_value = value;
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }

    template <typename Operation, typename Check>
    void run_benchmark(std::string_view name, std::size_t default_iterations, std::size_t samples,
                       std::size_t warmup_iterations, Operation &&operation, Check &&check)
    {
        if (!benchmark_selected(name)) { return; }

        const std::size_t override_iterations = env_size("HGRAPH_TYPE_ERASURE_PERF_ITERATIONS", 0, 0);
        const std::size_t iterations = override_iterations == 0 ? default_iterations : override_iterations;

        check(operation());
        for (std::size_t i = 0; i < warmup_iterations; ++i)
        {
            retain(operation());
        }

        std::vector<Sample> results;
        results.reserve(samples);
        for (std::size_t sample_index = 0; sample_index < samples; ++sample_index)
        {
            std::uint64_t checksum = 0;
            const auto start = std::chrono::steady_clock::now();
            {
                AllocationScope allocations;
                for (std::size_t i = 0; i < iterations; ++i)
                {
                    checksum += operation();
                }
                results.push_back(Sample{
                    .elapsed_ns =
                        std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count(),
                    .allocations = g_allocations.load(std::memory_order_relaxed),
                    .allocated_bytes = g_allocated_bytes.load(std::memory_order_relaxed),
                    .checksum = checksum,
                });
            }
            retain(checksum);
        }

        std::vector<double> elapsed_per_op;
        std::vector<std::size_t> allocations;
        std::vector<std::size_t> bytes;
        elapsed_per_op.reserve(results.size());
        allocations.reserve(results.size());
        bytes.reserve(results.size());
        for (const auto &sample : results)
        {
            elapsed_per_op.push_back(sample.elapsed_ns / static_cast<double>(iterations));
            allocations.push_back(sample.allocations);
            bytes.push_back(sample.allocated_bytes);
        }

        const auto checksum = results.front().checksum;
        if (!std::ranges::all_of(results, [checksum](const Sample &sample) { return sample.checksum == checksum; }))
        {
            throw std::runtime_error(std::string{name} + " produced an unstable checksum");
        }

        const double elapsed_median = median(elapsed_per_op);
        const double elapsed_mad = median_absolute_deviation(elapsed_per_op);
        std::cout << "benchmark"
                  << " name=" << name << " samples=" << samples << " iterations=" << iterations
                  << " median_ns_per_op=" << elapsed_median
                  << " min_ns_per_op=" << *std::ranges::min_element(elapsed_per_op)
                  << " max_ns_per_op=" << *std::ranges::max_element(elapsed_per_op)
                  << " p10_ns_per_op=" << percentile(elapsed_per_op, 0.10)
                  << " p90_ns_per_op=" << percentile(elapsed_per_op, 0.90)
                  << " mad_ns_per_op=" << elapsed_mad
                  << " relative_mad_percent="
                  << (elapsed_median == 0.0 ? 0.0 : elapsed_mad * 100.0 / elapsed_median)
                  << " median_allocations=" << median(allocations) << " median_allocations_per_op="
                  << static_cast<double>(median(allocations)) / static_cast<double>(iterations)
                  << " median_bytes=" << median(bytes)
                  << " median_bytes_per_op=" << static_cast<double>(median(bytes)) / static_cast<double>(iterations)
                  << " checksum=" << checksum << '\n';
    }

    template <typename Operation>
    void require_no_allocations(std::string_view name,
                                std::size_t iterations,
                                Operation &&operation)
    {
        retain(operation());
        std::uint64_t checksum = 0;
        {
            AllocationScope allocations;
            for (std::size_t index = 0; index < iterations; ++index)
            {
                checksum += operation();
            }
        }
        retain(checksum);
        const auto allocations =
            g_allocations.load(std::memory_order_relaxed);
        if (allocations != 0)
        {
            throw std::runtime_error(
                std::string{name} + " allocated " +
                std::to_string(allocations) + " times");
        }
    }

    struct Doubler
    {
        static constexpr auto name = "type_erasure_perf_doubler";
        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(hgraph::Wiring &, hgraph::Port<hgraph::TS<hgraph::Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * hgraph::Int{2}).template as<hgraph::TS<hgraph::Int>>();
        }
    };

    struct BenchmarkMapper
    {
        static constexpr auto name = "type_erasure_perf_benchmark_mapper";
        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(hgraph::Wiring &, hgraph::Port<hgraph::TS<hgraph::Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return ((ts + hgraph::Int{1}) * hgraph::Int{2}).template as<hgraph::TS<hgraph::Int>>();
        }
    };

    struct Negator
    {
        static constexpr auto name = "type_erasure_perf_negator";
        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(hgraph::Wiring &, hgraph::Port<hgraph::TS<hgraph::Int>> ts)
        {
            using namespace hgraph::stdlib::syntax;
            return (ts * hgraph::Int{-1}).template as<hgraph::TS<hgraph::Int>>();
        }
    };

    struct SparseTsdSource
    {
        static constexpr auto name              = "type_erasure_perf_sparse_tsd_source";
        static constexpr bool schedule_on_start = true;

        static void eval(hgraph::Scalar<"keys", hgraph::Int> keys,
                         hgraph::Scalar<"per_cycle", hgraph::Int> per_cycle,
                         hgraph::State<hgraph::Int> cycle,
                         hgraph::Out<hgraph::TSD<hgraph::Int, hgraph::TS<hgraph::Int>>> out)
        {
            const hgraph::Int current = cycle.get();
            if (current == 0)
            {
                for (hgraph::Int key = 0; key < keys.value(); ++key) { out.set(key, hgraph::Int{0}); }
            }
            else
            {
                const hgraph::Int base = current * per_cycle.value() % keys.value();
                for (hgraph::Int offset = 0; offset < per_cycle.value(); ++offset)
                {
                    out.set((base + offset) % keys.value(), current);
                }
            }
            cycle.set(current + 1);
        }
    };

    struct DenseTsdSource
    {
        static constexpr auto name              = "type_erasure_perf_dense_tsd_source";
        static constexpr bool schedule_on_start = true;

        static void eval(hgraph::Scalar<"keys", hgraph::Int> keys,
                         hgraph::State<hgraph::Int> cycle,
                         hgraph::Out<hgraph::TSD<hgraph::Int, hgraph::TS<hgraph::Int>>> out)
        {
            const hgraph::Int current = cycle.get();
            for (hgraph::Int key = 0; key < keys.value(); ++key) { out.set(key, current + key); }
            cycle.set(current + 1);
        }
    };

    struct ChurnTsdSource
    {
        static constexpr auto name              = "type_erasure_perf_churn_tsd_source";
        static constexpr bool schedule_on_start = true;

        static void eval(hgraph::Scalar<"live", hgraph::Int> live,
                         hgraph::Scalar<"per_cycle", hgraph::Int> per_cycle,
                         hgraph::State<hgraph::Int> cycle,
                         hgraph::Out<hgraph::TSD<hgraph::Int, hgraph::TS<hgraph::Int>>> out)
        {
            const hgraph::Int current = cycle.get();
            if (current == 0)
            {
                for (hgraph::Int key = 0; key < live.value(); ++key) { out.set(key, key); }
            }
            else
            {
                const hgraph::Int base = (current - 1) * per_cycle.value();
                for (hgraph::Int offset = 0; offset < per_cycle.value(); ++offset)
                {
                    static_cast<void>(out.erase(base + offset));
                    out.set(live.value() + base + offset, current);
                }
            }
            cycle.set(current + 1);
        }
    };

    struct SparseDynamicTslSource
    {
        static constexpr auto name              = "type_erasure_perf_sparse_dynamic_tsl_source";
        static constexpr bool schedule_on_start = true;

        static void eval(hgraph::Scalar<"size", hgraph::Int> size,
                         hgraph::Scalar<"per_cycle", hgraph::Int> per_cycle,
                         hgraph::State<hgraph::Int> cycle,
                         hgraph::Out<hgraph::TSL<hgraph::TS<hgraph::Int>>> out)
        {
            const hgraph::Int current = cycle.get();
            if (current == 0)
            {
                for (hgraph::Int index = 0; index < size.value(); ++index)
                {
                    out.set(static_cast<std::size_t>(index), index);
                }
            }
            else
            {
                const hgraph::Int base = current * per_cycle.value() % size.value();
                for (hgraph::Int offset = 0; offset < per_cycle.value(); ++offset)
                {
                    out.set(static_cast<std::size_t>((base + offset) % size.value()),
                            size.value() + current);
                }
            }
            cycle.set(current + 1);
        }
    };

    struct DynamicTslObservationSink
    {
        static constexpr auto name = "type_erasure_perf_dynamic_tsl_observation_sink";

        static void eval(hgraph::In<"values", hgraph::TSL<hgraph::TS<hgraph::Int>>> values)
        {
            g_graph_observation = values.modified() ? 1 : 0;
        }
    };

    struct ScalarObservationSink
    {
        static constexpr auto name = "type_erasure_perf_scalar_observation_sink";

        static void eval(hgraph::In<"value", hgraph::TS<hgraph::Int>> value)
        {
            g_graph_observation = value.modified() ? 1 : 0;
        }
    };
}  // namespace

void *operator new(std::size_t size)
{
    record_allocation(size);
    return allocate_unaligned(size);
}

void *operator new[](std::size_t size)
{
    record_allocation(size);
    return allocate_unaligned(size);
}

void *operator new(std::size_t size, std::align_val_t alignment)
{
    record_allocation(size);
    return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void *operator new[](std::size_t size, std::align_val_t alignment)
{
    record_allocation(size);
    return allocate_aligned(size, static_cast<std::size_t>(alignment));
}

void *operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new(size);
    }
    catch (...)
    {
        return nullptr;
    }
}

void *operator new[](std::size_t size, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new[](size);
    }
    catch (...)
    {
        return nullptr;
    }
}

void *operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new(size, alignment);
    }
    catch (...)
    {
        return nullptr;
    }
}

void *operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t &) noexcept
{
    try
    {
        return ::operator new[](size, alignment);
    }
    catch (...)
    {
        return nullptr;
    }
}

void operator delete(void *memory) noexcept
{
    std::free(memory);
}
void operator delete[](void *memory) noexcept
{
    std::free(memory);
}
void operator delete(void *memory, std::size_t) noexcept
{
    std::free(memory);
}
void operator delete[](void *memory, std::size_t) noexcept
{
    std::free(memory);
}
void operator delete(void *memory, const std::nothrow_t &) noexcept
{
    std::free(memory);
}
void operator delete[](void *memory, const std::nothrow_t &) noexcept
{
    std::free(memory);
}
void operator delete(void *memory, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete[](void *memory, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete(void *memory, std::size_t, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete[](void *memory, std::size_t, std::align_val_t) noexcept
{
    free_aligned(memory);
}
void operator delete(void *memory, std::align_val_t, const std::nothrow_t &) noexcept
{
    free_aligned(memory);
}
void operator delete[](void *memory, std::align_val_t, const std::nothrow_t &) noexcept
{
    free_aligned(memory);
}

int main()
{
    using namespace hgraph;
    using namespace hgraph::testing;

    stdlib::register_standard_operators();
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("type_erasure_perf_int");
    const auto *ts_int = registry.ts(int_meta);

    const std::size_t samples = env_size("HGRAPH_TYPE_ERASURE_PERF_SAMPLES", 7, 7);
    const std::size_t warmup = env_size("HGRAPH_TYPE_ERASURE_PERF_WARMUP", 64, 0);

    const char *filter = std::getenv("HGRAPH_TYPE_ERASURE_PERF_FILTER");
    const char *host_label = std::getenv("HGRAPH_TYPE_ERASURE_PERF_HOST");
#if defined(__apple_build_version__)
    constexpr std::string_view compiler_family{"appleclang"};
#elif defined(__clang__)
    constexpr std::string_view compiler_family{"clang"};
#elif defined(__GNUC__)
    constexpr std::string_view compiler_family{"gcc"};
#elif defined(_MSC_VER)
    constexpr std::string_view compiler_family{"msvc"};
#else
    constexpr std::string_view compiler_family{"unknown"};
#endif
#if defined(__aarch64__) || defined(_M_ARM64)
    constexpr std::string_view architecture{"arm64"};
#elif defined(__x86_64__) || defined(_M_X64)
    constexpr std::string_view architecture{"x86_64"};
#else
    constexpr std::string_view architecture{"unknown"};
#endif
    std::cout << std::fixed << std::setprecision(3) << "type_erasure_perf format=2 samples=" << samples
              << " warmup_iterations=" << warmup << " filter="
              << (filter == nullptr || *filter == '\0' ? "all" : filter) << " host="
              << (host_label == nullptr || *host_label == '\0' ? "unspecified" : host_label)
              << " compiler=" << compiler_family
#if defined(__clang__)
              << " compiler_version=" << __clang_major__ << '.' << __clang_minor__ << '.' << __clang_patchlevel__
#elif defined(__GNUC__)
              << " compiler_version=" << __GNUC__ << '.' << __GNUC_MINOR__ << '.' << __GNUC_PATCHLEVEL__
#elif defined(_MSC_VER)
              << " compiler_version=" << _MSC_VER
#else
              << " compiler_version=unknown"
#endif
              << " architecture=" << architecture << '\n';

    Value atomic_value{Int{41}};
    auto atomic_view = atomic_value.view();
    // Loading the source through an atomic pointer makes its binding, schema,
    // and payload opaque on every iteration without disabling optimization of
    // the access path being measured. A local volatile result is insufficient
    // because the compiler can still fold the source-side checked_as work.
    g_atomic_value_view_input.store(&atomic_view, std::memory_order_release);

    const Instant temporal_instant{Duration{41}};
    const InstantRange temporal_range =
        InstantRange::bounded(Instant{Duration{0}},
                              Instant{Duration{100}});
    const InstantRange temporal_middle =
        InstantRange::bounded(Instant{Duration{25}},
                              Instant{Duration{75}});
    const auto temporal_provider = make_time_zone_provider();
    const ZoneId temporal_zone{"UTC"};
    (void)hgraph::at_zone(
        temporal_instant, temporal_zone, *temporal_provider);

    const auto checked_add_operation = [&] {
        return static_cast<std::uint64_t>(
            checked_add(temporal_instant, Duration{1})
                .time_since_epoch()
                .count());
    };
    require_no_allocations(
        "temporal_checked_add", 10'000, checked_add_operation);
    run_benchmark(
        "temporal_checked_add", 200'000, samples, warmup,
        checked_add_operation,
        [](std::uint64_t value) {
            if (value != 42)
            {
                throw std::runtime_error(
                    "temporal checked add failed");
            }
        });

    const auto range_operation = [&] {
        const auto value =
            temporal_range.difference(temporal_middle);
        return static_cast<std::uint64_t>(
            value.size() + value[0].contains(Instant{Duration{1}}));
    };
    require_no_allocations(
        "temporal_range_difference", 10'000, range_operation);
    run_benchmark(
        "temporal_range_difference", 100'000, samples, warmup,
        range_operation,
        [](std::uint64_t value) {
            if (value != 3)
            {
                throw std::runtime_error(
                    "temporal range difference failed");
            }
        });

    const auto cached_zone_operation = [&] {
        return static_cast<std::uint64_t>(
            hgraph::at_zone(
                temporal_instant, temporal_zone, *temporal_provider)
                .offset_seconds());
    };
    require_no_allocations(
        "temporal_cached_zone", 10'000, cached_zone_operation);
    run_benchmark(
        "temporal_cached_zone", 50'000, samples, warmup,
        cached_zone_operation,
        [](std::uint64_t value) {
            if (value != 0)
            {
                throw std::runtime_error(
                    "temporal cached zone projection failed");
            }
        });

    run_benchmark(
        "atomic_value_read", 200000, samples, warmup,
        [&] {
            const auto *source = g_atomic_value_view_input.load(std::memory_order_relaxed);
            return static_cast<std::uint64_t>(source->checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 41)
            {
                throw std::runtime_error("atomic value read failed");
            }
        });

    TSOutput scalar_output{*ts_int};
    {
        auto mutation = scalar_output.begin_mutation(MIN_ST);
        if (!mutation.move_value_from(Value{Int{43}}))
        {
            throw std::runtime_error("scalar TS setup failed");
        }
    }
    run_benchmark(
        "scalar_ts_read", 100000, samples, warmup,
        [&] { return static_cast<std::uint64_t>(scalar_output.view(MIN_ST).value().checked_as<Int>()); },
        [](std::uint64_t value) {
            if (value != 43)
            {
                throw std::runtime_error("scalar TS read failed");
            }
        });

    const auto *bundle_meta = registry.tsb("TypeErasurePerfBundle", {{"left", ts_int}, {"right", ts_int}});
    const auto bundle_binding = ValuePlanFactory::instance().type_for(bundle_meta->value_schema);
    if (bundle_binding == nullptr)
    {
        throw std::runtime_error("bundle value binding is missing");
    }
    BundleBuilder bundle_builder{bundle_binding};
    bundle_builder.set("left", Value{Int{47}});
    bundle_builder.set("right", Value{Int{53}});
    TSOutput bundle_output{*bundle_meta};
    {
        auto mutation = bundle_output.begin_mutation(MIN_ST);
        if (!mutation.move_value_from(bundle_builder.build()))
        {
            throw std::runtime_error("composite TS setup failed");
        }
    }
    run_benchmark(
        "fixed_composite_ts_child_read", 50000, samples, warmup,
        [&] {
            auto output_view = bundle_output.view(MIN_ST);
            auto bundle_view = output_view.as_bundle();
            return static_cast<std::uint64_t>(bundle_view.field("left").value().checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 47)
            {
                throw std::runtime_error("composite TS child read failed");
            }
        });

    const auto *dynamic_list_meta = registry.tsl(ts_int, 0);
    ListBuilder dynamic_source_builder{ValuePlanFactory::instance().type_for(int_meta)};
    for (const Int value : {Int{59}, Int{60}, Int{61}, Int{62}})
        dynamic_source_builder.push_back(value);
    Value dynamic_source = dynamic_source_builder.build();
    TSOutput dynamic_output{dynamic_list_meta};
    {
        auto mutation = dynamic_output.begin_mutation(MIN_ST);
        if (!mutation.copy_value_from(dynamic_source.view()))
            throw std::runtime_error("dynamic TSL setup failed");
    }
    run_benchmark(
        "dynamic_tsl_pregrown_child_read", 50000, samples, warmup,
        [&] {
            auto output_view = dynamic_output.view(MIN_ST);
            auto list = output_view.as_list();
            return static_cast<std::uint64_t>(list.at(0).value().checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 59) throw std::runtime_error("dynamic TSL child read failed");
        });
    std::uint64_t dynamic_construct_time = 0;
    run_benchmark(
        "dynamic_tsl_construct_grow_four", 1000, samples, warmup,
        [&] {
            TSOutput output{dynamic_list_meta};
            auto mutation = output.begin_mutation(
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++dynamic_construct_time)});
            if (!mutation.copy_value_from(dynamic_source.view()))
                throw std::runtime_error("dynamic TSL construct/grow failed");
            return static_cast<std::uint64_t>(output.data_view().indexed_child_count());
        },
        [](std::uint64_t value) {
            if (value != 4) throw std::runtime_error("dynamic TSL construct/grow size mismatch");
        });

    Value tick_value{Int{61}};
    const auto *tick_window_meta = registry.tsw(int_meta, 4, 1);
    TSOutput tick_window{tick_window_meta};
    std::uint64_t tick_time = 0;
    for (std::size_t index = 0; index < 4; ++index)
    {
        auto window_data = tick_window.data_view();
        auto window = window_data.as_window();
        auto mutation = window.begin_mutation(
            MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++tick_time)});
        mutation.push(tick_value.view());
    }
    auto tick_bound_window_data = tick_window.data_view();
    auto tick_bound_window = tick_bound_window_data.as_window();
    run_benchmark(
        "tsw_tick_bound_back_read", 100000, samples, warmup,
        [&] { return static_cast<std::uint64_t>(tick_bound_window.back().checked_as<Int>()); },
        [](std::uint64_t value) {
            if (value != 61) throw std::runtime_error("tick TSW bound back read failed");
        });
    run_benchmark(
        "tsw_tick_view_back_read", 100000, samples, warmup,
        [&] {
            auto window_data = tick_window.data_view();
            auto window = window_data.as_window();
            return static_cast<std::uint64_t>(window.back().checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 61) throw std::runtime_error("tick TSW view back read failed");
        });
    run_benchmark(
        "tsw_tick_steady_push_evict_no_read", 20000, samples, warmup,
        [&] {
            auto window_data = tick_window.data_view();
            auto window = window_data.as_window();
            auto mutation = window.begin_mutation(
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++tick_time)});
            mutation.push(tick_value.view());
            return std::uint64_t{1};
        },
        [](std::uint64_t value) {
            if (value != 1) throw std::runtime_error("tick TSW steady mutation failed");
        });
    run_benchmark(
        "tsw_tick_steady_push_evict", 20000, samples, warmup,
        [&] {
            auto window_data = tick_window.data_view();
            auto window = window_data.as_window();
            auto mutation = window.begin_mutation(
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++tick_time)});
            mutation.push(tick_value.view());
            return static_cast<std::uint64_t>(mutation.back().checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 61) throw std::runtime_error("tick TSW steady eviction failed");
        });

    Value duration_value{Int{67}};
    const auto *duration_window_meta = registry.tsw_duration(int_meta, TimeDelta{10}, TimeDelta{0});
    TSOutput duration_window{duration_window_meta};
    std::uint64_t duration_time = 0;
    {
        auto window_data = duration_window.data_view();
        auto window = window_data.as_window();
        auto mutation = window.begin_mutation(
            MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(duration_time += 11)});
        mutation.push(duration_value.view());
    }
    auto duration_bound_window_data = duration_window.data_view();
    auto duration_bound_window = duration_bound_window_data.as_window();
    run_benchmark(
        "tsw_duration_bound_back_read", 100000, samples, warmup,
        [&] { return static_cast<std::uint64_t>(duration_bound_window.back().checked_as<Int>()); },
        [](std::uint64_t value) {
            if (value != 67) throw std::runtime_error("duration TSW bound back read failed");
        });
    run_benchmark(
        "tsw_duration_steady_push_evict_no_read", 20000, samples, warmup,
        [&] {
            auto window_data = duration_window.data_view();
            auto window = window_data.as_window();
            auto mutation = window.begin_mutation(
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(duration_time += 11)});
            mutation.push(duration_value.view());
            return std::uint64_t{1};
        },
        [](std::uint64_t value) {
            if (value != 1) throw std::runtime_error("duration TSW steady mutation failed");
        });
    run_benchmark(
        "tsw_duration_steady_push_evict", 20000, samples, warmup,
        [&] {
            auto window_data = duration_window.data_view();
            auto window = window_data.as_window();
            auto mutation = window.begin_mutation(
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(duration_time += 11)});
            mutation.push(duration_value.view());
            return static_cast<std::uint64_t>(mutation.back().checked_as<Int>());
        },
        [](std::uint64_t value) {
            if (value != 67) throw std::runtime_error("duration TSW steady eviction failed");
        });

    std::uint64_t node_evaluations = 0;
    NodeTypeMetaData node_schema;
    node_schema.display_name = "type_erasure_perf_node";
    node_schema.node_kind = NodeKind::Sink;
    NodeCallbacks node_callbacks;
    node_callbacks.evaluate = [&node_evaluations](const NodeView &, DateTime) { ++node_evaluations; };
    NodeValue node = NodeBuilder::native(std::move(node_schema), std::move(node_callbacks)).make_node();
    auto node_view = node.view();
    node_view.start(MIN_ST);
    std::uint64_t evaluation_time = 0;
    run_benchmark(
        "erased_native_node_evaluate", 100000, samples, warmup,
        [&] {
            ++evaluation_time;
            return static_cast<std::uint64_t>(
                node_view.evaluate(MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(evaluation_time)}));
        },
        [](std::uint64_t value) {
            if (value != 1)
            {
                throw std::runtime_error("native node evaluation failed");
            }
        });
    node_view.stop(MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(evaluation_time + 1)});
    retain(node_evaluations);

    NodeTypeMetaData graph_node_schema;
    graph_node_schema.display_name = "type_erasure_perf_graph_node";
    graph_node_schema.node_kind = NodeKind::Sink;
    NodeCallbacks graph_node_callbacks;
    graph_node_callbacks.evaluate = [](const NodeView &, DateTime) {};
    GraphBuilder graph_builder;
    graph_builder.label("type_erasure_perf_small_graph")
        .add_node(NodeBuilder::native(std::move(graph_node_schema), std::move(graph_node_callbacks)));
    run_benchmark(
        "small_graph_construct_destroy", 2000, samples, warmup,
        [&] {
            MockRootGraph graph{graph_builder};
            return static_cast<std::uint64_t>(graph.graph().node_count());
        },
        [](std::uint64_t value) {
            if (value != 1)
            {
                throw std::runtime_error("small graph construction failed");
            }
        });

    MockGraphExecutor unprofiled_executor{graph_builder, MIN_ST, MAX_ET};
    auto unprofiled_graph = unprofiled_executor.view().graph();
    unprofiled_graph.start(MIN_ST);
    std::uint64_t unprofiled_cycle = 0;
    run_benchmark(
        "evaluation_profiler_disabled_cycle", 20000, samples, warmup,
        [&] {
            const DateTime evaluation_time =
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++unprofiled_cycle)};
            unprofiled_executor.set_evaluation_time(evaluation_time);
            unprofiled_graph.schedule_node(0, evaluation_time);
            if (!unprofiled_graph.evaluate(evaluation_time))
            {
                throw std::runtime_error("unprofiled graph evaluation paused");
            }
            return std::uint64_t{1};
        },
        [](std::uint64_t value) {
            if (value != 1) { throw std::runtime_error("unprofiled graph evaluation failed"); }
        });
    unprofiled_graph.stop();

    EvaluationProfiler profiler;
    MockGraphExecutor profiled_executor{graph_builder, MIN_ST, MAX_ET};
    profiled_executor.view().lifecycle_observers().add(&profiler);
    auto profiled_graph = profiled_executor.view().graph();
    profiled_graph.start(MIN_ST);
    std::uint64_t profiled_cycle = 0;
    run_benchmark(
        "evaluation_profiler_enabled_cycle", 20000, samples, warmup,
        [&] {
            const DateTime evaluation_time =
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++profiled_cycle)};
            profiled_executor.set_evaluation_time(evaluation_time);
            profiled_graph.schedule_node(0, evaluation_time);
            if (!profiled_graph.evaluate(evaluation_time))
            {
                throw std::runtime_error("profiled graph evaluation paused");
            }
            return std::uint64_t{1};
        },
        [](std::uint64_t value) {
            if (value != 1) { throw std::runtime_error("profiled graph evaluation failed"); }
        });
    profiled_graph.stop();
    const auto profile = profiler.snapshot();
    if (profile.graph_cycles == 0 || profile.entries.size() != 2)
    {
        throw std::runtime_error("evaluation profiler benchmark produced no measurements");
    }

    Wiring nested_wiring;
    auto nested_source = wire<stdlib::const_, TSD<Str, TS<Int>>>(
        nested_wiring,
        stdlib::make_map<Str, Int>({{Str{"a"}, Int{1}}, {Str{"b"}, Int{2}},
                                    {Str{"c"}, Int{3}}}));
    static_cast<void>(wire<stdlib::map_>(nested_wiring, fn<Doubler>(), nested_source));
    static_cast<void>(wire<stdlib::mesh_>(nested_wiring, fn<Doubler>(), nested_source));
    static_cast<void>(wire<stdlib::reduce_>(nested_wiring, fn<stdlib::add_>(), nested_source));

    GraphBuilder nested_builder = std::move(nested_wiring).finish();
    MockGraphExecutor nested_executor{nested_builder, MIN_ST, MAX_ET};
    auto nested_graph = nested_executor.view().graph();
    nested_graph.start(MIN_ST);
    nested_executor.set_evaluation_time(MIN_ST);
    if (!nested_graph.evaluate(MIN_ST))
    {
        throw std::runtime_error("nested steady-state benchmark setup paused");
    }

    std::vector<std::size_t> nested_node_indices;
    std::uint64_t nested_checksum = 0;
    for (std::size_t index = 0; index < nested_graph.node_count(); ++index)
    {
        auto node = nested_graph.node_at(index);
        if (node.is<MapNodeView>())
        {
            nested_node_indices.push_back(index);
            nested_checksum += node.as<MapNodeView>().active_count();
        }
        else if (node.is<MeshNodeView>())
        {
            nested_node_indices.push_back(index);
            nested_checksum += node.as<MeshNodeView>().active_count();
        }
        else if (node.is<ReduceNodeView>())
        {
            nested_node_indices.push_back(index);
            nested_checksum += node.as<ReduceNodeView>().combiner_count();
        }
    }
    if (nested_node_indices.size() != 3 || nested_checksum == 0)
    {
        throw std::runtime_error("nested steady-state benchmark did not find its runtime nodes");
    }

    std::uint64_t nested_time = 0;
    run_benchmark(
        "nested_graph_steady_scheduled_scan", 20000, samples, warmup,
        [&] {
            const DateTime evaluation_time =
                MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++nested_time)};
            nested_executor.set_evaluation_time(evaluation_time);
            for (const std::size_t index : nested_node_indices)
            {
                nested_graph.schedule_node(index, evaluation_time);
            }
            if (!nested_graph.evaluate(evaluation_time))
            {
                throw std::runtime_error("nested steady-state benchmark paused");
            }
            return nested_checksum;
        },
        [nested_checksum](std::uint64_t value) {
            if (value != nested_checksum)
            {
                throw std::runtime_error("nested steady-state benchmark checksum failed");
            }
        });
    nested_graph.stop();

    const auto run_native_graph = [&](std::string_view name, GraphBuilder builder,
                                      std::size_t iterations,
                                      std::optional<std::uint64_t> expected_observation = std::nullopt) {
        MockGraphExecutor executor{builder, MIN_ST, MAX_ET};
        auto graph = executor.view().graph();
        graph.start(MIN_ST);
        executor.set_evaluation_time(MIN_ST);
        if (!graph.evaluate(MIN_ST))
        {
            throw std::runtime_error(std::string{name} + " setup paused");
        }

        std::uint64_t cycle = 0;
        run_benchmark(
            name, iterations, samples, warmup,
            [&] {
                const DateTime evaluation_time =
                    MIN_ST + TimeDelta{static_cast<TimeDelta::rep>(++cycle)};
                executor.set_evaluation_time(evaluation_time);
                if (expected_observation.has_value()) { g_graph_observation = 0; }
                graph.schedule_node(0, evaluation_time);
                if (!graph.evaluate(evaluation_time))
                {
                    throw std::runtime_error(std::string{name} + " paused");
                }
                return expected_observation.has_value() ? g_graph_observation : 1;
            },
            [name, expected_observation](std::uint64_t value) {
                const auto expected = expected_observation.value_or(1);
                if (expected_observation.has_value() && g_graph_observation != expected)
                {
                    throw std::runtime_error(std::string{name} + " did not publish the expected delta");
                }
                if (value != expected) { throw std::runtime_error(std::string{name} + " checksum failed"); }
            });
        graph.stop();
    };

    {
        Wiring wiring;
        auto source = wire<SparseTsdSource>(wiring, Int{2000}, Int{5});
        static_cast<void>(wire<stdlib::null_sink>(wiring, source));
        run_native_graph("native_sparse_tsd_source_cycle", std::move(wiring).finish(), 20000);
    }
    {
        Wiring wiring;
        auto source = wire<SparseTsdSource>(wiring, Int{2000}, Int{5});
        auto mapped = wire<stdlib::map_>(wiring, fn<BenchmarkMapper>(), source);
        static_cast<void>(wire<stdlib::null_sink>(wiring, mapped));
        run_native_graph("native_sparse_tsd_map_cycle", std::move(wiring).finish(), 20000);
    }
    {
        Wiring wiring;
        auto source = wire<SparseTsdSource>(wiring, Int{2000}, Int{5});
        auto reduced = wire<stdlib::reduce_>(wiring, fn<stdlib::add_>(), source);
        static_cast<void>(wire<stdlib::null_sink>(wiring, reduced));
        run_native_graph("native_sparse_tsd_reduce_cycle", std::move(wiring).finish(), 20000);
    }
    {
        Wiring wiring;
        auto source = wire<SparseTsdSource>(wiring, Int{2000}, Int{5});
        auto mapped = wire<stdlib::map_>(wiring, fn<BenchmarkMapper>(), source);
        auto reduced = wire<stdlib::reduce_>(wiring, fn<stdlib::add_>(), mapped);
        static_cast<void>(wire<stdlib::null_sink>(wiring, reduced));
        run_native_graph("native_sparse_tsd_map_reduce_cycle", std::move(wiring).finish(), 20000);
    }

    const auto add_native_tsd_variants = [&](std::string_view prefix, auto wire_source) {
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            static_cast<void>(wire<stdlib::null_sink>(wiring, source));
            run_native_graph(std::string{prefix} + "_source_cycle", std::move(wiring).finish(), 2000);
        }
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            auto mapped = wire<stdlib::map_>(wiring, fn<BenchmarkMapper>(), source);
            static_cast<void>(wire<stdlib::null_sink>(wiring, mapped));
            run_native_graph(std::string{prefix} + "_map_cycle", std::move(wiring).finish(), 2000);
        }
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            auto reduced = wire<stdlib::reduce_>(wiring, fn<stdlib::add_>(), source);
            static_cast<void>(wire<stdlib::null_sink>(wiring, reduced));
            run_native_graph(std::string{prefix} + "_reduce_cycle", std::move(wiring).finish(), 2000);
        }
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            auto mapped = wire<stdlib::map_>(wiring, fn<BenchmarkMapper>(), source);
            auto reduced = wire<stdlib::reduce_>(wiring, fn<stdlib::add_>(), mapped);
            static_cast<void>(wire<stdlib::null_sink>(wiring, reduced));
            run_native_graph(std::string{prefix} + "_map_reduce_cycle", std::move(wiring).finish(), 2000);
        }
    };

    add_native_tsd_variants("native_dense_tsd", [](Wiring &wiring) {
        return wire<DenseTsdSource>(wiring, Int{200});
    });
    add_native_tsd_variants("native_dense_400_tsd", [](Wiring &wiring) {
        return wire<DenseTsdSource>(wiring, Int{400});
    });
    add_native_tsd_variants("native_dense_1000_tsd", [](Wiring &wiring) {
        return wire<DenseTsdSource>(wiring, Int{1000});
    });
    add_native_tsd_variants("native_churn_tsd", [](Wiring &wiring) {
        return wire<ChurnTsdSource>(wiring, Int{200}, Int{5});
    });

    const auto add_native_dynamic_tsl_variants = [&](std::string_view prefix) {
        const auto wire_source = [](Wiring &wiring) {
            return wire<SparseDynamicTslSource>(wiring, Int{2000}, Int{5});
        };
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            static_cast<void>(wire<DynamicTslObservationSink>(wiring, source));
            run_native_graph(std::string{prefix} + "_source_cycle", std::move(wiring).finish(), 20000, 1);
        }
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            auto mapped = wire<stdlib::map_>(wiring, fn<BenchmarkMapper>(), source);
            static_cast<void>(wire<DynamicTslObservationSink>(wiring, mapped));
            run_native_graph(std::string{prefix} + "_map_cycle", std::move(wiring).finish(), 20000, 1);
        }
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            auto reduced = wire<stdlib::reduce_>(wiring, fn<stdlib::add_>(), source);
            static_cast<void>(wire<ScalarObservationSink>(wiring, reduced));
            run_native_graph(std::string{prefix} + "_reduce_cycle", std::move(wiring).finish(), 20000, 1);
        }
        {
            Wiring wiring;
            auto source = wire_source(wiring);
            auto mapped = wire<stdlib::map_>(wiring, fn<BenchmarkMapper>(), source);
            auto reduced = wire<stdlib::reduce_>(wiring, fn<stdlib::add_>(), mapped);
            static_cast<void>(wire<ScalarObservationSink>(wiring, reduced));
            run_native_graph(std::string{prefix} + "_map_reduce_cycle", std::move(wiring).finish(), 20000, 1);
        }
    };

    add_native_dynamic_tsl_variants("native_sparse_dynamic_tsl");

    const std::vector<std::optional<Str>> switch_keys{Str{"a"}, Str{"b"}, Str{"a"}, Str{"b"}};
    const std::vector<std::optional<Int>> switch_inputs{Int{3}, Int{4}, Int{5}, Int{6}};
    const auto switch_cases =
        stdlib::switch_cases({{Value{Str{"a"}}, fn<Doubler>()}, {Value{Str{"b"}}, fn<Negator>()}});
    run_benchmark(
        "alternating_switch_nested_graph_lifecycle", 20, samples, 1,
        [&] {
            const auto result = eval_node<stdlib::switch_>(switch_keys, switch_cases, switch_inputs);
            if (result.size() != 4 || !result[0].has_value() || !result[1].has_value() || !result[2].has_value() ||
                !result[3].has_value())
            {
                throw std::runtime_error("switch lifecycle result shape mismatch");
            }
            const auto a = result[0]->as<Int>();
            const auto b = result[1]->as<Int>();
            const auto c = result[2]->as<Int>();
            const auto d = result[3]->as<Int>();
            if (a != 6 || b != -4 || c != 10 || d != -6)
            {
                throw std::runtime_error("switch lifecycle result mismatch");
            }
            return static_cast<std::uint64_t>(a - b + c - d);
        },
        [](std::uint64_t value) {
            if (value != 26)
            {
                throw std::runtime_error("switch lifecycle checksum failed");
            }
        });

    return 0;
}
