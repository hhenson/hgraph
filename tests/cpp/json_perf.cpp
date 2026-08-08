#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace
{
    std::atomic<bool>        g_count_allocations{false};
    std::atomic<std::size_t> g_allocations{0};
    std::atomic<std::size_t> g_allocated_bytes{0};

    void record_allocation(std::size_t size) noexcept
    {
        if (g_count_allocations.load(std::memory_order_relaxed))
        {
            g_allocations.fetch_add(1, std::memory_order_relaxed);
            g_allocated_bytes.fetch_add(size, std::memory_order_relaxed);
        }
    }

    struct AllocationScope
    {
        AllocationScope()
        {
            g_allocations.store(0, std::memory_order_relaxed);
            g_allocated_bytes.store(0, std::memory_order_relaxed);
            g_count_allocations.store(true, std::memory_order_relaxed);
        }

        ~AllocationScope() { g_count_allocations.store(false, std::memory_order_relaxed); }
    };

    struct Metrics
    {
        std::string  name;
        std::size_t  ticks{0};
        double       milliseconds{0.0};
        std::size_t  allocations{0};
        std::size_t  allocated_bytes{0};
    };

    std::string make_payload(int field_count)
    {
        std::string payload;
        payload.reserve(static_cast<std::size_t>(field_count) * 64U);
        payload += "{\"target\":{\"answer\":41,\"label\":\"selected\",\"values\":[1,2,3,4]},";
        for (int i = 0; i < field_count; ++i)
        {
            if (i != 0) { payload += ','; }
            payload += "\"field";
            payload += std::to_string(i);
            payload += "\":{\"id\":";
            payload += std::to_string(i);
            payload += ",\"name\":\"instrument-";
            payload += std::to_string(i);
            payload += "\",\"prices\":[";
            for (int j = 0; j < 8; ++j)
            {
                if (j != 0) { payload += ','; }
                payload += std::to_string(i * 10 + j);
            }
            payload += "]}";
        }
        payload += '}';
        return payload;
    }

    std::string make_payload_with_space(int field_count)
    {
        std::string payload;
        payload.reserve(static_cast<std::size_t>(field_count) * 70U);
        payload += "{ \"target\" : { \"answer\" : 41, \"label\" : \"selected\", \"values\" : [1,2,3,4] }";
        for (int i = 0; i < field_count; ++i)
        {
            payload += ", \"field";
            payload += std::to_string(i);
            payload += "\" : { \"id\" : ";
            payload += std::to_string(i);
            payload += ", \"name\" : \"instrument-";
            payload += std::to_string(i);
            payload += "\", \"prices\" : [";
            for (int j = 0; j < 8; ++j)
            {
                if (j != 0) { payload += ", "; }
                payload += std::to_string(i * 10 + j);
            }
            payload += "] }";
        }
        payload += " }";
        return payload;
    }

    struct JsonExtractGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_extract_perf_graph";

        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(hgraph::Wiring &w,
                                                             hgraph::Port<hgraph::TS<hgraph::Str>> raw)
        {
            auto decoded = hgraph::wire<hgraph::stdlib::json_decode>(w, raw);
            auto target  = hgraph::wire<hgraph::stdlib::getitem_>(w, decoded, hgraph::Str{"target"});
            auto answer  = hgraph::wire<hgraph::stdlib::getitem_>(w, target, hgraph::Str{"answer"});
            return hgraph::wire<hgraph::stdlib::json_as_int>(w, answer).template as<hgraph::TS<hgraph::Int>>();
        }
    };

    struct JsonEncodeGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_encode_perf_graph";

        static hgraph::Port<hgraph::TS<hgraph::Str>> compose(hgraph::Wiring &w,
                                                             hgraph::Port<hgraph::TS<hgraph::Str>> raw)
        {
            auto decoded = hgraph::wire<hgraph::stdlib::json_decode>(w, raw);
            return hgraph::wire<hgraph::stdlib::json_encode, hgraph::TS<hgraph::Str>>(w, decoded)
                .template as<hgraph::TS<hgraph::Str>>();
        }
    };

    struct JsonEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_equality_perf_graph";

        static hgraph::Port<hgraph::TS<hgraph::Bool>>
        compose(hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Str>> lhs,
                hgraph::Port<hgraph::TS<hgraph::Str>> rhs)
        {
            auto decoded_lhs = hgraph::wire<hgraph::stdlib::json_decode>(w, lhs);
            auto decoded_rhs = hgraph::wire<hgraph::stdlib::json_decode>(w, rhs);
            return hgraph::wire<hgraph::stdlib::eq_>(w, decoded_lhs, decoded_rhs)
                .template as<hgraph::TS<hgraph::Bool>>();
        }
    };

    template <typename OutSchema>
    void wire_record(hgraph::Wiring &w, hgraph::Port<OutSchema> out, std::string_view key)
    {
        hgraph::wire<hgraph::stdlib::dense_record_impl>(w, out, std::string{key});
    }

    template <typename Graph>
    Metrics run_one_input(std::string name, const std::vector<std::optional<hgraph::Str>> &input)
    {
        hgraph::Wiring w;
        auto raw = hgraph::wire<hgraph::stdlib::replay_impl, hgraph::TS<hgraph::Str>>(w, std::string{"raw"});
        auto out = Graph::compose(w, raw);
        wire_record(w, out, "out");

        hgraph::GraphBuilder gb = std::move(w).finish();
        hgraph::testing::set_replay_values<hgraph::Str>(gb.global_state(), "raw", input);

        hgraph::GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(hgraph::MIN_ST).end_time(hgraph::MAX_ET);
        hgraph::GraphExecutorValue executor = eb.make_executor();
        auto                       view     = executor.view();

        const auto start = std::chrono::steady_clock::now();
        {
            AllocationScope allocations;
            view.run();
        }
        const auto end = std::chrono::steady_clock::now();
        return Metrics{
            std::move(name),
            input.size(),
            std::chrono::duration<double, std::milli>(end - start).count(),
            g_allocations.load(std::memory_order_relaxed),
            g_allocated_bytes.load(std::memory_order_relaxed),
        };
    }

    Metrics run_equality(const std::vector<std::optional<hgraph::Str>> &lhs,
                         const std::vector<std::optional<hgraph::Str>> &rhs)
    {
        hgraph::Wiring w;
        auto left = hgraph::wire<hgraph::stdlib::replay_impl, hgraph::TS<hgraph::Str>>(w, std::string{"lhs"});
        auto right = hgraph::wire<hgraph::stdlib::replay_impl, hgraph::TS<hgraph::Str>>(w, std::string{"rhs"});
        auto out = JsonEqualityGraph::compose(w, left, right);
        wire_record(w, out, "out");

        hgraph::GraphBuilder gb = std::move(w).finish();
        hgraph::testing::set_replay_values<hgraph::Str>(gb.global_state(), "lhs", lhs);
        hgraph::testing::set_replay_values<hgraph::Str>(gb.global_state(), "rhs", rhs);

        hgraph::GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(hgraph::MIN_ST).end_time(hgraph::MAX_ET);
        hgraph::GraphExecutorValue executor = eb.make_executor();
        auto                       view     = executor.view();

        const auto start = std::chrono::steady_clock::now();
        {
            AllocationScope allocations;
            view.run();
        }
        const auto end = std::chrono::steady_clock::now();
        return Metrics{
            "decode+semantic_equal",
            lhs.size(),
            std::chrono::duration<double, std::milli>(end - start).count(),
            g_allocations.load(std::memory_order_relaxed),
            g_allocated_bytes.load(std::memory_order_relaxed),
        };
    }

    void print_metrics(const Metrics &metrics)
    {
        const double ticks = static_cast<double>(metrics.ticks);
        std::cout << metrics.name
                  << " ticks=" << metrics.ticks
                  << " ms=" << metrics.milliseconds
                  << " us_per_tick=" << (metrics.milliseconds * 1000.0 / ticks)
                  << " allocs=" << metrics.allocations
                  << " allocs_per_tick=" << (static_cast<double>(metrics.allocations) / ticks)
                  << " bytes=" << metrics.allocated_bytes
                  << " bytes_per_tick=" << (static_cast<double>(metrics.allocated_bytes) / ticks)
                  << '\n';
    }

    int env_int(const char *name, int fallback)
    {
        const char *value = std::getenv(name);
        if (value == nullptr) { return fallback; }
        return std::max(1, std::atoi(value));
    }
}  // namespace

void *operator new(std::size_t size)
{
    record_allocation(size);
    if (void *memory = std::malloc(size)) { return memory; }
    throw std::bad_alloc{};
}

void *operator new[](std::size_t size)
{
    record_allocation(size);
    if (void *memory = std::malloc(size)) { return memory; }
    throw std::bad_alloc{};
}

void operator delete(void *memory) noexcept { std::free(memory); }
void operator delete[](void *memory) noexcept { std::free(memory); }
void operator delete(void *memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void *memory, std::size_t) noexcept { std::free(memory); }

int main()
{
    hgraph::stdlib::register_standard_operators();

    const int ticks = env_int("HGRAPH_JSON_PERF_TICKS", 2000);
    const int fields = env_int("HGRAPH_JSON_PERF_FIELDS", 200);
    const hgraph::Str payload{make_payload(fields)};
    const hgraph::Str equivalent_payload{make_payload_with_space(fields)};

    std::vector<std::optional<hgraph::Str>> input(static_cast<std::size_t>(ticks), payload);
    std::vector<std::optional<hgraph::Str>> input_equivalent(static_cast<std::size_t>(ticks), equivalent_payload);

    std::cout << "payload_bytes=" << payload.size()
              << " equivalent_payload_bytes=" << equivalent_payload.size()
              << " ticks=" << ticks
              << " fields=" << fields
              << '\n';
    print_metrics(run_one_input<JsonExtractGraph>("decode+extract_leaf", input));
    print_metrics(run_one_input<JsonEncodeGraph>("decode+encode", input));
    print_metrics(run_equality(input, input_equivalent));
}
