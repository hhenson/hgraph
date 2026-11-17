#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Run: hgraph_tsd_proxy_perf <entries> <changed> <value|reference> <slots|range>
// Times publication and delta consumption on a TSD[int, REF[TS[int]]] proxy.
// Setup is excluded; every tick checks the modified count and captured values.
int main(int argc, char **argv) {
    using namespace hgraph;
    if (argc != 5) {
        std::cerr << "Usage: hgraph_tsd_proxy_perf <entries> <changed> <value|reference> <slots|range>\n";
        return 2;
    }
    const auto n = std::stoull(argv[1]);
    const auto changed = std::stoull(argv[2]);
    const bool retarget = std::string{argv[3]} == "reference";
    const bool range = std::string{argv[4]} == "range";
    if (!changed || changed > n) return 2;
    if (!retarget && std::string{argv[3]} != "value") return 2;
    if (!range && std::string{argv[4]} != "slots") return 2;
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *source_schema = registry.tsd(integer, registry.ref(ts));
    const auto *requested = registry.tsd(integer, ts);
    Value payloads[] = {Value{std::int32_t{0}}, Value{std::int32_t{1}}};
    const auto initial = MIN_ST;
    std::vector<std::unique_ptr<TSOutput>> targets;
    targets.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        targets.push_back(std::make_unique<TSOutput>(ts));
        auto mutation = targets.back()->view(initial).begin_mutation(initial);
        static_cast<void>(mutation.copy_value_from(payloads[0].view()));
    }
    TSOutput alternate{ts};
    {
        auto mutation = alternate.view(initial).begin_mutation(initial);
        static_cast<void>(mutation.copy_value_from(payloads[1].view()));
    }
    TSOutput source{source_schema};
    auto source_data = source.data_view();
    auto source_dict = source_data.as_dict();
    {
        auto mutation = source_dict.begin_mutation(initial);
        for (std::size_t i = 0; i < n; ++i) {
            Value key{static_cast<std::int32_t>(i)};
            Value reference{TimeSeriesReference{targets[i]->view(initial)}};
            auto child = mutation.at(key.view());
            static_cast<void>(child.begin_mutation(initial).copy_value_from(reference.view()));
        }
    }
    auto handle = source.view(initial).binding_for(*requested);
    auto proxy_data = handle.data_view();
    auto proxy = proxy_data.as_dict();
    std::vector<std::size_t> selected;
    std::vector<Value> originals;
    std::vector<TSDataView> source_children;
    selected.reserve(changed);
    originals.reserve(changed);
    source_children.reserve(changed);
    for (std::size_t j = 0; j < changed; ++j) {
        const auto index = (n / 2 + j * (n / changed)) % n;
        selected.push_back(index);
        originals.emplace_back(TimeSeriesReference{targets[index]->view(initial)});
        Value key{static_cast<std::int32_t>(index)};
        source_children.push_back(source_dict.at(key.view()));
    }
    Value replacement{TimeSeriesReference{alternate.view(initial)}};
    std::int64_t tick = 0;
    std::uint64_t checksum = 0;
    auto run = [&](std::size_t iterations) {
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < iterations; ++i) {
            ++tick;
            const auto time = initial + TimeDelta{tick};
            const auto bit = static_cast<std::size_t>(tick & 1);
            for (std::size_t j = 0; j < changed; ++j) {
                if (retarget) {
                    auto mutation = source_children[j].begin_mutation(time);
                    static_cast<void>(mutation.copy_value_from(bit ? replacement.view() : originals[j].view()));
                } else {
                    auto mutation = targets[selected[j]]->view(time).begin_mutation(time);
                    static_cast<void>(mutation.copy_value_from(payloads[bit].view()));
                }
            }
            std::size_t count = 0;
            std::uint64_t sum = 0;
            if (range) {
                for (auto child : proxy.modified_values(time)) {
                    ++count;
                    sum += static_cast<std::uint64_t>(child.value().checked_as<std::int32_t>());
                }
            } else {
                for (auto slot = proxy.next_modified_slot(); slot != TS_DATA_NO_CHILD_ID;
                     slot = proxy.next_modified_slot(slot)) {
                    ++count;
                    sum += static_cast<std::uint64_t>(proxy.at_slot(slot).value().checked_as<std::int32_t>());
                }
            }
            if (count != changed || sum != changed * bit) {
                throw std::runtime_error("unexpected delta: count=" + std::to_string(count) + " sum=" + std::to_string(sum));
            }
            checksum += count + sum;
        }
        return std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count() / iterations;
    };
    run(8);
    const double calibration = run(8);
    const auto iterations = std::clamp(static_cast<std::size_t>(120000.0 / calibration), std::size_t{4}, std::size_t{100000});
    std::cout << "{\"n\":" << n << ",\"changed\":" << changed
              << ",\"mode\":\"" << argv[3] << "\",\"consumer\":\"" << argv[4]
              << "\",\"iterations\":" << iterations << ",\"us\":[";
    for (int rep = 0; rep < 7; ++rep) {
        if (rep) std::cout << ',';
        std::cout << std::fixed << std::setprecision(6) << run(iterations);
    }
    std::cout << "],\"checksum\":" << checksum << "}\n";
}
