#include <hgraph/fabric/fabric.h>

#include <hgraph/persistence/value_store.h>

#include <arrow/builder.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
namespace hg = hgraph;
namespace hgf = hgraph::fabric;
namespace hgps = hgraph::persistence::store;

constexpr std::size_t samples{9};
constexpr hg::DateTime base_time{hg::TimeDelta{1'800'000'000'000'000}};

template <typename T>
[[nodiscard]] T unwrap(arrow::Result<T> result, std::string_view operation) {
  if (!result.ok()) {
    throw std::runtime_error(std::string{operation} + ": " +
                             result.status().ToString());
  }
  return std::move(result).ValueOrDie();
}

void check(arrow::Status status, std::string_view operation) {
  if (!status.ok()) {
    throw std::runtime_error(std::string{operation} + ": " + status.ToString());
  }
}

[[nodiscard]] double median(std::vector<double> values) {
  std::ranges::sort(values);
  return values[values.size() / 2];
}

[[nodiscard]] hg::Frame scalar_frame(std::int64_t value) {
  arrow::Int64Builder builder;
  check(builder.Append(value), "append benchmark Frame value");
  auto array = unwrap(builder.Finish(), "finish benchmark Frame value");
  return hg::Frame{
      arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}),
                         {std::move(array)})};
}

[[nodiscard]] hg::Frame throughput_frame(std::size_t rows) {
  arrow::Int64Builder builder;
  check(builder.Reserve(static_cast<std::int64_t>(rows)),
        "reserve throughput Frame values");
  for (std::size_t row = 0; row < rows; ++row) {
    builder.UnsafeAppend(static_cast<std::int64_t>(row));
  }
  auto values = unwrap(builder.Finish(), "finish throughput Frame values");
  return hg::Frame{
      arrow::Table::Make(arrow::schema({arrow::field("left", arrow::int64()),
                                        arrow::field("right", arrow::int64())}),
                         {values, values})};
}

[[nodiscard]] std::shared_ptr<arrow::Buffer>
serialize_ipc(const hg::Frame &value) {
  auto output =
      unwrap(arrow::io::BufferOutputStream::Create(), "create IPC buffer");
  auto writer = unwrap(
      arrow::ipc::MakeFileWriter(output, value.table->schema(),
                                 arrow::ipc::IpcWriteOptions::Defaults()),
      "create IPC writer");
  check(writer->WriteTable(*value.table), "write IPC table");
  check(writer->Close(), "close IPC writer");
  return unwrap(output->Finish(), "finish IPC buffer");
}

void seed(const hgf::FabricConfig &config,
          const hgf::DataRevisionInput &input) {
  const auto frame_key =
      hgf::data_version_key(config.prefix, input.data_id, input.output_version);
  if (!config.frames.contains(frame_key)) {
    config.frames.write(frame_key, scalar_frame(input.output_version));
  }
  auto value = hgf::make_data_revision(input);
  const auto metadata = hgps::make_value_store(
      {.objects = config.objects, .codec = config.metadata_codec});
  const auto result = config.objects.put_immutable(
      hgf::revision_key(config.prefix, input.data_id, input.revision),
      metadata.encode(value.view()));
  if (result.status != hgps::ImmutableWriteStatus::Created) {
    throw std::runtime_error("failed to seed resolver benchmark");
  }
}

struct ResolutionScenario {
  std::string name{};
  std::size_t scale{};
  hgf::FabricConfig config{};
  std::vector<hg::Str> roots{};
  std::vector<hgf::ExposedRootVersion> exposed{};
  hgf::ResolutionStatus cold_status{hgf::ResolutionStatus::Ready};
  hgf::ResolutionStatus warm_status{hgf::ResolutionStatus::Unchanged};
};

[[nodiscard]] ResolutionScenario
same_output_scenario(hgf::RevisionId run_length) {
  auto config = hgf::make_memory_fabric_config("bench/resolution/same-output");
  for (hgf::RevisionId revision = 1; revision <= run_length; ++revision) {
    seed(config, hgf::DataRevisionInput{
                     .data_id = "parent",
                     .revision = revision,
                     .output_version = revision,
                     .as_of = base_time + hg::TimeDelta{revision},
                 });
    seed(config, hgf::DataRevisionInput{
                     .data_id = "rolling",
                     .revision = revision,
                     .output_version = run_length + 1,
                     .dependencies = {{"parent", revision}},
                     .as_of = base_time + hg::TimeDelta{revision},
                 });
  }
  seed(config, hgf::DataRevisionInput{
                   .data_id = "root",
                   .revision = 1,
                   .output_version = 1,
                   .dependencies = {{"rolling", run_length + 1}},
                   .as_of = base_time + hg::TimeDelta{1},
               });
  return ResolutionScenario{
      .name = "fabric.same_output_rolling_ancestry",
      .scale = static_cast<std::size_t>(run_length),
      .config = std::move(config),
      .roots = {"parent", "root"},
      .exposed = {{"parent", run_length}, {"root", 1}},
  };
}

[[nodiscard]] ResolutionScenario broad_scenario(std::size_t width) {
  auto config = hgf::make_memory_fabric_config("bench/resolution/broad");
  std::vector<hgf::DataDependencyInput> dependencies;
  dependencies.reserve(width);
  for (std::size_t index = 0; index < width; ++index) {
    auto data_id = std::string{"leaf-"} + std::to_string(index);
    seed(config, hgf::DataRevisionInput{
                     .data_id = data_id,
                     .revision = 1,
                     .output_version = 1,
                     .as_of = base_time + hg::TimeDelta{1},
                 });
    dependencies.push_back({std::move(data_id), 1});
  }
  seed(config, hgf::DataRevisionInput{
                   .data_id = "broad-root",
                   .revision = 1,
                   .output_version = 1,
                   .dependencies = std::move(dependencies),
                   .as_of = base_time + hg::TimeDelta{2},
               });
  return ResolutionScenario{
      .name = "fabric.broad_ancestry",
      .scale = width,
      .config = std::move(config),
      .roots = {"broad-root"},
      .exposed = {{"broad-root", 1}},
  };
}

[[nodiscard]] ResolutionScenario deep_scenario(std::size_t depth) {
  auto config = hgf::make_memory_fabric_config("bench/resolution/deep");
  std::string previous;
  for (std::size_t index = 0; index < depth; ++index) {
    auto data_id = std::string{"depth-"} + std::to_string(index);
    std::vector<hgf::DataDependencyInput> dependencies;
    if (!previous.empty()) {
      dependencies.push_back({previous, 1});
    }
    seed(config,
         hgf::DataRevisionInput{
             .data_id = data_id,
             .revision = 1,
             .output_version = 1,
             .dependencies = std::move(dependencies),
             .as_of = base_time +
                      hg::TimeDelta{static_cast<std::int64_t>(index + 1)},
         });
    previous = std::move(data_id);
  }
  return ResolutionScenario{
      .name = "fabric.deep_ancestry",
      .scale = depth,
      .config = std::move(config),
      .roots = {previous},
      .exposed = {{previous, 1}},
  };
}

[[nodiscard]] ResolutionScenario conflict_scenario(hgf::RevisionId width) {
  auto config = hgf::make_memory_fabric_config("bench/resolution/conflict");
  for (hgf::RevisionId revision = 1; revision <= width; ++revision) {
    seed(config, hgf::DataRevisionInput{
                     .data_id = "pivot",
                     .revision = revision,
                     .output_version = revision,
                     .as_of = base_time + hg::TimeDelta{revision},
                 });
    seed(config, hgf::DataRevisionInput{
                     .data_id = "left",
                     .revision = revision,
                     .output_version = revision,
                     .dependencies = {{"pivot", revision}},
                     .as_of = base_time + hg::TimeDelta{revision},
                 });
    seed(config, hgf::DataRevisionInput{
                     .data_id = "right",
                     .revision = revision,
                     .output_version = revision,
                     .dependencies = {{"pivot", width + 1 - revision}},
                     .as_of = base_time + hg::TimeDelta{revision},
                 });
  }
  return ResolutionScenario{
      .name = "fabric.conflict_heavy_ancestry",
      .scale = static_cast<std::size_t>(width),
      .config = std::move(config),
      .roots = {"left", "right"},
      .cold_status = hgf::ResolutionStatus::Ambiguous,
      .warm_status = hgf::ResolutionStatus::Ambiguous,
  };
}

void run_resolution_benchmark(ResolutionScenario scenario) {
  hgf::ConsistencyResolver resolver{std::move(scenario.config)};
  const auto cold = resolver.resolve_forest(scenario.roots);
  if (cold.status != scenario.cold_status) {
    throw std::runtime_error(scenario.name + " cold result was " +
                             std::string{hgf::enum_name(cold.status)});
  }

  std::vector<double> elapsed;
  elapsed.reserve(samples);
  hgf::ResolverMetrics metrics{};
  for (std::size_t sample = 0; sample < samples; ++sample) {
    const auto started = std::chrono::steady_clock::now();
    const auto result =
        resolver.resolve_forest(scenario.roots, scenario.exposed);
    elapsed.push_back(std::chrono::duration<double, std::micro>(
                          std::chrono::steady_clock::now() - started)
                          .count());
    if (result.status != scenario.warm_status ||
        result.metrics.revision_cache_misses != 0) {
      throw std::runtime_error(
          scenario.name + " warm cut missed immutable cache or changed status");
    }
    metrics = result.metrics;
  }

  std::cout << "resolver"
            << " name=" << scenario.name << " samples=" << samples
            << " scale=" << scenario.scale
            << " median_us=" << median(std::move(elapsed))
            << " status=" << hgf::enum_name(scenario.warm_status)
            << " revisions_examined=" << metrics.revisions_examined
            << " edges_examined=" << metrics.edges_examined
            << " candidate_selections=" << metrics.candidate_selections
            << " output_index_hits=" << metrics.output_index_hits
            << " revision_cache_hits=" << metrics.revision_cache_hits
            << " max_depth=" << metrics.maximum_backtracking_depth
            << " average_depth=" << metrics.average_backtracking_depth()
            << '\n';
}

void run_serialization_benchmark(const hg::Frame &value,
                                 std::size_t iterations) {
  const auto expected_size = serialize_ipc(value)->size();
  std::vector<double> throughput;
  throughput.reserve(samples);
  std::uint64_t checksum{};
  for (std::size_t sample = 0; sample < samples; ++sample) {
    const auto started = std::chrono::steady_clock::now();
    std::uint64_t sample_checksum{};
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
      sample_checksum +=
          static_cast<std::uint64_t>(serialize_ipc(value)->size());
    }
    const auto seconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    throughput.push_back(static_cast<double>(sample_checksum) / seconds /
                         (1024.0 * 1024.0));
    checksum = sample_checksum;
  }
  std::cout << "throughput"
            << " name=fabric.frame_arrow_ipc_serialize"
            << " samples=" << samples << " iterations=" << iterations
            << " payload_bytes=" << expected_size
            << " median_mib_per_s=" << median(std::move(throughput))
            << " checksum=" << checksum << '\n';
}

void run_object_store_benchmark(std::span<const std::byte> bytes,
                                std::size_t iterations) {
  std::vector<double> throughput;
  throughput.reserve(samples);
  std::uint64_t checksum{};
  for (std::size_t sample = 0; sample < samples; ++sample) {
    auto store = hgps::make_object_store(hgps::ObjectStoreConfig{});
    const auto started = std::chrono::steady_clock::now();
    std::uint64_t sample_checksum{};
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
      const auto key =
          std::string{"throughput/object-"} + std::to_string(iteration);
      const auto result = store.put_immutable(key, bytes);
      if (result.status != hgps::ImmutableWriteStatus::Created) {
        throw std::runtime_error("object-store throughput write conflicted");
      }
      const auto stored = store.get(key);
      if (!stored || stored->data.size() != bytes.size()) {
        throw std::runtime_error("object-store throughput read was incomplete");
      }
      sample_checksum += static_cast<std::uint64_t>(stored->data.size()) * 2;
    }
    const auto seconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    throughput.push_back(static_cast<double>(sample_checksum) / seconds /
                         (1024.0 * 1024.0));
    checksum = sample_checksum;
  }
  std::cout << "throughput"
            << " name=fabric.memory_object_store_put_get"
            << " samples=" << samples << " iterations=" << iterations
            << " payload_bytes=" << bytes.size()
            << " median_mib_per_s=" << median(std::move(throughput))
            << " checksum=" << checksum << '\n';
}
} // namespace

int main() {
  try {
    std::cout << std::fixed << std::setprecision(3)
              << "fabric_perf format=1 samples=" << samples << '\n';
    run_resolution_benchmark(same_output_scenario(2'048));
    run_resolution_benchmark(broad_scenario(128));
    run_resolution_benchmark(deep_scenario(256));
    run_resolution_benchmark(conflict_scenario(24));

    const auto value = throughput_frame(65'536);
    run_serialization_benchmark(value, 20);
    const auto encoded = serialize_ipc(value);
    run_object_store_benchmark(
        std::span{reinterpret_cast<const std::byte *>(encoded->data()),
                  static_cast<std::size_t>(encoded->size())},
        20);
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "fabric performance benchmark failed: " << error.what()
              << '\n';
    return 1;
  }
}
