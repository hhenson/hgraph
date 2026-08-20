#include <hgraph/fabric/fabric.h>

#include <arrow/builder.h>
#include <arrow/table.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <ranges>
#include <stdexcept>
#include <vector>

namespace {
namespace hg = hgraph;
namespace hgf = hgraph::fabric;

[[nodiscard]] hg::Frame frame(std::int64_t value) {
  arrow::Int64Builder builder;
  if (!builder.Append(value).ok()) {
    throw std::runtime_error("failed to append benchmark Frame value");
  }
  auto array = builder.Finish();
  if (!array.ok()) {
    throw std::runtime_error("failed to finish benchmark Frame value");
  }
  return hg::Frame{
      arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}),
                         {std::move(array).ValueOrDie()})};
}

void seed(const hgf::FabricConfig &config,
          const hgf::DataRevisionInput &input) {
  const auto frame_key =
      hgf::data_version_key(config.prefix, input.data_id, input.output_version);
  if (!config.frames.contains(frame_key)) {
    config.frames.write(frame_key, frame(input.output_version));
  }
  auto value = hgf::make_data_revision(input);
  const auto result = config.objects.put_immutable(
      hgf::revision_key(config.prefix, input.data_id, input.revision),
      hgf::encode_revision(value.view()));
  if (result.status !=
      hgraph::persistence::store::ImmutableWriteStatus::Created) {
    throw std::runtime_error("failed to seed resolver benchmark");
  }
}
} // namespace

int main() {
  try {
    constexpr hgf::RevisionId run_length{2'048};
    constexpr std::size_t samples{9};
    auto config = hgf::make_memory_fabric_config("bench/resolution");
    const hg::DateTime base{hg::TimeDelta{1'800'000'000'000'000}};
    for (hgf::RevisionId revision = 1; revision <= run_length; ++revision) {
      seed(config, hgf::DataRevisionInput{
                       .data_id = "parent",
                       .revision = revision,
                       .output_version = revision,
                       .as_of = base + hg::TimeDelta{revision},
                   });
      seed(config, hgf::DataRevisionInput{
                       .data_id = "rolling",
                       .revision = revision,
                       .output_version = run_length + 1,
                       .dependencies = {{"parent", revision}},
                       .as_of = base + hg::TimeDelta{revision},
                   });
    }
    seed(config, hgf::DataRevisionInput{
                     .data_id = "root",
                     .revision = 1,
                     .output_version = 1,
                     .dependencies = {{"rolling", run_length + 1}},
                     .as_of = base + hg::TimeDelta{1},
                 });

    hgf::ConsistencyResolver resolver{config};
    const auto cold = resolver.resolve_forest({"parent", "root"});
    if (cold.status != hgf::ResolutionStatus::Ready) {
      throw std::runtime_error("resolver benchmark cold cut did not resolve");
    }
    const std::vector<hgf::ExposedRootVersion> exposed{{"parent", run_length},
                                                       {"root", 1}};

    std::vector<double> elapsed;
    elapsed.reserve(samples);
    hgf::ResolverMetrics last{};
    for (std::size_t sample = 0; sample < samples; ++sample) {
      const auto started = std::chrono::steady_clock::now();
      const auto result = resolver.resolve_forest({"parent", "root"}, exposed);
      elapsed.push_back(std::chrono::duration<double, std::micro>(
                            std::chrono::steady_clock::now() - started)
                            .count());
      if (result.status != hgf::ResolutionStatus::Unchanged ||
          result.metrics.revision_cache_misses != 0) {
        throw std::runtime_error(
            "resolver benchmark warm cut missed immutable cache");
      }
      last = result.metrics;
    }
    std::ranges::sort(elapsed);
    std::cout << "benchmark name=fabric.same_output_rolling_ancestry"
              << " run_length=" << run_length << " samples=" << samples
              << " median_us=" << elapsed[elapsed.size() / 2]
              << " revisions_examined=" << last.revisions_examined
              << " output_index_hits=" << last.output_index_hits
              << " revision_cache_hits=" << last.revision_cache_hits
              << " max_depth=" << last.maximum_backtracking_depth
              << " average_depth=" << last.average_backtracking_depth() << '\n';
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "fabric resolver benchmark failed: " << error.what() << '\n';
    return 1;
  }
}
