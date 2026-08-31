#include <hgraph/fabric/fabric.h>

#include <hgraph/persistence/value_store.h>

#include "../src/impl/metadata_binding.h"

#include <hgraph/types/utils/counted_mutex.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {
namespace hg = hgraph;
namespace hgf = hgraph::fabric;
namespace hgps = hgraph::persistence::store;

constexpr hg::DateTime BASE_TIME{hg::TimeDelta{1'800'000'000'000'000}};

[[nodiscard]] hgps::ValueStore metadata_store(const hgf::FabricConfig &config) {
  return hgps::make_value_store(
      {.objects = config.objects, .codec = config.metadata_codec});
}

[[nodiscard]] hg::Frame frame(std::int64_t value, std::string field = "value") {
  arrow::Int64Builder builder;
  REQUIRE(builder.Append(value).ok());
  auto array = builder.Finish();
  REQUIRE(array.ok());
  return hg::Frame{arrow::Table::Make(
      arrow::schema({arrow::field(std::move(field), arrow::int64())}),
      {std::move(array).ValueOrDie()})};
}

void seed(const hgf::FabricConfig &config, std::string data_id,
          hgf::RevisionId revision, hgf::DataVersion output_version,
          std::vector<hgf::DataDependencyInput> dependencies = {},
          std::optional<hgf::DataVersion> self_predecessor = {},
          std::string field = "value", bool write_frame = true) {
  const std::string frame_key =
      hgf::data_version_key(config.prefix, data_id, output_version);
  if (write_frame && !config.frames.contains(frame_key)) {
    config.frames.write(frame_key, frame(output_version, std::move(field)));
  }
  const auto value = hgf::make_data_revision(hgf::DataRevisionInput{
      .data_id = std::move(data_id),
      .revision = revision,
      .output_version = output_version,
      .dependencies = std::move(dependencies),
      .self_predecessor = self_predecessor,
      .as_of = BASE_TIME + hg::TimeDelta{revision},
  });
  const auto decoded = hgf::data_revision_input(value.view());
  REQUIRE(config.objects
              .put_immutable(hgf::revision_key(config.prefix, decoded.data_id,
                                               decoded.revision),
                             metadata_store(config).encode(value.view()))
              .status == hgps::ImmutableWriteStatus::Created);
}

[[nodiscard]] const hgf::ResolvedRevision &
selection(const hgf::ForestResolution &result, std::string_view data_id) {
  REQUIRE(result.cut.has_value());
  const auto found = std::ranges::find(result.cut->revisions, data_id,
                                       &hgf::ResolvedRevision::data_id);
  REQUIRE(found != result.cut->revisions.end());
  return *found;
}

[[nodiscard]] const hgf::ForestResolution &
forest(const hgf::CoordinationResult &result, std::string_view root) {
  const auto found = std::ranges::find_if(
      result.forests, [root](const hgf::ForestResolution &candidate) {
        return std::ranges::find(candidate.roots, root) !=
               candidate.roots.end();
      });
  REQUIRE(found != result.forests.end());
  return *found;
}

void seed_bootstrap(const hgf::FabricConfig &config) {
  for (hgf::RevisionId revision = 1; revision <= 3; ++revision) {
    seed(config, "D1", revision, revision);
    seed(config, "D2", revision, revision, {{"D1", revision}});
  }
  seed(config, "D3", 1, 1, {{"D2", 1}});
  seed(config, "D3", 2, 2, {{"D2", 2}});
}
} // namespace

TEST_CASE("resolver reproduces the RFC D1 D2 D3 bootstrap cut") {
  auto config = hgf::make_memory_fabric_config("tests/resolution/bootstrap");
  seed_bootstrap(config);

  hgf::ConsistencyResolver resolver{config};
  const auto result = resolver.resolve_forest({"D1", "D3"});
  REQUIRE(result.status == hgf::ResolutionStatus::Ready);
  CHECK(selection(result, "D1").revision == 2);
  CHECK(selection(result, "D2").revision == 2);
  CHECK(selection(result, "D3").revision == 2);
  REQUIRE(result.changed_roots.size() == 2);
  CHECK(result.changed_roots[0].output_version == 2);
  CHECK(result.changed_roots[1].output_version == 2);
  CHECK(result.metrics.output_index_hits > 0);

  const auto latest = config.objects.get(hgf::latest_key(config.prefix, "D1"));
  REQUIRE(latest.has_value());
  CHECK(hgf::revision_reference_value(
            metadata_store(config).bind(hgf::revision_reference_meta()),
            hgf::MetadataObjectKind::Latest, latest->data) == 3);
  const auto as_of = config.objects.get(
      hgf::as_of_key(config.prefix, "D1", BASE_TIME + hg::TimeDelta{3}));
  REQUIRE(as_of.has_value());
  CHECK(hgf::revision_reference_value(
            metadata_store(config).bind(hgf::revision_reference_meta()),
            hgf::MetadataObjectKind::AsOf, as_of->data) == 3);
}

TEST_CASE(
    "an unchanged child acknowledgement releases a held parent atomically") {
  auto config = hgf::make_memory_fabric_config("tests/resolution/held");
  seed_bootstrap(config);
  hgf::ConsistencyCoordinator coordinator{config, {"D1", "D3"}};

  const auto initial = coordinator.resolve();
  REQUIRE(initial.forests.size() == 1);
  REQUIRE(initial.changed_roots.size() == 2);
  CHECK(selection(initial.forests.front(), "D1").output_version == 2);
  CHECK(selection(initial.forests.front(), "D3").output_version == 2);

  seed(config, "D3", 3, 2, {{"D2", 3}});
  const auto released = coordinator.resolve();
  REQUIRE(released.forests.size() == 1);
  REQUIRE(released.forests.front().status == hgf::ResolutionStatus::Ready);
  REQUIRE(released.changed_roots.size() == 1);
  CHECK(released.changed_roots.front().data_id == "D1");
  CHECK(released.changed_roots.front().output_version == 3);
  CHECK(selection(released.forests.front(), "D3").revision == 3);
  const auto hidden = std::ranges::find(released.committed_lineage, "D3",
                                        &hgf::ResolvedRevision::data_id);
  REQUIRE(hidden != released.committed_lineage.end());
  CHECK(hidden->revision == 3);

  const auto unchanged = coordinator.resolve();
  CHECK(unchanged.forests.front().status == hgf::ResolutionStatus::Unchanged);
  CHECK(unchanged.changed_roots.empty());
}

TEST_CASE("resolver reports all six normative outcomes") {
  SECTION("Pending") {
    auto config = hgf::make_memory_fabric_config("tests/resolution/pending");
    seed(config, "Z", 1, 1);
    seed(config, "Z", 2, 2);
    seed(config, "A", 1, 1, {{"Z", 1}});
    seed(config, "B", 1, 1, {{"Z", 2}});
    hgf::ConsistencyResolver resolver{config};
    CHECK(resolver.resolve_forest({"A", "B"}).status ==
          hgf::ResolutionStatus::Pending);
  }

  SECTION("Ambiguous") {
    auto config = hgf::make_memory_fabric_config("tests/resolution/ambiguous");
    seed(config, "X", 1, 1);
    seed(config, "X", 2, 2);
    seed(config, "A", 1, 1, {{"X", 1}});
    seed(config, "A", 2, 2, {{"X", 2}});
    seed(config, "B", 1, 1, {{"X", 2}});
    seed(config, "B", 2, 2, {{"X", 1}});
    hgf::ConsistencyResolver resolver{config};
    CHECK(resolver.resolve_forest({"A", "B"}).status ==
          hgf::ResolutionStatus::Ambiguous);
  }

  SECTION("Cyclic and self predecessor exclusion") {
    auto cyclic_config =
        hgf::make_memory_fabric_config("tests/resolution/cyclic");
    seed(cyclic_config, "A", 1, 1, {{"B", 1}});
    seed(cyclic_config, "B", 1, 1, {{"A", 1}});
    hgf::ConsistencyResolver cyclic{cyclic_config};
    CHECK(cyclic.resolve_forest({"A"}).status == hgf::ResolutionStatus::Cyclic);

    auto audit_config = hgf::make_memory_fabric_config("tests/resolution/self");
    seed(audit_config, "A", 1, 1);
    seed(audit_config, "A", 2, 2, {}, 1);
    hgf::ConsistencyResolver audit{audit_config};
    const auto result = audit.resolve_forest({"A"});
    CHECK(result.status == hgf::ResolutionStatus::Ready);
    CHECK(selection(result, "A").revision == 2);
  }

  SECTION("Corrupt missing accepted ancestry") {
    auto config = hgf::make_memory_fabric_config("tests/resolution/corrupt");
    seed(config, "A", 1, 1, {{"missing", 7}});
    hgf::ConsistencyResolver resolver{config};
    const auto result = resolver.resolve_forest({"A"});
    CHECK(result.status == hgf::ResolutionStatus::Corrupt);
    CHECK(result.diagnostic.find("missing data version") != std::string::npos);
  }

  SECTION("Ready then Unchanged") {
    auto config = hgf::make_memory_fabric_config("tests/resolution/unchanged");
    seed(config, "A", 1, 1);
    hgf::ConsistencyResolver resolver{config};
    CHECK(resolver.resolve_forest({"A"}).status ==
          hgf::ResolutionStatus::Ready);
    const std::vector<hgf::ExposedRootVersion> exposed{{"A", 1}};
    CHECK(resolver.resolve_forest({"A"}, exposed).status ==
          hgf::ResolutionStatus::Unchanged);
  }
}

TEST_CASE("accepted immutable and Frame corruption never becomes pending") {
  SECTION("missing Frame") {
    auto config =
        hgf::make_memory_fabric_config("tests/resolution/missing-frame");
    seed(config, "A", 1, 1, {}, {}, "value", false);
    hgf::ConsistencyResolver resolver{config};
    CHECK(resolver.resolve_forest({"A"}).status ==
          hgf::ResolutionStatus::Corrupt);
  }

  SECTION("fixed schema mismatch") {
    auto config = hgf::make_memory_fabric_config("tests/resolution/schema");
    seed(config, "A", 1, 1);
    seed(config, "A", 2, 2, {}, {}, "different");
    hgf::ConsistencyResolver resolver{config};
    CHECK(resolver.resolve_forest({"A"}).status ==
          hgf::ResolutionStatus::Corrupt);
  }
}

TEST_CASE(
    "coordinator dynamically merges splits and isolates forest failures") {
  SECTION("independent failure") {
    auto config = hgf::make_memory_fabric_config("tests/resolution/isolation");
    seed(config, "good", 1, 1);
    seed(config, "bad-a", 1, 1, {{"bad-b", 1}});
    seed(config, "bad-b", 1, 1, {{"bad-a", 1}});
    hgf::ConsistencyCoordinator coordinator{config, {"good", "bad-a"}};
    const auto result = coordinator.resolve();
    REQUIRE(result.forests.size() == 2);
    CHECK(forest(result, "good").status == hgf::ResolutionStatus::Ready);
    CHECK(forest(result, "bad-a").status == hgf::ResolutionStatus::Cyclic);
    REQUIRE(result.changed_roots.size() == 1);
    CHECK(result.changed_roots.front().data_id == "good");
  }

  SECTION("merge then split") {
    auto config =
        hgf::make_memory_fabric_config("tests/resolution/repartition");
    seed(config, "X", 1, 1);
    seed(config, "A", 1, 1, {{"X", 1}});
    seed(config, "B", 1, 1, {{"X", 1}});
    hgf::ConsistencyCoordinator coordinator{config, {"A", "B"}};
    const auto merged = coordinator.resolve();
    REQUIRE(merged.forests.size() == 1);

    seed(config, "Y", 1, 1);
    seed(config, "Z", 1, 1);
    seed(config, "A", 2, 2, {{"Y", 1}});
    seed(config, "B", 2, 2, {{"Z", 1}});
    const auto split = coordinator.resolve();
    REQUIRE(split.forests.size() == 2);
    CHECK(split.changed_roots.size() == 2);
    CHECK(forest(split, "A").status == hgf::ResolutionStatus::Ready);
    CHECK(forest(split, "B").status == hgf::ResolutionStatus::Ready);
  }

  SECTION("same-output acknowledgements replace superseded ancestry") {
    auto config = hgf::make_memory_fabric_config(
        "tests/resolution/same-output-repartition");
    seed(config, "X", 1, 1);
    seed(config, "A", 1, 1, {{"X", 1}});
    seed(config, "B", 1, 1, {{"X", 1}});
    hgf::ConsistencyCoordinator coordinator{config, {"A", "B"}};
    REQUIRE(coordinator.resolve().forests.size() == 1);

    seed(config, "Y", 1, 1);
    seed(config, "Z", 1, 1);
    seed(config, "A", 2, 1, {{"Y", 1}});
    seed(config, "B", 2, 1, {{"Z", 1}});
    const auto split = coordinator.resolve();
    REQUIRE(split.forests.size() == 2);
    CHECK(split.changed_roots.empty());
    CHECK(forest(split, "A").status == hgf::ResolutionStatus::Unchanged);
    CHECK(forest(split, "B").status == hgf::ResolutionStatus::Unchanged);
  }
}

TEST_CASE("root versions never regress below an exposed cut") {
  auto config = hgf::make_memory_fabric_config("tests/resolution/lower-bound");
  seed(config, "A", 1, 1);
  seed(config, "A", 2, 2);
  hgf::ConsistencyResolver resolver{config};
  const std::vector<hgf::ExposedRootVersion> exposed{{"A", 2}};
  const auto result = resolver.resolve_forest({"A"}, exposed);
  REQUIRE(result.status == hgf::ResolutionStatus::Unchanged);
  CHECK(selection(result, "A").output_version == 2);
  CHECK(result.changed_roots.empty());
}

TEST_CASE("coordinator measures conflated notice to ready latency") {
  auto config = hgf::make_memory_fabric_config("tests/resolution/latency");
  seed(config, "A", 1, 1);
  hgf::ConsistencyCoordinator coordinator{config, {"A"}};
  REQUIRE(coordinator.resolve().forests.front().status ==
          hgf::ResolutionStatus::Ready);

  seed(config, "A", 2, 2);
  coordinator.observe_notice("A", BASE_TIME + hg::TimeDelta{10});
  coordinator.observe_notice("A", BASE_TIME + hg::TimeDelta{12});
  const auto result = coordinator.resolve(BASE_TIME + hg::TimeDelta{25});
  REQUIRE(result.forests.front().metrics.notice_to_ready.has_value());
  CHECK(*result.forests.front().metrics.notice_to_ready == hg::TimeDelta{15});
}

TEST_CASE(
    "same-output rolling ancestry uses immutable caches and version indexes") {
  constexpr hgf::RevisionId count{256};
  auto config = hgf::make_memory_fabric_config("tests/resolution/index");
  for (hgf::RevisionId revision = 1; revision <= count; ++revision) {
    seed(config, "P", revision, revision);
    seed(config, "A", revision, count + 1, {{"P", revision}});
  }
  seed(config, "D", 1, 1, {{"A", count + 1}});

  hgf::ConsistencyResolver resolver{config};
  const auto initial = resolver.resolve_forest({"D", "P"});
  REQUIRE(initial.status == hgf::ResolutionStatus::Ready);
  CHECK(selection(initial, "A").revision == count);
  const std::vector<hgf::ExposedRootVersion> exposed{{"D", 1}, {"P", count}};
  const auto warm = resolver.resolve_forest({"D", "P"}, exposed);
  REQUIRE(warm.status == hgf::ResolutionStatus::Unchanged);
  CHECK(warm.metrics.revision_cache_misses == 0);
  CHECK(warm.metrics.revision_cache_hits > 0);
  CHECK(warm.metrics.output_index_hits >= static_cast<std::uint64_t>(count));
  CHECK(warm.metrics.maximum_backtracking_depth >= 3);
  CHECK(warm.metrics.average_backtracking_depth() > 0.0);
}

TEST_CASE("dynamic coordinator state copies the node's run binding") {
  auto config =
      hgf::make_memory_fabric_config("tests/resolution/run-binding");
  hgf::detail::FabricMetadataBinding binding{config};

  const auto before = hg::type_system_lock_count();
  auto coordinator = hgf::detail::BoundConsistencyFactory::coordinator(
      config, {"A"}, binding);
  CHECK(hg::type_system_lock_count() == before);
  CHECK(coordinator.resolve().forests.front().status ==
        hgf::ResolutionStatus::Pending);
}
