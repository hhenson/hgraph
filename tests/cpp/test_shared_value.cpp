#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/shared_value_pool.h>
#include <hgraph/types/value/value.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace {
struct SharedFixture {
  const hgraph::ValueTypeMetaData *plain_schema{nullptr};
  const hgraph::ValueTypeMetaData *shared_schema{nullptr};
  hgraph::ValueTypeRef plain_type{};
  hgraph::ValueTypeRef shared_type{};

  explicit SharedFixture(std::string name) {
    auto &registry = hgraph::TypeRegistry::instance();
    plain_schema = registry.bundle(
        std::move(name),
        {{"id", hgraph::scalar_descriptor<hgraph::Int>::value_meta()},
         {"text", hgraph::scalar_descriptor<hgraph::Str>::value_meta()}});
    shared_schema = registry.shared(plain_schema);
    plain_type = hgraph::ValuePlanFactory::instance().type_for(plain_schema);
    shared_type = hgraph::ValuePlanFactory::instance().type_for(shared_schema);
  }

  [[nodiscard]] hgraph::Value plain(hgraph::Int id, std::string text) const {
    hgraph::Value result{plain_type};
    auto fields = result.as_bundle().begin_mutation();
    fields["id"].set(id);
    fields["text"].set(std::move(text));
    return result;
  }

  [[nodiscard]] hgraph::Value shared(const hgraph::Value &source) const {
    return hgraph::Value{shared_type, source.view()};
  }
};
} // namespace

TEST_CASE("Shared values retain one immutable stable allocation",
          "[shared-value]") {
  using namespace hgraph;

  SharedFixture fixture{"tests::SharedStable"};
  Value source = fixture.plain(7, "first");

  CHECK(fixture.shared_schema->is_shared());
  CHECK(fixture.shared_schema->is_indirect());
  CHECK_FALSE(fixture.shared_schema->is_un_named_bundle());
  CHECK(fixture.shared_type.checked_plan().layout.size == sizeof(void *));
  CHECK_FALSE(fixture.shared_type.ops_ref().can_begin_mutation());

  Value first = fixture.shared(source);
  const void *stable = first.view().concrete().data();
  REQUIRE(stable != nullptr);
  CHECK(first.as_bundle()["id"].checked_as<Int>() == 7);
  CHECK(first.as_bundle()["text"].checked_as<Str>() == "first");
  CHECK_FALSE(first.view().concrete().writable_payload());
  CHECK_THROWS_AS(first.begin_mutation(), std::logic_error);

  {
    Value second = first;
    Value third = second.clone();
    CHECK(second.view().concrete().data() == stable);
    CHECK(third.view().concrete().data() == stable);
    CHECK(shared_value_pool_metrics().live_values == 1);
  }

  // The source is independent: materialisation completed before the shared
  // handle was published.
  source.as_bundle().begin_mutation()["id"].set(Int{99});
  CHECK(first.as_bundle()["id"].checked_as<Int>() == 7);
  CHECK(shared_value_pool_metrics().live_values == 1);

  first.reset();
  CHECK(shared_value_pool_metrics().live_values == 0);

  Value replacement_source = fixture.plain(8, "second");
  Value replacement = fixture.shared(replacement_source);
  CHECK(replacement.view().concrete().data() == stable);
}

TEST_CASE("Shared size classes are global across compatible Bundle schemas",
          "[shared-value]") {
  using namespace hgraph;

  SharedFixture first_fixture{"tests::SharedClassA"};
  SharedFixture second_fixture{"tests::SharedClassB"};
  const void *first_address = nullptr;
  {
    Value source = first_fixture.plain(1, "a");
    Value shared = first_fixture.shared(source);
    first_address = shared.view().concrete().data();
    REQUIRE(first_address != nullptr);
  }
  REQUIRE(shared_value_pool_metrics().live_values == 0);

  Value source = second_fixture.plain(2, "b");
  Value shared = second_fixture.shared(source);
  CHECK(shared.view().concrete().data() == first_address);
  CHECK(shared_value_pool_metrics().size_classes == 1);
}

TEST_CASE("Shared retain release and allocation are safe under contention",
          "[shared-value][concurrency]") {
  using namespace hgraph;

  SharedFixture fixture{"tests::SharedConcurrent"};
  Value source = fixture.plain(42, std::string(256, 'x'));
  Value root = fixture.shared(source);
  const void *root_address = root.view().concrete().data();

  constexpr std::size_t thread_count = 8;
  constexpr std::size_t iterations = 4'000;
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  threads.reserve(thread_count);
  for (std::size_t thread = 0; thread < thread_count; ++thread) {
    threads.emplace_back([&] {
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        Value retained = root;
        if (retained.view().concrete().data() != root_address ||
            retained.as_bundle()["id"].checked_as<Int>() != 42) {
          failed.store(true, std::memory_order_relaxed);
        }

        // Exercise concurrent acquire/release of separate values as
        // well as the strong-reference path. This is the ABA-heavy
        // allocator case.
        Value materialised{fixture.shared_type, source.view()};
        if (materialised.as_bundle()["text"].checked_as<Str>().size() != 256) {
          failed.store(true, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }

  CHECK_FALSE(failed.load(std::memory_order_relaxed));
  CHECK(shared_value_pool_metrics().live_values == 1);
}

TEST_CASE("Shared values preserve transparent JSON representation",
          "[shared-value][json]") {
  using namespace hgraph;

  SharedFixture fixture{"tests::SharedJson"};
  Value source = fixture.plain(12, "payload");
  Value shared = fixture.shared(source);

  const std::string encoded = to_json_string(shared.view());
  CHECK(encoded == R"({"id": 12, "text": "payload"})");
  Value decoded = from_json_string(fixture.shared_schema, encoded);
  CHECK(decoded.binding() == fixture.shared_type);
  CHECK(decoded.equals(shared));
  CHECK_FALSE(decoded.view().can_begin_mutation());
}

TEST_CASE("Shared schema accepts direct Bundle targets only",
          "[shared-value]") {
  using namespace hgraph;

  auto &registry = TypeRegistry::instance();
  CHECK_THROWS_AS(registry.shared(scalar_descriptor<Int>::value_meta()),
                  std::invalid_argument);

  const auto *bundle = registry.bundle(
      "tests::SharedDirect", {{"id", scalar_descriptor<Int>::value_meta()}});
  const auto *shared = registry.shared(bundle);
  CHECK_THROWS_AS(registry.shared(shared), std::invalid_argument);
}
