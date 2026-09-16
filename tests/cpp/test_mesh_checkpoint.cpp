#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/types/utils/key_slot_store.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

namespace {
using namespace hgraph;
using namespace hgraph::testing;

using MeshState = TSB<"CheckpointMeshState", Field<"total", TS<Int>>,
                      Field<"key_time", TS<DateTime>>>;

struct MeshAccumulator {
  static constexpr auto name = "checkpoint_mesh_accumulator";
  static void eval(In<"key", TS<Int>> key, In<"value", TS<Int>> input,
                   RecordableState<MeshState> state, Out<TS<Int>> output) {
    auto clock = state.field<"key_time">();
    if (clock.valid() && clock.value().checked_as<DateTime>() != key.base().last_modified_time()) {
      throw std::runtime_error("mesh key clock changed during recovery");
    }
    if (!clock.valid()) { clock.set(key.base().last_modified_time()); }
    auto total = state.field<"total">();
    const Int next = (total.valid() ? total.value().checked_as<Int>() : 0) + input.value();
    total.set(next);
    output.set(next);
  }
};

struct AccumulatingMesh {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, NamedPort<"values", TSD<Int, TS<Int>>> values) {
    return wire<stdlib::mesh_>(w, fn<MeshAccumulator>(), values).as<TSD<Int, TS<Int>>>();
  }
};
struct AccumulatingComponent {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values) {
    return stdlib::component<AccumulatingMesh>(w, "mesh_strategy", values);
  }
};

// The optional peer remains invalid for base instances. This ordinary compute
// node makes the mesh terminal owned while mesh_ref owns the dynamic alias.
struct AddOptionalPeer {
  static constexpr auto name = "checkpoint_mesh_add_peer";
  static void eval(In<"value", TS<Int>> input, In<"peer", TS<Int>, InputValidity::Unchecked> peer, Out<TS<Int>> output) {
    output.set(input.value() + (peer.valid() ? peer.value() : 0));
  }
};
struct RecursiveAccumulator {
  static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key,
                               Port<TS<Int>> value, Port<TS<Int>> link) {
    auto total = wire<MeshAccumulator>(w, key, value);
    auto peer = stdlib::mesh_ref<TS<Int>>(w, link);
    return wire<AddOptionalPeer>(w, total, peer);
  }
};
struct RecursiveMesh {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w,
      NamedPort<"values", TSD<Int, TS<Int>>> values,
      NamedPort<"links", TSD<Int, TS<Int>>> links) {
    return wire<stdlib::mesh_>(w, fn<RecursiveAccumulator>(), values, links)
        .as<TSD<Int, TS<Int>>>();
  }
};
struct RecursiveComponent {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values,
                                        Port<TSD<Int, TS<Int>>> links) {
    return stdlib::component<RecursiveMesh>(w, "mesh_strategy", values, links);
  }
};

struct KeyChain {
  static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key, Port<TS<Int>> link) {
    return wire<AddOptionalPeer>(w, key, stdlib::mesh_ref<TS<Int>>(w, link));
  }
};
struct DemandMesh {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, NamedPort<"links", TSD<Int, TS<Int>>> links) {
    return wire<stdlib::mesh_>(w, fn<KeyChain>(), links).as<TSD<Int, TS<Int>>>();
  }
};
struct DemandComponent {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> links) {
    return stdlib::component<DemandMesh>(w, "mesh_strategy", links);
  }
};

struct KeyCount {
  static void eval(In<"keys", TSS<Int>> keys, Out<TS<Int>> output) {
    output.set(static_cast<Int>(keys.size()));
  }
};
struct KeySetChild {
  static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>>) {
    return wire<KeyCount>(w, stdlib::mesh_keys_ref<Int>(w));
  }
};
struct KeySetMesh {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, NamedPort<"keys", TSS<Int>> keys) {
    return wire<stdlib::mesh_>(w, fn<KeySetChild>(), arg<"__keys__">(keys)).as<TSD<Int, TS<Int>>>();
  }
};
struct KeySetComponent {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys) {
    return stdlib::component<KeySetMesh>(w, "mesh_strategy", keys);
  }
};

Int stopped_children = 0;
struct FailingStopAccumulator {
  static void eval(In<"value", TS<Int>> input, RecordableState<MeshState> state, Out<TS<Int>> out) {
    auto total = state.field<"total">();
    const Int next = (total.valid() ? total.value().checked_as<Int>() : 0) + input.value();
    total.set(next);
    out.set(next);
  }
  static void stop(RecordableState<MeshState> state) {
    ++stopped_children;
    if (state.field<"total">().value().checked_as<Int>() < 0) {
      throw std::runtime_error("mesh child stop failed");
    }
  }
};
struct FailingStopMesh {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, NamedPort<"values", TSD<Int, TS<Int>>> values) {
    return wire<stdlib::mesh_>(w, fn<FailingStopAccumulator>(), values).as<TSD<Int, TS<Int>>>();
  }
};
struct FailingStopComponent {
  static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values) {
    return stdlib::component<FailingStopMesh>(w, "mesh_strategy", values);
  }
};

struct NeverTicks {
  static Port<TS<Int>> compose(Wiring &w) {
    return wire<stdlib::nothing, TS<Int>>(w);
  }
};
struct NeverTicksComponent {
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>) {
    return stdlib::component<NeverTicks>(w, "mesh_strategy");
  }
};

void configure(GlobalContext &context, std::optional<ComponentCheckpoint> &checkpoint) {
  configure_component_recovery(context.state().view(), ComponentRecoveryConfig{
      .component_id = "mesh_strategy",
      .load = [&checkpoint] { return checkpoint; },
      .commit = [&checkpoint](const ComponentCheckpoint &image) { checkpoint = image; },
  });
}
} // namespace

TEST_CASE("mesh checkpoint restores state and key clocks across membership churn", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + 2 * MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<AccumulatingComponent>(
      {.start_time = MIN_ST, .end_time = second},
      values<Value>(dict_delta<Int, TS<Int>>({{1, 2}, {2, 10}}), dict_delta<Int, TS<Int>>({{1, 3}}))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 2}, {2, 10}}), dict_delta<Int, TS<Int>>({{1, 5}})));
  REQUIRE(image.has_value());
  CHECK_OUTPUT(eval_node_with_options<AccumulatingComponent>(
      {.start_time = second, .end_time = second + 3 * MIN_TD},
      values<Value>(none, dict_delta<Int, TS<Int>>({{2, 1}}, {1}), dict_delta<Int, TS<Int>>({{1, 7}}))),
      values<Value>(none, dict_delta<Int, TS<Int>>({{2, 11}}, {1}), dict_delta<Int, TS<Int>>({{1, 7}})));
  const auto third = second + 3 * MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<AccumulatingComponent>(
      {.start_time = third, .end_time = third + MIN_TD},
      values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 1}}))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 8}, {2, 12}})));
}

TEST_CASE("mesh checkpoint restores recursive dependencies and subscriptions without replay", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<RecursiveComponent>(
      {.start_time = MIN_ST, .end_time = second},
      values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 2}, {3, 3}})),
      values<Value>(dict_delta<Int, TS<Int>>({{2, 1}, {3, 2}}))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 12}, {3, 15}})));
  CHECK_OUTPUT(eval_node_with_options<RecursiveComponent>(
      {.start_time = second, .end_time = second + 3 * MIN_TD},
      values<Value>(none, dict_delta<Int, TS<Int>>({{1, 5}}), none),
      values<Value>(none, none, dict_delta<Int, TS<Int>>({{3, 1}}))),
      values<Value>(none, dict_delta<Int, TS<Int>>({{1, 15}, {2, 17}, {3, 20}}),
                    dict_delta<Int, TS<Int>>({{3, 18}})));
}

TEST_CASE("mesh checkpoint retains on-demand instances and removes retired dependency chains", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<DemandComponent>(
      {.start_time = MIN_ST, .end_time = second},
      values<Value>(dict_delta<Int, TS<Int>>({{1, 0}, {2, 1}}))),
      values<Value>(dict_delta<Int, TS<Int>>({{0, 0}, {1, 1}, {2, 3}})));
  CHECK_OUTPUT(eval_node_with_options<DemandComponent>(
      {.start_time = second, .end_time = second + 3 * MIN_TD},
      values<Value>(none, dict_delta<Int, TS<Int>>({{2, 0}}), dict_delta<Int, TS<Int>>({}, {1, 2}))),
      values<Value>(none, dict_delta<Int, TS<Int>>({{2, 2}}), dict_delta<Int, TS<Int>>({}, {0, 1, 2})));
  const auto third = second + 3 * MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<DemandComponent>(
      {.start_time = third, .end_time = third + MIN_TD},
      values<Value>(dict_delta<Int, TS<Int>>({{2, 1}}))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 3}})));
}

TEST_CASE("mesh checkpoint restores key-set subscriptions quietly", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<KeySetComponent>(
      {.start_time = MIN_ST, .end_time = second}, values<Value>(set_delta<Int>({1, 2}, {}))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 2}, {2, 2}})));
  CHECK_OUTPUT(eval_node_with_options<KeySetComponent>(
      {.start_time = second, .end_time = second + 2 * MIN_TD},
      values<Value>(none, set_delta<Int>({3}, {1}))),
      values<Value>(none, dict_delta<Int, TS<Int>>({{2, 2}, {3, 2}}, {1})));
}

TEST_CASE("mesh checkpoint refuses malformed instance topology before evaluation", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  (void)eval_node_with_options<AccumulatingComponent>(
      {.start_time = MIN_ST, .end_time = second}, values<Value>(dict_delta<Int, TS<Int>>({{1, 2}})));
  REQUIRE(image.has_value());
  bool changed = false;
  for (auto &node : image->graph.nodes) {
    if (!node.custom.children.empty()) {
      node.custom.children.front().slot = KeySlotStore::npos;
      changed = true;
      break;
    }
  }
  REQUIRE(changed);
  CHECK_THROWS_WITH(eval_node_with_options<AccumulatingComponent>(
      {.start_time = second, .end_time = second + MIN_TD}, values<Value>(none)),
      Catch::Matchers::ContainsSubstring("mesh child slot, key or rank is inconsistent"));
}

TEST_CASE("mesh checkpoint keeps previous completion when resumed dependencies form a cycle", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  (void)eval_node_with_options<DemandComponent>(
      {.start_time = MIN_ST, .end_time = second},
      values<Value>(dict_delta<Int, TS<Int>>({{1, 0}, {2, 1}})));
  REQUIRE(image.has_value());
  const auto saved_cut = image->cut;
  CHECK_THROWS_WITH(eval_node_with_options<DemandComponent>(
      {.start_time = second, .end_time = second + MIN_TD},
      values<Value>(dict_delta<Int, TS<Int>>({{0, 2}}))),
      Catch::Matchers::ContainsSubstring("mesh_ has a dependency cycle"));
  CHECK(image->cut == saved_cut);
}

TEST_CASE("mesh checkpoint resumes a never-initialized empty mesh", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<AccumulatingComponent>(
      {.start_time = MIN_ST, .end_time = second}, values<Value>(none)), values<Value>(none));
  REQUIRE(image.has_value());
  CHECK_OUTPUT(eval_node_with_options<AccumulatingComponent>(
      {.start_time = second, .end_time = second + MIN_TD},
      values<Value>(dict_delta<Int, TS<Int>>({{4, 8}}))),
      values<Value>(dict_delta<Int, TS<Int>>({{4, 8}})));
}

TEST_CASE("mesh checkpoint bootstrap nothing source remains invalid after recovery", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  CHECK_OUTPUT(eval_node_with_options<NeverTicksComponent>(
      {.start_time = MIN_ST, .end_time = second}, values<Int>(1)), values<Int>(none));
  REQUIRE(image.has_value());
  CHECK_OUTPUT(eval_node_with_options<NeverTicksComponent>(
      {.start_time = second, .end_time = second + MIN_TD}, values<Int>(2)), values<Int>(none));
}

TEST_CASE("mesh checkpoint never commits a failed child stop and completes other teardown", "[checkpoint][mesh]") {
  stdlib::register_standard_operators();
  GlobalContext context;
  std::optional<ComponentCheckpoint> image;
  configure(context, image);
  const auto second = MIN_ST + MIN_TD;
  (void)eval_node_with_options<FailingStopComponent>(
      {.start_time = MIN_ST, .end_time = second},
      values<Value>(dict_delta<Int, TS<Int>>({{1, 2}, {2, 3}})));
  REQUIRE(image.has_value());
  const auto saved_cut = image->cut;
  stopped_children = 0;
  SECTION("completed day teardown") {
    CHECK_THROWS_WITH(eval_node_with_options<FailingStopComponent>(
        {.start_time = second, .end_time = second + MIN_TD},
        values<Value>(dict_delta<Int, TS<Int>>({{1, -10}, {2, 4}}))),
        Catch::Matchers::ContainsSubstring("mesh child stop failed"));
  }
  SECTION("membership removal teardown") {
    CHECK_THROWS_WITH(eval_node_with_options<FailingStopComponent>(
        {.start_time = second, .end_time = second + 2 * MIN_TD},
        values<Value>(dict_delta<Int, TS<Int>>({{1, -10}, {2, 4}}), dict_delta<Int, TS<Int>>({}, {1}))),
        Catch::Matchers::ContainsSubstring("mesh child stop failed"));
  }
  CHECK(image->cut == saved_cut);
  CHECK(stopped_children == 2);
}
