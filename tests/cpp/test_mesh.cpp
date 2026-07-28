// The ``mesh_`` higher-order operator (lib/std/operators/higher_order.h).
//
// mesh_ is like map_ over a TSD, but per-key instances may read each other's
// outputs (mesh_(func)[k]), create instances on demand, and evaluate in
// dependency-rank order within a cycle. See *Mesh*.
//
// With no cross-instance access, mesh_ is observably identical to map_ (same
// keyed children, same TSD<K, OUT> output).

#include <hgraph/lib/std/operators/impl/higher_order_impl.h> // mesh_ref (mesh_(func)[k])
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/wired_fn.h>

#include "nested_lifecycle_test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <span>
#include <string>
#include <vector>

namespace {
using namespace hgraph;
using namespace hgraph::testing;
using namespace std::string_literals;

struct AddOneG {
  static constexpr auto name = "mesh_add_one_g";
  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> ts) {
    using namespace hgraph::stdlib::syntax;
    return (ts + Int{1}).as<TS<Int>>();
  }
};

struct MeshOverloadMarkerG {
  static constexpr auto name = "mesh_overload_marker_g";
  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> ts) { return ts; }
};

struct MeshTimesTenG {
  static constexpr auto name = "mesh_times_ten_g";
  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> ts) {
    using namespace hgraph::stdlib::syntax;
    return (ts * Int{10}).as<TS<Int>>();
  }
};

struct MeshIdentitySpecialization {
  static constexpr auto name = "mesh_identity_specialization";

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    const WiredFn *func = context.scalar_as<WiredFn>("func");
    return func != nullptr && *func == fn<MeshOverloadMarkerG>();
  }

  static Port<TSD<Str, TS<Int>>>
  compose(Wiring &w, Scalar<"func", WiredFn>,
          Port<TSD<Str, TS<Int>>> values) {
    return wire<stdlib::map_>(w, fn<MeshTimesTenG>(), values)
        .as<TSD<Str, TS<Int>>>();
  }
};

// A key-consuming function (first parameter named "key", the Python rule).
struct AddKeyG {
  static constexpr auto name = "mesh_add_key_g";
  static Port<TS<Int>> compose(Wiring &, NamedPort<"key", TS<Int>> key,
                               Port<TS<Int>> ts) {
    using namespace hgraph::stdlib::syntax;
    return (key + ts).as<TS<Int>>();
  }
};

struct IdentityG {
  static constexpr auto name = "mesh_identity_g";
  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> ts) { return ts; }
};

struct KeyIdentityG {
  static constexpr auto name = "mesh_key_identity_g";
  static Port<TS<Int>> compose(Wiring &, NamedPort<"key", TS<Int>> key) {
    return key;
  }
};

struct CounterNode {
  static constexpr auto name = "mesh_tick_counter";
  static void eval(In<"ts", TS<Int>>, State<Int> count, Out<TS<Int>> out) {
    count.set(count.get() + 1);
    out.set(count.get());
  }
};

struct ContainsNamedMeshKeysG {
  static constexpr auto name = "mesh_contains_named_keys_g";
  static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> probe) {
    auto keys = stdlib::mesh_keys_ref<Str>(w, "named_mesh");
    return wire<stdlib::contains_>(w, keys, probe).as<TS<Bool>>();
  }
};

struct ConstABKeys {
  static constexpr auto name = "mesh_const_ab_keys";
  static Port<TSS<Str>> compose(Wiring &w) {
    return wire<stdlib::const_, TSS<Str>>(
        w, stdlib::make_set<Str>({Str{"a"}, Str{"b"}}));
  }
};

struct ConstBCKeys {
  static constexpr auto name = "mesh_const_bc_keys";
  static Port<TSS<Str>> compose(Wiring &w) {
    return wire<stdlib::const_, TSS<Str>>(
        w, stdlib::make_set<Str>({Str{"b"}, Str{"c"}}));
  }
};

struct NeverKeysNode {
  static constexpr auto name = "mesh_never_keys";
  static void eval(Out<TSS<Str>>) {}
};

struct NoKeys {
  static constexpr auto name = "mesh_no_keys";
  static Port<TSS<Str>> compose(Wiring &w) { return wire<NeverKeysNode>(w); }
};

struct MeshInvalidKeysGraph {
  static constexpr auto name = "mesh_invalid_keys_graph";
  static Port<TSD<Str, TS<Int>>>
  compose(Wiring &w, Port<TSD<Str, TS<Int>>> source, Port<TS<Str>> select) {
    auto keys = wire<stdlib::switch_>(
                    w, select,
                    stdlib::switch_cases({{Value{Str{"ab"}}, fn<ConstABKeys>()},
                                          {Value{Str{"bc"}}, fn<ConstBCKeys>()},
                                          {Value{Str{"none"}}, fn<NoKeys>()}}))
                    .as<TSS<Str>>();
    return wire<stdlib::mesh_>(w, fn<AddOneG>(), source, arg<"__keys__">(keys))
        .as<TSD<Str, TS<Int>>>();
  }
};

struct SwitchDoubleG {
  static constexpr auto name = "mesh_switch_double_g";
  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value) {
    using namespace hgraph::stdlib::syntax;
    return (value * Int{2}).as<TS<Int>>();
  }
};

struct SwitchNegateG {
  static constexpr auto name = "mesh_switch_negate_g";
  static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value) {
    using namespace hgraph::stdlib::syntax;
    return (value * Int{-1}).as<TS<Int>>();
  }
};

struct SwitchInMeshG {
  static constexpr auto name = "switch_in_mesh_g";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> mode,
                               Port<TS<Int>> value) {
    return wire<stdlib::switch_>(
               w, mode,
               stdlib::switch_cases(
                   {{Value{Str{"double"}}, fn<SwitchDoubleG>()},
                    {Value{Str{"negate"}}, fn<SwitchNegateG>()}}),
               value)
        .as<TS<Int>>();
  }
};

struct InvalidSwitchInMeshG {
  static constexpr auto name = "invalid_switch_in_mesh_g";

  static Port<TS<Int>> compose(Wiring &w,
                               NamedPort<"key", TS<Str>> key,
                               Port<TS<Str>> mode) {
    auto size = wire<stdlib::len_>(w, key).as<TS<Int>>();
    return wire<stdlib::switch_>(
               w, mode,
               stdlib::switch_cases(
                   {{Value{Str{"double"}}, fn<SwitchDoubleG>()},
                    {Value{Str{"negate"}}, fn<SwitchNegateG>()}}),
               size)
        .as<TS<Int>>();
  }
};

struct ReduceInvalidSwitchMeshG {
  static constexpr auto name = "reduce_invalid_switch_mesh_g";

  static Port<TS<Int>> compose(Wiring &w, Port<TSS<Str>> keys,
                               Port<TS<Str>> mode) {
    auto meshed =
        wire<stdlib::mesh_>(w, fn<InvalidSwitchInMeshG>(), mode,
                            arg<"__keys__">(keys))
            .as<TSD<Str, TS<Int>>>();
    return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), meshed, Int{0})
        .as<TS<Int>>();
  }
};

struct MeshSwitchZeroG {
  static constexpr auto name = "mesh_switch_zero_g";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>) {
    return wire<stdlib::const_, TS<Int>>(w, Int{10});
  }
};

struct MeshSwitchRefG {
  static constexpr auto name = "mesh_switch_ref_g";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>) {
    auto zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
    return stdlib::mesh_ref<TS<Int>>(w, zero);
  }
};

struct SwitchWithMeshRefG {
  static constexpr auto name = "switch_with_mesh_ref_g";
  static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key) {
    return wire<stdlib::switch_>(
               w, key,
               stdlib::switch_cases({{Value{Int{0}}, fn<MeshSwitchZeroG>()}},
                                    fn<MeshSwitchRefG>()),
               key)
        .as<TS<Int>>();
  }
};

using MeshBundle = UnNamedTSB<Field<"value", TS<Int>>, Field<"other", TS<Int>>>;

struct MeshBundleFromValueG {
  static constexpr auto name = "mesh_bundle_from_value_g";
  static Port<MeshBundle> compose(Wiring &w, Port<TS<Int>> value) {
    using namespace hgraph::stdlib::syntax;
    auto bundle =
        stdlib::to_tsb<MeshBundle>(w, value, (value + Int{10}).as<TS<Int>>());
    return wire<stdlib::pass_through_node>(w, bundle).as<MeshBundle>();
  }
};

struct MeshBundleZeroG {
  static constexpr auto name = "mesh_bundle_zero_g";
  static Port<MeshBundle> compose(Wiring &w, Port<TS<Int>>) {
    auto value = stdlib::to_tsb<MeshBundle>(
        w, wire<stdlib::const_, TS<Int>>(w, Int{10}),
        wire<stdlib::const_, TS<Int>>(w, Int{20}));
    return wire<stdlib::pass_through_node>(w, value).as<MeshBundle>();
  }
};

struct MeshBundleRefG {
  static constexpr auto name = "mesh_bundle_ref_g";
  static Port<MeshBundle> compose(Wiring &w, Port<TS<Int>>) {
    auto zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
    auto peer = stdlib::mesh_ref<MeshBundle>(w, zero);
    auto value = wire<stdlib::getitem_>(w, peer, Str{"value"}).as<TS<Int>>();
    auto other = wire<stdlib::getitem_>(w, peer, Str{"other"}).as<TS<Int>>();
    auto result = stdlib::to_tsb<MeshBundle>(w, value, other);
    return wire<stdlib::pass_through_node>(w, result).as<MeshBundle>();
  }
};

struct SwitchWithMeshBundleRefG {
  static constexpr auto name = "switch_with_mesh_bundle_ref_g";
  static Port<MeshBundle> compose(Wiring &w, NamedPort<"key", TS<Int>> key) {
    return wire<stdlib::switch_, MeshBundle>(
               w, key,
               stdlib::switch_cases({{Value{Int{0}}, fn<MeshBundleZeroG>()}},
                                    fn<MeshBundleRefG>()),
               key)
        .as<MeshBundle>();
  }
};

// A mesh whose instance graph contains a map_: each instance maps
// mesh_(F)[peer] over its per-key ``group`` (a TSD of peer keys) and sums the
// resolved siblings. The map child pauses on mesh_ref, so the MAP must
// propagate the pause (its per-child cursor) up to the mesh, which resolves the
// sibling and resumes. result = base + sum(siblings); an empty group sums to
// default 0.
struct MapInMeshG {
  static constexpr auto name = "mesh_map_in_g";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> peer) {
    return stdlib::mesh_ref<TS<Int>>(w, peer);
  }
};
struct MapInMeshF {
  static constexpr auto name = "mesh_map_in_f";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> base,
                               Port<TSD<Str, TS<Int>>> group) {
    using namespace hgraph::stdlib::syntax;
    Port<TSD<Str, TS<Int>>> mapped =
        wire<stdlib::map_>(w, fn<MapInMeshG>(), group).as<TSD<Str, TS<Int>>>();
    Port<TS<Int>> total = wire<stdlib::sum_>(w, mapped).as<TS<Int>>();
    Port<TS<Int>> zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
    Port<TS<Int>> base_total =
        wire<stdlib::default_>(w, total, zero).as<TS<Int>>();
    return (base + base_total).as<TS<Int>>();
  }
};

// A cross-instance chain: instance ``key`` depends on instance ``link`` (the
// per-key value of the multiplexed ``link`` TSD) and returns ``key +
// result(link)``. Keys with no link entry have an invalid ``link`` element, so
// mesh_(func)[link] stays invalid and default(.., 0) supplies the base.
// Exercises on-demand creation (the base key is created from inside another
// instance), dependency ranking, and same-cycle pause/resume.
struct ChainFn {
  static constexpr auto name = "mesh_chain_fn";
  static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key,
                               Port<TS<Int>> link) {
    using namespace hgraph::stdlib::syntax;
    Port<TS<Int>> dep = stdlib::mesh_ref<TS<Int>>(w, link);
    Port<TS<Int>> zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
    Port<TS<Int>> base = wire<stdlib::default_>(w, dep, zero).as<TS<Int>>();
    return (key + base).as<TS<Int>>();
  }
};

// result = val + result(link). With a changing ``val`` this exercises
// cross-cycle re-propagation: when an instance's output ticks, its dependents
// (subscribed to self[link] via the dynamic value input) re-evaluate the same
// cycle, in rank order.
struct ExprFn {
  static constexpr auto name = "mesh_expr_fn";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> val,
                               Port<TS<Int>> link) {
    using namespace hgraph::stdlib::syntax;
    Port<TS<Int>> dep = stdlib::mesh_ref<TS<Int>>(w, link);
    Port<TS<Int>> zero = wire<stdlib::const_, TS<Int>>(w, Int{0});
    Port<TS<Int>> base = wire<stdlib::default_>(w, dep, zero).as<TS<Int>>();
    return (val + base).as<TS<Int>>();
  }
};

struct MeshLifecycleRecorderTag {};

void wire_mesh_lifecycle_recorder(
    Wiring &w, const WiringPortRef &mesh_output,
    std::span<const WiringPortRef> triggers,
    std::vector<std::size_t> &active_counts,
    std::vector<std::size_t> &constructed_counts,
    std::vector<NestedLifecycleSnapshot> *lifecycle = nullptr) {
  std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
  fields.reserve(1 + triggers.size());
  fields.emplace_back("mesh", mesh_output.schema);
  for (std::size_t i = 0; i < triggers.size(); ++i) {
    fields.emplace_back("trigger" + std::to_string(i), triggers[i].schema);
  }
  const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

  std::vector<TSEndpointSchema> endpoints;
  endpoints.reserve(fields.size());
  endpoints.push_back(TSEndpointSchema::peered(mesh_output.schema));
  for (const WiringPortRef &trigger : triggers) {
    endpoints.push_back(TSEndpointSchema::peered(trigger.schema));
  }

  NodeTypeMetaData meta;
  meta.display_name = "mesh_lifecycle_recorder";
  meta.input_schema = input_schema;
  meta.node_kind = NodeKind::Sink;
  meta.valid_inputs = std::vector<std::size_t>{};

  NodeCallbacks callbacks;
  callbacks.evaluate = [&active_counts, &constructed_counts,
                        lifecycle](const NodeView &view, DateTime) {
    auto graph = view.graph();
    for (std::size_t i = 0; i < graph.node_count(); ++i) {
      auto node = graph.node_at(i);
      if (node.is<MeshNodeView>()) {
        auto mesh = node.as<MeshNodeView>();
        active_counts.push_back(mesh.active_count());
        constructed_counts.push_back(mesh.child_graph_count());
        if (lifecycle != nullptr) {
          lifecycle->push_back(NestedLifecycleCounters::snapshot());
        }
        return;
      }
    }
    throw std::logic_error(
        "mesh_lifecycle_recorder could not find a mesh node");
  };

  NodeBuilder builder = NodeBuilder::native(
      std::move(meta), std::move(callbacks),
      TSEndpointSchema::non_peered(input_schema, std::move(endpoints)));

  std::vector<WiringPortRef> inputs;
  inputs.reserve(1 + triggers.size());
  inputs.push_back(mesh_output);
  inputs.insert(inputs.end(), triggers.begin(), triggers.end());
  static_cast<void>(
      w.add_node(std::type_index(typeid(MeshLifecycleRecorderTag)),
                 std::move(builder), inputs, Value{}));
}
} // namespace

TEST_CASE("mesh_: with no cross-instance access a mesh is observably map_") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Int>>>(
          fn<AddOneG>(),
          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                        dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                        dict_delta<Str, TS<Int>>({}, {"b"s})))),
      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                    dict_delta<Str, TS<Int>>({{"a"s, 11}}),
                    dict_delta<Str, TS<Int>>({}, {"b"s})));
}

TEST_CASE("mesh_: a peer-instantiation func may consume the key") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // key + value, no cross-instance references: identical to map_ with a key
  // arg.
  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
                   fn<AddKeyG>(), values<Value>(dict_delta<Int, TS<Int>>(
                                      {{1, 10}, {2, 20}})))),
               values<Value>(dict_delta<Int, TS<Int>>({{1, 11}, {2, 22}})));
}

TEST_CASE("mesh_: a switch_ terminal writes through to the mesh element") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Str>>, TSD<Str, TS<Int>>>(
          fn<SwitchInMeshG>(),
          values<Value>(
              dict_delta<Str, TS<Str>>({{"a"s, "double"s}, {"b"s, "negate"s}})),
          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 3}, {"b"s, 4}})))),
      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 6}, {"b"s, -4}})));
}

TEST_CASE(
    "mesh_: reduce publishes an explicit zero while switch children are invalid") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      eval_node<ReduceInvalidSwitchMeshG>(
          values<Value>(set_delta<Str>({Str{"k"}}, {}), none, none,
                        set_delta<Str>({}, {Str{"k"}})),
          values<Str>(none, Str{"double"}, Str{"negate"}, none)),
      values<Int>(0, 2, -1, 0));
}

TEST_CASE("mesh_: a pass-through child output forwards the mapped element") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Int>>>(
          fn<IdentityG>(),
          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                        dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                        dict_delta<Str, TS<Int>>({}, {"b"s})))),
      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                    dict_delta<Str, TS<Int>>({{"a"s, 10}}),
                    dict_delta<Str, TS<Int>>({}, {"b"s})));
}

TEST_CASE("mesh_: invalid explicit keys clear children and output") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      (eval_node<MeshInvalidKeysGraph>(
          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}), none),
          values<Str>(Str{"ab"}, Str{"none"}))),
      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                    dict_delta<Str, TS<Int>>({}, {"a"s, "b"s})));
}

TEST_CASE(
    "mesh_: replacing the explicit key-set source reconciles full membership") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT((eval_node<MeshInvalidKeysGraph>(
                   values<Value>(dict_delta<Str, TS<Int>>(
                                     {{"a"s, 1}, {"b"s, 2}, {"c"s, 3}}),
                                 none),
                   values<Str>(Str{"ab"}, Str{"bc"}))),
               values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 2}, {"b"s, 3}}),
                             dict_delta<Str, TS<Int>>({{"c"s, 4}}, {"a"s})));
}

TEST_CASE("mesh_: a removed key re-added later gets a fresh child instance") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSD<Str, TS<Int>>>(
                   fn<CounterNode>(),
                   values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                                 dict_delta<Str, TS<Int>>({{"a"s, 2}}),
                                 dict_delta<Str, TS<Int>>({}, {"a"s}),
                                 dict_delta<Str, TS<Int>>({{"a"s, 3}})))),
               values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                             dict_delta<Str, TS<Int>>({{"a"s, 2}}),
                             dict_delta<Str, TS<Int>>({}, {"a"s}),
                             dict_delta<Str, TS<Int>>({{"a"s, 1}})));
}

TEST_CASE("mesh_: removed instances stop for one cycle before slot erase") {
  using namespace hgraph;
  stdlib::register_standard_operators();
  NestedLifecycleCounters::reset();

  std::vector<std::size_t> active_counts;
  std::vector<std::size_t> constructed_counts;
  std::vector<NestedLifecycleSnapshot> lifecycle;
  Wiring w;
  auto source = wire<stdlib::replay_impl, TSD<Str, TS<Int>>>(w, Str{"source"});
  auto keys = wire<stdlib::replay_impl, TSS<Str>>(w, Str{"keys"});
  auto mesh = wire<stdlib::mesh_>(w, fn<NestedLifecycleNode>(), source,
                                  arg<"__keys__">(keys))
                  .as<TSD<Str, TS<Int>>>();

  const std::array<WiringPortRef, 2> triggers{keys.erased(), source.erased()};
  wire_mesh_lifecycle_recorder(w, mesh.erased(), triggers, active_counts,
                               constructed_counts, &lifecycle);

  GraphBuilder gb = std::move(w).finish();
  set_replay_deltas(
      gb.global_state(), "source",
      values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                    dict_delta<Str, TS<Int>>({{"a"s, 3}}),
                    dict_delta<Str, TS<Int>>({{"b"s, 4}}),
                    dict_delta<Str, TS<Int>>({{"b"s, 5}})));
  set_replay_deltas(
      gb.global_state(), "keys",
      values<Value>(set_delta<Str>({"a"s}, {}), set_delta<Str>({"b"s}, {}),
                    set_delta<Str>({}, {"a"s}), set_delta<Str>({}, {"b"s})));

  {
    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK(lifecycle == std::vector<NestedLifecycleSnapshot>{
                           {1, 1, 1, 0, 0},
                           {2, 2, 2, 0, 0},
                           {2, 2, 2, 1, 0},
                           {2, 1, 2, 2, 1},
                       });
  }

  CHECK(active_counts == std::vector<std::size_t>{1, 2, 1, 0});
  CHECK(constructed_counts == std::vector<std::size_t>{1, 2, 2, 1});
  CHECK(NestedLifecycleCounters::snapshot() ==
        NestedLifecycleSnapshot{2, 0, 2, 2, 2});
}

TEST_CASE("mesh_: named key-set access forwards the mesh output key set") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Str>>>(
          fn<ContainsNamedMeshKeysG>(),
          values<Value>(dict_delta<Str, TS<Str>>({{"a"s, "b"s}, {"b"s, "x"s}})),
          arg<"__name__">(Str{"named_mesh"}))),
      values<Value>(dict_delta<Str, TS<Bool>>({{"a"s, true}, {"b"s, false}})));
}

TEST_CASE("mesh_: explicit keys can drive a key-only function") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSS<Int>>(
                   fn<KeyIdentityG>(),
                   arg<"__keys__">(values<Value>(set_delta<Int>({1, 2}, {}),
                                                 set_delta<Int>({3}, {1}))))),
               values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}}),
                             dict_delta<Int, TS<Int>>({{3, 3}}, {1})));
}

TEST_CASE(
    "mesh_: mesh_ref inside a switch branch preserves the dynamic terminal") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // Requested key 1 selects the mesh-ref branch, which creates key 0 on
  // demand. The switch must preserve mesh_ref's forwarding endpoint while
  // the mesh pauses key 1, evaluates key 0, and resumes the branch.
  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSS<Int>>(
                   fn<SwitchWithMeshRefG>(),
                   arg<"__keys__">(values<Value>(set_delta<Int>({1}, {}))))),
               values<Value>(dict_delta<Int, TS<Int>>({{0, 10}, {1, 10}})));
}

TEST_CASE("mesh_: structured mesh_ref outputs expose stable field endpoints") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSS<Int>>(
                   fn<SwitchWithMeshBundleRefG>(),
                   arg<"__keys__">(values<Value>(set_delta<Int>({1}, {}))))),
               values<Value>(dict_delta<Int, MeshBundle>(
                   {{0, tsb_delta<MeshBundle>(Int{10}, Int{20})},
                    {1, tsb_delta<MeshBundle>(Int{10}, Int{20})}})));
}

TEST_CASE("mesh_: forwarded structured child outputs publish later updates") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Int>>>(
          fn<MeshBundleFromValueG>(),
          values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                        dict_delta<Str, TS<Int>>({{"a"s, 2}})))),
      values<Value>(
          dict_delta<Str, MeshBundle>(
              {{"a"s, tsb_delta<MeshBundle>(Int{1}, Int{11})}}),
          dict_delta<Str, MeshBundle>(
              {{"a"s, tsb_delta<MeshBundle>(Int{2}, Int{12})}})));
}

TEST_CASE(
    "mesh_: cross-instance access settles a dependency chain in one cycle") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // link = {1:0, 2:1, 3:2}: __keys__ derives {1,2,3}; key 0 is created on
  // demand as the base (no link entry → default 0). result[k] = k +
  // result[link[k]]:
  //   0: 0;  1: 1 + result[0]=0 -> 1;  2: 2 + result[1]=1 -> 3;  3: 3 +
  //   result[2]=3 -> 6.
  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
                   fn<ChainFn>(), values<Value>(dict_delta<Int, TS<Int>>(
                                      {{1, 0}, {2, 1}, {3, 2}})))),
               values<Value>(
                   dict_delta<Int, TS<Int>>({{0, 0}, {1, 1}, {2, 3}, {3, 6}})));
}

TEST_CASE(
    "mesh_: a map_ inside a mesh instance propagates child pause/resume") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // base = {1:1, 2:2, 3:3}; group chains 3 -> 2 -> 1 (1 depends on nothing).
  // result[k] = base[k] + sum(mesh[peers]):
  //   1: 1 + 0;  2: 2 + result[1]=1 -> 3;  3: 3 + result[2]=3 -> 6.
  // The per-instance map child reads mesh_(F)[peer] and pauses; the map node
  // propagates that pause (its slot cursor) so the mesh resolves the sibling
  // and resumes.
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Int, TS<Int>>, TSD<Int, TSD<Str, TS<Int>>>>(
          fn<MapInMeshF>(),
          values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}, {3, 3}})),
          values<Value>(dict_delta<Int, TSD<Str, TS<Int>>>(
              {{1, dict_delta<Str, TS<Int>>({})},
               {2, dict_delta<Str, TS<Int>>({{"p"s, 1}})},
               {3, dict_delta<Str, TS<Int>>({{"p"s, 2}})}})))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 3}, {3, 6}})));
}

TEST_CASE("mesh_: a dependency cycle is a runtime error") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // link = {1:2, 2:1}: instance 1 depends on 2 and 2 depends on 1 — a cycle.
  REQUIRE_THROWS((eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
      fn<ChainFn>(),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 2}, {2, 1}})))));
}

TEST_CASE("mesh_: removing a key tears its instance down") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // Cycle 1: link={1:0, 2:1} -> keys {1,2} + on-demand base 0 -> {0:0, 1:1,
  // 2:3}. Cycle 2: remove key 2 from the key set; 2 has no dependents, so it is
  // removed.
  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
                   fn<ChainFn>(),
                   values<Value>(dict_delta<Int, TS<Int>>({{1, 0}, {2, 1}}),
                                 dict_delta<Int, TS<Int>>({}, {2})))),
               values<Value>(dict_delta<Int, TS<Int>>({{0, 0}, {1, 1}, {2, 3}}),
                             dict_delta<Int, TS<Int>>({}, {2})));
}

TEST_CASE(
    "mesh_: retargeting a dependency removes the old on-demand instance") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // Cycle 1: key 2 depends on on-demand key 1 -> {1:1, 2:3}.
  // Cycle 2: key 2 retargets to on-demand key 0. The old 2->1 edge is
  // retracted, so key 1 is removed in the same mesh evaluation.
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
          fn<ChainFn>(), values<Value>(dict_delta<Int, TS<Int>>({{2, 1}}),
                                       dict_delta<Int, TS<Int>>({{2, 0}})))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 3}}),
                    dict_delta<Int, TS<Int>>({{0, 0}, {2, 2}}, {1})));
}

TEST_CASE("mesh_: removing the last requester removes on-demand dependencies") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // Cycle 1: link={1:0, 2:1} -> on-demand base 0 plus requested keys 1 and 2.
  // Cycle 2: key 2 is removed; key 1 still depends on on-demand key 0.
  // Cycle 3: key 1 is removed; its outgoing 1->0 edge is retracted, so 0 is
  // no longer kept alive by the reverse dependency table.
  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
                   fn<ChainFn>(),
                   values<Value>(dict_delta<Int, TS<Int>>({{1, 0}, {2, 1}}),
                                 dict_delta<Int, TS<Int>>({}, {2}),
                                 dict_delta<Int, TS<Int>>({}, {1})))),
               values<Value>(dict_delta<Int, TS<Int>>({{0, 0}, {1, 1}, {2, 3}}),
                             dict_delta<Int, TS<Int>>({}, {2}),
                             dict_delta<Int, TS<Int>>({}, {0, 1})));
}

TEST_CASE(
    "mesh_: an invalid requested key stops forwarding the old dependency") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // key 2 initially reads key 1. Removing link[2] while key 2 evaluates makes
  // the requested peer invalid; mesh_ref should clear its forwarding link, so
  // the old dependency is not forwarded as a stale key-2 tick.
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
          fn<ExprFn>(),
          values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 2}}),
                        dict_delta<Int, TS<Int>>({{2, 3}})),
          values<Value>(dict_delta<Int, TS<Int>>({{2, 1}}),
                        dict_delta<Int, TS<Int>>({}, {2})))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 12}}), none));
}

TEST_CASE("mesh_: a changed input re-propagates through the dependency graph") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // val gives each key's base; link = {2:1, 3:2} chains 3 -> 2 -> 1. Cycle 1
  // settles {1:10, 2:10, 3:10}. Cycle 2 changes only val[1]=20; the tick
  // re-propagates the SAME cycle to 2 and 3 via the dynamic value subscription
  // -> {1:20, 2:20, 3:20}.
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
          fn<ExprFn>(),
          values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 0}, {3, 0}}),
                        dict_delta<Int, TS<Int>>({{1, 20}})),
          values<Value>(dict_delta<Int, TS<Int>>({{2, 1}, {3, 2}}),
                        dict_delta<Int, TS<Int>>({})))),
      values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 10}, {3, 10}}),
                    dict_delta<Int, TS<Int>>({{1, 20}, {2, 20}, {3, 20}})));
}

TEST_CASE("mesh_: retargeting onto an existing quiescent instance settles "
          "without a pause loop") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // Cycle 1: link={1:0, 2:0} -> keys {1,2}, on-demand base 0: {0:0, 1:1, 2:2}.
  // Cycle 2: retarget 2's link from 0 to 1. Instance 1 already EXISTS and has
  // nothing to do this cycle (quiescent); its current output is its settled
  // result, so mesh_ref(1) must read it directly rather than pausing for a
  // same-cycle evaluation that will never come (the settle loop only runs
  // due-or-paused instances). Regression: this deadlocked the settle loop
  // ("mesh_ failed to settle within the cycle").
  CHECK_OUTPUT((eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
                   fn<ChainFn>(),
                   values<Value>(dict_delta<Int, TS<Int>>({{1, 0}, {2, 0}}),
                                 dict_delta<Int, TS<Int>>({{2, 1}})))),
               values<Value>(dict_delta<Int, TS<Int>>({{0, 0}, {1, 1}, {2, 2}}),
                             dict_delta<Int, TS<Int>>({{2, 3}})));
}

TEST_CASE("mesh_: a re-ticked link to a quiescent dependency re-reads its "
          "settled result") {
  using namespace hgraph;
  stdlib::register_standard_operators();

  // Cycle 1: link={1:0, 2:1} -> {0:0, 1:1, 2:3}. Cycle 2 re-ticks link[2]=1
  // (same target): the subscribe node re-runs its dependency registration
  // against instance 1, which is quiescent this cycle. Same regression shape
  // as above via the re-tick (rather than retarget) path — the bench's churn
  // scenario reduces to this after a source-key removal rebinds a dependent.
  // The re-tick itself produces NO output tick (same target, dependency value
  // unchanged); pre-fix this deadlocked the settle loop instead.
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Int, TS<Int>>>(
          fn<ChainFn>(),
          values<Value>(dict_delta<Int, TS<Int>>({{1, 0}, {2, 1}}),
                        dict_delta<Int, TS<Int>>({{2, 1}})))),
      values<Value>(dict_delta<Int, TS<Int>>({{0, 0}, {1, 1}, {2, 3}}), none));
}

TEST_CASE("mesh_: a user overload may select on the wired function identity") {
  using namespace hgraph;
  stdlib::register_standard_operators();
  register_graph_overload<stdlib::mesh_, MeshIdentitySpecialization>();

  const auto input = values<Value>(
      dict_delta<Str, TS<Int>>({{Str{"a"}, 1}, {Str{"b"}, 2}}));
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Int>>>(
          fn<MeshOverloadMarkerG>(), input)),
      values<Value>(dict_delta<Str, TS<Int>>(
          {{Str{"a"}, 10}, {Str{"b"}, 20}})));
  CHECK_OUTPUT(
      (eval_node<stdlib::mesh_, TSD<Str, TS<Int>>>(fn<AddOneG>(), input)),
      values<Value>(dict_delta<Str, TS<Int>>(
          {{Str{"a"}, 2}, {Str{"b"}, 3}})));
}
