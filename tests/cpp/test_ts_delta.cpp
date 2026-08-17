// The runtime, type-erased delta machinery `capture_delta(const TSInputView&)
// -> Value` / `apply_delta(const TSOutputView&, const ValueView&)` —
// schema-as-data, dispatched through the live endpoint's TSDataOps table. This
// suite drives them directly through hand-authored probe source/sink nodes and
// shows the cycle-aligned buffers round-trip identically (against the canonical
// delta builders) for TS / SIGNAL / TSS / TSD / TSL / TSB / TSW.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace {
using namespace hgraph;
using namespace hgraph::testing; // none,
                                 // make_buffer/make_any/empty_any/cycle_offset,
                                 // set_replay_*, get_recorded_*
using Quote = TSB<"DeltaQuote", Field<"bid", TS<Int>>, Field<"ask", TS<Int>>>;
using QuoteWithSet =
    TSB<"DeltaQuoteWithSet", Field<"levels", TSS<Int>>, Field<"last", TS<Int>>>;

// The erased free functions take the base TSInputView / TSOutputView. A typed
// `In<Name,S>` reaches it uniformly via `base()`; a typed `Out<S>` is the base
// itself only for the scalar case (`Out<TS<T>> : TSOutputView`), while the
// container outputs compose one (reached via `base()`). (Phase 2's `Out<TsVar>`
// *is* a TSOutputView, so this asymmetry disappears for the real erased nodes.)
template <typename S> const TSOutputView &out_view(const Out<S> &out) {
  if constexpr (std::is_base_of_v<TSOutputView, Out<S>>) {
    return out;
  } else {
    return out.base();
  }
}

// A `record` clone whose eval captures the per-cycle delta with the runtime
// `capture_delta` instead of `ts_delta<S>::capture`. `ts` (an In<"ts", S>)
// slices to the erased `TSInputView` the function takes.
template <typename S> struct ProbeRecord {
  static constexpr auto name = "ts_delta_probe_record";
  static void start(Scalar<"key", Str> key, GlobalStateView gs) {
    gs.set(key.value(), make_buffer());
  }
  static void eval(In<"ts", S> ts, Scalar<"key", Str> key, GlobalStateView gs,
                   DateTime now) {
    const std::size_t offset = cycle_offset(now);
    const ValueView buffer = gs.get(key.value());
    auto list = buffer.as_list();
    std::size_t size = list.size();
    auto mutation = list.begin_mutation();
    while (size < offset) {
      mutation.push_back(empty_any().view());
      ++size;
    }
    mutation.push_back(make_any(capture_delta(ts.base())).view());
  }
};

// Capture a canonical full-state delta on each input tick. This is the
// transport snapshot form: fields that did not tick in the current cycle are
// still represented from their current values.
template <typename S> struct ProbeCurrentRecord {
  static constexpr auto name = "ts_delta_probe_current_record";
  static void start(Scalar<"key", Str> key, GlobalStateView gs) {
    gs.set(key.value(), make_buffer());
  }
  static void eval(In<"ts", S> ts, Scalar<"key", Str> key, GlobalStateView gs,
                   DateTime now) {
    const std::size_t offset = cycle_offset(now);
    auto list = gs.get(key.value()).as_list();
    auto mutation = list.begin_mutation();
    std::size_t size = list.size();
    while (size < offset) {
      mutation.push_back(empty_any().view());
      ++size;
    }
    mutation.push_back(make_any(capture_current_delta(ts.base())).view());
  }
};

// A `replay` clone whose eval re-creates ticks with the runtime `apply_delta`
// instead of `ts_delta<S>::apply`. `out` (an Out<S>) slices to the erased
// `TSOutputView` the function takes.
template <typename S> struct ProbeReplay {
  static constexpr auto name = "ts_delta_probe_replay";
  static constexpr bool schedule_on_start = true;
  static void eval(Scalar<"key", Str> key, GlobalStateView gs,
                   NodeScheduler sched, State<Int> index, Out<S> out) {
    const ValueView buffer = gs.get(key.value());
    if (!buffer.valid()) {
      return;
    }
    const auto list = buffer.as_list();
    const Int i = index.get();
    const std::size_t size = list.size();
    if (i >= Int{0} && static_cast<std::size_t>(i) < size) {
      const auto element = list.at(static_cast<std::size_t>(i)).as_any();
      if (element.has_value()) {
        apply_delta(out_view(out), element.get());
      }
    }
    const Int next = i + Int{1};
    index.set(next);
    if (next >= Int{0} && static_cast<std::size_t>(next) < size) {
      sched.schedule(MIN_TD);
    }
  }
};

// capture parity: replay (ts_delta::apply) -> ProbeRecord (capture_delta).
template <typename S> struct CaptureGraph {
  static constexpr auto name = "ts_delta_capture_graph";
  static void compose(Wiring &w) {
    auto src = wire<stdlib::replay_impl, S>(w, Str{"in"});
    wire<ProbeRecord<S>>(w, src, Str{"out"});
  }
};

// apply parity: ProbeReplay (apply_delta) -> record (ts_delta::capture).
template <typename S> struct ApplyGraph {
  static constexpr auto name = "ts_delta_apply_graph";
  static void compose(Wiring &w) {
    auto src = wire<ProbeReplay<S>>(w, Str{"in"});
    wire<stdlib::dense_record_impl>(w, src, Str{"out"});
  }
};

// both new functions: ProbeReplay (apply_delta) -> ProbeRecord (capture_delta).
template <typename S> struct RoundTripGraph {
  static constexpr auto name = "ts_delta_roundtrip_graph";
  static void compose(Wiring &w) {
    auto src = wire<ProbeReplay<S>>(w, Str{"in"});
    wire<ProbeRecord<S>>(w, src, Str{"out"});
  }
};

template <typename S> struct CurrentCaptureGraph {
  static constexpr auto name = "ts_delta_current_capture_graph";
  static void compose(Wiring &w) {
    auto src = wire<stdlib::replay_impl, S>(w, Str{"in"});
    wire<ProbeCurrentRecord<S>>(w, src, Str{"out"});
  }
};

template <typename Graph, typename Seed> auto run_graph(Seed seed) {
  GraphBuilder gb = build_graph<Graph>();
  seed(gb.global_state());
  GraphExecutorBuilder eb;
  eb.graph_builder(std::move(gb))
      .start_time(MIN_ST)
      .end_time(MIN_ST + TimeDelta{10});
  GraphExecutorValue ex = eb.make_executor();
  ex.view().run();
  return ex;
}
} // namespace

TEST_CASE("TSData realizations select a non-missing current-state policy") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::array schemas{
      schema_descriptor<TS<Int>>::ts_meta(),
      schema_descriptor<TSS<Int>>::ts_meta(),
      schema_descriptor<TSD<Int, TS<Int>>>::ts_meta(),
      schema_descriptor<TSL<TS<Int>, 2>>::ts_meta(),
      schema_descriptor<TSL<TS<Int>>>::ts_meta(),
      schema_descriptor<TSW<Int, 3, 1>>::ts_meta(),
      schema_descriptor<Quote>::ts_meta(),
      schema_descriptor<REF<TS<Int>>>::ts_meta(),
      schema_descriptor<SIGNAL>::ts_meta(),
  };

  for (const auto *schema : schemas) {
    CAPTURE(schema->name());
    const auto type = TSDataPlanFactory::instance().data_type_for(schema);
    REQUIRE(type);
    REQUIRE(type.ops_ref().current_state_ops != nullptr);
    REQUIRE(type.ops_ref().current_state_ops !=
            &ts_current_state_detail::missing_current_state_ops());
  }
}

TEST_CASE("current-state reconciliation preserves fixed-structure child validity") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const auto *schema = schema_descriptor<Quote>::ts_meta();
  TSOutput source{schema};
  TSOutput target{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(source.view(MIN_ST));

  Value first = tsb_delta<Quote>(1, std::nullopt);
  apply_delta(source.view(MIN_ST), first.view());
  reconcile_current_state(
      target.view(MIN_ST), input.view(nullptr, MIN_ST),
      TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false});

  auto initial_view = target.view(MIN_ST);
  auto initial = initial_view.as_bundle();
  REQUIRE(initial.at(0).valid());
  REQUIRE(initial.at(0).value().checked_as<Int>() == 1);
  REQUIRE_FALSE(initial.at(1).valid());

  const DateTime t2 = MIN_ST + MIN_TD;
  Value second = tsb_delta<Quote>(std::nullopt, 2);
  apply_delta(source.view(t2), second.view());
  reconcile_current_state(
      target.view(t2), input.view(nullptr, t2),
      TSCurrentReconcileOptions{TSCurrentReconcileScope::Incremental, false});
  auto updated_view = target.view(t2);
  auto updated = updated_view.as_bundle();
  REQUIRE(updated.at(0).value().checked_as<Int>() == 1);
  REQUIRE(updated.at(1).value().checked_as<Int>() == 2);

  const DateTime t3 = t2 + MIN_TD;
  {
    auto source_view = source.view(t3);
    auto source_bundle = source_view.as_bundle();
    auto child = source_bundle.at(0);
    auto mutation = child.begin_mutation(t3);
    REQUIRE(mutation.invalidate());
  }
  reconcile_current_state(
      target.view(t3), input.view(nullptr, t3),
      TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false});
  auto invalidated_view = target.view(t3);
  auto invalidated = invalidated_view.as_bundle();
  REQUIRE_FALSE(invalidated.at(0).valid());
  REQUIRE(invalidated.at(1).value().checked_as<Int>() == 2);

  const DateTime t4 = t3 + MIN_TD;
  reconcile_current_state(
      target.view(t4), input.view(nullptr, t4),
      TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false});
  REQUIRE_FALSE(target.view(t4).modified());
}

TEST_CASE("current-state reconciliation rejects incompatible TS topology") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *text = registry.register_scalar<Str>("str");
  const auto *list_value = registry.list(integer);
  const auto *atomic_list = registry.ts(list_value);
  const auto *structural_list = registry.tsl(registry.ts(integer), 0);

  // These expose the same Python/current-value shape, but one is an atomic
  // TS[List[Int]] and the other is an indexed TSL[TS[Int]]. Dispatching the
  // target's indexed strategy over the atomic source is never valid.
  REQUIRE(atomic_list->value_schema == structural_list->value_schema);
  TSOutput atomic_source{atomic_list};
  TSOutput list_target{structural_list};
  REQUIRE_THROWS_AS(
      reconcile_current_state(
          list_target.view(MIN_ST), atomic_source.data_view(),
          TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false}),
      std::invalid_argument);

  // The same distinction must be preserved recursively when the mismatched
  // child topology is hidden below an otherwise-compatible TSD root.
  const auto *atomic_dict = registry.tsd(text, atomic_list);
  const auto *structural_dict = registry.tsd(text, structural_list);
  REQUIRE(atomic_dict->value_schema == structural_dict->value_schema);
  TSOutput nested_source{atomic_dict};
  TSOutput nested_target{structural_dict};
  REQUIRE_THROWS_AS(
      reconcile_current_state(
          nested_target.view(MIN_ST), nested_source.data_view(),
          TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false}),
      std::invalid_argument);
}

TEST_CASE("full current-state reconciliation rejects dynamic TSL shrink") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  using DynamicList = TSL<TS<Int>>;
  const auto *schema = schema_descriptor<DynamicList>::ts_meta();
  TSOutput source{schema};
  TSOutput target{schema};

  Value source_delta = list_delta<TS<Int>>({{0, 1}});
  Value target_delta = list_delta<TS<Int>>({{0, 10}, {1, 20}});
  apply_delta(source.view(MIN_ST), source_delta.view());
  apply_delta(target.view(MIN_ST), target_delta.view());

  const DateTime next = MIN_ST + MIN_TD;
  REQUIRE_THROWS_AS(
      reconcile_current_state(
          target.view(next), source.data_view(),
          TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false}),
      std::invalid_argument);

  // Rejection happens before any child is partially reconciled.
  auto target_view = target.view(next);
  auto preserved = target_view.as_list();
  REQUIRE(preserved.size() == 2);
  REQUIRE(preserved.at(0).value().checked_as<Int>() == 10);
  REQUIRE(preserved.at(1).value().checked_as<Int>() == 20);
}

TEST_CASE("observable-delta policy rejects scheduling-only fixed-list invalidation") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  using One = TSL<TS<Int>, 1>;
  const auto *schema = schema_descriptor<One>::ts_meta();
  TSOutput source{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(source.view(MIN_ST));

  Value first = list_delta<TS<Int>>({{0, 1}});
  apply_delta(source.view(MIN_ST), first.view());

  const DateTime t2 = MIN_ST + MIN_TD;
  {
    auto source_view = source.view(t2);
    auto source_list = source_view.as_list();
    auto child = source_list.at(0);
    auto mutation = child.begin_mutation(t2);
    REQUIRE(mutation.invalidate());
  }
  auto source_input = input.view(nullptr, t2);
  REQUIRE(source_input.modified());
  Value delta = capture_delta(source_input);
  REQUIRE(delta.view().as_map().empty());
  REQUIRE_FALSE(delta_is_observable(source_input, delta.view()));
}

TEST_CASE(
    "ts_delta: capture_delta matches ts_delta<S>::capture for a scalar TS") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  auto ex = run_graph<CaptureGraph<TS<Int>>>([](const GlobalStateView &gs) {
    set_replay_values<Int>(gs, "in", {1, none, 3});
  });
  CHECK_OUTPUT(
      get_recorded_values<Int>(ex.view().graph().global_state(), "out"),
      {1, none, 3});
}

TEST_CASE("ts_delta: apply_delta matches ts_delta<S>::apply for a scalar TS") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  auto ex = run_graph<ApplyGraph<TS<Int>>>([](const GlobalStateView &gs) {
    set_replay_values<Int>(gs, "in", {4, none, 6});
  });
  CHECK_OUTPUT(
      get_recorded_values<Int>(ex.view().graph().global_state(), "out"),
      {4, none, 6});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSS set delta") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{set_delta<Int>({1, 2}, {}),
                                                 set_delta<Int>({3}, {1}),
                                                 set_delta<Int>({}, {2, 3})};

  // capture parity
  auto cap = run_graph<CaptureGraph<TSS<Int>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(cap.view().graph().global_state(), "out"),
               {set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1}),
                set_delta<Int>({}, {2, 3})});

  // apply parity
  auto app = run_graph<ApplyGraph<TSS<Int>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(app.view().graph().global_state(), "out"),
               {set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1}),
                set_delta<Int>({}, {2, 3})});
}

TEST_CASE(
    "ts_delta: capture/apply round-trip a fixed-TSL-of-scalar list delta") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{
      list_delta<TS<Int>>({{0, 1}, {1, 2}}), list_delta<TS<Int>>({{0, 5}}),
      list_delta<TS<Int>>({{1, 9}})};

  // both new functions together: ProbeReplay -> ProbeRecord must reproduce the
  // input.
  auto rt = run_graph<RoundTripGraph<TSL<TS<Int>, 2>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                list_delta<TS<Int>>({{0, 5}}), list_delta<TS<Int>>({{1, 9}})});
}

TEST_CASE(
    "ts_delta: capture/apply round-trip a dynamic TSL-of-scalar list delta") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{
      list_delta<TS<Int>>({{0, 1}}),
      list_delta<TS<Int>>({{0, 5}, {1, 9}}),
      list_delta<TS<Int>>({{1, 11}}),
  };

  auto rt = run_graph<RoundTripGraph<TSL<TS<Int>>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {list_delta<TS<Int>>({{0, 1}}),
                list_delta<TS<Int>>({{0, 5}, {1, 9}}),
                list_delta<TS<Int>>({{1, 11}})});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSD-of-scalar dict delta") {
  using namespace std::string_literals;

  (void)TypeRegistry::instance().register_scalar<Int>("int");
  (void)TypeRegistry::instance().register_scalar<Str>("str");
  const std::vector<std::optional<Value>> deltas{
      dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
      dict_delta<Str, TS<Int>>({{"a"s, 5}}, {"b"s}),
      dict_delta<Str, TS<Int>>({{"b"s, 9}}),
  };

  auto rt = run_graph<RoundTripGraph<TSD<Str, TS<Int>>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                dict_delta<Str, TS<Int>>({{"a"s, 5}}, {"b"s}),
                dict_delta<Str, TS<Int>>({{"b"s, 9}})});
}

TEST_CASE("ts_delta: TSD lenient removals tick only when a key exists") {
  using namespace std::string_literals;

  (void)TypeRegistry::instance().register_scalar<Int>("int");
  (void)TypeRegistry::instance().register_scalar<Str>("str");
  const std::vector<std::optional<Value>> deltas{
      dict_delta<Str, TS<Int>>({}, {"missing"s}),
      dict_delta<Str, TS<Int>>({}),
      dict_delta<Str, TS<Int>>({{"a"s, 1}}),
      dict_delta<Str, TS<Int>>({}, {"a"s}),
      dict_delta<Str, TS<Int>>({}, {"a"s}),
      dict_delta<Str, TS<Int>>({{"b"s, 2}}),
  };

  auto app = run_graph<ApplyGraph<TSD<Str, TS<Int>>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(app.view().graph().global_state(), "out"),
               {none, dict_delta<Str, TS<Int>>({}),
                dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                dict_delta<Str, TS<Int>>({}, {"a"s}), none,
                dict_delta<Str, TS<Int>>({{"b"s, 2}})});
}

TEST_CASE("TSD output slots use the graph's closed Bundle realization") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *text = registry.register_scalar<Str>("str");
  const auto *base = registry.bundle("tests.tsd.realization", "Request",
                                     {{"id", integer}}, {}, true);
  const auto *leaf = registry.bundle("tests.tsd.realization", "PostRequest",
                                     {{"id", integer}, {"body", text}}, {base});
  const auto *base_ts = registry.ts(base);
  const auto *dict_schema = registry.tsd(integer, base_ts);

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  TypeRealizationScope scope{snapshot.get()};
  TSOutput output{*dict_schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *dict_schema, TSEndpointSchema::peered(dict_schema))};
  input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));

  Value request{ValuePlanFactory::instance().type_for(leaf)};
  auto request_fields = request.as_bundle().begin_mutation();
  request_fields["id"].set(Int{7});
  request_fields["body"].set(Str{"payload"});

  Value key{Int{1}};
  auto output_view = output.view(MIN_ST);
  auto dict = output_view.as_dict();
  {
    auto mutation = dict.begin_mutation(MIN_ST);
    mutation.set(key.view(), request.view());
  }

  const auto stored = dict.at(key.view()).value().concrete();
  REQUIRE(stored.schema() == leaf);
  REQUIRE(stored.as_bundle()["body"].checked_as<Str>() == "payload");

  const Value captured = capture_delta(input.view(nullptr, MIN_ST));
  const auto modified = captured.view().as_bundle()["modified"].as_map();
  const auto captured_request = modified.at(key.view()).concrete();
  REQUIRE(captured_request.schema() == leaf);
  REQUIRE(captured_request.as_bundle()["body"].checked_as<Str>() == "payload");

  const Value current = capture_current_delta(input.view(nullptr, MIN_ST));
  const auto current_modified =
      current.view().as_bundle()["modified"].as_map();
  const auto current_request = current_modified.at(key.view()).concrete();
  REQUIRE(current_request.schema() == leaf);
  REQUIRE(current_request.as_bundle()["body"].checked_as<Str>() == "payload");
}

TEST_CASE("apply_current_value accepts a concrete closed Bundle alternative") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *text = registry.register_scalar<Str>("str");
  const auto *base = registry.bundle("tests.current_value", "Unit",
                                     {{"id", integer}}, {}, true);
  const auto *leaf = registry.bundle("tests.current_value", "NamedUnit",
                                     {{"id", integer}, {"name", text}}, {base});
  const auto *base_ts = registry.ts(base);

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  TypeRealizationScope scope{snapshot.get()};
  TSOutput output{*base_ts};

  Value concrete{ValuePlanFactory::instance().type_for(leaf)};
  auto fields = concrete.as_bundle().begin_mutation();
  fields["id"].set(Int{7});
  fields["name"].set(Str{"kg"});

  apply_current_value(output.view(MIN_ST), concrete.view());

  const auto stored = output.view(MIN_ST).value().concrete();
  REQUIRE(stored.schema() == leaf);
  REQUIRE(stored.as_bundle()["name"].checked_as<Str>() == "kg");
}

TEST_CASE(
    "TSD output slots realize polymorphic values nested in TSB elements") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *text = registry.register_scalar<Str>("str");
  const auto *base = registry.bundle("tests.tsd.tsb.realization", "Instrument",
                                     {{"id", integer}}, {}, true);
  const auto *leaf =
      registry.bundle("tests.tsd.tsb.realization", "Future",
                      {{"id", integer}, {"symbol", text}}, {base});
  const auto *base_ts = registry.ts(base);
  const auto *element =
      registry.tsb("TSDRealizedBundleElement", {{"instrument", base_ts}});
  const auto *dict_schema = registry.tsd(integer, element);

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  TypeRealizationScope scope{snapshot.get()};
  TSOutput output{*dict_schema};

  Value instrument{ValuePlanFactory::instance().type_for(leaf)};
  auto instrument_fields = instrument.as_bundle().begin_mutation();
  instrument_fields["id"].set(Int{7});
  instrument_fields["symbol"].set(Str{"EDZ6"});

  Value key{Int{1}};
  auto output_view = output.view(MIN_ST);
  auto dict = output_view.as_dict();
  {
    auto dict_mutation = dict.begin_mutation(MIN_ST);
    auto element_output = dict_mutation[key.view()];
    auto element_bundle = element_output.as_bundle();
    auto instrument_output = element_bundle.field("instrument");
    auto instrument_mutation = instrument_output.begin_mutation(MIN_ST);
    REQUIRE(instrument_mutation.copy_value_from(instrument.view()));
  }

  const auto stored_element = dict.at(key.view());
  const auto stored_bundle = stored_element.as_bundle();
  const auto stored_instrument = stored_bundle.field("instrument");
  const auto stored = stored_instrument.value().concrete();
  REQUIRE(stored.schema() == leaf);
  REQUIRE(stored.as_bundle()["symbol"].checked_as<Str>() == "EDZ6");
}

TEST_CASE("TSB output fields use the graph's closed Bundle realization") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *text = registry.register_scalar<Str>("str");
  const auto *base = registry.bundle("tests.tsb.realization", "Response",
                                     {{"id", integer}}, {}, true);
  const auto *leaf =
      registry.bundle("tests.tsb.realization", "DeleteResponse",
                      {{"id", integer}, {"reason", text}}, {base});
  const auto *base_ts = registry.ts(base);
  const auto *keyed = registry.tsd(integer, base_ts);
  const auto *bundle =
      registry.tsb("TSBRealizedResponseFields",
                   {{"response", base_ts}, {"responses", keyed}});

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  TypeRealizationScope scope{snapshot.get()};
  TSOutput output{*bundle};

  Value response{ValuePlanFactory::instance().type_for(leaf)};
  auto response_fields = response.as_bundle().begin_mutation();
  response_fields["id"].set(Int{7});
  response_fields["reason"].set(Str{"deleted"});

  auto output_view = output.view(MIN_ST);
  auto output_bundle = output_view.as_bundle();
  {
    auto mutation = output_bundle.field("response").begin_mutation(MIN_ST);
    REQUIRE(mutation.copy_value_from(response.view()));
  }
  Value key{Int{1}};
  auto responses = output_bundle.field("responses");
  auto response_dict = responses.as_dict();
  {
    auto mutation = response_dict.begin_mutation(MIN_ST);
    mutation.set(key.view(), response.view());
  }

  const auto stored = output_bundle.field("response").value().concrete();
  REQUIRE(stored.schema() == leaf);
  REQUIRE(stored.as_bundle()["reason"].checked_as<Str>() == "deleted");
  const auto keyed_stored = response_dict.at(key.view()).value().concrete();
  REQUIRE(keyed_stored.schema() == leaf);
  REQUIRE(keyed_stored.as_bundle()["reason"].checked_as<Str>() == "deleted");

  Value copied_value{responses.value()};
  const auto copied_stored = copied_value.as_map().at(key.view()).concrete();
  REQUIRE(copied_stored.schema() == leaf);
  REQUIRE(copied_stored.as_bundle()["reason"].checked_as<Str>() == "deleted");

  Value copied_delta{responses.delta_value()};
  const auto copied_modified =
      copied_delta.as_bundle()["modified"].as_map().at(key.view()).concrete();
  REQUIRE(copied_modified.schema() == leaf);
  REQUIRE(copied_modified.as_bundle()["reason"].checked_as<Str>() == "deleted");
}

TEST_CASE("ts_delta: capture/apply round-trip a TSB sparse field delta") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{
      tsb_delta<Quote>(1, 10),
      tsb_delta<Quote>(std::nullopt, 20),
      tsb_delta<Quote>(3, std::nullopt),
  };

  auto rt = run_graph<RoundTripGraph<Quote>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {tsb_delta<Quote>(1, 10), tsb_delta<Quote>(std::nullopt, 20),
                tsb_delta<Quote>(3, std::nullopt)});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSD of TSB values") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{
      dict_delta<Int, Quote>({{1, tsb_delta<Quote>(10, 20)}}),
      dict_delta<Int, Quote>({{1, tsb_delta<Quote>(11, std::nullopt)}}),
      dict_delta<Int, Quote>({{2, tsb_delta<Quote>(30, 40)}}, {1}),
  };

  auto rt = run_graph<RoundTripGraph<TSD<Int, Quote>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(
      get_recorded_deltas(rt.view().graph().global_state(), "out"),
      {dict_delta<Int, Quote>({{1, tsb_delta<Quote>(10, 20)}}),
       dict_delta<Int, Quote>({{1, tsb_delta<Quote>(11, std::nullopt)}}),
       dict_delta<Int, Quote>({{2, tsb_delta<Quote>(30, 40)}}, {1})});
}

TEST_CASE(
    "ts_delta: capture/apply round-trip a TSB with a container field delta") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{
      tsb_delta<QuoteWithSet>(set_delta<Int>({1, 2}, {}), std::nullopt),
      tsb_delta<QuoteWithSet>(std::nullopt, 5),
      tsb_delta<QuoteWithSet>(set_delta<Int>({3}, {1}), 6),
  };

  auto rt = run_graph<RoundTripGraph<QuoteWithSet>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(
      get_recorded_deltas(rt.view().graph().global_state(), "out"),
      {tsb_delta<QuoteWithSet>(set_delta<Int>({1, 2}, {}), std::nullopt),
       tsb_delta<QuoteWithSet>(std::nullopt, 5),
       tsb_delta<QuoteWithSet>(set_delta<Int>({3}, {1}), 6)});
}

TEST_CASE("ts_delta: capture_current_delta includes unchanged scalar and collection fields") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{
      tsb_delta<QuoteWithSet>(set_delta<Int>({1, 2}, {}), 5),
      tsb_delta<QuoteWithSet>(std::nullopt, 6),
  };

  auto captured = run_graph<CurrentCaptureGraph<QuoteWithSet>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(
      get_recorded_deltas(captured.view().graph().global_state(), "out"),
      {tsb_delta<QuoteWithSet>(set_delta<Int>({1, 2}, {}), 5),
       tsb_delta<QuoteWithSet>(set_delta<Int>({1, 2}, {}), 6)});
}

TEST_CASE("ts_delta: capture/apply round-trip a TSW scalar push delta") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{Value{Int{1}}, Value{Int{2}},
                                                 Value{Int{3}}};

  auto rt = run_graph<RoundTripGraph<TSW<Int, 3, 1>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {Value{Int{1}}, Value{Int{2}}, Value{Int{3}}});
}

TEST_CASE("ts_delta: legacy TSW delta capture rejects a clear-only tick") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const auto *schema = schema_descriptor<TSW<Int, 3, 1>>::ts_meta();
  TSOutput output{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};

  const auto t1 = MIN_ST;
  const auto t2 = t1 + MIN_TD;
  input.view(nullptr, t1).bind_output(output.view(t1));

  {
    auto data = output.data_view();
    auto window = data.as_window();
    auto mutation = window.begin_mutation(t1);
    Value one{Int{1}};
    mutation.push(one.view());
  }
  {
    auto data = output.data_view();
    auto window = data.as_window();
    auto mutation = window.begin_mutation(t2);
    mutation.clear();
  }

  auto cleared = input.view(nullptr, t2);
  REQUIRE(cleared.modified());
  REQUIRE_FALSE(cleared.delta_value().has_value());
  REQUIRE_THROWS_AS(capture_delta(cleared), std::logic_error);
}

TEST_CASE("ts_delta: legacy TSW delta capture rejects clear followed by push") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const auto *schema = schema_descriptor<TSW<Int, 3, 1>>::ts_meta();
  TSOutput output{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};

  const auto t1 = MIN_ST;
  input.view(nullptr, t1).bind_output(output.view(t1));

  {
    auto data = output.data_view();
    auto window = data.as_window();
    auto mutation = window.begin_mutation(t1);
    Value one{Int{1}};
    mutation.clear();
    mutation.push(one.view());
  }

  auto reset_and_pushed = input.view(nullptr, t1);
  REQUIRE(reset_and_pushed.modified());
  REQUIRE(reset_and_pushed.delta_value().checked_as<Int>() == 1);
  REQUIRE_THROWS_AS(capture_delta(reset_and_pushed), std::logic_error);
}

TEST_CASE("ts_delta: capture/apply round-trip a SIGNAL tick delta") {
  (void)TypeRegistry::instance().register_scalar<bool>("bool");
  const std::vector<std::optional<Value>> deltas{Value{true}, Value{true},
                                                 Value{true}};

  auto rt = run_graph<RoundTripGraph<SIGNAL>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {Value{true}, Value{true}, Value{true}});
}
