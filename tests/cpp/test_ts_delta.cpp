// The runtime, type-erased delta machinery `capture_delta(const TSInputView&)
// -> Value` / `apply_delta(const TSOutputView&, const ValueView&)` —
// schema-as-data, dispatched through the live endpoint's TSDataOps table. This
// suite drives them directly through hand-authored probe source/sink nodes and
// shows the cycle-aligned buffers round-trip identically (against the canonical
// delta builders) for TS / SIGNAL / TSS / TSD / TSL / TSB / TSW.

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/operators/stream.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/runtime/registry_snapshot.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/output_mutation.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/utils/key_slot_store.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/binary_codec.h>

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

using HeldSamples = TSL<TS<Int>, 2>;
using HeldBook = TSD<Int, TS<Int>>;
struct HeldListSource {
  static constexpr auto name = "held_list_source";
  static void eval(In<"action", TS<Int>> action, Out<HeldSamples> out) {
    if (action.value() == 1) { out.set(0, Int{7}); out.set(1, Int{100}); }
    if (action.value() == 2) { invalidate(out, 0); }
    if (action.value() == 3) { out.set(0, Int{9}); }
  }
};
struct HeldMapSource {
  static constexpr auto name = "held_map_source";
  static void eval(In<"action", TS<Int>> action, Out<HeldBook> out) {
    if (action.value() == 1) { out.set(Int{1}, Int{7}); out.set(Int{2}, Int{100}); }
    if (action.value() == 2) { invalidate(out, Int{1}); }
    if (action.value() == 3) { out.set(Int{1}, Int{9}); }
  }
};
template <typename S> struct HeldCollectionObserver {
  static constexpr auto name = "held_collection_observer";
  static void eval(In<"step", TS<Int>>, In<"value", S, InputActivity::Passive, InputValidity::Unchecked> value,
                   Out<TS<Bool>> out) {
    const auto before = type_system_lock_count();
    Value snapshot{value.base().value()};
    REQUIRE(type_system_lock_count() == before);
    if constexpr (std::is_same_v<S, HeldSamples>) {
      REQUIRE(snapshot.view().as_list().size() == 2);
      REQUIRE(snapshot.view().as_list().at(1).checked_as<Int>() == 100);
      const bool live = snapshot.view().as_list().at(0).has_value();
      REQUIRE(live == value[0].valid());
      out.set(live);
    } else {
      Value key{Int{1}};
      REQUIRE(snapshot.view().as_map().contains(key.view()));
      const bool live = snapshot.view().as_map().at(key.view()).has_value();
      REQUIRE(live == value[Int{1}].valid());
      out.set(live);
    }
  }
};
template <typename Source, typename S> struct HeldCollectionGraph {
  static constexpr auto name = "held_collection_graph";
  static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Int>> step) {
    return wire<HeldCollectionObserver<S>>(w, step, wire<Source>(w, action));
  }
};
struct SampleHeldBook {
  static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Int>> trigger, Port<TS<Int>> clock) {
    auto book = wire<HeldMapSource>(w, action);
    auto sampled = wire<stdlib::sample>(w, trigger, book).as<HeldBook>();
    return wire<HeldCollectionObserver<HeldBook>>(w, clock, sampled);
  }
};
// Retained and projected values have one semantic hash, including typed holes.
void check_held_hash_lookup(const ValueView &observed, const ValueView &retained) {
  REQUIRE(observed.equals(retained));
  REQUIRE(retained.equals(observed));
  CHECK(observed.hash() == retained.hash());
  KeySlotStore keys{retained.binding()};
  const auto inserted = keys.insert(retained);
  REQUIRE(inserted.inserted);
  CHECK(keys.find_slot(observed) == inserted.slot);
  CHECK_FALSE(keys.insert(observed).inserted);
  CHECK(keys.size() == 1);
}
} // namespace

TEST_CASE("public C++ wiring observes invalid collection children on an independent step", "[held-child-validity]") {
  const auto actions = values<Int>(1, none, 2, none, none, 3, none);
  const auto steps = values<Int>(none, 1, none, 1, 1, none, 1);
  const auto expected = values<Bool>(none, true, none, false, false, none, true);
  CHECK_OUTPUT((eval_node<HeldCollectionGraph<HeldListSource, HeldSamples>>(actions, steps)), expected);
  CHECK_OUTPUT((eval_node<HeldCollectionGraph<HeldMapSource, HeldBook>>(actions, steps)), expected);
}

TEST_CASE("Sampling a dictionary holds key membership and child validity until the next trigger") {
  stdlib::register_standard_operators();
  CHECK_OUTPUT(eval_node<SampleHeldBook>(
      values<Int>(1, 2, none, 3, none), values<Int>(1, none, 2, none, 3), values<Int>(0, 1, 2, 3, 4)),
      values<Bool>(true, true, false, false, true));
}

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

struct PublishGrowingList {
  static void eval(In<"whole", TS<Bool>> whole, Out<TSL<TS<Int>>> out) {
    if (whole.value()) {
      auto empty = ListBuilder{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())}.build();
      apply_current_value(out.base(), empty.view());
    } else {
      out.resize(0);
    }
  }
};

struct GrowingListIdentity {
  static Port<TSL<TS<Int>>> compose(Wiring &, Port<TSL<TS<Int>>> items) { return items; }
};

TEST_CASE("whole and sparse zero-length growing list writes initialize once") {
  auto empty = dynamic_list_delta<TS<Int>>({});
  auto zero = dynamic_list_delta<TS<Int>>({}, {}, 0);
  CHECK_OUTPUT(eval_node<PublishGrowingList>(values<Bool>(true, false, true, true)),
               values<Value>(empty, none, none, none));
  // Raw C++ replay inputs are authored edits. They have no observed-event provenance.
  CHECK_OUTPUT(eval_node<GrowingListIdentity>(values<Value>(zero, empty, zero)),
               values<Value>(empty, none, none));
}

TEST_CASE("owned lists derive validity from live positions and a published zero-length growing extent") {
  using Growing = TSL<TS<Int>>;
  TSOutput growing{schema_descriptor<Growing>::ts_meta()};
  const auto zero = dynamic_list_delta<TS<Int>>({}, {}, 0);
  auto first = growing.view(MIN_ST);
  CHECK_FALSE(first.valid());
  apply_delta(first, zero.view());
  CHECK(first.valid());
  CHECK(first.all_valid());
  CHECK(first.modified());
  auto second = growing.view(MIN_ST + MIN_TD);
  apply_delta(second, zero.view());
  CHECK_FALSE(second.modified());

  const auto empty = ListBuilder{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())}.build();
  for (Int step : {2, 3}) {
    auto view = growing.view(MIN_ST + step * MIN_TD);
    apply_current_value(view, empty.view());
    CHECK(view.valid());
    CHECK(view.all_valid());
    CHECK_FALSE(view.modified());
    CHECK(view.last_modified_time() == MIN_ST);
    CHECK_FALSE(view.as_list().size_modified().has_value());
  }
  auto sized = growing.view(MIN_ST + 4 * MIN_TD);
  auto list = sized.as_list();
  list.resize(2);
  CHECK(list.size() == 2);
  CHECK_FALSE(sized.valid());
  CHECK_FALSE(sized.modified());
  CHECK(sized.last_modified_time() == MIN_DT);
  CHECK_FALSE(sized.value().has_value());
  CHECK_FALSE(sized.delta_value().has_value());
  CHECK(list.size_modified() == 2);
  CHECK_FALSE(sized.all_valid());
  CHECK_FALSE(list.at(0).valid());
  CHECK_FALSE(list.at(1).valid());

  auto check_clear = [](TSOutput &owner) {
    auto filled = owner.view(MIN_ST + 5 * MIN_TD);
    apply_delta(filled, list_delta<TS<Int>>({10, 20}).view());
    REQUIRE(filled.all_valid());
    auto cleared = owner.view(MIN_ST + 6 * MIN_TD);
    apply_delta(cleared, dynamic_list_delta<TS<Int>>({}, {0, 1}).view());
    auto positions = cleared.as_list();
    CHECK_FALSE(cleared.valid());
    CHECK_FALSE(cleared.modified());
    CHECK(cleared.last_modified_time() == MIN_DT);
    CHECK_FALSE(cleared.value().has_value());
    CHECK_FALSE(cleared.delta_value().has_value());
    CHECK_FALSE(cleared.all_valid());
    CHECK(positions.size() == 2);
    CHECK_FALSE(positions.at(0).valid());
    CHECK_FALSE(positions.at(1).valid());
    auto removed = positions.removed_positions();
    CHECK(std::vector<std::size_t>(removed.begin(), removed.end()) == std::vector<std::size_t>{0, 1});
  };
  check_clear(growing);
  TSOutput fixed{schema_descriptor<TSL<TS<Int>, 2>>::ts_meta()};
  check_clear(fixed);
  auto restored = fixed.view(MIN_ST + 7 * MIN_TD);
  apply_delta(restored, list_delta<TS<Int>>({{1, 4}}).view());
  CHECK(restored.valid());
  CHECK_FALSE(restored.all_valid());
  CHECK(restored.modified());
  CHECK(restored.last_modified_time() == restored.evaluation_time());

  auto shrunk = growing.view(MIN_ST + 7 * MIN_TD);
  apply_current_value(shrunk, empty.view());
  auto final = shrunk.as_list();
  CHECK(shrunk.valid());
  CHECK(shrunk.all_valid());
  CHECK(final.size_modified() == 0);
  auto removed = final.removed_indices();
  CHECK(std::vector<std::size_t>(removed.begin(), removed.end()) == std::vector<std::size_t>{0, 1});
}

TEST_CASE("owned list vacancy follows same-cycle child rewrites and checkpoint restores") {
  auto &registry = TypeRegistry::instance();
  const auto *scalar = schema_descriptor<TS<Int>>::ts_meta();
  for (const auto *schema : {registry.tsl(scalar, 8), registry.tsl(scalar)}) {
    TSOutput owner{schema};
    auto root = owner.view(MIN_ST);
    auto list = root.as_list();
    if (schema->is_unbounded_tsl()) { list.resize(8); }
    CHECK_FALSE(root.valid());
    auto tail = list.at(7);
    apply_delta(tail, Value{Int{7}}.view());
    CHECK(root.valid());
    REQUIRE(tail.begin_mutation(MIN_ST).invalidate());
    CHECK_FALSE(root.valid());
    CHECK_FALSE(root.modified());
    apply_delta(list.at(4), Value{Int{4}}.view());
    CHECK(root.valid());
    CHECK(root.modified());

    // Restoring into fresh storage is silent and builds validity from held
    // children, including a sparse slot away from the beginning of the list.
    TSOutput restored{schema};
    auto read = restored.view(MIN_ST + MIN_TD);
    CHECK_FALSE(read.valid());
    restore_ts_checkpoint(read.data_view(), capture_ts_checkpoint(root.data_view()));
    CHECK(read.valid());
    CHECK_FALSE(read.all_valid());
    CHECK_FALSE(read.modified());
    CHECK(read.last_modified_time() == MIN_ST);
    auto restored_list = read.as_list();
    REQUIRE(restored_list.at(4).begin_mutation(read.evaluation_time()).invalidate());
    CHECK_FALSE(read.valid());
    CHECK(read.last_modified_time() == MIN_DT);
  }
}

TEST_CASE("held collection observations preserve invalid child holes", "[held-child-validity]") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  SECTION("fixed list retains positions without retained invalid payloads") {
    using Samples = TSL<TS<Int>, 2>;
    TSOutput source{schema_descriptor<Samples>::ts_meta()};
    Value initial = list_delta<TS<Int>>({{0, 7}, {1, 100}});
    apply_delta(source.view(MIN_ST), initial.view());
    const auto next = MIN_ST + MIN_TD;
    auto output = source.view(next);
    auto samples = output.as_list();
    auto first = samples.at(0);
    REQUIRE(first.begin_mutation(next).invalidate());
    REQUIRE_FALSE(first.valid());
    REQUIRE(output.valid());
    const auto observed = output.value();
    REQUIRE(observed.as_indexed_view().size() == 2);
    REQUIRE_FALSE(observed.as_indexed_view().at(0).has_value());
    REQUIRE(observed.as_indexed_view().at(1).checked_as<Int>() == 100);
    Value retained{observed};
    check_held_hash_lookup(observed, retained.view());
    REQUIRE_FALSE(retained.view().as_indexed_view().at(0).has_value());
    REQUIRE(retained.view().as_indexed_view().at(1).checked_as<Int>() == 100);
    REQUIRE(retained.schema() == observed.schema());
    Value replacement{Int{9}};
    REQUIRE(first.begin_mutation(next + MIN_TD).copy_value_from(replacement.view()));
    REQUIRE_FALSE(retained.view().as_indexed_view().at(0).has_value());
  }
  SECTION("growing list retains live positions and their holes") {
    using Samples = TSL<TS<Int>>;
    TSOutput source{schema_descriptor<Samples>::ts_meta()};
    Value initial = dynamic_list_delta<TS<Int>>({{0, 7}, {1, 100}});
    apply_delta(source.view(MIN_ST), initial.view());
    const auto next = MIN_ST + MIN_TD;
    auto output = source.view(next);
    auto samples = output.as_list();
    auto first = samples.at(0);
    REQUIRE(first.begin_mutation(next).invalidate());
    REQUIRE(output.valid());
    const auto observed = output.value();
    REQUIRE(observed.as_list().size() == 2);
    REQUIRE_FALSE(observed.as_list().at(0).has_value());
    Value retained{observed};
    check_held_hash_lookup(observed, retained.view());
    REQUIRE(retained.schema() == observed.schema());
    REQUIRE_FALSE(retained.view().as_list().at(0).has_value());
    REQUIRE(retained.view().as_list().at(1).checked_as<Int>() == 100);
  }
  SECTION("map retains invalid live membership as a hole") {
    using Book = TSD<Int, TS<Int>>;
    TSOutput source{schema_descriptor<Book>::ts_meta()};
    Value initial = dict_delta<Int, TS<Int>>({{1, 7}, {2, 100}});
    apply_delta(source.view(MIN_ST), initial.view());
    const auto next = MIN_ST + MIN_TD;
    auto output = source.view(next);
    Value key{Int{1}};
    auto first = output.as_dict().at(key.view());
    REQUIRE(first.begin_mutation(next).invalidate());
    REQUIRE_FALSE(first.valid());
    REQUIRE(output.valid());
    const auto observed = output.value();
    REQUIRE(observed.as_map().contains(key.view()));
    REQUIRE_FALSE(observed.as_map().at(key.view()).has_value());
    Value retained{observed};
    check_held_hash_lookup(observed, retained.view());
    REQUIRE(retained.view().as_map().contains(key.view()));
    REQUIRE_FALSE(retained.view().as_map().at(key.view()).has_value());
    REQUIRE(retained.schema() == observed.schema());
    const auto capture_before = type_system_lock_count();
    const auto before = runtime_registry_snapshot();
    const auto capture_cost = before.type_system_lock_acquisitions - capture_before;
    for (int repeat = 0; repeat < 10; ++repeat) {
      Value repeated{observed};
      REQUIRE_FALSE(repeated.view().as_map().at(key.view()).has_value());
    }
    REQUIRE(type_system_lock_count() == before.type_system_lock_acquisitions);
    const auto after = runtime_registry_snapshot();
    // Registry snapshot capture itself acquires cold-path registry locks.
    REQUIRE(after.type_system_lock_acquisitions - before.type_system_lock_acquisitions == capture_cost);
    REQUIRE(after.type_records == before.type_records);
    Value absent{Int{3}};
    REQUIRE_THROWS_AS(observed.as_map().at(absent.view()), std::out_of_range);
    Value sibling{Int{2}};
    REQUIRE(retained.view().as_map().at(sibling.view()).checked_as<Int>() == 100);
    Value replacement{Int{9}};
    REQUIRE(first.begin_mutation(next + MIN_TD).copy_value_from(replacement.view()));
    REQUIRE_FALSE(retained.view().as_map().at(key.view()).has_value());
  }
}

TEST_CASE("held collection retention keeps nested fixed-list holes", "[held-child-validity]") {
  using Child = TSL<TS<Int>, 2>;
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const auto partial = list_delta<TS<Int>>({{0, 7}});
  const auto full = list_delta<TS<Int>>({{0, 100}, {1, 200}});
  SECTION("fixed list child") {
    using Outer = TSL<Child, 2>;
    TSOutput source{schema_descriptor<Outer>::ts_meta()};
    const auto initial = list_delta<Child>({{0, partial}, {1, full}});
    apply_delta(source.view(MIN_ST), initial.view());
    const auto observed = source.view(MIN_ST).value();
    Value snapshot{observed};
    check_held_hash_lookup(observed, snapshot.view());
    const auto nested = snapshot.view().as_list().at(0).as_list();
    REQUIRE(nested.at(0).checked_as<Int>() == 7);
    REQUIRE_FALSE(nested.at(1).has_value());
    REQUIRE(snapshot.view().as_list().at(1).as_list().at(1).checked_as<Int>() == 200);
    auto output = source.view(MIN_ST + MIN_TD);
    auto list = output.as_list();
    auto first = list.at(0);
    REQUIRE(first.begin_mutation(MIN_ST + MIN_TD).invalidate());
    REQUIRE_FALSE(output.value().as_list().at(0).has_value());
    REQUIRE(nested.at(0).checked_as<Int>() == 7);
    REQUIRE_FALSE(nested.at(1).has_value());
  }
  SECTION("growing list child") {
    using Outer = TSL<Child>;
    TSOutput source{schema_descriptor<Outer>::ts_meta()};
    const auto initial = dynamic_list_delta<Child>({{0, partial}, {1, full}});
    apply_delta(source.view(MIN_ST), initial.view());
    const auto observed = source.view(MIN_ST).value();
    Value snapshot{observed};
    check_held_hash_lookup(observed, snapshot.view());
    REQUIRE_FALSE(snapshot.view().as_list().at(0).as_list().at(1).has_value());
  }
  SECTION("dictionary member") {
    using Outer = TSD<Int, Child>;
    TSOutput source{schema_descriptor<Outer>::ts_meta()};
    const auto initial = dict_delta<Int, Child>({{1, partial}, {2, full}});
    apply_delta(source.view(MIN_ST), initial.view());
    const auto observed = source.view(MIN_ST).value();
    Value snapshot{observed};
    check_held_hash_lookup(observed, snapshot.view());
    Value key{Int{1}};
    const auto nested = snapshot.view().as_map().at(key.view()).as_list();
    REQUIRE(nested.at(0).checked_as<Int>() == 7);
    REQUIRE_FALSE(nested.at(1).has_value());
    auto output = source.view(MIN_ST + MIN_TD);
    auto first = output.as_dict().at(key.view());
    REQUIRE(first.begin_mutation(MIN_ST + MIN_TD).invalidate());
    REQUIRE(output.value().as_map().contains(key.view()));
    REQUIRE_FALSE(output.value().as_map().at(key.view()).has_value());
    REQUIRE(nested.at(0).checked_as<Int>() == 7);
    REQUIRE_FALSE(nested.at(1).has_value());
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
  const auto *structural_list = registry.tsl(registry.ts(integer));

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

TEST_CASE("full current-state reconciliation truncates a dynamic TSL to the source") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  using DynamicList = TSL<TS<Int>>;
  const auto *schema = schema_descriptor<DynamicList>::ts_meta();
  TSOutput source{schema};
  TSOutput target{schema};

  Value source_delta = dynamic_list_delta<TS<Int>>({{0, 1}});
  Value target_delta = dynamic_list_delta<TS<Int>>({{0, 10}, {1, 20}});
  apply_delta(source.view(MIN_ST), source_delta.view());
  apply_delta(target.view(MIN_ST), target_delta.view());

  // RFC 0031: a full reconciliation replaces the list, so a shorter source
  // truncates the target instead of being rejected.
  const DateTime next = MIN_ST + MIN_TD;
  reconcile_current_state(
      target.view(next), source.data_view(),
      TSCurrentReconcileOptions{TSCurrentReconcileScope::Full, false});

  auto target_view = target.view(next);
  auto reconciled = target_view.as_list();
  REQUIRE(reconciled.size() == 1);
  REQUIRE(reconciled.at(0).value().checked_as<Int>() == 1);
  REQUIRE(std::vector<std::size_t>(reconciled.removed_indices().begin(),
                                   reconciled.removed_indices().end()) ==
          std::vector<std::size_t>{1});
}

TEST_CASE("withdrawing the last fixed-list position preserves its structural edit without publishing an aggregate delta") {
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
  REQUIRE_FALSE(source_input.modified());
  REQUIRE_FALSE(source_input.delta_value().has_value());
  Value delta = capture_delta(source_input);
  REQUIRE(delta.view().as_bundle().at("modified").as_map().empty());
  REQUIRE(delta.view().as_bundle().at("removed").as_set().contains(Value{Int{0}}.view()));
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

TEST_CASE("ts_delta: an input bound to an owned layout captures the output's value") {
  // A recursive field is laid out behind an owner (TS[Owned[Node]]) while a
  // consumer declares the logical type (TS[Node]); a storage category is a
  // layout detail, so the binding is legal and the capture must not refuse it.
  auto &registry = TypeRegistry::instance();
  auto &value_factory = ValuePlanFactory::instance();
  auto &ts_factory = TSDataPlanFactory::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *node = registry.bundle("TsDeltaOwnedNode", {{"value", integer}});
  const auto *ts_node = registry.ts(node);
  const auto *ts_owned_node = registry.ts(registry.owned(node));

  TSOutput output{ts_factory.output_type_for(ts_owned_node)};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *ts_node, TSEndpointSchema::peered(ts_node))};
  input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));

  Value source{value_factory.type_for(registry.owned(node))};
  source.as_bundle().begin_mutation().at("value").checked_mutable_as<Int>() = 7;
  REQUIRE(output.view(MIN_ST).begin_mutation(MIN_ST).copy_value_from(source.view()));

  const Value captured = capture_delta(input.view(nullptr, MIN_ST));
  CHECK(captured.view().equals(source.view()));
  // The delta has the consumer's declared type, not the producer's storage:
  // a recorder typed from the input (List[Node]) accepts it.
  CHECK(captured.view().schema() == node);

  // An empty owner (a null allocation) has nothing to project: it crosses as
  // a typed null of the consumer's type.
  const Value empty{value_factory.type_for(registry.owned(node))};
  REQUIRE(output.view(MIN_ST + MIN_TD).begin_mutation(MIN_ST + MIN_TD).copy_value_from(empty.view()));
  const Value captured_empty = capture_delta(input.view(nullptr, MIN_ST + MIN_TD));
  CHECK(captured_empty.view().schema() == node);
}

TEST_CASE("ts_delta: atomic capture constructs an immutable canonical owner") {
  auto &registry = TypeRegistry::instance();
  auto &value_factory = ValuePlanFactory::instance();
  auto &ts_factory = TSDataPlanFactory::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *ts_integer = registry.ts(integer);
  const auto canonical = value_factory.type_for(integer);

  // Model a graph-local representation whose portable owner is immutable,
  // as with a realized Python-object hierarchy.
  auto *graph_ops = new ValueOps{canonical.ops_ref()};
  auto *owner_ops = new ValueOps{canonical.ops_ref()};
  owner_ops->allows_mutation = false;
  const auto graph_binding =
      intern_value_type(*integer, canonical.checked_plan(), *graph_ops);
  const auto owner_binding =
      intern_value_type(*integer, canonical.checked_plan(), *owner_ops);
  register_value_owning_type(graph_binding, owner_binding);

  TSOutput output{
      ts_factory.output_type_for(ts_integer, graph_binding)};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *ts_integer, TSEndpointSchema::peered(ts_integer))};
  input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));

  Value source{Int{42}};
  REQUIRE(output.view(MIN_ST)
              .begin_mutation(MIN_ST)
              .copy_value_from(source.view()));
  REQUIRE(input.view(nullptr, MIN_ST).value().binding() == graph_binding);
  REQUIRE_FALSE(owner_binding.ops_ref().can_begin_mutation());

  const Value captured = capture_delta(input.view(nullptr, MIN_ST));
  REQUIRE(captured.binding() == owner_binding);
  REQUIRE(*static_cast<const Int *>(captured.view().data()) == 42);
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
      dynamic_list_delta<TS<Int>>({{0, 1}}),
      dynamic_list_delta<TS<Int>>({{0, 5}, {1, 9}}),
      dynamic_list_delta<TS<Int>>({{1, 11}}),
  };

  auto rt = run_graph<RoundTripGraph<TSL<TS<Int>>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {dynamic_list_delta<TS<Int>>({{0, 1}}, {}, 1),
                dynamic_list_delta<TS<Int>>({{0, 5}, {1, 9}}, {}, 2),
                dynamic_list_delta<TS<Int>>({{1, 11}})});
}

TEST_CASE(
    "ts_delta: capture/apply round-trip a dynamic TSL truncation") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  // Grow to three, truncate to one (also modifying the survivor), then grow
  // back. A captured truncation must replay to the same length (RFC 0031).
  const std::vector<std::optional<Value>> deltas{
      dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}),
      dynamic_list_delta<TS<Int>>({{0, 7}}, {1, 2}, 1),
      dynamic_list_delta<TS<Int>>({{1, 8}}),
  };

  auto rt = run_graph<RoundTripGraph<TSL<TS<Int>>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}, {}, 3),
                dynamic_list_delta<TS<Int>>({{0, 7}}, {1, 2}, 1),
                dynamic_list_delta<TS<Int>>({{1, 8}}, {}, 2)});
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

TEST_CASE("capture_delta realizes an atomic Bundle owner for a derived source") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *text = registry.register_scalar<Str>("str");
  const auto *base = registry.bundle("tests.atomic.delta", "Instrument",
                                     {{"id", integer}}, {}, true);
  const auto *leaf = registry.bundle("tests.atomic.delta", "Future",
                                     {{"id", integer}, {"symbol", text}}, {base});
  const auto *base_ts = registry.ts(base);

  const auto snapshot = TypeRealizationSnapshot::capture(registry);
  TypeRealizationScope scope{snapshot.get()};
  TSOutput source{*base_ts};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *base_ts, TSEndpointSchema::peered(base_ts))};
  input.view(nullptr, MIN_ST).bind_output(source.view(MIN_ST));

  Value future{ValuePlanFactory::instance().type_for(leaf)};
  auto fields = future.as_bundle().begin_mutation();
  fields["id"].set(Int{7});
  fields["symbol"].set(Str{"EDZ6"});
  REQUIRE(source.view(MIN_ST).begin_mutation(MIN_ST).copy_value_from(future.view()));

  const Value captured = capture_delta(input.view(nullptr, MIN_ST));
  REQUIRE(captured.binding() == snapshot->type_for(base));
  const auto concrete = captured.view().concrete();
  REQUIRE(concrete.schema() == leaf);
  REQUIRE(concrete.as_bundle()["symbol"].checked_as<Str>() == "EDZ6");
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

TEST_CASE("ts_delta: partial TSB capture preserves an omitted collection field") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");

  const Value partial =
      tsb_delta<QuoteWithSet>(std::nullopt, Int{5});
  const auto authored = partial.view().as_bundle();
  REQUIRE_FALSE(authored.at(0).has_value());
  REQUIRE(authored.at(1).checked_as<Int>() == 5);

  const auto *schema = schema_descriptor<QuoteWithSet>::ts_meta();
  TSOutput output{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(
      *schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
  apply_delta(output.view(MIN_ST), partial.view());

  const Value captured = capture_delta(input.view(nullptr, MIN_ST));
  const auto delta = captured.view().as_bundle();
  REQUIRE_FALSE(delta.at(0).has_value());
  REQUIRE(delta.at(1).checked_as<Int>() == 5);

  const Value explicit_empty = tsb_delta<QuoteWithSet>(
      set_delta<Int>({}, {}), std::nullopt);
  REQUIRE(explicit_empty.view().as_bundle().at(0).has_value());
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

TEST_CASE("ts_delta: legacy window arrivals capture as canonical deltas") {
  const std::vector<std::optional<Value>> samples{Value{Int{1}}, Value{Int{2}}, Value{Int{3}}};
  auto rt = run_graph<RoundTripGraph<TSW<Int, 2, 1>>>(
      [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", samples); });
  CHECK_OUTPUT(get_recorded_deltas(rt.view().graph().global_state(), "out"),
               {window_delta<Int>(1), window_delta<Int>(2), window_delta<Int>(3, {{1, MIN_ST}})});
}

TEST_CASE("captured duration change and local expiry drop their common prefix once") {
  const auto *schema = schema_descriptor<TSWDuration<Int, 2, 0>>::ts_meta();
  TSOutput output{schema};
  const auto now = MIN_ST + 3 * MIN_TD;
  apply_delta(output.view(MIN_ST), window_delta<Int>(7).view());
  apply_delta(output.view(MIN_ST + MIN_TD), window_delta<Int>(8).view());
  auto data = output.data_view();
  auto window = data.as_window();
  REQUIRE(window.begin_mutation(now).expire());

  SECTION("the captured prefix includes the already expired sample") {
    const auto change = window_delta<Int>(9, {{7, MIN_ST}, {8, MIN_ST + MIN_TD}});
    apply_delta(output.view(now), change.view());
    CHECK(capture_window_delta(window, now).equals(change));
    REQUIRE(window.size() == 1);
    CHECK(window.at(0).checked_as<Int>() == 9);
  }
  SECTION("an arrival-only change retains the local removal") {
    apply_delta(output.view(now), window_delta<Int>(9).view());
    CHECK(capture_window_delta(window, now).equals(window_delta<Int>(9, {{7, MIN_ST}})));
    REQUIRE(window.size() == 2);
    CHECK(window.at(0).checked_as<Int>() == 8);
  }
}

TEST_CASE("an authoritative removal consumes the window's one replay write") {
  const auto *schema = schema_descriptor<TSWDuration<Int, 2, 0>>::ts_meta();
  TSOutput output{schema};
  apply_delta(output.view(MIN_ST), window_delta<Int>(7).view());
  const auto now = MIN_ST + MIN_TD;
  const auto removal = window_delta<Int>(std::nullopt, {{7, MIN_ST}});
  apply_delta(output.view(now), removal.view());
  CHECK_THROWS_AS(apply_delta(output.view(now), removal.view()), std::logic_error);
  auto data = output.data_view();
  auto window = data.as_window();
  ListBuilder empty{ValuePlanFactory::instance().type_for(schema->value_type)};
  CHECK_THROWS_AS(window.begin_mutation(now).replace_samples(empty.build().view(), {}), std::logic_error);
  CHECK(capture_window_delta(window, now).equals(removal));
}

TEST_CASE("duration window deadlines saturate at the runtime time limit") {
  const auto *schema = schema_descriptor<TSWDuration<Int, 2, 0>>::ts_meta();
  TSOutput output{schema};
  auto data = output.data_view();
  auto window = data.as_window();
  CHECK(window.next_expiry_time() == MAX_DT);
  const auto arrival = MAX_ET - 3 * MIN_TD;
  window.begin_mutation(arrival).push(Value{Int{7}}.view());
  CHECK(window.next_expiry_time() == MAX_ET);
  auto last = window.begin_mutation(MAX_ET);
  REQUIRE(last.expire());
  last.push(Value{Int{8}}.view());
  CHECK(window.next_expiry_time() == MAX_DT);
}

TEST_CASE("ts_delta: clear and mixed window edits survive codec and delayed apply") {
  const auto *schema = schema_descriptor<TSWDuration<Int, 2, 0>>::ts_meta();
  TSOutput source{schema}, target{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(source.view(MIN_ST));
  auto replay = [&](Int offset, const Value &expected) {
    const auto time = MIN_ST + offset * MIN_TD;
    const auto captured = capture_delta(input.view(nullptr, time));
    REQUIRE(captured.equals(expected));
    const auto decoded = from_binary_string(schema->delta_value_schema, to_binary_string(captured.view()));
    const auto later = time + 100 * MIN_TD;
    apply_delta(target.view(later), decoded.view());
    auto data = target.data_view();
    CHECK(capture_window_delta(data.as_window(), later).equals(expected));
  };
  auto data = source.data_view();
  auto window = data.as_window();
  window.begin_mutation(MIN_ST).push(Value{Int{10}}.view());
  replay(0, window_delta<Int>(10));
  window.begin_mutation(MIN_ST + MIN_TD).push(Value{Int{20}}.view());
  replay(1, window_delta<Int>(20));
  {
    auto mutate = window.begin_mutation(MIN_ST + 2 * MIN_TD);
    mutate.clear();
    mutate.push(Value{Int{30}}.view());
  }
  replay(2, window_delta<Int>(30, {{10, MIN_ST}, {20, MIN_ST + MIN_TD}}, true));
  window.begin_mutation(MIN_ST + 3 * MIN_TD).clear();
  replay(3, window_delta<Int>(std::nullopt, {{30, MIN_ST + 2 * MIN_TD}}, true));
  window.begin_mutation(MIN_ST + 4 * MIN_TD).clear();
  replay(4, window_delta<Int>(std::nullopt, {}, true));
}

TEST_CASE("ts_delta: whole-window snapshots retain repeated timestamps and empty replacement") {
  const auto *schema = schema_descriptor<TSWDuration<Int, 3, 0>>::ts_meta();
  TSOutput source{schema}, target{schema};
  auto source_data = source.data_view();
  auto window = source_data.as_window();
  const auto apply_snapshot = [&](const std::vector<Int> &samples, Int offset) {
    ListBuilder list{ValuePlanFactory::instance().type_for(schema->value_type)};
    for (const Int sample : samples) { list.push_back(sample); }
    const std::vector<DateTime> times(samples.size(), MIN_ST);
    const auto time = MIN_ST + offset * MIN_TD;
    window.begin_mutation(time).replace_samples(list.build().view(), times);
    const auto delta = capture_window_delta(window, time);
    const auto fields = delta.view().as_bundle();
    REQUIRE(fields.at(3).has_value());
    CHECK(fields.at(3).as_indexed_view().size() == samples.size());
    CHECK(fields.at(1).as_indexed_view().size() == 0);
    const auto decoded = from_binary_string(schema->delta_value_schema, to_binary_string(delta.view()));
    const auto later = time + 10 * MIN_TD;
    apply_delta(target.view(later), decoded.view());
    auto target_data = target.data_view();
    auto result = target_data.as_window();
    REQUIRE(result.size() == samples.size());
    for (std::size_t i = 0; i < samples.size(); ++i) {
      CHECK(result.at(i).checked_as<Int>() == samples[i]);
      CHECK(result.time_at(i) == MIN_ST);
    }
    CHECK(capture_window_delta(result, later).equals(delta));
  };
  apply_snapshot({8, 13}, 1);
  apply_snapshot({}, 2);
}

TEST_CASE("ts_delta: a window-shaped sample remains one authored arrival") {
  // A window can hold values whose type is itself a window change set.
  // The declared element schema, not the presence of fields, selects the interpretation.
  const auto sample = window_delta<Int>(7);
  const auto *schema = TypeRegistry::instance().tsw(sample.schema(), 2, 1);
  TSOutput target{schema};
  apply_delta(target.view(MIN_ST), sample.view());
  auto data = target.data_view();
  auto window = data.as_window();
  REQUIRE(window.size() == 1);
  CHECK(window.front().equals(sample.view()));
  const auto change = capture_window_delta(window, MIN_ST);
  const auto added = change.view().as_bundle().field("added").as_indexed_view();
  REQUIRE(added.size() == 1);
  CHECK(added.at(0).equals(sample.view()));
}

TEST_CASE("ts_delta: reconciling held window history keeps source times") {
  const auto *schema = schema_descriptor<TSW<Int, 2, 1>>::ts_meta();
  TSOutput source{schema}, target{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(source.view(MIN_ST));
  auto data = source.data_view();
  auto window = data.as_window();
  window.begin_mutation(MIN_ST).push(Value{Int{5}}.view());
  const auto sampled = MIN_ST + 5 * MIN_TD;
  reconcile_current_state(target.view(sampled), input.view(nullptr, sampled));
  auto output = target.data_view();
  auto result = output.as_window();
  REQUIRE(result.size() == 1);
  CHECK(result.time_at(0) == MIN_ST);

  const auto next = sampled + MIN_TD;
  auto edit = window.begin_mutation(next);
  edit.clear();
  edit.push(Value{Int{5}}.view());
  // Same scalar contents, different observation time.
  reconcile_current_state(target.view(next), data);
  CHECK(result.time_at(0) == next);
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

// ABI 16: every layout records the portable delta type it was built for, and
// the empty / captured deltas are built as that type without touching the
// realization snapshot (python_bridge.rst, "Delta capture is registry-free").
TEST_CASE("ts_delta: layouts record the canonical delta binding and empty deltas are built as it") {
  auto &registry = TypeRegistry::instance();
  auto &factory = TSDataPlanFactory::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *scalar = registry.ts(integer);
  const std::vector<const TSValueTypeMetaData *> schemas{
      scalar,
      registry.tss(integer),
      registry.tsd(integer, scalar),
      registry.tsl(scalar, 2),
      registry.tsl(scalar),
      registry.tsb("DeltaBindingProbe", {{"value", scalar}, {"keys", registry.tss(integer)}}),
  };
  for (const auto *schema : schemas) {
    CAPTURE(schema->name());
    const auto type = factory.data_type_for(schema).as_role();
    REQUIRE(type);
    const auto &ops = type.ops_ref();
    const auto *layout = ops.layout_impl(ops.context);
    REQUIRE(layout != nullptr);
    REQUIRE(layout->canonical_delta_binding);
    CHECK(layout->canonical_delta_binding.schema() == schema->delta_value_schema);

    // Warm once (the first call may publish), then the empty delta of a
    // realized type is a pure build over the layout: no type-system lock.
    static_cast<void>(ops.empty_delta_impl(type));
    const auto before = type_system_lock_count();
    const Value empty = ops.empty_delta_impl(type);
    CHECK(type_system_lock_count() == before);
    CHECK(empty.binding() == layout->canonical_delta_binding);
    if (schema->kind != TSTypeKind::TS) { CHECK(empty.has_value()); }
  }
}

// The capture path itself: a probe that counts the type-system locks taken
// inside capture_delta alone (the recording harness around it is not the
// per-tick path). After a warm run, replaying three set deltas must capture
// each one without a lock.
namespace {
std::uint64_t g_capture_locks = 0;

template <typename S> struct ProbeCaptureLocks {
  static constexpr auto name = "ts_delta_probe_capture_locks";
  static void eval(In<"ts", S> ts) {
    const auto before = type_system_lock_count();
    const Value delta = capture_delta(ts.base());
    static_cast<void>(delta);
    g_capture_locks += type_system_lock_count() - before;
  }
};

template <typename S> struct CaptureLocksGraph {
  static constexpr auto name = "ts_delta_capture_locks_graph";
  static void compose(Wiring &w) {
    auto src = wire<stdlib::replay_impl, S>(w, Str{"in"});
    wire<ProbeCaptureLocks<S>>(w, src);
  }
};
} // namespace

TEST_CASE("ts_delta: capture_delta acquires no type-system lock per tick") {
  (void)TypeRegistry::instance().register_scalar<Int>("int");
  const std::vector<std::optional<Value>> deltas{set_delta<Int>({1, 2}, {}),
                                                 set_delta<Int>({3}, {1}),
                                                 set_delta<Int>({}, {2, 3})};
  const auto run = [&]() {
    g_capture_locks = 0;
    auto ex = run_graph<CaptureLocksGraph<TSS<Int>>>(
        [&](const GlobalStateView &gs) { set_replay_deltas(gs, "in", deltas); });
    static_cast<void>(ex);
    return g_capture_locks;
  };
  static_cast<void>(run());  // warm: the first capture may publish once
  CHECK(run() == 0);
}

TEST_CASE("ts_delta: explicit empty sparse patches validate only their target", "[empty-delta]") {
  auto &registry = TypeRegistry::instance();
  const auto *integer = registry.register_scalar<Int>("int");
  const auto *scalar = registry.ts(integer);
  const std::vector<const TSValueTypeMetaData *> schemas{
      registry.tss(integer), registry.tsd(integer, scalar),
      registry.tsl(scalar, 2), registry.tsl(scalar),
      registry.tsb("EmptyDeltaUnit", {}),
      registry.tsb("EmptyDeltaChildren", {{"list", registry.tsl(scalar, 2)}, {"scalar", scalar}})};
  for (const auto *schema : schemas) {
    CAPTURE(schema->name());
    TSOutput output{schema};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    const auto type = TSDataPlanFactory::instance().data_type_for(schema).as_role();
    const auto empty = type.ops_ref().empty_delta_impl(type);
    REQUIRE(empty.has_value());
    CHECK_FALSE(output.view(MIN_ST).valid());
    const auto before = type_system_lock_count();
    apply_delta(output.view(MIN_ST), empty.view());
    CHECK(type_system_lock_count() == before);
    if (schema->kind == TSTypeKind::TSL && !schema->is_unbounded_tsl()) {
      CHECK_FALSE(output.view(MIN_ST).valid());
      CHECK_FALSE(output.view(MIN_ST).modified());
      CHECK(output.view(MIN_ST).last_modified_time() == MIN_DT);
      CHECK_FALSE(output.view(MIN_ST).delta_value().has_value());
      continue;
    }
    CHECK(output.view(MIN_ST).valid());
    CHECK(output.view(MIN_ST).modified());
    auto captured = capture_delta(input.view(nullptr, MIN_ST));
    CHECK((captured.view() == empty.view()));
    CHECK(delta_is_observable(input.view(nullptr, MIN_ST), captured.view()));
    apply_delta(output.view(MIN_ST), empty.view());
    CHECK(output.view(MIN_ST).modified());
    const auto later = MIN_ST + MIN_TD;
    apply_delta(output.view(later), empty.view());
    CHECK(output.view(later).valid());
    CHECK_FALSE(output.view(later).modified());
    CHECK(output.view(later).last_modified_time() == MIN_ST);
    auto later_view = output.view(later);
    if (schema->kind == TSTypeKind::TSB) {
      auto bundle = later_view.as_bundle();
      for (std::size_t index = 0; index < schema->field_count(); ++index) {
        CHECK_FALSE(bundle.at(index).valid());
      }
      CHECK(output.view(later).all_valid() == (schema->field_count() == 0));
    } else {
      CHECK(output.view(later).all_valid());
    }
    CHECK(output.view(later).data_view().begin_mutation(later).invalidate());
    const auto again = later + MIN_TD;
    apply_delta(output.view(again), empty.view());
    CHECK(output.view(again).valid());
    CHECK(output.view(again).modified());
    CHECK((capture_delta(input.view(nullptr, again)).view() == empty.view()));
  }
}

TEST_CASE("ts_delta: nested empty patches publish only effective children", "[empty-delta]") {
  using Child = TSL<TS<Int>>;
  using Bundle = TSB<"EmptyNestedBundle", Field<"child", Child>, Field<"other", TS<Int>>>;
  const Value empty = list_delta<TS<Int>>(std::initializer_list<std::pair<std::size_t, Int>>{});
  const auto verify = [&](const TSValueTypeMetaData *schema, const Value &patch) {
    TSOutput output{schema};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    apply_delta(output.view(MIN_ST), patch.view());
    CHECK(output.view(MIN_ST).valid());
    CHECK(output.view(MIN_ST).modified());
    CHECK((capture_delta(input.view(nullptr, MIN_ST)).view() == patch.view()));
    const auto later = MIN_ST + MIN_TD;
    apply_delta(output.view(later), patch.view());
    CHECK_FALSE(output.view(later).modified());
    CHECK(output.view(later).last_modified_time() == MIN_ST);
  };
  verify(schema_descriptor<Bundle>::ts_meta(), tsb_delta<Bundle>(Value{empty.view()}, std::nullopt));
  verify(schema_descriptor<TSD<Int, Child>>::ts_meta(), dict_delta<Int, Child>({{7, empty}}));
  verify(schema_descriptor<TSL<Child, 2>>::ts_meta(), list_delta<Child>({{0, empty}}));
  verify(schema_descriptor<TSL<Child>>::ts_meta(), dynamic_list_delta<Child>({{0, empty}}, {}, 1));
}

TEST_CASE("capturing list-valued dictionary membership distinguishes vacant, removed and reinserted keys") {
  using Child = TSL<TS<Int>>;
  const auto *schema = schema_descriptor<TSD<Int, Child>>::ts_meta();
  TSOutput source{schema}, replay{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(source.view(MIN_ST));
  const Value key{Int{7}};
  const auto holes = dynamic_list_delta<TS<Int>>({}, {}, 2);
  const auto remove_key = dict_delta<Int, Child>({}, {7});

  const auto transport = [&](DateTime time) {
    const auto delta = capture_delta(input.view(nullptr, time));
    apply_observed_delta(replay.view(time), delta.view());
    return delta;
  };
  apply_delta(source.view(MIN_ST), dict_delta<Int, Child>({{7, holes}}).view());
  transport(MIN_ST);
  auto first_root = replay.view(MIN_ST);
  auto first = first_root.as_dict();
  REQUIRE(first.contains(key.view()));
  REQUIRE_FALSE(first.at(key.view()).valid());

  const auto cancel = MIN_ST + MIN_TD;
  apply_delta(source.view(cancel), remove_key.view());
  apply_delta(source.view(cancel), dict_delta<Int, Child>({{7, holes}}).view());
  const auto retained = transport(cancel);
  CHECK(retained.view().as_bundle().at("removed").as_set().empty());
  auto retained_root = replay.view(cancel);
  auto retained_book = retained_root.as_dict();
  REQUIRE(retained_book.contains(key.view()));
  auto held = retained_book.at(key.view());
  CHECK_FALSE(held.valid());
  CHECK(held.as_list().size() == 2);

  const auto erase = cancel + MIN_TD;
  apply_delta(source.view(erase), remove_key.view());
  const auto removed = transport(erase);
  CHECK(removed.view().as_bundle().at("removed").as_set().contains(key.view()));
  auto removed_root = replay.view(erase);
  CHECK_FALSE(removed_root.as_dict().contains(key.view()));
  const auto later = erase + MIN_TD;
  apply_delta(source.view(later), remove_key.view());
  CHECK_FALSE(source.view(later).modified());

  const auto reinsert = later + MIN_TD;
  const auto valued = dynamic_list_delta<TS<Int>>({{0, 5}}, {}, 1);
  apply_delta(source.view(reinsert), dict_delta<Int, Child>({{7, valued}}).view());
  transport(reinsert);
  auto restored_root = replay.view(reinsert);
  auto fresh = restored_root.as_dict().at(key.view());
  REQUIRE(fresh.valid());
  auto positions = fresh.as_list();
  CHECK(positions.size() == 1);
  CHECK(positions.at(0).value().checked_as<Int>() == 5);
}

TEST_CASE("ts_delta: cancelled producer tick is applied by consumer validity", "[empty-delta]") {
  const auto *schema = schema_descriptor<TSS<Int>>::ts_meta();
  TSOutput producer{schema}, fresh{schema}, held{schema};
  TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
  input.view(nullptr, MIN_ST).bind_output(producer.view(MIN_ST));
  const auto first = set_delta<Int>({9}, {});
  apply_delta(held.view(MIN_ST), first.view());
  const auto now = MIN_ST + MIN_TD;
  {
    auto producer_view = producer.view(now);
    auto mutation = producer_view.as_set().begin_mutation(now);
    const Value member{Int{7}};
    CHECK(mutation.add(member.view()));
    CHECK(mutation.remove(member.view()));
  }
  REQUIRE(producer.view(now).modified());
  const auto cancelled = capture_delta(input.view(nullptr, now));
  const auto empty = set_delta<Int>({}, {});
  CHECK((cancelled.view() == empty.view()));
  apply_delta(fresh.view(now), cancelled.view());
  CHECK(fresh.view(now).valid());
  CHECK(fresh.view(now).modified());
  apply_delta(held.view(now), cancelled.view());
  CHECK_FALSE(held.view(now).modified());
  CHECK(held.view(now).last_modified_time() == MIN_ST);
  auto held_view = held.view(now);
  CHECK(held_view.as_set().contains(Value{Int{9}}.view()));
}

TEST_CASE("ts_delta: empty publication markers preserve same-cycle ticks and list invalidations", "[empty-delta]") {
  const auto verify = [&](const TSValueTypeMetaData *schema, const Value &patch) {
    TSOutput output{schema};
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    input.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    const auto type = TSDataPlanFactory::instance().data_type_for(schema).as_role();
    const auto empty = type.ops_ref().empty_delta_impl(type);
    apply_delta(output.view(MIN_ST), empty.view());
    apply_delta(output.view(MIN_ST), patch.view());
    const auto invalidate_child = [&](DateTime time) {
      auto view = output.view(time);
      auto child = view.data_view().indexed_child_at(0);
      CHECK(child.begin_mutation(time).invalidate());
    };
    invalidate_child(MIN_ST);
    auto delta = capture_delta(input.view(nullptr, MIN_ST));
    CHECK((delta.view() == (schema->kind == TSTypeKind::TSL ? list_delta<TS<Int>>({}, {0}) : empty).view()));
    CHECK(delta_is_observable(input.view(nullptr, MIN_ST), delta.view()) == (schema->kind == TSTypeKind::TSB));
    const auto later = MIN_ST + MIN_TD;
    apply_delta(output.view(later), patch.view());
    invalidate_child(later);
    delta = capture_delta(input.view(nullptr, later));
    CHECK((delta.view() == (schema->kind == TSTypeKind::TSL ? list_delta<TS<Int>>({}, {0}) : empty).view()));
    CHECK_FALSE(delta_is_observable(input.view(nullptr, later), delta.view()));
    apply_delta(output.view(later), empty.view());
    CHECK_FALSE(delta_is_observable(input.view(nullptr, later), delta.view()));
  };
  verify(schema_descriptor<TSL<TS<Int>, 1>>::ts_meta(), list_delta<TS<Int>>({{0, 1}}));
  verify(schema_descriptor<Quote>::ts_meta(), tsb_delta<Quote>(Int{1}, std::nullopt));
}

TEST_CASE("ts_delta: sampled rebinds retain previously empty fixed and bundle publications", "[empty-delta][sampled]") {
  auto &registry = TypeRegistry::instance();
  const auto *scalar = schema_descriptor<TS<Int>>::ts_meta();
  const std::vector<const TSValueTypeMetaData *> schemas{
      registry.tsl(scalar), registry.tsl(scalar, 2),
      registry.tsb("SampledEmptyUnit", {}),
      registry.tsb("SampledEmptyFields", {{"value", scalar}})};
  for (const auto *schema : schemas) {
    CAPTURE(schema->name());
    const auto type = TSDataPlanFactory::instance().data_type_for(schema).as_role();
    const auto empty = type.ops_ref().empty_delta_impl(type);
    TSOutput first{schema}, second{schema}, invalid{schema};
    apply_delta(first.view(MIN_ST), empty.view());
    apply_delta(second.view(MIN_ST), empty.view());
    TSInput input{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    const auto sample_time = MIN_ST + MIN_TD;
    input.view(nullptr, sample_time).bind_output(first.view(sample_time));
    CHECK_FALSE(input.view(nullptr, sample_time).modified());
    CHECK_FALSE(input.view(nullptr, MIN_DT).delta_is_sampled_rebind());
    input.view(nullptr, sample_time).bind_output_sampled(second.view(sample_time), sample_time);
    auto sampled = input.view(nullptr, sample_time);
    if (schema->kind == TSTypeKind::TSL && !schema->is_unbounded_tsl()) {
      CHECK_FALSE(sampled.valid());
      CHECK_FALSE(sampled.modified());
      CHECK(sampled.last_modified_time() == MIN_DT);
      continue;
    }
    REQUIRE(sampled.valid());
    REQUIRE(sampled.modified());
    CHECK(sampled.last_modified_time() == sample_time);
    CHECK(second.view(sample_time).last_modified_time() == MIN_ST);
    const auto captured = capture_delta(sampled);
    CHECK((captured.view() == empty.view()));
    CHECK(delta_is_observable(sampled, captured.view()));
    const auto later = sample_time + MIN_TD;
    CHECK_FALSE(input.view(nullptr, later).modified());
    CHECK_FALSE(delta_is_observable(input.view(nullptr, later), captured.view()));
    input.view(nullptr, later).bind_output_sampled(invalid.view(later), later);
    CHECK_FALSE(input.view(nullptr, later).valid());
    CHECK_FALSE(delta_is_observable(input.view(nullptr, later), captured.view()));
  }
}

TEST_CASE("ts_delta: sampling an invalidated child notification publishes the valid empty parent", "[empty-delta][sampled]") {
  const auto verify = [&](const TSValueTypeMetaData *schema, const Value &patch) {
    TSOutput output{schema};
    TSInput ordinary{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    TSInput sample{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    ordinary.view(nullptr, MIN_ST).bind_output(output.view(MIN_ST));
    apply_delta(output.view(MIN_ST), patch.view());
    const auto now = MIN_ST + MIN_TD;
    auto producer = output.view(now);
    auto child = producer.data_view().indexed_child_at(0);
    REQUIRE(child.begin_mutation(now).invalidate());
    if (schema->kind == TSTypeKind::TSL) {
      CHECK_FALSE(producer.valid());
      CHECK_FALSE(producer.modified());
      sample.view(nullptr, now).bind_output_sampled(producer, now);
      const auto sampled = sample.view(nullptr, now);
      CHECK_FALSE(sampled.valid());
      CHECK_FALSE(sampled.modified());
      CHECK(sampled.last_modified_time() == MIN_DT);
      return;
    }
    REQUIRE(producer.valid());
    REQUIRE(producer.modified());
    auto direct = ordinary.view(nullptr, now);
    const auto empty = capture_delta(direct);
    CHECK(delta_is_observable(direct, empty.view()) == (schema->kind == TSTypeKind::TSL));
    sample.view(nullptr, now).bind_output_sampled(producer, now);
    auto sampled = sample.view(nullptr, now);
    REQUIRE(sampled.valid());
    REQUIRE(sampled.modified());
    REQUIRE(sampled.last_modified_time() == producer.last_modified_time());
    CHECK(sampled.delta_is_sampled_rebind());
    const auto captured = capture_delta(sampled);
    CHECK((captured.view() == empty.view()));
    CHECK(delta_is_observable(sampled, captured.view()));
    CHECK(delta_is_observable(direct, empty.view()) == (schema->kind == TSTypeKind::TSL));
    apply_delta(output.view(now), patch.view());
    REQUIRE(child.begin_mutation(now).invalidate());
    const auto after_update = capture_delta(sampled);
    CHECK((after_update.view() == empty.view()));
    CHECK(delta_is_observable(sampled, after_update.view()));
    CHECK(delta_is_observable(direct, empty.view()) == (schema->kind == TSTypeKind::TSL));
    CHECK_FALSE(delta_is_observable(sample.view(nullptr, now + MIN_TD), captured.view()));
  };
  verify(schema_descriptor<TSL<TS<Int>, 1>>::ts_meta(), list_delta<TS<Int>>({{0, 1}}));
  verify(schema_descriptor<Quote>::ts_meta(), tsb_delta<Quote>(Int{1}, std::nullopt));
}

TEST_CASE("TSL positional deltas survive capture and replay independently of length") {
  using namespace hgraph;
  for (bool growing : {false, true}) {
    CAPTURE(growing);
    const auto *schema = growing ? schema_descriptor<TSL<TS<Int>>>::ts_meta()
                                 : schema_descriptor<TSL<TS<Int>, 3>>::ts_meta();
    TSOutput original{schema}, replay{schema};
    TSInput observer{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    TSInput replay_observer{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    observer.view(nullptr, MIN_ST).bind_output(original.view(MIN_ST));
    replay_observer.view(nullptr, MIN_ST).bind_output(replay.view(MIN_ST));
    std::vector<Value> recording;
    auto now = MIN_ST;
    auto tick = [&](const Value &authored, const Value &expected) {
      apply_delta(original.view(now), authored.view());
      const auto in = observer.view(nullptr,now);
      REQUIRE(in.modified());
      recording.push_back(capture_delta(in));
      CHECK(recording.back().equals(expected));
      CHECK(Value{original.view(now).delta_value()}.equals(expected));
      now += MIN_TD;
    };
    const auto initial = list_delta<TS<Int>>({{0,10},{1,20},{2,30}});
    tick(initial, growing ? list_delta<TS<Int>>({{0,10},{1,20},{2,30}}, {}, 3) : initial);
    const auto clear = list_delta<TS<Int>>({}, {1});
    tick(clear,clear);
    auto held_output = original.view(now);
    auto held = held_output.as_list();
    CHECK(held.size() == 3);
    CHECK_FALSE(held.at(1).valid());
    CHECK(held.at(2).value().checked_as<Int>() == 30);
    if (growing) {
      tick(list_delta<TS<Int>>({}, {}, 1), list_delta<TS<Int>>({}, {1,2}, 1));
      auto grow = list_delta<TS<Int>>({}, {}, 3);
      tick(grow,grow);
      const auto update = list_delta<TS<Int>>({{2,31}});
      tick(update,update);
      tick(list_delta<TS<Int>>({}, {}, 0), list_delta<TS<Int>>({}, {0,1,2}, 0));
    }
    now = MIN_ST;
    for (const auto &captured : recording) {
      apply_delta(replay.view(now), captured.view());
      REQUIRE(capture_delta(replay_observer.view(nullptr,now)).equals(captured));
      now += MIN_TD;
    }
  }
}

TEST_CASE("TSL replay reads the positional maps and two-field deltas from earlier recordings") {
  const auto *schema = schema_descriptor<TSL<TS<Int>>>::ts_meta();
  TSOutput output{schema};
  auto first = list_delta<TS<Int>>({{0,10},{1,20},{2,30}});
  apply_delta(output.view(MIN_ST),first.view().as_bundle().at("modified"));
  const auto clear = list_delta<TS<Int>>({}, {1,2});
  const auto parts = clear.view().as_bundle();
  const auto *old_type = TypeRegistry::instance().un_named_bundle({
      {"removed",parts.at("removed").schema()}, {"modified",parts.at("modified").schema()}});
  BundleBuilder old{ValuePlanFactory::instance().type_for(old_type)};
  old.set("removed",parts.at("removed"));
  old.set("modified",parts.at("modified"));
  const auto retained = old.build();
  auto later = output.view(MIN_ST + MIN_TD);
  apply_delta(later,retained.view());
  CHECK(later.as_list().size_modified() == 1);
  auto after = later.as_list();
  CHECK(after.at(0).value().checked_as<Int>() == 10);
}

namespace {
struct WholeSetPublisher {
  static constexpr auto name = "whole_set_publisher";
  static void eval(In<"step", TS<Int>> step, Out<TSS<Int>> out) {
    if (step.value() == 2) {
      apply_delta(out.base(), set_delta<Int>({1}, {}).view());
      return;
    }
    SetBuilder whole{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
    if (step.value() < 2) { whole.insert(Int{1}); }
    apply_current_value(out.base(), whole.build().view());
  }
};

struct CountChangedMembers {
  static constexpr auto name = "count_changed_members";
  static void eval(In<"members", TSS<Int>> members, Out<TS<Int>> out) {
    out.set(static_cast<Int>(members.added().size() + members.removed().size()));
  }
};

struct SetPublicationGraph {
  static constexpr auto name = "set_publication_graph";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step) {
    return wire<CountChangedMembers>(w, wire<WholeSetPublisher>(w, step));
  }
};

using PublicationBook = TSD<Int, Quote>;
using PublicationShelf = TSB<"PublicationShelf", Field<"tag", TS<Int>>, Field<"book", PublicationBook>,
                             Field<"members", TSS<Int>>>;

struct ReadShelf {
  static constexpr auto name = "read_publication_shelf";
  static void eval(In<"shelf", PublicationShelf> shelf, Out<TS<Int>> out) {
    auto book = shelf.field<"book">();
    REQUIRE(shelf.field<"tag">().value() == 9);
    REQUIRE(shelf.field<"members">().size() == 1);
    for (const auto &[key, quote] : book.items()) { REQUIRE(quote.valid()); }
    out.set(static_cast<Int>(book.size()));
  }
};

using OwnedCollections = TSB<"OwnedPublicationCollections",
    Field<"book", TSD<Int, TS<Int>>>, Field<"members", TSS<Int>>, Field<"readings", TSL<TS<Int>>>>;

struct WriteCollections {
  static constexpr auto name = "write_publication_collections";
  static void eval(In<"step", TS<Int>> step, Out<OwnedCollections> out) {
    auto book = out.field<"book">();
    auto members = out.field<"members">();
    auto readings = out.field<"readings">();
    if (step.value() == 1 || step.value() == 3) {
      auto now = out.evaluation_time();
      REQUIRE(book.base().begin_mutation(now).invalidate());
      REQUIRE(members.base().begin_mutation(now).invalidate());
      REQUIRE(readings.base().begin_mutation(now).invalidate());
      if (step.value() == 1) { return; }
    }
    book.set(step.value(), Int{5});
    members.add(step.value());
    readings.set(0, Int{5});
    if (step.value() == 0) { readings.set(1, Int{6}); }
  }
};

struct ReadCollections {
  static constexpr auto name = "read_publication_collections";
  static void eval(In<"clock", TS<Int>> clock,
                   In<"collections", OwnedCollections, InputActivity::Passive, InputValidity::Unchecked> collections,
                   Out<TS<Int>> out) {
    auto book = collections.field<"book">();
    auto members = collections.field<"members">();
    auto readings = collections.field<"readings">();
    if (clock.value() == 1) {
      for (const auto *endpoint : {&book.base(), &members.base(), &readings.base()}) {
        CHECK_FALSE(endpoint->valid());
        CHECK_FALSE(endpoint->modified());
        CHECK(endpoint->last_modified_time() == MIN_DT);
      }
    } else {
      CHECK(book.contains(clock.value()));
      CHECK(members.contains(clock.value()));
      CHECK(readings[0].value() == 5);
    }
    out.set(static_cast<Int>(book.size() + members.size() + readings.size()));
  }
};

struct CollectionInvalidationGraph {
  static constexpr auto name = "collection_invalidation_graph";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step) {
    return wire<ReadCollections>(w, step, wire<WriteCollections>(w, step));
  }
};
} // namespace

TEST_CASE("collection publication: whole sets tick only on initialization and membership changes") {
  CHECK_OUTPUT(eval_node<SetPublicationGraph>(values<Int>(0, 1, 2, 3, 4)), values<Int>(1, none, none, 1, none));
}

TEST_CASE("collection publication: whole set and dictionary assignments preserve first and changed writes") {
  const auto integer = ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
  const Int key = 1, price = 7;
  SetBuilder members{integer};
  members.insert(key);
  MapBuilder entries{integer, integer};
  entries.set_item_copy(&key, &price);
  const auto populated_set = members.build(), populated_dict = entries.build();
  const auto check = [&](const TSValueTypeMetaData *schema, const Value &populated, bool equal_child_ticks) {
    const Value empty{ValuePlanFactory::instance().type_for(schema->value_schema)};
    enum class Write { current, copy, move };
    for (auto mode : {Write::current, Write::copy, Write::move}) {
      TSOutput owner{schema};
      DateTime last = MIN_DT;
      for (Int step = 0; step < 6; ++step) {
        const auto now = MIN_ST + step * MIN_TD;
        auto out = owner.view(now);
        Value value{step == 2 || step == 3 ? populated : empty};
        const bool expected = step == 0 || step == 2 || (step == 3 && equal_child_ticks) || step == 4;
        CAPTURE(schema->kind, static_cast<int>(mode), step);
        if (mode == Write::current) {
          apply_current_value(out, value.view());
        } else if (mode == Write::copy) {
          CHECK(out.begin_mutation(now).copy_value_from(value.view()) == expected);
        } else if (schema->kind == TSTypeKind::TSD) {
          auto mutation = out.data_view().as_dict().begin_mutation(now);
          CHECK(mutation.move_value_from(std::move(value)) == expected);
        } else {
          CHECK(out.begin_mutation(now).move_value_from(std::move(value)) == expected);
        }
        if (expected) { last = now; }
        CHECK(out.valid());
        CHECK(out.modified() == expected);
        CHECK(out.last_modified_time() == last);
      }
    }
  };
  check(schema_descriptor<TSS<Int>>::ts_meta(), populated_set, false);
  check(schema_descriptor<TSD<Int, TS<Int>>>::ts_meta(), populated_dict, true);
}

TEST_CASE("collection publication: sparse bundle fields update only effective dictionary children") {
  const auto empty_quote = [] { return tsb_delta<Quote>(std::nullopt, std::nullopt); };
  const auto new_quote = [&] { return dict_delta<Int, Quote>({{1, empty_quote()}}); };
  const auto remove_first = [] { return dict_delta<Int, Quote>({}, {1}); };
  const auto no_membership_change = [] { return set_delta<Int>({}, {}); };

  for (const bool named : {false, true}) {
    CAPTURE(named);
    const auto book_patch = [&](Value child) {
      if (!named) { return tsb_delta<PublicationShelf>(std::nullopt, std::move(child), std::nullopt); }
      BundleBuilder patch{ValuePlanFactory::instance().type_for(schema_descriptor<PublicationShelf>::ts_meta()->delta_value_schema)};
      patch.set("book", std::move(child));
      return patch.build();
    };
    const auto first = tsb_delta<PublicationShelf>(Int{9}, new_quote(), set_delta<Int>({4}, {}));
    const auto empty_set_patch = tsb_delta<PublicationShelf>(std::nullopt, std::nullopt, no_membership_change());
    CHECK_OUTPUT(eval_node<ReadShelf>(values<Value>(first, book_patch(new_quote()), empty_set_patch,
                    book_patch(dict_delta<Int, Quote>({{2, empty_quote()}})), book_patch(remove_first()), book_patch(remove_first()))),
                 values<Int>(1, none, none, 2, 1, none));
  }
}

TEST_CASE("collection publication: invalidation discards owned keys members and growing positions") {
  CHECK_OUTPUT(eval_node<CollectionInvalidationGraph>(values<Int>(0, 1, 2, 3)), values<Int>(4, 0, 3, 3));
}

TEST_CASE("collection publication: whole invalidation preserves removal-cycle child references") {
  const auto exercise = []<typename S>() {
    const auto *schema = schema_descriptor<S>::ts_meta();
    TSOutput output{schema};
    TSInput observer{TSInputBuilderFactory::checked_builder_for(*schema, TSEndpointSchema::peered(schema))};
    const auto start = MIN_ST;
    observer.view(nullptr, start).bind_output(output.view(start));
    const auto now = start + MIN_TD;
    const auto later = now + MIN_TD;
    const auto write = [&](DateTime at, Int key, Int value) {
      if constexpr (std::is_same_v<S, HeldBook>) {
        apply_delta(output.view(at), dict_delta<Int, TS<Int>>({{key, value}}).view());
      } else {
        apply_delta(output.view(at), dynamic_list_delta<TS<Int>>({{static_cast<std::size_t>(key), value}}).view());
      }
    };
    const auto child = [&](DateTime at, Int key) {
      auto parent = output.view(at);
      if constexpr (std::is_same_v<S, HeldBook>) {
        auto dict = parent.as_dict();
        return dict.at(Value{key}.view());
      } else {
        auto list = parent.as_list();
        return list.at(static_cast<std::size_t>(key));
      }
    };
    write(start, 0, 10);
    write(start, 1, 20);
    auto first = child(now, 0);
    auto second = child(now, 1);
    const TimeSeriesReference first_ref{first}, second_ref{second};

    if constexpr (std::is_same_v<S, HeldBook>) {
      apply_delta(output.view(now), dict_delta<Int, TS<Int>>({}, {1}).view());
    } else {
      auto parent = output.view(now);
      parent.as_list().resize(1);
    }
    REQUIRE(output.view(now).begin_mutation(now).invalidate());
    CHECK_FALSE(first_ref.is_valid(now));
    REQUIRE(second_ref.is_valid(now));
    CHECK(second.value().template checked_as<Int>() == 20);
    CHECK(second.last_modified_time() == start);

    write(now, 0, 30);
    const auto delta = capture_delta(observer.view(nullptr, now));
    if constexpr (std::is_same_v<S, HeldBook>) {
      CHECK((delta.view() == dict_delta<Int, TS<Int>>({{0, 30}}, {1}).view()));
    } else {
      CHECK((delta.view() == dynamic_list_delta<TS<Int>>({{0, 30}}, {1}, 1).view()));
    }
    REQUIRE(first_ref.is_valid(now));
    CHECK(first.value().template checked_as<Int>() == 30);
    CHECK(second_ref.is_valid(now));
    CHECK_FALSE(second_ref.is_valid(later));
    write(later, 1, 40);
    CHECK_FALSE(second_ref.is_valid(later));
    CHECK(child(later, 1).value().template checked_as<Int>() == 40);
  };
  SECTION("dictionary") { exercise.template operator()<HeldBook>(); }
  SECTION("growing list") { exercise.template operator()<TSL<TS<Int>>>(); }
}

namespace {
using RetainedBook = TSD<Int, Quote>;
using RetainedRows = TSL<Quote>;

template <typename S> struct WriteRetainedQuote {
  static constexpr auto name = "write_retained_quote";
  static void eval(In<"step", TS<Int>> step, Out<S> out) {
    const auto now = out.evaluation_time();
    const auto write_quote = [&](Int index, Value quote) {
      if constexpr (std::is_same_v<S, RetainedBook>) {
        apply_delta(out.base(), dict_delta<Int, Quote>({{index, std::move(quote)}}).view());
      } else {
        apply_delta(out.base(), dynamic_list_delta<Quote>({{static_cast<std::size_t>(index), std::move(quote)}}).view());
      }
    };
    const auto remove_second = [&] {
      if constexpr (std::is_same_v<S, RetainedBook>) {
        apply_delta(out.base(), dict_delta<Int, Quote>({}, {1}).view());
      } else {
        out.resize(1);
      }
    };
    if (step.value() == 0) {
      write_quote(0, tsb_delta<Quote>(Int{10}, std::nullopt));
      write_quote(1, tsb_delta<Quote>(Int{20}, std::nullopt));
      return;
    }
    remove_second();
    auto removed = out.removed_items();
    auto entry = *removed.begin();
    auto retained = entry.second.as_bundle();
    auto bid = retained.at("bid");
    REQUIRE(bid.valid());
    const auto saved_price = bid.value().template checked_as<Int>();
    const auto saved_time = bid.last_modified_time();

    REQUIRE(out.base().begin_mutation(now).invalidate());
    CHECK_FALSE(out.base().begin_mutation(now).invalidate());
    CHECK_FALSE(out.valid());
    CHECK_FALSE(out.modified());
    CHECK(bid.value().template checked_as<Int>() == saved_price);
    CHECK(bid.last_modified_time() == saved_time);

    write_quote(0, tsb_delta<Quote>(std::nullopt, Int{7}));
    if constexpr (std::is_same_v<S, RetainedBook>) {
      CHECK_FALSE(out[1].valid());
      auto removed_values = out.base().delta_value().as_bundle().at("removed").as_set();
      CHECK(removed_values.contains(Value{Int{1}}.view()));
    }
    write_quote(1, tsb_delta<Quote>(std::nullopt, std::nullopt));
    REQUIRE(out[1].valid());
    CHECK_FALSE(out[1].template field<"bid">().valid());
    if (step.value() == 1) {
      write_quote(1, tsb_delta<Quote>(Int{30}, std::nullopt));
      remove_second();
      write_quote(1, tsb_delta<Quote>(std::nullopt, std::nullopt));
    }
  }
};

template <typename S> struct ReadRetainedQuote {
  static constexpr auto name = "read_retained_quote";
  static void eval(In<"quotes", S> quotes, Out<TS<Int>> out) {
    auto bid = quotes[1].template field<"bid">();
    out.set(bid.valid() ? bid.value() : Int{-1});
  }
};

template <typename S> struct RetainedQuoteGraph {
  static constexpr auto name = "retained_quote_graph";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step) {
    return wire<ReadRetainedQuote<S>>(w, wire<WriteRetainedQuote<S>>(w, step));
  }
};
} // namespace

TEST_CASE("collection publication: whole-owner invalidation resets previously removed children only upon revival") {
  const auto steps = values<Int>(0, 1, 2);
  CHECK_OUTPUT(eval_node<RetainedQuoteGraph<RetainedBook>>(steps), values<Int>(20, 30, -1));
  CHECK_OUTPUT(eval_node<RetainedQuoteGraph<RetainedRows>>(steps), values<Int>(20, 30, -1));
}

namespace {
template <typename S> struct WriteOwnedWindows {
  static constexpr auto name = "write_recreated_windows";
  static void eval(In<"step", TS<Int>> step, Out<S> out) {
    const auto now = out.evaluation_time();
    if (step.value() == 0) {
      out[0].push(Int{1});
    } else if (step.value() == 1) {
      if constexpr (std::is_same_v<S, TSL<TSWDuration<Int, 4, 0>>>) {
        out.resize(1);
      } else {
        static_cast<void>(out.erase(Int{1}));
      }
      auto removed = out.removed_items();
      auto entry = *removed.begin();
      auto retained = entry.second.as_window();
      REQUIRE(out.base().begin_mutation(now).invalidate());
      CHECK(retained.at(0).template checked_as<Int>() == 20);
      CHECK(retained.time_at(0) == now - MIN_TD);
    } else {
      REQUIRE(out.base().begin_mutation(now).invalidate());
    }
    out[1].push(Int{20} + step.value());
  }
};

template <typename S> struct ReadOwnedWindow {
  static constexpr auto name = "read_recreated_window";
  static void eval(In<"items", S> items, Out<TS<Int>> out) {
    auto window = items[1];
    if (window.empty()) { out.set(Int{-1}); return; }
    REQUIRE(window.size() == 1);
    CHECK(window.time_at(0) == items.evaluation_time());
    out.set(window.at(0));
  }
};

template <typename S> struct RecreatedWindowGraph {
  static constexpr auto name = "recreated_window_graph";
  static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step) {
    return wire<ReadOwnedWindow<S>>(w, wire<WriteOwnedWindows<S>>(w, step));
  }
};
} // namespace

TEST_CASE("collection publication: recreated count and duration window children start with fresh history") {
  using CountBook = TSD<Int, TSW<Int, 4, 0>>;
  using DurationRows = TSL<TSWDuration<Int, 4, 0>>;
  const auto steps = values<Int>(0, 1, 2, none, none, none, none, none);
  CHECK_OUTPUT(eval_node<RecreatedWindowGraph<CountBook>>(steps), values<Int>(20, 21, 22, none, none, none, none, none));
  CHECK_OUTPUT(eval_node<RecreatedWindowGraph<DurationRows>>(steps), values<Int>(20, 21, 22, none, none, none, none, -1));
}
