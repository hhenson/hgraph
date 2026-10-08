#include <runtime.h>
#include <hgl/execution_error.h>
#include <sources.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/util/scope.h>
#include <spdlog/sinks/ostream_sink.h>
#include <sstream>

#include "wiring/backend.h"

#include <hgraph/lib/std/component.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace hgraph;
using namespace hgraph::testing;
namespace runtime = hgl::codegen::runtime;

namespace
{
    void session()
    {
        hgl::wiring::ensure_session();
        runtime::register_operators();
    }
}  // namespace

TEST_CASE("direct compiled scalar signatures prepare aggregate locals and helpers", "[codegen][runtime][prepared-locals]") {
    // Direct node wiring has no generated provider installer to prepare plans.
    CHECK_OUTPUT(eval_node<runtime::scalar_tuple_local>(values<Int>(7, 8)), values<Int>(7, 8));
    CHECK_OUTPUT(eval_node<runtime::scalar_list_local>(values<Int>(7, 8)), values<Int>(1, 1));
    CHECK_OUTPUT(eval_node<runtime::scalar_list_helper>(values<Int>(7, 8)), values<Int>(1, 1));
}

TEST_CASE("required unset scalar reads preserve their code and run normal stop cleanup", "[codegen][runtime][unset-read]") {
    session();
    using Number = runtime::UnsetObservedNumber::time_series;
    using Flag = runtime::UnsetObservedFlag::time_series;
    using Samples = runtime::UnsetObservedSamples::time_series;
    using Mapping = runtime::UnsetObservedMapping::time_series;
    using Known = runtime::UnsetKnownOuter::time_series;
    using KnownInner = runtime::UnsetKnownInner::time_series;
    using KnownNested = runtime::UnsetKnownNested::time_series;
    using Abstract = runtime::UnsetAbstractOuter::time_series;
    using Mutable = runtime::UnsetMutableOuter::time_series;
    using MutableInner = runtime::UnsetMutableInner::time_series;
    using Growing = runtime::UnsetGrowingSamples::time_series;
    const auto expect_unset = [](auto invoke) {
        try { invoke(); FAIL("required absent payload unexpectedly produced a result"); }
        catch (const std::exception &error) { CHECK(hgl::execution_error_code(error) == "value.unset_read"); }
    };
    expect_unset([&] { (void)eval_node<runtime::unset_number>(values<Value>(tsb_delta<Number>(Int{1}, std::nullopt))); });
    expect_unset([&] { (void)eval_node<runtime::unset_flag>(values<Value>(tsb_delta<Flag>(Int{1}, std::nullopt))); });
    expect_unset([&] { (void)eval_node<runtime::unset_length>(values<Value>(tsb_delta<Samples>(Int{1}, std::nullopt))); });
    expect_unset([&] { (void)eval_node<runtime::unset_list_index>(values<Value>(tsb_delta<Samples>(Int{1}, std::nullopt))); });
    expect_unset([&] { (void)eval_node<runtime::unset_map_index>(values<Value>(tsb_delta<Mapping>(Int{1}, std::nullopt))); });
    expect_unset([&] { (void)eval_node<runtime::unset_mutable_length>(values<Value>(tsb_delta<Samples>(Int{1}, std::nullopt))); });
    expect_unset([&] { (void)eval_node<runtime::unset_mutable_push>(values<Value>(tsb_delta<Growing>(Int{1}, std::nullopt))); });
    CHECK_OUTPUT(eval_node<runtime::unset_mutable_push>(values<Value>(tsb_delta<Growing>(Int{1}, dynamic_list_delta<TS<Int>>({{0, 4}})))), values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::unset_known_chain>(values<Value>(tsb_delta<Known>(Int{1}, std::nullopt))), values<Int>(1));
    CHECK_OUTPUT(eval_node<runtime::unset_known_staged>(values<Value>(tsb_delta<Known>(Int{1}, std::nullopt))), values<Int>(1));
    const auto known_present = tsb_delta<Known>(Int{1}, tsb_delta<KnownInner>(tsb_delta<KnownNested>(Int{0})));
    CHECK_OUTPUT(eval_node<runtime::unset_known_chain>(values<Value>(known_present)), values<Int>(1));
    CHECK_OUTPUT(eval_node<runtime::unset_abstract_known>(values<Value>(tsb_delta<Abstract>(Int{1}, std::nullopt))), values<Int>(1));
    const hgl::ordinary::PreparedValuePlan concrete{scalar_descriptor<runtime::UnsetAbstractConcrete::value_type>::value_meta()};
    const hgl::ordinary::PreparedValuePlan family{scalar_descriptor<runtime::UnsetAbstractInner::value_type>::value_meta()};
    const Value zero{Int{0}};
    const std::array<std::pair<std::size_t, ValueView>, 1> concrete_fields{{{0, zero.view()}}};
    CHECK_OUTPUT((eval_node<runtime::operators::unset_abstract_present, TS<runtime::UnsetAbstractInner::value_type>>(
                     values<Int>(1), values<Value>(family.retain(concrete.bundle(concrete_fields).view())))), values<Value>(Value{Int{1}}));
    using Lengths = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Int>>>;
    CHECK_OUTPUT(eval_node<runtime::unset_mutable_present>(values<Value>(tsb_delta<Mutable>(Int{1}, tsb_delta<MutableInner>(dynamic_list_delta<TS<Int>>({{0, 4}}))))),
                 values<Value>(tsb_delta<Lengths>(Int{2}, Int{1})));
    CHECK_OUTPUT(eval_node<runtime::unset_mutable_length>(values<Value>(tsb_delta<Samples>(Int{1}, list_delta<TS<Int>>({{0, 4}, {1, 5}})))), values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::unset_list_index>(values<Value>(tsb_delta<Samples>(Int{1}, list_delta<TS<Int>>({{0, 4}, {1, 5}})))), values<Int>(5));
    CHECK_OUTPUT(eval_node<runtime::unset_map_index>(values<Value>(tsb_delta<Mapping>(Int{1}, dict_delta<Int, TS<Int>>({{1, 4}})))), values<Int>(5));
    expect_unset([&] { (void)eval_node<runtime::unset_native_text>(values<Value>(tsb_delta<Number>(Int{1}, std::nullopt))); });
    CHECK_OUTPUT(eval_node<runtime::unset_number>(values<Value>(tsb_delta<Number>(Int{1}, Int{0}))), values<Int>(1));
    CHECK_OUTPUT(eval_node<runtime::unset_flag>(values<Value>(tsb_delta<Flag>(Int{1}, Bool{false}))), values<Int>(0));
    CHECK_OUTPUT(eval_node<runtime::unset_native_text>(values<Value>(tsb_delta<Number>(Int{1}, Int{0}))), values<Str>(Str{"0"}));
    using Pair = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    CHECK_OUTPUT(eval_node<runtime::unset_retain_partial>(values<Value>(tsb_delta<Pair>(Int{1}, std::nullopt))),
                 values<Value>(tsb_delta<Pair>(Int{1}, std::nullopt)));
    CHECK_OUTPUT(eval_node<runtime::unset_retain_partial>(values<Value>(tsb_delta<Pair>(Int{1}, Bool{false}))),
                 values<Value>(tsb_delta<Pair>(Int{1}, Bool{false})));
    CHECK_OUTPUT(eval_node<runtime::unset_rebuild_number>(values<Value>(tsb_delta<Number>(Int{1}, std::nullopt))),
                 values<Value>(tsb_delta<Number>(Int{1}, std::nullopt)));
    CHECK_OUTPUT(eval_node<runtime::unset_rebuild_number>(values<Value>(tsb_delta<Number>(Int{1}, Int{0}))),
                 values<Value>(tsb_delta<Number>(Int{1}, Int{0})));
    std::ostringstream captured;
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
    auto logger = std::make_shared<spdlog::logger>("unset-read-test", sink);
    logger->set_pattern("%v");
    log::set_logger(logger);
    const auto restore = make_scope_exit([]() noexcept { log::set_logger(nullptr); });
    expect_unset([&] { (void)eval_node<runtime::unset_error_cleanup>(values<Value>(tsb_delta<Number>(Int{1}, std::nullopt))); });
    auto trace = captured.str();
    for (auto pos = trace.find("\r\n"); pos != std::string::npos; pos = trace.find("\r\n", pos)) { trace.erase(pos, 1); }
    CHECK(trace == "before\nstopped\n");
    captured.str("");
    captured.clear();
    expect_unset([&] { (void)eval_node<runtime::unset_generator_retention>(); });
    trace = captured.str();
    for (auto pos = trace.find("\r\n"); pos != std::string::npos; pos = trace.find("\r\n", pos)) { trace.erase(pos, 1); }
    CHECK(trace == "retained\nresumed\n");
    CHECK_OUTPUT(eval_node<runtime::unset_generator_present>(), values<Int>(none, 1, 1));
    try { (void)eval_node<runtime::unset_global_field>(values<Int>(1)); FAIL("global absent field unexpectedly produced a result"); }
    catch (const std::exception &error) {
        CHECK(hgl::execution_error_code(error).empty());
        CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring("ordinary scalar value is absent"));
    }
    CHECK_OUTPUT(eval_node<runtime::unset_temporal_delta>(values<Value>(tsb_delta<Number>(Int{1}, std::nullopt))), values<Int>(none));
    CHECK_OUTPUT(eval_node<runtime::unset_temporal_delta>(values<Value>(tsb_delta<Number>(Int{1}, Int{0}))), values<Int>(0));
}

TEST_CASE("aggregate helper specializations keep bindings independent across nodes and runs", "[codegen][runtime][prepared-locals]") {
    using IntegerPair = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    using FloatPair = UnNamedTSB<Field<"0", TS<Float>>, Field<"1", TS<Bool>>>;
    for (std::size_t run = 0; run < 2; ++run) {
        CHECK_OUTPUT(eval_node<runtime::specialized_helpers_together>(values<Int>(7, 8), values<Float>(1.5, 2.5)), values<Bool>(true, true));
        CHECK_OUTPUT(eval_node<runtime::specialized_int_helper_node>(values<Int>(9)), values<Value>(tsb_delta<IntegerPair>(Int{9}, Bool{false})));
        CHECK_OUTPUT(eval_node<runtime::specialized_float_helper_node>(values<Float>(3.5)), values<Value>(tsb_delta<FloatPair>(Float{3.5}, Bool{false})));
    }
}

TEST_CASE("generated nominal records prepare exact held fields and retain sparse children", "[codegen][runtime][nominal-observation]") {
    session();
    using Pair = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    using Record = runtime::NominalObservedRecord::time_series;
    using Child = runtime::NominalObservedChild::time_series;
    const auto *ordinary = scalar_descriptor<runtime::NominalObservedRecord::value_type>::value_meta();
    const auto *held = schema_descriptor<Record>::ts_meta()->value_schema;
    REQUIRE(ordinary != held);
    CHECK(held->bundle_hierarchy->ordinary_origin == ordinary);
    CHECK(ordinary->fields[0].type->try_value_kind() == ValueTypeKind::Tuple);
    CHECK(held->fields[0].type == schema_descriptor<Pair>::ts_meta()->value_schema);
    const auto complete = tsb_delta<Record>(tsb_delta<Pair>(Int{7}, Bool{false}), std::nullopt);
    const auto partial = tsb_delta<Record>(tsb_delta<Pair>(Int{7}, std::nullopt), std::nullopt);
    const auto tick = tsb_delta<Record>(tsb_delta<Pair>(std::nullopt, Bool{false}), std::nullopt);
    CHECK_OUTPUT(eval_node<runtime::operators::nominal_construct>(values<Int>(7, none, 7)), values<Value>(complete, none, complete));
    CHECK_OUTPUT((eval_node<runtime::operators::nominal_copied, Record>(values<Value>(partial, tick))), values<Value>(partial, complete));
    CHECK_OUTPUT((eval_node<runtime::operators::nominal_read, Record>(values<Value>(complete))), values<Bool>(false));
    const auto child = tsb_delta<Child>(Int{4}, tsb_delta<Pair>(Int{7}, std::nullopt), std::nullopt);
    const auto child_tick = tsb_delta<Child>(std::nullopt, tsb_delta<Pair>(std::nullopt, Bool{false}), std::nullopt);
    const auto child_complete = tsb_delta<Child>(Int{4}, tsb_delta<Pair>(Int{7}, Bool{false}), std::nullopt);
    CHECK_OUTPUT((eval_node<runtime::operators::nominal_child_copied, Child>(values<Value>(child, child_tick))), values<Value>(child, child_complete));
    using Tuple = UnNamedTSB<Field<"0", Record>, Field<"1", TS<Bool>>>;
    CHECK_OUTPUT((eval_node<runtime::operators::nominal_tuple_read, Tuple>(values<Value>(tsb_delta<Tuple>(Value{complete}, Bool{true})))), values<Bool>(false));
    using List = TSL<Record, 2>;
    CHECK_OUTPUT((eval_node<runtime::operators::nominal_list_read, List>(values<Value>(list_delta<Record>({{0, complete}})))), values<Bool>(false));
    using Map = TSD<Int, Record>;
    CHECK_OUTPUT((eval_node<runtime::operators::nominal_map_read, Map>(values<Value>(dict_delta<Int, Record>({{4, complete}})))), values<Bool>(false));
    CHECK_OUTPUT(eval_node<runtime::operators::generic_tuple_int_wrapper>(values<Int>(7, 8)),
                 values<Value>(tsb_delta<Pair>(Int{7}, Bool{false}), tsb_delta<Pair>(Int{8}, Bool{false})));
}

TEST_CASE("generated normal record arguments preserve structural closure and Atomic identity", "[codegen][runtime][nominal-observation][generic]") {
    session();
    using OrdinaryPair = FixedTuple<Int, Bool>;
    using Pair = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    using PairPair = UnNamedTSB<Field<"0", Pair>, Field<"1", TS<Bool>>>;
    using TupleRecord = runtime::NominalObservedGenericRecord<OrdinaryPair>::time_series;
    const auto inner = tsb_delta<Pair>(Int{7}, std::nullopt);
    const auto nested = tsb_delta<TupleRecord>(tsb_delta<PairPair>(Value{inner}, Bool{false}));
    CHECK_OUTPUT((eval_node<runtime::operators::generic_tuple_record, TupleRecord>(values<Value>(nested))), values<Value>(nested));
    using OrdinaryList = hgl::ordinary::List<OrdinaryPair, 2>;
    using List = TSL<Pair, 2>;
    using ListPair = UnNamedTSB<Field<"0", List>, Field<"1", TS<Bool>>>;
    using ListRecord = runtime::NominalObservedGenericRecord<OrdinaryList>::time_series;
    const auto listed = tsb_delta<ListRecord>(tsb_delta<ListPair>(list_delta<Pair>({{0, inner}}), Bool{false}));
    CHECK_OUTPUT((eval_node<runtime::operators::generic_list_record, ListRecord>(values<Value>(listed))), values<Value>(listed));
    using OrdinaryMap = Map<Int, OrdinaryPair>;
    using MapTS = TSD<Int, Pair>;
    using MapPair = UnNamedTSB<Field<"0", MapTS>, Field<"1", TS<Bool>>>;
    using MapRecord = runtime::NominalObservedGenericRecord<OrdinaryMap>::time_series;
    const auto mapped = tsb_delta<MapRecord>(tsb_delta<MapPair>(dict_delta<Int, Pair>({{4, inner}}), Bool{false}));
    CHECK_OUTPUT((eval_node<runtime::operators::generic_map_record, MapRecord>(values<Value>(mapped))), values<Value>(mapped));
    using Record = runtime::NominalObservedRecord::time_series;
    using RecordPair = UnNamedTSB<Field<"0", Record>, Field<"1", TS<Bool>>>;
    using NamedRecord = runtime::NominalObservedGenericRecord<runtime::NominalObservedRecord::value_type>::time_series;
    const auto named = tsb_delta<NamedRecord>(tsb_delta<RecordPair>(tsb_delta<Record>(Value{inner}, std::nullopt), Bool{false}));
    CHECK_OUTPUT((eval_node<runtime::operators::generic_nominal_record, NamedRecord>(values<Value>(named))), values<Value>(named));
    using AtomicRecord = runtime::NominalGenericAtomicRecord<OrdinaryPair>::time_series;
    const auto *atomic_schema = schema_descriptor<AtomicRecord>::ts_meta();
    CHECK(atomic_schema->fields()[0].type == schema_descriptor<Pair>::ts_meta());
    CHECK(atomic_schema->fields()[1].type == schema_descriptor<TS<OrdinaryPair>>::ts_meta());
    const hgl::ordinary::PreparedValuePlan ordinary_pair{scalar_descriptor<OrdinaryPair>::value_meta()};
    const Value seven{Int{7}}, falsity{Bool{false}};
    const auto opaque = ordinary_pair.bundle(std::array{std::pair<std::size_t, ValueView>{0, seven.view()},
        std::pair<std::size_t, ValueView>{1, falsity.view()}});
    BundleBuilder atomic_builder{ValuePlanFactory::instance().type_for(atomic_schema->delta_value_schema)};
    atomic_builder.set(0, inner.view());
    atomic_builder.set(1, opaque.view());
    const auto atomic = atomic_builder.build();
    CHECK_OUTPUT((eval_node<runtime::operators::atomic_tuple_record, AtomicRecord>(values<Value>(atomic))), values<Value>(atomic));
}

TEST_CASE("generated complete publications reconcile recursive validity and Map membership", "[codegen][runtime][publication]") {
    session();
    using Pair = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    using Named = runtime::NominalPublicationPair::time_series;
    using List = TSL<TS<Int>, 2>;
    const auto steps = values<Int>(1, 2, 3);
    const auto expected = values<Bool>(true, false, false);
    using CanonicalNamed = NominalTSB<runtime::NominalPublicationPair::value_type, Field<"left", TS<Int>>, Field<"right", TS<Bool>>>;
    const auto canonical_full = values<Value>(tsb_delta<CanonicalNamed>(Int{1}, Bool{false}), none, none);
    const auto canonical_partial = values<Value>(tsb_delta<CanonicalNamed>(Int{2}, std::nullopt), tsb_delta<CanonicalNamed>(Int{2}, std::nullopt), none);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_value_validity, CanonicalNamed, CanonicalNamed>(steps, canonical_full, canonical_partial)), expected);
    const auto pair_full = values<Value>(tsb_delta<Pair>(Int{1}, Bool{false}), none, none);
    const auto pair_partial = values<Value>(tsb_delta<Pair>(Int{2}, std::nullopt), tsb_delta<Pair>(Int{2}, std::nullopt), none);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_tuple_result_validity, Pair, Pair>(steps, pair_full, pair_partial)), expected);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_generic_tuple_validity, Pair, Pair>(steps, pair_full, pair_partial)), expected);
    const auto list_full = values<Value>(list_delta<TS<Int>>({{0, Int{1}}, {1, Int{10}}}), none, none);
    const auto list_partial = values<Value>(list_delta<TS<Int>>({{0, Int{2}}}), list_delta<TS<Int>>({{0, Int{2}}}), none);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_list_result_validity, List, List>(steps, list_full, list_partial)), expected);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_generic_list_validity, List, List>(steps, list_full, list_partial)), expected);
    const auto named_full = values<Value>(tsb_delta<Named>(Int{1}, Bool{false}), none, none);
    const auto named_partial = values<Value>(tsb_delta<Named>(Int{2}, std::nullopt), tsb_delta<Named>(Int{2}, std::nullopt), none);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_value_validity, Named, Named>(steps, named_full, named_partial)), expected);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_delta_validity, Named, Named>(steps, named_full, named_partial)), values<Bool>(true, true, true));
    CHECK_OUTPUT((eval_node<runtime::operators::publication_generic_pair_validity, Named, Named>(steps, named_full, named_partial)), expected);
    CHECK_OUTPUT(eval_node<runtime::operators::publication_copied_membership>(steps), expected);
    CHECK_OUTPUT(eval_node<runtime::operators::publication_copied_invalid_membership>(steps), values<Bool>(true, true, true));
    CHECK_OUTPUT(eval_node<runtime::operators::publication_copied_map_validity>(steps), expected);
    CHECK_OUTPUT(eval_node<runtime::operators::publication_generic_map_membership>(steps), expected);
    CHECK_OUTPUT(eval_node<runtime::operators::publication_generic_map_validity>(steps), expected);
    using Nested = UnNamedTSB<Field<"0", Pair>, Field<"1", TS<Bool>>>;
    const auto nested_full = values<Value>(tsb_delta<Nested>(tsb_delta<Pair>(Int{1}, Bool{false}), Bool{false}), none, none);
    const auto nested_partial_value = tsb_delta<Nested>(tsb_delta<Pair>(Int{2}, std::nullopt), Bool{false});
    const auto nested_partial = values<Value>(nested_partial_value, nested_partial_value, none);
    CHECK_OUTPUT((eval_node<runtime::operators::publication_generic_nested_validity, Nested, Nested>(steps, nested_full, nested_partial)), expected);
}

TEST_CASE("generated runtime tuple results publish complete values and sparse positional deltas", "[codegen][runtime][tuple]")
{
    session();
    using Result = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    CHECK_OUTPUT(eval_node<runtime::operators::tuple_result>(values<Int>(1, 1, none, 2)),
                 values<Value>(tsb_delta<Result>(Int{1}, Bool{true}), tsb_delta<Result>(Int{1}, Bool{true}), none,
                               tsb_delta<Result>(Int{2}, Bool{true})));
    CHECK_OUTPUT(eval_node<runtime::operators::tuple_patch>(values<Int>(1, 2, 2, 1)),
                 values<Value>(tsb_delta<Result>(Int{1}, std::nullopt), tsb_delta<Result>(std::nullopt, Bool{true}),
                               tsb_delta<Result>(std::nullopt, Bool{true}), tsb_delta<Result>(Int{1}, std::nullopt)));
    const auto sparse = values<Value>(tsb_delta<Result>(Int{1}, std::nullopt),
                                     tsb_delta<Result>(std::nullopt, Bool{true}), tsb_delta<Result>(Int{1}, std::nullopt), none);
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_forward, Result>(sparse)), sparse);
    using Nested = UnNamedTSB<Field<"0", Result>, Field<"1", TS<Int>>>;
    CHECK_OUTPUT(eval_node<runtime::operators::nested_tuple_result>(values<Int>(1, 1, none, -2)),
                 values<Value>(tsb_delta<Nested>(tsb_delta<Result>(Int{1}, Bool{true}), Int{1}),
                               tsb_delta<Nested>(tsb_delta<Result>(Int{1}, Bool{true}), Int{1}), none,
                               tsb_delta<Nested>(tsb_delta<Result>(Int{-2}, Bool{false}), Int{-2})));
    const auto complete_input = values<Value>(tsb_delta<Result>(Int{7}, Bool{false}),
                                             tsb_delta<Result>(std::nullopt, Bool{true}), tsb_delta<Result>(Int{9}, std::nullopt));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_complete_forward, Result>(complete_input)),
                 values<Value>(tsb_delta<Result>(Int{7}, Bool{false}), tsb_delta<Result>(Int{7}, Bool{true}),
                               tsb_delta<Result>(Int{9}, Bool{true})));
    const auto partial_input = values<Value>(tsb_delta<Result>(std::nullopt, Bool{false}), tsb_delta<Result>(Int{7}, std::nullopt),
                                            tsb_delta<Result>(std::nullopt, Bool{true}), tsb_delta<Result>(Int{9}, std::nullopt), none);
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_first, Result>(partial_input)), values<Int>(none, 7, none, 9, none));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_second, Result>(partial_input)), values<Bool>(false, none, true, none, none));
    CHECK_OUTPUT(eval_node<runtime::operators::tuple_local_read>(values<Int>(1, -1, 0)), values<Bool>(true, false, false));
    const auto nested_input = values<Value>(tsb_delta<Nested>(tsb_delta<Result>(std::nullopt, Bool{false}), std::nullopt),
                                           tsb_delta<Nested>(tsb_delta<Result>(Int{7}, std::nullopt), std::nullopt),
                                           tsb_delta<Nested>(std::nullopt, Int{9}));
    CHECK_OUTPUT((eval_node<runtime::operators::nested_tuple_first, Nested>(nested_input)), values<Int>(none, 7, none));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_observation_copy, Result>(complete_input)), values<Int>(7, 7, 9));
    const auto embedded = values<Value>(tsb_delta<Nested>(tsb_delta<Result>(Int{7}, Bool{false}), Int{1}),
                                       tsb_delta<Nested>(tsb_delta<Result>(Int{7}, Bool{true}), Int{1}),
                                       tsb_delta<Nested>(tsb_delta<Result>(Int{9}, Bool{true}), Int{1}));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_embed_input, Result>(complete_input)), embedded);
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_embed_copy, Result>(complete_input)), embedded);
    const auto nested_complete_input = values<Value>(tsb_delta<Nested>(tsb_delta<Result>(Int{7}, Bool{false}), Int{1}),
                                                    tsb_delta<Nested>(tsb_delta<Result>(std::nullopt, Bool{true}), std::nullopt),
                                                    tsb_delta<Nested>(std::nullopt, Int{2}));
    CHECK_OUTPUT((eval_node<runtime::operators::nested_tuple_observation_copy, Nested>(nested_complete_input)),
                 values<Bool>(false, true, true));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_observed_generic, Result>(complete_input)),
                 values<Value>(tsb_delta<Result>(Int{7}, Bool{false}), tsb_delta<Result>(Int{7}, Bool{true}),
                               tsb_delta<Result>(Int{9}, Bool{true})));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_observed_generic_int, Result>(complete_input)),
                 values<Value>(tsb_delta<Result>(Int{7}, Bool{false}), tsb_delta<Result>(Int{7}, Bool{true}),
                               tsb_delta<Result>(Int{9}, Bool{true})));

    using List = TSL<Result, 2>;
    using ListTuple = UnNamedTSB<Field<"0", List>, Field<"1", TS<Bool>>>;
    const auto pairs = list_delta<Result>({{0, tsb_delta<Result>(Int{7}, Bool{false})},
                                          {1, tsb_delta<Result>(Int{8}, Bool{true})}});
    const auto list_initial = tsb_delta<ListTuple>(Value{pairs}, Bool{true});
    const auto list_tick = tsb_delta<ListTuple>(std::nullopt, Bool{false});
    const auto list_snapshot = tsb_delta<ListTuple>(Value{pairs}, Bool{false});
    const auto list_input = values<Value>(list_initial, list_tick, none, list_tick);
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_list_observe, ListTuple>(list_input)),
                 values<Bool>(false, false, none, false));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_list_copy, ListTuple>(list_input)),
                 values<Value>(list_initial, list_snapshot, none, list_snapshot));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_list_first, ListTuple>(list_input)),
                 values<Value>(tsb_delta<Result>(Int{7}, Bool{false}), tsb_delta<Result>(Int{7}, Bool{false}), none,
                               tsb_delta<Result>(Int{7}, Bool{false})));

    using Map = TSD<Int, Result>;
    using MapTuple = UnNamedTSB<Field<"0", Map>, Field<"1", TS<Bool>>>;
    const auto map_initial = tsb_delta<MapTuple>(dict_delta<Int, Result>(
        {{4, tsb_delta<Result>(Int{7}, Bool{false})}, {5, tsb_delta<Result>(Int{8}, Bool{true})}}), Bool{true});
    const auto map_remove = tsb_delta<MapTuple>(dict_delta<Int, Result>({}, {5}), std::nullopt);
    const auto map_snapshot = tsb_delta<MapTuple>(dict_delta<Int, Result>(
        {{4, tsb_delta<Result>(Int{7}, Bool{false})}}, {5}), Bool{true});
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_map_copy, MapTuple>(values<Value>(map_initial, none, map_remove))),
                 values<Value>(map_initial, none, map_snapshot));
    using Collections = UnNamedTSB<Field<"0", List>, Field<"1", Map>>;
    const auto collection_result = tsb_delta<Collections>(Value{pairs}, dict_delta<Int, Result>(
        {{4, tsb_delta<Result>(Int{7}, Bool{false})}}));
    CHECK_OUTPUT(eval_node<runtime::operators::tuple_collection_result>(values<Int>(7, none, 7)),
                 values<Value>(collection_result, none, collection_result));
}

TEST_CASE("generated tuple observations preserve absent required payloads", "[codegen][runtime][tuple][observation]")
{
    session();
    using Pair = UnNamedTSB<Field<"0", TS<Int>>, Field<"1", TS<Bool>>>;
    const auto partial = values<Value>(tsb_delta<Pair>(Int{7}, std::nullopt));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_observed_present_field, Pair>(partial)), values<Int>(7));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_complete_forward, Pair>(partial)), partial);
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_observed_partial_result, Pair>(partial)), partial);
    CHECK_THROWS_WITH((eval_node<runtime::operators::tuple_observed_absent_field, Pair>(partial)),
                      Catch::Matchers::ContainsSubstring("ordinary scalar value is absent"));
    using Nested = UnNamedTSB<Field<"0", TSL<Pair, 2>>, Field<"1", TS<Bool>>>;
    const auto nested_partial = values<Value>(tsb_delta<Nested>(
        list_delta<Pair>({{0, tsb_delta<Pair>(Int{7}, std::nullopt)}}), Bool{true}));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_list_observed_partial_result, Nested>(nested_partial)), nested_partial);
    CHECK_THROWS_WITH((eval_node<runtime::operators::tuple_list_observed_absent_field, Nested>(nested_partial)),
                      Catch::Matchers::ContainsSubstring("ordinary scalar value is absent"));
    CHECK_THROWS_WITH((eval_node<runtime::operators::tuple_list_observed_absent_slot, Nested>(nested_partial)),
                     Catch::Matchers::ContainsSubstring("ordinary scalar value is absent"));
    const auto absent_list = values<Value>(tsb_delta<Nested>(std::nullopt, Bool{true}));
    CHECK_THROWS_WITH((eval_node<runtime::operators::tuple_list_observed_absent_length, Nested>(absent_list)),
                     Catch::Matchers::ContainsSubstring("ordinary scalar value is absent"));
    using Map = UnNamedTSB<Field<"0", TSD<Int, Pair>>, Field<"1", TS<Bool>>>;
    const auto map_partial = values<Value>(tsb_delta<Map>(
        dict_delta<Int, Pair>({{4, tsb_delta<Pair>(Int{7}, std::nullopt)}}), Bool{true}));
    CHECK_OUTPUT((eval_node<runtime::operators::tuple_map_observed_partial, Map>(map_partial)), map_partial);
}

TEST_CASE("generated module registration owns a removable provider generation", "[codegen][runtime][lifecycle]")
{
    hgl::wiring::ensure_session();
    auto provider = runtime::register_operators();
    auto same = runtime::register_operators();
    CHECK(provider.valid());
    CHECK(provider.active());
    CHECK(same.active());
    CHECK(provider.key() == "hgl.codegen.runtime");
    CHECK(OperatorRegistry::instance().remove_provider(provider));
    CHECK_FALSE(provider.active());
    CHECK_FALSE(same.active());

    auto replacement = runtime::register_operators();
    CHECK(replacement.active());
    CHECK_FALSE(OperatorRegistry::instance().remove_provider(provider));
    CHECK(OperatorRegistry::instance().remove_provider(replacement));
}

TEST_CASE("generated runtime impl functions register as node overloads", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("a generated composition can wire a generated operator implementation", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute_graph>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("a generated runtime operator consumes a homogeneous argument pack", "[codegen][runtime][parameter-pack]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::all_runtime_graph>(values<Bool>(true, true), values<Bool>(false, true)),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<runtime::operators::positional_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})), values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::named_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})), values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::homogeneous_schema_count_graph>(values<Float>(1.0), values<Float>(2.0)),
                 values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::triggered_schema_count_graph>(
                     values<Float>(1.0, none, 2.0), values<Float>(none, 10.0, none), values<Float>(none, none, 20.0)),
                 values<Int>(2, none, 2));
    CHECK_OUTPUT(eval_node<runtime::operators::positional_schema_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})),
                 values<Int>(2));
    CHECK_OUTPUT(eval_node<runtime::operators::named_schema_count_graph>(values<Float>(1.0), values<Str>(Str{"x"})),
                 values<Int>(2));
    REQUIRE_THROWS_AS(eval_node<runtime::operators::all_runtime>(values<Bool>(true)), OperatorResolutionError);
}

TEST_CASE("generated runtime control flow definitely assigns typed locals", "[codegen][runtime][locals]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::absolute_local>(values<Float>(-2.0, 3.0)),
                 values<Float>(2.0, 3.0));
}

TEST_CASE("generated runtime predicates use modified-or and valid-and semantics", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::add_when_ready>(values<Float>(1.0, none, 3.0),
                                                        values<Float>(none, 10.0, 20.0)),
                 values<Float>(none, 11.0, 23.0));
}

TEST_CASE("generated activation analysis leaves sampled inputs passive", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::sample_on_trigger>(values<Float>(1.0, none, 2.0),
                                                           values<Float>(10.0, 20.0, none)),
                 values<Float>(10.0, none, 20.0));
}

TEST_CASE("generated runtime metadata reads the input selector", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::updated_at>(values<Float>(1.0, none, 2.0)),
                 values<DateTime>(MIN_ST, none, MIN_ST + 2 * MIN_TD));
}

TEST_CASE("generated signal inputs observe ticks without exposing payloads", "[codegen][runtime][signal]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::count_ticks>(values<Float>(1.0, 2.0, 3.0)), values<Int>(1, 2, 3));
    CHECK_OUTPUT(eval_node<runtime::operators::count_float_ticks>(values<Float>(1.0, 2.0, 3.0)), values<Int>(1, 2, 3));
}

TEST_CASE("generated runtime handlers share recordable state and run in source order", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::combined_total>(values<Float>(10.0, none, 2.0),
                                                        values<Float>(3.0, 1.0, none)),
                 values<Float>(7.0, 6.0, 8.0));
}

TEST_CASE("a generated composition can wire a generated runtime node", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::combined_total_graph>(values<Float>(10.0, none, 2.0),
                                                              values<Float>(3.0, 1.0, none)),
                 values<Float>(7.0, 6.0, 8.0));
}

TEST_CASE("generated inject out exposes the previous value and writes non-terminally", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::running_total>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("generated HGL mutates set outputs through the functional facade", "[codegen][runtime][collection]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::mutate_set>(values<Int>(1, 2)),
                 values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1})));
}

TEST_CASE("generated HGL mutates map outputs through the functional facade", "[codegen][runtime][collection]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::mutate_map>(values<Int>(1, 2, 3)),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
                               dict_delta<Str, TS<Int>>({{"b", 4}, {"c", 5}}, {"a"}),
                               dict_delta<Str, TS<Int>>({}, {"b", "c"})));
}

TEST_CASE("generated HGL mutates unbounded list outputs through the functional facade",
          "[codegen][runtime][collection]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::mutate_list>(values<Int>(1, 2, 3)),
                 values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                               dynamic_list_delta<TS<Int>>({{1, 3}}),
                               dynamic_list_delta<TS<Int>>({}, {0, 1})));
}

TEST_CASE("generated runtime lifecycle hooks run around evaluation", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::lifecycle_value>(values<Float>(4.0, 5.0)),
                 values<Float>(4.0, 5.0));
}

TEST_CASE("generated state initializers can use const parameters", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::configured_total>(values<Float>(1.0, 2.0), arg<"initial">(Float{5.0})),
                 values<Float>(6.0, 8.0));
}

TEST_CASE("generated runtime functions without when use ordinary input policy", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::unconditional_total>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("a generated composition can wire a private generated runtime node", "[codegen][runtime]")
{
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::private_total_graph>(values<Float>(1.0, 2.0, 3.0)),
                 values<Float>(1.0, 3.0, 6.0));
}

TEST_CASE("generated cache fields share one native slot and reinitialize on a new run", "[codegen][runtime][cache]") {
    session();
    for (int run = 0; run != 2; ++run) {
        CHECK_OUTPUT(eval_node<runtime::operators::cache_bundle>(values<Int>(2, 4, 9)), values<Float>(2.0, 3.0, 5.0));
    }
}

TEST_CASE("generated fixed-list scalar reads use guarded child validity", "[codegen][runtime]") {
    session();
    CHECK_OUTPUT((eval_node<runtime::operators::first_scalar, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{1, 9}}), list_delta<TS<Int>>({{0, 3}}), list_delta<TS<Int>>({{1, 10}}),
                                   list_delta<TS<Int>>({{0, 7}})))),
                 values<Int>(none, 3, 3, 7));
}

namespace
{
    template <typename Op> struct RecoverySource
    {
        static Port<TS<Int>> compose(Wiring &w) { return wire<Op>(w).template as<TS<Int>>(); }
    };
    template <typename Op> struct RecoverySourceComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>)
        {
            return stdlib::component<RecoverySource<Op>>(w, "source");
        }
    };
}

TEST_CASE("generated sources with caches or external clocks refuse checkpoint admission", "[codegen][runtime][checkpoint]")
{
    session();
    GlobalContext context;
    configure_component_recovery(context.state().view(), {.component_id = "source", .commit = [](const ComponentCheckpoint &) {}});
    CHECK_THROWS_WITH(eval_node<RecoverySourceComponent<runtime::operators::cached_source>>(values<Int>(none)),
                      Catch::Matchers::ContainsSubstring("unsupported node"));
    CHECK_THROWS_WITH(eval_node<RecoverySourceComponent<runtime::operators::clock_source>>(values<Int>(none)),
                      Catch::Matchers::ContainsSubstring("unsupported node"));
}

namespace
{
    template <typename Op> struct MixedStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input) { return wire<Op>(w, input).template as<TS<Int>>(); }
    };
    template <typename Op> struct MixedComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<MixedStrategy<Op>>(w, "strategy", input);
        }
    };
    EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }
}

// The pair that ADR 0011 gates the mixed case on: one generated node owning a
// `RecordableState<>` and a `State<>` at once, and the two behaving
// DIFFERENTLY across a restore. `mixed_total` returns `total * 10 + seen`, so
// one output reads both storages.
TEST_CASE("a generated node restores its state and rebuilds its cache", "[codegen][runtime][cache][checkpoint]")
{
    session();
    GlobalContext                      context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(),
                                 {.component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<MixedComponent<runtime::operators::mixed_total>>(interval(0, 2), values<Int>(1, 2)),
                 values<Int>(11, 32));
    REQUIRE(completed);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {.component_id = "strategy",
                                                          .load         = [&] { return std::optional{prior}; },
                                                          .commit = [&](const auto &image) { completed = image; }});
    // `total` is recordable and resumes at 3: 3+3=6, then 6+4=10. `seen` is a
    // cache, so `start` rebuilds it from its initializer and it counts 1, 2
    // again -- had it been restored too, this would read 63 and 104.
    CHECK_OUTPUT(
        eval_node_with_options<MixedComponent<runtime::operators::mixed_total>>(interval(2, 5), values<Int>(none, 3, 4)),
        values<Int>(none, 61, 102));
}

TEST_CASE("a generated node's state schema and cache struct both survive a restore whole",
          "[codegen][runtime][cache][checkpoint]")
{
    // Two fields on each side, so neither storage can be standing in for the
    // other: `(total + seen) * scale + count`.
    session();
    GlobalContext                      context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(),
                                 {.component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    CHECK_OUTPUT(eval_node_with_options<MixedComponent<runtime::operators::mixed_bundle>>(interval(0, 2), values<Int>(1, 2)),
                 values<Int>(5, 12));
    REQUIRE(completed);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {.component_id = "strategy",
                                                          .load         = [&] { return std::optional{prior}; },
                                                          .commit = [&](const auto &image) { completed = image; }});
    // Both state fields resume (total 3, seen 2); both cache fields are rebuilt
    // (count 0, scale 2), so (6+3)*2+1 = 19 and then (10+4)*2+2 = 30.
    CHECK_OUTPUT(
        eval_node_with_options<MixedComponent<runtime::operators::mixed_bundle>>(interval(2, 5), values<Int>(none, 3, 4)),
        values<Int>(none, 19, 30));
}

// The ordering the two storages depend on, checked by value rather than by
// reading the emitted text: `total` is seeded from the cache `seed`, and the
// cache `echo` is rebuilt from `total` AFTER a restore has supplied it.
TEST_CASE("a generated node seeds its storages in declaration order", "[codegen][runtime][cache][checkpoint]")
{
    session();
    GlobalContext                      context;
    std::optional<ComponentCheckpoint> completed;
    configure_component_recovery(context.state().view(),
                                 {.component_id = "strategy", .commit = [&](const auto &image) { completed = image; }});
    // seed 7 -> total 7 -> echo 7, so 8*10+7 and 10*10+7. States-first seeding
    // gave total the cache's default 0 and produced 10 and 30.
    CHECK_OUTPUT(eval_node_with_options<MixedComponent<runtime::operators::ordered_seed>>(interval(0, 2), values<Int>(1, 2)),
                 values<Int>(87, 107));
    REQUIRE(completed);
    const auto prior = *completed;
    configure_component_recovery(context.state().view(), {.component_id = "strategy",
                                                          .load         = [&] { return std::optional{prior}; },
                                                          .commit = [&](const auto &image) { completed = image; }});
    // `total` resumes at 10, so `echo` rebuilds to 10 -- a cache taking its
    // value from restored state, which is the pairing ADR 0011 exists for.
    CHECK_OUTPUT(
        eval_node_with_options<MixedComponent<runtime::operators::ordered_seed>>(interval(2, 5), values<Int>(none, 3, 4)),
        values<Int>(none, 140, 180));
}

TEST_CASE("a generated mixed node re-initializes both storages on a fresh run", "[codegen][runtime][cache]")
{
    // No checkpoint: `state` has nothing to restore, so a second run repeats
    // the first exactly. This is the construction half of the contract.
    session();
    for (int run = 0; run != 2; ++run) {
        CHECK_OUTPUT(eval_node<runtime::operators::mixed_total>(values<Int>(1, 2)), values<Int>(11, 32));
        CHECK_OUTPUT(eval_node<runtime::operators::mixed_bundle>(values<Int>(1, 2)), values<Int>(5, 12));
    }
}


TEST_CASE("generated yields admit strictly increasing targets across skips and resumptions", "[codegen][runtime][adr-0015]") {
    namespace source = hgl::codegen::sources;
    using Catch::Matchers::ContainsSubstring;
    CHECK_OUTPUT(eval_node<source::ordered_targets>(MIN_ST - MIN_TD, MIN_ST), values<Int>(2));
    CHECK_OUTPUT(eval_node<source::increasing_past_targets>(), values<Int>(3));
    CHECK_OUTPUT(eval_node<source::ordered_targets>(MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2),
                 values<Int>(none, -1, 2));
    // Every invocation starts without a predecessor, even before the epoch.
    CHECK_OUTPUT(eval_node<source::increasing_past_targets>(), values<Int>(3));
    for (const auto first : {MIN_ST - MIN_TD, MIN_ST, MIN_ST + MIN_TD * 2}) {
        CHECK_THROWS_WITH(eval_node<source::ordered_targets>(first, first), ContainsSubstring("non-increasing time"));
        CHECK_THROWS_WITH(eval_node<source::ordered_targets>(first, first - MIN_TD), ContainsSubstring("non-increasing time"));
    }
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(MIN_TD, TimeDelta::zero()),
                      ContainsSubstring("non-increasing time"));
}

TEST_CASE("generated relative yields reject negative durations before checked target addition", "[codegen][runtime][adr-0015]") {
    namespace source = hgl::codegen::sources;
    using Catch::Matchers::ContainsSubstring;
    CHECK_OUTPUT(eval_node<source::relative_targets>(TimeDelta::zero(), MIN_TD), values<Int>(-1, 2));
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(-MIN_TD, MIN_TD), ContainsSubstring("negative duration"));
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(MIN_TD, -MIN_TD), ContainsSubstring("negative duration"));
    CHECK_THROWS_WITH(eval_node<source::relative_targets>(TimeDelta::min(), MIN_TD), ContainsSubstring("negative duration"));
    CHECK_THROWS(eval_node<source::relative_targets>(TimeDelta::max(), MIN_TD));
}


TEST_CASE("generated yield operands run once in order, including skipped and rejected yields", "[codegen][runtime][adr-0015]") {
    namespace source = hgl::codegen::sources;
    std::ostringstream captured;
    const auto captured_lines = [&] {
        auto text = captured.str();
        for (auto pos = text.find("\r\n"); pos != std::string::npos; pos = text.find("\r\n", pos)) {
            text.erase(pos, 1);
        }
        return text;
    };
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
    auto logger = std::make_shared<spdlog::logger>("generator-operands-test", sink);
    logger->set_pattern("%v");
    log::set_logger(logger);
    const auto restore = make_scope_exit([]() noexcept { log::set_logger(nullptr); });
    SECTION("future operands are not reevaluated on resumption") {
        CHECK_OUTPUT(eval_node<source::observed_relative>(MIN_TD * 2), values<Int>(none, none, 7));
        CHECK(captured_lines() == "time\npayload\nafter\n");
    }
    SECTION("negative admission follows both operands and prevents continuation") {
        CHECK_THROWS_WITH(eval_node<source::observed_relative>(-MIN_TD),
                          Catch::Matchers::ContainsSubstring("negative duration"));
        CHECK(captured_lines() == "time\npayload\n");
    }
    SECTION("implicit overflow follows both operands and prevents continuation") {
        CHECK_THROWS(eval_node<source::observed_relative>(TimeDelta::max()));
        CHECK(captured_lines() == "time\npayload\n");
    }
    SECTION("past absolute targets evaluate their payload") {
        CHECK_OUTPUT(eval_node<source::observed_past>(), values<Int>(2));
        CHECK(captured_lines() == "payload\nafter\n");
    }
}

TEST_CASE("generated ordinary storage preserves nested owners and yielded temporaries", "[codegen][runtime][ordinary][adr-0015]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_literal_result>(values<Int>(1)), values<Int>(14));
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_nested_result>(values<Int>(1)), values<Bool>(true));
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_owned_delta_result>(values<Int>(1, 1)), values<Int>(10, 11));
}

TEST_CASE("generated const ordinary aggregates normalize configured storage before hooks", "[codegen][runtime][ordinary]") {
    session();
    ListBuilder builder{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
    builder.push_back(Int{10});
    builder.push_back(Int{20});
    auto configured = builder.build();
    CHECK_OUTPUT(eval_node<runtime::operators::ordinary_configured_lengths>(values<Int>(1), configured), values<Int>(23));
    CHECK(configured.as_list().size() == 2);
}

TEST_CASE("generated atomic list temporaries publish immediately and survive resumptions", "[codegen][runtime][ordinary][atomic]") {
    namespace source = hgl::codegen::sources;
    hgl::wiring::ensure_session();
    source::register_operators();
    const auto recorded = eval_node<source::operators::atomic_owned_yields>();
    const hgl::ordinary::PreparedValuePlan plan{scalar_descriptor<hgl::ordinary::List<Int>>::value_meta()};
    const auto snapshot = [&](std::initializer_list<Int> items) {
        auto value = plan.empty_list();
        for (const auto item : items) { plan.push(value.view(), Value{item}.view()); }
        return value;
    };
    REQUIRE(recorded.size() == 4);
    REQUIRE(recorded[0]);
    CHECK_FALSE(recorded[1]);
    REQUIRE(recorded[2]);
    REQUIRE(recorded[3]);
    CHECK(recorded[0]->equals(snapshot({1, 2})));
    CHECK(recorded[2]->equals(snapshot({3, 4})));
    CHECK(recorded[3]->equals(snapshot({})));
}

TEST_CASE("generic composition preserves its enclosing conditional body", "[codegen][runtime][signal]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::generic_then_conditional>(values<Bool>(true, false, true), values<Int>(1, 2, 3)),
        values<Int>(1, 2, 3));
}

TEST_CASE("generated generic compositions preserve scalar and structural signal observations", "[codegen][runtime][signal]") {
    session();
    CHECK_OUTPUT(eval_node<runtime::operators::generic_scalar_signal_count>(values<Int>(none, 0, 0, none, -7)),
                 values<Int>(none, 1, 2, none, 3));
    CHECK_OUTPUT((eval_node<runtime::operators::generic_structural_signal_count, TSL<TS<Int>, 2>>(
                     values<Value>(list_delta<TS<Int>>({{1, 0}}), none, list_delta<TS<Int>>({{1, 0}})))),
                 values<Int>(1, none, 2));
}

namespace {
    struct NativeGeneratedEnumForward {
        static constexpr auto name = "native_generated_enum_forward";
        static void eval(In<"value", TS<runtime::RuntimeMode>> value, Out<TS<runtime::RuntimeMode>> out) {
            const runtime::RuntimeMode member = value.value();
            out.set(member);
        }
    };
}

TEST_CASE("generated enums support typed native input and output", "[codegen][runtime][enum]") {
    session();
    using Mode = runtime::RuntimeMode;
    const auto low = Mode::member(INT64_MIN);
    const auto high = Mode::member(INT64_MAX);
    const auto first = Mode::member(-7);
    const auto arrivals = values<Value>(low, none, first, first, high);
    using NativeEnumOperator = Operator<"native_generated_enum_forward", In<"value", TS<Mode>>, Out<TS<Mode>>>;
    register_overload<NativeEnumOperator, NativeGeneratedEnumForward>();
    CHECK_OUTPUT((eval_node<NativeEnumOperator, TS<Mode>>(arrivals)), arrivals);
}

TEST_CASE("generated public enum markers retain nominal schemas and owning values", "[codegen][runtime][enum]") {
    session();
    using Mode = runtime::RuntimeMode;
    const auto low = Mode::member(INT64_MIN);
    const auto high = Mode::member(INT64_MAX);
    const auto first = Mode::member(-7);
    REQUIRE(low.schema() == scalar_descriptor<Mode>::value_meta());
    CHECK(low.schema()->is_enum());
    CHECK(low.schema() != scalar_descriptor<Int>::value_meta());
    CHECK_THROWS_AS(Mode::member(0), std::invalid_argument);
    CHECK_OUTPUT((eval_node<runtime::operators::enum_forward, TS<Mode>>(
        values<Value>(none, low, low, none, first, high, none))),
        values<Value>(none, low, low, none, first, high, none));
    CHECK_OUTPUT(eval_node<runtime::operators::enum_default_source>(values<Int>(1, 1)), values<Value>(first, first));
    CHECK_OUTPUT((eval_node<runtime::operators::enum_value_forward, TS<Mode>>(values<Value>(first, high))),
                 values<Value>(first, high));
    const auto sparse = [&](const Value &value) {
        MapBuilder builder{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta()), value.binding()};
        const Int index = 1;
        builder.set_item_copy(&index, value.view().data());
        return builder.build();
    };
    CHECK_OUTPUT((eval_node<runtime::operators::enum_fixed_forward, TSL<TS<Mode>, 2>>(
        values<Value>(sparse(first), none, sparse(high)))),
        values<Value>(sparse(first), none, sparse(high)));
}

TEST_CASE("generated scalar keys preserve signed zero and both infinities", "[codegen][runtime][scalar-keys]") {
    session();
    const Float infinity = std::numeric_limits<Float>::infinity();
    CHECK_OUTPUT((eval_node<runtime::operators::scalar_float_set, TSS<Float>>(
        values<Value>(set_delta<Float>({-0.0, infinity, -infinity}, {}), none,
                      set_delta<Float>({}, {0.0, infinity}), set_delta<Float>({infinity}, {-infinity})))),
        values<Value>(set_delta<Float>({0.0, -infinity, infinity}, {}), none,
                      set_delta<Float>({}, {-0.0, infinity}), set_delta<Float>({infinity}, {-infinity})));
    CHECK_OUTPUT((eval_node<runtime::operators::scalar_float_map, TSD<Float, TS<Int>>>(
        values<Value>(dict_delta<Float, TS<Int>>({{-0.0, 1}, {infinity, 2}, {-infinity, 3}}),
                      dict_delta<Float, TS<Int>>({{0.0, 1}}), none,
                      dict_delta<Float, TS<Int>>({}, {-0.0, infinity})))),
        values<Value>(dict_delta<Float, TS<Int>>({{0.0, 1}, {-infinity, 3}, {infinity, 2}}),
                      dict_delta<Float, TS<Int>>({{-0.0, 1}}), none,
                      dict_delta<Float, TS<Int>>({}, {0.0, infinity})));
}

TEST_CASE("generated scalar map recipes retain exact keys", "[codegen][runtime][scalar-keys]") {
    session();
    CHECK_OUTPUT((eval_node<runtime::operators::scalar_string_recipe>(values<Int>(1, none, 2))),
        values<Value>(dict_delta<Str, TS<Int>>({{"first", 1}, {"second", 2}}), none,
                      dict_delta<Str, TS<Int>>({{"second", 2}, {"first", 1}})));
}

TEST_CASE("generated ordinary collections retain runtime children and reject duplicates", "[codegen][runtime][atomic-collections]") {
    session();
    const auto snapshot = [](const Str &key, Int item) {
        ListBuilder row{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta()),
                        *scalar_descriptor<hgl::ordinary::List<Int>>::value_meta()};
        row.push_back(item);
        auto child = row.build();
        MapBuilder map{ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta()), child.binding()};
        map.set_item(Value{key}.view(), child.view());
        return map.build();
    };
    CHECK_OUTPUT(eval_node<runtime::operators::atomic_map_recipe>(values<Str>("row", none, "other"), values<Int>(1, none, 2)),
        values<Value>(snapshot("row", 1), none, snapshot("other", 2)));
    SetBuilder members{ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta())};
    members.insert(Value{Str{"alpha"}}.view());
    members.insert(Value{Str{"beta"}}.view());
    CHECK_OUTPUT(eval_node<runtime::operators::atomic_set_recipe>(values<Str>("alpha"), values<Str>("beta")),
        values<Value>(members.build()));
    CHECK_THROWS_WITH(eval_node<runtime::operators::atomic_set_recipe>(values<Str>("same"), values<Str>("same")),
        Catch::Matchers::ContainsSubstring("duplicate"));
}

TEST_CASE("generated map defaults retain ordinary struct children", "[codegen][runtime][atomic-collections]") {
    session();
    const auto recorded = eval_node<runtime::operators::atomic_struct_map_default>(values<Int>(1));
    REQUIRE(recorded.size() == 1);
    REQUIRE(recorded[0]);
    const auto map = recorded[0]->as_bundle().field("children").as_map();
    REQUIRE(map.size() == 1);
    const auto child = map.at(Value{Str{"x"}}.view()).as_bundle();
    CHECK(child.field("amount").checked_as<Int>() == 42);
    CHECK(child.field("values").as_list().empty());
}

TEST_CASE("generated map defaults retain the type of empty nested lists", "[codegen][runtime][atomic-collections]") {
    session();
    const auto recorded = eval_node<runtime::operators::atomic_collection_default>(values<Int>(1));
    REQUIRE(recorded.size() == 1);
    REQUIRE(recorded[0]);
    const auto map = recorded[0]->as_bundle().field("values").as_map();
    REQUIRE(map.size() == 1);
    const auto child = map.at(Value{Str{"empty"}}.view());
    CHECK(child.as_list().empty());
    CHECK(child.schema() == scalar_descriptor<hgl::ordinary::List<Int>>::value_meta());
}

TEST_CASE("prepared key aliases never repeat provider construction", "[codegen][runtime][prepared-keys]") {
    struct CountingProvider final : TimeZoneProvider {
        std::shared_ptr<const TimeZoneProvider> underlying{make_time_zone_provider()};
        mutable std::size_t contains_calls{};
        std::string_view version() const noexcept override { return underlying->version(); }
        bool contains(ZoneId zone) const noexcept override { ++contains_calls; return underlying->contains(zone); }
        OffsetInfo at(Instant instant, ZoneId zone) const override { return underlying->at(instant, zone); }
        LocalResolution resolve(CivilDateTime local, ZoneId zone) const override { return underlying->resolve(local, zone); }
    };
    session();
    GlobalState state;
    auto provider = std::make_shared<CountingProvider>();
    set_time_zone_provider(state.view(), provider);
    GlobalContext context{state};
    auto retained = runtime::hgl_values::prepared_zone_key_recipe_hgl_value();
    REQUIRE(provider->contains_calls == 1);
    const auto payload = retained.as_bundle().at(0);
    Value copied{payload};
    CHECK_OUTPUT((eval_node<runtime::operators::prepared_zone_key_forward, TSD<ZoneId, TS<ZoneId>>>(values<Value>(copied, none, copied))),
        values<Value>(copied, none, copied));
    CHECK(provider->contains_calls == 1);
    auto another = runtime::hgl_values::prepared_zone_key_recipe_hgl_value();
    CHECK(provider->contains_calls == 2);
    CHECK(another.equals(retained));
}

TEST_CASE("generated growing lists publish tail removal and regrowth", "[codegen][runtime][growing-list]") {
    session();
    auto expected = values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}),
        dynamic_list_delta<TS<Int>>({{0, 1}}, {1}), dynamic_list_delta<TS<Int>>({}, {0}),
        dynamic_list_delta<TS<Int>>({{0, 3}}));
    CHECK_OUTPUT((eval_node<runtime::operators::growing_forward, TSL<TS<Int>, unbounded_tsl_size>>(expected)), expected);
    CHECK_OUTPUT(eval_node<runtime::operators::growing_source>(), expected);
}

TEST_CASE("concrete rolling hooks publish returns assignments and source arrivals", "[codegen][runtime][rolling]") {
    session();
    const auto arrivals = values<Value>(Value{Int{10}}, none, Value{Int{10}}, Value{Int{20}});
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_concrete_return, TSW<Int, 2, 2>>(arrivals)), arrivals);
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_concrete_assign, TSWDuration<Int, 5, 1>>(arrivals)), arrivals);
    CHECK_OUTPUT(eval_node<runtime::operators::rolling_concrete_source>(), values<Int>(10, 10, 20));
    using Payload = hgl::ordinary::List<Int>;
    const hgl::ordinary::PreparedValuePlan list_plan{scalar_descriptor<Payload>::value_meta()};
    auto empty = list_plan.empty_list();
    auto one = list_plan.empty_list();
    list_plan.push(one.view(), Value{Int{1}}.view());
    const auto lists = values<Value>(one, empty, none, one);
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_concrete_list, TSW<Payload, 2>>(lists)), lists);
}

TEST_CASE("generated rolling publication forwards arrivals before readiness", "[codegen][runtime][rolling]") {
    session();
    const auto arrivals = values<Value>(Value{Int{10}}, none, Value{Int{10}}, Value{Int{20}});
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_ticks_forward, TSW<Int, 2, 2>>(arrivals)), arrivals);
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_observed_ticks, TSW<Int, 2, 2>>(arrivals)),
        values<Bool>(false, none, true, true));
    const auto duration_arrivals = values<Value>(Value{Int{10}}, none, Value{Int{20}}, none, none, none, none, none, Value{Int{30}});
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_duration_forward, TSWDuration<Int, 5, 1>>(duration_arrivals)), duration_arrivals);
    CHECK_OUTPUT((eval_node<runtime::operators::rolling_observed_duration, TSWDuration<Int, 5, 1>>(duration_arrivals)),
        values<Bool>(false, none, true, none, none, none, none, none, false));
}

TEST_CASE("generated complete optional snapshots replace field presence", "[codegen][runtime][optional]") {
    session();
    const auto recorded = eval_node<runtime::operators::optional_publication_source>(values<Int>(1, 2, 2, 1));
    REQUIRE(recorded.size() == 4);
    for (const auto &value : recorded) { REQUIRE(value); }
    const auto present = recorded[0]->as_bundle();
    CHECK(present.field("count").checked_as<Int>() == 0);
    CHECK(present.field("items").as_list().empty());
    for (std::size_t i : {1U, 2U}) {
        CHECK_FALSE(recorded[i]->as_bundle().element_valid(0));
        CHECK_FALSE(recorded[i]->as_bundle().element_valid(1));
    }
    CHECK(recorded[0]->equals(*recorded[3]));
    CHECK_FALSE(recorded[0]->equals(*recorded[1]));
    using Payload = runtime::NativeOptionalPublication::value_type;
    CHECK_OUTPUT((eval_node<runtime::operators::optional_publication_forward, TS<Payload>>(recorded)), recorded);
}

TEST_CASE("generated family publications retain concrete tags and optional fields", "[codegen][runtime][family]") {
    session();
    const auto recorded = eval_node<runtime::operators::native_family_source>(values<Int>(1, 2, 2, 1));
    REQUIRE(recorded.size() == 4);
    for (const auto &value : recorded) { REQUIRE(value); }
    const auto first = recorded[0]->view().concrete();
    const auto second = recorded[1]->view().concrete();
    CHECK(first.schema() == scalar_descriptor<runtime::NativePublicationFirst::value_type>::value_meta());
    CHECK(second.schema() == scalar_descriptor<runtime::NativePublicationSecond::value_type>::value_meta());
    CHECK_FALSE(first.as_bundle().element_valid(2));
    CHECK(second.as_bundle().field("count").checked_as<Int>() == 0);
    CHECK_FALSE(recorded[0]->equals(*recorded[1]));
    CHECK(recorded[1]->equals(*recorded[2]));
    using Family = runtime::NativePublicationFamily::value_type;
    CHECK_OUTPUT((eval_node<runtime::operators::native_family_forward, TS<Family>>(recorded)), recorded);
    const auto cold = runtime::hgl_values::native_family_capture_hgl_value();
    CHECK(cold.view().concrete().schema() == first.schema());
}

TEST_CASE("generated composite keys preserve optional identity and repeated updates", "[codegen][runtime][composite-keys]") {
    session();
    auto recipe = runtime::hgl_values::native_composite_key_recipe_hgl_value();
    Value payload{recipe.as_bundle().at(0)};
    using Key = runtime::NativeCompositeKey::value_type;
    const auto updates = values<Value>(payload, none, payload);
    const auto actual = eval_node<runtime::operators::native_composite_key_forward, TSD<Key, TS<Int>>>(updates);
    REQUIRE(actual[0]);
    const auto a = actual[0]->as_bundle().at(1).as_map();
    const auto b = payload.as_bundle().at(1).as_map();
    for (const auto entry : a) {
        const auto &key = entry.first;
        INFO("key hash=" << key.hash() << " schema=" << key.schema()->name());
        CHECK(b.contains(key));
    }
    CHECK_OUTPUT(actual, updates);
    CHECK(payload.as_bundle().at(1).as_map().size() == 2);
}

TEST_CASE("composite provider keys retain one cold initializer across aliases", "[codegen][runtime][composite-keys]") {
    struct CountingProvider final : TimeZoneProvider {
        std::shared_ptr<const TimeZoneProvider> underlying{make_time_zone_provider()};
        mutable std::size_t contains_calls{};
        std::string_view version() const noexcept override { return underlying->version(); }
        bool contains(ZoneId zone) const noexcept override { ++contains_calls; return underlying->contains(zone); }
        OffsetInfo at(Instant instant, ZoneId zone) const override { return underlying->at(instant, zone); }
        LocalResolution resolve(CivilDateTime local, ZoneId zone) const override { return underlying->resolve(local, zone); }
    };
    session();
    GlobalState state;
    auto provider = std::make_shared<CountingProvider>();
    set_time_zone_provider(state.view(), provider);
    GlobalContext context{state};
    const auto recipe = runtime::hgl_values::native_zone_key_recipe_hgl_value();
    CHECK(provider->contains_calls == 1);
    const auto members = recipe.as_bundle().at(0).as_bundle().at(0).as_set();
    REQUIRE(members.size() == 1);
    for (const auto member : members) {
        CHECK(member.as_bundle().field("zone").checked_as<ZoneId>() == ZoneId{"US/Eastern"});
    }
    CHECK(provider->contains_calls == 1);
}

TEST_CASE("generated family registration includes unused concrete members", "[codegen][runtime][family]") {
    session();
    const auto *family = scalar_descriptor<runtime::NativeUnusedFamily::value_type>::value_meta();
    const auto snapshot = TypeRealizationSnapshot::capture(TypeRegistry::instance());
    const auto &members = snapshot->alternatives(family);
    REQUIRE(members.size() == 1);
    CHECK(members.front()->name() == "hgl.codegen.runtime::NativeUnusedMember");
}

TEST_CASE("generated family plans capture providers installed after their module", "[codegen][runtime][family]") {
    session();
    using Family = runtime::NativeLateFamily::value_type;
    const auto register_member = [] {
        return TypeRegistry::instance().bundle("native.late", "Member", {{"label", scalar_descriptor<Str>::value_meta()}},
            {scalar_descriptor<Family>::value_meta()});
    };
    auto provider = OperatorRegistry::instance().register_installer("native.late", [register_member] { (void)register_member(); });
    const auto *member = register_member();
    BundleBuilder builder{ValuePlanFactory::instance().type_for(member)};
    builder.set(0, Value{Str{"late"}}.view());
    const auto concrete = builder.build();
    const auto captured = runtime::hgl_values::native_late_capture_hgl_value(concrete.view());
    CHECK(captured.view().concrete().schema() == member);
    CHECK_OUTPUT((eval_node<runtime::operators::native_late_forward, TS<Family>>(values<Value>(captured, none, captured))),
                 values<Value>(captured, none, captured));
    CHECK(OperatorRegistry::instance().remove_provider(provider));
}

TEST_CASE("generated ordinary subfamilies retain their live concrete member", "[codegen][runtime][family]") {
    session();
    const auto captured = runtime::hgl_values::native_subfamily_capture_hgl_value();
    CHECK(captured.view().concrete().schema() == scalar_descriptor<runtime::NativePublicationLeaf::value_type>::value_meta());
    CHECK(captured.view().concrete().as_bundle().field("items").as_list().size() == 1);
}
