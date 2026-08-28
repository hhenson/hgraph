#include "fabric_extension_probe.h"

#include <hgraph/lib/std/standard_types.h>
#include <hgraph/lib/std/operators/io.h>
#include <hgraph/lib/std/operators/table.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/runtime/registry_snapshot.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/storage_metrics.h>
#include <hgraph/types/table_type_ops.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/visitor.h>
#include <hgraph/types/type_pointer.h>
#include <hgraph/types/type_resolution.h>
#include <hgraph/types/utils/stable_slot_store.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/polymorphic_value_type.h>
#include <hgraph/types/value/shared_value_pool.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_hash.h>
#include <hgraph/types/value/visitor.h>

#include <arrow/api.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

namespace
{
    struct ConsumerExtensionScalar
    {
        std::int32_t value{0};
    };

    struct ConsumerReplylessService
    {
        static constexpr std::string_view name{"consumer_replyless"};
        using request_schema = hgraph::TS<hgraph::Int>;
    };

    struct ConsumerReplylessSink
    {
        static constexpr auto name = "consumer_replyless_sink";

        static void eval(hgraph::In<"requests", hgraph::TSD<hgraph::Int, hgraph::TS<hgraph::Int>>,
                                    hgraph::InputValidity::Unchecked>)
        {
        }
    };

    struct ConsumerReplylessGraph
    {
        static hgraph::Port<hgraph::TS<hgraph::Int>> compose(
            hgraph::Wiring &w, hgraph::Port<hgraph::TS<hgraph::Int>> request)
        {
            hgraph::service::register_request_reply_service<ConsumerReplylessService,
                                                            ConsumerReplylessSink>(w);
            static_assert(
                std::same_as<decltype(hgraph::wire<ConsumerReplylessService>(w, request)), void>);
            hgraph::wire<ConsumerReplylessService>(w, request);
            return request;
        }
    };

    void describe_consumer_table(hgraph::TableLayout &, const hgraph::TSValueTypeMetaData *,
                                 std::string, std::vector<std::size_t>, std::size_t)
    {
    }

    void emit_consumer_table(const hgraph::TableLayout &, const hgraph::TSInputView &, hgraph::Int,
                             hgraph::DateTime, hgraph::DateTime, bool,
                             const hgraph::TableRowSink &, bool)
    {
    }

    void apply_consumer_table(const hgraph::TableLayout &, const hgraph::TableRowSource &,
                              const hgraph::TSOutputView &)
    {
    }

    const hgraph::TableTypeOps consumer_table_ops{
        &describe_consumer_table,
        &emit_consumer_table,
        &apply_consumer_table,
    };

    void check_push_source_queue_contract()
    {
        using namespace hgraph;

        auto &registry = TypeRegistry::instance();
        const auto *integer = scalar_descriptor<Int>::value_meta();
        const auto *ts_int = registry.ts(integer);
        const auto *ts_tuple = registry.ts(registry.list(integer, 0, true));

        const auto check_capacity = [](const TSValueTypeMetaData &output_schema,
                                       PushSourcePolicy policy) {
            PushSourceSender sender;
            GraphBuilder graph_builder;
            graph_builder.add_node(make_push_source_node(
                output_schema, std::move(policy),
                [&sender](PushSourceSender started) {
                    sender = std::move(started);
                }));

            GraphExecutorBuilder executor_builder;
            executor_builder.graph_builder(std::move(graph_builder))
                .mode(GraphExecutorMode::RealTime)
                .start_time(MIN_ST)
                .end_time(MIN_ST + TimeDelta{1});
            auto executor = executor_builder.make_executor();
            auto graph = executor.view().graph();
            graph.start(MIN_ST);
            if (!sender.try_send(Int{1}) || sender.try_send(Int{2}))
            {
                throw std::runtime_error(
                    "installed bounded push-source sender does not report capacity");
            }
            graph.stop(MIN_ST);
            if (sender.valid() || sender.try_send(Int{3}))
            {
                throw std::runtime_error(
                    "installed push-source sender remains valid after stop");
            }
        };

        check_capacity(*ts_int, make_push_source_queue_policy(*ts_int, 1));
        check_capacity(*ts_tuple, make_push_source_burst_policy(*ts_tuple, 1));
    }

    void check_value_hash_contract()
    {
        using namespace hgraph;

        Value stored{Int{42}};
        Value lookup{Int{42}};
        auto lookup_view = lookup.view();
        std::unordered_set<Value, ValueHash, ValueEqual> values;
        values.insert(stored);
        if (!values.contains(lookup_view))
        {
            throw std::runtime_error(
                "installed ValueHash/ValueEqual cannot perform borrowed lookup");
        }
    }

    // ------------------------------------------------------------------
    // A trivial EXTERNAL record/replay backend (RFC 0025 checkpoint 3):
    // registers overloads of the core operator markers through public
    // installed headers only, guarded on its own backend id. Recordings
    // are plain GlobalState scalars (":probe:<key>.<i>" + a count) —
    // deliberately naive; the point is the seam, not the storage.
    // ------------------------------------------------------------------

    constexpr std::string_view kProbeBackend{"probe.mem"};

    [[nodiscard]] std::string probe_key(std::string_view key, std::string_view suffix)
    {
        return ":probe:" + std::string{key} + "." + std::string{suffix};
    }

    [[nodiscard]] hgraph::Int probe_count(hgraph::GlobalStateView gs, std::string_view key)
    {
        const auto value = gs.get(probe_key(key, "n"));
        return value.valid() ? value.checked_as<hgraph::Int>() : hgraph::Int{0};
    }

    struct probe_record_impl
    {
        static constexpr auto name = "probe_record";

        static bool requires_(const hgraph::ResolutionMap &, hgraph::OperatorCallContext context)
        {
            return hgraph::record_replay::effective_backend_is(context, kProbeBackend);
        }

        static auto defaults()
        {
            return std::tuple{hgraph::arg<"key">(hgraph::Str{"out"}),
                              hgraph::arg<"recordable_id">(hgraph::Str{""}),
                              hgraph::arg<"model">(hgraph::Str{})};
        }

        static void eval(hgraph::In<"ts", hgraph::TsVar<"S">, hgraph::InputValidity::Unchecked> ts,
                         hgraph::Scalar<"key", hgraph::Str> key,
                         hgraph::Scalar<"recordable_id", hgraph::Str>,
                         hgraph::Scalar<"model", hgraph::Str>, hgraph::GlobalStateView gs)
        {
            if (!ts.modified()) { return; }
            const hgraph::Int n = probe_count(gs, key.value());
            gs.set(probe_key(key.value(), std::to_string(n)), hgraph::Value{ts.value()});
            gs.set(probe_key(key.value(), "n"), hgraph::Value{hgraph::Int{n + 1}});
        }
    };

    struct probe_replay_impl
    {
        static constexpr auto name              = "probe_replay";
        static constexpr bool schedule_on_start = true;

        static bool requires_(const hgraph::ResolutionMap &, hgraph::OperatorCallContext context)
        {
            return hgraph::record_replay::effective_backend_is(context, kProbeBackend);
        }

        static auto defaults()
        {
            return std::tuple{hgraph::arg<"recordable_id">(hgraph::Str{""}),
                              hgraph::arg<"model">(hgraph::Str{})};
        }

        static void eval(hgraph::Scalar<"key", hgraph::Str> key,
                         hgraph::Scalar<"recordable_id", hgraph::Str>,
                         hgraph::Scalar<"model", hgraph::Str>, hgraph::GlobalStateView gs,
                         hgraph::NodeScheduler sched, hgraph::Out<hgraph::TsVar<"O">> out)
        {
            const hgraph::Int n = probe_count(gs, key.value());
            const auto cursor_key = probe_key(key.value(), "i");
            const auto cursor = gs.get(cursor_key);
            const hgraph::Int i = cursor.valid() ? cursor.checked_as<hgraph::Int>() : hgraph::Int{0};
            if (i >= n) { return; }
            hgraph::apply_delta(out, gs.get(probe_key(key.value(), std::to_string(i))));
            gs.set(cursor_key, hgraph::Value{hgraph::Int{i + 1}});
            if (i + 1 < n) { sched.schedule(hgraph::MIN_TD); }
        }
    };

    void install_probe_backend()
    {
        hgraph::register_overload<hgraph::stdlib::record, probe_record_impl>();
        hgraph::register_overload<hgraph::stdlib::replay, probe_replay_impl>();
    }

    void check_probe_backend_round_trip()
    {
        using namespace hgraph;

        // Registration intent recorded once; the rebuild call replays it —
        // including after a full registry reset (the extension contract).
        OperatorRegistry::instance().register_installer("hgraph.test.probe",
                                                        &install_probe_backend);
        OperatorRegistry::instance().run_installers();

        const auto run_round_trip = [] {
            Wiring wiring;
            record_replay::set_config(
                wiring.global_state(),
                record_replay::RecordReplayConfig{.backend = std::string{kProbeBackend}});
            auto source = wire<stdlib::replay, TS<Int>>(wiring, Str{"in"});
            wire<stdlib::record>(wiring, source, Str{"out"});

            GraphBuilder graph_builder = std::move(wiring).finish();
            const auto   seed          = graph_builder.global_state();
            seed.set(probe_key("in", "0"), Value{Int{11}});
            seed.set(probe_key("in", "1"), Value{Int{22}});
            seed.set(probe_key("in", "n"), Value{Int{2}});

            GraphExecutorBuilder executor_builder;
            executor_builder.graph_builder(std::move(graph_builder))
                .start_time(MIN_ST)
                .end_time(MIN_ST + MIN_TD * 6);
            GraphExecutorValue executor = executor_builder.make_executor();
            auto               view     = executor.view();
            view.run();

            const auto state = view.graph().global_state();
            if (probe_count(state, "out") != 2 ||
                state.get(probe_key("out", "0")).checked_as<Int>() != 11 ||
                state.get(probe_key("out", "1")).checked_as<Int>() != 22)
            {
                throw std::runtime_error(
                    "probe record/replay backend did not round trip through the installed SDK");
            }
        };

        run_round_trip();

        // The reset-and-rebuild replay of this installer is covered by the
        // in-tree installer tests and the Python extension consumer:
        // ``reset_all_registries()`` invalidates the interned metadata every
        // live value in this main() still points at, so a full reset cannot
        // run mid-consumer (the documented test-only reset hazard).
    }
}  // namespace

int main()
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<SchemaHeader>);
    static_assert(std::is_trivially_copyable_v<SchemaHeader>);
    static_assert(std::is_standard_layout_v<TypeRecord>);
    static_assert(std::is_trivially_copyable_v<TypeRecord>);
    static_assert(std::is_standard_layout_v<AnyPtr>);
    static_assert(std::is_trivially_copyable_v<AnyPtr>);
    // ABI 5 adds the cold-path compiled-child inspection contract.
    static_assert(NODE_OPS_ABI_VERSION == 5);
    static_assert(std::is_standard_layout_v<ChildGraphInspectionOps>);
    static_assert(std::is_trivially_copyable_v<ChildGraphInspectionOps>);
    static_assert(GRAPH_OPS_ABI_VERSION == 8);
    static_assert(EXECUTOR_OPS_ABI_VERSION == 5);
    // ABI 12: keyed and window TSData projections return binding and memory together.
    static_assert(TS_DATA_OPS_ABI_VERSION == 12);
    static_assert(sizeof(PolymorphicValueType) == 2 * sizeof(void *));
    static_assert(std::is_standard_layout_v<PolymorphicValueType>);
    static_assert(!std::is_polymorphic_v<TableTypeOps>);
    static_assert(std::is_standard_layout_v<TableTypeOps>);

    const TableTypeOps &empty_table_ops = empty_table_type_ops();
    if (empty_table_ops.describe == nullptr || empty_table_ops.emit == nullptr ||
        empty_table_ops.apply == nullptr)
    {
        throw std::runtime_error("installed table type-ops no-op contract is incomplete");
    }

    // The erased frame-store contract moved to hgraph-persistence
    // (RFC 0025 checkpoint 4); its installed consumer covers it.

    PolymorphicValueType empty_polymorphic_type;
    if (empty_polymorphic_type.binding())
    {
        throw std::runtime_error("installed polymorphic value type no-op contract is unusable");
    }

    GraphBuilder inline_graph;
    if (inline_graph.root_type().checked_plan().find_component("compound_scalar_storage") !=
        nullptr)
    {
        throw std::runtime_error("default installed graph unexpectedly planned pooled storage");
    }
    GraphBuilder pooled_graph;
    set_pooled_compound_scalar_storage(pooled_graph.global_state());
    if (!pooled_graph.type_realization()->pooled_compound_storage_enabled() ||
        pooled_graph.root_type().checked_plan().find_component("compound_scalar_storage") !=
            nullptr)
    {
        throw std::runtime_error(
            "installed pooled graph configuration retained root-owned storage");
    }

    StableSlotStore<StableSlotStateModel::ConstructedAndLive> stable_slots{
        MemoryUtils::StorageLayout{sizeof(std::uintptr_t), alignof(std::uintptr_t)}};
    stable_slots.reserve_to(4);
    auto *stable_value = std::construct_at(
        MemoryUtils::cast<std::uintptr_t>(stable_slots.slot_memory(0)), std::uintptr_t{41});
    stable_slots.mark_staged(0);
    if (!stable_slots.mark_live(0) ||
        stable_slots.representation() != StableSlotRepresentation::TaggedPointer ||
        stable_slots.live_slot_memory(0) != stable_value)
    {
        throw std::runtime_error("installed stable-slot tagged strategy is unusable");
    }
    std::destroy_at(stable_value);
    stable_slots.mark_free(0);

    StableSlotStore<StableSlotStateModel::ConstructedOnly> weak_stable_slots{
        MemoryUtils::StorageLayout{3, 1}};
    weak_stable_slots.reserve_to(4);
    if (weak_stable_slots.representation() != StableSlotRepresentation::Bitmap)
    {
        throw std::runtime_error("installed stable-slot bitmap strategy is unusable");
    }

    using ConsumerScalar = Bundle<"consumer::Scalar", Field<"number", Int>, Field<"label", Str>>;
    using SharedConsumerScalar = Shared<ConsumerScalar>;
    using ConsumerBundle = TSBFromScalar<ConsumerScalar>;
    static_assert(
        std::is_same_v<ConsumerBundle,
                       TSB<"consumer::Scalar", Field<"number", TS<Int>>, Field<"label", TS<Str>>>>);

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<std::int32_t>("int32");

    const auto *consumer_scalar_schema = scalar_descriptor<ConsumerScalar>::value_meta();
    const auto *shared_consumer_schema = scalar_descriptor<SharedConsumerScalar>::value_meta();
    if (!shared_consumer_schema->is_shared() ||
        shared_consumer_schema->element_type != consumer_scalar_schema)
    {
        throw std::runtime_error("installed Shared<T> descriptor is unusable");
    }
    const auto live_shared_values = shared_value_pool_metrics().live_values;
    {
        const auto consumer_scalar_type =
            ValuePlanFactory::instance().type_for(consumer_scalar_schema);
        const auto shared_consumer_type =
            ValuePlanFactory::instance().type_for(shared_consumer_schema);
        Value source{consumer_scalar_type};
        auto fields = source.as_bundle().begin_mutation();
        fields["number"].set(Int{17});
        fields["label"].set(Str{"shared"});

        Value shared{shared_consumer_type, source.view()};
        Value retained = shared;
        if (shared_consumer_type.checked_plan().layout.size != sizeof(void *) ||
            shared.view().can_begin_mutation() ||
            retained.view().concrete().data() != shared.view().concrete().data() ||
            retained.as_bundle()["number"].checked_as<Int>() != 17 ||
            shared_value_pool_metrics().live_values != live_shared_values + 1)
        {
            throw std::runtime_error("installed Shared<T> value contract is unusable");
        }
    }
    if (shared_value_pool_metrics().live_values != live_shared_values)
    {
        throw std::runtime_error("installed Shared<T> final release did not recycle its slot");
    }

    const RuntimeRegistrySnapshot runtime_registries = runtime_registry_snapshot();
    if (runtime_registries.type_records == 0)
    {
        throw std::runtime_error(
            "installed runtime registry snapshot did not observe seeded types");
    }

    static const std::byte consumer_node_type_id{};
    NodeTypeDescriptor     first_node_descriptor;
    first_node_descriptor.schema.display_name = "consumer_canonical_node";
    const NodeBuilder first_node = NodeBuilder::from_canonical_descriptor(
        std::move(first_node_descriptor), &consumer_node_type_id);
    NodeTypeDescriptor repeated_node_descriptor;
    repeated_node_descriptor.schema.display_name = "consumer_canonical_node";
    const NodeBuilder repeated_node = NodeBuilder::from_canonical_descriptor(
        std::move(repeated_node_descriptor), &consumer_node_type_id);
    if (repeated_node.type() != first_node.type())
    {
        throw std::runtime_error(
            "installed canonical node descriptor did not reuse its runtime type");
    }

    Value        value{std::int32_t{41}};
    auto         view = value.view();
    const AnyPtr pointer = AnyPtr::read_only(*view.record(), view.data());
    if (!pointer.valid() || pointer.family() != TypeFamily::Value || !pointer.read_only_access())
    {
        throw std::runtime_error("installed target produced an invalid erased pointer");
    }

    value.begin_mutation().as<std::int32_t>() = 73;
    if (value.view().checked_as<std::int32_t>() != 73)
    {
        throw std::runtime_error("installed target mutation round-trip failed");
    }

    const auto *consumer_scalar =
        registry.register_scalar<ConsumerExtensionScalar>("consumer::ExtensionScalar");
    const auto *consumer_ts = registry.ts(consumer_scalar);
    register_table_type_ops(consumer_ts, consumer_table_ops);
    if (&table_type_ops(consumer_ts) != &consumer_table_ops)
    {
        throw std::runtime_error("installed table type-ops registration is unusable");
    }

    const auto *consumer_bundle =
        registry.tsb("consumer::OpaqueBundle", {{"value", consumer_ts}});
    const auto built_in_bundle_type =
        TSDataPlanFactory::instance().data_type_for(consumer_bundle).as_role();
    struct ConsumerIndexedOps : IndexedTSDataOps
    {
        std::uintptr_t extension_marker{0x48524752u};
    };
    ConsumerIndexedOps consumer_ops;
    static_cast<IndexedTSDataOps &>(consumer_ops) =
        static_cast<const IndexedTSDataOps &>(built_in_bundle_type.ops_ref());
    if (consumer_ops.inspection_ops == nullptr ||
        consumer_ops.inspection_ops->field_count_impl(consumer_ops.context) != 1)
    {
        throw std::runtime_error(
            "installed TSData inspection operations are unusable");
    }
    const auto consumer_field =
        consumer_ops.inspection_ops->field_at_impl(consumer_ops.context, 0);
    if (std::string_view{consumer_field.name} != "value" ||
        consumer_field.type.schema() != consumer_ts)
    {
        throw std::runtime_error(
            "installed TSData inspection field metadata is incorrect");
    }
    const auto consumer_type = intern_ts_type(
        *consumer_bundle, TypeRole::Data, built_in_bundle_type.checked_plan(),
        consumer_ops, "consumer.opaque.bundle.data");
    const auto projected_consumer_type =
        ts_data_plan_factory_detail::tsd_value_projection_type(
            consumer_type, TypeRole::Output);
    if (consumer_type.ops() != &consumer_ops ||
        projected_consumer_type.ops() != &consumer_ops ||
        projected_consumer_type.record() == consumer_type.record() ||
        projected_consumer_type.record()->implementation_name() !=
            "ts.tsd.value.output")
    {
        throw std::runtime_error(
            "installed opaque TS operations were not projected by identity");
    }

    Value extension{ConsumerExtensionScalar{17}};
    Value boxed{any_type()};
    boxed.as_any().begin_mutation().set(extension.view());
    const bool visited_extension = visit(
        boxed.view(),
        [](AtomicView selected) {
            return selected.holds_alternative<ConsumerExtensionScalar>() &&
                   selected.checked_as<ConsumerExtensionScalar>().value == 17;
        },
        [](ValueView) { return false; });
    if (!visited_extension)
    {
        throw std::runtime_error("installed value visitor did not unwrap and "
                                 "dispatch the extension scalar");
    }

    static_cast<void>(stdlib::register_standard_types(registry));

    static_cast<void>(scalar_descriptor<stdlib::ToTableMode>::value_meta());
    Value table_mode{stdlib::ToTableMode::Sample};
    if (table_mode.view().checked_as<stdlib::ToTableMode>() != stdlib::ToTableMode::Sample)
    {
        throw std::runtime_error("installed ToTableMode scalar contract is unusable");
    }

    Value                       long_text{Str(256, 'x')};
    const DynamicStorageMetrics value_storage = long_text.view().dynamic_storage_metrics();
    if (value_storage.live_bytes < 257 || value_storage.reserved_bytes < value_storage.live_bytes)
    {
        throw std::runtime_error("installed Value dynamic storage metrics are unusable");
    }

    const auto *ts_int = registry.ts(scalar_descriptor<Int>::value_meta());
    TSOutput    output{*ts_int};
    auto        output_view = output.view(MIN_ST);
    const bool  visited_value = visit(
        output_view,
        [](TSValueOutputView selected) { return selected.schema()->kind == TSTypeKind::TS; },
        [](TSOutputView) { return false; });
    if (!visited_value)
    {
        throw std::runtime_error(
            "installed endpoint visitor dispatched the wrong time-series kind");
    }

    const auto *tss_int = registry.tss(scalar_descriptor<Int>::value_meta());
    TSOutput    set_output{*tss_int};
    {
        auto set_data = set_output.data_view();
        auto set = set_data.as_set();
        auto mutation = set.begin_mutation(MIN_ST);
        mutation.reserve(16);
    }
    const DynamicStorageMetrics set_storage = set_output.data_view().dynamic_storage_metrics();
    if (set_storage.live_bytes == 0 || set_storage.reserved_bytes < set_storage.live_bytes)
    {
        throw std::runtime_error("installed TSData dynamic storage metrics are unusable");
    }

    using TypedFrame = FrameOf<Bundle<"consumer::Row", Field<"value", Int>>,
                               Bundle<"consumer::Metadata", Field<"revision", Int>>>;
    const auto *typed_frame = scalar_type<TypedFrame>();
    if (typed_frame == nullptr || typed_frame->key_type == nullptr)
    {
        throw std::runtime_error("installed typed frame metadata schema is unusable");
    }

    arrow::Int64Builder values;
    if (!values.Append(42).ok())
    {
        throw std::runtime_error("installed Arrow builder is unusable");
    }
    std::shared_ptr<arrow::Array> array;
    if (!values.Finish(&array).ok())
    {
        throw std::runtime_error("installed Arrow array construction failed");
    }
    Frame         frame{arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}),
                                           {std::move(array)})};
    BundleBuilder metadata{ValuePlanFactory::instance().type_for(typed_frame->key_type)};
    metadata.set(0, Value{Int{7}});
    frame = with_frame_metadata(std::move(frame), metadata.build());
    if (!frame.has_metadata() ||
        frame_metadata(frame, typed_frame->key_type).as_bundle().at(0).checked_as<Int>() != 7)
    {
        throw std::runtime_error("installed Arrow frame metadata codec is unusable");
    }

    check_probe_backend_round_trip();
    check_push_source_queue_contract();
    check_value_hash_contract();
    hgraph_install_consumer::check_fabric_core_extension_seam();

    return 0;
}
