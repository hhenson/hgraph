#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/mesh_node.h>
#include <hgraph/runtime/node.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/lib/testing/mock_runtime.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/wired_fn.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

using namespace hgraph;

struct FixtureDerivedOutputView : TSOutputView
{
    FixtureDerivedOutputView() = default;
    explicit FixtureDerivedOutputView(TSOutputView view) : TSOutputView(std::move(view)) {}
};

struct FixtureSubscriber : Notifiable
{
    void notify(DateTime) override {}
};

FixtureSubscriber fixture_subscriber{};

Value fixture_atomic_value{};
Value fixture_string_value{};
Value fixture_bundle_value{};
Value fixture_list_value{};
Value fixture_set_value{};
Value fixture_map_value{};
TSOutput fixture_tss_output{};
TSOutput fixture_tsd_output{};
TSOutput fixture_tsb_output{};
NodeValue fixture_node_value{};
std::unique_ptr<testing::MockRootGraph> fixture_root_graph{};
AnyPtr fixture_atomic_pointer{};
AnyPtr fixture_string_pointer{};
AnyPtr fixture_bundle_pointer{};
AnyPtr fixture_list_pointer{};
AnyPtr fixture_set_pointer{};
AnyPtr fixture_map_pointer{};
TSOutputView fixture_tss_view{};
TSOutputView fixture_tsd_view{};
TSOutputView fixture_tsb_view{};
AnyPtr fixture_node_pointer{};
AnyPtr fixture_graph_pointer{};
AnyPtr fixture_nested_graph_pointer{};
AnyPtr fixture_switch_node_pointer{};
AnyPtr fixture_map_node_pointer{};
AnyPtr fixture_mesh_node_pointer{};
ValueView fixture_value_view{};
ValueTypeRef fixture_value_type{};
TSDataView fixture_ts_data_view{};
TSInputView fixture_ts_input_view{};
TSOutputView fixture_ts_output_view{};
FixtureDerivedOutputView fixture_derived_output_view{};
NodeView fixture_node_view{};
GraphView fixture_graph_view{};
AnyPtr fixture_typed_null_pointer{};
AnyPtr fixture_malformed_pointer{};
std::unique_ptr<TypeRecord> fixture_invalid_record_owner{};
TypeRecord *fixture_invalid_record{};
DebugDescriptor fixture_invalid_descriptor{};
std::unique_ptr<GraphExecutorValue> fixture_nested_executor{};

#if defined(_MSC_VER)
#define HGRAPH_DEBUGGER_NOINLINE __declspec(noinline)
#else
#define HGRAPH_DEBUGGER_NOINLINE __attribute__((noinline))
#endif

extern "C" HGRAPH_DEBUGGER_NOINLINE void hgraph_debugger_fixture_stop()
{
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" ::: "memory");
#endif
}

namespace
{
    using namespace hgraph::testing;

    struct DebuggerNestedIdentity
    {
        static constexpr auto name = "debugger_nested_identity";

        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out) { out.set(ts.value()); }
    };

    struct DebuggerNestedSink
    {
        static constexpr auto name = "debugger_nested_sink";
        static inline std::size_t evaluations{};

        static void eval(In<"switched", TS<Int>>,
                         In<"mapped", TSD<Int, TS<Int>>>,
                         In<"meshed", TSD<Int, TS<Int>>>)
        {
            if (++evaluations == 4) { hgraph_debugger_fixture_stop(); }
        }
    };
}  // namespace

int main()
{
    using namespace hgraph::testing;

    stdlib::register_standard_operators();
    auto &registry = TypeRegistry::instance();
    const auto *int_schema = registry.register_scalar<std::int32_t>("int32");
    const auto *bool_schema = registry.register_scalar<bool>("bool");

    fixture_atomic_value = Value{std::int32_t{42}};
    fixture_string_value = Value{Str{"debugger string"}};
    auto atomic_view = fixture_atomic_value.view();
    fixture_value_view = fixture_atomic_value.view();
    fixture_value_type = fixture_atomic_value.type();
    fixture_atomic_pointer = AnyPtr::read_only(*atomic_view.record(), atomic_view.data());
    auto string_view = fixture_string_value.view();
    fixture_string_pointer = AnyPtr::read_only(*string_view.record(), string_view.data());
    fixture_typed_null_pointer = AnyPtr::typed_null(*atomic_view.record());
    fixture_invalid_record_owner = std::make_unique<TypeRecord>(*atomic_view.record());
    fixture_invalid_record = fixture_invalid_record_owner.get();
    ++fixture_invalid_record->abi_version;
    fixture_invalid_descriptor = *atomic_view.record()->debug;
    ++fixture_invalid_descriptor.abi_version;

    const std::uintptr_t malformed_words[2]{0, reinterpret_cast<std::uintptr_t>(&fixture_atomic_value)};
    static_assert(sizeof(malformed_words) == sizeof(fixture_malformed_pointer));
    std::memcpy(&fixture_malformed_pointer, malformed_words, sizeof(malformed_words));

    const auto *bundle_schema = registry.bundle("DebuggerFixture", {{"number", int_schema}, {"enabled", bool_schema}});
    const ValueTypeRef bundle_type = ValuePlanFactory::instance().type_for(bundle_schema);
    BundleBuilder builder{bundle_type};
    builder.set("number", atomic_view);
    fixture_bundle_value = builder.build();
    auto bundle_view = fixture_bundle_value.view();
    fixture_bundle_pointer = AnyPtr::read_only(*bundle_view.record(), bundle_view.data());

    ListBuilder list_builder{registry.scalar_type<std::int32_t>()};
    list_builder.push_back<std::int32_t>(3);
    list_builder.push_back<std::int32_t>(5);
    list_builder.push_back<std::int32_t>(8);
    fixture_list_value = list_builder.build();
    auto list_view = fixture_list_value.view();
    fixture_list_pointer = AnyPtr::read_only(*list_view.record(), list_view.data());

    fixture_set_value = Value{ValuePlanFactory::instance().type_for(registry.mutable_set(int_schema))};
    {
        auto mutation = fixture_set_value.as_set().begin_mutation();
        static_cast<void>(mutation.add(Value{std::int32_t{7}}.view()));
        static_cast<void>(mutation.add(Value{std::int32_t{9}}.view()));
    }
    auto set_view = fixture_set_value.view();
    fixture_set_pointer = AnyPtr::read_only(*set_view.record(), set_view.data());

    const auto *map_schema = registry.mutable_map(int_schema, int_schema);
    fixture_map_value = Value{ValuePlanFactory::instance().type_for(map_schema)};
    auto map_mutation = fixture_map_value.as_map().begin_mutation();
    map_mutation.set_item(Value{std::int32_t{1}}.view(), Value{std::int32_t{10}}.view());
    map_mutation.set_item(Value{std::int32_t{2}}.view(), Value{std::int32_t{20}}.view());
    static_cast<void>(map_mutation.remove(Value{std::int32_t{2}}.view()));
    auto map_view = fixture_map_value.view();
    fixture_map_pointer = AnyPtr::read_only(*map_view.record(), map_view.data());

    const auto *ts_int = registry.ts(int_schema);
    fixture_tss_output = TSOutput{*registry.tss(int_schema)};
    {
        auto data = fixture_tss_output.data_view();
        auto mutation = data.as_set().begin_mutation(MIN_ST);
        static_cast<void>(mutation.add(Value{std::int32_t{7}}.view()));
        static_cast<void>(mutation.add(Value{std::int32_t{9}}.view()));
    }
    fixture_tss_view = fixture_tss_output.view(MIN_ST);

    fixture_tsd_output = TSOutput{*registry.tsd(int_schema, ts_int)};
    {
        auto view = fixture_tsd_output.view(MIN_ST);
        auto mutation = view.as_dict().begin_mutation(MIN_ST);
        auto element = mutation.at(Value{std::int32_t{3}}.view());
        auto element_mutation = element.begin_mutation(MIN_ST);
        static_cast<void>(element_mutation.copy_value_from(Value{std::int32_t{30}}.view()));
    }
    fixture_tsd_view = fixture_tsd_output.view(MIN_ST);

    const auto *tsb = registry.tsb("DebuggerTSBundle", {{"number", ts_int}, {"enabled", registry.ts(bool_schema)}});
    fixture_tsb_output = TSOutput{*tsb};
    const auto tsb_binding = ValuePlanFactory::instance().type_for(tsb->value_schema);
    BundleBuilder tsb_builder{tsb_binding};
    tsb_builder.set("number", Value{std::int32_t{12}});
    tsb_builder.set("enabled", Value{true});
    {
        auto mutation = fixture_tsb_output.begin_mutation(MIN_ST);
        static_cast<void>(mutation.copy_value_from(tsb_builder.build().view()));
    }
    fixture_tsb_view = fixture_tsb_output.view(MIN_ST);

    NodeTypeMetaData node_schema;
    node_schema.display_name = "debugger_fixture_node";
    node_schema.state_schema = int_schema;
    NodeBuilder node_builder = NodeBuilder::native(std::move(node_schema));
    fixture_node_value = node_builder.make_node();
    fixture_node_pointer = fixture_node_value.view().pointer().to_any();

    NodeTypeMetaData graph_node_schema;
    graph_node_schema.display_name = "debugger_fixture_graph_node";
    GraphBuilder graph_builder;
    graph_builder.label("debugger_fixture_graph")
        .add_node(NodeBuilder::native(std::move(graph_node_schema)));
    fixture_root_graph = std::make_unique<testing::MockRootGraph>(graph_builder);
    fixture_graph_pointer = fixture_root_graph->graph().pointer().to_any();

    Wiring nested_wiring;
    auto switch_key = wire<stdlib::replay_impl, TS<Str>>(nested_wiring, Str{"debugger_switch_key"});
    auto switch_value = wire<stdlib::replay_impl, TS<Int>>(nested_wiring, Str{"debugger_switch_value"});
    auto source = wire<stdlib::replay_impl, TSD<Int, TS<Int>>>(nested_wiring, Str{"debugger_source"});
    auto keys = wire<stdlib::replay_impl, TSS<Int>>(nested_wiring, Str{"debugger_keys"});

    auto switched = wire<stdlib::switch_>(
                        nested_wiring, switch_key,
                        stdlib::switch_cases({
                            {Value{Str{"left"}}, fn<DebuggerNestedIdentity>()},
                            {Value{Str{"right"}}, fn<DebuggerNestedIdentity>()},
                        }),
                        switch_value)
                        .as<TS<Int>>();
    auto mapped = wire<stdlib::map_>(nested_wiring, fn<DebuggerNestedIdentity>(), source,
                                     arg<"__keys__">(keys))
                      .as<TSD<Int, TS<Int>>>();
    auto meshed = wire<stdlib::mesh_>(nested_wiring, fn<DebuggerNestedIdentity>(), source,
                                      arg<"__keys__">(keys))
                      .as<TSD<Int, TS<Int>>>();
    static_cast<void>(wire<DebuggerNestedSink>(nested_wiring, switched, mapped, meshed));

    GraphBuilder nested_builder = std::move(nested_wiring).finish();
    nested_builder.label("debugger_nested_graph");
    set_replay_values(nested_builder.global_state(), "debugger_switch_key",
                      std::vector<std::optional<Str>>{
                          Str{"left"}, Str{"right"}, Str{"left"}, std::nullopt});
    set_replay_values(nested_builder.global_state(), "debugger_switch_value",
                      std::vector<std::optional<Int>>{1, 2, 3, 4});
    set_replay_deltas(
        nested_builder.global_state(), "debugger_source",
        std::vector<std::optional<Value>>{
            dict_delta<Int, TS<Int>>({{11, 1}, {22, 2}, {33, 3}}),
            std::nullopt, std::nullopt, std::nullopt});
    set_replay_deltas(
        nested_builder.global_state(), "debugger_keys",
        std::vector<std::optional<Value>>{
            set_delta<Int>({11}, {}),
            set_delta<Int>({22}, {}),
            set_delta<Int>({}, {11}),
            set_delta<Int>({33}, {22})});

    GraphExecutorBuilder nested_executor_builder;
    nested_executor_builder.graph_builder(std::move(nested_builder))
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});
    fixture_nested_executor =
        std::make_unique<GraphExecutorValue>(nested_executor_builder.make_executor());

    auto nested_graph = fixture_nested_executor->view().graph();
    fixture_graph_view = GraphView{nested_graph.pointer()};
    fixture_nested_graph_pointer = nested_graph.pointer().to_any();
    bool found_switch = false;
    bool found_map    = false;
    bool found_mesh   = false;
    for (std::size_t i = 0; i < nested_graph.node_count(); ++i)
    {
        auto node = nested_graph.node_at(i);
        if (!fixture_node_view.valid() && node.has_input() && node.has_output())
        {
            fixture_node_view = NodeView{node.pointer()};
            fixture_ts_input_view = node.input(MIN_ST);
            fixture_ts_output_view = node.output(MIN_ST);
            fixture_ts_output_view.subscribe(&fixture_subscriber);
            fixture_derived_output_view = FixtureDerivedOutputView{node.output(MIN_ST)};
            fixture_ts_data_view = fixture_ts_output_view.data_view().borrowed_ref();
        }
        if (node.is<SwitchNodeView>())
        {
            fixture_switch_node_pointer = node.pointer().to_any();
            found_switch = true;
        }
        if (node.is<MapNodeView>())
        {
            fixture_map_node_pointer = node.pointer().to_any();
            found_map = true;
        }
        if (node.is<MeshNodeView>())
        {
            fixture_mesh_node_pointer = node.pointer().to_any();
            found_mesh = true;
        }
    }
    if (!found_switch || !found_map || !found_mesh) { return 2; }

    DebuggerNestedSink::evaluations = 0;
    fixture_nested_executor->view().run();

    auto stopped_graph = fixture_nested_executor->view().graph();
    for (std::size_t i = 0; i < stopped_graph.node_count(); ++i)
    {
        auto node = stopped_graph.node_at(i);
        if (node.is<SwitchNodeView>())
        {
            auto view = node.as<SwitchNodeView>();
            if (view.stored_graph_count() != 2 || !view.child_graphs_use_in_place_storage()) { return 3; }
        }
        if (node.is<MapNodeView>())
        {
            auto view = node.as<MapNodeView>();
            if (view.child_graph_count() != 2 || !view.child_graphs_use_in_place_storage()) { return 4; }
        }
        if (node.is<MeshNodeView>())
        {
            auto view = node.as<MeshNodeView>();
            if (view.child_graph_count() != 2 || !view.child_graphs_use_in_place_storage()) { return 5; }
        }
    }

    hgraph_debugger_fixture_stop();
    const int result = fixture_atomic_value.view().checked_as<std::int32_t>() == 42 ? 0 : 1;

    // These debugger-visible owners are globals so LLDB/GDB can find them, but
    // their type registries are function statics first constructed in main.
    // Destroy the owners before main exits rather than relying on the reverse
    // order of unrelated static teardown.
    fixture_nested_executor.reset();
    fixture_root_graph.reset();
    fixture_node_value = NodeValue{};
    fixture_map_value = Value{};
    fixture_set_value = Value{};
    fixture_tss_view = TSOutputView{};
    fixture_tsd_view = TSOutputView{};
    fixture_tsb_view = TSOutputView{};
    fixture_tss_output = TSOutput{};
    fixture_tsd_output = TSOutput{};
    fixture_tsb_output = TSOutput{};
    fixture_list_value = Value{};
    fixture_bundle_value = Value{};
    fixture_atomic_value = Value{};
    fixture_invalid_record_owner.reset();
    return result;
}
