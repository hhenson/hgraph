#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_registry.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/python_export.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <unordered_set>

namespace hgraph::v2::test_detail
{
    using PairSchema = TSB<Field<"lhs", TS<int>>, Field<"rhs", TS<int>>>;
    using DictSchema = TSD<int, TS<int>>;

    static_assert(std::same_as<typename In<"pair", PairSchema>::view_type, TSBView<TSInputView>>);
    static_assert(std::same_as<typename Out<PairSchema>::view_type, TSBView<TSOutputView>>);
    static_assert(std::same_as<typename In<"dict", DictSchema>::view_type, TSDView<TSInputView>>);
    static_assert(std::same_as<typename Out<DictSchema>::view_type, TSDView<TSOutputView>>);
    static_assert(std::same_as<decltype(std::declval<const In<"lhs", TS<int>> &>().delta_value()), const int &>);
    static_assert(std::same_as<decltype(std::declval<const In<"pair", PairSchema> &>().delta_value()), View>);

    struct NoopNode
    {
        NoopNode() = delete;
        ~NoopNode() = delete;

        static void eval() {}
    };

    struct SumScalarInputs
    {
        SumScalarInputs() = delete;
        ~SumScalarInputs() = delete;

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.delta_value() + rhs.delta_value());
        }
    };

    struct SumNestedPair
    {
        SumNestedPair() = delete;
        ~SumNestedPair() = delete;

        using Pair = PairSchema;

        static void eval(In<"pair", Pair, InputActivity::Passive, InputValidity::Unchecked> pair, Out<TS<int>> out)
        {
            const int lhs = pair.view().field("lhs").value().as_atomic().as<int>();
            const int rhs = pair.view().field("rhs").value().as_atomic().as<int>();
            out.set(lhs + rhs);
        }
    };

    struct ExportedSumNode
    {
        ExportedSumNode() = delete;
        ~ExportedSumNode() = delete;

        static constexpr auto name = "exported_sum";

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct PolicyMetadataNode
    {
        PolicyMetadataNode() = delete;
        ~PolicyMetadataNode() = delete;

        static void eval(In<"lhs", TS<int>> lhs,
                         In<"rhs", TS<int>, InputActivity::Passive, InputValidity::Unchecked> rhs,
                         In<"strict", TS<int>, InputActivity::Active, InputValidity::AllValid> strict,
                         Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value() + strict.value());
        }
    };

    struct GenericGetItemNode
    {
        GenericGetItemNode() = delete;
        ~GenericGetItemNode() = delete;

        static constexpr auto name = "generic_get_item";

        using K = ScalarVar<"K">;
        using V = TsVar<"V">;

        static void eval(In<"ts", TSD<K, V>> ts, In<"key", TS<K>> key, Out<V> out)
        {
            static_cast<void>(ts);
            static_cast<void>(key);
            static_cast<void>(out);
        }
    };

    struct SumWithTypedState
    {
        SumWithTypedState() = delete;
        ~SumWithTypedState() = delete;

        static constexpr auto name = "typed_state_sum";

        static void start(State<int> state)
        {
            state.view().set_scalar(0);
        }

        static void eval(In<"lhs", TS<int>> lhs, State<int> state, Out<TS<int>> out)
        {
            auto &sum = state.view().template checked_as<int>();
            sum += lhs.value();
            out.set(sum);
        }
    };

    struct PreviousValueFromRecordableState
    {
        PreviousValueFromRecordableState() = delete;
        ~PreviousValueFromRecordableState() = delete;

        static constexpr auto name = "previous_value";
        using LocalState = TSB<Field<"last", TS<int>>>;

        static void eval(In<"lhs", TS<int>> lhs, RecordableState<LocalState> state, Out<TS<int>> out)
        {
            auto last = state.view().field("last");
            if (last.valid()) {
                out.set(last.value().as_atomic().as<int>());
            } else {
                out.set(-1);
            }
            last.value().set_scalar(lhs.value());
            state.mark_modified(last);
        }
    };

    [[nodiscard]] engine_time_t tick(int offset)
    {
        return MIN_DT + std::chrono::microseconds{offset};
    }

    [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, int64_t slot)
    {
        if (slot < 0) { throw std::out_of_range("v2 test path navigation requires non-negative slots"); }

        switch (schema.kind) {
            case TSKind::TSB:
                if (static_cast<size_t>(slot) >= schema.field_count()) {
                    throw std::out_of_range("v2 test TSB path navigation is out of range");
                }
                return schema.fields()[slot].ts_type;

            case TSKind::TSL:
                if (schema.fixed_size() == 0) {
                    throw std::invalid_argument("v2 test path navigation does not support dynamic TSL prefixes");
                }
                if (static_cast<size_t>(slot) >= schema.fixed_size()) {
                    throw std::out_of_range("v2 test TSL path navigation is out of range");
                }
                return schema.element_ts();

            default:
                throw std::invalid_argument("v2 test output navigation only supports TSB and fixed-size TSL");
        }
    }

    [[nodiscard]] TSOutputView traverse_output(TSOutputView view, const TSMeta *schema, PathView path)
    {
        const TSMeta *current_schema = schema;
        for (const int64_t slot : path) {
            if (current_schema == nullptr) { throw std::invalid_argument("v2 test output navigation requires a schema"); }

            switch (current_schema->kind) {
                case TSKind::TSB:
                    view = view.as_bundle()[slot];
                    break;

                case TSKind::TSL:
                    view = view.as_list()[slot];
                    break;

                default:
                    throw std::invalid_argument("v2 test output navigation only supports TSB and fixed-size TSL");
            }

            current_schema = child_schema_at(*current_schema, slot);
        }

        return view;
    }

    template <typename T>
    void publish_scalar_output(Node &node, const Path &path, T &&value, engine_time_t modified_time)
    {
        auto output = traverse_output(node.output_view(modified_time), node.output_schema(), path);
        output.value().set_scalar(std::forward<T>(value));

        LinkedTSContext context = output.linked_context();
        if (context.ts_state == nullptr) { throw std::logic_error("v2 test output leaf has no state to mark modified"); }
        context.ts_state->mark_modified(modified_time);
    }

    void ensure_python_hgraph_importable()
    {
        setenv("HGRAPH_USE_CPP", "0", 1);

        if (!Py_IsInitialized()) {
#ifdef HGRAPH_TEST_PYTHON_EXECUTABLE
            static wchar_t *python_executable = Py_DecodeLocale(HGRAPH_TEST_PYTHON_EXECUTABLE, nullptr);
            static std::wstring python_home_storage = std::filesystem::path{HGRAPH_TEST_PYTHON_HOME}.wstring();
            static wchar_t *python_home = python_home_storage.data();

            Py_SetProgramName(python_executable);
            Py_SetPythonHome(python_home);
#endif
            Py_Initialize();
        }

        nb::gil_scoped_acquire guard;
        nb::module_ sys = nb::module_::import_("sys");
        nb::list path = nb::borrow<nb::list>(sys.attr("path"));
        path.insert(0, "/Users/hhenson/CLionProjects/hgraph_2");
    }
}  // namespace hgraph::v2::test_detail

TEST_CASE("v2 graph wires scalar sources into a compute node with validity gating", "[v2][graph]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *input_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Inputs");

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("rhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sum").implementation<hgraph::v2::test_detail::SumScalarInputs>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 2, .input_path = {0}})
        .add_edge(hgraph::v2::Edge{.src_node = 1, .dst_node = 2, .input_path = {1}});

    auto graph = builder.make_graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 2, hgraph::v2::test_detail::tick(1));
    CHECK(graph.scheduled_time(2) == hgraph::v2::test_detail::tick(1));
    CHECK(graph.node_at(0).output_view(hgraph::v2::test_detail::tick(1)).modified());
    CHECK_FALSE(graph.node_at(0).output_view(hgraph::v2::test_detail::tick(2)).modified());

    graph.evaluate(hgraph::v2::test_detail::tick(1));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 0);

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(1), {}, 3, hgraph::v2::test_detail::tick(2));
    CHECK(graph.scheduled_time(2) == hgraph::v2::test_detail::tick(2));

    graph.evaluate(hgraph::v2::test_detail::tick(2));
    CHECK(graph.node_at(2).output_view().value().as_atomic().as<int>() == 5);
    CHECK(graph.node_at(2).output_view(hgraph::v2::test_detail::tick(2)).modified());
    CHECK(graph.scheduled_time(2) == hgraph::MIN_DT);
}

TEST_CASE("v2 graph binds bundle output child paths into nested input paths", "[v2][graph]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);
    const auto *pair_schema = ts_registry.tsb({{"lhs", scalar_ts}, {"rhs", scalar_ts}}, "Pair");
    const auto *nested_input_schema = ts_registry.tsb({{"pair", pair_schema}}, "NestedInput");

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("pair_source").output_schema(pair_schema).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sum").input_schema(nested_input_schema).implementation<hgraph::v2::test_detail::SumNestedPair>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .output_path = {0}, .dst_node = 1, .input_path = {0, 0}})
        .add_edge(hgraph::v2::Edge{.src_node = 0, .output_path = {1}, .dst_node = 1, .input_path = {0, 1}});

    auto graph = builder.make_graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {0}, 7, hgraph::v2::test_detail::tick(11));
    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {1}, 13, hgraph::v2::test_detail::tick(11));

    graph.schedule_node(1, hgraph::v2::test_detail::tick(11));
    graph.evaluate(hgraph::v2::test_detail::tick(11));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 20);
}

TEST_CASE("v2 nodes support typed local state", "[v2][graph][state]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("sum").implementation<hgraph::v2::test_detail::SumWithTypedState>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto graph = builder.make_graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 5, hgraph::v2::test_detail::tick(31));
    graph.evaluate(hgraph::v2::test_detail::tick(31));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 5);

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 7, hgraph::v2::test_detail::tick(32));
    graph.evaluate(hgraph::v2::test_detail::tick(32));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 12);
}

TEST_CASE("v2 nodes support recordable state", "[v2][graph][state]")
{
    auto &value_registry = hgraph::value::TypeRegistry::instance();
    auto &ts_registry = hgraph::TSTypeRegistry::instance();

    const auto *int_type = value_registry.register_type<int>("int");
    const auto *scalar_ts = ts_registry.ts(int_type);

    hgraph::v2::GraphBuilder builder;
    builder
        .add_node(hgraph::v2::NodeBuilder{}.label("lhs_source").output_schema(scalar_ts).implementation<hgraph::v2::test_detail::NoopNode>())
        .add_node(hgraph::v2::NodeBuilder{}.label("previous").implementation<hgraph::v2::test_detail::PreviousValueFromRecordableState>())
        .add_edge(hgraph::v2::Edge{.src_node = 0, .dst_node = 1, .input_path = {0}});

    auto graph = builder.make_graph();
    graph.start();

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 11, hgraph::v2::test_detail::tick(41));
    graph.evaluate(hgraph::v2::test_detail::tick(41));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == -1);

    hgraph::v2::test_detail::publish_scalar_output(graph.node_at(0), {}, 13, hgraph::v2::test_detail::tick(42));
    graph.evaluate(hgraph::v2::test_detail::tick(42));
    CHECK(graph.node_at(1).output_view().value().as_atomic().as<int>() == 11);
}

TEST_CASE("v2 static node signatures export Python wiring metadata", "[v2][python]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    nb::gil_scoped_acquire guard;
    nb::object signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::ExportedSumNode>::wiring_signature();

    CHECK(nb::cast<std::string>(signature.attr("name")) == "exported_sum");
    CHECK(nb::cast<std::string>(signature.attr("signature")) == "exported_sum(lhs: TS[int], rhs: TS[int]) -> TS[int]");
    CHECK(nb::len(nb::cast<nb::object>(signature.attr("args"))) == 2);
}

TEST_CASE("v2 static node signatures derive selector policies from In metadata", "[v2][signature]")
{
    using signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::PolicyMetadataNode>;

    CHECK(signature::active_input_names() == std::vector<std::string>{"lhs", "strict"});
    CHECK(signature::valid_input_names() == std::vector<std::string>{"lhs"});
    CHECK(signature::all_valid_input_names() == std::vector<std::string>{"strict"});
}

TEST_CASE("v2 static node signatures export linked template type variables", "[v2][python][signature]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    using signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::GenericGetItemNode>;

    CHECK(signature::input_schema() == nullptr);
    CHECK(signature::output_schema() == nullptr);
    CHECK(signature::unresolved_input_names() == std::vector<std::string>{"ts", "key"});

    nb::gil_scoped_acquire guard;
    nb::object wiring_signature = signature::wiring_signature();
    const auto unresolved_args = nb::cast<std::unordered_set<std::string>>(wiring_signature.attr("unresolved_args"));

    CHECK(nb::cast<std::string>(wiring_signature.attr("signature")) == "generic_get_item(ts: TSD[K, V], key: TS[K]) -> V");
    CHECK(unresolved_args.contains("ts"));
    CHECK(unresolved_args.contains("key"));
}

TEST_CASE("v2 static node signatures export state injectables", "[v2][python][signature]")
{
    hgraph::v2::test_detail::ensure_python_hgraph_importable();

    using typed_state_signature = hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::SumWithTypedState>;
    using recordable_state_signature =
        hgraph::v2::StaticNodeSignature<hgraph::v2::test_detail::PreviousValueFromRecordableState>;

    CHECK(typed_state_signature::has_state());
    CHECK_FALSE(typed_state_signature::has_recordable_state());
    CHECK(recordable_state_signature::has_recordable_state());

    nb::gil_scoped_acquire guard;
    nb::object state_signature = typed_state_signature::wiring_signature();
    nb::object recordable_signature = recordable_state_signature::wiring_signature();

    const auto state_args = nb::cast<std::vector<std::string>>(state_signature.attr("args"));
    const auto recordable_args = nb::cast<std::vector<std::string>>(recordable_signature.attr("args"));

    CHECK(state_args == std::vector<std::string>{"lhs", "_state"});
    CHECK(recordable_args == std::vector<std::string>{"lhs", "_recordable_state"});
    CHECK(nb::cast<bool>(state_signature.attr("uses_state")));
    CHECK(nb::cast<bool>(recordable_signature.attr("uses_recordable_state")));
}
