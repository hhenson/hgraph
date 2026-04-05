#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/v2/basic_nodes.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/nodes/python_generator_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/node.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/python_node_support.h>
#include <hgraph/types/v2/python_export.h>

namespace
{
    using hgraph::TSMeta;
    using hgraph::engine_time_t;
    using namespace hgraph::v2;

    struct V2PythonNodeSignature
    {
        std::string name;
        NodeTypeEnum node_type{NodeTypeEnum::COMPUTE_NODE};
        std::vector<std::string> args;
        nb::object time_series_inputs;
        nb::object time_series_output;
        nb::object scalars;
        nb::object src_location;
        nb::object active_inputs;
        nb::object valid_inputs;
        nb::object all_valid_inputs;
        nb::object context_inputs;
        nb::object injectable_inputs;
        size_t injectables{0};
        bool capture_exception{false};
        int64_t trace_back_depth{1};
        std::string wiring_path_name;
        std::string label;
        bool capture_values{false};
        std::string record_replay_id;
        bool has_nested_graphs{false};
        nb::object recordable_state_arg;
        nb::object recordable_state;
        std::string signature_text;

        [[nodiscard]] bool uses_scheduler() const noexcept
        {
            return (injectables & static_cast<size_t>(hgraph::InjectableTypesEnum::SCHEDULER)) != 0;
        }

        [[nodiscard]] bool uses_clock() const noexcept
        {
            return (injectables & static_cast<size_t>(hgraph::InjectableTypesEnum::CLOCK)) != 0;
        }

        [[nodiscard]] bool uses_engine() const noexcept
        {
            return (injectables & static_cast<size_t>(hgraph::InjectableTypesEnum::ENGINE_API)) != 0;
        }

        [[nodiscard]] bool uses_state() const noexcept
        {
            return (injectables & static_cast<size_t>(hgraph::InjectableTypesEnum::STATE)) != 0;
        }

        [[nodiscard]] bool uses_recordable_state() const noexcept
        {
            return (injectables & static_cast<size_t>(hgraph::InjectableTypesEnum::RECORDABLE_STATE)) != 0;
        }

        [[nodiscard]] bool uses_output_feedback() const noexcept
        {
            return (injectables & static_cast<size_t>(hgraph::InjectableTypesEnum::OUTPUT)) != 0;
        }

        [[nodiscard]] bool is_source_node() const noexcept
        {
            return node_type == NodeTypeEnum::PUSH_SOURCE_NODE || node_type == NodeTypeEnum::PULL_SOURCE_NODE;
        }

        [[nodiscard]] bool is_pull_source_node() const noexcept
        {
            return node_type == NodeTypeEnum::PULL_SOURCE_NODE;
        }

        [[nodiscard]] bool is_push_source_node() const noexcept
        {
            return node_type == NodeTypeEnum::PUSH_SOURCE_NODE;
        }

        [[nodiscard]] bool is_compute_node() const noexcept
        {
            return node_type == NodeTypeEnum::COMPUTE_NODE;
        }

        [[nodiscard]] bool is_sink_node() const noexcept
        {
            return node_type == NodeTypeEnum::SINK_NODE;
        }

        [[nodiscard]] bool is_recordable() const noexcept
        {
            return !record_replay_id.empty();
        }

        [[nodiscard]] nb::dict to_dict() const
        {
            nb::dict out;
            out["name"] = name;
            out["node_type"] = nb::cast(node_type);
            out["args"] = nb::cast(args);
            out["time_series_inputs"] = nb::borrow(time_series_inputs);
            out["time_series_output"] = nb::borrow(time_series_output);
            out["scalars"] = nb::borrow(scalars);
            out["src_location"] = nb::borrow(src_location);
            out["active_inputs"] = nb::borrow(active_inputs);
            out["valid_inputs"] = nb::borrow(valid_inputs);
            out["all_valid_inputs"] = nb::borrow(all_valid_inputs);
            out["context_inputs"] = nb::borrow(context_inputs);
            out["injectable_inputs"] = nb::borrow(injectable_inputs);
            out["injectables"] = injectables;
            out["capture_exception"] = capture_exception;
            out["trace_back_depth"] = trace_back_depth;
            out["wiring_path_name"] = wiring_path_name;
            out["label"] = label;
            out["capture_values"] = capture_values;
            out["record_replay_id"] = record_replay_id;
            out["has_nested_graphs"] = has_nested_graphs;
            out["recordable_state_arg"] = nb::borrow(recordable_state_arg);
            out["recordable_state"] = nb::borrow(recordable_state);
            out["signature"] = signature_text;
            return out;
        }
    };

    struct StaticSumNode
    {
        StaticSumNode() = delete;
        ~StaticSumNode() = delete;

        static constexpr auto name = "static_sum";

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct StaticPolicyNode
    {
        StaticPolicyNode() = delete;
        ~StaticPolicyNode() = delete;

        static constexpr auto name = "static_policy";

        static void eval(In<"lhs", TS<int>> lhs,
                         In<"rhs", TS<int>, InputActivity::Passive, InputValidity::Unchecked> rhs,
                         In<"strict", TS<int>, InputActivity::Active, InputValidity::AllValid> strict,
                         Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value() + strict.value());
        }
    };

    struct StaticGetItemNode
    {
        StaticGetItemNode() = delete;
        ~StaticGetItemNode() = delete;

        static constexpr auto name = "static_get_item";

        using K = ScalarVar<"K">;
        using V = TsVar<"V">;

        static void eval(In<"ts", TSD<K, V>> ts, In<"key", TS<K>> key, Out<V> out)
        {
            static_cast<void>(ts);
            static_cast<void>(key);
            static_cast<void>(out);
        }
    };

    struct StaticTypedStateNode
    {
        StaticTypedStateNode() = delete;
        ~StaticTypedStateNode() = delete;

        static constexpr auto name = "static_typed_state";

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

    struct StaticRecordableStateNode
    {
        StaticRecordableStateNode() = delete;
        ~StaticRecordableStateNode() = delete;

        static constexpr auto name = "static_recordable_state";
        using LocalState = TSB<Field<"last", TS<int>>>;

        static void eval(In<"lhs", TS<int>> lhs, RecordableState<LocalState> state, Out<TS<int>> out)
        {
            auto last = state.view().field("last");
            if (last.valid()) { out.set(last.value().as_atomic().as<int>()); }
            last.value().set_scalar(lhs.value());
            state.mark_modified(last);
        }
    };

    struct StaticClockNode
    {
        StaticClockNode() = delete;
        ~StaticClockNode() = delete;

        static constexpr auto name = "static_clock";

        static void eval(In<"lhs", TS<int>> lhs, EvaluationClock clock, Out<TS<int>> out)
        {
            static_cast<void>(clock);
            out.set(lhs.value());
        }
    };

    struct StaticTickNode
    {
        StaticTickNode() = delete;
        ~StaticTickNode() = delete;

        static constexpr auto name = "static_tick";
        static constexpr auto node_type = NodeTypeEnum::PULL_SOURCE_NODE;

        static void start(Graph &graph, Node &node, engine_time_t evaluation_time)
        {
            graph.schedule_node(node.node_index(), evaluation_time, true);
        }

        static void eval(Out<TS<int>> out)
        {
            out.set(42);
        }
    };

    struct StaticSinkNode
    {
        StaticSinkNode() = delete;
        ~StaticSinkNode() = delete;

        static constexpr auto name = "static_sink";
        static constexpr auto node_type = NodeTypeEnum::SINK_NODE;

        static inline int call_count{0};
        static inline int last_value{0};

        static void reset()
        {
            call_count = 0;
            last_value = 0;
        }

        static void eval(In<"ts", TS<int>> ts)
        {
            ++call_count;
            last_value = ts.value();
        }
    };

    [[nodiscard]] EvaluationMode normalize_evaluation_mode(nb::handle value)
    {
        nb::object mode = nb::borrow(value);
        if (nb::hasattr(mode, "name")) {
            const std::string name = nb::cast<std::string>(mode.attr("name"));
            if (name == "REAL_TIME") { return EvaluationMode::REAL_TIME; }
            if (name == "SIMULATION") { return EvaluationMode::SIMULATION; }
        }

        const int mode_value = nb::cast<int>(mode);
        switch (mode_value) {
            case static_cast<int>(EvaluationMode::REAL_TIME): return EvaluationMode::REAL_TIME;
            case static_cast<int>(EvaluationMode::SIMULATION): return EvaluationMode::SIMULATION;
            default: throw std::invalid_argument(fmt::format("Unknown v2 evaluation mode {}", mode_value));
        }
    }

    [[nodiscard]] nb::object object_or_none(const nb::object &value)
    {
        return value.is_valid() ? nb::borrow(value) : nb::none();
    }

    [[nodiscard]] nb::dict dict_or_empty(const nb::object &value)
    {
        if (value.is_valid() && !value.is_none()) { return nb::cast<nb::dict>(value); }
        return nb::dict();
    }

    [[nodiscard]] std::string string_or_empty(nb::handle value)
    {
        return value.is_none() ? std::string{} : nb::cast<std::string>(value);
    }

    [[nodiscard]] const TSMeta *cpp_ts_meta_or_none(nb::handle meta)
    {
        if (meta.is_none()) { return nullptr; }

        nb::object cpp_type = nb::borrow(meta).attr("cpp_type");
        if (cpp_type.is_none()) { return nullptr; }
        return nb::cast<const TSMeta *>(cpp_type);
    }

    [[nodiscard]] const TSMeta *input_schema_from_node_signature(nb::handle node_signature)
    {
        nb::object input_types = nb::borrow(node_signature).attr("time_series_inputs");
        if (input_types.is_none()) { return nullptr; }

        std::vector<std::pair<std::string, const TSMeta *>> fields;
        const std::vector<std::string> args = nb::cast<std::vector<std::string>>(nb::borrow(node_signature).attr("args"));
        fields.reserve(args.size());
        for (const auto &arg : args) {
            if (!PyMapping_HasKey(input_types.ptr(), nb::str(arg.c_str()).ptr())) { continue; }
            const TSMeta *schema =
                cpp_ts_meta_or_none(nb::steal<nb::object>(PyObject_GetItem(input_types.ptr(), nb::str(arg.c_str()).ptr())));
            if (schema == nullptr) { return nullptr; }
            fields.emplace_back(arg, schema);
        }

        if (fields.empty()) { return nullptr; }
        return hgraph::TSTypeRegistry::instance().tsb(fields, "python_v2.inputs");
    }

    [[nodiscard]] const TSMeta *output_schema_from_node_signature(nb::handle node_signature)
    {
        return cpp_ts_meta_or_none(nb::borrow(node_signature).attr("time_series_output"));
    }

    [[nodiscard]] const TSMeta *recordable_state_schema_from_node_signature(nb::handle node_signature)
    {
        if (!nb::cast<bool>(nb::borrow(node_signature).attr("uses_recordable_state"))) { return nullptr; }
        nb::object recordable_state = nb::borrow(node_signature).attr("recordable_state");
        if (recordable_state.is_none()) { return nullptr; }
        return cpp_ts_meta_or_none(recordable_state.attr("tsb_type"));
    }

    [[nodiscard]] V2PythonNodeSignature make_v2_node_signature(nb::handle signature)
    {
        if (nb::isinstance<V2PythonNodeSignature>(signature)) { return nb::cast<V2PythonNodeSignature>(signature); }

        nb::object node_signature = nb::borrow(signature);
        return V2PythonNodeSignature{
            .name = nb::cast<std::string>(node_signature.attr("name")),
            .node_type = nb::cast<NodeTypeEnum>(node_signature.attr("node_type")),
            .args = nb::cast<std::vector<std::string>>(node_signature.attr("args")),
            .time_series_inputs = nb::borrow(node_signature.attr("time_series_inputs")),
            .time_series_output = nb::borrow(node_signature.attr("time_series_output")),
            .scalars = nb::borrow(node_signature.attr("scalars")),
            .src_location = nb::borrow(node_signature.attr("src_location")),
            .active_inputs = nb::borrow(node_signature.attr("active_inputs")),
            .valid_inputs = nb::borrow(node_signature.attr("valid_inputs")),
            .all_valid_inputs = nb::borrow(node_signature.attr("all_valid_inputs")),
            .context_inputs = nb::borrow(node_signature.attr("context_inputs")),
            .injectable_inputs = nb::borrow(node_signature.attr("injectable_inputs")),
            .injectables = nb::cast<size_t>(node_signature.attr("injectables")),
            .capture_exception = nb::cast<bool>(node_signature.attr("capture_exception")),
            .trace_back_depth = nb::cast<int64_t>(node_signature.attr("trace_back_depth")),
            .wiring_path_name = nb::cast<std::string>(node_signature.attr("wiring_path_name")),
            .label = string_or_empty(node_signature.attr("label")),
            .capture_values = nb::cast<bool>(node_signature.attr("capture_values")),
            .record_replay_id = string_or_empty(node_signature.attr("record_replay_id")),
            .has_nested_graphs = nb::cast<bool>(node_signature.attr("has_nested_graphs")),
            .recordable_state_arg = nb::borrow(node_signature.attr("recordable_state_arg")),
            .recordable_state = nb::borrow(node_signature.attr("recordable_state")),
            .signature_text = nb::cast<std::string>(node_signature.attr("signature")),
        };
    }

    void register_v2_node_signature(nb::module_ &v2)
    {
        nb::enum_<NodeTypeEnum>(v2, "NodeTypeEnum")
            .value("PUSH_SOURCE_NODE", NodeTypeEnum::PUSH_SOURCE_NODE)
            .value("PULL_SOURCE_NODE", NodeTypeEnum::PULL_SOURCE_NODE)
            .value("COMPUTE_NODE", NodeTypeEnum::COMPUTE_NODE)
            .value("SINK_NODE", NodeTypeEnum::SINK_NODE)
            .export_values();

        nb::class_<V2PythonNodeSignature>(v2, "NodeSignature")
            .def("__init__",
                 [](V2PythonNodeSignature *self, nb::kwargs kwargs) {
                     new (self) V2PythonNodeSignature{
                         .name = nb::cast<std::string>(kwargs["name"]),
                         .node_type = nb::cast<NodeTypeEnum>(kwargs["node_type"]),
                         .args = nb::cast<std::vector<std::string>>(kwargs["args"]),
                         .time_series_inputs = nb::borrow(kwargs["time_series_inputs"]),
                         .time_series_output = nb::borrow(kwargs["time_series_output"]),
                         .scalars = nb::borrow(kwargs["scalars"]),
                         .src_location = nb::borrow(kwargs["src_location"]),
                         .active_inputs = nb::borrow(kwargs["active_inputs"]),
                         .valid_inputs = nb::borrow(kwargs["valid_inputs"]),
                         .all_valid_inputs = nb::borrow(kwargs["all_valid_inputs"]),
                         .context_inputs = nb::borrow(kwargs["context_inputs"]),
                         .injectable_inputs = nb::borrow(kwargs["injectable_inputs"]),
                         .injectables = nb::cast<size_t>(kwargs["injectables"]),
                         .capture_exception = nb::cast<bool>(kwargs["capture_exception"]),
                         .trace_back_depth = nb::cast<int64_t>(kwargs["trace_back_depth"]),
                         .wiring_path_name = nb::cast<std::string>(kwargs["wiring_path_name"]),
                         .label = string_or_empty(kwargs["label"]),
                         .capture_values = nb::cast<bool>(kwargs["capture_values"]),
                         .record_replay_id = string_or_empty(kwargs["record_replay_id"]),
                         .has_nested_graphs = nb::cast<bool>(kwargs["has_nested_graphs"]),
                         .recordable_state_arg = kwargs.contains("recordable_state_arg")
                             ? nb::borrow(kwargs["recordable_state_arg"])
                             : nb::none(),
                         .recordable_state = kwargs.contains("recordable_state") ? nb::borrow(kwargs["recordable_state"])
                                                                                : nb::none(),
                         .signature_text = kwargs.contains("signature") ? nb::cast<std::string>(kwargs["signature"])
                                                                        : std::string{},
                     };
                 })
            .def_prop_ro("name", [](const V2PythonNodeSignature &self) { return self.name; })
            .def_prop_ro("node_type", [](const V2PythonNodeSignature &self) { return self.node_type; })
            .def_prop_ro("args", [](const V2PythonNodeSignature &self) { return self.args; })
            .def_prop_ro("time_series_inputs",
                         [](const V2PythonNodeSignature &self) { return nb::borrow(self.time_series_inputs); })
            .def_prop_ro("time_series_output",
                         [](const V2PythonNodeSignature &self) { return nb::borrow(self.time_series_output); })
            .def_prop_ro("scalars", [](const V2PythonNodeSignature &self) { return nb::borrow(self.scalars); })
            .def_prop_ro("src_location", [](const V2PythonNodeSignature &self) { return nb::borrow(self.src_location); })
            .def_prop_ro("active_inputs", [](const V2PythonNodeSignature &self) { return nb::borrow(self.active_inputs); })
            .def_prop_ro("valid_inputs", [](const V2PythonNodeSignature &self) { return nb::borrow(self.valid_inputs); })
            .def_prop_ro("all_valid_inputs",
                         [](const V2PythonNodeSignature &self) { return nb::borrow(self.all_valid_inputs); })
            .def_prop_ro("context_inputs", [](const V2PythonNodeSignature &self) { return nb::borrow(self.context_inputs); })
            .def_prop_ro("injectable_inputs",
                         [](const V2PythonNodeSignature &self) { return nb::borrow(self.injectable_inputs); })
            .def_prop_ro("injectables", [](const V2PythonNodeSignature &self) { return self.injectables; })
            .def_prop_ro("capture_exception", [](const V2PythonNodeSignature &self) { return self.capture_exception; })
            .def_prop_ro("trace_back_depth", [](const V2PythonNodeSignature &self) { return self.trace_back_depth; })
            .def_prop_ro("wiring_path_name", [](const V2PythonNodeSignature &self) { return self.wiring_path_name; })
            .def_prop_ro("label", [](const V2PythonNodeSignature &self) { return self.label; })
            .def_prop_ro("capture_values", [](const V2PythonNodeSignature &self) { return self.capture_values; })
            .def_prop_ro("record_replay_id", [](const V2PythonNodeSignature &self) { return self.record_replay_id; })
            .def_prop_ro("has_nested_graphs", [](const V2PythonNodeSignature &self) { return self.has_nested_graphs; })
            .def_prop_ro("signature", [](const V2PythonNodeSignature &self) { return self.signature_text; })
            .def_prop_ro("uses_scheduler", &V2PythonNodeSignature::uses_scheduler)
            .def_prop_ro("uses_clock", &V2PythonNodeSignature::uses_clock)
            .def_prop_ro("uses_engine", &V2PythonNodeSignature::uses_engine)
            .def_prop_ro("uses_state", &V2PythonNodeSignature::uses_state)
            .def_prop_ro("uses_recordable_state", &V2PythonNodeSignature::uses_recordable_state)
            .def_prop_ro("uses_output_feedback", &V2PythonNodeSignature::uses_output_feedback)
            .def_prop_ro("recordable_state_arg",
                         [](const V2PythonNodeSignature &self) { return nb::borrow(self.recordable_state_arg); })
            .def_prop_ro("recordable_state",
                         [](const V2PythonNodeSignature &self) { return nb::borrow(self.recordable_state); })
            .def_prop_ro("is_source_node", &V2PythonNodeSignature::is_source_node)
            .def_prop_ro("is_pull_source_node", &V2PythonNodeSignature::is_pull_source_node)
            .def_prop_ro("is_push_source_node", &V2PythonNodeSignature::is_push_source_node)
            .def_prop_ro("is_compute_node", &V2PythonNodeSignature::is_compute_node)
            .def_prop_ro("is_sink_node", &V2PythonNodeSignature::is_sink_node)
            .def_prop_ro("is_recordable", &V2PythonNodeSignature::is_recordable)
            .def("to_dict", &V2PythonNodeSignature::to_dict)
            .def("__str__", [](const V2PythonNodeSignature &self) { return self.signature_text; })
            .def("__repr__", [](const V2PythonNodeSignature &self) {
                return fmt::format("v2.NodeSignature(name='{}', node_type={})", self.name, static_cast<int>(self.node_type));
            });
    }

    void apply_selector_policies(NodeBuilder &builder, nb::handle node_signature, const TSMeta *input_schema)
    {
        if (input_schema == nullptr) { return; }

        auto apply_names = [&](const char *attribute_name, bool default_to_all_inputs, auto add_slot) {
            nb::object names_obj = nb::borrow(node_signature).attr(attribute_name);
            std::vector<std::string> names;
            if (!names_obj.is_none()) {
                names.reserve(static_cast<size_t>(nb::len(names_obj)));
                for (auto item : names_obj) { names.push_back(nb::cast<std::string>(item)); }
            } else if (default_to_all_inputs) {
                nb::object inputs = nb::borrow(node_signature).attr("time_series_inputs");
                if (!inputs.is_none()) {
                    for (auto item : inputs.attr("keys")()) { names.push_back(nb::cast<std::string>(item)); }
                }
            }

            for (const auto &name : names) {
                for (size_t slot = 0; slot < input_schema->field_count(); ++slot) {
                    if (input_schema->fields()[slot].name == name) {
                        add_slot(slot);
                        break;
                    }
                }
            }
        };

        apply_names("active_inputs", true, [&](size_t slot) { builder.active_input(slot); });
        apply_names("valid_inputs", true, [&](size_t slot) { builder.valid_input(slot); });
        apply_names("all_valid_inputs", false, [&](size_t slot) { builder.all_valid_input(slot); });
    }

    [[nodiscard]] NodeBuilder make_python_node_builder(nb::object signature,
                                                       nb::dict scalars,
                                                       nb::object input_builder,
                                                       nb::object output_builder,
                                                       nb::object error_builder,
                                                       nb::object recordable_state_builder,
                                                       nb::object eval_fn,
                                                       nb::object start_fn,
                                                       nb::object stop_fn)
    {
        if (nb::cast<bool>(signature.attr("capture_exception"))) {
            throw std::invalid_argument("v2 Python-backed nodes do not yet support error capture outputs");
        }
        nb::object context_inputs = signature.attr("context_inputs");
        if (!context_inputs.is_none() && nb::len(context_inputs) > 0) {
            throw std::invalid_argument("v2 Python-backed nodes do not yet support context-manager inputs");
        }

        const NodeTypeEnum node_type = nb::cast<NodeTypeEnum>(signature.attr("node_type"));
        if (node_type == NodeTypeEnum::PUSH_SOURCE_NODE) {
            throw std::invalid_argument("v2 Python-backed nodes do not yet support push-source semantics");
        }

        const TSMeta *input_schema = input_schema_from_node_signature(signature);
        const TSMeta *output_schema = output_schema_from_node_signature(signature);
        const TSMeta *recordable_state_schema = recordable_state_schema_from_node_signature(signature);

        NodeBuilder builder;
        builder.node_type(node_type)
            .python_signature(nb::borrow(signature))
            .python_scalars(std::move(scalars))
            .python_input_builder(std::move(input_builder))
            .python_output_builder(std::move(output_builder))
            .python_error_builder(std::move(error_builder))
            .python_recordable_state_builder(std::move(recordable_state_builder))
            .python_implementation(std::move(eval_fn), std::move(start_fn), std::move(stop_fn))
            .implementation_name(nb::cast<std::string>(signature.attr("name")))
            .uses_scheduler(nb::cast<bool>(signature.attr("uses_scheduler")))
            .requires_resolved_schemas(true);

        if (input_schema != nullptr) { builder.input_schema(input_schema); }
        if (output_schema != nullptr) { builder.output_schema(output_schema); }
        if (recordable_state_schema != nullptr) { builder.recordable_state_schema(recordable_state_schema); }
        apply_selector_policies(builder, signature, input_schema);
        return builder;
    }

    struct V2GraphExecutor
    {
        V2GraphExecutor(GraphBuilder graph_builder, nb::object run_mode, std::vector<nb::object> observers, bool cleanup_on_error)
            : m_graph_builder(std::move(graph_builder)),
              m_run_mode(normalize_evaluation_mode(run_mode)),
              m_cleanup_on_error(cleanup_on_error)
        {
            if (!observers.empty()) {
                throw std::invalid_argument("v2 Python execution does not yet support Python lifecycle observers");
            }
        }

        [[nodiscard]] EvaluationMode run_mode() const noexcept { return m_run_mode; }

        [[nodiscard]] nb::object graph() const
        {
            throw std::logic_error("v2 Python graph access is not exposed yet; only execution is supported");
        }

        void run(engine_time_t start_time, engine_time_t end_time)
        {
            static_cast<void>(m_cleanup_on_error);
            auto engine = EvaluationEngineBuilder{}
                              .graph_builder(m_graph_builder)
                              .evaluation_mode(m_run_mode)
                              .start_time(start_time)
                              .end_time(end_time)
                              .build();
            engine.run();
        }

      private:
        GraphBuilder m_graph_builder;
        EvaluationMode m_run_mode{EvaluationMode::SIMULATION};
        bool m_cleanup_on_error{true};
    };

}

void export_nodes(nb::module_ &m) {
    using namespace hgraph;
    auto v2 = m.def_submodule("v2", "Experimental v2 static node exports");
    hgraph::v2::register_python_runtime_bindings(v2);
    register_v2_node_signature(v2);
    auto node_builder_cls = nb::class_<hgraph::v2::NodeBuilder>(v2, "NodeBuilder")
        .def_static(
            "python",
            &make_python_node_builder,
            "signature"_a,
            "scalars"_a,
            "input_builder"_a = nb::none(),
            "output_builder"_a = nb::none(),
            "error_builder"_a = nb::none(),
            "recordable_state_builder"_a = nb::none(),
            "eval_fn"_a,
            "start_fn"_a = nb::none(),
            "stop_fn"_a = nb::none())
        .def_static("make_signature", &make_v2_node_signature, "signature"_a)
        .def(
            "make_instance",
            [](const hgraph::v2::NodeBuilder &self, nb::tuple owning_graph_id, int64_t node_ndx) -> nb::object {
                static_cast<void>(self);
                static_cast<void>(owning_graph_id);
                static_cast<void>(node_ndx);
                throw std::logic_error(
                    "v2 NodeBuilder only supports execution through _hgraph.v2.GraphBuilder and _hgraph.v2.GraphExecutor");
            },
            "owning_graph_id"_a,
            "node_ndx"_a)
        .def("release_instance",
             [](const hgraph::v2::NodeBuilder &self, nb::handle item) {
                 static_cast<void>(self);
                 static_cast<void>(item);
             },
             "item"_a)
        .def_prop_ro("signature", [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.signature()); })
        .def_prop_ro("scalars", [](const hgraph::v2::NodeBuilder &self) { return dict_or_empty(self.scalars()); })
        .def_prop_ro("input_builder",
                     [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.input_builder()); })
        .def_prop_ro("output_builder",
                     [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.output_builder()); })
        .def_prop_ro("error_builder",
                     [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.error_builder()); })
        .def_prop_ro(
            "recordable_state_builder",
            [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.recordable_state_builder()); })
        .def_prop_ro("implementation_name", [](const hgraph::v2::NodeBuilder &self) { return self.implementation_name(); })
        .def_prop_ro(
            "input_schema",
            [](const hgraph::v2::NodeBuilder &self) -> nb::object {
                return self.input_schema() != nullptr ? nb::cast(self.input_schema(), nb::rv_policy::reference) : nb::none();
            })
        .def_prop_ro(
            "output_schema",
            [](const hgraph::v2::NodeBuilder &self) -> nb::object {
                return self.output_schema() != nullptr ? nb::cast(self.output_schema(), nb::rv_policy::reference) : nb::none();
            })
        .def_prop_ro(
            "requires_resolved_schemas",
            [](const hgraph::v2::NodeBuilder &self) { return self.requires_resolved_schemas(); })
        .def(
            "__repr__",
            [](const hgraph::v2::NodeBuilder &self) {
                return fmt::format("v2.NodeBuilder@{:p}[impl={}]", static_cast<const void *>(&self), self.implementation_name());
            })
            .def("__str__",
             [](const hgraph::v2::NodeBuilder &self) {
                 return fmt::format("v2.NodeBuilder@{:p}[impl={}]", static_cast<const void *>(&self), self.implementation_name());
             });
    node_builder_cls.attr("NODE_SIGNATURE") = v2.attr("NodeSignature");
    node_builder_cls.attr("NODE_TYPE_ENUM") = v2.attr("NodeTypeEnum");

    nb::class_<hgraph::v2::Edge>(v2, "Edge")
        .def(
            nb::init<int64_t, hgraph::v2::Path, int64_t, hgraph::v2::Path>(),
            "src_node"_a,
            "output_path"_a = hgraph::v2::Path{},
            "dst_node"_a,
            "input_path"_a = hgraph::v2::Path{})
        .def_rw("src_node", &hgraph::v2::Edge::src_node)
        .def_rw("output_path", &hgraph::v2::Edge::output_path)
        .def_rw("dst_node", &hgraph::v2::Edge::dst_node)
        .def_rw("input_path", &hgraph::v2::Edge::input_path)
        .def("__eq__", &hgraph::v2::Edge::operator==)
        .def("__lt__", &hgraph::v2::Edge::operator<)
        .def(
            "__hash__",
            [](const hgraph::v2::Edge &self) {
                size_t hash = std::hash<int64_t>{}(self.src_node);
                hash ^= std::hash<int64_t>{}(self.dst_node) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                for (const auto slot : self.output_path) {
                    hash ^= std::hash<int64_t>{}(slot) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                }
                for (const auto slot : self.input_path) {
                    hash ^= std::hash<int64_t>{}(slot) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                }
                return hash;
            });

    nb::class_<hgraph::v2::GraphBuilder>(v2, "GraphBuilder")
        .def(nb::init<>())
        .def(nb::init<std::vector<hgraph::v2::NodeBuilder>, std::vector<hgraph::v2::Edge>>(), "node_builders"_a, "edges"_a)
        .def(
            "add_node",
            [](hgraph::v2::GraphBuilder &self, const hgraph::v2::NodeBuilder &node_builder) -> hgraph::v2::GraphBuilder & {
                return self.add_node(node_builder);
            },
            "node_builder"_a,
            nb::rv_policy::reference_internal)
        .def(
            "add_edge",
            [](hgraph::v2::GraphBuilder &self, const hgraph::v2::Edge &edge) -> hgraph::v2::GraphBuilder & {
                return self.add_edge(edge);
            },
            "edge"_a,
            nb::rv_policy::reference_internal)
        .def(
            "make_instance",
            [](const hgraph::v2::GraphBuilder &self, nb::tuple graph_id, nb::handle parent_node, std::string label) -> nb::object {
                static_cast<void>(self);
                static_cast<void>(graph_id);
                static_cast<void>(parent_node);
                static_cast<void>(label);
                throw std::logic_error(
                    "v2 GraphBuilder only supports execution through _hgraph.v2.GraphExecutor");
            },
            "graph_id"_a,
            "parent_node"_a = nb::none(),
            "label"_a = "")
        .def(
            "make_and_connect_nodes",
            [](const hgraph::v2::GraphBuilder &self, nb::tuple graph_id, int64_t first_node_ndx) -> nb::object {
                static_cast<void>(self);
                static_cast<void>(graph_id);
                static_cast<void>(first_node_ndx);
                throw std::logic_error(
                    "v2 GraphBuilder only supports execution through _hgraph.v2.GraphExecutor");
            },
            "graph_id"_a,
            "first_node_ndx"_a)
        .def("release_instance",
             [](const hgraph::v2::GraphBuilder &self, nb::handle item) {
                 static_cast<void>(self);
                 static_cast<void>(item);
             },
             "item"_a)
        .def_prop_ro("size", &hgraph::v2::GraphBuilder::size)
        .def_prop_ro("alignment", &hgraph::v2::GraphBuilder::alignment)
        .def_prop_ro("node_builders",
                     [](const hgraph::v2::GraphBuilder &self) { return nb::tuple(nb::cast(self.node_builders())); })
        .def_prop_ro("edges", [](const hgraph::v2::GraphBuilder &self) { return nb::tuple(nb::cast(self.edges())); })
        .def(
            "__repr__",
            [](const hgraph::v2::GraphBuilder &self) {
                return fmt::format(
                    "v2.GraphBuilder@{:p}[nodes={}, edges={}]",
                    static_cast<const void *>(&self),
                    self.node_builders().size(),
                    self.edges().size());
            })
        .def("__str__",
             [](const hgraph::v2::GraphBuilder &self) {
                 return fmt::format(
                     "v2.GraphBuilder@{:p}[nodes={}, edges={}]",
                     static_cast<const void *>(&self),
                     self.node_builders().size(),
                     self.edges().size());
             });

    nb::enum_<hgraph::v2::EvaluationMode>(v2, "EvaluationMode")
        .value("REAL_TIME", hgraph::v2::EvaluationMode::REAL_TIME)
        .value("SIMULATION", hgraph::v2::EvaluationMode::SIMULATION)
        .export_values();

    nb::class_<hgraph::v2::EvaluationEngine>(v2, "EvaluationEngine")
        .def_prop_ro("evaluation_mode", &hgraph::v2::EvaluationEngine::evaluation_mode)
        .def_prop_ro("start_time", &hgraph::v2::EvaluationEngine::start_time)
        .def_prop_ro("end_time", &hgraph::v2::EvaluationEngine::end_time)
        .def("run", &hgraph::v2::EvaluationEngine::run);

    nb::class_<V2GraphExecutor>(v2, "GraphExecutor")
        .def(
            nb::init<hgraph::v2::GraphBuilder, nb::object, std::vector<nb::object>, bool>(),
            "graph_builder"_a,
            "run_mode"_a,
            "observers"_a = std::vector<nb::object>{},
            "cleanup_on_error"_a = true)
        .def_prop_ro("run_mode", &V2GraphExecutor::run_mode)
        .def_prop_ro("graph", &V2GraphExecutor::graph)
        .def("run", &V2GraphExecutor::run, "start_time"_a, "end_time"_a);

    nb::class_<hgraph::v2::EvaluationEngineBuilder>(v2, "EvaluationEngineBuilder")
        .def(nb::init<>())
        .def(
            "graph_builder",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               const hgraph::v2::GraphBuilder &graph_builder) -> hgraph::v2::EvaluationEngineBuilder & {
                return self.graph_builder(graph_builder);
            },
            "graph_builder"_a,
            nb::rv_policy::reference_internal)
        .def(
            "evaluation_mode",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               hgraph::v2::EvaluationMode evaluation_mode) -> hgraph::v2::EvaluationEngineBuilder & {
                return self.evaluation_mode(evaluation_mode);
            },
            "evaluation_mode"_a,
            nb::rv_policy::reference_internal)
        .def(
            "start_time",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               engine_time_t start_time) -> hgraph::v2::EvaluationEngineBuilder & { return self.start_time(start_time); },
            "start_time"_a,
            nb::rv_policy::reference_internal)
        .def(
            "end_time",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               engine_time_t end_time) -> hgraph::v2::EvaluationEngineBuilder & { return self.end_time(end_time); },
            "end_time"_a,
            nb::rv_policy::reference_internal)
        .def("build", &hgraph::v2::EvaluationEngineBuilder::build);

    v2.def("reset_static_sink_state", &StaticSinkNode::reset);
    v2.def("static_sink_call_count", [] { return StaticSinkNode::call_count; });
    v2.def("static_sink_last_value", [] { return StaticSinkNode::last_value; });

    hgraph::v2::export_compute_node<StaticSumNode>(v2);
    hgraph::v2::export_compute_node<StaticPolicyNode>(v2);
    hgraph::v2::export_compute_node<StaticGetItemNode>(v2);
    hgraph::v2::export_compute_node<StaticTypedStateNode>(v2);
    hgraph::v2::export_compute_node<StaticRecordableStateNode>(v2);
    hgraph::v2::export_compute_node<StaticClockNode>(v2);
    hgraph::v2::export_compute_node<StaticTickNode>(v2);
    hgraph::v2::export_compute_node<StaticSinkNode>(v2);
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::ConstNode>(
        v2, "hgraph._impl._operators._time_series_conversion", "const_default", "const");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::NothingNode>(
        v2, "hgraph._impl._operators._graph_operators", "nothing_impl", "nothing");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::NullSinkNode>(
        v2, "hgraph._impl._operators._graph_operators", "null_sink_impl", "null_sink");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::DebugPrintNode>(
        v2, "hgraph._impl._operators._graph_operators", "debug_print_impl", "debug_print");
}
