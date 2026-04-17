#include <hgraph/nodes/v2/basic_nodes.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/v2/child_graph.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/nested_node_builder.h>
#include <hgraph/types/v2/node_impl.h>
#include <hgraph/types/v2/path_constants.h>
#include <hgraph/types/v2/python_node_support.h>
#include <hgraph/types/v2/python_export.h>
#include <hgraph/types/v2/ref.h>

namespace
{
    using hgraph::TSMeta;
    using hgraph::TSKind;
    using hgraph::TSInputView;
    using hgraph::TSOutputView;
    using hgraph::TSViewContext;
    using hgraph::View;

    using hgraph::Value;
    using hgraph::BaseState;
    using hgraph::TSOutput;
    using hgraph::GlobalState;
    using hgraph::engine_time_t;
    using namespace hgraph::v2;
    namespace keys = hgraph::keys;

    namespace
    {
        constexpr size_t injectable_state_bit = 1;
        constexpr size_t injectable_recordable_state_bit = 2;
        constexpr size_t injectable_scheduler_bit = 4;
        constexpr size_t injectable_output_bit = 8;
        constexpr size_t injectable_clock_bit = 16;
        constexpr size_t injectable_engine_bit = 32;

        [[nodiscard]] Node *owner_node_from_linked_context(const hgraph::LinkedTSContext &context) noexcept
        {
            const auto owner_from_output = [&](TSOutput *output) noexcept -> Node * {
                if (output == nullptr) { return nullptr; }
                Node *owner = nullptr;
                std::visit(
                    [&](auto &state) {
                        hgraph::visit(
                            state.parent,
                            [&](Node *parent) noexcept { owner = parent; },
                            [](auto *) noexcept {},
                            []() noexcept {});
                    },
                    output->root_state_variant());
                return owner;
            };

            hgraph::BaseState *cursor = context.notification_state != nullptr ? context.notification_state : context.ts_state;
            while (cursor != nullptr) {
                Node *owner = nullptr;
                bool  advanced = false;
                hgraph::visit(
                    cursor->parent,
                    [&](hgraph::TSLState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](hgraph::TSDState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](hgraph::TSBState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](hgraph::SignalState *parent) {
                        cursor = parent;
                        advanced = true;
                    },
                    [&](Node *parent) {
                        owner = parent;
                        cursor = nullptr;
                        advanced = true;
                    },
                    [&](hgraph::TSInput *parent) {
                        static_cast<void>(parent);
                        cursor = nullptr;
                        advanced = true;
                    },
                    [&](hgraph::TSOutput *parent) {
                        owner = owner_from_output(parent);
                        cursor = nullptr;
                        advanced = true;
                    },
                    [] {});
                if (owner != nullptr) { return owner; }
                if (!advanced) { break; }
            }
            return owner_from_output(context.owning_output);
        }

        [[nodiscard]] bool input_has_binding(const TSInputView &view) noexcept
        {
            if (const auto *target = view.linked_target(); target != nullptr) { return target->is_bound(); }
            return false;
        }

        void disconnect_input(TSInputView view)
        {
            if (view.active()) { view.make_passive(); }
            if (input_has_binding(view)) { view.unbind_output(); }
        }

        void bind_reference_to_input(const TimeSeriesReference &ref, TSInputView input, engine_time_t evaluation_time)
        {
            disconnect_input(input);
            if (ref.is_empty()) { return; }

            if (ref.is_peered()) {
                input.bind_output(ref.target_view(evaluation_time));
                return;
            }

            const TSMeta *schema = input.ts_schema();
            if (schema == nullptr) { return; }

            switch (schema->kind) {
                case TSKind::TSB: {
                    auto bundle = input.as_bundle();
                    const auto &items = ref.items();
                    const size_t count = std::min(bundle.size(), items.size());
                    for (size_t i = 0; i < count; ++i) {
                        bind_reference_to_input(items[i], bundle[i], evaluation_time);
                    }
                    break;
                }
                case TSKind::TSL: {
                    auto list = input.as_list();
                    const auto &items = ref.items();
                    const size_t count = std::min(list.size(), items.size());
                    for (size_t i = 0; i < count; ++i) {
                        bind_reference_to_input(items[i], list[i], evaluation_time);
                    }
                    break;
                }
                default:
                    break;
            }
        }

        [[nodiscard]] bool view_is_ref_output(const TimeSeriesReference &ref) noexcept
        {
            return ref.is_peered() && ref.target().schema != nullptr && ref.target().schema->kind == TSKind::REF;
        }

        [[nodiscard]] const TimeSeriesReference *try_view_ref(const View &view) noexcept
        {
            return view.as_atomic().template try_as<TimeSeriesReference>();
        }

        [[nodiscard]] nb::object remove_if_exists_sentinel()
        {
            static nb::object value = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
            return nb::borrow(value);
        }

        [[nodiscard]] TSOutputView output_view_from_input(const TSInputView &input)
        {
            const TSViewContext resolved = input.context_ref().resolved();
            if (resolved.schema == nullptr || resolved.value_dispatch == nullptr || resolved.ts_dispatch == nullptr ||
                resolved.ts_state == nullptr) {
                return TSOutputView{TSViewContext::none(), TSViewContext::none(), input.evaluation_time(), nullptr};
            }

            return TSOutputView{
                TSViewContext{resolved.schema, resolved.value_dispatch, resolved.ts_dispatch, resolved.value_data, resolved.ts_state},
                TSViewContext::none(),
                input.evaluation_time(),
                resolved.owning_output,
                resolved.output_view_ops != nullptr ? resolved.output_view_ops : &hgraph::detail::default_output_view_ops(),
            };
        }

        void suppress_spurious_empty_dict_tick(const hgraph::TSDView<TSOutputView> &out, engine_time_t evaluation_time)
        {
            BaseState *state = out.view().context_ref().ts_state;
            const auto delta = out.view().value().as_map().delta();
            constexpr size_t no_slot = static_cast<size_t>(-1);
            const bool has_delta =
                delta.first_added_slot() != no_slot || delta.first_updated_slot() != no_slot || delta.first_removed_slot() != no_slot;
            if (state != nullptr && state->last_modified_time == evaluation_time && !has_delta) {
                state->last_modified_time = hgraph::MIN_DT;
            }
        }
    }  // namespace

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
            return (injectables & injectable_scheduler_bit) != 0;
        }

        [[nodiscard]] bool uses_clock() const noexcept
        {
            return (injectables & injectable_clock_bit) != 0;
        }

        [[nodiscard]] bool uses_engine() const noexcept
        {
            return (injectables & injectable_engine_bit) != 0;
        }

        [[nodiscard]] bool uses_state() const noexcept
        {
            return (injectables & injectable_state_bit) != 0;
        }

        [[nodiscard]] bool uses_recordable_state() const noexcept
        {
            return (injectables & injectable_recordable_state_bit) != 0;
        }

        [[nodiscard]] bool uses_output_feedback() const noexcept
        {
            return (injectables & injectable_output_bit) != 0;
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
                         In<"strict", TS<int>, InputValidity::AllValid> strict,
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

    struct StaticRefBoolNode
    {
        StaticRefBoolNode() = delete;
        ~StaticRefBoolNode() = delete;

        static constexpr auto name = "static_ref_bool";

        static void eval(In<"ts", REF<TS<int>>> ts, Out<TS<bool>> out)
        {
            out.set(ts.modified());
        }
    };

    struct StaticContextOutputNode
    {
        StaticContextOutputNode() = delete;
        ~StaticContextOutputNode() = delete;

        static constexpr auto name = "get_context_output";
        static constexpr auto node_type = NodeTypeEnum::PULL_SOURCE_NODE;

        using ContextTs = TsVar<"CONTEXT_TIME_SERIES_TYPE">;

        [[nodiscard]] static int64_t raw_subscription(State<int64_t> state)
        {
            return state.view().template checked_as<int64_t>();
        }

        [[nodiscard]] static BaseState *subscribed_state(State<int64_t> state)
        {
            const intptr_t raw = static_cast<intptr_t>(raw_subscription(state));
            return raw == 0 ? nullptr : reinterpret_cast<BaseState *>(raw);
        }

        static void set_subscribed_state(State<int64_t> state, BaseState *output_state)
        {
            state.view().set_scalar(static_cast<int64_t>(reinterpret_cast<intptr_t>(output_state)));
        }

        static void clear_subscription(Node &node, State<int64_t> state)
        {
            if (BaseState *output_state = subscribed_state(state); output_state != nullptr) {
                output_state->unsubscribe(&node);
            }
            state.view().set_scalar(static_cast<int64_t>(0));
        }

        static void start(Node &node, engine_time_t evaluation_time, State<int64_t> state)
        {
            state.view().set_scalar(static_cast<int64_t>(0));
            node.notify(evaluation_time);
        }

        static void stop(Node &node, State<int64_t> state)
        {
            clear_subscription(node, state);
        }

        static void eval(Node &node,
                         ScalarArg<"path", std::string> path,
                         ScalarArg<"depth", int> depth,
                         State<int64_t> state,
                         Out<REF<ContextTs>> out)
        {
            const auto &owning_graph_id = node.owning_graph_id();
            const int   requested_depth = depth.value();
            const int   use = requested_depth >= 0
                                  ? std::min<int>(requested_depth, static_cast<int>(owning_graph_id.size()))
                                  : std::max<int>(0, static_cast<int>(owning_graph_id.size()) + requested_depth);

            std::vector<int64_t> owning_graph_prefix;
            owning_graph_prefix.reserve(static_cast<size_t>(use));
            for (int i = 0; i < use; ++i) {
                owning_graph_prefix.push_back(owning_graph_id[static_cast<size_t>(i)]);
            }

            const std::string key = keys::context_output_key(owning_graph_prefix, path.value());
            nb::object        shared = GlobalState::get(key, nb::none());
            if (!shared.is_valid() || shared.is_none()) {
                std::string diag;
                try {
                    nb::object gs = GlobalState::instance();
                    nb::object keys_obj = gs.attr("keys")();
                    std::vector<std::string> ctx_keys;
                    for (auto item : nb::iter(keys_obj)) {
                        std::string s = nb::cast<std::string>(nb::str(item));
                        if (s.rfind("context-", 0) == 0) { ctx_keys.push_back(s); }
                    }
                    if (!ctx_keys.empty()) {
                        diag = fmt::format(" Available context keys: [{}]", fmt::join(ctx_keys, ", "));
                    }
                } catch (...) {}
                throw std::runtime_error(fmt::format("Missing shared output for path: {}{}", key, diag));
            }

            if (!nb::isinstance<TimeSeriesReference>(shared)) {
                throw std::runtime_error(
                    fmt::format("Context found an invalid shared output bound to {}: {}", key, nb::str(shared.type()).c_str()));
            }

            const auto ref = nb::cast<TimeSeriesReference>(shared);
            BaseState *output_state = nullptr;
            if (ref.is_peered()) {
                const auto &target = ref.target();
                output_state = target.notification_state != nullptr ? target.notification_state : target.ts_state;
            }

            if (output_state != nullptr) {
                BaseState *current_state  = subscribed_state(state);
                if (current_state != output_state) {
                    clear_subscription(node, state);
                    output_state->subscribe(&node);
                    set_subscribed_state(state, output_state);
                }
            } else if (raw_subscription(state) != 0) {
                clear_subscription(node, state);
            }

            out.view().from_python(shared);
        }
    };

    struct StaticCaptureContextNode
    {
        StaticCaptureContextNode() = delete;
        ~StaticCaptureContextNode() = delete;

        static constexpr auto name = "capture_context";
        static constexpr auto node_type = NodeTypeEnum::SINK_NODE;

        using ContextTs = TsVar<"TIME_SERIES_TYPE">;

        static void start(Node &node,
                          ScalarArg<"path", std::string> path,
                          In<"ts", REF<ContextTs>> ts,
                          State<std::string> state)
        {
            std::vector<int64_t> owning_graph_id = node.owning_graph_id();
            const auto           ref = TimeSeriesReference::make(ts.view());

            if (ref.is_peered()) {
                if (Node *owner = owner_node_from_linked_context(ref.target()); owner != nullptr) {
                    const auto &owner_graph_id = owner->owning_graph_id();
                    if (!owner_graph_id.empty()) { owning_graph_id = owner_graph_id; }
                }
            }

            const std::string key = keys::context_output_key(owning_graph_id, path.value());
            state.view().set_scalar(key);
            GlobalState::set(key, nb::cast(ref));
        }

        static void eval(ScalarArg<"path", std::string> path, In<"ts", REF<ContextTs>> ts, State<std::string> state)
        {
            static_cast<void>(path);
            static_cast<void>(ts);
            static_cast<void>(state);
        }

        static void stop(State<std::string> state)
        {
            if (const auto *key = state.view().as_atomic().template try_as<std::string>(); key != nullptr && !key->empty()) {
                GlobalState::remove(*key);
            }
        }
    };

    struct ValidImplNode
    {
        ValidImplNode() = delete;
        ~ValidImplNode() = delete;

        static constexpr auto name = "valid_impl";

        using V = TsVar<"V">;

        static void start(In<"ts", REF<V>> ts,
                          In<"ts_value", V> ts_value,
                          Out<TS<bool>> out)
        {
            static_cast<void>(ts);
            static_cast<void>(ts_value);
            out.set(false);
        }

        static void eval(In<"ts", REF<V>> ts,
                         In<"ts_value", V> ts_value,
                         Out<TS<bool>> out)
        {
            const auto current_ts_value_view = [&]() { return ts_value.view(); };

            if (ts.modified()) {
                TSInputView ts_value_view = current_ts_value_view();
                disconnect_input(ts_value_view);
                bind_reference_to_input(ts.value(), ts_value_view, ts_value_view.evaluation_time());
                ts_value_view = current_ts_value_view();

                if (ts_value_view.valid()) {
                    if (const bool *current = out.try_value(); current == nullptr || !*current) { out.set(true); }
                    return;
                }

                ts_value_view.make_active();
            }

            TSInputView ts_value_view = current_ts_value_view();
            if (ts_value_view.valid()) {
                if (ts_value_view.active()) { ts_value_view.make_passive(); }
                if (const bool *current = out.try_value(); current == nullptr || !*current) { out.set(true); }
                return;
            }

            if (const bool *current = out.try_value(); current == nullptr || *current) { out.set(false); }
        }
    };

    struct TsdGetItemDefaultNode
    {
        TsdGetItemDefaultNode() = delete;
        ~TsdGetItemDefaultNode() = delete;

        static constexpr auto name = "tsd_get_item_default";

        using K = ScalarVar<"K">;
        using V = TsVar<"V">;

        static void eval(In<"ts", REF<TSD<K, V>>, InputValidity::Unchecked> ts,
                         In<"key", TS<K>> key,
                         In<"_ref", REF<V>, InputValidity::Unchecked> ref,
                         In<"_ref_ref", REF<V>, InputValidity::Unchecked> ref_ref,
                         Out<REF<V>> out)
        {
            if (ts.modified() || key.modified()) {
                TSInputView ref_view = ref.view();
                TSInputView ref_ref_view = ref_ref.view();
                disconnect_input(ref_view);
                disconnect_input(ref_ref_view);

                if (ts.valid() && key.valid() && ts.value().is_peered()) {
                    const View key_value = key.view().value();
                    auto ts_ref = ts.value();
                    auto target_view = ts_ref.target_view(ref_view.evaluation_time());
                    auto source_dict = target_view.as_dict();
                    auto source_child = source_dict[key_value];
                    if (source_child.context_ref().is_bound()) {
                        ref_view.bind_output(source_child);
                        ref_view.make_active();
                    }
                }
            }

            if (ref.modified()) {
                TSInputView ref_ref_view = ref_ref.view();
                disconnect_input(ref_ref_view);
                if (ref.valid() && view_is_ref_output(ref.value())) {
                    bind_reference_to_input(ref.value(), ref_ref_view, ref_ref_view.evaluation_time());
                    ref_ref_view.make_active();
                }
            }

            TSInputView ref_view = ref.view();
            TSInputView ref_ref_view = ref_ref.view();
            TimeSeriesReference result = TimeSeriesReference::make();
            if (ts.valid() && !ts.value().is_empty()) {
                if (input_has_binding(ref_ref_view) && ref_ref_view.valid()) {
                    if (const auto *inner_ref = try_view_ref(ref_ref_view.value()); inner_ref != nullptr) { result = *inner_ref; }
                } else if (ref_view.valid()) {
                    if (const auto *outer_ref = try_view_ref(ref_view.value()); outer_ref != nullptr) { result = *outer_ref; }
                }
            }

            const bool out_changed = [&] {
                const auto *current = out.try_value();
                return current == nullptr || !(*current == result);
            }();
            if (out_changed) { out.set(result); }
        }
    };

    struct TsdGetItemsNode
    {
        TsdGetItemsNode() = delete;
        ~TsdGetItemsNode() = delete;

        static constexpr auto name = "tsd_get_items";

        using K = ScalarVar<"K">;
        using V = TsVar<"V">;

        static void eval(In<"ts", TSD<K, V>, InputValidity::Unchecked> ts,
                         In<"key", TSS<K>, InputValidity::Unchecked> key,
                         Out<TSD<K, REF<V>>> out)
        {
            const engine_time_t evaluation_time = out.evaluation_time();
            auto out_dict = out.view();

            std::vector<Value> tracked_keys;
            for (const View &tracked : out_dict.keys()) { tracked_keys.emplace_back(tracked); }
            nb::dict delta;

            TSOutputView source_root = output_view_from_input(ts.view().view());
            if (!(ts.valid() && key.valid() && source_root.context_ref().is_bound())) {
                for (const Value &tracked_key : tracked_keys) { delta[tracked_key.view().to_python()] = remove_if_exists_sentinel(); }
                if (!delta.empty()) {
                    out_dict.view().apply_result(delta);
                } else if (tracked_keys.empty()) {
                    suppress_spurious_empty_dict_tick(out_dict, evaluation_time);
                }
                return;
            }

            auto selected_keys = key.view().as_set();
            auto source_dict = source_root.as_dict();
            std::vector<Value> desired_keys;
            for (const View &selected : selected_keys.values()) {
                if (!source_dict.value().has_value() || !source_dict.value().as_map().contains(selected)) { continue; }

                desired_keys.emplace_back(selected);
                TSOutputView source_child = source_dict[selected];
                TimeSeriesReference result = TimeSeriesReference::make(source_child);
                if (const TSMeta *source_schema = source_child.ts_schema(); source_schema != nullptr && source_schema->kind == TSKind::REF) {
                    const auto *outer_ref = source_child.valid() ? try_view_ref(source_child.value()) : nullptr;
                    result = outer_ref != nullptr ? *outer_ref : TimeSeriesReference::make();
                }

                const TSOutputView current_child = out_dict[selected];
                const auto *current_ref =
                    current_child.value().has_value() ? current_child.value().as_atomic().template try_as<TimeSeriesReference>() : nullptr;
                const bool needs_emit =
                    !current_child.context_ref().is_bound() || current_ref == nullptr || !(*current_ref == result) || source_child.modified();
                if (needs_emit) { delta[selected.to_python()] = nb::cast(result); }
            }

            for (const Value &tracked_key : tracked_keys) {
                const bool still_selected = std::any_of(
                    desired_keys.begin(), desired_keys.end(), [&](const Value &desired) { return desired.view() == tracked_key.view(); });
                if (!still_selected) { delta[tracked_key.view().to_python()] = remove_if_exists_sentinel(); }
            }

            if (!delta.empty()) {
                out_dict.view().apply_result(delta);
            } else if (tracked_keys.empty()) {
                suppress_spurious_empty_dict_tick(out_dict, evaluation_time);
            }
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
        if (cpp_type.is_none()) {
            nb::object is_context_wired = nb::getattr(nb::borrow(meta), "is_context_wired", nb::bool_(false));
            if (nb::cast<bool>(is_context_wired)) {
                nb::object ts_type = nb::getattr(nb::borrow(meta), "ts_type", nb::none());
                if (!ts_type.is_none()) { return cpp_ts_meta_or_none(ts_type); }
            }
            return nullptr;
        }
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

    [[nodiscard]] const TSMeta *input_schema_for_arg(nb::handle node_signature, std::string_view arg_name)
    {
        nb::object input_types = nb::borrow(node_signature).attr("time_series_inputs");
        if (input_types.is_none()) { return nullptr; }
        if (!PyMapping_HasKey(input_types.ptr(), nb::str(arg_name.data(), arg_name.size()).ptr())) { return nullptr; }
        return cpp_ts_meta_or_none(nb::steal<nb::object>(
            PyObject_GetItem(input_types.ptr(), nb::str(arg_name.data(), arg_name.size()).ptr())));
    }

    [[nodiscard]] const TSMeta *navigate_bound_source_schema(const TSMeta *schema, hgraph::v2::PathView path)
    {
        const TSMeta *current = schema;
        for (const int64_t slot : path) {
            const TSMeta *collection_schema = current;
            if (collection_schema != nullptr && collection_schema->kind == TSKind::REF && collection_schema->element_ts() != nullptr) {
                collection_schema = collection_schema->element_ts();
            }
            if (collection_schema == nullptr) {
                throw std::invalid_argument("build_nested_node source path requires a schema");
            }

            if (slot == hgraph::v2::k_key_set_path) {
                if (collection_schema->kind != TSKind::TSD) {
                    throw std::logic_error("build_nested_node key_set source path requires a dict schema");
                }
                current = hgraph::TSTypeRegistry::instance().tss(collection_schema->key_type());
                continue;
            }

            if (slot < 0) { throw std::out_of_range("build_nested_node source path slot must be non-negative"); }

            switch (collection_schema->kind) {
                case TSKind::TSB:
                    {
                        const auto index = static_cast<size_t>(slot);
                        if (index >= collection_schema->field_count()) {
                            throw std::out_of_range("build_nested_node bundle source path is out of range");
                        }
                        current = collection_schema->fields()[index].ts_type;
                        break;
                    }
                case TSKind::TSL:
                    {
                        const auto index = static_cast<size_t>(slot);
                        if (collection_schema->fixed_size() != 0 && index >= collection_schema->fixed_size()) {
                            throw std::out_of_range("build_nested_node list source path is out of range");
                        }
                        current = collection_schema->element_ts();
                        break;
                    }
                case TSKind::TSD:
                    current = collection_schema->element_ts();
                    break;
                default:
                    throw std::invalid_argument("build_nested_node source path only supports bundle/list/dict navigation");
            }
        }
        return current;
    }

    [[nodiscard]] InputBindingMode infer_nested_input_binding_mode(const TSMeta *source_schema)
    {
        return source_schema != nullptr && source_schema->kind == TSKind::REF
                   ? InputBindingMode::CLONE_REF_BINDING
                   : InputBindingMode::BIND_DIRECT;
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

    [[nodiscard]] const TSMeta *error_output_schema_from_node_signature(nb::handle node_signature)
    {
        if (!nb::cast<bool>(nb::borrow(node_signature).attr("capture_exception"))) { return nullptr; }

        nb::module_ hgraph_mod = nb::module_::import_("hgraph");
        nb::object ts_type = nb::steal<nb::object>(PyObject_GetItem(hgraph_mod.attr("TS").ptr(), hgraph_mod.attr("NodeError").ptr()));
        nb::object meta = hgraph_mod.attr("HgTimeSeriesTypeMetaData").attr("parse_type")(ts_type);
        return cpp_ts_meta_or_none(meta);
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

    template <typename TBuilder>
    void apply_selector_policies(TBuilder &builder, nb::handle node_signature, const TSMeta *input_schema)
    {
        if (input_schema == nullptr) { return; }

        auto resolve_slots = [&](const char *attribute_name) {
            nb::object names_obj = nb::borrow(node_signature).attr(attribute_name);
            if (names_obj.is_none()) { return std::optional<std::vector<size_t>>{}; }

            std::vector<size_t> slots;
            slots.reserve(static_cast<size_t>(nb::len(names_obj)));
            for (auto item : names_obj) {
                const auto name = nb::cast<std::string>(item);
                for (size_t slot = 0; slot < input_schema->field_count(); ++slot) {
                    if (input_schema->fields()[slot].name == name) { slots.push_back(slot); }
                }
            }
            return std::optional<std::vector<size_t>>{std::move(slots)};
        };

        if (auto slots = resolve_slots("active_inputs")) { builder.set_active_inputs(std::move(*slots)); }
        if (auto slots = resolve_slots("valid_inputs")) { builder.set_valid_inputs(std::move(*slots)); }
        if (auto slots = resolve_slots("all_valid_inputs")) { builder.set_all_valid_inputs(std::move(*slots)); }
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
        const NodeTypeEnum node_type = nb::cast<NodeTypeEnum>(signature.attr("node_type"));

        const TSMeta *input_schema = input_schema_from_node_signature(signature);
        const TSMeta *output_schema = output_schema_from_node_signature(signature);
        const TSMeta *error_output_schema = error_output_schema_from_node_signature(signature);
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
        if (error_output_schema != nullptr) { builder.error_output_schema(error_output_schema); }
        if (recordable_state_schema != nullptr) { builder.recordable_state_schema(recordable_state_schema); }
        apply_selector_policies(builder, signature, input_schema);
        return builder;
    }

    [[nodiscard]] NodeBuilder make_nested_graph_node_builder(nb::object signature,
                                                             nb::handle scalars,
                                                             nb::object input_builder,
                                                             nb::object output_builder,
                                                             nb::object error_builder,
                                                             const GraphBuilder &nested_graph,
                                                             nb::dict input_node_ids,
                                                             nb::object output_node_id_obj,
                                                             bool try_except)
    {
        int64_t output_node_id = -1;
        if (output_node_id_obj.is_valid() && !output_node_id_obj.is_none()) {
            output_node_id = nb::cast<int64_t>(output_node_id_obj);
        }

        std::unordered_set<int64_t> stub_indices;
        for (auto item : input_node_ids.items()) {
            stub_indices.insert(nb::cast<int64_t>(nb::borrow<nb::tuple>(item)[1]));
        }
        if (output_node_id >= 0) { stub_indices.insert(output_node_id); }

        std::unordered_map<int64_t, int64_t> old_to_new;
        std::vector<NodeBuilder>             clean_builders;
        clean_builders.reserve(nested_graph.node_builder_count());

        int64_t new_index = 0;
        for (size_t old_index = 0; old_index < nested_graph.node_builder_count(); ++old_index) {
            const int64_t old_index_i64 = static_cast<int64_t>(old_index);
            if (stub_indices.contains(old_index_i64)) { continue; }

            NodeBuilder builder = nested_graph.node_builder_at(old_index);
            builder.public_node_index(old_index_i64);
            clean_builders.push_back(std::move(builder));
            old_to_new.emplace(old_index_i64, new_index++);
        }

        std::vector<Edge> clean_edges;
        clean_edges.reserve(nested_graph.edges().size());
        for (const auto &edge : nested_graph.edges()) {
            if (stub_indices.contains(edge.src_node) || stub_indices.contains(edge.dst_node)) { continue; }
            clean_edges.emplace_back(
                old_to_new.at(edge.src_node),
                edge.output_path,
                old_to_new.at(edge.dst_node),
                edge.input_path);
        }

        BoundaryBindingPlan plan;
        for (auto item : input_node_ids.items()) {
            const nb::tuple pair     = nb::borrow<nb::tuple>(item);
            const auto      arg_name = nb::cast<std::string>(pair[0]);
            const auto      stub_idx = nb::cast<int64_t>(pair[1]);
            const TSMeta   *arg_schema = input_schema_for_arg(signature, arg_name);

            for (const auto &edge : nested_graph.edges()) {
                if (edge.src_node == stub_idx && old_to_new.contains(edge.dst_node)) {
                    const TSMeta *source_schema = navigate_bound_source_schema(arg_schema, edge.output_path);
                    plan.inputs.push_back(InputBindingSpec{
                        .arg_name         = arg_name,
                        .mode             = infer_nested_input_binding_mode(source_schema),
                        .child_node_index = old_to_new.at(edge.dst_node),
                        .parent_input_path = edge.output_path,
                        .child_input_path = edge.input_path,
                        .ts_schema        = source_schema,
                    });
                }
            }
        }

        if (output_node_id >= 0) {
            for (const auto &edge : nested_graph.edges()) {
                if (edge.dst_node != output_node_id) { continue; }

                if (old_to_new.contains(edge.src_node)) {
                    plan.outputs.push_back(OutputBindingSpec{
                        .mode              = OutputBindingMode::ALIAS_CHILD_OUTPUT,
                        .child_node_index  = old_to_new.at(edge.src_node),
                        .child_output_path = edge.output_path,
                    });
                    continue;
                }

                for (auto item : input_node_ids.items()) {
                    const nb::tuple pair     = nb::borrow<nb::tuple>(item);
                    const auto      arg_name = nb::cast<std::string>(pair[0]);
                    const auto      stub_idx = nb::cast<int64_t>(pair[1]);
                    if (edge.src_node != stub_idx) { continue; }

                    plan.outputs.push_back(OutputBindingSpec{
                        .mode               = OutputBindingMode::ALIAS_PARENT_INPUT,
                        .parent_arg_name    = arg_name,
                        .child_node_index   = -1,
                        .child_output_path  = edge.output_path,
                        .parent_output_path = {},
                    });
                    break;
                }
            }
        }

        const auto *tmpl = ChildGraphTemplate::create(
            GraphBuilder{std::move(clean_builders), std::move(clean_edges)},
            std::move(plan),
            nb::cast<std::string>(signature.attr("name")),
            ChildGraphTemplateFlags{.has_output = output_node_id >= 0});

        nb::dict unwrapped_inputs;
        nb::object time_series_inputs = signature.attr("time_series_inputs");
        if (!time_series_inputs.is_none()) {
            for (auto item : nb::borrow<nb::dict>(time_series_inputs).items()) {
                const nb::tuple pair = nb::borrow<nb::tuple>(item);
                unwrapped_inputs[pair[0]] = nb::borrow(pair[1]).attr("dereference")();
            }
        }

        nb::object time_series_output = signature.attr("time_series_output");
        nb::object unwrapped_output =
            time_series_output.is_none() ? nb::none() : nb::borrow(time_series_output).attr("dereference")();

        nb::object unwrapped_signature = signature.attr("copy_with")(
            "time_series_inputs"_a = std::move(unwrapped_inputs),
            "time_series_output"_a = std::move(unwrapped_output));

        nb::object unwrapped_input_builder  = std::move(input_builder);
        nb::object unwrapped_output_builder = std::move(output_builder);
        nb::object unwrapped_error_builder  = std::move(error_builder);

        try {
            nb::object create_builders =
                nb::module_::import_("hgraph._wiring._wiring_node_class._wiring_node_class").attr("create_input_output_builders");
            nb::tuple builders = nb::cast<nb::tuple>(create_builders(unwrapped_signature, nb::none()));
            unwrapped_input_builder  = nb::borrow(builders[0]);
            unwrapped_output_builder = nb::borrow(builders[1]);
            unwrapped_error_builder  = nb::borrow(builders[2]);
        } catch (const nb::python_error &) {}

        hgraph::v2::NodeTypeEnum node_type = hgraph::v2::NodeTypeEnum::COMPUTE_NODE;
        const std::string        node_type_name = nb::cast<std::string>(signature.attr("node_type").attr("name"));
        if (node_type_name == "PUSH_SOURCE_NODE") {
            node_type = hgraph::v2::NodeTypeEnum::PUSH_SOURCE_NODE;
        } else if (node_type_name == "PULL_SOURCE_NODE") {
            node_type = hgraph::v2::NodeTypeEnum::PULL_SOURCE_NODE;
        } else if (node_type_name == "SINK_NODE") {
            node_type = hgraph::v2::NodeTypeEnum::SINK_NODE;
        }

        const TSMeta *input_schema = input_schema_from_node_signature(unwrapped_signature);
        const TSMeta *output_schema = output_schema_from_node_signature(unwrapped_signature);
        const TSMeta *error_output_schema = error_output_schema_from_node_signature(unwrapped_signature);

        NodeBuilder builder;
        builder.node_type(node_type)
            .python_signature(nb::borrow(unwrapped_signature))
            .python_scalars(dict_or_empty(nb::borrow(scalars)))
            .python_input_builder(std::move(unwrapped_input_builder))
            .python_output_builder(std::move(unwrapped_output_builder))
            .python_error_builder(std::move(unwrapped_error_builder))
            .implementation_name(nb::cast<std::string>(signature.attr("name")))
            .requires_resolved_schemas(true);

        if (input_schema != nullptr) { builder.input_schema(input_schema); }
        if (output_schema != nullptr) { builder.output_schema(output_schema); }
        if (error_output_schema != nullptr) { builder.error_output_schema(error_output_schema); }
        apply_selector_policies(builder, unwrapped_signature, input_schema);

        if (try_except) {
            hgraph::v2::try_except_graph_implementation(builder, tmpl);
        } else {
            hgraph::v2::nested_graph_implementation(builder, tmpl);
        }
        return builder;
    }

    struct V2GraphExecutor
    {
        V2GraphExecutor(GraphBuilder graph_builder, nb::object run_mode, std::vector<nb::object> observers, bool cleanup_on_error)
            : m_graph_builder(std::move(graph_builder)),
              m_run_mode(normalize_evaluation_mode(run_mode)),
              m_cleanup_on_error(cleanup_on_error)
        {
            static_cast<void>(observers);
        }

        [[nodiscard]] EvaluationMode run_mode() const noexcept { return m_run_mode; }

        [[nodiscard]] nb::object graph() const
        {
            throw std::logic_error("v2 Python graph access is not exposed yet; only execution is supported");
        }

        void run(engine_time_t start_time, engine_time_t end_time)
        {
            try {
                auto engine = EvaluationEngineBuilder{}
                                  .graph_builder(m_graph_builder)
                                  .evaluation_mode(m_run_mode)
                                  .start_time(start_time)
                                  .end_time(end_time)
                                  .cleanup_on_error(m_cleanup_on_error)
                                  .build();
                engine.run();
            } catch (const NodeException &e) {
                try {
                    nb::object hgraph_mod = nb::module_::import_("hgraph");
                    nb::object py_node_exc_cls = hgraph_mod.attr("NodeException");
                    const auto &error = e.error();
                    nb::tuple args = nb::make_tuple(nb::cast(error.signature_name),
                                                    nb::cast(error.label),
                                                    nb::cast(error.wiring_path),
                                                    nb::cast(error.error_msg),
                                                    nb::cast(error.stack_trace),
                                                    nb::cast(error.activation_back_trace),
                                                    nb::cast(error.additional_context));
                    PyErr_SetObject(py_node_exc_cls.ptr(), args.ptr());
                } catch (...) {
                    PyErr_SetString(PyExc_RuntimeError, e.what());
                }
                throw nb::python_error();
            } catch (const nb::python_error &) {
                throw;
            } catch (const std::exception &e) {
                if (PyErr_Occurred()) { throw nb::python_error(); }
                throw nb::builtin_exception(nb::exception_type::runtime_error, e.what());
            }
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
    auto bind_builder_common = [&](auto &cls, auto *builder_type_tag, std::string_view type_name) {
        using builder_type = std::remove_pointer_t<decltype(builder_type_tag)>;
        cls.def(
               "make_instance",
               [type_name](const builder_type &self, nb::tuple owning_graph_id, int64_t node_ndx) -> nb::object {
                   static_cast<void>(self);
                   static_cast<void>(owning_graph_id);
                   static_cast<void>(node_ndx);
                   throw std::logic_error(
                       fmt::format("v2 {} only supports execution through _hgraph.v2.GraphBuilder and _hgraph.v2.GraphExecutor",
                                   type_name));
               },
               "owning_graph_id"_a,
               "node_ndx"_a)
            .def("release_instance",
                 [](const builder_type &self, nb::handle item) {
                     static_cast<void>(self);
                     static_cast<void>(item);
                 },
                 "item"_a)
            .def_prop_ro("signature", [](const builder_type &self) { return object_or_none(self.signature()); })
            .def_prop_ro("scalars", [](const builder_type &self) { return dict_or_empty(self.scalars()); })
            .def_prop_ro("input_builder", [](const builder_type &self) { return object_or_none(self.input_builder()); })
            .def_prop_ro("output_builder", [](const builder_type &self) { return object_or_none(self.output_builder()); })
            .def_prop_ro("error_builder", [](const builder_type &self) { return object_or_none(self.error_builder()); })
            .def_prop_ro(
                "recordable_state_builder",
                [](const builder_type &self) { return object_or_none(self.recordable_state_builder()); })
            .def_prop_ro("implementation_name", [](const builder_type &self) { return self.implementation_name(); })
            .def_prop_ro(
                "input_schema",
                [](const builder_type &self) -> nb::object {
                    return self.input_schema() != nullptr ? nb::cast(self.input_schema(), nb::rv_policy::reference) : nb::none();
                })
            .def_prop_ro(
                "output_schema",
                [](const builder_type &self) -> nb::object {
                    return self.output_schema() != nullptr ? nb::cast(self.output_schema(), nb::rv_policy::reference) : nb::none();
                })
            .def_prop_ro(
                "requires_resolved_schemas",
                [](const builder_type &self) { return self.requires_resolved_schemas(); })
            .def("__repr__",
                 [type_name](const builder_type &self) {
                     return fmt::format(
                         "v2.{}@{:p}[impl={}]",
                         type_name,
                         static_cast<const void *>(&self),
                         self.implementation_name());
                 })
            .def("__str__",
                 [type_name](const builder_type &self) {
                     return fmt::format(
                         "v2.{}@{:p}[impl={}]",
                         type_name,
                         static_cast<const void *>(&self),
                         self.implementation_name());
                 });
    };

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
        .def_static("make_signature", &make_v2_node_signature, "signature"_a);
    bind_builder_common(node_builder_cls, static_cast<hgraph::v2::NodeBuilder *>(nullptr), "NodeBuilder");
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
        .def(
            "__init__",
            [](hgraph::v2::GraphBuilder *self, nb::iterable node_builders, std::vector<hgraph::v2::Edge> edges) {
                new (self) hgraph::v2::GraphBuilder{};
                for (auto item : node_builders) {
                    nb::object builder = nb::borrow(item);
                    if (nb::isinstance<hgraph::v2::NodeBuilder>(builder)) {
                        self->add_node(nb::cast<hgraph::v2::NodeBuilder>(builder));
                    } else {
                        throw std::invalid_argument("v2 GraphBuilder expects v2 node builders");
                    }
                }
                for (auto &edge : edges) { self->add_edge(std::move(edge)); }
            },
            "node_builders"_a,
            "edges"_a)
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
        .def("memory_size", &hgraph::v2::GraphBuilder::memory_size)
        .def_prop_ro("node_builders",
                     [](const hgraph::v2::GraphBuilder &self) {
                         nb::tuple out = nb::steal<nb::tuple>(PyTuple_New(static_cast<Py_ssize_t>(self.node_builder_count())));
                         for (size_t i = 0; i < self.node_builder_count(); ++i) {
                             nb::object item = nb::cast(self.node_builder_at(i));
                             PyTuple_SET_ITEM(
                                 out.ptr(),
                                 static_cast<Py_ssize_t>(i),
                                 item.release().ptr());
                         }
                         return out;
                     })
        .def_prop_ro("edges", [](const hgraph::v2::GraphBuilder &self) { return nb::tuple(nb::cast(self.edges())); })
        .def(
            "__repr__",
            [](const hgraph::v2::GraphBuilder &self) {
                return fmt::format(
                    "v2.GraphBuilder@{:p}[nodes={}, edges={}]",
                    static_cast<const void *>(&self),
                    self.node_builder_count(),
                    self.edges().size());
            })
        .def("__str__",
             [](const hgraph::v2::GraphBuilder &self) {
                 return fmt::format(
                     "v2.GraphBuilder@{:p}[nodes={}, edges={}]",
                     static_cast<const void *>(&self),
                     self.node_builder_count(),
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
    v2.def("reset_child_graph_template_registry", [] { ChildGraphTemplateRegistry::instance().reset(); });
    v2.def("child_graph_template_registry_size", [] { return ChildGraphTemplateRegistry::instance().size(); });
    v2.def("static_sink_call_count", [] { return StaticSinkNode::call_count; });
    v2.def("static_sink_last_value", [] { return StaticSinkNode::last_value; });
    v2.def(
        "build_nested_node",
        &make_nested_graph_node_builder,
        "signature"_a,
        "scalars"_a,
        "input_builder"_a = nb::none(),
        "output_builder"_a = nb::none(),
        "error_builder"_a = nb::none(),
        "nested_graph"_a,
        "input_node_ids"_a,
        "output_node_id"_a = nb::none(),
        "try_except"_a = false);

    hgraph::v2::export_compute_node<StaticSumNode>(v2);
    hgraph::v2::export_compute_node<StaticPolicyNode>(v2);
    hgraph::v2::export_compute_node<StaticGetItemNode>(v2);
    hgraph::v2::export_compute_node<StaticTypedStateNode>(v2);
    hgraph::v2::export_compute_node<StaticRecordableStateNode>(v2);
    hgraph::v2::export_compute_node<StaticClockNode>(v2);
    hgraph::v2::export_compute_node<StaticTickNode>(v2);
    hgraph::v2::export_compute_node<StaticRefBoolNode>(v2);
    hgraph::v2::export_compute_node_from_python_impl<StaticContextOutputNode>(
        v2,
        "hgraph._wiring._context_wiring",
        "get_context_output",
        "get_context_output");
    hgraph::v2::export_compute_node_from_python_impl<StaticCaptureContextNode>(
        v2,
        "hgraph._wiring._context_wiring",
        "capture_context",
        "capture_context");
    hgraph::v2::export_compute_node<StaticSinkNode>(v2);
    hgraph::v2::export_compute_node_from_python_impl<ValidImplNode>(
        v2, "hgraph._impl._operators._time_series_properties", "valid_impl", "valid_impl");
    hgraph::v2::export_compute_node_from_python_impl<TsdGetItemDefaultNode>(
        v2, "hgraph._impl._operators._tsd_operators", "tsd_get_item_default", "tsd_get_item_default");
    hgraph::v2::export_compute_node_from_python_impl<TsdGetItemsNode>(
        v2,
        "hgraph._impl._operators._tsd_operators",
        "tsd_get_items",
        "tsd_get_items",
        hgraph::v2::StaticNodeSignature<TsdGetItemsNode>::wiring_signature("tsd_get_items"));
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::ConstNode>(
        v2, "hgraph._impl._operators._time_series_conversion", "const_default", "const");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::NothingNode>(
        v2, "hgraph._impl._operators._graph_operators", "nothing_impl", "nothing");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::NullSinkNode>(
        v2, "hgraph._impl._operators._graph_operators", "null_sink_impl", "null_sink");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::DebugPrintNode>(
        v2, "hgraph._impl._operators._graph_operators", "debug_print_impl", "debug_print");
}
