#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/time_series/value/keyed_slot_store.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/boundary_binding.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/nested_node_builder.h>
#include <hgraph/types/node_builder.h>
#include <hgraph/types/path_constants.h>
#include <hgraph/types/python_node_support.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <fmt/format.h>
#include <new>
#include <optional>
#include <stdexcept>
#include <unordered_set>

#include <sul/dynamic_bitset.hpp>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] std::string schema_debug_label(const value::TypeMeta *schema)
        {
            if (schema == nullptr) { return "<null>"; }
            return fmt::format("{}@{:p}[kind={}]", schema->name != nullptr ? schema->name : "<unnamed>",
                               static_cast<const void *>(schema),
                               static_cast<int>(schema->kind));
        }

        enum class RootNodePort : size_t
        {
            Input = 0,
            Output = 1,
            ErrorOutput = 2,
            RecordableState = 3,
        };

        [[nodiscard]] TSOutputView bound_output_of(TSInputView view) noexcept {
            if (const LinkedTSContext *target = view.linked_target();
                target != nullptr && target->is_bound() && target->schema != nullptr && target->value_dispatch != nullptr &&
                target->ts_dispatch != nullptr && target->owning_output != nullptr) {
                return TSOutputView{
                    TSViewContext{TSContext{
                        target->schema,
                        target->value_dispatch,
                        target->ts_dispatch,
                        target->value_data,
                        target->ts_state,
                        target->owning_output,
                        target->output_view_ops,
                        target->notification_state != nullptr ? target->notification_state : target->ts_state,
                        target->pending_dict_child,
                    }},
                    TSViewContext::none(),
                    view.evaluation_time(),
                    target->owning_output,
                    target->output_view_ops != nullptr ? target->output_view_ops : &hgraph::detail::default_output_view_ops(),
                };
            }

            const TSViewContext source_context = view.context_ref().resolved();
            if (source_context.schema == nullptr || source_context.value_dispatch == nullptr || source_context.ts_dispatch == nullptr ||
                source_context.owning_output == nullptr) {
                return TSOutputView{};
            }

            return TSOutputView{
                TSViewContext{TSContext{
                    source_context.schema,
                    source_context.value_dispatch,
                    source_context.ts_dispatch,
                    source_context.value_data,
                    source_context.ts_state,
                    source_context.owning_output,
                    source_context.output_view_ops,
                    source_context.notification_state != nullptr ? source_context.notification_state : source_context.ts_state,
                    source_context.pending_dict_child,
                }},
                TSViewContext::none(),
                view.evaluation_time(),
                source_context.owning_output,
                source_context.output_view_ops != nullptr ? source_context.output_view_ops : &hgraph::detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] TSOutputView live_bound_output_of(TSInputView view) noexcept
        {
            if (const LinkedTSContext *target = view.linked_target(); target != nullptr && target->is_bound() &&
                target->schema != nullptr && target->value_dispatch != nullptr && target->ts_dispatch != nullptr &&
                target->owning_output != nullptr) {
                return TSOutputView{
                    TSViewContext{TSContext{
                        target->schema,
                        target->value_dispatch,
                        target->ts_dispatch,
                        target->value_data,
                        target->ts_state,
                        target->owning_output,
                        target->output_view_ops,
                        target->notification_state != nullptr ? target->notification_state : target->ts_state,
                        target->pending_dict_child,
                    }},
                    TSViewContext::none(),
                    view.evaluation_time(),
                    target->owning_output,
                    target->output_view_ops != nullptr ? target->output_view_ops : &hgraph::detail::default_output_view_ops(),
                };
            }

            return bound_output_of(view);
        }

        [[nodiscard]] bool sampled_this_tick(const TSViewContext &context, engine_time_t evaluation_time) noexcept
        {
            const auto snapshot = detail::transition_snapshot(context);
            return snapshot.active() && snapshot.modified_time == evaluation_time;
        }

        [[nodiscard]] bool input_changed(TSInputView view) noexcept
        {
            return view.modified() || sampled_this_tick(view.context_ref(), view.evaluation_time());
        }

        [[nodiscard]] bool output_changed(TSOutputView view) noexcept
        {
            return view.modified() || sampled_this_tick(view.context_ref(), view.evaluation_time());
        }

        [[nodiscard]] LinkedTSContext bound_output_context(const TSOutputView &view) noexcept
        {
            return view.ts_schema() != nullptr ? view.linked_context() : LinkedTSContext{};
        }

        [[nodiscard]] const TSMeta *unwrap_navigation_schema(const TSMeta *schema);

        [[nodiscard]] TSInputView resolve_parent_input_arg(Node &parent, std::string_view arg_name, engine_time_t evaluation_time)
        {
            return parent.input_view(evaluation_time).as_bundle().field(arg_name);
        }

        [[nodiscard]] TSInputView navigate_input(TSInputView view, PathView path)
        {
            const TSMeta *schema = view.ts_schema();
            for (const int64_t slot : path) {
                const TSMeta *collection_schema = unwrap_navigation_schema(schema);
                if (collection_schema == nullptr) { throw std::invalid_argument("nested input navigation requires a schema"); }

                if (slot == k_key_set_path) {
                    if (collection_schema->kind != TSKind::TSD) {
                        throw std::logic_error("nested input key_set navigation requires a dict schema");
                    }
                    view = view.as_dict().key_set().view();
                    schema = view.ts_schema();
                    continue;
                }

                switch (collection_schema->kind) {
                    case TSKind::TSB: view = view.as_bundle()[static_cast<size_t>(slot)]; break;
                    case TSKind::TSL: view = view.as_list()[static_cast<size_t>(slot)]; break;
                    default: throw std::invalid_argument("nested input navigation only supports TSB/TSL/key_set");
                }
                schema = view.ts_schema();
            }
            return view;
        }

        [[nodiscard]] TSInputView select_multiplexed_parent_input(TSInputView parent_field, const value::View &key)
        {
            const TSMeta *schema = unwrap_navigation_schema(parent_field.ts_schema());
            if (schema == nullptr) {
                throw std::logic_error("keyed nested binding requires a collection schema");
            }

            switch (schema->kind) {
                case TSKind::TSD: return parent_field.as_dict()[key];
                case TSKind::TSL: return parent_field.as_list()[static_cast<size_t>(key.as_atomic().as<int>())];
                default: throw std::logic_error("keyed nested binding only supports TSD and TSL parent inputs");
            }
        }

        [[nodiscard]] bool is_live_dict_key(const TSOutputView &view, const value::View &key)
        {
            const auto map = view.value().as_map();
            if (key.schema() != map.key_schema()) { return false; }

            const size_t slot = map.find_slot(key);
            if (slot == static_cast<size_t>(-1)) { return false; }

            const auto delta = map.delta();
            return slot < delta.slot_capacity() && delta.slot_occupied(slot) && !delta.slot_removed(slot);
        }

        [[nodiscard]] TSOutputView ensure_mapped_output_child(const TSOutputView &parent_output,
                                                              const value::View  &key,
                                                              engine_time_t       evaluation_time)
        {
            if (is_live_dict_key(parent_output, key)) { return parent_output.as_dict().at(key); }

            const TSMeta *schema = parent_output.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSD || schema->element_ts() == nullptr ||
                schema->element_ts()->value_type == nullptr) {
                throw std::logic_error("mapped nested output requires a TSD output schema");
            }

            Value blank_value(*schema->element_ts()->value_type, MutationTracking::Plain);
            blank_value.reset();
            {
                auto mutation = parent_output.value().as_map().begin_mutation(evaluation_time);
                mutation.set(key, blank_value.view());
            }
            return parent_output.as_dict().at(key);
        }

        [[nodiscard]] constexpr size_t align_up(size_t value, size_t alignment) noexcept {
            if (alignment == 0) { return value; }
            const size_t remainder = value % alignment;
            return remainder == 0 ? value : value + (alignment - remainder);
        }

        template <typename TTimeSeries>
        void set_root_parent(TTimeSeries *ts, Node *node, RootNodePort port) noexcept
        {
            if (ts == nullptr || node == nullptr) { return; }
            std::visit(
                [node, port](auto &state) {
                    state.parent = node;
                    state.index  = static_cast<size_t>(port);
                },
                ts->root_state_variant());
        }

        [[nodiscard]] const TSMeta *unwrap_navigation_schema(const TSMeta *schema) {
            if (schema != nullptr && schema->kind == TSKind::REF && schema->element_ts() != nullptr) {
                return schema->element_ts();
            }
            return schema;
        }

        [[nodiscard]] TSOutputView navigate_output(TSOutputView view, PathView path) {
            const TSMeta *schema = view.ts_schema();
            for (const int64_t slot : path) {
                const TSMeta *collection_schema = unwrap_navigation_schema(schema);
                if (collection_schema == nullptr) { throw std::invalid_argument("nested output navigation requires a schema"); }

                if (slot == k_key_set_path) {
                    if (collection_schema->kind != TSKind::TSD) {
                        throw std::logic_error("nested output key_set navigation requires a dict schema");
                    }
                    view   = view.as_dict().key_set().view();
                    schema = view.ts_schema();
                    continue;
                }

                switch (collection_schema->kind) {
                    case TSKind::TSB: view = view.as_bundle()[static_cast<size_t>(slot)]; break;
                    case TSKind::TSL: view = view.as_list()[static_cast<size_t>(slot)]; break;
                    default: throw std::invalid_argument("nested output navigation only supports TSB/TSL/key_set");
                }

                schema = view.ts_schema();
            }

            return view;
        }

        [[nodiscard]] std::string signature_attr_or_empty(const nb::object &python_signature, const char *name) {
            if (!python_signature.is_valid() || python_signature.is_none()) { return {}; }

            nb::object value = nb::getattr(python_signature, name, nb::none());
            if (!value.is_valid() || value.is_none()) { return {}; }
            return nb::cast<std::string>(value);
        }

        [[nodiscard]] NodeException python_node_exception(const nb::object &python_signature, std::string error_msg,
                                                          std::string additional_context) {
            return NodeException(NodeErrorInfo{signature_attr_or_empty(python_signature, "signature"),
                                               signature_attr_or_empty(python_signature, "label"),
                                               signature_attr_or_empty(python_signature, "wiring_path_name"), std::move(error_msg),
                                               "", "", std::move(additional_context)});
        }

        struct ResolvedNodeBuilders
        {
            const TSInputBuilder  *input_builder{nullptr};
            const TSOutputBuilder *output_builder{nullptr};
            const TSOutputBuilder *error_output_builder{nullptr};
            const ValueBuilder    *state_builder{nullptr};
            const TSOutputBuilder *recordable_state_builder{nullptr};
        };

        struct NodeMemoryLayout
        {
            size_t total_size{0};
            size_t alignment{alignof(Node)};
            size_t spec_offset{0};
            size_t scheduler_offset{0};
            size_t runtime_data_offset{0};
            size_t label_offset{0};
            size_t active_slots_offset{0};
            size_t valid_slots_offset{0};
            size_t all_valid_slots_offset{0};
            size_t input_object_offset{0};
            size_t input_storage_offset{0};
            size_t output_object_offset{0};
            size_t output_storage_offset{0};
            size_t error_output_object_offset{0};
            size_t error_output_storage_offset{0};
            size_t state_storage_offset{0};
            size_t recordable_state_object_offset{0};
            size_t recordable_state_storage_offset{0};
        };

        struct StaticNodeBuilderState
        {
            const NodeRuntimeOps           *runtime_ops{nullptr};
            const PushSourceNodeRuntimeOps *push_source_runtime_ops{nullptr};
            bool                            has_push_message_hook{false};
        };

        struct PythonNodeBuilderState
        {
            nb::object eval_fn;
            nb::object start_fn;
            nb::object stop_fn;
        };

        struct PythonNodeHeapState
        {
            nb::object   python_signature;
            nb::object   python_scalars;
            nb::object   eval_fn;
            nb::object   start_fn;
            nb::object   stop_fn;
            nb::object   node_handle;
            nb::object   output_handle;
            nb::dict     kwargs;
            nb::tuple    start_parameter_names;
            nb::tuple    stop_parameter_names;
            bool         generator_eval{false};
            nb::iterator generator;
            nb::object   next_value;
        };

        struct PythonNodeRuntimeData
        {
            TSInput             *input{nullptr};
            TSOutput            *output{nullptr};
            TSOutput            *error_output{nullptr};
            TSOutput            *recordable_state{nullptr};
            PythonNodeHeapState *heap_state{nullptr};
        };

        struct SwitchNodeBuilderState
        {
            std::vector<SwitchBranchTemplate> branches;
            bool reload_on_ticked{false};
        };

        struct SwitchNodeRuntimeData
        {
            TSInput            *input{nullptr};
            TSOutput           *output{nullptr};
            TSOutput           *error_output{nullptr};
            TSOutput           *recordable_state{nullptr};
            std::vector<SwitchBranchTemplate> branches;
            ChildGraphInstance  child_instance;
            std::optional<Value> active_key;
            size_t              active_branch_index{static_cast<size_t>(-1)};
            int64_t             next_child_graph_id{1};
            size_t              child_graph_storage_size{0};
            size_t              child_graph_storage_alignment{alignof(std::max_align_t)};
            bool                reload_on_ticked{false};
            bool                bound{false};
        };

        [[nodiscard]] nb::object push_queue_remove_sentinel() {
            static nb::object value = nb::module_::import_("hgraph").attr("REMOVE");
            return nb::borrow(value);
        }

        [[nodiscard]] nb::object push_queue_remove_if_exists_sentinel() {
            static nb::object value = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
            return nb::borrow(value);
        }

        [[nodiscard]] nb::object append_tuple_item(const nb::object &existing, const nb::object &item) {
            const size_t existing_len = PyTuple_Size(existing.ptr());
            nb::tuple    result       = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
            for (size_t i = 0; i < existing_len; ++i) {
                PyTuple_SET_ITEM(result.ptr(), i, nb::borrow(existing[i]).release().ptr());
            }
            PyTuple_SET_ITEM(result.ptr(), existing_len, nb::borrow(item).release().ptr());
            return std::move(result);
        }

        [[nodiscard]] bool bool_scalar_or(const nb::dict &scalars, std::string_view key, bool default_value) {
            const nb::str py_key{key.data(), key.size()};
            if (!PyMapping_HasKey(scalars.ptr(), py_key.ptr())) { return default_value; }
            return nb::cast<bool>(nb::steal<nb::object>(PyObject_GetItem(scalars.ptr(), py_key.ptr())));
        }

        [[nodiscard]] nb::object normalize_python_ref_result(const TSMeta *output_schema,
                                                             nb::object result,
                                                             engine_time_t evaluation_time)
        {
            if (!result.is_valid() || result.is_none() || output_schema == nullptr || output_schema->kind != TSKind::REF ||
                !nb::isinstance<TimeSeriesReference>(result)) {
                return result;
            }

            const TimeSeriesReference ref = nb::cast<TimeSeriesReference>(result);
            if (!ref.is_peered() || ref.target().schema == nullptr || ref.target().schema->kind != TSKind::REF) { return result; }

            TSOutputView target_view = ref.target_view(evaluation_time);
            const View target_value = target_view.value();
            if (target_value.has_value()) {
                if (const auto *inner_ref = target_value.as_atomic().template try_as<TimeSeriesReference>(); inner_ref != nullptr) {
                    return nb::cast(*inner_ref);
                }
            }

            return result;
        }

        [[nodiscard]] bool python_push_source_apply_message(Node &node, PythonNodeRuntimeData &runtime_data,
                                                            const value::Value &message, engine_time_t evaluation_time) {
            static_cast<void>(node);

            if (runtime_data.output == nullptr) { throw std::logic_error("v2 Python push-source nodes require an output"); }

            nb::gil_scoped_acquire guard;
            const nb::object       py_message  = message.to_python();
            TSOutputView           output_view = runtime_data.output->view(evaluation_time);

            const nb::dict scalars = runtime_data.heap_state != nullptr && runtime_data.heap_state->python_scalars.is_valid() &&
                                             !runtime_data.heap_state->python_scalars.is_none()
                                         ? nb::cast<nb::dict>(runtime_data.heap_state->python_scalars)
                                         : nb::dict();
            const bool     elide   = bool_scalar_or(scalars, "elide", false);
            const bool     batch   = bool_scalar_or(scalars, "batch", false);

            if (batch) {
                const TSMeta *schema = output_view.ts_schema();
                if (schema != nullptr && schema->kind == TSKind::TSD) {
                    const nb::dict   message_dict     = nb::cast<nb::dict>(py_message);
                    const nb::object remove           = push_queue_remove_sentinel();
                    const nb::object remove_if_exists = push_queue_remove_if_exists_sentinel();

                    for (auto item : message_dict.items()) {
                        const nb::tuple  pair  = nb::borrow<nb::tuple>(item);
                        const nb::object key   = nb::borrow(pair[0]);
                        const nb::object value = nb::borrow(pair[1]);
                        if (!value.is(remove) && !value.is(remove_if_exists)) { continue; }

                        Value key_value{*schema->key_type(), MutationTracking::Plain};
                        key_value.reset();
                        key_value.from_python(key);
                        TSOutputView child_view = output_view.as_dict().at(key_value.view());
                        if (child_view.context_ref().is_bound() && child_view.modified()) { return false; }
                    }

                    for (auto item : message_dict.items()) {
                        const nb::tuple  pair  = nb::borrow<nb::tuple>(item);
                        const nb::object key   = nb::borrow(pair[0]);
                        const nb::object value = nb::borrow(pair[1]);

                        Value key_value{*schema->key_type(), MutationTracking::Plain};
                        key_value.reset();
                        key_value.from_python(key);

                        if (value.is(remove) || value.is(remove_if_exists)) {
                            TSOutputView child_view = output_view.as_dict().at(key_value.view());
                            if (child_view.context_ref().is_bound()) { output_view.as_dict().erase(key_value.view()); }
                            continue;
                        }

                        TSOutputView child_view = output_view.as_dict().at(key_value.view());
                        if (child_view.context_ref().is_bound() && child_view.modified()) {
                            child_view.from_python(append_tuple_item(child_view.to_python(), value));
                        } else {
                            child_view.from_python(nb::make_tuple(value));
                        }
                    }
                    return true;
                }

                if (output_view.modified()) {
                    output_view.from_python(append_tuple_item(output_view.to_python(), py_message));
                } else {
                    output_view.from_python(nb::make_tuple(py_message));
                }
                return true;
            }

            if (elide || output_view.can_apply_result(py_message)) {
                output_view.apply_result(py_message);
                return true;
            }
            return false;
        }

        [[nodiscard]] nb::object call_python_push_source_start(nb::handle callable, nb::handle sender, nb::handle kwargs) {
            nb::gil_scoped_acquire guard;
            if (!callable.is_valid() || callable.is_none()) { return nb::none(); }
            nb::tuple args   = nb::make_tuple(nb::borrow(sender));
            PyObject *result = PyObject_Call(callable.ptr(), args.ptr(), kwargs.ptr());
            if (result == nullptr) { throw nb::python_error(); }
            return nb::steal<nb::object>(result);
        }

        [[nodiscard]] bool python_callable_is_generator_function(nb::handle callable) {
            if (!callable.is_valid() || callable.is_none()) { return false; }

            const nb::object code = nb::getattr(callable, "__code__", nb::none());
            if (!code.is_valid() || code.is_none()) { return false; }

            const nb::object flags = nb::getattr(code, "co_flags", nb::none());
            if (!flags.is_valid() || flags.is_none()) { return false; }

            constexpr int co_generator = 0x20;
            return (nb::cast<int>(flags) & co_generator) != 0;
        }

        template <typename TState> [[nodiscard]] const void *make_builder_state(TState state) {
            return new TState(std::move(state));
        }

        template <> [[nodiscard]] const void *make_builder_state(PythonNodeBuilderState state) {
            return new PythonNodeBuilderState(std::move(state));
        }

        template <typename TState> [[nodiscard]] const void *clone_builder_state(const void *ptr) {
            return ptr != nullptr ? new TState(*static_cast<const TState *>(ptr)) : nullptr;
        }

        template <> [[nodiscard]] const void *clone_builder_state<PythonNodeBuilderState>(const void *ptr) {
            if (ptr == nullptr) { return nullptr; }
            nb::gil_scoped_acquire guard;
            return new PythonNodeBuilderState(*static_cast<const PythonNodeBuilderState *>(ptr));
        }

        template <typename TState> void destroy_builder_state(const void *ptr) noexcept { delete static_cast<const TState *>(ptr); }

        template <> void destroy_builder_state<PythonNodeBuilderState>(const void *ptr) noexcept {
            if (ptr == nullptr) { return; }
            if (Py_IsInitialized() == 0) {
                // Python-backed builder state can outlive interpreter teardown
                // via registry-owned child templates. At shutdown we prefer a
                // bounded leak over touching dead interpreter state.
                return;
            }
            nb::gil_scoped_acquire guard;
            delete static_cast<const PythonNodeBuilderState *>(ptr);
        }

        [[nodiscard]] ResolvedNodeBuilders resolve_builders(const NodeBuilder                          &builder,
                                                            const std::vector<TSInputConstructionEdge> &inbound_edges) {
            ResolvedNodeBuilders builders;
            const bool has_dependency_only_edges =
                std::all_of(inbound_edges.begin(), inbound_edges.end(),
                            [](const TSInputConstructionEdge &edge) { return edge.input_path.empty(); });

            if (builder.input_schema() != nullptr) {
                try {
                    const TSInputConstructionPlan plan =
                        TSInputConstructionPlanCompiler::compile(*builder.input_schema(), inbound_edges);
                    builders.input_builder = &TSInputBuilderFactory::checked_builder_for(plan);
                } catch (const std::exception &e) {
                    throw std::invalid_argument(
                        fmt::format("input builder resolution failed for node '{}': {}", builder.label(), e.what()));
                }
            } else if (!inbound_edges.empty() && !has_dependency_only_edges) {
                std::vector<std::string> edge_descriptions;
                edge_descriptions.reserve(inbound_edges.size());
                for (const auto &edge : inbound_edges) {
                    edge_descriptions.push_back(fmt::format("src={} input_path={}",
                                                            edge.binding.src_node,
                                                            fmt::format("{}", edge.input_path)));
                }
                const std::string node_name =
                    !builder.label().empty() ? std::string{builder.label()} : builder.implementation_name();
                throw std::invalid_argument(
                    fmt::format("node '{}' without an input schema cannot accept inbound edges: {}",
                                node_name,
                                fmt::join(edge_descriptions, ", ")));
            }

            if (builder.output_schema() != nullptr) {
                builders.output_builder = &TSOutputBuilderFactory::checked_builder_for(builder.output_schema());
            }
            if (builder.error_output_schema() != nullptr) {
                builders.error_output_builder = &TSOutputBuilderFactory::checked_builder_for(builder.error_output_schema());
            }
            if (builder.error_output_schema() == nullptr && builder.error_builder().is_valid() &&
                !builder.error_builder().is_none()) {
                throw std::invalid_argument("nodes with error capture outputs require a resolved time-series schema");
            }
            if (builder.has_state() && builder.state_schema() == nullptr) {
                throw std::invalid_argument("nodes with typed State<...> require a resolved value schema");
            }
            if (builder.has_state() && builder.state_schema() != nullptr) {
                builders.state_builder = &ValueBuilderFactory::checked_builder_for(builder.state_schema());
            }
            if (builder.recordable_state_schema() != nullptr) {
                builders.recordable_state_builder = &TSOutputBuilderFactory::checked_builder_for(builder.recordable_state_schema());
            }
            if (builder.recordable_state_schema() == nullptr && builder.recordable_state_builder().is_valid() &&
                !builder.recordable_state_builder().is_none()) {
                throw std::invalid_argument("nodes with RecordableState<...> require a resolved time-series schema");
            }

            return builders;
        }

        [[nodiscard]] NodeMemoryLayout describe_layout(const NodeBuilder &builder, size_t runtime_data_size,
                                                       size_t                      runtime_data_alignment,
                                                       const ResolvedNodeBuilders &builders) noexcept {
            NodeMemoryLayout layout;
            layout.alignment = std::max(alignof(Node), alignof(BuiltNodeSpec));

            size_t offset      = sizeof(Node);
            offset             = align_up(offset, alignof(BuiltNodeSpec));
            layout.spec_offset = offset;
            offset += sizeof(BuiltNodeSpec);

            if (builder.uses_scheduler()) {
                layout.alignment        = std::max(layout.alignment, alignof(NodeScheduler));
                offset                  = align_up(offset, alignof(NodeScheduler));
                layout.scheduler_offset = offset;
                offset += sizeof(NodeScheduler);
            }

            layout.alignment           = std::max(layout.alignment, runtime_data_alignment);
            offset                     = align_up(offset, runtime_data_alignment);
            layout.runtime_data_offset = offset;
            offset += runtime_data_size;

            if (builders.state_builder != nullptr) {
                layout.alignment            = std::max(layout.alignment, builders.state_builder->alignment());
                offset                      = align_up(offset, builders.state_builder->alignment());
                layout.state_storage_offset = offset;
                offset += builders.state_builder->size();
            }

            if (!builder.label().empty()) {
                layout.label_offset = offset;
                offset += builder.label().size();
            }

            layout.alignment           = std::max(layout.alignment, alignof(size_t));
            offset                     = align_up(offset, alignof(size_t));
            layout.active_slots_offset = offset;
            offset += sizeof(size_t) * builder.active_inputs().size();

            offset                    = align_up(offset, alignof(size_t));
            layout.valid_slots_offset = offset;
            offset += sizeof(size_t) * builder.valid_inputs().size();

            offset                        = align_up(offset, alignof(size_t));
            layout.all_valid_slots_offset = offset;
            offset += sizeof(size_t) * builder.all_valid_inputs().size();

            if (builders.input_builder != nullptr) {
                layout.alignment           = std::max(layout.alignment, alignof(TSInput));
                offset                     = align_up(offset, alignof(TSInput));
                layout.input_object_offset = offset;
                offset += sizeof(TSInput);

                layout.alignment            = std::max(layout.alignment, builders.input_builder->alignment());
                offset                      = align_up(offset, builders.input_builder->alignment());
                layout.input_storage_offset = offset;
                offset += builders.input_builder->size();
            }

            if (builders.output_builder != nullptr) {
                layout.alignment            = std::max(layout.alignment, alignof(TSOutput));
                offset                      = align_up(offset, alignof(TSOutput));
                layout.output_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment             = std::max(layout.alignment, builders.output_builder->alignment());
                offset                       = align_up(offset, builders.output_builder->alignment());
                layout.output_storage_offset = offset;
                offset += builders.output_builder->size();
            }

            if (builders.error_output_builder != nullptr) {
                layout.alignment                  = std::max(layout.alignment, alignof(TSOutput));
                offset                            = align_up(offset, alignof(TSOutput));
                layout.error_output_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment                   = std::max(layout.alignment, builders.error_output_builder->alignment());
                offset                             = align_up(offset, builders.error_output_builder->alignment());
                layout.error_output_storage_offset = offset;
                offset += builders.error_output_builder->size();
            }

            if (builders.recordable_state_builder != nullptr) {
                layout.alignment                      = std::max(layout.alignment, alignof(TSOutput));
                offset                                = align_up(offset, alignof(TSOutput));
                layout.recordable_state_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment                       = std::max(layout.alignment, builders.recordable_state_builder->alignment());
                offset                                 = align_up(offset, builders.recordable_state_builder->alignment());
                layout.recordable_state_storage_offset = offset;
                offset += builders.recordable_state_builder->size();
            }

            layout.total_size = offset;
            return layout;
        }

        [[nodiscard]] std::span<const size_t> materialize_slots(std::byte *base, std::span<const size_t> source,
                                                                size_t slots_offset) {
            if (source.empty()) { return {}; }

            auto *slots = reinterpret_cast<size_t *>(base + slots_offset);
            std::copy(source.begin(), source.end(), slots);
            return {slots, source.size()};
        }

        void validate_static_contract(const NodeBuilder &builder) {
            const auto &state = detail::node_builder_type_state<StaticNodeBuilderState>(builder);
            if (builder.node_type() == NodeTypeEnum::PUSH_SOURCE_NODE && !state.has_push_message_hook) {
                throw std::logic_error("v2 push source nodes require a static bool apply_message(...) hook");
            }
        }

        void validate_python_contract(const NodeBuilder &builder) {
            const auto &state = detail::node_builder_type_state<PythonNodeBuilderState>(builder);
            if (!state.eval_fn.is_valid() || state.eval_fn.is_none()) {
                throw std::invalid_argument("v2 Python node builder requires an eval function");
            }
        }

        void destruct_static_node(Node &node) noexcept {
            const BuiltNodeSpec &spec         = node.spec();
            auto                &runtime_data = detail::runtime_data<detail::StaticNodeRuntimeData>(node);

            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.state_builder != nullptr && runtime_data.state_memory != nullptr) {
                runtime_data.state_builder->destruct(runtime_data.state_memory);
            }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }

            if (runtime_data.python_scalars.is_valid()) {
                nb::gil_scoped_acquire guard;
                std::destroy_at(&runtime_data);
            } else {
                std::destroy_at(&runtime_data);
            }
            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
        }

        void destruct_python_node(Node &node) noexcept {
            const BuiltNodeSpec &spec         = node.spec();
            auto                &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);

            if (runtime_data.heap_state != nullptr) {
                nb::gil_scoped_acquire guard;
                delete runtime_data.heap_state;
            }

            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }

            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void python_node_start(Node &node, engine_time_t) {
            auto                  &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            nb::gil_scoped_acquire guard;
            auto                  &heap_state = *runtime_data.heap_state;
            heap_state.kwargs =
                make_python_node_kwargs(heap_state.python_signature, heap_state.python_scalars, heap_state.node_handle);
            if (node.is_push_source_node()) {
                auto *receiver = node.graph() != nullptr ? node.graph()->push_message_receiver() : nullptr;
                if (receiver == nullptr) { throw std::logic_error("v2 Python push-source nodes require a push-message receiver"); }

                nb::object sender = nb::cpp_function([receiver, node_index = node.node_index()](nb::object message) {
                    nb::gil_scoped_acquire sender_guard;
                    receiver->enqueue({node_index, value::Value{std::move(message)}});
                });

                try {
                    static_cast<void>(call_python_push_source_start(heap_state.eval_fn, sender, heap_state.kwargs));
                } catch (const NodeException &) { throw; } catch (const std::exception &e) {
                    throw python_node_exception(heap_state.python_signature, e.what(), "During push-queue start");
                } catch (...) {
                    throw python_node_exception(heap_state.python_signature,
                                                "Unknown non-standard exception during push-queue start",
                                                "During push-queue start");
                }
                return;
            }
            if (node.is_pull_source_node() && heap_state.generator_eval) {
                heap_state.generator =
                    nb::cast<nb::iterator>(call_python_node_eval(heap_state.python_signature, heap_state.eval_fn, heap_state.kwargs));
                heap_state.next_value = nb::object();
                if (node.graph() != nullptr) { node.graph()->schedule_node(node.node_index(), node.evaluation_time()); }
                return;
            }
            static_cast<void>(
                call_python_callable_with_subset(heap_state.start_fn, heap_state.kwargs, heap_state.start_parameter_names));
        }

        void python_node_stop(Node &node, engine_time_t) {
            auto                  &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            nb::gil_scoped_acquire guard;
            auto                  &heap_state = *runtime_data.heap_state;
            static_cast<void>(
                call_python_callable_with_subset(heap_state.stop_fn, heap_state.kwargs, heap_state.stop_parameter_names));
        }

        void python_node_eval(Node &node, engine_time_t) {
            auto                  &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            nb::gil_scoped_acquire guard;
            auto                  &heap_state = *runtime_data.heap_state;
            try {
                if (node.is_push_source_node()) { return; }
                if (node.is_pull_source_node() && heap_state.generator_eval) {
                    const engine_time_t evaluation_time = node.evaluation_time();
                    auto                next_time       = MIN_DT;
                    nb::object          out;
                    const auto          sentinel = nb::iterator::sentinel();

                    auto datetime       = nb::module_::import_("datetime");
                    auto timedelta_type = datetime.attr("timedelta");
                    auto datetime_type  = datetime.attr("datetime");

                    for (nb::iterator value = ++heap_state.generator; value != sentinel; ++value) {
                        auto item = *value;
                        if (value.is_none()) {
                            out = nb::none();
                            break;
                        }

                        auto time = nb::cast<nb::object>(item[0]);
                        out       = nb::cast<nb::object>(item[1]);
                        if (nb::isinstance(time, timedelta_type)) {
                            next_time = evaluation_time + nb::cast<engine_time_delta_t>(time);
                        } else if (nb::isinstance(time, datetime_type)) {
                            next_time = nb::cast<engine_time_t>(time);
                        } else {
                            throw std::runtime_error("Type of time value not recognised");
                        }

                        if (next_time >= evaluation_time && !out.is_none()) { break; }
                    }

                    if (next_time > MIN_DT && next_time <= evaluation_time) {
                        if (heap_state.output_handle.is_valid() && !heap_state.output_handle.is_none() &&
                            nb::cast<engine_time_t>(heap_state.output_handle.attr("last_modified_time")) == next_time) {
                            throw std::runtime_error(fmt::format("Duplicate time produced by generator: [{:%FT%T%z}] - {}",
                                                                 next_time, nb::str(out).c_str()));
                        }
                        if (!out.is_none() && heap_state.output_handle.is_valid() && !heap_state.output_handle.is_none()) {
                            heap_state.output_handle.attr("apply_result")(out);
                        }
                        heap_state.next_value = nb::none();
                        python_node_eval(node, evaluation_time);
                        return;
                    }

                    if (heap_state.next_value.is_valid() && !heap_state.next_value.is_none()) {
                        if (heap_state.output_handle.is_valid() && !heap_state.output_handle.is_none()) {
                            heap_state.output_handle.attr("apply_result")(heap_state.next_value);
                        }
                        heap_state.next_value = nb::none();
                    }

                    if (next_time != MIN_DT) {
                        heap_state.next_value = out;
                        if (node.graph() != nullptr) { node.graph()->schedule_node(node.node_index(), next_time); }
                    }
                    return;
                }

                nb::object out = call_python_node_eval(heap_state.python_signature, heap_state.eval_fn, heap_state.kwargs);
                if (std::getenv("HGRAPH_DEBUG_PY_NODE") != nullptr && heap_state.python_signature.is_valid() &&
                    !heap_state.python_signature.is_none()) {
                    const std::string node_name = nb::cast<std::string>(heap_state.python_signature.attr("name"));
                    std::fprintf(stderr, "python_node_eval name=%s out=%s\n", node_name.c_str(), nb::str(out).c_str());
                    std::fflush(stderr);
                    if (node_name == "union_multiple_tss") {
                        auto summarize_tss = [](nb::handle value) {
                            if (!value.is_valid() || value.is_none()) { return std::string{"<none>"}; }
                            nb::object valid = nb::getattr(value, "valid", nb::none());
                            nb::object current = nb::getattr(value, "value", nb::none());
                            nb::object added = nb::none();
                            nb::object removed = nb::none();
                            if (PyObject_HasAttrString(value.ptr(), "added")) { added = value.attr("added")(); }
                            if (PyObject_HasAttrString(value.ptr(), "removed")) { removed = value.attr("removed")(); }
                            return fmt::format("valid={} value={} added={} removed={}",
                                               nb::str(valid).c_str(),
                                               nb::str(current).c_str(),
                                               nb::str(added).c_str(),
                                               nb::str(removed).c_str());
                        };
                        auto summarize_iter = [&](nb::handle iterable) {
                            if (!iterable.is_valid() || iterable.is_none()) { return std::string{"<none>"}; }
                            std::vector<std::string> parts;
                            for (auto item : iterable) { parts.push_back(summarize_tss(nb::borrow(item))); }
                            return fmt::format("[{}]", fmt::join(parts, "; "));
                        };
                        nb::object tsl = nb::steal<nb::object>(PyObject_GetItem(heap_state.kwargs.ptr(), nb::str("tsl").ptr()));
                        PyObject *output_item = PyObject_GetItem(heap_state.kwargs.ptr(), nb::str("_output").ptr());
                        nb::object output = output_item != nullptr ? nb::steal<nb::object>(output_item) : nb::none();
                        std::fprintf(stderr,
                                     "python_node_eval union_multiple_tss modified=%s valid=%s output_value=%s out=%s\n",
                                     summarize_iter(tsl.attr("modified_values")()).c_str(),
                                     summarize_iter(tsl.attr("valid_values")()).c_str(),
                                     summarize_tss(output).c_str(),
                                     nb::str(out).c_str());
                        std::fflush(stderr);
                    } else if (node_name == "bit_or_tsss") {
                        auto render_arg = [&](const char *name) {
                            if (!PyObject_HasAttrString(heap_state.kwargs.ptr(), name)) { return std::string{"<missing>"}; }
                            nb::object arg = nb::steal<nb::object>(PyObject_GetItem(heap_state.kwargs.ptr(), nb::str(name).ptr()));
                            if (!arg.is_valid() || arg.is_none()) { return std::string{"<none>"}; }
                            nb::object valid = nb::getattr(arg, "valid", nb::none());
                            nb::object value = nb::getattr(arg, "value", nb::none());
                            nb::object added = nb::none();
                            nb::object removed = nb::none();
                            if (PyObject_HasAttrString(arg.ptr(), "added")) { added = arg.attr("added")(); }
                            if (PyObject_HasAttrString(arg.ptr(), "removed")) { removed = arg.attr("removed")(); }
                            return fmt::format("valid={} value={} added={} removed={}",
                                               nb::str(valid).c_str(),
                                               nb::str(value).c_str(),
                                               nb::str(added).c_str(),
                                               nb::str(removed).c_str());
                        };
                        std::fprintf(stderr,
                                     "python_node_eval bit_or_tsss lhs=%s rhs=%s out=%s\n",
                                     render_arg("lhs").c_str(),
                                     render_arg("rhs").c_str(),
                                     nb::str(out).c_str());
                        std::fflush(stderr);
                    }
                }
                if (!out.is_none() && runtime_data.output != nullptr && heap_state.output_handle.is_valid() &&
                    !heap_state.output_handle.is_none()) {
                    const TSMeta *output_schema = runtime_data.output->view(node.evaluation_time()).ts_schema();
                    out = normalize_python_ref_result(output_schema, std::move(out), node.evaluation_time());
                    heap_state.output_handle.attr("apply_result")(out);
                }
            } catch (const NodeException &) { throw; } catch (const std::exception &e) {
                throw python_node_exception(heap_state.python_signature, e.what(), "During evaluation");
            } catch (...) {
                throw python_node_exception(heap_state.python_signature, "Unknown non-standard exception during node evaluation",
                                            "During evaluation");
            }
        }

        [[nodiscard]] bool python_node_has_input(const Node &node) noexcept {
            return node.data() != nullptr && detail::runtime_data<PythonNodeRuntimeData>(node).input != nullptr;
        }

        [[nodiscard]] bool python_node_has_output(const Node &node) noexcept {
            return node.data() != nullptr && detail::runtime_data<PythonNodeRuntimeData>(node).output != nullptr;
        }

        [[nodiscard]] bool python_node_has_error_output(const Node &node) noexcept {
            return node.data() != nullptr && detail::runtime_data<PythonNodeRuntimeData>(node).error_output != nullptr;
        }

        [[nodiscard]] bool python_node_has_recordable_state(const Node &node) noexcept {
            return node.data() != nullptr && detail::runtime_data<PythonNodeRuntimeData>(node).recordable_state != nullptr;
        }

        [[nodiscard]] bool python_node_apply_message(Node &node, const value::Value &message, engine_time_t evaluation_time) {
            auto &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            try {
                return python_push_source_apply_message(node, runtime_data, message, evaluation_time);
            } catch (const NodeException &) { throw; } catch (const std::exception &e) {
                auto &heap_state = *runtime_data.heap_state;
                throw python_node_exception(heap_state.python_signature, e.what(), "During push-source message application");
            } catch (...) {
                auto &heap_state = *runtime_data.heap_state;
                throw python_node_exception(heap_state.python_signature,
                                            "Unknown non-standard exception during push-source message application",
                                            "During push-source message application");
            }
        }

        [[nodiscard]] TSInputView python_node_input_view(Node &node, engine_time_t evaluation_time) {
            return detail::runtime_data<PythonNodeRuntimeData>(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView python_node_output_view(Node &node, engine_time_t evaluation_time) {
            return detail::runtime_data<PythonNodeRuntimeData>(node).output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView python_node_error_output_view(Node &node, engine_time_t evaluation_time) {
            auto *error_output = detail::runtime_data<PythonNodeRuntimeData>(node).error_output;
            return error_output != nullptr ? error_output->view(evaluation_time) : detail::invalid_output_view(evaluation_time);
        }

        [[nodiscard]] TSOutputView python_node_recordable_state_view(Node &node, engine_time_t evaluation_time) {
            auto *recordable_state = detail::runtime_data<PythonNodeRuntimeData>(node).recordable_state;
            return recordable_state != nullptr ? recordable_state->view(evaluation_time)
                                               : detail::invalid_output_view(evaluation_time);
        }

        [[nodiscard]] std::string python_node_runtime_label(const Node &node) {
            const auto &spec = node.spec();
            if (!spec.label.empty()) { return std::string(spec.label); }
            return "python_node";
        }

        const NodeRuntimeOps k_python_node_runtime_ops{
            &python_node_start,
            &python_node_stop,
            &python_node_eval,
            &python_node_has_input,
            &python_node_has_output,
            &python_node_has_error_output,
            &python_node_has_recordable_state,
            &python_node_input_view,
            &python_node_output_view,
            &python_node_error_output_view,
            &python_node_recordable_state_view,
            &python_node_runtime_label,
        };

        const PushSourceNodeRuntimeOps k_python_node_push_source_runtime_ops{
            &python_node_apply_message,
        };

        template <typename TState, typename TBuildRuntimeData, typename TPopulateRuntime>
        [[nodiscard]] Node *
        construct_node_chunk(const NodeBuilder &builder, void *memory, int64_t node_index,
                             const std::vector<TSInputConstructionEdge> &inbound_edges, const NodeRuntimeOps *runtime_ops,
                             const PushSourceNodeRuntimeOps *push_source_runtime_ops, void (*destruct_fn)(Node &) noexcept,
                             size_t runtime_data_size, size_t runtime_data_alignment, TBuildRuntimeData build_runtime_data,
                             TPopulateRuntime populate_runtime) {
            const auto builders = resolve_builders(builder, inbound_edges);
            const auto layout   = describe_layout(builder, runtime_data_size, runtime_data_alignment, builders);
            auto      *base     = static_cast<std::byte *>(memory);

            std::string_view label_view;
            if (!builder.label().empty()) {
                auto *label_ptr = reinterpret_cast<char *>(base + layout.label_offset);
                std::memcpy(label_ptr, builder.label().data(), builder.label().size());
                label_view = std::string_view{label_ptr, builder.label().size()};
            }

            const auto materialized_active_inputs = materialize_slots(base, builder.active_inputs(), layout.active_slots_offset);
            const auto materialized_valid_inputs  = materialize_slots(base, builder.valid_inputs(), layout.valid_slots_offset);
            const auto materialized_all_valid_inputs =
                materialize_slots(base, builder.all_valid_inputs(), layout.all_valid_slots_offset);

            TSInput       *input                    = nullptr;
            TSOutput      *output                   = nullptr;
            TSOutput      *error_output             = nullptr;
            void          *state_memory             = nullptr;
            TSOutput      *recordable_state         = nullptr;
            auto           cleanup_input            = hgraph::make_scope_exit([&] {
                if (input != nullptr) { input->~TSInput(); }
            });
            auto           cleanup_output           = hgraph::make_scope_exit([&] {
                if (output != nullptr) { output->~TSOutput(); }
            });
            auto           cleanup_error_output     = hgraph::make_scope_exit([&] {
                if (error_output != nullptr) { error_output->~TSOutput(); }
            });
            auto           cleanup_state            = hgraph::make_scope_exit([&] {
                if (builders.state_builder != nullptr && state_memory != nullptr) {
                    builders.state_builder->destruct(state_memory);
                }
            });
            auto           cleanup_recordable_state = hgraph::make_scope_exit([&] {
                if (recordable_state != nullptr) { recordable_state->~TSOutput(); }
            });
            void          *runtime_data             = nullptr;
            auto           cleanup_runtime_data     = hgraph::make_scope_exit([&] {
                if (runtime_data == nullptr) { return; }
                populate_runtime.destroy(runtime_data);
            });
            NodeScheduler *scheduler                = nullptr;
            auto           cleanup_scheduler        = hgraph::make_scope_exit([&] {
                if (scheduler != nullptr) { std::destroy_at(scheduler); }
            });
            BuiltNodeSpec *spec                     = nullptr;
            auto           cleanup_spec             = hgraph::make_scope_exit([&] {
                if (spec != nullptr) { std::destroy_at(spec); }
            });
            Node          *node                     = nullptr;
            auto           cleanup_node             = hgraph::make_scope_exit([&] {
                if (node != nullptr) { node->~Node(); }
            });

            if (builders.input_builder != nullptr) {
                input = new (base + layout.input_object_offset) TSInput{};
                builders.input_builder->construct_input(*input, base + layout.input_storage_offset,
                                                        TSInputBuilder::MemoryOwnership::External);
            }

            if (builders.output_builder != nullptr) {
                output = new (base + layout.output_object_offset) TSOutput{};
                builders.output_builder->construct_output(*output, base + layout.output_storage_offset,
                                                          TSOutputBuilder::MemoryOwnership::External);
            }

            if (builders.error_output_builder != nullptr) {
                error_output = new (base + layout.error_output_object_offset) TSOutput{};
                builders.error_output_builder->construct_output(*error_output, base + layout.error_output_storage_offset,
                                                                TSOutputBuilder::MemoryOwnership::External);
            }

            if (builders.state_builder != nullptr) {
                state_memory = base + layout.state_storage_offset;
                builders.state_builder->construct(state_memory);
            }

            if (builders.recordable_state_builder != nullptr) {
                recordable_state = new (base + layout.recordable_state_object_offset) TSOutput{};
                builders.recordable_state_builder->construct_output(
                    *recordable_state, base + layout.recordable_state_storage_offset, TSOutputBuilder::MemoryOwnership::External);
            }

            runtime_data = build_runtime_data(base + layout.runtime_data_offset, builders, input, output, error_output,
                                              state_memory, recordable_state);

            spec = new (base + layout.spec_offset) BuiltNodeSpec{
                runtime_ops,
                push_source_runtime_ops,
                destruct_fn,
                layout.scheduler_offset,
                layout.runtime_data_offset,
                builder.uses_scheduler(),
                label_view,
                builder.node_type(),
                builder.public_node_index() >= 0 ? builder.public_node_index() : node_index,
                builder.input_schema(),
                builder.output_schema(),
                builder.error_output_schema(),
                builder.recordable_state_schema(),
                builder.has_explicit_active_inputs(),
                builder.has_explicit_valid_inputs(),
                builder.has_explicit_all_valid_inputs(),
                materialized_active_inputs,
                materialized_valid_inputs,
                materialized_all_valid_inputs,
            };

            node = new (memory) Node(node_index, spec);
            set_root_parent(input, node, RootNodePort::Input);
            set_root_parent(output, node, RootNodePort::Output);
            set_root_parent(error_output, node, RootNodePort::ErrorOutput);
            set_root_parent(recordable_state, node, RootNodePort::RecordableState);
            if (builder.uses_scheduler()) { scheduler = new (base + layout.scheduler_offset) NodeScheduler{node}; }
            populate_runtime.initialise(builder, runtime_data, node, input, output, error_output, recordable_state);

            cleanup_node.release();
            cleanup_spec.release();
            cleanup_scheduler.release();
            cleanup_runtime_data.release();
            cleanup_recordable_state.release();
            cleanup_error_output.release();
            cleanup_state.release();
            cleanup_output.release();
            cleanup_input.release();
            return node;
        }

        [[nodiscard]] size_t static_builder_size(const NodeBuilder                          &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges) {
            validate_static_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(detail::StaticNodeRuntimeData), alignof(detail::StaticNodeRuntimeData), builders)
                .total_size;
        }

        [[nodiscard]] size_t static_builder_alignment(const NodeBuilder                          &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges) {
            validate_static_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(detail::StaticNodeRuntimeData), alignof(detail::StaticNodeRuntimeData), builders)
                .alignment;
        }

        [[nodiscard]] Node *construct_static_builder_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                        const std::vector<TSInputConstructionEdge> &inbound_edges) {
            validate_static_contract(builder);
            const auto &state = detail::node_builder_type_state<StaticNodeBuilderState>(builder);

            struct RuntimePopulator
            {
                static void *build(void *storage, const ResolvedNodeBuilders &builders, TSInput *input, TSOutput *output,
                                   TSOutput *error_output, void *state_memory, TSOutput *recordable_state,
                                   const NodeBuilder &builder) {
                    return new (storage) detail::StaticNodeRuntimeData{
                        input,
                        output,
                        error_output,
                        builders.state_builder,
                        state_memory,
                        recordable_state,
                        builder.scalars().is_valid() ? nb::borrow(builder.scalars()) : nb::object(),
                    };
                }

                static void destroy(void *runtime_data) {
                    auto &static_runtime = *static_cast<detail::StaticNodeRuntimeData *>(runtime_data);
                    if (static_runtime.python_scalars.is_valid()) {
                        nb::gil_scoped_acquire guard;
                        std::destroy_at(&static_runtime);
                    } else {
                        std::destroy_at(&static_runtime);
                    }
                }

                static void initialise(const NodeBuilder &, void *, Node *, TSInput *, TSOutput *, TSOutput *, TSOutput *) {}
            };

            struct RuntimeLifecycle
            {
                void destroy(void *runtime_data) const { RuntimePopulator::destroy(runtime_data); }
                void initialise(const NodeBuilder &builder, void *runtime_data, Node *node, TSInput *input, TSOutput *output,
                                TSOutput *error_output, TSOutput *recordable_state) const {
                    static_cast<void>(builder);
                    static_cast<void>(runtime_data);
                    static_cast<void>(node);
                    static_cast<void>(input);
                    static_cast<void>(output);
                    static_cast<void>(error_output);
                    static_cast<void>(recordable_state);
                }
            };

            return construct_node_chunk<StaticNodeBuilderState>(
                builder, memory, node_index, inbound_edges, state.runtime_ops, state.push_source_runtime_ops, &destruct_static_node,
                sizeof(detail::StaticNodeRuntimeData), alignof(detail::StaticNodeRuntimeData),
                [&](void *storage, const ResolvedNodeBuilders &builders, TSInput *input, TSOutput *output, TSOutput *error_output,
                    void *state_memory, TSOutput *recordable_state) {
                    return RuntimePopulator::build(storage, builders, input, output, error_output, state_memory, recordable_state,
                                                   builder);
                },
                RuntimeLifecycle{});
        }

        [[nodiscard]] size_t python_builder_size(const NodeBuilder                          &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges) {
            validate_python_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(PythonNodeRuntimeData), alignof(PythonNodeRuntimeData), builders).total_size;
        }

        [[nodiscard]] size_t python_builder_alignment(const NodeBuilder                          &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges) {
            validate_python_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(PythonNodeRuntimeData), alignof(PythonNodeRuntimeData), builders).alignment;
        }

        [[nodiscard]] Node *construct_python_builder_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                        const std::vector<TSInputConstructionEdge> &inbound_edges) {
            validate_python_contract(builder);

            struct RuntimeLifecycle
            {
                void destroy(void *runtime_data) const {
                    auto &python_runtime = *static_cast<PythonNodeRuntimeData *>(runtime_data);
                    if (python_runtime.heap_state != nullptr) {
                        nb::gil_scoped_acquire guard;
                        delete python_runtime.heap_state;
                    }
                    std::destroy_at(&python_runtime);
                }

                void initialise(const NodeBuilder &builder, void *runtime_data_ptr, Node *node, TSInput *input, TSOutput *output,
                                TSOutput *error_output, TSOutput *recordable_state) const {
                    auto       &runtime_data = *static_cast<PythonNodeRuntimeData *>(runtime_data_ptr);
                    const auto &state        = detail::node_builder_type_state<PythonNodeBuilderState>(builder);

                    nb::gil_scoped_acquire guard;
                    runtime_data.heap_state = new PythonNodeHeapState{
                        builder.signature().is_valid() ? nb::borrow(builder.signature()) : nb::object(),
                        builder.scalars().is_valid() ? nb::borrow(builder.scalars()) : nb::object(),
                        state.eval_fn.is_valid() ? nb::borrow(state.eval_fn) : nb::object(),
                        state.start_fn.is_valid() ? nb::borrow(state.start_fn) : nb::object(),
                        state.stop_fn.is_valid() ? nb::borrow(state.stop_fn) : nb::object(),
                        nb::object(),
                        nb::object(),
                        nb::dict(),
                        nb::make_tuple(),
                        nb::make_tuple(),
                        false,
                        nb::iterator{},
                        nb::object(),
                    };
                    runtime_data.heap_state->node_handle = make_python_node_handle(
                        runtime_data.heap_state->python_signature, runtime_data.heap_state->python_scalars, node, input, output,
                        error_output, recordable_state, builder.input_schema(), builder.output_schema(),
                        builder.error_output_schema(), builder.recordable_state_schema(), node->scheduler_if_present());
                    runtime_data.heap_state->output_handle = runtime_data.heap_state->node_handle.attr("output");
                    runtime_data.heap_state->start_parameter_names =
                        python_callable_parameter_names(runtime_data.heap_state->start_fn);
                    runtime_data.heap_state->stop_parameter_names =
                        python_callable_parameter_names(runtime_data.heap_state->stop_fn);
                    runtime_data.heap_state->generator_eval =
                        builder.node_type() == NodeTypeEnum::PULL_SOURCE_NODE &&
                        python_callable_is_generator_function(runtime_data.heap_state->eval_fn);
                }
            };

            return construct_node_chunk<PythonNodeBuilderState>(
                builder, memory, node_index, inbound_edges, &k_python_node_runtime_ops,
                builder.node_type() == NodeTypeEnum::PUSH_SOURCE_NODE ? &k_python_node_push_source_runtime_ops : nullptr,
                &destruct_python_node, sizeof(PythonNodeRuntimeData), alignof(PythonNodeRuntimeData),
                [](void *storage, const ResolvedNodeBuilders &, TSInput *input, TSOutput *output, TSOutput *error_output, void *,
                   TSOutput *recordable_state) -> void * {
                    return new (storage) PythonNodeRuntimeData{input, output, error_output, recordable_state, nullptr};
                },
                RuntimeLifecycle{});
        }

        void destruct_builder_node(const NodeBuilder &builder, Node &node) noexcept {
            static_cast<void>(builder);
            node.spec().destruct(node);
        }

        // ================================================================
        // Nested graph node family
        // ================================================================

        [[nodiscard]] NestedNodeRuntimeData &nested_runtime(Node &node) {
            return *static_cast<NestedNodeRuntimeData *>(node.data());
        }

        [[nodiscard]] const NestedNodeRuntimeData &nested_runtime(const Node &node) {
            return *static_cast<const NestedNodeRuntimeData *>(node.data());
        }

        [[nodiscard]] bool ensure_nested_child_bound(Node &node, NestedNodeRuntimeData &runtime, engine_time_t evaluation_time);

        void nested_node_start(Node &node, engine_time_t evaluation_time) {
            auto &runtime = nested_runtime(node);
            if (!runtime.child_instance.is_initialised()) {
                if (runtime.child_template == nullptr) {
                    throw std::logic_error("nested node start requires a child template");
                }
                runtime.child_instance.initialise(*runtime.child_template, node, node.node_id());
            }

            // Bind the child boundary before start(). Child nodes should see
            // the same startup contract as ordinary nodes: if they have inputs,
            // those inputs are already wired when start() runs. This matters
            // for start-time behaviors such as context capture/export.
            static_cast<void>(ensure_nested_child_bound(node, runtime, evaluation_time));

            if (!runtime.child_instance.is_started()) { runtime.child_instance.start(evaluation_time); }

            // If the nested operator has no input bindings (e.g., a nested graph
            // with only const/pull sources), schedule it for immediate evaluation.
            if (runtime.child_instance.boundary_plan().inputs.empty()) { node.notify(evaluation_time); }
        }

        void clear_child_output_links(TSOutputView parent_output, NestedNodeRuntimeData &runtime) {
            for (const auto &spec : runtime.child_instance.boundary_plan().outputs) {
                clear_output_link(navigate_output(parent_output, spec.parent_output_path));
            }
        }

        void rebind_nested_direct_inputs(Node &node, NestedNodeRuntimeData &runtime, engine_time_t evaluation_time)
        {
            if (!runtime.bound || runtime.child_instance.graph() == nullptr) { return; }

            std::unordered_set<std::string> changed_args;
            for (const auto &spec : runtime.child_instance.boundary_plan().inputs) {
                if (spec.child_node_index < 0) { continue; }

                switch (spec.mode) {
                    case InputBindingMode::BIND_DIRECT:
                    case InputBindingMode::CLONE_REF_BINDING:
                    case InputBindingMode::DETACH_RESTORE_BLANK:
                        {
                            TSInputView parent_input = resolve_parent_input_arg(node, spec.arg_name, evaluation_time);
                            if (!spec.parent_input_path.empty()) { parent_input = navigate_input(parent_input, spec.parent_input_path); }
                            if (input_changed(parent_input)) { changed_args.emplace(spec.arg_name); }
                            break;
                        }
                    case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                    case InputBindingMode::BIND_KEY_VALUE:
                        break;
                }
            }

            for (const std::string &arg_name : changed_args) {
                BoundaryBindingRuntime::rebind(runtime.child_instance.boundary_plan(),
                                              *runtime.child_instance.graph(),
                                              node,
                                              arg_name,
                                              evaluation_time);
            }
        }

        void prepare_child_output_links(TSOutput *output, const ChildGraphTemplate *child_template, bool try_except_output_root) {
            if (output == nullptr || child_template == nullptr) { return; }

            TSOutputView parent_output = output->view(MIN_DT);
            if (try_except_output_root) { parent_output = parent_output.as_bundle().field("out"); }

            for (const auto &spec : child_template->boundary_plan.outputs) {
                prepare_output_link(navigate_output(parent_output, spec.parent_output_path));
            }
        }

        void nested_node_stop(Node &node, engine_time_t evaluation_time) {
            auto &runtime = nested_runtime(node);
            if (runtime.bound) { clear_child_output_links(node.output_view(evaluation_time), runtime); }
            if (runtime.child_instance.is_started()) { runtime.child_instance.stop(evaluation_time); }
            if (runtime.bound && runtime.child_instance.graph() != nullptr) {
                BoundaryBindingRuntime::unbind(runtime.child_instance.boundary_plan(), *runtime.child_instance.graph());
                runtime.bound = false;
            }
        }

        void try_except_node_stop(Node &node, engine_time_t evaluation_time) {
            auto &runtime = nested_runtime(node);
            if (runtime.bound) { clear_child_output_links(node.output_view(evaluation_time).as_bundle().field("out"), runtime); }
            if (runtime.child_instance.is_started()) { runtime.child_instance.stop(evaluation_time); }
            if (runtime.bound && runtime.child_instance.graph() != nullptr) {
                BoundaryBindingRuntime::unbind(runtime.child_instance.boundary_plan(), *runtime.child_instance.graph());
                runtime.bound = false;
            }
        }

        void forward_child_outputs(Node &node, TSOutputView parent_output, NestedNodeRuntimeData &runtime, engine_time_t evaluation_time);

        [[nodiscard]] bool ensure_nested_child_bound(Node &node, NestedNodeRuntimeData &runtime, engine_time_t evaluation_time) {
            if (runtime.bound || node.graph() == nullptr) { return false; }
            BoundaryBindingRuntime::bind(runtime.child_instance.boundary_plan(), *runtime.child_instance.graph(), node,
                                         evaluation_time);
            runtime.bound = true;
            return true;
        }

        void publish_reduce_aggregate_output(const TSOutputView &target_output,
                                             const TSOutputView &source_output,
                                             engine_time_t       evaluation_time);

        void forward_child_outputs(Node &node, TSOutputView parent_output, NestedNodeRuntimeData &runtime, engine_time_t evaluation_time) {
            const auto &plan = runtime.child_instance.boundary_plan();
            for (const auto &spec : plan.outputs) {
                TSOutputView source_output;
                switch (spec.mode) {
                    case OutputBindingMode::ALIAS_CHILD_OUTPUT:
                        {
                            if (spec.child_node_index < 0) { continue; }
                            auto &child_node = runtime.child_instance.graph()->node_at(static_cast<size_t>(spec.child_node_index));
                            source_output = navigate_output(child_node.output_view(evaluation_time), spec.child_output_path);
                            break;
                        }
                    case OutputBindingMode::ALIAS_PARENT_INPUT:
                        {
                            if (runtime.input == nullptr) {
                                throw std::logic_error("parent-input output alias requires a parent input");
                            }
                            auto parent_input = runtime.input->view(&node, evaluation_time);
                            source_output = bound_output_of(parent_input.as_bundle().field(spec.parent_arg_name));
                            if (source_output.ts_schema() == nullptr) { continue; }
                            source_output = navigate_output(source_output, spec.child_output_path);
                            break;
                        }
                    default:
                        throw std::logic_error("forward_child_outputs does not support this output binding mode");
                }

                TSOutputView target_output = navigate_output(parent_output, spec.parent_output_path);

                if (const auto *parent_schema = target_output.ts_schema(); parent_schema != nullptr &&
                                                                           !binding_compatible_ts_schema(source_output.ts_schema(), parent_schema) &&
                                                                           source_output.owning_output() != nullptr) {
                    if (std::getenv("HGRAPH_DEBUG_BINDABLE") != nullptr) {
                        std::fprintf(stderr,
                                     "bindable site=node_builder:1355 bound=%d valid=%d source_kind=%d target_kind=%d\n",
                                     source_output.context_ref().is_bound(),
                                     source_output.valid(),
                                     source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                     parent_schema != nullptr ? static_cast<int>(parent_schema->kind) : -1);
                    }
                    source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
                }

                publish_reduce_aggregate_output(target_output, source_output, evaluation_time);
            }
        }

        [[nodiscard]] NodeErrorInfo fallback_try_except_error(const Node &node, std::string error_msg) {
            return NodeErrorInfo{
                node.runtime_label(), std::string{node.label()}, {}, std::move(error_msg), {}, {}, {},
            };
        }

        void publish_try_except_error(Node &node, engine_time_t evaluation_time, const NodeErrorInfo &error) {
            nb::gil_scoped_acquire guard;
            nb::object             py_error = nb::module_::import_("hgraph").attr("NodeError")(
                error.signature_name, error.label, error.wiring_path, error.error_msg, error.stack_trace,
                error.activation_back_trace, error.additional_context.empty() ? nb::none() : nb::cast(error.additional_context));

            auto output = node.output_view(evaluation_time);
            if (output.context_ref().schema != nullptr && output.context_ref().schema->kind == TSKind::TSB) {
                output.as_bundle().field("exception").from_python(py_error);
            } else {
                output.from_python(py_error);
            }

            if (node.has_error_output()) { node.error_output_view(evaluation_time).from_python(py_error); }
        }

        void stop_try_except_child_after_error(Node &node, engine_time_t evaluation_time) noexcept {
            try {
                try_except_node_stop(node, evaluation_time);
            } catch (...) {}
        }

        void nested_node_eval(Node &node, engine_time_t evaluation_time) {
            auto &runtime = nested_runtime(node);
            if (!runtime.child_instance.is_started()) { return; }

            if (ensure_nested_child_bound(node, runtime, evaluation_time)) {
                forward_child_outputs(node, node.output_view(evaluation_time), runtime, MIN_DT);
            }
            rebind_nested_direct_inputs(node, runtime, evaluation_time);
            runtime.child_instance.evaluate(evaluation_time);
            forward_child_outputs(node, node.output_view(evaluation_time), runtime, evaluation_time);
        }

        void try_except_node_eval(Node &node, engine_time_t evaluation_time) {
            auto &runtime = nested_runtime(node);
            if (!runtime.child_instance.is_started()) { return; }

            if (ensure_nested_child_bound(node, runtime, evaluation_time)) {
                forward_child_outputs(node, node.output_view(evaluation_time).as_bundle().field("out"), runtime, MIN_DT);
            }
            rebind_nested_direct_inputs(node, runtime, evaluation_time);

            try {
                runtime.child_instance.evaluate(evaluation_time);
            } catch (const NodeException &e) {
                stop_try_except_child_after_error(node, evaluation_time);
                publish_try_except_error(node, evaluation_time, e.error());
                return;
            } catch (const std::exception &e) {
                stop_try_except_child_after_error(node, evaluation_time);
                publish_try_except_error(node, evaluation_time, fallback_try_except_error(node, e.what()));
                return;
            } catch (...) {
                stop_try_except_child_after_error(node, evaluation_time);
                publish_try_except_error(node, evaluation_time,
                                         fallback_try_except_error(node, "Unknown non-standard exception during node evaluation"));
                return;
            }

            forward_child_outputs(node, node.output_view(evaluation_time).as_bundle().field("out"), runtime, evaluation_time);
        }

        [[nodiscard]] bool nested_has_input(const Node &node) noexcept {
            return node.data() != nullptr && nested_runtime(node).input != nullptr;
        }

        [[nodiscard]] bool nested_has_output(const Node &node) noexcept {
            return node.data() != nullptr && nested_runtime(node).output != nullptr;
        }

        [[nodiscard]] bool nested_has_error_output(const Node &node) noexcept {
            return node.data() != nullptr && nested_runtime(node).error_output != nullptr;
        }

        [[nodiscard]] bool nested_has_recordable_state(const Node & /*node*/) noexcept { return false; }

        [[nodiscard]] TSInputView nested_input_view(Node &node, engine_time_t evaluation_time) {
            if (!nested_has_input(node)) { return detail::invalid_input_view(evaluation_time); }
            return nested_runtime(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView nested_output_view(Node &node, engine_time_t evaluation_time) {
            if (!nested_has_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return nested_runtime(node).output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView nested_error_output_view(Node &node, engine_time_t evaluation_time) {
            if (!nested_has_error_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return nested_runtime(node).error_output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView nested_recordable_state_view(Node & /*node*/, engine_time_t evaluation_time) {
            return detail::invalid_output_view(evaluation_time);
        }

        [[nodiscard]] std::string nested_runtime_label(const Node &node) { return detail::default_runtime_label(node); }

        const NodeRuntimeOps k_nested_runtime_ops{
            &nested_node_start,
            &nested_node_stop,
            &nested_node_eval,
            &nested_has_input,
            &nested_has_output,
            &nested_has_error_output,
            &nested_has_recordable_state,
            &nested_input_view,
            &nested_output_view,
            &nested_error_output_view,
            &nested_recordable_state_view,
            &nested_runtime_label,
        };

        const NodeRuntimeOps k_try_except_runtime_ops{
            &nested_node_start,  &try_except_node_stop,     &try_except_node_eval,         &nested_has_input,
            &nested_has_output,  &nested_has_error_output,  &nested_has_recordable_state,  &nested_input_view,
            &nested_output_view, &nested_error_output_view, &nested_recordable_state_view, &nested_runtime_label,
        };

        void destruct_nested_node(Node &node) noexcept {
            const BuiltNodeSpec &spec         = node.spec();
            auto                &runtime_data = detail::runtime_data<NestedNodeRuntimeData>(node);

            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }

            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void validate_nested_contract(const NodeBuilder & /*builder*/) {}

        [[nodiscard]] size_t nested_builder_size(const NodeBuilder                          &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges) {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(NestedNodeRuntimeData), alignof(NestedNodeRuntimeData), builders).total_size;
        }

        [[nodiscard]] size_t nested_builder_alignment(const NodeBuilder                          &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges) {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(NestedNodeRuntimeData), alignof(NestedNodeRuntimeData), builders).alignment;
        }

        [[nodiscard]] Node *construct_nested_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                const std::vector<TSInputConstructionEdge> &inbound_edges,
                                                const NodeRuntimeOps                       *runtime_ops,
                                                bool                                        try_except_output_root) {
            const auto &state = detail::node_builder_type_state<NestedNodeBuilderState>(builder);

            struct RuntimeLifecycle
            {
                const ChildGraphTemplate *child_template;

                void destroy(void *runtime_data) const { std::destroy_at(static_cast<NestedNodeRuntimeData *>(runtime_data)); }

                void initialise(const NodeBuilder & /*builder*/, void *runtime_data_ptr, Node *node, TSInput * /*input*/,
                                TSOutput *output, TSOutput * /*error_output*/, TSOutput * /*recordable_state*/) const {
                    auto &runtime = *static_cast<NestedNodeRuntimeData *>(runtime_data_ptr);
                    runtime.child_template = child_template;
                    prepare_child_output_links(output, child_template, try_except_output_root);
                }

                bool try_except_output_root{false};
            };

            return construct_node_chunk<NestedNodeBuilderState>(
                builder, memory, node_index, inbound_edges, runtime_ops, nullptr, &destruct_nested_node,
                sizeof(NestedNodeRuntimeData), alignof(NestedNodeRuntimeData),
                [](void *storage, const ResolvedNodeBuilders &, TSInput *input, TSOutput *output, TSOutput *error_output,
                   void * /*state_memory*/, TSOutput *recordable_state) -> void * {
                    return new (storage) NestedNodeRuntimeData{input, output, error_output, recordable_state, nullptr, {}, false};
                },
                RuntimeLifecycle{state.child_template, try_except_output_root});
        }

        [[nodiscard]] Node *nested_construct_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                const std::vector<TSInputConstructionEdge> &inbound_edges) {
            return construct_nested_at(builder, memory, node_index, inbound_edges, &k_nested_runtime_ops, false);
        }

        [[nodiscard]] Node *try_except_construct_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                    const std::vector<TSInputConstructionEdge> &inbound_edges) {
            return construct_nested_at(builder, memory, node_index, inbound_edges, &k_try_except_runtime_ops, true);
        }

        [[nodiscard]] SwitchNodeRuntimeData &switch_runtime(Node &node)
        {
            return *static_cast<SwitchNodeRuntimeData *>(node.data());
        }

        [[nodiscard]] const SwitchBranchTemplate *active_switch_branch(const SwitchNodeRuntimeData &runtime) noexcept
        {
            return runtime.active_branch_index < runtime.branches.size() ? &runtime.branches[runtime.active_branch_index] : nullptr;
        }

        [[nodiscard]] int64_t next_switch_child_graph_id(SwitchNodeRuntimeData &runtime) noexcept
        {
            return -runtime.next_child_graph_id++;
        }

        struct SwitchChildGraphStorageLayout
        {
            size_t size{0};
            size_t alignment{alignof(std::max_align_t)};
        };

        [[nodiscard]] SwitchChildGraphStorageLayout
        describe_switch_child_graph_storage(const std::vector<SwitchBranchTemplate> &branches)
        {
            SwitchChildGraphStorageLayout layout;
            for (const auto &branch : branches) {
                if (branch.child_template == nullptr) { continue; }
                layout.size = std::max(layout.size, branch.child_template->graph_builder.memory_size());
                layout.alignment = std::max(layout.alignment, branch.child_template->graph_builder.alignment());
            }
            return layout;
        }

        [[nodiscard]] size_t switch_runtime_storage_size(const std::vector<SwitchBranchTemplate> &branches)
        {
            const auto layout = describe_switch_child_graph_storage(branches);
            return align_up(sizeof(SwitchNodeRuntimeData), layout.alignment) + layout.size;
        }

        [[nodiscard]] size_t switch_runtime_storage_alignment(const std::vector<SwitchBranchTemplate> &branches)
        {
            return std::max(alignof(SwitchNodeRuntimeData), describe_switch_child_graph_storage(branches).alignment);
        }

        [[nodiscard]] GraphStorageReservation switch_child_graph_storage(SwitchNodeRuntimeData &runtime) noexcept
        {
            if (runtime.child_graph_storage_size == 0) { return {}; }

            auto *storage = reinterpret_cast<std::byte *>(&runtime) +
                            align_up(sizeof(SwitchNodeRuntimeData), runtime.child_graph_storage_alignment);
            return GraphStorageReservation{storage, runtime.child_graph_storage_size, runtime.child_graph_storage_alignment};
        }

        [[nodiscard]] size_t select_switch_branch(const SwitchNodeRuntimeData &runtime, const value::View &selector)
        {
            size_t default_branch_index = static_cast<size_t>(-1);
            for (size_t index = 0; index < runtime.branches.size(); ++index) {
                const auto &branch = runtime.branches[index];
                if (branch.is_default) {
                    default_branch_index = index;
                    continue;
                }
                if (branch.selector_value.has_value() && branch.selector_value.view() == selector) { return index; }
            }

            if (default_branch_index != static_cast<size_t>(-1)) { return default_branch_index; }
            throw std::runtime_error(fmt::format("switch_ has no graph defined for selector {}", selector.to_string()));
        }

        [[nodiscard]] TSOutputView resolve_switch_source_output(Node &node,
                                                                SwitchNodeRuntimeData &runtime,
                                                                const OutputBindingSpec &spec,
                                                                engine_time_t evaluation_time)
        {
            switch (spec.mode) {
                case OutputBindingMode::ALIAS_CHILD_OUTPUT:
                    {
                        if (spec.child_node_index < 0 || runtime.child_instance.graph() == nullptr) { return {}; }
                        auto &child_node = runtime.child_instance.graph()->node_at(static_cast<size_t>(spec.child_node_index));
                        return navigate_output(child_node.output_view(evaluation_time), spec.child_output_path);
                    }
                case OutputBindingMode::ALIAS_PARENT_INPUT:
                    {
                        if (runtime.input == nullptr) {
                            throw std::logic_error("switch parent-input output alias requires a parent input");
                        }
                        auto parent_input = runtime.input->view(&node, evaluation_time);
                        TSOutputView source_output = bound_output_of(parent_input.as_bundle().field(spec.parent_arg_name));
                        if (source_output.ts_schema() == nullptr) { return {}; }
                        return navigate_output(source_output, spec.child_output_path);
                    }
                default:
                    throw std::logic_error("switch output binding mode is not supported");
            }
        }

        void clear_output_value(TSOutputView output);

        void clear_output_links_tree(TSOutputView output, bool clear_values)
        {
            const TSMeta *schema = output.ts_schema();
            if (schema != nullptr) {
                switch (schema->kind) {
                    case TSKind::TSB:
                        for (size_t index = 0; index < schema->field_count(); ++index) {
                            clear_output_links_tree(output.as_bundle()[index], clear_values);
                        }
                        break;
                    case TSKind::TSL:
                        for (size_t index = 0; index < schema->fixed_size(); ++index) {
                            clear_output_links_tree(output.as_list()[index], clear_values);
                        }
                        break;
                    case TSKind::TSD:
                        if (output.valid()) {
                            constexpr size_t no_slot = static_cast<size_t>(-1);
                            auto map = output.value().as_map();
                            for (size_t slot = map.first_live_slot(); slot != no_slot; slot = map.next_live_slot(slot)) {
                                Value key = map.delta().key_at_slot(slot).clone();
                                clear_output_links_tree(detail::ensure_dict_child_output_view(output, key.view()), clear_values);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            clear_output_link(output);
            if (clear_values) { clear_output_value(output); }
        }

        void clear_switch_output_links(Node &node, SwitchNodeRuntimeData &runtime, engine_time_t evaluation_time)
        {
            if (!node.has_output()) { return; }
            const auto *branch = active_switch_branch(runtime);
            if (branch == nullptr) { return; }

            TSOutputView parent_output = node.output_view(evaluation_time);
            for (const auto &spec : branch->child_template->boundary_plan.outputs) {
                TSOutputView target_output = navigate_output(parent_output, spec.parent_output_path);
                clear_output_links_tree(target_output, evaluation_time != MIN_DT);
            }
        }

        void stop_switch_child(Node &node, SwitchNodeRuntimeData &runtime, engine_time_t evaluation_time) noexcept
        {
            try {
                clear_switch_output_links(node, runtime, evaluation_time);
            } catch (...) {}
            try {
                if (runtime.child_instance.is_started()) { runtime.child_instance.stop(evaluation_time); }
            } catch (...) {}
            try {
                if (runtime.bound && runtime.child_instance.graph() != nullptr) {
                    BoundaryBindingRuntime::unbind(runtime.child_instance.boundary_plan(), *runtime.child_instance.graph());
                }
            } catch (...) {}
            runtime.bound = false;
        }

        [[nodiscard]] bool ensure_switch_child_bound(Node &node, SwitchNodeRuntimeData &runtime, engine_time_t evaluation_time)
        {
            if (runtime.bound || runtime.child_instance.graph() == nullptr) { return false; }
            BoundaryBindingRuntime::bind(runtime.child_instance.boundary_plan(), *runtime.child_instance.graph(), node, evaluation_time);
            runtime.bound = true;
            return true;
        }

        void forward_switch_child_outputs(Node &node, TSOutputView parent_output, SwitchNodeRuntimeData &runtime,
                                          engine_time_t evaluation_time)
        {
            const auto &plan = runtime.child_instance.boundary_plan();
            for (const auto &spec : plan.outputs) {
                TSOutputView source_output = resolve_switch_source_output(node, runtime, spec, evaluation_time);

                TSOutputView target_output = navigate_output(parent_output, spec.parent_output_path);

                if (const auto *parent_schema = target_output.ts_schema();
                    parent_schema != nullptr && !binding_compatible_ts_schema(source_output.ts_schema(), parent_schema) &&
                    source_output.owning_output() != nullptr) {
                    if (std::getenv("HGRAPH_DEBUG_BINDABLE") != nullptr) {
                        std::fprintf(stderr,
                                     "bindable site=node_builder:1720 bound=%d valid=%d source_kind=%d target_kind=%d\n",
                                     source_output.context_ref().is_bound(),
                                     source_output.valid(),
                                     source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                     parent_schema != nullptr ? static_cast<int>(parent_schema->kind) : -1);
                    }
                    source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
                }

                if (std::getenv("HGRAPH_DEBUG_SWITCH") != nullptr) {
                    std::fprintf(stderr,
                                 "switch_forward eval=%lld source_schema=%p source_valid=%d source_modified=%d target_schema=%p\n",
                                 static_cast<long long>(evaluation_time.time_since_epoch().count()),
                                 static_cast<const void *>(source_output.ts_schema()),
                                 source_output.valid(),
                                 output_changed(source_output),
                                 static_cast<const void *>(target_output.ts_schema()));
                }

                if (!source_output.ts_schema()) {
                    clear_output_links_tree(target_output, evaluation_time != MIN_DT);
                    if (evaluation_time != MIN_DT) { mark_output_view_modified(target_output, evaluation_time); }
                    continue;
                }

                if (!source_output.valid()) {
                    clear_output_links_tree(target_output, evaluation_time != MIN_DT);
                    if (evaluation_time != MIN_DT) { mark_output_view_modified(target_output, evaluation_time); }
                    continue;
                }

                if (const auto *schema = source_output.ts_schema();
                    schema != nullptr &&
                    (schema->kind == TSKind::TSB || schema->kind == TSKind::TSD || schema->kind == TSKind::TSS)) {
                    publish_reduce_aggregate_output(target_output, source_output, evaluation_time);
                    continue;
                }

                const bool rebound = bind_output_link(target_output, source_output);
                if (std::getenv("HGRAPH_DEBUG_SWITCH") != nullptr) {
                    std::fprintf(stderr, "switch_forward_bound eval=%lld rebound=%d target_valid=%d\n",
                                 static_cast<long long>(evaluation_time.time_since_epoch().count()), rebound,
                                 target_output.valid());
                }
                if (evaluation_time != MIN_DT && (rebound || output_changed(source_output))) {
                    mark_output_view_modified(target_output, evaluation_time);
                }
            }
        }

        void clear_output_value(TSOutputView output)
        {
            const TSMeta *schema = output.ts_schema();
            if (schema == nullptr) { return; }

            switch (schema->kind) {
                case TSKind::TSB:
                case TSKind::TSL:
                case TSKind::TSD:
                case TSKind::TSS:
                    output.clear();
                    break;
                default:
                    output.invalidate();
                    break;
            }
        }

        void activate_switch_branch(Node &node, SwitchNodeRuntimeData &runtime, size_t branch_index, const value::View &selector,
                                    engine_time_t evaluation_time)
        {
            // Switching branches must publish the delta from the old child to
            // the new child in the current tick. Clearing the old branch at
            // MIN_DT suppresses removals for collection outputs.
            stop_switch_child(node, runtime, evaluation_time);
            runtime.child_instance = ChildGraphInstance{};

            const auto &branch = runtime.branches[branch_index];
            std::vector<int64_t> graph_id = node.node_id();
            graph_id.push_back(next_switch_child_graph_id(runtime));

            runtime.child_instance.initialise(*branch.child_template, node, std::move(graph_id), selector.to_string(),
                                              switch_child_graph_storage(runtime));
            static_cast<void>(ensure_switch_child_bound(node, runtime, evaluation_time));
            runtime.child_instance.start(evaluation_time);
            runtime.active_branch_index = branch_index;
            runtime.active_key = selector.clone(MutationTracking::Plain);
        }

        [[nodiscard]] bool switch_has_input(const Node &node) noexcept
        {
            return node.data() != nullptr && switch_runtime(const_cast<Node &>(node)).input != nullptr;
        }

        [[nodiscard]] bool switch_has_output(const Node &node) noexcept
        {
            return node.data() != nullptr && switch_runtime(const_cast<Node &>(node)).output != nullptr;
        }

        [[nodiscard]] bool switch_has_error_output(const Node &node) noexcept
        {
            return node.data() != nullptr && switch_runtime(const_cast<Node &>(node)).error_output != nullptr;
        }

        [[nodiscard]] bool switch_has_recordable_state(const Node & /*node*/) noexcept { return false; }

        [[nodiscard]] TSInputView switch_input_view(Node &node, engine_time_t evaluation_time)
        {
            if (!switch_has_input(node)) { return detail::invalid_input_view(evaluation_time); }
            return switch_runtime(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView switch_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!switch_has_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return switch_runtime(node).output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView switch_error_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!switch_has_error_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return switch_runtime(node).error_output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView switch_recordable_state_view(Node & /*node*/, engine_time_t evaluation_time)
        {
            return detail::invalid_output_view(evaluation_time);
        }

        void switch_node_start(Node & /*node*/, engine_time_t /*evaluation_time*/) {}

        void switch_node_stop(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = switch_runtime(node);
            stop_switch_child(node, runtime, evaluation_time);
        }

        void switch_node_eval(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = switch_runtime(node);
            TSInputView selector_input = resolve_parent_input_arg(node, "key", evaluation_time);
            if (std::getenv("HGRAPH_DEBUG_SWITCH") != nullptr) {
                std::fprintf(stderr,
                             "switch_eval eval=%lld selector_valid=%d selector_modified=%d child_started=%d active_branch=%zu\n",
                             static_cast<long long>(evaluation_time.time_since_epoch().count()),
                             selector_input.valid(),
                             selector_input.modified(),
                             runtime.child_instance.is_started(),
                             runtime.active_branch_index);
            }
            if (!selector_input.valid()) {
                if (!runtime.child_instance.is_started()) { return; }

                runtime.child_instance.evaluate(evaluation_time);
                if (node.has_output()) {
                    TSOutputView output = node.output_view(evaluation_time);
                    forward_switch_child_outputs(node, output, runtime, evaluation_time);
                }
                return;
            }

            const value::View selector = selector_input.value();
            const size_t branch_index = select_switch_branch(runtime, selector);
            const bool raw_selector_changed =
                input_changed(selector_input) && (runtime.reload_on_ticked || !runtime.active_key.has_value() ||
                                                  runtime.active_key->view() != selector);
            const bool branch_changed =
                runtime.active_branch_index == static_cast<size_t>(-1) || runtime.active_branch_index != branch_index ||
                raw_selector_changed || !runtime.child_instance.is_initialised();

            if (branch_changed) { activate_switch_branch(node, runtime, branch_index, selector, evaluation_time); }
            if (!runtime.child_instance.is_started()) { return; }

            if (node.has_output()) {
                if (!branch_changed && ensure_switch_child_bound(node, runtime, evaluation_time)) {
                    forward_switch_child_outputs(node, node.output_view(evaluation_time), runtime, MIN_DT);
                }
            } else {
                static_cast<void>(ensure_switch_child_bound(node, runtime, evaluation_time));
            }

            if (runtime.bound) {
                std::unordered_set<std::string> changed_args;
                for (const auto &spec : runtime.child_instance.boundary_plan().inputs) {
                    if (spec.child_node_index < 0 || spec.arg_name == "key") { continue; }

                    switch (spec.mode) {
                        case InputBindingMode::BIND_DIRECT:
                        case InputBindingMode::CLONE_REF_BINDING:
                        case InputBindingMode::DETACH_RESTORE_BLANK:
                            {
                                TSInputView parent_input = resolve_parent_input_arg(node, spec.arg_name, evaluation_time);
                                if (!spec.parent_input_path.empty()) { parent_input = navigate_input(parent_input, spec.parent_input_path); }
                                if (input_changed(parent_input)) { changed_args.emplace(spec.arg_name); }
                                break;
                            }
                        case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                        case InputBindingMode::BIND_KEY_VALUE:
                            break;
                    }
                }

                for (const std::string &arg_name : changed_args) {
                    BoundaryBindingRuntime::rebind(runtime.child_instance.boundary_plan(),
                                                  *runtime.child_instance.graph(),
                                                  node,
                                                  arg_name,
                                                  evaluation_time);
                }
            }

            runtime.child_instance.evaluate(evaluation_time);
            if (node.has_output()) {
                TSOutputView output = node.output_view(evaluation_time);
                forward_switch_child_outputs(node, output, runtime, evaluation_time);
                if (branch_changed && !output.modified()) { clear_output_value(output); }
            }
        }

        const NodeRuntimeOps k_switch_runtime_ops{
            &switch_node_start,
            &switch_node_stop,
            &switch_node_eval,
            &switch_has_input,
            &switch_has_output,
            &switch_has_error_output,
            &switch_has_recordable_state,
            &switch_input_view,
            &switch_output_view,
            &switch_error_output_view,
            &switch_recordable_state_view,
            &nested_runtime_label,
        };

        void destruct_switch_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<SwitchNodeRuntimeData>(node);

            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }

            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void validate_switch_contract(const NodeBuilder & /*builder*/) {}

        [[nodiscard]] size_t switch_builder_size(const NodeBuilder &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto &state = detail::node_builder_type_state<SwitchNodeBuilderState>(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, switch_runtime_storage_size(state.branches),
                                   switch_runtime_storage_alignment(state.branches), builders)
                .total_size;
        }

        [[nodiscard]] size_t switch_builder_alignment(const NodeBuilder &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto &state = detail::node_builder_type_state<SwitchNodeBuilderState>(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, switch_runtime_storage_size(state.branches),
                                   switch_runtime_storage_alignment(state.branches), builders)
                .alignment;
        }

        [[nodiscard]] Node *switch_construct_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto &state = detail::node_builder_type_state<SwitchNodeBuilderState>(builder);

            struct RuntimeLifecycle
            {
                const std::vector<SwitchBranchTemplate> *branches;
                bool reload_on_ticked;
                size_t child_graph_storage_size;
                size_t child_graph_storage_alignment;

                void destroy(void *runtime_data) const { std::destroy_at(static_cast<SwitchNodeRuntimeData *>(runtime_data)); }

                void initialise(const NodeBuilder & /*builder*/, void *runtime_data_ptr, Node * /*node*/, TSInput * /*input*/,
                                TSOutput * /*output*/, TSOutput * /*error_output*/, TSOutput * /*recordable_state*/) const
                {
                    auto &runtime = *static_cast<SwitchNodeRuntimeData *>(runtime_data_ptr);
                    runtime.branches = *branches;
                    runtime.reload_on_ticked = reload_on_ticked;
                    runtime.child_graph_storage_size = child_graph_storage_size;
                    runtime.child_graph_storage_alignment = child_graph_storage_alignment;
                }
            };

            const auto child_graph_storage = describe_switch_child_graph_storage(state.branches);

            return construct_node_chunk<SwitchNodeBuilderState>(
                builder, memory, node_index, inbound_edges, &k_switch_runtime_ops, nullptr, &destruct_switch_node,
                switch_runtime_storage_size(state.branches), switch_runtime_storage_alignment(state.branches),
                [](void *storage, const ResolvedNodeBuilders &, TSInput *input, TSOutput *output, TSOutput *error_output,
                   void * /*state_memory*/, TSOutput *recordable_state) -> void * {
                    return new (storage) SwitchNodeRuntimeData{input, output, error_output, recordable_state};
                },
                RuntimeLifecycle{
                    &state.branches,
                    state.reload_on_ticked,
                    child_graph_storage.size,
                    child_graph_storage.alignment,
                });
        }

        struct ReduceAggregateRef
        {
            enum class Kind : uint8_t
            {
                Empty,
                Zero,
                Leaf,
                Node,
            };

            Kind kind{Kind::Empty};
            size_t index{static_cast<size_t>(-1)};

            [[nodiscard]] static ReduceAggregateRef empty() noexcept { return {}; }
            [[nodiscard]] static ReduceAggregateRef zero() noexcept { return {Kind::Zero, 0}; }
            [[nodiscard]] static ReduceAggregateRef leaf(size_t index) noexcept { return {Kind::Leaf, index}; }
            [[nodiscard]] static ReduceAggregateRef node(size_t index) noexcept { return {Kind::Node, index}; }
            [[nodiscard]] bool active() const noexcept { return kind != Kind::Empty; }
            [[nodiscard]] bool operator==(const ReduceAggregateRef &) const noexcept = default;
        };

        struct ReduceSourceSlotRuntime
        {
            explicit ReduceSourceSlotRuntime(Value key_arg)
                : key(std::move(key_arg))
            {
            }

            Value  key;
            size_t dense_leaf{static_cast<size_t>(-1)};
            bool   live{false};
        };

        struct ReduceOpRuntime
        {
            ChildGraphInstance child_instance;
            std::optional<TSOutput> published_output;
            ReduceAggregateRef left_source{};
            ReduceAggregateRef right_source{};
            LinkedTSContext    left_bound_context{};
            LinkedTSContext    right_bound_context{};
            engine_time_t      next_scheduled{MAX_DT};
            bool               bound{false};
        };

        struct ReduceNodeRuntimeData
        {
            TSInput           *input{nullptr};
            TSOutput          *output{nullptr};
            TSOutput          *error_output{nullptr};
            TSOutput          *recordable_state{nullptr};
            const ChildGraphTemplate *child_template{nullptr};
            const TSOutputBuilder *output_builder{nullptr};
            int64_t            next_child_graph_id{1};
            size_t             live_leaf_count{0};
            size_t             leaf_capacity{1};
            std::vector<size_t> dense_to_source_slot{};
            bool               structure_changed{false};
            bool               stores_initialized{false};
        };

        using ReduceSourceSlotStore = detail::KeyedPayloadStore<ReduceSourceSlotRuntime>;
        using ReduceOpStore = detail::StablePayloadStore<ReduceOpRuntime>;

        [[nodiscard]] constexpr size_t reduce_source_slot_store_offset() noexcept
        {
            return align_up(sizeof(ReduceNodeRuntimeData), alignof(ReduceSourceSlotStore));
        }

        [[nodiscard]] constexpr size_t reduce_op_store_offset() noexcept
        {
            return align_up(reduce_source_slot_store_offset() + sizeof(ReduceSourceSlotStore), alignof(ReduceOpStore));
        }

        [[nodiscard]] constexpr size_t reduce_runtime_storage_size() noexcept
        {
            return reduce_op_store_offset() + sizeof(ReduceOpStore);
        }

        [[nodiscard]] constexpr size_t reduce_runtime_storage_alignment() noexcept
        {
            return std::max({alignof(ReduceNodeRuntimeData), alignof(ReduceSourceSlotStore), alignof(ReduceOpStore)});
        }

        [[nodiscard]] ReduceNodeRuntimeData &reduce_runtime(Node &node) noexcept
        {
            return detail::runtime_data<ReduceNodeRuntimeData>(node);
        }

        [[nodiscard]] ReduceSourceSlotStore &reduce_source_slots(ReduceNodeRuntimeData &runtime) noexcept
        {
            auto *storage = reinterpret_cast<std::byte *>(&runtime) + reduce_source_slot_store_offset();
            return *std::launder(reinterpret_cast<ReduceSourceSlotStore *>(storage));
        }

        [[nodiscard]] ReduceOpStore &reduce_op_store(ReduceNodeRuntimeData &runtime) noexcept
        {
            auto *storage = reinterpret_cast<std::byte *>(&runtime) + reduce_op_store_offset();
            return *std::launder(reinterpret_cast<ReduceOpStore *>(storage));
        }

        [[nodiscard]] int64_t next_reduce_child_graph_id(ReduceNodeRuntimeData &runtime) noexcept
        {
            return -runtime.next_child_graph_id++;
        }

        [[nodiscard]] size_t reduce_internal_node_count(const ReduceNodeRuntimeData &runtime) noexcept
        {
            return runtime.leaf_capacity > 0 ? runtime.leaf_capacity - 1 : 0;
        }

        [[nodiscard]] size_t reduce_total_tree_size(const ReduceNodeRuntimeData &runtime) noexcept
        {
            return reduce_internal_node_count(runtime) + runtime.leaf_capacity;
        }

        void stop_reduce_op(const ChildGraphTemplate &child_template,
                            ReduceOpRuntime          &op,
                            engine_time_t             evaluation_time,
                            bool                      clear_published_output = true,
                            bool                      release_child_bindings = true) noexcept
        {
            try {
                static_cast<void>(child_template);
                static_cast<void>(release_child_bindings);
            } catch (...) {}
            try {
                if (op.child_instance.is_started()) { op.child_instance.stop(evaluation_time); }
            } catch (...) {}
            try {
                if (op.published_output.has_value()) {
                    TSOutputView published_output = op.published_output->view(evaluation_time);
                    if (clear_published_output) {
                        clear_output_links_tree(published_output, evaluation_time != MIN_DT);
                    } else if (release_child_bindings) {
                        clear_output_links_tree(published_output, false);
                    }
                }
            } catch (...) {}
            op.bound = false;
            op.left_source = {};
            op.right_source = {};
            op.left_bound_context = {};
            op.right_bound_context = {};
            op.next_scheduled = MAX_DT;
        }

        void dispose_reduce_op(const ChildGraphTemplate &child_template, ReduceOpRuntime &op, engine_time_t evaluation_time) noexcept
        {
            stop_reduce_op(child_template, op, evaluation_time, true, false);
            try {
                if (op.child_instance.is_initialised()) { op.child_instance.dispose(evaluation_time); }
            } catch (...) {}
            op.child_instance = ChildGraphInstance{};
        }

        void recycle_reduce_op(const ChildGraphTemplate &child_template, ReduceOpRuntime &op, engine_time_t evaluation_time) noexcept
        {
            stop_reduce_op(child_template, op, evaluation_time, false, true);
            try {
                if (op.child_instance.is_initialised()) { op.child_instance.dispose(evaluation_time); }
            } catch (...) {}
            op.child_instance = ChildGraphInstance{};
        }

        [[nodiscard]] bool reduce_has_input(const Node &node) noexcept
        {
            return node.data() != nullptr && reduce_runtime(const_cast<Node &>(node)).input != nullptr;
        }

        [[nodiscard]] bool reduce_has_output(const Node &node) noexcept
        {
            return node.data() != nullptr && reduce_runtime(const_cast<Node &>(node)).output != nullptr;
        }

        [[nodiscard]] bool reduce_has_error_output(const Node &node) noexcept
        {
            return node.data() != nullptr && reduce_runtime(const_cast<Node &>(node)).error_output != nullptr;
        }

        [[nodiscard]] bool reduce_has_recordable_state(const Node & /*node*/) noexcept { return false; }

        [[nodiscard]] TSInputView reduce_input_view(Node &node, engine_time_t evaluation_time)
        {
            if (!reduce_has_input(node)) { return detail::invalid_input_view(evaluation_time); }
            return reduce_runtime(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView reduce_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!reduce_has_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return reduce_runtime(node).output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView reduce_error_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!reduce_has_error_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return reduce_runtime(node).error_output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView reduce_recordable_state_view(Node & /*node*/, engine_time_t evaluation_time)
        {
            return detail::invalid_output_view(evaluation_time);
        }

        [[nodiscard]] TSOutputView reduce_bound_source_output(Node &node, std::string_view arg_name, engine_time_t evaluation_time)
        {
            TSInputView input = resolve_parent_input_arg(node, arg_name, evaluation_time);
            TSOutputView output = bound_output_of(input);
            if (output.ts_schema() != nullptr) {
                if (output.ts_schema()->kind != TSKind::REF) { return output; }
                if (!output.valid()) { return detail::invalid_output_view(evaluation_time); }

                const TimeSeriesReference reference = TimeSeriesReference::make(output);
                return reference.is_peered() ? reference.target_view(evaluation_time)
                                             : detail::invalid_output_view(evaluation_time);
            }

            if (const TSMeta *schema = input.ts_schema(); schema != nullptr && schema->kind == TSKind::REF) {
                const TimeSeriesReference reference = TimeSeriesReference::make(input);
                if (reference.is_peered()) { return reference.target_view(evaluation_time); }
            }
            return detail::invalid_output_view(evaluation_time);
        }

        void reduce_remove_live_leaf(ReduceNodeRuntimeData &runtime,
                                     ReduceSourceSlotStore &source_slots,
                                     size_t                 source_slot) noexcept
        {
            ReduceSourceSlotRuntime *payload = source_slots.try_slot(source_slot);
            if (payload == nullptr || !payload->live || payload->dense_leaf == static_cast<size_t>(-1) ||
                runtime.live_leaf_count == 0) {
                return;
            }

            const size_t dense_leaf = payload->dense_leaf;
            const size_t last_leaf = runtime.live_leaf_count - 1;
            if (dense_leaf != last_leaf) {
                const size_t moved_source_slot = runtime.dense_to_source_slot[last_leaf];
                runtime.dense_to_source_slot[dense_leaf] = moved_source_slot;
                if (ReduceSourceSlotRuntime *moved = source_slots.try_slot(moved_source_slot); moved != nullptr) {
                    moved->dense_leaf = dense_leaf;
                }
            }
            runtime.dense_to_source_slot.pop_back();
            runtime.live_leaf_count = last_leaf;
            payload->dense_leaf = static_cast<size_t>(-1);
            payload->live = false;
            runtime.structure_changed = true;
        }

        [[nodiscard]] ReduceAggregateRef reduce_leaf_ref(const ReduceNodeRuntimeData &runtime, size_t dense_leaf) noexcept
        {
            return dense_leaf < runtime.live_leaf_count && dense_leaf < runtime.dense_to_source_slot.size()
                       ? ReduceAggregateRef::leaf(runtime.dense_to_source_slot[dense_leaf])
                       : ReduceAggregateRef::empty();
        }

        [[nodiscard]] ReduceAggregateRef reduce_tree_child_ref(const ReduceNodeRuntimeData      &runtime,
                                                               const std::vector<ReduceAggregateRef> &internal_refs,
                                                               size_t                           child_position) noexcept
        {
            const size_t internal_count = reduce_internal_node_count(runtime);
            if (child_position < internal_count) { return internal_refs[child_position]; }
            return reduce_leaf_ref(runtime, child_position - internal_count);
        }

        void ensure_reduce_leaf_capacity(ReduceNodeRuntimeData &runtime, size_t required_leaf_count)
        {
            if (required_leaf_count == 0) { return; }
            while (runtime.leaf_capacity < required_leaf_count) { runtime.leaf_capacity *= 2; }
        }

        [[nodiscard]] TSOutputView reduce_resolve_aggregate_output(Node                            &node,
                                                                   ReduceNodeRuntimeData           &runtime,
                                                                   const std::vector<ReduceAggregateRef> &internal_refs,
                                                                   const ReduceAggregateRef        &ref,
                                                                   engine_time_t                    evaluation_time);
        void publish_reduce_aggregate_output(const TSOutputView &target_output,
                                             const TSOutputView &source_output,
                                             engine_time_t       evaluation_time);

        void ensure_reduce_op_started(Node &node, ReduceNodeRuntimeData &runtime, ReduceOpRuntime &op, engine_time_t evaluation_time)
        {
            if (!op.child_instance.is_initialised()) {
                std::vector<int64_t> graph_id = node.node_id();
                graph_id.push_back(next_reduce_child_graph_id(runtime));
                op.child_instance.initialise(*runtime.child_template, node, std::move(graph_id), "reduce");
            }
            if (!op.child_instance.is_started()) { op.child_instance.start(evaluation_time); }
        }

        void bind_reduce_op_inputs(const ChildGraphTemplate &child_template,
                                   Graph                    &child,
                                   const TSOutputView       &lhs_output,
                                   const TSOutputView       &rhs_output,
                                   engine_time_t             evaluation_time)
        {
            for (const auto &spec : child_template.boundary_plan.inputs) {
                if (spec.child_node_index < 0) { continue; }
                auto &child_node = child.node_at(static_cast<size_t>(spec.child_node_index));
                TSInputView child_input = navigate_input(child_node.input_view(evaluation_time), spec.child_input_path);

                if (child_input.active()) { child_input.make_passive(); }
                if (child_input.linked_target() != nullptr && child_input.linked_target()->is_bound()) {
                    child_input.unbind_output();
                }

                TSOutputView source_output = spec.arg_name == "lhs" ? lhs_output : rhs_output;
                if (source_output.ts_schema() != nullptr && !spec.parent_input_path.empty()) {
                    source_output = navigate_output(source_output, spec.parent_input_path);
                }

                if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr) {
                    const TSViewContext source_resolved = source_output.context_ref().resolved();
                    std::fprintf(stderr,
                                 "bind_reduce_op_inputs arg=%.*s child_node=%lld source_schema_kind=%d source_state=%p resolved_state=%p owning_output=%p\n",
                                 static_cast<int>(spec.arg_name.size()),
                                 spec.arg_name.data(),
                                 static_cast<long long>(spec.child_node_index),
                                 source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                 static_cast<void *>(source_output.context_ref().ts_state),
                                 static_cast<void *>(source_resolved.ts_state),
                                 static_cast<void *>(source_output.owning_output()));
                }

                switch (spec.mode) {
                    case InputBindingMode::BIND_DIRECT:
                    case InputBindingMode::CLONE_REF_BINDING:
                        BoundaryBindingRuntime::bind_from_output(child_input, source_output, spec.mode, evaluation_time);
                        break;
                    case InputBindingMode::DETACH_RESTORE_BLANK:
                        BoundaryBindingRuntime::bind_from_output(child_input, source_output, spec.mode, evaluation_time);
                        break;
                    case InputBindingMode::BIND_MULTIPLEXED_ELEMENT:
                    case InputBindingMode::BIND_KEY_VALUE:
                        throw std::logic_error("reduce child input binding requires keyed handling before evaluation");
                }
                child_node.notify(evaluation_time);
            }
        }

        void propagate_reduce_op_input_updates(const ChildGraphTemplate &child_template,
                                               Graph                    &child,
                                               bool                      lhs_changed,
                                               bool                      rhs_changed,
                                               engine_time_t             evaluation_time)
        {
            for (const auto &spec : child_template.boundary_plan.inputs) {
                if (spec.child_node_index < 0) { continue; }

                const bool changed =
                    spec.arg_name == "lhs" ? lhs_changed : (spec.arg_name == "rhs" ? rhs_changed : false);
                if (!changed) { continue; }

                auto &child_node = child.node_at(static_cast<size_t>(spec.child_node_index));
                TSInputView child_input = navigate_input(child_node.input_view(evaluation_time), spec.child_input_path);
                if (BaseState *state = child_input.context_mutable().ts_state; state != nullptr) {
                    state->mark_modified(evaluation_time);
                }
                child_node.notify(evaluation_time);
            }
        }

        [[nodiscard]] TSOutputView reduce_resolve_op_output_source(Node                                  &node,
                                                                   ReduceNodeRuntimeData                 &runtime,
                                                                   const std::vector<ReduceAggregateRef> &internal_refs,
                                                                   ReduceOpRuntime                       &op,
                                                                   const OutputBindingSpec               &spec,
                                                                   engine_time_t                          evaluation_time)
        {
            switch (spec.mode) {
                case OutputBindingMode::ALIAS_CHILD_OUTPUT:
                    {
                        if (spec.child_node_index < 0 || op.child_instance.graph() == nullptr) {
                            return detail::invalid_output_view(evaluation_time);
                        }
                        auto &child_node = op.child_instance.graph()->node_at(static_cast<size_t>(spec.child_node_index));
                        return navigate_output(child_node.output_view(evaluation_time), spec.child_output_path);
                    }
                case OutputBindingMode::ALIAS_PARENT_INPUT:
                    {
                        const ReduceAggregateRef source_ref =
                            spec.parent_arg_name == "lhs" ? op.left_source : op.right_source;
                        TSOutputView source = reduce_resolve_aggregate_output(node, runtime, internal_refs, source_ref, evaluation_time);
                        return source.ts_schema() != nullptr ? navigate_output(source, spec.child_output_path)
                                                             : detail::invalid_output_view(evaluation_time);
                    }
                default:
                    return detail::invalid_output_view(evaluation_time);
            }
        }

        [[nodiscard]] TSOutputView ensure_reduce_op_published_output(ReduceNodeRuntimeData &runtime,
                                                                     ReduceOpRuntime       &op,
                                                                     engine_time_t          evaluation_time)
        {
            if (runtime.output_builder == nullptr) {
                throw std::logic_error("reduce published output requires an output builder");
            }
            if (!op.published_output.has_value()) {
                op.published_output.emplace();
                runtime.output_builder->construct_output(*op.published_output);
            }
            return op.published_output->view(evaluation_time);
        }

        void publish_reduce_op_output(Node                                  &node,
                                      ReduceNodeRuntimeData                 &runtime,
                                      const std::vector<ReduceAggregateRef> &internal_refs,
                                      ReduceOpRuntime                       &op,
                                      const TSOutputView                    &published_output,
                                      engine_time_t                          evaluation_time)
        {
            const auto &plan = runtime.child_template->boundary_plan;
            if (plan.outputs.empty()) {
                clear_output_link(published_output);
                if (evaluation_time != MIN_DT) { clear_output_value(published_output); }
                return;
            }

            for (const auto &spec : plan.outputs) {
                TSOutputView source_output =
                    reduce_resolve_op_output_source(node, runtime, internal_refs, op, spec, evaluation_time);
                TSOutputView target_output = navigate_output(published_output, spec.parent_output_path);
                if (std::getenv("HGRAPH_DEBUG_REDUCE_VALUES") != nullptr) {
                    nb::gil_scoped_acquire guard;
                    std::string source_repr = "<invalid>";
                    if (source_output.ts_schema() != nullptr && source_output.valid()) {
                        source_repr = nb::cast<std::string>(nb::str(source_output.to_python()));
                    }
                    std::fprintf(stderr,
                                 "reduce_publish_spec mode=%d parent_path_size=%zu source_schema=%d source_valid=%d value=%s\n",
                                 static_cast<int>(spec.mode),
                                 spec.parent_output_path.size(),
                                 source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                 source_output.valid(),
                                 source_repr.c_str());
                }
                publish_reduce_aggregate_output(target_output, source_output, evaluation_time);
            }
        }

        void publish_reduce_aggregate_output(const TSOutputView &target_output,
                                             const TSOutputView &source_output,
                                             engine_time_t       evaluation_time)
        {
            if (!source_output.ts_schema()) {
                if (evaluation_time != MIN_DT) { clear_output_value(target_output); }
                clear_output_link(target_output);
                return;
            }

            if (!source_output.valid()) {
                if (evaluation_time != MIN_DT) { clear_output_value(target_output); }
                clear_output_link(target_output);
                return;
            }

            if (const auto *output_schema = target_output.ts_schema();
                output_schema != nullptr && output_schema->kind == TSKind::TSB &&
                source_output.ts_schema() != nullptr && source_output.ts_schema()->kind == TSKind::TSB) {
                const bool target_was_valid = target_output.valid();
                if (!target_was_valid) { target_output.clear(); }
                bool root_changed = !target_was_valid || output_changed(source_output);
                for (size_t index = 0; index < output_schema->field_count(); ++index) {
                    TSOutputView source_child = source_output.as_bundle()[index];
                    TSOutputView target_child = target_output.as_bundle()[index];

                    if (!source_child.valid()) {
                        if (evaluation_time != MIN_DT) {
                            clear_output_value(target_child);
                            root_changed = true;
                        }
                        clear_output_link(target_child);
                        continue;
                    }

                    const bool rebound = bind_output_link(target_child, source_child);
                    if (evaluation_time != MIN_DT && (rebound || output_changed(source_child) || !target_child.valid())) {
                        mark_output_view_modified(target_child, evaluation_time);
                        root_changed = true;
                    }
                }
                if (evaluation_time != MIN_DT && root_changed) { mark_output_view_modified(target_output, evaluation_time); }
                return;
            }

            if (const auto *output_schema = target_output.ts_schema();
                output_schema != nullptr && output_schema->kind == TSKind::TSD &&
                source_output.ts_schema() != nullptr && source_output.ts_schema()->kind == TSKind::TSD) {
                const bool target_was_valid = target_output.valid();
                if (!target_was_valid) { target_output.clear(); }
                bool root_changed = !target_was_valid || output_changed(source_output);
                auto source_map = source_output.value().as_map();
                const bool source_sampled = sampled_this_tick(source_output.context_ref(), evaluation_time);

                std::vector<Value> target_keys_to_remove;
                constexpr size_t no_slot = static_cast<size_t>(-1);
                auto target_map = target_output.value().as_map();
                auto target_mutation = target_map.begin_mutation(evaluation_time);
                for (size_t slot = target_map.first_live_slot(); slot != no_slot; slot = target_map.next_live_slot(slot)) {
                    Value key = target_map.delta().key_at_slot(slot).clone();
                    const size_t source_slot = source_map.find_slot(key.view());
                    if (source_slot == no_slot || source_map.delta().slot_removed(source_slot)) {
                        target_keys_to_remove.push_back(std::move(key));
                    }
                }

                for (const Value &key : target_keys_to_remove) {
                    TSOutputView target_child = detail::ensure_dict_child_output_view(target_output, key.view());
                    if (evaluation_time != MIN_DT) { clear_output_value(target_child); }
                    clear_output_link(target_child);
                    if (is_live_dict_key(target_output, key.view())) {
                        target_output.as_dict().erase(key.view());
                        root_changed = true;
                    }
                }

                for (size_t slot = source_map.first_live_slot(); slot != no_slot; slot = source_map.next_live_slot(slot)) {
                    Value key = source_map.delta().key_at_slot(slot).clone();
                    TSOutputView source_child = detail::ensure_dict_child_output_view(source_output, key.view());
                    TSOutputView target_child = ensure_mapped_output_child(target_output, key.view(), evaluation_time);

                    if (const auto *target_child_schema = target_child.ts_schema();
                        target_child_schema != nullptr &&
                        !binding_compatible_ts_schema(source_child.ts_schema(), target_child_schema) &&
                        source_child.owning_output() != nullptr) {
                        source_child = source_child.owning_output()->bindable_view(source_child, target_child_schema);
                    }

                    const bool rebound = bind_output_link(target_child, source_child);
                    const bool source_slot_changed =
                        source_sampled || source_map.delta().slot_added(slot) || source_map.delta().slot_updated(slot);
                    if (evaluation_time != MIN_DT && (rebound || source_slot_changed || !target_child.valid())) {
                        mark_output_view_modified(target_child, evaluation_time);
                        root_changed = true;
                    }
                }
                if (evaluation_time != MIN_DT && root_changed) { mark_output_view_modified(target_output, evaluation_time); }
                return;
            }

            if (const auto *output_schema = target_output.ts_schema();
                output_schema != nullptr && output_schema->kind == TSKind::TSS &&
                source_output.ts_schema() != nullptr && source_output.ts_schema()->kind == TSKind::TSS) {
                const bool target_was_valid = target_output.valid();
                if (!target_was_valid) { target_output.clear(); }
                bool root_changed = !target_was_valid || output_changed(source_output);

                auto source_set = source_output.value().as_set();
                auto target_set = target_output.value().as_set();
                auto mutation = target_set.begin_mutation(evaluation_time);

                std::vector<Value> values_to_remove;
                for (const View &value : target_set.values()) {
                    if (!source_set.contains(value)) { values_to_remove.emplace_back(value.clone()); }
                }
                for (const Value &value : values_to_remove) {
                    if (mutation.remove(value.view())) { root_changed = true; }
                }

                for (const View &value : source_set.values()) {
                    if (!target_set.contains(value)) {
                        if (mutation.add(value)) { root_changed = true; }
                    }
                }

                if (evaluation_time != MIN_DT && root_changed) { mark_output_view_modified(target_output, evaluation_time); }
                return;
            }

            TSOutputView bindable_source = source_output;
            if (const auto *parent_schema = target_output.ts_schema();
                parent_schema != nullptr && !binding_compatible_ts_schema(source_output.ts_schema(), parent_schema) &&
                source_output.owning_output() != nullptr) {
                bindable_source = source_output.owning_output()->bindable_view(source_output, parent_schema);
            }

            const bool rebound = bind_output_link(target_output, bindable_source);
            if (evaluation_time != MIN_DT && (rebound || output_changed(bindable_source) || !target_output.valid())) {
                mark_output_view_modified(target_output, evaluation_time);
            }
        }

        [[nodiscard]] bool publish_reduce_reference_output(const TSOutputView       &target_output,
                                                           const TimeSeriesReference &source_ref,
                                                           engine_time_t             evaluation_time)
        {
            if (source_ref.is_empty()) {
                const bool changed = target_output.valid();
                clear_output_link(target_output);
                if (changed && evaluation_time != MIN_DT) {
                    clear_output_value(target_output);
                    mark_output_view_modified(target_output, evaluation_time);
                }
                return changed;
            }

            if (source_ref.is_peered()) {
                publish_reduce_aggregate_output(target_output, source_ref.target_view(evaluation_time), evaluation_time);
                return true;
            }

            const TSMeta *target_schema = target_output.ts_schema();
            if (target_schema == nullptr) { return false; }

            switch (target_schema->kind) {
                case TSKind::TSB:
                    {
                        const bool target_was_valid = target_output.valid();
                        if (!target_was_valid) { target_output.clear(); }
                        bool root_changed = !target_was_valid;
                        for (size_t index = 0; index < target_schema->field_count(); ++index) {
                            TSOutputView target_child = target_output.as_bundle()[index];
                            const TimeSeriesReference &item =
                                index < source_ref.items().size() ? source_ref[index] : TimeSeriesReference::empty();
                            root_changed = publish_reduce_reference_output(target_child, item, evaluation_time) || root_changed;
                        }
                        if (evaluation_time != MIN_DT && root_changed) { mark_output_view_modified(target_output, evaluation_time); }
                        return root_changed;
                    }

                case TSKind::TSL:
                    {
                        const bool target_was_valid = target_output.valid();
                        if (!target_was_valid) { target_output.clear(); }
                        bool root_changed = !target_was_valid;
                        const size_t count = target_schema->fixed_size();
                        for (size_t index = 0; index < count; ++index) {
                            TSOutputView target_child = target_output.as_list()[index];
                            const TimeSeriesReference &item =
                                index < source_ref.items().size() ? source_ref[index] : TimeSeriesReference::empty();
                            root_changed = publish_reduce_reference_output(target_child, item, evaluation_time) || root_changed;
                        }
                        if (evaluation_time != MIN_DT && root_changed) { mark_output_view_modified(target_output, evaluation_time); }
                        return root_changed;
                    }

                default:
                    throw std::logic_error("publish_reduce_reference_output only supports collection references");
            }
        }

        [[nodiscard]] TSOutputView reduce_resolve_leaf_output(Node                  &node,
                                                              ReduceNodeRuntimeData &runtime,
                                                              size_t                 source_slot,
                                                              engine_time_t          evaluation_time)
        {
            ReduceSourceSlotStore &source_slots = reduce_source_slots(runtime);
            const ReduceSourceSlotRuntime *payload = source_slots.try_slot(source_slot);
            if (payload == nullptr) { return detail::invalid_output_view(evaluation_time); }

            TSOutputView source_root = reduce_bound_source_output(node, "ts", evaluation_time);
            if (source_root.ts_schema() == nullptr) { return detail::invalid_output_view(evaluation_time); }

            TSOutputView source_child = detail::ensure_dict_child_output_view(source_root, payload->key.view());
            if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr) {
                const TSViewContext resolved = source_child.context_ref().resolved();
                std::fprintf(stderr,
                             "reduce_resolve_leaf_output slot=%zu key=%s child_schema_kind=%d child_state=%p resolved_state=%p owning_output=%p\n",
                             source_slot,
                             payload->key.view().to_string().c_str(),
                             source_child.ts_schema() != nullptr ? static_cast<int>(source_child.ts_schema()->kind) : -1,
                             static_cast<void *>(source_child.context_ref().ts_state),
                             static_cast<void *>(resolved.ts_state),
                             static_cast<void *>(source_child.owning_output()));
            }
            if (const TSMeta *child_schema = source_child.ts_schema(); child_schema != nullptr && child_schema->kind == TSKind::REF) {
                if (!source_child.valid()) { return detail::invalid_output_view(evaluation_time); }
                const auto *ref = source_child.value().as_atomic().template try_as<TimeSeriesReference>();
                if (ref != nullptr && ref->is_peered()) { return ref->target_view(evaluation_time); }
                return detail::invalid_output_view(evaluation_time);
            }
            return source_child;
        }

        [[nodiscard]] TSOutputView reduce_resolve_aggregate_output(Node                            &node,
                                                                   ReduceNodeRuntimeData           &runtime,
                                                                   const std::vector<ReduceAggregateRef> &internal_refs,
                                                                   const ReduceAggregateRef        &ref,
                                                                   engine_time_t                    evaluation_time)
        {
            if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr) {
                std::fprintf(stderr,
                             "reduce_resolve_aggregate_output kind=%d index=%zu\n",
                             static_cast<int>(ref.kind),
                             ref.index);
            }
            switch (ref.kind) {
                case ReduceAggregateRef::Kind::Empty:
                    return detail::invalid_output_view(evaluation_time);
                case ReduceAggregateRef::Kind::Zero:
                    return reduce_bound_source_output(node, "zero", evaluation_time);
                case ReduceAggregateRef::Kind::Leaf:
                    return reduce_resolve_leaf_output(node, runtime, ref.index, evaluation_time);
                case ReduceAggregateRef::Kind::Node:
                    {
                        ReduceOpStore &op_store = reduce_op_store(runtime);
                        ReduceOpRuntime *op = op_store.try_slot(ref.index);
                        if (op == nullptr) { return detail::invalid_output_view(evaluation_time); }
                        if (!op->published_output.has_value()) { return detail::invalid_output_view(evaluation_time); }
                        return op->published_output->view(evaluation_time);
                    }
            }
            return detail::invalid_output_view(evaluation_time);
        }

        [[nodiscard]] ReduceAggregateRef reduce_root_ref(const ReduceNodeRuntimeData           &runtime,
                                                         const std::vector<ReduceAggregateRef> &internal_refs) noexcept
        {
            if (runtime.live_leaf_count == 0) { return ReduceAggregateRef::zero(); }
            if (reduce_internal_node_count(runtime) == 0) {
                return !runtime.dense_to_source_slot.empty()
                           ? ReduceAggregateRef::leaf(runtime.dense_to_source_slot.front())
                           : ReduceAggregateRef::empty();
            }
            return internal_refs.front();
        }

        void reduce_refresh_source_slots(Node &node, ReduceNodeRuntimeData &runtime, engine_time_t evaluation_time)
        {
            runtime.structure_changed = false;
            ReduceSourceSlotStore &source_slots = reduce_source_slots(runtime);
            TSOutputView source_root = reduce_bound_source_output(node, "ts", evaluation_time);
            const TSMeta *schema = source_root.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSD || !source_root.valid()) {
                for (size_t slot = 0; slot < source_slots.constructed.size(); ++slot) {
                    if (!source_slots.has_slot(slot)) { continue; }
                    reduce_remove_live_leaf(runtime, source_slots, slot);
                    source_slots.destroy_at(slot);
                }
                runtime.dense_to_source_slot.clear();
                runtime.live_leaf_count = 0;
                runtime.structure_changed = true;
                return;
            }

            const auto delta = source_root.value().as_map().delta();
            source_slots.reserve_to(delta.slot_capacity());

            for (size_t slot = 0; slot < source_slots.constructed.size(); ++slot) {
                if (source_slots.has_slot(slot) && !delta.slot_occupied(slot)) {
                    reduce_remove_live_leaf(runtime, source_slots, slot);
                    source_slots.destroy_at(slot);
                }
            }

            for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
                if (!delta.slot_occupied(slot) || !delta.slot_removed(slot)) { continue; }
                reduce_remove_live_leaf(runtime, source_slots, slot);
            }

            for (size_t slot = 0; slot < delta.slot_capacity(); ++slot) {
                if (!delta.slot_occupied(slot) || delta.slot_removed(slot)) { continue; }

                bool slot_live = true;
                if (schema->element_ts() != nullptr && schema->element_ts()->kind == TSKind::REF) {
                    TSOutputView source_child = detail::ensure_dict_child_output_view(source_root, delta.key_at_slot(slot));
                    slot_live = source_child.valid();
                }

                if (!slot_live) {
                    reduce_remove_live_leaf(runtime, source_slots, slot);
                    continue;
                }

                if (!source_slots.has_slot(slot)) {
                    source_slots.emplace_at(slot, delta.key_at_slot(slot).clone());
                }

                ReduceSourceSlotRuntime *payload = source_slots.try_slot(slot);
                if (payload == nullptr) { continue; }
                if (!payload->live) {
                    payload->key = delta.key_at_slot(slot).clone();
                }
                if (payload->live) { continue; }

                payload->dense_leaf = runtime.live_leaf_count;
                payload->live = true;
                runtime.dense_to_source_slot.push_back(slot);
                ++runtime.live_leaf_count;
                runtime.structure_changed = true;
            }

            ensure_reduce_leaf_capacity(runtime, runtime.live_leaf_count);

            if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                std::fprintf(stderr, "reduce_refresh_source_slots live=%zu capacity=%zu dense=[",
                             runtime.live_leaf_count, runtime.leaf_capacity);
                for (size_t i = 0; i < runtime.dense_to_source_slot.size(); ++i) {
                    std::fprintf(stderr, "%s%zu", i == 0 ? "" : ",", runtime.dense_to_source_slot[i]);
                }
                std::fprintf(stderr, "]\n");
            }
        }

        void reduce_evaluate_internal_nodes(Node                            &node,
                                            ReduceNodeRuntimeData           &runtime,
                                            std::vector<ReduceAggregateRef> &internal_refs,
                                            engine_time_t                    evaluation_time)
        {
            const size_t internal_count = reduce_internal_node_count(runtime);
            ReduceOpStore &op_store = reduce_op_store(runtime);
            for (size_t slot = internal_count; slot < op_store.constructed.size(); ++slot) {
                if (ReduceOpRuntime *op = op_store.try_slot(slot); op != nullptr) {
                    recycle_reduce_op(*runtime.child_template, *op, evaluation_time);
                }
            }
            op_store.reserve_to(internal_count);

            for (size_t index = internal_count; index-- > 0;) {
                const ReduceAggregateRef left_ref = reduce_tree_child_ref(runtime, internal_refs, 2 * index + 1);
                const ReduceAggregateRef right_ref = reduce_tree_child_ref(runtime, internal_refs, 2 * index + 2);

                if (!left_ref.active() && !right_ref.active()) {
                    if (ReduceOpRuntime *op = op_store.try_slot(index); op != nullptr) {
                        recycle_reduce_op(*runtime.child_template, *op, evaluation_time);
                    }
                    internal_refs[index] = ReduceAggregateRef::empty();
                    continue;
                }

                if (!left_ref.active() || !right_ref.active()) {
                    if (ReduceOpRuntime *op = op_store.try_slot(index); op != nullptr) {
                        recycle_reduce_op(*runtime.child_template, *op, evaluation_time);
                    }
                    internal_refs[index] = left_ref.active() ? left_ref : right_ref;
                    continue;
                }

                ReduceOpRuntime *op = op_store.try_slot(index);
                TSOutputView left_output = reduce_resolve_aggregate_output(node, runtime, internal_refs, left_ref, evaluation_time);
                TSOutputView right_output = reduce_resolve_aggregate_output(node, runtime, internal_refs, right_ref, evaluation_time);
                const LinkedTSContext left_context = bound_output_context(left_output);
                const LinkedTSContext right_context = bound_output_context(right_output);
                const bool binding_changed =
                    op == nullptr || !op->bound || op->left_source != left_ref || op->right_source != right_ref ||
                    !detail::linked_context_equal(op->left_bound_context, left_context) ||
                    !detail::linked_context_equal(op->right_bound_context, right_context);
                const bool force_recompute = runtime.structure_changed;
                const bool effective_binding_changed = binding_changed || force_recompute;
                if (op == nullptr) { op = &op_store.emplace_at(index); }
                if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                    std::fprintf(stderr,
                                 "reduce_internal index=%zu left=(kind=%d,index=%zu) right=(kind=%d,index=%zu) binding_changed=%d started=%d bound=%d\n",
                                 index,
                                 static_cast<int>(left_ref.kind),
                                 left_ref.index,
                                 static_cast<int>(right_ref.kind),
                                 right_ref.index,
                                 binding_changed,
                                 op->child_instance.is_started(),
                                 op->bound);
                }
                if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr) {
                    std::fprintf(stderr,
                                 "reduce_evaluate_internal_nodes index=%zu binding_changed=%d left=(kind=%d,index=%zu,state=%p,resolved=%p) right=(kind=%d,index=%zu,state=%p,resolved=%p)\n",
                                 index,
                                 binding_changed,
                                 static_cast<int>(left_ref.kind),
                                 left_ref.index,
                                 static_cast<void *>(left_output.context_ref().ts_state),
                                 static_cast<void *>(left_output.context_ref().resolved().ts_state),
                                 static_cast<int>(right_ref.kind),
                                 right_ref.index,
                                 static_cast<void *>(right_output.context_ref().ts_state),
                                 static_cast<void *>(right_output.context_ref().resolved().ts_state));
                }
                const bool scheduled_now = op->next_scheduled != MAX_DT && op->next_scheduled <= evaluation_time;

                if (!op->child_instance.is_initialised()) {
                    std::vector<int64_t> graph_id = node.node_id();
                    graph_id.push_back(next_reduce_child_graph_id(runtime));
                    op->child_instance.initialise(*runtime.child_template, node, std::move(graph_id), "reduce");
                }
                if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                    std::fprintf(stderr,
                                 "reduce_internal_prepare index=%zu initialised=%d started=%d bound=%d\n",
                                 index,
                                 op->child_instance.is_initialised(),
                                 op->child_instance.is_started(),
                                 op->bound);
                }
                if (!op->child_instance.is_started()) { op->child_instance.start(evaluation_time); }
                if (effective_binding_changed) {
                    if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                        std::fprintf(stderr, "reduce_internal_bind index=%zu\n", index);
                    }
                    bind_reduce_op_inputs(*runtime.child_template, *op->child_instance.graph(), left_output, right_output, evaluation_time);
                    op->left_source = left_ref;
                    op->right_source = right_ref;
                    op->left_bound_context = left_context;
                    op->right_bound_context = right_context;
                    op->bound = true;
                }

                if (effective_binding_changed || output_changed(left_output) || output_changed(right_output) || scheduled_now) {
                    const bool lhs_changed = effective_binding_changed || output_changed(left_output);
                    const bool rhs_changed = effective_binding_changed || output_changed(right_output);
                    if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                        std::fprintf(stderr,
                                     "reduce_internal_eval index=%zu left_modified=%d right_modified=%d scheduled_now=%d force_recompute=%d\n",
                                     index,
                                     output_changed(left_output),
                                     output_changed(right_output),
                                     scheduled_now,
                                     force_recompute);
                    }
                    if (!effective_binding_changed) {
                        propagate_reduce_op_input_updates(*runtime.child_template,
                                                          *op->child_instance.graph(),
                                                          lhs_changed,
                                                          rhs_changed,
                                                          evaluation_time);
                    }
                    op->child_instance.evaluate(evaluation_time);
                    if (effective_binding_changed) {
                        op->child_instance.evaluate(evaluation_time);
                    }
                    TSOutputView published_output = ensure_reduce_op_published_output(runtime, *op, evaluation_time);
                    publish_reduce_op_output(node, runtime, internal_refs, *op, published_output, evaluation_time);
                    if (std::getenv("HGRAPH_DEBUG_REDUCE_VALUES") != nullptr) {
                        nb::gil_scoped_acquire guard;
                        TSOutputView op_output = published_output;
                        std::string output_repr = "<invalid>";
                        if (op_output.ts_schema() != nullptr && op_output.valid()) {
                            output_repr = nb::cast<std::string>(nb::str(op_output.to_python()));
                        }
                        std::fprintf(stderr,
                                     "reduce_internal_value index=%zu value=%s\n",
                                     index,
                                     output_repr.c_str());
                    }
                }
                op->next_scheduled = op->child_instance.next_scheduled_time();
                internal_refs[index] = ReduceAggregateRef::node(index);
            }
        }

        void reduce_publish_output(Node                            &node,
                                   ReduceNodeRuntimeData           &runtime,
                                   const std::vector<ReduceAggregateRef> &internal_refs,
                                    engine_time_t                    evaluation_time)
        {
            if (!node.has_output()) { return; }

            TSOutputView output = node.output_view(evaluation_time);
            const ReduceAggregateRef root_ref = reduce_root_ref(runtime, internal_refs);
            TSOutputView source_output = reduce_resolve_aggregate_output(node, runtime, internal_refs, root_ref, evaluation_time);
            const bool force_publish = runtime.structure_changed && evaluation_time != MIN_DT;
            if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                std::fprintf(stderr,
                             "reduce_publish_output root_kind=%d root_index=%zu source_schema=%d source_valid=%d source_modified=%d source_last_modified=%lld output_valid=%d output_modified=%d\n",
                             static_cast<int>(root_ref.kind),
                             root_ref.index,
                             source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                             source_output.valid(),
                             source_output.modified(),
                             source_output.last_modified_time() == MIN_DT
                                 ? -1LL
                                 : static_cast<long long>(source_output.last_modified_time().time_since_epoch().count()),
                             output.valid(),
                             output.modified());
            }

            if (source_output.ts_schema() != nullptr && source_output.ts_schema()->kind == TSKind::REF) {
                const TimeSeriesReference source_ref = TimeSeriesReference::make(source_output);
                static_cast<void>(publish_reduce_reference_output(output, source_ref, evaluation_time));
                if (force_publish && source_output.valid()) {
                    if (BaseState *output_state = output.context_ref().ts_state; output_state != nullptr) {
                        output_state->mark_modified(evaluation_time);
                    }
                }
                return;
            }

            if (!source_output.ts_schema()) {
                clear_output_link(output);
                if (evaluation_time != MIN_DT) { clear_output_value(output); }
                return;
            }

            publish_reduce_aggregate_output(output, source_output, evaluation_time);
            if (force_publish && source_output.valid()) {
                if (BaseState *output_state = output.context_ref().ts_state; output_state != nullptr) {
                    output_state->mark_modified(evaluation_time);
                }
            }
        }

        void reduce_schedule(Node &node, ReduceNodeRuntimeData &runtime)
        {
            ReduceOpStore &op_store = reduce_op_store(runtime);
            node.scheduler().un_schedule("reduce");
            engine_time_t next_schedule = MAX_DT;
            for (size_t slot = 0; slot < op_store.constructed.size(); ++slot) {
                const ReduceOpRuntime *op = op_store.try_slot(slot);
                if (op == nullptr) { continue; }
                if (op->next_scheduled != MAX_DT && op->next_scheduled < next_schedule) {
                    next_schedule = op->next_scheduled;
                }
            }
            if (next_schedule != MAX_DT) { node.scheduler().schedule(next_schedule, std::string{"reduce"}); }
        }

        void reduce_node_start(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = reduce_runtime(node);
            node.notify(evaluation_time);
            reduce_schedule(node, runtime);
        }

        void reduce_node_stop(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = reduce_runtime(node);
            ReduceOpStore &op_store = reduce_op_store(runtime);
            for (size_t slot = 0; slot < op_store.constructed.size(); ++slot) {
                if (ReduceOpRuntime *op = op_store.try_slot(slot); op != nullptr) {
                    stop_reduce_op(*runtime.child_template, *op, evaluation_time);
                }
            }
            if (node.has_output()) { clear_output_link(node.output_view(evaluation_time)); }
        }

        void destruct_reduce_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<ReduceNodeRuntimeData>(node);
            ReduceSourceSlotStore *source_slots = runtime_data.stores_initialized ? &reduce_source_slots(runtime_data) : nullptr;
            ReduceOpStore *op_store = runtime_data.stores_initialized ? &reduce_op_store(runtime_data) : nullptr;

            if (runtime_data.child_template != nullptr && op_store != nullptr) {
                for (size_t slot = 0; slot < op_store->constructed.size(); ++slot) {
                    if (ReduceOpRuntime *op = op_store->try_slot(slot); op != nullptr) {
                        dispose_reduce_op(*runtime_data.child_template, *op, MIN_DT);
                        op_store->destroy_at(slot);
                    }
                }
            }
            if (source_slots != nullptr) {
                for (size_t slot = 0; slot < source_slots->constructed.size(); ++slot) {
                    if (source_slots->has_slot(slot)) { source_slots->destroy_at(slot); }
                }
            }

            if (op_store != nullptr) { std::destroy_at(op_store); }
            if (source_slots != nullptr) { std::destroy_at(source_slots); }
            runtime_data.stores_initialized = false;
            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }
            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void reduce_node_eval(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = reduce_runtime(node);
            reduce_refresh_source_slots(node, runtime, evaluation_time);

            std::vector<ReduceAggregateRef> internal_refs(reduce_internal_node_count(runtime), ReduceAggregateRef::empty());
            reduce_evaluate_internal_nodes(node, runtime, internal_refs, evaluation_time);
            reduce_publish_output(node, runtime, internal_refs, evaluation_time);
            reduce_schedule(node, runtime);
        }

        const NodeRuntimeOps k_reduce_runtime_ops{
            &reduce_node_start,
            &reduce_node_stop,
            &reduce_node_eval,
            &reduce_has_input,
            &reduce_has_output,
            &reduce_has_error_output,
            &reduce_has_recordable_state,
            &reduce_input_view,
            &reduce_output_view,
            &reduce_error_output_view,
            &reduce_recordable_state_view,
            &nested_runtime_label,
        };

        void validate_reduce_contract(const NodeBuilder & /*builder*/) {}

        [[nodiscard]] size_t reduce_builder_size(const NodeBuilder &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, reduce_runtime_storage_size(), reduce_runtime_storage_alignment(), builders).total_size;
        }

        [[nodiscard]] size_t reduce_builder_alignment(const NodeBuilder &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, reduce_runtime_storage_size(), reduce_runtime_storage_alignment(), builders).alignment;
        }

        [[nodiscard]] Node *reduce_construct_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                                const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto &state = detail::node_builder_type_state<ReduceNodeBuilderState>(builder);

            struct RuntimeLifecycle
            {
                const ChildGraphTemplate *child_template;

                void destroy(void *runtime_data) const
                {
                    auto *runtime = static_cast<ReduceNodeRuntimeData *>(runtime_data);
                    if (runtime->stores_initialized) {
                        std::destroy_at(&reduce_op_store(*runtime));
                        std::destroy_at(&reduce_source_slots(*runtime));
                    }
                    std::destroy_at(runtime);
                }

                void initialise(const NodeBuilder & /*builder*/, void *runtime_data_ptr, Node * /*node*/, TSInput * /*input*/,
                                TSOutput * /*output*/, TSOutput * /*error_output*/, TSOutput * /*recordable_state*/) const
                {
                    auto &runtime = *static_cast<ReduceNodeRuntimeData *>(runtime_data_ptr);
                    if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr && runtime.output != nullptr) {
                        TSOutputView output_view = runtime.output->view(MIN_DT);
                        const auto &root_state = std::get<TSDState>(runtime.output->root_state_variant());
                        std::fprintf(stderr,
                                     "reduce_runtime.initialise before output_owner=%p output_state=%p variant_index=%zu root_state_addr=%p root_storage_kind=%d\n",
                                     static_cast<void *>(runtime.output),
                                     static_cast<void *>(output_view.context_ref().ts_state),
                                     runtime.output->root_state_variant().index(),
                                     static_cast<const void *>(&root_state),
                                     static_cast<int>(root_state.storage_kind));
                    }
                    new (&reduce_source_slots(runtime)) ReduceSourceSlotStore{};
                    new (&reduce_op_store(runtime)) ReduceOpStore{};
                    runtime.stores_initialized = true;
                    runtime.child_template = child_template;
                    if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr && runtime.output != nullptr) {
                        TSOutputView output_view = runtime.output->view(MIN_DT);
                        const auto &root_state = std::get<TSDState>(runtime.output->root_state_variant());
                        std::fprintf(stderr,
                                     "reduce_runtime.initialise after output_owner=%p output_state=%p variant_index=%zu root_state_addr=%p root_storage_kind=%d source_slots=%p op_store=%p\n",
                                     static_cast<void *>(runtime.output),
                                     static_cast<void *>(output_view.context_ref().ts_state),
                                     runtime.output->root_state_variant().index(),
                                     static_cast<const void *>(&root_state),
                                     static_cast<int>(root_state.storage_kind),
                                     static_cast<void *>(&reduce_source_slots(runtime)),
                                     static_cast<void *>(&reduce_op_store(runtime)));
                    }
                }
            };

            Node *node = construct_node_chunk<ReduceNodeBuilderState>(
                builder, memory, node_index, inbound_edges, &k_reduce_runtime_ops, nullptr, &destruct_reduce_node,
                reduce_runtime_storage_size(),
                reduce_runtime_storage_alignment(),
                [](void *storage, const ResolvedNodeBuilders &builders, TSInput *input, TSOutput *output, TSOutput *error_output,
                   void * /*state_memory*/, TSOutput *recordable_state) -> void * {
                    return new (storage) ReduceNodeRuntimeData{
                        input, output, error_output, recordable_state, nullptr, builders.output_builder};
                },
                RuntimeLifecycle{state.child_template});
            if (std::getenv("HGRAPH_DEBUG_LINKS") != nullptr && node != nullptr && node->has_output()) {
                auto &runtime = reduce_runtime(*node);
                TSOutputView output = node->output_view(MIN_DT);
                const auto &root_state = std::get<TSDState>(runtime.output->root_state_variant());
                std::fprintf(stderr,
                             "reduce_construct_at node=%lld output_owner=%p output_state=%p variant_index=%zu root_state_addr=%p root_storage_kind=%d source_slots=%p op_store=%p runtime=%p runtime_storage_size=%zu\n",
                             static_cast<long long>(node_index),
                             static_cast<void *>(runtime.output),
                             static_cast<void *>(output.context_ref().ts_state),
                             runtime.output->root_state_variant().index(),
                             static_cast<const void *>(&root_state),
                             static_cast<int>(root_state.storage_kind),
                             static_cast<void *>(&reduce_source_slots(runtime)),
                             static_cast<void *>(&reduce_op_store(runtime)),
                             static_cast<void *>(&runtime),
                             reduce_runtime_storage_size());
            }
            return node;
        }

        struct NonAssociativeReduceNodeRuntimeData
        {
            TSInput           *input{nullptr};
            TSOutput          *output{nullptr};
            TSOutput          *error_output{nullptr};
            TSOutput          *recordable_state{nullptr};
            const ChildGraphTemplate *child_template{nullptr};
            int64_t            next_child_graph_id{1};
            size_t             active_count{0};
            bool               store_initialized{false};
        };

        using NonAssociativeReduceOpStore = detail::StablePayloadStore<ReduceOpRuntime>;

        [[nodiscard]] constexpr size_t non_associative_reduce_op_store_offset() noexcept
        {
            return align_up(sizeof(NonAssociativeReduceNodeRuntimeData), alignof(NonAssociativeReduceOpStore));
        }

        [[nodiscard]] constexpr size_t non_associative_reduce_runtime_storage_size() noexcept
        {
            return non_associative_reduce_op_store_offset() + sizeof(NonAssociativeReduceOpStore);
        }

        [[nodiscard]] constexpr size_t non_associative_reduce_runtime_storage_alignment() noexcept
        {
            return std::max(alignof(NonAssociativeReduceNodeRuntimeData), alignof(NonAssociativeReduceOpStore));
        }

        [[nodiscard]] NonAssociativeReduceNodeRuntimeData &non_associative_reduce_runtime(Node &node) noexcept
        {
            return detail::runtime_data<NonAssociativeReduceNodeRuntimeData>(node);
        }

        [[nodiscard]] NonAssociativeReduceOpStore &non_associative_reduce_op_store(NonAssociativeReduceNodeRuntimeData &runtime) noexcept
        {
            auto *storage = reinterpret_cast<std::byte *>(&runtime) + non_associative_reduce_op_store_offset();
            return *std::launder(reinterpret_cast<NonAssociativeReduceOpStore *>(storage));
        }

        [[nodiscard]] int64_t next_non_associative_reduce_child_graph_id(NonAssociativeReduceNodeRuntimeData &runtime) noexcept
        {
            return -runtime.next_child_graph_id++;
        }

        [[nodiscard]] bool non_associative_reduce_has_input(const Node &node) noexcept
        {
            return node.data() != nullptr && non_associative_reduce_runtime(const_cast<Node &>(node)).input != nullptr;
        }

        [[nodiscard]] bool non_associative_reduce_has_output(const Node &node) noexcept
        {
            return node.data() != nullptr && non_associative_reduce_runtime(const_cast<Node &>(node)).output != nullptr;
        }

        [[nodiscard]] bool non_associative_reduce_has_error_output(const Node &node) noexcept
        {
            return node.data() != nullptr && non_associative_reduce_runtime(const_cast<Node &>(node)).error_output != nullptr;
        }

        [[nodiscard]] bool non_associative_reduce_has_recordable_state(const Node & /*node*/) noexcept { return false; }

        [[nodiscard]] TSInputView non_associative_reduce_input_view(Node &node, engine_time_t evaluation_time)
        {
            if (!non_associative_reduce_has_input(node)) { return detail::invalid_input_view(evaluation_time); }
            return non_associative_reduce_runtime(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView non_associative_reduce_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!non_associative_reduce_has_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return non_associative_reduce_runtime(node).output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView non_associative_reduce_error_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!non_associative_reduce_has_error_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return non_associative_reduce_runtime(node).error_output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView non_associative_reduce_recordable_state_view(Node & /*node*/, engine_time_t evaluation_time)
        {
            return detail::invalid_output_view(evaluation_time);
        }

        [[nodiscard]] TSOutputView non_associative_reduce_leaf_output(Node &node,
                                                                      size_t index,
                                                                      engine_time_t evaluation_time)
        {
            TSOutputView source_root = reduce_bound_source_output(node, "ts", evaluation_time);
            const TSMeta *schema = source_root.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSD || !source_root.value().has_value()) {
                return detail::invalid_output_view(evaluation_time);
            }

            Value key{*schema->key_type(), MutationTracking::Plain};
            key.reset();
            key.view().set_scalar(static_cast<int64_t>(index));

            TSOutputView source_child = detail::ensure_dict_child_output_view(source_root, key.view());
            if (const TSMeta *child_schema = source_child.ts_schema(); child_schema != nullptr && child_schema->kind == TSKind::REF) {
                if (!source_child.valid()) { return detail::invalid_output_view(evaluation_time); }
                if (const auto *ref = source_child.value().as_atomic().template try_as<TimeSeriesReference>();
                    ref != nullptr && ref->is_peered()) {
                    return ref->target_view(evaluation_time);
                }
                return detail::invalid_output_view(evaluation_time);
            }
            return source_child;
        }

        [[nodiscard]] TSOutputView non_associative_reduce_op_output_source(const ChildGraphTemplate &child_template,
                                                                           ReduceOpRuntime          &op,
                                                                           const TSOutputView       &lhs_output,
                                                                           const TSOutputView       &rhs_output,
                                                                           engine_time_t             evaluation_time)
        {
            const auto &plan = child_template.boundary_plan;
            if (plan.outputs.empty()) { return detail::invalid_output_view(evaluation_time); }

            const auto &spec = plan.outputs.front();
            switch (spec.mode) {
                case OutputBindingMode::ALIAS_CHILD_OUTPUT:
                    {
                        if (spec.child_node_index < 0 || op.child_instance.graph() == nullptr) {
                            return detail::invalid_output_view(evaluation_time);
                        }
                        auto &child_node = op.child_instance.graph()->node_at(static_cast<size_t>(spec.child_node_index));
                        return navigate_output(child_node.output_view(evaluation_time), spec.child_output_path);
                    }
                case OutputBindingMode::ALIAS_PARENT_INPUT:
                    {
                        const TSOutputView &source = spec.parent_arg_name == "lhs" ? lhs_output : rhs_output;
                        return source.ts_schema() != nullptr ? navigate_output(source, spec.child_output_path)
                                                             : detail::invalid_output_view(evaluation_time);
                    }
                default:
                    return detail::invalid_output_view(evaluation_time);
            }
        }

        [[nodiscard]] size_t non_associative_reduce_source_size(Node &node, engine_time_t evaluation_time) noexcept
        {
            TSOutputView source_root = reduce_bound_source_output(node, "ts", evaluation_time);
            const TSMeta *schema = source_root.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSD || !source_root.value().has_value()) { return 0; }
            return source_root.value().as_map().size();
        }

        void non_associative_reduce_resize_chain(Node &node,
                                                 NonAssociativeReduceNodeRuntimeData &runtime,
                                                 engine_time_t evaluation_time)
        {
            const size_t required_count = non_associative_reduce_source_size(node, evaluation_time);
            NonAssociativeReduceOpStore &op_store = non_associative_reduce_op_store(runtime);

            for (size_t index = runtime.active_count; index-- > required_count;) {
                if (ReduceOpRuntime *op = op_store.try_slot(index); op != nullptr) {
                    dispose_reduce_op(*runtime.child_template, *op, evaluation_time);
                    op_store.destroy_at(index);
                }
            }

            op_store.reserve_to(required_count);
            for (size_t index = runtime.active_count; index < required_count; ++index) {
                op_store.emplace_at(index);
            }

            runtime.active_count = required_count;
        }

        [[nodiscard]] TSOutputView non_associative_reduce_resolve_output(Node &node,
                                                                         NonAssociativeReduceNodeRuntimeData &runtime,
                                                                         size_t index,
                                                                         const TSOutputView &lhs_output,
                                                                         const TSOutputView &rhs_output,
                                                                         engine_time_t evaluation_time)
        {
            NonAssociativeReduceOpStore &op_store = non_associative_reduce_op_store(runtime);
            ReduceOpRuntime *op = op_store.try_slot(index);
            if (op == nullptr) { return detail::invalid_output_view(evaluation_time); }
            return non_associative_reduce_op_output_source(*runtime.child_template, *op, lhs_output, rhs_output, evaluation_time);
        }

        void non_associative_reduce_evaluate_chain(Node &node,
                                                   NonAssociativeReduceNodeRuntimeData &runtime,
                                                   engine_time_t evaluation_time)
        {
            TSOutputView lhs_output = reduce_bound_source_output(node, "zero", evaluation_time);
            NonAssociativeReduceOpStore &op_store = non_associative_reduce_op_store(runtime);

            for (size_t index = 0; index < runtime.active_count; ++index) {
                TSOutputView rhs_output = non_associative_reduce_leaf_output(node, index, evaluation_time);
                if (!rhs_output.ts_schema()) {
                    throw std::runtime_error("non-associative reduce requires contiguous integer keyed inputs");
                }

                ReduceOpRuntime *op = op_store.try_slot(index);
                if (op == nullptr) {
                    throw std::logic_error("non-associative reduce missing runtime op slot");
                }

                const bool scheduled_now = op->next_scheduled != MAX_DT && op->next_scheduled <= evaluation_time;
                const LinkedTSContext lhs_context = bound_output_context(lhs_output);
                const LinkedTSContext rhs_context = bound_output_context(rhs_output);
                if (!op->child_instance.is_initialised()) {
                    std::vector<int64_t> graph_id = node.node_id();
                    graph_id.push_back(next_non_associative_reduce_child_graph_id(runtime));
                    op->child_instance.initialise(*runtime.child_template, node, std::move(graph_id), "reduce");
                }
                const bool binding_changed =
                    !op->bound ||
                    !detail::linked_context_equal(op->left_bound_context, lhs_context) ||
                    !detail::linked_context_equal(op->right_bound_context, rhs_context);
                if (!op->child_instance.is_started()) { op->child_instance.start(evaluation_time); }
                if (binding_changed) {
                    bind_reduce_op_inputs(*runtime.child_template, *op->child_instance.graph(), lhs_output, rhs_output, evaluation_time);
                    op->left_bound_context = lhs_context;
                    op->right_bound_context = rhs_context;
                    op->bound = true;
                }

                if (!lhs_output.ts_schema()) {
                    throw std::runtime_error("non-associative reduce requires a bound zero input");
                }

                if (binding_changed || output_changed(lhs_output) || output_changed(rhs_output) || scheduled_now) {
                    const bool lhs_changed = binding_changed || output_changed(lhs_output);
                    const bool rhs_changed = binding_changed || output_changed(rhs_output);
                    if (!binding_changed) {
                        propagate_reduce_op_input_updates(*runtime.child_template,
                                                          *op->child_instance.graph(),
                                                          lhs_changed,
                                                          rhs_changed,
                                                          evaluation_time);
                    }
                    op->child_instance.evaluate(evaluation_time);
                }
                op->next_scheduled = op->child_instance.next_scheduled_time();
                lhs_output = non_associative_reduce_resolve_output(node, runtime, index, lhs_output, rhs_output, evaluation_time);
            }
        }

        void non_associative_reduce_publish_output(Node &node,
                                                   NonAssociativeReduceNodeRuntimeData &runtime,
                                                   engine_time_t evaluation_time)
        {
            if (!node.has_output()) { return; }

            TSOutputView output = node.output_view(evaluation_time);
            TSOutputView source_output = reduce_bound_source_output(node, "zero", evaluation_time);
            if (runtime.active_count > 0) {
                NonAssociativeReduceOpStore &op_store = non_associative_reduce_op_store(runtime);
                for (size_t index = 0; index < runtime.active_count; ++index) {
                    ReduceOpRuntime *op = op_store.try_slot(index);
                    if (op == nullptr) { break; }
                    TSOutputView rhs_output = non_associative_reduce_leaf_output(node, index, evaluation_time);
                    source_output = non_associative_reduce_op_output_source(
                        *runtime.child_template, *op, source_output, rhs_output, evaluation_time);
                }
            }

            if (!source_output.ts_schema()) {
                if (evaluation_time != MIN_DT) { clear_output_value(output); }
                clear_output_link(output);
                return;
            }

            if (source_output.ts_schema()->kind == TSKind::REF) {
                const TimeSeriesReference source_ref = TimeSeriesReference::make(source_output);
                static_cast<void>(publish_reduce_reference_output(output, source_ref, evaluation_time));
                return;
            }

            if (!source_output.valid()) {
                if (evaluation_time != MIN_DT && output_changed(source_output)) { clear_output_value(output); }
                clear_output_link(output);
                return;
            }

            if (const auto *parent_schema = output.ts_schema();
                parent_schema != nullptr && !binding_compatible_ts_schema(source_output.ts_schema(), parent_schema) &&
                source_output.owning_output() != nullptr) {
                if (std::getenv("HGRAPH_DEBUG_BINDABLE") != nullptr) {
                    std::fprintf(stderr,
                                 "bindable site=node_builder:2962 bound=%d valid=%d source_kind=%d target_kind=%d\n",
                                 source_output.context_ref().is_bound(),
                                 source_output.valid(),
                                 source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                 parent_schema != nullptr ? static_cast<int>(parent_schema->kind) : -1);
                }
                source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
            }

            const bool rebound = bind_output_link(output, source_output);
            output = node.output_view(evaluation_time);
            if (evaluation_time != MIN_DT && (rebound || output_changed(source_output) || !output.valid())) {
                mark_output_view_modified(output, evaluation_time);
            }
        }

        void non_associative_reduce_schedule(Node &node, NonAssociativeReduceNodeRuntimeData &runtime)
        {
            NonAssociativeReduceOpStore &op_store = non_associative_reduce_op_store(runtime);
            node.scheduler().un_schedule("reduce");
            engine_time_t next_schedule = MAX_DT;
            for (size_t slot = 0; slot < op_store.constructed.size(); ++slot) {
                const ReduceOpRuntime *op = op_store.try_slot(slot);
                if (op == nullptr) { continue; }
                if (op->next_scheduled != MAX_DT && op->next_scheduled < next_schedule) {
                    next_schedule = op->next_scheduled;
                }
            }
            if (next_schedule != MAX_DT) { node.scheduler().schedule(next_schedule, std::string{"reduce"}); }
        }

        void non_associative_reduce_node_start(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = non_associative_reduce_runtime(node);
            if (node.has_output()) {
                const TSMeta *schema = node.output_view(evaluation_time).ts_schema();
                const bool publish_on_start =
                    schema != nullptr && schema->kind != TSKind::TSB && schema->kind != TSKind::TSL &&
                    schema->kind != TSKind::TSD && schema->kind != TSKind::TSS;
                if (publish_on_start) { non_associative_reduce_publish_output(node, runtime, evaluation_time); }
            }
            non_associative_reduce_schedule(node, runtime);
        }

        void non_associative_reduce_node_stop(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = non_associative_reduce_runtime(node);
            NonAssociativeReduceOpStore &op_store = non_associative_reduce_op_store(runtime);
            for (size_t slot = 0; slot < op_store.constructed.size(); ++slot) {
                if (ReduceOpRuntime *op = op_store.try_slot(slot); op != nullptr) {
                    dispose_reduce_op(*runtime.child_template, *op, evaluation_time);
                    op_store.destroy_at(slot);
                }
            }
            runtime.active_count = 0;
            if (node.has_output()) { clear_output_link(node.output_view(evaluation_time)); }
        }

        void destruct_non_associative_reduce_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<NonAssociativeReduceNodeRuntimeData>(node);
            NonAssociativeReduceOpStore *op_store =
                runtime_data.store_initialized ? &non_associative_reduce_op_store(runtime_data) : nullptr;

            if (runtime_data.child_template != nullptr && op_store != nullptr) {
                for (size_t slot = 0; slot < op_store->constructed.size(); ++slot) {
                    if (ReduceOpRuntime *op = op_store->try_slot(slot); op != nullptr) {
                        dispose_reduce_op(*runtime_data.child_template, *op, MIN_DT);
                        op_store->destroy_at(slot);
                    }
                }
            }

            if (op_store != nullptr) { std::destroy_at(op_store); }
            runtime_data.store_initialized = false;
            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }
            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void non_associative_reduce_node_eval(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = non_associative_reduce_runtime(node);
            non_associative_reduce_resize_chain(node, runtime, evaluation_time);
            non_associative_reduce_evaluate_chain(node, runtime, evaluation_time);
            non_associative_reduce_publish_output(node, runtime, evaluation_time);
            non_associative_reduce_schedule(node, runtime);
        }

        const NodeRuntimeOps k_non_associative_reduce_runtime_ops{
            &non_associative_reduce_node_start,
            &non_associative_reduce_node_stop,
            &non_associative_reduce_node_eval,
            &non_associative_reduce_has_input,
            &non_associative_reduce_has_output,
            &non_associative_reduce_has_error_output,
            &non_associative_reduce_has_recordable_state,
            &non_associative_reduce_input_view,
            &non_associative_reduce_output_view,
            &non_associative_reduce_error_output_view,
            &non_associative_reduce_recordable_state_view,
            &nested_runtime_label,
        };

        void validate_non_associative_reduce_contract(const NodeBuilder & /*builder*/) {}

        [[nodiscard]] size_t non_associative_reduce_builder_size(const NodeBuilder &builder,
                                                                 const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(
                       builder,
                       non_associative_reduce_runtime_storage_size(),
                       non_associative_reduce_runtime_storage_alignment(),
                       builders)
                .total_size;
        }

        [[nodiscard]] size_t non_associative_reduce_builder_alignment(
            const NodeBuilder &builder,
            const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(
                       builder,
                       non_associative_reduce_runtime_storage_size(),
                       non_associative_reduce_runtime_storage_alignment(),
                       builders)
                .alignment;
        }

        [[nodiscard]] Node *non_associative_reduce_construct_at(const NodeBuilder &builder,
                                                                void *memory,
                                                                int64_t node_index,
                                                                const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto &state = detail::node_builder_type_state<NonAssociativeReduceNodeBuilderState>(builder);

            struct RuntimeLifecycle
            {
                const ChildGraphTemplate *child_template;

                void destroy(void *runtime_data) const
                {
                    auto *runtime = static_cast<NonAssociativeReduceNodeRuntimeData *>(runtime_data);
                    if (runtime->store_initialized) { std::destroy_at(&non_associative_reduce_op_store(*runtime)); }
                    std::destroy_at(runtime);
                }

                void initialise(const NodeBuilder & /*builder*/,
                                void *runtime_data_ptr,
                                Node * /*node*/,
                                TSInput * /*input*/,
                                TSOutput * /*output*/,
                                TSOutput * /*error_output*/,
                                TSOutput * /*recordable_state*/) const
                {
                    auto &runtime = *static_cast<NonAssociativeReduceNodeRuntimeData *>(runtime_data_ptr);
                    new (&non_associative_reduce_op_store(runtime)) NonAssociativeReduceOpStore{};
                    runtime.store_initialized = true;
                    runtime.child_template = child_template;
                }
            };

            return construct_node_chunk<NonAssociativeReduceNodeBuilderState>(
                builder,
                memory,
                node_index,
                inbound_edges,
                &k_non_associative_reduce_runtime_ops,
                nullptr,
                &destruct_non_associative_reduce_node,
                non_associative_reduce_runtime_storage_size(),
                non_associative_reduce_runtime_storage_alignment(),
                [](void *storage, const ResolvedNodeBuilders &, TSInput *input, TSOutput *output, TSOutput *error_output,
                   void * /*state_memory*/, TSOutput *recordable_state) -> void * {
                    return new (storage) NonAssociativeReduceNodeRuntimeData{
                        input, output, error_output, recordable_state};
                },
                RuntimeLifecycle{state.child_template});
        }

        struct MapSlotRuntime
        {
            explicit MapSlotRuntime(Value key_arg)
                : key(std::move(key_arg))
            {
            }

            Value                    key;
            ChildGraphInstance       child_instance;
            std::optional<TSOutput>  key_output;
            bool                     bound{false};
            engine_time_t            next_scheduled{MAX_DT};
        };

        using MapSlotStore = detail::KeyedPayloadStore<MapSlotRuntime>;

        [[nodiscard]] constexpr size_t map_slot_store_offset() noexcept
        {
            return align_up(sizeof(MapNodeRuntimeData), alignof(MapSlotStore));
        }

        [[nodiscard]] constexpr size_t map_runtime_storage_size() noexcept
        {
            return map_slot_store_offset() + sizeof(MapSlotStore);
        }

        [[nodiscard]] constexpr size_t map_runtime_storage_alignment() noexcept
        {
            return std::max(alignof(MapNodeRuntimeData), alignof(MapSlotStore));
        }

        [[nodiscard]] int64_t next_map_child_graph_id(MapNodeRuntimeData &runtime) noexcept
        {
            // Keyed child graphs append a negative monotonic instance id to the
            // parent graph path. The sign distinguishes keyed child-instance
            // path segments from ordinary non-negative node ids while still
            // satisfying the "unique integer id per key instance" contract.
            return -runtime.next_child_graph_id++;
        }

        [[nodiscard]] MapSlotStore &map_slot_store(MapNodeRuntimeData &runtime) noexcept
        {
            auto *storage = reinterpret_cast<std::byte *>(&runtime) + map_slot_store_offset();
            return *std::launder(reinterpret_cast<MapSlotStore *>(storage));
        }

        [[nodiscard]] MapNodeRuntimeData &map_runtime(Node &node) noexcept
        {
            return detail::runtime_data<MapNodeRuntimeData>(node);
        }

        [[nodiscard]] bool is_multiplexed_arg(const MapNodeRuntimeData &state, std::string_view arg_name)
        {
            return std::find(state.multiplexed_args.begin(), state.multiplexed_args.end(), arg_name) != state.multiplexed_args.end();
        }

        [[nodiscard]] TSOutputView map_target_output(const TSOutputView &parent_output,
                                                     const value::View  &key,
                                                     PathView            path,
                                                     engine_time_t       evaluation_time)
        {
            TSOutputView child_output = ensure_mapped_output_child(parent_output, key, evaluation_time);
            return navigate_output(child_output, path);
        }

        [[nodiscard]] TSOutputView ensure_key_output(MapSlotRuntime &slot, const TSMeta *ts_schema, engine_time_t evaluation_time);

        void stop_map_slot(const ChildGraphTemplate &child_template, MapSlotRuntime &slot, engine_time_t evaluation_time) noexcept
        {
            try {
                if (slot.bound && slot.child_instance.graph() != nullptr) {
                    BoundaryBindingRuntime::unbind(child_template.boundary_plan, *slot.child_instance.graph());
                    slot.bound = false;
                }
            } catch (...) {}
            try {
                if (slot.child_instance.is_started()) { slot.child_instance.stop(evaluation_time); }
            } catch (...) {}
            slot.next_scheduled = MAX_DT;
        }

        void dispose_map_slot(const ChildGraphTemplate &child_template, MapSlotRuntime &slot, engine_time_t evaluation_time) noexcept
        {
            stop_map_slot(child_template, slot, evaluation_time);
            try {
                slot.child_instance.dispose(evaluation_time);
            } catch (...) {}
        }

        void clear_map_output_links(const ChildGraphTemplate &child_template,
                                    const TSOutputView      &parent_output,
                                    const value::View       &key)
        {
            if (!is_live_dict_key(parent_output, key)) { return; }

            TSOutputView key_output = parent_output.as_dict().at(key);
            for (const auto &spec : child_template.boundary_plan.outputs) {
                clear_output_link(navigate_output(key_output, spec.parent_output_path));
            }
        }

        void ensure_map_slot_started(Node &node, MapNodeRuntimeData &runtime, MapSlotRuntime &slot, engine_time_t evaluation_time)
        {
            if (!slot.child_instance.is_initialised()) {
                std::vector<int64_t> graph_id = node.node_id();
                graph_id.push_back(next_map_child_graph_id(runtime));

                slot.child_instance.initialise(*runtime.child_template, node, std::move(graph_id), slot.key.view().to_string());
            }

            if (!slot.child_instance.is_started()) { slot.child_instance.start(evaluation_time); }

            if (!slot.bound && slot.child_instance.graph() != nullptr) {
                BoundaryBindingRuntime::bind(slot.child_instance.boundary_plan(), *slot.child_instance.graph(), node, evaluation_time);
                BoundaryBindingRuntime::bind_keyed(slot.child_instance.boundary_plan(),
                                                   *slot.child_instance.graph(),
                                                   node,
                                                   ensure_key_output(slot, TSTypeRegistry::instance().ts(slot.key.schema()),
                                                                     evaluation_time),
                                                   slot.key.view(),
                                                   evaluation_time);
                slot.bound = true;
            }
        }

        [[nodiscard]] TSOutputView ensure_key_output(MapSlotRuntime &slot, const TSMeta *ts_schema, engine_time_t evaluation_time);

        void clear_map_slot_target_output(const TSOutputView &parent_output,
                                          const value::View  &key,
                                          PathView            parent_output_path,
                                          engine_time_t       evaluation_time)
        {
            if (!is_live_dict_key(parent_output, key)) { return; }

            TSOutputView key_output = parent_output.as_dict().at(key);
            TSOutputView target_output = navigate_output(key_output, parent_output_path);
            clear_output_link(target_output);
            static_cast<void>(evaluation_time);
        }

        void rebind_map_slot_inputs(Node                                   &node,
                                    MapSlotRuntime                         &slot,
                                    const std::unordered_set<std::string>  &modified_direct_args,
                                    bool                                    rebind_keyed_inputs,
                                    engine_time_t                           evaluation_time)
        {
            if (!slot.bound || slot.child_instance.graph() == nullptr) { return; }

            for (const auto &arg_name : modified_direct_args) {
                BoundaryBindingRuntime::rebind(slot.child_instance.boundary_plan(), *slot.child_instance.graph(), node, arg_name,
                                               evaluation_time);
            }
            if (rebind_keyed_inputs) {
                BoundaryBindingRuntime::bind_keyed(slot.child_instance.boundary_plan(),
                                                   *slot.child_instance.graph(),
                                                   node,
                                                   ensure_key_output(slot, TSTypeRegistry::instance().ts(slot.key.schema()),
                                                                     evaluation_time),
                                                   slot.key.view(),
                                                   evaluation_time);
            }
        }

        [[nodiscard]] TSOutputView ensure_key_output(MapSlotRuntime &slot, const TSMeta *ts_schema, engine_time_t evaluation_time)
        {
            if (ts_schema == nullptr) { throw std::logic_error("map key output requires a target schema"); }

            if (!slot.key_output.has_value()) {
                const auto &builder = TSOutputBuilderFactory::checked_builder_for(ts_schema);
                slot.key_output.emplace();
                builder.construct_output(*slot.key_output);
                TSOutputView key_source = slot.key_output->view(evaluation_time);
                if (key_source.value().schema() != slot.key.view().schema()) {
                    throw std::logic_error(fmt::format("map key output schema mismatch: {} != {}",
                                                       schema_debug_label(key_source.value().schema()),
                                                       schema_debug_label(slot.key.view().schema())));
                }
                key_source.value().copy_from(slot.key.view());
                mark_output_view_modified(key_source, evaluation_time);
            }

            TSOutputView key_source = slot.key_output->view(evaluation_time);
            if (key_source.ts_schema() != ts_schema) {
                throw std::logic_error("map key output time-series schema mismatch");
            }
            if (key_source.value().schema() != slot.key.view().schema()) {
                throw std::logic_error(fmt::format("map key output schema mismatch: {} != {}",
                                                   schema_debug_label(key_source.value().schema()),
                                                   schema_debug_label(slot.key.view().schema())));
            }

            return key_source;
        }

        [[nodiscard]] bool forward_map_slot_outputs(Node &node,
                                                    const MapNodeRuntimeData &runtime,
                                                    const TSOutputView &parent_output,
                                                    MapSlotRuntime &slot,
                                                    engine_time_t evaluation_time)
        {
            const auto &plan = slot.child_instance.boundary_plan();
            bool        slot_value_changed = false;
            for (const auto &spec : plan.outputs) {
                TSOutputView source_output;
                switch (spec.mode) {
                    case OutputBindingMode::ALIAS_CHILD_OUTPUT:
                        {
                            if (spec.child_node_index < 0 || slot.child_instance.graph() == nullptr) { continue; }
                            auto &child_node = slot.child_instance.graph()->node_at(static_cast<size_t>(spec.child_node_index));
                            source_output = navigate_output(child_node.output_view(evaluation_time), spec.child_output_path);
                            break;
                        }
                    case OutputBindingMode::ALIAS_PARENT_INPUT:
                        {
                            TSInputView parent_input = resolve_parent_input_arg(node, spec.parent_arg_name, evaluation_time);
                            if (is_multiplexed_arg(runtime, spec.parent_arg_name)) {
                                parent_input = select_multiplexed_parent_input(parent_input, slot.key.view());
                            }
                            source_output = bound_output_of(parent_input);
                            if (source_output.ts_schema() == nullptr) {
                                const TSMeta *parent_schema = parent_input.ts_schema();
                                if (parent_schema != nullptr && parent_schema->kind == TSKind::REF) {
                                    const TimeSeriesReference reference = TimeSeriesReference::make(parent_input);
                                    if (reference.is_peered()) { source_output = reference.target_view(evaluation_time); }
                                }
                            }
                            if (source_output.ts_schema() != nullptr) {
                                source_output = navigate_output(source_output, spec.child_output_path);
                            }
                            break;
                        }
                    case OutputBindingMode::ALIAS_KEY_VALUE:
                        {
                            TSOutputView target_output =
                                map_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);
                            source_output = ensure_key_output(slot, target_output.ts_schema(), evaluation_time);
                            break;
                        }
                }

                if (source_output.ts_schema() == nullptr) {
                    if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                        std::fprintf(stderr,
                                     "forward_map_slot_outputs key=%s source_schema=null mode=%d arg=%.*s\n",
                                     slot.key.view().to_string().c_str(),
                                     static_cast<int>(spec.mode),
                                     static_cast<int>(spec.parent_arg_name.size()),
                                     spec.parent_arg_name.data());
                    }
                    clear_map_slot_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);
                    continue;
                }

                if (!source_output.valid()) {
                    if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                        std::fprintf(stderr,
                                     "forward_map_slot_outputs key=%s source_invalid schema_kind=%d source_modified=%d source_last_modified=%lld mode=%d arg=%.*s\n",
                                     slot.key.view().to_string().c_str(),
                                     source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                     source_output.modified(),
                                     source_output.last_modified_time() == MIN_DT
                                         ? -1LL
                                         : static_cast<long long>(source_output.last_modified_time().time_since_epoch().count()),
                                     static_cast<int>(spec.mode),
                                     static_cast<int>(spec.parent_arg_name.size()),
                                     spec.parent_arg_name.data());
                    }
                    clear_map_slot_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);
                    continue;
                }

                TSOutputView target_output =
                    map_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);

                if (const auto *parent_schema = target_output.ts_schema();
                    parent_schema != nullptr && !binding_compatible_ts_schema(source_output.ts_schema(), parent_schema) &&
                    source_output.owning_output() != nullptr) {
                    if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                        std::fprintf(stderr,
                                     "forward_map_slot_outputs adapt key=%s source_kind=%d target_kind=%d mode=%d arg=%.*s\n",
                                     slot.key.view().to_string().c_str(),
                                     source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                     parent_schema != nullptr ? static_cast<int>(parent_schema->kind) : -1,
                                     static_cast<int>(spec.mode),
                                     static_cast<int>(spec.parent_arg_name.size()),
                                     spec.parent_arg_name.data());
                    }
                    if (std::getenv("HGRAPH_DEBUG_BINDABLE") != nullptr) {
                        std::fprintf(stderr,
                                     "bindable site=node_builder:3427 bound=%d valid=%d source_kind=%d target_kind=%d\n",
                                     source_output.context_ref().is_bound(),
                                     source_output.valid(),
                                     source_output.ts_schema() != nullptr ? static_cast<int>(source_output.ts_schema()->kind) : -1,
                                     parent_schema != nullptr ? static_cast<int>(parent_schema->kind) : -1);
                    }
                    source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
                }

                const bool rebound = bind_output_link(target_output, source_output);
                if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                    std::fprintf(stderr,
                                 "forward_map_slot_outputs key=%s rebound=%d source_valid=%d source_modified=%d source_last_modified=%lld target_valid=%d\n",
                                 slot.key.view().to_string().c_str(),
                                 rebound,
                                 source_output.valid(),
                                 source_output.modified(),
                                 source_output.last_modified_time() == MIN_DT
                                     ? -1LL
                                     : static_cast<long long>(source_output.last_modified_time().time_since_epoch().count()),
                                 target_output.valid());
                }
                if (evaluation_time != MIN_DT && (rebound || source_output.modified())) { slot_value_changed = true; }
            }
            return slot_value_changed;
        }

        void publish_map_slot_output_updates(const MapNodeRuntimeData  &runtime,
                                             const TSOutputView       &parent_output,
                                             const MapSlotStore       &slot_store,
                                             const std::vector<size_t> &changed_slots,
                                             engine_time_t             evaluation_time)
        {
            if (evaluation_time == MIN_DT || changed_slots.empty()) { return; }

            bool parent_changed = false;
            for (size_t slot_index : changed_slots) {
                const MapSlotRuntime *slot = slot_store.try_slot(slot_index);
                if (slot == nullptr) { continue; }

                for (const auto &spec : runtime.child_template->boundary_plan.outputs) {
                    TSOutputView target_output =
                        map_target_output(parent_output, slot->key.view(), spec.parent_output_path, evaluation_time);
                    mark_output_view_modified(target_output, evaluation_time);
                    parent_changed = true;
                }
            }
            if (parent_changed) { mark_output_view_modified(parent_output, evaluation_time); }
        }

        [[nodiscard]] bool has_modified_multiplexed_input(Node &node, const MapSlotRuntime &slot, engine_time_t evaluation_time)
        {
            if (!node.has_input()) { return false; }

            const auto &plan = slot.child_instance.boundary_plan();
            for (const auto &spec : plan.inputs) {
                if (spec.mode != InputBindingMode::BIND_MULTIPLEXED_ELEMENT) { continue; }

                TSInputView parent_field = resolve_parent_input_arg(node, spec.arg_name, evaluation_time);
                TSInputView parent_item = select_multiplexed_parent_input(parent_field, slot.key.view());
                if (!spec.parent_input_path.empty()) { parent_item = navigate_input(parent_item, spec.parent_input_path); }
                TSOutputView parent_output = bound_output_of(parent_item);
                if (parent_output.ts_schema() != nullptr) {
                    if (output_changed(parent_output) || input_changed(parent_item)) { return true; }
                } else if (input_changed(parent_item)) {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] bool map_has_input(const Node &node) noexcept
        {
            return node.data() != nullptr && map_runtime(const_cast<Node &>(node)).input != nullptr;
        }

        [[nodiscard]] bool map_has_output(const Node &node) noexcept
        {
            return node.data() != nullptr && map_runtime(const_cast<Node &>(node)).output != nullptr;
        }

        [[nodiscard]] bool map_has_error_output(const Node &node) noexcept
        {
            return node.data() != nullptr && map_runtime(const_cast<Node &>(node)).error_output != nullptr;
        }

        [[nodiscard]] bool map_has_recordable_state(const Node & /*node*/) noexcept { return false; }

        [[nodiscard]] TSInputView map_input_view(Node &node, engine_time_t evaluation_time)
        {
            if (!map_has_input(node)) { return detail::invalid_input_view(evaluation_time); }
            return map_runtime(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView map_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!map_has_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return map_runtime(node).output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView map_error_output_view(Node &node, engine_time_t evaluation_time)
        {
            if (!map_has_error_output(node)) { return detail::invalid_output_view(evaluation_time); }
            return map_runtime(node).error_output->view(evaluation_time);
        }

        [[nodiscard]] TSOutputView map_recordable_state_view(Node & /*node*/, engine_time_t evaluation_time)
        {
            return detail::invalid_output_view(evaluation_time);
        }

        void map_node_start(Node & /*node*/, engine_time_t /*evaluation_time*/) {}

        void map_node_stop(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = map_runtime(node);
            auto &slot_store = map_slot_store(runtime);
            TSOutputView parent_output = node.has_output() ? node.output_view(evaluation_time) : detail::invalid_output_view(evaluation_time);

            for (size_t slot = 0; slot < slot_store.constructed.size(); ++slot) {
                MapSlotRuntime *payload = slot_store.try_slot(slot);
                if (payload == nullptr) { continue; }
                if (node.has_output()) { clear_map_output_links(*runtime.child_template, parent_output, payload->key.view()); }
                stop_map_slot(*runtime.child_template, *payload, evaluation_time);
            }
        }

        void destruct_map_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<MapNodeRuntimeData>(node);
            MapSlotStore *slot_store = runtime_data.slot_store_initialized ? &map_slot_store(runtime_data) : nullptr;

            if (runtime_data.child_template != nullptr && slot_store != nullptr) {
                for (size_t slot = 0; slot < slot_store->constructed.size(); ++slot) {
                    if (MapSlotRuntime *payload = slot_store->try_slot(slot); payload != nullptr) {
                        dispose_map_slot(*runtime_data.child_template, *payload, MIN_DT);
                        slot_store->destroy_at(slot);
                    }
                }
            }

            if (slot_store != nullptr) { std::destroy_at(slot_store); }
            runtime_data.slot_store_initialized = false;
            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.error_output != nullptr) { runtime_data.error_output->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }
            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void map_node_eval(Node &node, engine_time_t evaluation_time)
        {
            auto &runtime = map_runtime(node);
            auto &slot_store = map_slot_store(runtime);
            TSInputView keys_input = resolve_parent_input_arg(node, runtime.keys_arg, evaluation_time);
            if (std::getenv("HGRAPH_DEBUG_KEY_SET") != nullptr) {
                const auto &graph_id = node.owning_graph_id();
                std::fprintf(stderr,
                             "map_node_eval graph_tail=%lld node=%lld label=%.*s keys_arg=%s schema_kind=%d valid=%d modified=%d linked_target=%p\n",
                             static_cast<long long>(graph_id.empty() ? -1 : graph_id.back()),
                             static_cast<long long>(node.public_node_index()),
                             static_cast<int>(node.label().size()),
                             node.label().data(),
                             runtime.keys_arg.c_str(),
                             keys_input.ts_schema() != nullptr ? static_cast<int>(keys_input.ts_schema()->kind) : -1,
                             keys_input.valid(),
                             keys_input.modified(),
                             static_cast<const void *>(keys_input.linked_target()));
                TSInputView node_input = node.input_view(evaluation_time);
                if (node_input.ts_schema() != nullptr && node_input.ts_schema()->kind == TSKind::TSB) {
                    auto input_bundle = node_input.as_bundle();
                    for (size_t field_index = 0; field_index < node.input_schema()->field_count(); ++field_index) {
                        TSInputView field = input_bundle[field_index];
                        std::fprintf(stderr,
                                     "  field[%zu] name=%s schema_kind=%d valid=%d modified=%d bound=%d active=%d\n",
                                     field_index,
                                     node.input_schema()->fields()[field_index].name,
                                     field.ts_schema() != nullptr ? static_cast<int>(field.ts_schema()->kind) : -1,
                                     field.valid(),
                                     field.modified(),
                                     field.linked_target() != nullptr && field.linked_target()->is_bound(),
                                     field.active());
                        if (field.ts_schema() != nullptr && field.ts_schema()->kind == TSKind::TSD && field.valid()) {
                            auto key_set = field.as_dict().key_set();
                            std::vector<std::string> field_keys;
                            if (key_set.view().value().has_value()) {
                                for (const View &key : key_set.values()) {
                                    field_keys.emplace_back(key.to_string());
                                }
                            }
                            std::fprintf(stderr, "    field_keys=[%s]\n", fmt::format("{}", fmt::join(field_keys, ",")).c_str());
                        }
                    }
                }
                std::fflush(stderr);
            }
            TSOutputView keys_output = live_bound_output_of(keys_input);
            const View keys_value = keys_output.ts_schema() != nullptr ? keys_output.value() : keys_input.value();
            const auto keys_delta = keys_value.as_set().delta();
            const bool keys_modified = input_changed(keys_input);
            if (std::getenv("HGRAPH_DEBUG_KEY_SET") != nullptr && keys_value.has_value()) {
                std::vector<std::string> live_keys;
                for (const View &key : keys_value.as_set().values()) {
                    live_keys.emplace_back(key.to_string());
                }
                std::fprintf(stderr,
                             "map_node_eval keys_live=[%s]\n",
                             fmt::format("{}", fmt::join(live_keys, ",")).c_str());
                std::fflush(stderr);
            }
            if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                size_t occupied_slots = 0;
                size_t live_slots = 0;
                size_t added_slots_count = 0;
                size_t removed_slots_count = 0;
                for (size_t slot = 0; slot < keys_delta.slot_capacity(); ++slot) {
                    if (!keys_delta.slot_occupied(slot)) { continue; }
                    ++occupied_slots;
                    if (!keys_delta.slot_removed(slot)) { ++live_slots; }
                    if (keys_delta.slot_added(slot)) { ++added_slots_count; }
                    if (keys_delta.slot_removed(slot)) { ++removed_slots_count; }
                }
                std::fprintf(stderr,
                             "map_node_eval keys slot_capacity=%zu occupied=%zu live=%zu added=%zu removed=%zu keys_output_valid=%d keys_output_modified=%d keys_input_valid=%d keys_input_modified=%d\n",
                             keys_delta.slot_capacity(),
                             occupied_slots,
                             live_slots,
                             added_slots_count,
                             removed_slots_count,
                             keys_output.valid(),
                             keys_output.ts_schema() != nullptr ? keys_output.modified() : 0,
                             keys_input.valid(),
                             keys_input.modified());
            }

            slot_store.reserve_to(keys_delta.slot_capacity());

            TSOutputView parent_output = node.has_output() ? node.output_view(evaluation_time) : detail::invalid_output_view(evaluation_time);
            std::optional<MapMutationView> parent_output_mutation;
            if (parent_output.ts_schema() != nullptr && parent_output.ts_schema()->kind == TSKind::TSD && parent_output.valid()) {
                parent_output_mutation.emplace(parent_output.value().as_map().begin_mutation(evaluation_time));
            }

            if (keys_modified) {
                for (size_t slot = 0; slot < keys_delta.slot_capacity(); ++slot) {
                    if (slot_store.has_slot(slot) && !keys_delta.slot_occupied(slot)) {
                        MapSlotRuntime &payload = *slot_store.try_slot(slot);
                        if (node.has_output()) {
                            clear_map_output_links(*runtime.child_template, parent_output, payload.key.view());
                        }
                        dispose_map_slot(*runtime.child_template, payload, evaluation_time);
                        slot_store.destroy_at(slot);
                    }
                }
            }

            sul::dynamic_bitset<> added_slots(keys_delta.slot_capacity());
            if (keys_modified) {
                for (size_t slot = 0; slot < keys_delta.slot_capacity(); ++slot) {
                    if (!keys_delta.slot_occupied(slot) || !keys_delta.slot_added(slot) || keys_delta.slot_removed(slot)) { continue; }
                    added_slots.set(slot);
                    if (slot_store.has_slot(slot)) {
                        MapSlotRuntime &payload = *slot_store.try_slot(slot);
                        if (node.has_output()) {
                            clear_map_output_links(*runtime.child_template, parent_output, payload.key.view());
                        }
                        dispose_map_slot(*runtime.child_template, payload, evaluation_time);
                        slot_store.destroy_at(slot);
                    }
                    slot_store.emplace_at(slot, keys_delta.at_slot(slot).clone());
                    MapSlotRuntime &payload = *slot_store.try_slot(slot);
                    ensure_map_slot_started(node, runtime, payload, evaluation_time);
                }

                for (size_t slot = 0; slot < keys_delta.slot_capacity(); ++slot) {
                    if (!keys_delta.slot_occupied(slot) || !keys_delta.slot_removed(slot)) { continue; }
                    MapSlotRuntime *payload = slot_store.try_slot(slot);
                    if (payload == nullptr) { continue; }
                    if (node.has_output()) {
                        clear_map_output_links(*runtime.child_template, parent_output, payload->key.view());
                        if (is_live_dict_key(parent_output, payload->key.view())) {
                            parent_output.as_dict().erase(payload->key.view());
                        }
                    }
                    stop_map_slot(*runtime.child_template, *payload, evaluation_time);
                }

                // Sync stale slots: when the input was rebound (keys_modified but
                // the underlying delta only has incremental changes), add live keys
                // from the map that aren't yet in the slot_store.
                if (keys_value.has_value() && keys_value.schema() != nullptr &&
                    keys_value.schema()->kind == value::TypeKind::Map) {
                    const auto map = keys_value.as_map();
                    for (const View &key : keys_value.as_set().values()) {
                        const size_t slot = map.find_slot(key);
                        if (slot == static_cast<size_t>(-1)) { continue; }
                        if (slot_store.has_slot(slot)) { continue; }
                        if (added_slots.size() > slot && added_slots.test(slot)) { continue; }
                        if (added_slots.size() <= slot) { added_slots.resize(slot + 1); }
                        added_slots.set(slot);
                        slot_store.reserve_to(slot + 1);
                        slot_store.emplace_at(slot, key.clone());
                        MapSlotRuntime &payload = *slot_store.try_slot(slot);
                        ensure_map_slot_started(node, runtime, payload, evaluation_time);
                    }
                }
            }

            std::unordered_set<std::string> modified_direct_args;
            std::unordered_set<std::string> modified_keyed_args;
            for (const auto &spec : runtime.child_template->boundary_plan.inputs) {
                const bool arg_modified = input_changed(resolve_parent_input_arg(node, spec.arg_name, evaluation_time));
                if (!arg_modified) { continue; }

                if (spec.mode == InputBindingMode::BIND_DIRECT || spec.mode == InputBindingMode::CLONE_REF_BINDING) {
                    modified_direct_args.insert(spec.arg_name);
                } else if (spec.mode == InputBindingMode::BIND_MULTIPLEXED_ELEMENT) {
                    modified_keyed_args.insert(spec.arg_name);
                }
            }

            std::vector<size_t> changed_output_slots;
            for (size_t slot = 0; slot < keys_delta.slot_capacity(); ++slot) {
                if (!keys_delta.slot_occupied(slot) || keys_delta.slot_removed(slot)) { continue; }
	                MapSlotRuntime *payload = slot_store.try_slot(slot);
	                if (payload == nullptr) { continue; }

	                const bool added = added_slots.test(slot);
	                const bool scheduled_now =
	                    payload->next_scheduled != MAX_DT && payload->next_scheduled <= evaluation_time;
	                const bool multiplexed_modified = has_modified_multiplexed_input(node, *payload, evaluation_time);
                    const bool keyed_rebind_required = !modified_keyed_args.empty();
	                const bool should_eval =
	                    added || scheduled_now || !modified_direct_args.empty() || keyed_rebind_required || multiplexed_modified;

                if (!should_eval) { continue; }

	                ensure_map_slot_started(node, runtime, *payload, evaluation_time);
	                rebind_map_slot_inputs(node,
                                           *payload,
                                           modified_direct_args,
                                           added || keyed_rebind_required || multiplexed_modified,
                                           evaluation_time);
	                payload->child_instance.evaluate(evaluation_time);
                if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr && payload->child_instance.graph() != nullptr) {
                    Graph &child_graph = *payload->child_instance.graph();
                    for (size_t child_index = 0; child_index < child_graph.entries().size(); ++child_index) {
                        Node &child_node = child_graph.node_at(child_index);
                        if (child_node.has_input() && child_node.input_schema() != nullptr &&
                            child_node.input_schema()->kind == TSKind::TSB) {
                            TSInputView child_input = child_node.input_view(evaluation_time);
                            auto input_bundle = child_input.as_bundle();
                            for (size_t input_slot = 0; input_slot < child_node.input_schema()->field_count(); ++input_slot) {
                                TSInputView field = input_bundle[input_slot];
                                std::fprintf(stderr,
                                             "map_slot_child_input key=%s node=%zu slot=%zu label=%.*s schema_kind=%d valid=%d modified=%d last_modified=%lld active=%d bound=%d\n",
                                             payload->key.view().to_string().c_str(),
                                             child_index,
                                             input_slot,
                                             static_cast<int>(child_node.label().size()),
                                             child_node.label().data(),
                                             field.ts_schema() != nullptr ? static_cast<int>(field.ts_schema()->kind) : -1,
                                             field.valid(),
                                             field.modified(),
                                             field.last_modified_time() == MIN_DT
                                                 ? -1LL
                                                 : static_cast<long long>(field.last_modified_time().time_since_epoch().count()),
                                             field.active(),
                                             field.linked_target() != nullptr && field.linked_target()->is_bound());
                            }
                        }
                        if (!child_node.has_output()) { continue; }
                        TSOutputView child_output = child_node.output_view(evaluation_time);
                        std::fprintf(stderr,
                                     "map_slot_child_output key=%s node=%zu label=%.*s schema_kind=%d valid=%d modified=%d last_modified=%lld\n",
                                     payload->key.view().to_string().c_str(),
                                     child_index,
                                     static_cast<int>(child_node.label().size()),
                                     child_node.label().data(),
                                     child_output.ts_schema() != nullptr ? static_cast<int>(child_output.ts_schema()->kind) : -1,
                                     child_output.valid(),
                                     child_output.modified(),
                                     child_output.last_modified_time() == MIN_DT
                                         ? -1LL
                                         : static_cast<long long>(child_output.last_modified_time().time_since_epoch().count()));
                    }
                }
                payload->next_scheduled = payload->child_instance.next_scheduled_time();
                if (node.has_output()) {
                    if (forward_map_slot_outputs(node, runtime, parent_output, *payload, evaluation_time)) {
                        changed_output_slots.push_back(slot);
                    }
                }
            }

            if (node.has_output()) {
                publish_map_slot_output_updates(runtime, parent_output, slot_store, changed_output_slots, evaluation_time);
                if (std::getenv("HGRAPH_DEBUG_REDUCE_MAP") != nullptr) {
                    std::fprintf(stderr,
                                 "map_node_eval output_specs=%zu slot_capacity=%zu changed_output_slots=%zu keys_modified=%d modified_direct=%zu modified_keyed=%zu parent_valid=%d parent_modified=%d\n",
                                 runtime.child_template->boundary_plan.outputs.size(),
                                 keys_delta.slot_capacity(),
                                 changed_output_slots.size(),
                                 keys_modified,
                                 modified_direct_args.size(),
                                 modified_keyed_args.size(),
                                 parent_output.valid(),
                                 parent_output.modified());
                }
                if (keys_modified && !parent_output.modified()) {
                    if (!parent_output.valid()) {
                        parent_output.clear();
                    } else {
                        mark_output_view_modified(parent_output, evaluation_time);
                    }
                }
            }

            node.scheduler().un_schedule("map");
            engine_time_t next_schedule = MAX_DT;
            for (size_t slot = 0; slot < slot_store.constructed.size(); ++slot) {
                const MapSlotRuntime *payload = slot_store.try_slot(slot);
                if (payload == nullptr) { continue; }
                if (payload->next_scheduled != MAX_DT && payload->next_scheduled < next_schedule) {
                    next_schedule = payload->next_scheduled;
                }
            }
            if (next_schedule != MAX_DT) { node.scheduler().schedule(next_schedule, std::string{"map"}); }
        }

        const NodeRuntimeOps k_map_runtime_ops{
            &map_node_start,
            &map_node_stop,
            &map_node_eval,
            &map_has_input,
            &map_has_output,
            &map_has_error_output,
            &map_has_recordable_state,
            &map_input_view,
            &map_output_view,
            &map_error_output_view,
            &map_recordable_state_view,
            &nested_runtime_label,
        };

        void validate_map_contract(const NodeBuilder & /*builder*/) {}

        [[nodiscard]] size_t map_builder_size(const NodeBuilder &builder,
                                              const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, map_runtime_storage_size(), map_runtime_storage_alignment(), builders).total_size;
        }

        [[nodiscard]] size_t map_builder_alignment(const NodeBuilder &builder,
                                                   const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, map_runtime_storage_size(), map_runtime_storage_alignment(), builders).alignment;
        }

        [[nodiscard]] Node *map_construct_at(const NodeBuilder &builder, void *memory, int64_t node_index,
                                             const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            const auto &state = detail::node_builder_type_state<MapNodeBuilderState>(builder);

            struct RuntimeLifecycle
            {
                const ChildGraphTemplate *child_template;
                std::string key_arg;
                std::string keys_arg;
                std::vector<std::string> multiplexed_args;

                void destroy(void *runtime_data) const
                {
                    auto *runtime = static_cast<MapNodeRuntimeData *>(runtime_data);
                    if (runtime->slot_store_initialized) { std::destroy_at(&map_slot_store(*runtime)); }
                    std::destroy_at(runtime);
                }

                void initialise(const NodeBuilder & /*builder*/, void *runtime_data_ptr, Node * /*node*/, TSInput * /*input*/,
                                TSOutput * /*output*/, TSOutput * /*error_output*/, TSOutput * /*recordable_state*/) const
                {
                    auto &runtime = *static_cast<MapNodeRuntimeData *>(runtime_data_ptr);
                    new (&map_slot_store(runtime)) MapSlotStore{};
                    runtime.slot_store_initialized = true;
                    runtime.child_template = child_template;
                    runtime.key_arg = key_arg;
                    runtime.keys_arg = keys_arg;
                    runtime.multiplexed_args = multiplexed_args;
                }
            };

            return construct_node_chunk<MapNodeBuilderState>(
                builder, memory, node_index, inbound_edges, &k_map_runtime_ops, nullptr, &destruct_map_node,
                map_runtime_storage_size(),
                map_runtime_storage_alignment(),
                [](void *storage, const ResolvedNodeBuilders &, TSInput *input, TSOutput *output, TSOutput *error_output,
                   void * /*state_memory*/, TSOutput *recordable_state) -> void * {
                    return new (storage) MapNodeRuntimeData{input, output, error_output, recordable_state, nullptr, {}, {}, {}, 1};
                },
                RuntimeLifecycle{state.child_template, state.key_arg, state.keys_arg, state.multiplexed_args});
        }

    }  // namespace

    namespace detail
    {
        template <typename TState> const TState &node_builder_type_state(const NodeBuilder &builder) {
            return builder.type_state<TState>();
        }
    }  // namespace detail

    NodeBuilder::NodeBuilder(const NodeBuilder &other)
        : m_label(other.m_label), m_node_type(other.m_node_type), m_has_explicit_node_type(other.m_has_explicit_node_type),
          m_input_schema(other.m_input_schema), m_output_schema(other.m_output_schema),
          m_error_output_schema(other.m_error_output_schema), m_has_state(other.m_has_state), m_state_schema(other.m_state_schema),
          m_has_recordable_state(other.m_has_recordable_state), m_recordable_state_schema(other.m_recordable_state_schema),
          m_uses_scheduler(other.m_uses_scheduler), m_has_explicit_scheduler(other.m_has_explicit_scheduler),
          m_active_inputs(other.m_active_inputs), m_valid_inputs(other.m_valid_inputs),
          m_all_valid_inputs(other.m_all_valid_inputs), m_has_explicit_active_inputs(other.m_has_explicit_active_inputs),
          m_has_explicit_valid_inputs(other.m_has_explicit_valid_inputs),
          m_has_explicit_all_valid_inputs(other.m_has_explicit_all_valid_inputs), m_python_signature(other.m_python_signature),
          m_python_scalars(other.m_python_scalars), m_python_input_builder(other.m_python_input_builder),
          m_python_output_builder(other.m_python_output_builder), m_python_error_builder(other.m_python_error_builder),
          m_python_recordable_state_builder(other.m_python_recordable_state_builder),
          m_implementation_name(other.m_implementation_name), m_public_node_index(other.m_public_node_index),
          m_requires_resolved_schemas(other.m_requires_resolved_schemas),
          m_type_ops(other.m_type_ops), m_type_state(other.m_type_state != nullptr && other.m_type_ops != nullptr
                                                         ? other.m_type_ops->clone_state(other.m_type_state)
                                                         : nullptr) {}

    NodeBuilder::NodeBuilder(NodeBuilder &&other) noexcept
        : m_label(std::move(other.m_label)), m_node_type(other.m_node_type),
          m_has_explicit_node_type(other.m_has_explicit_node_type), m_input_schema(other.m_input_schema),
          m_output_schema(other.m_output_schema), m_error_output_schema(other.m_error_output_schema),
          m_has_state(other.m_has_state), m_state_schema(other.m_state_schema),
          m_has_recordable_state(other.m_has_recordable_state), m_recordable_state_schema(other.m_recordable_state_schema),
          m_uses_scheduler(other.m_uses_scheduler), m_has_explicit_scheduler(other.m_has_explicit_scheduler),
          m_active_inputs(std::move(other.m_active_inputs)),
          m_valid_inputs(std::move(other.m_valid_inputs)), m_all_valid_inputs(std::move(other.m_all_valid_inputs)),
          m_has_explicit_active_inputs(other.m_has_explicit_active_inputs),
          m_has_explicit_valid_inputs(other.m_has_explicit_valid_inputs),
          m_has_explicit_all_valid_inputs(other.m_has_explicit_all_valid_inputs),
          m_python_signature(std::move(other.m_python_signature)), m_python_scalars(std::move(other.m_python_scalars)),
          m_python_input_builder(std::move(other.m_python_input_builder)),
          m_python_output_builder(std::move(other.m_python_output_builder)),
          m_python_error_builder(std::move(other.m_python_error_builder)),
          m_python_recordable_state_builder(std::move(other.m_python_recordable_state_builder)),
          m_implementation_name(std::move(other.m_implementation_name)), m_public_node_index(other.m_public_node_index),
          m_requires_resolved_schemas(other.m_requires_resolved_schemas), m_type_ops(other.m_type_ops),
          m_type_state(other.m_type_state) {
        other.m_type_ops   = nullptr;
        other.m_type_state = nullptr;
    }

    NodeBuilder &NodeBuilder::operator=(const NodeBuilder &other) {
        if (this == &other) { return *this; }

        reset_type_state();

        m_label                           = other.m_label;
        m_node_type                       = other.m_node_type;
        m_has_explicit_node_type          = other.m_has_explicit_node_type;
        m_input_schema                    = other.m_input_schema;
        m_output_schema                   = other.m_output_schema;
        m_error_output_schema             = other.m_error_output_schema;
        m_has_state                       = other.m_has_state;
        m_state_schema                    = other.m_state_schema;
        m_has_recordable_state            = other.m_has_recordable_state;
        m_recordable_state_schema         = other.m_recordable_state_schema;
        m_uses_scheduler                  = other.m_uses_scheduler;
        m_has_explicit_scheduler          = other.m_has_explicit_scheduler;
        m_active_inputs                   = other.m_active_inputs;
        m_valid_inputs                    = other.m_valid_inputs;
        m_all_valid_inputs                = other.m_all_valid_inputs;
        m_has_explicit_active_inputs      = other.m_has_explicit_active_inputs;
        m_has_explicit_valid_inputs       = other.m_has_explicit_valid_inputs;
        m_has_explicit_all_valid_inputs   = other.m_has_explicit_all_valid_inputs;
        m_python_signature                = other.m_python_signature;
        m_python_scalars                  = other.m_python_scalars;
        m_python_input_builder            = other.m_python_input_builder;
        m_python_output_builder           = other.m_python_output_builder;
        m_python_error_builder            = other.m_python_error_builder;
        m_python_recordable_state_builder = other.m_python_recordable_state_builder;
        m_implementation_name             = other.m_implementation_name;
        m_public_node_index               = other.m_public_node_index;
        m_requires_resolved_schemas       = other.m_requires_resolved_schemas;
        m_type_ops                        = other.m_type_ops;
        m_type_state                      = other.m_type_state != nullptr && other.m_type_ops != nullptr
                                                ? other.m_type_ops->clone_state(other.m_type_state)
                                                : nullptr;
        return *this;
    }

    NodeBuilder &NodeBuilder::operator=(NodeBuilder &&other) noexcept {
        if (this == &other) { return *this; }

        reset_type_state();

        m_label                           = std::move(other.m_label);
        m_node_type                       = other.m_node_type;
        m_has_explicit_node_type          = other.m_has_explicit_node_type;
        m_input_schema                    = other.m_input_schema;
        m_output_schema                   = other.m_output_schema;
        m_error_output_schema             = other.m_error_output_schema;
        m_has_state                       = other.m_has_state;
        m_state_schema                    = other.m_state_schema;
        m_has_recordable_state            = other.m_has_recordable_state;
        m_recordable_state_schema         = other.m_recordable_state_schema;
        m_uses_scheduler                  = other.m_uses_scheduler;
        m_has_explicit_scheduler          = other.m_has_explicit_scheduler;
        m_active_inputs                   = std::move(other.m_active_inputs);
        m_valid_inputs                    = std::move(other.m_valid_inputs);
        m_all_valid_inputs                = std::move(other.m_all_valid_inputs);
        m_has_explicit_active_inputs      = other.m_has_explicit_active_inputs;
        m_has_explicit_valid_inputs       = other.m_has_explicit_valid_inputs;
        m_has_explicit_all_valid_inputs   = other.m_has_explicit_all_valid_inputs;
        m_python_signature                = std::move(other.m_python_signature);
        m_python_scalars                  = std::move(other.m_python_scalars);
        m_python_input_builder            = std::move(other.m_python_input_builder);
        m_python_output_builder           = std::move(other.m_python_output_builder);
        m_python_error_builder            = std::move(other.m_python_error_builder);
        m_python_recordable_state_builder = std::move(other.m_python_recordable_state_builder);
        m_implementation_name             = std::move(other.m_implementation_name);
        m_public_node_index               = other.m_public_node_index;
        m_requires_resolved_schemas       = other.m_requires_resolved_schemas;
        m_type_ops                        = other.m_type_ops;
        m_type_state                      = other.m_type_state;

        other.m_type_ops   = nullptr;
        other.m_type_state = nullptr;
        return *this;
    }

    NodeBuilder::~NodeBuilder() { reset_type_state(); }

    void NodeBuilder::set_type_state(const NodeRuntimeOps &runtime_ops, const PushSourceNodeRuntimeOps *push_source_runtime_ops,
                                     bool has_push_message_hook) {
        reset_type_state();
        m_type_ops   = &static_type_ops();
        m_type_state = make_builder_state(StaticNodeBuilderState{&runtime_ops, push_source_runtime_ops, has_push_message_hook});
        m_type_ops->validate_contract(*this);
    }

    void NodeBuilder::set_python_type_state(nb::object eval_fn, nb::object start_fn, nb::object stop_fn) {
        reset_type_state();
        m_type_ops   = &python_type_ops();
        m_type_state = make_builder_state(PythonNodeBuilderState{std::move(eval_fn), std::move(start_fn), std::move(stop_fn)});
        m_type_ops->validate_contract(*this);
    }

    NodeBuilder &nested_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template) {
        if (child_template == nullptr) {
            throw std::invalid_argument("nested_graph_implementation requires a non-null child template");
        }
        static const NodeBuilder::TypeOps nested_ops{
            &validate_nested_contract,
            &nested_builder_size,
            &nested_builder_alignment,
            &nested_construct_at,
            &destruct_builder_node,
            &clone_builder_state<NestedNodeBuilderState>,
            &destroy_builder_state<NestedNodeBuilderState>,
        };
        builder.reset_type_state();
        builder.m_type_ops       = &nested_ops;
        builder.m_type_state     = make_builder_state(NestedNodeBuilderState{child_template});
        builder.m_uses_scheduler = true;
        builder.m_has_explicit_scheduler = true;
        return builder;
    }

    NodeBuilder &try_except_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template) {
        if (child_template == nullptr) {
            throw std::invalid_argument("try_except_graph_implementation requires a non-null child template");
        }
        static const NodeBuilder::TypeOps try_except_ops{
            &validate_nested_contract,
            &nested_builder_size,
            &nested_builder_alignment,
            &try_except_construct_at,
            &destruct_builder_node,
            &clone_builder_state<NestedNodeBuilderState>,
            &destroy_builder_state<NestedNodeBuilderState>,
        };
        builder.reset_type_state();
        builder.m_type_ops       = &try_except_ops;
        builder.m_type_state     = make_builder_state(NestedNodeBuilderState{child_template});
        builder.m_uses_scheduler = true;
        builder.m_has_explicit_scheduler = true;
        return builder;
    }

    NodeBuilder &map_graph_implementation(NodeBuilder &builder,
                                          const ChildGraphTemplate *child_template,
                                          std::string key_arg,
                                          std::string keys_arg,
                                          std::vector<std::string> multiplexed_args) {
        if (child_template == nullptr) {
            throw std::invalid_argument("map_graph_implementation requires a non-null child template");
        }
        static const NodeBuilder::TypeOps map_ops{
            &validate_map_contract,
            &map_builder_size,
            &map_builder_alignment,
            &map_construct_at,
            &destruct_builder_node,
            &clone_builder_state<MapNodeBuilderState>,
            &destroy_builder_state<MapNodeBuilderState>,
        };
        builder.reset_type_state();
        builder.m_type_ops = &map_ops;
        builder.m_type_state =
            make_builder_state(MapNodeBuilderState{child_template, std::move(key_arg), std::move(keys_arg), std::move(multiplexed_args)});
        builder.m_uses_scheduler = true;
        builder.m_has_explicit_scheduler = true;
        return builder;
    }

    NodeBuilder &reduce_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template)
    {
        if (child_template == nullptr) {
            throw std::invalid_argument("reduce_graph_implementation requires a non-null child template");
        }
        static const NodeBuilder::TypeOps reduce_ops{
            &validate_reduce_contract,
            &reduce_builder_size,
            &reduce_builder_alignment,
            &reduce_construct_at,
            &destruct_builder_node,
            &clone_builder_state<ReduceNodeBuilderState>,
            &destroy_builder_state<ReduceNodeBuilderState>,
        };
        builder.reset_type_state();
        builder.m_type_ops = &reduce_ops;
        builder.m_type_state = make_builder_state(ReduceNodeBuilderState{child_template});
        builder.m_uses_scheduler = true;
        builder.m_has_explicit_scheduler = true;
        return builder;
    }

    NodeBuilder &non_associative_reduce_graph_implementation(NodeBuilder &builder, const ChildGraphTemplate *child_template)
    {
        if (child_template == nullptr) {
            throw std::invalid_argument("non_associative_reduce_graph_implementation requires a non-null child template");
        }
        static const NodeBuilder::TypeOps non_associative_reduce_ops{
            &validate_non_associative_reduce_contract,
            &non_associative_reduce_builder_size,
            &non_associative_reduce_builder_alignment,
            &non_associative_reduce_construct_at,
            &destruct_builder_node,
            &clone_builder_state<NonAssociativeReduceNodeBuilderState>,
            &destroy_builder_state<NonAssociativeReduceNodeBuilderState>,
        };
        builder.reset_type_state();
        builder.m_type_ops = &non_associative_reduce_ops;
        builder.m_type_state = make_builder_state(NonAssociativeReduceNodeBuilderState{child_template});
        builder.m_uses_scheduler = true;
        builder.m_has_explicit_scheduler = true;
        return builder;
    }

    NodeBuilder &switch_graph_implementation(NodeBuilder &builder,
                                             std::vector<SwitchBranchTemplate> branches,
                                             bool reload_on_ticked)
    {
        if (branches.empty()) {
            throw std::invalid_argument("switch_graph_implementation requires at least one branch template");
        }
        for (const auto &branch : branches) {
            if (branch.child_template == nullptr) {
                throw std::invalid_argument("switch_graph_implementation requires non-null branch templates");
            }
        }

        static const NodeBuilder::TypeOps switch_ops{
            &validate_switch_contract,
            &switch_builder_size,
            &switch_builder_alignment,
            &switch_construct_at,
            &destruct_builder_node,
            &clone_builder_state<SwitchNodeBuilderState>,
            &destroy_builder_state<SwitchNodeBuilderState>,
        };
        builder.reset_type_state();
        builder.m_type_ops = &switch_ops;
        builder.m_type_state = make_builder_state(SwitchNodeBuilderState{std::move(branches), reload_on_ticked});
        builder.m_uses_scheduler = true;
        builder.m_has_explicit_scheduler = true;
        return builder;
    }

    const NodeBuilder::TypeOps &NodeBuilder::static_type_ops() {
        static const TypeOps ops{
            &validate_static_contract,
            &static_builder_size,
            &static_builder_alignment,
            &construct_static_builder_at,
            &destruct_builder_node,
            &clone_builder_state<StaticNodeBuilderState>,
            &destroy_builder_state<StaticNodeBuilderState>,
        };
        return ops;
    }

    const NodeBuilder::TypeOps &NodeBuilder::python_type_ops() {
        static const TypeOps ops{
            &validate_python_contract,
            &python_builder_size,
            &python_builder_alignment,
            &construct_python_builder_at,
            &destruct_builder_node,
            &clone_builder_state<PythonNodeBuilderState>,
            &destroy_builder_state<PythonNodeBuilderState>,
        };
        return ops;
    }

    void NodeBuilder::reset_type_state() noexcept {
        if (m_type_state != nullptr && m_type_ops != nullptr && m_type_ops->destroy_state != nullptr) {
            m_type_ops->destroy_state(m_type_state);
        }
        m_type_state = nullptr;
    }

    NodeBuilder &NodeBuilder::label(std::string value) {
        m_label = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::node_type(NodeTypeEnum value) {
        m_node_type              = value;
        m_has_explicit_node_type = true;
        if (m_type_ops != nullptr) { m_type_ops->validate_contract(*this); }
        return *this;
    }

    NodeBuilder &NodeBuilder::input_schema(const TSMeta *value) {
        m_input_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::output_schema(const TSMeta *value) {
        m_output_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::error_output_schema(const TSMeta *value) {
        m_error_output_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::recordable_state_schema(const TSMeta *value) {
        m_has_recordable_state    = value != nullptr;
        m_recordable_state_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::active_input(size_t slot) {
        m_active_inputs.emplace_back(slot);
        m_has_explicit_active_inputs = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::valid_input(size_t slot) {
        m_valid_inputs.emplace_back(slot);
        m_has_explicit_valid_inputs = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::all_valid_input(size_t slot) {
        m_all_valid_inputs.emplace_back(slot);
        m_has_explicit_all_valid_inputs = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::set_active_inputs(std::vector<size_t> slots) {
        m_active_inputs              = std::move(slots);
        m_has_explicit_active_inputs = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::set_valid_inputs(std::vector<size_t> slots) {
        m_valid_inputs              = std::move(slots);
        m_has_explicit_valid_inputs = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::set_all_valid_inputs(std::vector<size_t> slots) {
        m_all_valid_inputs              = std::move(slots);
        m_has_explicit_all_valid_inputs = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::python_signature(nb::object value) {
        m_python_signature = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_scalars(nb::dict value) {
        m_python_scalars = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_input_builder(nb::object value) {
        m_python_input_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_output_builder(nb::object value) {
        m_python_output_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_error_builder(nb::object value) {
        m_python_error_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_recordable_state_builder(nb::object value) {
        m_python_recordable_state_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_implementation(nb::object eval_fn, nb::object start_fn, nb::object stop_fn) {
        set_python_type_state(std::move(eval_fn), std::move(start_fn), std::move(stop_fn));
        return *this;
    }

    NodeBuilder &NodeBuilder::implementation_name(std::string value) {
        m_implementation_name = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::public_node_index(int64_t value) noexcept {
        m_public_node_index = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::uses_scheduler(bool value) noexcept {
        m_uses_scheduler = value;
        m_has_explicit_scheduler = true;
        return *this;
    }

    NodeBuilder &NodeBuilder::requires_resolved_schemas(bool value) noexcept {
        m_requires_resolved_schemas = value;
        return *this;
    }

    size_t NodeBuilder::size(const std::vector<TSInputConstructionEdge> &inbound_edges) const {
        validate_complete();
        return m_type_ops->size(*this, inbound_edges);
    }

    size_t NodeBuilder::alignment(const std::vector<TSInputConstructionEdge> &inbound_edges) const {
        validate_complete();
        return m_type_ops->alignment(*this, inbound_edges);
    }

    Node *NodeBuilder::construct_at(void *memory, int64_t node_index,
                                    const std::vector<TSInputConstructionEdge> &inbound_edges) const {
        if (memory == nullptr) { throw std::invalid_argument("v2 node builder requires non-null construction memory"); }
        validate_complete();
        return m_type_ops->construct_at(*this, memory, node_index, inbound_edges);
    }

    void NodeBuilder::destruct_at(Node &node) const noexcept {
        assert(m_type_ops != nullptr);
        m_type_ops->destruct_at(*this, node);
    }
}  // namespace hgraph
