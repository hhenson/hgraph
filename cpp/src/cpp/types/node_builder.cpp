#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/time_series/value/keyed_slot_store.h>
#include <hgraph/types/time_series/value/value.h>
#include <hgraph/types/boundary_binding.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/nested_node_builder.h>
#include <hgraph/types/node_builder.h>
#include <hgraph/types/path_constants.h>
#include <hgraph/types/python_node_support.h>
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
            const TSViewContext source_context = view.context_ref().resolved();
            if (source_context.schema == nullptr || source_context.value_dispatch == nullptr || source_context.ts_dispatch == nullptr ||
                source_context.ts_state == nullptr || source_context.owning_output == nullptr) {
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
                target->ts_state != nullptr && target->owning_output != nullptr) {
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
                    }},
                    TSViewContext::none(),
                    view.evaluation_time(),
                    target->owning_output,
                    target->output_view_ops != nullptr ? target->output_view_ops : &hgraph::detail::default_output_view_ops(),
                };
            }

            return bound_output_of(view);
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
                if (!out.is_none() && runtime_data.output != nullptr && heap_state.output_handle.is_valid() &&
                    !heap_state.output_handle.is_none()) {
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

                if (source_output.ts_schema() == nullptr) {
                    clear_output_link(target_output);
                    continue;
                }

                if (const auto *parent_schema = target_output.ts_schema(); parent_schema != nullptr &&
                                                                           source_output.ts_schema() != parent_schema &&
                                                                           source_output.owning_output() != nullptr) {
                    source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
                }

                const bool rebound = bind_output_link(target_output, source_output);
                if (evaluation_time != MIN_DT && (rebound || source_output.modified())) {
                    mark_output_view_modified(target_output, evaluation_time);
                }
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
            runtime.child_instance.evaluate(evaluation_time);
            forward_child_outputs(node, node.output_view(evaluation_time), runtime, evaluation_time);
        }

        void try_except_node_eval(Node &node, engine_time_t evaluation_time) {
            auto &runtime = nested_runtime(node);
            if (!runtime.child_instance.is_started()) { return; }

            if (ensure_nested_child_bound(node, runtime, evaluation_time)) {
                forward_child_outputs(node, node.output_view(evaluation_time).as_bundle().field("out"), runtime, MIN_DT);
            }

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

        void clear_switch_output_links(Node &node, SwitchNodeRuntimeData &runtime, engine_time_t evaluation_time)
        {
            if (!node.has_output()) { return; }
            const auto *branch = active_switch_branch(runtime);
            if (branch == nullptr) { return; }

            TSOutputView parent_output = node.output_view(evaluation_time);
            for (const auto &spec : branch->child_template->boundary_plan.outputs) {
                TSOutputView target_output = navigate_output(parent_output, spec.parent_output_path);
                clear_output_link(target_output);
                if (evaluation_time != MIN_DT) { clear_output_value(target_output); }
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
                if (source_output.ts_schema() == nullptr) {
                    clear_output_link(target_output);
                    if (evaluation_time != MIN_DT) { clear_output_value(target_output); }
                    continue;
                }

                if (!source_output.valid()) {
                    if (evaluation_time != MIN_DT && source_output.modified()) {
                        clear_output_link(target_output);
                        clear_output_value(target_output);
                    }
                    continue;
                }

                if (const auto *parent_schema = target_output.ts_schema();
                    parent_schema != nullptr && source_output.ts_schema() != parent_schema &&
                    source_output.owning_output() != nullptr) {
                    source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
                }

                const bool rebound = bind_output_link(target_output, source_output);
                if (evaluation_time != MIN_DT && (rebound || source_output.modified())) {
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
            if (!selector_input.valid()) { return; }

            const value::View selector = selector_input.value();
            const size_t branch_index = select_switch_branch(runtime, selector);
            const bool raw_selector_changed =
                selector_input.modified() && (runtime.reload_on_ticked || !runtime.active_key.has_value() ||
                                              runtime.active_key->view() != selector);
            const bool branch_changed =
                runtime.active_branch_index == static_cast<size_t>(-1) || runtime.active_branch_index != branch_index ||
                raw_selector_changed || !runtime.child_instance.is_initialised();

            if (branch_changed) { activate_switch_branch(node, runtime, branch_index, selector, evaluation_time); }
            if (!runtime.child_instance.is_started()) { return; }

            if (node.has_output()) {
                if (branch_changed) {
                    forward_switch_child_outputs(node, node.output_view(evaluation_time), runtime, MIN_DT);
                } else if (ensure_switch_child_bound(node, runtime, evaluation_time)) {
                    forward_switch_child_outputs(node, node.output_view(evaluation_time), runtime, MIN_DT);
                }
            } else {
                static_cast<void>(ensure_switch_child_bound(node, runtime, evaluation_time));
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

            if (!slot.bound && slot.child_instance.graph() != nullptr) {
                BoundaryBindingRuntime::bind(slot.child_instance.boundary_plan(), *slot.child_instance.graph(), node, MIN_DT);
                slot.bound = true;
            }

            if (!slot.child_instance.is_started()) { slot.child_instance.start(evaluation_time); }
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
            if (evaluation_time != MIN_DT && !key_output.valid()) { parent_output.as_dict().erase(key); }
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
                    clear_map_slot_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);
                    continue;
                }

                if (!source_output.valid()) {
                    clear_map_slot_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);
                    continue;
                }

                TSOutputView target_output =
                    map_target_output(parent_output, slot.key.view(), spec.parent_output_path, evaluation_time);

                if (const auto *parent_schema = target_output.ts_schema();
                    parent_schema != nullptr && source_output.ts_schema() != parent_schema &&
                    source_output.owning_output() != nullptr) {
                    source_output = source_output.owning_output()->bindable_view(source_output, parent_schema);
                }

                const bool rebound = bind_output_link(target_output, source_output);
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

            for (size_t slot_index : changed_slots) {
                const MapSlotRuntime *slot = slot_store.try_slot(slot_index);
                if (slot == nullptr) { continue; }

                for (const auto &spec : runtime.child_template->boundary_plan.outputs) {
                    TSOutputView target_output =
                        map_target_output(parent_output, slot->key.view(), spec.parent_output_path, evaluation_time);
                    mark_output_view_modified(target_output, evaluation_time);
                }
            }
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
                    if (parent_output.modified()) { return true; }
                } else if (parent_item.modified()) {
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
            auto &slot_store = map_slot_store(runtime_data);

            if (runtime_data.child_template != nullptr) {
                for (size_t slot = 0; slot < slot_store.constructed.size(); ++slot) {
                    if (MapSlotRuntime *payload = slot_store.try_slot(slot); payload != nullptr) {
                        dispose_map_slot(*runtime_data.child_template, *payload, MIN_DT);
                        slot_store.destroy_at(slot);
                    }
                }
            }

            std::destroy_at(&slot_store);
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
            TSOutputView keys_output = live_bound_output_of(keys_input);
            const View keys_value = keys_output.ts_schema() != nullptr ? keys_output.value() : keys_input.value();
            const auto keys_delta = keys_value.as_set().delta();
            const bool keys_modified = keys_output.ts_schema() != nullptr ? keys_output.modified() : keys_input.modified();

            slot_store.reserve_to(keys_delta.slot_capacity());

            TSOutputView parent_output = node.has_output() ? node.output_view(evaluation_time) : detail::invalid_output_view(evaluation_time);

            if (keys_modified) {
                for (size_t slot = 0; slot < keys_delta.slot_capacity(); ++slot) {
                    if (slot_store.has_slot(slot) && !keys_delta.slot_occupied(slot)) {
                        MapSlotRuntime &payload = *slot_store.try_slot(slot);
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
                        dispose_map_slot(*runtime.child_template, *slot_store.try_slot(slot), evaluation_time);
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
                        if (is_live_dict_key(parent_output, payload->key.view())) { parent_output.as_dict().erase(payload->key.view()); }
                    }
                    stop_map_slot(*runtime.child_template, *payload, evaluation_time);
                }
            }

            std::unordered_set<std::string> modified_direct_args;
            for (const auto &spec : runtime.child_template->boundary_plan.inputs) {
                if (spec.mode != InputBindingMode::BIND_DIRECT && spec.mode != InputBindingMode::CLONE_REF_BINDING) { continue; }
                if (resolve_parent_input_arg(node, spec.arg_name, evaluation_time).modified()) {
                    modified_direct_args.insert(spec.arg_name);
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
	                const bool should_eval =
	                    added || scheduled_now || !modified_direct_args.empty() || multiplexed_modified;

                if (!should_eval) { continue; }

	                ensure_map_slot_started(node, runtime, *payload, evaluation_time);
	                rebind_map_slot_inputs(node, *payload, modified_direct_args, added || multiplexed_modified, evaluation_time);
	                payload->child_instance.evaluate(evaluation_time);
                payload->next_scheduled = payload->child_instance.next_scheduled_time();
                if (node.has_output()) {
                    if (forward_map_slot_outputs(node, runtime, parent_output, *payload, evaluation_time)) {
                        changed_output_slots.push_back(slot);
                    }
                }
            }

            if (node.has_output()) {
                publish_map_slot_output_updates(runtime, parent_output, slot_store, changed_output_slots, evaluation_time);
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
                    std::destroy_at(&map_slot_store(*runtime));
                    std::destroy_at(runtime);
                }

                void initialise(const NodeBuilder & /*builder*/, void *runtime_data_ptr, Node * /*node*/, TSInput * /*input*/,
                                TSOutput * /*output*/, TSOutput * /*error_output*/, TSOutput * /*recordable_state*/) const
                {
                    auto &runtime = *static_cast<MapNodeRuntimeData *>(runtime_data_ptr);
                    runtime.child_template = child_template;
                    runtime.key_arg = key_arg;
                    runtime.keys_arg = keys_arg;
                    runtime.multiplexed_args = multiplexed_args;
                    new (&map_slot_store(runtime)) MapSlotStore{};
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
