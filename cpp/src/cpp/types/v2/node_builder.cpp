#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/v2/boundary_binding.h>
#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/nested_node_builder.h>
#include <hgraph/types/v2/node_builder.h>
#include <hgraph/types/v2/path_constants.h>
#include <hgraph/types/v2/python_node_support.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <fmt/format.h>
#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        enum class RootNodePort : size_t
        {
            Input = 0,
            Output = 1,
            ErrorOutput = 2,
            RecordableState = 3,
        };

        [[nodiscard]] TSOutputView bound_output_of(TSInputView view) noexcept {
            const TSViewContext &context = view.context_ref();
            BaseState           *state   = context.ts_state;
            if (state == nullptr || state->storage_kind != TSStorageKind::TargetLink) { return TSOutputView{}; }

            const auto &link_state = *static_cast<const TargetLinkState *>(state);
            if (!link_state.is_bound()) { return TSOutputView{}; }

            return TSOutputView{
                TSViewContext{link_state.target},
                TSViewContext::none(),
                view.evaluation_time(),
                link_state.target.owning_output,
                link_state.target.output_view_ops,
            };
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
                        fmt::format("v2 input builder resolution failed for node '{}': {}", builder.label(), e.what()));
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
                    fmt::format("v2 node '{}' without an input schema cannot accept inbound edges: {}",
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
                throw std::invalid_argument("v2 nodes with error capture outputs require a resolved time-series schema");
            }
            if (builder.has_state() && builder.state_schema() == nullptr) {
                throw std::invalid_argument("v2 nodes with typed State<...> require a resolved value schema");
            }
            if (builder.has_state() && builder.state_schema() != nullptr) {
                builders.state_builder = &ValueBuilderFactory::checked_builder_for(builder.state_schema());
            }
            if (builder.recordable_state_schema() != nullptr) {
                builders.recordable_state_builder = &TSOutputBuilderFactory::checked_builder_for(builder.recordable_state_schema());
            }
            if (builder.recordable_state_schema() == nullptr && builder.recordable_state_builder().is_valid() &&
                !builder.recordable_state_builder().is_none()) {
                throw std::invalid_argument("v2 nodes with RecordableState<...> require a resolved time-series schema");
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
                try {
                    runtime.child_instance.stop(evaluation_time);
                } catch (...) {}
                publish_try_except_error(node, evaluation_time, e.error());
                return;
            } catch (const std::exception &e) {
                try {
                    runtime.child_instance.stop(evaluation_time);
                } catch (...) {}
                publish_try_except_error(node, evaluation_time, fallback_try_except_error(node, e.what()));
                return;
            } catch (...) {
                try {
                    runtime.child_instance.stop(evaluation_time);
                } catch (...) {}
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
          m_uses_scheduler(other.m_uses_scheduler), m_active_inputs(other.m_active_inputs), m_valid_inputs(other.m_valid_inputs),
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
          m_uses_scheduler(other.m_uses_scheduler), m_active_inputs(std::move(other.m_active_inputs)),
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
}  // namespace hgraph::v2
