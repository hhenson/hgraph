#include <hgraph/types/time_series/ts_output_builder.h>
#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node_builder.h>
#include <hgraph/types/v2/python_node_support.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        [[nodiscard]] constexpr size_t align_up(size_t value, size_t alignment) noexcept
        {
            if (alignment == 0) { return value; }
            const size_t remainder = value % alignment;
            return remainder == 0 ? value : value + (alignment - remainder);
        }

        struct ResolvedNodeBuilders
        {
            const TSInputBuilder *input_builder{nullptr};
            const TSOutputBuilder *output_builder{nullptr};
            const ValueBuilder *state_builder{nullptr};
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
            size_t state_storage_offset{0};
            size_t recordable_state_object_offset{0};
            size_t recordable_state_storage_offset{0};
        };

        struct StaticNodeBuilderState
        {
            const NodeRuntimeOps *runtime_ops{nullptr};
            const PushSourceNodeRuntimeOps *push_source_runtime_ops{nullptr};
            bool has_push_message_hook{false};
        };

        struct PythonNodeBuilderState
        {
            nb::object eval_fn;
            nb::object start_fn;
            nb::object stop_fn;
        };

        struct PythonNodeHeapState
        {
            nb::object python_signature;
            nb::object python_scalars;
            nb::object eval_fn;
            nb::object start_fn;
            nb::object stop_fn;
            nb::object node_handle;
            nb::object output_handle;
            nb::dict kwargs;
            nb::tuple start_parameter_names;
            nb::tuple stop_parameter_names;
        };

        struct PythonNodeRuntimeData
        {
            TSInput *input{nullptr};
            TSOutput *output{nullptr};
            TSOutput *recordable_state{nullptr};
            PythonNodeHeapState *heap_state{nullptr};
        };

        template <typename TState>
        [[nodiscard]] std::shared_ptr<const void> make_builder_state(TState state)
        {
            return std::shared_ptr<const void>(new TState(std::move(state)), [](const void *ptr) {
                delete static_cast<const TState *>(ptr);
            });
        }

        template <>
        [[nodiscard]] std::shared_ptr<const void> make_builder_state(PythonNodeBuilderState state)
        {
            return std::shared_ptr<const void>(new PythonNodeBuilderState(std::move(state)), [](const void *ptr) {
                nb::gil_scoped_acquire guard;
                delete static_cast<const PythonNodeBuilderState *>(ptr);
            });
        }

        [[nodiscard]] ResolvedNodeBuilders resolve_builders(
            const NodeBuilder &builder,
            const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            ResolvedNodeBuilders builders;

            if (builder.input_schema() != nullptr) {
                const TSInputConstructionPlan plan = TSInputConstructionPlanCompiler::compile(*builder.input_schema(), inbound_edges);
                builders.input_builder = &TSInputBuilderFactory::checked_builder_for(plan);
            } else if (!inbound_edges.empty()) {
                throw std::invalid_argument("v2 nodes without an input schema cannot accept inbound edges");
            }

            if (builder.output_schema() != nullptr) {
                builders.output_builder = &TSOutputBuilderFactory::checked_builder_for(builder.output_schema());
            }
            if (builder.has_state() && builder.state_schema() == nullptr) {
                throw std::invalid_argument("v2 nodes with typed State<...> require a resolved value schema");
            }
            if (builder.has_state() && builder.state_schema() != nullptr) {
                builders.state_builder = &ValueBuilderFactory::checked_builder_for(builder.state_schema());
            }
            if (builder.recordable_state_schema() != nullptr) {
                builders.recordable_state_builder =
                    &TSOutputBuilderFactory::checked_builder_for(builder.recordable_state_schema());
            }
            if (builder.recordable_state_schema() == nullptr && builder.recordable_state_builder().is_valid() &&
                !builder.recordable_state_builder().is_none()) {
                throw std::invalid_argument("v2 nodes with RecordableState<...> require a resolved time-series schema");
            }

            return builders;
        }

        [[nodiscard]] NodeMemoryLayout describe_layout(const NodeBuilder &builder,
                                                       size_t runtime_data_size,
                                                       size_t runtime_data_alignment,
                                                       const ResolvedNodeBuilders &builders) noexcept
        {
            NodeMemoryLayout layout;
            layout.alignment = std::max(alignof(Node), alignof(BuiltNodeSpec));

            size_t offset = sizeof(Node);
            offset = align_up(offset, alignof(BuiltNodeSpec));
            layout.spec_offset = offset;
            offset += sizeof(BuiltNodeSpec);

            if (builder.uses_scheduler()) {
                layout.alignment = std::max(layout.alignment, alignof(NodeScheduler));
                offset = align_up(offset, alignof(NodeScheduler));
                layout.scheduler_offset = offset;
                offset += sizeof(NodeScheduler);
            }

            layout.alignment = std::max(layout.alignment, runtime_data_alignment);
            offset = align_up(offset, runtime_data_alignment);
            layout.runtime_data_offset = offset;
            offset += runtime_data_size;

            if (builders.state_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, builders.state_builder->alignment());
                offset = align_up(offset, builders.state_builder->alignment());
                layout.state_storage_offset = offset;
                offset += builders.state_builder->size();
            }

            if (!builder.label().empty()) {
                layout.label_offset = offset;
                offset += builder.label().size();
            }

            layout.alignment = std::max(layout.alignment, alignof(size_t));
            offset = align_up(offset, alignof(size_t));
            layout.active_slots_offset = offset;
            offset += sizeof(size_t) * builder.active_inputs().size();

            offset = align_up(offset, alignof(size_t));
            layout.valid_slots_offset = offset;
            offset += sizeof(size_t) * builder.valid_inputs().size();

            offset = align_up(offset, alignof(size_t));
            layout.all_valid_slots_offset = offset;
            offset += sizeof(size_t) * builder.all_valid_inputs().size();

            if (builders.input_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, alignof(TSInput));
                offset = align_up(offset, alignof(TSInput));
                layout.input_object_offset = offset;
                offset += sizeof(TSInput);

                layout.alignment = std::max(layout.alignment, builders.input_builder->alignment());
                offset = align_up(offset, builders.input_builder->alignment());
                layout.input_storage_offset = offset;
                offset += builders.input_builder->size();
            }

            if (builders.output_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, alignof(TSOutput));
                offset = align_up(offset, alignof(TSOutput));
                layout.output_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment = std::max(layout.alignment, builders.output_builder->alignment());
                offset = align_up(offset, builders.output_builder->alignment());
                layout.output_storage_offset = offset;
                offset += builders.output_builder->size();
            }

            if (builders.recordable_state_builder != nullptr) {
                layout.alignment = std::max(layout.alignment, alignof(TSOutput));
                offset = align_up(offset, alignof(TSOutput));
                layout.recordable_state_object_offset = offset;
                offset += sizeof(TSOutput);

                layout.alignment = std::max(layout.alignment, builders.recordable_state_builder->alignment());
                offset = align_up(offset, builders.recordable_state_builder->alignment());
                layout.recordable_state_storage_offset = offset;
                offset += builders.recordable_state_builder->size();
            }

            layout.total_size = offset;
            return layout;
        }

        [[nodiscard]] std::span<const size_t> materialize_slots(
            std::byte *base,
            std::span<const size_t> source,
            size_t slots_offset)
        {
            if (source.empty()) { return {}; }

            auto *slots = reinterpret_cast<size_t *>(base + slots_offset);
            std::copy(source.begin(), source.end(), slots);
            return {slots, source.size()};
        }

        void validate_static_contract(const NodeBuilder &builder)
        {
            const auto &state = detail::node_builder_type_state<StaticNodeBuilderState>(builder);
            if (builder.node_type() == NodeTypeEnum::PUSH_SOURCE_NODE && !state.has_push_message_hook) {
                throw std::logic_error("v2 push source nodes require a static bool apply_message(...) hook");
            }
        }

        void validate_python_contract(const NodeBuilder &builder)
        {
            const auto &state = detail::node_builder_type_state<PythonNodeBuilderState>(builder);
            if (!state.eval_fn.is_valid() || state.eval_fn.is_none()) {
                throw std::invalid_argument("v2 Python node builder requires an eval function");
            }
            if (builder.node_type() == NodeTypeEnum::PUSH_SOURCE_NODE) {
                throw std::invalid_argument("v2 Python-backed nodes do not yet support push-source semantics");
            }
        }

        void destruct_static_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<detail::StaticNodeRuntimeData>(node);

            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
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

        void destruct_python_node(Node &node) noexcept
        {
            const BuiltNodeSpec &spec = node.spec();
            auto &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);

            if (runtime_data.heap_state != nullptr) {
                nb::gil_scoped_acquire guard;
                delete runtime_data.heap_state;
            }

            if (runtime_data.recordable_state != nullptr) { runtime_data.recordable_state->~TSOutput(); }
            if (runtime_data.output != nullptr) { runtime_data.output->~TSOutput(); }
            if (runtime_data.input != nullptr) { runtime_data.input->~TSInput(); }

            if (auto *scheduler = node.scheduler_if_present(); scheduler != nullptr) { std::destroy_at(scheduler); }
            std::destroy_at(const_cast<BuiltNodeSpec *>(&spec));
            std::destroy_at(&runtime_data);
        }

        void python_node_start(Node &node, engine_time_t)
        {
            auto &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            nb::gil_scoped_acquire guard;
            auto &heap_state = *runtime_data.heap_state;
            heap_state.kwargs = make_python_node_kwargs(heap_state.python_signature, heap_state.python_scalars, heap_state.node_handle);
            static_cast<void>(
                call_python_callable_with_subset(heap_state.start_fn, heap_state.kwargs, heap_state.start_parameter_names));
        }

        void python_node_stop(Node &node, engine_time_t)
        {
            auto &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            nb::gil_scoped_acquire guard;
            auto &heap_state = *runtime_data.heap_state;
            static_cast<void>(
                call_python_callable_with_subset(heap_state.stop_fn, heap_state.kwargs, heap_state.stop_parameter_names));
        }

        void python_node_eval(Node &node, engine_time_t)
        {
            auto &runtime_data = detail::runtime_data<PythonNodeRuntimeData>(node);
            nb::gil_scoped_acquire guard;
            auto &heap_state = *runtime_data.heap_state;
            nb::object out = call_python_callable(heap_state.eval_fn, heap_state.kwargs);
            if (!out.is_none() && runtime_data.output != nullptr && heap_state.output_handle.is_valid() &&
                !heap_state.output_handle.is_none()) {
                heap_state.output_handle.attr("apply_result")(out);
            }
        }

        [[nodiscard]] bool python_node_has_input(const Node &node) noexcept
        {
            return node.data() != nullptr && detail::runtime_data<PythonNodeRuntimeData>(node).input != nullptr;
        }

        [[nodiscard]] bool python_node_has_output(const Node &node) noexcept
        {
            return node.data() != nullptr && detail::runtime_data<PythonNodeRuntimeData>(node).output != nullptr;
        }

        [[nodiscard]] TSInputView python_node_input_view(Node &node, engine_time_t evaluation_time)
        {
            return detail::runtime_data<PythonNodeRuntimeData>(node).input->view(&node, evaluation_time);
        }

        [[nodiscard]] TSOutputView python_node_output_view(Node &node, engine_time_t evaluation_time)
        {
            return detail::runtime_data<PythonNodeRuntimeData>(node).output->view(evaluation_time);
        }

        [[nodiscard]] std::string python_node_runtime_label(const Node &node)
        {
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
            &python_node_input_view,
            &python_node_output_view,
            &python_node_runtime_label,
        };

        template <typename TState, typename TBuildRuntimeData, typename TPopulateRuntime>
        [[nodiscard]] Node *construct_node_chunk(const NodeBuilder &builder,
                                                 void *memory,
                                                 int64_t node_index,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges,
                                                 const NodeRuntimeOps *runtime_ops,
                                                 const PushSourceNodeRuntimeOps *push_source_runtime_ops,
                                                 void (*destruct_fn)(Node &) noexcept,
                                                 size_t runtime_data_size,
                                                 size_t runtime_data_alignment,
                                                 TBuildRuntimeData build_runtime_data,
                                                 TPopulateRuntime populate_runtime)
        {
            const auto builders = resolve_builders(builder, inbound_edges);
            const auto layout = describe_layout(builder, runtime_data_size, runtime_data_alignment, builders);
            auto *base = static_cast<std::byte *>(memory);

            std::string_view label_view;
            if (!builder.label().empty()) {
                auto *label_ptr = reinterpret_cast<char *>(base + layout.label_offset);
                std::memcpy(label_ptr, builder.label().data(), builder.label().size());
                label_view = std::string_view{label_ptr, builder.label().size()};
            }

            const auto materialized_active_inputs = materialize_slots(base, builder.active_inputs(), layout.active_slots_offset);
            const auto materialized_valid_inputs = materialize_slots(base, builder.valid_inputs(), layout.valid_slots_offset);
            const auto materialized_all_valid_inputs =
                materialize_slots(base, builder.all_valid_inputs(), layout.all_valid_slots_offset);

            TSInput *input = nullptr;
            TSOutput *output = nullptr;
            void *state_memory = nullptr;
            TSOutput *recordable_state = nullptr;
            auto cleanup_input = hgraph::make_scope_exit([&] {
                if (input != nullptr) { input->~TSInput(); }
            });
            auto cleanup_output = hgraph::make_scope_exit([&] {
                if (output != nullptr) { output->~TSOutput(); }
            });
            auto cleanup_state = hgraph::make_scope_exit([&] {
                if (builders.state_builder != nullptr && state_memory != nullptr) { builders.state_builder->destruct(state_memory); }
            });
            auto cleanup_recordable_state = hgraph::make_scope_exit([&] {
                if (recordable_state != nullptr) { recordable_state->~TSOutput(); }
            });
            void *runtime_data = nullptr;
            auto cleanup_runtime_data = hgraph::make_scope_exit([&] {
                if (runtime_data == nullptr) { return; }
                populate_runtime.destroy(runtime_data);
            });
            NodeScheduler *scheduler = nullptr;
            auto cleanup_scheduler = hgraph::make_scope_exit([&] {
                if (scheduler != nullptr) { std::destroy_at(scheduler); }
            });
            BuiltNodeSpec *spec = nullptr;
            auto cleanup_spec = hgraph::make_scope_exit([&] {
                if (spec != nullptr) { std::destroy_at(spec); }
            });
            Node *node = nullptr;
            auto cleanup_node = hgraph::make_scope_exit([&] {
                if (node != nullptr) { node->~Node(); }
            });

            if (builders.input_builder != nullptr) {
                input = new (base + layout.input_object_offset) TSInput{};
                builders.input_builder->construct_input(
                    *input, base + layout.input_storage_offset, TSInputBuilder::MemoryOwnership::External);
            }

            if (builders.output_builder != nullptr) {
                output = new (base + layout.output_object_offset) TSOutput{};
                builders.output_builder->construct_output(
                    *output, base + layout.output_storage_offset, TSOutputBuilder::MemoryOwnership::External);
            }

            if (builders.state_builder != nullptr) {
                state_memory = base + layout.state_storage_offset;
                builders.state_builder->construct(state_memory);
            }

            if (builders.recordable_state_builder != nullptr) {
                recordable_state = new (base + layout.recordable_state_object_offset) TSOutput{};
                builders.recordable_state_builder->construct_output(
                    *recordable_state,
                    base + layout.recordable_state_storage_offset,
                    TSOutputBuilder::MemoryOwnership::External);
            }

            runtime_data = build_runtime_data(base + layout.runtime_data_offset, builders, input, output, state_memory, recordable_state);

            spec = new (base + layout.spec_offset) BuiltNodeSpec{
                runtime_ops,
                push_source_runtime_ops,
                destruct_fn,
                layout.scheduler_offset,
                layout.runtime_data_offset,
                builder.uses_scheduler(),
                label_view,
                builder.node_type(),
                builder.input_schema(),
                builder.output_schema(),
                materialized_active_inputs,
                materialized_valid_inputs,
                materialized_all_valid_inputs,
            };

            node = new (memory) Node(node_index, spec);
            if (builder.uses_scheduler()) { scheduler = new (base + layout.scheduler_offset) NodeScheduler{node}; }
            populate_runtime.initialise(builder, runtime_data, node, input, output, recordable_state);

            cleanup_node.release();
            cleanup_spec.release();
            cleanup_scheduler.release();
            cleanup_runtime_data.release();
            cleanup_recordable_state.release();
            cleanup_state.release();
            cleanup_output.release();
            cleanup_input.release();
            return node;
        }

        [[nodiscard]] size_t static_builder_size(const NodeBuilder &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            validate_static_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(detail::StaticNodeRuntimeData), alignof(detail::StaticNodeRuntimeData), builders)
                .total_size;
        }

        [[nodiscard]] size_t static_builder_alignment(const NodeBuilder &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            validate_static_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(detail::StaticNodeRuntimeData), alignof(detail::StaticNodeRuntimeData), builders)
                .alignment;
        }

        [[nodiscard]] Node *construct_static_builder_at(const NodeBuilder &builder,
                                                        void *memory,
                                                        int64_t node_index,
                                                        const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            validate_static_contract(builder);
            const auto &state = detail::node_builder_type_state<StaticNodeBuilderState>(builder);

            struct RuntimePopulator
            {
                static void *build(void *storage,
                                   const ResolvedNodeBuilders &builders,
                                   TSInput *input,
                                   TSOutput *output,
                                   void *state_memory,
                                   TSOutput *recordable_state,
                                   const NodeBuilder &builder)
                {
                    return new (storage) detail::StaticNodeRuntimeData{
                        input,
                        output,
                        builders.state_builder,
                        state_memory,
                        recordable_state,
                        builder.scalars().is_valid() ? nb::borrow(builder.scalars()) : nb::object(),
                    };
                }

                static void destroy(void *runtime_data)
                {
                    auto &static_runtime = *static_cast<detail::StaticNodeRuntimeData *>(runtime_data);
                    if (static_runtime.python_scalars.is_valid()) {
                        nb::gil_scoped_acquire guard;
                        std::destroy_at(&static_runtime);
                    } else {
                        std::destroy_at(&static_runtime);
                    }
                }

                static void initialise(const NodeBuilder &, void *, Node *, TSInput *, TSOutput *, TSOutput *) {}
            };

            struct RuntimeLifecycle
            {
                void destroy(void *runtime_data) const { RuntimePopulator::destroy(runtime_data); }
                void initialise(const NodeBuilder &builder,
                                void *runtime_data,
                                Node *node,
                                TSInput *input,
                                TSOutput *output,
                                TSOutput *recordable_state) const
                {
                    static_cast<void>(builder);
                    static_cast<void>(runtime_data);
                    static_cast<void>(node);
                    static_cast<void>(input);
                    static_cast<void>(output);
                    static_cast<void>(recordable_state);
                }
            };

            return construct_node_chunk<StaticNodeBuilderState>(
                builder,
                memory,
                node_index,
                inbound_edges,
                state.runtime_ops,
                state.push_source_runtime_ops,
                &destruct_static_node,
                sizeof(detail::StaticNodeRuntimeData),
                alignof(detail::StaticNodeRuntimeData),
                [&](void *storage,
                    const ResolvedNodeBuilders &builders,
                    TSInput *input,
                    TSOutput *output,
                    void *state_memory,
                    TSOutput *recordable_state) {
                    return RuntimePopulator::build(storage, builders, input, output, state_memory, recordable_state, builder);
                },
                RuntimeLifecycle{});
        }

        [[nodiscard]] size_t python_builder_size(const NodeBuilder &builder,
                                                 const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            validate_python_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(PythonNodeRuntimeData), alignof(PythonNodeRuntimeData), builders).total_size;
        }

        [[nodiscard]] size_t python_builder_alignment(const NodeBuilder &builder,
                                                      const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            validate_python_contract(builder);
            const auto builders = resolve_builders(builder, inbound_edges);
            return describe_layout(builder, sizeof(PythonNodeRuntimeData), alignof(PythonNodeRuntimeData), builders).alignment;
        }

        [[nodiscard]] Node *construct_python_builder_at(const NodeBuilder &builder,
                                                        void *memory,
                                                        int64_t node_index,
                                                        const std::vector<TSInputConstructionEdge> &inbound_edges)
        {
            validate_python_contract(builder);

            struct RuntimeLifecycle
            {
                void destroy(void *runtime_data) const
                {
                    auto &python_runtime = *static_cast<PythonNodeRuntimeData *>(runtime_data);
                    if (python_runtime.heap_state != nullptr) {
                        nb::gil_scoped_acquire guard;
                        delete python_runtime.heap_state;
                    }
                    std::destroy_at(&python_runtime);
                }

                void initialise(const NodeBuilder &builder,
                                void *runtime_data_ptr,
                                Node *node,
                                TSInput *input,
                                TSOutput *output,
                                TSOutput *recordable_state) const
                {
                    auto &runtime_data = *static_cast<PythonNodeRuntimeData *>(runtime_data_ptr);
                    const auto &state = detail::node_builder_type_state<PythonNodeBuilderState>(builder);

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
                    };
                    runtime_data.heap_state->node_handle = make_python_node_handle(
                        runtime_data.heap_state->python_signature,
                        runtime_data.heap_state->python_scalars,
                        node,
                        input,
                        output,
                        recordable_state,
                        builder.input_schema(),
                        builder.output_schema(),
                        builder.recordable_state_schema(),
                        node->scheduler_if_present());
                    runtime_data.heap_state->output_handle = runtime_data.heap_state->node_handle.attr("output");
                    runtime_data.heap_state->start_parameter_names =
                        python_callable_parameter_names(runtime_data.heap_state->start_fn);
                    runtime_data.heap_state->stop_parameter_names =
                        python_callable_parameter_names(runtime_data.heap_state->stop_fn);
                }
            };

            return construct_node_chunk<PythonNodeBuilderState>(
                builder,
                memory,
                node_index,
                inbound_edges,
                &k_python_node_runtime_ops,
                nullptr,
                &destruct_python_node,
                sizeof(PythonNodeRuntimeData),
                alignof(PythonNodeRuntimeData),
                [](void *storage,
                   const ResolvedNodeBuilders &,
                   TSInput *input,
                   TSOutput *output,
                   void *,
                   TSOutput *recordable_state) -> void * {
                    return new (storage) PythonNodeRuntimeData{input, output, recordable_state, nullptr};
                },
                RuntimeLifecycle{});
        }

        void destruct_builder_node(const NodeBuilder &builder, Node &node) noexcept
        {
            static_cast<void>(builder);
            node.spec().destruct(node);
        }
    }  // namespace

    namespace detail
    {
        template <typename TState>
        const TState &node_builder_type_state(const NodeBuilder &builder)
        {
            return builder.type_state<TState>();
        }
    }  // namespace detail

    void NodeBuilder::set_type_state(const NodeRuntimeOps &runtime_ops,
                                     const PushSourceNodeRuntimeOps *push_source_runtime_ops,
                                     bool has_push_message_hook)
    {
        m_type_ops = &static_type_ops();
        m_type_state = make_builder_state(StaticNodeBuilderState{&runtime_ops, push_source_runtime_ops, has_push_message_hook});
        m_type_ops->validate_contract(*this);
    }

    void NodeBuilder::set_python_type_state(nb::object eval_fn, nb::object start_fn, nb::object stop_fn)
    {
        m_type_ops = &python_type_ops();
        m_type_state = make_builder_state(PythonNodeBuilderState{std::move(eval_fn), std::move(start_fn), std::move(stop_fn)});
        m_type_ops->validate_contract(*this);
    }

    const NodeBuilder::TypeOps &NodeBuilder::static_type_ops()
    {
        static const TypeOps ops{
            &validate_static_contract,
            &static_builder_size,
            &static_builder_alignment,
            &construct_static_builder_at,
            &destruct_builder_node,
        };
        return ops;
    }

    const NodeBuilder::TypeOps &NodeBuilder::python_type_ops()
    {
        static const TypeOps ops{
            &validate_python_contract,
            &python_builder_size,
            &python_builder_alignment,
            &construct_python_builder_at,
            &destruct_builder_node,
        };
        return ops;
    }

    NodeBuilder &NodeBuilder::label(std::string value)
    {
        m_label = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::node_type(NodeTypeEnum value)
    {
        m_node_type = value;
        m_has_explicit_node_type = true;
        if (m_type_ops != nullptr) { m_type_ops->validate_contract(*this); }
        return *this;
    }

    NodeBuilder &NodeBuilder::input_schema(const TSMeta *value)
    {
        m_input_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::output_schema(const TSMeta *value)
    {
        m_output_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::recordable_state_schema(const TSMeta *value)
    {
        m_has_recordable_state = value != nullptr;
        m_recordable_state_schema = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::active_input(size_t slot)
    {
        m_active_inputs.emplace_back(slot);
        return *this;
    }

    NodeBuilder &NodeBuilder::valid_input(size_t slot)
    {
        m_valid_inputs.emplace_back(slot);
        return *this;
    }

    NodeBuilder &NodeBuilder::all_valid_input(size_t slot)
    {
        m_all_valid_inputs.emplace_back(slot);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_signature(nb::object value)
    {
        m_python_signature = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_scalars(nb::dict value)
    {
        m_python_scalars = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_input_builder(nb::object value)
    {
        m_python_input_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_output_builder(nb::object value)
    {
        m_python_output_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_error_builder(nb::object value)
    {
        m_python_error_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_recordable_state_builder(nb::object value)
    {
        m_python_recordable_state_builder = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::python_implementation(nb::object eval_fn, nb::object start_fn, nb::object stop_fn)
    {
        set_python_type_state(std::move(eval_fn), std::move(start_fn), std::move(stop_fn));
        return *this;
    }

    NodeBuilder &NodeBuilder::implementation_name(std::string value)
    {
        m_implementation_name = std::move(value);
        return *this;
    }

    NodeBuilder &NodeBuilder::uses_scheduler(bool value) noexcept
    {
        m_uses_scheduler = value;
        return *this;
    }

    NodeBuilder &NodeBuilder::requires_resolved_schemas(bool value) noexcept
    {
        m_requires_resolved_schemas = value;
        return *this;
    }

    size_t NodeBuilder::size(const std::vector<TSInputConstructionEdge> &inbound_edges) const
    {
        validate_complete();
        return m_type_ops->size(*this, inbound_edges);
    }

    size_t NodeBuilder::alignment(const std::vector<TSInputConstructionEdge> &inbound_edges) const
    {
        validate_complete();
        return m_type_ops->alignment(*this, inbound_edges);
    }

    Node *NodeBuilder::construct_at(void *memory,
                                    int64_t node_index,
                                    const std::vector<TSInputConstructionEdge> &inbound_edges) const
    {
        if (memory == nullptr) { throw std::invalid_argument("v2 node builder requires non-null construction memory"); }
        validate_complete();
        return m_type_ops->construct_at(*this, memory, node_index, inbound_edges);
    }

    void NodeBuilder::destruct_at(Node &node) const noexcept
    {
        assert(m_type_ops != nullptr);
        m_type_ops->destruct_at(*this, node);
    }
}  // namespace hgraph::v2
