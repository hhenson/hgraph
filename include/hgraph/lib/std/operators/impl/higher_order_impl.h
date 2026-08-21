#ifndef HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
#define HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H

#include <hgraph/lib/std/operators/conversion.h>  // nothing (placeholder for the mesh_subscribe value input)
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/lib/std/operators/impl/reduce_layout.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/types/operator_type_resolution.h>
#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/mesh_node.h>
#include <hgraph/runtime/ordered_reduce_node.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/runtime/tsl_map_node.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    using namespace hgraph::operator_type_resolution;

    /**
     * Default overloads for the higher-order operators — ordinary registered
     * candidates (a graph overload per kind, like any other operator family).
     * User specialisations register alongside and are selected by the standard
     * pattern ranking + ``requires_`` gating, e.g. a concrete overload gated on
     * the wired function's identity.
     */

    namespace higher_order_impl_detail
    {
        inline void append_external_service_inputs(
            Wiring &w,
            std::vector<NestedServiceInput> external_inputs,
            std::vector<const TSValueTypeMetaData *> &schemas,
            std::vector<std::uint8_t> &arg_tags,
            std::vector<WiringPortRef> &ports)
        {
            for (NestedServiceInput &input : external_inputs)
            {
                WiringPortRef source = subgraph_wiring_detail::materialize_external_service_input(
                    w, std::move(input));
                schemas.push_back(source.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(source.arg_tag));
                ports.push_back(std::move(source));
            }
        }

        struct ExternalServiceSlot
        {
            NestedServiceInput input{};
            std::size_t        slot{0};
        };

        [[nodiscard]] inline std::size_t external_service_slot(
            NestedServiceInput input,
            std::vector<ExternalServiceSlot> &external_services,
            std::vector<WiringPortRef> &slots)
        {
            const auto existing = std::ranges::find_if(
                external_services, [&](const ExternalServiceSlot &candidate) {
                    return candidate.input.service_path == input.service_path &&
                           candidate.input.service_kind == input.service_kind &&
                           candidate.input.definition == input.definition &&
                           candidate.input.source_schema == input.source_schema &&
                           candidate.input.arg_tag == input.arg_tag &&
                           candidate.input.builder.label() == input.builder.label();
                });
            if (existing != external_services.end()) { return existing->slot; }

            const std::size_t slot = slots.size();
            slots.push_back(WiringPortRef::null_source(input.source_schema));
            external_services.push_back(ExternalServiceSlot{std::move(input), slot});
            return slot;
        }

        inline void materialize_external_service_slots(
            Wiring &w, std::vector<ExternalServiceSlot> external_services,
            std::vector<WiringPortRef> &slots)
        {
            for (ExternalServiceSlot &external : external_services)
            {
                slots[external.slot] = subgraph_wiring_detail::materialize_external_service_input(
                    w, std::move(external.input));
            }
        }

        inline void configure_passive_transport_inputs(
            NodeTypeMetaData &meta,
            std::span<const WiringPortRef> inputs)
        {
            std::vector<std::size_t> active;
            active.reserve(inputs.size());
            bool has_passive = false;
            for (std::size_t index = 0; index < inputs.size(); ++index)
            {
                if (inputs[index].arg_tag == WiringPortRef::ArgTag::Passive)
                {
                    has_passive = true;
                }
                else
                {
                    active.push_back(index);
                }
            }
            if (has_passive) { meta.active_inputs = std::move(active); }
        }

        [[nodiscard]] inline std::vector<WiringInputRef> higher_order_input_refs(
            std::span<const WiringPortRef> inputs,
            std::span<const std::size_t> passive_materializers = {})
        {
            std::vector<WiringInputRef> refs;
            refs.reserve(inputs.size());
            for (std::size_t index = 0; index < inputs.size(); ++index)
            {
                refs.push_back(WiringInputRef{
                    .source = inputs[index],
                    // Passive transports normally omit their producer edge so
                    // higher-order feedback can close a cycle. A synthetic
                    // materializer is the exception: the consumer needs its
                    // initial REF publication before the first child sample.
                    .rank_dependency =
                        inputs[index].arg_tag != WiringPortRef::ArgTag::Passive ||
                        std::find(passive_materializers.begin(), passive_materializers.end(), index) !=
                            passive_materializers.end(),
                });
            }
            return refs;
        }

        /** Nested map-like runtimes rebind broadcast arguments from one root
            output handle. A fixed TSB/TSL assembled from child ports has no
            such root, so materialize its structural REF once in the parent.
            The multiplexed inputs retain their collection storage and every
            map/mesh child shares the live broadcast reference. */
        [[nodiscard]] inline std::vector<std::size_t> materialize_structural_broadcast_inputs(
            Wiring &w,
            std::vector<WiringPortRef> &inputs,
            std::vector<const TSValueTypeMetaData *> &schemas,
            std::span<const std::size_t> multiplexed_inputs)
        {
            auto &registry = TypeRegistry::instance();
            std::vector<std::size_t> passive_materializers;
            for (std::size_t index = 0; index < inputs.size(); ++index)
            {
                if (std::find(multiplexed_inputs.begin(), multiplexed_inputs.end(), index) !=
                        multiplexed_inputs.end() ||
                    !inputs[index].is_structural_source())
                {
                    continue;
                }

                const auto tag = inputs[index].arg_tag;
                inputs[index].arg_tag = WiringPortRef::ArgTag::None;
                inputs[index] = graph_wiring_detail::adapt_source_for_input(
                    w, registry.ref(inputs[index].schema),
                    std::move(inputs[index]));
                inputs[index].arg_tag = tag;
                schemas[index] = inputs[index].schema;
                if (tag == WiringPortRef::ArgTag::Passive)
                {
                    passive_materializers.push_back(index);
                }
            }
            return passive_materializers;
        }

        inline void bind_graph_output(ResolutionMap &resolution,
                                      const TSValueTypeMetaData *output,
                                      std::string_view legacy_var = {})
        {
            bind_output(resolution, output, legacy_var);
        }

        struct lifted_reduce_tsl_node_tag
        {
        };

        [[nodiscard]] inline const LiftedKernel *resolve_lifted_kernel_for_schemas(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> input_schemas,
            const TSValueTypeMetaData *expected_output = nullptr)
        {
            if (func.lifted != nullptr) { return func.lifted->valid() ? func.lifted : nullptr; }
            if (func.operator_name.empty()) { return nullptr; }
            if (func.variadic ? input_schemas.size() < func.arity : func.arity != input_schemas.size())
            {
                return nullptr;
            }

            std::vector<WiringArg> args;
            args.reserve(input_schemas.size());
            for (const TSValueTypeMetaData *schema : input_schemas)
            {
                WiringArg arg;
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port.schema = schema;
                args.push_back(std::move(arg));
            }

            // A resolution probe: failure means "no kernel", not an error.
            return fallback_on_exception(static_cast<const LiftedKernel *>(nullptr), [&]() -> const LiftedKernel * {
                ResolvedOperatorCall resolved =
                    OperatorRegistry::instance().resolve(func.operator_name,
                                                         std::span<const WiringArg>{args.data(), args.size()},
                                                         func.has_output,
                                                         expected_output);
                const LiftedKernel *kernel = resolved.impl != nullptr ? resolved.impl->lifted_kernel : nullptr;
                return kernel != nullptr && kernel->valid() ? kernel : nullptr;
            });
        }

        // A fixed-size TSL second argument (shared requires_ of the TSL overloads;
        // the dynamic-TSL/TSD reductions are separate, future overloads).
        [[nodiscard]] inline bool reduce_ts_is_fixed_tsl(OperatorCallContext context, std::size_t arity)
        {
            if (context.args.size() != arity) { return false; }
            const auto *schema = time_series_schema_at_as<AnyTSL>(context, 1);
            return schema != nullptr && schema->fixed_size() > 0;
        }

        [[nodiscard]] inline bool ordered_reduce_requested(OperatorCallContext context)
        {
            const Bool *is_associative = context.scalar_as<Bool>("is_associative");
            return is_associative != nullptr && !*is_associative;
        }

        [[nodiscard]] inline const LiftedKernel *lifted_reduce_tsl_kernel(OperatorCallContext context)
        {
            if (!reduce_ts_is_fixed_tsl(context, 2) || context.args[1].from_variadic_tail)
            {
                return nullptr;
            }

            const auto *collection = time_series_schema_at_as<AnyTSL>(context, 1);
            if (collection == nullptr || collection->fixed_size() == 0) { return nullptr; }
            const auto *element = time_series_schema_as<AnyTS>(collection->element_ts());
            if (element == nullptr) { return nullptr; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return nullptr; }

            std::array<const TSValueTypeMetaData *, 2> input_schemas{element, element};
            const LiftedKernel *kernel =
                resolve_lifted_kernel_for_schemas(*func,
                                                  std::span<const TSValueTypeMetaData *const>{input_schemas.data(),
                                                                                              input_schemas.size()},
                                                  element);
            if (kernel == nullptr || kernel->arity != 2 || !kernel->associative)
            {
                return nullptr;
            }

            if (!time_series_schema_equivalent(kernel->input_schema(0), element) ||
                !time_series_schema_equivalent(kernel->input_schema(1), element) ||
                !time_series_schema_equivalent(kernel->output_schema(), element))
            {
                return nullptr;
            }
            return kernel;
        }

        [[nodiscard]] inline WiringPortRef wire_lifted_reduce_tsl(Wiring &w,
                                                                  const WiredFn &func,
                                                                  const LiftedKernel *kernel,
                                                                  const WiringPortRef &ts)
        {
            if (kernel == nullptr || !kernel->valid())
            {
                throw std::invalid_argument("reduce: lifted fast path requires a valid lifted function");
            }

            const auto *collection = time_series_schema_as<AnyTSL>(ts.schema);
            if (collection == nullptr || collection->fixed_size() == 0)
            {
                throw std::invalid_argument("reduce: lifted fast path requires a fixed-size TSL input");
            }
            const auto *element = time_series_schema_as<AnyTS>(collection->element_ts());
            if (element == nullptr)
            {
                throw std::invalid_argument("reduce: lifted fast path requires scalar TS elements");
            }
            auto &registry = TypeRegistry::instance();

            const auto *input_schema = registry.un_named_tsb({{"ts", ts.schema}});
            const auto *output_schema = element;
            std::array<WiringPortRef, 1> inputs{ts};

            NodeTypeMetaData node_schema;
            node_schema.display_name = "reduce_lifted";
            node_schema.input_schema = input_schema;
            node_schema.output_schema = output_schema;
            node_schema.node_kind = NodeKind::Compute;
            node_schema.valid_inputs = std::vector<std::size_t>{0};

            NodeCallbacks callbacks;
            callbacks.evaluate = [kernel](const NodeView &view, DateTime evaluation_time) {
                auto input = view.input(evaluation_time);
                auto root = input.as_bundle();
                auto ts_field = root.field("ts");
                auto list = ts_field.as_list();

                std::optional<Value> accumulator;
                for (std::size_t i = 0; i < list.size(); ++i)
                {
                    auto item = list[i];
                    if (!item.valid()) { continue; }
                    if (!accumulator.has_value())
                    {
                        accumulator.emplace(item.value());
                        continue;
                    }

                    std::array<ValueView, 2> args{accumulator->view(), item.value()};
                    *accumulator = kernel->eval(std::span<const ValueView>{args.data(), args.size()});
                }
                if (!accumulator.has_value()) { return; }

                auto output = view.output(evaluation_time);
                auto mutation = output.begin_mutation(evaluation_time);
                if (!mutation.move_value_from(std::move(*accumulator)))
                {
                    throw std::logic_error("reduce: lifted fast path failed to move the result");
                }
            };

            NodeBuilder builder = NodeBuilder::native(std::move(node_schema), std::move(callbacks));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));

            WiringPortRef out = w.add_node(std::type_index(typeid(lifted_reduce_tsl_node_tag)),
                                           std::move(builder),
                                           std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                                           Value{func});
            return out;
        }

        inline void resolve_lifted_reduce_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            const LiftedKernel *kernel = lifted_reduce_tsl_kernel(context);
            if (kernel == nullptr) { return; }
            bind_graph_output(resolution, kernel->output_schema());
        }

        /* Cost: O(N) kernel applications per tick (full recompute of the
           fixed TSL fold — no modified gating), O(1) retained. Auto-selected
           over the incremental combiner tree (nested_graphs.rst) for fixed
           sizes, trading per-tick work for zero nested-graph machinery. */
        struct reduce_lifted_tsl
        {
            static constexpr auto name = "reduce_lifted_tsl";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_lifted_reduce_tsl_output(resolution, context);
            }

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return lifted_reduce_tsl_kernel(context) != nullptr;
            }

            static auto compose(Wiring &w,
                                Scalar<"func", WiredFn> func,
                                NamedPort<"ts", TSL<TS<ScalarVar<"T">>>> ts)
            {
                WiringPortRef ts_ref = ts.erased();
                const auto *collection = time_series_schema_as<AnyTSL>(ts_ref.schema);
                if (collection == nullptr)
                {
                    throw std::invalid_argument("reduce: lifted fast path requires a fixed-size TSL input");
                }
                const TSValueTypeMetaData *element = collection->element_ts();
                std::array<const TSValueTypeMetaData *, 2> input_schemas{element, element};
                const LiftedKernel *kernel =
                    resolve_lifted_kernel_for_schemas(func.value(),
                                                      std::span<const TSValueTypeMetaData *const>{
                                                          input_schemas.data(), input_schemas.size()},
                                                      element);
                return wire_lifted_reduce_tsl(w, func.value(), kernel, ts_ref);
            }
        };

        /**
         * Ordered fixed-TSL wiring. Its required live ``zero`` is the initial
         * accumulator/default for invalid positions; this is intentionally
         * distinct from the optional-zero contract of associative ``reduce``.
         */
        [[nodiscard]] inline WiringPortRef reduce_tsl_wire(Wiring &w, const WiredFn &combiner,
                                                           const WiringPortRef &ts, const WiringPortRef &zero,
                                                           bool associative = true)
        {
            if (!combiner.valid())
            {
                throw std::invalid_argument("reduce: 'func' must be a wirable function (fn<X>())");
            }
            if (combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "reduce: the combiner must take (lhs, rhs) time-series inputs and produce an output");
            }

            const TSValueTypeMetaData *schema  = ts.schema;
            const TSValueTypeMetaData *element = schema->element_ts();
            const std::size_t          size    = schema->fixed_size();

            const auto ts_arg = [](WiringPortRef ref) {
                WiringArg arg;
                arg.kind = WiringArg::Kind::TimeSeries;
                arg.port = std::move(ref);
                return arg;
            };

            std::vector<WiringPortRef> elements;
            elements.reserve(size);
            for (std::size_t i = 0; i < size; ++i)
            {
                const std::array<WiringArg, 2> leaf_args{
                    ts_arg(subgraph_wiring_detail::tsl_element_ref(ts, i, element)),
                    ts_arg(zero),
                };
                elements.push_back(
                    wire_operator(w, "default", {leaf_args.data(), leaf_args.size()}, true).output.erased());
            }
            return reduce_layout(w, combiner, std::move(elements), associative);
        }

        [[nodiscard]] inline WiringPortRef wire_reduce_tsd(
            Wiring &w, const Scalar<"func", WiredFn> &func, WiringPortRef ts,
            std::optional<WiringPortRef> zero);

        inline void resolve_reduce_tsl_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at_as<AnyTSL>(context, 1);
            if (schema == nullptr) { return; }
            bind_graph_output(resolution, schema->element_ts(), "V");
        }

        struct reduce_variadic_tsl
        {
            static constexpr auto name = "reduce_variadic_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 2) && context.args[1].from_variadic_tail;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts)
            {
                return wire_reduce_tsd(w, func, ts.erased(), std::nullopt);
            }
        };

        /** ``reduce(func, ts: TSL[V, SIZE]) -> V`` without a default value. */
        struct reduce_tsl
        {
            static constexpr auto name = "reduce_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 2) && !context.args[1].from_variadic_tail;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts)
            {
                return wire_reduce_tsd(w, func, ts.erased(), std::nullopt);
            }
        };

        /**
         * ``reduce(func, ts: TSL[V, SIZE], zero) -> V`` — the explicit-zero
         * arity (Python's third parameter): ``zero`` is a plain value wired as
         * ``const(zero)`` at the element schema.
         */
        struct reduce_tsl_zero
        {
            static constexpr auto name = "reduce_tsl_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 3);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts,
                                         Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const TSValueTypeMetaData *element = ts.erased().schema->element_ts();

                WiringArg zero_arg;
                zero_arg.kind         = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta  = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();

                return wire_reduce_tsd(w, func, ts.erased(), zero);
            }
        };

        /** Ordered fixed-TSL reduction selected explicitly by ``is_associative=false``. */
        struct reduce_ordered_tsl
        {
            static constexpr auto name = "reduce_ordered_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_fixed_tsl(context, 4) &&
                       context.args[2].kind == WiringArg::Kind::TimeSeries &&
                       ordered_reduce_requested(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution) || context.args.size() < 3 ||
                    context.args[2].kind != WiringArg::Kind::TimeSeries)
                {
                    return;
                }
                bind_graph_output(resolution, context.args[2].port.schema, "V");
            }

            static WiringPortRef compose(Wiring &w,
                                         Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"E">>> ts,
                                         NamedPort<"zero", TsVar<"V">> zero,
                                         Scalar<"is_associative", Bool> is_associative)
            {
                if (is_associative.value())
                {
                    throw std::invalid_argument("ordered reduce requires is_associative=false");
                }
                return reduce_tsl_wire(w, func.value(), ts.erased(), zero.erased(), false);
            }
        };
    }  // namespace higher_order_impl_detail

    namespace higher_order_impl_detail
    {
        struct reduce_tsd_node_tag
        {
        };

        struct reduce_ordered_tsd_node_tag
        {
        };

        [[nodiscard]] inline const TSValueTypeMetaData *reduce_collection_element(const WiringPortRef &ts)
        {
            if (const auto *tsd = time_series_schema_as<AnyTSD>(ts.schema)) { return tsd->element_ts(); }
            if (const auto *tsl = time_series_schema_as<AnyTSL>(ts.schema)) { return tsl->element_ts(); }
            return nullptr;
        }

        /**
         * The associative collection-reduce wiring core (see *Nested Graphs >
         * reduce*): compile the binary combiner once, add ONE reduce node whose
         * outer inputs are ``[ts]`` with an optional trailing ``zero``, and
         * whose forwarding output publishes the root aggregate.
         */
        [[nodiscard]] inline WiringPortRef wire_reduce_tsd(
            Wiring &w, const Scalar<"func", WiredFn> &func, WiringPortRef ts,
            std::optional<WiringPortRef> zero)
        {
            const WiredFn &combiner = func.value();
            if (!combiner.valid() || combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "reduce: 'func' must be a wirable (lhs, rhs) -> value function (fn<X>())");
            }

            const auto *element = reduce_collection_element(ts);
            if (element == nullptr)
            {
                throw std::invalid_argument("reduce: the collection input must be a TSD or TSL");
            }
            auto       &registry = TypeRegistry::instance();

            const std::array<const TSValueTypeMetaData *, 2> schemas{element, element};
            CompiledSubGraph combiner_graph = combiner.compile(
                w, {schemas.data(), schemas.size()});
            if (!combiner_graph.captured_inputs.empty())
            {
                throw std::invalid_argument(
                    "reduce: the combiner captured outer ports - outer-port capture is only supported by map_ yet");
            }
            if (combiner_graph.output_schema == nullptr || !combiner_graph.output_binding.has_value())
            {
                throw std::invalid_argument("reduce: the combiner must produce an output");
            }
            if (!time_series_schema_equivalent(registry.dereference(combiner_graph.output_schema),
                                               registry.dereference(element)))
            {
                throw std::invalid_argument(
                    "reduce: the combiner output schema must match the collection's element schema");
            }

            // The empty result aliases one root output. A structural fixed
            // collection has only child sources, so materialise that uncommon
            // zero shape through the standard native pass-through node.
            if (zero.has_value() && zero->is_structural_source())
            {
                *zero = wire<pass_through_node>(w, Port<void>{w, std::move(*zero)}).erased();
            }

            ReduceNodeSpec spec;
            spec.child.graph_builder  = std::move(combiner_graph.graph_builder);
            spec.child.input_bindings = std::move(combiner_graph.input_bindings);
            spec.child.output_binding = combiner_graph.output_binding;
            spec.lifted_kernel = resolve_lifted_kernel_for_schemas(
                combiner, std::span<const TSValueTypeMetaData *const>{schemas.data(), schemas.size()}, element);
            if (spec.lifted_kernel != nullptr &&
                (spec.lifted_kernel->arity != 2 || !spec.lifted_kernel->associative))
            {
                spec.lifted_kernel = nullptr;
            }
            spec.has_zero = zero.has_value();

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> input_fields{
                {"ts", ts.schema}};
            std::vector<WiringPortRef> inputs;
            inputs.reserve(spec.has_zero ? 2 : 1);
            inputs.push_back(std::move(ts));
            if (zero.has_value())
            {
                input_fields.emplace_back("zero", zero->schema);
                inputs.push_back(std::move(*zero));
            }
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(input_fields);
            const auto *output_schema = element;

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            WiringPortRef out = w.add_node(
                std::type_index(typeid(reduce_tsd_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{combiner},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "reduce";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    NodeBuilder builder = reduce_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        [[nodiscard]] inline WiringPortRef wire_ordered_reduce_tsd(
            Wiring &w,
            const Scalar<"func", WiredFn> &func,
            WiringPortRef ts,
            WiringPortRef zero)
        {
            const WiredFn &combiner = func.value();
            if (!combiner.valid() || combiner.arity != 2 || !combiner.has_output)
            {
                throw std::invalid_argument(
                    "ordered reduce: 'func' must be a wirable (accumulator, element) -> accumulator function");
            }

            const auto *tsd_schema = time_series_schema_as<AnyTSD>(ts.schema);
            const auto *tsl_schema = time_series_schema_as<AnyTSL>(ts.schema);
            if ((tsd_schema == nullptr || tsd_schema->key_type() != scalar_descriptor<Int>::value_meta()) &&
                (tsl_schema == nullptr || tsl_schema->fixed_size() != 0))
            {
                throw std::invalid_argument("ordered reduce requires TSD[int, E] or dynamic TSL[E]");
            }
            const auto *element = tsd_schema != nullptr ? tsd_schema->element_ts() : tsl_schema->element_ts();
            const std::array<const TSValueTypeMetaData *, 2> schemas{zero.schema, element};
            CompiledSubGraph combiner_graph = combiner.compile(
                w, {schemas.data(), schemas.size()});
            if (!combiner_graph.captured_inputs.empty())
            {
                throw std::invalid_argument("ordered reduce does not support combiner captures");
            }
            if (combiner_graph.output_schema == nullptr || !combiner_graph.output_binding.has_value())
            {
                throw std::invalid_argument("ordered reduce combiner must produce an output");
            }
            auto &registry = TypeRegistry::instance();
            if (!time_series_schema_equivalent(registry.dereference(combiner_graph.output_schema),
                                               registry.dereference(zero.schema)))
            {
                throw std::invalid_argument(
                    "ordered reduce combiner output schema must match the accumulator/zero schema");
            }

            if (zero.is_structural_source())
            {
                zero = wire<pass_through_node>(w, Port<void>{w, std::move(zero)}).erased();
            }

            OrderedReduceNodeSpec spec;
            spec.child.graph_builder = std::move(combiner_graph.graph_builder);
            spec.child.input_bindings = std::move(combiner_graph.input_bindings);
            spec.child.output_binding = combiner_graph.output_binding;

            const auto *input_schema = registry.un_named_tsb({{"ts", ts.schema}, {"zero", zero.schema}});
            const std::array<WiringPortRef, 2> inputs{std::move(ts), std::move(zero)};

            WiringNodeSchema node_schema;
            node_schema.input = input_schema;
            node_schema.output = inputs[1].schema;

            return w.add_node(
                std::type_index(typeid(reduce_ordered_tsd_node_tag)),
                node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                Value{combiner},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name = "reduce_ordered";
                    meta.input_schema = input_schema;
                    meta.output_schema = inputs[1].schema;

                    NodeBuilder builder = ordered_reduce_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
        }

        inline void resolve_reduce_tsd_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (output_bound(resolution)) { return; }
            const auto *schema = time_series_schema_at_as<AnyTSD>(context, 1);
            if (schema == nullptr) { return; }
            bind_graph_output(resolution, schema->element_ts(), "V");
        }

        [[nodiscard]] inline bool reduce_ts_is_tsd(OperatorCallContext context, std::size_t expected_args)
        {
            if (context.args.size() != expected_args) { return false; }
            return time_series_schema_at_as<AnyTSD>(context, 1) != nullptr;
        }

        [[nodiscard]] inline bool reduce_ts_is_dynamic_tsl(OperatorCallContext context,
                                                            std::size_t expected_args)
        {
            if (context.args.size() != expected_args) { return false; }
            const auto *schema = time_series_schema_at_as<AnyTSL>(context, 1);
            return schema != nullptr && schema->fixed_size() == 0;
        }

        struct reduce_ordered_tsd
        {
            static constexpr auto name = "reduce_ordered_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                if (!reduce_ts_is_tsd(context, 4) ||
                    context.args[2].kind != WiringArg::Kind::TimeSeries ||
                    !ordered_reduce_requested(context))
                {
                    return false;
                }
                const auto *tsd = time_series_schema_at_as<AnyTSD>(context, 1);
                return tsd != nullptr && tsd->key_type() == scalar_descriptor<Int>::value_meta();
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (output_bound(resolution) || context.args.size() < 3 ||
                    context.args[2].kind != WiringArg::Kind::TimeSeries)
                {
                    return;
                }
                bind_graph_output(resolution, context.args[2].port.schema, "V");
            }

            static WiringPortRef compose(
                Wiring &w,
                Scalar<"func", WiredFn> func,
                NamedPort<"ts", TSD<Int, TsVar<"E">>> ts,
                NamedPort<"zero", TsVar<"V">> zero,
                Scalar<"is_associative", Bool> is_associative)
            {
                if (is_associative.value())
                {
                    throw std::invalid_argument("ordered reduce requires is_associative=false");
                }
                return wire_ordered_reduce_tsd(w, func, ts.erased(), zero.erased());
            }
        };

        /** ``reduce(func, ts: TSD[K, V]) -> V`` without an empty value. */
        struct reduce_tsd
        {
            static constexpr auto name = "reduce_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_tsd(context, 2);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsd_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts)
            {
                return wire_reduce_tsd(w, func, ts.erased(), std::nullopt);
            }
        };

        /** ``reduce(func, ts: TSD[K, V], zero) -> V`` — the explicit-zero arity. */
        struct reduce_tsd_zero
        {
            static constexpr auto name = "reduce_tsd_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_tsd(context, 3);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsd_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts,
                                         Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const auto *element =
                    TypeRegistry::instance().dereference(ts.erased().schema)->element_ts();

                WiringArg zero_arg;
                zero_arg.kind         = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta  = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();

                return wire_reduce_tsd(w, func, ts.erased(), zero);
            }
        };

        /** ``reduce(func, ts: TSD[K, V], zero: V) -> V`` with a live TS zero. */
        struct reduce_tsd_ts_zero
        {
            static constexpr auto name = "reduce_tsd_ts_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_tsd(context, 3) &&
                       context.args[2].kind == WiringArg::Kind::TimeSeries;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsd_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSD<ScalarVar<"K">, TsVar<"V">>> ts,
                                         NamedPort<"zero", TsVar<"V">> zero)
            {
                return wire_reduce_tsd(w, func, ts.erased(), zero.erased());
            }
        };

        /** Dynamic TSL associative reduction uses the same in-place tree as TSD. */
        struct reduce_dynamic_tsl
        {
            static constexpr auto name = "reduce_dynamic_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_dynamic_tsl(context, 2);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts)
            {
                return wire_reduce_tsd(w, func, ts.erased(), std::nullopt);
            }
        };

        struct reduce_dynamic_tsl_zero
        {
            static constexpr auto name = "reduce_dynamic_tsl_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_dynamic_tsl(context, 3) &&
                       context.args[2].kind == WiringArg::Kind::Scalar;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts,
                                         Scalar<"zero", ScalarVar<"Z">> zero_value)
            {
                const auto *element = ts.erased().schema->element_ts();
                WiringArg zero_arg;
                zero_arg.kind = WiringArg::Kind::Scalar;
                zero_arg.scalar_value = Value{zero_value.value()};
                zero_arg.scalar_meta = zero_arg.scalar_value.schema();
                const WiringPortRef zero =
                    wire_operator(w, "const", {&zero_arg, 1}, true, element).output.erased();
                return wire_reduce_tsd(w, func, ts.erased(), zero);
            }
        };

        struct reduce_dynamic_tsl_ts_zero
        {
            static constexpr auto name = "reduce_dynamic_tsl_ts_zero";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_dynamic_tsl(context, 3) &&
                       context.args[2].kind == WiringArg::Kind::TimeSeries;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_reduce_tsl_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"V">>> ts,
                                         NamedPort<"zero", TsVar<"V">> zero)
            {
                return wire_reduce_tsd(w, func, ts.erased(), zero.erased());
            }
        };

        struct reduce_ordered_dynamic_tsl
        {
            static constexpr auto name = "reduce_ordered_dynamic_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return reduce_ts_is_dynamic_tsl(context, 4) &&
                       context.args[2].kind == WiringArg::Kind::TimeSeries &&
                       ordered_reduce_requested(context);
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (!output_bound(resolution) && context.args.size() >= 3 &&
                    context.args[2].kind == WiringArg::Kind::TimeSeries)
                {
                    bind_graph_output(resolution, context.args[2].port.schema, "V");
                }
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         NamedPort<"ts", TSL<TsVar<"E">>> ts,
                                         NamedPort<"zero", TsVar<"V">> zero,
                                         Scalar<"is_associative", Bool> is_associative)
            {
                if (is_associative.value())
                {
                    throw std::invalid_argument("ordered reduce requires is_associative=false");
                }
                return wire_ordered_reduce_tsd(w, func, ts.erased(), zero.erased());
            }
        };

        struct switch_node_tag
        {
        };

        struct dispatch_switch_node_tag
        {
        };

        struct dispatch_key_node_tag
        {
        };

        [[nodiscard]] inline const TSValueTypeMetaData *switch_branch_output_child_schema(
            const TSValueTypeMetaData &schema,
            std::size_t index)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    if (index >= schema.field_count())
                    {
                        throw std::out_of_range("switch_: branch output source path is out of range for TSB");
                    }
                    return schema.fields()[index].type;

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        throw std::invalid_argument(
                            "switch_: branch output source path requires fixed-size TSL prefixes");
                    }
                    if (index >= schema.fixed_size())
                    {
                        throw std::out_of_range("switch_: branch output source path is out of range for TSL");
                    }
                    return schema.element_ts();

                default:
                    throw std::invalid_argument(
                        "switch_: branch output source path can only traverse TSB or fixed-size TSL");
            }
        }

        [[nodiscard]] inline std::size_t switch_branch_output_child_count(
            const TSValueTypeMetaData &schema)
        {
            switch (schema.kind)
            {
                case TSTypeKind::TSB:
                    return schema.field_count();

                case TSTypeKind::TSL:
                    if (schema.fixed_size() == 0)
                    {
                        throw std::invalid_argument(
                            "switch_: branch output source path requires fixed-size TSL prefixes");
                    }
                    return schema.fixed_size();

                default:
                    throw std::invalid_argument(
                        "switch_: branch output source path can only traverse TSB or fixed-size TSL");
            }
        }

        [[nodiscard]] inline TSEndpointSchema switch_branch_output_endpoint_schema_for(
            const TSValueTypeMetaData *schema,
            const std::vector<std::size_t> &source_path,
            bool peered_terminal,
            std::size_t depth = 0)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("switch_: branch output binding requires an output schema");
            }
            if (depth == source_path.size())
            {
                return peered_terminal ? TSEndpointSchema::peered(schema) : TSEndpointSchema::owned(schema);
            }

            const auto selected = source_path[depth];
            const auto count    = switch_branch_output_child_count(*schema);
            if (selected >= count)
            {
                throw std::out_of_range("switch_: branch output source path is out of range");
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(count);
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto *child_schema = switch_branch_output_child_schema(*schema, index);
                children.push_back(index == selected
                                       ? switch_branch_output_endpoint_schema_for(
                                             child_schema, source_path, peered_terminal, depth + 1)
                                       : TSEndpointSchema::owned(child_schema));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }

        [[nodiscard]] inline const TSValueTypeMetaData *switch_branch_output_schema_at(
            const TSValueTypeMetaData *schema,
            const std::vector<std::size_t> &source_path)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("switch_: branch output binding requires an output schema");
            }
            for (const std::size_t index : source_path)
            {
                schema = switch_branch_output_child_schema(*schema, index);
            }
            return schema;
        }

        [[nodiscard]] inline bool switch_branch_requires_preserved_terminal(
            const SingleNestedGraphNodeSpec &spec,
            const TSValueTypeMetaData *switch_output_schema)
        {
            if (!spec.output_binding.has_value() ||
                spec.output_binding->kind != NestedGraphOutputBinding::Kind::ChildOutput)
            {
                throw std::logic_error("switch_: every branch must terminate at a child output");
            }

            const NestedGraphEndpoint &source = spec.output_binding->source;
            if (!source.path.empty()) { return true; }

            const NodeBuilder &terminal = spec.graph_builder.nodes().at(source.node);
            const TSEndpointSchema &terminal_override = terminal.output_endpoint();
            const NodeTypeMetaData *terminal_meta = terminal.type().schema();
            const TSEndpointSchema &terminal_declared =
                terminal_meta != nullptr ? terminal_meta->output_endpoint_schema : terminal_override;
            const TSEndpointSchema &terminal_endpoint =
                !terminal_override.empty() ? terminal_override : terminal_declared;
            const auto *terminal_schema =
                terminal_meta != nullptr ? terminal_meta->output_schema : nullptr;
            const auto *branch_output_schema =
                switch_branch_output_schema_at(terminal_schema, source.path);
            if (switch_output_schema != nullptr &&
                switch_output_schema->kind != TSTypeKind::REF &&
                branch_output_schema->kind == TSTypeKind::REF &&
                time_series_schema_equivalent(
                    TypeRegistry::instance().dereference(branch_output_schema),
                    switch_output_schema))
            {
                return true;
            }
            return !terminal_endpoint.empty() &&
                   (terminal_endpoint.is_peered() || terminal_endpoint.is_non_peered());
        }

        inline void configure_switch_branch_output(SingleNestedGraphNodeSpec &spec,
                                                    const TSValueTypeMetaData *switch_output_schema,
                                                    bool preserve_terminal)
        {
            if (!spec.output_binding.has_value() ||
                spec.output_binding->kind != NestedGraphOutputBinding::Kind::ChildOutput)
            {
                throw std::logic_error("switch_: every branch must terminate at a child output");
            }

            const NestedGraphEndpoint &source = spec.output_binding->source;
            NodeBuilder &terminal = spec.graph_builder.node_at(source.node);
            const NodeTypeMetaData *terminal_meta = terminal.type().schema();
            const auto *terminal_schema = terminal_meta != nullptr ? terminal_meta->output_schema : nullptr;
            const auto *branch_output_schema = switch_branch_output_schema_at(terminal_schema, source.path);

            if (preserve_terminal) { return; }

            // When a VALUE branch participates in a REF-shaped switch, its
            // terminal must own the value inside the branch's A/B graph slot.
            // The switch publishes a reference to that terminal. All other
            // branches write directly into the fixed switch output.
            const bool peered_terminal =
                switch_output_schema == nullptr || switch_output_schema->kind != TSTypeKind::REF ||
                branch_output_schema->kind == TSTypeKind::REF;
            terminal.output_endpoint(
                switch_branch_output_endpoint_schema_for(terminal_schema, source.path, peered_terminal));
        }

        /**
         * Compile one branch over the outer time-series slots. ``named_slots``
         * are the keyword arguments as ``(name, outer slot)`` pairs — resolved
         * per branch against ITS parameter names (branches may name and order
         * their parameters differently, like Python). A branch consumes the key
         * only when its first parameter is named ``key``.
         */
        [[nodiscard]] inline SingleNestedGraphNodeSpec compile_switch_branch(
            const WiredFn &branch,
            const WiringPortRef &key_source,
            std::vector<WiringPortRef> &slot_sources,
            std::size_t positional_count,
            std::span<const std::pair<std::string, std::size_t>> named_slots,
            const TSValueTypeMetaData *&output_schema,
            std::optional<bool> &branches_have_output,
            std::vector<ExternalServiceSlot> &external_services,
            Wiring *parent = nullptr)
        {
            if (!branch.valid())
            {
                throw std::invalid_argument("switch_: every branch must be a wirable function (fn<X>())");
            }

            std::vector<std::size_t> positional_slots(positional_count);
            for (std::size_t i = 0; i < positional_count; ++i) { positional_slots[i] = i; }

            const auto bound_slots = bind_wired_fn_args<std::size_t>(
                "switch_", branch, {positional_slots.data(), positional_slots.size()}, named_slots);

            std::vector<WiringPortRef> boundary_shapes;
            boundary_shapes.reserve(branch.arity);
            if (bound_slots.takes_leading_key)
            {
                boundary_shapes.push_back(subgraph_wiring_detail::boundary_shape(
                    key_source, boundary_shapes.size(), {}));
            }
            for (const std::size_t slot : bound_slots.ordered)
            {
                boundary_shapes.push_back(subgraph_wiring_detail::boundary_shape(
                    slot_sources[slot], boundary_shapes.size(), {}));
            }

            const auto shapes = std::span<const WiringPortRef>{
                boundary_shapes.data(), boundary_shapes.size()};
            CompiledSubGraph compiled = parent != nullptr
                                            ? branch.compile(*parent, shapes)
                                            : branch.compile(shapes);
            const std::size_t declared_arity = boundary_shapes.size();
            std::vector<std::size_t> captured_slots;
            captured_slots.reserve(compiled.captured_inputs.size());
            for (const WiringPortRef &captured : compiled.captured_inputs)
            {
                const auto existing = std::find_if(
                    slot_sources.begin(), slot_sources.end(),
                    [&](const WiringPortRef &source) { return source.same_source_as(captured); });
                if (existing != slot_sources.end())
                {
                    captured_slots.push_back(static_cast<std::size_t>(existing - slot_sources.begin()));
                }
                else
                {
                    captured_slots.push_back(slot_sources.size());
                    slot_sources.push_back(captured);
                }
            }
            std::vector<std::size_t> external_slots;
            external_slots.reserve(compiled.external_service_inputs.size());
            for (NestedServiceInput &external : compiled.external_service_inputs)
            {
                external_slots.push_back(external_service_slot(
                    std::move(external), external_services, slot_sources));
            }

            const bool branch_has_output = compiled.output_schema != nullptr;
            const auto *branch_output_schema = compiled.output_schema;
            if (!branches_have_output.has_value()) { branches_have_output = branch_has_output; }
            else if (*branches_have_output != branch_has_output)
            {
                throw std::invalid_argument(
                    "switch_: branches must either all produce an output or all be sinks");
            }
            if (branch_has_output && output_schema == nullptr) { output_schema = branch_output_schema; }
            else if (branch_has_output &&
                     !time_series_schema_equivalent(output_schema, branch_output_schema))
            {
                // Branches may differ only in REF-ness (one produces the
                // value, another the reference - hgraph parity): the switch
                // output takes the REFERENCE shape; value branches adapt at
                // the boundary binding.
                auto &registry = TypeRegistry::instance();
                if (time_series_schema_equivalent(registry.dereference(output_schema),
                                                  registry.dereference(branch_output_schema)))
                {
                    if (branch_output_schema->kind == TSTypeKind::REF) { output_schema = branch_output_schema; }
                }
                else
                {
                    throw std::invalid_argument("switch_: all branches must produce the same output schema");
                }
            }

            SingleNestedGraphNodeSpec spec;
            spec.graph_builder  = std::move(compiled.graph_builder);
            spec.input_bindings = std::move(compiled.input_bindings);
            spec.output_binding = compiled.output_binding;
            // Re-target boundary ordinals onto the outer input root: ordinal 0
            // is the key when the branch consumes it; every other parameter
            // maps through its resolved outer slot (+1 past the key input).
            const std::size_t offset = bound_slots.takes_leading_key ? 1 : 0;
            const auto retarget_boundary_path = [&](std::vector<std::size_t> &path) {
                if (path.empty())
                {
                    throw std::logic_error("switch_: boundary binding path is empty");
                }
                const std::size_t ordinal = path[0];
                if (ordinal >= declared_arity)
                {
                    const std::size_t appended_index = ordinal - declared_arity;
                    if (appended_index < captured_slots.size())
                    {
                        path[0] = 1 + captured_slots[appended_index];
                        return;
                    }
                    const std::size_t external_index = appended_index - captured_slots.size();
                    if (external_index >= external_slots.size())
                    {
                        throw std::logic_error("switch_: appended boundary ordinal is out of range");
                    }
                    path[0] = 1 + external_slots[external_index];
                    return;
                }
                if (bound_slots.takes_leading_key && ordinal == 0)
                {
                    path[0] = 0;
                    return;
                }
                if (ordinal < offset || ordinal - offset >= bound_slots.ordered.size())
                {
                    throw std::logic_error("switch_: boundary binding ordinal is not mapped to an outer input");
                }
                path[0] = 1 + bound_slots.ordered[ordinal - offset];
            };
            for (NestedGraphInputBinding &binding : spec.input_bindings)
            {
                retarget_boundary_path(binding.source_path);
            }
            if (spec.output_binding.has_value() &&
                spec.output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                retarget_boundary_path(spec.output_binding->parent_source_path);

                // Direct boundary returns have no child terminal to re-home
                // into the switch output. Materialise the identity as an
                // ordinary child node so every branch uses the same terminal
                // forwarding protocol.
                ResolutionMap resolution;
                resolution.bind_ts("S", compiled.output_schema);

                NodeBuilder terminal;
                terminal.implementation<pass_through_node>(resolution);

                const auto &source_path = spec.output_binding->parent_source_path;
                const WiringPortRef *terminal_source = nullptr;
                if (source_path[0] == 0)
                {
                    terminal_source = &key_source;
                }
                else
                {
                    const std::size_t slot = source_path[0] - 1;
                    if (slot >= slot_sources.size())
                    {
                        throw std::logic_error("switch_: direct branch output source is outside the input slots");
                    }
                    terminal_source = &slot_sources[slot];
                }
                for (std::size_t path_index = 1;
                     path_index < source_path.size() && terminal_source->is_structural_source();
                     ++path_index)
                {
                    const auto &children = terminal_source->structural_children();
                    if (source_path[path_index] >= children.size())
                    {
                        throw std::logic_error("switch_: direct branch output path is outside a structural source");
                    }
                    terminal_source = &children[source_path[path_index]];
                }

                const NodeTypeMetaData *terminal_meta = terminal.type().schema();
                if (terminal_meta == nullptr || terminal_meta->input_schema == nullptr)
                {
                    throw std::logic_error("switch_: direct branch terminal has no input schema");
                }
                std::vector<TSEndpointSchema> terminal_inputs;
                terminal_inputs.push_back(graph_wiring_detail::endpoint_for_source(
                    compiled.output_schema, *terminal_source));
                terminal.input_endpoint(TSEndpointSchema::non_peered(
                    terminal_meta->input_schema, std::move(terminal_inputs)));

                const std::size_t terminal_index = spec.graph_builder.node_count();
                spec.graph_builder.add_node(std::move(terminal));
                spec.input_bindings.push_back(NestedGraphInputBinding{
                    .source_path = spec.output_binding->parent_source_path,
                    .target = NestedGraphEndpoint{.node = terminal_index, .path = {0}},
                });
                spec.output_binding = NestedGraphOutputBinding{
                    .source = NestedGraphEndpoint{.node = terminal_index},
                };
            }
            return spec;
        }

        /** Add the already-compiled branch set through the one switch runtime. */
        [[nodiscard]] inline WiringPortRef add_compiled_switch(
            Wiring &w, WiringPortRef key, std::vector<WiringPortRef> ts,
            SwitchNodeSpec spec, const TSValueTypeMetaData *output_schema,
            Value config, std::type_index definition, const char *display_name)
        {
            const auto *key_schema = TypeRegistry::instance().dereference(key.schema);
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(1 + ts.size());
            fields.emplace_back("key", key_schema);
            for (std::size_t i = 0; i < ts.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), ts[i].schema);
            }
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            std::vector<WiringPortRef> inputs;
            inputs.reserve(1 + ts.size());
            inputs.push_back(std::move(key));
            for (WiringPortRef &port : ts) { inputs.push_back(std::move(port)); }

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            return w.add_node(
                definition, node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, std::move(config),
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = display_name;
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    NodeBuilder builder = switch_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
        }

        /** The shared switch wiring: compile every branch, then add one switch node. */
        [[nodiscard]] inline WiringPortRef wire_switch(Wiring &w, WiringPortRef key, const SwitchCases &cases,
                                                       std::vector<WiringPortRef> ts,
                                                       std::vector<std::pair<std::string, WiringPortRef>> kwargs,
                                                       bool output_required)
        {
            if (cases.cases.empty() && !cases.default_branch.has_value())
            {
                throw std::invalid_argument("switch_: requires at least one case");
            }

            // Outer time-series slots: positional args, then keyword args in
            // call order. Branches resolve their parameters onto these slots
            // (keywords by each branch's own parameter names).
            const std::size_t positional_count = ts.size();
            WiringPortRef key_boundary = key;
            key_boundary.schema = TypeRegistry::instance().dereference(key.schema);
            std::vector<std::pair<std::string, std::size_t>> named_slots;
            named_slots.reserve(kwargs.size());
            for (std::size_t i = 0; i < kwargs.size(); ++i)
            {
                named_slots.emplace_back(kwargs[i].first, positional_count + i);
                ts.push_back(kwargs[i].second);
            }

            const TSValueTypeMetaData *output_schema = nullptr;
            std::optional<bool>        branches_have_output;
            SwitchNodeSpec             spec;
            std::vector<ExternalServiceSlot> external_services;
            spec.reload_on_ticked = cases.reload_on_ticked;
            spec.branches.reserve(cases.cases.size());
            for (const SwitchCase &entry : cases.cases)
            {
                spec.branches.push_back(SwitchBranch{
                    .key  = entry.key,
                    .spec = compile_switch_branch(entry.branch, key_boundary,
                                                  ts, positional_count,
                                                  {named_slots.data(), named_slots.size()}, output_schema,
                                                  branches_have_output, external_services, &w),
                });
            }
            if (cases.default_branch.has_value())
            {
                spec.default_branch = compile_switch_branch(*cases.default_branch, key_boundary,
                                                            ts,
                                                            positional_count,
                                                            {named_slots.data(), named_slots.size()},
                                                            output_schema, branches_have_output,
                                                            external_services, &w);
            }

            materialize_external_service_slots(w, std::move(external_services), ts);

            if (!branches_have_output.has_value() || *branches_have_output != output_required)
            {
                throw std::invalid_argument(output_required
                                                ? "switch_: every branch must produce an output"
                                                : "switch_sink_: every branch must be a sink");
            }
            if (output_required)
            {
                const auto requires_preserved_terminal = [output_schema](const SingleNestedGraphNodeSpec &branch) {
                    return switch_branch_requires_preserved_terminal(branch, output_schema);
                };
                spec.output_forwards_to_child_terminal =
                    std::any_of(spec.branches.begin(), spec.branches.end(),
                                [&](const SwitchBranch &branch) {
                                    return requires_preserved_terminal(branch.spec);
                                }) ||
                    (spec.default_branch.has_value() &&
                     requires_preserved_terminal(*spec.default_branch));
                for (SwitchBranch &branch : spec.branches)
                {
                    configure_switch_branch_output(
                        branch.spec, output_schema, spec.output_forwards_to_child_terminal);
                }
                if (spec.default_branch.has_value())
                {
                    configure_switch_branch_output(
                        *spec.default_branch, output_schema,
                        spec.output_forwards_to_child_terminal);
                }
            }

            return add_compiled_switch(
                w, std::move(key), std::move(ts), std::move(spec), output_schema,
                Value{cases}, std::type_index(typeid(switch_node_tag)), "switch_");
        }

        [[nodiscard]] inline std::optional<bool> probe_switch_output_mode(
            OperatorCallContext context, const TSValueTypeMetaData *&output_schema)
        {
            const SwitchCases *cases = context.scalar_as<SwitchCases>("cases");
            if (cases == nullptr) { return std::nullopt; }
            const WiredFn *branch = !cases->cases.empty()
                                        ? &cases->cases.front().branch
                                        : (cases->default_branch.has_value() ? &*cases->default_branch : nullptr);
            if (branch == nullptr || context.args.empty() ||
                context.args[0].kind != WiringArg::Kind::TimeSeries)
            {
                return std::nullopt;
            }
            WiringPortRef key_source = context.args[0].port;
            key_source.schema = TypeRegistry::instance().dereference(key_source.schema);

            std::vector<WiringPortRef> slot_sources;
            for (std::size_t i = 2; i < context.args.size(); ++i)
            {
                if (context.args[i].kind != WiringArg::Kind::TimeSeries) { return std::nullopt; }
                slot_sources.push_back(context.args[i].port);
            }
            const std::size_t positional_count = slot_sources.size();
            std::vector<std::pair<std::string, std::size_t>> named_slots;
            for (const auto &[name, kw_arg] : context.kwargs)
            {
                if (kw_arg.kind != WiringArg::Kind::TimeSeries) { continue; }
                named_slots.emplace_back(name, slot_sources.size());
                slot_sources.push_back(kw_arg.port);
            }

            std::vector<std::size_t> positional_slots(positional_count);
            for (std::size_t i = 0; i < positional_count; ++i) { positional_slots[i] = i; }
            (void)bind_wired_fn_args<std::size_t>(
                "switch_", *branch,
                {positional_slots.data(), positional_slots.size()},
                {named_slots.data(), named_slots.size()});

            if (const auto *declared_output = branch->output_schema())
            {
                output_schema = declared_output;
                return true;
            }

            std::optional<bool> branches_have_output;
            std::vector<ExternalServiceSlot> external_services;
            (void)compile_switch_branch(*branch, key_source,
                                        slot_sources, positional_count,
                                        {named_slots.data(), named_slots.size()}, output_schema,
                                        branches_have_output, external_services);
            return branches_have_output;
        }

        /**
         * The output schema is whatever the branches compute — discover it for
         * the resolver by compiling the first branch (a side-effect-free
         * wiring-time computation), unless an explicit output schema already
         * bound ``O``.
         */
        inline void resolve_switch_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }

            const TSValueTypeMetaData *output_schema = nullptr;
            const auto mode = probe_switch_output_mode(context, output_schema);
            if (mode.value_or(false))
            {
                bind_graph_output(resolution, output_schema, "O");
            }
        }

        /** ``switch_(key, cases, *ts, **kwargs)`` — keywords resolve per branch. */
        struct switch_impl
        {
            static constexpr auto name = "switch_impl";

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_switch_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w, NamedPort<"key", TS<ScalarVar<"K">>> key,
                                         Scalar<"cases", SwitchCases> cases, VarIn<"ts", TsVar<"TS">> ts,
                                         VarKwIn<"kwargs"> kwargs)
            {
                return wire_switch(w, key.erased(), cases.value(),
                                   std::vector<WiringPortRef>{ts.begin(), ts.end()},
                                   std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(),
                                                                                      kwargs.end()},
                                   true);
            }
        };

        struct switch_sink_impl
        {
            static constexpr auto name = "switch_sink_impl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                return fallback_on_exception(false, [&] {
                    const TSValueTypeMetaData *output_schema = nullptr;
                    const auto mode = probe_switch_output_mode(context, output_schema);
                    return mode.has_value() && !*mode;
                });
            }

            static void compose(Wiring &w, NamedPort<"key", TS<ScalarVar<"K">>> key,
                                Scalar<"cases", SwitchCases> cases, VarIn<"ts", TsVar<"TS">> ts,
                                VarKwIn<"kwargs"> kwargs)
            {
                (void)wire_switch(w, key.erased(), cases.value(),
                                  std::vector<WiringPortRef>{ts.begin(), ts.end()},
                                  std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(),
                                                                                     kwargs.end()},
                                  false);
            }
        };

        struct try_except_node_tag
        {
        };

        [[nodiscard]] inline const TSValueTypeMetaData *try_except_output_schema(
            const TSValueTypeMetaData *child_output)
        {
            if (child_output == nullptr) { return node_error_ts_meta(); }
            return TypeRegistry::instance().un_named_tsb(
                {{"exception", node_error_ts_meta()}, {"out", child_output}});
        }

        [[nodiscard]] inline CompiledSubGraph compile_try_except_child(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> schemas,
            Wiring *parent = nullptr)
        {
            if (!func.valid())
            {
                throw std::invalid_argument("try_except: 'func' must be a wirable function");
            }
            CompiledSubGraph compiled = parent != nullptr
                                            ? func.compile(*parent, schemas)
                                            : func.compile(schemas);
            if (compiled.output_binding.has_value() != (compiled.output_schema != nullptr))
            {
                throw std::invalid_argument(
                    "try_except: the function output schema and nested output binding must agree");
            }
            return compiled;
        }

        [[nodiscard]] inline WiringPortRef wire_try_except(
            Wiring &w,
            const WiredFn &func,
            std::vector<WiringPortRef> positional,
            std::vector<std::pair<std::string, WiringPortRef>> named,
            ErrorCaptureOptions error_capture)
        {
            auto bound = bind_wired_fn_args<WiringPortRef>(
                "try_except", func, {positional.data(), positional.size()},
                {named.data(), named.size()}, {});

            std::vector<const TSValueTypeMetaData *> schemas;
            schemas.reserve(bound.ordered.size());
            for (const WiringPortRef &port : bound.ordered) { schemas.push_back(port.schema); }

            CompiledSubGraph compiled = compile_try_except_child(
                func, {schemas.data(), schemas.size()}, &w);

            std::vector<WiringPortRef> inputs = std::move(bound.ordered);
            inputs.reserve(inputs.size() + compiled.captured_inputs.size() +
                           compiled.external_service_inputs.size());
            for (WiringPortRef &captured : compiled.captured_inputs)
            {
                inputs.push_back(std::move(captured));
            }
            compiled.captured_inputs.clear();
            for (NestedServiceInput &external : compiled.external_service_inputs)
            {
                inputs.push_back(subgraph_wiring_detail::materialize_external_service_input(
                    w, std::move(external)));
            }
            compiled.external_service_inputs.clear();
            if (compiled.input_schemas.size() != inputs.size())
            {
                throw std::logic_error(
                    "try_except: compiled boundary schema count does not match the outer inputs");
            }

            const TSValueTypeMetaData *input_schema = nullptr;
            if (!compiled.input_schemas.empty())
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(compiled.input_schemas.size());
                for (std::size_t index = 0; index < compiled.input_schemas.size(); ++index)
                {
                    fields.emplace_back(std::to_string(index), compiled.input_schemas[index]);
                }
                input_schema = TypeRegistry::instance().un_named_tsb(fields);
            }

            const bool has_output = compiled.output_schema != nullptr;
            const TSValueTypeMetaData *output_schema = try_except_output_schema(compiled.output_schema);

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            (void)scalar_descriptor<TryExceptCallConfig>::value_meta();
            return w.add_node(
                std::type_index(typeid(try_except_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                Value{TryExceptCallConfig{func, error_capture}}, [&] {
                    NodeTypeMetaData meta;
                    meta.display_name  = "try_except";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;

                    SingleNestedGraphNodeSpec spec;
                    spec.graph_builder  = std::move(compiled.graph_builder);
                    spec.input_bindings = std::move(compiled.input_bindings);
                    if (has_output)
                    {
                        spec.output_binding = std::move(compiled.output_binding);
                        spec.output_binding->target_path = {1};
                    }

                    NodeBuilder builder = try_except_node(
                        std::move(meta), std::move(spec), {}, error_capture);
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
        }

        [[nodiscard]] inline std::optional<const TSValueTypeMetaData *> probe_try_except_output(
            OperatorCallContext context)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return std::nullopt; }
            if (const TSValueTypeMetaData *declared = func->output_schema(); declared != nullptr)
            {
                return try_except_output_schema(declared);
            }
            if (context.args.empty()) { return std::nullopt; }

            std::vector<const TSValueTypeMetaData *> positional;
            for (std::size_t index = 1; index < context.args.size(); ++index)
            {
                if (context.args[index].kind == WiringArg::Kind::TimeSeries)
                {
                    positional.push_back(context.args[index].port.schema);
                }
            }
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> named;
            for (const auto &[name, arg] : context.kwargs)
            {
                if (arg.kind != WiringArg::Kind::TimeSeries) { return std::nullopt; }
                named.emplace_back(name, arg.port.schema);
            }
            auto bound = bind_wired_fn_args<const TSValueTypeMetaData *>(
                "try_except", *func, {positional.data(), positional.size()},
                {named.data(), named.size()}, {});
            CompiledSubGraph compiled = compile_try_except_child(
                *func, {bound.ordered.data(), bound.ordered.size()});
            return try_except_output_schema(compiled.output_schema);
        }

        struct try_except_impl
        {
            static constexpr auto name = "try_except_impl";

            static auto defaults()
            {
                return std::tuple{arg<"__trace_back_depth__">(Int{1}),
                                  arg<"__capture_values__">(Bool{false})};
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                if (resolution.find_ts("__out__") != nullptr) { return; }
                (void)fallback_on_exception(false, [&] {
                    const auto output = probe_try_except_output(context);
                    if (!output.has_value()) { return false; }
                    bind_graph_output(resolution, *output, "O");
                    return true;
                });
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         VarIn<"args", TsVar<"A">> positional,
                                         Scalar<"__trace_back_depth__", Int> trace_back_depth,
                                         Scalar<"__capture_values__", Bool> capture_values,
                                         VarKwIn<"kwargs"> kwargs)
            {
                if (trace_back_depth.value() < 0)
                {
                    throw std::invalid_argument("try_except: trace-back depth must be non-negative");
                }
                return wire_try_except(
                    w, func.value(),
                    std::vector<WiringPortRef>{positional.begin(), positional.end()},
                    std::vector<std::pair<std::string, WiringPortRef>>{kwargs.begin(), kwargs.end()},
                    ErrorCaptureOptions{
                        .trace_back_depth = static_cast<std::size_t>(trace_back_depth.value()),
                        .capture_values = capture_values.value(),
                    });
            }
        };

        struct DispatchSelectionPlan
        {
            static constexpr Int no_match  = Int{-1};
            static constexpr Int ambiguous = Int{-2};

            std::vector<std::vector<const ValueTypeMetaData *>> alternatives{};
            std::vector<Int>                                  selected_case{};
            bool                                              has_default{false};
        };

        [[nodiscard]] inline bool dispatch_case_more_specific(
            const DispatchCase &candidate, const DispatchCase &other)
        {
            bool strict = false;
            for (std::size_t i = 0; i < candidate.types.size(); ++i)
            {
                if (!TypeRegistry::instance().bundle_is_a(candidate.types[i], other.types[i]))
                {
                    return false;
                }
                strict = strict || candidate.types[i] != other.types[i];
            }
            return strict;
        }

        [[nodiscard]] inline const ValueTypeMetaData *dispatch_bundle_schema(
            const TSValueTypeMetaData *schema) noexcept
        {
            const auto *value_schema = TypeRegistry::instance().dereference(schema);
            return value_schema != nullptr && value_schema->kind == TSTypeKind::TS
                       ? value_schema->value_schema
                       : nullptr;
        }

        inline void present_dispatch_values(
            std::span<WiringPortRef> slots,
            std::span<const std::size_t> dispatch_slots)
        {
            auto &registry = TypeRegistry::instance();
            for (const std::size_t slot : dispatch_slots)
            {
                if (slot >= slots.size())
                {
                    throw std::out_of_range("dispatch_: dispatch argument index is out of range");
                }
                slots[slot].schema = registry.dereference(slots[slot].schema);
            }
        }

        [[nodiscard]] inline DispatchSelectionPlan make_dispatch_selection_plan(
            const DispatchCases &cases,
            std::span<const TSValueTypeMetaData *const> slot_schemas)
        {
            if (cases.cases.empty() && !cases.default_branch.has_value())
            {
                throw std::invalid_argument("dispatch_: requires at least one case");
            }
            if (cases.dispatch_args.empty())
            {
                throw std::invalid_argument("dispatch_: at least one dispatch argument is required");
            }

            DispatchSelectionPlan plan;
            plan.has_default = cases.default_branch.has_value();
            plan.alternatives.reserve(cases.dispatch_args.size());

            std::vector<bool> selected_slots(slot_schemas.size(), false);
            std::size_t table_size = 1;
            for (const std::size_t slot : cases.dispatch_args)
            {
                if (slot >= slot_schemas.size())
                {
                    throw std::out_of_range("dispatch_: dispatch argument index is out of range");
                }
                if (selected_slots[slot])
                {
                    throw std::invalid_argument("dispatch_: dispatch argument indexes must be unique");
                }
                selected_slots[slot] = true;

                const auto *declared = dispatch_bundle_schema(slot_schemas[slot]);
                if (declared == nullptr || !declared->is_named_bundle())
                {
                    throw std::invalid_argument(
                        "dispatch_: selected arguments must be scalar TS values over named Bundles");
                }

                auto alternatives = TypeRegistry::instance().bundle_descendants(declared);
                if (alternatives.empty())
                {
                    throw std::invalid_argument(
                        "dispatch_: a selected Bundle has no concrete alternatives");
                }
                if (table_size > std::numeric_limits<std::size_t>::max() / alternatives.size())
                {
                    throw std::length_error("dispatch_: closed alternative product is too large");
                }
                table_size *= alternatives.size();
                plan.alternatives.push_back(std::move(alternatives));
            }

            for (const DispatchCase &entry : cases.cases)
            {
                if (!entry.branch.valid())
                {
                    throw std::invalid_argument(
                        "dispatch_: every case must contain a wirable function");
                }
                if (entry.types.size() != cases.dispatch_args.size())
                {
                    throw std::invalid_argument(
                        "dispatch_: every case must declare one type per dispatch argument");
                }
                for (std::size_t i = 0; i < entry.types.size(); ++i)
                {
                    const auto *target = entry.types[i];
                    const auto *declared = dispatch_bundle_schema(
                        slot_schemas[cases.dispatch_args[i]]);
                    if (target == nullptr || !target->is_named_bundle() ||
                        !TypeRegistry::instance().bundle_is_a(target, declared))
                    {
                        throw std::invalid_argument(
                            "dispatch_: case types must derive from their selected argument type");
                    }
                }
            }
            if (cases.default_branch.has_value() && !cases.default_branch->valid())
            {
                throw std::invalid_argument(
                    "dispatch_: the default case must contain a wirable function");
            }

            const std::size_t case_count = cases.cases.size();
            std::vector<bool> more_specific(case_count * case_count, false);
            for (std::size_t lhs = 0; lhs < case_count; ++lhs)
            {
                for (std::size_t rhs = 0; rhs < case_count; ++rhs)
                {
                    if (lhs != rhs)
                    {
                        more_specific[lhs * case_count + rhs] =
                            dispatch_case_more_specific(cases.cases[lhs], cases.cases[rhs]);
                    }
                }
            }

            plan.selected_case.resize(table_size, DispatchSelectionPlan::no_match);
            std::vector<const ValueTypeMetaData *> actual(cases.dispatch_args.size());
            std::vector<bool> matches(case_count, false);
            for (std::size_t flat = 0; flat < table_size; ++flat)
            {
                std::size_t remaining = flat;
                for (std::size_t i = plan.alternatives.size(); i-- > 0;)
                {
                    const auto &dimension = plan.alternatives[i];
                    actual[i] = dimension[remaining % dimension.size()];
                    remaining /= dimension.size();
                }

                std::fill(matches.begin(), matches.end(), false);
                for (std::size_t c = 0; c < case_count; ++c)
                {
                    matches[c] = true;
                    for (std::size_t i = 0; i < actual.size(); ++i)
                    {
                        if (!TypeRegistry::instance().bundle_is_a(actual[i], cases.cases[c].types[i]))
                        {
                            matches[c] = false;
                            break;
                        }
                    }
                }

                std::size_t winner = case_count;
                std::size_t winner_count = 0;
                for (std::size_t c = 0; c < case_count; ++c)
                {
                    if (!matches[c]) { continue; }
                    bool dominates = true;
                    for (std::size_t other = 0; other < case_count; ++other)
                    {
                        if (c != other && matches[other] &&
                            !more_specific[c * case_count + other])
                        {
                            dominates = false;
                            break;
                        }
                    }
                    if (dominates)
                    {
                        winner = c;
                        ++winner_count;
                    }
                }
                if (winner_count == 1)
                {
                    plan.selected_case[flat] = static_cast<Int>(winner);
                }
                else if (std::ranges::any_of(matches, [](bool value) { return value; }))
                {
                    plan.selected_case[flat] = DispatchSelectionPlan::ambiguous;
                }
            }
            return plan;
        }

        [[nodiscard]] inline WiringPortRef wire_dispatch_key(
            Wiring &w, const DispatchCases &cases,
            std::span<const WiringPortRef> slots)
        {
            std::vector<WiringPortRef> selected_inputs;
            std::vector<const TSValueTypeMetaData *> slot_schemas;
            selected_inputs.reserve(cases.dispatch_args.size());
            slot_schemas.reserve(slots.size());
            for (const WiringPortRef &slot : slots) { slot_schemas.push_back(slot.schema); }
            for (const std::size_t slot : cases.dispatch_args)
            {
                if (slot >= slots.size())
                {
                    throw std::out_of_range("dispatch_: dispatch argument index is out of range");
                }
                selected_inputs.push_back(slots[slot]);
            }

            DispatchSelectionPlan plan = make_dispatch_selection_plan(
                cases, {slot_schemas.data(), slot_schemas.size()});
            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(selected_inputs.size());
            for (std::size_t i = 0; i < selected_inputs.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), selected_inputs[i].schema);
            }
            const auto *input_schema = registry.un_named_tsb(fields);
            const auto *output_schema = registry.ts(scalar_descriptor<Int>::value_meta());

            NodeTypeMetaData node_schema;
            node_schema.display_name = "dispatch_key";
            node_schema.input_schema = input_schema;
            node_schema.output_schema = output_schema;
            node_schema.node_kind = NodeKind::Compute;
            std::vector<std::size_t> required(selected_inputs.size());
            for (std::size_t i = 0; i < required.size(); ++i) { required[i] = i; }
            node_schema.valid_inputs = std::move(required);

            NodeCallbacks callbacks;
            callbacks.evaluate = [plan = std::move(plan)](const NodeView &view,
                                                          DateTime evaluation_time) {
                auto input = view.input(evaluation_time);
                auto bundle = input.as_bundle();
                std::size_t flat = 0;
                for (std::size_t i = 0; i < plan.alternatives.size(); ++i)
                {
                    const auto concrete = bundle[i].value().concrete();
                    const auto *actual = concrete.schema();
                    const auto &dimension = plan.alternatives[i];
                    const auto found = std::ranges::find(dimension, actual);
                    if (found == dimension.end())
                    {
                        throw std::runtime_error(
                            "dispatch_: active Bundle leaf was not visible when the dispatch plan was wired");
                    }
                    flat = flat * dimension.size() +
                           static_cast<std::size_t>(std::distance(dimension.begin(), found));
                }

                const Int selected = plan.selected_case[flat];
                if (selected == DispatchSelectionPlan::ambiguous)
                {
                    throw std::invalid_argument(
                        "Ambiguous dispatch: active Bundle types match multiple incomparable cases");
                }
                if (selected == DispatchSelectionPlan::no_match && !plan.has_default)
                {
                    throw std::runtime_error(
                        "No suitable overload: no dispatch case matches the active Bundle types");
                }

                auto output = view.output(evaluation_time);
                if (!output.valid() || output.value().checked_as<Int>() != selected)
                {
                    auto mutation = output.begin_mutation(evaluation_time);
                    Value value{selected};
                    if (!mutation.move_value_from(std::move(value)))
                    {
                        throw std::logic_error("dispatch_: failed to publish the selected case");
                    }
                }
            };

            NodeBuilder builder = NodeBuilder::native(std::move(node_schema), std::move(callbacks));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema,
                std::span<const WiringPortRef>{selected_inputs.data(), selected_inputs.size()}));
            return w.add_node(
                std::type_index(typeid(dispatch_key_node_tag)), std::move(builder),
                std::span<const WiringPortRef>{selected_inputs.data(), selected_inputs.size()},
                Value{cases});
        }

        [[nodiscard]] inline WiringPortRef wire_checked_downcast(
            Wiring &w, WiringPortRef source, const TSValueTypeMetaData *target)
        {
            WiringArg arg;
            arg.kind = WiringArg::Kind::TimeSeries;
            arg.port = source;
            std::array<WiringArg, 1> args{std::move(arg)};
            ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
                "downcast_",
                std::span<const WiringArg>{args.data(), args.size()}, true, target);
            OperatorWireResult result = resolved.impl->wire(
                w, resolved.map, resolved.args, resolved.kwargs);
            if (!result.has_output)
            {
                throw std::logic_error("dispatch_: checked downcast produced no output");
            }
            return result.output.erased();
        }

        [[nodiscard]] inline SingleNestedGraphNodeSpec compile_dispatch_branch(
            Wiring *parent,
            const WiredFn &branch,
            std::span<const ValueTypeMetaData *const> case_types,
            std::span<const std::size_t> dispatch_slots,
            std::vector<WiringPortRef> &slot_sources,
            std::size_t positional_count,
            std::span<const std::pair<std::string, std::size_t>> named_slots,
            const TSValueTypeMetaData *&output_schema,
            std::vector<ExternalServiceSlot> &external_services)
        {
            if (!branch.valid())
            {
                throw std::invalid_argument(
                    "dispatch_: every branch must be a wirable function (fn<X>())");
            }
            if (!case_types.empty() && case_types.size() != dispatch_slots.size())
            {
                throw std::invalid_argument(
                    "dispatch_: branch type count does not match the selected arguments");
            }

            std::vector<std::size_t> positional_slots(positional_count);
            for (std::size_t i = 0; i < positional_count; ++i) { positional_slots[i] = i; }
            const auto bound_slots = bind_wired_fn_args<std::size_t>(
                "dispatch_", branch,
                {positional_slots.data(), positional_slots.size()}, named_slots, "");

            Wiring child = parent != nullptr ? parent->child_wiring()
                                             : Wiring{WiringKind::SubGraph};
            std::vector<const TSValueTypeMetaData *> boundary_schemas;
            std::vector<WiringPortRef> branch_ports;
            boundary_schemas.reserve(bound_slots.ordered.size());
            branch_ports.reserve(bound_slots.ordered.size());
            for (std::size_t ordinal = 0; ordinal < bound_slots.ordered.size(); ++ordinal)
            {
                const std::size_t slot = bound_slots.ordered[ordinal];
                const auto *schema = slot_sources[slot].schema;
                boundary_schemas.push_back(schema);
                WiringPortRef port = subgraph_wiring_detail::boundary_shape(
                    slot_sources[slot], ordinal, {});
                for (std::size_t selected = 0; selected < dispatch_slots.size(); ++selected)
                {
                    if (dispatch_slots[selected] != slot || case_types.empty()) { continue; }
                    const auto *target = TypeRegistry::instance().ts(case_types[selected]);
                    if (target != schema)
                    {
                        port = wire_checked_downcast(child, std::move(port), target);
                    }
                    break;
                }
                branch_ports.push_back(std::move(port));
            }

            WiringPortRef branch_output = branch.wire(
                child, {branch_ports.data(), branch_ports.size()});
            CompiledSubGraph compiled = std::move(child).finish_subgraph(
                branch_output, std::move(boundary_schemas));
            const std::size_t declared_arity = bound_slots.ordered.size();
            std::vector<std::size_t> captured_slots;
            captured_slots.reserve(compiled.captured_inputs.size());
            for (const WiringPortRef &captured : compiled.captured_inputs)
            {
                const auto existing = std::find_if(
                    slot_sources.begin(), slot_sources.end(),
                    [&](const WiringPortRef &source) { return source.same_source_as(captured); });
                if (existing != slot_sources.end())
                {
                    captured_slots.push_back(static_cast<std::size_t>(existing - slot_sources.begin()));
                }
                else
                {
                    captured_slots.push_back(slot_sources.size());
                    slot_sources.push_back(captured);
                }
            }
            std::vector<std::size_t> external_slots;
            external_slots.reserve(compiled.external_service_inputs.size());
            for (NestedServiceInput &external : compiled.external_service_inputs)
            {
                external_slots.push_back(external_service_slot(
                    std::move(external), external_services, slot_sources));
            }
            if (compiled.output_schema == nullptr)
            {
                throw std::invalid_argument("dispatch_: every branch must produce an output");
            }
            if (output_schema == nullptr) { output_schema = compiled.output_schema; }
            else if (!time_series_schema_equivalent(output_schema, compiled.output_schema))
            {
                auto &registry = TypeRegistry::instance();
                if (time_series_schema_equivalent(registry.dereference(output_schema),
                                                  registry.dereference(compiled.output_schema)))
                {
                    if (compiled.output_schema->kind == TSTypeKind::REF)
                    {
                        output_schema = compiled.output_schema;
                    }
                }
                else
                {
                    throw std::invalid_argument(
                        "dispatch_: all branches must produce the same output schema");
                }
            }

            SingleNestedGraphNodeSpec spec;
            spec.graph_builder = std::move(compiled.graph_builder);
            spec.input_bindings = std::move(compiled.input_bindings);
            spec.output_binding = compiled.output_binding;
            const auto retarget_boundary_path = [&](std::vector<std::size_t> &path) {
                if (path.empty())
                {
                    throw std::logic_error("dispatch_: boundary binding path is empty");
                }
                const std::size_t ordinal = path[0];
                if (ordinal >= declared_arity)
                {
                    const std::size_t appended_index = ordinal - declared_arity;
                    if (appended_index < captured_slots.size())
                    {
                        path[0] = 1 + captured_slots[appended_index];
                        return;
                    }
                    const std::size_t external_index = appended_index - captured_slots.size();
                    if (external_index >= external_slots.size())
                    {
                        throw std::logic_error("dispatch_: appended boundary ordinal is out of range");
                    }
                    path[0] = 1 + external_slots[external_index];
                    return;
                }
                if (ordinal >= bound_slots.ordered.size())
                {
                    throw std::logic_error("dispatch_: boundary binding ordinal is not mapped to an outer input");
                }
                path[0] = 1 + bound_slots.ordered[ordinal];
            };
            for (NestedGraphInputBinding &binding : spec.input_bindings)
            {
                retarget_boundary_path(binding.source_path);
            }
            if (spec.output_binding.has_value() &&
                spec.output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                retarget_boundary_path(spec.output_binding->parent_source_path);
                ResolutionMap resolution;
                resolution.bind_ts("S", compiled.output_schema);
                NodeBuilder terminal;
                terminal.implementation<pass_through_node>(resolution);
                const std::size_t terminal_index = spec.graph_builder.node_count();
                spec.graph_builder.add_node(std::move(terminal));
                spec.input_bindings.push_back(NestedGraphInputBinding{
                    .source_path = spec.output_binding->parent_source_path,
                    .target = NestedGraphEndpoint{.node = terminal_index, .path = {0}},
                });
                spec.output_binding = NestedGraphOutputBinding{
                    .source = NestedGraphEndpoint{.node = terminal_index},
                };
            }
            return spec;
        }

        [[nodiscard]] inline WiringPortRef wire_dispatch(
            Wiring &w, const DispatchCases &cases, std::vector<WiringPortRef> ts,
            std::vector<std::pair<std::string, WiringPortRef>> kwargs)
        {
            const std::size_t positional_count = ts.size();
            std::vector<std::pair<std::string, std::size_t>> named_slots;
            named_slots.reserve(kwargs.size());
            for (std::size_t i = 0; i < kwargs.size(); ++i)
            {
                named_slots.emplace_back(kwargs[i].first, positional_count + i);
                ts.push_back(kwargs[i].second);
            }

            present_dispatch_values(
                {ts.data(), ts.size()},
                {cases.dispatch_args.data(), cases.dispatch_args.size()});

            WiringPortRef key = wire_dispatch_key(
                w, cases, std::span<const WiringPortRef>{ts.data(), ts.size()});

            const TSValueTypeMetaData *output_schema = nullptr;
            SwitchNodeSpec spec;
            std::vector<ExternalServiceSlot> external_services;
            spec.branches.reserve(cases.cases.size());
            for (std::size_t i = 0; i < cases.cases.size(); ++i)
            {
                const DispatchCase &entry = cases.cases[i];
                spec.branches.push_back(SwitchBranch{
                    .key = Value{static_cast<Int>(i)},
                    .spec = compile_dispatch_branch(
                        &w,
                        entry.branch, {entry.types.data(), entry.types.size()},
                        {cases.dispatch_args.data(), cases.dispatch_args.size()},
                        ts, positional_count,
                        {named_slots.data(), named_slots.size()}, output_schema,
                        external_services),
                });
            }
            if (cases.default_branch.has_value())
            {
                spec.default_branch = compile_dispatch_branch(
                    &w,
                    *cases.default_branch, {},
                    {cases.dispatch_args.data(), cases.dispatch_args.size()},
                    ts, positional_count,
                    {named_slots.data(), named_slots.size()}, output_schema,
                    external_services);
            }
            materialize_external_service_slots(w, std::move(external_services), ts);
            spec.output_forwards_to_child_terminal =
                std::any_of(spec.branches.begin(), spec.branches.end(),
                            [output_schema](const SwitchBranch &branch) {
                                return switch_branch_requires_preserved_terminal(
                                    branch.spec, output_schema);
                            }) ||
                (spec.default_branch.has_value() &&
                 switch_branch_requires_preserved_terminal(
                     *spec.default_branch, output_schema));
            for (SwitchBranch &branch : spec.branches)
            {
                configure_switch_branch_output(
                    branch.spec, output_schema, spec.output_forwards_to_child_terminal);
            }
            if (spec.default_branch.has_value())
            {
                configure_switch_branch_output(
                    *spec.default_branch, output_schema,
                    spec.output_forwards_to_child_terminal);
            }
            return add_compiled_switch(
                w, std::move(key), std::move(ts), std::move(spec), output_schema,
                Value{cases}, std::type_index(typeid(dispatch_switch_node_tag)), "dispatch_");
        }

        inline void resolve_dispatch_output(ResolutionMap &resolution,
                                            OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            const DispatchCases *cases = context.scalar_as<DispatchCases>("cases");
            if (cases == nullptr) { return; }

            const DispatchCase *entry = !cases->cases.empty() ? &cases->cases.front() : nullptr;
            const WiredFn *branch = entry != nullptr
                                        ? &entry->branch
                                        : (cases->default_branch.has_value()
                                               ? &*cases->default_branch
                                               : nullptr);
            if (branch == nullptr) { return; }

            std::vector<WiringPortRef> slot_sources;
            for (std::size_t i = 1; i < context.args.size(); ++i)
            {
                if (context.args[i].kind != WiringArg::Kind::TimeSeries) { return; }
                slot_sources.push_back(context.args[i].port);
            }
            const std::size_t positional_count = slot_sources.size();
            std::vector<std::pair<std::string, std::size_t>> named_slots;
            for (const auto &[name, kw_arg] : context.kwargs)
            {
                if (kw_arg.kind != WiringArg::Kind::TimeSeries) { continue; }
                named_slots.emplace_back(name, slot_sources.size());
                slot_sources.push_back(kw_arg.port);
            }

            present_dispatch_values(
                {slot_sources.data(), slot_sources.size()},
                {cases->dispatch_args.data(), cases->dispatch_args.size()});

            (void)fallback_on_exception(false, [&] {
                std::vector<const TSValueTypeMetaData *> slot_schemas;
                slot_schemas.reserve(slot_sources.size());
                for (const WiringPortRef &source : slot_sources) { slot_schemas.push_back(source.schema); }
                (void)make_dispatch_selection_plan(*cases, {slot_schemas.data(), slot_schemas.size()});
                const TSValueTypeMetaData *output_schema = nullptr;
                std::vector<ExternalServiceSlot> external_services;
                const auto types = entry != nullptr
                                       ? std::span<const ValueTypeMetaData *const>{
                                             entry->types.data(), entry->types.size()}
                                       : std::span<const ValueTypeMetaData *const>{};
                (void)compile_dispatch_branch(
                    context.wiring,
                    *branch, types,
                    {cases->dispatch_args.data(), cases->dispatch_args.size()},
                    slot_sources, positional_count,
                    {named_slots.data(), named_slots.size()}, output_schema,
                    external_services);
                bind_graph_output(resolution, output_schema, "O");
                return true;
            });
        }

        struct dispatch_impl
        {
            static constexpr auto name = "dispatch_impl";

            static void resolve_default_types(ResolutionMap &resolution,
                                              OperatorCallContext context)
            {
                resolve_dispatch_output(resolution, context);
            }

            static WiringPortRef compose(Wiring &w,
                                         Scalar<"cases", DispatchCases> cases,
                                         VarIn<"ts", TsVar<"TS">> ts,
                                         VarKwIn<"kwargs"> kwargs)
            {
                return wire_dispatch(
                    w, cases.value(), std::vector<WiringPortRef>{ts.begin(), ts.end()},
                    std::vector<std::pair<std::string, WiringPortRef>>{
                        kwargs.begin(), kwargs.end()});
            }
        };
    }  // namespace higher_order_impl_detail

    namespace higher_order_impl_detail
    {
        struct map_node_tag
        {
        };
        struct mesh_node_tag
        {
        };
        struct lifted_map_tsl_node_tag
        {
        };

        struct dynamic_tsl_map_node_tag
        {};

        /**
         * Classify the ``map_`` time-series arguments against ``func``'s
         * ordered parameters. A whole-time-series type variable multiplexes
         * the first TSD argument and later TSDs with the established key type;
         * a later TSD with a different key type binds whole. A parameter that
         * accepts the concrete TSD always binds it whole, while a parameter
         * accepting only the element multiplexes it. The first multiplexed
         * TSD establishes the key type and later multiplexed TSDs must agree.
         */
        struct MapArgClassification
        {
            std::vector<bool>                        is_multiplexed{};     ///< per ts arg (call order)
            std::vector<bool>                        exclude_from_keys{};  ///< per ts arg: ``no_key`` tag
            std::vector<const TSValueTypeMetaData *> child_schemas{};      ///< per ts arg: element or whole schema
            const ValueTypeMetaData                 *key_meta{nullptr};
        };

        struct MapParameterAcceptance
        {
            bool whole{false};
            bool element{false};
            bool whole_variable{false};
        };

        /** Schema a keyed/dynamic-list container must allocate for one child.
            A synthetic structural adapter remains an implementation detail,
            while an operator-authored REF terminal is the endpoint that the
            container actually forwards to and must therefore be retained. */
        [[nodiscard]] inline const TSValueTypeMetaData *mapped_element_schema(
            const CompiledSubGraph &compiled)
        {
            const auto *logical = compiled.output_schema;
            const auto *terminal = compiled.terminal_output_schema;
            if (logical == nullptr || terminal == nullptr ||
                compiled.terminal_is_structural_adapter ||
                terminal->kind != TSTypeKind::REF)
            {
                return logical;
            }

            auto &registry = TypeRegistry::instance();
            return time_series_schema_equivalent(
                       registry.dereference(logical),
                       registry.dereference(terminal))
                       ? terminal
                       : logical;
        }

        /** Configure a whole child terminal against one public container
            element. A terminal with endpoint topology of its own, including
            the synthetic REF used for composed fixed structures, stays owned
            by the child and the element forwards to it. Only an ordinary
            owned terminal with the exact public schema is re-homed so it can
            write directly into the container element. */
        [[nodiscard]] inline MapOutputBindingMode configure_mapped_child_terminal(
            NodeBuilder &terminal,
            const TSValueTypeMetaData *output_schema,
            const TSValueTypeMetaData *terminal_output_schema,
            std::string_view operation_name)
        {
            const NodeTypeMetaData *terminal_meta = terminal.type().schema();
            const auto *declared_terminal_schema =
                terminal_meta != nullptr ? terminal_meta->output_schema : nullptr;
            if (output_schema == nullptr || terminal_output_schema == nullptr ||
                declared_terminal_schema == nullptr ||
                !time_series_schema_equivalent(
                    declared_terminal_schema, terminal_output_schema))
            {
                throw std::logic_error(fmt::format(
                    "{}: compiled child terminal schema is inconsistent",
                    operation_name));
            }

            auto &registry = TypeRegistry::instance();
            const bool exact_match = time_series_schema_equivalent(
                output_schema, terminal_output_schema);
            if (!exact_match &&
                !time_series_schema_equivalent(
                    registry.dereference(output_schema),
                    registry.dereference(terminal_output_schema)))
            {
                throw std::invalid_argument(fmt::format(
                    "{}: child terminal schema is incompatible with its public output",
                    operation_name));
            }

            const TSEndpointSchema &terminal_override = terminal.output_endpoint();
            const TSEndpointSchema &terminal_declared =
                terminal_meta != nullptr
                    ? terminal_meta->output_endpoint_schema
                    : terminal_override;
            const TSEndpointSchema &terminal_endpoint =
                !terminal_override.empty() ? terminal_override : terminal_declared;
            if (!exact_match ||
                (!terminal_endpoint.empty() &&
                 (terminal_endpoint.is_peered() || terminal_endpoint.is_non_peered())))
            {
                return MapOutputBindingMode::OutputElementForwardsToChildTerminal;
            }

            terminal.output_endpoint(TSEndpointSchema::peered(output_schema));
            return MapOutputBindingMode::ChildTerminalWritesElement;
        }

        /** Determine whether one mapped parameter accepts a TSD whole or its
            element. A bare pattern variable on a user callable denotes a
            whole-time-series parameter; abstract operator markers retain the
            established element-wise default because their variables are
            resolved by overload selection after map has chosen its boundary. */
        [[nodiscard]] inline MapParameterAcceptance map_parameter_acceptance(
            const WiredFn &func, std::size_t parameter,
            const TSValueTypeMetaData *whole, const TSValueTypeMetaData *element)
        {
            MapParameterAcceptance result;
            if (const auto *expected = func.input_schema(parameter))
            {
                result.whole = graph_wiring_detail::input_accepts_output_schema(expected, whole);
                result.element = graph_wiring_detail::input_accepts_output_schema(expected, element);
            }

            if (auto pattern = func.input_pattern(parameter))
            {
                ResolutionMap whole_resolution;
                ResolutionMap element_resolution;
                const bool pattern_accepts_whole =
                    input_ts_pattern_match(*pattern, whole, whole_resolution);
                const bool pattern_accepts_element =
                    input_ts_pattern_match(*pattern, element, element_resolution);
                result.whole_variable =
                    pattern_accepts_whole && pattern_accepts_element &&
                    pattern->kind == TypePattern::Kind::Var && func.operator_name.empty();
                result.element = result.element || pattern_accepts_element;
                result.whole = result.whole ||
                    (pattern_accepts_whole &&
                     (!pattern_accepts_element ||
                      pattern->kind == TypePattern::Kind::TSD ||
                      result.whole_variable));
            }
            else if (func.input_schema(parameter) == nullptr)
            {
                // Unreflective runtime operators and unannotated lambdas keep
                // map_'s long-standing element-wise default.
                result.element = true;
            }
            return result;
        }

        /**
         * Classification honours the wiring-time argument tags:
         * ``pass_through`` forces broadcast whatever the kind; ``no_key``
         * forces a TSD to multiplex but excludes it from key-set inference.
         */
        [[nodiscard]] inline MapArgClassification classify_map_args(
            const WiredFn &func,
            bool takes_leading_key,
            std::span<const TSValueTypeMetaData *const> ts_schemas,
            std::span<const std::uint8_t> arg_tags,
            const ValueTypeMetaData *fallback_key_meta = nullptr,
            bool require_multiplexed_tsd = true,
            std::string_view operation_name = "map_") {
            MapArgClassification result;
            result.is_multiplexed.reserve(ts_schemas.size());
            result.exclude_from_keys.reserve(ts_schemas.size());
            result.child_schemas.reserve(ts_schemas.size());
            bool seen_tsd = false;
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                const auto tag = i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i])
                                                     : WiringPortRef::ArgTag::None;
                const auto *tsd = time_series_schema_as<AnyTSD>(ts_schemas[i]);
                const bool first_tsd = tsd != nullptr && !seen_tsd;
                seen_tsd = seen_tsd || tsd != nullptr;
                const auto parameter = i + (takes_leading_key ? 1 : 0);
                const auto acceptance = tsd != nullptr
                                            ? map_parameter_acceptance(
                                                  func, parameter, ts_schemas[i], tsd->element_ts())
                                            : MapParameterAcceptance{};
                const bool force_multiplex = tag == WiringPortRef::ArgTag::NoKey;
                const bool generic_multiplex =
                    acceptance.whole_variable &&
                    (first_tsd ||
                     (result.key_meta != nullptr && tsd->key_type() == result.key_meta));
                const bool is_multiplexed =
                    tsd != nullptr && tag != WiringPortRef::ArgTag::PassThrough &&
                    (force_multiplex || generic_multiplex || !acceptance.whole);
                if (is_multiplexed)
                {
                    if (!acceptance.element)
                    {
                        throw std::invalid_argument(fmt::format(
                            "{}: a multiplexed TSD value is incompatible with the mapped function parameter",
                            operation_name));
                    }
                    if (result.key_meta == nullptr) { result.key_meta = tsd->key_type(); }
                    else if (tsd->key_type() != result.key_meta)
                    {
                        throw std::invalid_argument(fmt::format(
                            "{}: every multiplexed TSD must share the same key type",
                            operation_name));
                    }
                    result.is_multiplexed.push_back(true);
                    result.exclude_from_keys.push_back(force_multiplex);
                    result.child_schemas.push_back(tsd->element_ts());
                }
                else
                {
                    if (tag == WiringPortRef::ArgTag::NoKey && require_multiplexed_tsd)
                    {
                        throw std::invalid_argument(fmt::format(
                            "{}: 'no_key' applies to multiplexed TSD inputs only",
                            operation_name));
                    }
                    result.is_multiplexed.push_back(false);
                    result.exclude_from_keys.push_back(false);
                    result.child_schemas.push_back(ts_schemas[i]);
                }
            }
            if (result.key_meta == nullptr)
            {
                // A key-only map (an explicit __keys__ with no multiplexed
                // dictionaries) takes its key type from the key set.
                result.key_meta = fallback_key_meta;
            }
            if (result.key_meta == nullptr && require_multiplexed_tsd)
            {
                throw std::invalid_argument(fmt::format(
                    "{}: at least one input must be a multiplexed TSD (or supply an explicit '__keys__')",
                    operation_name));
            }
            return result;
        }

        /** Resolve and validate the lifecycle key set shared by keyed map and
            mesh nodes. An explicit ``__keys__`` wins; otherwise only the
            classified multiplexed inputs participate. ``pass_through`` inputs
            are absent from that list, while ``no_key`` inputs carry the
            classifier's ``exclude_from_keys`` bit and are skipped here. */
        [[nodiscard]] inline WiringPortRef wire_keyed_lifecycle_keys(
            Wiring &w,
            std::optional<WiringPortRef> explicit_keys,
            std::span<const std::size_t> multiplexed_inputs,
            const MapArgClassification &classified,
            std::span<const WiringPortRef> ordered,
            std::string_view operation_name)
        {
            WiringPortRef keys;
            if (explicit_keys.has_value())
            {
                keys = std::move(*explicit_keys);
            }
            else
            {
                std::vector<WiringArg> key_set_args;
                key_set_args.reserve(multiplexed_inputs.size());
                for (const std::size_t mux_index : multiplexed_inputs)
                {
                    if (classified.exclude_from_keys[mux_index]) { continue; }
                    WiringArg key_set_arg;
                    key_set_arg.kind = WiringArg::Kind::TimeSeries;
                    key_set_arg.port = subgraph_wiring_detail::tsd_key_set_ref(ordered[mux_index]);
                    key_set_args.push_back(std::move(key_set_arg));
                }
                if (key_set_args.empty())
                {
                    throw std::invalid_argument(fmt::format(
                        "{}: every multiplexed input is no_key — supply an explicit '__keys__'",
                        operation_name));
                }
                keys = key_set_args.size() == 1
                           ? key_set_args.front().port
                           : wire_operator(
                                 w, "union", {key_set_args.data(), key_set_args.size()}, true)
                                 .output.erased();
            }

            auto &registry = TypeRegistry::instance();
            const auto *actual_keys_schema = registry.dereference(keys.schema);
            const auto *expected_keys_schema = registry.tss(classified.key_meta);
            if (actual_keys_schema != expected_keys_schema)
            {
                throw std::invalid_argument(fmt::format(
                    "{}: '__keys__' must be a TSS of the mapped key type (got '{}', expected '{}')",
                    operation_name,
                    actual_keys_schema != nullptr ? actual_keys_schema->name()
                                                  : std::string_view{"<unbound>"},
                    expected_keys_schema != nullptr ? expected_keys_schema->name()
                                                    : std::string_view{"<unbound>"}));
            }
            return keys;
        }

        /**
         * Compile the ``map_`` child template over the classified time-series
         * args, deriving the boundary-arg source table and, for an output
         * function, the ``TSD<K, OUT>`` output schema. The caller has already
         * resolved name-based key consumption, so this helper sees a schema
         * count that either matches ``func`` directly or has one extra slot
         * for the key.
         */
        [[nodiscard]] inline MapNodeSpec compile_map_child(const WiredFn &func,
                                                           std::span<const TSValueTypeMetaData *const> ts_schemas,
                                                           std::span<const std::uint8_t> arg_tags,
                                                           const TSValueTypeMetaData *&output_schema,
                                                           std::vector<WiringPortRef> *captured = nullptr,
                                                           const ValueTypeMetaData *fallback_key_meta = nullptr,
                                                           std::vector<NestedServiceInput> *external_services = nullptr,
                                                           Wiring *parent = nullptr,
                                                           std::string_view operation_name = "map_")
        {
            if (!func.valid())
            {
                throw std::invalid_argument(fmt::format(
                    "{}: 'func' must be a wirable function (fn<X>())", operation_name));
            }

            const std::size_t base_arity = ts_schemas.size();
            const bool        takes_key  = !func.variadic && func.arity == base_arity + 1;
            if (!takes_key && (func.variadic ? base_arity < func.arity : func.arity != base_arity))
            {
                throw std::invalid_argument(fmt::format(
                    "{}: 'func' must take one parameter per time-series argument, with an optional key "
                    "already resolved by name",
                    operation_name));
            }

            auto &registry = TypeRegistry::instance();
            const MapArgClassification classified = classify_map_args(
                func, takes_key, ts_schemas, arg_tags, fallback_key_meta, true,
                operation_name);

            const auto *key_ts = registry.ts(classified.key_meta);

            std::vector<const TSValueTypeMetaData *> schemas;
            schemas.reserve(base_arity + (takes_key ? 1 : 0));
            if (takes_key) { schemas.push_back(key_ts); }
            schemas.insert(schemas.end(), classified.child_schemas.begin(), classified.child_schemas.end());

            const auto child_schemas = std::span<const TSValueTypeMetaData *const>{
                schemas.data(), schemas.size()};
            CompiledSubGraph compiled = parent != nullptr
                                            ? func.compile(*parent, child_schemas)
                                            : func.compile(child_schemas);
            const bool child_has_output = compiled.output_schema != nullptr;
            if (compiled.output_binding.has_value() != child_has_output)
            {
                throw std::invalid_argument(fmt::format(
                    "{}: the function output schema and nested output binding must agree",
                    operation_name));
            }
            if (child_has_output)
            {
                const auto *element_schema = compiled.output_schema;
                if (const auto *declared = func.output_schema(); declared != nullptr)
                {
                    const bool exact_match = time_series_schema_equivalent(
                        declared, compiled.output_schema);
                    const bool ref_transparent_match =
                        time_series_schema_equivalent(
                            registry.dereference(declared),
                            registry.dereference(compiled.output_schema));
                    if (!exact_match && !ref_transparent_match)
                    {
                        throw std::invalid_argument(fmt::format(
                            "{}: the function's wired output is incompatible with its declared output schema",
                            operation_name));
                    }
                    if (exact_match || declared->kind == TSTypeKind::REF)
                    {
                        // A REF produced by an actual operator such as
                        // switch_ remains the child's public output identity.
                        element_schema = declared;
                    }
                }
                if (const auto *physical = mapped_element_schema(compiled);
                    physical != compiled.output_schema)
                {
                    element_schema = physical;
                }
                // TSD / dynamic-TSL elements embed since the storage-stability
                // ruling (938a125): slot-backed TSData is construct-only in
                // stable slots, so container children never relocate.
                output_schema = registry.tsd(classified.key_meta, element_schema);
            }
            else
            {
                output_schema = nullptr;
            }

            MapNodeSpec spec;
            spec.child.graph_builder  = std::move(compiled.graph_builder);
            spec.child.input_bindings = std::move(compiled.input_bindings);
            spec.child.output_binding = compiled.output_binding;
            spec.key_output_schema    = takes_key ? key_ts : nullptr;

            if (child_has_output &&
                spec.child.output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                spec.output_binding_mode = MapOutputBindingMode::OutputElementForwardsToParentSource;
            }
            else if (child_has_output)
            {
                if (!spec.child.output_binding->source.path.empty())
                {
                    // A projected child terminal is a field/element of the
                    // source node, so its root endpoint cannot be re-homed as
                    // the map element. Keep the child topology intact and
                    // forward the map element to the walked terminal instead.
                    spec.output_binding_mode = MapOutputBindingMode::OutputElementForwardsToChildTerminal;
                }
                else
                {
                    // The design intent: every key has a REAL element instantiated in
                    // the parent's owned TSD output, and the child's terminal node
                    // WRITES THROUGH to it — its output is re-homed as a forwarding
                    // endpoint that the map node points at the parent element. No copy.
                    NodeBuilder &terminal =
                        spec.child.graph_builder.node_at(spec.child.output_binding->source.node);
                    const auto *out = output_schema->element_ts();
                    spec.output_binding_mode = configure_mapped_child_terminal(
                        terminal, out, compiled.terminal_output_schema,
                        operation_name);
                }
            }

            spec.args.reserve(func.arity);
            if (takes_key) { spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Key}); }
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                if (classified.is_multiplexed[i])
                {
                    spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Element, .outer_index = i});
                    spec.multiplexed_inputs.push_back(i);
                }
                else
                {
                    spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::OuterInput, .outer_index = i});
                }
            }
            // Outer-port CAPTURES (nested_graphs.rst): the compose body
            // referenced enclosing-wiring ports. Each becomes an extra
            // pass-through (broadcast) input appended after the declared
            // arguments; the caller wires the captured ports.
            if (!compiled.captured_inputs.empty())
            {
                if (captured == nullptr)
                {
                    throw std::invalid_argument(
                        "the child graph captured outer ports but the caller did not provide a capture sink");
                }
                for (std::size_t i = 0; i < compiled.captured_inputs.size(); ++i)
                {
                    spec.args.push_back(
                        MapArgSource{.kind = MapArgSourceKind::OuterInput, .outer_index = ts_schemas.size() + i});
                }
                *captured = std::move(compiled.captured_inputs);
            }
            for (std::size_t i = 0; i < compiled.external_service_inputs.size(); ++i)
            {
                spec.args.push_back(MapArgSource{
                    .kind        = MapArgSourceKind::OuterInput,
                    .outer_index = ts_schemas.size() +
                                   (captured != nullptr ? captured->size() : compiled.captured_inputs.size()) + i,
                });
            }
            if (external_services != nullptr)
            {
                *external_services = std::move(compiled.external_service_inputs);
            }
            return spec;
        }

        /** Key meta from an explicit ``__keys__`` (a TSS port): its element. */
        [[nodiscard]] inline const ValueTypeMetaData *keys_kwarg_element(OperatorCallContext context)
        {
            const auto element_of = [](const WiringPortRef &port) -> const ValueTypeMetaData * {
                const auto *schema = time_series_schema_as<AnyTSS>(port.schema);
                if (schema != nullptr && schema->value_schema != nullptr)
                {
                    return schema->value_schema->element_type;
                }
                return nullptr;
            };
            for (const auto &[name, kw_arg] : context.kwargs)
            {
                if (name == "__keys__" && kw_arg.kind == WiringArg::Kind::TimeSeries)
                {
                    return element_of(kw_arg.port);
                }
            }
            for (const WiringArg &arg : context.args)
            {
                if (arg.name == "__keys__" && arg.kind == WiringArg::Kind::TimeSeries)
                {
                    return element_of(arg.port);
                }
            }
            return nullptr;
        }

        [[nodiscard]] inline std::optional<const TSValueTypeMetaData *> try_resolve_map_output_schema(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> ts_schemas,
            std::span<const std::uint8_t> arg_tags,
            const ValueTypeMetaData *fallback_key_meta = nullptr,
            std::string_view operation_name = "map_")
        {
            // OperatorRegistry already probes each overload behind an exception
            // boundary and records what() as that candidate's rejection reason.
            // Let classification/compilation failures reach that boundary so an
            // invalid map call retains its actionable diagnostic.
            const TSValueTypeMetaData *output_schema = nullptr;
            std::vector<WiringPortRef> captured;
            std::vector<NestedServiceInput> external_services;
            (void)compile_map_child(func, ts_schemas, arg_tags, output_schema, &captured,
                                    fallback_key_meta, &external_services, nullptr,
                                    operation_name);
            if (output_schema == nullptr) { return std::optional<const TSValueTypeMetaData *>{}; }
            return std::optional<const TSValueTypeMetaData *>{output_schema};
        }

        [[nodiscard]] inline std::optional<bool> try_resolve_map_output_mode(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> ts_schemas,
            std::span<const std::uint8_t> arg_tags,
            const ValueTypeMetaData *fallback_key_meta = nullptr)
        {
            const TSValueTypeMetaData *output_schema = nullptr;
            std::vector<WiringPortRef> captured;
            std::vector<NestedServiceInput> external_services;
            (void)compile_map_child(func, ts_schemas, arg_tags, output_schema, &captured,
                                    fallback_key_meta, &external_services);
            return std::optional<bool>{output_schema != nullptr};
        }

        /**
         * The shared map wiring over the **func-parameter-ordered** time-series
         * list (positional + keyword arguments already resolved onto the
         * function's parameters): compile the child template, add one map node.
         */
        [[nodiscard]] inline WiringPortRef wire_map(Wiring &w, const Scalar<"func", WiredFn> &func,
                                                    std::string_view key_arg,
                                                    std::vector<WiringPortRef> ordered,
                                                    std::optional<WiringPortRef> keys,
                                                    bool output_required)
        {
            std::vector<const TSValueTypeMetaData *> ts_schemas;
            std::vector<std::uint8_t>                arg_tags;
            ts_schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered)
            {
                ts_schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            const TSValueTypeMetaData *output_schema = nullptr;
            const ValueTypeMetaData *explicit_key_meta = nullptr;
            if (keys.has_value())
            {
                const auto *keys_schema = time_series_schema_as<AnyTSS>(keys->schema);
                if (keys_schema != nullptr && keys_schema->value_schema != nullptr)
                {
                    explicit_key_meta = keys_schema->value_schema->element_type;
                }
            }
            const bool takes_key =
                !func.value().variadic && func.value().arity == ts_schemas.size() + 1;
            const MapArgClassification classified = classify_map_args(
                func.value(), takes_key,
                {ts_schemas.data(), ts_schemas.size()}, {arg_tags.data(), arg_tags.size()},
                explicit_key_meta);
            std::vector<WiringPortRef> captured;
            std::vector<NestedServiceInput> external_services;
            MapNodeSpec spec = compile_map_child(func.value(), {ts_schemas.data(), ts_schemas.size()},
                                                 {arg_tags.data(), arg_tags.size()}, output_schema, &captured,
                                                 explicit_key_meta, &external_services, &w);
            if ((output_schema != nullptr) != output_required)
            {
                throw std::invalid_argument(output_required
                                                ? "map_: 'func' must produce an output"
                                                : "map_sink_: 'func' must be a sink");
            }
            // Captured outer ports join the node's inputs as PASS-THROUGH
            // broadcasts after the declared arguments (spec.args already
            // references these positions).
            for (WiringPortRef &outer : captured)
            {
                ts_schemas.push_back(outer.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(WiringPortRef::ArgTag::PassThrough));
                ordered.push_back(std::move(outer));
            }
            append_external_service_inputs(
                w, std::move(external_services), ts_schemas, arg_tags, ordered);

            // The lifecycle key set: an explicit ``__keys__`` when supplied,
            // else derived from the multiplexed dicts — ``keys_(tsd)`` for
            // one, ``union(keys_(tsd)...)`` for several (the Python wiring:
            // ``__keys__ = union(*key_sets)``); ``no_key``-tagged dicts are
            // excluded from the inference. The runtime is always keys-driven;
            // there is no in-node union scan.
            auto &registry = TypeRegistry::instance();
            const auto passive_materializers = materialize_structural_broadcast_inputs(
                w, ordered, ts_schemas,
                {spec.multiplexed_inputs.data(),
                 spec.multiplexed_inputs.size()});
            WiringPortRef lifecycle_keys = wire_keyed_lifecycle_keys(
                w, std::move(keys),
                {spec.multiplexed_inputs.data(), spec.multiplexed_inputs.size()},
                classified, {ordered.data(), ordered.size()}, "map_");
            spec.keys_input_index = ordered.size();

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(ts_schemas.size() + 1);
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                const auto *input_schema =
                    i < classified.is_multiplexed.size() && classified.is_multiplexed[i]
                        ? registry.dereference(ts_schemas[i])
                        : ts_schemas[i];
                fields.emplace_back(std::to_string(i), input_schema);
            }
            fields.emplace_back("__keys__", lifecycle_keys.schema);
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            std::vector<WiringPortRef> inputs = std::move(ordered);
            inputs.push_back(std::move(lifecycle_keys));

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            // The call configuration joins the interning identity: equal
            // inputs with a different function, key-arg name, or argument
            // tags must not dedup to one node. Unlike SwitchCases (a declared
            // operator parameter), MapCallConfig is built here, so register
            // the scalar at the point of use.
            (void)scalar_descriptor<MapCallConfig>::value_meta();
            const auto input_refs = higher_order_input_refs(
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                {passive_materializers.data(), passive_materializers.size()});
            WiringPortRef out = w.add_node(
                std::type_index(typeid(map_node_tag)), node_schema,
                std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
                Value{MapCallConfig{func.value(), Str{key_arg}, Str{}, arg_tags}},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "map_";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;
                    configure_passive_transport_inputs(
                        meta, std::span<const WiringPortRef>{inputs.data(), inputs.size()});

                    NodeBuilder builder = map_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        // The mesh wiring-time scope (the enclosing mesh a mesh_(func)[k] resolves to)
        // lives on the global wiring singleton ``OperatorRegistry`` — see its
        // push_mesh_scope / pop_mesh_scope / resolve_mesh_scope. wire_mesh pushes it
        // around the child compile; the child compiles in a fresh Wiring, so the scope
        // cannot live on a Wiring instance (and the build is single-threaded, so it is a
        // plain global, not a thread-local).
        struct mesh_subscribe_node_tag
        {
        };

        struct mesh_key_set_node_tag
        {
        };

        /**
         * ``mesh_`` wiring. Mirrors ``wire_map``: compile the child, derive the key set
         * (``__keys__`` or the union of multiplexed key sets), then add one mesh
         * node. The mesh node owns the same ``TSD<K, OUT>`` output as ``map_``; with
         * no internal ``mesh_(func)[k]`` requests it is observably ``map_``. The
         * spec additionally always carries a per-instance ``TS<K>`` key output
         * (read by the self-context once cross-instance access lands).
         */
        [[nodiscard]] inline WiringPortRef wire_mesh(Wiring &w, const Scalar<"func", WiredFn> &func,
                                                     std::string_view key_arg,
                                                     std::string_view mesh_name,
                                                     std::vector<WiringPortRef> ordered,
                                                     std::optional<WiringPortRef> keys = std::nullopt)
        {
            std::vector<const TSValueTypeMetaData *> ts_schemas;
            std::vector<std::uint8_t>                arg_tags;
            ts_schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered)
            {
                ts_schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            // A declared element type enables mesh self-reference during child
            // compilation. An erased function may instead become concrete only
            // after wiring (for example, a generic service client); those
            // functions are compiled without a mesh scope and infer the element
            // from the resulting TSD schema.
            const TSValueTypeMetaData *element_schema = func.value().output_schema();
            const ValueTypeMetaData *explicit_key_meta = nullptr;
            if (keys.has_value())
            {
                const auto *keys_schema = time_series_schema_as<AnyTSS>(keys->schema);
                if (keys_schema != nullptr && keys_schema->value_schema != nullptr)
                {
                    explicit_key_meta = keys_schema->value_schema->element_type;
                }
            }
            const bool takes_key =
                !func.value().variadic && func.value().arity == ts_schemas.size() + 1;
            const MapArgClassification classified = classify_map_args(
                func.value(), takes_key,
                {ts_schemas.data(), ts_schemas.size()}, {arg_tags.data(), arg_tags.size()},
                explicit_key_meta, true, "mesh_");

            const TSValueTypeMetaData *output_schema = nullptr;
            MapNodeSpec                map_spec;
            std::vector<WiringPortRef> captured;
            std::vector<NestedServiceInput> external_services;
            if (element_schema != nullptr)
            {
                OperatorRegistry::instance().push_mesh_scope(
                    element_schema, classified.key_meta, std::string{mesh_name});
                auto pop = make_scope_exit([] noexcept { OperatorRegistry::instance().pop_mesh_scope(); });
                map_spec = compile_map_child(func.value(), {ts_schemas.data(), ts_schemas.size()},
                                             {arg_tags.data(), arg_tags.size()}, output_schema, &captured,
                                             explicit_key_meta, &external_services, &w, "mesh_");
            }
            else
            {
                map_spec = compile_map_child(func.value(), {ts_schemas.data(), ts_schemas.size()},
                                             {arg_tags.data(), arg_tags.size()}, output_schema, &captured,
                                             explicit_key_meta, &external_services, &w, "mesh_");
                const auto *inferred = time_series_schema_as<AnyTSD>(output_schema);
                if (inferred == nullptr)
                {
                    throw std::invalid_argument(
                        "mesh_: 'func' output type could not be inferred during child wiring");
                }
                element_schema = inferred->element_ts();
            }

            for (WiringPortRef &outer : captured)
            {
                ts_schemas.push_back(outer.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(WiringPortRef::ArgTag::PassThrough));
                ordered.push_back(std::move(outer));
            }
            append_external_service_inputs(
                w, std::move(external_services), ts_schemas, arg_tags, ordered);

            auto &registry = TypeRegistry::instance();

            const auto passive_materializers = materialize_structural_broadcast_inputs(
                w, ordered, ts_schemas,
                {map_spec.multiplexed_inputs.data(),
                 map_spec.multiplexed_inputs.size()});

            WiringPortRef lifecycle_keys = wire_keyed_lifecycle_keys(
                w, std::move(keys),
                {map_spec.multiplexed_inputs.data(), map_spec.multiplexed_inputs.size()},
                classified, {ordered.data(), ordered.size()}, "mesh_");

            // Transcribe the map child spec into a mesh spec (a superset).
            MeshNodeSpec spec;
            spec.child              = std::move(map_spec.child);
            spec.args               = std::move(map_spec.args);
            spec.multiplexed_inputs = std::move(map_spec.multiplexed_inputs);
            spec.keys_input_index   = ordered.size();
            spec.output_binding_mode = map_spec.output_binding_mode;
            // Every mesh instance owns a TS<K> key output, independent of whether
            // func takes a key. mesh_subscribe reads the current requester key from
            // the enclosing mesh evaluation context.
            spec.key_output_schema  = registry.ts(output_schema->key_type());

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(ts_schemas.size() + 1);
            for (std::size_t i = 0; i < ts_schemas.size(); ++i)
            {
                const auto *input_schema =
                    i < classified.is_multiplexed.size() && classified.is_multiplexed[i]
                        ? registry.dereference(ts_schemas[i])
                        : ts_schemas[i];
                fields.emplace_back(std::to_string(i), input_schema);
            }
            fields.emplace_back("__keys__", lifecycle_keys.schema);
            const auto *input_schema = registry.un_named_tsb(fields);

            std::vector<WiringPortRef> inputs = std::move(ordered);
            inputs.push_back(std::move(lifecycle_keys));

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            (void)scalar_descriptor<MapCallConfig>::value_meta();
            const auto input_refs = higher_order_input_refs(
                std::span<const WiringPortRef>{inputs.data(), inputs.size()},
                {passive_materializers.data(), passive_materializers.size()});
            WiringPortRef out = w.add_node(
                std::type_index(typeid(mesh_node_tag)), node_schema,
                std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
                Value{MapCallConfig{func.value(), Str{key_arg}, Str{mesh_name}, arg_tags}},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "mesh_";
                    meta.input_schema  = input_schema;
                    meta.output_schema = output_schema;
                    configure_passive_transport_inputs(
                        meta, std::span<const WiringPortRef>{inputs.data(), inputs.size()});

                    NodeBuilder builder = mesh_node(std::move(meta), std::move(spec));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
            return out;
        }

        // ``mesh_(func)[k]`` access — wires a mesh_subscribe node that reads the sibling
        // instance ``[k]``. The output type comes from the enclosing mesh scope (resolved
        // innermost, or by name). The node takes ``item`` (the requested key) and returns
        // the mesh element type; at runtime it forwards ``self[item]``, pausing until that
        // sibling has produced its result this cycle.
        [[nodiscard]] inline WiringPortRef mesh_ref_erased(Wiring &w, const WiringPortRef &key,
                                                           const WiringPortRef &value_placeholder,
                                                           std::string_view name = {})
        {
            const TSValueTypeMetaData *out_schema = OperatorRegistry::instance().resolve_mesh_scope(name);
            if (out_schema == nullptr)
            {
                throw std::logic_error(
                    "mesh_(func)[k] used outside a mesh scope (no enclosing mesh is being wired)");
            }

            // {item, value}: ``item`` is the requested key (wired); ``value`` starts at a
            // never-ticking ``nothing<OUT>`` placeholder and is rebound at runtime to
            // self[item] (forwards it and makes the node reactive to the sibling's ticks).
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields{
                {"item", key.schema}, {"value", out_schema}};
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = out_schema;

            std::array<WiringPortRef, 2> inputs{key, value_placeholder};
            return w.add_node(
                std::type_index(typeid(mesh_subscribe_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "mesh_subscribe";
                    meta.input_schema  = input_schema;
                    meta.output_schema = out_schema;
                    NodeBuilder builder = mesh_subscribe_node(std::move(meta));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
        }

        // ``mesh_keys_ref`` access — wires a node that forwards the enclosing mesh's
        // live output key set (``self.key_set()``) without copying it.
        [[nodiscard]] inline WiringPortRef mesh_key_set_ref_erased(
            Wiring &w,
            const WiringPortRef &value_placeholder,
            const ValueTypeMetaData *expected_key_type,
            std::string_view name = {})
        {
            const ValueTypeMetaData *key_type = OperatorRegistry::instance().resolve_mesh_key_scope(name);
            if (key_type == nullptr)
            {
                throw std::logic_error(
                    "mesh_keys_ref used outside a mesh scope (no enclosing mesh is being wired)");
            }
            if (expected_key_type != nullptr && key_type != expected_key_type)
            {
                throw std::invalid_argument("mesh_keys_ref key type does not match the enclosing mesh key type");
            }

            const auto *out_schema = TypeRegistry::instance().tss(key_type);
            if (TypeRegistry::instance().dereference(value_placeholder.schema) != out_schema)
            {
                throw std::invalid_argument("mesh_keys_ref placeholder schema does not match the mesh key set");
            }

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields{{"value", out_schema}};
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = out_schema;

            std::array<WiringPortRef, 1> inputs{value_placeholder};
            return w.add_node(
                std::type_index(typeid(mesh_key_set_node_tag)), node_schema,
                std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{Str{name}},
                [&]() {
                    NodeTypeMetaData meta;
                    meta.display_name  = "mesh_key_set";
                    meta.input_schema  = input_schema;
                    meta.output_schema = out_schema;
                    NodeBuilder builder = mesh_key_set_node(std::move(meta));
                    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                        input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                    return builder;
                });
        }

        struct OrderedMapSchemas
        {
            std::vector<const TSValueTypeMetaData *> schemas{};
            std::vector<std::uint8_t>               arg_tags{};   ///< per schema: WiringPortRef::ArgTag
            bool                                    takes_key{false};
        };

        [[nodiscard]] inline std::optional<OrderedMapSchemas> ordered_map_schemas(OperatorCallContext context,
                                                                                  std::string_view default_key_arg)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return std::nullopt; }

            // Normalized args place map_'s own scalars before the variadic
            // function arguments. Plain values in that tail are promoted to
            // const sources during composition; use the same natural TS schema
            // here so output probing sees the complete function signature.
            std::vector<WiringPortRef> positional;
            const std::size_t tail_start = context.params.empty() ? 0 : context.params.size() - 1;
            for (std::size_t index = tail_start; index < context.args.size(); ++index)
            {
                const WiringArg &argument = context.args[index];
                if (argument.kind == WiringArg::Kind::TimeSeries)
                {
                    positional.push_back(argument.port);
                }
                else if (const auto *schema = argument.scalar_value.schema())
                {
                    positional.push_back(
                        WiringPortRef::null_source(TypeRegistry::instance().ts(schema)));
                }
                else { return std::nullopt; }
            }
            std::vector<std::pair<std::string, WiringPortRef>> named;
            named.reserve(context.kwargs.size());
            for (const auto &[name, kw_arg] : context.kwargs)
            {
                if (name == "__keys__") { continue; }   // map_'s own argument, not func's
                if (kw_arg.kind == WiringArg::Kind::TimeSeries)
                {
                    named.emplace_back(name, kw_arg.port);
                }
                else if (const auto *schema = kw_arg.scalar_value.schema())
                {
                    named.emplace_back(
                        name, WiringPortRef::null_source(TypeRegistry::instance().ts(schema)));
                }
                else { return std::nullopt; }
            }

            const std::string *key_override = context.scalar_as<Str>("__key_arg__");

            try
            {
                auto bound = bind_wired_fn_args<WiringPortRef>(
                    "map_", *func, {positional.data(), positional.size()}, {named.data(), named.size()},
                    key_override != nullptr ? std::string_view{*key_override} : default_key_arg);

                OrderedMapSchemas ordered;
                ordered.takes_key = bound.takes_leading_key;
                ordered.schemas.reserve(bound.ordered.size());
                ordered.arg_tags.reserve(bound.ordered.size());
                for (const WiringPortRef &ref : bound.ordered)
                {
                    ordered.schemas.push_back(ref.schema);
                    ordered.arg_tags.push_back(static_cast<std::uint8_t>(ref.arg_tag));
                }
                return ordered;
            }
            catch (const std::invalid_argument &)
            {
                return std::nullopt;
            }
        }

        /** Bind the output var ``O`` for the resolver: ``TSD<K, OUT(func)>``. */
        inline void resolve_map_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }
            auto ordered = ordered_map_schemas(context, "key");
            if (!ordered.has_value()) { return; }

            // A graph's declared return schema is only its public contract.
            // Its compiled terminal can be transparently compatible but more
            // precise, most notably REF[TSB] from a keyed TSD lookup exposed as
            // TSB.  Compile the child before allocating the map output so the
            // outer TSD retains that terminal schema instead of incorrectly
            // allocating owned TSB storage.
            auto output_schema = try_resolve_map_output_schema(
                *func, {ordered->schemas.data(), ordered->schemas.size()},
                {ordered->arg_tags.data(), ordered->arg_tags.size()}, keys_kwarg_element(context));
            if (!output_schema.has_value()) { return; }
            bind_graph_output(resolution, *output_schema, "O");
        }

        /** The first genuinely multiplexed collection decides the map kernel. */
        [[nodiscard]] inline const TSValueTypeMetaData *first_map_collection(
            OperatorCallContext context, std::string_view operation_name = "map_")
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return nullptr; }
            auto ordered = ordered_map_schemas(context, "key");
            if (!ordered.has_value()) { ordered = ordered_map_schemas(context, "ndx"); }
            if (!ordered.has_value()) { return nullptr; }
            const MapArgClassification classified = classify_map_args(
                *func, ordered->takes_key,
                {ordered->schemas.data(), ordered->schemas.size()},
                {ordered->arg_tags.data(), ordered->arg_tags.size()},
                keys_kwarg_element(context), false, operation_name);
            for (std::size_t i = 0; i < ordered->schemas.size(); ++i)
            {
                const auto tag = i < ordered->arg_tags.size()
                                     ? static_cast<WiringPortRef::ArgTag>(ordered->arg_tags[i])
                                     : WiringPortRef::ArgTag::None;
                if (tag == WiringPortRef::ArgTag::PassThrough)
                {
                    continue;   // pass_through never selects the kernel
                }
                if (const auto *schema = time_series_schema_as<AnyTSD>(ordered->schemas[i]))
                {
                    if (!classified.is_multiplexed[i]) { continue; }
                    return schema;
                }
                if (const auto *schema = time_series_schema_as<AnyTSL>(ordered->schemas[i]))
                {
                    return schema;
                }
            }
            return nullptr;
        }

        /**
         * ``map_(func, *args, **kwargs)`` over TSDs — keyed runtime children,
         * the Python shape: no fixed anchor parameter; positional + keyword
         * arguments resolve onto ``func``'s parameters. TSDs whose parameters
         * accept their elements multiplex (union key set). A whole-time-series
         * type variable multiplexes the first TSD or a later same-key TSD and
         * binds a later different-key TSD directly. Concrete whole-TSD
         * parameters and all other time-series shapes bind directly.
         */
        /**
         * Split the ``__keys__`` special out of the collected kwargs — it is
         * an argument of ``map_`` itself (the explicit demultiplexing key
         * set), not of ``func``.
         */
        [[nodiscard]] inline std::optional<WiringPortRef> split_keys_kwarg(
            std::vector<std::pair<std::string, WiringPortRef>> &named)
        {
            for (auto it = named.begin(); it != named.end(); ++it)
            {
                if (it->first == "__keys__")
                {
                    std::optional<WiringPortRef> keys{std::move(it->second)};
                    named.erase(it);
                    return keys;
                }
            }
            return std::nullopt;
        }

        struct map_impl_tsd
        {
            static constexpr auto name = "map_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *collection = first_map_collection(context);
                if (collection != nullptr) { return time_series_schema_as<AnyTSD>(collection) != nullptr; }
                // A KEY-ONLY map: no multiplexed collections, an explicit
                // __keys__ supplies the key set (and the key type).
                return keys_kwarg_element(context) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_map_output(resolution, context);
            }

            static auto defaults()
            {
                return std::tuple{arg<"__key_arg__">(Str{"key"})};
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         VarIn<"args", TsVar<"B">> positional,
                                         Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                std::optional<WiringPortRef> keys = split_keys_kwarg(named);
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                return wire_map(w, func, key_arg.value(), std::move(bound.ordered), std::move(keys), true);
            }
        };

        struct map_sink_impl_tsd
        {
            static constexpr auto name = "map_sink_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *collection = first_map_collection(context);
                const bool tsd_shape = collection != nullptr
                                           ? time_series_schema_as<AnyTSD>(collection) != nullptr
                                           : keys_kwarg_element(context) != nullptr;
                if (!tsd_shape) { return false; }

                const WiredFn *func = context.scalar_as<WiredFn>("func");
                if (func == nullptr) { return false; }
                auto ordered = ordered_map_schemas(context, "key");
                if (!ordered.has_value()) { return false; }
                const auto mode = try_resolve_map_output_mode(
                    *func, {ordered->schemas.data(), ordered->schemas.size()},
                    {ordered->arg_tags.data(), ordered->arg_tags.size()}, keys_kwarg_element(context));
                return mode.has_value() && !*mode;
            }

            static auto defaults()
            {
                return std::tuple{arg<"__key_arg__">(Str{"key"})};
            }

            static void compose(Wiring &w, Scalar<"func", WiredFn> func,
                                VarIn<"args", TsVar<"B">> positional,
                                Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                std::optional<WiringPortRef> keys = split_keys_kwarg(named);
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                (void)wire_map(w, func, key_arg.value(), std::move(bound.ordered), std::move(keys), false);
            }
        };

        /**
         * ``mesh_(func, *args, **kwargs)`` over TSDs. Same call shape as
         * ``map_impl_tsd``; lowers to a mesh node via ``wire_mesh``. With no
         * cross-instance ``mesh_(func)[k]`` requests this is observably ``map_``.
         */
        // mesh output resolution: prefer the declared function output because a
        // mesh_(func)[k] in the body needs the scope established by wire_mesh.
        // When the output is erased, try the same child inference as map_; that
        // supports functions which become concrete while wiring (for example a
        // generic service client). A function using mesh self-reference without
        // a declared output still fails this probe and remains unresolved.
        inline void resolve_mesh_output(ResolutionMap &resolution, OperatorCallContext context)
        {
            if (resolution.find_ts("__out__") != nullptr) { return; }
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }
            auto ordered = ordered_map_schemas(context, "key");
            if (!ordered.has_value()) { return; }
            const TSValueTypeMetaData *element = func->output_schema();
            if (element == nullptr)
            {
                const auto inferred = try_resolve_map_output_schema(
                    *func, {ordered->schemas.data(), ordered->schemas.size()},
                    {ordered->arg_tags.data(), ordered->arg_tags.size()},
                    keys_kwarg_element(context), "mesh_");
                if (inferred.has_value()) { bind_graph_output(resolution, *inferred, "O"); }
                return;
            }
            const MapArgClassification classified =
                classify_map_args(*func, ordered->takes_key,
                                  {ordered->schemas.data(), ordered->schemas.size()},
                                  {ordered->arg_tags.data(), ordered->arg_tags.size()},
                                  keys_kwarg_element(context), true, "mesh_");
            const auto *output_schema = TypeRegistry::instance().tsd(classified.key_meta, element);
            bind_graph_output(resolution, output_schema, "O");
        }

        struct mesh_impl_tsd
        {
            static constexpr auto name = "mesh_tsd";

            static bool requires_(const ResolutionMap &, OperatorCallContext context)
            {
                const auto *collection = first_map_collection(context, "mesh_");
                return collection != nullptr
                           ? time_series_schema_as<AnyTSD>(collection) != nullptr
                           : keys_kwarg_element(context) != nullptr;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context)
            {
                resolve_mesh_output(resolution, context);
            }

            static auto defaults()
            {
                return std::tuple{arg<"__key_arg__">(Str{"key"}), arg<"__name__">(Str{""})};
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func,
                                         VarIn<"args", TsVar<"B">> positional,
                                         Scalar<"__key_arg__", Str> key_arg,
                                         Scalar<"__name__", Str> mesh_name,
                                         VarKwIn<"kwargs"> kwargs)
            {
                const std::vector<WiringPortRef> pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                std::optional<WiringPortRef> keys = split_keys_kwarg(named);
                auto bound = bind_wired_fn_args<WiringPortRef>("mesh_", func.value(),
                                                               {pos.data(), pos.size()},
                                                               {named.data(), named.size()},
                                                               key_arg.value());
                return wire_mesh(w, func, key_arg.value(), mesh_name.value(),
                                 std::move(bound.ordered), std::move(keys));
            }
        };
        /**
         * ``map_`` over a fixed-size TSL is a **wiring-time expansion** —
         * Python's ``_map_no_index``: one inline application of ``func`` per
         * index (key = ``const(i)`` when ``func`` takes it first), assembled
         * into a structural TSL output. No runtime node is involved.
         */
        /** A tail arg multiplexes in the TSL form when it is a fixed TSL of the SAME size. */
        [[nodiscard]] inline bool tsl_arg_is_multiplexed(const TSValueTypeMetaData *schema, std::size_t size)
        {
            const auto *tsl = time_series_schema_as<AnyTSL>(schema);
            return tsl != nullptr && tsl->fixed_size() == size;
        }

        struct LiftedMapTslPlan
        {
            const LiftedKernel *kernel{nullptr};
            const TSValueTypeMetaData *output_schema{nullptr};
            std::vector<bool> multiplexed{};
            std::vector<std::uint8_t> arg_tags{};
            std::size_t size{0};
            bool dynamic{false};
        };

        [[nodiscard]] inline std::optional<LiftedMapTslPlan> lifted_map_tsl_plan(
            const WiredFn &func,
            std::span<const TSValueTypeMetaData *const> schemas,
            std::span<const std::uint8_t> arg_tags,
            bool takes_key)
        {
            if (takes_key || !func.has_output ||
                (func.variadic ? schemas.size() < func.arity : func.arity != schemas.size()))
            {
                return std::nullopt;
            }

            std::size_t size = 0;
            bool found_collection = false;
            for (std::size_t i = 0; i < schemas.size(); ++i)
            {
                const auto tag = i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i])
                                                     : WiringPortRef::ArgTag::None;
                if (tag == WiringPortRef::ArgTag::NoKey) { return std::nullopt; }
                if (tag == WiringPortRef::ArgTag::PassThrough) { continue; }
                const auto *schema = time_series_schema_as<AnyTSL>(schemas[i]);
                if (schema != nullptr)
                {
                    size = schema->fixed_size();
                    found_collection = true;
                    break;
                }
            }
            if (!found_collection) { return std::nullopt; }

            LiftedMapTslPlan plan;
            plan.size = size;
            plan.dynamic = size == 0;
            plan.multiplexed.reserve(schemas.size());
            plan.arg_tags.assign(arg_tags.begin(), arg_tags.end());

            std::vector<const TSValueTypeMetaData *> expected_schemas;
            expected_schemas.reserve(schemas.size());
            for (std::size_t i = 0; i < schemas.size(); ++i)
            {
                const auto tag = i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i])
                                                     : WiringPortRef::ArgTag::None;
                const bool multiplexed = tag != WiringPortRef::ArgTag::PassThrough &&
                                         tsl_arg_is_multiplexed(schemas[i], size);
                const TSValueTypeMetaData *expected =
                    multiplexed ? time_series_schema_as<AnyTSL>(schemas[i])->element_ts() : schemas[i];
                expected_schemas.push_back(expected);
                plan.multiplexed.push_back(multiplexed);
            }

            const LiftedKernel *kernel =
                resolve_lifted_kernel_for_schemas(func,
                                                  std::span<const TSValueTypeMetaData *const>{expected_schemas.data(),
                                                                                              expected_schemas.size()});
            if (kernel == nullptr || kernel->arity != schemas.size()) { return std::nullopt; }

            for (std::size_t i = 0; i < expected_schemas.size(); ++i)
            {
                const TSValueTypeMetaData *expected = expected_schemas[i];
                if (!time_series_schema_equivalent(kernel->input_schema(i), expected))
                {
                    return std::nullopt;
                }
            }
            plan.kernel = kernel;
            auto &registry = TypeRegistry::instance();
            plan.output_schema = registry.tsl(kernel->output_schema(), size);
            return plan;
        }

        [[nodiscard]] inline std::optional<LiftedMapTslPlan> lifted_map_tsl_plan(OperatorCallContext context)
        {
            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return std::nullopt; }
            auto ordered = ordered_map_schemas(context, "ndx");
            if (!ordered.has_value()) { return std::nullopt; }
            return lifted_map_tsl_plan(*func,
                                       {ordered->schemas.data(), ordered->schemas.size()},
                                       {ordered->arg_tags.data(), ordered->arg_tags.size()},
                                       ordered->takes_key);
        }

        [[nodiscard]] inline WiringPortRef wire_lifted_map_tsl(Wiring &w,
                                                               const WiredFn &func,
                                                               std::string_view key_arg,
                                                               std::vector<WiringPortRef> ordered)
        {
            std::vector<const TSValueTypeMetaData *> schemas;
            std::vector<std::uint8_t> arg_tags;
            schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered)
            {
                schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            std::optional<LiftedMapTslPlan> plan =
                lifted_map_tsl_plan(func,
                                    {schemas.data(), schemas.size()},
                                    {arg_tags.data(), arg_tags.size()},
                                    /*takes_key=*/false);
            if (!plan.has_value())
            {
                throw std::invalid_argument("map_: lifted TSL fast path requires a compatible lifted scalar function");
            }

            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(schemas.size());
            for (std::size_t i = 0; i < schemas.size(); ++i)
            {
                fields.emplace_back(std::to_string(i), schemas[i]);
            }
            const auto *input_schema = registry.un_named_tsb(fields);

            NodeTypeMetaData node_schema;
            node_schema.display_name = "map_lifted_tsl";
            node_schema.input_schema = input_schema;
            node_schema.output_schema = plan->output_schema;
            node_schema.node_kind = NodeKind::Compute;

            const LiftedKernel *kernel = plan->kernel;
            std::vector<bool> multiplexed = plan->multiplexed;
            const std::size_t size = plan->size;
            const bool dynamic = plan->dynamic;
            NodeCallbacks callbacks;
            callbacks.evaluate =
                [kernel, multiplexed = std::move(multiplexed), size, dynamic](const NodeView &view,
                                                                              DateTime evaluation_time) {
                    auto input_root = view.input(evaluation_time);
                    auto bundle = input_root.as_bundle();
                    auto output_root = view.output(evaluation_time);
                    auto output = output_root.as_list();

                    std::size_t runtime_size = size;
                    if (dynamic)
                    {
                        for (std::size_t arg = 0; arg < multiplexed.size(); ++arg)
                        {
                            if (!multiplexed[arg]) { continue; }
                            auto input = bundle[arg];
                            runtime_size = std::max(runtime_size, input.as_list().size());
                        }
                    }

                    // Scratch hoisted out of the element loop: one reusable
                    // buffer instead of an allocation per element per tick on
                    // the fast path (audit 2026-08-15).
                    std::vector<ValueView> values;
                    values.reserve(multiplexed.size());
                    for (std::size_t i = 0; i < runtime_size; ++i)
                    {
                        values.clear();
                        bool ready = true;
                        bool input_modified = false;
                        for (std::size_t arg = 0; arg < multiplexed.size(); ++arg)
                        {
                            auto input = bundle[arg];
                            if (multiplexed[arg])
                            {
                                auto list = input.as_list();
                                if (i >= list.size())
                                {
                                    ready = false;
                                    break;
                                }
                                auto item = list[i];
                                if (!item.valid())
                                {
                                    ready = false;
                                    break;
                                }
                                input_modified = input_modified || item.modified();
                                values.emplace_back(item.value());
                            }
                            else
                            {
                                if (!input.valid())
                                {
                                    ready = false;
                                    break;
                                }
                                input_modified = input_modified || input.modified();
                                values.emplace_back(input.value());
                            }
                        }
                        if (!ready) { continue; }

                        const bool output_valid = i < output.size() && output[i].valid();
                        if (!input_modified && output_valid) { continue; }

                        Value result = kernel->eval(std::span<const ValueView>{values.data(), values.size()});
                        auto output_item = output[i];
                        auto mutation = output_item.begin_mutation(evaluation_time);
                        if (!mutation.move_value_from(std::move(result)))
                        {
                            throw std::logic_error("map_: lifted TSL fast path failed to move an element result");
                        }
                    }
                };

            NodeBuilder builder = NodeBuilder::native(std::move(node_schema), std::move(callbacks));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{ordered.data(), ordered.size()}));

            (void)scalar_descriptor<MapCallConfig>::value_meta();
            WiringPortRef out = w.add_node(std::type_index(typeid(lifted_map_tsl_node_tag)), std::move(builder),
                                           std::span<const WiringPortRef>{ordered.data(), ordered.size()},
                                           Value{MapCallConfig{func, Str{key_arg}, Str{}, arg_tags}});
            return out;
        }

        [[nodiscard]] inline WiringPortRef wire_fixed_map_tsl(Wiring &w, const WiredFn &func, bool takes_key,
                                                              std::vector<WiringPortRef> ordered, bool output_required) {
            auto &registry = TypeRegistry::instance();

            // The first fixed TSL anchors the size; every same-size fixed TSL
            // multiplexes per index, the rest broadcast whole.
            std::size_t size = 0;
            for (const WiringPortRef &port : ordered) {
                if (port.arg_tag == WiringPortRef::ArgTag::NoKey) {
                    throw std::invalid_argument("map_: 'no_key' applies to TSD maps only");
                }
                if (port.arg_tag == WiringPortRef::ArgTag::PassThrough) { continue; }
                const auto *schema = time_series_schema_as<AnyTSL>(port.schema);
                if (schema != nullptr && schema->fixed_size() > 0) {
                    size = schema->fixed_size();
                    break;
                }
            }
            if (size == 0) { throw std::invalid_argument("map_: at least one input must be a fixed-size TSL"); }
            if (func.has_output != output_required) {
                throw std::invalid_argument(output_required ? "map_: 'func' must produce an output"
                                                            : "map_sink_: 'func' must be a sink");
            }

            const auto *key_meta = scalar_descriptor<Int>::value_meta();
            const auto *key_ts   = registry.ts(key_meta);

            std::vector<WiringPortRef> children;
            if (output_required) { children.reserve(size); }
            for (std::size_t i = 0; i < size; ++i) {
                std::vector<WiringPortRef> args;
                args.reserve(func.arity);
                if (takes_key) {
                    WiringArg key_arg;
                    key_arg.kind         = WiringArg::Kind::Scalar;
                    key_arg.scalar_value = Value{static_cast<Int>(i)};
                    key_arg.scalar_meta  = key_meta;
                    args.push_back(wire_operator(w, "const", {&key_arg, 1}, true, key_ts).output.erased());
                }
                for (const WiringPortRef &tail : ordered) {
                    if (tail.arg_tag != WiringPortRef::ArgTag::PassThrough && tsl_arg_is_multiplexed(tail.schema, size)) {
                        const auto *tail_element = time_series_schema_as<AnyTSL>(tail.schema)->element_ts();
                        args.push_back(subgraph_wiring_detail::tsl_element_ref(tail, i, tail_element));
                    } else {
                        args.push_back(tail);
                    }
                }

                WiringPortRef out = func.wire(w, {args.data(), args.size()});
                if (!output_required) { continue; }
                if (out.schema == nullptr) { throw std::invalid_argument("map_: 'func' must produce an output"); }
                if (!children.empty() && !time_series_schema_equivalent(registry.dereference(out.schema),
                                                                        registry.dereference(children.front().schema))) {
                    throw std::invalid_argument("map_: 'func' produced differing output schemas across the TSL indices");
                }
                children.push_back(std::move(out));
            }

            if (!output_required) { return {}; }
            const auto *output_schema = registry.tsl(children.front().schema, size);
            return WiringPortRef::structural_source(output_schema, std::move(children));
        }

        [[nodiscard]] inline TslMapNodeSpec compile_dynamic_tsl_map_child(const WiredFn &func, bool takes_index,
                                                                          std::span<const TSValueTypeMetaData *const> ts_schemas,
                                                                          std::span<const std::uint8_t>               arg_tags,
                                                                          const TSValueTypeMetaData                 *&output_schema,
                                                                          std::vector<WiringPortRef>                 &captured,
                                                                          std::vector<NestedServiceInput>            &external_services,
                                                                          Wiring *parent = nullptr) {
            if (!func.valid()) { throw std::invalid_argument("map_: 'func' must be a wirable function (fn<X>())"); }

            auto       &registry = TypeRegistry::instance();
            const auto *index_ts = registry.ts(scalar_descriptor<Int>::value_meta());

            std::vector<bool>                        multiplexed;
            std::vector<const TSValueTypeMetaData *> child_schemas;
            multiplexed.reserve(ts_schemas.size());
            child_schemas.reserve(ts_schemas.size() + (takes_index ? 1 : 0));
            if (takes_index) { child_schemas.push_back(index_ts); }

            bool found_dynamic_tsl = false;
            for (std::size_t i = 0; i < ts_schemas.size(); ++i) {
                const auto tag =
                    i < arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(arg_tags[i]) : WiringPortRef::ArgTag::None;
                if (tag == WiringPortRef::ArgTag::NoKey) { throw std::invalid_argument("map_: 'no_key' applies to TSD maps only"); }
                const auto *tsl            = time_series_schema_as<AnyTSL>(ts_schemas[i]);
                const bool  is_multiplexed = tag != WiringPortRef::ArgTag::PassThrough && tsl != nullptr && tsl->fixed_size() == 0;
                multiplexed.push_back(is_multiplexed);
                child_schemas.push_back(is_multiplexed ? tsl->element_ts() : ts_schemas[i]);
                found_dynamic_tsl = found_dynamic_tsl || is_multiplexed;
            }
            if (!found_dynamic_tsl) { throw std::invalid_argument("map_: at least one input must be a dynamic TSL"); }

            const auto schemas = std::span<const TSValueTypeMetaData *const>{
                child_schemas.data(), child_schemas.size()};
            CompiledSubGraph compiled = parent != nullptr
                                            ? func.compile(*parent, schemas)
                                            : func.compile(schemas);
            const bool       child_has_output = compiled.output_schema != nullptr;
            if (compiled.output_binding.has_value() != child_has_output) {
                throw std::invalid_argument("map_: the function output schema and nested "
                                            "output binding must agree");
            }

            TslMapNodeSpec spec;
            spec.child.graph_builder  = std::move(compiled.graph_builder);
            spec.child.input_bindings = std::move(compiled.input_bindings);
            spec.child.output_binding = compiled.output_binding;
            spec.index_output_schema  = takes_index ? index_ts : nullptr;

            if (child_has_output) {
                const auto &binding = *spec.child.output_binding;
                if (binding.kind == NestedGraphOutputBinding::Kind::ParentInput) {
                    throw std::invalid_argument("map_: dynamic TSL child pass-through outputs are not supported");
                }
                if (!binding.source.path.empty()) {
                    throw std::invalid_argument("map_: dynamic TSL function output must be a whole node output");
                }
                if (binding.source.node >= spec.child.graph_builder.node_count()) {
                    throw std::invalid_argument("map_: dynamic TSL function output node is out of range");
                }

                const auto *element_schema = mapped_element_schema(compiled);
                NodeBuilder &terminal =
                    spec.child.graph_builder.node_at(binding.source.node);
                spec.output_binding_mode = configure_mapped_child_terminal(
                    terminal, element_schema,
                    compiled.terminal_output_schema, "map_");
                output_schema = registry.tsl(element_schema, 0);
            } else {
                output_schema = nullptr;
            }

            spec.args.reserve(child_schemas.size() + compiled.captured_inputs.size() +
                              compiled.external_service_inputs.size());
            if (takes_index) { spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Key}); }
            for (std::size_t i = 0; i < ts_schemas.size(); ++i) {
                if (multiplexed[i]) {
                    spec.args.push_back(MapArgSource{
                        .kind        = MapArgSourceKind::Element,
                        .outer_index = i,
                    });
                    spec.multiplexed_inputs.push_back(i);
                } else {
                    spec.args.push_back(MapArgSource{
                        .kind        = MapArgSourceKind::OuterInput,
                        .outer_index = i,
                    });
                }
            }
            for (std::size_t i = 0; i < compiled.captured_inputs.size(); ++i) {
                spec.args.push_back(MapArgSource{
                    .kind        = MapArgSourceKind::OuterInput,
                    .outer_index = ts_schemas.size() + i,
                });
            }
            for (std::size_t i = 0; i < compiled.external_service_inputs.size(); ++i) {
                spec.args.push_back(MapArgSource{
                    .kind        = MapArgSourceKind::OuterInput,
                    .outer_index = ts_schemas.size() + compiled.captured_inputs.size() + i,
                });
            }
            captured = std::move(compiled.captured_inputs);
            external_services = std::move(compiled.external_service_inputs);
            return spec;
        }

        [[nodiscard]] inline WiringPortRef wire_dynamic_map_tsl(Wiring &w, const WiredFn &func, std::string_view key_arg,
                                                                bool takes_index, std::vector<WiringPortRef> ordered,
                                                                bool output_required) {
            std::vector<const TSValueTypeMetaData *> ts_schemas;
            std::vector<std::uint8_t>                arg_tags;
            ts_schemas.reserve(ordered.size());
            arg_tags.reserve(ordered.size());
            for (const WiringPortRef &port : ordered) {
                ts_schemas.push_back(port.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(port.arg_tag));
            }

            const TSValueTypeMetaData *output_schema = nullptr;
            std::vector<WiringPortRef> captured;
            std::vector<NestedServiceInput> external_services;
            TslMapNodeSpec spec = compile_dynamic_tsl_map_child(func, takes_index, {ts_schemas.data(), ts_schemas.size()},
                                                                {arg_tags.data(), arg_tags.size()}, output_schema, captured,
                                                                external_services, &w);
            if ((output_schema != nullptr) != output_required) {
                throw std::invalid_argument(output_required ? "map_: 'func' must produce an output"
                                                            : "map_sink_: 'func' must be a sink");
            }

            for (WiringPortRef &outer : captured) {
                ts_schemas.push_back(outer.schema);
                arg_tags.push_back(static_cast<std::uint8_t>(WiringPortRef::ArgTag::PassThrough));
                ordered.push_back(std::move(outer));
            }
            append_external_service_inputs(
                w, std::move(external_services), ts_schemas, arg_tags, ordered);

            const auto passive_materializers = materialize_structural_broadcast_inputs(
                w, ordered, ts_schemas,
                {spec.multiplexed_inputs.data(),
                 spec.multiplexed_inputs.size()});

            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(ts_schemas.size());
            for (std::size_t i = 0; i < ts_schemas.size(); ++i) { fields.emplace_back(std::to_string(i), ts_schemas[i]); }
            const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

            WiringNodeSchema node_schema;
            node_schema.input  = input_schema;
            node_schema.output = output_schema;

            (void)scalar_descriptor<MapCallConfig>::value_meta();
            const auto input_refs = higher_order_input_refs(
                std::span<const WiringPortRef>{ordered.data(), ordered.size()},
                {passive_materializers.data(), passive_materializers.size()});
            return w.add_node(std::type_index(typeid(dynamic_tsl_map_node_tag)), node_schema,
                              std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
                              Value{MapCallConfig{func, Str{key_arg}, Str{}, arg_tags}}, [&]() {
                                  NodeTypeMetaData meta;
                                  meta.display_name  = "map_";
                                  meta.input_schema  = input_schema;
                                  meta.output_schema = output_schema;
                                  configure_passive_transport_inputs(
                                      meta, std::span<const WiringPortRef>{ordered.data(), ordered.size()});

                                  NodeBuilder builder = tsl_map_node(std::move(meta), std::move(spec));
                                  builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                                      input_schema, std::span<const WiringPortRef>{ordered.data(), ordered.size()}));
                                  return builder;
                              });
        }

        [[nodiscard]] inline WiringPortRef wire_map_tsl(Wiring &w, const WiredFn &func, std::string_view key_arg, bool takes_key,
                                                        std::vector<WiringPortRef> ordered, bool output_required) {
            bool        found_collection = false;
            std::size_t size             = 0;
            for (const WiringPortRef &port : ordered) {
                if (port.arg_tag == WiringPortRef::ArgTag::NoKey) {
                    throw std::invalid_argument("map_: 'no_key' applies to TSD maps only");
                }
                if (port.arg_tag == WiringPortRef::ArgTag::PassThrough) { continue; }
                const auto *schema = time_series_schema_as<AnyTSL>(port.schema);
                if (schema != nullptr) {
                    found_collection = true;
                    size             = schema->fixed_size();
                    break;
                }
            }
            if (!found_collection) { throw std::invalid_argument("map_: at least one input must be a TSL"); }
            if (size == 0) { return wire_dynamic_map_tsl(w, func, key_arg, takes_key, std::move(ordered), output_required); }
            return wire_fixed_map_tsl(w, func, takes_key, std::move(ordered), output_required);
        }

        /** Bind the output var ``O`` for the resolver: ``TSL<OUT(func), SIZE>``. */
        inline void resolve_map_tsl_output(ResolutionMap &resolution, OperatorCallContext context) {
            if (resolution.find_ts("__out__") != nullptr) { return; }

            const WiredFn *func = context.scalar_as<WiredFn>("func");
            if (func == nullptr) { return; }

            auto &registry = TypeRegistry::instance();
            auto  ordered  = ordered_map_schemas(context, "ndx");
            if (!ordered.has_value()) { return; }

            try {
                auto tag_at = [&](std::size_t index) {
                    return index < ordered->arg_tags.size() ? static_cast<WiringPortRef::ArgTag>(ordered->arg_tags[index])
                                                            : WiringPortRef::ArgTag::None;
                };

                // A no_key TSL still anchors the size here so resolution
                // succeeds and compose can reject it with the clear message.
                bool        found_collection = false;
                std::size_t size             = 0;
                for (std::size_t i = 0; i < ordered->schemas.size(); ++i) {
                    if (tag_at(i) == WiringPortRef::ArgTag::PassThrough) { continue; }
                    const auto *tsl = time_series_schema_as<AnyTSL>(ordered->schemas[i]);
                    if (tsl != nullptr) {
                        found_collection = true;
                        size             = tsl->fixed_size();
                        break;
                    }
                }
                if (!found_collection) { return; }

                std::vector<const TSValueTypeMetaData *> schemas;
                schemas.reserve(func->arity);
                if (ordered->takes_key) { schemas.push_back(registry.ts(scalar_descriptor<Int>::value_meta())); }
                for (std::size_t i = 0; i < ordered->schemas.size(); ++i) {
                    if (tag_at(i) != WiringPortRef::ArgTag::PassThrough && tsl_arg_is_multiplexed(ordered->schemas[i], size)) {
                        schemas.push_back(time_series_schema_as<AnyTSL>(ordered->schemas[i])->element_ts());
                    } else {
                        schemas.push_back(ordered->schemas[i]);
                    }
                }

                const auto child_schemas =
                    std::span<const TSValueTypeMetaData *const>{schemas.data(),
                                                               schemas.size()};
                CompiledSubGraph compiled = context.wiring != nullptr
                                                ? func->compile(*context.wiring,
                                                                child_schemas)
                                                : func->compile(child_schemas);
                if (compiled.output_schema != nullptr) {
                    bind_graph_output(resolution, registry.tsl(compiled.output_schema, size), "O");
                }
            } catch (...) {
                // Leave unresolved; the real wiring path reports the error.
            }
        }

        inline void resolve_lifted_map_tsl_output(ResolutionMap &resolution, OperatorCallContext context) {
            auto plan = lifted_map_tsl_plan(context);
            if (plan.has_value() && plan->output_schema != nullptr) { bind_graph_output(resolution, plan->output_schema, "O"); }
        }

        struct map_lifted_tsl
        {
            static constexpr auto name = "map_lifted_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context) {
                return lifted_map_tsl_plan(context).has_value();
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context) {
                resolve_lifted_map_tsl_output(resolution, context);
            }

            static auto defaults()
            { return std::tuple{arg<"__key_arg__">(Str{"ndx"})};
            }

            static auto compose(Wiring &w, Scalar<"func", WiredFn> func, VarIn<"args", TsVar<"B">> positional,
                                Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs) {
                const std::vector<WiringPortRef>                   pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                if (split_keys_kwarg(named).has_value()) {
                    throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
                }
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(), {pos.data(), pos.size()},
                                                               {named.data(), named.size()}, key_arg.value());
                if (bound.takes_leading_key) {
                    throw std::invalid_argument("map_: lifted TSL fast path does not support index arguments yet");
                }
                return wire_lifted_map_tsl(w, func.value(), key_arg.value(), std::move(bound.ordered));
            }
        };

        /**
         * ``map_(func, *args, **kwargs)`` over a fixed TSL — wiring-time
         * expansion over the first fixed TSL in ``func`` parameter order. Any
         * fixed TSL of the SAME size multiplexes per index; everything else
         * broadcasts whole.
         */
        struct map_impl_tsl
        {
            static constexpr auto name = "map_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context) {
                const auto *collection = first_map_collection(context);
                if (time_series_schema_as<AnyTSL>(collection) == nullptr || lifted_map_tsl_plan(context).has_value()) {
                    return false;
                }
                const auto *func = context.scalar_as<WiredFn>("func");
                return func != nullptr && func->has_output;
            }

            static void resolve_default_types(ResolutionMap &resolution, OperatorCallContext context) {
                resolve_map_tsl_output(resolution, context);
            }

            static auto defaults()
            { return std::tuple{arg<"__key_arg__">(Str{"ndx"})};
            }

            static WiringPortRef compose(Wiring &w, Scalar<"func", WiredFn> func, VarIn<"args", TsVar<"B">> positional,
                                         Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs) {
                const std::vector<WiringPortRef>                   pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                if (split_keys_kwarg(named).has_value()) {
                    throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
                }
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(), {pos.data(), pos.size()},
                                                               {named.data(), named.size()}, key_arg.value());
                return wire_map_tsl(w, func.value(), key_arg.value(), bound.takes_leading_key, std::move(bound.ordered), true);
            }
        };

        /** Outputless ``map_`` over fixed or dynamic TSLs. */
        struct map_sink_impl_tsl
        {
            static constexpr auto name = "map_sink_tsl";

            static bool requires_(const ResolutionMap &, OperatorCallContext context) {
                const auto *collection = first_map_collection(context);
                if (time_series_schema_as<AnyTSL>(collection) == nullptr) { return false; }
                const auto *func = context.scalar_as<WiredFn>("func");
                return func != nullptr && !func->has_output;
            }

            static auto defaults()
            { return std::tuple{arg<"__key_arg__">(Str{"ndx"})};
            }

            static void compose(Wiring &w, Scalar<"func", WiredFn> func, VarIn<"args", TsVar<"B">> positional,
                                Scalar<"__key_arg__", Str> key_arg, VarKwIn<"kwargs"> kwargs) {
                const std::vector<WiringPortRef>                   pos{positional.begin(), positional.end()};
                std::vector<std::pair<std::string, WiringPortRef>> named{kwargs.begin(), kwargs.end()};
                if (split_keys_kwarg(named).has_value()) {
                    throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
                }
                auto bound = bind_wired_fn_args<WiringPortRef>("map_", func.value(), {pos.data(), pos.size()},
                                                               {named.data(), named.size()}, key_arg.value());
                (void)wire_map_tsl(w, func.value(), key_arg.value(), bound.takes_leading_key, std::move(bound.ordered), false);
            }
        };
    }  // namespace higher_order_impl_detail

    /**
     * ``mesh_(func)[k]`` cross-instance access, called inside a mesh function (or a
     * sub-graph wired within the mesh's scope). Returns the result of the sibling
     * instance for key ``k``, creating it on demand and recording a dependency so the
     * mesh evaluates it first (same cycle, via pause/resume). ``OutS`` is the mesh
     * element type; ``name`` optionally selects an enclosing named mesh.
     */
    template <typename OutS, typename KeyPort>
    [[nodiscard]] Port<OutS> mesh_ref(Wiring &w, KeyPort key, std::string_view name = {}) {
        // A never-ticking placeholder seeds the dynamic ``value`` input; mesh_subscribe
        // rebinds it to the sibling output (self[item]) at runtime.
        Port<OutS> placeholder = wire<nothing, OutS>(w);
        return Port<OutS>{
            w, higher_order_impl_detail::mesh_ref_erased(w, key.erased(), placeholder.erased(), name)};
    }

    /**
     * ``mesh_keys_ref<K>(w)`` cross-instance key-set access, called inside a mesh
     * function. Returns the enclosing mesh output key set as ``TSS<K>`` by
     * forwarding to ``self.key_set()``; no value copy is performed. ``name``
     * optionally selects an enclosing named mesh.
     */
    template <typename KeyT>
    [[nodiscard]] Port<TSS<KeyT>> mesh_keys_ref(Wiring &w, std::string_view name = {})
    {
        Port<TSS<KeyT>> placeholder = wire<nothing, TSS<KeyT>>(w);
        return Port<TSS<KeyT>>{
            w, higher_order_impl_detail::mesh_key_set_ref_erased(
                   w, placeholder.erased(), scalar_descriptor<KeyT>::value_meta(), name)};
    }

    void register_higher_order_operators();
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_OPERATORS_IMPL_HIGHER_ORDER_IMPL_H
