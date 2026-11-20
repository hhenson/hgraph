#ifndef HGRAPH_TYPES_SUBGRAPH_WIRING_H
#define HGRAPH_TYPES_SUBGRAPH_WIRING_H

#include <hgraph/runtime/canonical_error.h>
#include <hgraph/runtime/nested_graph_node.h>
#include <hgraph/runtime/node_error.h>
#include <hgraph/runtime/try_except_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/compiled_subgraph.h>

#include <array>
#include <cstddef>
#include <initializer_list>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace subgraph_wiring_detail
    {
        [[nodiscard]] inline WiringPortRef materialize_external_service_input(
            Wiring &w, NestedServiceInput input)
        {
            w.register_service_client_path(
                input.service_path, input.service_kind,
                input.interface_name, input.specialization);
            Value scalars = input.builder.scalars();
            WiringPortRef source = w.add_node(
                input.definition, std::move(input.builder), std::span<const WiringPortRef>{},
                std::move(scalars));
            for (NestedServiceRank &rank : input.ranks)
            {
                w.register_service_client_rank(
                    std::move(rank.path), rank.kind, source.peered_node(), rank.receive);
            }
            source.arg_tag = input.arg_tag;
            return source;
        }

        // Interning identity for a nested_<G> node instance.
        template <typename G>
        struct nested_marker
        {
        };


    }  // namespace subgraph_wiring_detail

    /**
     * Wire sub-graph ``G`` into ``w`` as a **nested-graph node** (non-flattening).
     *
     * Unlike ``wire<G>`` — which inlines the compose body into the enclosing
     * wiring — ``nested_<G>`` compiles ``G`` into its own child graph (via
     * ``compile_subgraph``) and adds one ``single_nested_graph_node`` that owns
     * and evaluates it. Arguments follow the same rule as ``wire<G>``: a
     * ``Port`` (or a ``{…}`` structural initializer) for each ``Port`` parameter
     * and a scalar value for each ``Scalar`` parameter, in compose order.
     * Returns the nested node's output port (or ``void`` for a sink sub-graph).
     *
     * Instances are interned like ordinary nodes: same ``G`` + equal inputs +
     * equal scalar values dedup to one nested node — the child graph builder and
     * its program-lifetime context are only created on an intern miss.
     */
    namespace subgraph_wiring_detail
    {
        // The shared front half of a non-flattening sub-graph call: collect +
        // schema-check the outer input sources, compile ``G`` against their
        // boundary shapes, and build the outer node's input TSB + the scalar
        // interning bundle. The owning-node specifics (output schema, node
        // builder) are supplied by the caller (``nested_`` / ``try_except_``).
        struct SubgraphCallPlan
        {
            std::vector<WiringPortRef> inputs{};
            const TSValueTypeMetaData *input_schema{nullptr};
            Value                      scalars{};
            CompiledSubGraph           compiled{};
        };

        template <typename G, typename... Args>
        [[nodiscard]] SubgraphCallPlan build_subgraph_call(Wiring &w, const Args &...args)
        {
            using sig    = StaticGraphSignature<G>;
            using params = typename sig::param_types;

            auto arg_tuple = std::forward_as_tuple(args...);

            // 1. Collect + schema-check the outer input sources (the Port positions)
            //    and mirror each as the boundary shape the child compiles against.
            std::vector<WiringPortRef> inputs;
            std::vector<WiringPortRef> shapes;
            inputs.reserve(sig::input_count());
            shapes.reserve(sig::input_count());
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                        if constexpr (graph_wiring_detail::is_port<P>::value)
                        {
                            using A = std::remove_cvref_t<std::tuple_element_t<I, std::tuple<const Args &...>>>;
                            static_assert(graph_wiring_detail::is_port<A>::value ||
                                              graph_wiring_detail::is_structural_source_arg<A>::value,
                                          "sub-graph call: a time-series input expects a Port argument or "
                                          "structural initializer");
                            const auto *expected = schema_descriptor<typename P::schema>::ts_meta();

                            WiringPortRef ref;
                            if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                            {
                                ref = graph_wiring_detail::structural_source_for_input_schema(expected,
                                                                                              std::get<I>(arg_tuple));
                            }
                            else
                            {
                                ref = std::get<I>(arg_tuple).erased();
                                if constexpr (graph_wiring_detail::is_erased_port<A>::value)
                                {
                                    if (!graph_wiring_detail::input_binds_output_schema(expected, ref.schema))
                                    {
                                        throw std::logic_error(
                                            "sub-graph call: erased input port schema does not match the "
                                            "sub-graph's time-series input");
                                    }
                                }
                                else
                                {
                                    static_assert(graph_wiring_detail::statically_accepts_output_v<
                                                      typename P::schema, typename A::schema>,
                                                  "sub-graph call: input port schema does not match the "
                                                  "sub-graph's time-series input");
                                }
                            }
                            ref = graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref));
                            shapes.push_back(subgraph_wiring_detail::boundary_shape(ref, inputs.size(), {}));
                            inputs.push_back(std::move(ref));
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<sig::param_count()>{});

            // 2. Compile the child graph against the boundary shapes; scalar args
            //    are baked into the child nodes.
            constexpr auto scalar_positions =
                subgraph_wiring_detail::scalar_positions_of<params, sig::scalar_count()>();
            CompiledSubGraph compiled = [&]<std::size_t... S>(std::index_sequence<S...>) {
                return subgraph_wiring_detail::compile_subgraph_impl<G>(
                    &w,
                    std::span<const WiringPortRef>{shapes.data(), shapes.size()},
                    std::forward_as_tuple(std::get<scalar_positions[S]>(arg_tuple)...));
            }(std::make_index_sequence<sig::scalar_count()>{});

            inputs.reserve(inputs.size() + compiled.captured_inputs.size() +
                           compiled.external_service_inputs.size());
            for (WiringPortRef &captured : compiled.captured_inputs)
            {
                inputs.push_back(std::move(captured));
            }
            compiled.captured_inputs.clear();
            for (NestedServiceInput &external : compiled.external_service_inputs)
            {
                inputs.push_back(materialize_external_service_input(w, std::move(external)));
            }
            compiled.external_service_inputs.clear();

            // 3. The outer node's input TSB over the boundary args.
            const TSValueTypeMetaData *input_schema = nullptr;
            if (!compiled.input_schemas.empty())
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(compiled.input_schemas.size());
                for (std::size_t i = 0; i < compiled.input_schemas.size(); ++i)
                {
                    fields.emplace_back(std::to_string(i), compiled.input_schemas[i]);
                }
                input_schema = TypeRegistry::instance().un_named_tsb(fields);
            }

            // 4. Scalar configuration bundle: the nested node does not consume it
            //    at runtime (the values are baked into the child graph), but it
            //    must enter the interning identity so equal scalars dedup and
            //    distinct ones do not.
            Value scalars;
            if constexpr (sig::scalar_count() > 0)
            {
                std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields;
                fields.reserve(sig::scalar_count());
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                            if constexpr (static_node_detail::is_scalar_selector<P>::value)
                            {
                                fields.emplace_back(std::string{P::field_name.sv()},
                                                    graph_wiring_detail::scalar_argument_meta(std::get<I>(arg_tuple)));
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<sig::param_count()>{});

                const auto binding = ValuePlanFactory::instance().type_for(
                    TypeRegistry::instance().un_named_bundle(fields));
                BundleBuilder bundle{binding};
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                            if constexpr (static_node_detail::is_scalar_selector<P>::value)
                            {
                                Value field = graph_wiring_detail::make_scalar_field<P>(std::get<I>(arg_tuple));
                                bundle.set(P::field_name.sv(), std::move(field));
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<sig::param_count()>{});
                scalars = bundle.build();
            }

            return SubgraphCallPlan{std::move(inputs), input_schema, std::move(scalars), std::move(compiled)};
        }
    }  // namespace subgraph_wiring_detail

    template <typename G, typename... Args>
    auto nested_(Wiring &w, const Args &...args)
    {
        using sig = StaticGraphSignature<G>;
        static_assert(sizeof...(Args) == sig::param_count(),
                      "nested_<G>: argument count must match the sub-graph's Port + Scalar parameters "
                      "(in compose order)");

        auto plan = subgraph_wiring_detail::build_subgraph_call<G>(w, args...);

        // Add the nested node; the builder (and its program-lifetime child graph
        // context) is only created when interning does not dedup.
        WiringNodeSchema node_schema;
        node_schema.input  = plan.input_schema;
        node_schema.output = plan.compiled.output_schema;

        WiringPortRef out = w.add_node(
            std::type_index(typeid(subgraph_wiring_detail::nested_marker<G>)), node_schema,
            std::span<const WiringPortRef>{plan.inputs.data(), plan.inputs.size()}, std::move(plan.scalars),
            [&]() {
                NodeTypeMetaData meta;
                if constexpr (static_node_detail::has_name<G>) { meta.display_name = G::name; }
                else { meta.display_name = "nested"; }
                meta.input_schema  = plan.input_schema;
                meta.output_schema = plan.compiled.output_schema;

                SingleNestedGraphNodeSpec spec;
                spec.graph_builder  = std::move(plan.compiled.graph_builder);
                spec.input_bindings = std::move(plan.compiled.input_bindings);
                spec.output_binding = plan.compiled.output_binding;

                NodeBuilder builder = single_nested_graph_node(std::move(meta), std::move(spec));
                builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    plan.input_schema, std::span<const WiringPortRef>{plan.inputs.data(), plan.inputs.size()}));
                return builder;
            });

        using output_type = std::remove_cvref_t<typename sig::output_type>;
        if constexpr (!std::is_void_v<output_type>)
        {
            return output_type{w, std::move(out)};
        }
    }

    namespace subgraph_wiring_detail
    {
        // Interning identity for a try_except<G> node instance (distinct from
        // nested_marker<G> so a graph wired both ways does not dedup).
        template <typename G>
        struct try_except_marker
        {
        };
    }  // namespace subgraph_wiring_detail

    /**
     * Wire sub-graph ``G`` into ``w`` wrapped in a **try/except node**: the child
     * graph runs under a try/catch and the node produces ``TSB[{exception, out}]``
     * — ``out`` forwards the wrapped graph's output, ``exception`` ticks a
     * ``NodeError`` when the child raises (the graph keeps running). A **sink**
     * sub-graph (no output) yields a bare ``TS<NodeError>``. Arguments follow the
     * same rule as ``nested_<G>``. Returns the (erased) output port.
     *
     * In canonical mode the child forwards its output to the first field,
     * ``value``, and evaluation failures publish to ``error`` (or the sink
     * wrapper's sole output) as ``TS<CanonicalError>``.
     * See the developer guide *Error handling*.
     */
    template <typename G, typename... Args>
        requires (sizeof...(Args) == StaticGraphSignature<G>::param_count())
    [[nodiscard]] Port<void> try_except_(Wiring &w, ErrorCaptureOptions options, const Args &...args)
    {
        auto plan = subgraph_wiring_detail::build_subgraph_call<G>(w, args...);

        auto       &registry     = TypeRegistry::instance();
        const auto *error_ts     = options.canonical_errors ? ts_type<TS<CanonicalError>>() : node_error_ts_meta();
        const auto *child_output = plan.compiled.output_schema;
        const bool  has_out      = child_output != nullptr;

        const TSValueTypeMetaData *output_schema = error_ts;
        const std::size_t out_index = options.canonical_errors ? 0 : 1;
        if (has_out)
        {
            output_schema = options.canonical_errors
                ? registry.un_named_tsb({{"value", child_output}, {"error", error_ts}})
                : registry.un_named_tsb({{"exception", error_ts}, {"out", child_output}});
        }

        // Equal graph calls with different capture policies are distinct nodes.
        if (options != ErrorCaptureOptions{})
        {
            Value depth{static_cast<std::uint64_t>(options.trace_back_depth)};
            Value values{options.capture_values};
            std::vector<std::pair<std::string, const ValueTypeMetaData *>> fields{
                {"depth", depth.schema()}, {"capture_values", values.schema()}};
            if (plan.scalars.has_value()) { fields.emplace_back("arguments", plan.scalars.schema()); }
            BundleBuilder key{ValuePlanFactory::instance().type_for(registry.un_named_bundle(fields))};
            key.set("depth", std::move(depth)).set("capture_values", std::move(values));
            if (plan.scalars.has_value()) { key.set("arguments", std::move(plan.scalars)); }
            plan.scalars = key.build();
        }

        WiringNodeSchema node_schema;
        node_schema.input  = plan.input_schema;
        node_schema.output = output_schema;

        WiringPortRef out = w.add_node(
            std::type_index(typeid(subgraph_wiring_detail::try_except_marker<G>)), node_schema,
            std::span<const WiringPortRef>{plan.inputs.data(), plan.inputs.size()}, std::move(plan.scalars),
            [&]() {
                NodeTypeMetaData meta;
                if constexpr (static_node_detail::has_name<G>) { meta.display_name = G::name; }
                else { meta.display_name = "try_except"; }
                meta.input_schema  = plan.input_schema;
                meta.output_schema = output_schema;

                SingleNestedGraphNodeSpec spec;
                spec.graph_builder  = std::move(plan.compiled.graph_builder);
                spec.input_bindings = std::move(plan.compiled.input_bindings);
                // Record the child terminal + the bundle's ``out`` field index so
                // the nested output binder can forward the child output directly
                // (sink sub-graphs have no output to forward).
                if (has_out && plan.compiled.output_binding.has_value())
                {
                    spec.output_binding              = plan.compiled.output_binding;
                    spec.output_binding->target_path = {out_index};
                }

                NodeBuilder builder = try_except_node(std::move(meta), std::move(spec), {}, options);
                builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    plan.input_schema, std::span<const WiringPortRef>{plan.inputs.data(), plan.inputs.size()}));
                return builder;
            });

        return Port<void>{w, std::move(out)};
    }

    template <typename G, typename... Args>
        requires (sizeof...(Args) == StaticGraphSignature<G>::param_count())
    [[nodiscard]] Port<void> try_except_(Wiring &w, const Args &...args)
    {
        return try_except_<G>(w, ErrorCaptureOptions{}, args...);
    }

    /** Per-key error capture for a TSD ``map_`` output. */
    template <typename Key, typename ValueSchema>
    [[nodiscard]] Port<TSD<Key, TS<NodeError>>> exception_time_series(
        const Port<TSD<Key, ValueSchema>> &port,
        ErrorCaptureOptions options = {})
    {
        Wiring &w = port.checked_wiring();
        w.activate_error_capture(port.erased().peered_node(), node_error_ts_meta(), options);
        return error_output(port).template as<TSD<Key, TS<NodeError>>>();
    }

    /**
     * Python's ``exception_time_series(port)`` — activate error capture on the
     * port's producing node and return its scalar error-output time series.
     * TSD ``map_`` outputs select the overload above and return one error
     * series per mapped key.
     */
    template <typename Schema>
    [[nodiscard]] Port<TS<NodeError>> exception_time_series(
        const Port<Schema> &port,
        ErrorCaptureOptions options = {})
    {
        Wiring &w = port.checked_wiring();
        w.activate_error_capture(port.erased().peered_node(), node_error_ts_meta(), options);
        return error_output(port).template as<TS<NodeError>>();
    }

    /**
     * Brace-initializer entry points for ``nested_<G>(w, {a, b}, …)`` — the
     * ``wire<>`` rule applies: only the first three positions accept a bare
     * ``{…}``; later positions wrap explicitly as a
     * ``WiringStructuralSourceArg`` / ``WiringNamedStructuralSourceArg``.
     */
    template <typename G, typename... Rest>
    auto nested_(Wiring &w, std::initializer_list<WiringPortRef> first, const Rest &...rest)
    {
        return nested_<G>(w, WiringStructuralSourceArg{first}, rest...);
    }

    template <typename G, typename... Rest>
    auto nested_(Wiring &w, std::initializer_list<WiringNamedPortRef> first, const Rest &...rest)
    {
        return nested_<G>(w, WiringNamedStructuralSourceArg{first}, rest...);
    }

    template <typename G, typename A0, typename... Rest>
    auto nested_(Wiring &w, const A0 &a0, std::initializer_list<WiringPortRef> second, const Rest &...rest)
    {
        return nested_<G>(w, a0, WiringStructuralSourceArg{second}, rest...);
    }

    template <typename G, typename A0, typename... Rest>
    auto nested_(Wiring &w, const A0 &a0, std::initializer_list<WiringNamedPortRef> second, const Rest &...rest)
    {
        return nested_<G>(w, a0, WiringNamedStructuralSourceArg{second}, rest...);
    }

    template <typename G, typename A0, typename A1, typename... Rest>
    auto nested_(Wiring &w, const A0 &a0, const A1 &a1, std::initializer_list<WiringPortRef> third,
                 const Rest &...rest)
    {
        return nested_<G>(w, a0, a1, WiringStructuralSourceArg{third}, rest...);
    }

    template <typename G, typename A0, typename A1, typename... Rest>
    auto nested_(Wiring &w, const A0 &a0, const A1 &a1, std::initializer_list<WiringNamedPortRef> third,
                 const Rest &...rest)
    {
        return nested_<G>(w, a0, a1, WiringNamedStructuralSourceArg{third}, rest...);
    }

    namespace subgraph_wiring_detail
    {
        /**
         * Erased core of ``tsl_element``: project the ``index``-th element ref
         * out of a TSL-shaped wiring source of any kind (peered output path,
         * structural child, sub-graph boundary). Used by the typed projection
         * below and by erased consumers such as the ``reduce`` operator impl.
         */
        [[nodiscard]] inline WiringPortRef tsl_element_ref(const WiringPortRef &ts, std::size_t index,
                                                           const TSValueTypeMetaData *element_schema)
        {
            switch (ts.source_kind())
            {
                case WiringPortRef::SourceKind::Peered:
                {
                    std::vector<std::size_t> path = ts.peered_path();
                    path.push_back(index);
                    return WiringPortRef::peered_source(ts.peered_node(), std::move(path), element_schema,
                                                        ts.peered_output_kind());
                }
                case WiringPortRef::SourceKind::Structural:
                    return ts.structural_children()[index];
                case WiringPortRef::SourceKind::Boundary:
                {
                    std::vector<std::size_t> path = ts.boundary_path();
                    path.push_back(index);
                    return ts.projected_boundary_source(std::move(path), element_schema);
                }
                case WiringPortRef::SourceKind::Delayed:
                {
                    std::vector<std::size_t> path = ts.delayed_path();
                    path.push_back(index);
                    return ts.projected_delayed_source(std::move(path), element_schema);
                }
                case WiringPortRef::SourceKind::Null:
                    return WiringPortRef::null_source(element_schema);
                case WiringPortRef::SourceKind::Unbound:
                    break;
            }
            throw std::logic_error("tsl_element: the TSL port is unbound");
        }

        /**
         * Erased core of TSB field projection: project the ``index``-th field ref
         * out of a TSB-shaped wiring source of any kind.
         */
        [[nodiscard]] inline WiringPortRef tsb_field_ref(const WiringPortRef &ts, std::size_t index,
                                                         const TSValueTypeMetaData *field_schema)
        {
            switch (ts.source_kind())
            {
                case WiringPortRef::SourceKind::Peered:
                {
                    std::vector<std::size_t> path = ts.peered_path();
                    path.push_back(index);
                    return WiringPortRef::peered_source(ts.peered_node(), std::move(path), field_schema,
                                                        ts.peered_output_kind());
                }
                case WiringPortRef::SourceKind::Structural:
                    return ts.structural_children()[index];
                case WiringPortRef::SourceKind::Boundary:
                {
                    std::vector<std::size_t> path = ts.boundary_path();
                    path.push_back(index);
                    return ts.projected_boundary_source(std::move(path), field_schema);
                }
                case WiringPortRef::SourceKind::Delayed:
                {
                    std::vector<std::size_t> path = ts.delayed_path();
                    path.push_back(index);
                    return ts.projected_delayed_source(std::move(path), field_schema);
                }
                case WiringPortRef::SourceKind::Null:
                    return WiringPortRef::null_source(field_schema);
                case WiringPortRef::SourceKind::Unbound:
                    break;
            }
            throw std::logic_error("tsb_field: the TSB port is unbound");
        }

        /**
         * Project a TSD port's **key set** (``TSS[K]``) — a zero-copy view
         * over the same output, addressed at runtime via the
         * ``ts_key_set_path_component`` path sentinel. Usable on peered and
         * boundary sources (a structural TSD source has no single backing
         * dict).
         */
        [[nodiscard]] inline WiringPortRef tsd_key_set_ref(const WiringPortRef &ts)
        {
            auto       &registry = TypeRegistry::instance();
            const auto *deref    = registry.dereference(ts.schema);
            if (deref == nullptr || deref->kind != TSTypeKind::TSD)
            {
                throw std::invalid_argument("keys_: the input must be a TSD");
            }
            const auto *key_set_schema = registry.tss(deref->key_type());

            switch (ts.source_kind())
            {
                case WiringPortRef::SourceKind::Peered:
                {
                    std::vector<std::size_t> path = ts.peered_path();
                    path.push_back(ts_key_set_path_component);
                    return WiringPortRef::peered_source(ts.peered_node(), std::move(path), key_set_schema,
                                                        ts.peered_output_kind());
                }
                case WiringPortRef::SourceKind::Boundary:
                {
                    std::vector<std::size_t> path = ts.boundary_path();
                    path.push_back(ts_key_set_path_component);
                    return ts.projected_boundary_source(std::move(path), key_set_schema);
                }
                case WiringPortRef::SourceKind::Delayed:
                {
                    std::vector<std::size_t> path = ts.delayed_path();
                    path.push_back(ts_key_set_path_component);
                    return ts.projected_delayed_source(std::move(path), key_set_schema);
                }
                case WiringPortRef::SourceKind::Null:
                    return WiringPortRef::null_source(key_set_schema);
                case WiringPortRef::SourceKind::Structural:
                case WiringPortRef::SourceKind::Unbound:
                    break;
            }
            throw std::logic_error("keys_: the TSD port must be a peered or boundary source");
        }
    }  // namespace subgraph_wiring_detail

    /**
     * Project the ``index``-th element port out of a fixed-size TSL port — a
     * wiring-time projection only, usable on any source kind (peered output
     * path, structural child, sub-graph boundary).
     */
    template <typename E, auto N>
    [[nodiscard]] Port<E> tsl_element(const Port<TSL<E, N>> &ts, std::size_t index)
    {
        if (index >= N) { throw std::out_of_range("tsl_element: index is out of range for the fixed TSL"); }
        return Port<E>{ts.wiring(), subgraph_wiring_detail::tsl_element_ref(ts.erased(), index,
                                                                            schema_descriptor<E>::ts_meta())};
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_SUBGRAPH_WIRING_H
