#ifndef HGRAPH_TYPES_COMPILED_SUBGRAPH_H
#define HGRAPH_TYPES_COMPILED_SUBGRAPH_H

#include <hgraph/runtime/nested_graph_node.h>
#include <hgraph/types/graph_wiring_core.h>

namespace hgraph
{
    struct NestedServiceRank
    {
        std::string path{};
        std::string kind{};
        bool        receive{true};
    };

    /**
     * A service transport source used by a compiled child but implemented by
     * an enclosing graph. The exact wiring identity and builder are retained
     * so the owner can intern the same endpoint in its wiring; child inputs
     * then bind directly to that outer source through the normal nested-graph
     * boundary protocol.
     */
    struct NestedServiceInput
    {
        std::string                        service_path{};
        std::string                        service_kind{};
        std::string                        interface_name{};
        std::string                        specialization{};
        std::type_index                    definition{typeid(void)};
        NodeBuilder                        builder{};
        const TSValueTypeMetaData         *source_schema{nullptr};
        WiringPortRef::ArgTag              arg_tag{WiringPortRef::ArgTag::None};
        std::vector<NestedServiceRank>      ranks{};
    };

    /**
     * A wiring-compiled child-graph template: the ranked child ``GraphBuilder``
     * plus the boundary binding specs that connect it to an owning nested node.
     *
     * Produced by ``compile_subgraph<G>`` (the only place a sub-graph ``compose``
     * binds to a child graph) and consumed by the nested-graph operators —
     * ``nested_<G>`` today; ``switch_``/``map_`` build on the same artifact.
     * See the developer guide *Nested Graphs*.
     */
    struct CompiledSubGraph
    {
        GraphBuilder                            graph_builder{};
        std::vector<NestedGraphInputBinding>    input_bindings{};
        std::optional<NestedGraphOutputBinding> output_binding{};
        /** Boundary time-series input schemas, in compose ``Port`` parameter order. */
        std::vector<const TSValueTypeMetaData *> input_schemas{};
        /** Public/logical result schema of the compiled graph. */
        const TSValueTypeMetaData               *output_schema{nullptr};
        /** Physical schema of the endpoint named by ``output_binding``.
            This differs from ``output_schema`` only when compilation adds an
            internal terminal (for example REF<TSB/TSL> for a newly composed
            fixed structural result). Consumers normally expose
            ``output_schema``; container owners also use this to retain an
            operator-authored REF endpoint in their element allocation. */
        const TSValueTypeMetaData               *terminal_output_schema{nullptr};
        /** The physical REF terminal was synthesized solely to give a newly
            composed fixed structure one endpoint. Unlike an operator-authored
            REF result, this adapter is not part of the callable's result. */
        bool terminal_is_structural_adapter{false};
        /**
         * Outer-port CAPTURES (nested_graphs.rst): OUTER-wiring ports the
         * compose body referenced (closure capture). One per boundary index
         * appended after the declared inputs - the caller binds them
         * (map_: pass-through broadcast args). Consumers without capture
         * support must reject a non-empty list.
         */
        std::vector<WiringPortRef> captured_inputs{};
        /** Service endpoint sources supplied by the graph that owns this child. */
        std::vector<NestedServiceInput> external_service_inputs{};
    };

    namespace subgraph_wiring_detail
    {
        template <typename Params, std::size_t I>
        [[nodiscard]] constexpr std::size_t ports_before()
        {
            return []<std::size_t... K>(std::index_sequence<K...>) {
                return (std::size_t{0} + ... +
                        (graph_wiring_detail::is_port<
                             std::remove_cvref_t<std::tuple_element_t<K, Params>>>::value
                             ? std::size_t{1}
                             : std::size_t{0}));
            }(std::make_index_sequence<I>{});
        }

        template <typename Params, std::size_t I>
        [[nodiscard]] constexpr std::size_t scalars_before()
        {
            return []<std::size_t... K>(std::index_sequence<K...>) {
                return (std::size_t{0} + ... +
                        (static_node_detail::is_scalar_selector<
                             std::remove_cvref_t<std::tuple_element_t<K, Params>>>::value
                             ? std::size_t{1}
                             : std::size_t{0}));
            }(std::make_index_sequence<I>{});
        }

        // Positions of the Scalar parameters within a compose parameter tuple,
        // so wiring arguments can be split by role at compile time.
        template <typename Params, std::size_t Count>
        [[nodiscard]] constexpr std::array<std::size_t, Count> scalar_positions_of()
        {
            return []<std::size_t... I>(std::index_sequence<I...>) {
                std::array<std::size_t, Count> positions{};
                [[maybe_unused]] std::size_t   next = 0;
                (
                    [&] {
                        using P = std::remove_cvref_t<std::tuple_element_t<I, Params>>;
                        if constexpr (static_node_detail::is_scalar_selector<P>::value) { positions[next++] = I; }
                    }(),
                    ...);
                return positions;
            }(std::make_index_sequence<std::tuple_size_v<Params>>{});
        }

        // Mirror an outer wiring source as the sub-graph's boundary shape: a
        // peered (or erased) source becomes a boundary placeholder, a structural
        // source keeps its shape with boundary leaves (so child endpoints derive
        // as non-peered with bindable leaf slots), and a null leaf stays null
        // (the child slot simply never binds).
        [[nodiscard]] inline WiringPortRef boundary_shape(const WiringPortRef &source,
                                                          std::size_t arg_index,
                                                          std::vector<std::size_t> path)
        {
            if (source.is_structural_source())
            {
                const auto &source_children = source.structural_children();
                std::vector<WiringPortRef> children;
                children.reserve(source_children.size());
                for (std::size_t index = 0; index < source_children.size(); ++index)
                {
                    std::vector<std::size_t> child_path = path;
                    child_path.push_back(index);
                    children.push_back(boundary_shape(source_children[index], arg_index, std::move(child_path)));
                }
                return WiringPortRef::structural_source(source.schema, std::move(children));
            }
            if (source.is_null_source()) { return source; }
            return WiringPortRef::boundary_source(arg_index, std::move(path), source.schema);
        }

        /**
         * Compile sub-graph ``G`` against a fresh wiring whose ``Port``
         * parameters carry the supplied boundary shapes (or plain boundary
         * placeholders when ``boundary_shapes`` is empty), then convert the
         * boundary-sourced edges into binding specs via ``finish_subgraph``.
         */
        template <typename G, typename ScalarTuple>
        [[nodiscard]] CompiledSubGraph compile_subgraph_impl(Wiring *parent,
                                                             std::span<const WiringPortRef> boundary_shapes,
                                                             ScalarTuple &&scalar_tuple)
        {
            using sig    = StaticGraphSignature<G>;
            using params = typename sig::param_types;

            Wiring w = parent != nullptr ? parent->child_wiring()
                                         : Wiring{WiringKind::SubGraph};

            // Boundary arg schemas, in Port-parameter order.
            std::vector<const TSValueTypeMetaData *> input_schemas;
            input_schemas.reserve(sig::input_count());
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                        if constexpr (graph_wiring_detail::is_port<P>::value)
                        {
                            static_assert(!graph_wiring_detail::is_erased_port<P>::value,
                                          "compile_subgraph<G>: a sub-graph boundary Port must carry a concrete "
                                          "schema");
                            input_schemas.push_back(schema_descriptor<typename P::schema>::ts_meta());
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<sig::param_count()>{});

            auto make_param = [&]<std::size_t I>() {
                using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                if constexpr (graph_wiring_detail::is_port<P>::value)
                {
                    constexpr std::size_t ordinal = ports_before<params, I>();
                    if (!boundary_shapes.empty()) { return P{w, boundary_shapes[ordinal]}; }
                    const auto *schema = schema_descriptor<typename P::schema>::ts_meta();
                    return P{w, WiringPortRef::boundary_source(ordinal, {}, schema)};
                }
                else
                {
                    return graph_wiring_detail::make_scalar_param<P>(
                        std::get<scalars_before<params, I>()>(scalar_tuple));
                }
            };

            // The child graph holds its label on the wiring path while it
            // composes and finalizes, including deferred wiring (WIR-4).
            CompiledSubGraph compiled = [&]<std::size_t... I>(std::index_sequence<I...>) {
                const WiringPathScope path{w, static_node_detail::diagnostic_name<G>()};
                if constexpr (std::is_void_v<typename sig::output_type>)
                {
                    G::compose(w, make_param.template operator()<I>()...);
                    return std::move(w).finish_subgraph(std::nullopt, std::move(input_schemas));
                }
                else
                {
                    auto out = G::compose(w, make_param.template operator()<I>()...);
                    return std::move(w).finish_subgraph(out.erased(), std::move(input_schemas));
                }
            }(std::make_index_sequence<sig::param_count()>{});

            if constexpr (static_node_detail::has_name<G>) { compiled.graph_builder.label(std::string{G::name}); }
            return compiled;
        }
    }  // namespace subgraph_wiring_detail

    /**
     * Compile sub-graph ``G`` into a ``CompiledSubGraph``.
     *
     * ``G::compose(Wiring &, Port..., Scalar...)`` runs against a **fresh**
     * wiring whose ``Port`` parameters are boundary placeholders (no stub nodes);
     * ``finish_subgraph`` then converts every boundary-sourced input edge into a
     * nested-graph input binding and the returned output port into the output
     * binding (a returned boundary input becomes the pass-through
     * ``ParentInput`` mode). ``scalar_args`` supplies the ``Scalar`` parameters
     * (in compose order); their values are baked into the compiled child nodes.
     */
    template <typename G, typename... ScalarArgs>
    [[nodiscard]] CompiledSubGraph compile_subgraph(ScalarArgs &&...scalar_args)
    {
        static_assert(sizeof...(ScalarArgs) == StaticGraphSignature<G>::scalar_count(),
                      "compile_subgraph<G>: argument count must match the sub-graph's Scalar parameters "
                      "(in compose order)");
        return subgraph_wiring_detail::compile_subgraph_impl<G>(
            nullptr, {}, std::forward_as_tuple(std::forward<ScalarArgs>(scalar_args)...));
    }

    /** Compile with the enclosing wiring so the child can capture its ports. */
    template <typename G, typename... ScalarArgs>
    [[nodiscard]] CompiledSubGraph compile_subgraph(Wiring &parent, ScalarArgs &&...scalar_args)
    {
        static_assert(sizeof...(ScalarArgs) == StaticGraphSignature<G>::scalar_count(),
                      "compile_subgraph<G>: supply the graph's Scalar parameters in compose order");
        return subgraph_wiring_detail::compile_subgraph_impl<G>(
            &parent, {}, std::forward_as_tuple(std::forward<ScalarArgs>(scalar_args)...));
    }

    struct SwitchBranchGraph
    {
        Value key;
        CompiledSubGraph graph;
        bool takes_key{false};
    };

    enum class MapKeyArg : std::uint8_t { None, First };

    namespace compiled
    {
        /** Wire compiled children directly, without registering standard operators.
         * Child inputs are positional. The optional leading key precedes ordinary
         * inputs. Captured ports are bound whole. Each call owns its compiled graphs.
         */
        [[nodiscard]] HGRAPH_EXPORT WiringPortRef switch_(Wiring &w, WiringPortRef key,
            std::span<const WiringPortRef> inputs, std::vector<SwitchBranchGraph> branches,
            std::optional<SwitchBranchGraph> default_branch = {}, bool reload_on_ticked = false);
        HGRAPH_EXPORT void switch_sink_(Wiring &w, WiringPortRef key, std::span<const WiringPortRef> inputs,
            std::vector<SwitchBranchGraph> branches, std::optional<SwitchBranchGraph> default_branch = {},
            bool reload_on_ticked = false);

        /** The first non-key input is one dictionary element or dynamic list element.
         * Each later input is a broadcast. Explicit lifecycle keys apply to dictionaries.
         */
        [[nodiscard]] HGRAPH_EXPORT WiringPortRef map_(Wiring &w, WiringPortRef collection,
            std::span<const WiringPortRef> broadcast, CompiledSubGraph child,
            MapKeyArg key = MapKeyArg::None, std::optional<WiringPortRef> keys = {});
        HGRAPH_EXPORT void map_sink_(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
            CompiledSubGraph child, MapKeyArg key = MapKeyArg::None, std::optional<WiringPortRef> keys = {});
    }  // namespace compiled
}  // namespace hgraph

#endif  // HGRAPH_TYPES_COMPILED_SUBGRAPH_H
