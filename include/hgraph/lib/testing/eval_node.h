#ifndef HGRAPH_LIB_TESTING_EVAL_NODE_H
#define HGRAPH_LIB_TESTING_EVAL_NODE_H

#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/call_args.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/util/date_time.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph::testing
{
    /**
     * Per-time-series-schema harness trait — the single extension point that lets
     * ``eval_node`` exchange the right per-cycle "harness element" with the test and
     * wire the matching ``replay`` source / ``record`` sink.
     *
     * The harness element is the typed scalar ``T`` for scalar-delta leaves
     * (``TS<T>`` and tick-count ``TSW<T,...>``, so a test writes ``{1, none, 3}``),
     * ``bool`` for ``SIGNAL``, and the **canonical delta ``Value``** for collection
     * kinds (``TSS`` / ``TSD`` / ``TSL`` / …, built by ``set_delta`` / ``dict_delta`` /
     * ``list_delta`` and compared with ``Value::equals``). One adapter covers every
     * schema — ``replay`` / ``record`` are SINGLE erased nodes resolved at wiring
     * (``replay`` from the explicit type ``S``, ``record`` from its connected input
     * port), so no per-kind variant is needed.
     */
    template <typename S> struct harness_element { using type = Value; };
    template <typename T> struct harness_element<TS<T>> { using type = T; };
    template <typename T> struct harness_element<TS<HomogeneousTuple<T>>> { using type = Value; };
    template <typename T, auto... Dimensions>
    struct harness_element<TS<ArrayOf<T, Dimensions...>>> { using type = Value; };
    template <typename T> struct harness_element<TS<SeriesOf<T>>> { using type = Series; };
    template <typename T, typename M> struct harness_element<TS<FrameOf<T, M>>> { using type = Frame; };
    template <typename T, std::size_t Period, std::size_t MinPeriod>
    struct harness_element<TSW<T, Period, MinPeriod>>
    {
        using type = T;
    };
    template <> struct harness_element<SIGNAL> { using type = bool; };

    template <typename S>
    struct ts_harness
    {
        using element                       = typename harness_element<S>::type;
        static constexpr bool is_scalar     = static_node_detail::is_scalar_ts<S>::value;

        // Source: the output type is supplied explicitly (S), giving a typed Port<S>.
        static auto wire_replay(Wiring &w, const std::string &key) { return wire<stdlib::replay_impl, S>(w, key); }

        static void seed(const GlobalStateView &gs, std::string_view key, const std::vector<std::optional<element>> &seq)
        {
            if constexpr (is_scalar) { set_replay_values<element>(gs, key, seq); }
            else { set_replay_deltas(gs, key, seq); }
        }

        // Sink: the input type resolves from the connected port.
        template <typename Port>
        static void wire_record(Wiring &w, Port port, const std::string &key)
        {
            wire<stdlib::dense_record_impl>(w, port, key);
        }

        static std::vector<std::optional<element>> read(const GlobalStateView &gs, std::string_view key)
        {
            if constexpr (is_scalar) { return get_recorded_values<element>(gs, key); }
            else { return get_recorded_deltas(gs, key); }
        }
    };

    /** Value-layer Bundles use their canonical erased ``Value`` as the test
        element. They are scalar TS values at runtime, but unlike a C++ atomic
        there is no plain payload type to construct or extract. */
    template <typename S>
    struct bundle_ts_harness
    {
        using element = Value;

        static auto wire_replay(Wiring &w, const std::string &key)
        {
            return wire<stdlib::replay_impl, S>(w, key);
        }

        static void seed(const GlobalStateView &gs, std::string_view key,
                         const std::vector<std::optional<Value>> &seq)
        {
            set_replay_deltas(gs, key, seq);
        }

        template <typename Port>
        static void wire_record(Wiring &w, Port port, const std::string &key)
        {
            wire<stdlib::dense_record_impl>(w, port, key);
        }

        static std::vector<std::optional<Value>> read(const GlobalStateView &gs,
                                                      std::string_view key)
        {
            return get_recorded_deltas(gs, key);
        }
    };

    template <fixed_string Name, typename... Fields>
    struct ts_harness<TS<Bundle<Name, Fields...>>>
        : bundle_ts_harness<TS<Bundle<Name, Fields...>>>
    {
    };

    template <typename... Fields>
    struct ts_harness<TS<UnNamedBundle<Fields...>>>
        : bundle_ts_harness<TS<UnNamedBundle<Fields...>>>
    {
    };

    template <typename T>
    struct ts_harness<TS<HomogeneousTuple<T>>>
        : bundle_ts_harness<TS<HomogeneousTuple<T>>>
    {
    };

    template <typename T, auto... Dimensions>
    struct ts_harness<TS<ArrayOf<T, Dimensions...>>>
        : bundle_ts_harness<TS<ArrayOf<T, Dimensions...>>>
    {
    };

    namespace eval_node_detail
    {
        inline void copy_completed_global_state(const GraphView &graph)
        {
            if (auto *state = GlobalContext::active_state()) { state->view().copy_from(graph.global_state()); }
        }

        // The node's wire-time parameters (In + Scalar, in eval order).
        template <typename NodeT>
        using wire_params_t = typename StaticNodeSignature<NodeT>::wire_param_types;

        // The node's (single) output time-series schema.
        template <typename NodeT>
        using output_schema_t = typename StaticNodeSignature<NodeT>::output_schema_type;

        template <typename NodeT, typename Params>
        struct first_input_element;

        template <typename NodeT>
        struct first_input_element<NodeT, std::tuple<>>
        {
            static_assert(static_node_detail::always_false_v<NodeT>,
                          "eval_node: NodeT must have at least one time-series input");
            using type = void;
        };

        template <typename NodeT, typename First, typename... Rest>
        struct first_input_element_impl
        {
            static_assert(static_node_detail::always_false_v<NodeT>,
                          "eval_node: the first In/Scalar parameter must be a time-series input");
            using type = void;
        };

        template <typename NodeT, fixed_string Name, typename Schema, auto... Policies, typename... Rest>
        struct first_input_element_impl<NodeT, In<Name, Schema, Policies...>, Rest...>
        {
            using type = typename ts_harness<Schema>::element;
        };

        template <typename NodeT, typename First, typename... Rest>
        struct first_input_element<NodeT, std::tuple<First, Rest...>>
            : first_input_element_impl<NodeT, First, Rest...>
        {
        };

        // A concrete static node has a static ``eval``; an operator marker does not;
        // a graph has a static ``compose``.
        template <typename T>
        concept has_eval = requires { &T::eval; };

        template <typename T>
        concept is_graph = graph_wiring_detail::is_graph_def<T> && !has_eval<T>;

        // ---- graph (compose) reflection for the eval_node graph overload ----
        // Lazily guarded like the node reflection below: these appear in the
        // overload's *signature*, so a non-graph type must not hard-error.
        template <typename GraphT, bool IsGraph, bool HasInput>
        struct graph_first_input_element_impl
        {
            using type = Value;
        };

        template <typename GraphT>
        struct graph_first_input_element_impl<GraphT, true, true>
        {
            using first = std::remove_cvref_t<
                std::tuple_element_t<0, typename StaticGraphSignature<GraphT>::param_types>>;
            static_assert(graph_wiring_detail::is_port<first>::value,
                          "eval_node<G>: the first compose parameter must be a time-series Port");
            using type = typename ts_harness<typename first::schema>::element;
        };

        template <typename GraphT, bool = is_graph<GraphT>>
        struct graph_first_input_element
        {
            using type = Value;
        };
        template <typename GraphT>
        struct graph_first_input_element<GraphT, true>
            : graph_first_input_element_impl<GraphT, true, (StaticGraphSignature<GraphT>::input_count() >= 1)>
        {
        };

        template <typename P>
        struct port_schema_or_void
        {
            using type = void;   // void compose return / non-port — rejected in the body
        };
        template <typename S>
        struct port_schema_or_void<Port<S>>
        {
            using type = S;
        };

        template <typename GraphT, bool = is_graph<GraphT>>
        struct graph_output_element
        {
            using type = Value;
        };
        template <typename GraphT>
        struct graph_output_element<GraphT, true>
        {
            using out_port = std::remove_cvref_t<typename StaticGraphSignature<GraphT>::output_type>;
            using schema   = typename port_schema_or_void<out_port>::type;
            using type     = std::conditional_t<std::is_void_v<schema>, Value,
                                                typename ts_harness<schema>::element>;
        };

        // ``first_input_element_t`` / ``output_element_t`` appear in the *signature* of the
        // concrete ``eval_node`` overload. For an operator marker (no ``eval``) those
        // ``StaticNodeSignature``-based computations would be a hard error (not SFINAE), so
        // they are guarded: a non-eval type falls back to ``Value`` and the concrete overload
        // is removed by its ``operator_tag`` constraint instead of failing to compile.
        template <typename NodeT, bool HasInput>
        struct first_input_element_from_signature
        {
            using type = Value;
        };
        template <typename NodeT>
        struct first_input_element_from_signature<NodeT, true>
        {
            using type = typename first_input_element<NodeT, wire_params_t<NodeT>>::type;
        };

        template <typename NodeT, bool = has_eval<NodeT>>
        struct first_input_element_lazy
        {
            using type = Value;
        };
        template <typename NodeT>
        struct first_input_element_lazy<NodeT, true>
            : first_input_element_from_signature<NodeT, (StaticNodeSignature<NodeT>::input_count() >= 1)>
        {
        };

        template <typename NodeT, bool = has_eval<NodeT>>
        struct output_element_lazy
        {
            using type = Value;
        };
        template <typename NodeT>
        struct output_element_lazy<NodeT, true>
        {
            using type = typename ts_harness<output_schema_t<NodeT>>::element;
        };

        // Per-cycle harness element of the first input / the output (T, SetDelta<T>, ...).
        template <typename NodeT>
        using first_input_element_t = typename first_input_element_lazy<NodeT>::type;

        template <typename NodeT>
        using output_element_t = typename output_element_lazy<NodeT>::type;

        template <typename A>
        using payload_t = call_args_detail::payload_t<A>;
        template <typename A>
        inline constexpr bool is_named_arg_v = call_args_detail::is_named_arg_v<A>;
        using call_args_detail::payload_at;
        using call_args_detail::payload_of;

        // A time-series input argument to the operator harness is a ``vector<optional<T>>``;
        // any other argument is a wiring-time scalar.
        template <typename A>
        struct is_optional_vector : std::false_type
        {
        };
        template <typename T>
        struct is_optional_vector<std::vector<std::optional<T>>> : std::true_type
        {
        };
        template <typename A>
        struct optional_vector_element;
        template <typename T>
        struct optional_vector_element<std::vector<std::optional<T>>>
        {
            using type = T;
        };

        template <typename... A>
        inline constexpr bool any_optional_vector_v =
            (false || ... || is_optional_vector<payload_t<A>>::value);

        // A ``vector<optional<Value>>`` input is a canonical-delta sequence and
        // carries no schema of its own; the schema is supplied positionally via
        // ``eval_node``'s explicit template arguments (same convention as the
        // explicit output schema on the source-style overload).
        template <typename A>
        struct is_value_sequence : std::false_type
        {
        };
        template <>
        struct is_value_sequence<std::vector<std::optional<Value>>> : std::true_type
        {
        };
        template <typename A>
        inline constexpr bool is_value_sequence_v = is_value_sequence<payload_t<A>>::value;

        template <typename... A>
        [[nodiscard]] consteval std::size_t value_sequence_count()
        {
            return (std::size_t{0} + ... + (is_value_sequence_v<A> ? std::size_t{1} : std::size_t{0}));
        }

        // The number of canonical-delta inputs before argument ``I`` — the index of
        // ``I``'s schema within the explicit template-argument list.
        template <std::size_t I, typename... A>
        [[nodiscard]] consteval std::size_t value_sequences_before()
        {
            return []<std::size_t... K>(std::index_sequence<K...>) {
                return (std::size_t{0} + ... +
                        (is_value_sequence_v<std::tuple_element_t<K, std::tuple<A...>>> ? std::size_t{1}
                                                                                        : std::size_t{0}));
            }(std::make_index_sequence<I>{});
        }

        inline std::string input_key(std::size_t index) { return "eval_node::in" + std::to_string(index); }

        template <typename T>
        void label_if_named(GraphBuilder &graph_builder)
        {
            if constexpr (static_node_detail::has_name<T>) { graph_builder.label(std::string{T::name}); }
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename ArgsTuple>
        [[nodiscard]] decltype(auto) bound_payload_at(const ArgsTuple &args,
                                                      [[maybe_unused]] std::string_view call_name)
        {
            using Param = std::tuple_element_t<ParamIndex, ParamsTuple>;
            constexpr std::size_t arg_index =
                call_args_detail::bound_arg_index<ParamIndex, ParamsTuple, ArgsTuple>();
            if constexpr (arg_index == call_args_detail::npos)
            {
                // Compile-time failure (a runtime throw here would return void and
                // cascade into wire<>'s overload set — an unreadable error). The
                // failed requirement names the missing parameter type.
                static_assert(call_args_detail::missing_required_argument<Param>::value,
                              "eval_node/wire: missing required argument — the "
                              "'missing_required_argument<…>' type in this error names the In<\"name\", …> / "
                              "Scalar<\"name\", T> parameter that was not supplied; pass it positionally, by "
                              "name via arg<\"name\">(value), or give the node a defaults() entry");
            }
            else
            {
                return call_args_detail::payload_at<arg_index>(args);
            }
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename ArgsTuple, typename DefaultsTuple>
        [[nodiscard]] decltype(auto) bound_payload_or_default_at(const ArgsTuple &args,
                                                                 const DefaultsTuple &defaults,
                                                                 [[maybe_unused]] std::string_view call_name)
        {
            using Param = std::tuple_element_t<ParamIndex, ParamsTuple>;
            constexpr std::size_t arg_index =
                call_args_detail::bound_arg_index<ParamIndex, ParamsTuple, ArgsTuple>();
            constexpr std::size_t default_index =
                call_args_detail::default_arg_index<ParamIndex, ParamsTuple, DefaultsTuple>();
            if constexpr (arg_index != call_args_detail::npos)
            {
                return call_args_detail::payload_at<arg_index>(args);
            }
            else if constexpr (default_index != call_args_detail::npos)
            {
                return call_args_detail::payload_at<default_index>(defaults);
            }
            else
            {
                // Compile-time failure (a runtime throw here would return void and
                // cascade into wire<>'s overload set — an unreadable error). The
                // failed requirement names the missing parameter type.
                static_assert(call_args_detail::missing_required_argument<Param>::value,
                              "eval_node/wire: missing required argument — the "
                              "'missing_required_argument<…>' type in this error names the In<\"name\", …> / "
                              "Scalar<\"name\", T> parameter that was not supplied; pass it positionally, by "
                              "name via arg<\"name\">(value), or give the node a defaults() entry");
            }
        }

        template <typename NodeT, typename... Args>
        [[nodiscard]] std::vector<std::optional<output_element_t<NodeT>>> eval_source_node(Args &&...args)
        {
            using sig         = StaticNodeSignature<NodeT>;
            using wire_params = wire_params_t<NodeT>;
            using out_schema  = output_schema_t<NodeT>;
            constexpr std::size_t arg_count = sizeof...(Args);

            static_assert(sig::output_count() == 1, "eval_node: source NodeT must have exactly one output");
            static_assert(arg_count <= std::tuple_size_v<wire_params>,
                          "eval_node: too many arguments for the source node's Scalar parameters");

            Wiring w;
            auto   all          = std::forward_as_tuple(std::forward<Args>(args)...);
            auto   default_args = call_args_detail::default_args_for<NodeT>();
            call_args_detail::validate_call_args<wire_params>("eval_node<NodeT>", all, default_args);
            auto   out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
                return wire<NodeT>(
                    w, bound_payload_or_default_at<I, wire_params>(all, default_args, "eval_node<NodeT>")...);
            }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
            ts_harness<out_schema>::wire_record(w, out_port, std::string{"eval_node::out"});

            GraphBuilder gb = std::move(w).finish();
            label_if_named<NodeT>(gb);
            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
            GraphExecutorValue executor = eb.make_executor();
            auto               view     = executor.view();
            view.run();
            copy_completed_global_state(view.graph());

            return ts_harness<out_schema>::read(view.graph().global_state(), "eval_node::out");
        }

        template <typename NodeT, typename... Args>
        [[nodiscard]] std::vector<std::optional<output_element_t<NodeT>>> eval_input_node(Args &&...args)
        {
            using sig         = StaticNodeSignature<NodeT>;
            using wire_params = wire_params_t<NodeT>;
            using out_schema  = output_schema_t<NodeT>;
            constexpr std::size_t arg_count = sizeof...(Args);

            static_assert(sig::output_count() == 1, "eval_node: NodeT must have exactly one output");
            static_assert(sig::input_count() >= 1, "eval_node: NodeT must have at least one time-series input");
            static_assert(arg_count <= std::tuple_size_v<wire_params>,
                          "eval_node: too many arguments for the node's In + Scalar parameters");
            static_assert(static_node_detail::is_input_selector<std::tuple_element_t<0, wire_params>>::value,
                          "eval_node: the first In/Scalar parameter must be a time-series input");

            Wiring w;
            auto   all          = std::forward_as_tuple(std::forward<Args>(args)...);
            auto   default_args = call_args_detail::default_args_for<NodeT>();
            call_args_detail::validate_call_args<wire_params>("eval_node<NodeT>", all, default_args);

            // Pass 1: wire the matching replay per time-series input (returning its
            // port) and pass scalar payloads straight through, building wire<NodeT>.
            auto wire_arg = [&]<std::size_t I>() {
                using P = std::tuple_element_t<I, wire_params>;
                if constexpr (static_node_detail::is_input_selector<P>::value)
                {
                    return ts_harness<typename P::schema>::wire_replay(w, input_key(I));
                }
                else
                {
                    return bound_payload_or_default_at<I, wire_params>(all, default_args, "eval_node<NodeT>");
                }
            };

            auto out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
                return wire<NodeT>(w, wire_arg.template operator()<I>()...);
            }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});

            ts_harness<out_schema>::wire_record(w, out_port, std::string{"eval_node::out"});
            GraphBuilder gb = std::move(w).finish();
            label_if_named<NodeT>(gb);

            // Pass 2: seed each time-series input's replay buffer (same key per
            // slot) and track the longest input.
            std::size_t max_len = 0;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::tuple_element_t<I, wire_params>;
                        if constexpr (static_node_detail::is_input_selector<P>::value)
                        {
                            const auto &seq = bound_payload_at<I, wire_params>(all, "eval_node<NodeT>");
                            max_len         = std::max(max_len, seq.size());
                            ts_harness<typename P::schema>::seed(gb.global_state(), input_key(I), seq);
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});

            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
            GraphExecutorValue executor = eb.make_executor();
            auto               view     = executor.view();
            view.run();
            copy_completed_global_state(view.graph());

            // Cycle-align: pad to the longest input, never truncate (see record).
            auto out = ts_harness<out_schema>::read(view.graph().global_state(), "eval_node::out");
            if (out.size() < max_len) { out.resize(max_len); }
            return out;
        }

        template <typename GraphT, typename... Args>
        [[nodiscard]] std::vector<std::optional<typename graph_output_element<GraphT>::type>>
        eval_source_graph(Args &&...args)
        {
            using sig        = StaticGraphSignature<GraphT>;
            using params     = typename sig::param_types;
            using out_schema = typename graph_output_element<GraphT>::schema;
            constexpr std::size_t arg_count = sizeof...(Args);

            static_assert(sig::input_count() == 0,
                          "eval_node<G>: source-style graph must not have time-series inputs");
            static_assert(arg_count <= sig::param_count(),
                          "eval_node<G>: too many arguments for the graph's Scalar parameters");
            static_assert(!std::is_void_v<std::remove_cvref_t<typename sig::output_type>>,
                          "eval_node<G>: the graph must return an output port");

            Wiring w;
            auto   all          = std::forward_as_tuple(std::forward<Args>(args)...);
            auto   default_args = call_args_detail::default_args_for<GraphT>();
            call_args_detail::validate_call_args<params>("eval_node<G>", all, default_args);

            auto wire_arg = [&]<std::size_t I>() {
                using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                static_assert(!graph_wiring_detail::is_port<P>::value,
                              "eval_node<G>: source-style graph cannot have time-series inputs");
                return graph_wiring_detail::make_scalar_param<P>(
                    bound_payload_or_default_at<I, params>(all, default_args, "eval_node<G>"));
            };

            auto out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
                return GraphT::compose(w, wire_arg.template operator()<I>()...);
            }(std::make_index_sequence<sig::param_count()>{});

            ts_harness<out_schema>::wire_record(w, out_port, std::string{"eval_node::out"});
            GraphBuilder gb = std::move(w).finish();
            label_if_named<GraphT>(gb);

            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
            GraphExecutorValue executor = eb.make_executor();
            auto               view     = executor.view();
            view.run();
            copy_completed_global_state(view.graph());

            return ts_harness<out_schema>::read(view.graph().global_state(), "eval_node::out");
        }

        template <typename GraphT, typename... Args>
        [[nodiscard]] std::vector<std::optional<typename graph_output_element<GraphT>::type>>
        eval_input_graph(Args &&...args)
        {
            using sig    = StaticGraphSignature<GraphT>;
            using params = typename sig::param_types;
            constexpr std::size_t arg_count = sizeof...(Args);

            static_assert(sig::input_count() >= 1, "eval_node<G>: the graph must have at least one time-series input");
            static_assert(arg_count <= sig::param_count(),
                          "eval_node<G>: too many arguments for the graph's Port + Scalar parameters");

            using out_schema = typename graph_output_element<GraphT>::schema;
            static_assert(!std::is_void_v<std::remove_cvref_t<typename sig::output_type>>,
                          "eval_node<G>: the graph must return an output port");

            Wiring w;
            auto   all          = std::forward_as_tuple(std::forward<Args>(args)...);
            auto   default_args = call_args_detail::default_args_for<GraphT>();
            call_args_detail::validate_call_args<params>("eval_node<G>", all, default_args);

            // Pass 1: wire the matching replay per Port parameter (returning the
            // typed port compose expects) and pass scalar payloads straight through.
            auto wire_arg = [&]<std::size_t I>() {
                using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                if constexpr (graph_wiring_detail::is_port<P>::value)
                {
                    static_assert(!graph_wiring_detail::is_erased_port<P>::value,
                                  "eval_node<G>: graph time-series inputs must declare concrete schemas");
                    return ts_harness<typename P::schema>::wire_replay(w, input_key(I));
                }
                else
                {
                    return graph_wiring_detail::make_scalar_param<P>(
                        bound_payload_or_default_at<I, params>(all, default_args, "eval_node<G>"));
                }
            };

            auto out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
                return GraphT::compose(w, wire_arg.template operator()<I>()...);
            }(std::make_index_sequence<sig::param_count()>{});

            if constexpr (std::is_void_v<out_schema>) { wire<stdlib::dense_record_impl>(w, out_port, std::string{"eval_node::out"}); }
            else { ts_harness<out_schema>::wire_record(w, out_port, std::string{"eval_node::out"}); }
            GraphBuilder gb = std::move(w).finish();
            label_if_named<GraphT>(gb);

            // Pass 2: seed each Port parameter's replay buffer; track the longest input.
            std::size_t max_len = 0;
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                (
                    [&] {
                        using P = std::remove_cvref_t<std::tuple_element_t<I, params>>;
                        if constexpr (graph_wiring_detail::is_port<P>::value)
                        {
                            const auto &seq = bound_payload_at<I, params>(all, "eval_node<G>");
                            max_len         = std::max(max_len, seq.size());
                            ts_harness<typename P::schema>::seed(gb.global_state(), input_key(I), seq);
                        }
                    }(),
                    ...);
            }(std::make_index_sequence<sig::param_count()>{});

            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
            GraphExecutorValue executor = eb.make_executor();
            auto               view     = executor.view();
            view.run();
            copy_completed_global_state(view.graph());

            auto out = [&] {
                if constexpr (std::is_void_v<out_schema>)
                {
                    return get_recorded_deltas(view.graph().global_state(), "eval_node::out");
                }
                else
                {
                    return ts_harness<out_schema>::read(view.graph().global_state(), "eval_node::out");
                }
            }();
            if (out.size() < max_len) { out.resize(max_len); }
            return out;
        }
    }  // namespace eval_node_detail

    /**
     * Evaluate a source-style static node: no time-series input, exactly one output.
     *
     * This is the source counterpart of the input-driven ``eval_node`` overload below.
     * Scalar arguments are passed in the node's wire-parameter order, the output is recorded,
     * and the returned sequence contains every output tick produced until quiescence.
     */
    template <typename NodeT, typename... Args>
        requires(!std::derived_from<NodeT, operator_tag> &&
                 eval_node_detail::has_eval<NodeT> &&
                 StaticNodeSignature<NodeT>::input_count() == 0)
    [[nodiscard]] std::vector<std::optional<eval_node_detail::output_element_t<NodeT>>>
    eval_node(Args &&...args)
    {
        return eval_node_detail::eval_source_node<NodeT>(std::forward<Args>(args)...);
    }

    /**
     * Evaluate a node over per-cycle inputs and return its per-cycle outputs — the
     * C++ counterpart of the Python ``eval_node`` harness.
     *
     * Arguments are given in the node's **eval-parameter order** (its ``In`` and
     * ``Scalar`` parameters): a **time-series input** is a
     * ``std::vector<std::optional<E>>`` where ``E`` is that input's harness element
     * (``T`` for scalar-delta leaves, ``bool`` for ``SIGNAL``, and the canonical
     * delta ``Value`` for collection kinds such as ``TSS`` / ``TSD`` / fixed or
     * dynamic ``TSL`` — see :cpp:class:`ts_harness`),
     * one element per evaluation cycle from ``MIN_ST`` (``none`` = no tick), and a
     * **scalar input** is the value itself. The harness wires the matching ``replay``
     * per time-series input, the node, then the matching ``record`` on the output,
     * seeds the inputs, runs to quiescence, and returns the recorded output
     * (cycle-aligned, at least as long as the longest input, never truncated — a node
     * may emit beyond the input window).
     *
     * The **first** parameter must be a time-series input (so it can be a braced
     * list — its element type is inferred from the node); any later time-series
     * inputs are passed as ``std::vector<std::optional<E>>``. The node must have
     * exactly one output. Scalar (``TS``) and any container kind supported by the
     * erased replay/record delta path are accepted on inputs and the output.
     */
    template <typename NodeT, typename... Rest>
        requires(!std::derived_from<NodeT, operator_tag> && eval_node_detail::has_eval<NodeT>)
    [[nodiscard]] std::vector<std::optional<eval_node_detail::output_element_t<NodeT>>>
    eval_node(const std::vector<std::optional<eval_node_detail::first_input_element_t<NodeT>>> &input0, Rest &&...rest)
    {
        return eval_node_detail::eval_input_node<NodeT>(input0, std::forward<Rest>(rest)...);
    }

    template <typename NodeT, typename First, typename... Rest>
        requires(!std::derived_from<NodeT, operator_tag> &&
                 eval_node_detail::has_eval<NodeT> &&
                 StaticNodeSignature<NodeT>::input_count() >= 1 &&
                 eval_node_detail::is_named_arg_v<First> &&
                 eval_node_detail::any_optional_vector_v<First, Rest...>)
    [[nodiscard]] std::vector<std::optional<eval_node_detail::output_element_t<NodeT>>>
    eval_node(First &&first, Rest &&...rest)
    {
        return eval_node_detail::eval_input_node<NodeT>(std::forward<First>(first), std::forward<Rest>(rest)...);
    }

    /**
     * Evaluate an **operator** over per-cycle inputs — the operator counterpart of
     * ``eval_node``. The overload to wire is resolved **at wiring time** from the supplied
     * argument schemas (operator dispatch), and the resolved port already carries the
     * output schema; the result is therefore returned **type-erased** as the per-cycle
     * canonical ``Value`` deltas and is compared with ``Value`` equality (``CHECK_OUTPUT``).
     *
     * Arguments are in the operator's call order: a **time-series input** is a
     * ``std::vector<std::optional<E>>`` — the scalar leaf ``E`` implies ``TS<E>``
     * (build it with ``testing::values<T>(...)``), while a canonical-delta sequence
     * (``E = Value``, built with ``values<Value>(list_delta..., ...)``) takes its
     * time-series schema from the **explicit template arguments**, in input order —
     * the same convention as the explicit output schema on the source-style
     * overloads (e.g. ``eval_node<reduce_, TSL<TS<Int>, 5>>(fn<add_>(), in)``). A
     * **scalar input** is the value itself (including a ``WiredFn`` — ``fn<X>()`` —
     * for higher-order operators). Scope: the operator must have at least one
     * time-series input and exactly one output — sinks (no output) and sources (no
     * time-series input) use the source-style overloads below.
     */
    template <typename Op, typename... InSchemas, typename... Args>
        requires(std::derived_from<Op, operator_tag> && eval_node_detail::any_optional_vector_v<Args...>)
    [[nodiscard]] std::vector<std::optional<Value>> eval_node(Args &&...args)
    {
        static_assert(Op::has_output, "eval_node<Op>: the operator must have an output (sinks are out of scope)");
        static_assert(sizeof...(InSchemas) == eval_node_detail::value_sequence_count<Args...>(),
                      "eval_node<Op, InSchemas...>: supply exactly one time-series schema per canonical-delta "
                      "(vector<optional<Value>>) input, in input order");

        constexpr std::size_t arg_count = sizeof...(Args);
        using in_schemas                = std::tuple<InSchemas...>;
        Wiring w;
        auto   all = std::forward_as_tuple(std::forward<Args>(args)...);

        // Pass 1: wire a replay per time-series input (vector<optional<T>> -> TS<T>;
        // vector<optional<Value>> -> the next explicit schema, in order), pass scalar
        // values straight through, dispatch the operator, then record its output.
        auto wire_arg = [&]<std::size_t I>() {
            using A0 = std::remove_cvref_t<
                std::tuple_element_t<I, std::remove_reference_t<decltype(all)>>>;
            using A              = eval_node_detail::payload_t<A0>;
            [[maybe_unused]] constexpr bool named = eval_node_detail::is_named_arg_v<A0>;
            auto rewrap          = [&](auto port) {
                if constexpr (named)
                {
                    return NamedArg<decltype(port)>{std::get<I>(all).name, std::move(port)};
                }
                else { return port; }
            };
            if constexpr (eval_node_detail::is_value_sequence<A>::value)
            {
                using S = std::tuple_element_t<eval_node_detail::value_sequences_before<I, Args...>(), in_schemas>;
                return rewrap(ts_harness<S>::wire_replay(w, eval_node_detail::input_key(I)));
            }
            else if constexpr (eval_node_detail::is_optional_vector<A>::value)
            {
                using T = typename eval_node_detail::optional_vector_element<A>::type;
                return rewrap(wire<stdlib::replay_impl, TS<T>>(w, eval_node_detail::input_key(I)));
            }
            else
            {
                return std::get<I>(all);  // scalar value (named or not — wire<> handles both)
            }
        };

        auto out_port = [&]<std::size_t... I>(std::index_sequence<I...>) {
            return wire<Op>(w, wire_arg.template operator()<I>()...);
        }(std::make_index_sequence<arg_count>{});

        wire<stdlib::dense_record_impl>(w, out_port, std::string{"eval_node::out"});
        GraphBuilder gb = std::move(w).finish();
        eval_node_detail::label_if_named<Op>(gb);

        // Pass 2: seed each time-series input's replay buffer; track the longest input.
        std::size_t max_len = 0;
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (
                [&] {
                    using A0 = std::remove_cvref_t<
                        std::tuple_element_t<I, std::remove_reference_t<decltype(all)>>>;
                    using A  = eval_node_detail::payload_t<A0>;
                    if constexpr (eval_node_detail::is_value_sequence<A>::value)
                    {
                        const auto &seq = eval_node_detail::payload_of(std::get<I>(all));
                        max_len         = std::max(max_len, seq.size());
                        set_replay_deltas(gb.global_state(), eval_node_detail::input_key(I), seq);
                    }
                    else if constexpr (eval_node_detail::is_optional_vector<A>::value)
                    {
                        using T         = typename eval_node_detail::optional_vector_element<A>::type;
                        const auto &seq = eval_node_detail::payload_of(std::get<I>(all));
                        max_len         = std::max(max_len, seq.size());
                        set_replay_values<T>(gb.global_state(), eval_node_detail::input_key(I), seq);
                    }
                }(),
                ...);
        }(std::make_index_sequence<arg_count>{});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();
        eval_node_detail::copy_completed_global_state(view.graph());

        // Type-erased per-cycle deltas, read at the wiring-resolved output schema; pad to
        // the longest input, never truncate.
        auto out = get_recorded_deltas(view.graph().global_state(), "eval_node::out");
        if (out.size() < max_len) { out.resize(max_len); }
        return out;
    }

    /**
     * Evaluate a source-style operator: scalar arguments only, exactly one output.
     *
     * Use ``eval_node<Op>(args...)`` when the operator can resolve its output from its
     * scalar arguments, or ``eval_node<Op, OutSchema>(args...)`` when the output must be
     * supplied explicitly, mirroring ``wire<Op, OutSchema>(w, args...)``.
     */
    template <typename Op, typename... Args>
        requires(std::derived_from<Op, operator_tag> &&
                 Op::has_output &&
                 !eval_node_detail::any_optional_vector_v<Args...>)
    [[nodiscard]] std::vector<std::optional<Value>> eval_node(Args &&...args)
    {
        Wiring w;
        auto   out_port = wire<Op>(w, std::forward<Args>(args)...);
        wire<stdlib::dense_record_impl>(w, out_port, std::string{"eval_node::out"});

        GraphBuilder gb = std::move(w).finish();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();
        eval_node_detail::copy_completed_global_state(view.graph());

        return get_recorded_deltas(view.graph().global_state(), "eval_node::out");
    }

    template <typename Op, typename OutSchema, typename... Args>
        requires(std::derived_from<Op, operator_tag> &&
                 Op::has_output &&
                 !std::is_void_v<OutSchema> &&
                 !eval_node_detail::any_optional_vector_v<Args...>)
    [[nodiscard]] std::vector<std::optional<Value>> eval_node(Args &&...args)
    {
        Wiring w;
        auto   out_port = wire<Op, OutSchema>(w, std::forward<Args>(args)...);
        wire<stdlib::dense_record_impl>(w, out_port, std::string{"eval_node::out"});

        GraphBuilder gb = std::move(w).finish();
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MAX_ET);
        GraphExecutorValue executor = eb.make_executor();
        auto               view     = executor.view();
        view.run();
        eval_node_detail::copy_completed_global_state(view.graph());

        return get_recorded_deltas(view.graph().global_state(), "eval_node::out");
    }

    /**
     * Evaluate a source-style graph — a struct with
     * ``compose(Wiring &, Scalar...)`` that returns an output port. This is the
     * graph counterpart of the source-node overload above.
     */
    template <typename GraphT, typename... Args>
        requires(!std::derived_from<GraphT, operator_tag> &&
                 eval_node_detail::is_graph<GraphT> &&
                 StaticGraphSignature<GraphT>::input_count() == 0)
    [[nodiscard]] std::vector<std::optional<typename eval_node_detail::graph_output_element<GraphT>::type>>
    eval_node(Args &&...args)
    {
        return eval_node_detail::eval_source_graph<GraphT>(std::forward<Args>(args)...);
    }

    /**
     * Evaluate a **graph** over per-cycle inputs — a struct with a static
     * ``compose(Wiring &, Port..., Scalar...)`` that returns an output port.
     * The harness wires a ``replay`` per declared ``Port`` parameter, passes
     * scalar values straight through, runs ``compose``, records the returned
     * output, and reads it back typed (or as canonical ``Value`` deltas when
     * the graph returns an erased ``Port<void>``). Same call shape as the node
     * overload: arguments in compose-parameter order, the first parameter a
     * time-series input.
     */
    template <typename GraphT, typename... Rest>
        requires(!std::derived_from<GraphT, operator_tag> && eval_node_detail::is_graph<GraphT>)
    [[nodiscard]] std::vector<std::optional<typename eval_node_detail::graph_output_element<GraphT>::type>>
    eval_node(const std::vector<std::optional<typename eval_node_detail::graph_first_input_element<GraphT>::type>> &input0,
              Rest &&...rest)
    {
        return eval_node_detail::eval_input_graph<GraphT>(input0, std::forward<Rest>(rest)...);
    }

    template <typename GraphT, typename First, typename... Rest>
        requires(!std::derived_from<GraphT, operator_tag> &&
                 eval_node_detail::is_graph<GraphT> &&
                 StaticGraphSignature<GraphT>::input_count() >= 1 &&
                 eval_node_detail::is_named_arg_v<First> &&
                 eval_node_detail::any_optional_vector_v<First, Rest...>)
    [[nodiscard]] std::vector<std::optional<typename eval_node_detail::graph_output_element<GraphT>::type>>
    eval_node(First &&first, Rest &&...rest)
    {
        return eval_node_detail::eval_input_graph<GraphT>(std::forward<First>(first), std::forward<Rest>(rest)...);
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_EVAL_NODE_H
