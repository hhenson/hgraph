#ifndef HGRAPH_TYPES_ADAPTOR_WIRING_H
#define HGRAPH_TYPES_ADAPTOR_WIRING_H

#include <hgraph/runtime/shared_output_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/type_pattern.h>

#include <array>
#include <concepts>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <typeindex>
#include <type_traits>
#include <utility>

namespace hgraph::adaptor
{
    struct interface
    {
    };

    /** Alias of the shared ``hgraph::BoundaryPath`` (RFC 0011 step 8): the
     *  two path types were field-identical, and a single implementation can
     *  only span services and adaptors if they are one type. */
    using AdaptorPath = BoundaryPath;

    [[nodiscard]] inline AdaptorPath path(std::string_view value)
    {
        if (value.empty()) { throw std::invalid_argument("adaptor path must not be empty"); }
        return AdaptorPath{std::string{value}};
    }

    template <typename... Args>
        requires(sizeof...(Args) > 0)
    [[nodiscard]] inline AdaptorPath path(std::string_view value, const Args &...args)
    {
        if (value.empty()) { throw std::invalid_argument("adaptor path must not be empty"); }
        auto typed = wiring_path_detail::typed_path_value(value, args...);
        return AdaptorPath{std::move(typed.value), std::move(typed.resolution), typed.has_typed_suffix};
    }

    /**
     * C++ adaptor wiring.
     *
     * ``Interface`` is a descriptor type:
     *
     * .. code-block:: cpp
     *
     *    struct Loopback {
     *        static constexpr std::string_view name{"loopback"};
     *        using input_schema = TS<Int>;
     *        using output_schema = TS<Int>;
     *    };
     *
     * ``register_adaptor<Loopback, Impl>(w)`` wires ``Impl`` with an
     * implementation-side graph. Client code calls ``wire<Loopback>(w, input)``;
     * adaptor implementations call ``from_graph<Loopback>(w)`` to fetch client
     * input and ``to_graph<Loopback>(w, output)`` to publish their output.
     * Source-only and sink-only descriptors omit ``input_schema`` or
     * ``output_schema`` respectively.
     */
    namespace detail
    {
        template <typename Interface, typename = void>
        struct has_input_schema : std::false_type
        {
        };

        template <typename Interface>
        struct has_input_schema<Interface, std::void_t<typename Interface::input_schema>> : std::true_type
        {
        };

        template <typename Interface, typename = void>
        struct has_output_schema : std::false_type
        {
        };

        template <typename Interface>
        struct has_output_schema<Interface, std::void_t<typename Interface::output_schema>> : std::true_type
        {
        };

        template <typename Interface>
        using input_schema_t = typename Interface::input_schema;

        template <typename Interface>
        using output_schema_t = typename Interface::output_schema;

        template <typename Schema>
        void bind_schema_resolution(ResolutionMap &resolution,
                                    const TSValueTypeMetaData *concrete,
                                    std::string_view context)
        {
            if constexpr (!schema_descriptor<Schema>::is_concrete())
            {
                if (concrete == nullptr || !input_ts_pattern_match(to_pattern<Schema>(), concrete, resolution))
                {
                    throw std::invalid_argument(
                        std::string{context} + " does not match generic adaptor schema " +
                        ts_pattern_to_string(to_pattern<Schema>()));
                }
            }
        }

        template <typename Schema>
        [[nodiscard]] const TSValueTypeMetaData *resolved_schema_meta(const ResolutionMap &resolution,
                                                                      std::string_view context)
        {
            if constexpr (schema_descriptor<Schema>::is_concrete())
            {
                return schema_descriptor<Schema>::ts_meta();
            }
            else
            {
                const auto *meta = ts_pattern_resolve(to_pattern<Schema>(), resolution);
                if (meta == nullptr)
                {
                    throw std::invalid_argument(
                        std::string{context} + " has unresolved generic adaptor schema " +
                        ts_pattern_to_string(to_pattern<Schema>()));
                }
                return meta;
            }
        }

        template <typename Interface>
        concept adaptor_interface =
            std::derived_from<Interface, interface> &&
            (has_input_schema<Interface>::value || has_output_schema<Interface>::value);

        template <typename Impl>
        concept graph_implementation = requires { &Impl::compose; };

        template <typename Impl>
        concept node_implementation = requires { &Impl::eval; };

        template <typename Impl, bool IsGraph = graph_implementation<Impl>, bool IsNode = node_implementation<Impl>>
        struct implementation_params
        {
            using type = std::tuple<>;
        };

        template <typename Impl>
        struct implementation_params<Impl, true, false>
        {
            using type = typename StaticGraphSignature<Impl>::param_types;
        };

        template <typename Impl>
        struct implementation_params<Impl, false, true>
        {
            using type = typename StaticNodeSignature<Impl>::wire_param_types;
        };

        template <typename Impl>
        using implementation_params_t = typename implementation_params<Impl>::type;

        template <typename T>
        struct is_path_scalar : std::false_type
        {
        };

        template <fixed_string Name, typename T>
        struct is_path_scalar<Scalar<Name, T>>
            : std::bool_constant<Name.sv() == std::string_view{"path"} && std::same_as<T, Str>>
        {
        };

        template <typename Params, std::size_t... I>
        [[nodiscard]] consteval bool has_path_scalar(std::index_sequence<I...>)
        {
            return (false || ... || is_path_scalar<std::tuple_element_t<I, Params>>::value);
        }

        template <typename Impl>
        [[nodiscard]] consteval bool implementation_accepts_path()
        {
            using params = implementation_params_t<Impl>;
            return has_path_scalar<params>(std::make_index_sequence<std::tuple_size_v<params>>{});
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_name()
        {
            std::string_view name{Interface::name};
            if (name.empty()) { throw std::invalid_argument("adaptor name must not be empty"); }
            return std::string{name};
        }

        template <typename Interface>
        [[nodiscard]] AdaptorPath default_adaptor_path()
        {
            if constexpr (requires { std::string_view{Interface::default_path}; })
            {
                return path(std::string_view{Interface::default_path});
            }
            else
            {
                std::string value = adaptor_name<Interface>();
                value.append("_default");
                return AdaptorPath{std::move(value)};
            }
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_base_path(const AdaptorPath &user_path)
        {
            constexpr std::string_view prefix{"adaptor://"};
            if (user_path.value.starts_with(prefix)) { return user_path.value; }
            std::string full{prefix};
            full.append(user_path.value);
            full.push_back('/');
            full.append(adaptor_name<Interface>());
            return full;
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_from_graph_path(const AdaptorPath &user_path)
        {
            std::string full = adaptor_base_path<Interface>(user_path);
            full.append("/from_graph");
            return full;
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_to_graph_path(const AdaptorPath &user_path)
        {
            std::string full = adaptor_base_path<Interface>(user_path);
            full.append("/to_graph");
            return full;
        }

        template <typename Interface>
        void append_required_stub_endpoints(std::vector<WiringServiceImplementationEndpoint> &endpoints,
                                            const AdaptorPath &user_path)
        {
            if constexpr (has_input_schema<Interface>::value)
            {
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    adaptor_from_graph_path<Interface>(user_path), user_path.resolution});
            }
            if constexpr (has_output_schema<Interface>::value)
            {
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    adaptor_to_graph_path<Interface>(user_path), user_path.resolution});
            }
        }

        template <typename Interface, typename InputSchema>
        [[nodiscard]] AdaptorPath resolve_client_path(AdaptorPath user_path, const Port<InputSchema> &input)
        {
            ResolutionMap inferred = user_path.resolution;
            bind_schema_resolution<input_schema_t<Interface>>(
                inferred, input.erased().schema, "adaptor input");
            return wiring_path_detail::with_resolution(std::move(user_path), inferred);
        }

        [[nodiscard]] inline Value path_key_value(const std::string &full_path)
        {
            // Shared implementation (RFC 0011 step 8).
            return wiring_path_detail::boundary_path_key(full_path);
        }

        // Per-ROLE identity markers (the services runtime-identity ruling,
        // 2026-07-05): the adaptor identity rides the name-qualified path.
        struct input_stub_source_marker
        {
        };

        struct input_capture_marker
        {
        };

        struct output_source_marker
        {
        };

        struct output_capture_marker
        {
        };

        template <typename Interface>
        [[nodiscard]] Port<REF<input_schema_t<Interface>>> input_stub_source(Wiring &w, const AdaptorPath &user_path)
        {
            using input_schema = input_schema_t<Interface>;

            std::string full_path = adaptor_from_graph_path<Interface>(user_path);
            const auto *input_meta = resolved_schema_meta<input_schema>(
                user_path.resolution, "adaptor input");
            const auto *ref_meta = TypeRegistry::instance().ref(input_meta);

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state = ref_meta->value_schema;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(input_stub_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), input_meta]() {
                    return make_shared_output_source_node(path, *input_meta, false);
                });
            return Port<REF<input_schema>>{w, std::move(port)};
        }

        template <typename Interface, typename InputSchema>
        const WiringInstance *capture_input(Wiring &w,
                                            Port<InputSchema> input,
                                            Port<REF<input_schema_t<Interface>>> source,
                                            const AdaptorPath &user_path)
        {
            using input_schema = input_schema_t<Interface>;

            std::array<WiringPortRef, 2> sources{input.erased(), source.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            const auto *input_meta = input.erased().schema;
            if (input_meta == nullptr)
            {
                input_meta = resolved_schema_meta<input_schema>(user_path.resolution, "adaptor input");
            }
            NodeBuilder builder = make_shared_output_capture_node(
                adaptor_from_graph_path<Interface>(user_path), *input_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(input_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            w.add_same_cycle_pair(capture.peered_node(), source.node());
            return capture.peered_node();
        }

        template <typename Interface>
        [[nodiscard]] Port<REF<output_schema_t<Interface>>> output_source(Wiring &w, const AdaptorPath &user_path)
        {
            using output_schema = output_schema_t<Interface>;

            std::string full_path = adaptor_to_graph_path<Interface>(user_path);
            const auto *target_meta = resolved_schema_meta<output_schema>(
                user_path.resolution, "adaptor output");
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state = ref_meta->value_schema;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(output_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Interface, typename OutputSchema>
        const WiringInstance *capture_output(Wiring &w,
                                             Port<OutputSchema> output,
                                             Port<REF<output_schema_t<Interface>>> shared_output,
                                             const AdaptorPath &user_path)
        {
            using output_schema = output_schema_t<Interface>;

            std::array<WiringPortRef, 2> sources{output.erased(), shared_output.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            const auto *output_meta = output.erased().schema;
            if (output_meta == nullptr)
            {
                output_meta = resolved_schema_meta<output_schema>(user_path.resolution, "adaptor output");
            }
            NodeBuilder builder = make_shared_output_capture_node(
                adaptor_to_graph_path<Interface>(user_path), *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(output_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            w.add_same_cycle_pair(capture.peered_node(), shared_output.node());
            return capture.peered_node();
        }

        template <typename Impl, typename... Args>
        decltype(auto) wire_impl(Wiring &w, const AdaptorPath &user_path, const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                return wire<Impl>(w, args..., arg<"path">(Str{user_path.value}));
            }
            else
            {
                return wire<Impl>(w, args...);
            }
        }

        template <typename Interface, typename InputSchema>
        void client_from_graph(
            Wiring &w,
            AdaptorPath user_path,
            Port<InputSchema> input)
        {
            w.register_service_client_path(adaptor_base_path<Interface>(user_path), "adaptor");
            const std::string endpoint = adaptor_from_graph_path<Interface>(user_path);
            auto source = input_stub_source<Interface>(w, user_path);
            w.register_service_rank_anchor(endpoint, source.node());
            const WiringInstance *capture = capture_input<Interface>(w, std::move(input), source, user_path);
            w.register_service_client_rank(endpoint, "adaptor", capture, false);
        }

        template <typename Interface>
        [[nodiscard]] Port<output_schema_t<Interface>> client_to_graph(Wiring &w, AdaptorPath user_path)
        {
            w.register_service_client_path(adaptor_base_path<Interface>(user_path), "adaptor");
            const std::string endpoint = adaptor_to_graph_path<Interface>(user_path);
            auto source = output_source<Interface>(w, user_path);
            w.register_service_client_rank(endpoint, "adaptor", source.node(), true);
            if constexpr (schema_descriptor<output_schema_t<Interface>>::is_concrete())
            {
                return source.template as<output_schema_t<Interface>>();
            }
            else
            {
                return Port<output_schema_t<Interface>>{w, source.erased()};
            }
        }

        template <typename Impl, typename... Args>
        void wire_impl_with_scope(Wiring &w,
                                  const AdaptorPath &user_path,
                                  std::string description,
                                  std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                  const Args &...args)
        {
            auto scope = w.service_implementation_scope(std::move(description), std::move(required_endpoints));
            wire_impl<Impl>(w, user_path, args...);
            scope.complete();
        }
    }  // namespace detail

    template <typename Interface>
        requires detail::has_input_schema<Interface>::value
    [[nodiscard]] Port<detail::input_schema_t<Interface>> from_graph(
        Wiring &w, AdaptorPath user_path);

    template <typename Interface>
        requires detail::has_output_schema<Interface>::value
    void to_graph(Wiring &w, AdaptorPath user_path, auto output);

    template <typename Interface, typename Impl, typename... Args>
    void register_adaptor(Wiring &w, AdaptorPath user_path, const Args &...args)
    {
        static_assert(detail::adaptor_interface<Interface>,
                      "register_adaptor requires a type derived from adaptor::interface");
        std::string base_path = detail::adaptor_base_path<Interface>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "adaptor " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "adaptor");
                std::vector<WiringServiceImplementationEndpoint> required_endpoints;
                detail::append_required_stub_endpoints<Interface>(required_endpoints, user_path);
                std::apply([&](const auto &...stored) {
                    detail::wire_impl_with_scope<Impl>(
                        target, user_path, "adaptor " + base_path,
                        std::move(required_endpoints), stored...);
                }, stored_args);
            });
    }

    template <typename Interface, typename Impl, typename... Args>
    void register_adaptor(Wiring &w, const Args &...args)
    {
        register_adaptor<Interface, Impl>(w, detail::default_adaptor_path<Interface>(), args...);
    }

    template <typename Interface, typename Impl, typename... Args>
    void register_automatic_adaptor(
        Wiring &w, AdaptorPath user_path, const Args &...args)
    {
        static_assert(detail::adaptor_interface<Interface>,
                      "register_automatic_adaptor requires an adaptor interface");
        std::string base_path = detail::adaptor_base_path<Interface>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "automatic adaptor " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "adaptor");
                std::vector<WiringServiceImplementationEndpoint> required_endpoints;
                detail::append_required_stub_endpoints<Interface>(required_endpoints, user_path);
                auto scope = target.service_implementation_scope(
                    "automatic adaptor " + base_path, std::move(required_endpoints));
                std::apply([&](const auto &...stored) {
                    if constexpr (detail::has_input_schema<Interface>::value)
                    {
                        auto input = from_graph<Interface>(target, user_path);
                        if constexpr (detail::has_output_schema<Interface>::value)
                        {
                            auto output = detail::wire_impl<Impl>(target, user_path, input, stored...);
                            to_graph<Interface>(target, user_path, output);
                        }
                        else
                        {
                            static_cast<void>(detail::wire_impl<Impl>(target, user_path, input, stored...));
                        }
                    }
                    else if constexpr (detail::has_output_schema<Interface>::value)
                    {
                        auto output = detail::wire_impl<Impl>(target, user_path, stored...);
                        to_graph<Interface>(target, user_path, output);
                    }
                    else
                    {
                        static_cast<void>(detail::wire_impl<Impl>(target, user_path, stored...));
                    }
                }, stored_args);
                scope.complete();
            });
    }

    template <typename Interface, typename Impl, typename... Args>
    void register_automatic_adaptor(Wiring &w, const Args &...args)
    {
        register_automatic_adaptor<Interface, Impl>(
            w, detail::default_adaptor_path<Interface>(), args...);
    }

    template <typename Impl, typename... Interfaces, typename... Args>
    void register_adaptors(Wiring &w, AdaptorPath user_path, const Args &...args)
    {
        static_assert(sizeof...(Interfaces) > 0,
                      "register_adaptors requires at least one adaptor interface");
        static_assert((detail::adaptor_interface<Interfaces> && ...),
                      "register_adaptors requires adaptor::interface descriptor types");
        std::vector<std::string> base_paths{
            detail::adaptor_base_path<Interfaces>(user_path)...};
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            std::move(base_paths), "multi-adaptor implementation",
            [user_path = std::move(user_path), stored_args = std::move(stored_args)](Wiring &target) {
                (target.register_built_service_path(
                    detail::adaptor_base_path<Interfaces>(user_path), "adaptor"), ...);
                std::vector<WiringServiceImplementationEndpoint> required_endpoints;
                (detail::append_required_stub_endpoints<Interfaces>(required_endpoints, user_path), ...);
                std::apply([&](const auto &...stored) {
                    detail::wire_impl_with_scope<Impl>(
                        target, user_path, "multi-adaptor implementation",
                        std::move(required_endpoints), stored...);
                }, stored_args);
            });
    }

    template <typename Interface>
        requires detail::has_input_schema<Interface>::value
    [[nodiscard]] Port<detail::input_schema_t<Interface>> from_graph(Wiring &w, AdaptorPath user_path)
    {
        const std::string endpoint = detail::adaptor_from_graph_path<Interface>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "adaptor");
        auto source = detail::input_stub_source<Interface>(w, user_path);
        w.register_service_rank_anchor(detail::adaptor_from_graph_path<Interface>(user_path), source.node());
        if constexpr (schema_descriptor<detail::input_schema_t<Interface>>::is_concrete())
        {
            return source.template as<detail::input_schema_t<Interface>>();
        }
        else
        {
            return Port<detail::input_schema_t<Interface>>{w, source.erased()};
        }
    }

    template <typename Interface>
        requires detail::has_input_schema<Interface>::value
    [[nodiscard]] Port<detail::input_schema_t<Interface>> from_graph(Wiring &w)
    {
        return from_graph<Interface>(w, detail::default_adaptor_path<Interface>());
    }

    template <typename Interface>
        requires detail::has_output_schema<Interface>::value
    void to_graph(Wiring &w,
                  AdaptorPath user_path,
                  auto output)
    {
        const std::string endpoint = detail::adaptor_to_graph_path<Interface>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "adaptor");
        auto shared_output = detail::output_source<Interface>(w, user_path);
        const WiringInstance *capture =
            detail::capture_output<Interface>(w, std::move(output), shared_output, user_path);
        w.register_service_rank_anchor(detail::adaptor_to_graph_path<Interface>(user_path), capture);
    }

    template <typename Interface>
        requires detail::has_output_schema<Interface>::value
    void to_graph(Wiring &w, auto output)
    {
        to_graph<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(output));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(
        Wiring &w,
        AdaptorPath user_path,
        auto input)
    {
        user_path = detail::resolve_client_path<Interface>(std::move(user_path), input);
        detail::client_from_graph<Interface>(w, user_path, std::move(input));
        return detail::client_to_graph<Interface>(w, std::move(user_path));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(
        Wiring &w,
        auto input)
    {
        return adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }

    template <typename Interface>
        requires(!detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(Wiring &w, AdaptorPath user_path)
    {
        return detail::client_to_graph<Interface>(w, std::move(user_path));
    }

    template <typename Interface>
        requires(!detail::has_input_schema<Interface>::value &&
                 detail::has_output_schema<Interface>::value)
    [[nodiscard]] Port<detail::output_schema_t<Interface>> adaptor(Wiring &w)
    {
        return adaptor<Interface>(w, detail::default_adaptor_path<Interface>());
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void adaptor(Wiring &w, AdaptorPath user_path, auto input)
    {
        user_path = detail::resolve_client_path<Interface>(std::move(user_path), input);
        detail::client_from_graph<Interface>(w, std::move(user_path), std::move(input));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void adaptor(Wiring &w, auto input)
    {
        adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void sink_adaptor(Wiring &w, AdaptorPath user_path, auto input)
    {
        adaptor<Interface>(w, std::move(user_path), std::move(input));
    }

    template <typename Interface>
        requires(detail::has_input_schema<Interface>::value &&
                 !detail::has_output_schema<Interface>::value)
    void sink_adaptor(Wiring &w, auto input)
    {
        sink_adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }
}  // namespace hgraph::adaptor

namespace hgraph::graph_wiring_detail
{
    template <typename Interface, typename OutSchema, typename... Args>
        requires adaptor::detail::adaptor_interface<Interface>
    struct wire_customization<Interface, OutSchema, Args...>
    {
        static constexpr bool enabled = true;

        static auto wire(Wiring &w, const Args &...args)
        {
            static_assert(std::is_void_v<OutSchema>,
                          "wire<Adaptor, OutSchema>: adaptor output schema is defined by the adaptor interface");
            return adaptor::adaptor<Interface>(w, args...);
        }
    };
}  // namespace hgraph::graph_wiring_detail

#endif  // HGRAPH_TYPES_ADAPTOR_WIRING_H
