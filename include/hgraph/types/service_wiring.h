#ifndef HGRAPH_TYPES_SERVICE_WIRING_H
#define HGRAPH_TYPES_SERVICE_WIRING_H

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/runtime/feedback_node.h>
#include <hgraph/runtime/service_node.h>
#include <hgraph/runtime/shared_output_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
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

namespace hgraph::service
{
    struct ServicePath
    {
        std::string   value{};
        ResolutionMap resolution{};
        bool          has_typed_suffix{false};
    };

    [[nodiscard]] inline ServicePath path(std::string_view value)
    {
        if (value.empty()) { throw std::invalid_argument("service path must not be empty"); }
        return ServicePath{std::string{value}};
    }

    template <typename... Args>
        requires(sizeof...(Args) > 0)
    [[nodiscard]] inline ServicePath path(std::string_view value, const Args &...args)
    {
        if (value.empty()) { throw std::invalid_argument("service path must not be empty"); }
        auto typed = wiring_path_detail::typed_path_value(value, args...);
        return ServicePath{std::move(typed.value), std::move(typed.resolution), typed.has_typed_suffix};
    }

    /**
     * C++ service wiring.
     *
     * ``Service`` is a descriptor type:
     *
     * .. code-block:: cpp
     *
     *    struct Prices {
     *        static constexpr std::string_view name{"prices"};
     *        using key_type = Str;
     *        using value_schema = TS<Int>;
     *    };
     *
     * Reference services expose one shared output:
     *
     * .. code-block:: cpp
     *
     *    struct Accounts {
     *        static constexpr std::string_view name{"accounts"};
     *        using output_schema = TSS<Str>;
     *    };
     *
     *    register_reference_service<Accounts, StaticAccounts>(w);
     *    auto accounts = wire<Accounts>(w);
     *
     * Use ``service::path("name")`` as the first argument after ``w`` to bind
     * or call a non-default service path.
     *
     * Subscription services add a key stream:
     *
     * ``register_subscription_service<Prices, Impl>(w)`` wires ``Impl`` with a
     * ``TSS<key_type>`` subscription input and captures its
     * ``TSD<key_type, value_schema>`` output as a shared reference.
     * ``wire<Prices>(w, key)`` records that client's subscription and returns the
     * selected service value by reference; no service value is copied by the
     * wiring layer.
     *
     * Request/reply services collect client requests through a feedback-style
     * source/capture pair:
     *
     * .. code-block:: cpp
     *
     *    struct AddOne {
     *        static constexpr std::string_view name{"add_one"};
     *        using request_schema = TS<Int>;
     *        using response_schema = TS<Int>;
     *    };
     *
     *    register_request_reply_service<AddOne, AddOneImpl>(w);
     *    auto reply = wire<AddOne>(w, request);
     *
     * The request source owns ``TSD<Int, request_schema>`` plus a mutable
     * request-delta state. Client capture sinks update that state, and the
     * source emits the cumulative delta on the next scheduled tick. Responses
     * pass through one outer-graph feedback edge before publication. This is
     * the temporal boundary that permits request/reply implementations to call
     * the same service recursively without introducing a rank cycle.
     */

    namespace detail
    {
        template <typename>
        inline constexpr bool always_false_v = false;

        template <typename T>
        struct is_service_path : std::false_type
        {
        };

        template <>
        struct is_service_path<ServicePath> : std::true_type
        {
        };

        template <typename T>
        inline constexpr bool is_service_path_v = is_service_path<std::remove_cvref_t<T>>::value;

        template <typename Service, typename = void>
        struct has_key_value_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_key_value_schema<Service, std::void_t<typename Service::key_type, typename Service::value_schema>>
            : std::true_type
        {
        };

        template <typename Service, typename = void>
        struct has_reference_output_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_reference_output_schema<Service, std::void_t<typename Service::output_schema>> : std::true_type
        {
        };

        template <typename Service, typename = void>
        struct has_adaptor_input_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_adaptor_input_schema<Service, std::void_t<typename Service::input_schema>> : std::true_type
        {
        };

        template <typename Service, typename = void>
        struct has_request_reply_schema : std::false_type
        {
        };

        template <typename Service>
        struct has_request_reply_schema<
            Service,
            std::void_t<typename Service::request_schema, typename Service::response_schema>>
            : std::true_type
        {
        };

        template <typename Service>
        concept reference_service_interface =
            has_reference_output_schema<Service>::value &&
            !has_adaptor_input_schema<Service>::value &&
            !has_key_value_schema<Service>::value &&
            !has_request_reply_schema<Service>::value;

        template <typename Service>
        concept subscription_service_interface =
            has_key_value_schema<Service>::value &&
            !has_reference_output_schema<Service>::value &&
            !has_request_reply_schema<Service>::value;

        template <typename Service>
        concept request_reply_service_interface =
            has_request_reply_schema<Service>::value &&
            !has_reference_output_schema<Service>::value &&
            !has_key_value_schema<Service>::value;

        template <typename Service>
        concept service_interface =
            reference_service_interface<Service> ||
            subscription_service_interface<Service> ||
            request_reply_service_interface<Service>;

        template <typename Service>
        using key_type_t = typename Service::key_type;

        template <typename Service>
        using value_schema_t = typename Service::value_schema;

        template <typename Service>
        using reference_output_schema_t = typename Service::output_schema;

        template <typename Service>
        using output_schema_t = TSD<key_type_t<Service>, value_schema_t<Service>>;

        template <typename Service>
        using request_schema_t = typename Service::request_schema;

        template <typename Service>
        using response_schema_t = typename Service::response_schema;

        template <typename Service>
        using request_input_schema_t = TSD<Int, request_schema_t<Service>>;

        template <typename Service>
        using request_output_schema_t = TSD<Int, response_schema_t<Service>>;

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
                        std::string{context} + " does not match generic service schema " +
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
                        std::string{context} + " has unresolved generic service schema " +
                        ts_pattern_to_string(to_pattern<Schema>()));
                }
                return meta;
            }
        }

        template <typename Scalar>
        void bind_scalar_resolution(ResolutionMap &resolution,
                                    const TSValueTypeMetaData *concrete,
                                    std::string_view context)
        {
            if constexpr (!scalar_descriptor<Scalar>::is_concrete())
            {
                const auto *scalar_meta = concrete != nullptr ? concrete->value_schema : nullptr;
                if (scalar_meta == nullptr || !scalar_pattern_match(to_scalar_pattern<Scalar>(), scalar_meta, resolution))
                {
                    throw std::invalid_argument(
                        std::string{context} + " does not match generic service key " +
                        scalar_pattern_to_string(to_scalar_pattern<Scalar>()));
                }
            }
        }

        template <typename Scalar>
        [[nodiscard]] const ValueTypeMetaData *resolved_scalar_meta(const ResolutionMap &resolution,
                                                                    std::string_view context)
        {
            if constexpr (scalar_descriptor<Scalar>::is_concrete())
            {
                return scalar_descriptor<Scalar>::value_meta();
            }
            else
            {
                const auto *meta = scalar_pattern_resolve(to_scalar_pattern<Scalar>(), resolution);
                if (meta == nullptr)
                {
                    throw std::invalid_argument(
                        std::string{context} + " has unresolved generic service key " +
                        scalar_pattern_to_string(to_scalar_pattern<Scalar>()));
                }
                return meta;
            }
        }

        template <typename T>
        concept graph_implementation = requires { &T::compose; };

        template <typename T>
        concept node_implementation = requires { &T::eval; };

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

        template <typename T>
        struct is_service_input_param : std::false_type
        {
        };

        template <fixed_string Name, typename S, auto... Policies>
        struct is_service_input_param<In<Name, S, Policies...>> : std::true_type
        {
        };

        template <fixed_string Name, typename S>
        struct is_service_input_param<NamedPort<Name, S>> : std::true_type
        {
        };

        template <typename S>
        struct is_service_input_param<Port<S>> : std::true_type
        {
        };

        template <typename Params, std::size_t I, bool Done>
        struct first_service_input_param_impl;

        template <typename Params, std::size_t I, bool Found>
        struct first_service_input_param_choice;

        template <typename Params, std::size_t I>
        struct first_service_input_param_impl<Params, I, true>
        {
            using type = void;
        };

        template <typename Params, std::size_t I>
        struct first_service_input_param_impl<Params, I, false>
        {
            using candidate = std::tuple_element_t<I, Params>;
            using type = typename first_service_input_param_choice<
                Params, I, is_service_input_param<candidate>::value>::type;
        };

        template <typename Params, std::size_t I>
        struct first_service_input_param_choice<Params, I, true>
        {
            using type = std::tuple_element_t<I, Params>;
        };

        template <typename Params, std::size_t I>
        struct first_service_input_param_choice<Params, I, false>
        {
            using type = typename first_service_input_param_impl<
                Params, I + 1, (I + 1 >= std::tuple_size_v<Params>)>::type;
        };

        template <typename Params>
        using first_service_input_param =
            first_service_input_param_impl<Params, 0, (std::tuple_size_v<Params> == 0)>;

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

        template <typename Impl, typename OutputSchema, typename... Args>
        [[nodiscard]] Port<OutputSchema> wire_service_impl(Wiring &w, const ServicePath &user_path, const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                auto output = wire<Impl>(w, args..., arg<"path">(Str{user_path.value}));
                if constexpr (schema_descriptor<OutputSchema>::is_concrete())
                {
                    return output.template as<OutputSchema>();
                }
                else
                {
                    return Port<OutputSchema>{w, output.erased()};
                }
            }
            else
            {
                auto output = wire<Impl>(w, args...);
                if constexpr (schema_descriptor<OutputSchema>::is_concrete())
                {
                    return output.template as<OutputSchema>();
                }
                else
                {
                    return Port<OutputSchema>{w, output.erased()};
                }
            }
        }

        template <typename Impl, typename PortT>
        [[nodiscard]] auto service_input_arg(const PortT &port)
        {
            using input_param = typename first_service_input_param<implementation_params_t<Impl>>::type;
            if constexpr (!std::is_void_v<input_param> && requires { input_param::field_name; })
            {
                return arg<input_param::field_name>(port);
            }
            else
            {
                return port;
            }
        }

        template <typename Service>
        [[nodiscard]] std::string service_name(std::string_view error_prefix)
        {
            std::string_view name{Service::name};
            if (name.empty())
            {
                throw std::invalid_argument(std::string{error_prefix} + " service name must not be empty");
            }
            return std::string{name};
        }

        template <typename Service>
        [[nodiscard]] ServicePath default_service_path()
        {
            if constexpr (requires { std::string_view{Service::default_path}; })
            {
                return path(std::string_view{Service::default_path});
            }
            else
            {
                std::string user_path = service_name<Service>("default");
                user_path.append("_default");
                return ServicePath{std::move(user_path)};
            }
        }

        template <typename Service>
        [[nodiscard]] std::string service_full_path(std::string_view prefix,
                                                    std::string_view error_prefix,
                                                    const ServicePath &user_path)
        {
            const std::string name = service_name<Service>(error_prefix);
            if (user_path.value.starts_with(prefix)) { return user_path.value; }
            std::string path{prefix};
            path.append(user_path.value);
            path.push_back('/');
            path.append(name);
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string reference_base_path(const ServicePath &user_path)
        {
            return service_full_path<Service>("ref_svc://", "reference", user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string subscription_base_path(const ServicePath &user_path)
        {
            return service_full_path<Service>("subs_svc://", "subscription", user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string request_reply_base_path(const ServicePath &user_path)
        {
            return service_full_path<Service>("reqrepl_svc://", "request-reply", user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string reference_output_path(const ServicePath &user_path)
        {
            return reference_base_path<Service>(user_path);
        }

        template <typename Service>
        [[nodiscard]] std::string subscriptions_path(const ServicePath &user_path)
        {
            std::string path = subscription_base_path<Service>(user_path);
            path.append("/subs");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string output_path(const ServicePath &user_path)
        {
            std::string path = subscription_base_path<Service>(user_path);
            path.append("/out");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string request_input_path(const ServicePath &user_path)
        {
            std::string path = request_reply_base_path<Service>(user_path);
            path.append("/request");
            return path;
        }

        template <typename Service>
        [[nodiscard]] std::string request_reply_output_path(const ServicePath &user_path)
        {
            std::string path = request_reply_base_path<Service>(user_path);
            path.append("/replies");
            return path;
        }

        template <typename Service>
        void append_required_stub_endpoints(std::vector<WiringServiceImplementationEndpoint> &endpoints,
                                            const ServicePath &user_path)
        {
            if constexpr (reference_service_interface<Service>)
            {
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    reference_output_path<Service>(user_path), user_path.resolution});
            }
            else if constexpr (subscription_service_interface<Service>)
            {
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    subscriptions_path<Service>(user_path), user_path.resolution});
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    output_path<Service>(user_path), user_path.resolution});
            }
            else if constexpr (request_reply_service_interface<Service>)
            {
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    request_input_path<Service>(user_path), user_path.resolution});
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    request_reply_output_path<Service>(user_path), user_path.resolution});
            }
        }

        template <typename Service, typename RequestSchema>
        [[nodiscard]] ServicePath resolve_request_reply_client_path(ServicePath user_path,
                                                                    const Port<RequestSchema> &request)
        {
            ResolutionMap inferred = user_path.resolution;
            bind_schema_resolution<request_schema_t<Service>>(
                inferred, request.erased().schema, "request/reply service request");
            return wiring_path_detail::with_resolution(std::move(user_path), inferred);
        }

        struct request_id_source_marker
        {
        };

        [[nodiscard]] inline Port<TS<Int>> request_id_source(Wiring &w)
        {
            NodeBuilder builder = make_request_id_source_node();
            WiringPortRef source = w.add_unique_node(
                std::type_index(typeid(request_id_source_marker)), std::move(builder),
                std::span<const WiringPortRef>{}, Value{});
            return Port<TS<Int>>{w, std::move(source)};
        }

        [[nodiscard]] inline Value path_key_value(const std::string &full_path)
        {
            // Used only by Wiring's intern key; service source nodes do not carry path scalars at runtime.
            return Value{Str{full_path}};
        }

        // Per-ROLE identity markers (ruling 2026-07-05, services.rst
        // "Runtime service identity"): the SERVICE identity rides the
        // name-qualified full-path scalar, so the markers are plain roles —
        // (role-typeid, full-path, schemas) is a total node key, and the
        // same identity serves C++ templates and runtime (Python)
        // descriptors alike.
        struct reference_output_source_marker
        {
        };

        struct reference_output_capture_marker
        {
        };

        struct subscription_source_marker
        {
        };

        struct subscription_capture_marker
        {
        };

        struct subscription_response_gate_marker
        {
        };

        struct shared_output_source_marker
        {
        };

        struct shared_output_capture_marker
        {
        };

        struct request_input_source_marker
        {
        };

        struct request_input_capture_marker
        {
        };

        struct request_reply_output_source_marker
        {
        };

        struct request_reply_output_capture_marker
        {
        };

        struct request_reply_feedback_source_marker
        {
        };

        struct request_reply_feedback_sink_marker
        {
        };

        [[nodiscard]] inline WiringPortRef request_reply_response_feedback(
            Wiring &w,
            WiringPortRef response,
            const TSValueTypeMetaData &schema)
        {
            WiringPortRef feedback = w.add_unique_node(
                std::type_index(typeid(request_reply_feedback_source_marker)),
                make_feedback_source_node(schema), std::span<const WiringPortRef>{}, Value{});

            std::array<WiringPortRef, 2> sources{
                graph_wiring_detail::adapt_source_for_input(w, &schema, std::move(response)),
                feedback,
            };
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder sink = make_feedback_sink_node(schema);
            sink.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                sink.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            static_cast<void>(w.add_unique_node(
                std::type_index(typeid(request_reply_feedback_sink_marker)), std::move(sink),
                std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{}));
            return feedback;
        }

        template <typename Service>
        [[nodiscard]] Port<REF<reference_output_schema_t<Service>>> reference_shared_output_source(
            Wiring &w,
            const ServicePath &user_path)
        {
            using output_schema = reference_output_schema_t<Service>;

            std::string full_path = reference_output_path<Service>(user_path);
            const auto *target_meta = resolved_schema_meta<output_schema>(
                user_path.resolution, "reference service output");
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(reference_output_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<TSS<key_type_t<Service>>> subscription_source(Wiring &w, const ServicePath &user_path)
        {
            using key_type = key_type_t<Service>;

            std::string full_path = subscriptions_path<Service>(user_path);
            const auto *key_meta = resolved_scalar_meta<key_type>(
                user_path.resolution, "subscription service key");
            const auto *out_meta = TypeRegistry::instance().tss(key_meta);

            WiringNodeSchema schema;
            schema.output = out_meta;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(subscription_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), key_meta]() {
                    return make_subscription_key_source_node(path, *key_meta);
                });
            return Port<TSS<key_type>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<REF<output_schema_t<Service>>> shared_output_source(Wiring &w,
                                                                               const ServicePath &user_path)
        {
            using output_schema = output_schema_t<Service>;

            std::string full_path = output_path<Service>(user_path);
            const auto *key_meta = resolved_scalar_meta<key_type_t<Service>>(
                user_path.resolution, "subscription service key");
            const auto *value_meta = resolved_schema_meta<value_schema_t<Service>>(
                user_path.resolution, "subscription service value");
            const auto *target_meta = TypeRegistry::instance().tsd(key_meta, value_meta);
            const auto *ref_meta    = TypeRegistry::instance().ref(target_meta);

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(shared_output_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<request_input_schema_t<Service>> request_input_source(
            Wiring &w,
            const ServicePath &user_path)
        {
            using input_schema = request_input_schema_t<Service>;
            using request_schema = request_schema_t<Service>;

            std::string full_path = request_input_path<Service>(user_path);
            const auto *request_meta = resolved_schema_meta<request_schema>(
                user_path.resolution, "request/reply service request");
            const auto *out_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), request_meta);

            WiringNodeSchema schema;
            schema.output = out_meta;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(request_input_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), request_meta]() {
                    return make_request_input_source_node(path, *request_meta);
                });
            return Port<input_schema>{w, std::move(port)};
        }

        template <typename Service>
        [[nodiscard]] Port<REF<request_output_schema_t<Service>>> request_reply_output_source(
            Wiring &w,
            const ServicePath &user_path)
        {
            using output_schema = request_output_schema_t<Service>;

            std::string full_path = request_reply_output_path<Service>(user_path);
            const auto *response_meta = resolved_schema_meta<response_schema_t<Service>>(
                user_path.resolution, "request/reply service response");
            const auto *target_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), response_meta);
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(request_reply_output_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Service>
        const WiringInstance *capture_subscription_key(Wiring &w,
                                                       Port<TS<key_type_t<Service>>> key,
                                                       Port<TSS<key_type_t<Service>>> subscriptions,
                                                       const ServicePath &user_path)
        {
            using key_type = key_type_t<Service>;

            std::array<WiringPortRef, 2> sources{key.erased(), subscriptions.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            const auto *key_meta = resolved_scalar_meta<key_type>(
                user_path.resolution, "subscription service key");
            NodeBuilder builder = make_subscription_key_capture_node(
                subscriptions_path<Service>(user_path), *key_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(subscription_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            // Root captures rank before the shared key source and publish in
            // the same cycle. A dynamically-started nested capture defers its
            // outer hand-off one cycle; observed-time batches keep successive
            // transitions distinct.
            return capture.peered_node();
        }

        template <typename Service, typename RequestSchema>
        const WiringInstance *capture_request_input(Wiring &w,
                                                    Port<RequestSchema> request,
                                                    Port<request_input_schema_t<Service>> requests,
                                                    const ServicePath &user_path,
                                                    Port<TS<Int>> request_id)
        {
            using request_schema = request_schema_t<Service>;

            std::array<WiringPortRef, 3> sources{
                request.erased(), requests.erased(), request_id.erased()};
            std::array<WiringInputRef, 3> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
                WiringInputRef{.source = sources[2]},
            }};
            const auto *request_meta = resolved_schema_meta<request_schema>(
                user_path.resolution, "request/reply service request");
            NodeBuilder builder = make_request_input_capture_node(
                request_input_path<Service>(user_path), *request_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(request_input_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            // Request/reply alone forwards on the next cycle; the temporal
            // break is what permits recursive request/reply implementations.
            return capture.peered_node();
        }

        template <typename Service, typename Impl>
        const WiringInstance *capture_reference_service_output(Wiring &w,
                                                               Port<reference_output_schema_t<Service>> output,
                                                               Port<REF<reference_output_schema_t<Service>>> shared_output,
                                                               const ServicePath &user_path)
        {
            using output_schema = reference_output_schema_t<Service>;

            std::array<WiringPortRef, 2> sources{output.erased(), shared_output.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            const auto *output_meta = output.erased().schema;
            if (output_meta == nullptr)
            {
                output_meta = resolved_schema_meta<output_schema>(
                    user_path.resolution, "reference service output");
            }
            NodeBuilder builder = make_shared_output_capture_node(
                reference_output_path<Service>(user_path), *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(reference_output_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            // Shared-output relays are RANK-CORRECT and same-cycle: the rank
            // dependency places the paired source after this capture (and
            // Wiring::finish's topological sort re-ranks once ALL captures are
            // known), so the capture schedules the source for the CURRENT
            // evaluation time — no next-cycle workaround. Request/reply stubs
            // above still schedule their transport sources for the next cycle.
            // The same rule applies at every add_rank_dependency site below
            // and in adaptor_wiring.h.
            w.add_same_cycle_pair(capture.peered_node(), shared_output.node());
            return capture.peered_node();
        }

        template <typename Service, typename Impl>
        const WiringInstance *capture_service_output(Wiring &w,
                                                     Port<output_schema_t<Service>> output,
                                                     Port<REF<output_schema_t<Service>>> shared_output,
                                                     const ServicePath &user_path)
        {
            std::array<WiringPortRef, 2> sources{output.erased(), shared_output.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            const auto *output_meta = output.erased().schema;
            if (output_meta == nullptr)
            {
                const auto *key_meta = resolved_scalar_meta<key_type_t<Service>>(
                    user_path.resolution, "subscription service key");
                const auto *value_meta = resolved_schema_meta<value_schema_t<Service>>(
                    user_path.resolution, "subscription service value");
                output_meta = TypeRegistry::instance().tsd(key_meta, value_meta);
            }
            NodeBuilder builder = make_shared_output_capture_node(
                output_path<Service>(user_path), *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(shared_output_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            w.add_same_cycle_pair(capture.peered_node(), shared_output.node());
            return capture.peered_node();
        }

        template <typename Service, typename Impl>
        const WiringInstance *capture_request_reply_service_output(
            Wiring &w,
            Port<request_output_schema_t<Service>> output,
            Port<REF<request_output_schema_t<Service>>> shared_output,
            const ServicePath &user_path)
        {
            const auto *response_meta = resolved_schema_meta<response_schema_t<Service>>(
                user_path.resolution, "request/reply service response");
            const auto *output_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), response_meta);
            WiringPortRef feedback = request_reply_response_feedback(
                w, output.erased(), *output_meta);
            std::array<WiringPortRef, 2> sources{std::move(feedback), shared_output.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(
                request_reply_output_path<Service>(user_path), *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(
                std::type_index(typeid(request_reply_output_capture_marker)),
                std::move(builder),
                std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                Value{});
            w.add_same_cycle_pair(capture.peered_node(), shared_output.node());
            return capture.peered_node();
        }

        template <typename Impl, typename... Args>
        void wire_service_graph(Wiring &w, const ServicePath &user_path, const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                static_cast<void>(wire<Impl>(w, args..., arg<"path">(Str{user_path.value})));
            }
            else
            {
                static_cast<void>(wire<Impl>(w, args...));
            }
        }

        template <typename Impl, typename... Args>
        void wire_service_graph_with_scope(Wiring &w,
                                           const ServicePath &user_path,
                                           std::string description,
                                           std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                           const Args &...args)
        {
            auto scope = w.service_implementation_scope(std::move(description), std::move(required_endpoints));
            wire_service_graph<Impl>(w, user_path, args...);
            scope.complete();
        }
    }  // namespace detail

    template <typename Service>
    [[nodiscard]] Port<typename Service::output_schema> reference_service(Wiring &w, ServicePath user_path)
    {
        using output_schema = detail::reference_output_schema_t<Service>;
        w.register_service_client_path(detail::reference_base_path<Service>(user_path), "reference service");
        auto source = detail::reference_shared_output_source<Service>(w, user_path);
        w.register_service_client_rank(
            detail::reference_base_path<Service>(user_path), "reference service", source.node(), true);
        if constexpr (schema_descriptor<output_schema>::is_concrete())
        {
            return source.template as<output_schema>();
        }
        else
        {
            WiringPortRef output = source.erased();
            output.schema = detail::resolved_schema_meta<output_schema>(
                user_path.resolution, "reference service output");
            return Port<output_schema>{w, std::move(output)};
        }
    }

    template <typename Service>
    [[nodiscard]] Port<typename Service::output_schema> reference_service(Wiring &w)
    {
        return reference_service<Service>(w, detail::default_service_path<Service>());
    }

    template <typename Service, typename Impl, typename... Args>
    void register_reference_service(Wiring &w, ServicePath user_path, const Args &...args)
    {
        using output_schema = detail::reference_output_schema_t<Service>;
        const std::string base_path = detail::reference_base_path<Service>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "reference service " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "reference service");
                auto shared_output = detail::reference_shared_output_source<Service>(target, user_path);
                auto output = std::apply(
                    [&](const auto &...stored) {
                        return detail::wire_service_impl<Impl, output_schema>(target, user_path, stored...);
                    }, stored_args);
                const WiringInstance *capture =
                    detail::capture_reference_service_output<Service, Impl>(
                        target, output, shared_output, user_path);
                target.register_service_rank_anchor(base_path, capture);
            });
    }

    template <typename Service, typename Impl, typename... Args>
    void register_reference_service(Wiring &w, const Args &...args)
    {
        register_reference_service<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    [[nodiscard]] Port<TSS<detail::key_type_t<Service>>> impl_input(Wiring &w, ServicePath user_path)
    {
        const std::string endpoint = detail::subscriptions_path<Service>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "subscription service");
        auto source = detail::subscription_source<Service>(w, user_path);
        w.register_service_rank_anchor(detail::subscriptions_path<Service>(user_path), source.node());
        return source;
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    [[nodiscard]] Port<TSS<detail::key_type_t<Service>>> impl_input(Wiring &w)
    {
        return impl_input<Service>(w, detail::default_service_path<Service>());
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    [[nodiscard]] Port<detail::request_input_schema_t<Service>> impl_input(Wiring &w, ServicePath user_path)
    {
        const std::string endpoint = detail::request_input_path<Service>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "request/reply service");
        auto source = detail::request_input_source<Service>(w, user_path);
        w.register_service_rank_anchor(detail::request_input_path<Service>(user_path), source.node());
        return source;
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    [[nodiscard]] Port<detail::request_input_schema_t<Service>> impl_input(Wiring &w)
    {
        return impl_input<Service>(w, detail::default_service_path<Service>());
    }

    struct explicit_impl_output_marker
    {
    };

    template <typename Service>
        requires detail::reference_service_interface<Service>
    void impl_output(Wiring &w,
                     ServicePath user_path,
                     Port<detail::reference_output_schema_t<Service>> output)
    {
        const std::string endpoint = detail::reference_output_path<Service>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "reference service");
        auto shared_output = detail::reference_shared_output_source<Service>(w, user_path);
        const WiringInstance *capture = detail::capture_reference_service_output<Service, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
        w.register_service_rank_anchor(detail::reference_base_path<Service>(user_path), capture);
    }

    template <typename Service>
        requires detail::reference_service_interface<Service>
    void impl_output(Wiring &w, Port<detail::reference_output_schema_t<Service>> output)
    {
        impl_output<Service>(w, detail::default_service_path<Service>(), std::move(output));
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    void impl_output(Wiring &w,
                     ServicePath user_path,
                     Port<detail::output_schema_t<Service>> output)
    {
        const std::string endpoint = detail::output_path<Service>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "subscription service");
        auto shared_output = detail::shared_output_source<Service>(w, user_path);
        const WiringInstance *capture = detail::capture_service_output<Service, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
        w.register_service_rank_anchor(detail::output_path<Service>(user_path), capture);
    }

    template <typename Service>
        requires detail::subscription_service_interface<Service>
    void impl_output(Wiring &w, Port<detail::output_schema_t<Service>> output)
    {
        impl_output<Service>(w, detail::default_service_path<Service>(), std::move(output));
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    void impl_output(Wiring &w,
                     ServicePath user_path,
                     Port<detail::request_output_schema_t<Service>> output)
    {
        const std::string endpoint = detail::request_reply_output_path<Service>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "request/reply service");
        auto shared_output = detail::request_reply_output_source<Service>(w, user_path);
        const WiringInstance *capture =
            detail::capture_request_reply_service_output<Service, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
        w.register_service_rank_anchor(detail::request_reply_output_path<Service>(user_path), capture);
    }

    template <typename Service>
        requires detail::request_reply_service_interface<Service>
    void impl_output(Wiring &w, Port<detail::request_output_schema_t<Service>> output)
    {
        impl_output<Service>(w, detail::default_service_path<Service>(), std::move(output));
    }

    template <typename Impl, typename... Services, typename... Args>
    void register_services(Wiring &w, ServicePath user_path, const Args &...args)
    {
        static_assert(sizeof...(Services) > 0,
                      "register_services requires at least one service interface");
        static_assert((detail::service_interface<Services> && ...),
                      "register_services requires service descriptor types");
        (
            [&] {
                if constexpr (detail::reference_service_interface<Services>)
                {
                    w.register_built_service_path(
                        detail::reference_base_path<Services>(user_path), "reference service");
                }
                else if constexpr (detail::subscription_service_interface<Services>)
                {
                    w.register_built_service_path(
                        detail::subscription_base_path<Services>(user_path), "subscription service");
                }
                else if constexpr (detail::request_reply_service_interface<Services>)
                {
                    w.register_built_service_path(
                        detail::request_reply_base_path<Services>(user_path), "request/reply service");
                }
            }(),
            ...);
        std::vector<WiringServiceImplementationEndpoint> required_endpoints;
        (detail::append_required_stub_endpoints<Services>(required_endpoints, user_path), ...);
        detail::wire_service_graph_with_scope<Impl>(
            w, user_path, "multi-service implementation", std::move(required_endpoints), args...);
    }

    template <typename Service>
    class SubscriptionService
    {
      public:
        using key_type      = detail::key_type_t<Service>;
        using value_schema  = detail::value_schema_t<Service>;
        using output_schema = detail::output_schema_t<Service>;

        SubscriptionService() noexcept = default;
        SubscriptionService(Wiring &w,
                            Port<TSS<key_type>> subscriptions,
                            Port<REF<output_schema>> output,
                            ServicePath user_path) noexcept
            : wiring_(&w),
              subscriptions_(std::move(subscriptions)),
              output_(std::move(output)),
              path_(std::move(user_path))
        {
        }

        [[nodiscard]] Port<TSS<key_type>> subscriptions() const { return subscriptions_; }
        [[nodiscard]] Port<REF<output_schema>> output() const { return output_; }

        [[nodiscard]] Port<value_schema> operator()(Port<TS<key_type>> key) const
        {
            if (wiring_ == nullptr) { throw std::logic_error("subscription service handle is not bound"); }
            const WiringInstance *capture =
                detail::capture_subscription_key<Service>(*wiring_, key, subscriptions_, path_);
            wiring_->register_service_client_rank(
                detail::subscriptions_path<Service>(path_), "subscription service", capture, false);
            auto sampled = wire<stdlib::getitem_>(
                *wiring_, output_.template as<output_schema>(), key)
                .template as<value_schema>();
            std::array<WiringPortRef, 3> sources{
                sampled.erased(), key.erased(), subscriptions_.erased()};
            std::array<WiringInputRef, 3> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1]},
                WiringInputRef{.source = sources[2]},
            }};
            const auto *key_meta = detail::resolved_scalar_meta<key_type>(
                path_.resolution, "subscription service key");
            const auto *response_meta = detail::resolved_schema_meta<value_schema>(
                path_.resolution, "subscription service response");
            NodeBuilder builder = make_subscription_response_gate_node(
                *key_meta, *response_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef gated = wiring_->add_node(
                std::type_index(typeid(detail::subscription_response_gate_marker)),
                std::move(builder),
                std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{});
            return Port<value_schema>{*wiring_, std::move(gated)};
        }

      private:
        Wiring                  *wiring_{nullptr};
        Port<TSS<key_type>>      subscriptions_{};
        Port<REF<output_schema>> output_{};
        ServicePath              path_{};
    };

    template <typename Service>
    [[nodiscard]] SubscriptionService<Service> subscription_service(Wiring &w, ServicePath user_path)
    {
        w.register_service_client_path(detail::subscription_base_path<Service>(user_path), "subscription service");
        auto subscriptions = detail::subscription_source<Service>(w, user_path);
        auto shared_output = detail::shared_output_source<Service>(w, user_path);
        w.register_service_rank_anchor(detail::subscriptions_path<Service>(user_path), subscriptions.node());
        w.register_service_client_rank(
            detail::output_path<Service>(user_path), "subscription service", shared_output.node(), true);
        return SubscriptionService<Service>{w, subscriptions, shared_output, std::move(user_path)};
    }

    template <typename Service>
    [[nodiscard]] SubscriptionService<Service> subscription_service(Wiring &w)
    {
        return subscription_service<Service>(w, detail::default_service_path<Service>());
    }

    template <typename Service, typename Impl, typename... Args>
    void register_subscription_service(Wiring &w, ServicePath user_path, const Args &...args)
    {
        using output_schema = detail::output_schema_t<Service>;

        const std::string base_path = detail::subscription_base_path<Service>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "subscription service " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "subscription service");
                auto subscriptions = detail::subscription_source<Service>(target, user_path);
                auto shared_output = detail::shared_output_source<Service>(target, user_path);
                target.register_service_rank_anchor(
                    detail::subscriptions_path<Service>(user_path), subscriptions.node());
                auto output = std::apply(
                    [&](const auto &...stored) {
                        return detail::wire_service_impl<Impl, output_schema>(
                            target, user_path, detail::service_input_arg<Impl>(subscriptions), stored...);
                    }, stored_args);
                const WiringInstance *capture = detail::capture_service_output<Service, Impl>(
                    target, output, shared_output, user_path);
                target.register_service_rank_anchor(
                    detail::output_path<Service>(user_path), capture);
            });
    }

    template <typename Service, typename Impl, typename... Args>
    void register_subscription_service(Wiring &w, const Args &...args)
    {
        register_subscription_service<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] SubscriptionService<Service> subscription_service_impl(Wiring &w,
                                                                         ServicePath user_path,
                                                                         const Args &...args)
    {
        register_subscription_service<Service, Impl>(w, user_path, args...);
        return subscription_service<Service>(w, std::move(user_path));
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] SubscriptionService<Service> subscription_service_impl(Wiring &w, const Args &...args)
    {
        return subscription_service_impl<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service, typename RequestSchema>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service(
        Wiring &w,
        Port<RequestSchema> request,
        ServicePath user_path)
    {
        using output_schema   = detail::request_output_schema_t<Service>;
        using response_schema = detail::response_schema_t<Service>;

        user_path = detail::resolve_request_reply_client_path<Service>(std::move(user_path), request);
        w.register_service_client_path(detail::request_reply_base_path<Service>(user_path), "request/reply service");
        auto request_id      = detail::request_id_source(w);
        auto requests        = detail::request_input_source<Service>(w, user_path);
        auto replies         = detail::request_reply_output_source<Service>(w, user_path);

        w.register_service_rank_anchor(detail::request_input_path<Service>(user_path), requests.node());
        static_cast<void>(
            detail::capture_request_input<Service>(w, std::move(request), requests, user_path, request_id));
        // Request/reply responses cross an explicit feedback edge. Unlike the
        // other service flavours, clients therefore do not participate in
        // indirect service ranking; this also permits request/reply cycles.
        if constexpr (schema_descriptor<output_schema>::is_concrete())
        {
            auto reply = wire<stdlib::getitem_>(w, replies.template as<output_schema>(), request_id);
            if constexpr (schema_descriptor<response_schema>::is_concrete())
            {
                return reply.template as<response_schema>();
            }
            else
            {
                return Port<response_schema>{w, reply.erased()};
            }
        }
        else
        {
            auto dict = Port<output_schema>{w, replies.erased()};
            auto reply = wire<stdlib::getitem_>(w, dict, request_id);
            return Port<response_schema>{w, reply.erased()};
        }
    }

    template <typename Service, typename RequestSchema>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service(
        Wiring &w,
        Port<RequestSchema> request)
    {
        return request_reply_service<Service>(
            w, std::move(request), detail::default_service_path<Service>());
    }

    template <typename Service, typename Impl, typename... Args>
    void register_request_reply_service(Wiring &w, ServicePath user_path, const Args &...args)
    {
        using output_schema = detail::request_output_schema_t<Service>;

        const std::string base_path = detail::request_reply_base_path<Service>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "request/reply service " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "request/reply service");
                auto requests = detail::request_input_source<Service>(target, user_path);
                auto replies  = detail::request_reply_output_source<Service>(target, user_path);
                target.register_service_rank_anchor(
                    detail::request_input_path<Service>(user_path), requests.node());
                auto output = std::apply(
                    [&](const auto &...stored) {
                        return detail::wire_service_impl<Impl, output_schema>(
                            target, user_path, detail::service_input_arg<Impl>(requests), stored...);
                    }, stored_args);
                const WiringInstance *capture =
                    detail::capture_request_reply_service_output<Service, Impl>(
                        target, output, replies, user_path);
                target.register_service_rank_anchor(
                    detail::request_reply_output_path<Service>(user_path), capture);
            });
    }

    template <typename Service, typename Impl, typename... Args>
    void register_request_reply_service(Wiring &w, const Args &...args)
    {
        register_request_reply_service<Service, Impl>(w, detail::default_service_path<Service>(), args...);
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service_impl(
        Wiring &w,
        Port<typename Service::request_schema> request,
        ServicePath user_path,
        const Args &...args)
    {
        register_request_reply_service<Service, Impl>(w, user_path, args...);
        return request_reply_service<Service>(w, std::move(request), std::move(user_path));
    }

    template <typename Service, typename Impl, typename... Args>
    [[nodiscard]] Port<typename Service::response_schema> request_reply_service_impl(
        Wiring &w,
        Port<typename Service::request_schema> request,
        const Args &...args)
    {
        return request_reply_service_impl<Service, Impl>(
            w, std::move(request), detail::default_service_path<Service>(), args...);
    }
}  // namespace hgraph::service

namespace hgraph::service_adaptor
{
    struct interface
    {
    };

    using ServiceAdaptorPath = service::ServicePath;

    [[nodiscard]] inline ServiceAdaptorPath path(std::string_view value)
    {
        return service::path(value);
    }

    template <typename... Args>
        requires(sizeof...(Args) > 0)
    [[nodiscard]] inline ServiceAdaptorPath path(std::string_view value, const Args &...args)
    {
        return service::path(value, args...);
    }

    /**
     * Multi-client adaptor wiring.
     *
     * Client code calls ``wire<Interface>(w, input)`` and receives that client's
     * output when the interface declares ``output_schema``. Implementation
     * graphs call ``from_graph<Interface>()`` to receive all client inputs as
     * ``TSD<Int, input_schema>``. Duplex interfaces additionally call
     * ``to_graph`` to publish ``TSD<Int, output_schema>`` replies keyed by the
     * same client id; sink-only interfaces have no reply endpoint.
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
        concept service_adaptor_interface =
            std::derived_from<Interface, interface> &&
            has_input_schema<Interface>::value;

        template <typename Interface>
        using input_schema_t = typename Interface::input_schema;

        template <typename Interface>
        using output_schema_t = typename Interface::output_schema;

        template <typename Interface>
        using request_input_schema_t = TSD<Int, input_schema_t<Interface>>;

        template <typename Interface>
        using request_output_schema_t = TSD<Int, output_schema_t<Interface>>;

        template <typename Interface, typename InputSchema>
        [[nodiscard]] ServiceAdaptorPath resolve_client_path(ServiceAdaptorPath user_path,
                                                             const Port<InputSchema> &input)
        {
            ResolutionMap inferred = user_path.resolution;
            service::detail::bind_schema_resolution<input_schema_t<Interface>>(
                inferred, input.erased().schema, "service adaptor input");
            return wiring_path_detail::with_resolution(std::move(user_path), inferred);
        }

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
            if (name.empty()) { throw std::invalid_argument("service adaptor name must not be empty"); }
            return std::string{name};
        }

        template <typename Interface>
        [[nodiscard]] ServiceAdaptorPath default_adaptor_path()
        {
            if constexpr (requires { std::string_view{Interface::default_path}; })
            {
                return path(std::string_view{Interface::default_path});
            }
            else
            {
                std::string value = adaptor_name<Interface>();
                value.append("_default");
                return ServiceAdaptorPath{std::move(value)};
            }
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_base_path(const ServiceAdaptorPath &user_path)
        {
            constexpr std::string_view prefix{"service_adaptor://"};
            if (user_path.value.starts_with(prefix)) { return user_path.value; }
            std::string full{prefix};
            full.append(user_path.value);
            full.push_back('/');
            full.append(adaptor_name<Interface>());
            return full;
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_from_graph_path(const ServiceAdaptorPath &user_path)
        {
            std::string full = adaptor_base_path<Interface>(user_path);
            full.append("/from_graph");
            return full;
        }

        template <typename Interface>
        [[nodiscard]] std::string adaptor_to_graph_path(const ServiceAdaptorPath &user_path)
        {
            std::string full = adaptor_base_path<Interface>(user_path);
            full.append("/to_graph");
            return full;
        }

        template <typename Interface>
        void append_required_stub_endpoints(std::vector<WiringServiceImplementationEndpoint> &endpoints,
                                            const ServiceAdaptorPath &user_path)
        {
            endpoints.push_back(WiringServiceImplementationEndpoint{
                adaptor_from_graph_path<Interface>(user_path), user_path.resolution});
            if constexpr (has_output_schema<Interface>::value)
            {
                endpoints.push_back(WiringServiceImplementationEndpoint{
                    adaptor_to_graph_path<Interface>(user_path), user_path.resolution});
            }
        }

        [[nodiscard]] inline Value path_key_value(const std::string &full_path)
        {
            return Value{Str{full_path}};
        }

        struct request_input_source_marker
        {
        };

        struct request_input_capture_marker
        {
        };

        struct output_source_marker
        {
        };

        struct output_capture_marker
        {
        };

        template <typename Interface>
        [[nodiscard]] Port<request_input_schema_t<Interface>> request_input_source(
            Wiring &w,
            const ServiceAdaptorPath &user_path)
        {
            using input_schema = input_schema_t<Interface>;
            using requests_schema = request_input_schema_t<Interface>;

            std::string full_path = adaptor_from_graph_path<Interface>(user_path);
            const auto *input_meta = service::detail::resolved_schema_meta<input_schema>(
                user_path.resolution, "service adaptor input");
            const auto *out_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), input_meta);

            WiringNodeSchema schema;
            schema.output = out_meta;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(request_input_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), input_meta]() {
                    return make_request_input_source_node(path, *input_meta);
                });
            return Port<requests_schema>{w, std::move(port)};
        }

        template <typename Interface>
        [[nodiscard]] Port<REF<request_output_schema_t<Interface>>> output_source(
            Wiring &w,
            const ServiceAdaptorPath &user_path)
        {
            using output_schema = request_output_schema_t<Interface>;

            std::string full_path = adaptor_to_graph_path<Interface>(user_path);
            const auto *response_meta = service::detail::resolved_schema_meta<output_schema_t<Interface>>(
                user_path.resolution, "service adaptor output");
            const auto *target_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), response_meta);
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);

            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            Value path_key = path_key_value(full_path);

            WiringPortRef port = w.add_node(
                std::type_index(typeid(output_source_marker)), schema,
                std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
            return Port<REF<output_schema>>{w, std::move(port)};
        }

        template <typename Interface, typename InputSchema>
        const WiringInstance *capture_request_input(Wiring &w,
                                                    Port<InputSchema> request,
                                                    Port<request_input_schema_t<Interface>> requests,
                                                    const ServiceAdaptorPath &user_path,
                                                    Port<TS<Int>> request_id)
        {
            using input_schema = input_schema_t<Interface>;

            std::array<WiringPortRef, 3> sources{
                request.erased(), requests.erased(), request_id.erased()};
            std::array<WiringInputRef, 3> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
                WiringInputRef{.source = sources[2]},
            }};
            const auto *input_meta = request.erased().schema;
            if (input_meta == nullptr)
            {
                input_meta = service::detail::resolved_schema_meta<input_schema>(
                    user_path.resolution, "service adaptor input");
            }
            NodeBuilder builder = make_request_input_capture_node(
                adaptor_from_graph_path<Interface>(user_path),
                *input_meta,
                true);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));

            WiringPortRef capture = w.add_node(std::type_index(typeid(request_input_capture_marker)),
                                               std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            // Service-adaptor sends are rank-correct and same-cycle in their
            // owning graph. A dynamically-started child hands off to its outer
            // source on the following cycle because that outer rank may already
            // have passed. Only request/reply is unconditionally deferred to
            // permit recursion.
            return capture.peered_node();
        }

        template <typename Interface, typename Impl, typename OutputSchema>
        const WiringInstance *capture_output(Wiring &w,
                                             Port<OutputSchema> output,
                                             Port<REF<request_output_schema_t<Interface>>> shared_output,
                                             const ServiceAdaptorPath &user_path)
        {
            std::array<WiringPortRef, 2> sources{output.erased(), shared_output.erased()};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            const auto *output_meta = output.erased().schema;
            if (output_meta == nullptr)
            {
                const auto *response_meta = service::detail::resolved_schema_meta<output_schema_t<Interface>>(
                    user_path.resolution, "service adaptor output");
                output_meta = TypeRegistry::instance().tsd(
                    scalar_descriptor<Int>::value_meta(), response_meta);
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
        void wire_impl(Wiring &w, const ServiceAdaptorPath &user_path, const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                static_cast<void>(wire<Impl>(w, args..., arg<"path">(Str{user_path.value})));
            }
            else
            {
                static_cast<void>(wire<Impl>(w, args...));
            }
        }

        template <typename Impl, typename OutputSchema, typename... Args>
        [[nodiscard]] Port<OutputSchema> wire_impl_output(Wiring &w,
                                                          const ServiceAdaptorPath &user_path,
                                                          const Args &...args)
        {
            if constexpr (implementation_accepts_path<Impl>())
            {
                auto output = wire<Impl>(w, args..., arg<"path">(Str{user_path.value}));
                if constexpr (schema_descriptor<OutputSchema>::is_concrete())
                {
                    return output.template as<OutputSchema>();
                }
                else
                {
                    return Port<OutputSchema>{w, output.erased()};
                }
            }
            else
            {
                auto output = wire<Impl>(w, args...);
                if constexpr (schema_descriptor<OutputSchema>::is_concrete())
                {
                    return output.template as<OutputSchema>();
                }
                else
                {
                    return Port<OutputSchema>{w, output.erased()};
                }
            }
        }

        template <typename Impl, typename... Args>
        void wire_impl_with_scope(Wiring &w,
                                  const ServiceAdaptorPath &user_path,
                                  std::string description,
                                  std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                  const Args &...args)
        {
            auto scope = w.service_implementation_scope(std::move(description), std::move(required_endpoints));
            wire_impl<Impl>(w, user_path, args...);
            scope.complete();
        }
    }  // namespace detail

    template <typename Interface, typename Impl, typename... Args>
    void register_service_adaptor(Wiring &w, ServiceAdaptorPath user_path, const Args &...args)
    {
        static_assert(detail::service_adaptor_interface<Interface>,
                      "register_service_adaptor requires a type derived from service_adaptor::interface");
        std::string base_path = detail::adaptor_base_path<Interface>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "service adaptor " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "service adaptor");
                std::vector<WiringServiceImplementationEndpoint> required_endpoints;
                detail::append_required_stub_endpoints<Interface>(required_endpoints, user_path);
                std::apply([&](const auto &...stored) {
                    detail::wire_impl_with_scope<Impl>(
                        target, user_path, "service adaptor " + base_path,
                        std::move(required_endpoints), stored...);
                }, stored_args);
            });
    }

    template <typename Interface, typename Impl, typename... Args>
    void register_service_adaptor(Wiring &w, const Args &...args)
    {
        register_service_adaptor<Interface, Impl>(w, detail::default_adaptor_path<Interface>(), args...);
    }

    template <typename Impl, typename... Interfaces, typename... Args>
    void register_service_adaptors(Wiring &w, ServiceAdaptorPath user_path, const Args &...args)
    {
        static_assert(sizeof...(Interfaces) > 0,
                      "register_service_adaptors requires at least one service adaptor interface");
        static_assert((detail::service_adaptor_interface<Interfaces> && ...),
                      "register_service_adaptors requires service_adaptor::interface descriptor types");
        std::vector<std::string> base_paths{
            detail::adaptor_base_path<Interfaces>(user_path)...};
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            std::move(base_paths), "multi-service-adaptor implementation",
            [user_path = std::move(user_path), stored_args = std::move(stored_args)](Wiring &target) {
                (target.register_built_service_path(
                    detail::adaptor_base_path<Interfaces>(user_path), "service adaptor"), ...);
                std::vector<WiringServiceImplementationEndpoint> required_endpoints;
                (detail::append_required_stub_endpoints<Interfaces>(required_endpoints, user_path), ...);
                std::apply([&](const auto &...stored) {
                    detail::wire_impl_with_scope<Impl>(
                        target, user_path, "multi-service-adaptor implementation",
                        std::move(required_endpoints), stored...);
                }, stored_args);
            });
    }

    template <typename Interface>
    [[nodiscard]] Port<detail::request_input_schema_t<Interface>> from_graph(
        Wiring &w,
        ServiceAdaptorPath user_path);

    template <typename Interface, typename OutputPort>
    void to_graph(Wiring &w, ServiceAdaptorPath user_path, OutputPort output);

    template <typename Interface, typename Impl, typename... Args>
    void register_service_adaptor_impl(Wiring &w, ServiceAdaptorPath user_path, const Args &...args)
    {
        static_assert(detail::service_adaptor_interface<Interface>,
                      "register_service_adaptor_impl requires a service adaptor interface");

        std::string base_path = detail::adaptor_base_path<Interface>(user_path);
        auto stored_args = std::tuple<std::decay_t<Args>...>{args...};
        w.register_service_implementation_candidate(
            {base_path}, "service adaptor impl " + base_path,
            [user_path = std::move(user_path), stored_args = std::move(stored_args), base_path](Wiring &target) {
                target.register_built_service_path(base_path, "service adaptor");
                std::vector<WiringServiceImplementationEndpoint> required_endpoints;
                detail::append_required_stub_endpoints<Interface>(required_endpoints, user_path);
                auto scope = target.service_implementation_scope(
                    "service adaptor impl " + base_path, std::move(required_endpoints));
                auto requests = from_graph<Interface>(target, user_path);
                if constexpr (detail::has_output_schema<Interface>::value)
                {
                    using output_schema = detail::request_output_schema_t<Interface>;
                    auto replies = std::apply([&](const auto &...stored) {
                        return detail::wire_impl_output<Impl, output_schema>(
                            target, user_path, requests, stored...);
                    }, stored_args);
                    to_graph<Interface>(target, user_path, replies);
                }
                else
                {
                    std::apply([&](const auto &...stored) {
                        detail::wire_impl<Impl>(target, user_path, requests, stored...);
                    }, stored_args);
                }
                scope.complete();
            });
    }

    template <typename Interface, typename Impl, typename... Args>
    void register_service_adaptor_impl(Wiring &w, const Args &...args)
    {
        register_service_adaptor_impl<Interface, Impl>(
            w, detail::default_adaptor_path<Interface>(), args...);
    }

    template <typename Interface>
    [[nodiscard]] Port<detail::request_input_schema_t<Interface>> from_graph(
        Wiring &w,
        ServiceAdaptorPath user_path)
    {
        static_assert(detail::service_adaptor_interface<Interface>,
                      "service_adaptor::from_graph requires a service adaptor interface");
        const std::string endpoint = detail::adaptor_from_graph_path<Interface>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "service adaptor");
        auto source = detail::request_input_source<Interface>(w, user_path);
        w.register_service_rank_anchor(detail::adaptor_from_graph_path<Interface>(user_path), source.node());
        return source;
    }

    template <typename Interface>
    [[nodiscard]] Port<detail::request_input_schema_t<Interface>> from_graph(Wiring &w)
    {
        return from_graph<Interface>(w, detail::default_adaptor_path<Interface>());
    }

    struct explicit_impl_output_marker
    {
    };

    template <typename Interface, typename OutputPort>
    void to_graph(Wiring &w,
                  ServiceAdaptorPath user_path,
                  OutputPort output)
    {
        static_assert(detail::service_adaptor_interface<Interface>,
                      "service_adaptor::to_graph requires a service adaptor interface");
        static_assert(detail::has_output_schema<Interface>::value,
                      "service_adaptor::to_graph requires an interface with output_schema");
        const std::string endpoint = detail::adaptor_to_graph_path<Interface>(user_path);
        wiring_path_detail::merge_resolution(
            user_path.resolution, w.service_implementation_stub_resolution(endpoint));
        w.register_service_implementation_stub(endpoint, "service adaptor");
        auto shared_output = detail::output_source<Interface>(w, user_path);
        const WiringInstance *capture = detail::capture_output<Interface, explicit_impl_output_marker>(
            w, std::move(output), shared_output, user_path);
        w.register_service_rank_anchor(detail::adaptor_to_graph_path<Interface>(user_path), capture);
    }

    template <typename Interface>
    void to_graph(Wiring &w, auto output)
    {
        to_graph<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(output));
    }

    template <typename Interface>
    auto adaptor(
        Wiring &w,
        ServiceAdaptorPath user_path,
        auto input)
    {
        static_assert(detail::service_adaptor_interface<Interface>,
                      "service_adaptor::adaptor requires a service adaptor interface");

        user_path = detail::resolve_client_path<Interface>(std::move(user_path), input);
        auto request_id      = service::detail::request_id_source(w);
        w.register_service_client_path(detail::adaptor_base_path<Interface>(user_path), "service adaptor");
        const std::string from_endpoint = detail::adaptor_from_graph_path<Interface>(user_path);
        auto requests        = detail::request_input_source<Interface>(w, user_path);
        w.register_service_rank_anchor(from_endpoint, requests.node());
        const WiringInstance *capture =
            detail::capture_request_input<Interface>(w, std::move(input), requests, user_path, request_id);
        w.register_service_client_rank(from_endpoint, "service adaptor", capture, false);
        if constexpr (detail::has_output_schema<Interface>::value)
        {
            using replies_schema = detail::request_output_schema_t<Interface>;
            using output_schema = detail::output_schema_t<Interface>;
            const std::string to_endpoint = detail::adaptor_to_graph_path<Interface>(user_path);
            auto replies = detail::output_source<Interface>(w, user_path);
            w.register_service_client_rank(to_endpoint, "service adaptor", replies.node(), true);
            if constexpr (schema_descriptor<replies_schema>::is_concrete())
            {
                auto reply = wire<stdlib::getitem_>(w, replies.template as<replies_schema>(), request_id);
                if constexpr (schema_descriptor<output_schema>::is_concrete())
                {
                    return reply.template as<output_schema>();
                }
                else
                {
                    return Port<output_schema>{w, reply.erased()};
                }
            }
            else
            {
                auto dict = Port<replies_schema>{w, replies.erased()};
                auto reply = wire<stdlib::getitem_>(w, dict, request_id);
                return Port<output_schema>{w, reply.erased()};
            }
        }
    }

    template <typename Interface>
    auto adaptor(
        Wiring &w,
        auto input)
    {
        return adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), std::move(input));
    }

    /** Pack the fields of a TSB request schema from separate client ports. */
    template <typename Interface, typename... Inputs>
        requires(sizeof...(Inputs) > 1)
    auto adaptor(
        Wiring &w,
        ServiceAdaptorPath user_path,
        const Inputs &...inputs)
    {
        using input_schema = detail::input_schema_t<Interface>;
        static_assert(stdlib::collection_detail::is_tsb_schema_v<input_schema>,
                      "multi-input service adaptors require a TSB input_schema");
        return adaptor<Interface>(
            w, std::move(user_path), stdlib::to_tsb<input_schema>(w, inputs...));
    }

    /** Pack the fields of a TSB request schema from separate client ports. */
    template <typename Interface, typename... Inputs>
        requires(sizeof...(Inputs) > 1)
    auto adaptor(
        Wiring &w,
        const Inputs &...inputs)
    {
        return adaptor<Interface>(w, detail::default_adaptor_path<Interface>(), inputs...);
    }
}  // namespace hgraph::service_adaptor

namespace hgraph::graph_wiring_detail
{
    template <typename Interface, typename OutSchema, typename... Args>
        requires service_adaptor::detail::service_adaptor_interface<Interface>
    struct wire_customization<Interface, OutSchema, Args...>
    {
        static constexpr bool enabled = true;

        static auto wire(Wiring &w, const Args &...args)
        {
            static_assert(std::is_void_v<OutSchema>,
                          "wire<ServiceAdaptor, OutSchema>: output schema is defined by the interface");
            return service_adaptor::adaptor<Interface>(w, args...);
        }
    };

    template <typename Service, typename OutSchema, typename... Args>
        requires service::detail::service_interface<Service>
    struct wire_customization<Service, OutSchema, Args...>
    {
        static constexpr bool enabled = true;

        static auto wire(Wiring &w, const Args &...args)
        {
            static_assert(std::is_void_v<OutSchema>,
                          "wire<Service, OutSchema>: service output schema is defined by the service descriptor");

            auto arg_tuple = std::forward_as_tuple(args...);
            if constexpr (service::detail::reference_service_interface<Service>)
            {
                static_assert(sizeof...(Args) <= 1,
                              "wire<ReferenceService>: expected zero arguments or service::path(...)");
                if constexpr (sizeof...(Args) == 0)
                {
                    return service::reference_service<Service>(w);
                }
                else
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(service::detail::is_service_path_v<A0>,
                                  "wire<ReferenceService>: first argument must be service::path(...)");
                    return service::reference_service<Service>(w, std::get<0>(arg_tuple));
                }
            }
            else if constexpr (service::detail::subscription_service_interface<Service>)
            {
                static_assert(sizeof...(Args) == 1 || sizeof...(Args) == 2,
                              "wire<SubscriptionService>: expected key or service::path(...), key");
                if constexpr (sizeof...(Args) == 1)
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(!service::detail::is_service_path_v<A0>,
                                  "wire<SubscriptionService>: missing key after service::path(...)");
                    return service::subscription_service<Service>(w)(std::get<0>(arg_tuple));
                }
                else
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(service::detail::is_service_path_v<A0>,
                                  "wire<SubscriptionService>: first argument must be service::path(...)");
                    return service::subscription_service<Service>(w, std::get<0>(arg_tuple))(std::get<1>(arg_tuple));
                }
            }
            else if constexpr (service::detail::request_reply_service_interface<Service>)
            {
                static_assert(sizeof...(Args) == 1 || sizeof...(Args) == 2,
                              "wire<RequestReplyService>: expected request or service::path(...), request");
                if constexpr (sizeof...(Args) == 1)
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(!service::detail::is_service_path_v<A0>,
                                  "wire<RequestReplyService>: missing request after service::path(...)");
                    return service::request_reply_service<Service>(w, std::get<0>(arg_tuple));
                }
                else
                {
                    using A0 = std::tuple_element_t<0, std::tuple<Args...>>;
                    static_assert(service::detail::is_service_path_v<A0>,
                                  "wire<RequestReplyService>: first argument must be service::path(...)");
                    return service::request_reply_service<Service>(w, std::get<1>(arg_tuple), std::get<0>(arg_tuple));
                }
            }
            else
            {
                static_assert(service::detail::always_false_v<Service>, "unsupported service descriptor");
            }
        }
    };
}  // namespace hgraph::graph_wiring_detail

#endif  // HGRAPH_TYPES_SERVICE_WIRING_H
