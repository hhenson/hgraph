#include <hgraph/types/service_runtime.h>

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/keyed_service_transport.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/adaptor_wiring.h>
#include <hgraph/types/request_reply_transport.h>
#include <hgraph/types/service_wiring.h>

#include <unordered_set>

#include <array>
#include <algorithm>
#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>

namespace hgraph
{
    namespace
    {
        struct DescriptorIdentity
        {
            std::string                name;
            std::string                specialization;
            ServiceFlavour             flavour;
            const TSValueTypeMetaData *output_schema;
            const ValueTypeMetaData   *key_type;
            const TSValueTypeMetaData *value_schema;
            const TSValueTypeMetaData *request_schema;
            const TSValueTypeMetaData *response_schema;
            const TSValueTypeMetaData *input_schema;

            friend bool operator==(const DescriptorIdentity &, const DescriptorIdentity &) = default;
        };

        struct DescriptorIdentityHash
        {
            [[nodiscard]] std::size_t operator()(const DescriptorIdentity &identity) const noexcept
            {
                std::size_t seed = 0;
                const auto combine = [&seed](const auto &value) {
                    const std::size_t hash = std::hash<std::decay_t<decltype(value)>>{}(value);
                    seed ^= hash + 0x9e3779b9U + (seed << 6U) + (seed >> 2U);
                };
                combine(identity.name);
                combine(identity.specialization);
                combine(identity.flavour);
                combine(identity.output_schema);
                combine(identity.key_type);
                combine(identity.value_schema);
                combine(identity.request_schema);
                combine(identity.response_schema);
                combine(identity.input_schema);
                return seed;
            }
        };

        [[nodiscard]] DescriptorIdentity descriptor_identity(const RuntimeServiceDescriptor &descriptor)
        {
            return DescriptorIdentity{
                .name            = descriptor.name,
                .specialization  = descriptor.specialization,
                .flavour         = descriptor.flavour,
                .output_schema   = descriptor.output_schema,
                .key_type        = descriptor.key_type,
                .value_schema    = descriptor.value_schema,
                .request_schema  = descriptor.request_schema,
                .response_schema = descriptor.response_schema,
                .input_schema    = descriptor.input_schema,
            };
        }

        /** Immortal (registry rule): descriptor addresses must stay stable. */
        [[nodiscard]] std::unordered_map<DescriptorIdentity, RuntimeServiceDescriptor *, DescriptorIdentityHash> &
        descriptor_registry()
        {
            static auto *registry =
                new std::unordered_map<DescriptorIdentity, RuntimeServiceDescriptor *, DescriptorIdentityHash>{};
            return *registry;
        }

        [[nodiscard]] std::string user_or_default_path(const RuntimeServiceDescriptor &descriptor,
                                                       std::string_view path)
        {
            if (!path.empty()) { return std::string{path}; }
            if (!descriptor.default_path.empty()) { return descriptor.default_path; }
            return descriptor.name + "_default";
        }

        // The name-qualified full-path grammar — IDENTICAL to the template
        // side (service_full_path): "<prefix><user-path>/<name>".
        [[nodiscard]] std::string full_path(std::string_view prefix, const RuntimeServiceDescriptor &descriptor,
                                            std::string_view user_path)
        {
            std::string resolved = user_or_default_path(descriptor, user_path);
            if (resolved.starts_with(prefix)) { return resolved; }
            std::string result{prefix};
            result.append(resolved);
            result.push_back('/');
            result.append(descriptor.name);
            return result;
        }

        [[nodiscard]] std::string reference_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("ref_svc://", d, p);
        }

        [[nodiscard]] std::string subscription_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("subs_svc://", d, p);
        }

        [[nodiscard]] std::string request_reply_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("reqrepl_svc://", d, p);
        }

        [[nodiscard]] std::string service_adaptor_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("service_adaptor://", d, p);
        }

        void require_flavour(const RuntimeServiceDescriptor &descriptor, ServiceFlavour flavour, const char *what)
        {
            if (descriptor.flavour != flavour)
            {
                throw std::invalid_argument("service '" + descriptor.name + "': not a " + what + " service");
            }
        }

        // --- the erased node factories (the template detail bodies over
        // runtime metas; same role markers, same node makers) ---

        [[nodiscard]] WiringPortRef shared_output_source_node(Wiring &w, std::type_index role,
                                                              const TSValueTypeMetaData *target_meta,
                                                              std::string path)
        {
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);
            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            return w.add_node(role, schema, std::span<const WiringPortRef>{},
                              service::detail::path_key_value(path),
                              [path, target_meta]() { return make_shared_output_source_node(path, *target_meta); });
        }

        [[nodiscard]] WiringPortRef request_id_source_node(Wiring &w)
        {
            return w.add_unique_node(
                std::type_index(typeid(service::detail::request_id_source_marker)),
                make_request_id_source_node(), std::span<const WiringPortRef>{}, Value{});
        }

        [[nodiscard]] const WiringInstance *shared_output_capture_node(Wiring &w, std::type_index role,
                                                                       const TSValueTypeMetaData *output_meta,
                                                                       const std::string &path,
                                                                       const WiringPortRef &output,
                                                                       const WiringPortRef &shared_output)
        {
            std::array<WiringPortRef, 2>  sources{output, shared_output};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(path, *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef capture = w.add_node(role, std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()},
                                               Value{});
            // Shared-output relays are rank-correct and same-cycle (see the
            // template counterpart in service_wiring.h).
            w.add_same_cycle_pair(capture.peered_node(), shared_output.peered_node());
            return capture.peered_node();
        }

        /** Combine the flavour's own transport input (if any) with the extra
         *  time-series inputs supplied at registration, and check the total
         *  against the implementation's arity. Registration inputs follow the
         *  flavour input, matching the adaptor contract. */
        [[nodiscard]] std::vector<WiringPortRef> combine_impl_inputs(
            const RuntimeServiceDescriptor &descriptor,
            const WiredFn &impl,
            std::span<const WiringPortRef> flavour_inputs,
            std::span<const WiringPortRef> registration_inputs)
        {
            std::vector<WiringPortRef> inputs;
            inputs.reserve(flavour_inputs.size() + registration_inputs.size());
            inputs.insert(inputs.end(), flavour_inputs.begin(), flavour_inputs.end());
            inputs.insert(inputs.end(), registration_inputs.begin(), registration_inputs.end());
            if (impl.arity != inputs.size())
            {
                throw std::invalid_argument(
                    "service '" + descriptor.name +
                    "' implementation input count does not match supplied inputs");
            }
            return inputs;
        }

        [[nodiscard]] WiringPortRef wire_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                              const WiredFn &impl, std::span<const WiringPortRef> inputs)
        {
            if (!impl.valid())
            {
                throw std::invalid_argument("service '" + descriptor.name + "': implementation is not wirable");
            }
            return impl.wire(w, inputs);
        }

        [[nodiscard]] WiringPortRef describe_service_output(
            const RuntimeServiceDescriptor &descriptor,
            const TSValueTypeMetaData *expected,
            WiringPortRef output)
        {
            if (expected == nullptr || output.schema == nullptr ||
                !graph_wiring_detail::input_accepts_output_schema(expected, output.schema))
            {
                throw std::invalid_argument(
                    "service '" + descriptor.name +
                    "' implementation output does not match the interface schema");
            }
            // Match the typed service path's Port::as<InterfaceOutput>(). The
            // implementation may expose a REF-transparent storage shape (for
            // example map_ of a structural child), but that representation is
            // private to the implementation and must not leak through the
            // declared service boundary.
            output.schema = expected;
            return output;
        }
    }  // namespace

    const RuntimeServiceDescriptor &intern_service_descriptor(RuntimeServiceDescriptor descriptor)
    {
        if (descriptor.name.empty()) { throw std::invalid_argument("service descriptor requires a name"); }
        auto &registry = descriptor_registry();
        auto  found    = registry.find(descriptor_identity(descriptor));
        if (found != registry.end()) { return *found->second; }
        auto *record = new RuntimeServiceDescriptor{std::move(descriptor)};
        registry.emplace(descriptor_identity(*record), record);
        return *record;
    }

    const RuntimeServiceDescriptor *find_service_descriptor(std::string_view name)
    {
        const auto &registry = descriptor_registry();
        const RuntimeServiceDescriptor *result = nullptr;
        for (const auto &[identity, descriptor] : registry)
        {
            if (identity.name != name || !identity.specialization.empty()) { continue; }
            if (result != nullptr) { return nullptr; }
            result = descriptor;
        }
        return result;
    }

    std::vector<std::string> service_descriptor_names()
    {
        std::vector<std::string> names;
        names.reserve(descriptor_registry().size());
        for (const auto &[identity, descriptor] : descriptor_registry())
        {
            (void)identity;
            names.push_back(descriptor->name);
        }
        std::sort(names.begin(), names.end());
        names.erase(std::unique(names.begin(), names.end()), names.end());
        return names;
    }

    // ------------------------------------------------------------------
    // Reference
    // ------------------------------------------------------------------

    WiringPortRef reference_service_client(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                           std::string_view path)
    {
        require_flavour(descriptor, ServiceFlavour::Reference, "reference");
        const std::string base = reference_base(descriptor, path);
        w.register_service_client_path(
            base, "reference service", descriptor.name, descriptor.specialization);
        WiringPortRef source = shared_output_source_node(
            w, std::type_index(typeid(service::detail::reference_output_source_marker)),
            descriptor.output_schema, base);
        w.register_service_client_rank(base, "reference service", source.peered_node(), true);
        // Clients read the impl output by REF; the typed facade (Port::as)
        // erases to a descriptive-schema patch - input binding inserts the
        // REF adaptation exactly as for the template path.
        source.schema = descriptor.output_schema;
        return source;
    }

    void register_reference_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                         std::string_view path, const WiredFn &impl,
                                         std::span<const WiringPortRef> implementation_inputs,
                                         bool default_fallback)
    {
        require_flavour(descriptor, ServiceFlavour::Reference, "reference");
        const std::string base = reference_base(descriptor, path);
        const auto *descriptor_ptr = &descriptor;
        std::vector<WiringPortRef> stored_inputs{
            implementation_inputs.begin(), implementation_inputs.end()};
        auto materialize = [descriptor_ptr, impl, stored_inputs](Wiring &target,
                                                                 std::string_view requested_path) {
                const std::string requested_base = reference_base(*descriptor_ptr, requested_path);
                target.register_built_service_path(requested_base, "reference service");
                WiringPortRef shared = shared_output_source_node(
                    target, std::type_index(typeid(service::detail::reference_output_source_marker)),
                    descriptor_ptr->output_schema, requested_base);
                // A reference service has no transport input, so the whole
                // input list is the registration configuration.
                const auto impl_inputs = combine_impl_inputs(
                    *descriptor_ptr, impl, {}, stored_inputs);
                WiringPortRef output = describe_service_output(
                    *descriptor_ptr, descriptor_ptr->output_schema,
                    wire_impl(target, *descriptor_ptr, impl, impl_inputs));
                const WiringInstance *capture = shared_output_capture_node(
                    target, std::type_index(typeid(service::detail::reference_output_capture_marker)),
                    descriptor_ptr->output_schema, requested_base, output, shared);
                target.register_service_rank_anchor(requested_base, capture);
            };
        if (default_fallback)
        {
            w.register_default_service_implementation_candidate(
                "ref_svc://", "/" + descriptor.name,
                "default reference service " + descriptor.name, std::move(materialize));
        }
        else
        {
            w.register_service_implementation_candidate(
                {base}, "reference service " + base,
                [materialize = std::move(materialize), base](Wiring &target) {
                    materialize(target, base);
                });
        }
    }

    // ------------------------------------------------------------------
    // Subscription
    // ------------------------------------------------------------------

    WiringPortRef subscription_service_subscribe(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                 std::string_view path, const WiringPortRef &key)
    {
        require_flavour(descriptor, ServiceFlavour::Subscription, "subscription");
        const std::string base       = subscription_base(descriptor, path);
        const std::string subs_path  = base + "/subs";
        const std::string out_path   = base + "/out";
        w.register_service_client_path(
            base, "subscription service", descriptor.name, descriptor.specialization);

        const auto *subs_meta = TypeRegistry::instance().tss(descriptor.key_type);
        WiringNodeSchema subs_schema;
        subs_schema.output = subs_meta;
        WiringPortRef subscriptions = w.add_node(
            std::type_index(typeid(service::detail::subscription_source_marker)), subs_schema,
            std::span<const WiringPortRef>{}, service::detail::path_key_value(subs_path),
            [subs_path, key_meta = descriptor.key_type]() {
                return make_subscription_key_source_node(subs_path, *key_meta);
            });

        const auto *dict_meta = TypeRegistry::instance().tsd(descriptor.key_type, descriptor.value_schema);
        WiringPortRef shared = shared_output_source_node(
            w, std::type_index(typeid(service::detail::shared_output_source_marker)), dict_meta, out_path);

        w.register_service_rank_anchor(subs_path, subscriptions.peered_node());
        // Subscription keys use the same input adaptation as request/reply
        // payloads. In particular, a concrete Bundle leaf is materialized in
        // the service interface's closed base union before capture.
        WiringPortRef adapted_key = graph_wiring_detail::adapt_source_for_input(
            w, TypeRegistry::instance().ts(descriptor.key_type), key);
        // The subscribed value: the shared TSD (descriptive-schema patched,
        // as Port::as does) keyed at the subscribed key.
        WiringPortRef dict = shared;
        dict.schema        = dict_meta;
        WiringArg dict_arg;
        dict_arg.kind = WiringArg::Kind::TimeSeries;
        dict_arg.port = dict;
        WiringArg key_arg;
        key_arg.kind = WiringArg::Kind::TimeSeries;
        key_arg.port = adapted_key;
        std::array<WiringArg, 2> item_args{dict_arg, key_arg};
        WiringPortRef output = wire_operator(
            w, "getitem_", std::span<const WiringArg>{item_args.data(), item_args.size()}).output;
        output.schema = descriptor.value_schema;
        WiringPortRef gated = keyed_service_transport::defer_subscription_client(
            w,
            base,
            subs_path,
            out_path,
            *descriptor.key_type,
            *descriptor.value_schema,
            std::move(adapted_key),
            subscriptions,
            std::move(output),
            shared,
            std::type_index(typeid(service::detail::subscription_capture_marker)),
            std::type_index(typeid(service::detail::subscription_response_gate_marker)));
        gated.schema = descriptor.value_schema;
        return gated;
    }

    void register_subscription_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                            std::string_view path, const WiredFn &impl,
                                            std::span<const WiringPortRef> implementation_inputs,
                                            bool default_fallback)
    {
        require_flavour(descriptor, ServiceFlavour::Subscription, "subscription");
        const std::string base      = subscription_base(descriptor, path);
        const auto *descriptor_ptr = &descriptor;
        std::vector<WiringPortRef> stored_inputs{
            implementation_inputs.begin(), implementation_inputs.end()};
        auto materialize = [descriptor_ptr, impl, stored_inputs](Wiring &target,
                                                                 std::string_view requested_path) {
                const std::string requested_base = subscription_base(*descriptor_ptr, requested_path);
                const std::string subs_path = requested_base + "/subs";
                const std::string out_path  = requested_base + "/out";
                target.register_built_service_path(requested_base, "subscription service");
                const auto *subs_meta = TypeRegistry::instance().tss(descriptor_ptr->key_type);
                WiringNodeSchema subs_schema;
                subs_schema.output = subs_meta;
                WiringPortRef subscriptions = target.add_node(
                    std::type_index(typeid(service::detail::subscription_source_marker)), subs_schema,
                    std::span<const WiringPortRef>{}, service::detail::path_key_value(subs_path),
                    [subs_path, key_meta = descriptor_ptr->key_type]() {
                        return make_subscription_key_source_node(subs_path, *key_meta);
                    });
                const auto *dict_meta = TypeRegistry::instance().tsd(
                    descriptor_ptr->key_type, descriptor_ptr->value_schema);
                WiringPortRef shared = shared_output_source_node(
                    target, std::type_index(typeid(service::detail::shared_output_source_marker)),
                    dict_meta, out_path);
                target.register_service_rank_anchor(subs_path, subscriptions.peered_node());
                auto scope = target.service_implementation_scope(
                    "subscription service " + requested_base,
                    std::vector<WiringServiceImplementationEndpoint>{}, false);
                keyed_service_transport::register_implementation_input(
                    target, requested_base, subscriptions.peered_node());
                std::array<WiringPortRef, 1> transport{subscriptions};
                const auto impl_inputs = combine_impl_inputs(
                    *descriptor_ptr, impl, transport, stored_inputs);
                WiringPortRef output = describe_service_output(
                    *descriptor_ptr, dict_meta,
                    wire_impl(target, *descriptor_ptr, impl, impl_inputs));
                keyed_service_transport::defer_subscription_implementation_output(
                    target,
                    requested_base,
                    out_path,
                    *dict_meta,
                    std::move(output),
                    std::move(shared),
                    std::type_index(typeid(service::detail::shared_output_capture_marker)));
                scope.complete();
            };
        if (default_fallback)
        {
            w.register_default_service_implementation_candidate(
                "subs_svc://", "/" + descriptor.name,
                "default subscription service " + descriptor.name, std::move(materialize));
        }
        else
        {
            w.register_service_implementation_candidate(
                {base}, "subscription service " + base,
                [materialize = std::move(materialize), base](Wiring &target) {
                    materialize(target, base);
                });
        }
    }

    // ------------------------------------------------------------------
    // Request/reply
    // ------------------------------------------------------------------

    namespace
    {
        [[nodiscard]] WiringPortRef keyed_request_input_source_node(
            Wiring &w, const TSValueTypeMetaData *request_schema, const std::string &request_path,
            std::type_index role)
        {
            const auto *dict_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), request_schema);
            WiringNodeSchema schema;
            schema.output = dict_meta;
            return w.add_node(
                role, schema, std::span<const WiringPortRef>{},
                service::detail::path_key_value(request_path),
                [request_path, request_schema]() {
                    return make_request_input_source_node(request_path, *request_schema);
                });
        }

        [[nodiscard]] WiringPortRef request_input_source_node(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                              const std::string &request_path)
        {
            return keyed_request_input_source_node(
                w, descriptor.request_schema, request_path,
                std::type_index(typeid(service::detail::request_input_source_marker)));
        }

        [[nodiscard]] WiringPortRef keyed_reply_output_source_node(
            Wiring &w, const TSValueTypeMetaData *response_schema, const std::string &replies_path,
            std::type_index role)
        {
            const auto *dict_meta = TypeRegistry::instance().tsd(
                scalar_descriptor<Int>::value_meta(), response_schema);
            return shared_output_source_node(w, role, dict_meta, replies_path);
        }

        [[nodiscard]] WiringPortRef reply_output_source_node(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                             const std::string &replies_path)
        {
            return keyed_reply_output_source_node(
                w, descriptor.response_schema, replies_path,
                std::type_index(typeid(service::detail::request_reply_output_source_marker)));
        }
    }  // namespace

    WiringPortRef request_reply_service_call(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path, const WiringPortRef &request)
    {
        require_flavour(descriptor, ServiceFlavour::RequestReply, "request/reply");
        const std::string base         = request_reply_base(descriptor, path);
        const std::string request_path = base + "/request";
        const std::string replies_path = base + "/replies";
        w.register_service_client_path(
            base, "request/reply service", descriptor.name, descriptor.specialization);

        WiringPortRef request_id = request_id_source_node(w);
        WiringPortRef requests = request_input_source_node(w, descriptor, request_path);
        w.register_service_rank_anchor(request_path, requests.peered_node());

        // A REPLY-LESS service is a keyed sink and takes the ranked same-cycle
        // relay (RFC 0012). Reply-full clients are deferred until their
        // implementation has materialized so RFC 0014 can choose the least
        // costly transport that preserves its dependency cycles.
        const bool reply_less = descriptor.response_schema == nullptr;

        if (reply_less)
        {
            WiringPortRef adapted_request = graph_wiring_detail::adapt_source_for_input(
                w, descriptor.request_schema, request);
            std::array<WiringPortRef, 3> sources{
                std::move(adapted_request), requests, request_id};
            std::array<WiringInputRef, 3> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
                WiringInputRef{.source = sources[2]},
            }};
            NodeBuilder builder = make_request_input_capture_node(
                request_path, *descriptor.request_schema, true);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef capture = w.add_node(
                std::type_index(typeid(service::detail::request_input_capture_marker)),
                std::move(builder),
                std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{});
            // A reply-less client is a sending boundary, like a sink-only
            // adaptor. At the root the capture ranks before the request source.
            // A dynamically-started nested capture retains the runtime's
            // next-cycle outer hand-off, matching released hgraph.
            w.register_service_client_rank(
                request_path, "request/reply service", capture.peered_node(), false);
            return {};
        }

        WiringPortRef replies = reply_output_source_node(w, descriptor, replies_path);
        WiringPortRef adapted_request = graph_wiring_detail::adapt_source_for_input(
            w, descriptor.request_schema, request);
        request_reply_transport::defer_client(
            w,
            base,
            request_path,
            replies_path,
            *descriptor.request_schema,
            std::move(adapted_request),
            requests,
            request_id,
            replies,
            std::type_index(typeid(service::detail::request_input_capture_marker)));

        WiringPortRef dict = replies;
        dict.schema        = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                          descriptor.response_schema);
        WiringArg dict_arg;
        dict_arg.kind = WiringArg::Kind::TimeSeries;
        dict_arg.port = dict;
        WiringArg id_arg;
        id_arg.kind = WiringArg::Kind::TimeSeries;
        id_arg.port = request_id;
        std::array<WiringArg, 2> item_args{dict_arg, id_arg};
        WiringPortRef output = wire_operator(
            w, "getitem_", std::span<const WiringArg>{item_args.data(), item_args.size()}).output;
        output.schema = descriptor.response_schema;
        return output;
    }

    void register_request_reply_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path, const WiredFn &impl,
                                             std::span<const WiringPortRef> implementation_inputs,
                                             bool default_fallback)
    {
        require_flavour(descriptor, ServiceFlavour::RequestReply, "request/reply");
        const std::string base         = request_reply_base(descriptor, path);
        const auto *descriptor_ptr = &descriptor;
        std::vector<WiringPortRef> stored_inputs{
            implementation_inputs.begin(), implementation_inputs.end()};
        auto materialize = [descriptor_ptr, impl, stored_inputs](Wiring &target,
                                                                 std::string_view requested_path) {
                const std::string requested_base = request_reply_base(*descriptor_ptr, requested_path);
                const std::string request_path = requested_base + "/request";
                const std::string replies_path = requested_base + "/replies";
                target.register_built_service_path(requested_base, "request/reply service");
                WiringPortRef requests = request_input_source_node(target, *descriptor_ptr, request_path);
                target.register_service_rank_anchor(request_path, requests.peered_node());
                std::array<WiringPortRef, 1> transport{requests};
                const auto impl_inputs = combine_impl_inputs(
                    *descriptor_ptr, impl, transport, stored_inputs);
                if (descriptor_ptr->response_schema == nullptr)
                {
                    WiringPortRef output = wire_impl(
                        target, *descriptor_ptr, impl, impl_inputs);
                    if (output.schema != nullptr)
                    {
                        throw std::invalid_argument("reply-less service '" + descriptor_ptr->name +
                                                    "' implementation returned an output");
                    }
                    return;
                }
                auto scope = target.service_implementation_scope(
                    "request/reply service " + requested_base,
                    std::vector<WiringServiceImplementationEndpoint>{}, false);
                request_reply_transport::register_implementation_input(
                    target, requested_base, requests.peered_node());
                WiringPortRef output = wire_impl(
                    target, *descriptor_ptr, impl, impl_inputs);
                WiringPortRef replies = reply_output_source_node(target, *descriptor_ptr, replies_path);
                const auto *dict_meta = TypeRegistry::instance().tsd(
                    scalar_descriptor<Int>::value_meta(), descriptor_ptr->response_schema);
                output = describe_service_output(*descriptor_ptr, dict_meta, std::move(output));
                request_reply_transport::defer_implementation_output(
                    target,
                    requested_base,
                    replies_path,
                    *dict_meta,
                    std::move(output),
                    std::move(replies),
                    std::type_index(typeid(service::detail::request_reply_output_capture_marker)));
                scope.complete();
            };
        if (default_fallback)
        {
            w.register_default_service_implementation_candidate(
                "reqrepl_svc://", "/" + descriptor.name,
                "default request/reply service " + descriptor.name, std::move(materialize));
        }
        else
        {
            w.register_service_implementation_candidate(
                {base}, "request/reply service " + base,
                [materialize = std::move(materialize), base](Wiring &target) {
                    materialize(target, base);
                });
        }
    }

    // ------------------------------------------------------------------
    // Multi-interface implementations (register_services + impl_input /
    // impl_output, erased)
    // ------------------------------------------------------------------

    namespace
    {
        [[nodiscard]] std::string adaptor_base(
            const RuntimeServiceDescriptor &d, std::string_view p)
        {
            return full_path("adaptor://", d, p);
        }

        void append_adaptor_required_endpoints(
            const RuntimeServiceDescriptor &descriptor,
            const std::string &base,
            std::vector<WiringServiceImplementationEndpoint> &endpoints)
        {
            if (descriptor.input_schema != nullptr)
            {
                endpoints.push_back(
                    WiringServiceImplementationEndpoint{base + "/from_graph", {}});
            }
            if (descriptor.output_schema != nullptr)
            {
                endpoints.push_back(
                    WiringServiceImplementationEndpoint{base + "/to_graph", {}});
            }
        }

        [[nodiscard]] std::string flavour_base(const RuntimeServiceDescriptor &d, std::string_view p)
        {
            switch (d.flavour)
            {
                case ServiceFlavour::Reference: return reference_base(d, p);
                case ServiceFlavour::Subscription: return subscription_base(d, p);
                case ServiceFlavour::RequestReply: return request_reply_base(d, p);
                case ServiceFlavour::ServiceAdaptor: return service_adaptor_base(d, p);
                case ServiceFlavour::Adaptor: return adaptor_base(d, p);
            }
            throw std::invalid_argument("service '" + d.name + "': not a service flavour");
        }

        [[nodiscard]] std::string_view flavour_prefix(ServiceFlavour flavour)
        {
            switch (flavour)
            {
                case ServiceFlavour::Reference: return "ref_svc://";
                case ServiceFlavour::Subscription: return "subs_svc://";
                case ServiceFlavour::RequestReply: return "reqrepl_svc://";
                case ServiceFlavour::ServiceAdaptor: return "service_adaptor://";
                case ServiceFlavour::Adaptor: return "adaptor://";
            }
            throw std::invalid_argument("unknown service flavour");
        }
    }  // namespace

    WiringPortRef service_impl_input(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path)
    {
        switch (descriptor.flavour)
        {
            case ServiceFlavour::Subscription: {
                const std::string endpoint = subscription_base(descriptor, path) + "/subs";
                w.register_service_implementation_stub(endpoint, "subscription service");
                const auto *subs_meta = TypeRegistry::instance().tss(descriptor.key_type);
                WiringNodeSchema schema;
                schema.output = subs_meta;
                WiringPortRef source = w.add_node(
                    std::type_index(typeid(service::detail::subscription_source_marker)), schema,
                    std::span<const WiringPortRef>{}, service::detail::path_key_value(endpoint),
                    [endpoint, key_meta = descriptor.key_type]() {
                        return make_subscription_key_source_node(endpoint, *key_meta);
                    });
                w.register_service_rank_anchor(endpoint, source.peered_node());
                keyed_service_transport::register_implementation_input(
                    w, subscription_base(descriptor, path), source.peered_node());
                return source;
            }
            case ServiceFlavour::RequestReply: {
                const std::string base = request_reply_base(descriptor, path);
                const std::string endpoint = base + "/request";
                w.register_service_implementation_stub(endpoint, "request/reply service");
                WiringPortRef source = request_input_source_node(w, descriptor, endpoint);
                w.register_service_rank_anchor(endpoint, source.peered_node());
                if (descriptor.response_schema != nullptr)
                {
                    request_reply_transport::register_implementation_input(
                        w, base, source.peered_node());
                }
                return source;
            }
            case ServiceFlavour::ServiceAdaptor:
                return service_adaptor_from_graph(w, descriptor, path);
            case ServiceFlavour::Adaptor:
                return adaptor_from_graph(w, descriptor, path);
            default:
                throw std::invalid_argument("service '" + descriptor.name + "': flavour has no impl input");
        }
    }

    void service_impl_output(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                             const WiringPortRef &out)
    {
        switch (descriptor.flavour)
        {
            case ServiceFlavour::Reference: {
                const std::string base = reference_base(descriptor, path);
                w.register_service_implementation_stub(base, "reference service");
                WiringPortRef shared = shared_output_source_node(
                    w, std::type_index(typeid(service::detail::reference_output_source_marker)),
                    descriptor.output_schema, base);
                const WiringInstance *capture = shared_output_capture_node(
                    w, std::type_index(typeid(service::detail::reference_output_capture_marker)),
                    out.schema != nullptr ? out.schema : descriptor.output_schema, base, out, shared);
                w.register_service_rank_anchor(base, capture);
                return;
            }
            case ServiceFlavour::Subscription: {
                const std::string endpoint = subscription_base(descriptor, path) + "/out";
                w.register_service_implementation_stub(endpoint, "subscription service");
                const auto *dict_meta =
                    TypeRegistry::instance().tsd(descriptor.key_type, descriptor.value_schema);
                WiringPortRef shared = shared_output_source_node(
                    w, std::type_index(typeid(service::detail::shared_output_source_marker)), dict_meta, endpoint);
                keyed_service_transport::defer_subscription_implementation_output(
                    w,
                    subscription_base(descriptor, path),
                    endpoint,
                    *dict_meta,
                    out,
                    std::move(shared),
                    std::type_index(typeid(service::detail::shared_output_capture_marker)));
                return;
            }
            case ServiceFlavour::RequestReply: {
                if (descriptor.response_schema == nullptr)
                {
                    throw std::invalid_argument("reply-less service '" + descriptor.name +
                                                "' has no implementation output");
                }
                const std::string base = request_reply_base(descriptor, path);
                const std::string endpoint = base + "/replies";
                w.register_service_implementation_stub(endpoint, "request/reply service");
                WiringPortRef shared = reply_output_source_node(w, descriptor, endpoint);
                const auto *dict_meta = TypeRegistry::instance().tsd(scalar_descriptor<Int>::value_meta(),
                                                                     descriptor.response_schema);
                request_reply_transport::defer_implementation_output(
                    w,
                    base,
                    endpoint,
                    *dict_meta,
                    out,
                    std::move(shared),
                    std::type_index(typeid(service::detail::request_reply_output_capture_marker)));
                return;
            }
            case ServiceFlavour::ServiceAdaptor:
                service_adaptor_to_graph(w, descriptor, path, out);
                return;
            case ServiceFlavour::Adaptor:
                adaptor_to_graph(w, descriptor, path, out);
                return;
            default:
                throw std::invalid_argument("service '" + descriptor.name + "': flavour has no impl output");
        }
    }

    namespace
    {
    void materialize_multi_service_impl(Wiring &w, std::span<const RuntimeServiceDescriptor *const> descriptors,
                                        std::string_view path, const WiredFn &impl,
                                        std::span<const WiringPortRef> implementation_inputs)
    {
        if (descriptors.empty())
        {
            throw std::invalid_argument("register_multi_service_impl requires at least one interface");
        }
        std::vector<WiringServiceImplementationEndpoint> required_endpoints;
        std::string description{"multi-service implementation:"};
        for (const RuntimeServiceDescriptor *descriptor : descriptors)
        {
            const std::string base = flavour_base(*descriptor, path);
            const char *what = descriptor->flavour == ServiceFlavour::Reference       ? "reference service"
                               : descriptor->flavour == ServiceFlavour::Subscription  ? "subscription service"
                               : descriptor->flavour == ServiceFlavour::RequestReply  ? "request/reply service"
                               : descriptor->flavour == ServiceFlavour::Adaptor         ? "adaptor"
                                                                                       : "service adaptor";
            w.register_built_service_path(base, what);
            description += ' ';
            description += base;
            switch (descriptor->flavour)
            {
                case ServiceFlavour::Reference:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base, {}});
                    break;
                case ServiceFlavour::Subscription:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/subs", {}});
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/out", {}});
                    break;
                case ServiceFlavour::RequestReply:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/request", {}});
                    if (descriptor->response_schema != nullptr)
                    {
                        required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/replies", {}});
                    }
                    break;
                case ServiceFlavour::ServiceAdaptor:
                    required_endpoints.push_back(WiringServiceImplementationEndpoint{base + "/from_graph", {}});
                    if (descriptor->output_schema != nullptr)
                    {
                        required_endpoints.push_back(
                            WiringServiceImplementationEndpoint{base + "/to_graph", {}});
                    }
                    break;
                case ServiceFlavour::Adaptor:
                    append_adaptor_required_endpoints(
                        *descriptor, base, required_endpoints);
                    break;
                default: break;
            }
        }
        auto scope = w.service_implementation_scope(std::move(description), std::move(required_endpoints));
        if (impl.arity != implementation_inputs.size())
        {
            throw std::invalid_argument(
                "manual multi-interface implementation input count does not match supplied inputs");
        }
        static_cast<void>(wire_impl(w, *descriptors.front(), impl, implementation_inputs));
        scope.complete();
    }
    }

    void register_multi_service_impl(Wiring &w, std::span<const RuntimeServiceDescriptor *const> descriptors,
                                     std::string_view path, const WiredFn &impl,
                                     std::span<const WiringPortRef> implementation_inputs,
                                     bool default_fallback)
    {
        if (descriptors.empty())
        {
            throw std::invalid_argument("register_multi_service_impl requires at least one interface");
        }
        std::vector<const RuntimeServiceDescriptor *> stored_descriptors(descriptors.begin(), descriptors.end());
        std::vector<WiringPortRef> stored_inputs(implementation_inputs.begin(), implementation_inputs.end());
        if (default_fallback)
        {
            std::vector<std::pair<std::string, std::string>> selectors;
            selectors.reserve(stored_descriptors.size());
            for (const auto *descriptor : stored_descriptors)
            {
                selectors.emplace_back(
                    std::string{flavour_prefix(descriptor->flavour)}, "/" + descriptor->name);
            }
            w.register_default_service_implementation_candidate(
                std::move(selectors), "default multi-service implementation",
                [stored_descriptors = std::move(stored_descriptors), impl,
                 stored_inputs = std::move(stored_inputs)](Wiring &target, std::string_view requested_path) {
                    std::string user_path;
                    for (const auto *descriptor : stored_descriptors)
                    {
                        const std::string_view prefix = flavour_prefix(descriptor->flavour);
                        const std::string suffix = "/" + descriptor->name;
                        if (requested_path.starts_with(prefix) && requested_path.ends_with(suffix))
                        {
                            user_path = std::string{requested_path.substr(
                                prefix.size(), requested_path.size() - prefix.size() - suffix.size())};
                            break;
                        }
                    }
                    if (user_path.empty())
                    {
                        throw std::logic_error("default multi-service request does not match its interfaces");
                    }
                    materialize_multi_service_impl(
                        target, stored_descriptors, user_path, impl, stored_inputs);
                });
        }
        else
        {
            std::vector<std::string> paths;
            paths.reserve(stored_descriptors.size());
            for (const auto *descriptor : stored_descriptors)
            {
                paths.push_back(flavour_base(*descriptor, path));
            }
            std::string stored_path{path};
            w.register_service_implementation_candidate(
                std::move(paths), "multi-service implementation",
                [stored_descriptors = std::move(stored_descriptors), stored_path = std::move(stored_path),
                 impl, stored_inputs = std::move(stored_inputs)](Wiring &target) {
                    materialize_multi_service_impl(target, stored_descriptors, stored_path, impl, stored_inputs);
                });
        }
    }

    // ------------------------------------------------------------------
    // Adaptors (adaptor_wiring.h erased; same recipe as the services)
    // ------------------------------------------------------------------

    WiringPortRef adaptor_from_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        if (descriptor.input_schema == nullptr)
        {
            throw std::invalid_argument("adaptor '" + descriptor.name + "' has no input schema");
        }
        const std::string endpoint = adaptor_base(descriptor, path) + "/from_graph";
        w.register_service_implementation_stub(endpoint, "adaptor");
        const auto *ref_meta = TypeRegistry::instance().ref(descriptor.input_schema);
        WiringNodeSchema schema;
        schema.output = ref_meta;
        schema.state  = ref_meta->value_schema;
        WiringPortRef source = w.add_node(
            std::type_index(typeid(adaptor::detail::input_stub_source_marker)), schema,
            std::span<const WiringPortRef>{}, adaptor::detail::path_key_value(endpoint),
            [endpoint, input_meta = descriptor.input_schema]() {
                return make_shared_output_source_node(endpoint, *input_meta, false);
            });
        w.register_service_rank_anchor(endpoint, source.peered_node());
        source.schema = descriptor.input_schema;   // the erased Port::as facade
        return source;
    }

    void adaptor_to_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                          const WiringPortRef &out)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        if (descriptor.output_schema == nullptr)
        {
            throw std::invalid_argument("adaptor '" + descriptor.name + "' has no output schema");
        }
        const std::string endpoint = adaptor_base(descriptor, path) + "/to_graph";
        w.register_service_implementation_stub(endpoint, "adaptor");
        WiringPortRef shared = shared_output_source_node(
            w, std::type_index(typeid(adaptor::detail::output_source_marker)), descriptor.output_schema, endpoint);
        const WiringInstance *capture = shared_output_capture_node(
            w, std::type_index(typeid(adaptor::detail::output_capture_marker)),
            out.schema != nullptr ? out.schema : descriptor.output_schema, endpoint, out, shared);
        w.register_service_rank_anchor(endpoint, capture);
    }

    WiringPortRef adaptor_client(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                                 const WiringPortRef *in)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        const std::string base = adaptor_base(descriptor, path);
        w.register_service_client_path(
            base, "adaptor", descriptor.name, descriptor.specialization);
        if (descriptor.input_schema != nullptr)
        {
            if (in == nullptr)
            {
                throw std::invalid_argument("adaptor '" + descriptor.name + "' requires an input");
            }
            const std::string endpoint = base + "/from_graph";
            const auto *ref_meta = TypeRegistry::instance().ref(descriptor.input_schema);
            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            WiringPortRef source = w.add_node(
                std::type_index(typeid(adaptor::detail::input_stub_source_marker)), schema,
                std::span<const WiringPortRef>{}, adaptor::detail::path_key_value(endpoint),
                [endpoint, input_meta = descriptor.input_schema]() {
                    return make_shared_output_source_node(endpoint, *input_meta, false);
                });
            w.register_service_rank_anchor(endpoint, source.peered_node());
            const WiringInstance *capture = shared_output_capture_node(
                w, std::type_index(typeid(adaptor::detail::input_capture_marker)),
                in->schema != nullptr ? in->schema : descriptor.input_schema, endpoint, *in, source);
            w.register_service_client_rank(endpoint, "adaptor", capture, false);
        }
        if (descriptor.output_schema != nullptr)
        {
            const std::string endpoint = base + "/to_graph";
            WiringPortRef shared = shared_output_source_node(
                w, std::type_index(typeid(adaptor::detail::output_source_marker)), descriptor.output_schema,
                endpoint);
            w.register_service_client_rank(endpoint, "adaptor", shared.peered_node(), true);
            shared.schema = descriptor.output_schema;   // the erased Port::as facade
            return shared;
        }
        return WiringPortRef{};
    }

    namespace
    {
    void materialize_adaptor_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                                  const WiredFn &impl, AdaptorImplMode mode,
                                  std::span<const WiringPortRef> implementation_inputs)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        const std::string base = adaptor_base(descriptor, path);
        w.register_built_service_path(base, "adaptor");
        std::vector<WiringServiceImplementationEndpoint> required_endpoints;
        append_adaptor_required_endpoints(descriptor, base, required_endpoints);
        auto scope = w.service_implementation_scope("adaptor " + base, std::move(required_endpoints));
        std::array<WiringPortRef, 1> automatic_inputs{};
        std::span<const WiringPortRef> impl_inputs = implementation_inputs;
        if (mode == AdaptorImplMode::Automatic)
        {
            if (!implementation_inputs.empty())
            {
                throw std::invalid_argument(
                    "automatic adaptor implementation does not accept additional transport inputs");
            }
            const std::size_t expected_arity = descriptor.input_schema != nullptr ? 1 : 0;
            if (impl.arity != expected_arity)
            {
                throw std::invalid_argument(
                    "automatic adaptor implementation input count does not match the interface");
            }
            if (descriptor.input_schema != nullptr)
            {
                automatic_inputs[0] = adaptor_from_graph(w, descriptor, path);
                impl_inputs = std::span<const WiringPortRef>{automatic_inputs.data(), 1};
            }
        }
        else if (impl.arity != implementation_inputs.size())
        {
            throw std::invalid_argument(
                "manual adaptor implementation input count does not match supplied inputs");
        }
        WiringPortRef output = wire_impl(w, descriptor, impl, impl_inputs);
        if (mode == AdaptorImplMode::Automatic && descriptor.output_schema != nullptr)
        {
            if (output.schema == nullptr)
            {
                throw std::invalid_argument(
                    "direct adaptor implementation did not produce its declared output");
            }
            adaptor_to_graph(w, descriptor, path, output);
        }
        scope.complete();
    }
    }

    void register_adaptor_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
                               const WiredFn &impl, AdaptorImplMode mode,
                               std::span<const WiringPortRef> implementation_inputs,
                               bool default_fallback)
    {
        require_flavour(descriptor, ServiceFlavour::Adaptor, "adaptor");
        const std::string base = adaptor_base(descriptor, path);
        const auto *descriptor_ptr = &descriptor;
        std::vector<WiringPortRef> stored_inputs(implementation_inputs.begin(), implementation_inputs.end());
        auto materialize = [descriptor_ptr, impl, mode,
             stored_inputs = std::move(stored_inputs)](Wiring &target, std::string_view requested_path) {
                materialize_adaptor_impl(
                    target, *descriptor_ptr, requested_path, impl, mode, stored_inputs);
            };
        if (default_fallback)
        {
            w.register_default_service_implementation_candidate(
                "adaptor://", "/" + descriptor.name,
                "default adaptor " + descriptor.name, std::move(materialize));
        }
        else
        {
            w.register_service_implementation_candidate(
                {base}, "adaptor " + base,
                [materialize = std::move(materialize), base](Wiring &target) {
                    materialize(target, base);
                });
        }
    }

    void register_unbound_adaptor_impl(
        Wiring &w, const WiredFn &impl,
        std::span<const WiringPortRef> implementation_inputs)
    {
        std::vector<WiringPortRef> stored_inputs(implementation_inputs.begin(), implementation_inputs.end());
        w.register_catch_all_service_implementation_candidate(
            "unbound adaptor implementation",
            [impl, stored_inputs = std::move(stored_inputs)](Wiring &target) {
                if (impl.arity != stored_inputs.size())
                {
                    throw std::invalid_argument(
                        "unbound adaptor implementation input count does not match supplied inputs");
                }
                if (!impl.valid())
                {
                    throw std::invalid_argument("unbound adaptor implementation is not wirable");
                }
                const auto clients = target.service_client_records();
                std::vector<WiringServiceImplementationEndpoint> endpoints;
                for (const auto &client : clients)
                {
                    if (std::ranges::none_of(endpoints, [&](const auto &endpoint) {
                            return endpoint.endpoint == client.endpoint_path;
                        }))
                    {
                        endpoints.push_back(WiringServiceImplementationEndpoint{
                            client.endpoint_path, {}});
                    }
                }
                auto scope = target.service_implementation_scope(
                    "unbound adaptor implementation", std::move(endpoints), false);
                static_cast<void>(impl.wire(target, stored_inputs));
                const auto used_endpoints = target.service_implementation_used_endpoints();
                scope.complete();
                std::unordered_set<std::string> claimed;
                for (const auto &client : clients)
                {
                    const bool used = std::ranges::any_of(
                        used_endpoints, [&](const std::string &endpoint) {
                            return endpoint == client.base_path ||
                                   (endpoint.starts_with(client.base_path) &&
                                    endpoint.size() > client.base_path.size() &&
                                    endpoint[client.base_path.size()] == '/');
                        });
                    if (used && claimed.insert(client.base_path).second)
                    {
                        target.register_built_service_path(
                            client.base_path, client.kind);
                    }
                }
            });
    }

    // ------------------------------------------------------------------
    // Service adaptors (per-client keyed exchange)
    // ------------------------------------------------------------------

    WiringPortRef service_adaptor_from_graph(Wiring &w,
                                             const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path)
    {
        require_flavour(descriptor, ServiceFlavour::ServiceAdaptor, "service adaptor");
        if (descriptor.input_schema == nullptr)
        {
            throw std::invalid_argument("service adaptor '" + descriptor.name + "' has no input schema");
        }
        const std::string endpoint = service_adaptor_base(descriptor, path) + "/from_graph";
        w.register_service_implementation_stub(endpoint, "service adaptor");
        WiringPortRef source = keyed_request_input_source_node(
            w, descriptor.input_schema, endpoint,
            std::type_index(typeid(service_adaptor::detail::request_input_source_marker)));
        w.register_service_rank_anchor(endpoint, source.peered_node());
        return source;
    }

    void service_adaptor_to_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                  std::string_view path, const WiringPortRef &out)
    {
        require_flavour(descriptor, ServiceFlavour::ServiceAdaptor, "service adaptor");
        if (descriptor.output_schema == nullptr)
        {
            throw std::invalid_argument("service adaptor '" + descriptor.name + "' has no output schema");
        }
        const std::string endpoint = service_adaptor_base(descriptor, path) + "/to_graph";
        w.register_service_implementation_stub(endpoint, "service adaptor");
        WiringPortRef shared = keyed_reply_output_source_node(
            w, descriptor.output_schema, endpoint,
            std::type_index(typeid(service_adaptor::detail::output_source_marker)));
        const auto *dict_meta = TypeRegistry::instance().tsd(
            scalar_descriptor<Int>::value_meta(), descriptor.output_schema);
        const WiringInstance *capture = shared_output_capture_node(
            w, std::type_index(typeid(service_adaptor::detail::output_capture_marker)),
            out.schema != nullptr ? out.schema : dict_meta, endpoint, out, shared);
        w.register_service_rank_anchor(endpoint, capture);
    }

    void service_adaptor_client_from_graph(Wiring &w,
                                           const RuntimeServiceDescriptor &descriptor,
                                           std::string_view path,
                                           const WiringPortRef &in,
                                           const WiringPortRef &request_id)
    {
        require_flavour(descriptor, ServiceFlavour::ServiceAdaptor, "service adaptor");
        if (descriptor.input_schema == nullptr)
        {
            throw std::invalid_argument(
                "service adaptor '" + descriptor.name + "' has no input schema");
        }
        const std::string base          = service_adaptor_base(descriptor, path);
        const std::string request_path  = base + "/from_graph";
        w.register_service_client_path(
            base, "service adaptor", descriptor.name, descriptor.specialization);

        WiringPortRef requests = keyed_request_input_source_node(
            w, descriptor.input_schema, request_path,
            std::type_index(typeid(service_adaptor::detail::request_input_source_marker)));
        w.register_service_rank_anchor(request_path, requests.peered_node());

        WiringPortRef adapted_input = graph_wiring_detail::adapt_source_for_input(
            w, descriptor.input_schema, in);
        std::array<WiringPortRef, 3> sources{std::move(adapted_input), requests, request_id};
        std::array<WiringInputRef, 3> inputs{{
            WiringInputRef{.source = sources[0]},
            WiringInputRef{.source = sources[1], .rank_dependency = false},
            WiringInputRef{.source = sources[2]},
        }};
        NodeBuilder builder = make_request_input_capture_node(
            request_path, *descriptor.input_schema, true);
        builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
            builder.type().schema()->input_schema,
            std::span<const WiringPortRef>{sources.data(), sources.size()}));
        WiringPortRef capture = w.add_node(
            std::type_index(typeid(service_adaptor::detail::request_input_capture_marker)),
            std::move(builder), std::span<const WiringInputRef>{inputs.data(), inputs.size()},
            Value{});
        w.register_service_client_rank(request_path, "service adaptor", capture.peered_node(), false);
    }

    WiringPortRef service_adaptor_client_to_graph(Wiring &w,
                                                  const RuntimeServiceDescriptor &descriptor,
                                                  std::string_view path,
                                                  const WiringPortRef &request_id)
    {
        require_flavour(descriptor, ServiceFlavour::ServiceAdaptor, "service adaptor");
        if (descriptor.output_schema == nullptr)
        {
            throw std::invalid_argument(
                "service adaptor '" + descriptor.name + "' has no output schema");
        }
        const std::string base         = service_adaptor_base(descriptor, path);
        const std::string replies_path = base + "/to_graph";
        w.register_service_client_path(
            base, "service adaptor", descriptor.name, descriptor.specialization);
        WiringPortRef replies = keyed_reply_output_source_node(
            w, descriptor.output_schema, replies_path,
            std::type_index(typeid(service_adaptor::detail::output_source_marker)));
        w.register_service_client_rank(replies_path, "service adaptor", replies.peered_node(), true);

        WiringPortRef dict = replies;
        dict.schema = TypeRegistry::instance().tsd(
            scalar_descriptor<Int>::value_meta(), descriptor.output_schema);
        WiringArg dict_arg;
        dict_arg.kind = WiringArg::Kind::TimeSeries;
        dict_arg.port = dict;
        WiringArg id_arg;
        id_arg.kind = WiringArg::Kind::TimeSeries;
        id_arg.port = request_id;
        std::array<WiringArg, 2> item_args{dict_arg, id_arg};
        WiringPortRef output = wire_operator(
            w, "getitem_", std::span<const WiringArg>{item_args.data(), item_args.size()}).output;
        output.schema = descriptor.output_schema;
        return output;
    }

    WiringPortRef service_adaptor_client(Wiring &w,
                                         const RuntimeServiceDescriptor &descriptor,
                                         std::string_view path,
                                         const WiringPortRef &in)
    {
        if (descriptor.input_schema == nullptr)
        {
            throw std::invalid_argument(
                "service adaptor '" + descriptor.name + "' requires an input schema");
        }
        WiringPortRef request_id = request_id_source_node(w);
        service_adaptor_client_from_graph(w, descriptor, path, in, request_id);
        return descriptor.output_schema != nullptr
            ? service_adaptor_client_to_graph(w, descriptor, path, request_id)
            : WiringPortRef{};
    }

    namespace
    {
    void materialize_service_adaptor_impl(Wiring &w,
                                          const RuntimeServiceDescriptor &descriptor,
                                          std::string_view path,
                                          const WiredFn &impl)
    {
        require_flavour(descriptor, ServiceFlavour::ServiceAdaptor, "service adaptor");
        const std::string base = service_adaptor_base(descriptor, path);
        w.register_built_service_path(base, "service adaptor");
        std::vector<WiringServiceImplementationEndpoint> required_endpoints{
            WiringServiceImplementationEndpoint{base + "/from_graph", {}},
        };
        if (descriptor.output_schema != nullptr)
        {
            required_endpoints.push_back(
                WiringServiceImplementationEndpoint{base + "/to_graph", {}});
        }
        auto scope = w.service_implementation_scope(
            "service adaptor " + base, std::move(required_endpoints));
        WiringPortRef requests = service_adaptor_from_graph(w, descriptor, path);
        std::array<WiringPortRef, 1> impl_inputs{requests};
        WiringPortRef replies = wire_impl(w, descriptor, impl, impl_inputs);
        if (descriptor.output_schema != nullptr)
        {
            service_adaptor_to_graph(w, descriptor, path, replies);
        }
        else if (replies.schema != nullptr)
        {
            throw std::invalid_argument(
                "sink-only service adaptor '" + descriptor.name +
                "' implementation returned an output");
        }
        scope.complete();
    }
    }

    void register_service_adaptor_impl(Wiring &w,
                                       const RuntimeServiceDescriptor &descriptor,
                                       std::string_view path,
                                       const WiredFn &impl,
                                       bool default_fallback)
    {
        require_flavour(descriptor, ServiceFlavour::ServiceAdaptor, "service adaptor");
        const std::string base = service_adaptor_base(descriptor, path);
        const auto *descriptor_ptr = &descriptor;
        auto materialize = [descriptor_ptr, impl](Wiring &target, std::string_view requested_path) {
            materialize_service_adaptor_impl(target, *descriptor_ptr, requested_path, impl);
        };
        if (default_fallback)
        {
            w.register_default_service_implementation_candidate(
                "service_adaptor://", "/" + descriptor.name,
                "default service adaptor " + descriptor.name, std::move(materialize));
        }
        else
        {
            w.register_service_implementation_candidate(
                {base}, "service adaptor " + base,
                [materialize = std::move(materialize), base](Wiring &target) {
                    materialize(target, base);
                });
        }
    }
}  // namespace hgraph
