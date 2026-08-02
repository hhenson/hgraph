#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/adaptor_wiring.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/service_runtime.h>
#include <hgraph/types/service_wiring.h>

#include <catch2/catch_test_macros.hpp>

// Runtime service identity (services.rst, rulings 2026-07-05): the erased
// flavour flows over RuntimeServiceDescriptor share the role markers and
// name-qualified path grammar with the C++ templates, so an erased
// registration UNIFIES with a template client on the same path - the
// python-impl-for-C++-stub direction, proven structurally in C++.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct RuntimeBaseValue
    {
        static constexpr std::string_view name{"runtime_base_value"};
        using output_schema = TS<Int>;
    };

    struct RuntimeConstGraph
    {
        [[maybe_unused]] static constexpr auto name = "runtime_const_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<stdlib::const_>(w, Int{41}).as<TS<Int>>();
        }
    };

    struct RuntimeNumericValue
    {
        static constexpr std::string_view name{"runtime_numeric_value"};
        using output_schema = TS<ScalarVar<"NUMBER", Int, Float>>;
    };

    struct ErasedRegisterTemplateClient
    {
        [[maybe_unused]] static constexpr auto name = "erased_register_template_client";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto &registry = TypeRegistry::instance();
            RuntimeServiceDescriptor descriptor;
            descriptor.name          = std::string{RuntimeBaseValue::name};
            descriptor.flavour       = ServiceFlavour::Reference;
            descriptor.output_schema = registry.ts(scalar_descriptor<Int>::value_meta());
            const auto *interned     = &intern_service_descriptor(std::move(descriptor));

            // Register through the ERASED flow (a WiredFn impl - the python
            // shape) and consume through the TEMPLATE client on the same
            // path: identity must unify.
            register_reference_service_impl(w, *interned, "prices", fn<RuntimeConstGraph>());
            auto value = service::reference_service<RuntimeBaseValue>(w, service::path("prices"));
            return wire<stdlib::add_>(w, value, wire<stdlib::const_>(w, Int{1}).as<TS<Int>>()).as<TS<Int>>();
        }
    };

    struct ErasedSpecializationRegisterTemplateClient
    {
        [[maybe_unused]] static constexpr auto name = "erased_specialization_register_template_client";

        static Port<TS<Int>> compose(Wiring &w)
        {
            RuntimeServiceDescriptor descriptor;
            descriptor.name           = std::string{RuntimeNumericValue::name};
            descriptor.specialization = "NUMBER=int";
            descriptor.flavour        = ServiceFlavour::Reference;
            descriptor.output_schema  = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
            const auto *interned       = &intern_service_descriptor(std::move(descriptor));

            register_reference_service_impl(
                w, *interned, "numeric[NUMBER=int]", fn<RuntimeConstGraph>());
            return service::reference_service<RuntimeNumericValue>(
                       w, service::path("numeric", arg<"NUMBER">(scalar_type<Int>())))
                .as<TS<Int>>();
        }
    };

    struct RuntimeEchoServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"runtime_echo_service_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct RuntimeEchoServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "runtime_echo_service_adaptor_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &, Port<TSD<Int, TS<Int>>> requests)
        {
            return requests;
        }
    };

    using RuntimeMultiFields =
        TSB<"RuntimeMultiFields", Field<"lhs", TS<Int>>, Field<"rhs", TS<Int>>>;

    struct RuntimeMultiFieldServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"runtime_multi_field_service_adaptor"};
        using input_schema = RuntimeMultiFields;
        using output_schema = RuntimeMultiFields;
    };

    struct RuntimeMultiFieldServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "runtime_multi_field_service_adaptor_impl";

        static Port<TSD<Int, RuntimeMultiFields>> compose(
            Wiring &, Port<TSD<Int, RuntimeMultiFields>> requests)
        {
            return requests;
        }
    };

    struct RuntimeMultiFieldServiceAdaptorGraph
    {
        [[maybe_unused]] static constexpr auto name = "runtime_multi_field_service_adaptor_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            service_adaptor::register_service_adaptor_impl<
                RuntimeMultiFieldServiceAdaptor, RuntimeMultiFieldServiceAdaptorImpl>(
                w, service_adaptor::path("multi"));
            auto reply = wire<RuntimeMultiFieldServiceAdaptor>(
                w, service_adaptor::path("multi"), lhs, rhs);
            auto reply_lhs = wire<stdlib::getattr_>(w, reply, Str{"lhs"}).as<TS<Int>>();
            auto reply_rhs = wire<stdlib::getattr_>(w, reply, Str{"rhs"}).as<TS<Int>>();
            return wire<stdlib::add_>(w, reply_lhs, reply_rhs).as<TS<Int>>();
        }
    };

    struct ErasedServiceAdaptorRegisterTemplateClients
    {
        [[maybe_unused]] static constexpr auto name = "erased_service_adaptor_register_template_clients";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            auto &registry = TypeRegistry::instance();
            RuntimeServiceDescriptor descriptor;
            descriptor.name          = std::string{RuntimeEchoServiceAdaptor::name};
            descriptor.flavour       = ServiceFlavour::ServiceAdaptor;
            descriptor.input_schema  = registry.ts(scalar_descriptor<Int>::value_meta());
            descriptor.output_schema = descriptor.input_schema;
            const auto *interned = &intern_service_descriptor(std::move(descriptor));

            register_service_adaptor_impl(
                w, *interned, "echo", fn<RuntimeEchoServiceAdaptorImpl>());
            const auto custom = service_adaptor::path("echo");
            auto lhs_reply = wire<RuntimeEchoServiceAdaptor>(w, custom, lhs);
            auto rhs_reply = wire<RuntimeEchoServiceAdaptor>(w, custom, rhs);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct RuntimeAutomaticAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"runtime_automatic_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct RuntimeAutomaticAdaptorImpl
    {
        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> input)
        {
            return input;
        }
    };

    struct ErasedAutomaticAdaptorRegisterTemplateClient
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            auto &registry = TypeRegistry::instance();
            RuntimeServiceDescriptor descriptor;
            descriptor.name          = std::string{RuntimeAutomaticAdaptor::name};
            descriptor.flavour       = ServiceFlavour::Adaptor;
            descriptor.input_schema  = registry.ts(scalar_descriptor<Int>::value_meta());
            descriptor.output_schema = descriptor.input_schema;
            const auto *interned = &intern_service_descriptor(std::move(descriptor));

            register_adaptor_impl(
                w, *interned, "echo", fn<RuntimeAutomaticAdaptorImpl>(),
                AdaptorImplMode::Automatic);
            return wire<RuntimeAutomaticAdaptor>(w, adaptor::path("echo"), input);
        }
    };

    struct RuntimeReplylessImpl
    {
        [[maybe_unused]] static constexpr auto name = "runtime_replyless_impl";

        static void compose(Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            static_cast<void>(wire<stdlib::null_sink>(w, requests));
        }
    };

    struct ErasedReplylessServiceGraph
    {
        [[maybe_unused]] static constexpr auto name = "erased_replyless_service_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            auto &registry = TypeRegistry::instance();
            RuntimeServiceDescriptor descriptor;
            descriptor.name = "runtime_replyless_service";
            descriptor.flavour = ServiceFlavour::RequestReply;
            descriptor.request_schema = registry.ts(scalar_descriptor<Int>::value_meta());
            const auto *interned = &intern_service_descriptor(std::move(descriptor));

            register_request_reply_service_impl(
                w, *interned, "events", fn<RuntimeReplylessImpl>());
            const WiringPortRef reply = request_reply_service_call(
                w, *interned, "events", request.erased());
            if (reply.schema != nullptr)
            {
                throw std::logic_error("a reply-less service returned a client port");
            }
            return request;
        }
    };
}  // namespace

TEST_CASE("service runtime: an erased registration serves a template client")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedRegisterTemplateClient>(), values<Int>(42));
}

TEST_CASE("service runtime: an erased numeric specialization serves a generic template client")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedSpecializationRegisterTemplateClient>(), values<Int>(41));
}

TEST_CASE("service runtime: descriptor interning includes erased schemas")
{
    RuntimeServiceDescriptor first;
    first.name          = "intern_check";
    first.flavour       = ServiceFlavour::Reference;
    first.output_schema = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
    const auto *interned = &intern_service_descriptor(first);

    // Same name + schemas: the SAME record (python re-import tolerance).
    CHECK(&intern_service_descriptor(first) == interned);

    RuntimeServiceDescriptor different = first;
    different.output_schema = TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta());
    const auto *different_record = &intern_service_descriptor(different);

    CHECK(different_record != interned);
    CHECK(&intern_service_descriptor(different) == different_record);
    CHECK(find_service_descriptor("intern_check") == nullptr);
}

TEST_CASE("service runtime: generic descriptor specializations have independent concrete schemas")
{
    RuntimeServiceDescriptor integer;
    integer.name           = "specialized_service";
    integer.specialization = "NUMBER=int";
    integer.flavour        = ServiceFlavour::Reference;
    integer.output_schema  = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
    const auto *integer_record = &intern_service_descriptor(integer);

    RuntimeServiceDescriptor floating = integer;
    floating.specialization = "NUMBER=float";
    floating.output_schema  = TypeRegistry::instance().ts(scalar_descriptor<Float>::value_meta());
    const auto *float_record = &intern_service_descriptor(floating);

    CHECK(integer_record != float_record);
    CHECK(&intern_service_descriptor(integer) == integer_record);
    CHECK(&intern_service_descriptor(floating) == float_record);
    CHECK(find_service_descriptor("specialized_service") == nullptr);
}

TEST_CASE("service runtime: erased service-adaptor registration serves typed clients")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedServiceAdaptorRegisterTemplateClients>(
                     values<Int>(1, none, 2), values<Int>(10, none, 20)),
                 values<Int>(11, none, 22));
}

TEST_CASE("service runtime: C++ service adaptors carry multi-field requests and responses")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<RuntimeMultiFieldServiceAdaptorGraph>(
                     values<Int>(1, none, 2), values<Int>(10, none, 20)),
                 values<Int>(11, none, 22));
}

TEST_CASE("service runtime: erased automatic adaptor registration serves a typed client")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedAutomaticAdaptorRegisterTemplateClient>(
                     values<Int>(1, none, 2)),
                 values<Int>(1, none, 2));
}

TEST_CASE("service runtime: erased reply-less requests have no client output")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<ErasedReplylessServiceGraph>(values<Int>(1, none, 2)),
                 values<Int>(1, none, 2));
}
