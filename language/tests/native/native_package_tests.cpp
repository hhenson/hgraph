#include <hgl/native_package.h>

#include "descriptor/module_descriptor_reader.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <stdexcept>
#include <string>

namespace
{
    hgl::native::Package package() {
        using namespace hgl::native;

        const ValueType f64   = ValueType::canonical(ScalarType::F64);
        const ValueType state = ValueType::native("acme.stats::state");
        return Package{
            .module_identity   = "acme.stats",
            .language_version  = "0.1-test",
            .provider_identity = "acme.stats.native",
            .types =
                {
                    Type{
                        .category      = TypeCategory::OpaqueState,
                        .identity      = "acme.stats::state",
                        .cpp_type      = "acme::stats::State",
                        .public_header = "acme/stats.h",
                    },
                },
            .declarations =
                {
                    Declaration{
                        .category         = DeclarationCategory::Constructor,
                        .identity         = "acme.stats::make_state",
                        .cpp_symbol       = "acme::stats::make_state",
                        .result_type      = state,
                        .result_policy    = ValuePolicy{.ownership = Ownership::Owned},
                        .phases           = {Phase::Start},
                        .effects          = {Effect::Allocation},
                        .exception_policy = ExceptionPolicy::Translated,
                    },
                    Declaration{
                        .identity   = "acme.stats::update",
                        .cpp_symbol = "acme::stats::update",
                        .parameters =
                            {
                                Parameter{
                                    .name = "state",
                                    .type = state,
                                    .policy =
                                        ValuePolicy{
                                            .ownership     = Ownership::Borrowed,
                                            .mutable_value = true,
                                        },
                                },
                                Parameter{.name = "value", .type = f64},
                            },
                        .result_type = f64,
                        .phases      = {Phase::Evaluation},
                        .effects     = {Effect::Mutation},
                    },
                    Declaration{
                        .category   = DeclarationCategory::Lifecycle,
                        .identity   = "acme.stats::close",
                        .cpp_symbol = "acme::stats::close",
                        .parameters =
                            {
                                Parameter{
                                    .name = "state",
                                    .type = state,
                                    .policy =
                                        ValuePolicy{
                                            .ownership     = Ownership::Borrowed,
                                            .mutable_value = true,
                                        },
                                },
                            },
                        .phases  = {Phase::Stop},
                        .effects = {Effect::Mutation},
                    },
                },
            .build =
                Build{
                    .cmake_packages   = {"acme_stats"},
                    .imported_targets = {"acme::stats"},
                },
        };
    }
}  // namespace

TEST_CASE("native package API emits a sealed descriptor") {
    const std::string json   = hgl::native::descriptor_json(package());
    const auto        parsed = hgl::descriptor::read_json(json);

    REQUIRE(parsed);
    CHECK(parsed.value->module_identity == "acme.stats");
    CHECK(parsed.value->provider_identity == "acme.stats.native");
    CHECK(parsed.value->native_types.size() == 1U);
    CHECK(parsed.value->native_declarations.size() == 3U);
    CHECK(parsed.value->build.public_headers == std::vector<std::string>{"acme/stats.h"});
    CHECK(parsed.value->descriptor_fingerprint.starts_with("sha256:"));
}

TEST_CASE("native package descriptor order is canonical") {
    hgl::native::Package reordered = package();
    std::ranges::reverse(reordered.declarations);
    reordered.build.public_headers = {"acme/stats.h", "acme/stats.h"};
    reordered.build.cmake_packages = {"z_package", "acme_stats"};

    hgl::native::Package canonical = package();
    canonical.build.cmake_packages = {"acme_stats", "z_package"};
    CHECK(hgl::native::descriptor_json(reordered) == hgl::native::descriptor_json(canonical));
}

TEST_CASE("native package API describes overloaded collection-view functions") {
    using namespace hgl::native;

    const ValueType i64 = ValueType::canonical(ScalarType::I64);
    const ValueType t   = ValueType::type_parameter("T");
    Package         source{
        .module_identity  = "acme.collections",
        .language_version = "0.1-test",
        .declarations =
            {
                Declaration{
                    .identity   = "acme.collections::len",
                    .cpp_symbol = "acme::collections::len",
                    .generics =
                        {
                            GenericParameter{.name = "T"},
                            GenericParameter{.name = "N", .is_const = true, .type = i64},
                        },
                    .parameters =
                        {
                            Parameter{.name = "value", .type = ValueType::list(t, "N"), .access = ParameterAccess::InputView},
                        },
                    .result_type = i64,
                    .phases      = {Phase::Evaluation},
                },
                Declaration{
                    .identity   = "acme.collections::len",
                    .cpp_symbol = "acme::collections::len",
                    .generics   = {GenericParameter{.name = "T"}},
                    .parameters =
                        {
                            Parameter{.name = "value", .type = ValueType::set(t), .access = ParameterAccess::InputView},
                        },
                    .result_type = i64,
                    .phases      = {Phase::Evaluation},
                },
            },
    };

    const std::string canonical = descriptor_json(source);
    std::ranges::reverse(source.declarations);
    CHECK(descriptor_json(source) == canonical);

    const auto parsed = hgl::descriptor::read_json(canonical);
    REQUIRE(parsed);
    REQUIRE(parsed.value->native_declarations.size() == 2U);
    CHECK(parsed.value->native_declarations[0].identity == "acme.collections::len");
    CHECK(parsed.value->native_declarations[1].identity == "acme.collections::len");
    CHECK(parsed.value->native_declarations[0].parameters.front().access == hgl::descriptor::NativeParameterAccess::InputView);
    CHECK(parsed.value->native_declarations[1].parameters.front().access == hgl::descriptor::NativeParameterAccess::InputView);
}

TEST_CASE("native package API describes payload-erased input-view functions") {
    using namespace hgl::native;

    Package source{
        .module_identity  = "acme.signals",
        .language_version = "0.1-test",
        .declarations =
            {
                Declaration{
                    .identity   = "acme.signals::valid",
                    .cpp_symbol = "acme::signals::valid",
                    .parameters =
                        {
                            Parameter{.name = "value", .type = ValueType::signal(), .access = ParameterAccess::InputView},
                        },
                    .result_type = ValueType::canonical(ScalarType::Bool),
                    .phases      = {Phase::Evaluation},
                },
            },
    };

    const auto parsed = hgl::descriptor::read_json(descriptor_json(source));
    REQUIRE(parsed);
    REQUIRE(parsed.value->native_declarations.size() == 1U);
    const auto &native = parsed.value->native_declarations.front();
    CHECK(parsed.value->types[native.signature.parameters.front().type].category == hgl::descriptor::TypeCategory::Signal);
    CHECK(native.parameters.front().access == hgl::descriptor::NativeParameterAccess::InputView);
}

TEST_CASE("native package API applies the descriptor safety envelope") {
    hgl::native::Package invalid = package();
    invalid.declarations[1].effects.push_back(hgl::native::Effect::Blocking);

    CHECK_THROWS_WITH(hgl::native::descriptor_json(invalid),
                      "$.native.declarations[2].effects: evaluation native functions must be non-blocking");
}

TEST_CASE("native package API rejects undeclared nominal signature types") {
    hgl::native::Package invalid               = package();
    invalid.declarations[1].parameters[1].type = hgl::native::ValueType::native("acme.stats::missing");

    CHECK_THROWS_WITH(hgl::native::descriptor_json(invalid), "native signature names undeclared type 'acme.stats::missing'");
}

TEST_CASE("native package API rejects C++ source in place of an exact symbol") {
    hgl::native::Package invalid            = package();
    invalid.declarations.front().identity   = "acme.stats::a_make_state";
    invalid.declarations.front().cpp_symbol = "acme::stats::make_state(); injected";

    CHECK_THROWS_WITH(hgl::native::descriptor_json(invalid),
                      "$.native.declarations[0].cpp_symbol: C++ symbol must be one exact qualified identifier");
}

TEST_CASE("native package API rejects a constructor without a result type") {
    hgl::native::Package invalid = package();
    invalid.declarations.front().result_type.reset();

    CHECK_THROWS_WITH(hgl::native::descriptor_json(invalid),
                      "$.native.declarations[1].signature.result: a native constructor must declare its result type");
}
