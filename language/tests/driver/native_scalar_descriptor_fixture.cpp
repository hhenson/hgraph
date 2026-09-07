#include <hgl/native_package.h>

#include <exception>
#include <iostream>
#include <string>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "usage: native_scalar_descriptor_fixture <output>\n";
        return 2;
    }
    try {
        using namespace hgl::native;
        const ValueType i64 = ValueType::canonical(ScalarType::I64);
        const ValueType t   = ValueType::type_parameter("T");
        const ValueType k   = ValueType::type_parameter("K");
        const ValueType v   = ValueType::type_parameter("V");
        Package         package{
            .module_identity   = "checks.native_dependency",
            .language_version  = "0.1-test",
            .provider_identity = "checks.native_dependency",
            .declarations =
                {
                    Declaration{
                        .identity   = "checks.native_dependency::blend",
                        .cpp_symbol = "checks::native_dependency::blend",
                        .parameters =
                            {
                                Parameter{.name = "value", .type = ValueType::canonical(ScalarType::F64)},
                                Parameter{.name = "window", .type = ValueType::canonical(ScalarType::I64), .is_const = true},
                            },
                        .result_type = ValueType::canonical(ScalarType::F64),
                        .phases      = {Phase::Evaluation},
                    },
                    Declaration{
                        .identity   = "checks.native_dependency::len",
                        .cpp_symbol = "checks::native_dependency::len",
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
                        .identity   = "checks.native_dependency::len",
                        .cpp_symbol = "checks::native_dependency::len",
                        .generics   = {GenericParameter{.name = "T"}},
                        .parameters =
                            {
                                Parameter{.name = "value", .type = ValueType::set(t), .access = ParameterAccess::InputView},
                            },
                        .result_type = i64,
                        .phases      = {Phase::Evaluation},
                    },
                    Declaration{
                        .identity   = "checks.native_dependency::len",
                        .cpp_symbol = "checks::native_dependency::len",
                        .generics   = {GenericParameter{.name = "K"}, GenericParameter{.name = "V"}},
                        .parameters =
                            {
                                Parameter{.name = "value", .type = ValueType::map(k, v), .access = ParameterAccess::InputView},
                            },
                        .result_type = i64,
                        .phases      = {Phase::Evaluation},
                    },
                },
            .build =
                Build{
                    .public_headers   = {"checks/native_dependency.h"},
                    .cmake_packages   = {"checks_native_dependency"},
                    .imported_targets = {"checks::native_dependency"},
                },
        };
        write_descriptor(package, argv[1]);
        return 0;
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
