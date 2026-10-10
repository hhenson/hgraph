#include <exception>
#include <hgl/native_package.h>
#include <iostream>
#include <string>

int main(int argc, char **argv) {
    if (argc != 3) { return 2; }
    try {
        using namespace hgl::native;
        const std::string module = argv[2];
        Package           package{.module_identity = module, .language_version = "0.1-test", .provider_identity = module};
        for (const auto &name : {"Token", "TokenOther", "TextOnly"}) {
            const bool comparable = std::string_view{name} != "TextOnly";
            package.types.push_back(Type{
                .category           = TypeCategory::AtomicValue,
                .identity           = module + "::" + name,
                .cpp_type           = std::string{"checks::native_atomic_values::"} + name,
                .public_header      = "checks/native_atomic_values.h",
                .canonical_identity = std::string{"hgraph.std::"} + name,
                .value_contract     = AtomicValueContract{.owning_copy   = true,
                                                          .text          = true,
                                                          .equality      = comparable,
                                                          .hash          = comparable,
                                                          .order         = comparable,
                                                          .serialization = true},
                .exported           = true,
            });
            const std::string maker  = comparable ? std::string_view{name} == "Token" ? "token" : "token_other" : "text_only";
            const auto        native = ValueType::native(module + "::" + name);
            const auto        str    = ValueType::canonical(ScalarType::Str);
            for (const bool observer : {false, true}) {
                const auto helper = maker + (observer ? "_text" : "");
                package.declarations.push_back(Declaration{
                    .identity         = module + "::" + helper,
                    .cpp_symbol       = "checks::native_atomic_values::" + helper,
                    .parameters       = {{.name = observer ? "value" : "text", .type = observer ? native : str}},
                    .result_type      = observer ? str : native,
                    .phases           = {Phase::Wiring, Phase::Start, Phase::Evaluation, Phase::Stop},
                    .execution_role   = ExecutionRole::Value,
                    .exception_policy = ExceptionPolicy::Translated,
                });
            }
        }
        package.build.public_headers = {"checks/native_atomic_values.h"};
        write_descriptor(package, argv[1]);
        return 0;
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
