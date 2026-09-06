#include <hgl/native_package.h>

#include <filesystem>
#include <iostream>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "usage: hgl_native_package_consumer <descriptor>\n";
        return 2;
    }

    using namespace hgl::native;
    Package package{
        .module_identity  = "consumer.native",
        .language_version = "installed-test",
        .declarations =
            {
                Declaration{
                    .identity    = "consumer.native::scale",
                    .cpp_symbol  = "consumer::scale",
                    .parameters  = {Parameter{.name = "value", .type = ValueType::canonical(ScalarType::F64)}},
                    .result_type = ValueType::canonical(ScalarType::F64),
                    .phases      = {Phase::Evaluation},
                },
            },
        .build = Build{.public_headers = {"consumer/native.h"}},
    };
    write_descriptor(package, std::filesystem::path{argv[1]});
}
