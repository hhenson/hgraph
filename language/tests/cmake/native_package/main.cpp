#include <hgl/native_package.h>
#include <native.h>
#include <operators.h>
#include <standard.h>

#include <filesystem>
#include <iostream>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "usage: hgl_native_package_consumer <descriptor>\n";
        return 2;
    }

    if (hgraph_::native::native::len(hgraph::Str{"hgl"}) != 3 || hgraph_::native::native::is_empty(hgraph::Str{"hgl"})) {
        std::cerr << "installed hgraph.native functions returned the wrong value\n";
        return 3;
    }
    const auto provider = hgraph_::std_::register_operators();
    if (!provider.active()) {
        std::cerr << "installed hgraph.std operators did not register\n";
        return 4;
    }
    const auto operators = hgraph_::operators_::register_operators();
    if (!operators.active()) {
        std::cerr << "installed hgraph.operators candidates did not register\n";
        return 5;
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
