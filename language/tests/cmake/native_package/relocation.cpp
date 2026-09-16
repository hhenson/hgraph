#include <hgl/native_package.h>

#include <iostream>

int main() {
    const hgl::native::Package package{
        .module_identity  = "relocation.native",
        .language_version = "installed-test",
    };
    const auto descriptor = hgl::native::descriptor_json(package);
    if (descriptor.find("relocation.native") == std::string::npos) {
        std::cerr << "relocated native package produced an invalid descriptor\n";
        return 1;
    }
}
