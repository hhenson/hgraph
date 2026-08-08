#include <hgraph/hgraph.h>
#include <hgraph/types/metadata/type_registry.h>

#include <cstdint>
#include <iostream>

int main() {
    if (hgraph::version() != hgraph::version_string) {
        return 1;
    }

    if (hgraph::version_major != 0 || hgraph::version_minor != 8 || hgraph::version_patch != 0) {
        return 1;
    }

    auto       &registry = hgraph::TypeRegistry::instance();
    const auto *int_meta = registry.value_type("int");
    if (int_meta == nullptr || int_meta != registry.scalar_type<hgraph::Int>().schema()) {
        return 1;
    }
    if (registry.time_series_type("TS[int]") != registry.ts(int_meta)) {
        return 1;
    }

    std::cout << "hgraph " << hgraph::version() << '\n';
    return 0;
}
