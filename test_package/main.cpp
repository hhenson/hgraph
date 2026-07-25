// Conan test-package consumer: exercises the installed SDK end to end —
// registry, value layer, and a temporal value — through hgraph::core.
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/temporal.h>
#include <hgraph/types/value/value.h>

#include <cstdint>
#include <cstdio>
#include <stdexcept>

int main()
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<std::int64_t>("int");

    const Value value{std::int64_t{42}};
    if (value.view().checked_as<std::int64_t>() != 42)
    {
        throw std::logic_error("value round trip failed");
    }

    const Duration day = checked_add(Duration{0}, Duration{86'400'000'000});
    if (format_duration(day) != "86400000000us")
    {
        throw std::logic_error("temporal round trip failed");
    }

    std::puts("hgraph conan consumer ok");
    return 0;
}
