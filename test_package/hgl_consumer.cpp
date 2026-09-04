// Conan test-package HGL consumer (a copy of packaging/smoke/consumer, which
// cannot be reached from an exported recipe): smoke.hgl compiled by
// `hgl emit-cpp` through hgl_add_module() and evaluated with the public
// harness, both as the generated graph struct and by registry name.
#include <smoke.h>

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/eval_node.h>

#include <cstdio>
#include <optional>
#include <stdexcept>
#include <vector>

int main()
{
    using hgraph::Float;
    using ticks = std::vector<std::optional<Float>>;

    hgraph::stdlib::register_standard_operators();
    smoke::register_operators();

    if (hgraph::testing::eval_node<smoke::twice>(ticks{1.0, 2.0}) != ticks{2.0, 4.0})
    {
        throw std::logic_error("smoke.twice did not double its input");
    }
    // Dispatch through the registry marker: the erased harness yields Values.
    const auto by_name = hgraph::testing::eval_node<smoke::ops::twice>(ticks{3.0});
    if (by_name.size() != 1 || !by_name[0] || by_name[0]->as<Float>() != 6.0)
    {
        throw std::logic_error("smoke.twice is not registered by name");
    }

    std::puts("hgl smoke consumer ok");
    return 0;
}
