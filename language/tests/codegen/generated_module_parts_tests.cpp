#include <part-api.h>

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("generated module parts share private declarations", "[language][generated][module-parts]")
{
    CHECK_OUTPUT(eval_node<checks::parts::forwarded>(values<Int>(1, 2, 3)), values<Int>(1, 2, 3));
}
