#include <hgraph/runtime/logger.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    struct LoggedNode
    {
        static void eval(hgraph::In<"value", hgraph::TS<hgraph::Int>> value, hgraph::LoggerView logger)
        {
            logger.info("value {}", value.value());
        }
    };
}

TEST_CASE("the logger header still supports existing static node authoring")
{
    hgraph::NodeBuilder builder;
    builder.implementation<LoggedNode>();
    REQUIRE(builder.type().schema() != nullptr);
}
