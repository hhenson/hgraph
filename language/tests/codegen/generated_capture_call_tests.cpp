#include <capture-call-selection.h>
#include <hgl/execution_error.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/logger.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <catch2/catch_test_macros.hpp>
using namespace hgraph;
using namespace hgraph::testing;

TEST_CASE("capture evaluates an effectful written argument once", "[capture][call]") {
    const auto provider = checks::capture_calls::register_operators();
    REQUIRE(provider.active());
    auto sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(32);
    struct Restore { ~Restore() { log::set_logger(nullptr); } } restore;
    log::set_logger(std::make_shared<spdlog::logger>("capture-call", sink));
    CHECK_OUTPUT(eval_node<checks::capture_calls::selected_argument>(values<Int>(1, 2)), values<Int>(16, 26));
    std::size_t count = 0;
    for (const auto &entry : sink->last_formatted()) {
        if (entry.find("argument evaluated") != std::string::npos) { ++count; }
    }
    CHECK(count == 2);
}

TEST_CASE("generated handlers retain active lifecycle failure phase", "[capture][lifecycle]") {
    const auto provider = checks::capture_calls::register_operators();
    REQUIRE(provider.active());
    for (const Int value : {Int{1}, Int{2}}) {
        bool escaped = false;
        try { (void)eval_node<checks::capture_calls::capture_lifecycle>(values<Int>(value)); }
        catch (const std::exception &failure) {
            escaped = true;
            CHECK(execution_failure_phase(failure) == (value == 1 ? FailurePhase::Start : FailurePhase::Stop));
            const auto error = canonicalize_failure({}, MIN_ST, "evaluate", failure);
            REQUIRE(error.context);
            CHECK(error.context->phase == (value == 1 ? "start" : "stop"));
        }
        CHECK(escaped);
    }
}
