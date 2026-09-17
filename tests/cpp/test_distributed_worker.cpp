// Starting a worker process, and the bootstrap it depends on (RFC 0037).
//
// The correctness of a distributed run is asserted in test_distributed_map.cpp,
// against plain map_. What is tested here is the part that has no analogue in
// process: that a worker really is another process, that it finds the child it
// was asked for, and that every way of NOT finding it fails where a caller can
// read the failure.
//
// The bootstrap rule is the reason any of this exists. A worker cannot be sent
// its graph, so it rebuilds it from a registration both programs link, and the
// name is all that crosses. Every case below is about that name.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/distributed_map.h>
#include <hgraph/runtime/distributed_process.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/types/metadata/type_registry.h>

#include "distributed_worker_recipes.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <vector>
#include <filesystem>
#include <chrono>
#include <catch2/generators/catch_generators.hpp>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    long long this_pid()
    {
#ifdef _WIN32
        return static_cast<long long>(::GetCurrentProcessId());
#else
        return static_cast<long long>(::getpid());
#endif
    }

    const DateTime worker_end = MIN_ST + TimeDelta{10000};

    std::string test_recipe_key()
    {
        return distributed_map_recipe_key<Int, Int, Int>(fn<hgraph_test::RunningTotalG>());
    }
}  // namespace

TEST_CASE("distributed worker: a spawned worker is another process, and serves")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    WorkerProcess worker =
        spawn_worker(HGRAPH_TEST_WORKER_PROGRAM, test_recipe_key(), MIN_ST, worker_end);
    REQUIRE(worker.running());
    CHECK(worker.pid() != this_pid());

    // A cycle with nothing staged: the child has no work, so the interesting
    // assertion is that the whole round trip -- spawn, recipe lookup, decode,
    // evaluate, encode -- completed in that process and came back clean.
    const BoundarySlots slots = distributed_map_slots<Int, Int, Int>();
    CycleRequest        request;
    request.evaluation_time = MIN_ST;
    worker.channel().send(encode_request(slots, request));

    std::string payload;
    REQUIRE(worker.channel().receive(payload));
    const CycleReply reply = decode_reply(slots, payload);
    CHECK(reply.error.empty());
    CHECK(reply.collected.empty());

    // Closing the channel is how a worker is told to finish; a clean exit code
    // is how we know it did, rather than being killed on the grace timeout.
    CHECK(worker.wait_for_exit() == 0);
    CHECK_FALSE(worker.running());
}

TEST_CASE("distributed worker: a second wait is harmless")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    WorkerProcess worker =
        spawn_worker(HGRAPH_TEST_WORKER_PROGRAM, test_recipe_key(), MIN_ST, worker_end);
    CHECK(worker.wait_for_exit() == 0);
    CHECK(worker.wait_for_exit() == 0);   // and the destructor makes a third
}

TEST_CASE("distributed worker: the recipe name is what the two sides agree on")
{
    hgraph_test::register_distributed_test_recipes();

    // Registering the same recipe again is how a header-driven registration
    // behaves when it runs from more than one translation unit.
    CHECK_NOTHROW(hgraph_test::register_distributed_test_recipes());

    const WorkerRecipe *found = worker_recipe(test_recipe_key());
    REQUIRE(found != nullptr);
    CHECK(found->valid());

    // A different recipe under a live name would let the two programs build
    // different children while believing they agreed.
    CHECK_THROWS_WITH(
        register_worker_recipe(test_recipe_key(),
                               WorkerRecipe{+[]() -> GraphBuilder { return GraphBuilder{}; },
                                            +[]() -> BoundarySlots { return BoundarySlots{}; }}),
        Catch::Matchers::ContainsSubstring("one name must mean one child graph"));
}

TEST_CASE("distributed worker: an unknown recipe names what this program does know")
{
    hgraph_test::register_distributed_test_recipes();

    std::string recipe{"--hgraph-worker-recipe=not-a-recipe"};
    std::string read{"--hgraph-worker-read=3"};
    std::string write{"--hgraph-worker-write=3"};
    char       *argv[]{const_cast<char *>("host"), recipe.data(), read.data(), write.data()};

    CHECK_THROWS_WITH(run_worker_if_requested(4, argv),
                      Catch::Matchers::ContainsSubstring("no recipe named 'not-a-recipe'") &&
                          Catch::Matchers::ContainsSubstring("hgraph.dmap_"));
}

TEST_CASE("distributed worker: a program that was not launched as one carries on")
{
    char *argv[]{const_cast<char *>("host"), const_cast<char *>("--some-other-flag")};
    CHECK_FALSE(run_worker_if_requested(2, argv));
}

TEST_CASE("distributed worker: a worker launched without a channel refuses to serve")
{
    hgraph_test::register_distributed_test_recipes();

    std::string recipe = "--hgraph-worker-recipe=" + test_recipe_key();
    char       *argv[]{const_cast<char *>("host"), recipe.data()};

    CHECK_THROWS_WITH(run_worker_if_requested(2, argv),
                      Catch::Matchers::ContainsSubstring("without a channel"));
}

TEST_CASE("distributed worker: an awkward recipe name survives the launch")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    stdlib::register_standard_operators();
    hgraph_test::register_distributed_test_recipes();

    // Whether a DERIVED key contains a space is a property of the compiler,
    // so this asks the question on every platform rather than on the one that
    // happens to answer it. Found the hard way: MSVC renders a type name as
    // "struct ns::Name<...>", and an unquoted launch handed the worker the
    // word "struct" as its whole recipe name.
    WorkerProcess worker = spawn_worker(HGRAPH_TEST_WORKER_PROGRAM,
                                        hgraph_test::awkward_recipe_name, MIN_ST, worker_end);
    REQUIRE(worker.running());

    const BoundarySlots slots = distributed_map_slots<Int, Int, Int>();
    CycleRequest        request;
    request.evaluation_time = MIN_ST;
    worker.channel().send(encode_request(slots, request));

    std::string payload;
    REQUIRE(worker.channel().receive(payload));
    CHECK(decode_reply(slots, payload).error.empty());
    CHECK(worker.wait_for_exit() == 0);
}

TEST_CASE("distributed worker: bootstrap and evaluation hangs terminate within the deadline")
{
    const auto mode = GENERATE("--test-hang-bootstrap", "--test-hang-reply");
    const std::vector<std::string> arguments{mode};
    DistributedWorker worker{spawn_worker(HGRAPH_TEST_WORKER_PROGRAM, test_recipe_key(),
                                          MIN_ST, worker_end, arguments), std::chrono::milliseconds{100}};
    const auto slots = distributed_map_slots<Int, Int, Int>();
    CycleRequest request;
    request.evaluation_time = MIN_ST;
    const auto started = std::chrono::steady_clock::now();
    worker.dispatch(slots, request);
    CHECK_THROWS_WITH(worker.collect(slots), Catch::Matchers::ContainsSubstring("deadline exceeded"));
    CHECK_NOTHROW(worker.stop());
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds{2});
}

TEST_CASE("distributed worker: Unicode executable paths survive spawning")
{
    namespace fs = std::filesystem;
    const auto original = fs::path{HGRAPH_TEST_WORKER_PROGRAM};
    const auto filename = std::u8string{u8"hgraph-\u6d4b\u8bd5-\u03bb-"} +
                          fs::path{std::to_string(this_pid())}.u8string() + original.extension().u8string();
    const auto executable = original.parent_path() / fs::path{filename};
    fs::copy_file(original, executable, fs::copy_options::overwrite_existing);
    auto cleanup = make_scope_exit([&] { std::error_code ignored; fs::remove(executable, ignored); });
    const auto utf8 = executable.u8string();
    const std::string path{reinterpret_cast<const char *>(utf8.data()), utf8.size()};
    auto process = spawn_worker(path, test_recipe_key(), MIN_ST, worker_end);
    DistributedWorker worker{std::move(process), std::chrono::seconds{10}};
    const auto slots = distributed_map_slots<Int, Int, Int>();
    CycleRequest request;
    request.evaluation_time = MIN_ST;
    worker.dispatch(slots, request);
    CHECK(worker.collect(slots).error.empty());
    CHECK_NOTHROW(worker.stop());
}
