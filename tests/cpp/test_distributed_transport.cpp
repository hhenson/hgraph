// A connected pair carrying framed messages (RFC 0037).
//
// The transport is the last piece, and the least interesting on purpose: the
// framing and the messages already exist, so this only has to move bytes
// without losing or splicing them. What it must get right is the things a
// byte stream does that a function call does not -- a read that returns half a
// message, a read that returns two, and a far end that goes away.
//
// The final case runs a real worker over it, on another thread, and asserts
// the same equality every layer below has asserted: what comes back over the
// pipe is what the child produces driven directly.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/distributed_transport.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <thread>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    struct RunningTotalNode
    {
        static constexpr auto name = "transport_running_total";

        static void eval(In<"ts", TS<Int>> ts, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += ts.value();
            out.set(total.get());
        }
    };

    struct ChildGraph
    {
        static constexpr auto name = "transport_child_graph";
        static void           compose(Wiring &w)
        {
            auto in    = wire<boundary_source_impl, TS<Int>>(w, Str{"in"});
            auto total = wire<RunningTotalNode>(w, in);
            wire<boundary_sink_impl>(w, total, Str{"out"});
        }
    };

    BoundarySlots int_slots()
    {
        BoundarySlots slots;
        slots.add("in", scalar_descriptor<Int>::value_meta(), SlotDirection::Input);
        slots.add("out", scalar_descriptor<Int>::value_meta(), SlotDirection::Output);
        return slots;
    }

    const DateTime test_end = MIN_ST + TimeDelta{10000};
}  // namespace

TEST_CASE("distributed transport: a message survives the round trip")
{
    PipeEndpoint a;
    PipeEndpoint b;
    connected_pipe_pair(a, b);
    REQUIRE(a.open());
    REQUIRE(b.open());

    a.send("hello");
    std::string got;
    REQUIRE(b.receive(got));
    CHECK(got == "hello");

    b.send("and back");
    REQUIRE(a.receive(got));
    CHECK(got == "and back");
}

TEST_CASE("distributed transport: message boundaries survive being batched")
{
    // Several sends before any receive: the reader will get them in one read,
    // and must still hand them back one at a time and in order. This is the
    // case a length prefix exists for.
    PipeEndpoint a;
    PipeEndpoint b;
    connected_pipe_pair(a, b);

    a.send("one");
    a.send("two");
    a.send("three");

    std::string got;
    REQUIRE(b.receive(got));
    CHECK(got == "one");
    REQUIRE(b.receive(got));
    CHECK(got == "two");
    REQUIRE(b.receive(got));
    CHECK(got == "three");
}

TEST_CASE("distributed transport: a message larger than one read survives")
{
    // Bigger than the internal chunk, so receive() must loop rather than
    // assume one read is one message.
    PipeEndpoint a;
    PipeEndpoint b;
    connected_pipe_pair(a, b);

    const std::string big(200000, 'x');
    // Written from another thread: a pipe's buffer is far smaller than this,
    // so a single-threaded send would block against its own unread reader.
    std::thread writer{[&] { a.send(big); }};

    std::string got;
    REQUIRE(b.receive(got));
    writer.join();
    CHECK(got.size() == big.size());
    CHECK(got == big);
}

TEST_CASE("distributed transport: a clean close ends the stream")
{
    PipeEndpoint a;
    PipeEndpoint b;
    connected_pipe_pair(a, b);
    a.close();   // the far side exits between messages: an orderly end
    std::string got;
    CHECK_FALSE(b.receive(got));

    // NOT covered here: the far side dying PART-WAY through a message, which
    // receive() reports by throwing rather than by ending the stream. It is
    // unreachable through this API -- send() frames whole messages, so no peer
    // using it can leave a partial one behind -- and reaching it would need a
    // raw-write entry point that exists only for the test. The two halves are
    // covered separately: read_frame refuses every short prefix
    // (test_distributed_protocol.cpp) and the branch here is the buffer being
    // non-empty when the stream ends.
}

TEST_CASE("distributed transport: a worker served over the pipe equals one driven directly")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const auto             slots = int_slots();
    const std::vector<Int> inputs{1, 2, 3, 4};

    // Driven directly, as the reference.
    std::vector<Int>     direct;
    DistributedChildHost local{build_graph<ChildGraph>(), test_end};
    local.start(MIN_ST);
    DateTime when = MIN_ST;
    for (const Int value : inputs)
    {
        local.stage("in", Value{value}.view());
        REQUIRE(local.step(when));
        direct.push_back(local.collect("out").view().checked_as<Int>());
        when = when + MIN_TD;
    }
    local.stop();

    // And over the pipe: a worker thread owning its own child, serving whatever
    // arrives until the caller closes. This is the worker process, minus the
    // process.
    PipeEndpoint caller;
    PipeEndpoint worker;
    connected_pipe_pair(caller, worker);

    std::thread worker_thread{[&] {
        DistributedChildHost host{build_graph<ChildGraph>(), test_end};
        host.start(MIN_ST);
        std::string payload;
        while (worker.receive(payload))
        {
            const CycleRequest request = decode_request(slots, payload);
            worker.send(encode_reply(slots, serve_cycle(host, slots, request)));
        }
        host.stop();
    }};

    std::vector<Int> over_pipe;
    when = MIN_ST;
    for (const Int value : inputs)
    {
        CycleRequest request;
        request.evaluation_time = when;
        request.staged.push_back(SlotDelta{slots.index_of("in"), Value{value}});
        caller.send(encode_request(slots, request));

        std::string payload;
        REQUIRE(caller.receive(payload));
        const CycleReply reply = decode_reply(slots, payload);
        REQUIRE(reply.error.empty());
        REQUIRE(reply.collected.size() == 1);
        over_pipe.push_back(reply.collected[0].delta.view().checked_as<Int>());
        when = when + MIN_TD;
    }
    caller.close();   // ends the worker's loop
    worker_thread.join();

    CHECK(over_pipe == direct);
    CHECK(over_pipe == std::vector<Int>{1, 3, 6, 10});
}
