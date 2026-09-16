// The messages between a dmap_ caller and one worker (RFC 0037, stage 3).
//
// The assertion that matters is the last one: a child driven THROUGH the
// encoded protocol must produce exactly what the same child produces driven
// directly. Everything before it checks the failure modes a wire format must
// not have -- a truncated frame accepted, a slot silently dropped, a payload
// decoded against the wrong schema.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    const ValueTypeMetaData *int_schema() { return scalar_descriptor<Int>::value_meta(); }
    const ValueTypeMetaData *str_schema() { return scalar_descriptor<Str>::value_meta(); }

    BoundarySlots two_slots()
    {
        BoundarySlots slots;
        slots.add("in", int_schema());
        slots.add("out", int_schema());
        return slots;
    }

    struct RunningTotal
    {
        static constexpr auto name = "protocol_running_total";

        static void eval(In<"ts", TS<Int>> ts, State<Int> total, Out<TS<Int>> out)
        {
            total.modify() += ts.value();
            out.set(total.get());
        }
    };

    struct BoundaryChildGraph
    {
        static constexpr auto name = "protocol_child_graph";
        static void           compose(Wiring &w)
        {
            auto in    = wire<boundary_source_impl, TS<Int>>(w, Str{"in"});
            auto total = wire<RunningTotal>(w, in);
            wire<boundary_sink_impl>(w, total, Str{"out"});
        }
    };

    const DateTime test_end = MIN_ST + TimeDelta{1000};
}  // namespace

TEST_CASE("distributed protocol: a request round trips")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    CycleRequest sent;
    sent.evaluation_time = MIN_ST + TimeDelta{1234};
    sent.staged.push_back(SlotDelta{0, Value{Int{42}}});

    const auto got = decode_request(slots, encode_request(slots, sent));
    CHECK(got.evaluation_time == sent.evaluation_time);
    REQUIRE(got.staged.size() == 1);
    CHECK(got.staged[0].slot == 0);
    CHECK(got.staged[0].delta.view() == sent.staged[0].delta.view());
}

TEST_CASE("distributed protocol: a reply round trips, including what it wants next")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    CycleReply sent;
    sent.next_scheduled_time = MIN_ST + TimeDelta{99};
    sent.collected.push_back(SlotDelta{1, Value{Int{7}}});

    const auto got = decode_reply(slots, encode_reply(slots, sent));
    CHECK(got.next_scheduled_time == sent.next_scheduled_time);
    REQUIRE(got.collected.size() == 1);
    CHECK(got.collected[0].delta.view() == sent.collected[0].delta.view());
    CHECK(got.error.empty());
}

TEST_CASE("distributed protocol: MAX_DT survives, because it is the common case")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    // A child that wants nothing reports MAX_DT on most cycles. A time codec
    // that mangled the sentinel would schedule the caller into a busy loop.
    CycleReply sent;
    sent.next_scheduled_time = MAX_DT;
    CHECK(decode_reply(slots, encode_reply(slots, sent)).next_scheduled_time == MAX_DT);

    CycleRequest at_min;
    at_min.evaluation_time = MIN_ST;
    CHECK(decode_request(slots, encode_request(slots, at_min)).evaluation_time == MIN_ST);
}

TEST_CASE("distributed protocol: an error is carried rather than thrown away")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    CycleReply sent;
    sent.error = "node[3 'child.boom'] evaluate failed";
    const auto got = decode_reply(slots, encode_reply(slots, sent));
    CHECK(got.error == sent.error);
    CHECK(got.collected.empty());
}

TEST_CASE("distributed protocol: a slot outside the boundary is refused")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    // Refused on the way OUT as well as in: an out-of-range slot written here
    // would look like a corrupt stream at the far end, and the sender is where
    // the bug is.
    CycleRequest bad;
    bad.staged.push_back(SlotDelta{5, Value{Int{1}}});
    CHECK_THROWS_WITH(encode_request(slots, bad),
                      Catch::Matchers::ContainsSubstring("outside the boundary"));
}

TEST_CASE("distributed protocol: a payload whose schema is not the slot's is refused")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("string");
    const auto slots = two_slots();

    // Positional encoding moves correctness into the schema agreement, so the
    // mismatch has to be caught rather than encoded and misread as an Int.
    CycleRequest wrong;
    wrong.staged.push_back(SlotDelta{0, Value{Str{"not an int"}}});
    CHECK_THROWS_WITH(encode_request(slots, wrong),
                      Catch::Matchers::ContainsSubstring("boundary declares"));
}

TEST_CASE("distributed protocol: framing yields whole messages only")
{
    const std::string payload = "a message of some length";
    const std::string framed  = write_frame(payload);

    std::string_view got;
    std::size_t      consumed = 0;

    // A stream delivers whatever it delivers, so every short prefix must be
    // reported as "not yet" rather than as a message.
    for (std::size_t cut = 0; cut < framed.size(); ++cut)
    {
        CHECK_FALSE(read_frame(std::string_view{framed}.substr(0, cut), got, consumed));
    }
    REQUIRE(read_frame(framed, got, consumed));
    CHECK(got == payload);
    CHECK(consumed == framed.size());

    // Two messages in one buffer: the first is returned and the second is left
    // for the next call, which is what `consumed` exists to make possible.
    const std::string pair = framed + write_frame("second");
    REQUIRE(read_frame(pair, got, consumed));
    CHECK(got == payload);
    std::string_view second;
    std::size_t      second_consumed = 0;
    REQUIRE(read_frame(std::string_view{pair}.substr(consumed), second, second_consumed));
    CHECK(second == "second");
}

TEST_CASE("distributed protocol: a child driven over the wire equals one driven directly")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    const std::vector<Int> inputs{1, 2, 3, 4};

    // Directly, as stage 2 does it.
    std::vector<Int>     direct;
    DistributedChildHost local{build_graph<BoundaryChildGraph>(), test_end};
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

    // And through the protocol: encode a request, decode it on the far side,
    // drive the child with it, encode the reply, decode it back.
    std::vector<Int>     over_wire;
    DistributedChildHost remote{build_graph<BoundaryChildGraph>(), test_end};
    remote.start(MIN_ST);
    when = MIN_ST;
    for (const Int value : inputs)
    {
        CycleRequest request;
        request.evaluation_time = when;
        request.staged.push_back(SlotDelta{slots.index_of("in"), Value{value}});
        const std::string frame = write_frame(encode_request(slots, request));

        std::string_view payload;
        std::size_t      consumed = 0;
        REQUIRE(read_frame(frame, payload, consumed));
        const CycleRequest received = decode_request(slots, payload);

        for (const auto &staged : received.staged)
        {
            remote.stage(slots.name_at(staged.slot), staged.delta.view());
        }
        REQUIRE(remote.step(received.evaluation_time));

        CycleReply reply;
        reply.next_scheduled_time = remote.next_scheduled_time();
        if (Value out = remote.collect("out"); out.has_value())
        {
            reply.collected.push_back(SlotDelta{slots.index_of("out"), std::move(out)});
        }
        const CycleReply returned =
            decode_reply(slots, [&] {
                std::string_view reply_payload;
                std::size_t      reply_consumed = 0;
                const std::string reply_frame = write_frame(encode_reply(slots, reply));
                REQUIRE(read_frame(reply_frame, reply_payload, reply_consumed));
                return std::string{reply_payload};
            }());

        REQUIRE(returned.collected.size() == 1);
        over_wire.push_back(returned.collected[0].delta.view().checked_as<Int>());
        when = when + MIN_TD;
    }
    remote.stop();

    CHECK(over_wire == direct);
    CHECK(over_wire == std::vector<Int>{1, 3, 6, 10});
}
