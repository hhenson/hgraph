// The messages between a dmap_ caller and one worker (RFC 0037, stage 3).
//
// The assertion that matters is the last one: a child driven THROUGH the
// encoded protocol must produce exactly what the same child produces driven
// directly. Everything before it checks the failure modes a wire format must
// not have -- a truncated frame accepted, a slot silently dropped, a payload
// decoded against the wrong schema.

#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/metadata/value_plan_factory.h>

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
#include <hgraph/types/value/binary_codec.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    const ValueTypeMetaData *int_schema() { return scalar_descriptor<Int>::value_meta(); }

    BoundarySlots two_slots()
    {
        BoundarySlots slots;
        slots.add("in", int_schema(), SlotDirection::Input);
        slots.add("out", int_schema(), SlotDirection::Output);
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

    /** Asks for a cycle of its own, which is what a caller must honour. */
    struct SelfScheduling
    {
        static constexpr auto name = "protocol_self_scheduling";

        static void eval(In<"ts", TS<Int>> ts, NodeScheduler sched, Out<TS<Int>> out)
        {
            out.set(ts.value());
            sched.schedule(TimeDelta{10});
        }
    };

    struct SelfSchedulingChildGraph
    {
        static constexpr auto name = "protocol_self_scheduling_graph";
        static void           compose(Wiring &w)
        {
            auto in   = wire<boundary_source_impl, TS<Int>>(w, Str{"in"});
            auto next = wire<SelfScheduling>(w, in);
            wire<boundary_sink_impl>(w, next, Str{"out"});
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

TEST_CASE("distributed protocol: unset payloads and impossible inventories are refused")
{
    const auto slots = two_slots();
    CycleRequest request;
    request.staged.push_back(SlotDelta{0, Value{}});
    CHECK_THROWS_WITH(encode_request(slots, request),
                      Catch::Matchers::ContainsSubstring("unset"));
    std::string bytes(sizeof(std::int64_t), '\0');
    write_varint(std::numeric_limits<std::uint64_t>::max(), bytes);
    CHECK_THROWS_WITH(decode_request(slots, bytes),
                      Catch::Matchers::ContainsSubstring("truncated slot inventory"));
}

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

TEST_CASE("distributed worker: serve_cycle is the whole of a worker's behaviour")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);

    std::vector<Int> collected;
    DateTime         when = MIN_ST;
    for (const Int value : {Int{1}, Int{2}, Int{3}, Int{4}})
    {
        CycleRequest request;
        request.evaluation_time = when;
        request.staged.push_back(SlotDelta{slots.index_of("in"), Value{value}});

        const CycleReply reply = serve_cycle(host, slots, request);
        REQUIRE(reply.error.empty());
        REQUIRE(reply.collected.size() == 1);
        collected.push_back(reply.collected[0].delta.view().checked_as<Int>());
        when = when + MIN_TD;
    }
    host.stop();

    CHECK(collected == std::vector<Int>{1, 3, 6, 10});
}

TEST_CASE("distributed worker: a cycle with no output reports none, not an empty one")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);

    CycleRequest first;
    first.evaluation_time = MIN_ST;
    first.staged.push_back(SlotDelta{slots.index_of("in"), Value{Int{5}}});
    REQUIRE(serve_cycle(host, slots, first).collected.size() == 1);

    // Nothing staged: the caller had a cycle this child was not part of.
    CycleRequest idle;
    idle.evaluation_time = MIN_ST + TimeDelta{1};
    const CycleReply reply = serve_cycle(host, slots, idle);
    CHECK(reply.error.empty());
    CHECK(reply.collected.empty());
    host.stop();
}

TEST_CASE("distributed worker: a failure is reported, because the caller cannot catch it")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    DistributedChildHost host{build_graph<BoundaryChildGraph>(), test_end};
    host.start(MIN_ST);

    // Staging into an OUTPUT slot is a caller bug. Across a process boundary
    // an exception cannot propagate, so it has to come back as a reply.
    CycleRequest wrong;
    wrong.evaluation_time = MIN_ST;
    wrong.staged.push_back(SlotDelta{slots.index_of("out"), Value{Int{1}}});

    const CycleReply reply = serve_cycle(host, slots, wrong);
    CHECK_THAT(reply.error, Catch::Matchers::ContainsSubstring("output and cannot be staged"));
    CHECK(reply.collected.empty());
    CHECK(reply.next_scheduled_time == MAX_DT);
    host.stop();
}

TEST_CASE("distributed worker: stepping over the child's own due work is an error, not a lost tick")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const auto slots = two_slots();

    DistributedChildHost host{build_graph<SelfSchedulingChildGraph>(), test_end};
    host.start(MIN_ST);

    CycleRequest first;
    first.evaluation_time = MIN_ST;
    first.staged.push_back(SlotDelta{slots.index_of("in"), Value{Int{1}}});
    const CycleReply served = serve_cycle(host, slots, first);
    REQUIRE(served.error.empty());
    // The child asked for a cycle of its own, and said so in the reply.
    REQUIRE(served.next_scheduled_time == MIN_ST + TimeDelta{10});

    // Overrunning it would silently discard that cycle, so it is refused and
    // the refusal travels back rather than becoming a missing tick.
    CycleRequest late;
    late.evaluation_time = MIN_ST + TimeDelta{50};
    const CycleReply reply = serve_cycle(host, slots, late);
    CHECK_THAT(reply.error, Catch::Matchers::ContainsSubstring("skip work already due"));

    // Honouring it works, and the child is undisturbed by the refusal.
    CycleRequest due;
    due.evaluation_time = MIN_ST + TimeDelta{10};
    CHECK(serve_cycle(host, slots, due).error.empty());
    host.stop();
}

TEST_CASE("distributed protocol: malformed and oversized prefixes fail before payload arrival")
{
    std::string_view payload = "unchanged";
    std::size_t consumed = 23;
    for (int final_byte : {0x80, 0x81, 0x02, 0x7f})
    {
        std::string prefix(9, static_cast<char>(0x80));
        CHECK_FALSE(read_frame(prefix, payload, consumed));
        prefix.push_back(static_cast<char>(final_byte));
        CHECK_THROWS_WITH(read_frame(prefix, payload, consumed),
                          Catch::Matchers::ContainsSubstring("overflow"));
    }
    std::string oversized;
    write_varint(DEFAULT_MAX_FRAME_SIZE + 1, oversized);
    CHECK_THROWS_WITH(read_frame(oversized, payload, consumed),
                      Catch::Matchers::ContainsSubstring("size limit"));
    CHECK(payload == "unchanged");
    CHECK(consumed == 23);
    CHECK_THROWS_WITH(write_frame("four", 3), Catch::Matchers::ContainsSubstring("size limit"));
    const auto bounded = write_frame("four", 4);
    REQUIRE(read_frame(bounded, payload, consumed, 4));
    CHECK(payload == "four");
    CHECK_THROWS_WITH(read_frame(bounded, payload, consumed, 3),
                      Catch::Matchers::ContainsSubstring("size limit"));
}

TEST_CASE("distributed protocol: slot direction is checked on encode and decode")
{
    const auto slots = two_slots();
    CycleRequest request;
    request.staged.push_back({0, Value{Int{4}}});
    auto request_bytes = encode_request(slots, request);
    // Eight time bytes, one inventory count byte, then the slot index.
    request_bytes[9] = 1;
    CHECK_THROWS_WITH(decode_request(slots, request_bytes),
                      Catch::Matchers::ContainsSubstring("slot direction"));
    request.staged[0].slot = 1;
    CHECK_THROWS_WITH(encode_request(slots, request),
                      Catch::Matchers::ContainsSubstring("slot direction"));
    CycleReply reply;
    reply.collected.push_back({1, Value{Int{4}}});
    auto reply_bytes = encode_reply(slots, reply);
    reply_bytes[9] = 0;
    CHECK_THROWS_WITH(decode_reply(slots, reply_bytes),
                      Catch::Matchers::ContainsSubstring("slot direction"));
    reply.collected[0].slot = 0;
    CHECK_THROWS_WITH(encode_reply(slots, reply),
                      Catch::Matchers::ContainsSubstring("slot direction"));
}

TEST_CASE("distributed protocol: slot payloads share one decode budget")
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.un_named_bundle({});
    BoundarySlots slots;
    slots.add("first", schema, SlotDirection::Input);
    slots.add("second", schema, SlotDirection::Input);
    const auto empty = BundleBuilder{ValuePlanFactory::instance().type_for(schema)}.build();
    CycleRequest request;
    request.staged.push_back({0, empty});
    request.staged.push_back({1, empty});
    const auto bytes = encode_request(slots, request);
    CHECK_THROWS_WITH(decode_request(slots, bytes, {3, 256}),
                      Catch::Matchers::ContainsSubstring("work limit"));
    CHECK(decode_request(slots, bytes, {4, 256}).staged.size() == 2);
}

TEST_CASE("distributed protocol: duplicate slot updates are rejected on encode and decode")
{
    const auto slots = two_slots();
    CycleRequest request;
    request.staged.push_back({0, Value{Int{4}}});
    auto request_bytes = encode_request(slots, request);
    const auto request_entry = request_bytes.substr(9); // time, count, then entries
    request_bytes[8] = 2;
    request_bytes += request_entry;
    CHECK_THROWS_WITH(decode_request(slots, request_bytes),
                      Catch::Matchers::ContainsSubstring("duplicate slot"));
    request.staged.push_back({0, Value{Int{5}}});
    CHECK_THROWS_WITH(encode_request(slots, request),
                      Catch::Matchers::ContainsSubstring("duplicate slot"));

    CycleReply reply;
    reply.collected.push_back({1, Value{Int{4}}});
    auto reply_bytes = encode_reply(slots, reply);
    reply_bytes.pop_back(); // remove the empty error string before duplicating
    const auto reply_entry = reply_bytes.substr(9);
    reply_bytes[8] = 2;
    reply_bytes += reply_entry;
    reply_bytes.push_back('\0');
    CHECK_THROWS_WITH(decode_reply(slots, reply_bytes),
                      Catch::Matchers::ContainsSubstring("duplicate slot"));
    reply.collected.push_back({1, Value{Int{5}}});
    CHECK_THROWS_WITH(encode_reply(slots, reply),
                      Catch::Matchers::ContainsSubstring("duplicate slot"));
}
