// The binary codec's session and frame (RFC 0040, stage 1).
//
// A session is what lets an ``Any`` be written at all: the box names its
// content's schema in a table the encoding carries once. The rule under test
// is the same as the codec's -- decode(encode(v)) equals v -- plus the two
// promises the frame makes: a value that needs no table pays almost nothing
// for one, and bytes are refused by name rather than misread.

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/binary_session.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>

namespace
{
    using namespace hgraph;
    using Catch::Matchers::ContainsSubstring;

    Value empty_any()
    {
        return Value{ValuePlanFactory::instance().type_for(TypeRegistry::instance().any())};
    }

    Value any_of(Value content)
    {
        Value box = empty_any();
        box.as_any().begin_mutation().set(std::move(content));
        return box;
    }

    Value framed_round_trip(const Value &value, BinaryProfile profile = BinaryProfile::Compact)
    {
        const std::string bytes = encode_binary_frame(value.view(), profile);
        return decode_binary_frame(value.view().schema(), bytes);
    }

    void check_framed(const Value &value)
    {
        CHECK(framed_round_trip(value).view() == value.view());
        CHECK(framed_round_trip(value, BinaryProfile::Fast).view() == value.view());
    }
}  // namespace

TEST_CASE("binary session: a value that needs no table pays two bytes for one")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const Value value{Int{42}};

    const std::string bytes = encode_binary_frame(value.view());
    // profile, revision, four bytes of payload length, the payload, and an
    // empty value table and endpoint table. The reader's schema is not named.
    CHECK(bytes.size() == 6 + to_binary_string(value.view()).size() + 2);
    CHECK(decode_binary_frame(value.view().schema(), bytes).view() == value.view());
}

TEST_CASE("binary session: an Any round trips, empty, full and nested")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *text = registry.register_scalar<Str>("str");

    check_framed(empty_any());
    check_framed(any_of(Value{Int{-7}}));
    check_framed(any_of(Value{Str{"boxed text"}}));
    check_framed(any_of(any_of(Value{Float{2.5}})));
    check_framed(any_of(any_of(empty_any())));

    const auto *schema = registry.un_named_bundle({{"count", integer}, {"label", text}});
    BundleBuilder fields{ValuePlanFactory::instance().type_for(schema)};
    fields.set("count", Value{Int{7}});
    fields.set("label", Value{Str{"seven"}});
    check_framed(any_of(fields.build()));

    MapBuilder map{registry.scalar_type<Str>(), registry.scalar_type<Int>()};
    map.set_item(Value{Str{"key"}}.view(), Value{Int{8}}.view());
    check_framed(any_of(map.build()));

    // Every structural kind a box can hold has to be nameable in the table.
    CyclicBufferBuilder ring{registry.scalar_type<Int>(), 2};
    ring.push_back(Int{1});
    ring.push_back(Int{2});
    ring.push_back(Int{3});
    check_framed(any_of(ring.build()));
    QueueBuilder queue{registry.scalar_type<Int>(), 3};
    queue.push(Int{10});
    queue.push(Int{20});
    check_framed(any_of(queue.build()));

    const Value restored = framed_round_trip(any_of(Value{Int{-7}}));
    REQUIRE(restored.as_any().has_value());
    CHECK(restored.as_any().value_schema() == integer);
    CHECK(restored.as_any().get().checked_as<Int>() == -7);
}

TEST_CASE("binary session: boxes of one schema share one table entry")
{
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<Int>("int");
    const auto *any = registry.any();
    const auto *pair = registry.un_named_bundle({{"a", any}, {"b", any}});

    const auto bundle_of = [&](Value a, Value b) {
        BundleBuilder fields{ValuePlanFactory::instance().type_for(pair)};
        fields.set("a", std::move(a));
        fields.set("b", std::move(b));
        return fields.build();
    };
    const Value one = bundle_of(any_of(Value{Int{1}}), empty_any());
    const Value two = bundle_of(any_of(Value{Int{1}}), any_of(Value{Int{2}}));
    check_framed(one);
    check_framed(two);

    // The second box costs its tag and its eight bytes, less the one byte an
    // empty box took: the schema it names is already in the table.
    CHECK(encode_binary_frame(two.view()).size() == encode_binary_frame(one.view()).size() + 8);
}

TEST_CASE("binary session: a full Any outside a session is refused by name")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const Value full = any_of(Value{Int{3}});

    CHECK_THROWS_WITH(to_binary_string(full.view()), ContainsSubstring("session"));

    // The bytes of a full box, met by a reader that has no session.
    const std::string frame = encode_binary_frame(full.view());
    const std::string payload = frame.substr(6, frame.size() - 6);
    CHECK_THROWS_WITH(from_binary_string(full.view().schema(), payload), ContainsSubstring("session"));

    // An empty box names no schema, so it needs no table.
    const Value empty = empty_any();
    CHECK(from_binary_string(empty.view().schema(), to_binary_string(empty.view())).view() == empty.view());

    CHECK_THROWS_WITH(bind_binary_converter(full.view().schema()).portable_hash(full.view()),
                      ContainsSubstring("partition key"));
}

TEST_CASE("binary session: a frame records its profile and a reader never guesses")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    const Value value = any_of(Value{Int{11}});

    const std::string compact = encode_binary_frame(value.view(), BinaryProfile::Compact);
    const std::string fast = encode_binary_frame(value.view(), BinaryProfile::Fast);
    CHECK(binary_frame_profile(compact) == BinaryProfile::Compact);
    CHECK(binary_frame_profile(fast) == BinaryProfile::Fast);
    CHECK(bind_binary_converter(value.view().schema(), BinaryProfile::Fast).profile() == BinaryProfile::Fast);
    CHECK(bind_binary_converter(value.view().schema()).profile() == BinaryProfile::Compact);

    std::string unknown_profile = compact;
    unknown_profile[0] = '\x07';
    CHECK_THROWS_WITH(decode_binary_frame(value.view().schema(), unknown_profile), ContainsSubstring("profile 7"));

    std::string later_revision = compact;
    later_revision[1] = static_cast<char>(binary_profile_revision(BinaryProfile::Compact) + 1);
    CHECK_THROWS_WITH(decode_binary_frame(value.view().schema(), later_revision), ContainsSubstring("revision"));
}

TEST_CASE("binary session: a damaged frame is refused, not guessed")
{
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    const Value       value = any_of(Value{Str{"some text"}});
    const std::string bytes = encode_binary_frame(value.view());

    for (std::size_t cut = 0; cut < bytes.size(); ++cut)
    {
        CHECK_THROWS(decode_binary_frame(value.view().schema(), std::string_view{bytes}.substr(0, cut)));
    }
    CHECK_THROWS_WITH(decode_binary_frame(value.view().schema(), bytes + '\0'), ContainsSubstring("trailing"));

    // A box that names a schema the table does not have.
    std::string wrong_index = bytes;
    wrong_index[6] = '\x09';
    CHECK_THROWS_WITH(decode_binary_frame(value.view().schema(), wrong_index), ContainsSubstring("names schema"));
}
