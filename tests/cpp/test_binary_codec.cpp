// The binary value codec (RFC 0017's field-wise path).
//
// One assertion, applied to every shape: decode(encode(v)) equals v. A codec
// that is merely close is worse than none, because the damage surfaces as a
// wrong value in a child graph rather than as a decode error.

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/metadata/value_plan_factory.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <vector>

namespace
{
    using namespace hgraph;

    /** encode -> decode -> compare, the whole point of the codec. */
    void check_round_trip(const Value &value)
    {
        const std::string bytes   = to_binary_string(value.view());
        const Value       decoded = from_binary_string(value.view().schema(), bytes);
        CHECK(decoded.view() == value.view());
    }

    template <typename T> void check_atom(T raw)
    {
        check_round_trip(Value{raw});
    }
}  // namespace

TEST_CASE("binary codec: atoms round trip")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Float>("float");
    (void)TypeRegistry::instance().register_scalar<Bool>("bool");
    (void)TypeRegistry::instance().register_scalar<Str>("string");

    check_atom(Int{0});
    check_atom(Int{-1});
    check_atom(std::numeric_limits<Int>::min());
    check_atom(std::numeric_limits<Int>::max());
    check_atom(Float{0.0});
    check_atom(Float{-1.5});
    check_atom(Bool{true});
    check_atom(Bool{false});
}

TEST_CASE("binary codec: strings round trip, including the awkward ones")
{
    (void)TypeRegistry::instance().register_scalar<Str>("string");

    // Length-prefixed bytes, so none of these is special to the format.
    check_atom(Str{""});
    check_atom(Str{"plain"});
    check_atom(Str{"with \"quotes\" and \\backslash"});
    check_atom(Str{"embedded\0nul", 12});
    check_atom(Str{"unicode \xe2\x9c\x93 tick"});
    check_atom(Str(1000, 'x'));   // multi-byte varint length
}

TEST_CASE("binary codec: byte strings round trip")
{
    (void)TypeRegistry::instance().register_scalar<Bytes>("bytes");

    // Arbitrary binary content, including NULs and bytes that are not valid
    // UTF-8, has to survive unchanged.
    check_round_trip(Value{Bytes{}});
    check_round_trip(Value{bytes_("plain")});
    check_round_trip(Value{Bytes{std::string{"with\0nul", 8}}});

    std::string every_byte;
    for (int i = 0; i < 256; ++i) { every_byte.push_back(static_cast<char>(i)); }
    check_round_trip(Value{Bytes{every_byte}});

    check_round_trip(Value{Bytes{std::string(1000, '\xff')}});   // multi-byte varint length
}

TEST_CASE("binary codec: a varint round trips across its width boundaries")
{
    // Counts and lengths are LEB128 because they are almost always small; the
    // boundaries are where an off-by-one in the shift loop would hide.
    for (const std::uint64_t value : {std::uint64_t{0}, std::uint64_t{1}, std::uint64_t{127},
                                      std::uint64_t{128}, std::uint64_t{16383}, std::uint64_t{16384},
                                      std::uint64_t{1} << 31, std::uint64_t{1} << 63,
                                      ~std::uint64_t{0}})
    {
        std::string out;
        write_varint(value, out);
        BinaryReader reader{out, 0};
        CHECK(read_varint(reader) == value);
        CHECK(reader.remaining() == 0);
    }
}

TEST_CASE("binary codec: a truncated buffer is refused, not guessed")
{
    (void)TypeRegistry::instance().register_scalar<Str>("string");

    const Value       value = Value{Str{"some text"}};
    const std::string bytes = to_binary_string(value.view());
    REQUIRE(bytes.size() > 2);

    // Every short prefix must fail rather than return a partial value: a
    // silently truncated delta is a wrong tick in a child graph.
    for (std::size_t cut = 1; cut < bytes.size(); ++cut)
    {
        CHECK_THROWS(from_binary_string(value.view().schema(), std::string_view{bytes}.substr(0, cut)));
    }
}

TEST_CASE("binary codec: trailing bytes are refused")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const Value value = Value{Int{7}};
    std::string bytes = to_binary_string(value.view());
    bytes.push_back('\0');
    CHECK_THROWS_WITH(from_binary_string(value.view().schema(), bytes),
                      Catch::Matchers::ContainsSubstring("trailing bytes"));
}

TEST_CASE("binary codec: the last varint byte cannot overflow uint64")
{
    for (const int last : {0x02, 0x7f, 0x81})
    {
        std::string bytes(9, static_cast<char>(0x80));
        bytes.push_back(static_cast<char>(last));
        BinaryReader reader{bytes, 0};
        CHECK_THROWS_WITH(read_varint(reader), Catch::Matchers::ContainsSubstring("overflow"));
    }
    BinaryReader invalid{"x", 2};
    CHECK_THROWS_WITH(invalid.take(1), Catch::Matchers::ContainsSubstring("truncated"));
}

TEST_CASE("binary codec: temporal and composite boundary values round trip")
{
    check_atom(DateTime{TimeDelta{123456}});
    check_atom(TimeDelta{7654});
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *text = registry.register_scalar<Str>("str");
    const auto *schema = registry.un_named_bundle({{"count", integer}, {"label", text}});
    BundleBuilder fields{ValuePlanFactory::instance().type_for(schema)};
    fields.set("count", Value{Int{7}});
    const auto partial = fields.build();
    check_round_trip(partial);
    CHECK_FALSE(from_binary_string(schema, to_binary_string(partial.view())).view().as_bundle().at(1).has_value());
    ListBuilder list{registry.scalar_type<Int>(), *registry.list(integer, 0, true)};
    list.push_back(Int{2});
    list.push_back(Int{3});
    check_round_trip(list.build());
    MapBuilder map{registry.scalar_type<Str>(), registry.scalar_type<Int>()};
    map.set_item(Value{Str{"key"}}.view(), Value{Int{8}}.view());
    check_round_trip(map.build());
}
