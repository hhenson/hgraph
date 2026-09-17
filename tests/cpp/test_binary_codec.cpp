// The binary value codec (RFC 0017's field-wise path).
//
// One assertion, applied to every shape: decode(encode(v)) equals v. A codec
// that is merely close is worse than none, because the damage surfaces as a
// wrong value in a child graph rather than as a decode error.

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/temporal.h>
#include <arrow/api.h>
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
    check_atom(Time{});
    check_atom(Time{45'296'000'789});
    check_atom(Time{86'399'999'999});
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

TEST_CASE("binary codec: named time zones use names rather than process handles")
{
    const ZoneId zone{"Europe/London"};
    check_atom(zone);
    check_atom(ZoneId{});
    const auto bytes = to_binary_string(Value{zone}.view());
    CHECK(bytes.find("Europe/London") != std::string::npos);
    const auto zoned = ZonedDateTime::from_resolved(Instant{Duration{1234567}}, zone, 3600);
    check_atom(zoned);
    check_atom(ZonedDateTime{});
    CHECK(to_binary_string(Value{zoned}.view()).find("Europe/London") != std::string::npos);
}

TEST_CASE("binary codec: Frame and Series IPC owns decoded data and preserves Arrow metadata")
{
    arrow::Int64Builder builder;
    REQUIRE(builder.Append(42).ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append(-7).ok());
    const auto array = builder.Finish().ValueOrDie();
    const auto schema = arrow::schema({arrow::field("value", arrow::int64())},
                                      arrow::key_value_metadata({"client"}, {"retained"}));
    const Frame frame{arrow::Table::Make(schema, {std::make_shared<arrow::ChunkedArray>(array)})};
    Value decoded;
    {
        auto bytes = to_binary_string(Value{frame}.view());
        decoded = from_binary_string(scalar_descriptor<Frame>::value_meta(), bytes);
        bytes.assign(bytes.size(), 'x');
    }
    REQUIRE(decoded.view().checked_as<Frame>().table);
    CHECK(decoded.view().checked_as<Frame>().table->Equals(*frame.table, true));
    const auto series = from_binary_string(scalar_descriptor<Series>::value_meta(),
                                          to_binary_string(Value{Series{array}}.view()));
    REQUIRE(series.view().checked_as<Series>().array);
    CHECK(series.view().checked_as<Series>().array->Equals(array));
    CHECK_FALSE(from_binary_string(scalar_descriptor<Frame>::value_meta(),
                                  to_binary_string(Value{Frame{}}.view())).view().checked_as<Frame>().has_value());
    CHECK_FALSE(from_binary_string(scalar_descriptor<Series>::value_meta(),
                                  to_binary_string(Value{Series{}}.view())).view().checked_as<Series>().has_value());
    const Series empty{arrow::MakeArrayOfNull(arrow::int64(), 0).ValueOrDie()};
    const auto empty_decoded = from_binary_string(scalar_descriptor<Series>::value_meta(),
                                                  to_binary_string(Value{empty}.view()));
    REQUIRE(empty_decoded.view().checked_as<Series>().array);
    CHECK(empty_decoded.view().checked_as<Series>().array->length() == 0);
}

TEST_CASE("binary codec: typed Frame and Series retain their declared schemas")
{
    auto &registry = TypeRegistry::instance();
    const auto *rows = registry.bundle("BinaryFrameRow", {{"value", scalar_descriptor<Int>::value_meta()}});
    const auto *schema = registry.frame(rows);
    arrow::Int64Builder builder;
    REQUIRE(builder.Append(9).ok());
    const auto array = builder.Finish().ValueOrDie();
    const Frame frame{arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}),
                                       {std::make_shared<arrow::ChunkedArray>(array)})};
    const Value value{ValuePlanFactory::instance().type_for(schema), &frame};
    const auto decoded = from_binary_string(schema, to_binary_string(value.view()));
    CHECK(decoded.schema() == schema);
    CHECK(decoded.view().checked_as<Frame>().table->Equals(*frame.table));
    const auto *series_schema = registry.series(scalar_descriptor<Int>::value_meta());
    const Series series{array};
    const Value typed_series{ValuePlanFactory::instance().type_for(series_schema), &series};
    const auto decoded_series = from_binary_string(series_schema, to_binary_string(typed_series.view()));
    CHECK(decoded_series.schema() == series_schema);
    CHECK(decoded_series.view().checked_as<Series>().array->Equals(array));
}

TEST_CASE("binary codec: nullable list and map values preserve unset entries")
{
    auto &registry = TypeRegistry::instance();
    const auto integer = registry.scalar_type<Int>();
    const auto *nullable = registry.nullable_tuple(integer.schema());
    ListBuilder list{integer, *nullable};
    list.push_back(Int{1});
    list.push_back_unset();
    list.push_back(Int{3});
    const auto value = list.build();
    check_round_trip(value);
    const auto decoded = from_binary_string(nullable, to_binary_string(value.view()));
    CHECK_FALSE(decoded.as_list().at(1).has_value());
    MapBuilder map{registry.scalar_type<Str>(), integer};
    map.set_item_unset(Value{Str{"missing"}}.view());
    map.set_item(Value{Str{"value"}}.view(), Value{Int{7}}.view());
    check_round_trip(map.build());
}

TEST_CASE("binary codec: Owned and Shared bundles transport their values")
{
    auto &registry = TypeRegistry::instance();
    const auto *plain = registry.bundle("BinaryIndirect", {{"id", scalar_descriptor<Int>::value_meta()}});
    BundleBuilder builder{ValuePlanFactory::instance().type_for(plain)};
    builder.set("id", Value{Int{7}});
    const auto source = builder.build();
    for (const auto *schema : {registry.owned(plain), registry.shared(plain)})
    {
        const auto binding = ValuePlanFactory::instance().type_for(schema);
        const Value value{binding, source.view()};
        const auto decoded = from_binary_string(schema, to_binary_string(value.view()));
        CHECK(decoded.as_bundle()["id"].checked_as<Int>() == 7);
        CHECK(decoded.view().concrete().data() != value.view().concrete().data());
    }
}

TEST_CASE("binary codec: queues and rotated ring buffers preserve logical order")
{
    const auto integer = TypeRegistry::instance().scalar_type<Int>();
    CyclicBufferBuilder ring{integer, 2};
    ring.push_back(Int{1});
    ring.push_back(Int{2});
    ring.push_back(Int{3});
    check_round_trip(ring.build());
    QueueBuilder queue{integer, 3};
    queue.push(Int{10});
    queue.push(Int{20});
    check_round_trip(queue.build());
}

TEST_CASE("binary codec: bound plans retain separate closed Bundle realizations")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *text = scalar_descriptor<Str>::value_meta();
    const auto *base = registry.bundle("binary.test", "Base", {{"id", integer}}, {}, true);
    const auto *first = registry.bundle("binary.test", "First", {{"id", integer}, {"label", text}}, {base});
    auto first_snapshot = TypeRealizationSnapshot::capture(registry);
    BoundBinaryConverter first_plan;
    {
        const TypeRealizationScope scope{first_snapshot.get()};
        first_plan = bind_binary_converter(base);
    }
    const auto *second = registry.bundle("binary.test", "Second", {{"id", integer}, {"quantity", integer}}, {base});
    auto second_snapshot = TypeRealizationSnapshot::capture(registry);
    BoundBinaryConverter second_plan;
    {
        const TypeRealizationScope scope{second_snapshot.get()};
        second_plan = bind_binary_converter(base);
    }
    BundleBuilder first_builder{first_snapshot->exact_type_for(first)};
    first_builder.set("id", Value{Int{7}});
    first_builder.set("label", Value{Str{"complete derived payload"}});
    const auto first_value = first_builder.build();
    BundleBuilder second_builder{second_snapshot->exact_type_for(second)};
    second_builder.set("id", Value{Int{8}});
    second_builder.set("quantity", Value{Int{99}});
    const auto second_value = second_builder.build();
    std::string first_bytes;
    first_plan.write(first_value.view(), first_bytes);
    BinaryReader first_reader{first_bytes};
    auto first_decoded = second_plan.read(first_reader);
    CHECK(first_decoded.view().concrete().schema() == first);
    CHECK(first_decoded.view().concrete().as_bundle()["label"].checked_as<Str>() == "complete derived payload");
    std::string second_bytes;
    second_plan.write(second_value.view(), second_bytes);
    BinaryReader second_reader{second_bytes};
    CHECK_THROWS_WITH(first_plan.read(second_reader), Catch::Matchers::ContainsSubstring("captured realization"));
    CHECK_THROWS_WITH(first_plan.write(second_value.view(), first_bytes), Catch::Matchers::ContainsSubstring("captured realization"));
    const auto locks_before = type_system_lock_count();
    for (int i = 0; i < 8; ++i)
    {
        std::string bytes;
        second_plan.write(second_value.view(), bytes);
        BinaryReader reader{bytes};
        const auto decoded = second_plan.read(reader);
        const auto quantity = decoded.view().concrete().as_bundle()["quantity"].checked_as<Int>();
        CHECK(quantity == 99);
    }
    CHECK(type_system_lock_count() == locks_before);
    CHECK(binary_converter(base).binding == ValuePlanFactory::instance().type_for(base));
}

TEST_CASE("binary codec: portable hashes ignore container insertion order and floating zero sign")
{
    const auto integer = TypeRegistry::instance().scalar_type<Int>();
    SetBuilder left{integer};
    SetBuilder right{integer};
    for (const Int item : {1, 2, 3}) left.insert(Value{item}.view());
    for (const Int item : {3, 2, 1}) right.insert(Value{item}.view());
    const auto a = left.build();
    const auto b = right.build();
    const auto set_codec = bind_binary_converter(a.schema());
    CHECK(a.view() == b.view());
    CHECK(set_codec.portable_hash(a.view()) == set_codec.portable_hash(b.view()));
    MapBuilder first{integer, integer};
    MapBuilder second{integer, integer};
    for (const Int item : {1, 2, 3}) first.set_item(Value{item}.view(), Value{item * 2}.view());
    for (const Int item : {3, 2, 1}) second.set_item(Value{item}.view(), Value{item * 2}.view());
    const auto first_map = first.build();
    const auto second_map = second.build();
    const auto map_codec = bind_binary_converter(first_map.schema());
    CHECK(map_codec.portable_hash(first_map.view()) == map_codec.portable_hash(second_map.view()));
    const auto floats = bind_binary_converter(scalar_descriptor<Float>::value_meta());
    CHECK(floats.portable_hash(Value{Float{0.0}}.view()) == floats.portable_hash(Value{Float{-0.0}}.view()));
    const auto zone = Value{ZoneId{"Europe/London"}};
    const auto zones = bind_binary_converter(zone.schema());
    std::uint64_t name_hash = 14695981039346656037ULL;
    for (const unsigned char character : std::string_view{"Europe/London"})
        name_hash = (name_hash ^ character) * 1099511628211ULL;
    CHECK(zones.portable_hash(zone.view()) == name_hash);
}

TEST_CASE("binary codec: temporal ranges encode semantic endpoints without padding")
{
    const auto start = Instant{Duration{100}};
    const auto end = Instant{Duration{200}};
    check_atom(InstantRange{});
    check_atom(InstantRange::all());
    check_atom(InstantRange::from(start));
    check_atom(InstantRange::until(end, Boundary::Closed));
    check_atom(InstantRange::bounded(start, end, Boundary::Open, Boundary::Closed));
    check_atom(InstantRangeSet{InstantRange::bounded(start, end)});
    check_atom(CivilDateRange::all());
    check_atom(CivilDateRangeSet{});
    CHECK(to_binary_string(Value{InstantRange{}}.view()).size() == 1);
    const auto ranges = bind_binary_converter(scalar_descriptor<InstantRange>::value_meta());
    const auto value = Value{InstantRange::bounded(start, end)};
    const auto decoded = from_binary_string(value.schema(), to_binary_string(value.view()));
    CHECK(ranges.portable_hash(value.view()) == ranges.portable_hash(decoded.view()));
}

TEST_CASE("binary codec: collection work is bounded even for zero-byte elements")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = scalar_descriptor<Int>::value_meta();
    const auto *list = registry.list(integer);
    // Reuse the real list reader with a zero-byte element decoder. This probes
    // the work contract without relying on unsupported zero-stride storage.
    BinaryConverter zero_element{binary_converter(integer)};
    zero_element.read_ = +[](const BinaryConverter &, BinaryReader &) -> Value { return Value{Int{0}}; };
    BinaryConverter zero_list{binary_converter(list)};
    zero_list.children = {&zero_element};
    std::string attack;
    write_varint(std::numeric_limits<std::uint64_t>::max(), attack);
    BinaryReader attack_reader{attack};
    CHECK_THROWS_WITH(zero_list.read(attack_reader), Catch::Matchers::ContainsSubstring("work limit"));
    std::string tiny;
    write_varint(3, tiny);
    BinaryReader tiny_reader{tiny};
    const auto zero_values = zero_list.read(tiny_reader);
    CHECK(zero_values.view().as_list().size() == 3);
    CHECK(tiny_reader.remaining() == 0);

    // Public decoding also shares one budget across nested integer lists.
    const auto *nested = registry.list(list);
    std::string small;
    write_varint(3, small);
    const auto element = to_binary_string(Value{Int{42}}.view());
    for (int i = 0; i < 3; ++i)
    {
        write_varint(2, small);
        small += element;
        small += element;
    }
    CHECK_THROWS_WITH(from_binary_string(nested, small, {18, 256}),
                      Catch::Matchers::ContainsSubstring("work limit"));
    CHECK(from_binary_string(nested, small, {19, 256}).view().as_list().size() == 3);
    CHECK_THROWS_WITH(from_binary_string(nested, small, {100, 2}),
                      Catch::Matchers::ContainsSubstring("depth limit"));
}

TEST_CASE("binary codec: subreaders share work and restore depth after failure")
{
    const auto codec = bind_binary_converter(scalar_descriptor<Int>::value_meta());
    const auto bytes = to_binary_string(Value{Int{7}}.view());
    const auto joined = bytes + bytes;
    BinaryReader parent{joined, 0, {1, 1}};
    auto first = parent.subreader(bytes.size());
    CHECK(codec.read(first).view().checked_as<Int>() == 7);
    auto second = parent.subreader(bytes.size());
    CHECK_THROWS_WITH(codec.read(second), Catch::Matchers::ContainsSubstring("work limit"));
    BinaryReader truncated{{}, 0, {4, 1}};
    CHECK_THROWS_WITH(codec.read(truncated), Catch::Matchers::ContainsSubstring("truncated"));
    CHECK_THROWS_WITH(codec.read(truncated), Catch::Matchers::ContainsSubstring("truncated"));
}

TEST_CASE("binary codec: default and moved-from converters refuse safely")
{
    const auto check_refusal = [](const BinaryConverter &converter)
    {
        REQUIRE(converter.write_ != nullptr);
        REQUIRE(converter.read_ != nullptr);
        REQUIRE(converter.hash_ != nullptr);
        std::string output;
        BinaryReader reader;
        const Value value{Int{4}};
        CHECK_THROWS_WITH(converter.write(value.view(), output), Catch::Matchers::ContainsSubstring("unbound"));
        CHECK_THROWS_WITH(converter.read(reader), Catch::Matchers::ContainsSubstring("unbound"));
        CHECK_THROWS_WITH(converter.hash_(converter, value.view()), Catch::Matchers::ContainsSubstring("unbound"));
    };
    BinaryConverter empty;
    check_refusal(empty);
    BinaryConverter source{binary_converter(scalar_descriptor<Int>::value_meta())};
    BinaryConverter moved{std::move(source)};
    check_refusal(source);
    std::string bytes;
    moved.write(Value{Int{42}}.view(), bytes);
    BinaryReader reader{bytes};
    CHECK(moved.read(reader).view().checked_as<Int>() == 42);
    source = std::move(moved);
    check_refusal(moved);
    BinaryReader again{bytes};
    CHECK(source.read(again).view().checked_as<Int>() == 42);
}
