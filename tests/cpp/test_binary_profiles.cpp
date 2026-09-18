// The binary codec's profiles (RFC 0040): the same value, the same answer, two
// encodings with different costs.
//
// What is asserted for every shape and both profiles is the codec's one rule:
// decode(encode(v)) equals v. What is measured is what the profiles are for.

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/binary_session.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;

    const ValueTypeMetaData *row_schema()
    {
        auto &registry = TypeRegistry::instance();
        return registry.un_named_bundle({
            {"id", registry.register_scalar<Int>("int")},
            {"price", registry.register_scalar<Float>("float")},
            {"quantity", registry.register_scalar<Float>("float")},
            {"time", registry.register_scalar<DateTime>("datetime")},
            {"live", registry.register_scalar<Bool>("bool")},
            {"symbol", registry.register_scalar<Str>("str")},
        });
    }

    Value row(std::int64_t index)
    {
        BundleBuilder fields{ValuePlanFactory::instance().type_for(row_schema())};
        fields.set("id", Value{Int{index}});
        fields.set("price", Value{Float{100.0 + static_cast<double>(index % 97) * 0.25}});
        fields.set("quantity", Value{Float{static_cast<double>(1 + index % 13)}});
        fields.set("time", Value{MIN_ST + TimeDelta{index * 1000}});
        fields.set("live", Value{Bool{index % 3 != 0}});
        fields.set("symbol", Value{Str{"SYM" + std::to_string(index % 50)}});
        return fields.build();
    }

    Value int_list(std::size_t count)
    {
        ListBuilder list{TypeRegistry::instance().scalar_type<Int>()};
        for (std::size_t index = 0; index < count; ++index) { list.push_back(Value{Int{static_cast<Int>(index * 7)}}.view()); }
        return list.build();
    }

    Value float_list(std::size_t count)
    {
        ListBuilder list{TypeRegistry::instance().scalar_type<Float>()};
        for (std::size_t index = 0; index < count; ++index)
        {
            list.push_back(Value{Float{static_cast<double>(index) * 0.5}}.view());
        }
        return list.build();
    }

    Value row_list(std::size_t count)
    {
        ListBuilder list{ValuePlanFactory::instance().type_for(row_schema())};
        for (std::size_t index = 0; index < count; ++index) { list.push_back(row(static_cast<std::int64_t>(index)).view()); }
        return list.build();
    }

    Value int_float_map(std::size_t count)
    {
        auto &registry = TypeRegistry::instance();
        MapBuilder map{registry.scalar_type<Int>(), registry.scalar_type<Float>()};
        for (std::size_t index = 0; index < count; ++index)
        {
            map.set_item(Value{Int{static_cast<Int>(index)}}.view(), Value{Float{static_cast<double>(index)}}.view());
        }
        return map.build();
    }

    struct Measured
    {
        double      encode_us{0};
        double      decode_us{0};
        std::size_t bytes{0};
    };

    Measured measure(const Value &value, const BoundBinaryConverter &converter, std::size_t repeats)
    {
        Measured   result;
        std::string bytes;
        auto started = std::chrono::steady_clock::now();
        for (std::size_t repeat = 0; repeat < repeats; ++repeat)
        {
            bytes.clear();
            converter.write(value.view(), bytes);
        }
        result.encode_us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - started).count() /
                           static_cast<double>(repeats);
        result.bytes = bytes.size();

        started = std::chrono::steady_clock::now();
        for (std::size_t repeat = 0; repeat < repeats; ++repeat)
        {
            BinaryReader reader{bytes, 0, BinaryDecodeLimits{.max_work = 1'000'000'000}};
            const Value  decoded = converter.read(reader);
            REQUIRE(decoded.has_value());
        }
        result.decode_us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - started).count() /
                           static_cast<double>(repeats);
        return result;
    }
}  // namespace

TEST_CASE("binary profiles: every shape round trips under both", "[binary-profiles]")
{
    for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
    {
        for (const Value &value : {int_list(0), int_list(1), int_list(257), float_list(64), row_list(0), row_list(1),
                                   row_list(65), int_float_map(0), int_float_map(33)})
        {
            const auto  converter = bind_binary_converter(value.view().schema(), profile);
            std::string bytes;
            converter.write(value.view(), bytes);
            BinaryReader reader{bytes};
            const Value  decoded = converter.read(reader);
            CHECK(reader.remaining() == 0);
            CHECK(decoded.view() == value.view());
        }
    }
}

namespace
{
    /** Round trip under both profiles; returns the Fast bytes for a test to damage. */
    std::string check_both(const Value &value)
    {
        std::string fast_bytes;
        for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
        {
            const auto  converter = bind_binary_converter(value.view().schema(), profile);
            std::string bytes;
            converter.write(value.view(), bytes);
            BinaryReader reader{bytes};
            const Value  decoded = converter.read(reader);
            CHECK(reader.remaining() == 0);
            CHECK(decoded.view() == value.view());
            if (profile == BinaryProfile::Fast) { fast_bytes = std::move(bytes); }
        }
        return fast_bytes;
    }
}  // namespace

TEST_CASE("binary profiles: a column keeps the rows that lack a field", "[binary-profiles]")
{
    auto &registry = TypeRegistry::instance();
    const auto  row_binding = ValuePlanFactory::instance().type_for(row_schema());

    // Every field missing from some row, the fixed-width ones and the text one
    // alike, plus a row with nothing set and one with everything.
    ListBuilder rows{row_binding};
    for (std::int64_t index = 0; index < 70; ++index)
    {
        BundleBuilder fields{row_binding};
        if (index % 2 == 0) { fields.set("id", Value{Int{index}}); }
        if (index % 3 == 0) { fields.set("price", Value{Float{1.5 * static_cast<double>(index)}}); }
        if (index % 5 != 0) { fields.set("time", Value{MIN_ST + TimeDelta{index}}); }
        if (index % 7 == 0) { fields.set("live", Value{Bool{index % 14 == 0}}); }
        if (index % 4 != 1) { fields.set("symbol", Value{Str{index % 8 == 0 ? std::string{} : std::string(static_cast<std::size_t>(index), 'x')}}); }
        rows.push_back(fields.build().view());
    }
    rows.push_back(BundleBuilder{row_binding}.build().view());
    rows.push_back(row(99).view());
    const Value value = rows.build();
    static_cast<void>(check_both(value));

    // An empty string that is SET is not an unset one.
    const auto  converter = bind_binary_converter(value.view().schema(), BinaryProfile::Fast);
    std::string bytes;
    converter.write(value.view(), bytes);
    BinaryReader reader{bytes};
    const Value  decoded = converter.read(reader);
    const auto   decoded_rows = decoded.view().as_list();
    CHECK(decoded_rows.at(0).as_bundle().at(5).has_value());          // index 0: set, empty
    CHECK(decoded_rows.at(0).as_bundle().at(5).checked_as<Str>().empty());
    CHECK_FALSE(decoded_rows.at(1).as_bundle().at(5).has_value());    // index 1: unset
    CHECK_FALSE(decoded_rows.at(70).as_bundle().at(0).has_value());   // the row with nothing set
    static_cast<void>(registry);
}

TEST_CASE("binary profiles: columns nest, and the shapes with no column form still travel", "[binary-profiles]")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *text = registry.register_scalar<Str>("str");

    // A row holding a list, a nested row and a map: each is a column of its own
    // values, written by that field's converter.
    const auto *inner = registry.un_named_bundle({{"a", integer}, {"b", text}});
    const auto *outer = registry.un_named_bundle({
        {"values", registry.list(integer)},
        {"inner", inner},
        {"lookup", registry.map(text, integer)},
    });
    const auto outer_binding = ValuePlanFactory::instance().type_for(outer);
    ListBuilder nested{outer_binding};
    for (std::int64_t index = 0; index < 9; ++index)
    {
        BundleBuilder fields{outer_binding};
        ListBuilder values{registry.scalar_type<Int>()};
        for (std::int64_t item = 0; item < index; ++item) { values.push_back(Value{Int{item * index}}.view()); }
        fields.set("values", values.build());
        if (index % 2 == 0)
        {
            BundleBuilder inner_fields{ValuePlanFactory::instance().type_for(inner)};
            inner_fields.set("a", Value{Int{index}});
            if (index % 4 == 0) { inner_fields.set("b", Value{Str{"inner"}}); }
            fields.set("inner", inner_fields.build());
        }
        MapBuilder lookup{registry.scalar_type<Str>(), registry.scalar_type<Int>()};
        lookup.set_item(Value{Str{"k" + std::to_string(index)}}.view(), Value{Int{index}}.view());
        fields.set("lookup", lookup.build());
        nested.push_back(fields.build().view());
    }
    static_cast<void>(check_both(nested.build()));

    // Tuples are dense rows: no validity words to keep.
    const auto *pair = registry.tuple({integer, text});
    const auto  pair_binding = ValuePlanFactory::instance().type_for(pair);
    ListBuilder pairs{pair_binding};
    for (std::int64_t index = 0; index < 5; ++index)
    {
        BundleBuilder fields{pair_binding};
        fields.set(0, Value{Int{index}});
        fields.set(1, Value{Str{std::to_string(index)}});
        pairs.push_back(fields.build().view());
    }
    static_cast<void>(check_both(pairs.build()));

    // A nullable list has no block form and keeps the field-wise one.
    ListBuilder nullable{registry.scalar_type<Int>(), *registry.nullable_tuple(integer)};
    nullable.push_back(Int{1});
    nullable.push_back_unset();
    nullable.push_back(Int{3});
    static_cast<void>(check_both(nullable.build()));

    // The other sequences of atoms, and a map that holds an unset value.
    SetBuilder set{registry.scalar_type<Int>()};
    for (std::int64_t index = 0; index < 40; ++index) { (void)set.insert(Value{Int{index * 3}}.view()); }
    static_cast<void>(check_both(set.build()));
    CyclicBufferBuilder ring{registry.scalar_type<Int>(), 4};
    for (std::int64_t index = 0; index < 7; ++index) { ring.push_back(Int{index}); }
    static_cast<void>(check_both(ring.build()));
    QueueBuilder queue{registry.scalar_type<Int>(), 8};
    for (std::int64_t index = 0; index < 5; ++index) { queue.push(Int{index}); }
    static_cast<void>(check_both(queue.build()));
    ListBuilder flags{registry.scalar_type<Bool>()};
    for (std::int64_t index = 0; index < 19; ++index) { flags.push_back(Value{Bool{index % 3 == 0}}.view()); }
    static_cast<void>(check_both(flags.build()));

    MapBuilder holes{registry.scalar_type<Int>(), registry.scalar_type<Float>()};
    for (std::int64_t index = 0; index < 20; ++index)
    {
        const Int key{index};
        if (index % 6 == 0) { holes.set_item_unset(Value{key}.view()); }
        else { holes.set_item(Value{key}.view(), Value{Float{static_cast<double>(index)}}.view()); }
    }
    static_cast<void>(check_both(holes.build()));
}

TEST_CASE("binary profiles: damaged Fast bytes are refused, not guessed", "[binary-profiles]")
{
    auto &registry = TypeRegistry::instance();

    const auto refuse_every_truncation = [](const Value &value) {
        const auto  converter = bind_binary_converter(value.view().schema(), BinaryProfile::Fast);
        std::string bytes;
        converter.write(value.view(), bytes);
        for (std::size_t cut = 0; cut < bytes.size(); ++cut)
        {
            BinaryReader reader{std::string_view{bytes}.substr(0, cut)};
            CHECK_THROWS(converter.read(reader));
        }
    };
    refuse_every_truncation(int_list(9));
    refuse_every_truncation(row_list(3));
    refuse_every_truncation(int_float_map(5));

    // A boolean block is checked as a block.
    ListBuilder flags{registry.scalar_type<Bool>()};
    for (std::int64_t index = 0; index < 4; ++index) { flags.push_back(Value{Bool{true}}.view()); }
    const Value flag_list = flags.build();
    const auto  flag_converter = bind_binary_converter(flag_list.view().schema(), BinaryProfile::Fast);
    std::string flag_bytes;
    flag_converter.write(flag_list.view(), flag_bytes);
    flag_bytes.back() = '\x02';
    BinaryReader flag_reader{flag_bytes};
    CHECK_THROWS(flag_converter.read(flag_reader));

    // A key block that repeats a key is not a map.
    const Value map = int_float_map(3);
    const auto  map_converter = bind_binary_converter(map.view().schema(), BinaryProfile::Fast);
    std::string map_bytes;
    map_converter.write(map.view(), map_bytes);
    // count, then three eight-byte keys: make the second equal the first.
    std::copy_n(map_bytes.begin() + 1, 8, map_bytes.begin() + 9);
    BinaryReader map_reader{map_bytes};
    CHECK_THROWS(map_converter.read(map_reader));

    // An untrusted count is charged before anything is allocated for it.
    std::string bomb;
    write_varint(50'000'000, bomb);
    BinaryReader bomb_reader{bomb};
    CHECK_THROWS(bind_binary_converter(row_list(1).view().schema(), BinaryProfile::Fast).read(bomb_reader));
}

namespace
{
    Value list_of(std::initializer_list<Int> values)
    {
        ListBuilder list{TypeRegistry::instance().scalar_type<Int>()};
        for (const auto value : values) { list.push_back(Value{value}.view()); }
        return list.build();
    }

    std::string compact_bytes(const Value &value)
    {
        std::string bytes;
        bind_binary_converter(value.view().schema(), BinaryProfile::Compact).write(value.view(), bytes);
        return bytes;
    }

    Value compact_round_trip(const Value &value)
    {
        const auto   converter = bind_binary_converter(value.view().schema(), BinaryProfile::Compact);
        const auto   bytes = compact_bytes(value);
        BinaryReader reader{bytes};
        Value        decoded = converter.read(reader);
        CHECK(reader.remaining() == 0);
        return decoded;
    }
}  // namespace

TEST_CASE("binary profiles: Compact picks a column's encoding from the values", "[binary-profiles]")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    // count, encoding byte, then the column. The encodings are the codec's:
    // 0 raw, 1 varint, 2 delta, 3 constant, 4 bits.
    const auto encoding_of = [](const Value &value) { return static_cast<int>(compact_bytes(value).at(1)); };

    const Value steady = list_of({1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007});
    CHECK(encoding_of(steady) == 2);                      // sorted: the steps are small
    CHECK(compact_bytes(steady).size() == 1 + 1 + 2 + 7);   // first value, then seven one-byte steps

    const Value small = list_of({3, -90, 17, 250, -4, 61, 8, -33});
    CHECK(encoding_of(small) == 1);                       // unordered but small: each its own varint

    const Value same = list_of({42, 42, 42, 42, 42});
    CHECK(encoding_of(same) == 3);
    CHECK(compact_bytes(same).size() == 1 + 1 + 8);

    const auto low = std::numeric_limits<Int>::min();
    const auto high = std::numeric_limits<Int>::max();
    // Differences wrap, so the step from the lowest integer to the highest is
    // minus one and even this alternation is a delta column.
    const Value swings = list_of({low, high, low, high, low + 1, high - 1});
    CHECK(encoding_of(swings) == 2);
    const Value wide = list_of({Int{0x5A17C3E9B2D40F61}, Int{-0x3C8E51F7A96B2D04}, Int{0x71B4E02D9C6A38F5},
                                Int{-0x2F93D7C1054BE86A}, Int{0x48D6A1F3E70B5C92}});
    CHECK(encoding_of(wide) == 0);                        // nothing beats eight bytes each

    for (const Value &value : {steady, small, same, swings, wide, list_of({}), list_of({7}), list_of({high, low}),
                               list_of({0, high, -1, low, 1})})
    {
        CHECK(compact_round_trip(value).view() == value.view());
    }

    // A lone integer is a varint; a lone instant is not, because it never is small.
    CHECK(compact_bytes(Value{Int{5}}).size() == 1);
    CHECK(compact_bytes(Value{Int{-5}}).size() == 1);
    CHECK(compact_bytes(Value{TimeDelta{250}}).size() == 2);
    CHECK(compact_bytes(Value{MIN_ST + TimeDelta{1}}).size() == 8);
    for (const Value &value : {Value{Int{0}}, Value{low}, Value{high}, Value{TimeDelta{-1}}, Value{MIN_ST}, Value{MAX_DT}})
    {
        CHECK(compact_round_trip(value).view() == value.view());
    }

    // Booleans pack eight to the byte.
    ListBuilder flags{TypeRegistry::instance().scalar_type<Bool>()};
    for (std::int64_t index = 0; index < 20; ++index) { flags.push_back(Value{Bool{index % 3 == 0}}.view()); }
    const Value flag_list = flags.build();
    CHECK(encoding_of(flag_list) == 4);
    CHECK(compact_bytes(flag_list).size() == 1 + 1 + 3);
    CHECK(compact_round_trip(flag_list).view() == flag_list.view());

    // Instants in a column are what delta encoding is for.
    ListBuilder times{TypeRegistry::instance().scalar_type<DateTime>()};
    for (std::int64_t index = 0; index < 100; ++index) { times.push_back(Value{MIN_ST + TimeDelta{index * 1'000'000}}.view()); }
    const Value time_list = times.build();
    CHECK(encoding_of(time_list) == 2);
    CHECK(compact_bytes(time_list).size() < 100 * 8 / 2);
    CHECK(compact_round_trip(time_list).view() == time_list.view());
}

TEST_CASE("binary profiles: Compact writes repeated text once", "[binary-profiles]")
{
    auto &registry = TypeRegistry::instance();
    const auto *text = registry.register_scalar<Str>("str");
    const auto *one = registry.un_named_bundle({{"symbol", text}});
    const auto  binding = ValuePlanFactory::instance().type_for(one);

    const auto rows_of = [&](std::size_t count, std::size_t distinct) {
        ListBuilder rows{binding};
        for (std::size_t index = 0; index < count; ++index)
        {
            BundleBuilder fields{binding};
            fields.set("symbol", Value{Str{"a-rather-long-symbol-name-" + std::to_string(index % distinct)}});
            rows.push_back(fields.build().view());
        }
        return rows.build();
    };
    const Value repeated = rows_of(400, 5);
    const Value unique = rows_of(400, 400);
    // Five names and four hundred one-byte indices, against four hundred names.
    CHECK(compact_bytes(repeated).size() < compact_bytes(unique).size() / 10);
    CHECK(compact_round_trip(repeated).view() == repeated.view());
    CHECK(compact_round_trip(unique).view() == unique.view());
    // Too few rows to be worth a dictionary.
    const Value few = rows_of(3, 1);
    CHECK(compact_round_trip(few).view() == few.view());
}

TEST_CASE("binary profiles: an enum is its assigned integer", "[binary-profiles]")
{
    auto &registry = TypeRegistry::instance();
    const auto *side = registry.enum_type("BinaryProfilesSide", {{"Buy", 1}, {"Sell", -1}, {"Far", 1'000'000}});
    const auto  binding = ValuePlanFactory::instance().type_for(side);
    for (const Int assigned : {Int{1}, Int{-1}, Int{1'000'000}})
    {
        Value value{binding};
        std::memcpy(const_cast<void *>(value.view().data()), &assigned, sizeof(assigned));
        static_cast<void>(check_both(value));
        CHECK(compact_bytes(value).size() <= 3);
    }
}

TEST_CASE("binary profiles: damaged Compact bytes are refused, not guessed", "[binary-profiles]")
{
    const auto refuse_every_truncation = [](const Value &value) {
        const auto converter = bind_binary_converter(value.view().schema(), BinaryProfile::Compact);
        const auto bytes = compact_bytes(value);
        for (std::size_t cut = 0; cut < bytes.size(); ++cut)
        {
            BinaryReader reader{std::string_view{bytes}.substr(0, cut)};
            CHECK_THROWS(converter.read(reader));
        }
    };
    refuse_every_truncation(int_list(9));
    refuse_every_truncation(row_list(12));
    refuse_every_truncation(int_float_map(5));

    // An encoding that is not one for the atom: bit-packing is for booleans,
    // varints for integers.
    const Value floats = float_list(4);
    auto        float_bytes = compact_bytes(floats);
    float_bytes[1] = '\x01';
    BinaryReader float_reader{float_bytes};
    CHECK_THROWS(bind_binary_converter(floats.view().schema(), BinaryProfile::Compact).read(float_reader));

    const Value ints = list_of({1, 2, 3});
    auto        int_bytes = compact_bytes(ints);
    int_bytes[1] = '\x04';
    BinaryReader int_reader{int_bytes};
    CHECK_THROWS(bind_binary_converter(ints.view().schema(), BinaryProfile::Compact).read(int_reader));
    int_bytes[1] = '\x63';
    BinaryReader unknown_reader{int_bytes};
    CHECK_THROWS(bind_binary_converter(ints.view().schema(), BinaryProfile::Compact).read(unknown_reader));
}

TEST_CASE("binary profiles: revision 0 is still bound and read, and a later one is refused", "[binary-profiles]")
{
    const Value value = row_list(7);
    const auto *schema = value.view().schema();

    // No profile named means the RFC 0017 bytes, and they never move.
    const auto legacy = bind_binary_converter(schema);
    CHECK(legacy.profile() == BinaryProfile::Compact);
    CHECK(legacy.revision() == 0);
    std::string legacy_bytes;
    legacy.write(value.view(), legacy_bytes);
    CHECK(legacy_bytes == to_binary_string(value.view()));
    CHECK(from_binary_string(schema, legacy_bytes).view() == value.view());

    // A reader of stored bytes names the revision it was told.
    const auto stated = bind_binary_converter(schema, BinaryProfile::Compact, 0);
    BinaryReader reader{legacy_bytes};
    CHECK(stated.read(reader).view() == value.view());

    const auto current = bind_binary_converter(schema, BinaryProfile::Compact);
    CHECK(current.revision() == binary_profile_revision(BinaryProfile::Compact));
    CHECK(current.revision() >= 1);
    CHECK(compact_bytes(value) != legacy_bytes);

    CHECK_THROWS(bind_binary_converter(schema, BinaryProfile::Compact,
                                       static_cast<std::uint8_t>(binary_profile_revision(BinaryProfile::Compact) + 1)));
}

TEST_CASE("binary profiles: a run of keys is a column where the profile has one", "[binary-profiles]")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<Str>("str");
    std::vector<Value> keys;
    for (std::int64_t key = 0; key < 50; ++key) { keys.emplace_back(Int{key * 10 + 3}); }
    std::vector<Value> names;
    for (std::int64_t key = 0; key < 6; ++key) { names.emplace_back(Str{"k" + std::to_string(key)}); }

    const auto round_trip = [](const std::vector<Value> &values, const BoundBinaryConverter &converter) {
        std::string  bytes;
        BinaryWriter writer{bytes};
        converter.write_run(values, writer);
        BinaryReader       reader{bytes};
        std::vector<Value> decoded;
        converter.read_run(values.size(), reader, decoded);
        CHECK(reader.remaining() == 0);
        REQUIRE(decoded.size() == values.size());
        for (std::size_t index = 0; index < values.size(); ++index) { CHECK(decoded[index].view() == values[index].view()); }
        return bytes.size();
    };

    const auto *integer = keys.front().view().schema();
    const auto field_wise = round_trip(keys, bind_binary_converter(integer));
    const auto compact = round_trip(keys, bind_binary_converter(integer, BinaryProfile::Compact));
    const auto fast = round_trip(keys, bind_binary_converter(integer, BinaryProfile::Fast));
    CHECK(field_wise == 50 * 8);
    CHECK(fast == 50 * 8);
    CHECK(compact == 1 + 1 + 49);   // encoding, first key, forty-nine one-byte steps

    // Anything that is not a fixed-width atom is each value in turn.
    const auto *name = names.front().view().schema();
    CHECK(round_trip(names, bind_binary_converter(name, BinaryProfile::Compact)) ==
          round_trip(names, bind_binary_converter(name)));
    CHECK(round_trip({}, bind_binary_converter(integer, BinaryProfile::Compact)) == 0);
}

TEST_CASE("binary profiles: what each costs", "[.][codec-benchmark]")
{
    struct Case
    {
        const char *name;
        Value       value;
        std::size_t repeats;
    };
    Case cases[] = {
        {"list<int> x 1,000,000", int_list(1'000'000), 5},
        {"list<float> x 1,000,000", float_list(1'000'000), 5},
        {"list<row6> x 100,000", row_list(100'000), 5},
        {"map<int,float> x 100,000", int_float_map(100'000), 5},
    };
    std::printf("%-26s %-10s %12s %12s %12s\n", "shape", "form", "encode_ms", "decode_ms", "bytes");
    for (const auto &entry : cases)
    {
        // Field-wise is the RFC 0017 encoding, revision 0: what both profiles
        // are measured against.
        const auto *schema = entry.value.view().schema();
        const std::pair<const char *, BoundBinaryConverter> forms[] = {
            {"field-wise", bind_binary_converter(schema)},
            {"Compact", bind_binary_converter(schema, BinaryProfile::Compact)},
            {"Fast", bind_binary_converter(schema, BinaryProfile::Fast)},
        };
        for (const auto &[name, converter] : forms)
        {
            const auto measured = measure(entry.value, converter, entry.repeats);
            std::printf("%-26s %-10s %12.3f %12.3f %12zu\n", entry.name, name, measured.encode_us / 1000.0,
                        measured.decode_us / 1000.0, measured.bytes);
        }
    }
}
