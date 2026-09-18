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
#include <string>

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

    Measured measure(const Value &value, BinaryProfile profile, std::size_t repeats)
    {
        const auto converter = bind_binary_converter(value.view().schema(), profile);
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
    std::printf("%-26s %-8s %12s %12s %12s\n", "shape", "profile", "encode_ms", "decode_ms", "bytes");
    for (const auto &entry : cases)
    {
        for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
        {
            const auto measured = measure(entry.value, profile, entry.repeats);
            std::printf("%-26s %-8s %12.3f %12.3f %12zu\n", entry.name,
                        profile == BinaryProfile::Fast ? "Fast" : "Compact", measured.encode_us / 1000.0,
                        measured.decode_us / 1000.0, measured.bytes);
        }
    }
}
