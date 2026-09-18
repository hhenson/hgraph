// A scalar's wire form comes from its registration (RFC 0040, stage 4).
//
// The codec builds in the atoms hgraph defines. Every other scalar either says
// how it travels, declares that its storage image is its wire form, or cannot
// be wired into anything that needs one -- and the refusal names the scalar,
// what needed it, and how to fix it.

#include <hgraph/runtime/logger.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/binary_session.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <spdlog/sinks/ostream_sink.h>

#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>

namespace
{
    using namespace hgraph;
    using Catch::Matchers::ContainsSubstring;

    // Not trivially copyable, so it has no storage image to fall back on.
    struct AtomQuote
    {
        std::string symbol{};
        double      price{0.0};

        friend bool operator==(const AtomQuote &, const AtomQuote &) = default;
    };

    // Trivially copyable but not plain numeric storage: two fields.
    struct AtomPoint
    {
        std::int32_t x{0};
        std::int32_t y{0};

        friend bool operator==(const AtomPoint &, const AtomPoint &) = default;
    };

    // Never given a wire form by any test.
    struct AtomOrphan
    {
        std::string text{};

        friend bool operator==(const AtomOrphan &, const AtomOrphan &) = default;
    };

    void write_quote(const void *value, const void *, std::string &out)
    {
        const auto &quote = *static_cast<const AtomQuote *>(value);
        write_varint(quote.symbol.size(), out);
        out.append(quote.symbol);
        char price[sizeof(double)];
        std::memcpy(price, &quote.price, sizeof(price));
        out.append(price, sizeof(price));
    }

    void read_quote(void *value, const void *, BinaryReader &reader)
    {
        auto      &quote = *static_cast<AtomQuote *>(value);
        const auto size = static_cast<std::size_t>(read_varint(reader));
        quote.symbol.assign(reinterpret_cast<const char *>(reader.take(size)), size);
        std::memcpy(&quote.price, reader.take(sizeof(double)), sizeof(double));
    }

    // A reader that stops a byte early: a bug in somebody's registration.
    void read_quote_short(void *value, const void *, BinaryReader &reader)
    {
        auto      &quote = *static_cast<AtomQuote *>(value);
        const auto size = static_cast<std::size_t>(read_varint(reader));
        quote.symbol.assign(reinterpret_cast<const char *>(reader.take(size)), size);
        static_cast<void>(reader.take(sizeof(double) - 1));
    }

    Value round_trip(const Value &value, BinaryProfile profile)
    {
        const auto   converter = bind_binary_converter(value.view().schema(), profile);
        std::string  bytes;
        converter.write(value.view(), bytes);
        BinaryReader reader{bytes};
        Value        decoded = converter.read(reader);
        CHECK(reader.remaining() == 0);
        return decoded;
    }
}  // namespace

TEST_CASE("binary atoms: a scalar with no wire form is refused by name, with what needed it and the fix")
{
    auto       &registry = TypeRegistry::instance();
    const auto *orphan = registry.register_scalar<AtomOrphan>("tests.binary_atoms.Orphan");
    const auto *rows = registry.list(registry.un_named_bundle({{"id", registry.register_scalar<Int>("int")}, {"note", orphan}}));

    // Found when the converter is bound -- wiring time -- not at the first value.
    CHECK_THROWS_AS(bind_binary_converter(orphan, BinaryProfile::Compact), BinaryWireFormError);
    CHECK_THROWS_AS(bind_binary_converter(rows, BinaryProfile::Fast), BinaryWireFormError);
    try
    {
        static_cast<void>(bind_binary_converter(rows, BinaryProfile::Fast));
        FAIL("a scalar with no wire form was bound");
    }
    catch (const BinaryWireFormError &error)
    {
        CHECK(error.scalar() == "tests.binary_atoms.Orphan");
        CHECK_THAT(error.what(), ContainsSubstring("scalar 'tests.binary_atoms.Orphan' has no wire form"));
        CHECK_THAT(error.what(), ContainsSubstring("register_binary_atom"));
        CHECK_THAT(error.what(), ContainsSubstring("declare_portable_binary_atom"));
    }

    // What is being built says so, and the message then carries it.
    try
    {
        static_cast<void>(bind_binary_converter_for(rows, BinaryProfile::Fast, "dmap_ boundary slot 'notes'"));
        FAIL("a scalar with no wire form was bound");
    }
    catch (const BinaryWireFormError &error)
    {
        CHECK_THAT(error.what(), ContainsSubstring("required by dmap_ boundary slot 'notes'"));
        CHECK_THAT(error.what(), ContainsSubstring(std::string{rows->name()}));
        CHECK(error.scalar() == "tests.binary_atoms.Orphan");
    }
    CHECK_NOTHROW(bind_binary_converter_for(registry.register_scalar<Int>("int"), BinaryProfile::Fast, "anything"));
}

TEST_CASE("binary atoms: a registered wire form makes a scalar part of the format")
{
    auto       &registry = TypeRegistry::instance();
    const auto *quote = registry.register_scalar<AtomQuote>("tests.binary_atoms.Quote");
    register_binary_atom(quote, BinaryAtomOps{.write = &write_quote, .read = &read_quote});

    const Value one{AtomQuote{"VOD.L", 72.5}};
    for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
    {
        CHECK(round_trip(one, profile).view().checked_as<AtomQuote>() == AtomQuote{"VOD.L", 72.5});
    }

    // Wherever a value can go: a field of rows written by column, a map's
    // value, and inside an Any.
    const auto *row = registry.un_named_bundle({{"id", registry.register_scalar<Int>("int")}, {"quote", quote}});
    const auto  row_binding = ValuePlanFactory::instance().type_for(row);
    ListBuilder rows{row_binding};
    for (std::int64_t index = 0; index < 12; ++index)
    {
        BundleBuilder fields{row_binding};
        fields.set("id", Value{Int{index}});
        if (index % 3 != 0) { fields.set("quote", Value{AtomQuote{"S" + std::to_string(index), 1.5 * static_cast<double>(index)}}); }
        rows.push_back(fields.build().view());
    }
    const Value row_list = rows.build();
    for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
    {
        CHECK(round_trip(row_list, profile).view() == row_list.view());
    }

    Value boxed{ValuePlanFactory::instance().type_for(registry.any())};
    boxed.as_any().begin_mutation().set(one.view());
    CHECK(decode_binary_frame(boxed.view().schema(), encode_binary_frame(boxed.view())).view() == boxed.view());

    // The codec frames the form with its length, so a registration that reads
    // too little is caught at that value.
    register_binary_atom(quote, BinaryAtomOps{.write = &write_quote, .read = &read_quote_short});
    const auto   broken = bind_binary_converter(quote, BinaryProfile::Compact);
    std::string  bytes;
    broken.write(one.view(), bytes);
    BinaryReader reader{bytes};
    CHECK_THROWS_WITH(broken.read(reader), ContainsSubstring("left 1 bytes unread"));
    register_binary_atom(quote, BinaryAtomOps{.write = &write_quote, .read = &read_quote});

    CHECK_THROWS(register_binary_atom(quote, BinaryAtomOps{.write = &write_quote}));
    CHECK_THROWS(register_binary_atom(row, BinaryAtomOps{.write = &write_quote, .read = &read_quote}));
}

TEST_CASE("binary atoms: a portable scalar's storage image is its wire form")
{
    auto       &registry = TypeRegistry::instance();
    const auto *point = registry.register_scalar<AtomPoint>("tests.binary_atoms.Point");

    // Trivially copyable is not enough: a handle or a pointer is that too.
    CHECK_THROWS_AS(bind_binary_converter(point, BinaryProfile::Fast), BinaryWireFormError);

    declare_portable_binary_atom(point);
    const Value value{AtomPoint{3, -4}};
    std::string bytes;
    bind_binary_converter(point, BinaryProfile::Fast).write(value.view(), bytes);
    CHECK(bytes.size() == sizeof(AtomPoint));
    for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
    {
        CHECK(round_trip(value, profile).view().checked_as<AtomPoint>() == AtomPoint{3, -4});
    }

    // Being fixed width, a run of them is a block.
    ListBuilder points{registry.scalar_type<AtomPoint>()};
    for (std::int32_t index = 0; index < 9; ++index) { points.push_back(Value{AtomPoint{index, -index}}.view()); }
    const Value list = points.build();
    for (const auto profile : {BinaryProfile::Compact, BinaryProfile::Fast})
    {
        CHECK(round_trip(list, profile).view() == list.view());
    }

    // A type that is not trivially copyable has no image to declare.
    CHECK_THROWS_WITH(declare_portable_binary_atom(registry.register_scalar<AtomOrphan>("tests.binary_atoms.Orphan")),
                      ContainsSubstring("not trivially copyable"));
}

TEST_CASE("binary atoms: binding a schema that will pickle warns, once")
{
    auto       &registry = TypeRegistry::instance();
    const auto *quote = registry.register_scalar<AtomQuote>("tests.binary_atoms.OpaqueQuote");
    // An opaque form is what the Python bridge registers for an object that
    // has no schema: it works, and the author should know it is happening.
    register_binary_atom(quote, BinaryAtomOps{.write = &write_quote, .read = &read_quote, .opaque = true});
    const auto *holder = registry.un_named_bundle({{"payload", quote}});

    std::ostringstream captured;
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
    log::set_logger(std::make_shared<spdlog::logger>("binary-atoms-test", sink));
    const auto restore = make_scope_exit([]() noexcept { log::set_logger(nullptr); });

    static_cast<void>(bind_binary_converter(holder, BinaryProfile::Compact));
    log::logger().flush();
    const std::string first = captured.str();
    CHECK_THAT(first, ContainsSubstring("which are pickled"));
    CHECK_THAT(first, ContainsSubstring("tests.binary_atoms.OpaqueQuote"));
    CHECK_THAT(first, ContainsSubstring(std::string{holder->name()}));

    // Once per schema: a second bind of the same one is silent...
    static_cast<void>(bind_binary_converter(holder, BinaryProfile::Fast));
    log::logger().flush();
    CHECK(captured.str() == first);
    // ...and it still round trips, because pickling is never an error.
    BundleBuilder fields{ValuePlanFactory::instance().type_for(holder)};
    fields.set("payload", Value{AtomQuote{"X", 1.0}});
    const Value value = fields.build();
    CHECK(round_trip(value, BinaryProfile::Compact).view() == value.view());
}
