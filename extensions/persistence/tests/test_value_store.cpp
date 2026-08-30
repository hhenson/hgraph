// RFC 0030: the typed value store and its pluggable codecs.
#include <hgraph/persistence/object_store.h>
#include <hgraph/persistence/value_codec.h>
#include <hgraph/persistence/value_store.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <span>
#include <string>
#include <string_view>

using namespace hgraph;
using namespace hgraph::persistence::store;

namespace
{
    using Inner = Bundle<"tests.value_store::Inner", Field<"symbol", Str>>;
    using Record = Bundle<"tests.value_store::Record", Field<"name", Str>,
                          Field<"count", Int>, Field<"inner", Inner>>;

    [[nodiscard]] const ValueTypeMetaData *record_meta()
    {
        return scalar_descriptor<Record>::value_meta();
    }

    [[nodiscard]] Value record_value(std::string_view name, Int count,
                                     std::string_view inner_symbol)
    {
        BundleBuilder inner{ValuePlanFactory::instance().type_for(
            scalar_descriptor<Inner>::value_meta())};
        inner.set("symbol", Value{Str{inner_symbol}}.view());

        BundleBuilder builder{ValuePlanFactory::instance().type_for(record_meta())};
        builder.set("name", Value{Str{name}}.view());
        builder.set("count", Value{count}.view());
        builder.set("inner", inner.build().view());
        return builder.build();
    }

    [[nodiscard]] ValueStore memory_store(std::string codec = {})
    {
        register_builtin_value_codecs();
        return make_value_store(
            ValueStoreConfig{.objects = make_object_store(ObjectStoreConfig{}),
                             .codec   = std::move(codec)});
    }

    /** A second codec, so "pluggable" is exercised rather than asserted. It
        stores the JSON form reversed, which is enough to prove the store
        dispatches on the recorded name rather than assuming a format. */
    void reversing_encode(void *, const ValueView &value, ObjectBytes &out)
    {
        const std::string text = to_json_string(value);
        for (auto character = text.rbegin(); character != text.rend(); ++character)
        {
            out.push_back(static_cast<std::byte>(*character));
        }
    }

    Value reversing_decode(void *, const ValueTypeMetaData *schema,
                           std::span<const std::byte> encoded)
    {
        std::string text;
        text.reserve(encoded.size());
        for (auto byte = encoded.rbegin(); byte != encoded.rend(); ++byte)
        {
            text.push_back(std::to_integer<char>(*byte));
        }
        return from_json_string(schema, text);
    }

    void register_reversing_codec()
    {
        register_value_codec(
            "test-reversed", nullptr,
            ValueCodecOps{.encode = &reversing_encode, .decode = &reversing_decode});
    }
}  // namespace

TEST_CASE("value store: a declared struct round trips without extension codec code")
{
    const auto store = memory_store();
    const Value written = record_value("alpha", 7, "BOM");

    store.write("records/alpha", written.view());
    const Value read = store.read("records/alpha", record_meta());

    CHECK(read.view().equals(written.view()));
}

TEST_CASE("value store: json is the default codec")
{
    const auto store = memory_store();
    CHECK(store.default_codec() == std::string{JSON_VALUE_CODEC});

    const ObjectBytes encoded = store.encode(record_value("a", 1, "X").view());
    CHECK(value_envelope_codec(encoded) == std::string{JSON_VALUE_CODEC});
}

TEST_CASE("value store: the default is configurable and a call can override it")
{
    register_reversing_codec();

    const auto reversed_default = memory_store("test-reversed");
    CHECK(reversed_default.default_codec() == "test-reversed");
    CHECK(value_envelope_codec(reversed_default.encode(record_value("a", 1, "X").view())) ==
          "test-reversed");

    // A per-call override wins over the store default, in both directions.
    CHECK(value_envelope_codec(reversed_default.encode(record_value("a", 1, "X").view(),
                                                       JSON_VALUE_CODEC)) ==
          std::string{JSON_VALUE_CODEC});

    const auto json_default = memory_store();
    CHECK(value_envelope_codec(
              json_default.encode(record_value("a", 1, "X").view(), "test-reversed")) ==
          "test-reversed");
}

TEST_CASE("value store: a read resolves the codec the object was written with")
{
    register_reversing_codec();
    // One store, two codecs, no out-of-band agreement: this is the case the
    // envelope exists for.
    const auto  store = memory_store();
    const Value first = record_value("alpha", 1, "BOM");
    const Value second = record_value("beta", 2, "FRONT");

    store.write("records/first", first.view());
    store.write("records/second", second.view(), "test-reversed");

    CHECK(store.read("records/first", record_meta()).view().equals(first.view()));
    CHECK(store.read("records/second", record_meta()).view().equals(second.view()));
}

TEST_CASE("value store: an unknown codec fails closed rather than guessing")
{
    const auto store = memory_store();
    CHECK_THROWS_AS(store.encode(record_value("a", 1, "X").view(), "no-such-codec"),
                    std::invalid_argument);

    // A misspelled default is rejected where it is configured, not on first use.
    CHECK_THROWS_AS(make_value_store(ValueStoreConfig{
                        .objects = make_object_store(ObjectStoreConfig{}),
                        .codec   = "no-such-codec"}),
                    std::invalid_argument);
}

TEST_CASE("value store: bytes without the envelope are recognised, not mis-parsed")
{
    register_builtin_value_codecs();
    // A legacy object predating RFC 0030 must be distinguishable so a migration
    // can read both formats. The magic check is how that is done.
    const ObjectBytes legacy{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
    CHECK_FALSE(value_envelope_codec(legacy).has_value());

    const auto store = memory_store();
    CHECK_THROWS_AS(store.decode(record_meta(), legacy), std::invalid_argument);
}

TEST_CASE("value store: missing keys are absent rather than an error for try_read")
{
    const auto store = memory_store();
    CHECK_FALSE(store.try_read("records/absent", record_meta()).has_value());
    CHECK_THROWS_AS(store.read("records/absent", record_meta()), ObjectStoreError);
}

TEST_CASE("value store: compare_exchange forwards the store's version token")
{
    const auto store = memory_store();
    const Value first = record_value("alpha", 1, "BOM");
    const Value second = record_value("alpha", 2, "BOM");

    const auto created = store.compare_exchange("records/cas", first.view(), std::nullopt);
    REQUIRE(created.exchanged);

    // A stale expectation loses, and the winner's value is unchanged.
    const auto stale = store.compare_exchange("records/cas", second.view(), "not-the-token");
    CHECK_FALSE(stale.exchanged);
    CHECK(store.read("records/cas", record_meta()).view().equals(first.view()));
}

TEST_CASE("value codec registry: names are listed and re-registration is idempotent")
{
    register_builtin_value_codecs();
    register_builtin_value_codecs();  // idempotent

    CHECK(value_codec_registered(JSON_VALUE_CODEC));
    CHECK_FALSE(value_codec_registered("no-such-codec"));

    const auto names = value_codec_names();
    CHECK(std::find(names.begin(), names.end(), std::string{JSON_VALUE_CODEC}) != names.end());

    // Two implementations under one name is a build error, not last-writer-wins.
    CHECK_THROWS_AS(register_value_codec(JSON_VALUE_CODEC, nullptr,
                                         ValueCodecOps{.encode = &reversing_encode,
                                                       .decode = &reversing_decode}),
                    std::invalid_argument);
}
