// RFC 0030: the typed value store and its pluggable codecs.
#include <hgraph/persistence/object_store.h>
#include <hgraph/persistence/value_codec.h>
#include <hgraph/persistence/value_store.h>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#if !defined(_WIN32)
#include <unistd.h>
#endif

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
            "test-reversed", "rev", nullptr,
            ValueCodecOps{.encode = &reversing_encode, .decode = &reversing_decode});
    }

    [[nodiscard]] std::string as_text(std::span<const std::byte> bytes)
    {
        std::string text;
        text.reserve(bytes.size());
        for (const auto byte : bytes)
        {
            text.push_back(std::to_integer<char>(byte));
        }
        return text;
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

TEST_CASE("value store: a stored json object is a json document and nothing else")
{
    // The point of the format is that something outside this codebase can read
    // it: a text editor, jq, json.load. Any framing of ours would break that,
    // so the stored bytes must equal the codec's output exactly.
    const auto  store = memory_store();
    const Value written = record_value("alpha", 7, "BOM");

    const ObjectBytes encoded = store.encode(written.view());
    const std::string text = as_text(encoded);

    CHECK(text == to_json_string(written.view()));
    CHECK(text.front() == '{');
    CHECK(text.back() == '}');
    CHECK(text.find("alpha") != std::string::npos);
}

TEST_CASE("value store: json is the default codec and names the object .json")
{
    const auto store = memory_store();
    CHECK(store.default_codec() == std::string{JSON_VALUE_CODEC});
    CHECK(store.resolve_key("records/alpha") == "records/alpha.json");
    CHECK(codec_for_key("records/alpha.json") == std::string{JSON_VALUE_CODEC});
    CHECK_FALSE(codec_for_key("records/alpha").has_value());
    // A dot in a directory name is not a format.
    CHECK_FALSE(codec_for_key("records.v2/alpha").has_value());
}

TEST_CASE("value store: the default is configurable and a call or key can override it")
{
    register_reversing_codec();

    const auto reversed_default = memory_store("test-reversed");
    CHECK(reversed_default.default_codec() == "test-reversed");
    CHECK(reversed_default.resolve_key("records/a") == "records/a.rev");

    // A per-call override wins over the store default, in both directions.
    CHECK(reversed_default.resolve_key("records/a", JSON_VALUE_CODEC) == "records/a.json");
    const auto json_default = memory_store();
    CHECK(json_default.resolve_key("records/a", "test-reversed") == "records/a.rev");

    // An extension already on the key is the most explicit statement and wins
    // over both.
    CHECK(json_default.resolve_key("records/a.rev") == "records/a.rev");
    CHECK(json_default.resolve_key("records/a.rev", JSON_VALUE_CODEC) == "records/a.rev");
}

TEST_CASE("value store: a read resolves the codec from the stored key")
{
    register_reversing_codec();
    // One store, two codecs, no out-of-band agreement. The key carries the
    // format, so the reader does not need to be told which was used.
    const auto  store = memory_store();
    const Value first = record_value("alpha", 1, "BOM");
    const Value second = record_value("beta", 2, "FRONT");

    store.write("records/first", first.view());
    store.write("records/second", second.view(), "test-reversed");

    CHECK(store.read("records/first", record_meta()).view().equals(first.view()));
    // Found by listing, because the caller asks for the logical key and the
    // object was written under a non-default codec.
    CHECK(store.read("records/second", record_meta()).view().equals(second.view()));
    // And directly, when the caller names the encoded key.
    CHECK(store.read("records/second.rev", record_meta()).view().equals(second.view()));
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

TEST_CASE("value store: an unregistered extension is not treated as a format")
{
    register_builtin_value_codecs();
    // A key ending in something we do not know is not silently decoded with the
    // default; it simply does not name a codec, so the object is written under
    // the default extension instead of being mistaken for a foreign format.
    CHECK_FALSE(codec_for_key("records/alpha.bin").has_value());

    const auto store = memory_store();
    CHECK(store.resolve_key("records/alpha.bin") == "records/alpha.bin.json");
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
    CHECK_THROWS_AS(register_value_codec(JSON_VALUE_CODEC, "json", nullptr,
                                         ValueCodecOps{.encode = &reversing_encode,
                                                       .decode = &reversing_decode}),
                    std::invalid_argument);

    // Two codecs cannot claim one extension: a stored object would be
    // ambiguous, which the key-based scheme cannot tolerate.
    CHECK_THROWS_AS(register_value_codec("another-json", "json", nullptr,
                                         ValueCodecOps{.encode = &reversing_encode,
                                                       .decode = &reversing_decode}),
                    std::invalid_argument);
}


TEST_CASE("value store: a local-backend object is a json file on disk")
{
    // The requirement in plain terms: open the file in a text editor and see
    // json. Asserting on encoded bytes is a proxy; this reads the file back
    // through the filesystem with no hgraph code in the path.
    register_builtin_value_codecs();
    const auto root = std::filesystem::temp_directory_path() /
                      ("hgraph_value_store_" + std::to_string(::getpid()));
    std::filesystem::create_directories(root);

    const auto store = make_value_store(ValueStoreConfig{
        .objects = make_object_store(ObjectStoreConfig{LocalLocation{root.string()}})});

    const Value written = record_value("alpha", 7, "BOM");
    store.write("records/alpha", written.view());

    const auto path = root / "records" / "alpha.json";
    REQUIRE(std::filesystem::exists(path));

    std::ifstream file{path};
    REQUIRE(file.is_open());
    const std::string contents{std::istreambuf_iterator<char>{file},
                               std::istreambuf_iterator<char>{}};

    CHECK(contents == to_json_string(written.view()));
    CHECK(contents.starts_with("{"));
    CHECK(contents.ends_with("}"));
    CHECK(contents.find("\"name\"") != std::string::npos);

    std::filesystem::remove_all(root);
}
