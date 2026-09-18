// RFC 0030: the typed value store and its pluggable codecs.
#include <hgraph/persistence/object_store.h>
#include <hgraph/persistence/value_codec.h>
#include <hgraph/persistence/value_store.h>

#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

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
    ValueCodecBinding reversing_bind(void *, const ValueTypeMetaData *schema)
    {
        auto binding = std::make_shared<BoundJsonConverter>(
            bind_json_converter(schema));
        return ValueCodecBinding{binding, binding.get()};
    }

    void reversing_encode(void *, const void *bound, const ValueView &value,
                          ObjectBytes &out)
    {
        std::string text;
        static_cast<const BoundJsonConverter *>(bound)->write(value, text);
        for (auto character = text.rbegin(); character != text.rend(); ++character)
        {
            out.push_back(static_cast<std::byte>(*character));
        }
    }

    Value reversing_decode(void *, const void *bound,
                           std::span<const std::byte> encoded)
    {
        std::string text;
        text.reserve(encoded.size());
        for (auto byte = encoded.rbegin(); byte != encoded.rend(); ++byte)
        {
            text.push_back(std::to_integer<char>(*byte));
        }
        return from_json_string(
            *static_cast<const BoundJsonConverter *>(bound), text);
    }

    Value wrong_schema_decode(void *, const void *, std::span<const std::byte>)
    {
        return Value{Str{"wrong schema"}};
    }

    struct OwnedBindingPlan
    {
        BoundJsonConverter converter{};
        std::shared_ptr<int> lifetime{};
    };

    struct OwnedBindingContext
    {
        std::weak_ptr<int> last_binding{};
    };

    ValueCodecBinding owned_binding_bind(void *raw_context,
                                         const ValueTypeMetaData *schema)
    {
        auto token = std::make_shared<int>(1);
        static_cast<OwnedBindingContext *>(raw_context)->last_binding = token;
        auto plan = std::make_shared<OwnedBindingPlan>(
            OwnedBindingPlan{bind_json_converter(schema), std::move(token)});
        return ValueCodecBinding{plan, &plan->converter};
    }

    void register_reversing_codec()
    {
        register_value_codec(
            "test-reversed", nullptr,
            ValueCodecOps{.bind = &reversing_bind,
                          .encode = &reversing_encode,
                          .decode = &reversing_decode});
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

    REQUIRE(store.write("records/alpha", written.view()).status ==
            ImmutableWriteStatus::Created);
    const Value read = store.read("records/alpha", record_meta());

    CHECK(read.view().equals(written.view()));
}

TEST_CASE("value store: a stored json object is a json document and nothing else")
{
    // A store that is MEANT to hold JSON names the json codec (RFC 0040: JSON
    // is a representation, never a default). The point of the format is that
    // something outside this codebase can read it: a text editor, jq,
    // json.load. Any framing of ours would break that, so the stored bytes
    // must equal the codec's output exactly.
    const auto  store = memory_store(std::string{JSON_VALUE_CODEC});
    const Value written = record_value("alpha", 7, "BOM");

    const ObjectBytes encoded = store.encode(written.view());
    const std::string text = as_text(encoded);

    CHECK(text == to_json_string(written.view()));
    CHECK(text.front() == '{');
    CHECK(text.back() == '}');
    CHECK(text.find("alpha") != std::string::npos);
}

TEST_CASE("value store: binary is the default codec, and its objects are not text")
{
    // RFC 0040: what a store holds unless it is told otherwise is the binary
    // value codec's Compact profile in a compression block. JSON is for a
    // store that is meant to hold JSON, and is never what a store falls back to.
    const auto store = memory_store();
    CHECK(store.default_codec() == std::string{BINARY_VALUE_CODEC});
    CHECK(std::string{DEFAULT_VALUE_CODEC} == std::string{BINARY_VALUE_CODEC});

    const Value       written = record_value("alpha", 7, "BOM");
    const ObjectBytes encoded = store.encode(written.view());
    REQUIRE_FALSE(encoded.empty());
    // A compression block begins with its codec byte, 0 to 2; JSON text cannot.
    CHECK(std::to_integer<unsigned>(encoded.front()) <= 2);
    CHECK(as_text(encoded) != to_json_string(written.view()));
    CHECK(store.decode(record_meta(), encoded).view() == written.view());

    // binary-fast is the Fast profile as it is: a frame, never compressed.
    const ObjectBytes fast = store.encode(written.view(), BINARY_FAST_VALUE_CODEC);
    CHECK(std::to_integer<unsigned>(fast.front()) == 1);   // the frame's profile byte: Fast
    CHECK(store.decode(record_meta(), fast, BINARY_FAST_VALUE_CODEC).view() == written.view());

    // Bytes of one codec read as another are a decode failure, not a guess.
    CHECK_THROWS(store.decode(record_meta(), store.encode(written.view(), JSON_VALUE_CODEC)));
}

TEST_CASE("value store: the owner of stored bytes bounds what decoding them may allocate")
{
    // A compressed object's size says nothing about what it expands to, and
    // the length it claims sizes an allocation. So a limit on the stored bytes
    // is a limit on nothing: the reader states one on the decoded object.
    const auto store = memory_store();
    hgraph::ListBuilder names{hgraph::TypeRegistry::instance().scalar_type<hgraph::Str>()};
    for (int index = 0; index < 4'000; ++index) { names.push_back(Value{hgraph::Str{"a-repeated-name"}}.view()); }
    const Value       repetitive = names.build();
    const auto        codec = store.bind(repetitive.view().schema());
    const ObjectBytes encoded = codec.encode(repetitive.view());
    REQUIRE(encoded.size() < 4'096);   // it compressed well: tens of kilobytes became this

    CHECK(codec.decode(encoded).view() == repetitive.view());
    CHECK(codec.decode(encoded, 1u << 20).view() == repetitive.view());
    CHECK_THROWS_WITH(codec.decode(encoded, 8'192), Catch::Matchers::ContainsSubstring("more than the 8192 allowed"));

    // A codec that does not compress is bounded by the stored size itself.
    const auto        json = store.bind(repetitive.view().schema(), JSON_VALUE_CODEC);
    const ObjectBytes text = json.encode(repetitive.view());
    CHECK(json.decode(text, text.size()).view() == repetitive.view());
    CHECK_THROWS_WITH(json.decode(text, text.size() - 1), Catch::Matchers::ContainsSubstring("exceeds the size its reader allows"));
}

TEST_CASE("value store: the key is the caller's, untouched")
{
    // Keys are not rewritten, so a caller whose keys are structured, parsed or
    // range-scanned keeps them exactly as written -- and one that wants an
    // extension simply writes it.
    register_builtin_value_codecs();
    const auto  store = memory_store();
    const Value written = record_value("alpha", 7, "BOM");

    REQUIRE(store.write("records/0000000000000000001", written.view()).status ==
            ImmutableWriteStatus::Created);
    REQUIRE(store.write("records/alpha.json", written.view()).status ==
            ImmutableWriteStatus::Created);

    CHECK(store.try_read("records/0000000000000000001", record_meta()).has_value());
    CHECK(store.try_read("records/alpha.json", record_meta()).has_value());
    // Nothing was written anywhere else.
    CHECK_FALSE(store.try_read("records/alpha", record_meta()).has_value());
    CHECK_FALSE(
        store.try_read("records/0000000000000000001.json", record_meta()).has_value());
}

TEST_CASE("value store: the codec is configuration, defaulted or given per call")
{
    register_reversing_codec();

    const auto reversed_default = memory_store("test-reversed");
    CHECK(reversed_default.default_codec() == "test-reversed");

    const Value written = record_value("alpha", 7, "BOM");
    const auto  json_default = memory_store(std::string{JSON_VALUE_CODEC});

    // The store default applies when a call names nothing...
    CHECK(as_text(json_default.encode(written.view())) == to_json_string(written.view()));
    CHECK(as_text(reversed_default.encode(written.view())) !=
          to_json_string(written.view()));

    // ...and a per-call codec overrides it, in both directions.
    CHECK(as_text(reversed_default.encode(written.view(), JSON_VALUE_CODEC)) ==
          to_json_string(written.view()));
    CHECK(as_text(json_default.encode(written.view(), "test-reversed")) !=
          to_json_string(written.view()));
}

TEST_CASE("value store: a read names the codec its object was written with")
{
    register_reversing_codec();
    const auto  store = memory_store();
    const Value written = record_value("beta", 2, "FRONT");

    REQUIRE(store.write("records/beta", written.view(), "test-reversed").status ==
            ImmutableWriteStatus::Created);

    CHECK(store.read("records/beta", record_meta(), "test-reversed")
              .view()
              .equals(written.view()));

    // Reading it with the wrong codec is a configuration error, and reports
    // itself rather than returning a plausible wrong answer.
    CHECK_THROWS(store.read("records/beta", record_meta()));
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
    REQUIRE(created.current.has_value());
    CHECK(created.current->value.view().equals(first.view()));

    // A stale expectation loses, and the winner's value is unchanged.
    const auto stale = store.compare_exchange("records/cas", second.view(), "not-the-token");
    CHECK_FALSE(stale.exchanged);
    REQUIRE(stale.current.has_value());
    CHECK(stale.current->value.view().equals(first.view()));
}

TEST_CASE("bound value store keeps byte storage behind its typed contract")
{
    const auto store = memory_store();
    const auto bound = store.bind_schema(record_meta());
    const Value first = record_value("alpha", 1, "BOM");
    const Value second = record_value("alpha", 2, "BOM");

    const auto created = bound.compare_exchange("records/typed-cas", first.view(),
                                                std::nullopt);
    REQUIRE(created.exchanged);
    REQUIRE(created.current.has_value());
    CHECK(created.current->value.view().equals(first.view()));

    const auto stale = bound.compare_exchange("records/typed-cas", second.view(),
                                              "not-the-token");
    REQUIRE_FALSE(stale.exchanged);
    REQUIRE(stale.current.has_value());
    CHECK(stale.current->value.view().equals(first.view()));
    CHECK(bound.read("records/typed-cas").view().equals(first.view()));
}

TEST_CASE("bound codecs retain implementation context and reset moved-from handles")
{
    auto context = std::make_shared<int>(7);
    std::weak_ptr<int> lifetime = context;
    ValueCodec codec{
        "owned-test", context,
        ValueCodecOps{.bind = &reversing_bind,
                      .encode = &reversing_encode,
                      .decode = &reversing_decode}};
    context.reset();

    auto bound = codec.bind(record_meta());
    codec = {};
    REQUIRE_FALSE(lifetime.expired());

    auto moved = std::move(bound);
    CHECK_FALSE(bound);
    CHECK_THROWS_AS(bound.encode(record_value("x", 1, "Y").view()),
                    std::logic_error);
    CHECK(moved.decode(moved.encode(record_value("x", 1, "Y").view()))
              .view()
              .equals(record_value("x", 1, "Y").view()));

    moved = {};
    CHECK(lifetime.expired());
}

TEST_CASE("bound codecs retain schema-binding ownership")
{
    auto context = std::make_shared<OwnedBindingContext>();
    ValueCodec codec{
        "owned-binding", context,
        ValueCodecOps{.bind = &owned_binding_bind,
                      .encode = &reversing_encode,
                      .decode = &reversing_decode}};

    auto bound = codec.bind(record_meta());
    REQUIRE_FALSE(context->last_binding.expired());
    codec = {};
    REQUIRE_FALSE(context->last_binding.expired());
    CHECK(bound.decode(bound.encode(record_value("x", 1, "Y").view()))
              .view()
              .equals(record_value("x", 1, "Y").view()));

    bound = {};
    CHECK(context->last_binding.expired());
}

TEST_CASE("bound codecs reject schema confusion in both directions")
{
    const auto json = value_codec(JSON_VALUE_CODEC).bind(record_meta());
    CHECK_THROWS_AS(json.encode(Value{Str{"not a record"}}.view()),
                    std::invalid_argument);

    ValueCodec wrong{
        "wrong-schema", nullptr,
        ValueCodecOps{.bind = &reversing_bind,
                      .encode = &reversing_encode,
                      .decode = &wrong_schema_decode}};
    const auto bound = wrong.bind(record_meta());
    CHECK_THROWS_AS(bound.decode({}), std::invalid_argument);
}

TEST_CASE("value codec ad-hoc calls preserve escaped polymorphic values")
{
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *base = registry.bundle(
        "tests.value_codec_polymorphic", "Base", {{"id", integer}}, {}, true);
    const auto *child = registry.bundle(
        "tests.value_codec_polymorphic", "Child",
        {{"id", integer}, {"quantity", integer}}, {base});
    const auto realization = TypeRealizationSnapshot::capture(registry);

    Value escaped;
    {
        TypeRealizationScope scope{realization.get()};
        escaped = from_json_string(
            bind_json_converter(base),
            R"({"__type__": "tests.value_codec_polymorphic::Child", "id": 1, "quantity": 2})");
    }
    REQUIRE(escaped.view().concrete().schema() == child);

    ObjectBytes encoded;
    const auto codec = value_codec(JSON_VALUE_CODEC);
    codec.encode(escaped.view(), encoded);
    const auto text = as_text(encoded);
    CHECK(text.find(
              R"("__type__": "tests.value_codec_polymorphic::Child")") !=
          std::string::npos);
    CHECK(text.find(R"("quantity": 2)") != std::string::npos);
    CHECK(codec.decode(base, encoded).view().concrete().schema() == child);
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
                                         ValueCodecOps{.bind = &reversing_bind,
                                                       .encode = &reversing_encode,
                                                       .decode = &reversing_decode}),
                    std::invalid_argument);

}


TEST_CASE("value store: a local-backend object is a json file on disk")
{
    // The requirement in plain terms: open the file in a text editor and see
    // json. Asserting on encoded bytes is a proxy; this reads the file back
    // through the filesystem with no hgraph code in the path.
    register_builtin_value_codecs();
    const auto root =
        std::filesystem::temp_directory_path() /
        ("hgraph_value_store_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root);

    // A store meant to be read by other tools names the json codec.
    const auto store = make_value_store(ValueStoreConfig{
        .objects = make_object_store(ObjectStoreConfig{LocalLocation{root.string()}}),
        .codec = std::string{JSON_VALUE_CODEC}});

    const Value written = record_value("alpha", 7, "BOM");
    // The caller names the file, including its extension: that is the whole
    // point of leaving keys alone.
    REQUIRE(store.write("records/alpha.json", written.view()).status ==
            ImmutableWriteStatus::Created);

    const auto path = root / "records" / "alpha.json";
    REQUIRE(std::filesystem::exists(path));

    std::string contents;
    {
        // Scoped so the handle is closed before the cleanup below: Windows
        // refuses to remove a file that is still open, where POSIX allows it.
        std::ifstream file{path};
        REQUIRE(file.is_open());
        contents.assign(std::istreambuf_iterator<char>{file},
                        std::istreambuf_iterator<char>{});
    }

    CHECK(contents == to_json_string(written.view()));
    CHECK(contents.starts_with("{"));
    CHECK(contents.ends_with("}"));
    CHECK(contents.find("\"name\"") != std::string::npos);

    // Best-effort: a cleanup failure must not be reported as a codec failure.
    std::error_code cleanup;
    std::filesystem::remove_all(root, cleanup);
}

TEST_CASE("value store: the built-in codecs need no registration call")
{
    // A standalone consumer of the installed SDK constructs the advertised
    // default store without running any extension's registration. The codecs a
    // conforming persistence build provides (RFC 0030, RFC 0040) must already
    // be there -- not installed as a side effect of something else.
    for (const auto name : {BINARY_VALUE_CODEC, BINARY_FAST_VALUE_CODEC, JSON_VALUE_CODEC})
    {
        CHECK(value_codec_registered(name));
        CHECK_NOTHROW(value_codec(name));
    }

    const auto store = make_value_store(
        ValueStoreConfig{.objects = make_object_store(ObjectStoreConfig{})});
    const Value written = record_value("alpha", 7, "BOM");
    CHECK(store.decode(record_meta(), store.encode(written.view())).view() == written.view());
}

TEST_CASE("value store: the default codec is not looked up per call")
{
    // The store resolves its default at construction, so an ordinary call must
    // not reach the codec registry and take its TypeSystemMutex. Naming a
    // different codec does perform that lookup, so the two paths differ by
    // exactly the registry traffic this store avoids -- the difference, not an
    // absolute count, is the honest assertion.
    //
    // Both codecs are warmed first: their run-bound JSON plans are composed on
    // first sight of a schema, and that one-off cost would otherwise swamp the
    // measurement. Neither count is zero afterwards because these unbound
    // convenience calls deliberately bind a fresh schema plan per value; that
    // is why evaluation code retains BoundValueStore instead.
    //
    // The same codec is used on both sides -- as one store's default, and named
    // on a store whose default is something else -- so the conversion costs
    // the same and only the lookup differs.
    register_reversing_codec();
    const auto  as_default = memory_store("test-reversed");
    const auto  by_name = memory_store();
    const Value written = record_value("alpha", 7, "BOM");

    static_cast<void>(as_default.encode(written.view()));
    static_cast<void>(by_name.encode(written.view(), "test-reversed"));

    const auto default_before = type_system_lock_count();
    for (int index = 0; index < 16; ++index)
    {
        static_cast<void>(as_default.encode(written.view()));
    }
    const auto default_cost = type_system_lock_count() - default_before;

    const auto named_before = type_system_lock_count();
    for (int index = 0; index < 16; ++index)
    {
        static_cast<void>(by_name.encode(written.view(), "test-reversed"));
    }
    const auto named_cost = type_system_lock_count() - named_before;

    // Both encode one value through one conversion; only the named path
    // additionally resolves its codec by name.
    CHECK(default_cost < named_cost);
}
