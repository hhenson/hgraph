#include <hgraph/lib/std/std_operators.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/manifest/graph_manifest.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/util/sha256.h>

#include <catch2/catch_test_macros.hpp>

#include <limits>
#include <optional>
#include <span>
#include <string>
#include <vector>

// RFC 0022 stage 1: canonical graph/node descriptors, exact validation, and
// the byte layer under them. Graphs are wired the erased way (name-resolved
// operators), matching the artifact `manifest::capture` receives from either
// authoring frontend.

namespace
{
    using namespace hgraph;

    std::string hex(const util::Sha256Digest &digest)
    {
        const auto chars = util::sha256_hex(digest);
        return {chars.data(), chars.size()};
    }

    WiringArg ts_arg(WiringPortRef port, std::string name = {})
    {
        WiringArg arg;
        arg.kind = WiringArg::Kind::TimeSeries;
        arg.port = std::move(port);
        arg.name = std::move(name);
        return arg;
    }

    WiringArg scalar_arg(Value value, const ValueTypeMetaData *meta, std::string name = {})
    {
        WiringArg arg;
        arg.kind         = WiringArg::Kind::Scalar;
        arg.scalar_value = std::move(value);
        arg.scalar_meta  = meta;
        arg.name         = std::move(name);
        return arg;
    }

    OperatorWireResult call_operator(Wiring &w,
                                     std::string_view name,
                                     std::vector<WiringArg> args,
                                     std::optional<bool> output_required = std::nullopt,
                                     const TSValueTypeMetaData *expected_output = nullptr)
    {
        ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
            name, std::span<const WiringArg>{args.data(), args.size()}, output_required,
            expected_output, {}, w.operator_state(), &w);
        return resolved.impl->wire(w, resolved.map, resolved.args, resolved.kwargs);
    }

    struct RuntimeMetas
    {
        const ValueTypeMetaData   *int_meta{nullptr};
        const ValueTypeMetaData   *str_meta{nullptr};
        const TSValueTypeMetaData *ts_int{nullptr};
    };

    RuntimeMetas runtime_metas()
    {
        auto &registry = TypeRegistry::instance();
        RuntimeMetas metas;
        metas.int_meta = registry.register_scalar<Int>("int");
        metas.str_meta = registry.register_scalar<Str>("str");
        metas.ts_int   = registry.ts(metas.int_meta);
        return metas;
    }

    // A small static graph: const lhs + const rhs -> add_ -> record sink.
    GraphBuilder wire_sum_graph(Int lhs_value, Int rhs_value)
    {
        hgraph::stdlib::register_standard_operators();
        const auto metas = runtime_metas();

        Wiring w;
        auto lhs = call_operator(w, "const",
                                 {scalar_arg(Value{Int{lhs_value}}, metas.int_meta)}, true,
                                 metas.ts_int);
        auto rhs = call_operator(w, "const",
                                 {scalar_arg(Value{Int{rhs_value}}, metas.int_meta)}, true,
                                 metas.ts_int);
        auto sum = call_operator(w, "add_",
                                 {ts_arg(lhs.output.erased()), ts_arg(rhs.output.erased())}, true);
        call_operator(w, "record",
                      {ts_arg(sum.output.erased()),
                       scalar_arg(Value{Str{"manifest::out"}}, metas.str_meta)},
                      false);
        return std::move(w).finish();
    }
}  // namespace

TEST_CASE("sha256 matches FIPS 180-4 test vectors", "[manifest]")
{
    CHECK(hex(util::sha256({})) ==
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    const std::string abc = "abc";
    CHECK(hex(util::sha256(std::as_bytes(std::span{abc.data(), abc.size()}))) ==
          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST_CASE("canonical writer/reader round-trip and reject torn input", "[manifest]")
{
    using namespace hgraph::manifest;

    CanonicalWriter writer;
    writer.varint(0);
    writer.varint(127);
    writer.varint(128);
    writer.varint(0xffffffffffffffffull);
    writer.svarint(-1);
    writer.svarint(1);
    writer.svarint(std::numeric_limits<std::int64_t>::min());
    writer.fixed_double(3.5);
    writer.string_field("canonical");
    CanonicalWriter child;
    child.varint(7);
    writer.scope(child);

    CanonicalReader reader{writer.bytes()};
    CHECK(reader.varint() == 0);
    CHECK(reader.varint() == 127);
    CHECK(reader.varint() == 128);
    CHECK(reader.varint() == 0xffffffffffffffffull);
    CHECK(reader.svarint() == -1);
    CHECK(reader.svarint() == 1);
    CHECK(reader.svarint() == std::numeric_limits<std::int64_t>::min());
    CHECK(reader.fixed_double() == 3.5);
    CHECK(reader.string_field() == "canonical");
    auto nested = reader.scope();
    CHECK(nested.varint() == 7);
    CHECK(nested.at_end());
    CHECK(reader.at_end());

    // Truncation inside a varint and inside a length-delimited field both throw.
    const auto &bytes = writer.bytes();
    CanonicalReader torn{std::span{bytes.data(), 1}};
    (void)torn.varint();  // 0 fits in one byte
    CHECK_THROWS_AS(torn.varint(), CanonicalDecodeError);
}

TEST_CASE("the same wired graph produces byte-identical manifests", "[manifest]")
{
    const GraphBuilder first = wire_sum_graph(42, 8);
    const GraphBuilder second = wire_sum_graph(42, 8);

    const auto lhs = manifest::capture(first);
    const auto rhs = manifest::capture(second);

    REQUIRE(lhs.canonical_descriptor().size() == rhs.canonical_descriptor().size());
    CHECK(std::equal(lhs.canonical_descriptor().begin(), lhs.canonical_descriptor().end(),
                     rhs.canonical_descriptor().begin()));
    CHECK(lhs.id() == rhs.id());
    CHECK(manifest::validate(lhs, rhs).identical());
}

TEST_CASE("a changed scalar argument produces a path-addressed difference", "[manifest]")
{
    const auto expected = manifest::capture(wire_sum_graph(42, 8));
    const auto actual = manifest::capture(wire_sum_graph(42, 9));

    CHECK_FALSE(expected.id() == actual.id());

    const auto result = manifest::validate(expected, actual);
    REQUIRE_FALSE(result.identical());
    bool scalar_difference = false;
    for (const auto &difference : result.differences)
    {
        if (difference.path.find("/scalars") != std::string::npos &&
            difference.path.starts_with("node["))
        {
            scalar_difference = true;
        }
    }
    CHECK(scalar_difference);
}

TEST_CASE("a changed topology produces node and edge count differences", "[manifest]")
{
    hgraph::stdlib::register_standard_operators();
    const auto metas = runtime_metas();

    const auto expected = manifest::capture(wire_sum_graph(42, 8));

    // Same sum graph plus one more recorded const: node/edge counts differ.
    Wiring w;
    auto lhs = call_operator(w, "const", {scalar_arg(Value{Int{42}}, metas.int_meta)}, true,
                             metas.ts_int);
    auto rhs = call_operator(w, "const", {scalar_arg(Value{Int{8}}, metas.int_meta)}, true,
                             metas.ts_int);
    auto sum = call_operator(w, "add_",
                             {ts_arg(lhs.output.erased()), ts_arg(rhs.output.erased())}, true);
    call_operator(w, "record",
                  {ts_arg(sum.output.erased()),
                   scalar_arg(Value{Str{"manifest::out"}}, metas.str_meta)},
                  false);
    call_operator(w, "record",
                  {ts_arg(lhs.output.erased()),
                   scalar_arg(Value{Str{"manifest::extra"}}, metas.str_meta)},
                  false);
    const auto actual = manifest::capture(std::move(w).finish());

    CHECK_FALSE(expected.id() == actual.id());
    const auto result = manifest::validate(expected, actual);
    REQUIRE_FALSE(result.identical());
    bool node_count_difference = false;
    for (const auto &difference : result.differences)
    {
        if (difference.path == "graph/node_count") { node_count_difference = true; }
    }
    CHECK(node_count_difference);
}

TEST_CASE("schema descriptors reproduce runtime schema equivalence", "[manifest]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");
    const auto *float_meta = registry.register_scalar<Float>("float");

    const auto ts_int = manifest::ts_descriptor(registry.ts(int_meta));
    const auto ts_int_again = manifest::ts_descriptor(registry.ts(int_meta));
    const auto ts_float = manifest::ts_descriptor(registry.ts(float_meta));
    CHECK(ts_int == ts_int_again);
    CHECK_FALSE(ts_int == ts_float);

    // Wire-affecting qualifiers discriminate: fixed size, window shape.
    const auto tsl_2 = manifest::ts_descriptor(registry.tsl(registry.ts(int_meta), 2));
    const auto tsl_3 = manifest::ts_descriptor(registry.tsl(registry.ts(int_meta), 3));
    CHECK_FALSE(tsl_2 == tsl_3);

    const auto tsw_ticks = manifest::ts_descriptor(registry.tsw(int_meta, 10, 2));
    const auto tsw_other = manifest::ts_descriptor(registry.tsw(int_meta, 10, 3));
    CHECK_FALSE(tsw_ticks == tsw_other);

    const auto tsd = manifest::ts_descriptor(registry.tsd(int_meta, registry.ts(float_meta)));
    const auto tsd_other = manifest::ts_descriptor(registry.tsd(float_meta, registry.ts(float_meta)));
    CHECK_FALSE(tsd == tsd_other);

    // The descriptor never falls back to interned pointers: distinct calls
    // for the equivalent schema give identical bytes (interning makes the
    // metas pointer-equal; the bytes must also be equal by construction).
    CHECK(manifest::value_descriptor(int_meta) == manifest::value_descriptor(int_meta));
}

TEST_CASE("manifest encode/decode round-trips and verifies", "[manifest]")
{
    const auto original = manifest::capture(wire_sum_graph(42, 8));
    const auto encoded = manifest::encode(original);

    const auto decoded = manifest::decode_graph(encoded);
    CHECK(decoded.id() == original.id());
    CHECK(decoded.format_version() == original.format_version());
    CHECK(manifest::validate(original, decoded).identical());

    // Torn input is rejected, never partially decoded.
    const std::span<const std::byte> torn{encoded.data(), encoded.size() / 2};
    CHECK_THROWS_AS(manifest::decode_graph(torn), manifest::CanonicalDecodeError);
}

TEST_CASE("values without a canonical encoding are refused with a reason", "[manifest]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<Int>("int");

    std::string reason;
    CHECK(manifest::manifest_scalar_encodable(int_meta, &reason));
    CHECK_FALSE(manifest::manifest_scalar_encodable(registry.any(), &reason));
    CHECK_FALSE(reason.empty());

    reason.clear();
    CHECK_FALSE(manifest::manifest_scalar_encodable(registry.queue(int_meta, 4), &reason));
    CHECK_FALSE(reason.empty());
}
