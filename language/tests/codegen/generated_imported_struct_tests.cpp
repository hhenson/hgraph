// Struct imports (ADR 0013) through generated C++. The modules are
// codegen/imported-structs/{shapes,instruments}.hgl, whose `hgl test` cases run
// the same programs through direct wiring; each case here asserts the same
// ticks, so the two backends agree tick for tick.
#include <instruments.h>
#include <shapes.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>

using namespace hgraph;
using namespace hgraph::testing;
namespace shapes   = checks::imported_shapes;
namespace consumer = checks::imported_struct_consumer;

namespace
{
    using FutureValue = typename consumer::Future::value_type;

    using EquityValue = typename shapes::Equity::value_type;

    using FutureSymbol = Operator<"checks.imported_struct_consumer.future_symbol", In<"future", TS<FutureValue>>, Out<TS<Str>>>;
    using FutureExpiry = Operator<"checks.imported_struct_consumer.future_expiry", In<"future", TS<FutureValue>>, Out<TS<Int>>>;
    using Build        = Operator<"checks.imported_struct_consumer.build", In<"lot", TS<Int>>, Out<TS<EquityValue>>>;
    using EquityLot    = Operator<"checks.imported_struct_consumer.equity_lot", In<"equity", TS<EquityValue>>, Out<TS<Int>>>;

    using LegValue = typename shapes::Leg::value_type;
    using NextTenor = Operator<"checks.imported_struct_consumer.next_tenor", In<"leg", TS<LegValue>>, Out<TS<Str>>>;

    /// Leg(tenor: outer, next: Leg(tenor: inner)).
    Value leg(const std::string &outer, const std::string &inner) {
        Value root{ValuePlanFactory::instance().type_for(scalar_descriptor<LegValue>::value_meta())};
        auto  fields = root.as_bundle().begin_mutation();
        fields["tenor"].set(Str{outer});
        fields["next"].as_bundle().begin_mutation()["tenor"].set(Str{inner});
        return root;
    }

    Value equity(const std::string &symbol, std::int64_t lot) {
        Value root{ValuePlanFactory::instance().type_for(scalar_descriptor<EquityValue>::value_meta())};
        auto  fields = root.as_bundle().begin_mutation();
        fields["symbol"].set(Str{symbol});
        fields["lot"].set(Int{lot});
        return root;
    }

    Value future(const std::string &symbol, std::int64_t expiry) {
        Value root{ValuePlanFactory::instance().type_for(scalar_descriptor<FutureValue>::value_meta())};
        auto  fields = root.as_bundle().begin_mutation();
        fields["symbol"].set(Str{symbol});
        fields["expiry"].set(Int{expiry});
        return root;
    }
}  // namespace

TEST_CASE("an imported struct is the exporting module's own type", "[codegen][generated][struct-imports]") {
    // There is ONE C++ definition and ONE schema: the consumer's `Future`
    // inherits the exporter's `Instrument` itself, not a copy of it, so a
    // value never converts on the way across.
    const auto *instrument = scalar_descriptor<typename shapes::Instrument::value_type>::value_meta();
    const auto *equity     = scalar_descriptor<typename shapes::Equity::value_type>::value_meta();
    const auto *future     = scalar_descriptor<FutureValue>::value_meta();

    CHECK(std::string{instrument->name()} == "checks.imported_shapes::Instrument");
    CHECK(std::string{equity->name()} == "checks.imported_shapes::Equity");
    // The local child keeps ITS namespace while its parent keeps the owner's.
    CHECK(std::string{future->name()} == "checks.imported_struct_consumer::Future");

    // Both children carry the inherited field, and it is the same field.
    REQUIRE(future->field_count == 2U);
    REQUIRE(equity->field_count == 2U);
    CHECK(std::string{future->fields[0].name} == "symbol");
    CHECK(future->fields[0].type == instrument->fields[0].type);
    CHECK(equity->fields[0].type == instrument->fields[0].type);
}

TEST_CASE("generated C++ reads a local child of an imported family", "[codegen][generated][struct-imports]") {
    // The same two assertions `hgl test` makes on this module through direct
    // wiring (hgraph_language_test_imported_struct_consumer).
    hgl::wiring::ensure_session();
    consumer::register_operators();
    CHECK_OUTPUT((eval_node<FutureSymbol, TS<FutureValue>>(values<Value>(future("fu", 3)))), values<Str>("fu"));
    CHECK_OUTPUT((eval_node<FutureExpiry, TS<FutureValue>>(values<Value>(future("fu", 3)))), values<Int>(3));
}

TEST_CASE("generated C++ constructs a value of an imported struct", "[codegen][generated][struct-imports]") {
    // The value is the EXPORTING module's type: built here, read back here,
    // with no conversion between and one schema behind both.
    hgl::wiring::ensure_session();
    consumer::register_operators();
    CHECK_OUTPUT(eval_node<Build>(values<Int>(5)), values<Value>(equity("eq", 5)));
    CHECK_OUTPUT((eval_node<EquityLot, TS<EquityValue>>(values<Value>(equity("eq", 7)))), values<Int>(7));
}

TEST_CASE("generated C++ rebuilds an imported recursive struct's edge", "[codegen][generated][struct-imports]") {
    // ADR 0012's acceptance item, on the backend it was missing from: the
    // descriptor carries the edge's mandatory `= null`, the importer rebuilds
    // the edge, and this module is COMPILED -- so the schema below is the one
    // generated C++ registered, not one direct wiring built.
    const auto *edge = scalar_descriptor<LegValue>::value_meta();
    REQUIRE(edge->field_count == 2U);
    REQUIRE(edge->fields[1].type != nullptr);
    CHECK(edge->fields[1].type->is_owned());
    CHECK(edge->fields[1].type->element_type == edge);
    CHECK(std::string{edge->name()} == "checks.imported_shapes::Leg");

    // The same tick `hgl test` asserts on this module through direct wiring.
    hgl::wiring::ensure_session();
    consumer::register_operators();
    CHECK_OUTPUT((eval_node<NextTenor, TS<LegValue>>(values<Value>(leg("1Y", "2Y")))), values<Str>("2Y"));
}
