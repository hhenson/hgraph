#include <catch2/catch_test_macros.hpp>

#include <hgraph/v2/cpp_wiring/static_wiring.h>

#include <Python.h>

#include <cstdlib>
#include <string_view>

namespace {

void ensure_python_runtime() {
    static const bool initialized = [] {
        if (!Py_IsInitialized()) {
            setenv("PYTHONPATH", HGRAPH_TEST_PYTHONPATH, 1);

            static wchar_t* python_program = Py_DecodeLocale(HGRAPH_TEST_PYTHON_EXECUTABLE, nullptr);
            static wchar_t* python_home = Py_DecodeLocale(HGRAPH_TEST_PYTHON_HOME, nullptr);
            Py_SetProgramName(python_program);
            Py_SetPythonHome(python_home);
            Py_Initialize();
        }
        return true;
    }();
    (void) initialized;
}

using namespace hgraph::v2::cpp_wiring;

using PointSchema = Bundle<
    name<"CppWiringPoint">,
    field<"x", double>,
    field<"y", int64_t>
>;

using PointTS = TS<PointSchema>;
using PriceTS = TS<double>;
using QuoteTS = TSB<
    name<"CppWiringQuote">,
    field<"bid", PriceTS>,
    field<"point", PointTS>
>;

using PriceListTS = TSL<PriceTS, 10>;
using PriceDictTS = TSD<int64_t, PriceTS>;
using ActiveIdsTS = TSS<int64_t>;
using PriceRefTS = REF<PriceTS>;
using PriceWindowTS = TSW<double, 10, 3>;

} // namespace

namespace hgraph::v2::cpp_wiring {

TEST_CASE("static wiring value bundles are registered once") {
    const value::TypeMeta* point = PointSchema::schema();

    REQUIRE(point == PointSchema::schema());
    REQUIRE(point->kind == value::TypeKind::Bundle);
    REQUIRE(point->field_count == 2);
    REQUIRE(std::string_view{point->fields[0].name} == "x");
    REQUIRE(std::string_view{point->fields[1].name} == "y");
    REQUIRE(point->fields[0].type == value::scalar_type_meta<double>());
    REQUIRE(point->fields[1].type == value::scalar_type_meta<int64_t>());
    REQUIRE(value::TypeRegistry::instance().get_bundle_by_name("CppWiringPoint") == point);
}

TEST_CASE("static wiring composes scalar, bundle, and bundle-ts schemas") {
    ensure_python_runtime();

    const TSMeta* price = PriceTS::schema();
    const TSMeta* point = PointTS::schema();
    const TSMeta* quote = QuoteTS::schema();

    REQUIRE(price == PriceTS::schema());
    REQUIRE(price->kind == TSKind::TSValue);
    REQUIRE(price->value_type == value::scalar_type_meta<double>());

    REQUIRE(point->kind == TSKind::TSValue);
    REQUIRE(point->value_type == PointSchema::schema());

    REQUIRE(quote == QuoteTS::schema());
    REQUIRE(quote->kind == TSKind::TSB);
    REQUIRE(quote->field_count == 2);
    REQUIRE(std::string_view{quote->bundle_name} == "CppWiringQuote");
    REQUIRE(std::string_view{quote->fields[0].name} == "bid");
    REQUIRE(std::string_view{quote->fields[1].name} == "point");
    REQUIRE(quote->fields[0].ts_type == price);
    REQUIRE(quote->fields[1].ts_type == point);
    REQUIRE(quote->value_type != nullptr);
    REQUIRE(quote->value_type->kind == value::TypeKind::Bundle);
    REQUIRE(quote->value_type->field_count == 2);
}

TEST_CASE("static wiring collection and reference schemas use the existing TS registry") {
    const TSMeta* list_ts = PriceListTS::schema();
    const TSMeta* dict_ts = PriceDictTS::schema();
    const TSMeta* set_ts = ActiveIdsTS::schema();
    const TSMeta* ref_ts = PriceRefTS::schema();
    const TSMeta* window_ts = PriceWindowTS::schema();
    const TSMeta* signal_ts = SIGNAL::schema();

    REQUIRE(list_ts == PriceListTS::schema());
    REQUIRE(list_ts->kind == TSKind::TSL);
    REQUIRE(list_ts->element_ts == PriceTS::schema());
    REQUIRE(list_ts->fixed_size == 10);
    REQUIRE(list_ts->value_type != nullptr);
    REQUIRE(list_ts->value_type->kind == value::TypeKind::List);
    REQUIRE(list_ts->value_type->element_type == value::scalar_type_meta<double>());
    REQUIRE(list_ts->value_type->fixed_size == 10);

    REQUIRE(dict_ts->kind == TSKind::TSD);
    REQUIRE(dict_ts->key_type == value::scalar_type_meta<int64_t>());
    REQUIRE(dict_ts->element_ts == PriceTS::schema());
    REQUIRE(dict_ts->value_type != nullptr);
    REQUIRE(dict_ts->value_type->kind == value::TypeKind::Map);
    REQUIRE(dict_ts->value_type->key_type == value::scalar_type_meta<int64_t>());
    REQUIRE(dict_ts->value_type->element_type == value::scalar_type_meta<double>());

    REQUIRE(set_ts->kind == TSKind::TSS);
    REQUIRE(set_ts->value_type != nullptr);
    REQUIRE(set_ts->value_type->kind == value::TypeKind::Set);
    REQUIRE(set_ts->value_type->element_type == value::scalar_type_meta<int64_t>());

    REQUIRE(ref_ts == PriceRefTS::schema());
    REQUIRE(ref_ts->kind == TSKind::REF);
    REQUIRE(ref_ts->element_ts == PriceTS::schema());
    REQUIRE(ref_ts->value_type != nullptr);

    REQUIRE(window_ts->kind == TSKind::TSW);
    REQUIRE(window_ts->value_type == value::scalar_type_meta<double>());
    REQUIRE_FALSE(window_ts->is_duration_based);
    REQUIRE(window_ts->window.tick.period == 10);
    REQUIRE(window_ts->window.tick.min_period == 3);

    REQUIRE(signal_ts == SIGNAL::schema());
    REQUIRE(signal_ts->kind == TSKind::SIGNAL);
    REQUIRE(signal_ts->value_type == value::scalar_type_meta<bool>());
}

} // namespace hgraph::v2::cpp_wiring
