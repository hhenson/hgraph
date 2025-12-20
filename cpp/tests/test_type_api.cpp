//
// Test for the declarative type API
//

#include <hgraph/types/type_api.h>
#include <iostream>
#include <cassert>

using namespace hgraph;
using namespace hgraph::types;

void test_scalar_types() {
    std::cout << "Testing scalar types..." << std::endl;

    auto* int_type = type_of<int>();
    auto* float_type = type_of<float>();
    auto* double_type = type_of<double>();

    assert(int_type != nullptr);
    assert(float_type != nullptr);
    assert(double_type != nullptr);
    assert(int_type != float_type);

    // Same type should return same pointer (interning)
    assert(type_of<int>() == int_type);

    std::cout << "  int: " << int_type->type_name_str() << std::endl;
    std::cout << "  float: " << float_type->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_ts_types() {
    std::cout << "Testing TS types..." << std::endl;

    auto* ts_int = ts_type<TS<int>>();
    auto* ts_float = ts_type<TS<float>>();

    assert(ts_int != nullptr);
    assert(ts_float != nullptr);
    assert(ts_int != ts_float);
    assert(ts_int->ts_kind == TSKind::TS);

    // Interning check
    assert(ts_type<TS<int>>() == ts_int);

    std::cout << "  TS<int>: " << ts_int->type_name_str() << std::endl;
    std::cout << "  TS<float>: " << ts_float->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_tss_types() {
    std::cout << "Testing TSS types..." << std::endl;

    auto* tss_int = ts_type<TSS<int>>();

    assert(tss_int != nullptr);
    assert(tss_int->ts_kind == TSKind::TSS);
    assert(ts_type<TSS<int>>() == tss_int);  // Interning

    std::cout << "  TSS<int>: " << tss_int->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_tsl_types() {
    std::cout << "Testing TSL types..." << std::endl;

    auto* tsl_3 = ts_type<TSL<TS<int>, 3>>();
    auto* tsl_5 = ts_type<TSL<TS<int>, 5>>();

    assert(tsl_3 != nullptr);
    assert(tsl_5 != nullptr);
    assert(tsl_3 != tsl_5);  // Different sizes
    assert(tsl_3->ts_kind == TSKind::TSL);

    // Interning check
    auto* tsl_3_check = ts_type<TSL<TS<int>, 3>>();
    assert(tsl_3_check == tsl_3);

    std::cout << "  TSL<TS<int>, 3>: " << tsl_3->type_name_str() << std::endl;
    std::cout << "  TSL<TS<int>, 5>: " << tsl_5->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_tsd_types() {
    std::cout << "Testing TSD types..." << std::endl;

    auto* tsd = ts_type<TSD<int, TS<float>>>();

    assert(tsd != nullptr);
    assert(tsd->ts_kind == TSKind::TSD);
    auto* tsd_check = ts_type<TSD<int, TS<float>>>();
    assert(tsd_check == tsd);  // Interning

    std::cout << "  TSD<int, TS<float>>: " << tsd->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_tsw_types() {
    std::cout << "Testing TSW types..." << std::endl;

    // Count-based window (legacy form)
    auto* tsw_count = ts_type<TSW<float, 10, 1>>();
    assert(tsw_count != nullptr);
    assert(tsw_count->ts_kind == TSKind::TSW);
    auto* tsw_count2 = ts_type<TSW<float, 10, 1>>();
    assert(tsw_count2 == tsw_count);  // Interning

    std::cout << "  TSW<float, 10, 1>: " << tsw_count->type_name_str() << std::endl;

    // Time-based window (60 seconds)
    auto* tsw_time = ts_type<TSW_Time<float, Seconds<60>>>();
    assert(tsw_time != nullptr);
    assert(tsw_time->ts_kind == TSKind::TSW);
    auto* tsw_time_check = ts_type<TSW_Time<float, Seconds<60>>>();
    assert(tsw_time_check == tsw_time);  // Interning

    std::cout << "  TSW_Time<float, Seconds<60>>: " << tsw_time->type_name_str() << std::endl;

    // Time-based window with min count (5 minutes, min 3 values)
    auto* tsw_min = ts_type<TSW_Time<float, Minutes<5>, Count<3>>>();
    assert(tsw_min != nullptr);
    assert(tsw_min->ts_kind == TSKind::TSW);

    std::cout << "  TSW_Time<float, Minutes<5>, Count<3>>: " << tsw_min->type_name_str() << std::endl;

    // Different time units
    auto* tsw_ms = ts_type<TSW_Time<int, Milliseconds<500>>>();
    auto* tsw_hr = ts_type<TSW_Time<double, Hours<1>>>();
    assert(tsw_ms != nullptr);
    assert(tsw_hr != nullptr);

    std::cout << "  TSW_Time<int, Milliseconds<500>>: " << tsw_ms->type_name_str() << std::endl;
    std::cout << "  TSW_Time<double, Hours<1>>: " << tsw_hr->type_name_str() << std::endl;

    std::cout << "  PASSED" << std::endl;
}

void test_ref_types() {
    std::cout << "Testing REF types..." << std::endl;

    auto* ref = ts_type<REF<TS<int>>>();

    assert(ref != nullptr);
    assert(ref->ts_kind == TSKind::REF);
    assert(ts_type<REF<TS<int>>>() == ref);  // Interning

    std::cout << "  REF<TS<int>>: " << ref->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_tsb_types() {
    std::cout << "Testing TSB types..." << std::endl;

    // Simple bundle without name
    auto* point_unnamed = ts_type<TSB<
        Field<"x", TS<int>>,
        Field<"y", TS<int>>
    >>();

    assert(point_unnamed != nullptr);
    assert(point_unnamed->ts_kind == TSKind::TSB);

    std::cout << "  TSB<Field<x, TS<int>>, Field<y, TS<int>>>: "
              << point_unnamed->type_name_str() << std::endl;

    // Bundle with name
    auto* point_named = ts_type<TSB<
        Field<"x", TS<float>>,
        Field<"y", TS<float>>,
        Name<"Point2D">
    >>();

    assert(point_named != nullptr);
    assert(point_named->ts_kind == TSKind::TSB);
    assert(point_named->name != nullptr);

    std::cout << "  TSB<..., Name<Point2D>>: " << point_named->type_name_str()
              << " (name=" << point_named->name << ")" << std::endl;

    // Interning check
    auto* point_named2 = ts_type<TSB<Field<"x", TS<float>>, Field<"y", TS<float>>, Name<"Point2D">>>();
    assert(point_named2 == point_named);

    std::cout << "  PASSED" << std::endl;
}

void test_nested_types() {
    std::cout << "Testing nested types..." << std::endl;

    // TSL of TSB
    auto* nested = ts_type<TSL<
        TSB<Field<"value", TS<int>>>,
        2
    >>();

    assert(nested != nullptr);
    assert(nested->ts_kind == TSKind::TSL);

    std::cout << "  TSL<TSB<Field<value, TS<int>>>, 2>: " << nested->type_name_str() << std::endl;

    // TSD with TSB value
    auto* dict_of_bundles = ts_type<TSD<
        int,
        TSB<Field<"name", TS<int>>, Field<"count", TS<int>>>
    >>();

    assert(dict_of_bundles != nullptr);
    assert(dict_of_bundles->ts_kind == TSKind::TSD);

    std::cout << "  TSD<int, TSB<...>>: " << dict_of_bundles->type_name_str() << std::endl;

    std::cout << "  PASSED" << std::endl;
}

// ============================================================================
// Runtime API Tests
// ============================================================================

void test_runtime_ts() {
    std::cout << "Testing runtime::ts..." << std::endl;

    auto* int_meta = type_of<int>();
    auto* ts_int = runtime::ts(int_meta);

    assert(ts_int != nullptr);
    assert(ts_int->ts_kind == TSKind::TS);

    // Should match compile-time version
    assert(ts_int == ts_type<TS<int>>());

    std::cout << "  runtime::ts(int): " << ts_int->type_name_str() << std::endl;
    std::cout << "  Matches compile-time: YES" << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_runtime_tss() {
    std::cout << "Testing runtime::tss..." << std::endl;

    auto* int_meta = type_of<int>();
    auto* tss_int = runtime::tss(int_meta);

    assert(tss_int != nullptr);
    assert(tss_int->ts_kind == TSKind::TSS);

    // Should match compile-time version
    assert(tss_int == ts_type<TSS<int>>());

    std::cout << "  runtime::tss(int): " << tss_int->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_runtime_tsl() {
    std::cout << "Testing runtime::tsl..." << std::endl;

    auto* ts_int = runtime::ts(type_of<int>());
    auto* tsl_3 = runtime::tsl(ts_int, 3);

    assert(tsl_3 != nullptr);
    assert(tsl_3->ts_kind == TSKind::TSL);

    // Should match compile-time version
    auto* ct_tsl_3 = ts_type<TSL<TS<int>, 3>>();
    assert(tsl_3 == ct_tsl_3);

    std::cout << "  runtime::tsl(TS<int>, 3): " << tsl_3->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_runtime_tsd() {
    std::cout << "Testing runtime::tsd..." << std::endl;

    auto* ts_float = runtime::ts(type_of<float>());
    auto* tsd_type = runtime::tsd(type_of<int>(), ts_float);

    assert(tsd_type != nullptr);
    assert(tsd_type->ts_kind == TSKind::TSD);

    // Should match compile-time version
    auto* ct_tsd_type = ts_type<TSD<int, TS<float>>>();
    assert(tsd_type == ct_tsd_type);

    std::cout << "  runtime::tsd(int, TS<float>): " << tsd_type->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_runtime_tsb() {
    std::cout << "Testing runtime::tsb..." << std::endl;

    auto* ts_int = runtime::ts(type_of<int>());
    auto* ts_float = runtime::ts(type_of<float>());

    std::vector<std::pair<std::string, const TSMeta*>> fields = {
        {"x", ts_int},
        {"y", ts_float}
    };

    auto* point = runtime::tsb(fields, "RuntimePoint");

    assert(point != nullptr);
    assert(point->ts_kind == TSKind::TSB);
    assert(point->name != nullptr);
    assert(std::string(point->name) == "RuntimePoint");

    // Interning check
    auto* point2 = runtime::tsb(fields, "RuntimePoint");
    assert(point == point2);

    std::cout << "  runtime::tsb({x: TS<int>, y: TS<float>}, RuntimePoint): "
              << point->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_runtime_tsw() {
    std::cout << "Testing runtime::tsw..." << std::endl;

    // Count-based window
    auto* tsw_count = runtime::tsw(type_of<float>(), 10, 1);
    assert(tsw_count != nullptr);
    assert(tsw_count->ts_kind == TSKind::TSW);

    // Should match compile-time version
    auto* ct_tsw_count = ts_type<TSW<float, 10, 1>>();
    assert(tsw_count == ct_tsw_count);

    std::cout << "  runtime::tsw(float, 10, 1): " << tsw_count->type_name_str() << std::endl;

    // Time-based window (60 seconds = 60,000,000 microseconds)
    auto* tsw_time = runtime::tsw_time(type_of<float>(), 60'000'000, 0);
    assert(tsw_time != nullptr);
    assert(tsw_time->ts_kind == TSKind::TSW);

    // Should match compile-time version
    auto* ct_tsw_time = ts_type<TSW_Time<float, Seconds<60>>>();
    assert(tsw_time == ct_tsw_time);

    std::cout << "  runtime::tsw_time(float, 60s): " << tsw_time->type_name_str() << std::endl;

    std::cout << "  PASSED" << std::endl;
}

void test_runtime_ref() {
    std::cout << "Testing runtime::ref..." << std::endl;

    auto* ts_int = runtime::ts(type_of<int>());
    auto* ref_type = runtime::ref(ts_int);

    assert(ref_type != nullptr);
    assert(ref_type->ts_kind == TSKind::REF);

    // Should match compile-time version
    assert(ref_type == ts_type<REF<TS<int>>>());

    std::cout << "  runtime::ref(TS<int>): " << ref_type->type_name_str() << std::endl;
    std::cout << "  PASSED" << std::endl;
}

void test_runtime_compile_time_equivalence() {
    std::cout << "Testing runtime/compile-time equivalence..." << std::endl;

    // Build the same type both ways and verify they're identical (same pointer)

    // TS<int>
    auto* rt_ts = runtime::ts(type_of<int>());
    auto* ct_ts = ts_type<TS<int>>();
    assert(rt_ts == ct_ts);

    // TSL<TS<int>, 5>
    auto* rt_tsl = runtime::tsl(rt_ts, 5);
    auto* ct_tsl = ts_type<TSL<TS<int>, 5>>();
    assert(rt_tsl == ct_tsl);

    // TSD<int, TS<float>>
    auto* rt_tsd = runtime::tsd(type_of<int>(), runtime::ts(type_of<float>()));
    auto* ct_tsd = ts_type<TSD<int, TS<float>>>();
    assert(rt_tsd == ct_tsd);

    std::cout << "  All runtime types match compile-time equivalents" << std::endl;
    std::cout << "  PASSED" << std::endl;
}

int main() {
    std::cout << "=== Type API Tests ===" << std::endl << std::endl;

    // Compile-time API tests
    std::cout << "--- Compile-Time API ---" << std::endl;
    test_scalar_types();
    test_ts_types();
    test_tss_types();
    test_tsl_types();
    test_tsd_types();
    test_tsw_types();
    test_ref_types();
    test_tsb_types();
    test_nested_types();

    // Runtime API tests
    std::cout << std::endl << "--- Runtime API ---" << std::endl;
    test_runtime_ts();
    test_runtime_tss();
    test_runtime_tsl();
    test_runtime_tsd();
    test_runtime_tsb();
    test_runtime_tsw();
    test_runtime_ref();
    test_runtime_compile_time_equivalence();

    std::cout << std::endl << "=== All tests passed! ===" << std::endl;
    return 0;
}
