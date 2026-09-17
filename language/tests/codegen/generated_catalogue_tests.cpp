#include <operators.h>
#include <standard.h>

#include "wiring/backend.h"

#include <hgraph/lib/std/operators/operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <cmath>
#include <limits>

using namespace hgraph;
using namespace hgraph::testing;
namespace scalar   = hgraph_::operators_::operators;
namespace standard = hgraph_::std_::operators;

namespace
{
    void register_catalogue() {
        hgl::wiring::ensure_session();
        hgraph_::operators_::register_operators();
        hgraph_::std_::register_operators();
    }

    template <typename Hgl, typename Native, typename... Args> void check_parity(const Args &...args) {
        CHECK_OUTPUT(eval_node<Hgl>(args...), eval_node<Native>(args...));
    }

    Date calendar_date(int year, unsigned month, unsigned day) {
        return Date{std::chrono::year{year} / std::chrono::month{month} / std::chrono::day{day}};
    }
}  // namespace

TEST_CASE("catalogue scalar bodies preserve sparse and repeated native ticks", "[codegen][catalogue]") {
    register_catalogue();
    const auto lhs = values<Int>(none, 7, 7, -7, none, 0);
    const auto rhs = values<Int>(3, none, 3, -3, 2, 2);
    check_parity<scalar::add_, stdlib::add_>(lhs, rhs);
    check_parity<scalar::sub_, stdlib::sub_>(lhs, rhs);
    check_parity<scalar::mul_, stdlib::mul_>(lhs, rhs);
    check_parity<scalar::div_, stdlib::div_>(lhs, rhs);
    check_parity<scalar::floordiv_, stdlib::floordiv_>(lhs, rhs);
    check_parity<scalar::mod_, stdlib::mod_>(lhs, rhs);
    check_parity<scalar::eq_, stdlib::eq_>(lhs, rhs);
    check_parity<scalar::ne_, stdlib::ne_>(lhs, rhs);
    check_parity<scalar::lt_, stdlib::lt_>(lhs, rhs);
    check_parity<scalar::le_, stdlib::le_>(lhs, rhs);
    check_parity<scalar::gt_, stdlib::gt_>(lhs, rhs);
    check_parity<scalar::ge_, stdlib::ge_>(lhs, rhs);
    check_parity<scalar::neg_, stdlib::neg_>(lhs);
    check_parity<scalar::abs_, stdlib::abs_>(lhs);
    check_parity<scalar::pos_, stdlib::pos_>(lhs);
    check_parity<scalar::sign, stdlib::sign>(lhs);
    check_parity<scalar::min_, stdlib::min_>(lhs, rhs);
    check_parity<scalar::max_, stdlib::max_>(lhs, rhs);
    check_parity<scalar::round_, stdlib::round_>(values<Float>(2.675, 2.5, 3.5, -2.5, none), values<Int>(2, 0, 0, 0, -1));
    check_parity<scalar::ln, stdlib::ln>(values<Float>(1.0, none, 2.0, 2.0));
    CHECK_THROWS_WITH(eval_node<scalar::div_>(values<Int>(1), values<Int>(0)),
                      Catch::Matchers::ContainsSubstring("div_: division by zero"));
    CHECK_THROWS_WITH(eval_node<scalar::floordiv_>(values<Int>(1), values<Int>(0)),
                      Catch::Matchers::ContainsSubstring("floordiv_: division by zero"));
    CHECK_THROWS_WITH(eval_node<scalar::mod_>(values<Int>(1), values<Int>(0)),
                      Catch::Matchers::ContainsSubstring("mod_: division by zero"));
}

TEST_CASE("catalogue lifecycle bodies passivate exactly as the native nodes do", "[codegen][catalogue][lifecycle]") {
    register_catalogue();
    check_parity<standard::until_true, stdlib::until_true>(values<Bool>(false, false, true, false, true));
    check_parity<standard::freeze, stdlib::freeze>(values<Bool>(false, false, true, false), values<Int>(1, 2, 3, 4));
    check_parity<standard::freeze, stdlib::freeze>(values<Bool>(none, true, false), values<Int>(1, none, 3));
    check_parity<standard::take, stdlib::take>(values<Int>(1, 2, 3, 4), Int{2});
    check_parity<standard::take, stdlib::take>(values<Int>(1, none, 3, 4), Int{2});
    check_parity<standard::take, stdlib::take>(values<Int>(1, 2), Int{0});
    // schedule keeps its tick counter in a cache, outside record/replay, as
    // the native node keeps it in State<Int>.
    check_parity<standard::schedule, stdlib::schedule>(MIN_TD * 2, Bool{false}, Int{3}, Bool{false});
    check_parity<standard::schedule, stdlib::schedule>(MIN_TD * 2, Bool{true}, Int{2}, Bool{false});
    check_parity<standard::schedule, stdlib::schedule>(MIN_TD, Bool{true}, Int{0}, Bool{false});
}

TEST_CASE("catalogue checked kernels raise exactly as the native operators do", "[codegen][catalogue][throws]") {
    register_catalogue();
    check_parity<scalar::pow_, stdlib::pow_>(values<Int>(2, 3, none, -2), values<Int>(10, none, 2, 3));
    check_parity<scalar::pow_, stdlib::pow_>(values<Float>(2.0, 4.0, 0.0), values<Float>(0.5, -1.0, 2.0));
    check_parity<scalar::pow_, stdlib::pow_>(values<Int>(4, 2), values<Float>(0.5, -1.0));
    check_parity<scalar::pow_, stdlib::pow_>(values<Float>(2.5, 9.0), values<Int>(2, 0));
    check_parity<scalar::lshift_, stdlib::lshift_>(values<Int>(1, 0, -1, 3), values<Int>(4, 70, 1, 62));
    check_parity<scalar::rshift_, stdlib::rshift_>(values<Int>(-16, 1, -1), values<Int>(2, 70, 70));
    check_parity<scalar::substr, stdlib::substr>(values<Str>("hgraph", "hgraph", none, "hgraph"), values<Int>(1, -2, 0, 4),
                                                 values<Int>(3, 100, 2, 2));

    // The HGL bodies raise the native kernel's own exception. It ends the
    // evaluation and reaches the harness exactly as the native node's does.
    CHECK_THROWS_WITH(eval_node<scalar::pow_>(values<Int>(2), values<Int>(-1)),
                      Catch::Matchers::ContainsSubstring("pow_: negative exponent cannot produce an integer result"));
    CHECK_THROWS_WITH(eval_node<scalar::pow_>(values<Int>(2), values<Int>(64)),
                      Catch::Matchers::ContainsSubstring("overflow"));
    CHECK_THROWS_WITH(eval_node<scalar::pow_>(values<Float>(0.0), values<Float>(-1.0)),
                      Catch::Matchers::ContainsSubstring("pow_: zero cannot be raised to a negative power"));
    CHECK_THROWS_WITH(eval_node<scalar::lshift_>(values<Int>(1), values<Int>(-1)),
                      Catch::Matchers::ContainsSubstring("shift count must be non-negative"));
    CHECK_THROWS_WITH(eval_node<scalar::lshift_>(values<Int>(1), values<Int>(64)),
                      Catch::Matchers::ContainsSubstring("shift count is too large"));
    CHECK_THROWS_WITH(eval_node<scalar::rshift_>(values<Int>(1), values<Int>(-1)),
                      Catch::Matchers::ContainsSubstring("shift count must be non-negative"));
}

TEST_CASE("catalogue equality keeps native tolerance and Boolean bodies keep truthiness", "[codegen][catalogue]") {
    register_catalogue();
    const auto floating = values<Float>(0.0, -0.0, 1.0, 1.0 + 1e-11, 2.0);
    const auto rhs      = values<Float>(0.0, 1.0, 1.0 + 1e-11, 1.0, 2.0 + 1e-8);
    check_parity<scalar::eq_, stdlib::eq_>(floating, rhs);
    check_parity<scalar::ne_, stdlib::ne_>(floating, rhs);
    check_parity<scalar::eq_, stdlib::eq_>(values<Int>(0, 1, 2), values<Float>(1e-11, 1.0, 2.0 + 1e-8));
    check_parity<scalar::eq_, stdlib::eq_>(values<Float>(1e-11, 1.0, 2.0 + 1e-8), values<Int>(0, 1, 2));
    check_parity<scalar::and_, stdlib::and_>(floating, rhs);
    check_parity<scalar::or_, stdlib::or_>(floating, rhs);
    check_parity<scalar::not_, stdlib::not_>(floating);
    check_parity<scalar::and_, stdlib::and_>(values<Int>(0, 1, -1), values<Float>(1.0, 0.0, -1.0));
    check_parity<scalar::or_, stdlib::or_>(values<Float>(0.0, 1.0, -1.0), values<Int>(1, 0, -1));
    const auto text = values<Str>("", "a", Str{"\0", 1}, "a");
    check_parity<scalar::not_, stdlib::not_>(text);
    check_parity<scalar::and_, stdlib::and_>(text, values<Str>("x", "", "y", "a"));
    check_parity<scalar::or_, stdlib::or_>(text, values<Str>("x", "", "y", "a"));
    check_parity<scalar::bit_and, stdlib::bit_and>(values<Int>(6, -1, 0), values<Int>(3, 7, 1));
    check_parity<scalar::bit_or, stdlib::bit_or>(values<Int>(6, -1, 0), values<Int>(3, 7, 1));
    check_parity<scalar::bit_xor, stdlib::bit_xor>(values<Int>(6, -1, 0), values<Int>(3, 7, 1));
    check_parity<scalar::invert_, stdlib::invert_>(values<Int>(-1, 0, 1));
    check_parity<scalar::invert_, stdlib::invert_>(values<Bool>(true, false));
    check_parity<scalar::bit_and, stdlib::bit_and>(values<Bool>(true, false), values<Bool>(false, false));
    check_parity<scalar::bit_or, stdlib::bit_or>(values<Bool>(true, false), values<Bool>(false, false));
    check_parity<scalar::bit_xor, stdlib::bit_xor>(values<Bool>(true, false), values<Bool>(false, false));
}

TEST_CASE("catalogue extrema preserve NaNs and signed-zero selection", "[codegen][catalogue]") {
    register_catalogue();
    const Float nan = std::numeric_limits<Float>::quiet_NaN();
    const auto  lhs = values<Float>(nan, 1.0, -0.0, 0.0);
    const auto  rhs = values<Float>(1.0, nan, 0.0, -0.0);
    for (const auto &pair : {std::pair{eval_node<scalar::min_>(lhs, rhs), eval_node<stdlib::min_>(lhs, rhs)},
                             std::pair{eval_node<scalar::max_>(lhs, rhs), eval_node<stdlib::max_>(lhs, rhs)}}) {
        REQUIRE(pair.first.size() == pair.second.size());
        for (std::size_t i = 0; i < pair.first.size(); ++i) {
            REQUIRE(pair.first[i]);
            REQUIRE(pair.second[i]);
            const Float actual   = pair.first[i]->view().checked_as<Float>();
            const Float expected = pair.second[i]->view().checked_as<Float>();
            if (std::isnan(expected)) {
                CHECK(std::isnan(actual));
            } else {
                CHECK(actual == expected);
                CHECK(std::signbit(actual) == std::signbit(expected));
            }
        }
    }
}

TEST_CASE("catalogue scalar streams preserve sampling counting and reopen behavior", "[codegen][catalogue]") {
    register_catalogue();
    const auto ticks = values<Int>(none, 0, 0, none, 1, 1, 0);
    check_parity<standard::dedup, stdlib::dedup>(ticks);
    check_parity<standard::dedup, stdlib::dedup>(values<Str>(none, "", "", "x", "x", ""));
    for (const Int count : {-1, 0, 1, 3, 99}) { check_parity<standard::drop, stdlib::drop>(ticks, count); }
    check_parity<standard::drop, stdlib::drop>(ticks);
    check_parity<standard::sample, stdlib::sample>(values<Bool>(none, true, none, true, true), values<Int>(1, 2, 3, 4, none));
    check_parity<standard::sample, stdlib::sample>(values<Bool>(true, true, none, true), values<Int>(none, 0, 1, none));
    check_parity<standard::filter_, stdlib::filter_>(values<Bool>(true, false, false, true, true, none),
                                                     values<Int>(1, 2, 3, none, none, 4));
    check_parity<standard::filter_, stdlib::filter_>(values<Bool>(true, false, true), values<Int>(1, none, none));
    check_parity<standard::filter_, stdlib::filter_>(values<Bool>(none, false, true), values<Int>(1, 2, 2));
}

TEST_CASE("catalogue calendar bodies preserve date-specific no-change policies", "[codegen][catalogue]") {
    register_catalogue();
    const auto dates = values<Date>(none, calendar_date(2024, 2, 28), calendar_date(2024, 2, 29), calendar_date(2024, 3, 1),
                                    calendar_date(2025, 3, 1));
    check_parity<standard::year, stdlib::year>(dates);
    check_parity<standard::month, stdlib::month>(dates);
    check_parity<standard::month_of_year, stdlib::month_of_year>(dates);
    check_parity<standard::day, stdlib::day>(dates);
    check_parity<standard::day_of_month, stdlib::day_of_month>(dates);
    check_parity<standard::weekday, stdlib::weekday>(dates);
    check_parity<standard::isoweekday, stdlib::isoweekday>(dates);
    const auto instants = values<DateTime>(DateTime{TimeDelta{-1}}, DateTime{}, none, DateTime{TimeDelta{3'600'000'001}},
                                           DateTime{TimeDelta{3'600'000'001}});
    check_parity<standard::year, stdlib::year>(instants);
    check_parity<standard::month, stdlib::month>(instants);
    check_parity<standard::month_of_year, stdlib::month_of_year>(instants);
    check_parity<standard::day, stdlib::day>(instants);
    check_parity<standard::day_of_month, stdlib::day_of_month>(instants);
    check_parity<standard::weekday, stdlib::weekday>(instants);
    check_parity<standard::isoweekday, stdlib::isoweekday>(instants);
    check_parity<standard::hour, stdlib::hour>(instants);
    check_parity<standard::minute, stdlib::minute>(instants);
    check_parity<standard::second, stdlib::second>(instants);
    check_parity<standard::microsecond, stdlib::microsecond>(instants);
    check_parity<standard::datepart, stdlib::datepart>(instants);
    check_parity<standard::timestamp, stdlib::timestamp>(instants);
    check_parity<scalar::eq_, stdlib::eq_>(dates, dates);
    check_parity<scalar::lt_, stdlib::lt_>(instants, instants);
    check_parity<scalar::eq_epsilon, stdlib::eq_>(values<Float>(0.0, 1.0, 2.0), values<Float>(0.1, 1.1, 2.5), 0.2);
    const auto times = values<Time>(Time{0}, Time{3'661'123'456}, Time{3'661'123'456});
    check_parity<standard::hour, stdlib::hour>(times);
    check_parity<standard::minute, stdlib::minute>(times);
    check_parity<standard::second, stdlib::second>(times);
    check_parity<standard::microsecond, stdlib::microsecond>(times);
    const auto durations = values<TimeDelta>(TimeDelta{-1}, TimeDelta{0}, none, TimeDelta{86'400'000'001});
    check_parity<standard::days, stdlib::days>(durations);
    check_parity<standard::seconds, stdlib::seconds>(durations);
    check_parity<standard::microseconds, stdlib::microseconds>(durations);
    check_parity<standard::total_seconds, stdlib::total_seconds>(durations);
    check_parity<standard::last_modified_time, stdlib::last_modified_time>(values<Int>(none, 1, 1, none, 2));
    check_parity<standard::last_modified_date, stdlib::last_modified_date>(values<Int>(none, 1, 1, none, 2));
}

TEST_CASE("catalogue folds retain native tick and reset policies", "[codegen][catalogue]") {
    register_catalogue();
    const auto integers = values<Int>(none, 0, 3, 3, none, -2, 0);
    const auto floating = values<Float>(none, 0.0, 1.0, 1.0, none, -2.0, 0.0);
    check_parity<standard::sum, stdlib::sum_>(integers);
    check_parity<standard::sum, stdlib::sum_>(floating);
    check_parity<standard::sum_reset, stdlib::sum_>(integers, values<Bool>(true, none, false, none, true, none, false));
    check_parity<standard::sum_reset, stdlib::sum_>(floating, values<Bool>(true, none, false, none, true, none, false));
    check_parity<standard::mean, stdlib::mean>(integers);
    check_parity<standard::mean, stdlib::mean>(floating);
    check_parity<standard::min_, stdlib::min_>(values<Bool>(true, true, false, true));
    check_parity<standard::max_, stdlib::max_>(values<Bool>(false, false, true, false));
    check_parity<standard::min_, stdlib::min_>(integers);
    check_parity<standard::max_, stdlib::max_>(integers);
    check_parity<standard::min_, stdlib::min_>(floating);
    check_parity<standard::max_, stdlib::max_>(floating);
    check_parity<scalar::mean, stdlib::mean>(values<Int>(9000000000000000000LL), values<Int>(9000000000000000000LL));
    check_parity<scalar::contains_, stdlib::contains_>(values<Str>("abc", "", Str{"a\0b", 3}), values<Str>("bc", "", Str{"\0", 1}));
    check_parity<standard::dedup, stdlib::dedup>(values<Float>(0.0, 0.5e-15, 1e-15, 1.5e-15, 2e-15));
    check_parity<standard::dedup_float, stdlib::dedup>(values<Float>(0.0, 0.5, 1.0, none, 1.5, 1.5),
                                                       values<Float>(1.0, none, none, 0.1, -1.0, 0.0));
    check_parity<standard::if_true, stdlib::if_true>(values<Bool>(false, true, none, true, false, true));
    check_parity<standard::if_true, stdlib::if_true>(values<Bool>(false, true, none, true, false, true), true);
}

namespace
{
    struct ObserveInteger
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts) {
            wire<standard::null_sink>(w, ts);
            return ts;
        }
    };
    template <typename From, typename To> struct NativeConvert
    {
        static Port<TS<To>> compose(Wiring &w, Port<TS<From>> ts) { return wire<stdlib::convert, TS<To>>(w, ts); }
    };
}  // namespace
TEST_CASE("catalogue conversions use exposed native value functions", "[codegen][catalogue]") {
    register_catalogue();
    check_parity<standard::to_int, NativeConvert<Float, Int>>(values<Float>(none, 1.9, -1.9, 0.0));
    check_parity<standard::to_int, NativeConvert<Bool, Int>>(values<Bool>(false, true, true));
    check_parity<standard::to_float, NativeConvert<Int, Float>>(values<Int>(none, 0, -7, 9000000000000000000LL));
    check_parity<standard::to_float, NativeConvert<Bool, Float>>(values<Bool>(false, true, true));
    check_parity<standard::to_bool, NativeConvert<Int, Bool>>(values<Int>(0, 1, -1));
    check_parity<standard::to_bool, NativeConvert<Float, Bool>>(values<Float>(0.0, -0.0, 1.0));
    check_parity<standard::to_date, NativeConvert<DateTime, Date>>(values<DateTime>(DateTime{TimeDelta{-1}}, DateTime{}));
    check_parity<standard::to_datetime, NativeConvert<Date, DateTime>>(values<Date>(calendar_date(2024, 2, 29)));
    CHECK_OUTPUT(eval_node<standard::pass_through>(values<Int>(none, 0, 0, 1)), values<Int>(none, 0, 0, 1));
    CHECK_OUTPUT(eval_node<ObserveInteger>(values<Int>(none, 1, 2)), values<Int>(none, 1, 2));
    check_parity<scalar::pos_, stdlib::pos_>(values<TimeDelta>(TimeDelta{-1}, TimeDelta{0}));
}
TEST_CASE("catalogue membership preserves pre-valid and item resample semantics", "[codegen][catalogue]") {
    register_catalogue();
    const auto sets  = values<Value>(none, set_delta<Int>({1}, {}), set_delta<Int>({2}, {}), set_delta<Int>({}, {1}));
    const auto items = values<Int>(1, none, 1, none);
    CHECK_OUTPUT((eval_node<standard::contains_, TSS<Int>>(sets, items)), (eval_node<stdlib::contains_, TSS<Int>>(sets, items)));
    const auto subsets = values<Value>(set_delta<Int>({1}, {}), none, set_delta<Int>({2}, {}), set_delta<Int>({}, {1}));
    CHECK_OUTPUT((eval_node<standard::contains_, TSS<Int>, TSS<Int>>(sets, subsets)),
                 (eval_node<stdlib::contains_, TSS<Int>, TSS<Int>>(sets, subsets)));
    const auto maps = values<Value>(none, dict_delta<Str, TS<Float>>({{"a", 1.0}}), dict_delta<Str, TS<Float>>({{"a", 2.0}}),
                                    dict_delta<Str, TS<Float>>({}, {"a"}));
    const auto keys = values<Str>("a", none, "a", none);
    CHECK_OUTPUT((eval_node<standard::contains_, TSD<Str, TS<Float>>>(maps, keys)),
                 (eval_node<stdlib::contains_, TSD<Str, TS<Float>>>(maps, keys)));
    const auto lists   = values<Value>(none, list_delta<TS<Int>>({none, 2, 3}), list_delta<TS<Int>>({1, none, none}),
                                       list_delta<TS<Int>>({none, 3, none}));
    const auto needles = values<Int>(2, none, 2, 3);
    CHECK_OUTPUT((eval_node<standard::index_of, TSL<TS<Int>, 3>>(lists, needles)),
                 (eval_node<stdlib::index_of, TSL<TS<Int>, 3>>(lists, needles)));
}

namespace
{
    struct NativeCollectSet
    {
        static Port<TSS<Int>> compose(Wiring &w, Port<TS<Int>> ts) { return wire<stdlib::collect, TSS<Int>>(w, ts); }
    };
    struct NativeCollectMap
    {
        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, Port<TS<Str>> key, Port<TS<Int>> ts) {
            return wire<stdlib::collect, TSD<Str, TS<Int>>>(w, key, ts);
        }
    };
}  // namespace
TEST_CASE("catalogue accumulators retain memberships and publish native keyed ticks", "[codegen][catalogue]") {
    register_catalogue();
    const auto ticks = values<Int>(none, 1, 1, 2, 1);
    // Native duplicate adds publish an empty membership delta. HGL upsert
    // suppresses that event, so TSS collect remains a catalogue blocker.
    CHECK_OUTPUT(eval_node<NativeCollectSet>(ticks), values<Value>(none, set_delta<Int>({1}, {}), set_delta<Int>({}, {}),
                                                                   set_delta<Int>({2}, {}), set_delta<Int>({}, {})));
    const auto keys    = values<Str>("a", none, "a", "b", "a");
    const auto values_ = values<Int>(none, 1, 1, 2, 1);
    check_parity<standard::collect_map, NativeCollectMap>(keys, values_);
    check_parity<standard::make_tsd, stdlib::make_tsd>(keys, values_);
    check_parity<standard::make_tsd_remove, stdlib::make_tsd>(keys, values_, values<Bool>(none, none, true, none, false));
    check_parity<standard::make_tsd_remove, stdlib::make_tsd>(values<Str>("a", none, none, "b"), values<Int>(1, 2, 3, none),
                                                              values<Bool>(true, none, none, none));
    CHECK_OUTPUT(eval_node<standard::tick_count>(ticks), values<Int>(none, 1, 2, 3, 4));
}
