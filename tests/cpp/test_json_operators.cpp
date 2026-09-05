#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/counted_mutex.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/temporal.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <string>

// JSON serialization — step 1 of the record/replay/table design record: the
// interned per-schema JsonConverter (serializer-ops pattern) plus the
// to_json/from_json operators. The wire format mirrors the Python
// implementation (release/0.5:hgraph/_impl/_operators/_to_json.py).

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    DateTime utc_instant(
        int year, unsigned month, unsigned day, int hour = 0,
        int minute = 0, int second = 0, int microsecond = 0)
    {
        using namespace std::chrono;
        const Date date{
            std::chrono::year{year}, std::chrono::month{month},
            std::chrono::day{day}};
        return DateTime{
            duration_cast<microseconds>(sys_days{date}.time_since_epoch()) +
            hours{hour} + minutes{minute} + seconds{second} +
            microseconds{microsecond}};
    }

    struct FromJsonGraph
    {
        [[maybe_unused]] static constexpr auto name = "from_json_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TS<Int>>(w, ts).as<TS<Int>>();
        }
    };

    struct FromJsonDateTimeGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "from_json_datetime_graph";

        static Port<TS<DateTime>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TS<DateTime>>(w, ts)
                .as<TS<DateTime>>();
        }
    };

    struct FromJsonFixedListGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "from_json_fixed_list_graph";

        static Port<TSL<TS<Int>, 2>> compose(Wiring &w,
                                              Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TSL<TS<Int>, 2>>(w, ts)
                .as<TSL<TS<Int>, 2>>();
        }
    };

    struct FromJsonDynamicListGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "from_json_dynamic_list_graph";

        static Port<TSL<TS<Int>>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TSL<TS<Int>>>(w, ts).as<TSL<TS<Int>>>();
        }
    };

    struct DynamicListJsonRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "dynamic_list_json_round_trip_graph";

        static Port<TSL<TS<Int>>> compose(Wiring &w, Port<TSL<TS<Int>>> ts)
        {
            auto text = wire<stdlib::to_json>(w, ts, true).as<TS<Str>>();
            return wire<stdlib::from_json, TSL<TS<Int>>>(w, text).as<TSL<TS<Int>>>();
        }
    };

    struct FromJsonSetGraph
    {
        [[maybe_unused]] static constexpr auto name = "from_json_set_graph";

        static Port<TSS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            return wire<stdlib::from_json, TSS<Int>>(w, ts).as<TSS<Int>>();
        }
    };

    struct JsonRoundTripGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_round_trip_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> ts)
        {
            auto text = wire<stdlib::to_json>(w, ts);
            return wire<stdlib::from_json, TS<Float>>(w, text).as<TS<Float>>();
        }
    };

    struct JsonDynamicLeafGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_leaf_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            auto decoded = wire<stdlib::json_decode>(w, ts);
            auto nested  = wire<stdlib::getitem_>(w, decoded, Str{"nested"});
            auto values  = wire<stdlib::getitem_>(w, nested, Str{"values"});
            auto last    = wire<stdlib::getitem_>(w, values, Int{-1});
            return wire<stdlib::json_as_int>(w, last).as<TS<Int>>();
        }
    };

    struct JsonDynamicEncodeGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_encode_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> ts)
        {
            auto decoded = wire<stdlib::json_decode>(w, ts);
            return wire<stdlib::json_encode, TS<Str>>(w, decoded).as<TS<Str>>();
        }
    };

    struct JsonDynamicEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_equality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> lhs, Port<TS<Str>> rhs)
        {
            auto decoded_lhs = wire<stdlib::json_decode>(w, lhs);
            auto decoded_rhs = wire<stdlib::json_decode>(w, rhs);
            return wire<stdlib::eq_>(w, decoded_lhs, decoded_rhs).as<TS<Bool>>();
        }
    };

    [[nodiscard]] Value eager_reference_json()
    {
        const auto &str_binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Str>::value_meta());

        MapBuilder target_entries{str_binding, stdlib::json_tree::json_value_binding()};
        Value      answer_key{Str{"answer"}};
        Value      answer_node = stdlib::json_tree::box(Value{Int{41}});
        target_entries.set_item_copy(answer_key.view().data(), answer_node.view().data());

        MapBuilder root_entries{str_binding, stdlib::json_tree::json_value_binding()};
        Value      target_key{Str{"target"}};
        Value      target_node = stdlib::json_tree::box(target_entries.build());
        root_entries.set_item_copy(target_key.view().data(), target_node.view().data());
        return stdlib::json_tree::box(root_entries.build());
    }

    struct EagerJsonReferenceNode
    {
        static constexpr auto name = "eager_json_reference_node";

        static void resolve_default_types(ResolutionMap &resolution)
        {
            if (resolution.find_ts("O") != nullptr) { return; }
            resolution.bind_ts("O", TypeRegistry::instance().ts(stdlib::json_tree::json_meta()));
        }

        static void eval(In<"trigger", TS<Str>> trigger, Out<TsVar<"O">> out)
        {
            static_cast<void>(trigger);
            stdlib::json_tree::publish(static_cast<const TSOutputView &>(out), eager_reference_json());
        }
    };

    struct JsonDynamicLazyEagerEqualityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_equality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::eq_>(w, decoded, eager).as<TS<Bool>>();
        }
    };

    struct JsonDynamicLazyEagerInequalityGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_inequality_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::ne_>(w, decoded, eager).as<TS<Bool>>();
        }
    };

    struct JsonDynamicLazyEagerCompareGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_dynamic_lazy_eager_compare_graph";

        static Port<TS<stdlib::CmpResult>> compose(Wiring &w, Port<TS<Str>> raw)
        {
            auto decoded = wire<stdlib::json_decode>(w, raw);
            auto eager   = wire<EagerJsonReferenceNode>(w, raw);
            return wire<stdlib::cmp_>(w, decoded, eager).as<TS<stdlib::CmpResult>>();
        }
    };

    // if_then_else publishes a REFERENCE, and combine_json packs its keyword
    // arguments into a structural bundle it then serializes. Wired natively,
    // so the deref rule is exercised through C++ dispatch rather than only
    // through the Python bridge.
    struct CombineJsonReferenceGraph
    {
        [[maybe_unused]] static constexpr auto name = "combine_json_reference_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Bool>> choose_lhs, Port<TS<Int>> lhs,
                                     Port<TS<Int>> rhs)
        {
            auto selected = wire<stdlib::if_then_else>(w, choose_lhs, lhs, rhs);
            auto combined = wire<stdlib::combine_json>(w, arg<"v">(selected));
            return wire<stdlib::json_encode, TS<Str>>(w, combined).as<TS<Str>>();
        }
    };

    struct CombineJsonArrayGraph
    {
        [[maybe_unused]] static constexpr auto name = "combine_json_array_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> number,
                                     Port<TS<Str>> text)
        {
            auto combined = wire<stdlib::combine_json>(w, number, text);
            return wire<stdlib::json_encode, TS<Str>>(w, combined).as<TS<Str>>();
        }
    };

    struct JsonArrayAddGraph
    {
        [[maybe_unused]] static constexpr auto name = "json_array_add_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> encoded,
                                     Port<TS<Int>> item)
        {
            auto lhs = wire<stdlib::json_decode>(w, encoded);
            auto rhs = wire<stdlib::combine_json>(w, item);
            auto sum = wire<stdlib::add_>(w, lhs, rhs);
            return wire<stdlib::json_encode, TS<Str>>(w, sum).as<TS<Str>>();
        }
    };

    struct InvalidJsonArrayAddGraph
    {
        [[maybe_unused]] static constexpr auto name = "invalid_json_array_add_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> lhs_text,
                                     Port<TS<Str>> rhs_text)
        {
            auto lhs = wire<stdlib::json_decode>(w, lhs_text);
            auto rhs = wire<stdlib::json_decode>(w, rhs_text);
            auto sum = wire<stdlib::add_>(w, lhs, rhs);
            return wire<stdlib::json_encode, TS<Str>>(w, sum).as<TS<Str>>();
        }
    };
}  // namespace

TEST_CASE("json operators: to_json serializes per tick")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(42, none, -1)),
                 values<Str>(Str{"42"}, none, Str{"-1"}));
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Str>(Str{"a\"b"})), values<Str>(Str{"\"a\\\"b\""}));
    // The delta of a TS<scalar> IS its value: delta mode matches value mode.
    CHECK_OUTPUT(eval_node<stdlib::to_json>(values<Int>(7), true), values<Str>(Str{"7"}));
}

TEST_CASE("json operators: from_json parses into the resolved output type")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FromJsonGraph>(values<Str>(Str{"3"}, none, Str{"-9"})),
                 values<Int>(3, none, -9));
}

TEST_CASE("json operators: from_json converts compact JSON arrays into fixed TSL storage")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<FromJsonFixedListGraph>(
            values<Str>(Str{"[1, 2]"})),
        values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}})));
}

TEST_CASE("json operators: a bare TSS array adds without removing")
{
    // release/0.5 parity: from_json applies the payload as a DELTA, so a bare
    // array ADDS its members and leaves absent members alone. Applying it as a
    // whole-set replace silently dropped membership between ticks - 1 and 2
    // were removed on the second tick.
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<FromJsonSetGraph>(values<Str>(Str{"[1, 2]"}, Str{"[3]"})),
        values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {})));
    // Removal stays available through the explicit delta form.
    CHECK_OUTPUT(
        eval_node<FromJsonSetGraph>(
            values<Str>(Str{"[1, 2]"}, Str{"{\"removed\": [2]}"})),
        values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {2})));
}

TEST_CASE("json operators: a null TSL element does not tick")
{
    // release/0.5 parity: null means "this element has no value this tick".
    // Parsing the array as one value rejected null against a typed element, so
    // the whole call failed rather than ticking element 1 alone.
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<FromJsonFixedListGraph>(
            values<Str>(Str{"[1, 2]"}, Str{"[null, 9]"})),
        values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                      list_delta<TS<Int>>({{1, 9}})));
}

TEST_CASE("json operators: a dynamic TSL array sets the list length")
{
    // RFC 0031: the array IS the list, so a shorter array truncates it and the
    // removed indices show up in the delta.
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<FromJsonDynamicListGraph>(
            values<Str>(Str{"[1, 2, 3]"}, Str{"[null, 9]"}, Str{"[]"})),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}, {2, 3}}),
                      dynamic_list_delta<TS<Int>>({{1, 9}}, {2}),
                      dynamic_list_delta<TS<Int>>({}, {0, 1})));
}

TEST_CASE("json operators: a dynamic TSL delta round-trips its removals")
{
    // The object form is the canonical {"removed", "modified"} delta, so
    // to_json -> from_json reproduces a truncation (RFC 0031).
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<DynamicListJsonRoundTripGraph>(
            values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                          dynamic_list_delta<TS<Int>>({{0, 7}}, {1}))),
        values<Value>(dynamic_list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                      dynamic_list_delta<TS<Int>>({{0, 7}}, {1})));
}

TEST_CASE("json operators: temporal parsing uses the shared native format registry")
{
    stdlib::register_standard_operators();
    register_json_datetime_format("%d/%m/%Y %H:%M:%S");
    CHECK_OUTPUT(
        eval_node<FromJsonDateTimeGraph>(values<Str>(
            Str{"\"2024-06-13T11:15:30+01:00\""},
            Str{"\"13/06/2024 10:15:30\""})),
        values<DateTime>(
            utc_instant(2024, 6, 13, 10, 15, 30),
            utc_instant(2024, 6, 13, 10, 15, 30)));
}

TEST_CASE("json operators: to_json -> from_json round-trips through a graph")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonRoundTripGraph>(values<Float>(1.5, none, -0.25)),
                 values<Float>(1.5, none, -0.25));
}

TEST_CASE("dynamic json operators: decoded values support lazy path extraction")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicLeafGraph>(
                     values<Str>(Str{"{\"nested\":{\"values\":[1,2,3,4]}}"},
                                 Str{"{\"nested\":{\"values\":[5,6]}}"})),
                 values<Int>(4, 6));
}

TEST_CASE("dynamic json operators: decoded values encode canonically")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"a\":1,\"b\":[true,null,\"x\"]}"})),
                 values<Str>(Str{"{\"a\": 1, \"b\": [true, null, \"x\"]}"}));
}

TEST_CASE("dynamic json operators: re-encoding escapes control characters")
{
    stdlib::register_standard_operators();
    // Regression: the dynamic encoder kept its own escaper which handled only
    // " \\ \n \t \r and emitted every other sub-0x20 byte RAW — invalid JSON
    // per RFC 8259, and unparseable on the way back. Both encoders now share
    // json_detail::append_escaped. Values AND keys go through it.
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"a\":\"x\\u0008y\"}"})),
                 values<Str>(Str{"{\"a\": \"x\\by\"}"}));
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"a\":\"x\\u000cy\"}"})),
                 values<Str>(Str{"{\"a\": \"x\\fy\"}"}));
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"a\":\"x\\u001fy\"}"})),
                 values<Str>(Str{"{\"a\": \"x\\u001fy\"}"}));
    CHECK_OUTPUT(eval_node<JsonDynamicEncodeGraph>(
                     values<Str>(Str{"{\"k\\u0009k\":1}"})),
                 values<Str>(Str{"{\"k\\tk\": 1}"}));
}

TEST_CASE("dynamic json operators: equality is semantic not raw string equality")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicEqualityGraph>(
                     values<Str>(Str{"{\"a\":1,\"b\":[2,3]}"}),
                     values<Str>(Str{"{ \"b\" : [2, 3], \"a\" : 1 }"})),
                 values<Bool>(true));
}

TEST_CASE("dynamic json operators: combine serializes the value behind a reference")
{
    stdlib::register_standard_operators();
    // Without the deref rule the ref token reaches the encoder: natively that
    // raises "JSON tree: unsupported node content", while the same graph
    // through the Python bridge silently serialized "<ref>" (see
    // python/tests/ported/_operators/test_json.py). Same defect, and only one
    // of the two ends up looking like data - which is why both are covered.
    CHECK_OUTPUT(eval_node<CombineJsonReferenceGraph>(values<Bool>(true), values<Int>(8), values<Int>(-6)),
                 values<Str>(Str{"{\"v\": 8}"}));
    CHECK_OUTPUT(eval_node<CombineJsonReferenceGraph>(values<Bool>(false), values<Int>(8), values<Int>(-6)),
                 values<Str>(Str{"{\"v\": -6}"}));
}

TEST_CASE("dynamic json operators: positional combine builds an array")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<CombineJsonArrayGraph>(values<Int>(1), values<Str>(Str{"text"})),
        values<Str>(Str{"[1, \"text\"]"}));
}

TEST_CASE("dynamic json operators: add concatenates lazy and eager arrays")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<JsonArrayAddGraph>(
            values<Str>(Str{"[1, {\"a\": 2}]"}), values<Int>(3)),
        values<Str>(Str{"[1, {\"a\": 2}, 3]"}));
}

TEST_CASE("dynamic json operators: add rejects non-array operands")
{
    stdlib::register_standard_operators();

    SECTION("left operand")
    {
        REQUIRE_THROWS_WITH(
            eval_node<InvalidJsonArrayAddGraph>(
                values<Str>(Str{"{\"a\": 1}"}), values<Str>(Str{"[]"})),
            Catch::Matchers::ContainsSubstring(
                "JSON array concatenation requires array operands; left operand"));
    }

    SECTION("right operand")
    {
        REQUIRE_THROWS_WITH(
            eval_node<InvalidJsonArrayAddGraph>(
                values<Str>(Str{"[]"}), values<Str>(Str{"{\"a\": 1}"})),
            Catch::Matchers::ContainsSubstring(
                "JSON array concatenation requires array operands; right operand"));
    }
}

TEST_CASE("dynamic json operators: equality is independent of lazy or eager storage")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerEqualityGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"},
                                 Str{"{\"target\":{\"answer\":42}}"})),
                 values<Bool>(true, false));
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerInequalityGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"},
                                 Str{"{\"target\":{\"answer\":42}}"})),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<JsonDynamicLazyEagerCompareGraph>(
                     values<Str>(Str{"{\"target\":{\"answer\":41}}"})),
                 values<stdlib::CmpResult>(stdlib::CmpResult::EQ));
}
