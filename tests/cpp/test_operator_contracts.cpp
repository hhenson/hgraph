// Native regressions for the runtime specification's operator contracts
// (docs/source/runtime_spec/operators.md, OP-1 to OP-11). Each case is the
// minimized recipe of a parity issue whose reasoned expectation matched
// released hgraph (runtime_spec/validation/parity); the Python-authored twin
// of each case is python/tests/test_operator_contracts.py.

#include <hgraph/lib/std/operators/impl/io_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using IntDict = TSD<Int, TS<Int>>;

    // io_write_slot takes a plain function pointer, so the buffer lives here.
    inline std::vector<std::string> printed_lines{};
    inline void capture_line(std::string_view line, bool) { printed_lines.emplace_back(line); }

    struct CapturedPrint
    {
        stdlib::IoWriteFn previous;
        CapturedPrint() : previous(stdlib::io_write_slot())
        {
            printed_lines.clear();
            stdlib::io_write_slot() = &capture_line;
        }
        ~CapturedPrint() { stdlib::io_write_slot() = previous; }
    };

    /** ``print_("v={}", ts)``, wired as print_'s compose wires it. */
    struct PrintValueGraph
    {
        static constexpr auto name = "operator_contracts_print_value";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto format = wire<stdlib::const_>(w, Str{"v={}"}).as<TS<Str>>();
            WiringPortRef packed = stdlib::io_impl_detail::pack_format_args({ts.erased()}, {});
            wire<stdlib::print_sink_op>(w, format, Port<void>{w, std::move(packed)}, Bool{true});
            return ts;
        }
    };

    /** ``print_("v={}", ts)`` through the public operator. */
    struct PublicPrintGraph
    {
        static constexpr auto name = "operator_contracts_public_print";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            wire<stdlib::print_>(w, Str{"v={}"}, ts);
            return ts;
        }
    };

    /** ``assert_(condition, "failed with {}", detail)`` through the public operator. */
    struct PublicAssertGraph
    {
        static constexpr auto name = "operator_contracts_public_assert";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Bool>> condition, Port<TS<Int>> detail)
        {
            wire<stdlib::assert_>(w, condition, Str{"failed with {}"}, detail);
            return condition;
        }
    };

    using InnerDict = TSD<Str, TS<Int>>;
    using NestedDict = TSD<Str, InnerDict>;

    /** ``convert[TSD[str, TSD[str, TS[int]]]](key, convert[TSD[str, TS[int]]](key, value))``. */
    struct NestedConvertGraph
    {
        static constexpr auto name = "operator_contracts_nested_convert";

        static Port<NestedDict> compose(Wiring &w, Port<TS<Int>> value, Port<TS<Str>> key)
        {
            auto inner = wire<stdlib::convert, InnerDict>(w, key, value);
            return wire<stdlib::convert, NestedDict>(w, key, inner);
        }
    };
}  // namespace

TEST_CASE("operator contracts: a TSD union forwards the most recent tick (OP-4, OP-5)")
{
    stdlib::register_standard_operators();

    // Parity #1069: rhs re-ticks a key lhs already holds. The most recent tick
    // wins; lhs had not ticked in that cycle, however stale its slot bit.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, -17}}), none, none),
                     values<Value>(none, none, dict_delta<Int, TS<Int>>({{1, -13}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, -17}}), none,
                               dict_delta<Int, TS<Int>>({{1, -13}})));

    // Parity #982: a forwarded tick of an equal value is still published.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, IntDict, IntDict>(
                     values<Value>(none, none, none, dict_delta<Int, TS<Int>>({{3, -19}})),
                     values<Value>(none, none, dict_delta<Int, TS<Int>>({{3, -19}}), none))),
                 values<Value>(none, none, dict_delta<Int, TS<Int>>({{3, -19}}),
                               dict_delta<Int, TS<Int>>({{3, -19}})));

    // A same-cycle tie goes to lhs.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}), none,
                                   dict_delta<Int, TS<Int>>({{1, 3}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                                   dict_delta<Int, TS<Int>>({{1, 20}}),
                                   dict_delta<Int, TS<Int>>({{1, 30}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                               dict_delta<Int, TS<Int>>({{1, 20}}),
                               dict_delta<Int, TS<Int>>({{1, 3}})));
}

TEST_CASE("operator contracts: TSD symmetric difference needs both operands (OP-6)")
{
    stdlib::register_standard_operators();

    // Parity #959: a never-ticked lhs is nil, not the empty dictionary.
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, IntDict, IntDict>(
                     values<Value>(none, none),
                     values<Value>(none, dict_delta<Int, TS<Int>>({{1, -19}})))),
                 values<Value>(none, none));

    // Keys that ticked before the other operand arrived backfill on admission.
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}), none),
                     values<Value>(none, dict_delta<Int, TS<Int>>({{2, 2}})))),
                 values<Value>(none, dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}})));

    // Parity #1040: key 1 changes holder from lhs to rhs in the cycle rhs
    // ticks it, so the equal value is forwarded.
    CHECK_OUTPUT((eval_node<stdlib::bit_xor, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{2, -13}}), none,
                                   dict_delta<Int, TS<Int>>({{1, -19}}), none,
                                   dict_delta<Int, TS<Int>>({}, {1})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, -19}}),
                                   dict_delta<Int, TS<Int>>({{2, -18}}),
                                   dict_delta<Int, TS<Int>>({{1, -19}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   dict_delta<Int, TS<Int>>({{1, -19}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, -19}, {2, -13}}),
                               dict_delta<Int, TS<Int>>({}, {2}),
                               dict_delta<Int, TS<Int>>({}, {1}),
                               dict_delta<Int, TS<Int>>({{1, -19}}),
                               dict_delta<Int, TS<Int>>({{1, -19}})));
}

TEST_CASE("operator contracts: a TSD difference validates on admission (OP-5)")
{
    stdlib::register_standard_operators();

    // Parity #961: every lhs key is also in rhs, so the first admitted result
    // is the empty dictionary, which still validates the output.
    CHECK_OUTPUT((eval_node<stdlib::sub_, IntDict, IntDict>(
                     values<Value>(dict_delta<Int, TS<Int>>({{3, 2}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{3, -17}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({})));
}

TEST_CASE("operator contracts: a nested child forwards only its own changes (OP-4)")
{
    stdlib::register_standard_operators();
    using Inner = TSD<Int, TS<Int>>;
    using Nested = TSD<Int, Inner>;

    // Codex review on #1627: forwarding the whole inner value re-ticked the
    // unchanged sibling 11; only 10 changed.
    CHECK_OUTPUT((eval_node<stdlib::bit_or, Nested, Nested>(
                     values<Value>(dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 1}, {11, 2}})}}),
                                   dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 5}})}})),
                     values<Value>(dict_delta<Int, Inner>({{2, dict_delta<Int, TS<Int>>({{20, 3}})}}), none))),
                 values<Value>(dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 1}, {11, 2}})},
                                                       {2, dict_delta<Int, TS<Int>>({{20, 3}})}}),
                               dict_delta<Int, Inner>({{1, dict_delta<Int, TS<Int>>({{10, 5}})}})));
}

TEST_CASE("operator contracts: an aggregate publishes nothing over an invalid collection (OP-2)")
{
    stdlib::register_standard_operators();
    using IntList = TSL<TS<Int>, 2>;

    // Parity #1476 / #1538: no element of the list ever ticks.
    CHECK_OUTPUT((eval_node<stdlib::sum_, IntList>(values<Value>(none, none))), values<Int>(none, none));
    // Admitted once one element is valid; the valid elements are read.
    CHECK_OUTPUT((eval_node<stdlib::sum_, IntList>(values<Value>(none, list_delta<TS<Int>>({{1, 5}})))),
                 values<Int>(none, 5));

    // A never-ticked set is not the empty set: nothing, not 0 or a default.
    CHECK_OUTPUT((eval_node<stdlib::sum_, TSS<Int>>(values<Value>(none, none))), values<Int>(none, none));
    CHECK_OUTPUT((eval_node<stdlib::sum_, TSS<Int>>(values<Value>(set_delta<Int>({}, {}), none))),
                 values<Int>(0, none));
    CHECK_OUTPUT((eval_node<stdlib::sum_, TSD<Int, TS<Int>>>(values<Value>(none, none))),
                 values<Int>(none, none));
}

TEST_CASE("operator contracts: all_ and any_ publish nothing before an argument is valid (OP-2)")
{
    stdlib::register_standard_operators();

    // Parity #1181, #1246, #1355, #1494.
    CHECK_OUTPUT(eval_node<stdlib::all_>(values<Bool>(none), values<Bool>(none), values<Bool>(none)),
                 values<Bool>(none));
    CHECK_OUTPUT(eval_node<stdlib::any_>(values<Bool>(none), values<Bool>(none)), values<Bool>(none));
    // An argument not yet valid reads as released hgraph's None: falsy.
    CHECK_OUTPUT(eval_node<stdlib::all_>(values<Bool>(true, true), values<Bool>(none, true)),
                 values<Bool>(false, true));
    CHECK_OUTPUT(eval_node<stdlib::any_>(values<Bool>(none, false), values<Bool>(true, none)),
                 values<Bool>(true, true));
}

TEST_CASE("operator contracts: print_ waits for every argument (OP-8)")
{
    stdlib::register_standard_operators();

    // Parity #1122, #1339, #1564, #1613: the argument never ticks, so nothing
    // is printed -- no placeholder line.
    {
        CapturedPrint capture;
        static_cast<void>(eval_node<PrintValueGraph>(values<Int>(none, none)));
        CHECK(printed_lines.empty());
    }
    // Once the argument is valid the format ticks print as usual.
    {
        CapturedPrint capture;
        static_cast<void>(eval_node<PrintValueGraph>(values<Int>(none, 5, 6)));
        CHECK(printed_lines == std::vector<std::string>{"v=5", "v=6"});
    }
}

TEST_CASE("operator contracts: public print_ and assert_ wait for their arguments (OP-8)")
{
    stdlib::register_standard_operators();
    {
        CapturedPrint capture;
        static_cast<void>(eval_node<PublicPrintGraph>(values<Int>(none, 5, 6)));
        CHECK(printed_lines == std::vector<std::string>{"v=5", "v=6"});
    }
    // A failing condition whose argument is not yet valid raises nothing.
    CHECK_OUTPUT(eval_node<PublicAssertGraph>(values<Bool>(false), values<Int>(none)), values<Bool>(false));
    CHECK_THROWS(eval_node<PublicAssertGraph>(values<Bool>(true, false), values<Int>(none, 3)));
}

TEST_CASE("operator contracts: a nested entry keeps an invalid child invalid")
{
    stdlib::register_standard_operators();

    // Parity #963-#965 (the TSD row of the time-series value/delta table): the
    // inner dictionary holds key "c" with a child that never ticks, so the
    // outer delta holds "c" with an empty inner delta -- never a default 0.
    CHECK_OUTPUT((eval_node<NestedConvertGraph>(values<Int>(none, 5), values<Str>(Str{"c"}, none))),
                 values<Value>(dict_delta<Str, InnerDict>({{Str{"c"}, dict_delta<Str, TS<Int>>({})}}),
                               dict_delta<Str, InnerDict>(
                                   {{Str{"c"}, dict_delta<Str, TS<Int>>({{Str{"c"}, 5}})}})));
}

TEST_CASE("operator contracts: a string in a container renders as Python's repr (OP-9)")
{
    using value_ops_detail::quote_string;

    // Parity #1062: a C1 control, a vertical tab and an unassigned plane-8
    // code point are all escaped, each in Python's shortest form.
    CHECK(quote_string("\xc2\x87") == "'\\x87'");
    CHECK(quote_string("\x0b\xf2\x8c\xa9\xbd") == "'\\x0b\\U0008ca7d'");
    // Parity #960: printable letters stay, an unassigned plane-6 code point does not.
    CHECK(quote_string("\xc3\x81\xf1\xa3\xae\xba\xc3\x9e\xc3\x86") == "'\xc3\x81\\U00063bba\xc3\x9e\xc3\x86'");
    // Separators, format characters and DEL.
    CHECK(quote_string("a\xc2\xa0" "b") == "'a\\xa0b'");
    CHECK(quote_string("\xe2\x80\x8d") == "'\\u200d'");
    CHECK(quote_string("\xe2\x80\xa8") == "'\\u2028'");
    CHECK(quote_string("\x7f") == "'\\x7f'");
    // Printable text, named escapes and Python's choice of quote are unchanged.
    CHECK(quote_string("caf\xc3\xa9 \xf0\x9f\x98\x80") == "'caf\xc3\xa9 \xf0\x9f\x98\x80'");
    CHECK(quote_string("a\tb\nc\\") == "'a\\tb\\nc\\\\'");
    CHECK(quote_string("it's") == "\"it's\"");
    CHECK(quote_string("it's \"x\"\xc2\x87") == "'it\\'s \"x\"\\x87'");
    // Bytes that are not UTF-8 -- here an encoded surrogate -- show as bytes.
    CHECK(quote_string("\xed\xa0\x80") == "'\\xed\\xa0\\x80'");
}

TEST_CASE("operator contracts: a map's text has its members in no specified order (OP-9)")
{
    stdlib::register_standard_operators();

    // Owner ruling 2026-09-24 (parity #1082): after "a" is removed and added
    // again the rendering may list it first or last; its members are fixed.
    const auto rendered = eval_node<stdlib::str_, TSD<Str, TS<Int>>>(values<Value>(
        dict_delta<Str, TS<Int>>({{Str{"a"}, 2}}),
        dict_delta<Str, TS<Int>>({{Str{"b"}, -19}}, {Str{"a"}}),
        dict_delta<Str, TS<Int>>({{Str{"a"}, -19}})));
    REQUIRE(rendered.size() == 3);
    CHECK(rendered[0]->view().checked_as<Str>() == "{'a': 2}");
    CHECK(rendered[1]->view().checked_as<Str>() == "{'b': -19}");
    const Str last = rendered[2]->view().checked_as<Str>();
    CHECK((last == "{'a': -19, 'b': -19}" || last == "{'b': -19, 'a': -19}"));
}
