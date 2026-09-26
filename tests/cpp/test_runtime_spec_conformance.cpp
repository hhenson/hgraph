#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using Dict = TSD<Str, TS<Int>>;

    struct MembershipSource
    {
        static void eval(In<"step", TS<Int>> step, Out<Dict> out)
        {
            const Str key{"X"};
            switch (step.value())
            {
                case 1: static_cast<void>(out.at(key)); break;
                case 2: case 6: out.set(key, Int{7}); break;
                case 3: {
                    auto child = out.at(key);
                    static_cast<void>(child.begin_mutation(child.evaluation_time()).invalidate());
                    break;
                }
                case 4: static_cast<void>(out.erase(key)); break;
            }
        }
    };

    struct ObserveMembership
    {
        static void eval(In<"step", TS<Int>> step,
                         In<"ts", Dict, InputValidity::Unchecked, InputActivity::Passive> ts,
                         Out<TS<Int>> out)
        {
            const auto n = step.value();
            std::size_t added = 0, removed = 0;
            for ([[maybe_unused]] const auto &key : ts.added_keys()) { ++added; }
            for ([[maybe_unused]] const auto &key : ts.removed_keys()) { ++removed; }
            CHECK(added == (n == 1 || n == 6 ? 1 : 0));
            CHECK(removed == (n == 4 ? 1 : 0));
            CHECK(ts.data_view().key_set().modified(ts.evaluation_time()) == (n == 1 || n == 4 || n == 6));
            auto key_set = ts.data_view().key_set();
            if (key_set.modified(ts.evaluation_time()))
            {
                CHECK(std::ranges::distance(key_set.added()) == (n == 4 ? 0 : 1));
                CHECK(std::ranges::distance(key_set.removed()) == (n == 4 ? 1 : 0));
                const auto delta = key_set.base().delta_value(ts.evaluation_time());
                CHECK(delta.as_indexed().at(0).as_set().size() == (n == 4 ? 0 : 1));
                CHECK(delta.as_indexed().at(1).as_set().size() == (n == 4 ? 1 : 0));
            }
            out.set(n);
        }
    };

    struct MembershipGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step)
        {
            return wire<ObserveMembership>(w, step, wire<MembershipSource>(w, step)).as<TS<Int>>();
        }
    };

    template <bool Right>
    struct DictionarySource
    {
        static void eval(In<"step", TS<Int>> step, Out<Dict> out)
        {
            if (step.value() == 0)
            {
                out.set(Str{Right ? "Y" : "X"}, Int{Right ? 2 : 1});
                out.set(Str{"Z"}, Int{Right ? 3 : 9});
            }
            if constexpr (Right) { if (step.value() == 1) { out.set(Str{"Y"}, Int{4}); } }
        }
    };

    struct SelectDictionary
    {
        static void eval(In<"step", TS<Int>> step,
                         In<"a", REF<Dict>, InputActivity::Passive> a,
                         In<"b", REF<Dict>, InputActivity::Passive> b, Out<REF<Dict>> out)
        {
            if (step.value() == 0 || step.value() == 4) { out.set(a.value()); }
            if (step.value() == 2) { out.set(b.value()); }
            if (step.value() == 3) { out.set(TimeSeriesReference::empty()); }
        }
    };

    struct ObserveDictionary
    {
        static void eval(In<"step", TS<Int>> step,
                         In<"ts", Dict, InputValidity::Unchecked, InputActivity::Passive> ts,
                         Out<TS<Int>> out)
        {
            const auto n = step.value();
            if (n == 2 || n == 4)
            {
                for (const auto &[key, child] : ts.items())
                {
                    static_cast<void>(key);
                    CHECK(child.modified());
                    CHECK(child.last_modified_time() == ts.evaluation_time());
                }
            }
            if (n == 2)
            {
                CHECK(capture_delta(ts.base()).equals(dict_delta<Str, TS<Int>>({{"Y", 4}, {"Z", 3}}, {"X"})));
                std::size_t removed = 0;
                for (const auto &[key, child] : ts.removed_items())
                {
                    CHECK(key.checked_as<Str>() == "X");
                    CHECK(child.value() == 1);
                    ++removed;
                }
                CHECK(removed == 1);
            }
            if (n == 3)
            {
                CHECK_FALSE(ts.valid());
                CHECK(ts.modified());
                CHECK(ts.last_modified_time() == MIN_DT);
                CHECK(capture_delta(ts.base()).equals(dict_delta<Str, TS<Int>>({}, {"Y", "Z"})));
                std::size_t removed = 0;
                for (const auto &[key, child] : ts.removed_items())
                {
                    CHECK(child.value() == (key.checked_as<Str>() == "Y" ? 4 : 3));
                    ++removed;
                }
                CHECK(removed == 2);
            }
            out.set(n);
        }
    };

    struct DictionaryGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step)
        {
            auto a = wire<DictionarySource<false>>(w, step);
            auto b = wire<DictionarySource<true>>(w, step);
            return wire<ObserveDictionary>(w, step, wire<SelectDictionary>(w, step, a, b)).as<TS<Int>>();
        }
    };

    struct SelectScalar
    {
        static void eval(In<"step", TS<Int>> step, In<"value", REF<TS<Int>>> value, Out<REF<TS<Int>>> out)
        {
            out.set(step.value() == 0 ? value.value() : TimeSeriesReference::empty());
        }
    };

    struct ObserveScalar
    {
        static void eval(In<"step", TS<Int>> step,
                         In<"value", TS<Int>, InputValidity::Unchecked, InputActivity::Passive> value, Out<TS<Int>> out)
        {
            CHECK(value.valid() == (step.value() == 0));
            CHECK(value.modified() == (step.value() == 0));
            CHECK(value.last_modified_time() == (step.value() == 0 ? MIN_ST : MIN_DT));
            out.set(step.value());
        }
    };

    struct ScalarReferenceGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step, Port<TS<Int>> value)
        {
            return wire<ObserveScalar>(w, step, wire<SelectScalar>(w, step, value)).as<TS<Int>>();
        }
    };

    struct SaveReference
    {
        static void eval(In<"step", TS<Int>> step,
                         In<"ref", REF<TS<Int>>, InputValidity::Unchecked, InputActivity::Passive> ref,
                         State<TimeSeriesReference> saved, Out<REF<TS<Int>>> out)
        {
            if (step.value() == 0) { saved.set(ref.value()); }
            if (step.value() == 0 || step.value() == 3 || step.value() == 5) { out.set(saved.get()); }
        }
    };

    struct ObserveSavedReference
    {
        static void eval(In<"step", TS<Int>> step,
                         In<"ts", TS<Int>, InputValidity::Unchecked, InputActivity::Passive> ts,
                         Out<TS<Int>> out)
        {
            CHECK(ts.modified() == (step.value() == 0));
            out.set(ts.valid() ? ts.value() : Int{-1});
        }
    };

    struct SavedReferenceGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step, Port<Dict> values)
        {
            auto ref = wire<stdlib::getitem_>(w, values, Str{"X"});
            return wire<ObserveSavedReference>(w, step, wire<SaveReference>(w, step, ref)).as<TS<Int>>();
        }
    };
}

TEST_CASE("Runtime rules: membership and key set observations through native wiring", "[runtime-contract]")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<MembershipGraph>({0, 1, 2, 3, 4, 5, 6, 7}), {0, 1, 2, 3, 4, 5, 6, 7});
}

TEST_CASE("Runtime rules: dictionary sampling and withdrawal through native wiring", "[runtime-contract]")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<DictionaryGraph>({0, 1, 2, 3, 4, 5}), {0, 1, 2, 3, 4, 5});
}

TEST_CASE("Runtime rules: saved reference expires while dictionary reclamation remains lazy", "[runtime-contract]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<SavedReferenceGraph>(values<Int>(0, 1, 2, 3, 4, 5, 6),
        values<Value>(dict_delta<Str, TS<Int>>({{"X", 7}}), none,
            dict_delta<Str, TS<Int>>({}, {"X"}), none, dict_delta<Str, TS<Int>>({{"X", 9}}), none, none)),
        {7, 7, 7, -1, -1, -1, -1});
}

TEST_CASE("Runtime rules: scalar reference withdrawal resets observed time through native wiring", "[runtime-contract]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    CHECK_OUTPUT(eval_node<ScalarReferenceGraph>(values<Int>(0, 1), values<Int>(7, none)), {0, 1});
}
