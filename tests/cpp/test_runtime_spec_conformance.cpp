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
            const bool present = n == 1 || n == 2 || n == 3 || n >= 6;
            CHECK(ts.contains(Str{"X"}) == present);
            if (present) {
                const auto child = ts.at(Str{"X"});
                const bool valid = n == 2 || n >= 6;
                CHECK(child.valid() == valid);
                if (valid) { CHECK(child.value() == 7); }
            }
            CHECK_FALSE(ts.invalidated());
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

    struct RecordMembershipGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step)
        {
            const auto book = wire<MembershipSource>(w, step);
            wire<stdlib::record>(w, book, Str{"book"}, arg<"recordable_id">(Str{"members"}));
            return wire<ObserveMembership>(w, step, book);
        }
    };

    struct ReplayMembershipGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step)
        {
            const auto book = wire<stdlib::replay, Dict>(w, Str{"book"}, arg<"recordable_id">(Str{"members"}));
            return wire<ObserveMembership>(w, step, book);
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
            CHECK_FALSE(value.invalidated());
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

    enum ScalarAction : Int { Publish, Clear, ClearTwice, PublishThenClear, ClearThenPublish };

    struct ApplyScalarAction
    {
        static void eval(In<"action", TS<Int>> action, Out<TS<Int>> out)
        {
            CHECK_FALSE(out.invalidated());

            const auto publish = [&] {
                out.set(Int{42});
                CHECK_FALSE(out.invalidated());
                CHECK(out.modified());
            };
            const auto clear = [&] {
                const bool was_valid = out.valid();
                const bool cleared_this_cycle = out.invalidated();
                REQUIRE(out.begin_mutation(out.evaluation_time()).invalidate() == was_valid);
                CHECK(out.invalidated() == (was_valid || cleared_this_cycle));
                CHECK_FALSE(out.modified());
                CHECK(out.last_modified_time() == MIN_DT);
            };

            switch (action.value())
            {
                case Publish: publish(); break;
                case Clear: clear(); break;
                case ClearTwice: clear(); clear(); break;
                case PublishThenClear: publish(); clear(); break;
                case ClearThenPublish: clear(); publish(); break;
            }
        }
    };

    template <typename Trigger, InputActivity Activity = InputActivity::Passive>
    struct PriceOnScalarPublication
    {
        static void eval(In<"clock", TS<Bool>>,
                         In<"trigger", Trigger, InputValidity::Unchecked, Activity> trigger,
                         In<"price", TS<Int>, InputActivity::Passive> price, Out<TS<Int>> out)
        {
            if constexpr (std::is_same_v<Trigger, SIGNAL>) { CHECK_FALSE(trigger.invalidated()); }
            if (trigger.modified()) { out.set(price.value()); }
        }
    };

    template <typename Trigger, InputActivity Activity = InputActivity::Passive>
    struct DirectScalarPriceLatch
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Bool>> clock, Port<TS<Int>> price)
        {
            return wire<PriceOnScalarPublication<Trigger, Activity>>(w, clock, wire<ApplyScalarAction>(w, action), price);
        }
    };

    template <typename Trigger>
    struct SelectedScalarPriceLatch
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Int>> selection,
                                     Port<TS<Bool>> clock, Port<TS<Int>> price)
        {
            auto source = wire<ApplyScalarAction>(w, action);
            auto ref = wire<SelectScalar>(w, selection, source);
            return wire<PriceOnScalarPublication<Trigger>>(w, clock, ref, price);
        }
    };

    struct SampleOnScalarPublication
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Int>> price)
        {
            return wire<stdlib::sample>(w, wire<ApplyScalarAction>(w, action), price).as<TS<Int>>();
        }
    };

    template <InputActivity Activity>
    struct ObserveScalarInvalidation
    {
        static void eval(In<"data", TS<Int>, InputValidity::Unchecked, Activity> data,
                         In<"probe", TS<Bool>, InputValidity::Unchecked>, Out<TS<Bool>> out)
        {
            const bool invalidated = data.invalidated();
            if (invalidated)
            {
                CHECK_FALSE(data.valid());
                CHECK_FALSE(data.modified());
                CHECK(data.last_modified_time() == MIN_DT);
            }
            out.set(invalidated);
        }
    };

    template <InputActivity Activity>
    struct ScalarInvalidationGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Bool>> probe)
        {
            return wire<ObserveScalarInvalidation<Activity>>(w, wire<ApplyScalarAction>(w, action), probe);
        }
    };

    using StampSet = TSS<Int>;
    using StampDict = TSD<Int, TS<Int>>;
    using StampFixedList = TSL<TS<Int>, 1>;
    using StampGrowingList = TSL<TS<Int>>;
    using StampBundle = UnNamedTSB<Field<"value", TS<Int>>>;
    using StampWindow = TSW<Int, 2, 1>;

    template <typename Owner>
    struct ApplyOwnerAction
    {
        static void eval(In<"action", TS<Int>> action, Out<Owner> out)
        {
            const Int value = action.value();
            if (value <= 0) {
                auto change = out.base().data_view().begin_mutation(out.evaluation_time());
                if (value < 0) { static_cast<void>(change.invalidate()); }
                else { change.mark_empty_delta(); }
            } else if constexpr (std::is_same_v<Owner, StampSet>) {
                out.add(value);
            } else if constexpr (std::is_same_v<Owner, StampDict>) {
                out[0].set(value);
            } else if constexpr (std::is_same_v<Owner, StampBundle>) {
                out.template field<"value">().set(value);
            } else if constexpr (std::is_same_v<Owner, StampWindow>) {
                out.push(value);
            } else {
                if constexpr (std::is_same_v<Owner, StampGrowingList>) { out.resize(1); }
                out[0].set(value);
            }
        }
    };

    template <typename Stamp, bool ReadValidity>
    struct ObserveTimestamp
    {
        static void eval(In<"clock", TS<Bool>>, In<"stamp", TS<Stamp>, InputActivity::Passive, InputValidity::Unchecked> stamp,
                         Out<TS<Bool>> out)
        {
            out.set(ReadValidity ? stamp.valid() : stamp.modified());
        }
    };

    template <typename Source, typename Timestamp, bool ReadValidity>
    struct ReadTimestampGraph
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Bool>> clock)
        {
            auto stamp = wire<Timestamp>(w, wire<Source>(w, action));
            using Stamp = std::conditional_t<std::is_same_v<Timestamp, stdlib::last_modified_date>, Date, DateTime>;
            return wire<ObserveTimestamp<Stamp, ReadValidity>>(w, clock, stamp);
        }
    };

    template <typename Source, typename Timestamp>
    struct SampleOnTimestampGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> action, Port<TS<Int>> price)
        {
            auto stamp = wire<Timestamp>(w, wire<Source>(w, action));
            return wire<stdlib::sample, TS<Int>>(w, stamp, price);
        }
    };

    template <typename Timestamp>
    struct SampleOnSelectedTimestampGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> selection, Port<TS<Int>> value, Port<TS<Int>> price)
        {
            auto selected = wire<SelectScalar>(w, selection, value);
            return wire<stdlib::sample, TS<Int>>(w, wire<Timestamp>(w, selected), price);
        }
    };

    template <typename Owner>
    struct SelectOwner
    {
        static void eval(In<"select", TS<Bool>> select,
                         In<"owner", REF<Owner>, InputActivity::Passive, InputValidity::Unchecked> owner,
                         Out<REF<Owner>> out)
        {
            out.set(select.value() ? owner.value() : TimeSeriesReference::empty());
        }
    };

    template <typename Owner, typename Source, typename Timestamp>
    struct SampleOnSelectedOwnerGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> select, Port<TS<Int>> action, Port<TS<Int>> price)
        {
            auto owner = wire<Source>(w, action);
            auto selected = wire<SelectOwner<Owner>>(w, select, owner);
            return wire<stdlib::sample, TS<Int>>(w, wire<Timestamp>(w, selected), price);
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

TEST_CASE("Native SIGNAL price latches read direct scalar publications on an independent clock", "[invalidation][signal]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    const auto actions = values<Int>(Clear, Publish, none, Clear, ClearTwice, Publish, Publish,
                                     PublishThenClear, Publish, ClearThenPublish);
    const auto clock = values<Bool>(true, true, true, true, true, true, true, true, true, true);
    const auto prices = values<Int>(6, 10, 20, none, none, 30, 31, 40, 50, 60);
    const auto expected = values<Int>(none, 10, none, none, none, 30, 31, none, 50, 60);
    CHECK(eval_node<SampleOnScalarPublication>(actions, prices) == expected);
    CHECK(eval_node<DirectScalarPriceLatch<TS<Int>>>(actions, clock, prices) == expected);
    CHECK(eval_node<DirectScalarPriceLatch<SIGNAL>>(actions, clock, prices) == expected);
    CHECK(eval_node<DirectScalarPriceLatch<SIGNAL, InputActivity::Active>>(actions, clock, prices) == expected);
}

TEST_CASE("Native scalar SIGNAL latches preserve sampled reference controls", "[invalidation][signal][reference]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    const auto actions = values<Int>(Publish, none, Clear, none, Publish, none, Clear, Publish, none,
                                     none, none, Clear, ClearThenPublish);
    const auto selection = values<Int>(0, none, none, none, none, 1, none, none, 0, none, 1, 0, none);
    const auto clock = values<Bool>(true, true, true, true, true, true, true, true, true, true, true, true, true);
    const auto prices = values<Int>(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130);
    // The unbound owner publishes before its held value is sampled on rebind.
    // Switching back to an invalidated owner at the twelfth clock stays quiet.
    const auto expected = values<Int>(10, none, none, none, 50, none, none, none, 90, none, none, none, 130);
    CHECK(eval_node<SelectedScalarPriceLatch<TS<Int>>>(actions, selection, clock, prices) == expected);
    CHECK(eval_node<SelectedScalarPriceLatch<SIGNAL>>(actions, selection, clock, prices) == expected);
}

TEST_CASE("Runtime rules: membership and key set observations through native wiring", "[runtime-contract]")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<MembershipGraph>({0, 1, 2, 3, 4, 5, 6, 7}), {0, 1, 2, 3, 4, 5, 6, 7});
}

TEST_CASE("Memory replay preserves membership, scalar invalidation and key-set timing")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    GlobalContext context;
    record_replay::set_config(context.state().view(), {.backend = std::string{record_replay::MEMORY}});
    const auto steps = values<Int>(0, 1, 2, 3, 4, 5, 6, 7);
    CHECK_OUTPUT(eval_node<RecordMembershipGraph>(steps), steps);
    CHECK_OUTPUT(eval_node<ReplayMembershipGraph>(steps), steps);

    // A component restarted while X is invalid still sees its membership.
    for (const auto step : {1, 2, 3, 4}) {
        TSOutput seed{schema_descriptor<Dict>::ts_meta()};
        const auto output = seed.view(MIN_ST + step * MIN_TD);
        record_replay::publish_recorded_seed(context.state().view(), "members.book", output);
        const Value x{Str{"X"}};
        const auto dict = output.as_dict();
        CHECK(dict.contains(x.view()) == (step != 4));
        if (step != 4) { CHECK(dict.at(x.view()).valid() == (step == 2)); }
    }
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

TEST_CASE("An active scalar input wakes on a clear and observes the final write", "[runtime-contract][invalidation]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    // Only the initial probe wakes the observer independently of the scalar.
    // Back-to-back publish-then-clear cycles must each be visible from an invalid starting state.
    const auto actions = values<Int>(none, Clear, Publish, ClearTwice, Clear, PublishThenClear, PublishThenClear,
                                     Publish, ClearThenPublish, none);
    const auto probes = values<Bool>(true);
    CHECK_OUTPUT(eval_node<ScalarInvalidationGraph<InputActivity::Active>>(actions, probes),
                 {false, none, false, true, none, true, true, false, false, none});
}

TEST_CASE("A passive scalar input observes a clear only on a coincident probe", "[runtime-contract][invalidation]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    const auto actions = values<Int>(none, Publish, Clear, none, Publish, Clear, Clear);
    const auto probes = values<Bool>(true, none, none, true, true, true, true);
    CHECK_OUTPUT(eval_node<ScalarInvalidationGraph<InputActivity::Passive>>(actions, probes),
                 {false, none, none, false, false, true, false});
}

TEST_CASE("Whole-owner invalidation withdraws modification timestamps without sampling", "[runtime-contract][invalidation]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    const auto clocks = values<Bool>(true, true, true, true, true, true, true, true);
    const auto prices = values<Int>(10, none, 20, none, none, 30, none, 40);
    const auto check = [&]<typename Source>(const auto &actions) {
        const auto check_stamp = [&]<typename Timestamp>() {
            CHECK_OUTPUT((eval_node<ReadTimestampGraph<Source, Timestamp, true>>(actions, clocks)),
                         {false, true, true, false, false, false, true, true});
            CHECK_OUTPUT((eval_node<ReadTimestampGraph<Source, Timestamp, false>>(actions, clocks)),
                         {false, true, false, false, false, false, true, true});
            CHECK_OUTPUT((eval_node<SampleOnTimestampGraph<Source, Timestamp>>(actions, prices)),
                         {none, 10, none, none, none, none, 30, 40});
        };
        check_stamp.template operator()<stdlib::last_modified_time>();
        check_stamp.template operator()<stdlib::last_modified_wall_clock_time>();
        check_stamp.template operator()<stdlib::last_modified_date>();
    };
    const auto collection = values<Int>(none, 7, none, -1, -1, none, 7, 8);
    SECTION("scalar, including equal writes") {
        check.template operator()<ApplyScalarAction>(values<Int>(none, Publish, none, Clear, Clear, none, Publish, Publish));
    }
    const auto empty_collection = values<Int>(none, 0, none, -1, -1, none, 0, 8);
    SECTION("set") { check.template operator()<ApplyOwnerAction<StampSet>>(empty_collection); }
    SECTION("dictionary") { check.template operator()<ApplyOwnerAction<StampDict>>(empty_collection); }
    SECTION("fixed list") { check.template operator()<ApplyOwnerAction<StampFixedList>>(collection); }
    SECTION("growing list") { check.template operator()<ApplyOwnerAction<StampGrowingList>>(collection); }
    SECTION("bundle") { check.template operator()<ApplyOwnerAction<StampBundle>>(collection); }
    SECTION("window") { check.template operator()<ApplyOwnerAction<StampWindow>>(collection); }
}

TEST_CASE("Modification timestamps preserve sampled reference transitions", "[runtime-contract][invalidation]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    const auto selection = values<Int>(1, none, 0, none, 1, none, 0, none);
    const auto value = values<Int>(7, 8, none, 9, none, 10, none, none);
    const auto price = values<Int>(1, 2, 3, 4, 5, 6, 7, 8);
    const auto check = [&]<typename Timestamp>() {
        CHECK_OUTPUT(eval_node<SampleOnSelectedTimestampGraph<Timestamp>>(selection, value, price),
                     {none, none, 3, 4, 5, none, 7, none});
    };
    check.template operator()<stdlib::last_modified_time>();
    check.template operator()<stdlib::last_modified_wall_clock_time>();
    check.template operator()<stdlib::last_modified_date>();
}

TEST_CASE("A selected owner's invalidation withdraws its timestamps while the selector is held",
          "[runtime-contract][invalidation]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    const auto select = values<Bool>(true, none, none, none, none, false, none, true, none);
    const auto prices = values<Int>(1, 2, 3, 4, 5, 6, 7, 8, 9);
    const auto check = [&]<typename Owner, typename Source>(const auto &actions) {
        const auto check_stamp = [&]<typename Timestamp>() {
            const auto result = eval_node<SampleOnSelectedOwnerGraph<Owner, Source, Timestamp>>(select, actions, prices);
            if constexpr (std::is_same_v<Owner, StampFixedList> || std::is_same_v<Owner, StampBundle>) {
                CHECK_OUTPUT(result, {1, none, none, none, 5, none, none, 8, 9});
            } else {
                CHECK_OUTPUT(result, {1, none, none, none, 5, 6, none, 8, 9});
            }
        };
        check_stamp.template operator()<stdlib::last_modified_time>();
        check_stamp.template operator()<stdlib::last_modified_wall_clock_time>();
        check_stamp.template operator()<stdlib::last_modified_date>();
    };
    SECTION("scalar") {
        check.template operator()<TS<Int>, ApplyScalarAction>(
            values<Int>(Publish, none, Clear, none, Publish, none, Publish, none, Publish));
    }
    const auto actions = values<Int>(7, none, -1, none, 8, none, 9, none, 10);
    SECTION("set") { check.template operator()<StampSet, ApplyOwnerAction<StampSet>>(actions); }
    SECTION("dictionary") { check.template operator()<StampDict, ApplyOwnerAction<StampDict>>(actions); }
    SECTION("fixed list") { check.template operator()<StampFixedList, ApplyOwnerAction<StampFixedList>>(actions); }
    SECTION("growing list") { check.template operator()<StampGrowingList, ApplyOwnerAction<StampGrowingList>>(actions); }
    SECTION("bundle") { check.template operator()<StampBundle, ApplyOwnerAction<StampBundle>>(actions); }
    SECTION("window") { check.template operator()<StampWindow, ApplyOwnerAction<StampWindow>>(actions); }
}
