#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/generated.h>
#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output/forwarding.h>
#include <hgraph/util/scope.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using N         = TS<Int>;
    using Book      = TSD<Int, N>;
    using FixList   = TSL<N, 2>;
    using DictOfFix = TSD<Int, FixList>;

    struct ForwardingBook
    {
        static Port<Book> compose(Wiring &w, Port<Book> source) {
            const auto      *book   = ts_type<Book>();
            const auto      *inputs = TypeRegistry::instance().un_named_tsb({{"source", book}});
            NodeTypeMetaData meta;
            meta.display_name           = "forward_keyed_source";
            meta.input_schema           = inputs;
            meta.output_schema          = book;
            meta.output_endpoint_schema = TSEndpointSchema::peered(book);
            NodeCallbacks callbacks;
            callbacks.start = [](const NodeView &node, DateTime time) {
                auto input     = node.input(time);
                auto arguments = input.as_bundle();
                node.output(time).bind_forwarding_target_sampled(arguments.at(0).bound_output());
            };
            callbacks.stop = [](const NodeView &node, DateTime) { node.output(MIN_DT).clear_forwarding_target(); };
            const std::array arguments{source.erased()};
            auto             builder = NodeBuilder::native(std::move(meta), std::move(callbacks),
                                                           TSEndpointSchema::non_peered(inputs, {TSEndpointSchema::peered(book)}));
            return {w, w.add_node(typeid(ForwardingBook), std::move(builder), arguments, Value{})};
        }
    };

    struct DirectBookSwitch
    {
        struct First
        {
            static Port<Book> compose(Wiring &w, Port<Book> a, Port<Book>) { return ForwardingBook::compose(w, a); }
        };
        struct Second
        {
            static Port<Book> compose(Wiring &w, Port<Book>, Port<Book> b) { return ForwardingBook::compose(w, b); }
        };
        static Port<Book> compose(Wiring &w, Port<N> choose, Port<Book> a, Port<Book> b) {
            std::vector<SwitchBranchGraph> cases{
                {Value{Int{0}}, compile_subgraph<First>(w)},
                {Value{Int{1}}, compile_subgraph<Second>(w)},
            };
            const std::array arguments{a.erased(), b.erased()};
            return {w, compiled::switch_(w, choose.erased(), arguments, std::move(cases))};
        }
    };

    template <typename T> struct SelectTarget
    {
        static void eval(In<"choice", N> choice, In<"left", REF<T>, InputActivity::Passive, InputValidity::Unchecked> left,
                         In<"right", REF<T>, InputActivity::Passive, InputValidity::Unchecked> right, Out<REF<T>> out) {
            switch (choice.value()) {
                case 0: out.set(left.value()); break;
                case 1: out.set(right.value()); break;
                default: out.set(TimeSeriesReference::empty()); break;
            }
        }
    };

    template <typename T> struct ForwardingMux
    {
        struct LeftBranch
        {
            static Port<T> compose(Wiring &, Port<T> l, Port<T>) { return l; }
        };
        struct RightBranch
        {
            static Port<T> compose(Wiring &, Port<T>, Port<T> r) { return r; }
        };
        static Port<T> compose(Wiring &w, Port<N> sel, Port<T> l, Port<T> r) {
            std::vector<SwitchBranchGraph> cases{
                {Value{Int{0}}, compile_subgraph<LeftBranch>(w)},
                {Value{Int{1}}, compile_subgraph<RightBranch>(w)},
            };
            const std::array inputs{l.erased(), r.erased()};
            return {w, compiled::switch_(w, sel.erased(), inputs, std::move(cases))};
        }
    };

    template <typename T> struct OuterAfterInnerGraph
    {
        static Port<T> compose(Wiring &w, Port<N> inner_choice, Port<N> outer_choice, Port<T> a, Port<T> empty_or_other,
                               Port<T> unpub) {
            const auto inner_ref = wire<SelectTarget<T>>(w, inner_choice, a, empty_or_other).template as<REF<T>>();
            const auto inner_link =
                ForwardingMux<T>::compose(w, wire<stdlib::const_, N>(w, Int{0}), inner_ref.template as<T>(), unpub);
            const auto outer_ref = wire<SelectTarget<T>>(w, outer_choice, inner_link, unpub).template as<REF<T>>();
            return outer_ref.template as<T>();
        }
    };

    TEST_CASE("Keyed links preserve prior keys when both endpoints move in the same cycle", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a          = values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}));
        const auto empty_dict = values<Value>(dict_delta<Int, N>({}));
        const auto unpub      = values<Value>(none, none);

        // Subcase 1a: Inner withdraws (2) AND Outer withdraws (2) in cycle 1
        CHECK_OUTPUT((eval_node<OuterAfterInnerGraph<Book>>(values<Int>(0, 2), values<Int>(0, 2), a, empty_dict, unpub)),
                     values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}), dict_delta<Int, N>({}, {1, 2})));

        // Subcase 1b: Inner switches to empty_dict (1) AND Outer withdraws (2) in cycle 1
        CHECK_OUTPUT((eval_node<OuterAfterInnerGraph<Book>>(values<Int>(0, 1), values<Int>(0, 2), a, empty_dict, unpub)),
                     values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}), dict_delta<Int, N>({}, {1, 2})));

        // Subcase 1c: Inner withdraws (2) AND Outer rebinds to unpublished (1) in cycle 1
        CHECK_OUTPUT((eval_node<OuterAfterInnerGraph<Book>>(values<Int>(0, 2), values<Int>(0, 1), a, empty_dict, unpub)),
                     values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}), dict_delta<Int, N>({}, {1, 2})));
    }

    struct RoundTripToSameProducer
    {
        static Port<Book> compose(Wiring &w, Port<N> inner_choice, Port<N> outer_choice, Port<Book> a, Port<Book> b) {
            const auto inner = ForwardingMux<Book>::compose(w, inner_choice, a, b);
            return ForwardingMux<Book>::compose(w, outer_choice, inner, a);
        }
    };

    TEST_CASE("Returning to the same keyed producer cancels a sampled move", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a   = values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}));
        const auto b   = values<Value>(dict_delta<Int, N>({{9, 90}}));
        const auto res = eval_node<RoundTripToSameProducer>(values<Int>(0, 1), values<Int>(0, 1), a, b);
        REQUIRE(res.size() == 2);
        CHECK_FALSE(res[1].has_value());
    }

    struct ThreeKeyedSwitches
    {
        static Port<Book> compose(Wiring &w, Port<N> choice, Port<Book> a, Port<Book> b) {
            const auto fixed  = wire<stdlib::const_, N>(w, Int{0});
            const auto first  = DirectBookSwitch::compose(w, choice, a, b);
            const auto second = DirectBookSwitch::compose(w, fixed, first, b);
            return DirectBookSwitch::compose(w, fixed, second, b);
        }
    };

    TEST_CASE("An idle retarget samples overlap through three keyed switches", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a = dict_delta<Int, N>({{1, 10}, {2, 20}});
        const auto b = dict_delta<Int, N>({{2, 21}, {3, 30}});
        CHECK_OUTPUT(eval_node<ThreeKeyedSwitches>(values<Int>(0, 1), values<Value>(a), values<Value>(b)),
                     values<Value>(a, dict_delta<Int, N>({{2, 21}, {3, 30}}, {1})));
    }

    TEST_CASE("Set withdrawal is delivered through an unchanged outer endpoint", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        CHECK_OUTPUT(eval_node<OuterAfterInnerGraph<TSS<Int>>>(values<Int>(0, 2), values<Int>(0),
                                                               values<Value>(set_delta<Int>({1, 2}, {})), values<Value>(none),
                                                               values<Value>(none)),
                     values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1, 2})));
    }

    struct RefCopySwitchOverForwardingBranch
    {
        struct Branch0
        {
            static Port<REF<Book>> compose(Wiring &w, Port<Book> a, Port<Book> b) {
                const auto fwd = ForwardingBook::compose(w, a);
                return wire<SelectTarget<Book>>(w, wire<stdlib::const_, N>(w, Int{0}), fwd, b).as<REF<Book>>();
            }
        };
        struct Branch1
        {
            static Port<REF<Book>> compose(Wiring &w, Port<Book> a, Port<Book> b) {
                return wire<SelectTarget<Book>>(w, wire<stdlib::const_, N>(w, Int{1}), a, b).as<REF<Book>>();
            }
        };
        static Port<REF<Book>> compose(Wiring &w, Port<N> sel, Port<Book> a, Port<Book> b) {
            std::vector<SwitchBranchGraph> cases{
                {Value{Int{0}}, compile_subgraph<Branch0>(w)},
                {Value{Int{1}}, compile_subgraph<Branch1>(w)},
            };
            const std::array      inputs{a.erased(), b.erased()};
            const Port<REF<Book>> ref_out{w, compiled::switch_(w, sel.erased(), inputs, std::move(cases))};
            return ref_out;
        }
    };

    struct RefCopySwitchValue
    {
        static Port<Book> compose(Wiring &w, Port<N> sel, Port<Book> a, Port<Book> b) {
            return RefCopySwitchOverForwardingBranch::compose(w, sel, a, b).as<Book>();
        }
    };

    TEST_CASE("A REF switch retains the stopped keyed endpoint's prior keys", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a = values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}));
        const auto b = values<Value>(dict_delta<Int, N>({{2, 200}, {3, 300}}));
        CHECK_OUTPUT(eval_node<RefCopySwitchValue>(values<Int>(0, 1), a, b),
                     values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}), dict_delta<Int, N>({{2, 200}, {3, 300}}, {1})));
    }

    struct SaveKeyedReference
    {
        static void eval(In<"save", TS<Bool>> save, In<"selected", REF<Book>, InputActivity::Passive> selected,
                         Out<REF<Book>> out) {
            if (save.value()) { out.set(selected.value()); }
        }
    };

    struct ReadSavedKeyedReference
    {
        static void eval(In<"clock", N>, In<"saved", REF<Book>, InputActivity::Passive> saved,
                         In<"value", Book, InputActivity::Passive, InputValidity::Unchecked> value, Out<TS<Int>> out) {
            out.set(!saved.value().has_output() ? -1 : value.base().valid() ? static_cast<Int>(value.base().as_dict().size()) : 0);
        }
    };

    struct SavedKeyedBranchReference
    {
        static Port<N> compose(Wiring &w, Port<N> sel, Port<Book> a, Port<Book> b, Port<TS<Bool>> save, Port<N> clock) {
            const auto selected = RefCopySwitchOverForwardingBranch::compose(w, sel, a, b);
            const auto saved    = wire<SaveKeyedReference>(w, save, selected).as<REF<Book>>();
            return wire<ReadSavedKeyedReference>(w, clock, saved, saved.as<Book>());
        }
    };

    TEST_CASE("Saved REF to a stopped keyed forwarding endpoint expires", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a = values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}));
        const auto b = values<Value>(dict_delta<Int, N>({{3, 30}}));
        CHECK_OUTPUT(eval_node<SavedKeyedBranchReference>(values<Int>(0, 1), a, b, values<Bool>(true), values<Int>(0, 1, 2)),
                     values<Int>(2, 2, -1));
    }

    struct MappedForwardingRefChild
    {
        static Port<REF<Book>> compose(Wiring &w, Port<Book> item) {
            const auto fwd = ForwardingMux<Book>::compose(w, wire<stdlib::const_, N>(w, Int{0}), item, item);
            return wire<SelectTarget<Book>>(w, wire<stdlib::const_, N>(w, Int{0}), fwd, fwd).as<REF<Book>>();
        }
    };
    struct PickKeyFromRefMap
    {
        static void eval(In<"m", TSD<Int, REF<Book>>, InputActivity::Active, InputValidity::Unchecked> m, In<"k", N> k,
                         Out<REF<Book>> out) {
            const auto  dict = m.base().as_dict();
            const Value key{k.value()};
            if (dict.contains(key.view()) && dict.at(key.view()).valid()) {
                out.set(dict.at(key.view()).value().checked_as<TimeSeriesReference>());
            } else {
                out.set(TimeSeriesReference::empty());
            }
        }
    };
    struct MapExportedForwardingRefGraph
    {
        static Port<Book> compose(Wiring &w, Port<TSD<Int, Book>> shelf, Port<N> pick) {
            const auto mapped = wire<stdlib::map_>(w, fn<MappedForwardingRefChild>(), shelf).as<TSD<Int, REF<Book>>>();
            const auto sel    = wire<PickKeyFromRefMap>(w, mapped, pick).as<REF<Book>>();
            return sel.as<Book>();
        }
    };

    TEST_CASE("A higher-rank REF consumer retains a retired map child's prior keys", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto shelf = values<Value>(
            dict_delta<Int, Book>({{0, dict_delta<Int, N>({{1, 10}, {2, 20}})}, {1, dict_delta<Int, N>({{2, 200}, {3, 300}})}}),
            dict_delta<Int, Book>({}, {0}));
        CHECK_OUTPUT(eval_node<MapExportedForwardingRefGraph>(shelf, values<Int>(0, 1)),
                     values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}), dict_delta<Int, N>({{2, 200}, {3, 300}}, {1})));
    }

    struct SingleKeyActiveWatcher
    {
        static void start(In<"d", Book, InputActivity::Passive, InputValidity::Unchecked> d) {
            d.base().make_key_active(Value{Int{1}}.view());
        }
        static void eval(In<"d", Book, InputActivity::Passive, InputValidity::Unchecked> d, Out<TS<Str>> out) {
            const auto  dict = d.base().as_dict();
            const Value k{Int{1}};
            if (d.base().valid() && dict.contains(k.view()) && dict.at(k.view()).valid()) {
                out.set(std::to_string(dict.at(k.view()).value().checked_as<Int>()));
            } else {
                out.set("absent");
            }
        }
    };
    struct WatchKeyAcrossWithdrawal
    {
        static Port<TS<Str>> compose(Wiring &w, Port<N> choice, Port<Book> a, Port<Book> b) {
            const auto ref = wire<SelectTarget<Book>>(w, choice, a, b).as<REF<Book>>();
            return wire<SingleKeyActiveWatcher>(w, ref);
        }
    };

    TEST_CASE("Withdrawal wakes a single-key watcher", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a = values<Value>(dict_delta<Int, N>({{1, 10}, {2, 20}}));
        const auto b = values<Value>(dict_delta<Int, N>({{2, 200}}));
        CHECK_OUTPUT(eval_node<WatchKeyAcrossWithdrawal>(values<Int>(0, 2), a, b), values<Str>("10", "absent"));
    }

    struct DirectSelectBook
    {
        static Port<Book> compose(Wiring &w, Port<N> choice, Port<Book> a, Port<Book> b) {
            return wire<SelectTarget<Book>>(w, choice, a, b).as<Book>();
        }
    };

    TEST_CASE("R3-5b C7 unpublished target publishes then link withdraws in same cycle emits bogus empty delta",
              "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto a = values<Value>(none, dict_delta<Int, N>({{1, 10}}));
        const auto b = values<Value>(none, none);
        CHECK_OUTPUT(eval_node<DirectSelectBook>(values<Int>(0, 2), a, b), values<Value>(none, none));
    }

    //       list slot) to B mixes A's slot index into B.key_at_slot!
    struct MakeDictOfFixWithVacantKey
    {
        static void eval(In<"step", N> step, Out<DictOfFix> out) {
            if (step.value() == 0) {
                auto f0 = out.at(Int{100});
                Out<N>{f0.at(0), out.evaluation_time()}.set(Int{1});
                auto f1 = out.at(Int{200});
                Out<N>{f1.at(0), out.evaluation_time()}.set(Int{2});
            } else if (step.value() == 1) {
                auto   f1 = out.at(Int{200});
                Out<N> c0{f1.at(0), out.evaluation_time()};
                static_cast<void>(c0.begin_mutation(out.evaluation_time()).invalidate());
            } else if (step.value() == 2) {
                auto mut = out.begin_mutation(out.evaluation_time());
                static_cast<void>(mut.erase(Value{Int{200}}.view()));
            }
        }
    };
    struct RetargetDictOfFix
    {
        static Port<DictOfFix> compose(Wiring &w, Port<N> step_a, Port<N> choice, Port<DictOfFix> b) {
            const auto a = wire<MakeDictOfFixWithVacantKey>(w, step_a).as<DictOfFix>();
            return wire<SelectTarget<DictOfFix>>(w, choice, a, b).as<DictOfFix>();
        }
    };

    TEST_CASE("Retarget captures dictionary membership removals of vacant list keys", "[keyed-forwarding]") {
        stdlib::register_standard_operators();
        const auto b   = values<Value>(dict_delta<Int, FixList>({{999, list_delta<N>({9, 9})}}), none, none);
        const auto res = eval_node<RetargetDictOfFix>(values<Int>(0, 1, 2), values<Int>(0, none, 1), b);
        REQUIRE(res.size() == 3);
        CHECK(res[2].has_value());
        if (res[2].has_value()) {
            // Notice: at t=0 key 200 joined; at t=1 key 200 remained in dictionary membership (TS-19: clearing a child preserves
            // its key!); at t=2 A erased 200 and link retargeted to B={999:[9,9]}, so BOTH 100 and 200 left membership!
            CHECK(res[2]->view().equals(dict_delta<Int, FixList>({{999, list_delta<N>({9, 9})}}, {100, 200}).view()));
        }
    }

    const TSValueTypeMetaData *book_schema() {
        auto       &registry = TypeRegistry::instance();
        const auto *key      = registry.register_scalar<Int>("int");
        return registry.tsd(key, registry.ts(key));
    }

    void set_book(TSOutput &output, DateTime time, Int key, Int value) {
        const auto view = output.view(time);
        apply_delta(view, dict_delta<Int, N>({{key, value}}).view());
    }

    std::set<Int> set_keys(const Range<ValueView> &keys) {
        std::set<Int> found;
        for (const auto &key : keys) { found.insert(key.checked_as<Int>()); }
        return found;
    }

    TEST_CASE("Restored follower adopts its endpoint when the endpoint restores later", "[keyed-forwarding]") {
        const auto *schema   = book_schema();
        const auto  endpoint = TSEndpointSchema::peered(schema);
        const auto  later    = MIN_ST + MIN_TD;
        TSOutput    a{schema}, b{schema};
        set_book(a, MIN_ST, 1, 10);
        set_book(b, MIN_ST, 2, 20);
        TSOutput inner{endpoint}, outer{endpoint};
        inner.view(MIN_ST).bind_forwarding_target(a.view(MIN_ST));
        outer.view(MIN_ST).bind_forwarding_target(inner.view(MIN_ST));
        const auto inner_checkpoint = inner.view(MIN_ST).checkpoint_forwarding();
        const auto outer_checkpoint = outer.view(MIN_ST).checkpoint_forwarding();

        TSOutput restored_inner{endpoint}, restored_outer{endpoint};
        restored_outer.view(MIN_ST).restore_checkpoint_forwarding(restored_inner.view(MIN_ST), outer_checkpoint);
        CHECK_FALSE(restored_outer.view(MIN_ST).valid());
        restored_inner.view(MIN_ST).restore_checkpoint_forwarding(a.view(MIN_ST), inner_checkpoint);
        REQUIRE(restored_outer.view(MIN_ST).valid());
        restored_inner.view(later).bind_forwarding_target_sampled(b.view(later));
        const auto out = restored_outer.view(later);
        CHECK(out.forwarding_target().same_as(restored_inner.view(later).handle()));
        CHECK(resolve_forwarding_source(out.borrowed_ref()).handle().same_as(b.view(later).handle()));
        CHECK(set_keys(out.as_dict().removed_keys()) == std::set<Int>{1});
        CHECK(set_keys(out.as_dict().added_keys()) == std::set<Int>{2});
    }

    TEST_CASE("Producer destruction detaches keyed followers at every depth", "[keyed-forwarding][lifetime]") {
        const auto             *schema   = book_schema();
        const auto              endpoint = TSEndpointSchema::peered(schema);
        std::optional<TSOutput> producer{std::in_place, schema};
        set_book(*producer, MIN_ST, 1, 10);
        TSOutput inner{endpoint}, middle{endpoint}, outer{endpoint};
        inner.view(MIN_ST).bind_forwarding_target(producer->view(MIN_ST));
        middle.view(MIN_ST).bind_forwarding_target(inner.view(MIN_ST));
        outer.view(MIN_ST).bind_forwarding_target(middle.view(MIN_ST));
        REQUIRE(outer.view(MIN_ST).valid());

        SECTION("The innermost endpoint is still running") {}
        SECTION("The innermost endpoint stopped first") {
            inner.view(MIN_DT).clear_forwarding_target();
            CHECK_FALSE(inner.view(MIN_ST).valid());
            CHECK(outer.view(MIN_ST).valid());
        }
        producer.reset();
        for (auto *link : {&inner, &middle, &outer}) {
            const auto view = link->view(MIN_ST + MIN_TD);
            CHECK_FALSE(view.forwarding_bound());
            CHECK_FALSE(view.valid());
            CHECK(view.as_dict().empty());
        }
    }

    struct InsertObserver final : SlotObserver
    {
        std::size_t insertions{0};
        void        on_capacity(std::size_t, std::size_t) override {}
        void        on_insert(std::size_t) override { ++insertions; }
        void        on_remove(std::size_t) override {}
        void        on_erase(std::size_t) override {}
        void        on_clear() override {}
    };

    TEST_CASE("A keyed round trip restores the key-set clock and slot observer", "[keyed-forwarding]") {
        const auto    *schema   = book_schema();
        const auto     endpoint = TSEndpointSchema::peered(schema);
        const auto     move     = MIN_ST + MIN_TD;
        InsertObserver observer;
        TSOutput       a{schema}, b{schema}, inner{endpoint}, outer{endpoint};
        set_book(a, MIN_ST, 1, 10);
        set_book(b, MIN_ST, 2, 20);
        inner.view(MIN_ST).bind_forwarding_target(a.view(MIN_ST));
        outer.view(MIN_ST).bind_forwarding_target(inner.view(MIN_ST));
        const auto before = outer.view(MIN_ST);
        auto       keys   = before.as_dict().key_set().data_view().borrowed_ref();
        keys.as_set().subscribe_slot_observer(&observer);
        auto detach = make_scope_exit([&] { keys.as_set().unsubscribe_slot_observer(&observer); });

        bool ticked = false;
        SECTION("The prior producer stays idle") {}
        SECTION("The prior producer ticks in the move cycle") {
            set_book(a, move, 5, 50);
            ticked = true;
        }
        inner.view(move).bind_forwarding_target_sampled(b.view(move));
        outer.view(move).bind_forwarding_target_sampled(a.view(move));
        const auto after = outer.view(move);
        CHECK(after.modified() == ticked);
        CHECK(after.as_dict().key_set().modified() == ticked);
        CHECK(set_keys(after.as_dict().removed_keys()).empty());
        CHECK(set_keys(after.as_dict().added_keys()) == (ticked ? std::set<Int>{5} : std::set<Int>{}));
        const auto insertions = observer.insertions;
        set_book(b, move + MIN_TD, 3, 30);
        CHECK(observer.insertions == insertions);
        set_book(a, move + MIN_TD, 4, 40);
        CHECK(observer.insertions == insertions + 1);
    }

    TEST_CASE("A set link follows a dictionary forwarding key-set projection", "[keyed-forwarding]") {
        const auto *schema     = book_schema();
        const auto *set_schema = TypeRegistry::instance().tss(schema->key_type());
        const auto  move       = MIN_ST + MIN_TD;
        const auto  withdrawal = move + MIN_TD;
        TSOutput    a{schema}, b{schema}, dictionary{TSEndpointSchema::peered(schema)};
        TSOutput    keys{TSEndpointSchema::peered(set_schema)};
        set_book(a, MIN_ST, 1, 10);
        set_book(b, MIN_ST, 2, 20);
        dictionary.view(MIN_ST).bind_forwarding_target(a.view(MIN_ST));
        const auto dictionary_view = dictionary.view(MIN_ST);
        keys.view(MIN_ST).bind_forwarding_target(dictionary_view.as_dict().key_set());
        REQUIRE(keys.view(MIN_ST).valid());

        dictionary.view(move).bind_forwarding_target_sampled(b.view(move));
        const auto moved = keys.view(move);
        CHECK(set_keys(moved.as_set().added_values()) == std::set<Int>{2});
        CHECK(set_keys(moved.as_set().removed_values()) == std::set<Int>{1});
        dictionary.view(withdrawal).clear_forwarding_target_sampled();
        const auto withdrawn = keys.view(withdrawal);
        CHECK_FALSE(withdrawn.valid());
        CHECK(withdrawn.modified());
        CHECK(set_keys(withdrawn.as_set().removed_values()) == std::set<Int>{2});
    }

    TEST_CASE("Two moves through unpublished producers retain the first producer", "[keyed-forwarding]") {
        const auto *schema   = book_schema();
        const auto  endpoint = TSEndpointSchema::peered(schema);
        const auto  move     = MIN_ST + MIN_TD;
        TSOutput    a{schema}, b{schema}, c{schema}, inner{endpoint}, outer{endpoint};
        set_book(a, MIN_ST, 1, 10);
        inner.view(MIN_ST).bind_forwarding_target(a.view(MIN_ST));
        outer.view(MIN_ST).bind_forwarding_target(inner.view(MIN_ST));

        // Change the outer designation first, then move the endpoint it follows.
        outer.view(move).bind_forwarding_target_sampled(inner.view(move));
        inner.view(move).bind_forwarding_target_sampled(b.view(move));
        inner.view(move).bind_forwarding_target_sampled(c.view(move));
        const auto out = outer.view(move);
        CHECK_FALSE(out.valid());
        CHECK(out.modified());
        CHECK(set_keys(out.as_dict().removed_keys()) == std::set<Int>{1});
    }

    struct KeyedSwitchChain
    {
        static Port<Book> compose(Wiring &w, Port<N> select, Port<Book> first, Port<Book> alternate, Scalar<"length", Int> length) {
            const auto fixed  = wire<stdlib::const_, N>(w, Int{0});
            auto       output = DirectBookSwitch::compose(w, select, first, alternate);
            for (Int link = 1; link < length.value(); ++link) { output = DirectBookSwitch::compose(w, fixed, output, alternate); }
            return output;
        }
    };

    TEST_CASE("Direct keyed switch-chain growth, tick and move scale with link count", "[.][keyed-forwarding-scaling]") {
        stdlib::register_standard_operators();
        using Clock                                        = std::chrono::steady_clock;
        constexpr Int                     iterations       = 256;
        const auto                        first            = dict_delta<Int, N>({{0, 1}, {1, 2}});
        const auto                        alternate        = dict_delta<Int, N>({{1, 3}, {2, 4}});
        const auto                        idle_choice      = values<Int>(0);
        const auto                        idle_values      = values<Value>(first);
        const auto                        alternate_values = values<Value>(alternate);
        std::vector<std::optional<Int>>   moving_choice{0};
        std::vector<std::optional<Value>> ticking_values{first};
        for (Int i = 0; i < iterations; ++i) {
            moving_choice.emplace_back((i + 1) % 2);
            ticking_values.emplace_back(dict_delta<Int, N>({{0, i + 5}}));
        }
        const auto elapsed = [&](Int links, const auto &choices, const auto &left, std::size_t expected) {
            const auto   start  = Clock::now();
            const auto   result = eval_node<KeyedSwitchChain>(choices, left, alternate_values, links);
            const double ns     = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
            REQUIRE(result.size() == expected);
            REQUIRE(result.back().has_value());
            return ns;
        };
        for (const Int links : {100, 200, 400, 800, 1600}) {
            const auto   n      = static_cast<double>(links);
            const double growth = elapsed(links, idle_choice, idle_values, 1);
            const double ticks  = elapsed(links, idle_choice, ticking_values, ticking_values.size());
            const double moves  = elapsed(links, moving_choice, idle_values, moving_choice.size());
            std::cout << "keyed_switch_chain links=" << links << " growth_us_per_link=" << growth / (1000.0 * n)
                      << " tick_ns_per_link=" << (ticks - growth) / (iterations * n)
                      << " move_ns_per_link=" << (moves - growth) / (iterations * n) << '\n';
        }
    }

}  // namespace
