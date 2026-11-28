// A saved REF whose target is a forwarding output inside a switch_ branch
// (hgraph#1938).
//
// The from-REF link alternative observes every forwarding output a REF
// names, so a re-targeted forwarding output re-applies the link. When
// switch_ retires the branch owning that output, the output's observers are
// told through Notifiable::source_invalidated; the alternative must drop its
// handle there. A later overwrite of the saved REF otherwise unsubscribes
// through the stale handle: a Debug assertion on a branch that is retired,
// and a use-after-free when the branch slot is reused (switch 0 -> 1 -> 0).

#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/generated.h>
#include <hgraph/types/time_series/ts_output/forwarding.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using N    = TS<Int>;
    using Book = TSD<Int, N>;

    template <typename T> struct SavedRefExpirySelect
    {
        static void eval(In<"choice", N> choice,
                         In<"left", REF<T>, InputActivity::Passive, InputValidity::Unchecked> left,
                         Out<REF<T>> out)
        {
            static_cast<void>(choice);
            out.set(left.value());
        }
    };

    template <typename T> struct SavedRefExpirySave
    {
        static void eval(In<"save", TS<Bool>> save,
                         In<"selected", REF<T>, InputActivity::Passive, InputValidity::Unchecked> selected,
                         Out<REF<T>> out)
        {
            if (save.value()) { out.set(selected.value()); }
        }
    };

    /** The size of the dictionary the saved REF names, or -1 while it names none. */
    struct SavedRefExpiryObserve
    {
        static void eval(In<"clock", N> clock,
                         In<"saved", Book, InputActivity::Passive, InputValidity::Unchecked> saved, Out<N> out)
        {
            static_cast<void>(clock);
            out.set(saved.base().valid() ? static_cast<Int>(saved.base().as_dict().size()) : Int{-1});
        }
    };

    /** A node whose output forwards to the output bound to its input. */
    struct SavedRefExpiryForwardingBook
    {
        static Port<Book> compose(Wiring &w, Port<Book> source)
        {
            const auto      *book   = ts_type<Book>();
            const auto      *inputs = TypeRegistry::instance().un_named_tsb({{"source", book}});
            NodeTypeMetaData meta;
            meta.display_name           = "saved_ref_expiry_forwarding_book";
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
            auto builder = NodeBuilder::native(std::move(meta), std::move(callbacks),
                                               TSEndpointSchema::non_peered(inputs, {TSEndpointSchema::peered(book)}));
            return {w, w.add_node(typeid(SavedRefExpiryForwardingBook), std::move(builder), arguments, Value{})};
        }
    };

    /** Each outer branch returns a REF to a forwarding output it owns. */
    struct SavedRefToForwardingBranch
    {
        template <bool Right> struct Branch
        {
            static Port<REF<Book>> compose(Wiring &w, Port<Book> a, Port<Book> b)
            {
                const auto zero       = wire<stdlib::const_, N>(w, Int{0});
                const auto forwarding = SavedRefExpiryForwardingBook::compose(w, Right ? b : a);
                return wire<SavedRefExpirySelect<Book>>(w, zero, forwarding).as<REF<Book>>();
            }
        };

        static Port<N> compose(Wiring &w, Port<N> select, Port<Book> a, Port<Book> b, Port<TS<Bool>> save,
                               Port<N> clock)
        {
            std::vector<SwitchBranchGraph> cases{
                {Value{Int{0}}, compile_subgraph<Branch<false>>(w)},
                {Value{Int{1}}, compile_subgraph<Branch<true>>(w)},
            };
            const std::array      inputs{a.erased(), b.erased()};
            const Port<REF<Book>> selected{w, compiled::switch_(w, select.erased(), inputs, std::move(cases))};
            const auto            saved = wire<SavedRefExpirySave<Book>>(w, save, selected).as<REF<Book>>();
            return wire<SavedRefExpiryObserve>(w, clock, saved.as<Book>());
        }
    };

    /** The value the saved REF names, or -1 while it names none. */
    struct SavedRefExpiryObserveSum
    {
        static void eval(In<"clock", N> clock, In<"saved", N, InputActivity::Passive, InputValidity::Unchecked> saved,
                         Out<N> out)
        {
            static_cast<void>(clock);
            out.set(saved.base().valid() ? saved.value() : Int{-1});
        }
    };

    /** Each outer branch returns a REF to its reduce_ output, which forwards
        to the combiner tree: the shape the Python test builds. */
    struct SavedRefToReduceBranch
    {
        template <bool Right> struct Branch
        {
            static Port<REF<N>> compose(Wiring &w, Port<Book> a, Port<Book> b)
            {
                const auto zero = wire<stdlib::const_, N>(w, Int{0});
                const auto sum  = wire<stdlib::reduce_>(w, fn<stdlib::add_>(), Right ? b : a, Int{0}).as<N>();
                return wire<SavedRefExpirySelect<N>>(w, zero, sum).as<REF<N>>();
            }
        };

        static Port<N> compose(Wiring &w, Port<N> select, Port<Book> a, Port<Book> b, Port<TS<Bool>> save,
                               Port<N> clock)
        {
            std::vector<SwitchBranchGraph> cases{
                {Value{Int{0}}, compile_subgraph<Branch<false>>(w)},
                {Value{Int{1}}, compile_subgraph<Branch<true>>(w)},
            };
            const std::array    inputs{a.erased(), b.erased()};
            const Port<REF<N>>  selected{w, compiled::switch_(w, select.erased(), inputs, std::move(cases))};
            const auto          saved = wire<SavedRefExpirySave<N>>(w, save, selected).as<REF<N>>();
            return wire<SavedRefExpiryObserveSum>(w, clock, saved.as<N>());
        }
    };
}  // namespace

TEST_CASE("Saved REF to a forwarding branch output survives branch retirement and an overwrite",
          "[reference][switch][hgraph#1938]")
{
    stdlib::register_standard_operators();
    // Branch 0 is saved, switch_ retires it at 1, and the REF is overwritten
    // with branch 1's forwarding output at 3.
    CHECK_OUTPUT(eval_node<SavedRefToForwardingBranch>(
                     values<Int>(0, 1, none, none, none),
                     values<Value>(dict_delta<Int, N>({{1, 10}}), none, none, none, none),
                     values<Value>(dict_delta<Int, N>({{2, 20}, {3, 30}}), none, none, none, none),
                     values<Bool>(true, false, false, true, false),
                     values<Int>(0, 1, 2, 3, 4)),
                 values<Int>(1, -1, -1, 2, 2));
}

TEST_CASE("Saved REF to a forwarding branch output survives branch slot reuse and an overwrite",
          "[reference][switch][hgraph#1938]")
{
    stdlib::register_standard_operators();
    // Switching 0 -> 1 -> 0 rebuilds branch 0 in the slot the saved output
    // lived in; the overwrite at 3 names the rebuilt branch's output.
    CHECK_OUTPUT(eval_node<SavedRefToForwardingBranch>(
                     values<Int>(0, 1, 0, none, none),
                     values<Value>(dict_delta<Int, N>({{1, 10}}), none, none, none, dict_delta<Int, N>({{4, 40}})),
                     values<Value>(dict_delta<Int, N>({{2, 20}, {3, 30}}), none, none, none, none),
                     values<Bool>(true, false, false, true, false),
                     values<Int>(0, 1, 2, 3, 4)),
                 values<Int>(1, -1, -1, 1, 2));
}

TEST_CASE("Saved REF to a reduce_ output survives branch retirement and an overwrite",
          "[reference][switch][hgraph#1938]")
{
    stdlib::register_standard_operators();
    // The stopped branch's sum stays readable until switch_ expires it.
    CHECK_OUTPUT(eval_node<SavedRefToReduceBranch>(
                     values<Int>(0, 1, none, none, none),
                     values<Value>(dict_delta<Int, N>({{1, 10}}), none, none, none, none),
                     values<Value>(dict_delta<Int, N>({{2, 20}, {3, 30}}), none, none, none, none),
                     values<Bool>(true, false, false, true, false),
                     values<Int>(0, 1, 2, 3, 4)),
                 values<Int>(10, 10, -1, 50, 50));
}

TEST_CASE("Saved REF to a reduce_ output survives branch slot reuse and an overwrite",
          "[reference][switch][hgraph#1938]")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<SavedRefToReduceBranch>(
                     values<Int>(0, 1, 0, none, none),
                     values<Value>(dict_delta<Int, N>({{1, 10}}), none, none, none, dict_delta<Int, N>({{4, 40}})),
                     values<Value>(dict_delta<Int, N>({{2, 20}, {3, 30}}), none, none, none, none),
                     values<Bool>(true, false, false, true, false),
                     values<Int>(0, 1, 2, 3, 4)),
                 values<Int>(10, 10, -1, 10, 50));
}
