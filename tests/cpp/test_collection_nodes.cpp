#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdint>

#include <optional>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace std::string_literals;

    using IntDict = TSD<Str, TS<Int>>;
    using IntWindow = TSW<Int, 3, 1>;
    using Quote = TSB<"Quote", Field<"bid", TS<Int>>, Field<"ask", TS<Int>>>;
    using QuoteList = TSL<Quote, 2>;
    using QuoteDict = TSD<Str, Quote>;

    struct DictSpread
    {
        static constexpr auto name = "dict_spread";
        static void           eval(In<"in", TS<Int>> in, Out<IntDict> out)
        {
            out["a"s].set(in.value());
            if (in.value() >= 2) { out["b"s].set(in.value() * 10); }
        }
    };

    struct DictTotal
    {
        static constexpr auto name = "dict_total";
        static void           eval(In<"d", IntDict> d, Out<TS<Int>> out)
        {
            Int total = 0;
            for (auto &&[key, child] : d.valid_items())
            {
                static_cast<void>(key);
                total += child.value();
            }
            out.set(total);
        }
    };

    struct DictGraph
    {
        static constexpr auto name = "dict_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto d   = wire<DictSpread>(w, src);
            auto sum = wire<DictTotal>(w, d);
            wire<stdlib::dense_record_impl>(w, sum, Str{"out"});
        }
    };

    struct DictDeltaGraph
    {
        static constexpr auto name = "dict_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, IntDict>(w, Str{"in"});
            wire<stdlib::dense_record_impl>(w, src, Str{"out"});
        }
    };

    struct WindowPush
    {
        static constexpr auto name = "window_push";
        static void           eval(In<"in", TS<Int>> in, Out<IntWindow> out) { out.push(in.value()); }
    };

    struct WindowTotal
    {
        static constexpr auto name = "window_total";
        static void           eval(In<"w", IntWindow> w, Out<TS<Int>> out)
        {
            Int total = 0;
            for (std::size_t i = 0; i < w.size(); ++i) { total += w[i]; }
            out.set(total);
        }
    };

    struct WindowGraph
    {
        static constexpr auto name = "window_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto win = wire<WindowPush>(w, src);
            auto sum = wire<WindowTotal>(w, win);
            wire<stdlib::dense_record_impl>(w, sum, Str{"out"});
        }
    };

    struct WindowDeltaGraph
    {
        static constexpr auto name = "window_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, IntWindow>(w, Str{"in"});
            wire<stdlib::dense_record_impl>(w, src, Str{"out"});
        }
    };

    struct QuoteSpread
    {
        static constexpr auto name = "quote_spread";
        static void           eval(In<"in", TS<Int>> in, Out<Quote> out)
        {
            out.field<"bid">().set(in.value());
            out.field<"ask">().set(in.value() * 10);
        }
    };

    struct QuoteTotal
    {
        static constexpr auto name = "quote_total";
        static void           eval(In<"q", Quote> q, Out<TS<Int>> out)
        {
            out.set(q.field<"bid">().value() + q.field<"ask">().value());
        }
    };

    struct QuoteGraph
    {
        static constexpr auto name = "quote_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto q   = wire<QuoteSpread>(w, src);
            auto sum = wire<QuoteTotal>(w, q);
            wire<stdlib::dense_record_impl>(w, sum, Str{"out"});
        }
    };

    struct QuoteDeltaGraph
    {
        static constexpr auto name = "quote_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, Quote>(w, Str{"in"});
            wire<stdlib::dense_record_impl>(w, src, Str{"out"});
        }
    };

    struct QuoteListDeltaGraph
    {
        static constexpr auto name = "quote_list_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, QuoteList>(w, Str{"in"});
            wire<stdlib::dense_record_impl>(w, src, Str{"out"});
        }
    };

    struct QuoteDictDeltaGraph
    {
        static constexpr auto name = "quote_dict_delta_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, QuoteDict>(w, Str{"in"});
            wire<stdlib::dense_record_impl>(w, src, Str{"out"});
        }
    };

    struct Pulse
    {
        static constexpr auto name = "pulse";
        static void           eval(In<"in", TS<Int>> in, Out<SIGNAL> out)
        {
            static_cast<void>(in);
            out.tick();
        }
    };

    struct CountPulses
    {
        static constexpr auto name = "count_pulses";
        static void           eval(In<"pulse", SIGNAL> pulse, State<Int> count, Out<TS<Int>> out)
        {
            if (pulse.ticked())
            {
                const Int next = count.get() + 1;
                count.set(next);
                out.set(next);
            }
        }
    };

    struct SignalGraph
    {
        static constexpr auto name = "signal_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto sig = wire<Pulse>(w, src);
            auto cnt = wire<CountPulses>(w, sig);
            wire<stdlib::dense_record_impl>(w, cnt, Str{"out"});
        }
    };

    struct SignalFromTsGraph
    {
        static constexpr auto name = "signal_from_ts_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto cnt = wire<CountPulses>(w, src);
            wire<stdlib::dense_record_impl>(w, cnt, Str{"out"});
        }
    };

    struct SignalFromDictGraph
    {
        static constexpr auto name = "signal_from_dict_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto d   = wire<DictSpread>(w, src);
            auto cnt = wire<CountPulses>(w, d);
            wire<stdlib::dense_record_impl>(w, cnt, Str{"out"});
        }
    };

    struct SignalFromWindowGraph
    {
        static constexpr auto name = "signal_from_window_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto win = wire<WindowPush>(w, src);
            auto cnt = wire<CountPulses>(w, win);
            wire<stdlib::dense_record_impl>(w, cnt, Str{"out"});
        }
    };

    struct SignalDeltaToBool
    {
        static constexpr auto name = "signal_delta_to_bool";
        static void           eval(In<"pulse", SIGNAL> pulse, Out<TS<bool>> out)
        {
            Value delta = capture_delta(pulse.base());
            out.set(delta.view().checked_as<bool>());
        }
    };

    struct SignalDeltaFromTsGraph
    {
        static constexpr auto name = "signal_delta_from_ts_graph";
        static void           compose(Wiring &w)
        {
            auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto out = wire<SignalDeltaToBool>(w, src);
            wire<stdlib::dense_record_impl>(w, out, Str{"out"});
        }
    };

    struct MirrorWindowForEvalNode
    {
        static constexpr auto name = "mirror_window_for_eval_node";
        static void           eval(In<"w", IntWindow> w, Out<IntWindow> out)
        {
            if (w.modified()) { out.apply(w.delta()); }
        }
    };

    struct MirrorSignalForEvalNode
    {
        static constexpr auto name = "mirror_signal_for_eval_node";
        static void           eval(In<"pulse", SIGNAL> pulse, Out<SIGNAL> out)
        {
            if (pulse.ticked()) { out.tick(); }
        }
    };
}  // namespace

TEST_CASE("collections: TSD typed output creates keys and typed input iterates child values")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    GraphBuilder gb = build_graph<DictGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 22, 33});
}

TEST_CASE("collections: TSD replay and record round-trip removed and modified deltas")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    const std::vector<std::optional<Value>> deltas{
        dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
        dict_delta<Str, TS<Int>>({{"a"s, 5}}, {"b"s}),
        dict_delta<Str, TS<Int>>({{"b"s, 9}}),
    };

    GraphBuilder gb = build_graph<DictDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                  dict_delta<Str, TS<Int>>({{"a"s, 5}}, {"b"s}),
                  dict_delta<Str, TS<Int>>({{"b"s, 9}})});
}

TEST_CASE("collections: TSW typed output pushes values and typed input reads the window")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<WindowGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3, 4});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 3, 6, 9});
}

TEST_CASE("collections: TSW replay and record round-trip scalar push deltas")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{Value{Int{1}}, Value{Int{2}}, Value{Int{3}}, Value{Int{4}}};

    GraphBuilder gb = build_graph<WindowDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {Value{Int{1}}, Value{Int{2}}, Value{Int{3}}, Value{Int{4}}});
}

TEST_CASE("collections: eval_node exchanges bare scalar deltas for TSW")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    CHECK_OUTPUT(testing::eval_node<MirrorWindowForEvalNode>({1, none, 3, 4}), {1, none, 3, 4});
}

TEST_CASE("collections: TSB typed field selectors work through node wiring")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<QuoteGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {11, 22, 33});
}

TEST_CASE("collections: TSB replay and record round-trip sparse field deltas")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    const std::vector<std::optional<Value>> deltas{
        tsb_delta<Quote>(1, 10),
        tsb_delta<Quote>(std::nullopt, 20),
        tsb_delta<Quote>(3, std::nullopt),
    };

    GraphBuilder gb = build_graph<QuoteDeltaGraph>();
    testing::set_replay_deltas(gb.global_state(), "in", deltas);

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                 {tsb_delta<Quote>(1, 10), tsb_delta<Quote>(std::nullopt, 20), tsb_delta<Quote>(3, std::nullopt)});
}

TEST_CASE("collections: TSL and TSD replay and record recurse through TSB children")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    {
        const std::vector<std::optional<Value>> deltas{
            list_delta<Quote>({{0, tsb_delta<Quote>(1, 10)}, {1, tsb_delta<Quote>(2, 20)}}),
            list_delta<Quote>({{1, tsb_delta<Quote>(std::nullopt, 30)}}),
            list_delta<Quote>({{0, tsb_delta<Quote>(4, std::nullopt)}}),
        };

        GraphBuilder gb = build_graph<QuoteListDeltaGraph>();
        testing::set_replay_deltas(gb.global_state(), "in", deltas);

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                     {list_delta<Quote>({{0, tsb_delta<Quote>(1, 10)}, {1, tsb_delta<Quote>(2, 20)}}),
                      list_delta<Quote>({{1, tsb_delta<Quote>(std::nullopt, 30)}}),
                      list_delta<Quote>({{0, tsb_delta<Quote>(4, std::nullopt)}})});
    }

    {
        const std::vector<std::optional<Value>> deltas{
            dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(1, 10)}, {"b"s, tsb_delta<Quote>(2, 20)}}),
            dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(std::nullopt, 30)}}, {"b"s}),
            dict_delta<Str, Quote>({{"b"s, tsb_delta<Quote>(4, std::nullopt)}}),
        };

        GraphBuilder gb = build_graph<QuoteDictDeltaGraph>();
        testing::set_replay_deltas(gb.global_state(), "in", deltas);

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_deltas(ex.view().graph().global_state(), "out"),
                     {dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(1, 10)}, {"b"s, tsb_delta<Quote>(2, 20)}}),
                      dict_delta<Str, Quote>({{"a"s, tsb_delta<Quote>(std::nullopt, 30)}}, {"b"s}),
                      dict_delta<Str, Quote>({{"b"s, tsb_delta<Quote>(4, std::nullopt)}})});
    }
}

TEST_CASE("collections: SIGNAL typed output ticks and typed input observes the tick")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    GraphBuilder gb = build_graph<SignalGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
}

TEST_CASE("collections: SIGNAL input binds directly to a scalar TS output")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<SignalFromTsGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, none, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, none, 2});
}

TEST_CASE("collections: SIGNAL input binds directly to collection and window outputs")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    {
        GraphBuilder gb = build_graph<SignalFromDictGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
    }

    {
        GraphBuilder gb = build_graph<SignalFromWindowGraph>();
        testing::set_replay_values<Int>(gb.global_state(), "in", {1, 2, 3});

        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();

        CHECK_OUTPUT(testing::get_recorded_values<Int>(ex.view().graph().global_state(), "out"), {1, 2, 3});
    }
}

TEST_CASE("collections: SIGNAL input bound to TS captures a bool tick delta")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    GraphBuilder gb = build_graph<SignalDeltaFromTsGraph>();
    testing::set_replay_values<Int>(gb.global_state(), "in", {1, none, 3});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    CHECK_OUTPUT(testing::get_recorded_values<bool>(ex.view().graph().global_state(), "out"), {true, none, true});
}

TEST_CASE("collections: eval_node exchanges bool ticks for SIGNAL")
{
    (void)TypeRegistry::instance().register_scalar<bool>("bool");

    CHECK_OUTPUT(testing::eval_node<MirrorSignalForEvalNode>({true, none, true}), {true, none, true});
}

// ---------------------------------------------------------------------------
// keys_ / union operators (operators/collection.h) — the building blocks
// map_ composes for its derived __keys__ lifecycle set.
// ---------------------------------------------------------------------------

TEST_CASE("collections: keys_ mirrors a TSD's key membership as a TSS")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    // The key set is a ZERO-COPY view with DEDICATED modified tracking
    // (stamped only by membership changes): a pure value tick on the dict is
    // silent on the key-set surface.
    CHECK_OUTPUT((eval_node<stdlib::keys_, TSD<Str, TS<Int>>>(
                     values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                                   dict_delta<Str, TS<Int>>({{"a"s, 9}}),   // value tick: no key change
                                   dict_delta<Str, TS<Int>>({}, {"a"s})))),
                 values<Value>(set_delta<Str>({"a"s, "b"s}, {}),
                               none,
                               set_delta<Str>({}, {"a"s})));
}

TEST_CASE("collections: rekey maps TSD values through scalar key time-series")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::rekey, TSD<Int, TS<Int>>, TSD<Int, TS<Str>>>(
                     values<Value>(none,
                                   dict_delta<Int, TS<Int>>({{1, 1}, {3, 3}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}}),
                                   none,
                                   dict_delta<Int, TS<Int>>({}, {2}),
                                   dict_delta<Int, TS<Int>>({{2, 4}})),
                     values<Value>(dict_delta<Int, TS<Str>>({{1, "a"s}, {2, "b"s}}),
                                   none,
                                   none,
                                   dict_delta<Int, TS<Str>>({{1, "c"s}}),
                                   none,
                                   none))),
                 values<Value>(none,
                               dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                               dict_delta<Str, TS<Int>>({{"b"s, 2}}),
                               dict_delta<Str, TS<Int>>({{"c"s, 1}}, {"a"s}),
                               dict_delta<Str, TS<Int>>({}, {"b"s}),
                               dict_delta<Str, TS<Int>>({{"b"s, 4}})));
}

TEST_CASE("collections: rekey removes output when scalar key mapping is removed")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::rekey, TSD<Int, TS<Int>>, TSD<Int, TS<Str>>>(
                     values<Value>(none,
                                   dict_delta<Int, TS<Int>>({{1, 1}}),
                                   none,
                                   none,
                                   dict_delta<Int, TS<Int>>({{1, 2}})),
                     values<Value>(dict_delta<Int, TS<Str>>({{1, "a"s}}),
                                   none,
                                   dict_delta<Int, TS<Str>>({}, {1}),
                                   none,
                                   none))),
                 values<Value>(none,
                               dict_delta<Str, TS<Int>>({{"a"s, 1}}),
                               dict_delta<Str, TS<Int>>({}, {"a"s}),
                               none,
                               none));
}

TEST_CASE("collections: flip swaps TSD scalar values into keys")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::flip, TSD<Int, TS<Str>>>(
                     values<Value>(dict_delta<Int, TS<Str>>({{1, "a"s}, {2, "b"s}}),
                                   dict_delta<Int, TS<Str>>({{1, "c"s}}),
                                   dict_delta<Int, TS<Str>>({}, {2})))),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a"s, 1}, {"b"s, 2}}),
                               dict_delta<Str, TS<Int>>({{"c"s, 1}}, {"a"s}),
                               dict_delta<Str, TS<Int>>({}, {"b"s})));
}

TEST_CASE("collections: partition splits a TSD by scalar partition keys")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::partition, TSD<Int, TS<Int>>, TSD<Int, TS<Str>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}, {3, 3}}),
                                   dict_delta<Int, TS<Int>>({{1, 4}, {2, 5}, {3, 6}}),
                                   dict_delta<Int, TS<Int>>({}, {1}),
                                   none,
                                   none),
                     values<Value>(dict_delta<Int, TS<Str>>({{1, "odd"s}}),
                                   dict_delta<Int, TS<Str>>({{2, "even"s}, {3, "odd"s}}),
                                   none,
                                   dict_delta<Int, TS<Str>>({}, {2}),
                                   dict_delta<Int, TS<Str>>({{3, "prime"s}})))),
                 values<Value>(
                     dict_delta<Str, TSD<Int, TS<Int>>>({{"odd"s, dict_delta<Int, TS<Int>>({{1, 1}})}}),
                     dict_delta<Str, TSD<Int, TS<Int>>>(
                         {{"odd"s, dict_delta<Int, TS<Int>>({{1, 4}, {3, 6}})},
                          {"even"s, dict_delta<Int, TS<Int>>({{2, 5}})}}),
                     dict_delta<Str, TSD<Int, TS<Int>>>({{"odd"s, dict_delta<Int, TS<Int>>({}, {1})}}),
                     dict_delta<Str, TSD<Int, TS<Int>>>({{"even"s, dict_delta<Int, TS<Int>>({}, {2})}}),
                     dict_delta<Str, TSD<Int, TS<Int>>>(
                         {{"prime"s, dict_delta<Int, TS<Int>>({{3, 6}})},
                          {"odd"s, dict_delta<Int, TS<Int>>({}, {3})}})));
}

TEST_CASE("collections: unpartition flattens nested TSD inner deltas")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::unpartition, TSD<Str, TSD<Int, TS<Int>>>>(
                     values<Value>(
                         dict_delta<Str, TSD<Int, TS<Int>>>({{"odd"s, dict_delta<Int, TS<Int>>({{1, 1}})}}),
                         dict_delta<Str, TSD<Int, TS<Int>>>(
                             {{"odd"s, dict_delta<Int, TS<Int>>({{1, 4}, {3, 6}})},
                              {"even"s, dict_delta<Int, TS<Int>>({{2, 5}})}}),
                         dict_delta<Str, TSD<Int, TS<Int>>>({{"odd"s, dict_delta<Int, TS<Int>>({}, {1})}}),
                         dict_delta<Str, TSD<Int, TS<Int>>>({{"even"s, dict_delta<Int, TS<Int>>({}, {2})}}),
                         dict_delta<Str, TSD<Int, TS<Int>>>(
                             {{"prime"s, dict_delta<Int, TS<Int>>({{3, 6}})},
                              {"odd"s, dict_delta<Int, TS<Int>>({}, {3})}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                               dict_delta<Int, TS<Int>>({{1, 4}, {3, 6}, {2, 5}}),
                               dict_delta<Int, TS<Int>>({}, {1}),
                               dict_delta<Int, TS<Int>>({}, {2}),
                               dict_delta<Int, TS<Int>>({{3, 6}})));
}

TEST_CASE("collections: union removes an element only when no input still holds it")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::union_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2}, {}),
                                   set_delta<Int>({}, {2}),   // 2 still in rhs
                                   none),
                     values<Value>(set_delta<Int>({2, 3}, {}),
                                   none,
                                   set_delta<Int>({}, {2})))),  // now gone everywhere
                 values<Value>(set_delta<Int>({1, 2, 3}, {}),
                               none,
                               set_delta<Int>({}, {2})));
}

TEST_CASE("collections: union folds across three inputs")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::union_, TSS<Int>, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1}, {})),
                     values<Value>(set_delta<Int>({2}, {})),
                     values<Value>(set_delta<Int>({3}, {})))),
                 values<Value>(set_delta<Int>({1, 2, 3}, {})));
}

TEST_CASE("collections: intersection keeps only keys held by every TSS input")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::intersection_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {})),
                     values<Value>(set_delta<Int>({3, 4, 5}, {})))),
                 values<Value>(set_delta<Int>({3}, {})));
}

TEST_CASE("collections: difference subtracts rhs keys from lhs")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::difference_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {})),
                     values<Value>(set_delta<Int>({3, 4, 5}, {})))),
                 values<Value>(set_delta<Int>({1, 2}, {})));
}

TEST_CASE("collections: symmetric_difference keeps keys held by exactly one side")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::symmetric_difference_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {})),
                     values<Value>(set_delta<Int>({3, 4, 5}, {})))),
                 values<Value>(set_delta<Int>({1, 2, 4, 5}, {})));
}

TEST_CASE("collections: symmetric_difference folds across three inputs")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::symmetric_difference_, TSS<Int>, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2}, {})),
                     values<Value>(set_delta<Int>({2, 3}, {})),
                     values<Value>(set_delta<Int>({3, 4}, {})))),
                 values<Value>(set_delta<Int>({1, 4}, {})));
}

TEST_CASE("collections: TSS bitwise and subtraction operators mirror set algebra")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::bit_or, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2}, {}),
                                   set_delta<Int>({}, {2}),
                                   none),
                     values<Value>(set_delta<Int>({2, 3}, {}),
                                   none,
                                   set_delta<Int>({}, {2})))),
                 values<Value>(set_delta<Int>({1, 2, 3}, {}),
                               none,
                               set_delta<Int>({}, {2})));

    CHECK_OUTPUT((eval_node<stdlib::bit_and, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {}),
                                   set_delta<Int>({}, {3}),
                                   none),
                     values<Value>(set_delta<Int>({2, 3, 4}, {}),
                                   none,
                                   set_delta<Int>({1}, {})))),
                 values<Value>(set_delta<Int>({2, 3}, {}),
                               set_delta<Int>({}, {3}),
                               set_delta<Int>({1}, {})));

    CHECK_OUTPUT((eval_node<stdlib::sub_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2, 3}, {}),
                                   none,
                                   set_delta<Int>({4}, {})),
                     values<Value>(set_delta<Int>({2}, {}),
                                   set_delta<Int>({3}, {}),
                                   none))),
                 values<Value>(set_delta<Int>({1, 3}, {}),
                               set_delta<Int>({}, {3}),
                               set_delta<Int>({4}, {})));

    CHECK_OUTPUT((eval_node<stdlib::bit_xor, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({1, 2}, {}),
                                   none),
                     values<Value>(set_delta<Int>({2, 3}, {}),
                                   set_delta<Int>({}, {2})))),
                 values<Value>(set_delta<Int>({1, 3}, {}),
                               set_delta<Int>({2}, {})));
}

TEST_CASE("collections: TSD bitwise and subtraction operators mirror set algebra")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    // hgraph gating: BOTH inputs must be valid - lhs-before-rhs emits
    // nothing until rhs arrives, then the withheld keys backfill.
    CHECK_OUTPUT((eval_node<stdlib::sub_, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}})),
                     values<Value>(none,
                                   dict_delta<Int, TS<Int>>({{3, 2}})))),
                 values<Value>(none,
                               dict_delta<Int, TS<Int>>({{1, 1}, {2, 2}})));

    CHECK_OUTPUT((eval_node<stdlib::bit_or, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{2, 3}}),
                                   dict_delta<Int, TS<Int>>({{3, 2}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 3}}),
                               dict_delta<Int, TS<Int>>({{2, 2}, {3, 2}})));

    // A DISJOINT first tick VALIDATES (the empty dict), hgraph parity.
    CHECK_OUTPUT((eval_node<stdlib::bit_and, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{2, 3}}),
                                   dict_delta<Int, TS<Int>>({{3, 2}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({}),
                               dict_delta<Int, TS<Int>>({{2, 2}})));

    CHECK_OUTPUT((eval_node<stdlib::bit_xor, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}})),
                     values<Value>(dict_delta<Int, TS<Int>>({{2, 3}}),
                                   dict_delta<Int, TS<Int>>({{3, 2}})))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 1}, {2, 3}}),
                               dict_delta<Int, TS<Int>>({{3, 2}}, {2})));
}

TEST_CASE("collections: TSD set operators reselect remaining values after overlap changes")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::bit_or, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                                   dict_delta<Int, TS<Int>>({}, {1})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 20}}),
                                   none))),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                               dict_delta<Int, TS<Int>>({{1, 20}})));

    CHECK_OUTPUT((eval_node<stdlib::bit_xor, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}}),
                                   dict_delta<Int, TS<Int>>({}, {1})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 20}}),
                                   none))),
                 values<Value>(none,
                               dict_delta<Int, TS<Int>>({{1, 20}})));
}

TEST_CASE("collections: TSS truthiness and equality mirror Python operators")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::not_, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1}, {}),
                                   set_delta<Int>({}, {1})))),
                 values<Bool>(true, false, true));

    CHECK_OUTPUT((eval_node<stdlib::and_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1}, {})),
                     values<Value>(set_delta<Int>({1}, {}),
                                   set_delta<Int>({2}, {})))),
                 values<Bool>(false, true));

    CHECK_OUTPUT((eval_node<stdlib::or_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1}, {})),
                     values<Value>(set_delta<Int>({1}, {}),
                                   set_delta<Int>({2}, {})))),
                 values<Bool>(true, true));

    CHECK_OUTPUT((eval_node<stdlib::eq_, TSS<Int>, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1, 2}, {}),
                                   set_delta<Int>({3}, {})),
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1}, {}),
                                   set_delta<Int>({2, 3}, {})))),
                 values<Bool>(true, false, true));
}

TEST_CASE("collections: TSD truthiness and equality mirror Python operators")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::not_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({}),
                                   dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({}, {1})))),
                 values<Bool>(true, false, true));

    CHECK_OUTPUT((eval_node<stdlib::eq_, TSD<Int, TS<Int>>, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({}),
                                   dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}}),
                                   dict_delta<Int, TS<Int>>({{1, 10}})),
                     values<Value>(dict_delta<Int, TS<Int>>({}),
                                   dict_delta<Int, TS<Int>>({{2, 2}}),
                                   dict_delta<Int, TS<Int>>({{1, 1}}),
                                   none))),
                 values<Bool>(true, false, true, false));
}

TEST_CASE("collections: TSS unary min max and sum reduce current values")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::min_, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1, 2, -1, 3}, {}),
                                   set_delta<Int>({}, {-1})))),
                 values<Int>(none, -1, 1));

    CHECK_OUTPUT((eval_node<stdlib::max_, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1, 2, -1, 3}, {}),
                                   set_delta<Int>({}, {3})))),
                 values<Int>(none, 3, 2));

    CHECK_OUTPUT((eval_node<stdlib::sum_, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1, 2, -1, 3}, {}),
                                   set_delta<Int>({}, {-1})))),
                 values<Int>(0, 5, 6));
}

TEST_CASE("collections: TSS unary mean std and variance match Python analytics")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    const auto mean_out = eval_node<stdlib::mean, TSS<Int>>(
        values<Value>(set_delta<Int>({}, {}),
                      set_delta<Int>({1}, {}),
                      set_delta<Int>({1, 2, -1, 3}, {}),
                      set_delta<Int>({}, {-1})));
    REQUIRE(mean_out.size() == 4);
    REQUIRE(mean_out[0].has_value());
    CHECK(std::isnan(mean_out[0]->view().checked_as<Float>()));
    REQUIRE(mean_out[1].has_value());
    CHECK(mean_out[1]->view().checked_as<Float>() == 1.0);
    REQUIRE(mean_out[2].has_value());
    CHECK(mean_out[2]->view().checked_as<Float>() == 1.25);
    REQUIRE(mean_out[3].has_value());
    CHECK(mean_out[3]->view().checked_as<Float>() == 2.0);

    const auto std_out = eval_node<stdlib::std_, TSS<Int>>(
        values<Value>(set_delta<Int>({}, {}),
                      set_delta<Int>({1}, {}),
                      set_delta<Int>({1, 2}, {}),
                      set_delta<Int>({-1, 3}, {})));
    REQUIRE(std_out.size() == 4);
    REQUIRE(std_out[0].has_value());
    CHECK(std_out[0]->view().checked_as<Float>() == 0.0);
    REQUIRE(std_out[1].has_value());
    CHECK(std_out[1]->view().checked_as<Float>() == 0.0);
    REQUIRE(std_out[2].has_value());
    CHECK(std::abs(std_out[2]->view().checked_as<Float>() - std::sqrt(0.5)) < 1e-12);
    REQUIRE(std_out[3].has_value());
    CHECK(std::abs(std_out[3]->view().checked_as<Float>() - std::sqrt(35.0 / 12.0)) < 1e-12);

    CHECK_OUTPUT((eval_node<stdlib::var_, TSS<Int>>(
                     values<Value>(set_delta<Int>({}, {}),
                                   set_delta<Int>({1}, {}),
                                   set_delta<Int>({1, 2}, {}),
                                   set_delta<Int>({-1, 3}, {})))),
                 values<Float>(0.0, 0.0, 0.5, 35.0 / 12.0));
}

TEST_CASE("collections: TSD unary min max and sum reduce valid child values")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::min_, TSD<Int, TS<Float>>>(
                     values<Value>(dict_delta<Int, TS<Float>>({}),
                                   dict_delta<Int, TS<Float>>({{3, 2.0}, {1, -1.0}}),
                                   dict_delta<Int, TS<Float>>({{1, 4.0}}),
                                   dict_delta<Int, TS<Float>>({}, {3})))),
                 values<Float>(none, -1.0, 2.0, 4.0));

    CHECK_OUTPUT((eval_node<stdlib::max_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({}),
                                   dict_delta<Int, TS<Int>>({{3, 2}, {100, -100}}),
                                   dict_delta<Int, TS<Int>>({{100, 5}}),
                                   dict_delta<Int, TS<Int>>({}, {100})))),
                 values<Int>(none, 2, 5, 2));

    CHECK_OUTPUT((eval_node<stdlib::sum_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({}),
                                   dict_delta<Int, TS<Int>>({{3, 2}, {1, 100}}),
                                   dict_delta<Int, TS<Int>>({{1, -1}}),
                                   dict_delta<Int, TS<Int>>({}, {3})))),
                 values<Int>(0, 102, 1, -1));
}

TEST_CASE("collections: TSD unary mean std and variance reduce valid child values")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    const auto mean_out = eval_node<stdlib::mean, TSD<Int, TS<Int>>>(
        values<Value>(dict_delta<Int, TS<Int>>({}),
                      dict_delta<Int, TS<Int>>({{3, 2}, {1, 10}}),
                      dict_delta<Int, TS<Int>>({{1, -1}}),
                      dict_delta<Int, TS<Int>>({}, {3})));
    REQUIRE(mean_out.size() == 4);
    REQUIRE(mean_out[0].has_value());
    CHECK(std::isnan(mean_out[0]->view().checked_as<Float>()));
    REQUIRE(mean_out[1].has_value());
    CHECK(mean_out[1]->view().checked_as<Float>() == 6.0);
    REQUIRE(mean_out[2].has_value());
    CHECK(mean_out[2]->view().checked_as<Float>() == 0.5);
    REQUIRE(mean_out[3].has_value());
    CHECK(mean_out[3]->view().checked_as<Float>() == -1.0);

    const auto std_out = eval_node<stdlib::std_, TSD<Int, TS<Int>>>(
        values<Value>(dict_delta<Int, TS<Int>>({}),
                      dict_delta<Int, TS<Int>>({{1, 1}}),
                      dict_delta<Int, TS<Int>>({{2, 2}}),
                      dict_delta<Int, TS<Int>>({{3, -1}, {4, 3}})));
    REQUIRE(std_out.size() == 4);
    REQUIRE(std_out[0].has_value());
    CHECK(std_out[0]->view().checked_as<Float>() == 0.0);
    REQUIRE(std_out[1].has_value());
    CHECK(std_out[1]->view().checked_as<Float>() == 0.0);
    REQUIRE(std_out[2].has_value());
    CHECK(std::abs(std_out[2]->view().checked_as<Float>() - std::sqrt(0.5)) < 1e-12);
    REQUIRE(std_out[3].has_value());
    CHECK(std::abs(std_out[3]->view().checked_as<Float>() - std::sqrt(35.0 / 12.0)) < 1e-12);

    CHECK_OUTPUT((eval_node<stdlib::var_, TSD<Int, TS<Int>>>(
                     values<Value>(dict_delta<Int, TS<Int>>({}),
                                   dict_delta<Int, TS<Int>>({{1, 1}}),
                                   dict_delta<Int, TS<Int>>({{2, 2}}),
                                   dict_delta<Int, TS<Int>>({{3, -1}, {4, 3}})))),
                 values<Float>(0.0, 0.0, 0.5, 35.0 / 12.0));
}

TEST_CASE("collections: TSL unary min max and sum reduce valid child values")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::min_, TSL<TS<Int>, 5>>(
                     values<Value>(list_delta<TS<Int>>({}),
                                   list_delta<TS<Int>>({3, 5, 2, 8, 10}),
                                   list_delta<TS<Int>>({{2, 20}})))),
                 values<Int>(none, 2, 3));

    CHECK_OUTPUT((eval_node<stdlib::max_, TSL<TS<Float>, 3>>(
                     values<Value>(list_delta<TS<Float>>({}),
                                   list_delta<TS<Float>>({1.5, -2.0, 4.0}),
                                   list_delta<TS<Float>>({{2, 0.0}})))),
                 values<Float>(none, 4.0, 1.5));

    CHECK_OUTPUT((eval_node<stdlib::sum_, TSL<TS<Int>, 5>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                                   list_delta<TS<Int>>({{2, 3}, {3, 4}, {4, 5}}),
                                   list_delta<TS<Int>>({{0, 10}})))),
                 values<Int>(3, 15, 24));
}

TEST_CASE("collections: TSL unary mean std and variance match Python analytics")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    stdlib::register_standard_operators();

    CHECK_OUTPUT((eval_node<stdlib::mean, TSL<TS<Int>, 5>>(
                     values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                                   list_delta<TS<Int>>({{2, 3}, {3, 4}, {4, 5}}),
                                   list_delta<TS<Int>>({{0, 10}})))),
                 values<Float>(0.6, 3.0, 4.8));

    const auto std_out = eval_node<stdlib::std_, TSL<TS<Int>, 5>>(
        values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                      list_delta<TS<Int>>({{2, 3}, {3, 4}, {4, 5}}),
                      list_delta<TS<Int>>({{0, 10}})));
    REQUIRE(std_out.size() == 3);
    REQUIRE(std_out[0].has_value());
    CHECK(std::abs(std_out[0]->view().checked_as<Float>() - std::sqrt(0.5)) < 1e-12);
    REQUIRE(std_out[1].has_value());
    CHECK(std::abs(std_out[1]->view().checked_as<Float>() - std::sqrt(2.5)) < 1e-12);
    REQUIRE(std_out[2].has_value());
    CHECK(std::abs(std_out[2]->view().checked_as<Float>() - std::sqrt(9.7)) < 1e-12);

    const auto var_out = eval_node<stdlib::var_, TSL<TS<Int>, 5>>(
        values<Value>(list_delta<TS<Int>>({{0, 1}, {1, 2}}),
                      list_delta<TS<Int>>({{2, 3}, {3, 4}, {4, 5}}),
                      list_delta<TS<Int>>({{0, 10}})));
    REQUIRE(var_out.size() == 3);
    REQUIRE(var_out[0].has_value());
    CHECK(std::abs(var_out[0]->view().checked_as<Float>() - 0.5) < 1e-12);
    REQUIRE(var_out[1].has_value());
    CHECK(std::abs(var_out[1]->view().checked_as<Float>() - 2.5) < 1e-12);
    REQUIRE(var_out[2].has_value());
    CHECK(std::abs(var_out[2]->view().checked_as<Float>() - 9.7) < 1e-12);
}

TEST_CASE("collections: the TSD key-set view is read-only but reports the owner's mutations")
{
    using namespace hgraph;
    (void)TypeRegistry::instance().register_scalar<Int>("int");
    (void)TypeRegistry::instance().register_scalar<std::string>("string");

    const auto *schema = ts_type<TSD<Str, TS<Int>>>();
    TSOutput    output{schema};

    const DateTime t0 = MIN_ST;
    const DateTime t1 = MIN_ST + TimeDelta{1};

    // The owner mutates through the DICT surface; the key set reports it.
    {
        auto view     = output.view(t0);
        auto dict     = view.as_dict();
        auto mutation = dict.begin_mutation(t0);
        mutation.set(Value{Str{"a"}}.view(), Value{Int{1}}.view());
    }
    auto view_t1 = output.view(t1);
    auto dict_t1 = view_t1.as_dict();
    auto keys    = dict_t1.key_set();
    CHECK(keys.as_set().contains(Value{Str{"a"}}.view()));
    CHECK(keys.last_modified_time() == t0);

    // The view itself can never mutate.
    REQUIRE_THROWS_AS(keys.begin_mutation(t1), std::logic_error);
}
