#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/adaptor_wiring.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/types/static_node.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <typeindex>
#include <vector>

/**
 * Push sources inside service implementations.
 *
 * A service/adaptor implementation is NOT a nested graph in hg_cpp: registration
 * only records a materializer candidate. ``finish_top_level`` calls
 * ``Wiring::build_services()`` automatically, while the supported explicit
 * ``build_services()`` boundary runs the same materializers with
 * ``WiredFn::wire(target, …)``, which INLINES the implementation into the
 * registering (top-level) wiring. An implementation's push source is therefore an
 * ordinary root-graph node and lands in the push prefix.
 *
 * That is what these tests pin. The upstream Python reference instead compiles a
 * service impl into a nested graph node (``create_graph_builder(sink_nodes, False)``)
 * and rejects push sources there; genuine nested children here still reject them
 * (``GraphBuilder::nested_type()``), so the two edges are asserted side by side.
 *
 * See ``docs/source/developer_guide/services.rst`` ("Implementations are inlined").
 */

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    /** Synchronize publication of the sender from the executor's start thread.
     *
     *  ``PushSourceSender`` is safe to use from another thread after publication,
     *  but assigning and inspecting the multi-field handle concurrently would be
     *  a data race. The latch supplies the required happens-before edge. */
    class SenderLatch
    {
      public:
        void reset()
        {
            std::lock_guard lock{mutex_};
            sender_.reset();
        }

        void publish(PushSourceSender sender)
        {
            {
                std::lock_guard lock{mutex_};
                sender_ = std::move(sender);
            }
            ready_.notify_all();
        }

        [[nodiscard]] std::optional<PushSourceSender> await()
        {
            std::unique_lock lock{mutex_};
            if (!ready_.wait_for(lock, std::chrono::seconds{2}, [this] { return sender_.has_value(); }))
            {
                return std::nullopt;
            }
            return sender_;
        }

      private:
        std::mutex                      mutex_{};
        std::condition_variable         ready_{};
        std::optional<PushSourceSender> sender_{};
    };

    // A static ``compose`` has nowhere to capture test state, so the latches that
    // receive implementation senders at start live at namespace scope.
    inline SenderLatch ticks_sender{};
    inline SenderLatch alt_ticks_sender{};
    inline SenderLatch quotes_sender{};
    inline SenderLatch replies_sender{};
    inline SenderLatch adaptor_sender{};

    inline std::vector<Int> observed{};

    struct TicksTag
    {
    };
    struct AltTicksTag
    {
    };
    struct QuotesTag
    {
    };
    struct RepliesTag
    {
    };
    struct AdaptorTicksTag
    {
    };
    struct SinkTag
    {
    };

    /** Wire a push source of ``S`` into ``w`` and return its port.
     *
     *  ``add_unique_node`` (not ``add_node``) matches ``PyWiring::push_source``:
     *  a push source's identity is its allocation site. Interning would collapse
     *  two push sources that differ only in their ``on_start`` callback, because
     *  the callback lives in the builder context and not in the scalars. */
    template <typename S>
    [[nodiscard]] Port<S> push_source_port(Wiring &w, std::type_index def, SenderLatch &sender)
    {
        return Port<S>{w,
                       w.add_unique_node(def,
                                         make_push_source_node(
                                             *ts_type<S>(),
                                             [&sender](PushSourceSender started_sender) {
                                                 sender.publish(std::move(started_sender));
                                             }),
                                         std::span<const WiringPortRef>{},
                                         Value{})};
    }

    /** Collect the scalar values reaching ``port``, stopping the executor once
     *  ``stop_after`` have arrived so the test never waits out its end time. */
    void collect(Wiring &w, Port<TS<Int>> port, std::size_t stop_after)
    {
        const auto      *ts_int = ts_type<TS<Int>>();
        const std::array inputs{port.erased()};
        w.add_unique_node(std::type_index(typeid(SinkTag)),
                          collecting_scalar_sink<Int>(*single_input_schema(*ts_int), *ts_int, observed, stop_after),
                          std::span<const WiringPortRef>{inputs},
                          Value{});
    }

    // ---------------------------------------------------------------------
    // Reference service
    // ---------------------------------------------------------------------

    struct LiveTicksService
    {
        static constexpr std::string_view name{"live_ticks"};
        using output_schema = TS<Int>;
    };

    struct LiveTicksImpl
    {
        [[maybe_unused]] static constexpr auto name = "live_ticks_impl";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return push_source_port<TS<Int>>(w, std::type_index(typeid(TicksTag)), ticks_sender);
        }
    };

    struct ReferencePushGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_push_graph";

        static void compose(Wiring &w)
        {
            service::register_reference_service<LiveTicksService, LiveTicksImpl>(w);
            collect(w, wire<LiveTicksService>(w), 1);
        }
    };

    // A second, independent reference service — two implementations, two push
    // sources, both hoisted into the same root prefix.
    struct AltTicksService
    {
        static constexpr std::string_view name{"alt_ticks"};
        using output_schema = TS<Int>;
    };

    struct AltTicksImpl
    {
        [[maybe_unused]] static constexpr auto name = "alt_ticks_impl";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return push_source_port<TS<Int>>(w, std::type_index(typeid(AltTicksTag)), alt_ticks_sender);
        }
    };

    struct TwoPushServicesGraph
    {
        [[maybe_unused]] static constexpr auto name = "two_push_services_graph";

        static void compose(Wiring &w)
        {
            service::register_reference_service<LiveTicksService, LiveTicksImpl>(w);
            service::register_reference_service<AltTicksService, AltTicksImpl>(w);
            collect(w, wire<stdlib::add_>(w, wire<LiveTicksService>(w), wire<AltTicksService>(w)).as<TS<Int>>(), 1);
        }
    };

    // ---------------------------------------------------------------------
    // Subscription service
    // ---------------------------------------------------------------------

    struct QuotesService
    {
        static constexpr std::string_view name{"push_quotes"};
        using key_type     = Int;
        using value_schema = TS<Int>;
    };

    /** Broadcast each pushed value to every live subscription key. The push
     *  source carries the external feed; the keys arrive from clients. */
    struct QuotesFanOutNode
    {
        static constexpr auto name = "quotes_fan_out_node";

        static void eval(In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
                         In<"tick", TS<Int>>                           tick,
                         Out<TSD<Int, TS<Int>>>                        out)
        {
            if (!keys.valid() || !tick.valid()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (Int removed : keys.removed()) { static_cast<void>(mutation.erase(Value{removed}.view())); }
            for (Int key : keys.values()) { mutation.set(Value{key}.view(), Value{tick.value()}.view()); }
        }
    };

    struct QuotesImpl
    {
        [[maybe_unused]] static constexpr auto name = "push_quotes_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            auto tick = push_source_port<TS<Int>>(w, std::type_index(typeid(QuotesTag)), quotes_sender);
            return wire<QuotesFanOutNode>(w, keys, tick).as<TSD<Int, TS<Int>>>();
        }
    };

    struct SubscriptionPushGraph
    {
        [[maybe_unused]] static constexpr auto name = "subscription_push_graph";

        static void compose(Wiring &w)
        {
            service::register_subscription_service<QuotesService, QuotesImpl>(w);
            collect(w, wire<QuotesService>(w, wire<stdlib::const_>(w, Int{7}).as<TS<Int>>()), 1);
        }
    };

    // ---------------------------------------------------------------------
    // Request/reply service
    // ---------------------------------------------------------------------

    struct PushEchoService
    {
        static constexpr std::string_view name{"push_echo"};
        using request_schema  = TS<Int>;
        using response_schema = TS<Int>;
    };

    /** Reply to each outstanding request with the latest pushed value. */
    struct PushEchoImplNode
    {
        static constexpr auto name = "push_echo_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         In<"tick", TS<Int>>                                         tick,
                         Out<TSD<Int, TS<Int>>>                                      out)
        {
            if (!requests.valid() || !tick.valid()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                static_cast<void>(request);
                static_cast<void>(mutation.erase(Value{request_id}.view()));
            }
            for (const auto &[request_id, request] : requests.items())
            {
                if (!request.valid()) { continue; }
                mutation.set(Value{request_id}.view(), Value{tick.value()}.view());
            }
        }
    };

    struct PushEchoImpl
    {
        [[maybe_unused]] static constexpr auto name = "push_echo_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            auto tick = push_source_port<TS<Int>>(w, std::type_index(typeid(RepliesTag)), replies_sender);
            return wire<PushEchoImplNode>(w, requests, tick).as<TSD<Int, TS<Int>>>();
        }
    };

    struct RequestReplyPushGraph
    {
        [[maybe_unused]] static constexpr auto name = "request_reply_push_graph";

        static void compose(Wiring &w)
        {
            service::register_request_reply_service<PushEchoService, PushEchoImpl>(w);
            collect(w, wire<PushEchoService>(w, wire<stdlib::const_>(w, Int{1}).as<TS<Int>>()), 1);
        }
    };

    // ---------------------------------------------------------------------
    // Adaptor (confirmatory — adaptors were designed for exactly this)
    // ---------------------------------------------------------------------

    // SOURCE-ONLY. This was duplex when the test was written, because a
    // source-only adaptor also satisfied reference_service_interface and
    // wire<Interface> was ambiguous in any translation unit including both
    // boundary headers. RFC 0011 step 9 made the concepts disjoint, so the
    // workaround is gone and this is the shape the test always wanted.
    struct TickAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"tick_adaptor"};
        using output_schema = TS<Int>;
    };

    struct TickAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "tick_adaptor_impl";

        static void compose(Wiring &w)
        {
            adaptor::to_graph<TickAdaptor>(
                w, push_source_port<TS<Int>>(w, std::type_index(typeid(AdaptorTicksTag)), adaptor_sender));
        }
    };

    struct AdaptorPushGraph
    {
        [[maybe_unused]] static constexpr auto name = "adaptor_push_graph";

        static void compose(Wiring &w)
        {
            adaptor::register_adaptor<TickAdaptor, TickAdaptorImpl>(w);
            collect(w, wire<TickAdaptor>(w), 1);
        }
    };

    // ---------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------

    void reset_case()
    {
        hgraph::stdlib::register_standard_operators();
        observed.clear();
        ticks_sender.reset();
        alt_ticks_sender.reset();
        quotes_sender.reset();
        replies_sender.reset();
        adaptor_sender.reset();
    }

    /** Count the push-source prefix the runtime will drain. */
    [[nodiscard]] std::size_t push_prefix(const GraphBuilder &builder)
    {
        return builder.root_type().schema()->push_source_nodes_end;
    }

    [[nodiscard]] GraphExecutorValue start_realtime(GraphBuilder builder)
    {
        const DateTime       start = wall_now();
        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(builder))
            .mode(GraphExecutorMode::RealTime)
            .start_time(start)
            .end_time(start + TimeDelta{5'000'000});   // generous: the sink stops the run
        return executor_builder.make_executor();
    }

}   // namespace

TEST_CASE("service push sources: a reference service implementation owns a root push source")
{
    reset_case();

    GraphBuilder graph_builder = build_graph<ReferencePushGraph>();

    // The implementation was INLINED, so its push source is a node of the root
    // graph and occupies the push prefix the real-time drain loop walks. A
    // nested-graph implementation could not produce this shape at all.
    REQUIRE(graph_builder.node_count() > 1);
    CHECK(graph_builder.nodes()[0].type().schema()->node_kind == NodeKind::PushSource);
    CHECK(push_prefix(graph_builder) == 1);

    GraphExecutorValue executor = start_realtime(std::move(graph_builder));
    auto               view     = executor.view();

    AsyncGraphExecutorRun runner{view};
    auto sender = ticks_sender.await();
    REQUIRE(sender.has_value());
    sender->send_blocking(Int{42});
    runner.join();

    CHECK(observed == std::vector<Int>{Int{42}});
}

TEST_CASE("service push sources: two implementations contribute two push sources to one prefix")
{
    reset_case();

    GraphBuilder graph_builder = build_graph<TwoPushServicesGraph>();

    // Distinct nodes: ``add_unique_node`` keeps them apart even though both
    // builders carry identical schemas and empty scalars.
    REQUIRE(push_prefix(graph_builder) == 2);
    CHECK(graph_builder.nodes()[0].type().schema()->node_kind == NodeKind::PushSource);
    CHECK(graph_builder.nodes()[1].type().schema()->node_kind == NodeKind::PushSource);

    GraphExecutorValue executor = start_realtime(std::move(graph_builder));
    auto               view     = executor.view();

    AsyncGraphExecutorRun runner{view};
    auto ticks     = ticks_sender.await();
    auto alt_ticks = alt_ticks_sender.await();
    REQUIRE(ticks.has_value());
    REQUIRE(alt_ticks.has_value());
    ticks->send_blocking(Int{40});
    alt_ticks->send_blocking(Int{2});
    runner.join();

    REQUIRE(observed.size() == 1);
    CHECK(observed.front() == Int{42});
}

TEST_CASE("service push sources: a subscription service implementation owns a root push source")
{
    reset_case();

    GraphBuilder graph_builder = build_graph<SubscriptionPushGraph>();
    REQUIRE(push_prefix(graph_builder) == 1);

    GraphExecutorValue executor = start_realtime(std::move(graph_builder));
    auto               view     = executor.view();

    AsyncGraphExecutorRun runner{view};
    auto sender = quotes_sender.await();
    REQUIRE(sender.has_value());
    sender->send_blocking(Int{99});
    runner.join();

    CHECK(observed == std::vector<Int>{Int{99}});
}

TEST_CASE("service push sources: a request/reply service implementation owns a root push source")
{
    reset_case();

    GraphBuilder graph_builder = build_graph<RequestReplyPushGraph>();
    REQUIRE(push_prefix(graph_builder) == 1);

    GraphExecutorValue executor = start_realtime(std::move(graph_builder));
    auto               view     = executor.view();

    AsyncGraphExecutorRun runner{view};
    auto sender = replies_sender.await();
    REQUIRE(sender.has_value());
    sender->send_blocking(Int{55});
    runner.join();

    CHECK(observed == std::vector<Int>{Int{55}});
}

TEST_CASE("service push sources: an adaptor implementation owns a root push source")
{
    reset_case();

    GraphBuilder graph_builder = build_graph<AdaptorPushGraph>();
    REQUIRE(push_prefix(graph_builder) == 1);

    GraphExecutorValue executor = start_realtime(std::move(graph_builder));
    auto               view     = executor.view();

    AsyncGraphExecutorRun runner{view};
    auto sender = adaptor_sender.await();
    REQUIRE(sender.has_value());
    sender->send_blocking(Int{13});
    runner.join();

    CHECK(observed == std::vector<Int>{Int{13}});
}

TEST_CASE("service push sources: an implementation push source still requires a real-time executor")
{
    reset_case();

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(build_graph<ReferencePushGraph>())
        .mode(GraphExecutorMode::Simulation)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{1});

    CHECK_THROWS_AS(executor_builder.make_executor(), std::invalid_argument);
}

TEST_CASE("service push sources: a graph carrying an implementation push source has no nested type")
{
    reset_case();

    // The complementary edge: inlining is what makes the implementation's push
    // source legal. The same builder used as a CHILD graph is still rejected,
    // because the nested graph type is never interned when a push prefix exists.
    GraphBuilder graph_builder = build_graph<ReferencePushGraph>();
    REQUIRE(push_prefix(graph_builder) == 1);
    CHECK_THROWS_AS((void)graph_builder.nested_type(), std::invalid_argument);
}
