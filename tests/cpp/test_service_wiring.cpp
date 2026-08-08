#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/catch_test_macros.hpp>

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/adaptor_wiring.h>
#include <hgraph/types/context_wiring.h>
#include <hgraph/types/service_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <cstddef>
#include <concepts>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct PricesService
    {
        static constexpr std::string_view name{"prices"};
        using key_type     = Int;
        using value_schema = TS<Int>;
    };

    struct DerivedPricesService
    {
        static constexpr std::string_view name{"derived_prices"};
        using key_type     = Int;
        using value_schema = TS<Int>;
    };

    using StructuredPrice =
        TSB<"StructuredPrice", Field<"base", TS<Int>>, Field<"offset", TS<Int>>>;

    struct StructuredPricesService
    {
        static constexpr std::string_view name{"structured_prices"};
        using key_type     = Int;
        using value_schema = StructuredPrice;
    };

    using SubscriptionSetBundle =
        TSB<"SubscriptionSetBundle", Field<"values", TSS<Int>>>;

    struct SetBundleService
    {
        static constexpr std::string_view name{"set_bundle"};
        using key_type     = Str;
        using value_schema = SubscriptionSetBundle;
    };

    using BaseSubscriptionRequest =
        Bundle<"tests.service::BaseSubscriptionRequest", Field<"id", Int>>;

    struct BundlePricesService
    {
        static constexpr std::string_view name{"bundle_prices"};
        using key_type     = BaseSubscriptionRequest;
        using value_schema = TS<Int>;
    };

    const ValueTypeMetaData *register_derived_subscription_request()
    {
        auto &registry = TypeRegistry::instance();
        return registry.bundle(
            "tests.service", "DerivedSubscriptionRequest",
            {{"id", scalar_descriptor<Int>::value_meta()},
             {"multiplier", scalar_descriptor<Int>::value_meta()}},
            {scalar_descriptor<BaseSubscriptionRequest>::value_meta()});
    }

    Value derived_subscription_request(
        const ValueTypeMetaData *schema, Int id, Int multiplier)
    {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema)};
        builder.set("id", Value{id}.view());
        builder.set("multiplier", Value{multiplier}.view());
        return builder.build();
    }

    struct ReferencePricesService
    {
        static constexpr std::string_view name{"reference_prices"};
        using output_schema = TSD<Int, TS<Int>>;
    };

    struct AddOneService
    {
        static constexpr std::string_view name{"add_one"};
        using request_schema  = TS<Int>;
        using response_schema = TS<Int>;
    };

    struct AddTenService
    {
        static constexpr std::string_view name{"add_ten"};
        using request_schema  = TS<Int>;
        using response_schema = TS<Int>;
    };

    struct ReplylessPublishService
    {
        static constexpr std::string_view name{"replyless_publish"};
        using request_schema = TS<Int>;
    };

    using PairRequest = TSB<"PairRequest",
                            Field<"left", TS<Int>>,
                            Field<"right", TS<Int>>>;
    using IfIntRefBundle =
        UnNamedTSB<Field<"true", REF<TS<Int>>>, Field<"false", REF<TS<Int>>>>;

    struct SumPairService
    {
        static constexpr std::string_view name{"sum_pair"};
        using request_schema  = PairRequest;
        using response_schema = TS<Int>;
    };

    struct BaseValueService
    {
        static constexpr std::string_view name{"base_value"};
        using output_schema = TS<Int>;
    };

    struct DerivedValueService
    {
        static constexpr std::string_view name{"derived_value"};
        using output_schema = TS<Int>;
    };

    using KafkaReplayState =
        TSB<"KafkaReplayState",
            Field<"msg", TS<Int>>,
            Field<"recovered", TS<Bool>>>;

    struct KafkaHistoryService
    {
        static constexpr std::string_view name{"kafka_history"};
        using output_schema = KafkaReplayState;
    };

    struct KafkaLiveService
    {
        static constexpr std::string_view name{"kafka_live"};
        using output_schema = TS<Int>;
    };

    struct KafkaHistorySource
    {
        static constexpr auto name              = "kafka_history_source";
        static constexpr bool schedule_on_start = true;

        static void eval(NodeScheduler sched, State<Int> cycle,
                         Out<KafkaReplayState> out)
        {
            if (cycle.get() == Int{0})
            {
                out.field<"msg">().set(Int{10});
                out.field<"recovered">().set(Bool{false});
                cycle.set(Int{1});
                sched.schedule(MIN_TD);
            }
            else
            {
                out.field<"recovered">().set(Bool{true});
            }
        }
    };

    struct KafkaLiveSource
    {
        static constexpr auto name              = "kafka_live_source";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{20}); }
    };

    inline int kafka_service_compositions = 0;

    struct KafkaReplayAndLiveImpl
    {
        [[maybe_unused]] static constexpr auto name =
            "kafka_replay_and_live_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            ++kafka_service_compositions;
            const auto custom = service::path(path.value());
            service::impl_output<KafkaHistoryService>(
                w, custom, wire<KafkaHistorySource>(w));
            service::impl_output<KafkaLiveService>(
                w, custom, wire<KafkaLiveSource>(w));
        }
    };

    struct KafkaReplayAndLiveClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "kafka_replay_and_live_client_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            const auto topic = service::path("orders");
            service::register_services<
                KafkaReplayAndLiveImpl,
                KafkaHistoryService,
                KafkaLiveService>(w, topic);

            auto history = wire<KafkaHistoryService>(w, topic);
            auto recovered = wire<stdlib::getitem_>(
                                 w, history, Str{"recovered"})
                                 .as<TS<Bool>>();
            auto historical_message = wire<stdlib::getitem_>(
                                          w, history, Str{"msg"})
                                          .as<TS<Int>>();

            // Two public clients consume the same per-topic live reference.
            // The one multi-service implementation is the native sharing
            // boundary used by the Python Kafka adaptor.
            auto first_live = wire<KafkaLiveService>(w, topic);
            auto second_live = wire<KafkaLiveService>(w, topic);
            auto first = wire<stdlib::if_then_else>(
                             w, recovered, first_live, historical_message)
                             .as<TS<Int>>();
            auto second = wire<stdlib::if_then_else>(
                              w, recovered, second_live, historical_message)
                              .as<TS<Int>>();
            return wire<stdlib::add_>(w, first, second).as<TS<Int>>();
        }
    };

    struct ReferencePricesImplNode
    {
        static constexpr auto name              = "reference_prices_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key_7{Int{7}};
            Value price_7{Int{70}};
            Value key_8{Int{8}};
            Value price_8{Int{80}};
            mutation.set(key_7.view(), price_7.view());
            mutation.set(key_8.view(), price_8.view());
        }
    };

    struct ReferencePricesAltImplNode
    {
        static constexpr auto name              = "reference_prices_alt_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key_7{Int{7}};
            Value price_7{Int{700}};
            Value key_8{Int{8}};
            Value price_8{Int{800}};
            mutation.set(key_7.view(), price_7.view());
            mutation.set(key_8.view(), price_8.view());
        }
    };

    struct ReferencePricesPathImplNode
    {
        static constexpr auto name              = "reference_prices_path_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"path", Str> path, Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key{Int{7}};
            Value price{path.value() == "premium" ? Int{777} : Int{70}};
            mutation.set(key.view(), price.view());
        }
    };

    struct TenSourceNode
    {
        static constexpr auto name              = "ten_source_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Int>> out) { out.set(Int{10}); }
    };

    struct AddOneValueNode
    {
        static constexpr auto name = "add_one_value_node";

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value() + Int{1});
        }
    };

    struct DeltaValuePassThroughNode
    {
        static constexpr auto name = "delta_value_pass_through";

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            const ValueView delta = value.delta_value();
            if (delta.has_value()) { out.set(delta.checked_as<Int>()); }
        }
    };

    struct BaseValueImpl
    {
        [[maybe_unused]] static constexpr auto name = "base_value_impl";

        static Port<TS<Int>> compose(Wiring &w)
        {
            return wire<TenSourceNode>(w);
        }
    };

    struct DerivedValueImpl
    {
        [[maybe_unused]] static constexpr auto name = "derived_value_impl";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto base = wire<BaseValueService>(w);
            return wire<AddOneValueNode>(w, base);
        }
    };

    inline int lazy_reference_compositions = 0;

    struct CountingBaseValueImpl
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            ++lazy_reference_compositions;
            return wire<TenSourceNode>(w);
        }
    };

    struct UnusedReferenceServiceGraph
    {
        static void compose(Wiring &w)
        {
            service::register_reference_service<BaseValueService, CountingBaseValueImpl>(w);
        }
    };

    struct RequestedReferenceServiceGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<BaseValueService, CountingBaseValueImpl>(w);
            return wire<BaseValueService>(w);
        }
    };

    struct TypedReferencePricesPathImplNode
    {
        static constexpr auto name              = "typed_reference_prices_path_impl_node";
        static constexpr bool schedule_on_start = true;

        static void eval(Scalar<"path", Str> path, Out<TSD<Int, TS<Int>>> out)
        {
            auto mutation = out.begin_mutation(out.evaluation_time());
            Value key{Int{7}};
            Value price{path.value().find("premium") != std::string::npos ? Int{777} : Int{70}};
            mutation.set(key.view(), price.view());
        }
    };

    struct PricesImplNode
    {
        static constexpr auto name = "prices_impl_node";

        static void eval(In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!keys.valid()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (Int removed : keys.removed()) { static_cast<void>(mutation.erase(Value{removed}.view())); }
            for (Int key : keys.values())
            {
                Value key_value{key};
                Value price{key * Int{10}};
                mutation.set(key_value.view(), price.view());
            }
        }
    };

    struct OffsetPricesImplNode
    {
        static constexpr auto name = "offset_prices_impl_node";

        static void eval(
            In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
            In<"offset", TS<Int>> offset,
            Out<TSD<Int, TS<Int>>> out)
        {
            if (!keys.valid()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (Int removed : keys.removed())
            {
                static_cast<void>(mutation.erase(Value{removed}.view()));
            }
            for (Int key : keys.values())
            {
                Value key_value{key};
                Value price{key * Int{10} + offset.value()};
                mutation.set(key_value.view(), price.view());
            }
        }
    };

    struct BundlePriceForKeyNode
    {
        static constexpr auto name = "bundle_price_for_key_node";

        static void eval(In<"key", TS<BaseSubscriptionRequest>> key,
                         Out<TS<Int>> out)
        {
            const Int id = static_cast<const TSInputView &>(key)
                               .value()
                               .as_bundle()
                               .field("id")
                               .checked_as<Int>();
            out.set(id * Int{10});
        }
    };

    struct BundlePriceForKeyGraph
    {
        [[maybe_unused]] static constexpr auto name = "bundle_price_for_key_graph";

        static Port<TS<Int>> compose(
            Wiring &w, NamedPort<"key", TS<BaseSubscriptionRequest>> key)
        {
            return wire<BundlePriceForKeyNode>(w, key);
        }
    };

    struct DelayedSetBundleResponseNode
    {
        static constexpr auto name = "delayed_set_bundle_response_node";

        static void eval(In<"keys", TSS<Str>, InputValidity::Unchecked> keys,
                         NodeScheduler scheduler,
                         State<Int> phase,
                         Out<TSD<Str, SubscriptionSetBundle>> out)
        {
            if (!keys.valid()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const Str &removed : keys.removed())
            {
                const Value key{removed};
                static_cast<void>(mutation.erase(key.view()));
            }

            if (phase.get() == Int{0})
            {
                for (const Str &key : keys.values())
                {
                    const Value key_value{key};
                    static_cast<void>(mutation.at(key_value.view()));
                }
                phase.set(Int{1});
                scheduler.schedule(MIN_TD);
                return;
            }

            if (phase.get() == Int{1})
            {
                for (const Str &key : keys.values())
                {
                    const Value key_value{key};
                    auto child = mutation.at(key_value.view());
                    auto values = child.indexed_child_at(0);
                    auto set_mutation = values.as_set().begin_mutation(
                        out.evaluation_time());
                    const Value item{Int{1}};
                    static_cast<void>(set_mutation.add(item.view()));
                }
                phase.set(Int{2});
            }
        }
    };

    struct SetBundleServiceImpl
    {
        [[maybe_unused]] static constexpr auto name = "set_bundle_service_impl";

        static Port<TSD<Str, SubscriptionSetBundle>> compose(
            Wiring &w, Port<TSS<Str>> keys)
        {
            return wire<DelayedSetBundleResponseNode>(w, keys);
        }
    };

    struct SetBundleServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "set_bundle_service_client_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Str>> key)
        {
            service::register_subscription_service<SetBundleService, SetBundleServiceImpl>(w);
            auto response = wire<SetBundleService>(w, key);
            auto values = wire<stdlib::getitem_>(w, response, Str{"values"}).as<TSS<Int>>();
            return wire<stdlib::contains_>(w, values, Int{1}).as<TS<Bool>>();
        }
    };

    struct PricesPathImplNode
    {
        static constexpr auto name = "prices_path_impl_node";

        static void eval(Scalar<"path", Str> path,
                         In<"keys", TSS<Int>, InputValidity::Unchecked> keys,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!keys.valid()) { return; }

            const Int multiplier = path.value() == "premium" ? Int{100} : Int{10};
            auto mutation = out.begin_mutation(out.evaluation_time());
            for (Int removed : keys.removed()) { static_cast<void>(mutation.erase(Value{removed}.view())); }
            for (Int key : keys.values())
            {
                Value key_value{key};
                Value price{key * multiplier};
                mutation.set(key_value.view(), price.view());
            }
        }
    };

    struct AddOneImplNode
    {
        static constexpr auto name = "add_one_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{1}};
                mutation.set(request_id, response.view());
            }
        }
    };

    inline std::vector<std::pair<std::size_t, Int>> observed_replyless_requests;

    struct ObserveReplylessRequestsNode
    {
        static constexpr auto name = "observe_replyless_requests_node";

        static void eval(
            DateTime evaluation_time,
            In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests)
        {
            if (!requests.modified()) { return; }
            const auto cycle = static_cast<std::size_t>(
                (evaluation_time - MIN_ST) / MIN_TD);
            for (const auto &[request_id, request] : requests.modified_items())
            {
                (void)request_id;
                if (request.valid())
                {
                    observed_replyless_requests.emplace_back(cycle, request.value());
                }
            }
        }
    };

    struct ReplylessPublishImplGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "replyless_publish_impl_graph";

        static void compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            static_cast<void>(wire<ObserveReplylessRequestsNode>(w, requests));
        }
    };

    struct AddOnePathImplNode
    {
        static constexpr auto name = "add_one_path_impl_node";

        static void eval(Scalar<"path", Str> path,
                         In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            const Int addend = path.value() == "premium" ? Int{100} : Int{1};
            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + addend};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddTenImplNode
    {
        static constexpr auto name = "add_ten_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{10}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct SumPairImplNode
    {
        static constexpr auto name = "sum_pair_impl_node";

        static void eval(In<"requests", TSD<Int, PairRequest>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                auto left  = request.field<"left">();
                auto right = request.field<"right">();
                if (!left.valid() || !right.valid()) { continue; }

                Value response{left.value() + right.value()};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddTwentyServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"add_twenty_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct AddThirtyServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"add_thirty_adaptor"};
        using input_schema  = TS<Int>;
        using output_schema = TS<Int>;
    };

    struct PublishPairServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"publish_pair_adaptor"};
        using input_schema = PairRequest;
    };

    inline std::vector<std::tuple<Int, Int, Int>> published_pair_requests;

    struct PublishPairServiceAdaptorImplNode
    {
        static constexpr auto name = "publish_pair_service_adaptor_impl_node";

        static void eval(
            In<"requests", TSD<Int, PairRequest>, InputValidity::Unchecked> requests)
        {
            if (!requests.modified()) { return; }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                auto left = request.field<"left">();
                auto right = request.field<"right">();
                if (left.valid() && right.valid())
                {
                    published_pair_requests.emplace_back(
                        request_id.checked_as<Int>(), left.value(), right.value());
                }
            }
        }
    };

    struct GenericAddOneService
    {
        static constexpr std::string_view name{"generic_add_one"};
        using request_schema  = TS<ScalarVar<"NUMBER", Int, Float>>;
        using response_schema = TS<ScalarVar<"NUMBER", Int, Float>>;
    };

    struct GenericServiceAdaptor : service_adaptor::interface
    {
        static constexpr std::string_view name{"generic_service_adaptor"};
        using input_schema  = TS<ScalarVar<"T">>;
        using output_schema = TS<ScalarVar<"T">>;
    };

    struct AddTwentyServiceAdaptorImplNode
    {
        static constexpr auto name = "add_twenty_service_adaptor_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{20}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddThirtyServiceAdaptorImplNode
    {
        static constexpr auto name = "add_thirty_service_adaptor_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Int>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Int>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }

                Value response{request.value() + Int{30}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct AddTwentyServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "add_twenty_service_adaptor_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service_adaptor::path(path.value());
            auto requests = service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
            auto replies = wire<AddTwentyServiceAdaptorImplNode>(w, requests).as<TSD<Int, TS<Int>>>();
            service_adaptor::to_graph<AddTwentyServiceAdaptor>(w, custom, replies);
        }
    };

    struct MultiServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_adaptor_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service_adaptor::path(path.value());
            auto add_twenty_requests = service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
            auto add_thirty_requests = service_adaptor::from_graph<AddThirtyServiceAdaptor>(w, custom);
            auto add_twenty_replies = wire<AddTwentyServiceAdaptorImplNode>(w, add_twenty_requests)
                .as<TSD<Int, TS<Int>>>();
            auto add_thirty_replies = wire<AddThirtyServiceAdaptorImplNode>(w, add_thirty_requests)
                .as<TSD<Int, TS<Int>>>();
            service_adaptor::to_graph<AddTwentyServiceAdaptor>(w, custom, add_twenty_replies);
            service_adaptor::to_graph<AddThirtyServiceAdaptor>(w, custom, add_thirty_replies);
        }
    };

    struct GenericServiceAdaptorImpl
    {
        [[maybe_unused]] static constexpr auto name = "generic_service_adaptor_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<ScalarVar<"T">>>> requests)
        {
            return wire<AddTwentyServiceAdaptorImplNode>(w, requests.as<TSD<Int, TS<Int>>>())
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MissingServiceAdaptorOutputImpl
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_adaptor_output_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service_adaptor::path(path.value());
            (void)service_adaptor::from_graph<AddTwentyServiceAdaptor>(w, custom);
        }
    };

    struct MissingMultiServiceOutputImpl
    {
        [[maybe_unused]] static constexpr auto name = "missing_multi_service_output_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            (void)service::impl_input<AddOneService>(w, custom);
        }
    };

    struct GenericAddOneImpl
    {
        [[maybe_unused]] static constexpr auto name = "generic_add_one_impl";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w, Port<TSD<Int, TS<ScalarVar<"NUMBER", Int, Float>>>> requests)
        {
            return wire<AddOneImplNode>(w, requests.as<TSD<Int, TS<Int>>>()).as<TSD<Int, TS<Int>>>();
        }
    };

    struct AddHalfImplNode
    {
        static constexpr auto name = "add_half_impl_node";

        static void eval(In<"requests", TSD<Int, TS<Float>>, InputValidity::Unchecked> requests,
                         Out<TSD<Int, TS<Float>>> out)
        {
            if (!requests.modified()) { return; }

            auto mutation = out.begin_mutation(out.evaluation_time());
            for (const auto &[request_id, request] : requests.removed_items())
            {
                (void)request;
                static_cast<void>(mutation.erase(request_id));
            }
            for (const auto &[request_id, request] : requests.modified_items())
            {
                if (!request.valid())
                {
                    static_cast<void>(mutation.erase(request_id));
                    continue;
                }
                Value response{request.value() + Float{0.5}};
                mutation.set(request_id, response.view());
            }
        }
    };

    struct GenericAddHalfImpl
    {
        [[maybe_unused]] static constexpr auto name = "generic_add_half_impl";

        static Port<TSD<Int, TS<Float>>> compose(
            Wiring &w, Port<TSD<Int, TS<ScalarVar<"NUMBER", Int, Float>>>> requests)
        {
            return wire<AddHalfImplNode>(w, requests.as<TSD<Int, TS<Float>>>())
                .as<TSD<Int, TS<Float>>>();
        }
    };

    template <typename T>
    struct TemplateAddService
    {
        static constexpr std::string_view name{"template_add"};
        using request_schema  = TS<T>;
        using response_schema = TS<T>;
    };

    struct PricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            return wire<PricesImplNode>(w, keys).as<TSD<Int, TS<Int>>>();
        }
    };

    struct ServiceDependentPricesImpl
    {
        [[maybe_unused]] static constexpr auto name =
            "service_dependent_prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            auto offset = wire<BaseValueService>(w, service::path("subscription_offset"));
            return wire<OffsetPricesImplNode>(w, keys, offset)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    inline std::vector<std::pair<std::vector<Int>, std::vector<Int>>> observed_subscription_keys;

    struct ObserveSubscriptionKeysNode
    {
        static constexpr auto name = "observe_subscription_keys_node";

        static void eval(In<"keys", TSS<Int>> keys)
        {
            std::vector<Int> added;
            std::vector<Int> removed;
            for (Int key : keys.added()) { added.push_back(key); }
            for (Int key : keys.removed()) { removed.push_back(key); }
            observed_subscription_keys.emplace_back(
                std::move(added), std::move(removed));
        }
    };

    struct ObservedPricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "observed_prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            static_cast<void>(wire<ObserveSubscriptionKeysNode>(w, keys));
            return wire<PricesImplNode>(w, keys).as<TSD<Int, TS<Int>>>();
        }
    };

    struct BundlePricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "bundle_prices_impl";

        static Port<TSD<BaseSubscriptionRequest, TS<Int>>> compose(
            Wiring &w, Port<TSS<BaseSubscriptionRequest>> keys)
        {
            return wire<stdlib::map_>(w, fn<BundlePriceForKeyGraph>(),
                                      arg<"__keys__">(keys))
                .as<TSD<BaseSubscriptionRequest, TS<Int>>>();
        }
    };

    struct ReferencePricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "reference_prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w)
        {
            return wire<ReferencePricesImplNode>(w).as<TSD<Int, TS<Int>>>();
        }
    };

    struct ReferencePricesAltImpl
    {
        [[maybe_unused]] static constexpr auto name = "reference_prices_alt_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w)
        {
            return wire<ReferencePricesAltImplNode>(w).as<TSD<Int, TS<Int>>>();
        }
    };

    struct ReferencePricePathInjectionGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_path_injection_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesPathImplNode>(
                w, service::path("premium"));
            auto prices = wire<ReferencePricesService>(w, service::path("premium"));
            return wire<stdlib::getitem_>(w, prices, Int{7}).as<TS<Int>>();
        }
    };

    struct ReferenceServiceDependencyGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_service_dependency_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<DerivedValueService, DerivedValueImpl>(w);
            service::register_reference_service<BaseValueService, BaseValueImpl>(w);
            return wire<DerivedValueService>(w);
        }
    };

    struct TypedReferencePricePathGraph
    {
        [[maybe_unused]] static constexpr auto name = "typed_reference_price_path_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<ReferencePricesService, TypedReferencePricesPathImplNode>(
                w, service::path("typed_prices", arg<"tier">(Str{"standard"})));
            service::register_reference_service<ReferencePricesService, TypedReferencePricesPathImplNode>(
                w, service::path("typed_prices", arg<"tier">(Str{"premium"})));
            service::register_reference_service<ReferencePricesService, TypedReferencePricesPathImplNode>(
                w, service::path("typed_prices", arg<"tier">(Str{"premium/special, value"})));
            auto prices = wire<ReferencePricesService>(
                w, service::path("typed_prices", arg<"tier">(Str{"premium/special, value"})));
            return wire<stdlib::getitem_>(w, prices, Int{7}).as<TS<Int>>();
        }
    };

    struct DuplicateReferenceServiceGraph
    {
        [[maybe_unused]] static constexpr auto name = "duplicate_reference_service_graph";

        static void compose(Wiring &w)
        {
            const auto custom = service::path("duplicate");
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(w, custom);
            service::register_reference_service<ReferencePricesService, ReferencePricesAltImpl>(w, custom);
        }
    };

    struct ReferencePriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(w);
            auto prices = wire<ReferencePricesService>(w);
            return wire<stdlib::getitem_>(w, prices, instrument).as<TS<Int>>();
        }
    };

    struct ReferencePricePathClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "reference_price_path_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_reference_service<ReferencePricesService, ReferencePricesImpl>(
                w, service::path("primary"));
            service::register_reference_service<ReferencePricesService, ReferencePricesAltImpl>(
                w, service::path("secondary"));
            auto prices = wire<ReferencePricesService>(w, service::path("secondary"));
            return wire<stdlib::getitem_>(w, prices, instrument).as<TS<Int>>();
        }
    };

    struct PriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            return wire<PricesService>(w, instrument);
        }
    };

    struct ServiceDependentPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "service_dependent_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_reference_service<BaseValueService, BaseValueImpl>(
                w, service::path("subscription_offset"));
            service::register_subscription_service<
                PricesService, ServiceDependentPricesImpl>(
                w, service::path("dependent_prices"));
            return wire<PricesService>(
                w, service::path("dependent_prices"), instrument);
        }
    };

    struct SubscriptionContextBranch
    {
        [[maybe_unused]] static constexpr auto name =
            "subscription_context_branch";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>)
        {
            return context::get<TS<Int>>(w, "subscription_price");
        }
    };

    struct SubscriptionContextSwitchGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "subscription_context_switch_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<Int>> instrument, Port<TS<Str>> selector)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            auto price = wire<PricesService>(w, instrument);
            context::scope<"subscription_price"> context_scope{w, price};
            return wire<stdlib::switch_>(
                       w, selector,
                       stdlib::switch_cases({
                           {Value{Str{"left"}}, fn<SubscriptionContextBranch>()},
                           {Value{Str{"right"}}, fn<SubscriptionContextBranch>()},
                       }),
                       instrument)
                .as<TS<Int>>();
        }
    };

    struct ObservedPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "observed_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, ObservedPricesImpl>(w);
            return wire<PricesService>(w, instrument);
        }
    };

    struct BundlePriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "bundle_price_client_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<BaseSubscriptionRequest>> request)
        {
            service::register_subscription_service<BundlePricesService, BundlePricesImpl>(w);
            return wire<BundlePricesService>(w, request);
        }
    };

    struct PriceForKeyGraph
    {
        [[maybe_unused]] static constexpr auto name = "price_for_key_graph";

        static Port<TS<Int>> compose(Wiring &, NamedPort<"key", TS<Int>> key)
        {
            using namespace hgraph::stdlib::syntax;
            return (key * Int{10}).as<TS<Int>>();
        }
    };

    struct StructuredPriceForKeyGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "structured_price_for_key_graph";

        static Port<StructuredPrice> compose(
            Wiring &w, NamedPort<"key", TS<Int>> key)
        {
            using namespace hgraph::stdlib::syntax;
            auto offset = (key * Int{10}).as<TS<Int>>();
            return wire<stdlib::pass_through_node>(
                       w, stdlib::to_tsb<StructuredPrice>(w, key, offset))
                .as<StructuredPrice>();
        }
    };

    struct StructuredPriceReferenceForKeyGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "structured_price_reference_for_key_graph";

        struct AsReference
        {
            [[maybe_unused]] static constexpr auto name =
                "structured_price_as_reference";

            static Port<REF<StructuredPrice>> compose(
                Wiring &, Port<REF<StructuredPrice>> price)
            {
                return price;
            }
        };

        static Port<StructuredPrice> compose(
            Wiring &w, NamedPort<"key", TS<Int>> key)
        {
            using namespace hgraph::stdlib::syntax;
            auto offset = wire<PricesService>(w, service::path("base_prices"), key);
            auto routed = wire<stdlib::if_, IfIntRefBundle>(
                              w, key == key, offset)
                              .as<IfIntRefBundle>();
            auto offset_ref = wire<stdlib::getitem_>(w, routed, Str{"true"});
            auto price = stdlib::to_tsb<StructuredPrice>(w, key, offset_ref);
            return wire<AsReference>(w, price).as<StructuredPrice>();
        }
    };

    struct StructuredPricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "structured_prices_impl";

        static Port<TSD<Int, StructuredPrice>> compose(
            Wiring &w, Port<TSS<Int>> keys)
        {
            return wire<stdlib::map_>(w, fn<StructuredPriceForKeyGraph>(),
                                      arg<"__keys__">(keys))
                .as<TSD<Int, StructuredPrice>>();
        }
    };

    struct StructuredReferencePricesImpl
    {
        [[maybe_unused]] static constexpr auto name =
            "structured_reference_prices_impl";

        static Port<void> compose(
            Wiring &w, Port<TSS<Int>> keys)
        {
            return wire<stdlib::map_>(w, fn<StructuredPriceReferenceForKeyGraph>(),
                                      arg<"__keys__">(keys));
        }
    };

    struct MappedPricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "mapped_prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            return wire<stdlib::map_>(w, fn<PriceForKeyGraph>(),
                                      arg<"__keys__">(keys))
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct DerivedPriceForKeyGraph
    {
        [[maybe_unused]] static constexpr auto name = "derived_price_for_key_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key)
        {
            using namespace hgraph::stdlib::syntax;
            auto price = wire<StructuredPricesService>(w, service::path("base"), key);
            auto offset = wire<stdlib::getitem_>(w, price, Str{"offset"}).as<TS<Int>>();
            return (offset + Int{1}).as<TS<Int>>();
        }
    };

    struct DerivedPricesImpl
    {
        [[maybe_unused]] static constexpr auto name = "derived_prices_impl";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSS<Int>> keys)
        {
            return wire<stdlib::map_>(w, fn<DerivedPriceForKeyGraph>(),
                                      arg<"__keys__">(keys))
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct NestedSubscriptionClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "nested_subscription_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, MappedPricesImpl>(
                w, service::path("base_prices"));
            service::register_subscription_service<StructuredPricesService, StructuredReferencePricesImpl>(
                w, service::path("base"));
            service::register_subscription_service<DerivedPricesService, DerivedPricesImpl>(w);
            return wire<DerivedPricesService>(w, instrument);
        }
    };

    struct MappedPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "mapped_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, MappedPricesImpl>(w);
            return wire<PricesService>(w, instrument);
        }
    };

    struct PriceServiceForKeyGraph
    {
        [[maybe_unused]] static constexpr auto name = "price_service_for_key_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"key", TS<Int>> key)
        {
            return wire<PricesService>(w, key);
        }
    };

    struct MappedPriceReductionClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "mapped_price_reduction_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TSS<Int>> instruments)
        {
            service::register_subscription_service<PricesService, MappedPricesImpl>(w);
            auto prices = wire<stdlib::map_>(
                w, fn<PriceServiceForKeyGraph>(), arg<"__keys__">(instruments));
            return wire<stdlib::reduce_>(w, fn<stdlib::add_>(), prices, Int{0})
                .as<TS<Int>>();
        }
    };

    struct LateDuplicatePriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "late_duplicate_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> early, Port<TS<Int>> late)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            static_cast<void>(wire<stdlib::null_sink>(w, wire<PricesService>(w, early)));
            return wire<DeltaValuePassThroughNode>(w, wire<PricesService>(w, late));
        }
    };

    struct PathPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "path_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesPathImplNode>(w, service::path("premium"));
            return wire<PricesService>(w, service::path("premium"), instrument);
        }
    };

    struct RegisteredPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "registered_price_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> instrument)
        {
            service::register_subscription_service<PricesService, PricesImpl>(w);
            return wire<PricesService>(w, instrument);
        }
    };

    struct AddStructuredBroadcastPriceGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "add_structured_broadcast_price_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value,
                                     Port<StructuredPrice> price)
        {
            using namespace hgraph::stdlib::syntax;
            auto offset = wire<stdlib::getitem_>(w, price, Str{"offset"})
                              .as<TS<Int>>();
            return (value + offset).as<TS<Int>>();
        }
    };

    struct StructuredSubscriptionBroadcastMapClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "structured_subscription_broadcast_map_client_graph";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w, Port<TS<Int>> instrument, Port<TSS<Int>> keys,
            Port<TSD<Int, TS<Int>>> values)
        {
            service::register_subscription_service<StructuredPricesService,
                                                   StructuredPricesImpl>(w);
            auto price = wire<StructuredPricesService>(w, instrument);
            return wire<stdlib::map_>(w, fn<AddStructuredBroadcastPriceGraph>(),
                                      values, price, arg<"__keys__">(keys))
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct StructuredPriceClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "structured_price_client_graph";

        static Port<StructuredPrice> compose(Wiring &w,
                                              Port<TS<Int>> instrument)
        {
            service::register_subscription_service<StructuredPricesService,
                                                   StructuredPricesImpl>(w);
            return wire<StructuredPricesService>(w, instrument);
        }
    };

    struct AddOneClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(w);
            return wire<AddOneService>(w, request);
        }
    };

    struct DecoupledRequestReplyImplGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "decoupled_request_reply_impl_graph";

        static void compose(
            Wiring &w,
            Port<TSD<Int, TS<Int>>> responses,
            Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            auto requests = service::from_graph<AddOneService>(w, custom);
            static_cast<void>(wire<ObserveReplylessRequestsNode>(w, requests));
            service::to_graph<AddOneService>(w, custom, responses);
        }
    };

    struct DecoupledSubscriptionImplGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "decoupled_subscription_impl_graph";

        static void compose(
            Wiring &w,
            Port<TSD<Int, TS<Int>>> responses,
            Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            auto keys = service::from_graph<PricesService>(w, custom);
            static_cast<void>(wire<ObserveSubscriptionKeysNode>(w, keys));
            service::to_graph<PricesService>(w, custom, responses);
        }
    };

    struct DecoupledSubscriptionClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "decoupled_subscription_client_graph";

        static Port<TS<Int>> compose(
            Wiring &w,
            Port<TS<Int>> key,
            Port<TSD<Int, TS<Int>>> responses)
        {
            const auto custom = service::path("decoupled_prices");
            service::register_services<
                DecoupledSubscriptionImplGraph, PricesService>(
                w, custom, responses);
            return wire<PricesService>(w, custom, key);
        }
    };

    struct DecoupledRequestReplyClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "decoupled_request_reply_client_graph";

        static Port<TS<Int>> compose(
            Wiring &w,
            Port<TS<Int>> request,
            Port<TSD<Int, TS<Int>>> responses)
        {
            const auto custom = service::path("decoupled");
            service::register_services<
                DecoupledRequestReplyImplGraph, AddOneService>(
                    w, custom, responses);
            return wire<AddOneService>(w, custom, request);
        }
    };

    struct ReplylessPublishTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "replyless_publish_two_client_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            const auto custom = service::path("events");
            service::register_request_reply_service<
                ReplylessPublishService, ReplylessPublishImplGraph>(w, custom);
            static_assert(std::same_as<
                          decltype(wire<ReplylessPublishService>(w, custom, lhs)),
                          void>);
            wire<ReplylessPublishService>(w, custom, lhs);
            wire<ReplylessPublishService>(w, custom, rhs);
            return lhs;
        }
    };

    struct ReplylessPublishMappedFunction
    {
        [[maybe_unused]] static constexpr auto name =
            "replyless_publish_mapped_function";

        static void compose(Wiring &w, Port<TS<Int>> request)
        {
            wire<ReplylessPublishService>(
                w, service::path("mapped_events"), request);
        }
    };

    struct ReplylessPublishMappedClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "replyless_publish_mapped_client_graph";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            service::register_request_reply_service<
                ReplylessPublishService, ReplylessPublishImplGraph>(
                    w, service::path("mapped_events"));
            wire<stdlib::map_sink_>(
                w, fn<ReplylessPublishMappedFunction>(), requests);
            return requests;
        }
    };

    struct ReplylessPublishStubImplGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "replyless_publish_stub_impl_graph";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            auto requests = service::impl_input<ReplylessPublishService>(
                w, service::path(path.value()));
            static_cast<void>(wire<ObserveReplylessRequestsNode>(w, requests));
        }
    };

    struct ReplylessPublishStubClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "replyless_publish_stub_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service::path("replyless_stub");
            service::register_services<
                ReplylessPublishStubImplGraph, ReplylessPublishService>(w, custom);
            wire<ReplylessPublishService>(w, custom, request);
            return request;
        }
    };

    struct RecursiveReplylessPublishImplGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "recursive_replyless_publish_impl_graph";

        static void compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            auto total = wire<stdlib::reduce_>(
                             w, fn<stdlib::add_>(), requests, Int{0})
                             .as<TS<Int>>();
            wire<ReplylessPublishService>(
                w, service::path("replyless_cycle"), total);
        }
    };

    struct RecursiveReplylessPublishClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "recursive_replyless_publish_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service::path("replyless_cycle");
            service::register_request_reply_service<
                ReplylessPublishService, RecursiveReplylessPublishImplGraph>(
                    w, custom);
            wire<ReplylessPublishService>(w, custom, request);
            return request;
        }
    };

    struct AddOnePathClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_path_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<AddOneService, AddOnePathImplNode>(
                w, service::path("premium"));
            return wire<AddOneService>(w, service::path("premium"), request);
        }
    };

    struct AddOneTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_two_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs_request, Port<TS<Int>> rhs_request)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(w);
            auto lhs_reply = wire<AddOneService>(w, lhs_request);
            auto rhs_reply = wire<AddOneService>(w, rhs_request);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct AddOneStagedClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "add_one_staged_client_graph";

        static auto compose(
            Wiring &w,
            Port<TS<Int>> lhs_request,
            Port<TS<Int>> rhs_request)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(w);
            auto lhs_reply = wire<AddOneService>(w, lhs_request);
            auto rhs_reply = wire<AddOneService>(w, rhs_request);
            return stdlib::to_tsl<TSL<TS<Int>, 2>>(w, lhs_reply, rhs_reply);
        }
    };

    struct SumPairClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "sum_pair_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>> right)
        {
            service::register_request_reply_service<SumPairService, SumPairImplNode>(w);
            auto request = stdlib::to_tsb<PairRequest>(w, left, right);
            return wire<SumPairService>(w, request);
        }
    };

    struct MissingServiceImplementationGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_implementation_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            return wire<AddOneService>(w, request);
        }
    };

    struct AddOneMappedFunction
    {
        [[maybe_unused]] static constexpr auto name = "add_one_mapped_function";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            using namespace hgraph::stdlib::syntax;
            return (wire<AddOneService>(w, service::path("mapped"), request) + Int{1}).as<TS<Int>>();
        }
    };

    struct MappedServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "mapped_service_client_graph";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(
                w, service::path("mapped"));
            return wire<stdlib::map_>(w, fn<AddOneMappedFunction>(), requests)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MappedServiceSwitchAlpha
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_service_switch_alpha";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            return wire<AddOneService>(
                w, service::path("mapped_switch_request_reply"), value);
        }
    };

    struct MappedServiceSwitchBeta
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_service_switch_beta";

        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value)
        {
            using namespace hgraph::stdlib::syntax;
            return (value * Int{2} - Int{2}).as<TS<Int>>();
        }
    };

    struct MappedServiceSwitchAddTwo
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_service_switch_add_two";

        static Port<TS<Int>> compose(Wiring &, Port<TS<Int>> value)
        {
            using namespace hgraph::stdlib::syntax;
            return (value + Int{2}).as<TS<Int>>();
        }
    };

    struct MappedRequestReplySwitchFunction
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_request_reply_switch_function";

        static Port<TS<Int>> compose(
            Wiring &w, NamedPort<"key", TS<Int>>,
            Port<TS<Int>> value, Port<TS<Str>> selector)
        {
            return wire<stdlib::switch_>(
                       w, selector,
                       stdlib::switch_cases(
                           {{Value{Str{"alpha"}}, fn<MappedServiceSwitchAlpha>()},
                            {Value{Str{"beta"}}, fn<MappedServiceSwitchBeta>()}}),
                       value)
                .as<TS<Int>>();
        }
    };

    struct MappedRequestReplySwitchGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_request_reply_switch_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> values,
            Port<TS<Str>> selector)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(
                w, service::path("mapped_switch_request_reply"));
            auto mapped = wire<stdlib::map_>(
                              w, fn<MappedRequestReplySwitchFunction>(),
                              values, selector)
                              .as<TSD<Int, TS<Int>>>();
            return wire<stdlib::reduce_>(
                       w, fn<stdlib::add_>(), mapped, Int{0})
                .as<TS<Int>>();
        }
    };

    struct MappedRequestReplySwitchMapGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_request_reply_switch_map_graph";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> values,
            Port<TS<Str>> selector)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(
                w, service::path("mapped_switch_request_reply"));
            return wire<stdlib::map_>(
                       w, fn<MappedRequestReplySwitchFunction>(),
                       values, selector)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MeshedRequestReplySwitchGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "meshed_request_reply_switch_graph";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> values,
            Port<TS<Str>> selector)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(
                w, service::path("mapped_switch_request_reply"));
            return wire<stdlib::mesh_>(
                       w, fn<MappedRequestReplySwitchFunction>(),
                       values, selector)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MappedSubscriptionSwitchFunction
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_subscription_switch_function";

        static Port<TS<Int>> compose(
            Wiring &w, NamedPort<"key", TS<Int>> key,
            Port<TS<Int>> value, Port<TS<Str>> selector)
        {
            using namespace hgraph::stdlib::syntax;
            auto quoted =
                (wire<PricesService>(
                     w, service::path("mapped_switch_subscription"), key) +
                 value)
                    .as<TS<Int>>();
            return wire<stdlib::switch_>(
                       w, selector,
                       stdlib::switch_cases(
                           {{Value{Str{"alpha"}}, fn<MappedServiceSwitchAddTwo>()},
                            {Value{Str{"beta"}}, fn<MappedServiceSwitchBeta>()}}),
                       quoted)
                .as<TS<Int>>();
        }
    };

    struct MappedSubscriptionSwitchGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "mapped_subscription_switch_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TSD<Int, TS<Int>>> values,
            Port<TS<Str>> selector)
        {
            service::register_subscription_service<PricesService, MappedPricesImpl>(
                w, service::path("mapped_switch_subscription"));
            auto mapped = wire<stdlib::map_>(
                              w, fn<MappedSubscriptionSwitchFunction>(),
                              values, selector)
                              .as<TSD<Int, TS<Int>>>();
            return wire<stdlib::reduce_>(
                       w, fn<stdlib::add_>(), mapped, Int{0})
                .as<TS<Int>>();
        }
    };

    struct RecursiveAddOneMappedFunction
    {
        [[maybe_unused]] static constexpr auto name = "recursive_add_one_mapped_function";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            using namespace hgraph::stdlib::syntax;

            auto routed = wire<stdlib::if_, IfIntRefBundle>(w, request == Int{0}, request)
                              .as<IfIntRefBundle>();
            auto zero = wire<stdlib::getitem_>(w, routed, Str{"true"}).as<TS<Int>>();
            auto non_zero = wire<stdlib::getitem_>(w, routed, Str{"false"}).as<TS<Int>>();
            auto one = wire<stdlib::const_, TS<Int>>(w, Int{1});
            auto base = wire<stdlib::sample>(w, zero, one).as<TS<Int>>();
            auto recurse =
                (wire<AddOneService>(w, service::path("recursive"), non_zero - Int{1}) + Int{1})
                    .as<TS<Int>>();
            return wire<stdlib::merge>(w, base, recurse).as<TS<Int>>();
        }
    };

    struct RecursiveAddOneImplGraph
    {
        [[maybe_unused]] static constexpr auto name = "recursive_add_one_impl_graph";

        static Port<TSD<Int, TS<Int>>> compose(
            Wiring &w,
            Port<TSD<Int, TS<Int>>> requests)
        {
            return wire<stdlib::map_>(w, fn<RecursiveAddOneMappedFunction>(), requests)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct RecursiveAddOneClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "recursive_add_one_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<AddOneService, RecursiveAddOneImplGraph>(
                w, service::path("recursive"));
            return wire<AddOneService>(w, service::path("recursive"), request);
        }
    };

    struct MeshedServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "meshed_service_client_graph";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            service::register_request_reply_service<AddOneService, AddOneImplNode>(
                w, service::path("mapped"));
            return wire<stdlib::mesh_>(w, fn<AddOneMappedFunction>(), requests)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MappedSubscriptionFunction
    {
        [[maybe_unused]] static constexpr auto name = "mapped_subscription_function";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> key)
        {
            return wire<PricesService>(w, service::path("mapped_prices"), key);
        }
    };

    struct MappedSubscriptionClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "mapped_subscription_client_graph";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> keys)
        {
            service::register_subscription_service<PricesService, PricesImpl>(
                w, service::path("mapped_prices"));
            return wire<stdlib::map_>(w, fn<MappedSubscriptionFunction>(), keys)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct MissingMappedServiceImplementationGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_mapped_service_implementation_graph";

        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> requests)
        {
            return wire<stdlib::map_>(w, fn<AddOneMappedFunction>(), requests)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct IllegalServiceStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "illegal_service_stub_graph";

        static void compose(Wiring &w)
        {
            (void)service::impl_input<AddOneService>(w);
        }
    };

    struct MissingMultiServiceStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_multi_service_stub_graph";

        static void compose(Wiring &w)
        {
            const auto custom = service::path("missing");
            service::register_services<MissingMultiServiceOutputImpl, AddOneService>(w, custom);
            // register_services is LAZY (RFC 0011 step 4), so an implementation
            // nothing asks for is never materialized and never validated. A
            // client makes the candidate wanted, at which point the
            // required-endpoint scope fires as before.
            static_cast<void>(wire<AddOneService>(
                w, custom, wire<stdlib::const_>(w, Int{1}).as<TS<Int>>()));
        }
    };

    // RFC 0011 step 5: a GENERIC reference service whose implementation
    // returns the wrong resolved type. The concrete path has always rejected
    // this via Port::as<>; the non-concrete branch of wire_service_impl did
    // not compare against the resolved meta at all.
    struct GenericRateService
    {
        static constexpr std::string_view name{"generic_rate"};
        using output_schema = TS<ScalarVar<"NUMBER", Int, Float>>;
    };

    struct MismatchedGenericRateImpl
    {
        [[maybe_unused]] static constexpr auto name = "mismatched_generic_rate_impl";

        // Declared NUMBER=Int by the registration path, but produces a Float.
        static Port<TS<Float>> compose(Wiring &w)
        {
            return wire<stdlib::const_>(w, Float{1.5}).as<TS<Float>>();
        }
    };

    struct MismatchedGenericRateGraph
    {
        [[maybe_unused]] static constexpr auto name = "mismatched_generic_rate_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            service::register_reference_service<GenericRateService, MismatchedGenericRateImpl>(
                w, service::path("generic_rate", arg<"NUMBER">(scalar_type<Int>())));
            return wire<GenericRateService>(
                w, service::path("generic_rate", arg<"NUMBER">(scalar_type<Int>()))).as<TS<Int>>();
        }
    };

    // RFC 0011 step 9: a SOURCE-ONLY adaptor (output, no input) in a
    // translation unit that includes BOTH service_wiring.h and
    // adaptor_wiring.h. Before the concepts were made disjoint this did not
    // compile - wire<Interface> matched both wire_customization
    // specializations. It now lowers onto the reference-service machinery.
    struct SourceOnlyFeedAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"source_only_feed"};
        using output_schema = TS<Int>;
    };

    struct SourceOnlyFeedImpl
    {
        [[maybe_unused]] static constexpr auto name = "source_only_feed_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            adaptor::to_graph<SourceOnlyFeedAdaptor>(
                w, service::path(path.value()), wire<stdlib::const_>(w, Int{9}).as<TS<Int>>());
        }
    };

    struct SourceOnlyFeedGraph
    {
        [[maybe_unused]] static constexpr auto name = "source_only_feed_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            const auto custom = service::path("feed");
            adaptor::register_adaptor<SourceOnlyFeedAdaptor, SourceOnlyFeedImpl>(w, custom);
            return wire<SourceOnlyFeedAdaptor>(w, custom);
        }
    };

    // RFC 0011 step 7: ONE implementation spanning an adaptor and a service.
    // This is the shape services.rst documents as "a sink-only interface in, a
    // source-only interface out" - now expressible as a single atomic
    // registration rather than two.
    struct CollectAdaptor : adaptor::interface
    {
        static constexpr std::string_view name{"collect_in"};
        using input_schema = TS<Int>;
    };

    struct PublishService
    {
        static constexpr std::string_view name{"publish_out"};
        using output_schema = TS<Int>;
    };

    struct MixedFlavourImpl
    {
        [[maybe_unused]] static constexpr auto name = "mixed_flavour_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            auto collected = adaptor::from_graph<CollectAdaptor>(w, custom);
            service::to_graph<PublishService>(
                w, custom, wire<AddOneValueNode>(w, collected).as<TS<Int>>());
        }
    };

    struct MixedFlavourGraph
    {
        [[maybe_unused]] static constexpr auto name = "mixed_flavour_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            const auto custom = service::path("mixed");
            service::register_services<MixedFlavourImpl, CollectAdaptor, PublishService>(w, custom);
            adaptor::adaptor<CollectAdaptor>(w, custom, value);
            return wire<PublishService>(w, custom);
        }
    };

    inline int single_interface_stub_compositions = 0;

    // RFC 0011 step 4: register_services with ONE interface is the
    // single-interface BY-STUB registration - the quadrant services lacked
    // against register_adaptor / register_automatic_adaptor.
    struct SingleInterfaceStubImpl
    {
        [[maybe_unused]] static constexpr auto name = "single_interface_stub_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            ++single_interface_stub_compositions;
            const auto custom = service::path(path.value());
            auto requests = service::from_graph<AddOneService>(w, custom);
            service::to_graph<AddOneService>(
                w, custom, wire<AddOneImplNode>(w, requests).as<TSD<Int, TS<Int>>>());
        }
    };

    struct SingleInterfaceStubClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "single_interface_stub_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service::path("single_stub");
            service::register_services<SingleInterfaceStubImpl, AddOneService>(w, custom);
            return wire<AddOneService>(w, custom, request);
        }
    };

    struct UnrequestedMultiServiceGraph
    {
        [[maybe_unused]] static constexpr auto name = "unrequested_multi_service_graph";

        static void compose(Wiring &w)
        {
            service::register_services<SingleInterfaceStubImpl, AddOneService>(
                w, service::path("unrequested"));
        }
    };

    struct MultiRequestReplyImpl
    {
        [[maybe_unused]] static constexpr auto name = "multi_request_reply_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            auto add_one_requests = service::impl_input<AddOneService>(w, custom);
            auto add_ten_requests = service::impl_input<AddTenService>(w, custom);
            auto add_one_replies = wire<AddOneImplNode>(w, add_one_requests).as<TSD<Int, TS<Int>>>();
            auto add_ten_replies = wire<AddTenImplNode>(w, add_ten_requests).as<TSD<Int, TS<Int>>>();
            service::impl_output<AddOneService>(w, custom, add_one_replies);
            service::impl_output<AddTenService>(w, custom, add_ten_replies);
        }
    };

    // RFC 0011 step 3: the adaptor spelling of impl_input/impl_output. Same
    // wiring, so the same graph must produce the same result.
    struct MultiRequestReplyFromToGraphImpl
    {
        [[maybe_unused]] static constexpr auto name = "multi_request_reply_from_to_graph_impl";

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            const auto custom = service::path(path.value());
            auto add_one_requests = service::from_graph<AddOneService>(w, custom);
            auto add_ten_requests = service::from_graph<AddTenService>(w, custom);
            auto add_one_replies = wire<AddOneImplNode>(w, add_one_requests).as<TSD<Int, TS<Int>>>();
            auto add_ten_replies = wire<AddTenImplNode>(w, add_ten_requests).as<TSD<Int, TS<Int>>>();
            service::to_graph<AddOneService>(w, custom, add_one_replies);
            service::to_graph<AddTenService>(w, custom, add_ten_replies);
        }
    };

    struct MultiServiceFromToGraphClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_from_to_graph_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service::path("multi_from_to");
            service::register_services<
                MultiRequestReplyFromToGraphImpl, AddOneService, AddTenService>(w, custom);
            auto add_one = wire<AddOneService>(w, custom, request);
            auto add_ten = wire<AddTenService>(w, custom, request);
            return wire<stdlib::add_>(w, add_one, add_ten).as<TS<Int>>();
        }
    };

    struct MultiServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service::path("multi");
            service::register_services<MultiRequestReplyImpl, AddOneService, AddTenService>(w, custom);
            auto add_one = wire<AddOneService>(w, custom, request);
            auto add_ten = wire<AddTenService>(w, custom, request);
            return wire<stdlib::add_>(w, add_one, add_ten).as<TS<Int>>();
        }
    };

    template <typename... Interfaces>
    struct MixedServicePermutationImpl
    {
        [[maybe_unused]] static constexpr auto name =
            "mixed_service_permutation_impl";
        inline static int compositions = 0;

        template <typename Interface>
        static void wire_interface(Wiring &w, const service::ServicePath &custom)
        {
            if constexpr (std::same_as<Interface, BaseValueService>)
            {
                service::impl_output<BaseValueService>(w, custom, wire<TenSourceNode>(w));
            }
            else if constexpr (std::same_as<Interface, DerivedValueService>)
            {
                service::impl_output<DerivedValueService>(
                    w, custom,
                    wire<stdlib::const_>(w, Int{11}).as<TS<Int>>());
            }
            else if constexpr (std::same_as<Interface, PricesService>)
            {
                auto keys = service::impl_input<PricesService>(w, custom);
                service::impl_output<PricesService>(
                    w, custom,
                    wire<PricesImplNode>(w, keys).as<TSD<Int, TS<Int>>>());
            }
            else if constexpr (std::same_as<Interface, DerivedPricesService>)
            {
                auto keys = service::impl_input<DerivedPricesService>(w, custom);
                auto offset = wire<stdlib::const_>(w, Int{1}).as<TS<Int>>();
                service::impl_output<DerivedPricesService>(
                    w, custom,
                    wire<OffsetPricesImplNode>(w, keys, offset)
                        .as<TSD<Int, TS<Int>>>());
            }
            else if constexpr (std::same_as<Interface, AddOneService>)
            {
                auto requests = service::impl_input<AddOneService>(w, custom);
                service::impl_output<AddOneService>(
                    w, custom,
                    wire<AddOneImplNode>(w, requests).as<TSD<Int, TS<Int>>>());
            }
            else
            {
                static_assert(std::same_as<Interface, AddTenService>);
                auto requests = service::impl_input<AddTenService>(w, custom);
                service::impl_output<AddTenService>(
                    w, custom,
                    wire<AddTenImplNode>(w, requests).as<TSD<Int, TS<Int>>>());
            }
        }

        static void compose(Wiring &w, Scalar<"path", Str> path)
        {
            ++compositions;
            const auto custom = service::path(path.value());
            // Follow the declared interface order so the matrix also proves
            // that stub discovery and keyed transport finalization are not
            // order-dependent.
            (wire_interface<Interfaces>(w, custom), ...);
        }
    };

    template <typename... Interfaces>
    struct MixedServicePermutationClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "mixed_service_permutation_client_graph";

        template <typename Interface>
        static Port<TS<Int>> client(
            Wiring &w, const service::ServicePath &custom,
            Port<TS<Int>> key, Port<TS<Int>> request)
        {
            return [&]() {
                if constexpr (std::same_as<Interface, BaseValueService>)
                {
                    return wire<BaseValueService>(w, custom);
                }
                else if constexpr (std::same_as<Interface, DerivedValueService>)
                {
                    return wire<DerivedValueService>(w, custom);
                }
                else if constexpr (std::same_as<Interface, PricesService>)
                {
                    return wire<PricesService>(w, custom, key);
                }
                else if constexpr (std::same_as<Interface, DerivedPricesService>)
                {
                    return wire<DerivedPricesService>(w, custom, key);
                }
                else if constexpr (std::same_as<Interface, AddOneService>)
                {
                    return wire<AddOneService>(w, custom, request);
                }
                else
                {
                    static_assert(std::same_as<Interface, AddTenService>);
                    return wire<AddTenService>(w, custom, request);
                }
            }();
        }

        static Port<TSL<TS<Int>, sizeof...(Interfaces)>> compose(
            Wiring &w, Port<TS<Int>> key, Port<TS<Int>> request)
        {
            const auto custom = service::path("mixed_permutation");
            service::register_services<
                MixedServicePermutationImpl<Interfaces...>, Interfaces...>(
                    w, custom);

            // Tuple list-initialization evaluates left-to-right, preserving the
            // declared client order before the structural list is assembled.
            auto clients = std::tuple{
                client<Interfaces>(w, custom, key, request)...};
            return std::apply(
                [&](const auto &...ports) {
                    return stdlib::to_tsl<
                        TSL<TS<Int>, sizeof...(Interfaces)>>(w, ports...);
                },
                clients);
        }
    };

    template <typename Interface>
    constexpr Int mixed_service_expected_value()
    {
        if constexpr (std::same_as<Interface, BaseValueService>) { return 10; }
        else if constexpr (std::same_as<Interface, DerivedValueService>) { return 11; }
        else if constexpr (std::same_as<Interface, PricesService>) { return 20; }
        else if constexpr (std::same_as<Interface, DerivedPricesService>) { return 21; }
        else if constexpr (std::same_as<Interface, AddOneService>) { return 8; }
        else
        {
            static_assert(std::same_as<Interface, AddTenService>);
            return 17;
        }
    }

    template <typename Interface>
    constexpr bool mixed_service_is_reference =
        std::same_as<Interface, BaseValueService>
        || std::same_as<Interface, DerivedValueService>;

    template <typename Interface>
    std::optional<Int> mixed_service_reference_value()
    {
        if constexpr (mixed_service_is_reference<Interface>)
        {
            return mixed_service_expected_value<Interface>();
        }
        else { return std::nullopt; }
    }

    template <typename Interface>
    std::optional<Int> mixed_service_keyed_value()
    {
        if constexpr (mixed_service_is_reference<Interface>) { return std::nullopt; }
        else { return mixed_service_expected_value<Interface>(); }
    }

    template <typename... Interfaces>
    void check_mixed_service_permutation()
    {
        using Implementation = MixedServicePermutationImpl<Interfaces...>;
        Implementation::compositions = 0;
        const auto actual =
            eval_node<MixedServicePermutationClientGraph<Interfaces...>>(
                values<Int>(2), values<Int>(7));
        constexpr bool has_keyed_interface =
            ((std::same_as<Interfaces, PricesService>
              || std::same_as<Interfaces, DerivedPricesService>
              || std::same_as<Interfaces, AddOneService>
              || std::same_as<Interfaces, AddTenService>) || ...);
        constexpr bool has_reference_interface =
            (mixed_service_is_reference<Interfaces> || ...);
        auto reference_delta = list_delta<TS<Int>>(
            std::vector<std::optional<Int>>{
                mixed_service_reference_value<Interfaces>()...});
        auto keyed_delta = list_delta<TS<Int>>(
            std::vector<std::optional<Int>>{
                mixed_service_keyed_value<Interfaces>()...});
        if constexpr (has_reference_interface && has_keyed_interface)
        {
            CHECK_OUTPUT(actual, values<Value>(reference_delta, keyed_delta));
        }
        else if constexpr (has_keyed_interface)
        {
            CHECK_OUTPUT(actual, values<Value>(none, keyed_delta));
        }
        else
        {
            CHECK_OUTPUT(actual, values<Value>(reference_delta));
        }
        CHECK(Implementation::compositions == 1);
    }

    struct ServiceAdaptorTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_two_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs_request, Port<TS<Int>> rhs_request)
        {
            const auto custom = service_adaptor::path("multi_client");
            service_adaptor::register_service_adaptor<AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImpl>(
                w, custom);
            auto lhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, lhs_request);
            auto rhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, rhs_request);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct ServiceAdaptorParameterizedClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "service_adaptor_parameterized_client_graph";

        static Port<TS<Int>> compose(
            Wiring &w,
            Port<TS<Int>> lhs_request,
            Port<TS<Int>> rhs_request,
            Port<TS<Int>> direct_request)
        {
            const auto transformed = service_adaptor::path(
                "parameterized", arg<"passthrough">(Bool{false}));
            const auto passthrough = service_adaptor::path(
                "parameterized", arg<"passthrough">(Bool{true}));
            service_adaptor::register_service_adaptor_impl<
                AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImplNode>(
                    w, transformed);
            service_adaptor::register_service_adaptor_impl<
                AddTwentyServiceAdaptor, AddThirtyServiceAdaptorImplNode>(
                    w, passthrough);

            auto lhs_reply = wire<AddTwentyServiceAdaptor>(
                w, transformed, lhs_request);
            auto rhs_reply = wire<AddTwentyServiceAdaptor>(
                w, transformed, rhs_request);
            auto direct_reply = wire<AddTwentyServiceAdaptor>(
                w, passthrough, direct_request);
            return wire<stdlib::add_>(
                w,
                wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>(),
                direct_reply).as<TS<Int>>();
        }
    };

    struct ServiceAdaptorSingleClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_single_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service_adaptor::path("single_client");
            service_adaptor::register_service_adaptor<AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImpl>(
                w, custom);
            return wire<AddTwentyServiceAdaptor>(w, custom, request);
        }
    };

    struct ServiceAdaptorSwitchBranch
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_switch_branch";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            return wire<AddTwentyServiceAdaptor>(
                w, service_adaptor::path("switch_adaptor"), request);
        }
    };

    struct ServiceAdaptorSwitchGraph
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_switch_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<Str>> selector, Port<TS<Int>> request)
        {
            service_adaptor::register_service_adaptor<
                AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImpl>(
                    w, service_adaptor::path("switch_adaptor"));
            return wire<stdlib::switch_>(
                       w, selector,
                       stdlib::switch_cases({
                           {Value{Str{"adaptor"}}, fn<ServiceAdaptorSwitchBranch>()},
                       }),
                       request)
                .as<TS<Int>>();
        }
    };

    struct ServiceAdaptorImplTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "service_adaptor_impl_two_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> lhs_request, Port<TS<Int>> rhs_request)
        {
            const auto custom = service_adaptor::path("multi_client_impl");
            service_adaptor::register_service_adaptor_impl<AddTwentyServiceAdaptor, AddTwentyServiceAdaptorImplNode>(
                w, custom);
            auto lhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, lhs_request);
            auto rhs_reply = wire<AddTwentyServiceAdaptor>(w, custom, rhs_request);
            return wire<stdlib::add_>(w, lhs_reply, rhs_reply).as<TS<Int>>();
        }
    };

    struct SinkServiceAdaptorTwoClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "sink_service_adaptor_two_client_graph";

        static Port<TS<Int>> compose(
            Wiring &w, Port<TS<Int>> lhs, Port<TS<Int>> rhs,
            Port<TS<Int>> other_lhs, Port<TS<Int>> other_rhs)
        {
            const auto custom = service_adaptor::path("sink_multi_client");
            service_adaptor::register_service_adaptor_impl<
                PublishPairServiceAdaptor, PublishPairServiceAdaptorImplNode>(w, custom);
            wire<PublishPairServiceAdaptor>(w, custom, lhs, rhs);
            wire<PublishPairServiceAdaptor>(w, custom, other_lhs, other_rhs);
            return lhs;
        }
    };

    struct SinkServiceAdaptorConstantFieldGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "sink_service_adaptor_constant_field_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            const auto custom = service_adaptor::path("sink_constant_field");
            service_adaptor::register_service_adaptor_impl<
                PublishPairServiceAdaptor, PublishPairServiceAdaptorImplNode>(w, custom);
            auto constant = wire<stdlib::const_>(w, Int{7}).as<TS<Int>>();
            wire<PublishPairServiceAdaptor>(w, custom, constant, value);
            return value;
        }
    };

    struct MultiServiceAdaptorClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "multi_service_adaptor_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service_adaptor::path("multi_service_adaptor");
            service_adaptor::register_service_adaptors<
                MultiServiceAdaptorImpl, AddTwentyServiceAdaptor, AddThirtyServiceAdaptor>(w, custom);
            auto add_twenty = wire<AddTwentyServiceAdaptor>(w, custom, request);
            auto add_thirty = wire<AddThirtyServiceAdaptor>(w, custom, request);
            return wire<stdlib::add_>(w, add_twenty, add_thirty).as<TS<Int>>();
        }
    };

    struct MultiServiceAdaptorQualifiedClientGraph
    {
        [[maybe_unused]] static constexpr auto name =
            "multi_service_adaptor_qualified_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto custom = service_adaptor::path(
                "qualified_multi", arg<"passthrough">(Bool{false}));
            service_adaptor::register_service_adaptors<
                MultiServiceAdaptorImpl,
                AddTwentyServiceAdaptor,
                AddThirtyServiceAdaptor>(w, custom);
            auto add_twenty = wire<AddTwentyServiceAdaptor>(w, custom, request);
            auto add_thirty = wire<AddThirtyServiceAdaptor>(w, custom, request);
            return wire<stdlib::add_>(w, add_twenty, add_thirty).as<TS<Int>>();
        }
    };

    struct GenericServiceAdaptorClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_service_adaptor_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service_adaptor::register_service_adaptor_impl<
                GenericServiceAdaptor, GenericServiceAdaptorImpl>(
                    w, service_adaptor::path("generic_service_adaptor", arg<"T">(scalar_type<Int>())));
            return wire<GenericServiceAdaptor>(w, service_adaptor::path("generic_service_adaptor"), request)
                .as<TS<Int>>();
        }
    };

    struct MissingServiceAdaptorImplementationGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_adaptor_implementation_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            return wire<AddTwentyServiceAdaptor>(w, service_adaptor::path("missing_service_adaptor"), request);
        }
    };

    struct IllegalServiceAdaptorStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "illegal_service_adaptor_stub_graph";

        static void compose(Wiring &w)
        {
            (void)service_adaptor::from_graph<AddTwentyServiceAdaptor>(w);
        }
    };

    struct MissingServiceAdaptorStubGraph
    {
        [[maybe_unused]] static constexpr auto name = "missing_service_adaptor_stub_graph";

        static void compose(Wiring &w)
        {
            service_adaptor::register_service_adaptor<
                AddTwentyServiceAdaptor, MissingServiceAdaptorOutputImpl>(
                    w, service_adaptor::path("missing_stub"));
        }
    };

    struct TemplateServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "template_service_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            const auto typed = service::path("template", arg<"T">(Str{"Int"}));
            service::register_request_reply_service<TemplateAddService<Int>, AddOneImplNode>(w, typed);
            return wire<TemplateAddService<Int>>(w, typed, request);
        }
    };

    struct GenericServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_service_client_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> request)
        {
            service::register_request_reply_service<GenericAddOneService, GenericAddOneImpl>(
                w, service::path("generic", arg<"NUMBER">(scalar_type<Int>())));
            return wire<GenericAddOneService>(w, service::path("generic"), request).as<TS<Int>>();
        }
    };

    struct GenericFloatServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_float_service_client_graph";

        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> request)
        {
            service::register_request_reply_service<GenericAddOneService, GenericAddHalfImpl>(
                w, service::path("generic", arg<"NUMBER">(scalar_type<Float>())));
            return wire<GenericAddOneService>(w, service::path("generic"), request).as<TS<Float>>();
        }
    };

    struct GenericStringServiceClientGraph
    {
        [[maybe_unused]] static constexpr auto name = "generic_string_service_client_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> request)
        {
            return wire<GenericAddOneService>(w, service::path("generic"), request).as<TS<Str>>();
        }
    };

}  // namespace

TEST_CASE("service wiring: reference service client reads implementation output by reference")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferencePriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(70, none, 80));
}

TEST_CASE("service wiring: reference service paths keep shared outputs separate")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferencePricePathClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(700, none, 800));
}

TEST_CASE("service wiring: reference implementation can receive the service path scalar")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferencePricePathInjectionGraph>(), values<Int>(777));
}

TEST_CASE("service wiring: service implementation can depend on a later registered service")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ReferenceServiceDependencyGraph>(), values<Int>(11));
}

TEST_CASE("service wiring: implementation candidates materialize only on demand")
{
    hgraph::stdlib::register_standard_operators();

    lazy_reference_compositions = 0;
    CHECK_NOTHROW(build_graph<UnusedReferenceServiceGraph>());
    CHECK(lazy_reference_compositions == 0);

    CHECK_OUTPUT(eval_node<RequestedReferenceServiceGraph>(), values<Int>(10));
    CHECK(lazy_reference_compositions == 1);
}

TEST_CASE("service wiring: scalar-qualified paths keep implementations separate")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TypedReferencePricePathGraph>(), values<Int>(777));
}

TEST_CASE("service wiring: duplicate implementation registrations are rejected")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_THROWS_AS(build_graph<DuplicateReferenceServiceGraph>(), std::invalid_argument);
}

TEST_CASE("service wiring: subscription client reads implementation output by reference")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 70, none, 80));
}

TEST_CASE("service wiring: decoupled subscription transport is direct")
{
    hgraph::stdlib::register_standard_operators();
    observed_subscription_keys.clear();

    CHECK_OUTPUT(
        eval_node<DecoupledSubscriptionClientGraph>(
            values<Int>(7),
            values<Value>(dict_delta<Int, TS<Int>>({{7, 70}}))),
        values<Int>(70));
    CHECK(observed_subscription_keys ==
          std::vector<std::pair<std::vector<Int>, std::vector<Int>>>{
              {{7}, {}}});
}

TEST_CASE("service wiring: self-coupled subscription defers only its key relay")
{
    hgraph::stdlib::register_standard_operators();

    Wiring wiring;
    auto key = ts_harness<TS<Int>>::wire_replay(
        wiring, "subscription_direct_response_owner");
    static_cast<void>(PriceClientGraph::compose(wiring, key));
    const GraphBuilder graph = std::move(wiring).finish();

    std::size_t scheduled_gates = 0;
    std::size_t feedback_sources = 0;
    std::size_t feedback_sinks = 0;
    for (const NodeBuilder &node : graph.nodes())
    {
        const NodeTypeMetaData *meta = node.type().schema();
        if (meta == nullptr || meta->display_name == nullptr) { continue; }
        const std::string_view name{meta->display_name};
        scheduled_gates += name == "subscription_response_gate"
                           && meta->uses_scheduler
                               ? 1
                               : 0;
        feedback_sources += name == "feedback_source" ? 1 : 0;
        feedback_sinks += name == "feedback_sink" ? 1 : 0;
    }
    CHECK(scheduled_gates == 0);
    CHECK(feedback_sources == 0);
    CHECK(feedback_sinks == 0);
}

TEST_CASE("service wiring: service-dependent subscription defers only its key relay")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<ServiceDependentPriceClientGraph>(values<Int>(7)),
        values<Int>(none, 80));

    Wiring wiring;
    auto key = ts_harness<TS<Int>>::wire_replay(
        wiring, "subscription_plan_owner");
    static_cast<void>(ServiceDependentPriceClientGraph::compose(wiring, key));
    const GraphBuilder graph = std::move(wiring).finish();
    std::size_t scheduled_gates = 0;
    for (const NodeBuilder &node : graph.nodes())
    {
        const NodeTypeMetaData *meta = node.type().schema();
        if (meta == nullptr || meta->display_name == nullptr) { continue; }
        scheduled_gates += std::string_view{meta->display_name}
                               == "subscription_response_gate"
                           && meta->uses_scheduler
                               ? 1
                               : 0;
    }
    CHECK(scheduled_gates == 0);
}

TEST_CASE("service wiring: subscription response crosses a nested context boundary")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<SubscriptionContextSwitchGraph>(
            values<Int>(7), values<Str>(Str{"left"})),
        values<Int>(none, 70));
}

TEST_CASE("service wiring: successive subscription keys are published in order")
{
    hgraph::stdlib::register_standard_operators();
    observed_subscription_keys.clear();

    CHECK_OUTPUT(eval_node<ObservedPriceClientGraph>(values<Int>(7, none, 8, 7)),
                 values<Int>(none, 70, none, none, 70));
    CHECK(observed_subscription_keys ==
          std::vector<std::pair<std::vector<Int>, std::vector<Int>>>{
              {{7}, {}}, {{8}, {7}}, {{7}, {8}}});
}

TEST_CASE("service wiring: subscription keys preserve registered derived Bundles")
{
    hgraph::stdlib::register_standard_operators();
    const auto *derived = register_derived_subscription_request();
    const Value request = derived_subscription_request(derived, Int{7}, Int{3});

    CHECK_OUTPUT(eval_node<BundlePriceClientGraph>(values<Value>(request)),
                 values<Int>(none, 70));
}

TEST_CASE("service wiring: subscription implementation may terminate in map_")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MappedPriceClientGraph>(values<Int>(7)),
                 values<Int>(none, 70));
}

TEST_CASE("service wiring: mapped subscription implementation can call another subscription service")
{
    hgraph::stdlib::register_standard_operators();

    const auto result = eval_node<NestedSubscriptionClientGraph>(values<Int>(7));
    REQUIRE_FALSE(result.empty());
    REQUIRE(result.back().has_value());
    CHECK(*result.back() == Int{71});
}

TEST_CASE("service wiring: mapped subscription results retain their declared value schema")
{
    hgraph::stdlib::register_standard_operators();

    // The mapped subscription slot exists before its forwarded value is valid.
    // Reduction is over the currently-valid subset, so its explicit identity is
    // published until the first price arrives.
    CHECK_OUTPUT(eval_node<MappedPriceReductionClientGraph>(
                     values<Value>(set_delta<Int>({7}, {}))),
                 values<Int>(0, 70));
}

TEST_CASE("service wiring: a late client samples a key kept live by another client")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<LateDuplicatePriceClientGraph>(
                     values<Int>(7, none, none), values<Int>(none, none, 7)),
                 values<Int>(none, none, 70));
}

TEST_CASE("service wiring: a late structured subscription reply samples existing mapped children")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<StructuredSubscriptionBroadcastMapClientGraph>(
                     values<Int>(7),
                     values<Value>(set_delta<Int>({1, 2}, {})),
                     values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}))),
                 values<Value>(dict_delta<Int, TS<Int>>({}),
                               dict_delta<Int, TS<Int>>({{1, 80}, {2, 90}})));
}

TEST_CASE("service wiring: structured subscription replies publish their bundle")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<StructuredPriceClientGraph>(values<Int>(7)),
                 values<Value>(none,
                               tsb_delta<StructuredPrice>(Int{7}, Int{70})));
}

TEST_CASE("service wiring: subscription service supports explicit paths")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<PathPriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 700, none, 800));
}

TEST_CASE("service wiring: implementation registration is separate from client use")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<RegisteredPriceClientGraph>(values<Int>(7, none, 8)),
                 values<Int>(none, 70, none, 80));
}

TEST_CASE("service wiring: request/reply client receives keyed implementation response")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<AddOneClientGraph>(values<Int>(1)), values<Int>(none, 2));
}

TEST_CASE("service wiring: decoupled request/reply transport is direct")
{
    hgraph::stdlib::register_standard_operators();
    observed_replyless_requests.clear();
    const Int response_key = next_request_id() + Int{1};

    CHECK_OUTPUT(
        eval_node<DecoupledRequestReplyClientGraph>(
            values<Int>(7),
            values<Value>(dict_delta<Int, TS<Int>>({{response_key, 107}}))),
        values<Int>(107));
    CHECK(observed_replyless_requests ==
          std::vector<std::pair<std::size_t, Int>>{{0, 7}});
}

TEST_CASE("service wiring: reply-less clients use the typed same-cycle sink path")
{
    hgraph::stdlib::register_standard_operators();
    observed_replyless_requests.clear();

    CHECK_OUTPUT(
        eval_node<ReplylessPublishTwoClientGraph>(
            values<Int>(1, none, 2), values<Int>(101, none, 102)),
        values<Int>(1, none, 2));

    std::ranges::sort(observed_replyless_requests);
    CHECK(observed_replyless_requests ==
          std::vector<std::pair<std::size_t, Int>>{
              {0, 1}, {0, 101}, {2, 2}, {2, 102}});
}

TEST_CASE("service wiring: a mapped reply-less client defers its outer hand-off")
{
    hgraph::stdlib::register_standard_operators();
    observed_replyless_requests.clear();

    const auto requests = values<Value>(
        dict_delta<Int, TS<Int>>({{1, 10}}),
        dict_delta<Int, TS<Int>>({}, {1}),
        dict_delta<Int, TS<Int>>({{2, 20}}));
    CHECK_OUTPUT(eval_node<ReplylessPublishMappedClientGraph>(requests), requests);
    CHECK(observed_replyless_requests ==
          std::vector<std::pair<std::size_t, Int>>{{1, 10}, {3, 20}});
}

TEST_CASE("service wiring: reply-less supports the typed implementation-stub API")
{
    hgraph::stdlib::register_standard_operators();
    observed_replyless_requests.clear();

    CHECK_OUTPUT(
        eval_node<ReplylessPublishStubClientGraph>(values<Int>(7)),
        values<Int>(7));
    CHECK(observed_replyless_requests ==
          std::vector<std::pair<std::size_t, Int>>{{0, 7}});
}

TEST_CASE("service wiring: a root reply-less dependency cycle is rejected")
{
    hgraph::stdlib::register_standard_operators();
    CHECK_THROWS_WITH(
        (void)eval_node<RecursiveReplylessPublishClientGraph>(values<Int>(1)),
        Catch::Matchers::ContainsSubstring("cycle"));
}

TEST_CASE("service wiring: request/reply service supports explicit paths")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<AddOnePathClientGraph>(values<Int>(7)), values<Int>(none, 107));
}

TEST_CASE("service wiring: request/reply source emits cumulative client requests")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<AddOneTwoClientGraph>(values<Int>(1), values<Int>(10)),
                 values<Int>(none, 13));
}

TEST_CASE("service wiring: request relay preserves requests from successive cycles")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<AddOneClientGraph>(values<Int>(1, 2)),
        values<Int>(none, 2, 3));

    CHECK_OUTPUT(
        eval_node<AddOneStagedClientGraph>(
            values<Int>(1, none), values<Int>(none, 10)),
        values<Value>(
            none,
            list_delta<TS<Int>>({2, none}),
            list_delta<TS<Int>>({none, 11})));
}

TEST_CASE("service wiring: request/reply transports recursive bundle deltas")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SumPairClientGraph>(values<Int>(1, none, 2),
                                               values<Int>(10, none, none)),
                 values<Int>(none, 11, none, 12));
}

TEST_CASE("service wiring: self-contained request/reply omits response feedback")
{
    hgraph::stdlib::register_standard_operators();

    Wiring wiring;
    auto requests = ts_harness<TSD<Int, TS<Int>>>::wire_replay(
        wiring, "request_reply_feedback_owner");
    static_cast<void>(MappedServiceClientGraph::compose(wiring, requests));
    const GraphBuilder first_snapshot = wiring.snapshot();
    const GraphBuilder graph = wiring.snapshot();
    CHECK(graph.nodes().size() == first_snapshot.nodes().size());
    std::size_t feedback_sources = 0;
    std::size_t feedback_sinks = 0;
    for (const NodeBuilder &node : graph.nodes())
    {
        const NodeTypeMetaData *meta = node.type().schema();
        if (meta == nullptr || meta->display_name == nullptr) { continue; }
        const std::string_view name{meta->display_name};
        feedback_sources += name == "feedback_source" ? 1 : 0;
        feedback_sinks += name == "feedback_sink" ? 1 : 0;
    }
    CHECK(feedback_sources == 0);
    CHECK(feedback_sinks == 0);
}

TEST_CASE("service wiring: a service-dependent request/reply retains full feedback")
{
    hgraph::stdlib::register_standard_operators();

    Wiring wiring;
    auto request = ts_harness<TS<Int>>::wire_replay(
        wiring, "request_reply_recursive_feedback_owner");
    static_cast<void>(RecursiveAddOneClientGraph::compose(wiring, request));
    const GraphBuilder graph = std::move(wiring).finish();
    std::size_t feedback_sources = 0;
    std::size_t feedback_sinks = 0;
    for (const NodeBuilder &node : graph.nodes())
    {
        const NodeTypeMetaData *meta = node.type().schema();
        if (meta == nullptr || meta->display_name == nullptr) { continue; }
        const std::string_view name{meta->display_name};
        feedback_sources += name == "feedback_source" ? 1 : 0;
        feedback_sinks += name == "feedback_sink" ? 1 : 0;
    }
    CHECK(feedback_sources == 1);
    CHECK(feedback_sinks == 1);
}

TEST_CASE("service wiring: map and mesh children call an outer request/reply service")
{
    hgraph::stdlib::register_standard_operators();

    const auto requests = values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}));
    const auto expected = values<Value>(
        dict_delta<Int, TS<Int>>({}),
        dict_delta<Int, TS<Int>>({{1, 12}, {2, 22}}));
    CHECK_OUTPUT(eval_node<MappedServiceClientGraph>(requests), expected);
    CHECK_OUTPUT(eval_node<MeshedServiceClientGraph>(requests), expected);
}

TEST_CASE("service wiring: request/reply under map switch retains late keys")
{
    hgraph::stdlib::register_standard_operators();

    // The explicit reduction identity is observable while all mapped switch
    // terminals are invalid.
    CHECK_OUTPUT(
        eval_node<MappedRequestReplySwitchGraph>(
            values<Value>(
                dict_delta<Int, TS<Int>>({{1, 3}}),
                dict_delta<Int, TS<Int>>({{2, 4}}),
                none,
                dict_delta<Int, TS<Int>>({}, {1})),
            values<Str>(Str{"alpha"}, none, Str{"beta"}, none)),
        values<Int>(0, 4, 10, 6));
}

TEST_CASE("service wiring: request/reply switch flip removes an invalid map output")
{
    hgraph::stdlib::register_standard_operators();

    // Issues #105/#117/#119/#133/#145: the request/reply-backed branch is
    // transiently invalid after the flip. The mapped TSD removes the old
    // beta output, then adds the alpha response when it arrives.
    CHECK_OUTPUT(
        eval_node<MappedRequestReplySwitchMapGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({{1, 4}}), none),
            values<Str>(Str{"beta"}, Str{"alpha"})),
        values<Value>(
            dict_delta<Int, TS<Int>>({{1, 6}}),
            dict_delta<Int, TS<Int>>({}, {1}),
            dict_delta<Int, TS<Int>>({{1, 5}})));
}

TEST_CASE("service wiring: a mapped response survives a new key in its delivery cycle")
{
    hgraph::stdlib::register_standard_operators();

    // Issue #175: key 2 arrives EXACTLY in key 1's response-delivery cycle.
    // The map's input-event fast path must still evaluate the child that is
    // due by its own internal schedule — the wake-up was previously lost and
    // key 1's response never arrived at all. (Key 2 one cycle later is the
    // neighbouring test's shape and always worked.)
    CHECK_OUTPUT(
        eval_node<MappedRequestReplySwitchMapGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({{1, 4}}),
                          none,
                          dict_delta<Int, TS<Int>>({{2, 10}}),
                          none,
                          none),
            values<Str>(Str{"alpha"}, none, none, none, none)),
        values<Value>(dict_delta<Int, TS<Int>>({}),
                      dict_delta<Int, TS<Int>>({{1, 5}}),
                      dict_delta<Int, TS<Int>>({}),
                      dict_delta<Int, TS<Int>>({{2, 11}}),
                      none));
}

TEST_CASE("service wiring: a meshed response survives a new key in its delivery cycle")
{
    hgraph::stdlib::register_standard_operators();

    // The mesh worklist must merge a child due by its own internal schedule
    // with a different child created by the outer key tick in the same cycle.
    CHECK_OUTPUT(
        eval_node<MeshedRequestReplySwitchGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({{1, 4}}),
                          none,
                          dict_delta<Int, TS<Int>>({{2, 10}}),
                          none,
                          none),
            values<Str>(Str{"alpha"}, none, none, none, none)),
        values<Value>(dict_delta<Int, TS<Int>>({}),
                      dict_delta<Int, TS<Int>>({{1, 5}}),
                      dict_delta<Int, TS<Int>>({}),
                      dict_delta<Int, TS<Int>>({{2, 11}}),
                      none));
}

TEST_CASE("service wiring: subscription under map switch retains late keys")
{
    hgraph::stdlib::register_standard_operators();

    // Issue #95: reduction is over the currently-valid subset. Its explicit
    // identity is therefore observable while every mapped switch terminal is
    // invalid, and key 1 publishes 15 while key 2 is still a phantom slot.
    // Later complete aggregates still match released hgraph.
    CHECK_OUTPUT(
        eval_node<MappedSubscriptionSwitchGraph>(
            values<Value>(
                dict_delta<Int, TS<Int>>({{1, 3}}),
                dict_delta<Int, TS<Int>>({{2, 4}}),
                none,
                dict_delta<Int, TS<Int>>({}, {1})),
            values<Str>(Str{"alpha"}, none, Str{"beta"}, none)),
        values<Int>(0, 15, 70, 46));
}

TEST_CASE("service wiring: structured subscription response waits for a valid field")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<SetBundleServiceClientGraph>(values<Str>(Str{"a"})),
                 values<Bool>(none, none, true));
}

TEST_CASE("service wiring: a mapped request/reply implementation can call itself recursively")
{
    hgraph::stdlib::register_standard_operators();

    const auto result = eval_node<RecursiveAddOneClientGraph>(values<Int>(3));
    REQUIRE_FALSE(result.empty());
    REQUIRE(result.back().has_value());
    CHECK(*result.back() == Int{4});
}

TEST_CASE("service wiring: mapped children forward subscription keys to an outer service")
{
    hgraph::stdlib::register_standard_operators();

    const auto keys = values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}));
    const auto expected = values<Value>(
        dict_delta<Int, TS<Int>>({}),
        dict_delta<Int, TS<Int>>({{1, 100}, {2, 200}}));
    CHECK_OUTPUT(eval_node<MappedSubscriptionClientGraph>(keys), expected);
}

TEST_CASE("service wiring: validates missing implementations and illegal stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_THROWS_AS((void)eval_node<MissingServiceImplementationGraph>(values<Int>(1)), std::invalid_argument);
    CHECK_THROWS_AS(
        (void)eval_node<MissingMappedServiceImplementationGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({{1, 1}}))),
        std::invalid_argument);
    CHECK_THROWS_AS(build_graph<IllegalServiceStubGraph>(), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<MissingMultiServiceStubGraph>(), std::invalid_argument);
}

TEST_CASE("service wiring: multi-interface implementation graph wires explicit stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MultiServiceClientGraph>(values<Int>(1)), values<Int>(none, 13));
}

TEST_CASE("service wiring: every service-interface flavour permutation shares one implementation")
{
    hgraph::stdlib::register_standard_operators();

    // Exercise both orders of same-flavour and mixed-flavour pairs, followed
    // by every ordering of a reference/subscription/request-reply pack.
    check_mixed_service_permutation<BaseValueService, DerivedValueService>();
    check_mixed_service_permutation<DerivedValueService, BaseValueService>();
    check_mixed_service_permutation<PricesService, DerivedPricesService>();
    check_mixed_service_permutation<DerivedPricesService, PricesService>();
    check_mixed_service_permutation<AddOneService, AddTenService>();
    check_mixed_service_permutation<AddTenService, AddOneService>();

    // Reference=10, subscription(key=2)=20, request/reply(7)=8.
    check_mixed_service_permutation<BaseValueService, PricesService>();
    check_mixed_service_permutation<PricesService, BaseValueService>();
    check_mixed_service_permutation<BaseValueService, AddOneService>();
    check_mixed_service_permutation<AddOneService, BaseValueService>();
    check_mixed_service_permutation<PricesService, AddOneService>();
    check_mixed_service_permutation<AddOneService, PricesService>();

    check_mixed_service_permutation<
        BaseValueService, PricesService, AddOneService>();
    check_mixed_service_permutation<
        BaseValueService, AddOneService, PricesService>();
    check_mixed_service_permutation<
        PricesService, BaseValueService, AddOneService>();
    check_mixed_service_permutation<
        PricesService, AddOneService, BaseValueService>();
    check_mixed_service_permutation<
        AddOneService, BaseValueService, PricesService>();
    check_mixed_service_permutation<
        AddOneService, PricesService, BaseValueService>();
}

TEST_CASE("service wiring: a source-only adaptor is unambiguous alongside services")
{
    hgraph::stdlib::register_standard_operators();

    // RFC 0011 step 9. The value of this case is largely that it COMPILES:
    // this file includes both boundary headers, which previously made
    // wire<SourceOnlyFeedAdaptor> an ambiguous partial specialization.
    CHECK_OUTPUT(eval_node<SourceOnlyFeedGraph>(), values<Int>(9));
}

TEST_CASE("service wiring: one implementation may span an adaptor and a service")
{
    hgraph::stdlib::register_standard_operators();

    // RFC 0011 step 7. register_services describes each member through
    // boundary_detail::group_member, so the pack may mix families when both
    // headers are visible. Value in through the sink-only adaptor, out through
    // the reference service, +1 in between.
    CHECK_OUTPUT(eval_node<MixedFlavourGraph>(values<Int>(1, 2)), values<Int>(2, 3));
}

TEST_CASE("service wiring: a generic implementation output must match the resolved interface schema")
{
    hgraph::stdlib::register_standard_operators();

    // RFC 0011 step 5. Without the resolved-meta comparison in the
    // non-concrete branch of wire_service_impl this builds happily and the
    // capture is created over the IMPLEMENTATION's schema while the source
    // uses the interface's - the same unchecked mismatch adaptors had.
    CHECK_THROWS_AS(build_graph<MismatchedGenericRateGraph>(), std::invalid_argument);
}

TEST_CASE("service wiring: a single-interface implementation may publish by stub")
{
    hgraph::stdlib::register_standard_operators();

    // RFC 0011 step 4. register_services with one interface publishes through
    // from_graph/to_graph rather than by returning a port.
    single_interface_stub_compositions = 0;
    CHECK_OUTPUT(eval_node<SingleInterfaceStubClientGraph>(values<Int>(1)),
                 values<Int>(none, 2));
    CHECK(single_interface_stub_compositions == 1);
}

TEST_CASE("service wiring: register_services materializes only on demand")
{
    hgraph::stdlib::register_standard_operators();

    // RFC 0011 step 4: register_services is now LAZY, matching
    // register_adaptors and the erased register_multi_service_impl. An
    // implementation nothing requests is never composed.
    single_interface_stub_compositions = 0;
    CHECK_NOTHROW(build_graph<UnrequestedMultiServiceGraph>());
    CHECK(single_interface_stub_compositions == 0);
}

TEST_CASE("service wiring: service from_graph/to_graph spell the same wiring as impl_input/impl_output")
{
    hgraph::stdlib::register_standard_operators();

    // RFC 0011 step 3. Identical to the case above, written with the adaptor
    // verbs - they are aliases onto impl_input/impl_output, not a second
    // mechanism, so the observable result must match exactly.
    CHECK_OUTPUT(eval_node<MultiServiceFromToGraphClientGraph>(values<Int>(1)),
                 values<Int>(none, 13));
}

TEST_CASE("service wiring: shared replay service hands clients to the live reference")
{
    hgraph::stdlib::register_standard_operators();

    kafka_service_compositions = 0;
    CHECK_OUTPUT(
        eval_node<KafkaReplayAndLiveClientGraph>(),
        values<Int>(20, 40));
    CHECK(kafka_service_compositions == 1);
}

TEST_CASE("service wiring: service adaptors collect multiple client requests")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ServiceAdaptorTwoClientGraph>(values<Int>(1), values<Int>(10)),
                 values<Int>(51));
}

TEST_CASE("service wiring: qualified adaptor paths separate scalar configurations")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<ServiceAdaptorParameterizedClientGraph>(
            values<Int>(1), values<Int>(10), values<Int>(100)),
        values<Int>(181));
}

TEST_CASE("service wiring: a dynamically started adaptor branch hands off on the next cycle")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<ServiceAdaptorSwitchGraph>(
            values<Str>(Str{"adaptor"}), values<Int>(1)),
        values<Int>(none, 21));
}

TEST_CASE("service wiring: service adaptors preserve successive requests from one client")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ServiceAdaptorSingleClientGraph>(values<Int>(1, 2, 3)),
                 values<Int>(21, 22, 23));
}

TEST_CASE("service wiring: service_adaptor_impl auto-wires single-interface implementations")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ServiceAdaptorImplTwoClientGraph>(values<Int>(1), values<Int>(10)),
                 values<Int>(51));
}

TEST_CASE("service wiring: sink-only service adaptors collect multiple bundled clients")
{
    hgraph::stdlib::register_standard_operators();
    published_pair_requests.clear();

    CHECK_OUTPUT(
        eval_node<SinkServiceAdaptorTwoClientGraph>(
            values<Int>(1, none, 2, none, 3),
            values<Int>(10, none, 20, none, 30),
            values<Int>(3, none, 4, none, 5),
            values<Int>(30, none, 40, none, 50)),
        values<Int>(1, none, 2, none, 3));

    REQUIRE(published_pair_requests.size() == 6);
    const auto first_client = std::get<0>(published_pair_requests[0]);
    const auto second_client = std::get<0>(published_pair_requests[1]);
    CHECK(first_client != second_client);
    CHECK(published_pair_requests == std::vector<std::tuple<Int, Int, Int>>{
        {first_client, 1, 10}, {second_client, 3, 30},
        {first_client, 2, 20}, {second_client, 4, 40},
        {first_client, 3, 30}, {second_client, 5, 50}});
}

TEST_CASE("service wiring: service adaptor first requests snapshot static bundle fields")
{
    hgraph::stdlib::register_standard_operators();
    published_pair_requests.clear();

    CHECK_OUTPUT(
        eval_node<SinkServiceAdaptorConstantFieldGraph>(
            values<Int>(10, none, 20)),
        values<Int>(10, none, 20));

    REQUIRE(published_pair_requests.size() == 2);
    const auto request_id = std::get<0>(published_pair_requests[0]);
    CHECK(published_pair_requests == std::vector<std::tuple<Int, Int, Int>>{
        {request_id, 7, 10}, {request_id, 7, 20}});
}

TEST_CASE("service wiring: multi-interface service adaptors wire explicit stubs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<MultiServiceAdaptorClientGraph>(values<Int>(1)), values<Int>(52));
}

TEST_CASE("service wiring: qualified paths preserve multi-service-adaptor identity")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(
        eval_node<MultiServiceAdaptorQualifiedClientGraph>(values<Int>(1)),
        values<Int>(52));
}

TEST_CASE("service wiring: service adaptors validate requested implementations and ignore unused candidates")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_THROWS_AS((void)eval_node<MissingServiceAdaptorImplementationGraph>(values<Int>(1)), std::invalid_argument);
    CHECK_THROWS_AS(build_graph<IllegalServiceAdaptorStubGraph>(), std::invalid_argument);
    CHECK_NOTHROW(build_graph<MissingServiceAdaptorStubGraph>());
}

TEST_CASE("service wiring: templated service descriptors bind as concrete interfaces")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<TemplateServiceClientGraph>(values<Int>(3)), values<Int>(none, 4));
}

TEST_CASE("service wiring: generic service descriptors resolve from client inputs")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<GenericServiceClientGraph>(values<Int>(3)), values<Int>(none, 4));
    CHECK_OUTPUT(eval_node<GenericFloatServiceClientGraph>(values<Float>(1.5)),
                 values<Float>(none, 2.0));
    CHECK_OUTPUT(eval_node<GenericServiceAdaptorClientGraph>(values<Int>(3)), values<Int>(23));
    CHECK_THROWS_AS((void)eval_node<GenericStringServiceClientGraph>(values<Str>("not numeric")),
                    std::invalid_argument);
}
