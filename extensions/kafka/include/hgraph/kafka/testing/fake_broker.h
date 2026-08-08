#ifndef HGRAPH_KAFKA_TESTING_FAKE_BROKER_H
#define HGRAPH_KAFKA_TESTING_FAKE_BROKER_H

#include <hgraph/kafka/export.h>
#include <hgraph/kafka/service.h>
#include <hgraph/kafka/value_builders.h>

#include <chrono>
#include <memory>
#include <vector>

namespace hgraph::kafka::testing
{
    namespace detail
    {
        struct FakeRuntimeAccess;
    }

    struct FakePublishedRecord
    {
        Int   request_id{};
        Str   topic{};
        Value record{};
    };

    class HGRAPH_KAFKA_EXPORT FakeBroker
    {
      public:
        FakeBroker();
        ~FakeBroker();

        FakeBroker(const FakeBroker &)            = delete;
        FakeBroker &operator=(const FakeBroker &) = delete;

        [[nodiscard]] bool wait_until_attached(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_until_detached(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_subscription_updates(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_publications(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_commits(std::size_t count, std::chrono::milliseconds timeout) const;

        [[nodiscard]] std::size_t                      attach_count() const;
        [[nodiscard]] std::vector<Value>               subscription_deltas() const;
        [[nodiscard]] std::vector<FakePublishedRecord> published_records() const;
        [[nodiscard]] std::vector<Value>               committed_cursors() const;

        void emit_subscription(Value subscription_key, Value record, Value cursor,
                               KafkaSubscriptionState state = KafkaSubscriptionState::Live);

        void emit_event(Value event);

      private:
        friend struct detail::FakeRuntimeAccess;
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    using FakeBrokerPtr = std::shared_ptr<FakeBroker>;

    /** Register the fake transport through the same four-interface service
     * implementation used to prove the native graph boundary. This function is
     * intentionally confined to the testing namespace; production configuration
     * never accepts a broker/factory object. */
    HGRAPH_KAFKA_EXPORT void register_fake_service(Wiring &w, service::ServicePath path, Value service_config,
                                                   FakeBrokerPtr broker);
}  // namespace hgraph::kafka::testing

#endif  // HGRAPH_KAFKA_TESTING_FAKE_BROKER_H
