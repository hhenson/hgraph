#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_data.h>
#include <hgraph/types/value/value.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
    hgraph::TSDataTypeRef proxy_data_type_for(const hgraph::TSValueTypeMetaData &schema,
                                              hgraph::TSRoleTypeRef element_type)
    {
        return hgraph::tsd_proxy_data_type_for(
            schema, element_type,
            hgraph::ValuePlanFactory::instance().type_for(schema.key_type()));
    }

    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (auto it = range.begin(); it != range.end(); ++it) { ++count; }
        return count;
    }

    void key_value_builder(hgraph::TSDProxy      &proxy,
                           std::size_t           slot,
                           const hgraph::TSDataView &target,
                           const hgraph::TSDataView &source,
                           hgraph::DateTime modified_time,
                           const void           *)
    {
        (void)source;
        const auto key = proxy.source_dict().key_at_slot(slot);
        auto       mutation = target.begin_mutation(modified_time);
        if (!mutation.copy_value_from(key))
        {
            throw std::logic_error("TSDProxy test value builder did not materialise the source key");
        }
    }

    void window_key_builder(hgraph::TSDProxy         &proxy,
                            std::size_t                slot,
                            const hgraph::TSDataView  &target,
                            const hgraph::TSDataView  &source,
                            hgraph::DateTime           modified_time,
                            const void                *)
    {
        (void)source;
        auto window = target.as_window();
        auto mutation = window.begin_mutation(modified_time);
        mutation.push(proxy.source_dict().key_at_slot(slot));
    }

    void sparse_bundle_key_builder(hgraph::TSDProxy         &proxy,
                                   std::size_t                slot,
                                   const hgraph::TSDataView  &target,
                                   const hgraph::TSDataView  &source,
                                   hgraph::DateTime           modified_time,
                                   const void                *)
    {
        (void)source;
        auto bundle = target.as_bundle();
        auto child = bundle.at(0);
        auto mutation = child.begin_mutation(modified_time);
        if (!mutation.copy_value_from(proxy.source_dict().key_at_slot(slot)))
        {
            throw std::logic_error("TSDProxy sparse-list builder did not materialise the source key");
        }
    }

    struct CountingBuilderContext
    {
        mutable std::size_t calls{0};
    };

    void counting_key_value_builder(hgraph::TSDProxy         &proxy,
                                    std::size_t                slot,
                                    const hgraph::TSDataView  &target,
                                    const hgraph::TSDataView  &source,
                                    hgraph::DateTime           modified_time,
                                    const void                *context)
    {
        const auto *counting = static_cast<const CountingBuilderContext *>(context);
        REQUIRE(counting != nullptr);
        ++counting->calls;
        key_value_builder(proxy, slot, target, source, modified_time, nullptr);
    }

    struct FailingValueBuilderContext
    {
        mutable std::size_t calls{0};
        mutable std::size_t failures_remaining{0};
    };

    void failing_source_value_builder(hgraph::TSDProxy         &,
                                      std::size_t,
                                      const hgraph::TSDataView  &target,
                                      const hgraph::TSDataView  &source,
                                      hgraph::DateTime           modified_time,
                                      const void                *context)
    {
        const auto *failing = static_cast<const FailingValueBuilderContext *>(context);
        REQUIRE(failing != nullptr);
        ++failing->calls;
        if (failing->failures_remaining != 0)
        {
            --failing->failures_remaining;
            throw std::runtime_error("injected TSDProxy builder failure");
        }
        if (!source.has_current_value())
        {
            throw std::logic_error("TSDProxy failing test builder requires a current source value");
        }
        auto mutation = target.begin_mutation(modified_time);
        static_cast<void>(mutation.copy_value_from(source.value()));
    }

    bool source_value_identity_matches(const hgraph::TSDProxy &,
                                       std::size_t,
                                       const hgraph::TSDataView &target,
                                       const hgraph::TSDataView &source,
                                       const void *)
    {
        if (target.has_current_value() != source.has_current_value()) { return false; }
        return !source.has_current_value() || target.value() == source.value();
    }

    const hgraph::TSDProxyValueOps key_value_ops{&key_value_builder, nullptr};
    const hgraph::TSDProxyValueOps window_key_ops{&window_key_builder, nullptr};
    const hgraph::TSDProxyValueOps sparse_bundle_key_ops{&sparse_bundle_key_builder, nullptr};
    const hgraph::TSDProxyValueOps counting_key_value_ops{&counting_key_value_builder, nullptr};
    const hgraph::TSDProxyValueOps failing_value_ops{&failing_source_value_builder, nullptr};
    const hgraph::TSDProxyValueOps failing_value_refresh_ops{
        &failing_source_value_builder,
        &source_value_identity_matches,
    };

    struct IdentityCountingContext
    {
        mutable std::size_t builds{0};
        mutable std::size_t matches{0};
    };

    void identity_counting_builder(hgraph::TSDProxy &,
                                   std::size_t,
                                   const hgraph::TSDataView &target,
                                   const hgraph::TSDataView &source,
                                   hgraph::DateTime modified_time,
                                   const void *context)
    {
        const auto *counts = static_cast<const IdentityCountingContext *>(context);
        REQUIRE(counts != nullptr);
        ++counts->builds;
        auto mutation = target.begin_mutation(modified_time);
        static_cast<void>(mutation.copy_value_from(source.value()));
    }

    bool identity_counting_matcher(const hgraph::TSDProxy &,
                                   std::size_t,
                                   const hgraph::TSDataView &target,
                                   const hgraph::TSDataView &source,
                                   const void *context)
    {
        const auto *counts = static_cast<const IdentityCountingContext *>(context);
        REQUIRE(counts != nullptr);
        ++counts->matches;
        return target.has_current_value() == source.has_current_value() &&
               (!source.has_current_value() || target.value() == source.value());
    }

    const hgraph::TSDProxyValueOps identity_counting_ops{
        &identity_counting_builder,
        &identity_counting_matcher,
    };

    struct RecordingSlotObserver final : hgraph::SlotObserver
    {
        std::vector<std::string> events;
        std::size_t clears{0};

        void on_capacity(std::size_t, std::size_t) override { events.emplace_back("capacity"); }
        void on_insert(std::size_t slot) override { events.emplace_back("insert:" + std::to_string(slot)); }
        void on_remove(std::size_t slot) override { events.emplace_back("remove:" + std::to_string(slot)); }
        void on_erase(std::size_t slot) override { events.emplace_back("erase:" + std::to_string(slot)); }
        void on_clear() override
        {
            ++clears;
            events.emplace_back("clear");
        }
    };

    struct InvalidationObserver final : hgraph::Notifiable
    {
        std::size_t invalidations{0};
        const hgraph::TSDataTracking *source{nullptr};

        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *invalidated) noexcept override
        {
            ++invalidations;
            source = invalidated;
        }
    };

    struct ReentrantBindAttempt
    {
        hgraph::TSData *proxy{nullptr};
        hgraph::TSData *replacement{nullptr};
        std::size_t attempts{0};
        std::size_t rejected{0};
        std::string message;

        void attempt() noexcept
        {
            ++attempts;
            try
            {
                auto proxy_view = proxy->view();
                auto replacement_view = replacement->view();
                hgraph::bind_tsd_proxy(proxy_view, replacement_view.as_dict(), &key_value_ops,
                                       nullptr, hgraph::MIN_ST);
            }
            catch (const std::logic_error &error)
            {
                ++rejected;
                message = error.what();
            }
            catch (...) { message = "unexpected exception"; }

            if (proxy != nullptr)
            {
                auto proxy_view = proxy->view();
                static_cast<hgraph::TSDProxy *>(const_cast<void *>(proxy_view.data()))->stop();
            }
        }
    };

    struct ReentrantRootObserver final : hgraph::Notifiable, ReentrantBindAttempt
    {
        void notify(hgraph::DateTime) override {}
        void source_invalidated(const hgraph::TSDataTracking *) noexcept override { attempt(); }
    };

    struct ReentrantClearObserver final : hgraph::SlotObserver, ReentrantBindAttempt
    {
        std::size_t clears{0};
        void on_capacity(std::size_t, std::size_t) override {}
        void on_insert(std::size_t) override {}
        void on_remove(std::size_t) override {}
        void on_erase(std::size_t) override {}
        void on_clear() override
        {
            ++clears;
            attempt();
        }
    };
}

TEST_CASE("TSDProxy source invalidation clears live and pending children once and supports rebind")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    const auto proxy_type = proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()});

    REQUIRE(source_type.ops_ref().ownership_ops != nullptr);
    REQUIRE(proxy_type.ops_ref().ownership_ops != nullptr);
    REQUIRE(proxy_type.ops_ref().ownership_ops != source_type.ops_ref().ownership_ops);

    std::optional<TSData> source{std::in_place, source_type};
    std::optional<TSData> proxy{std::in_place, proxy_type};
    Value key_one{1};
    Value key_two{2};
    Value key_three{3};
    {
        auto source_view = source->view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto one = mutation.at(key_one.view());
        auto two = mutation.at(key_two.view());
        REQUIRE(one.begin_mutation(MIN_ST).copy_value_from(key_one.view()));
        REQUIRE(two.begin_mutation(MIN_ST).copy_value_from(key_two.view()));
    }
    {
        auto proxy_view = proxy->view();
        auto source_view = source->view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &key_value_ops, nullptr, MIN_ST);
    }

    RecordingSlotObserver slots;
    InvalidationObserver root;
    auto proxy_view = proxy->view();
    proxy_view.as_dict().key_set().subscribe_slot_observer(&slots);
    proxy_view.subscribe(&root);
    const auto proxy_tracking = &proxy_view.tracking();
    auto source_before_remove = source->view();
    const auto removed_slot = source_before_remove.as_dict().find_slot(key_one.view());

    const auto t2 = MIN_ST + TimeDelta{1};
    {
        auto source_view = source->view();
        auto mutation = source_view.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(key_one.view()));
    }
    REQUIRE(std::ranges::find(slots.events, "remove:" + std::to_string(removed_slot)) != slots.events.end());

    source.reset();
    REQUIRE(root.invalidations == 1);
    REQUIRE(root.source == proxy_tracking);
    REQUIRE(slots.clears == 1);
    auto invalidated_proxy = proxy->view();
    REQUIRE_FALSE(invalidated_proxy.has_current_value());
    REQUIRE(invalidated_proxy.as_dict().size() == 0);
    REQUIRE(invalidated_proxy.as_dict().slot_capacity() == 0);

    TSData replacement{source_type};
    const auto t3 = t2 + TimeDelta{1};
    {
        auto replacement_view = replacement.view();
        auto mutation = replacement_view.as_dict().begin_mutation(t3);
        auto child = mutation.at(key_three.view());
        REQUIRE(child.begin_mutation(t3).copy_value_from(key_three.view()));
    }
    {
        auto rebound_proxy = proxy->view();
        auto replacement_view = replacement.view();
        bind_tsd_proxy(rebound_proxy, replacement_view.as_dict(), &key_value_ops, nullptr, t3);
    }
    REQUIRE(slots.clears == 1);
    auto rebound = proxy->view();
    REQUIRE(rebound.as_dict().contains(key_three.view()));
    REQUIRE(rebound.as_dict().at(key_three.view()).value().checked_as<std::int32_t>() == 3);

    proxy.reset();
    REQUIRE(slots.clears == 1);
}

TEST_CASE("TSDProxy source invalidation cascades immediately through nested proxies")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    const auto proxy_type = proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()});

    std::optional<TSData> source{std::in_place, source_type};
    TSData first{proxy_type};
    TSData second{proxy_type};
    Value key{1};
    {
        auto source_view = source->view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(key.view());
        REQUIRE(child.begin_mutation(MIN_ST).copy_value_from(key.view()));
    }
    {
        auto first_view = first.view();
        auto source_view = source->view();
        bind_tsd_proxy(first_view, source_view.as_dict(), &key_value_ops, nullptr, MIN_ST);
    }
    {
        auto second_view = second.view();
        auto first_view = first.view();
        bind_tsd_proxy(second_view, first_view.as_dict(), &key_value_ops, nullptr, MIN_ST);
    }

    RecordingSlotObserver slots;
    auto second_before_invalidation = second.view();
    second_before_invalidation.as_dict().key_set().subscribe_slot_observer(&slots);
    source.reset();

    auto invalidated_first = first.view();
    auto invalidated_second = second.view();
    REQUIRE_FALSE(invalidated_first.has_current_value());
    REQUIRE_FALSE(invalidated_second.has_current_value());
    REQUIRE(invalidated_first.as_dict().size() == 0);
    REQUIRE(invalidated_second.as_dict().size() == 0);
    REQUIRE(slots.clears == 1);
}

TEST_CASE("TSDProxy rejects reentrant root and clear callback binds until invalidation completes")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    const auto proxy_type = proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()});
    std::optional<TSData> source{std::in_place, source_type};
    TSData replacement{source_type};
    TSData proxy{proxy_type};
    Value old_key{1};
    Value new_key{2};
    {
        auto source_view = source->view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(old_key.view());
        REQUIRE(child.begin_mutation(MIN_ST).copy_value_from(old_key.view()));
    }
    {
        auto replacement_view = replacement.view();
        auto mutation = replacement_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(new_key.view());
        REQUIRE(child.begin_mutation(MIN_ST).copy_value_from(new_key.view()));
    }
    auto proxy_view = proxy.view();
    auto source_view = source->view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &key_value_ops, nullptr, MIN_ST);

    ReentrantRootObserver root_attempt;
    root_attempt.proxy = &proxy;
    root_attempt.replacement = &replacement;
    ReentrantClearObserver clear_attempt;
    clear_attempt.proxy = &proxy;
    clear_attempt.replacement = &replacement;
    proxy_view.subscribe(&root_attempt);
    proxy_view.as_dict().key_set().subscribe_slot_observer(&clear_attempt);

    source.reset();
    const std::string expected =
        "TSDProxy cannot bind while source invalidation is in progress";
    REQUIRE(root_attempt.attempts == 1);
    REQUIRE(root_attempt.rejected == 1);
    REQUIRE(root_attempt.message == expected);
    REQUIRE(clear_attempt.clears == 1);
    REQUIRE(clear_attempt.attempts == 1);
    REQUIRE(clear_attempt.rejected == 1);
    REQUIRE(clear_attempt.message == expected);
    REQUIRE(replacement.view().observer_count() == 0);

    proxy_view = proxy.view();
    auto &storage = *static_cast<TSDProxy *>(const_cast<void *>(proxy_view.data()));
    REQUIRE_FALSE(storage.source_available());
    REQUIRE(proxy_view.as_dict().size() == 0);
    storage.stop();
    storage.stop();
    REQUIRE(clear_attempt.clears == 1);
    REQUIRE(replacement.view().observer_count() == 0);

    auto replacement_view = replacement.view();
    REQUIRE_NOTHROW(bind_tsd_proxy(proxy_view, replacement_view.as_dict(), &key_value_ops,
                                   nullptr, MIN_ST));
    REQUIRE(storage.source_available());
    REQUIRE(replacement.view().observer_count() == 1);
    REQUIRE(proxy_view.as_dict().at(new_key.view()).value().checked_as<std::int32_t>() == 2);
    proxy_view.as_dict().key_set().unsubscribe_slot_observer(&clear_attempt);
    storage.stop();
}

TEST_CASE("TSDProxy cache-style resync never rebuilds a pending removed child")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);

    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        REQUIRE(child.begin_mutation(t1).copy_value_from(key.view()));
    }

    CountingBuilderContext builds;
    auto proxy_view = proxy.view();
    auto source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &counting_key_value_ops, &builds, t1);
    auto initial = proxy_view.as_dict();
    const auto slot = initial.find_slot(key.view());
    const auto address = initial.at_slot(slot).data();
    const auto capacity = initial.slot_capacity();
    REQUIRE(builds.calls == 1);

    RecordingSlotObserver lifecycle;
    initial.key_set().subscribe_slot_observer(&lifecycle);
    {
        auto mutation_source = source.view();
        auto mutation = mutation_source.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(key.view()));
    }
    REQUIRE(builds.calls == 1);
    REQUIRE(lifecycle.events == std::vector<std::string>{"remove:" + std::to_string(slot)});

    proxy_view = proxy.view();
    source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &counting_key_value_ops, &builds, t2);
    auto pending_view = proxy.view();
    auto pending = pending_view.as_dict();
    REQUIRE(builds.calls == 1);
    REQUIRE_FALSE(pending.slot_live(slot));
    REQUIRE(pending.slot_removed(slot));
    REQUIRE(pending.slot_occupied(slot));
    REQUIRE(pending.at_slot(slot).data() == address);
    REQUIRE(pending.slot_capacity() == capacity);
    REQUIRE(lifecycle.events == std::vector<std::string>{"remove:" + std::to_string(slot)});

    {
        auto mutation_source = source.view();
        auto mutation = mutation_source.as_dict().begin_mutation(t2);
        auto retained = mutation.at(key.view());
        REQUIRE(retained.has_current_value());
    }
    auto resurrected_view = proxy.view();
    auto resurrected = resurrected_view.as_dict();
    REQUIRE(builds.calls == 2);
    REQUIRE(resurrected.slot_live(slot));
    REQUIRE(resurrected.at_slot(slot).data() == address);
    REQUIRE(resurrected.slot_capacity() == capacity);
    REQUIRE(lifecycle.events == std::vector<std::string>{
                                    "remove:" + std::to_string(slot),
                                    "insert:" + std::to_string(slot)});
    resurrected.key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy cache-style resync builds deferred same-time inserts without announcing them twice")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);

    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key_one{1};
    Value key_two{2};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    CountingBuilderContext builds;
    auto proxy_view = proxy.view();
    auto source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &counting_key_value_ops, &builds, t1);
    REQUIRE(builds.calls == 0);

    RecordingSlotObserver lifecycle;
    proxy_view.as_dict().key_set().subscribe_slot_observer(&lifecycle);
    {
        auto source_data = source.view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        auto one = mutation.at(key_one.view());
        REQUIRE(one.begin_mutation(t2).copy_value_from(key_one.view()));
        auto two = mutation.at(key_two.view());
        REQUIRE(two.begin_mutation(t2).copy_value_from(key_two.view()));
    }

    source_view = source.view();
    auto source_dict = source_view.as_dict();
    const auto slot_one = source_dict.find_slot(key_one.view());
    const auto slot_two = source_dict.find_slot(key_two.view());
    REQUIRE(builds.calls == 1);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot_one)) == 1);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot_two)) == 1);

    proxy_view = proxy.view();
    bind_tsd_proxy(proxy_view, source_dict, &counting_key_value_ops, &builds, t2);
    auto cached = proxy_view.as_dict();
    REQUIRE(builds.calls == 2);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot_one)) == 1);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot_two)) == 1);
    REQUIRE(cached.at(key_one.view()).value().checked_as<std::int32_t>() == 1);
    REQUIRE(cached.at(key_two.view()).value().checked_as<std::int32_t>() == 2);

    proxy_view = proxy.view();
    source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &counting_key_value_ops, &builds, t2);
    REQUIRE(builds.calls == 2);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot_one)) == 1);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot_two)) == 1);
    proxy_view.as_dict().key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy same-time identity reconciliation builds once and repeat reads only compare")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value value{11};
        REQUIRE(child.begin_mutation(t1).copy_value_from(value.view()));
    }

    IdentityCountingContext counts;
    auto proxy_view = proxy.view();
    auto source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &identity_counting_ops, &counts, t1,
                   TSDProxyChildRefresh::OnChildTick);
    REQUIRE(counts.builds == 1);

    {
        auto source_data = source.view();
        auto child = source_data.as_dict().at(key.view());
        Value first{12};
        REQUIRE(child.begin_mutation(t2).copy_value_from(first.view()));
        Value second{13};
        static_cast<void>(child.begin_mutation(t2).copy_value_from(second.view()));
    }
    REQUIRE(counts.builds == 2);

    proxy_view = proxy.view();
    auto dict = proxy_view.as_dict();
    REQUIRE(dict.at(key.view()).value().checked_as<std::int32_t>() == 13);
    REQUIRE(counts.builds == 3);
    const auto matches_after_reconcile = counts.matches;
    REQUIRE(dict.at(key.view()).value().checked_as<std::int32_t>() == 13);
    REQUIRE(dict.at(key.view()).value().checked_as<std::int32_t>() == 13);
    REQUIRE(counts.builds == 3);
    REQUIRE(counts.matches == matches_after_reconcile + 2);
}

TEST_CASE("TSDProxy stale child records join the current updated window")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer  = registry.register_scalar<std::int32_t>("int32");
    const auto *ts       = registry.ts(integer);
    const auto *tsd      = registry.tsd(integer, ts);
    const auto  source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto  element_type = TSDataPlanFactory::instance().data_type_for(ts);

    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key_one{1};
    Value key_two{2};
    Value value_one{11};
    Value value_two{22};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        mutation.set(key_one.view(), value_one.view());
        mutation.set(key_two.view(), value_two.view());
    }

    auto proxy_view = proxy.view();
    auto source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &key_value_ops, nullptr, t1);

    proxy_view = proxy.view();
    auto proxy_dict = proxy_view.as_dict();
    const auto slot_one = proxy_dict.find_slot(key_one.view());
    const auto slot_two = proxy_dict.find_slot(key_two.view());
    REQUIRE(slot_one != TS_DATA_NO_CHILD_ID);
    REQUIRE(slot_two != TS_DATA_NO_CHILD_ID);

    auto &storage = *static_cast<TSDProxy *>(const_cast<void *>(proxy_view.data()));
    storage.record_child_modified(slot_one, t2);
    REQUIRE(storage.tracking().record_modified(t2));
    REQUIRE(storage.child_updated(slot_one));
    REQUIRE_FALSE(storage.child_updated(slot_two));

    storage.record_child_modified(slot_two, t1);
    REQUIRE_FALSE(storage.tracking().record_modified(t1));
    REQUIRE(storage.tracking().last_modified_time == t2);
    REQUIRE(storage.child_updated(slot_one));
    REQUIRE(storage.child_updated(slot_two));
}

TEST_CASE("TSDProxy announced insertion retries a failed build without a second insert")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};

    FailingValueBuilderContext builds{.failures_remaining = 2};
    auto proxy_view = proxy.view();
    auto source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops, &builds, t1);
    RecordingSlotObserver lifecycle;
    proxy_view.as_dict().key_set().subscribe_slot_observer(&lifecycle);

    REQUIRE_THROWS_AS(([&] {
        auto source_data = source.view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        auto child = mutation.at(key.view());
        Value value{11};
        static_cast<void>(child.begin_mutation(t2).copy_value_from(value.view()));
    })(), std::runtime_error);
    source_view = source.view();
    const auto slot = source_view.as_dict().find_slot(key.view());
    REQUIRE(builds.calls == 1);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);

    proxy_view = proxy.view();
    auto pending = proxy_view.as_dict();
    REQUIRE_THROWS_AS(pending.at(key.view()), std::runtime_error);
    REQUIRE(builds.calls == 2);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);

    {
        auto source_data = source.view();
        auto child = source_data.as_dict().at(key.view());
        Value value{12};
        REQUIRE(child.begin_mutation(t3).copy_value_from(value.view()));
    }
    proxy_view = proxy.view();
    auto recovered = proxy_view.as_dict();
    REQUIRE(builds.calls == 3);
    REQUIRE(recovered.at(key.view()).value().checked_as<std::int32_t>() == 12);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);
    recovered.key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy insert-owed resurrection survives builder failure and delta rollover")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    const auto t4 = t3 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value value{11};
        REQUIRE(child.begin_mutation(t1).copy_value_from(value.view()));
    }

    FailingValueBuilderContext builds;
    auto proxy_view = proxy.view();
    auto source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_refresh_ops, &builds, t1,
                   TSDProxyChildRefresh::OnChildTick);
    auto initial = proxy_view.as_dict();
    const auto slot = initial.find_slot(key.view());
    auto &proxy_storage = *static_cast<TSDProxy *>(const_cast<void *>(proxy_view.data()));
    const auto address = proxy_storage.owned_child_memory(slot);
    const auto capacity = proxy_storage.child_capacity();
    REQUIRE(builds.calls == 1);
    RecordingSlotObserver lifecycle;
    initial.key_set().subscribe_slot_observer(&lifecycle);

    builds.failures_remaining = 1;
    REQUIRE_THROWS_AS(([&] {
        auto source_data = source.view();
        auto mutation = source_data.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(key.view()));
        static_cast<void>(mutation.at(key.view()));
    })(), std::runtime_error);
    REQUIRE(builds.calls == 2);
    REQUIRE(proxy_storage.owned_child_memory(slot) == address);
    REQUIRE(proxy_storage.child_capacity() == capacity);
    REQUIRE(std::ranges::count(lifecycle.events, "remove:" + std::to_string(slot)) == 1);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 0);

    {
        auto source_data = source.view();
        auto child = source_data.as_dict().at(key.view());
        Value value{12};
        REQUIRE(child.begin_mutation(t3).copy_value_from(value.view()));
    }
    proxy_view = proxy.view();
    auto recovered = proxy_view.as_dict();
    REQUIRE(builds.calls == 3);
    REQUIRE(proxy_storage.owned_child_memory(slot) == address);
    REQUIRE(proxy_storage.child_capacity() == capacity);
    REQUIRE(recovered.at(key.view()).value().checked_as<std::int32_t>() == 12);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);

    {
        auto source_data = source.view();
        auto child = source_data.as_dict().at(key.view());
        Value value{13};
        REQUIRE(child.begin_mutation(t4).copy_value_from(value.view()));
    }
    proxy_view = proxy.view();
    auto after_tick = proxy_view.as_dict();
    REQUIRE(builds.calls == 4);
    REQUIRE(after_tick.at(key.view()).value().checked_as<std::int32_t>() == 13);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);
    after_tick.key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy initial unannounced build failure retains insert debt until retry")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value value{11};
        REQUIRE(child.begin_mutation(t1).copy_value_from(value.view()));
    }

    FailingValueBuilderContext builds{.failures_remaining = 1};
    auto proxy_view = proxy.view();
    RecordingSlotObserver lifecycle;
    proxy_view.as_dict().key_set().subscribe_slot_observer(&lifecycle);
    auto source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, t1), std::runtime_error);
    REQUIRE(builds.calls == 1);
    REQUIRE(std::ranges::count_if(lifecycle.events, [](const auto &event) {
        return event.starts_with("insert:");
    }) == 0);

    proxy_view = proxy.view();
    source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops, &builds, t2);
    auto recovered = proxy_view.as_dict();
    const auto slot = recovered.find_slot(key.view());
    REQUIRE(builds.calls == 2);
    REQUIRE(recovered.at(key.view()).value().checked_as<std::int32_t>() == 11);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);

    bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops, &builds, t2);
    REQUIRE(builds.calls == 2);
    REQUIRE(std::ranges::count(lifecycle.events, "insert:" + std::to_string(slot)) == 1);
    recovered.key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy rejects sentinel build times before invoking the builder")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value value{11};
        REQUIRE(child.begin_mutation(t1).copy_value_from(value.view()));
    }

    FailingValueBuilderContext builds;
    auto proxy_view = proxy.view();
    auto source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, MIN_DT), std::invalid_argument);
    REQUIRE(builds.calls == 0);
    proxy_view = proxy.view();
    source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, MAX_DT), std::invalid_argument);
    REQUIRE(builds.calls == 0);

    proxy_view = proxy.view();
    source_view = source.view();
    bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops, &builds, t1);
    REQUIRE(builds.calls == 1);
    auto recovered = proxy_view.as_dict();
    REQUIRE(recovered.at(key.view()).value().checked_as<std::int32_t>() == 11);
}

TEST_CASE("TSDProxy rejects sentinel lifecycle times for an untouched empty source without side effects")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    FailingValueBuilderContext builds;
    RecordingSlotObserver lifecycle;
    auto proxy_view = proxy.view();
    proxy_view.as_dict().key_set().subscribe_slot_observer(&lifecycle);
    auto &storage = *static_cast<TSDProxy *>(const_cast<void *>(proxy_view.data()));
    const auto source_observers = source.view().observer_count();

    const auto require_unconfigured = [&] {
        REQUIRE_FALSE(storage.source_available());
        REQUIRE_FALSE(storage.element_type());
        REQUIRE(storage.child_capacity() == 0);
        REQUIRE(storage.tracking().last_modified_time == MIN_DT);
        REQUIRE(source.view().observer_count() == source_observers);
        REQUIRE(builds.calls == 0);
        REQUIRE(lifecycle.events.empty());
    };

    auto source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, MIN_DT), std::invalid_argument);
    require_unconfigured();
    source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, MAX_DT), std::invalid_argument);
    require_unconfigured();

    source_view = source.view();
    REQUIRE_NOTHROW(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                   &builds, MIN_ST));
    REQUIRE(storage.source_available());
    REQUIRE(storage.tracking().last_modified_time == MIN_DT);
    REQUIRE(source.view().observer_count() == source_observers + 1);
    storage.stop();

    source_view = source.view();
    REQUIRE_NOTHROW(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                   &builds, MAX_ET));
    REQUIRE(storage.source_available());
    REQUIRE(storage.tracking().last_modified_time != MAX_DT);
    REQUIRE(source.view().observer_count() == source_observers + 1);
    storage.stop();
    proxy_view.as_dict().key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy rejects sentinel lifecycle times for a touched empty source without side effects")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts = registry.ts(integer);
    const auto *tsd = registry.tsd(integer, ts);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts);
    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd, TSRoleTypeRef{element_type.as_role()})};
    Value key{1};
    const auto t1 = MIN_ST;
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value value{11};
        REQUIRE(child.begin_mutation(t1).copy_value_from(value.view()));
        REQUIRE(mutation.erase(key.view()));
    }
    auto touched_source = source.view();
    REQUIRE(touched_source.has_current_value());
    REQUIRE(touched_source.as_dict().size() == 0);

    FailingValueBuilderContext builds;
    RecordingSlotObserver lifecycle;
    auto proxy_view = proxy.view();
    proxy_view.as_dict().key_set().subscribe_slot_observer(&lifecycle);
    auto &storage = *static_cast<TSDProxy *>(const_cast<void *>(proxy_view.data()));
    const auto source_observers = source.view().observer_count();

    const auto require_unconfigured = [&] {
        REQUIRE_FALSE(storage.source_available());
        REQUIRE_FALSE(storage.element_type());
        REQUIRE(storage.child_capacity() == 0);
        REQUIRE(storage.tracking().last_modified_time == MIN_DT);
        REQUIRE(source.view().observer_count() == source_observers);
        REQUIRE(builds.calls == 0);
        REQUIRE(lifecycle.events.empty());
    };

    auto source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, MIN_DT), std::invalid_argument);
    require_unconfigured();
    source_view = source.view();
    REQUIRE_THROWS_AS(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                     &builds, MAX_DT), std::invalid_argument);
    require_unconfigured();

    source_view = source.view();
    REQUIRE_NOTHROW(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                   &builds, MIN_ST));
    REQUIRE(storage.source_available());
    REQUIRE(storage.tracking().last_modified_time == MIN_ST);
    REQUIRE(storage.tracking().last_modified_time != MAX_DT);
    REQUIRE(source.view().observer_count() == source_observers + 1);
    storage.stop();

    source_view = source.view();
    REQUIRE_NOTHROW(bind_tsd_proxy(proxy_view, source_view.as_dict(), &failing_value_ops,
                                   &builds, MAX_ET));
    REQUIRE(storage.source_available());
    REQUIRE(storage.tracking().last_modified_time == MAX_ET);
    REQUIRE(storage.tracking().last_modified_time != MAX_DT);
    REQUIRE(source.view().observer_count() == source_observers + 1);
    storage.stop();
    proxy_view.as_dict().key_set().unsubscribe_slot_observer(&lifecycle);
}

TEST_CASE("TSDProxy constructs builder-created values from source key slots")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(int_meta, ts_int);

    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd_int);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts_int);

    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*tsd_int, TSRoleTypeRef{element_type.as_role()})};

    Value key_one{1};
    Value key_two{2};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto mutation    = source_dict.begin_mutation(MIN_ST);
        auto child       = mutation.at(key_one.view());
        Value value{11};
        auto  child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto proxy_view = proxy.view();
    auto bind_source_view = source.view();
    bind_tsd_proxy(proxy_view, bind_source_view.as_dict(), &key_value_ops, nullptr, MIN_ST);

    auto initial_view = proxy.view();
    auto initial = initial_view.as_dict();
    REQUIRE(std::string{initial_view.storage_type().record()->implementation_name()} == "ts.tsd.proxy.data");
    REQUIRE(std::string{initial.key_set().base().storage_type().record()->implementation_name()} ==
            "ts.tsd.key-set.data");
    REQUIRE(std::string{initial.at(key_one.view()).storage_type().record()->implementation_name()} ==
            "ts.tsd.value.data");
    REQUIRE(initial.value().binding().ops_ref().kind == ValueOpsKind::Map);
    REQUIRE(initial.delta_value(MIN_ST).binding().ops_ref().kind == ValueOpsKind::Indexed);
    REQUIRE(initial.value().as_map().key_set().binding().ops_ref().kind == ValueOpsKind::Set);
    REQUIRE(initial.modified(MIN_ST));
    REQUIRE(initial.contains(key_one.view()));
    REQUIRE(range_count(initial.items()) == 1);
    REQUIRE(initial.at(key_one.view()).value().checked_as<std::int32_t>() == 1);
    REQUIRE_THROWS_AS(proxy.view().begin_mutation(MIN_ST), std::logic_error);

    const auto t2 = MIN_ST + TimeDelta{1};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto mutation    = source_dict.begin_mutation(t2);
        auto child       = mutation.at(key_two.view());
        Value value{22};
        auto  child_mutation = child.begin_mutation(t2);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto after_add_view = proxy.view();
    auto after_add = after_add_view.as_dict();
    REQUIRE(after_add.modified(t2));
    REQUIRE(after_add.contains(key_one.view()));
    REQUIRE(after_add.contains(key_two.view()));
    REQUIRE(range_count(after_add.items()) == 2);
    REQUIRE(after_add.at(key_two.view()).value().checked_as<std::int32_t>() == 2);

    const auto t3 = t2 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto mutation    = source_dict.begin_mutation(t3);
        REQUIRE(mutation.erase(key_one.view()));
    }

    auto after_remove_view = proxy.view();
    auto after_remove = after_remove_view.as_dict();
    REQUIRE(after_remove.modified(t3));
    REQUIRE_FALSE(after_remove.contains(key_one.view()));
    REQUIRE(after_remove.contains(key_two.view()));
    REQUIRE(range_count(after_remove.items()) == 1);

    auto removed = after_remove.key_set().removed();
    REQUIRE(range_count(removed) == 1);
    REQUIRE((*removed.begin()).checked_as<std::int32_t>() == 1);

    const auto t4 = t3 + TimeDelta{1};
    {
        auto source_view = source.view();
        auto source_dict = source_view.as_dict();
        auto child       = source_dict.at(key_two.view());
        Value value{222};
        auto  child_mutation = child.begin_mutation(t4);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto after_value_tick_view = proxy.view();
    auto after_value_tick = after_value_tick_view.as_dict();
    REQUIRE_FALSE(after_value_tick.modified(t4));
    REQUIRE(after_value_tick.at(key_two.view()).value().checked_as<std::int32_t>() == 2);
}

TEST_CASE("TSDProxy projected values preserve canonical hash equality comparison and capabilities")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(int_meta, ts_int);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(tsd_int);
    const auto element_type = TSDataPlanFactory::instance().data_type_for(ts_int);

    TSData source_a{source_type};
    TSData source_b{source_type};
    TSData source_different{source_type};
    TSData proxy_a{proxy_data_type_for(*tsd_int, TSRoleTypeRef{element_type.as_role()})};
    TSData proxy_b{proxy_data_type_for(*tsd_int, TSRoleTypeRef{element_type.as_role()})};
    TSData proxy_different{proxy_data_type_for(*tsd_int, TSRoleTypeRef{element_type.as_role()})};

    const auto add_key = [](TSData &source, std::int32_t key, std::int32_t value) {
        Value key_value{key};
        Value child_value{value};
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(key_value.view());
        auto child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(child_value.view()));
    };
    add_key(source_a, 1, 11);
    add_key(source_a, 2, 22);
    add_key(source_b, 2, 222);
    add_key(source_b, 1, 111);
    add_key(source_different, 1, 11);
    add_key(source_different, 3, 33);

    const auto bind_proxy = [](TSData &proxy, TSData &source) {
        auto proxy_view = proxy.view();
        auto source_view = source.view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &key_value_ops, nullptr, MIN_ST);
    };
    bind_proxy(proxy_a, source_a);
    bind_proxy(proxy_b, source_b);
    bind_proxy(proxy_different, source_different);

    auto proxy_a_view = proxy_a.view();
    auto proxy_b_view = proxy_b.view();
    auto proxy_different_view = proxy_different.view();
    auto dict_a = proxy_a_view.as_dict();
    auto dict_b = proxy_b_view.as_dict();
    auto dict_different = proxy_different_view.as_dict();
    auto map_a = dict_a.value();
    auto map_b = dict_b.value();
    auto map_different = dict_different.value();
    auto keys_a = map_a.as_map().key_set();
    auto keys_b = map_b.as_map().key_set();
    auto keys_different = map_different.as_map().key_set();
    auto delta_a = dict_a.delta_value(MIN_ST);
    auto delta_b = dict_b.delta_value(MIN_ST);
    auto delta_different = dict_different.delta_value(MIN_ST);

    REQUIRE(map_a.hash() == map_b.hash());
    REQUIRE(map_a.equals(map_b));
    REQUIRE(std::is_eq(map_a.compare(map_b)));
    REQUIRE_FALSE(map_a.equals(map_different));
    REQUIRE(map_a.compare(map_different) == std::partial_ordering::unordered);
    REQUIRE_FALSE(map_a.to_string().empty());

    REQUIRE(keys_a.hash() == keys_b.hash());
    REQUIRE(keys_a.equals(keys_b));
    REQUIRE(std::is_eq(keys_a.compare(keys_b)));
    REQUIRE_FALSE(keys_a.equals(keys_different));
    REQUIRE(keys_a.compare(keys_different) == std::partial_ordering::unordered);
    REQUIRE_FALSE(keys_a.to_string().empty());

    REQUIRE(delta_a.hash() == delta_b.hash());
    REQUIRE(delta_a.equals(delta_b));
    REQUIRE(std::is_eq(delta_a.compare(delta_b)));
    REQUIRE_FALSE(delta_a.equals(delta_different));
    REQUIRE(delta_a.compare(delta_different) == std::partial_ordering::unordered);
    REQUIRE_FALSE(delta_a.to_string().empty());

    constexpr auto semantic_capabilities = TypeCapabilities::Hashable |
                                           TypeCapabilities::Equatable |
                                           TypeCapabilities::Comparable;
    const auto require_canonical_capabilities = [&](const ValueView &projected) {
        const auto canonical = ValuePlanFactory::instance().type_for(projected.schema());
        REQUIRE(canonical);
        REQUIRE((projected.binding().capabilities() & semantic_capabilities) ==
                (canonical.capabilities() & semantic_capabilities));
    };
    require_canonical_capabilities(map_a);
    require_canonical_capabilities(keys_a);
    require_canonical_capabilities(delta_a);
}

TEST_CASE("TSDProxy non-atomic maps project child value and delta storage")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *source_schema = registry.tsd(int_meta, ts_int);
    const auto *window_schema = registry.tsw(int_meta, 1, 1);
    const auto *proxy_schema = registry.tsd(int_meta, window_schema);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(source_schema);
    const auto window_type = TSDataPlanFactory::instance().data_type_for(window_schema);
    REQUIRE(window_type);

    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*proxy_schema, TSRoleTypeRef{window_type.as_role()})};
    Value key{7};
    const auto t1 = MIN_ST + TimeDelta{1};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        Value source_value{70};
        auto child_mutation = child.begin_mutation(t1);
        REQUIRE(child_mutation.copy_value_from(source_value.view()));
    }
    {
        auto proxy_view = proxy.view();
        auto source_view = source.view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &window_key_ops, nullptr, t1);
    }

    auto proxy_view = proxy.view();
    auto dict = proxy_view.as_dict();
    auto live = dict.value();
    auto delta = dict.delta_value(t1).as_bundle();
    auto modified = delta.at(1);
    const auto *live_ops = checked_value_ops<MapValueOps>(live.binding(), "TSDProxy live map test");
    const auto *modified_ops = checked_value_ops<MapValueOps>(modified.binding(), "TSDProxy delta map test");
    const auto live_type = live_ops->value_binding(live_ops->context, live.data());
    const auto modified_type = modified_ops->value_binding(modified_ops->context, modified.data());
    REQUIRE(live_type.schema() == window_schema->value_schema);
    REQUIRE(modified_type.schema() == window_schema->delta_value_schema);
    REQUIRE(live_type != modified_type);

    const auto *live_keyed = live_ops->value_at(live_ops->context, live.data(), key.view().data());
    const auto *live_indexed = live_ops->value_at_index(live_ops->context, live.data(), 0);
    REQUIRE(live_keyed != nullptr);
    REQUIRE(live_indexed == live_keyed);
    auto live_direct = ValueView{live_type, live_keyed}.as_list();
    REQUIRE(live_direct.size() == 1);
    REQUIRE(live_direct.at(0).checked_as<std::int32_t>() == 7);
    auto live_values = live_ops->make_values_range(live_ops->context, live.data());
    auto live_projected = *live_values.begin();
    REQUIRE(live_projected.binding() == live_type);
    REQUIRE(live_projected.data() == live_keyed);

    const auto *modified_keyed = modified_ops->value_at(modified_ops->context, modified.data(), key.view().data());
    const auto *modified_indexed = modified_ops->value_at_index(modified_ops->context, modified.data(), 0);
    REQUIRE(modified_keyed != nullptr);
    REQUIRE(modified_indexed == modified_keyed);
    REQUIRE(ValueView{modified_type, modified_keyed}.checked_as<std::int32_t>() == 7);
    auto modified_values = modified_ops->make_values_range(modified_ops->context, modified.data());
    auto modified_projected = *modified_values.begin();
    REQUIRE(modified_projected.binding() == modified_type);
    REQUIRE(modified_projected.data() == modified_keyed);

    Value live_owned{live};
    Value modified_owned{modified};
    CAPTURE(live.to_string(), live_owned.to_string(), live.hash(), live_owned.view().hash());
    REQUIRE(live.hash() == live_owned.view().hash());
    REQUIRE(live.equals(live_owned.view()));
    REQUIRE(std::is_eq(live.compare(live_owned.view())));
    REQUIRE(live.to_string() == live_owned.to_string());
    REQUIRE(modified.hash() == modified_owned.view().hash());
    REQUIRE(modified.equals(modified_owned.view()));
    REQUIRE(std::is_eq(modified.compare(modified_owned.view())));
    REQUIRE(modified.to_string() == modified_owned.to_string());
}

TEST_CASE("TSDProxy non-atomic owning copy preserves nested typed holes")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int_meta);
    const auto *source_schema = registry.tsd(int_meta, ts_int);
    const auto *bundle_schema = registry.tsb("TSDProxySparseBundle", {{"present", ts_int}, {"hole", ts_int}});
    const auto *proxy_schema = registry.tsd(int_meta, bundle_schema);
    const auto source_type = TSDataPlanFactory::instance().data_type_for(source_schema);
    const auto bundle_type = TSDataPlanFactory::instance().data_type_for(bundle_schema);

    TSData source{source_type};
    TSData proxy{proxy_data_type_for(*proxy_schema, TSRoleTypeRef{bundle_type.as_role()})};
    Value key{9};
    {
        auto source_view = source.view();
        auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
        auto child = mutation.at(key.view());
        Value source_value{90};
        auto child_mutation = child.begin_mutation(MIN_ST);
        REQUIRE(child_mutation.copy_value_from(source_value.view()));
    }
    {
        auto proxy_view = proxy.view();
        auto source_view = source.view();
        bind_tsd_proxy(proxy_view, source_view.as_dict(), &sparse_bundle_key_ops, nullptr, MIN_ST);
    }

    auto proxy_view = proxy.view();
    auto live = proxy_view.as_dict().value();
    auto sparse = live.as_map().at(key.view()).as_bundle();
    REQUIRE(sparse.size() == 2);
    REQUIRE(sparse.at(0).checked_as<std::int32_t>() == 9);
    REQUIRE(sparse.at(1).bound());
    REQUIRE_FALSE(sparse.at(1).has_value());

    Value owned{live};
    auto owned_sparse = owned.view().as_map().at(key.view()).as_bundle();
    REQUIRE(owned_sparse.size() == 2);
    REQUIRE(owned_sparse.at(0).checked_as<std::int32_t>() == 9);
    REQUIRE(owned_sparse.at(1).bound());
    REQUIRE_FALSE(owned_sparse.at(1).has_value());
    REQUIRE(live.equals(owned.view()));
    REQUIRE(live.hash() == owned.view().hash());
}
