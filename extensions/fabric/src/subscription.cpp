#include "impl/subscription_runtime.h"

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/types/metadata/type_registry.h>

#include <algorithm>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <utility>

namespace hgraph::fabric::detail
{
    namespace
    {
        struct IdLess
        {
            using is_transparent = void;

            [[nodiscard]] bool operator()(std::string_view lhs,
                                          std::string_view rhs) const noexcept
            {
                return canonical_data_id_less(lhs, rhs);
            }
        };

        struct IngressRootUpdate
        {
            Str                  data_id{};
            RevisionId           revision{};
            DataVersion          output_version{};
            std::optional<Frame> frame{};
        };

        struct IngressBatch
        {
            std::vector<IngressRootUpdate> roots{};
        };

        struct RuntimeCore
        {
            FabricConfig                              config;
            std::vector<Str>                          roots;
            ConsistencyCoordinator                    coordinator;
            std::map<Str, RevisionId, IdLess>         emitted_revisions{};
            std::set<Str, IdLess>                     observed{};

            RuntimeCore(FabricConfig configured, std::vector<Str> configured_roots)
                : config(std::move(configured)), roots(std::move(configured_roots)),
                  coordinator(config, roots), observed(roots.begin(), roots.end())
            {
            }

            void observe(const CoordinationResult &result)
            {
                for (const auto &forest : result.forests)
                {
                    observed.insert(forest.observed_data_ids.begin(),
                                    forest.observed_data_ids.end());
                }
            }

            [[nodiscard]] std::optional<IngressBatch>
            batch(CoordinationResult result)
            {
                for (const auto &forest : result.forests)
                {
                    if (forest.status == ResolutionStatus::Corrupt ||
                        forest.status == ResolutionStatus::Ambiguous ||
                        forest.status == ResolutionStatus::Cyclic)
                    {
                        throw std::runtime_error(
                            "fabric subscription consistency failure for root '" +
                            forest.roots.front() + "': " +
                            std::string{enum_name(forest.status)} + ": " +
                            forest.diagnostic);
                    }
                }
                observe(result);
                std::map<Str, Frame, IdLess> changed;
                for (auto &root : result.changed_roots)
                {
                    changed.emplace(root.data_id, std::move(root.frame));
                }

                IngressBatch batch;
                for (const auto &root : roots)
                {
                    const auto selected = std::ranges::lower_bound(
                        result.committed_lineage, root, canonical_data_id_less,
                        &ResolvedRevision::data_id);
                    if (selected == result.committed_lineage.end() ||
                        selected->data_id != root)
                    {
                        continue;
                    }
                    const auto prior = emitted_revisions.find(root);
                    if (prior != emitted_revisions.end() &&
                        prior->second == selected->revision)
                    {
                        continue;
                    }

                    std::optional<Frame> frame;
                    if (auto value = changed.find(root); value != changed.end())
                    {
                        frame = std::move(value->second);
                    }
                    batch.roots.push_back(IngressRootUpdate{
                        .data_id = root,
                        .revision = selected->revision,
                        .output_version = selected->output_version,
                        .frame = std::move(frame),
                    });
                    emitted_revisions[root] = selected->revision;
                }
                if (batch.roots.empty()) { return std::nullopt; }
                return batch;
            }
        };

        struct StrategyOps
        {
            void (*start)(void *context, std::weak_ptr<IngressBridge> bridge);
            std::optional<IngressBatch> (*evaluate)(void *context, DateTime now,
                                                    NodeScheduler scheduler);
            void (*stop)(void *context) noexcept;
            void (*destroy)(void *context) noexcept;
        };

        void empty_start(void *, std::weak_ptr<IngressBridge>) {}
        std::optional<IngressBatch> empty_evaluate(void *, DateTime,
                                                   NodeScheduler)
        {
            return std::nullopt;
        }
        void empty_stop(void *) noexcept {}
        void empty_destroy(void *) noexcept {}

        [[nodiscard]] const StrategyOps &empty_strategy_ops() noexcept
        {
            static const StrategyOps ops{
                &empty_start, &empty_evaluate, &empty_stop, &empty_destroy};
            return ops;
        }

        class ErasedStrategy final
        {
          public:
            ErasedStrategy() noexcept = default;

            template <typename T>
            explicit ErasedStrategy(std::unique_ptr<T> value)
                : context_(value.release()), ops_(&ops_for<T>())
            {
            }

            ErasedStrategy(const ErasedStrategy &) = delete;
            ErasedStrategy &operator=(const ErasedStrategy &) = delete;

            ErasedStrategy(ErasedStrategy &&other) noexcept
                : context_(std::exchange(other.context_, nullptr)),
                  ops_(std::exchange(other.ops_, &empty_strategy_ops()))
            {
            }

            ErasedStrategy &operator=(ErasedStrategy &&other) noexcept
            {
                if (this != &other)
                {
                    reset();
                    context_ = std::exchange(other.context_, nullptr);
                    ops_ = std::exchange(other.ops_, &empty_strategy_ops());
                }
                return *this;
            }

            ~ErasedStrategy() { reset(); }

            void start(std::weak_ptr<IngressBridge> bridge) const
            {
                ops_->start(context_, std::move(bridge));
            }

            [[nodiscard]] std::optional<IngressBatch>
            evaluate(DateTime now, NodeScheduler scheduler) const
            {
                return ops_->evaluate(context_, now, scheduler);
            }

            void stop() const noexcept { ops_->stop(context_); }

            void reset() noexcept
            {
                ops_->destroy(context_);
                context_ = nullptr;
                ops_ = &empty_strategy_ops();
            }

          private:
            template <typename T>
            [[nodiscard]] static const StrategyOps &ops_for() noexcept
            {
                static const StrategyOps ops{
                    [](void *context, std::weak_ptr<IngressBridge> bridge) {
                        static_cast<T *>(context)->start(std::move(bridge));
                    },
                    [](void *context, DateTime now, NodeScheduler scheduler) {
                        return static_cast<T *>(context)->evaluate(now, scheduler);
                    },
                    [](void *context) noexcept {
                        static_cast<T *>(context)->stop();
                    },
                    [](void *context) noexcept { delete static_cast<T *>(context); },
                };
                return ops;
            }

            void              *context_{};
            const StrategyOps *ops_{&empty_strategy_ops()};
        };

        struct SnapshotStrategy
        {
            RuntimeCore core;
            DateTime    as_of;
            bool        complete{};

            SnapshotStrategy(FabricConfig config, std::vector<Str> roots,
                             DateTime configured_as_of)
                : core(std::move(config), std::move(roots)),
                  as_of(configured_as_of)
            {
            }

            void start(std::weak_ptr<IngressBridge>) {}

            [[nodiscard]] std::optional<IngressBatch>
            evaluate(DateTime, NodeScheduler)
            {
                if (complete) { return std::nullopt; }
                complete = true;
                return core.batch(core.coordinator.resolve_at(as_of));
            }

            void stop() noexcept { complete = true; }
        };

        struct ReplayStrategy
        {
            RuntimeCore                              core;
            DateTime                                 end_time;
            std::map<Str, std::vector<DateTime>, IdLess> histories{};

            ReplayStrategy(FabricConfig config, std::vector<Str> roots,
                           DateTime configured_end)
                : core(std::move(config), std::move(roots)),
                  end_time(configured_end)
            {
            }

            void start(std::weak_ptr<IngressBridge>) {}

            [[nodiscard]] const std::vector<DateTime> &history(const Str &data_id)
            {
                auto [entry, inserted] = histories.try_emplace(data_id);
                if (!inserted) { return entry->second; }

                const std::string category = as_of_key_prefix(
                    core.config.prefix, data_id);
                const std::string prefix = category + "/";
                std::optional<std::string> cursor;
                for (;;)
                {
                    const auto page = core.config.objects.list(
                        prefix,
                        cursor.has_value()
                            ? std::optional<std::string_view>{*cursor}
                            : std::nullopt,
                        1000);
                    for (const auto &object : page.objects)
                    {
                        if (!object.key.starts_with(prefix))
                        {
                            throw std::runtime_error(
                                "fabric as-of listing escaped its data id");
                        }
                        const auto encoded = std::string_view{object.key}.substr(
                            prefix.size());
                        entry->second.emplace_back(TimeDelta{
                            decode_fabric_ordinal(encoded)});
                    }
                    if (!page.next_start_after.has_value()) { break; }
                    cursor = page.next_start_after;
                }
                return entry->second;
            }

            [[nodiscard]] std::optional<DateTime> next_after(DateTime now)
            {
                std::optional<DateTime> next;
                for (const auto &data_id : core.observed)
                {
                    const auto &times = history(data_id);
                    const auto found = std::ranges::upper_bound(times, now);
                    if (found == times.end() || *found >= end_time) { continue; }
                    if (!next.has_value() || *found < *next) { next = *found; }
                }
                return next;
            }

            [[nodiscard]] std::optional<IngressBatch>
            evaluate(DateTime now, NodeScheduler scheduler)
            {
                if (now >= end_time) { return std::nullopt; }
                auto result = core.coordinator.resolve_at(now);
                core.observe(result);
                if (const auto next = next_after(now); next.has_value())
                {
                    scheduler.schedule(*next);
                }
                return core.batch(std::move(result));
            }

            void stop() noexcept {}
        };

        struct LiveStrategy
        {
            struct PendingProposal
            {
                RevisionId                           revision{};
                DateTime                             noticed_at{MIN_DT};
                DateTime                             next_attempt{MIN_DT};
                TimeDelta                            backoff{std::chrono::milliseconds{1}};
            };

            static constexpr TimeDelta MAX_BACKOFF{
                std::chrono::seconds{1}};
            static constexpr std::string_view RETRY_TAG{
                "fabric-storage-retry"};

            RuntimeCore                              core;
            NotificationSubscription                 subscription{};
            std::map<Str, PendingProposal, IdLess>   pending{};
            bool                                     startup{true};
            bool                                     wall_clock{};

            LiveStrategy(FabricConfig config, std::vector<Str> roots,
                         bool use_wall_clock)
                : core(std::move(config), std::move(roots)),
                  wall_clock(use_wall_clock)
            {
            }

            void start(std::weak_ptr<IngressBridge> bridge)
            {
                subscription = core.config.notifications.subscribe();
                subscription.set_waker([bridge = std::move(bridge)] {
                    if (const auto owner = bridge.lock()) { owner->wake(); }
                });
            }

            void drain(DateTime now)
            {
                while (auto notification = subscription.try_pop())
                {
                    if (!core.observed.contains(notification->data_id))
                    {
                        continue;
                    }
                    const Value decoded = decode_revision(notification->revision);
                    const DataRevisionInput revision =
                        data_revision_input(decoded.view());
                    auto found = pending.find(notification->data_id);
                    if (found != pending.end() &&
                        found->second.revision > revision.revision)
                    {
                        continue;
                    }
                    pending[notification->data_id] = PendingProposal{
                        .revision = revision.revision,
                        .noticed_at = now,
                        .next_attempt = now,
                    };
                }
            }

            [[nodiscard]] bool admit(DateTime now)
            {
                bool admitted{};
                for (auto item = pending.begin(); item != pending.end();)
                {
                    if (item->second.next_attempt > now)
                    {
                        ++item;
                        continue;
                    }
                    const auto slot = core.config.objects.get(revision_key(
                        core.config.prefix, item->first,
                        item->second.revision));
                    if (!slot.has_value())
                    {
                        item->second.next_attempt = now + item->second.backoff;
                        item->second.backoff = std::min(
                            item->second.backoff * 2, MAX_BACKOFF);
                        ++item;
                        continue;
                    }

                    // Decode the durable winner. A different proposal payload is
                    // deliberately ignored: immutable persistence is authoritative.
                    const Value winner = decode_revision(slot->data);
                    const DataRevisionInput accepted =
                        data_revision_input(winner.view());
                    if (accepted.data_id != item->first ||
                        accepted.revision != item->second.revision)
                    {
                        throw std::runtime_error(
                            "fabric notification references a malformed durable slot");
                    }
                    core.coordinator.observe_notice(item->first,
                                                    item->second.noticed_at);
                    item = pending.erase(item);
                    admitted = true;
                }
                return admitted;
            }

            void schedule_retry(DateTime now, NodeScheduler scheduler)
            {
                std::optional<DateTime> next;
                for (const auto &[data_id, proposal] : pending)
                {
                    static_cast<void>(data_id);
                    if (!next.has_value() || proposal.next_attempt < *next)
                    {
                        next = proposal.next_attempt;
                    }
                }
                if (!next.has_value())
                {
                    if (scheduler.has_tag(RETRY_TAG))
                    {
                        scheduler.un_schedule(std::string{RETRY_TAG});
                    }
                    return;
                }

                if (scheduler.tag_is_scheduled_now(std::string{RETRY_TAG}))
                {
                    static_cast<void>(
                        scheduler.pop_tag(std::string{RETRY_TAG}));
                }
                if (!scheduler.has_tag(RETRY_TAG) ||
                    *next < scheduler.tag_time(RETRY_TAG))
                {
                    const DateTime target = std::max(*next, now + MIN_TD);
                    scheduler.schedule(target, std::string{RETRY_TAG},
                                       wall_clock);
                }
            }

            [[nodiscard]] std::optional<IngressBatch>
            evaluate(DateTime now, NodeScheduler scheduler)
            {
                std::optional<IngressBatch> batch;
                if (startup)
                {
                    // Establish the durable image and therefore its dynamic
                    // ancestry before filtering notices accumulated during
                    // startup. This is the in-process form of the RFC no-gap
                    // subscription-before-image handoff.
                    batch = core.batch(core.coordinator.resolve(now));
                }
                drain(now);
                const bool admitted = admit(now);

                if (admitted)
                {
                    if (auto update = core.batch(core.coordinator.resolve(now));
                        update.has_value())
                    {
                        if (!batch.has_value()) { batch.emplace(); }
                        batch->roots.insert(
                            batch->roots.end(),
                            std::make_move_iterator(update->roots.begin()),
                            std::make_move_iterator(update->roots.end()));
                    }
                }
                startup = false;
                schedule_retry(now, scheduler);
                return batch;
            }

            void stop() noexcept
            {
                subscription.close();
                pending.clear();
            }
        };

        [[nodiscard]] ErasedStrategy make_strategy(
            SubscriptionMode mode, DateTime as_of, EngineControlView engine,
            FabricConfig config, const std::vector<Str> &roots)
        {
            switch (mode)
            {
                case SubscriptionMode::Snapshot:
                    return ErasedStrategy{std::make_unique<SnapshotStrategy>(
                        std::move(config), roots, as_of)};
                case SubscriptionMode::Replay:
                    return ErasedStrategy{std::make_unique<ReplayStrategy>(
                        std::move(config), roots, engine.end_time())};
                case SubscriptionMode::Live:
                    return ErasedStrategy{std::make_unique<LiveStrategy>(
                        std::move(config), roots,
                        engine.mode() == GraphExecutorMode::RealTime)};
                case SubscriptionMode::Auto:
                    break;
            }
            throw std::invalid_argument(
                "fabric ingress strategy must be resolved before construction");
        }
    }  // namespace

    struct IngressBridge::Impl
    {
        explicit Impl(std::vector<Str> configured_roots)
            : roots(std::move(configured_roots))
        {
            std::ranges::sort(roots, canonical_data_id_less);
            roots.erase(std::ranges::unique(roots).begin(), roots.end());
            if (roots.empty())
            {
                throw std::invalid_argument(
                    "fabric ingress bridge requires at least one root");
            }
            for (const auto &root : roots) { require_data_id(root); }
        }

        std::vector<Str> roots{};
        std::mutex mutex{};
        AsyncNodeWakeSender wake_sender{};
        bool active{};
        ErasedStrategy strategy{};
    };

    IngressBridge::IngressBridge(std::vector<Str> roots)
        : impl_(std::make_unique<Impl>(std::move(roots)))
    {
    }

    IngressBridge::~IngressBridge() { stop(); }

    void IngressBridge::start(SubscriptionMode requested_mode, DateTime as_of,
                              EngineControlView engine,
                              GlobalStateView global_state,
                              AsyncNodeWakeSender wake_sender)
    {
        const auto configured = fabric_config(global_state);
        if (!configured.has_value())
        {
            throw std::logic_error(
                "hgraph.fabric.subscribe_data requires FabricConfig in GlobalState");
        }
        SubscriptionMode resolved = requested_mode;
        if (resolved == SubscriptionMode::Auto)
        {
            resolved = engine.mode() == GraphExecutorMode::RealTime
                           ? configured->default_real_time
                           : configured->default_simulation;
        }
        {
            const std::scoped_lock lock{impl_->mutex};
            if (resolved == SubscriptionMode::Live && !wake_sender.valid())
            {
                throw std::logic_error(
                    "fabric ingress requires a live async node wake sender");
            }
            impl_->wake_sender = wake_sender;
            impl_->active = resolved == SubscriptionMode::Live;
        }
        impl_->strategy = make_strategy(resolved, as_of, engine, *configured,
                                        impl_->roots);
        impl_->strategy.start(weak_from_this());
    }

    void IngressBridge::evaluate(DateTime now, NodeScheduler scheduler,
                                 Out<IngressSignals> &out)
    {
        const auto batch = impl_->strategy.evaluate(now, scheduler);
        if (!batch.has_value()) { return; }
        for (const auto &root : batch->roots)
        {
            auto signal = out[root.data_id];
            if (root.frame.has_value())
            {
                signal.template field<"frame">().set(*root.frame);
            }
            signal.template field<"version">().set(root.output_version);
            signal.template field<"revision">().set(root.revision);
        }
    }

    void IngressBridge::stop() noexcept
    {
        if (!impl_) { return; }
        {
            const std::scoped_lock lock{impl_->mutex};
            impl_->active = false;
        }
        impl_->strategy.stop();
        impl_->strategy.reset();
        const std::scoped_lock lock{impl_->mutex};
        impl_->wake_sender = {};
    }

    void IngressBridge::wake()
    {
        AsyncNodeWakeSender sender;
        {
            const std::scoped_lock lock{impl_->mutex};
            if (!impl_->active || !impl_->wake_sender.valid()) { return; }
            sender = impl_->wake_sender;
        }
        sender.wake();
    }

    Port<IngressSignals> wire_ingress_group(Wiring &wiring,
                                             std::vector<Str> roots,
                                             SubscriptionMode mode,
                                             DateTime as_of)
    {
        IngressBridgeHandle bridge{
            std::make_shared<IngressBridge>(std::move(roots))};

        switch (mode)
        {
            case SubscriptionMode::Auto:
                return wire<IngressCoordinatorNode<SubscriptionMode::Auto>>(
                    wiring, bridge, as_of);
            case SubscriptionMode::Live:
                return wire<IngressCoordinatorNode<SubscriptionMode::Live>>(
                    wiring, bridge, as_of);
            case SubscriptionMode::Replay:
                return wire<IngressCoordinatorNode<SubscriptionMode::Replay>>(
                    wiring, bridge, as_of);
            case SubscriptionMode::Snapshot:
                return wire<IngressCoordinatorNode<SubscriptionMode::Snapshot>>(
                    wiring, bridge, as_of);
        }
        throw std::invalid_argument("unsupported fabric subscription mode");
    }
}  // namespace hgraph::fabric::detail
