#include "impl/service_runtime.h"

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/runtime/evaluation_clock.h>

#include <algorithm>
#include <charconv>
#include <deque>
#include <iterator>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::fabric::detail
{
namespace
{
inline constexpr char KEY_SEPARATOR{'\n'};

struct IdLess
{
    using is_transparent = void;

    [[nodiscard]] bool operator()(std::string_view lhs, std::string_view rhs) const noexcept
    {
        return canonical_data_id_less(lhs, rhs);
    }
};

[[nodiscard]] DateTime wall_time() noexcept
{
    return std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
}

[[nodiscard]] Int parse_integer(std::string_view text, std::string_view subject)
{
    Int value{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size())
    {
        throw std::invalid_argument("invalid fabric " + std::string{subject});
    }
    return value;
}

[[nodiscard]] std::pair<Str, Int> split_key(std::string_view encoded, std::string_view subject)
{
    const auto separator = encoded.rfind(KEY_SEPARATOR);
    if (separator == std::string_view::npos)
    {
        throw std::invalid_argument("invalid fabric " + std::string{subject});
    }
    Str data_id{encoded.substr(0, separator)};
    require_data_id(data_id);
    return {std::move(data_id), parse_integer(encoded.substr(separator + 1), subject)};
}

[[nodiscard]] std::vector<Str> roots_of(const std::vector<SubscriptionSpec> &subscriptions)
{
    std::vector<Str> roots;
    roots.reserve(subscriptions.size());
    for (const auto &subscription : subscriptions)
    {
        roots.push_back(subscription.data_id);
    }
    std::ranges::sort(roots, canonical_data_id_less);
    roots.erase(std::ranges::unique(roots).begin(), roots.end());
    return roots;
}

[[nodiscard]] std::vector<SubscriptionSpec> canonical_subscriptions(std::vector<SubscriptionSpec> subscriptions)
{
    std::ranges::sort(subscriptions,
                      [](const SubscriptionSpec &lhs, const SubscriptionSpec &rhs) { return lhs.key < rhs.key; });
    subscriptions.erase(std::ranges::unique(subscriptions, {}, &SubscriptionSpec::key).begin(), subscriptions.end());
    return subscriptions;
}

struct RootBatch
{
    std::vector<RootUpdate> changed{};
    std::vector<ResolvedRevision> selected{};
};

struct RuntimeCore
{
    FabricConfig config;
    std::vector<Str> roots;
    ConsistencyCoordinator coordinator;
    std::map<Str, RevisionId, IdLess> emitted_revisions{};
    std::set<Str, IdLess> observed{};

    RuntimeCore(FabricConfig configured, std::vector<Str> configured_roots)
        : config(std::move(configured)), roots(std::move(configured_roots)), coordinator(config, roots),
          observed(roots.begin(), roots.end())
    {
    }

    void observe(const CoordinationResult &result)
    {
        for (const auto &forest : result.forests)
        {
            observed.insert(forest.observed_data_ids.begin(), forest.observed_data_ids.end());
            if (forest.cut.has_value())
            {
                for (const auto &revision : forest.cut->revisions)
                {
                    observed.insert(revision.data_id);
                }
            }
        }
    }

    [[nodiscard]] std::optional<RootBatch> batch(CoordinationResult result)
    {
        for (const auto &forest : result.forests)
        {
            if (forest.status == ResolutionStatus::Corrupt || forest.status == ResolutionStatus::Ambiguous ||
                forest.status == ResolutionStatus::Cyclic)
            {
                throw std::runtime_error("fabric subscription consistency failure for root '" + forest.roots.front() +
                                         "': " + std::string{enum_name(forest.status)} + ": " + forest.diagnostic);
            }
        }
        observe(result);

        RootBatch batch;
        batch.changed = std::move(result.changed_roots);
        for (const auto &root : roots)
        {
            const auto selected = std::ranges::lower_bound(result.committed_lineage, root, canonical_data_id_less,
                                                           &ResolvedRevision::data_id);
            if (selected == result.committed_lineage.end() || selected->data_id != root)
            {
                continue;
            }
            const auto prior = emitted_revisions.find(root);
            if (prior != emitted_revisions.end() && prior->second == selected->revision)
            {
                continue;
            }
            batch.selected.push_back(*selected);
            emitted_revisions[root] = selected->revision;
        }
        if (batch.selected.empty())
        {
            return std::nullopt;
        }
        return batch;
    }
};

[[nodiscard]] std::optional<DeliveryBatch> deliver(RootBatch batch, const std::vector<SubscriptionSpec> &subscriptions)
{
    std::map<Str, Frame, IdLess> frames;
    for (auto &root : batch.changed)
    {
        frames.emplace(root.data_id, std::move(root.frame));
    }

    DeliveryBatch delivery;
    for (const auto &selected : batch.selected)
    {
        for (const auto &subscription : subscriptions)
        {
            if (subscription.data_id != selected.data_id)
            {
                continue;
            }
            std::optional<Frame> frame;
            if (const auto found = frames.find(selected.data_id); found != frames.end())
            {
                frame = found->second;
            }
            delivery.roots.push_back(DeliveredRoot{
                .key = subscription.key,
                .data_id = subscription.data_id,
                .revision = selected.revision,
                .output_version = selected.output_version,
                .frame = std::move(frame),
            });
        }
    }
    if (delivery.roots.empty())
    {
        return std::nullopt;
    }
    return delivery;
}

struct SnapshotGroup
{
    std::vector<SubscriptionSpec> subscriptions{};
    std::unique_ptr<RuntimeCore> core{};
    bool complete{};
};

struct SnapshotSession
{
    std::vector<SubscriptionSpec> subscriptions{};
    std::map<DateTime, SnapshotGroup> groups{};

    void configure(const FabricConfig &config, std::vector<SubscriptionSpec> requested)
    {
        requested = canonical_subscriptions(std::move(requested));
        if (requested == subscriptions)
        {
            return;
        }
        subscriptions = std::move(requested);
        groups.clear();
        for (const auto &subscription : subscriptions)
        {
            auto &group = groups[subscription.as_of];
            group.subscriptions.push_back(subscription);
        }
        for (auto &[as_of, group] : groups)
        {
            static_cast<void>(as_of);
            group.core = std::make_unique<RuntimeCore>(config, roots_of(group.subscriptions));
        }
    }

    [[nodiscard]] std::optional<DeliveryBatch> evaluate()
    {
        DeliveryBatch combined;
        for (auto &[as_of, group] : groups)
        {
            if (group.complete)
            {
                continue;
            }
            group.complete = true;
            auto batch = group.core->batch(group.core->coordinator.resolve_at(as_of));
            if (!batch.has_value())
            {
                continue;
            }
            auto delivery = deliver(std::move(*batch), group.subscriptions);
            if (!delivery.has_value())
            {
                continue;
            }
            combined.roots.insert(combined.roots.end(), std::make_move_iterator(delivery->roots.begin()),
                                  std::make_move_iterator(delivery->roots.end()));
        }
        if (combined.roots.empty())
        {
            return std::nullopt;
        }
        return combined;
    }
};

struct ReplaySession
{
    FabricConfig config{};
    std::vector<SubscriptionSpec> subscriptions{};
    std::unique_ptr<RuntimeCore> core{};
    DateTime start_time{MIN_DT};
    DateTime end_time{MAX_DT};
    std::optional<DateTime> cursor{};
    std::map<Str, std::vector<DateTime>, IdLess> histories{};

    void configure(const FabricConfig &configured, std::vector<SubscriptionSpec> requested)
    {
        requested = canonical_subscriptions(std::move(requested));
        if (requested == subscriptions && core)
        {
            return;
        }
        config = configured;
        subscriptions = std::move(requested);
        histories.clear();
        core = subscriptions.empty() ? nullptr : std::make_unique<RuntimeCore>(config, roots_of(subscriptions));
        cursor = core ? std::optional<DateTime>{start_time} : std::nullopt;
    }

    [[nodiscard]] const std::vector<DateTime> &history(const Str &data_id)
    {
        auto [entry, inserted] = histories.try_emplace(data_id);
        if (!inserted)
        {
            return entry->second;
        }

        const std::string category = as_of_key_prefix(config.prefix, data_id);
        const std::string prefix = category + "/";
        std::optional<std::string> cursor;
        for (;;)
        {
            const auto page = config.objects.list(
                prefix, cursor.has_value() ? std::optional<std::string_view>{*cursor} : std::nullopt, 1000);
            for (const auto &object : page.objects)
            {
                if (!object.key.starts_with(prefix))
                {
                    throw std::runtime_error("fabric as-of listing escaped its data id");
                }
                entry->second.emplace_back(
                    TimeDelta{decode_fabric_ordinal(std::string_view{object.key}.substr(prefix.size()))});
            }
            if (!page.next_start_after.has_value())
            {
                break;
            }
            cursor = page.next_start_after;
        }
        return entry->second;
    }

    [[nodiscard]] std::optional<DateTime> next_after(DateTime now)
    {
        std::optional<DateTime> next;
        for (const auto &data_id : core->observed)
        {
            const auto &times = history(data_id);
            const auto found = std::ranges::upper_bound(times, now);
            if (found == times.end() || *found >= end_time)
            {
                continue;
            }
            if (!next.has_value() || *found < *next)
            {
                next = *found;
            }
        }
        return next;
    }

    [[nodiscard]] std::optional<DeliveryBatch> evaluate(DateTime now, NodeScheduler scheduler)
    {
        if (!core)
        {
            return std::nullopt;
        }
        const DateTime logical_time = cursor.value_or(now);
        if (logical_time >= end_time)
        {
            return std::nullopt;
        }
        auto result = core->coordinator.resolve_at(logical_time);
        core->observe(result);
        cursor = next_after(logical_time);
        if (cursor.has_value())
        {
            if (*cursor > now)
            {
                scheduler.schedule(*cursor);
            }
            else
            {
                scheduler.schedule(MIN_TD);
            }
        }
        auto batch = core->batch(std::move(result));
        return batch.has_value() ? deliver(std::move(*batch), subscriptions) : std::nullopt;
    }
};

struct LiveSession
{
    struct CachedNotice
    {
        DataRevisionInput revision{};
        DateTime noticed_at{MIN_DT};
    };

    FabricConfig config{};
    std::vector<SubscriptionSpec> subscriptions{};
    std::unique_ptr<RuntimeCore> core{};
    std::map<Str, CachedNotice, IdLess> notice_cache{};
    std::map<Str, RevisionId, IdLess> admitted{};
    bool startup{true};

    void configure(const FabricConfig &configured, std::vector<SubscriptionSpec> requested)
    {
        requested = canonical_subscriptions(std::move(requested));
        if (requested == subscriptions && core)
        {
            return;
        }
        config = configured;
        subscriptions = std::move(requested);
        core = subscriptions.empty() ? nullptr : std::make_unique<RuntimeCore>(config, roots_of(subscriptions));
        admitted.clear();
        startup = true;
    }

    [[nodiscard]] std::optional<DeliveryBatch> evaluate(std::vector<DataRevisionInput> revisions, DateTime now)
    {
        DeliveryBatch combined;
        for (auto &revision : revisions)
        {
            auto found = notice_cache.find(revision.data_id);
            if (found == notice_cache.end())
            {
                if (notice_cache.size() >= 4096)
                {
                    throw std::overflow_error("fabric live notice cache is full");
                }
                Str data_id = revision.data_id;
                notice_cache.emplace(std::move(data_id), CachedNotice{std::move(revision), now});
            }
            else if (found->second.revision.revision < revision.revision)
            {
                found->second = CachedNotice{std::move(revision), now};
            }
            else if (found->second.revision.revision == revision.revision && found->second.revision != revision)
            {
                throw std::runtime_error("fabric live notice conflicts with its cached revision");
            }
        }

        if (!core)
        {
            return std::nullopt;
        }
        if (startup)
        {
            auto initial = core->batch(core->coordinator.resolve(now));
            if (initial.has_value())
            {
                if (auto values = deliver(std::move(*initial), subscriptions); values.has_value())
                {
                    combined = std::move(*values);
                }
            }
            startup = false;
        }

        for (;;)
        {
            bool admitted_any{};
            for (;;)
            {
                bool admitted_this_pass{};
                for (const auto &[data_id, cached] : notice_cache)
                {
                    if (!core->observed.contains(data_id))
                    {
                        continue;
                    }
                    const auto prior = admitted.find(data_id);
                    if (prior != admitted.end() && prior->second >= cached.revision.revision)
                    {
                        continue;
                    }
                    core->coordinator.observe_accepted_revision(cached.revision);
                    core->coordinator.observe_notice(data_id, cached.noticed_at);
                    admitted[data_id] = cached.revision.revision;
                    for (const auto &dependency : cached.revision.dependencies)
                    {
                        core->observed.insert(dependency.data_id);
                    }
                    admitted_this_pass = true;
                    admitted_any = true;
                }
                if (!admitted_this_pass)
                {
                    break;
                }
            }
            if (!admitted_any)
            {
                break;
            }
            auto update = core->batch(core->coordinator.resolve_cached(now));
            if (update.has_value())
            {
                if (auto values = deliver(std::move(*update), subscriptions); values.has_value())
                {
                    combined.roots.insert(combined.roots.end(), std::make_move_iterator(values->roots.begin()),
                                          std::make_move_iterator(values->roots.end()));
                }
            }
        }
        if (combined.roots.empty())
        {
            return std::nullopt;
        }
        return combined;
    }
};
} // namespace

struct FabricServiceRuntime::Impl
{
    FabricServicePlanHandle plan{};
    std::optional<FabricConfig> config{};
    SnapshotSession snapshot{};
    SnapshotSession planned_snapshot{};
    ReplaySession replay{};
    ReplaySession planned_replay{};
    LiveSession live{};
    LiveSession planned_live{};
    std::map<Str, std::unique_ptr<PublisherStateMachine>, IdLess> publishers{};
    std::map<Str, std::deque<PublicationRequestInput>, IdLess> publication_queues{};
    std::map<Str, RevisionId, IdLess> advertised{};
    bool running{};

    [[nodiscard]] FabricConfig &configured()
    {
        if (!running || !config.has_value())
        {
            throw std::logic_error("fabric service runtime is not started");
        }
        return *config;
    }

    void begin_next(std::string_view data_id)
    {
        auto queue = publication_queues.find(data_id);
        if (queue == publication_queues.end() || queue->second.empty())
        {
            return;
        }
        auto machine = publishers.find(data_id);
        if (machine == publishers.end())
        {
            machine =
                publishers.emplace(Str{data_id}, std::make_unique<PublisherStateMachine>(configured(), Str{data_id}))
                    .first;
        }
        if (!publication_terminal(machine->second->state()) && machine->second->state() != PublicationState::Idle)
        {
            return;
        }

        PublicationRequestInput request = std::move(queue->second.front());
        queue->second.pop_front();
        machine->second->begin(PublicationInput{
            .output = std::move(request.output),
            .dependencies = std::move(request.dependencies),
            .self_predecessor = request.self_predecessor,
            .system_time = wall_time(),
        });
    }
};

FabricServiceRuntime::FabricServiceRuntime(FabricServicePlanHandle plan) : impl_(std::make_unique<Impl>())
{
    if (!plan.value)
    {
        throw std::invalid_argument("fabric service runtime requires a wiring plan");
    }
    impl_->plan = std::move(plan);
}

FabricServiceRuntime::~FabricServiceRuntime()
{
    stop();
}

void FabricServiceRuntime::start(GlobalStateView global_state)
{
    if (impl_->running)
    {
        throw std::logic_error("fabric service runtime started twice");
    }
    impl_->config = fabric_config(global_state);
    if (!impl_->config.has_value())
    {
        throw std::logic_error("hgraph.fabric service requires FabricConfig in GlobalState");
    }
    require_valid_config(*impl_->config);
    impl_->running = true;
}

void FabricServiceRuntime::stop() noexcept
{
    if (!impl_)
    {
        return;
    }
    impl_->running = false;
    impl_->publishers.clear();
    impl_->publication_queues.clear();
    impl_->advertised.clear();
    impl_->snapshot = {};
    impl_->planned_snapshot = {};
    impl_->replay = {};
    impl_->planned_replay = {};
    impl_->live = {};
    impl_->planned_live = {};
    impl_->config.reset();
}

void FabricServiceRuntime::configure_replay_window(DateTime start_time, DateTime end_time)
{
    impl_->replay.start_time = start_time;
    impl_->replay.end_time = end_time;
    impl_->planned_replay.start_time = start_time;
    impl_->planned_replay.end_time = end_time;
}

std::optional<DeliveryBatch> FabricServiceRuntime::snapshot(std::vector<SubscriptionSpec> subscriptions)
{
    impl_->snapshot.configure(impl_->configured(), std::move(subscriptions));
    return impl_->snapshot.evaluate();
}

std::optional<DeliveryBatch> FabricServiceRuntime::planned_snapshot()
{
    impl_->planned_snapshot.configure(impl_->configured(), impl_->plan.value->snapshot);
    return impl_->planned_snapshot.evaluate();
}

std::optional<DeliveryBatch> FabricServiceRuntime::replay(std::vector<SubscriptionSpec> subscriptions, DateTime now,
                                                          NodeScheduler scheduler)
{
    impl_->replay.configure(impl_->configured(), std::move(subscriptions));
    return impl_->replay.evaluate(now, scheduler);
}

std::optional<DeliveryBatch> FabricServiceRuntime::planned_replay(DateTime now, NodeScheduler scheduler)
{
    impl_->planned_replay.configure(impl_->configured(), impl_->plan.value->replay);
    return impl_->planned_replay.evaluate(now, scheduler);
}

std::optional<DeliveryBatch> FabricServiceRuntime::live(std::vector<SubscriptionSpec> subscriptions,
                                                        std::vector<DataRevisionInput> revisions, DateTime now)
{
    impl_->live.configure(impl_->configured(), std::move(subscriptions));
    return impl_->live.evaluate(std::move(revisions), now);
}

std::optional<DeliveryBatch> FabricServiceRuntime::planned_live(std::vector<DataRevisionInput> revisions, DateTime now)
{
    impl_->planned_live.configure(impl_->configured(), impl_->plan.value->live);
    return impl_->planned_live.evaluate(std::move(revisions), now);
}

void FabricServiceRuntime::publish(PublicationRequestInput request)
{
    require_data_id(request.data_id);
    auto &queue = impl_->publication_queues[request.data_id];
    if (queue.size() >= 1024)
    {
        throw std::overflow_error("fabric publication queue is full");
    }
    const Str data_id = request.data_id;
    queue.push_back(std::move(request));
    impl_->begin_next(data_id);
}

std::vector<DataRevisionInput> FabricServiceRuntime::advance_publications()
{
    std::vector<DataRevisionInput> accepted;
    for (auto &[data_id, machine] : impl_->publishers)
    {
        for (std::size_t step = 0; step < 16; ++step)
        {
            const PublicationState before = machine->state();
            const PublicationState after = machine->advance();
            if (publication_terminal(after))
            {
                break;
            }
            if (after == before && after == PublicationState::NotificationPending)
            {
                break;
            }
        }
        if (machine->state() == PublicationState::Published)
        {
            const auto revision = machine->accepted_revision();
            if (revision.has_value() && impl_->advertised[data_id] < revision->revision)
            {
                impl_->advertised[data_id] = revision->revision;
                accepted.push_back(*revision);
            }
        }
        impl_->begin_next(data_id);
    }
    return accepted;
}

bool FabricServiceRuntime::publication_work_pending() const noexcept
{
    if (!impl_->running)
    {
        return false;
    }
    if (std::ranges::any_of(impl_->publication_queues, [](const auto &item) { return !item.second.empty(); }))
    {
        return true;
    }
    return std::ranges::any_of(impl_->publishers,
                               [](const auto &item) { return !publication_terminal(item.second->state()); });
}

std::optional<std::tuple<Str, DataVersion, Frame>> FabricServiceRuntime::load(std::string_view requested_data_id,
                                                                              DataVersion version) const
{
    if (!impl_->running || !impl_->config.has_value())
    {
        throw std::logic_error("fabric service runtime is not started");
    }
    Str data_id{requested_data_id};
    require_data_id(data_id);
    if (version <= 0)
    {
        throw std::invalid_argument("fabric load version must be positive");
    }
    Frame frame = impl_->config->frames.read(data_version_key(impl_->config->prefix, data_id, version));
    if (!frame.has_value())
    {
        return std::nullopt;
    }
    return std::tuple<Str, DataVersion, Frame>{std::move(data_id), version, std::move(frame)};
}

std::vector<std::pair<Str, Str>> FabricServiceRuntime::diagnostics() const
{
    return {
        {"lifecycle", impl_->running ? "running" : "stopped"},
        {"publishers", std::to_string(impl_->publishers.size())},
    };
}

Str subscription_key(Str data_id, SubscriptionMode mode, DateTime as_of)
{
    require_data_id(data_id);
    if (mode != SubscriptionMode::Snapshot)
    {
        return data_id;
    }
    return data_id + KEY_SEPARATOR + std::to_string(as_of.time_since_epoch().count());
}

SubscriptionSpec decode_subscription_key(std::string_view key, SubscriptionMode mode)
{
    if (mode != SubscriptionMode::Snapshot)
    {
        Str data_id{key};
        require_data_id(data_id);
        return SubscriptionSpec{.key = data_id, .data_id = std::move(data_id)};
    }
    auto [data_id, micros] = split_key(key, "snapshot subscription key");
    return SubscriptionSpec{
        .key = Str{key},
        .data_id = std::move(data_id),
        .as_of = DateTime{TimeDelta{micros}},
    };
}

void FabricServicePlan::add(SubscriptionSpec subscription, SubscriptionMode mode)
{
    auto *target = [&]() -> std::vector<SubscriptionSpec> * {
        switch (mode)
        {
        case SubscriptionMode::Live:
            return &live;
        case SubscriptionMode::Replay:
            return &replay;
        case SubscriptionMode::Snapshot:
            return &snapshot;
        }
        throw std::invalid_argument("unsupported fabric planned subscription mode");
    }();
    const auto found = std::ranges::find(*target, subscription.key, &SubscriptionSpec::key);
    if (found != target->end())
    {
        if (*found != subscription)
        {
            throw std::invalid_argument("fabric planned subscription key has conflicting policy");
        }
        return;
    }
    target->push_back(std::move(subscription));
}

FabricServicePlanHandle service_plan(Wiring &wiring, std::string_view path)
{
    struct PlanRegistry
    {
        std::map<Str, FabricServicePlanHandle> paths{};
    };
    auto registry = std::static_pointer_cast<PlanRegistry>(wiring.acquire_extension_state(
        std::type_index(typeid(PlanRegistry)), [] { return std::make_shared<PlanRegistry>(); }));
    auto [found, inserted] = registry->paths.try_emplace(Str{path});
    if (inserted)
    {
        found->second.value = std::make_shared<FabricServicePlan>();
    }
    return found->second;
}

void plan_subscription(Wiring &wiring, SubscriptionSpec subscription, SubscriptionMode mode, std::string_view path)
{
    service_plan(wiring, path).value->add(std::move(subscription), mode);
}

} // namespace hgraph::fabric::detail
