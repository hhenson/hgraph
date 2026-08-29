#include "impl/service_state.h"

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/runtime/evaluation_clock.h>

#include <algorithm>
#include <deque>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::fabric::detail
{
    namespace
    {
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

        [[nodiscard]] std::vector<SubscriptionSpec>
        canonical_subscriptions(std::vector<SubscriptionSpec> subscriptions)
        {
            std::ranges::sort(subscriptions,
                              [](const SubscriptionSpec &lhs, const SubscriptionSpec &rhs)
                              { return lhs.key < rhs.key; });
            subscriptions.erase(
                std::ranges::unique(subscriptions, {}, &SubscriptionSpec::key).begin(),
                subscriptions.end());
            return subscriptions;
        }

        struct RootBatch
        {
            std::vector<RootUpdate> changed{};
            std::vector<ResolvedRevision> selected{};
        };

        struct ResolutionMetrics
        {
            ResolverMetrics resolver{};
            std::uint64_t calls{};
            std::uint64_t forests_ready{};
            std::uint64_t forests_unchanged{};
            std::uint64_t forests_pending{};
            std::uint64_t forests_ambiguous{};
            std::uint64_t forests_cyclic{};
            std::uint64_t forests_corrupt{};
            std::uint64_t notice_to_ready_samples{};
            TimeDelta::rep notice_to_ready_total{};

            void observe(const CoordinationResult &result)
            {
                ++calls;
                for (const auto &forest : result.forests)
                {
                    switch (forest.status)
                    {
                    case ResolutionStatus::Ready:
                        ++forests_ready;
                        break;
                    case ResolutionStatus::Unchanged:
                        ++forests_unchanged;
                        break;
                    case ResolutionStatus::Pending:
                        ++forests_pending;
                        break;
                    case ResolutionStatus::Ambiguous:
                        ++forests_ambiguous;
                        break;
                    case ResolutionStatus::Cyclic:
                        ++forests_cyclic;
                        break;
                    case ResolutionStatus::Corrupt:
                        ++forests_corrupt;
                        break;
                    }
                    resolver.accepted_heads_observed += forest.metrics.accepted_heads_observed;
                    resolver.revision_cache_hits += forest.metrics.revision_cache_hits;
                    resolver.revision_cache_misses += forest.metrics.revision_cache_misses;
                    resolver.frame_cache_hits += forest.metrics.frame_cache_hits;
                    resolver.frame_cache_misses += forest.metrics.frame_cache_misses;
                    resolver.output_index_hits += forest.metrics.output_index_hits;
                    resolver.output_index_misses += forest.metrics.output_index_misses;
                    resolver.revisions_examined += forest.metrics.revisions_examined;
                    resolver.edges_examined += forest.metrics.edges_examined;
                    resolver.candidate_selections += forest.metrics.candidate_selections;
                    resolver.backtracking_depth_sum += forest.metrics.backtracking_depth_sum;
                    resolver.maximum_backtracking_depth =
                        std::max(resolver.maximum_backtracking_depth,
                                 forest.metrics.maximum_backtracking_depth);
                    if (forest.metrics.notice_to_ready.has_value())
                    {
                        ++notice_to_ready_samples;
                        notice_to_ready_total += forest.metrics.notice_to_ready->count();
                    }
                }
            }

            void add(const ResolutionMetrics &other)
            {
                calls += other.calls;
                forests_ready += other.forests_ready;
                forests_unchanged += other.forests_unchanged;
                forests_pending += other.forests_pending;
                forests_ambiguous += other.forests_ambiguous;
                forests_cyclic += other.forests_cyclic;
                forests_corrupt += other.forests_corrupt;
                notice_to_ready_samples += other.notice_to_ready_samples;
                notice_to_ready_total += other.notice_to_ready_total;
                resolver.accepted_heads_observed += other.resolver.accepted_heads_observed;
                resolver.revision_cache_hits += other.resolver.revision_cache_hits;
                resolver.revision_cache_misses += other.resolver.revision_cache_misses;
                resolver.frame_cache_hits += other.resolver.frame_cache_hits;
                resolver.frame_cache_misses += other.resolver.frame_cache_misses;
                resolver.output_index_hits += other.resolver.output_index_hits;
                resolver.output_index_misses += other.resolver.output_index_misses;
                resolver.revisions_examined += other.resolver.revisions_examined;
                resolver.edges_examined += other.resolver.edges_examined;
                resolver.candidate_selections += other.resolver.candidate_selections;
                resolver.backtracking_depth_sum += other.resolver.backtracking_depth_sum;
                resolver.maximum_backtracking_depth = std::max(
                    resolver.maximum_backtracking_depth, other.resolver.maximum_backtracking_depth);
            }
        };

        struct ResolutionCore
        {
            FabricConfig config;
            std::vector<Str> roots;
            ConsistencyCoordinator coordinator;
            std::map<Str, RevisionId, IdLess> emitted_revisions{};
            std::set<Str, IdLess> observed{};
            ResolutionMetrics metrics{};

            ResolutionCore(FabricConfig configured, std::vector<Str> configured_roots)
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
                metrics.observe(result);
                for (const auto &forest : result.forests)
                {
                    if (forest.status == ResolutionStatus::Corrupt ||
                        forest.status == ResolutionStatus::Ambiguous ||
                        forest.status == ResolutionStatus::Cyclic)
                    {
                        throw std::runtime_error(
                            "fabric subscription consistency failure for root '" +
                            forest.roots.front() + "': " + std::string{enum_name(forest.status)} +
                            ": " + forest.diagnostic);
                    }
                }
                observe(result);

                RootBatch batch;
                batch.changed = std::move(result.changed_roots);
                for (const auto &root : roots)
                {
                    const auto selected = std::ranges::lower_bound(result.committed_lineage, root,
                                                                   canonical_data_id_less,
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

        [[nodiscard]] std::optional<DeliveryBatch>
        deliver(RootBatch batch, const std::vector<SubscriptionSpec> &subscriptions)
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

        struct ReplaySession
        {
            FabricConfig config{};
            std::vector<SubscriptionSpec> subscriptions{};
            std::unique_ptr<ResolutionCore> core{};
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
                core = subscriptions.empty()
                           ? nullptr
                           : std::make_unique<ResolutionCore>(config, roots_of(subscriptions));
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
                std::optional<std::string> page_cursor;
                for (;;)
                {
                    const auto page = config.objects.list(
                        prefix,
                        page_cursor.has_value() ? std::optional<std::string_view>{*page_cursor}
                                                : std::nullopt,
                        1000);
                    for (const auto &object : page.objects)
                    {
                        if (!object.key.starts_with(prefix))
                        {
                            throw std::runtime_error("fabric as-of listing escaped its data id");
                        }
                        entry->second.emplace_back(TimeDelta{decode_fabric_ordinal(
                            std::string_view{object.key}.substr(prefix.size()))});
                    }
                    if (!page.next_start_after.has_value())
                    {
                        break;
                    }
                    page_cursor = page.next_start_after;
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

            [[nodiscard]] std::optional<DeliveryBatch> evaluate(DateTime now,
                                                                NodeScheduler scheduler)
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

            void collect_metrics(ResolutionMetrics &metrics) const
            {
                if (core)
                {
                    metrics.add(core->metrics);
                }
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
            std::unique_ptr<ResolutionCore> core{};
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
                core = subscriptions.empty()
                           ? nullptr
                           : std::make_unique<ResolutionCore>(config, roots_of(subscriptions));
                notice_cache.clear();
                admitted.clear();
                startup = true;
            }

            [[nodiscard]] std::optional<DeliveryBatch>
            evaluate(std::vector<DataRevisionInput> revisions, DateTime now, bool reconcile)
            {
                DeliveryBatch combined;
                if (!core)
                {
                    return std::nullopt;
                }
                for (auto &revision : revisions)
                {
                    // A complete notice is an optimisation over durable recovery, not
                    // an authority. Retain it only once the active consistency forest
                    // identifies the data id as a root or dependency; unrelated topic
                    // traffic must not consume this session's bounded cache.
                    if (!core->observed.contains(revision.data_id))
                    {
                        continue;
                    }
                    auto found = notice_cache.find(revision.data_id);
                    if (found == notice_cache.end())
                    {
                        if (notice_cache.size() >= FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION)
                        {
                            throw std::overflow_error("fabric live notice cache is full");
                        }
                        Str data_id = revision.data_id;
                        notice_cache.emplace(std::move(data_id),
                                             CachedNotice{std::move(revision), now});
                    }
                    else if (found->second.revision.revision < revision.revision)
                    {
                        found->second = CachedNotice{std::move(revision), now};
                    }
                    else if (found->second.revision.revision == revision.revision &&
                             found->second.revision != revision)
                    {
                        throw std::runtime_error(
                            "fabric live notice conflicts with its cached revision");
                    }
                }

                if (startup || reconcile)
                {
                    auto initial = core->batch(core->coordinator.resolve(now));
                    if (initial.has_value())
                    {
                        if (auto values = deliver(std::move(*initial), subscriptions);
                            values.has_value())
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
                            if (prior != admitted.end() &&
                                prior->second >= cached.revision.revision)
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
                        if (auto values = deliver(std::move(*update), subscriptions);
                            values.has_value())
                        {
                            combined.roots.insert(combined.roots.end(),
                                                  std::make_move_iterator(values->roots.begin()),
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

            void collect_metrics(ResolutionMetrics &metrics) const
            {
                if (core)
                {
                    metrics.add(core->metrics);
                }
            }
        };
    } // namespace

    namespace
    {
        struct DiagnosticState
        {
            std::map<Str, FabricDiagnosticEventInput, IdLess> events{};

            void record(Str component, Str category, Str message, Bool retriable, Bool fatal)
            {
                Str path = component + "." + category;
                auto found = events.find(path);
                if (found == events.end() && events.size() >= FABRIC_DIAGNOSTIC_EVENT_LIMIT - 1U)
                {
                    path = "diagnostics.capacity";
                    component = "diagnostics";
                    category = "capacity";
                    message = "additional Fabric diagnostic paths were conflated at the "
                              "configured limit";
                    retriable = false;
                    fatal = false;
                    found = events.find(path);
                }
                if (found == events.end())
                {
                    found = events
                                .emplace(path,
                                         FabricDiagnosticEventInput{
                                             .component = std::move(component),
                                             .category = std::move(category),
                                             .message = std::move(message),
                                             .retriable = retriable,
                                             .fatal = fatal,
                                         })
                                .first;
                }
                else
                {
                    found->second.message = std::move(message);
                    found->second.retriable = retriable;
                    found->second.fatal = fatal;
                }
                if (found->second.occurrences < std::numeric_limits<Int>::max())
                {
                    ++found->second.occurrences;
                }
            }
        };

        template <typename Operation>
        decltype(auto) with_failure_diagnostic(DiagnosticState &diagnostics, Str component,
                                               Str category, Operation &&operation)
        {
            try
            {
                return std::forward<Operation>(operation)();
            }
            catch (const std::exception &error)
            {
                diagnostics.record(std::move(component), std::move(category), error.what(), false,
                                   true);
                throw;
            }
        }

        [[nodiscard]] std::vector<std::pair<Str, Str>>
        resolution_metrics(const ResolutionMetrics &metrics)
        {
            return {
                {"resolution.calls", std::to_string(metrics.calls)},
                {"resolution.forests.ready", std::to_string(metrics.forests_ready)},
                {"resolution.forests.unchanged", std::to_string(metrics.forests_unchanged)},
                {"resolution.forests.pending", std::to_string(metrics.forests_pending)},
                {"resolution.forests.ambiguous", std::to_string(metrics.forests_ambiguous)},
                {"resolution.forests.cyclic", std::to_string(metrics.forests_cyclic)},
                {"resolution.forests.corrupt", std::to_string(metrics.forests_corrupt)},
                {"resolution.accepted_heads_observed",
                 std::to_string(metrics.resolver.accepted_heads_observed)},
                {"resolution.revision_cache.hits",
                 std::to_string(metrics.resolver.revision_cache_hits)},
                {"resolution.revision_cache.misses",
                 std::to_string(metrics.resolver.revision_cache_misses)},
                {"resolution.frame_cache.hits", std::to_string(metrics.resolver.frame_cache_hits)},
                {"resolution.frame_cache.misses",
                 std::to_string(metrics.resolver.frame_cache_misses)},
                {"resolution.output_index.hits",
                 std::to_string(metrics.resolver.output_index_hits)},
                {"resolution.output_index.misses",
                 std::to_string(metrics.resolver.output_index_misses)},
                {"resolution.revisions_examined",
                 std::to_string(metrics.resolver.revisions_examined)},
                {"resolution.edges_examined", std::to_string(metrics.resolver.edges_examined)},
                {"resolution.candidate_selections",
                 std::to_string(metrics.resolver.candidate_selections)},
                {"__resolution.backtracking_depth.sum",
                 std::to_string(metrics.resolver.backtracking_depth_sum)},
                {"resolution.backtracking_depth.average",
                 std::to_string(metrics.resolver.average_backtracking_depth())},
                {"resolution.backtracking_depth.maximum",
                 std::to_string(metrics.resolver.maximum_backtracking_depth)},
                {"resolution.notice_to_ready.samples",
                 std::to_string(metrics.notice_to_ready_samples)},
                {"resolution.notice_to_ready.microseconds_total",
                 std::to_string(metrics.notice_to_ready_total)},
            };
        }

        [[nodiscard]] FabricNodeDiagnostics node_diagnostics(const ResolutionMetrics &metrics,
                                                             const DiagnosticState &diagnostics)
        {
            return FabricNodeDiagnostics{
                .metrics = resolution_metrics(metrics),
                .events = {diagnostics.events.begin(), diagnostics.events.end()},
            };
        }

        [[nodiscard]] FabricConfig checked_config(FabricConfig config)
        {
            require_valid_config(config);
            return config;
        }
    } // namespace

    struct ReplayNodeState::Impl
    {
        std::optional<FabricConfig> config{};
        std::vector<SubscriptionSpec> planned{};
        ReplaySession session{};
        DiagnosticState diagnostics{};
    };

    ReplayNodeState::ReplayNodeState() : impl_(std::make_unique<Impl>()) {}
    ReplayNodeState::~ReplayNodeState() = default;
    ReplayNodeState::ReplayNodeState(ReplayNodeState &&) noexcept = default;
    ReplayNodeState &ReplayNodeState::operator=(ReplayNodeState &&) noexcept = default;

    void ReplayNodeState::start(FabricConfig config, DateTime start_time, DateTime end_time,
                                std::vector<SubscriptionSpec> planned)
    {
        if (impl_->config.has_value())
        {
            throw std::logic_error("fabric replay node started twice");
        }
        impl_->config = checked_config(std::move(config));
        impl_->planned = std::move(planned);
        impl_->session.start_time = start_time;
        impl_->session.end_time = end_time;
    }

    void ReplayNodeState::stop() noexcept
    {
        impl_->session = {};
        impl_->diagnostics = {};
        impl_->planned.clear();
        impl_->config.reset();
    }

    std::optional<DeliveryBatch> ReplayNodeState::evaluate_planned(DateTime now,
                                                                   NodeScheduler scheduler)
    {
        return evaluate(impl_->planned, now, scheduler);
    }

    std::optional<DeliveryBatch>
    ReplayNodeState::evaluate(std::vector<SubscriptionSpec> subscriptions, DateTime now,
                              NodeScheduler scheduler)
    {
        if (!impl_->config.has_value())
        {
            throw std::logic_error("fabric replay node is not started");
        }
        return with_failure_diagnostic(impl_->diagnostics, "store", "replay",
                                       [&]
                                       {
                                           impl_->session.configure(*impl_->config,
                                                                    std::move(subscriptions));
                                           return impl_->session.evaluate(now, scheduler);
                                       });
    }

    FabricNodeDiagnostics ReplayNodeState::diagnostics() const
    {
        ResolutionMetrics metrics;
        impl_->session.collect_metrics(metrics);
        return node_diagnostics(metrics, impl_->diagnostics);
    }

    struct LiveNodeState::Impl
    {
        std::optional<FabricConfig> config{};
        std::vector<SubscriptionSpec> planned{};
        LiveSession session{};
        DiagnosticState diagnostics{};
    };

    LiveNodeState::LiveNodeState() : impl_(std::make_unique<Impl>()) {}
    LiveNodeState::~LiveNodeState() = default;
    LiveNodeState::LiveNodeState(LiveNodeState &&) noexcept = default;
    LiveNodeState &LiveNodeState::operator=(LiveNodeState &&) noexcept = default;

    void LiveNodeState::start(FabricConfig config, std::vector<SubscriptionSpec> planned)
    {
        if (impl_->config.has_value())
        {
            throw std::logic_error("fabric live node started twice");
        }
        impl_->config = checked_config(std::move(config));
        impl_->planned = std::move(planned);
    }

    void LiveNodeState::stop() noexcept
    {
        impl_->session = {};
        impl_->diagnostics = {};
        impl_->planned.clear();
        impl_->config.reset();
    }

    std::optional<DeliveryBatch>
    LiveNodeState::evaluate_planned(std::vector<DataRevisionInput> revisions, DateTime now,
                                    bool reconcile)
    {
        return evaluate(impl_->planned, std::move(revisions), now, reconcile);
    }

    std::optional<DeliveryBatch>
    LiveNodeState::evaluate(std::vector<SubscriptionSpec> subscriptions,
                            std::vector<DataRevisionInput> revisions, DateTime now, bool reconcile)
    {
        if (!impl_->config.has_value())
        {
            throw std::logic_error("fabric live node is not started");
        }
        return with_failure_diagnostic(
            impl_->diagnostics, "fabric", "live",
            [&]
            {
                impl_->session.configure(*impl_->config, std::move(subscriptions));
                return impl_->session.evaluate(std::move(revisions), now, reconcile);
            });
    }

    FabricNodeDiagnostics LiveNodeState::diagnostics() const
    {
        ResolutionMetrics metrics;
        impl_->session.collect_metrics(metrics);
        FabricNodeDiagnostics result = node_diagnostics(metrics, impl_->diagnostics);
        result.metrics.emplace_back("live.notices",
                                    std::to_string(impl_->session.notice_cache.size()));
        result.metrics.emplace_back("live.notice_limit_per_session",
                                    std::to_string(FABRIC_LIVE_NOTICE_LIMIT_PER_SESSION));
        return result;
    }

    struct PublicationNodeState::Impl
    {
        std::optional<FabricConfig> config{};
        std::map<Str, std::unique_ptr<PublisherStateMachine>, IdLess> publishers{};
        std::map<Str, std::deque<PublicationRequestInput>, IdLess> queues{};
        std::map<Str, RevisionId, IdLess> advertised{};
        DiagnosticState diagnostics{};
        bool graph_notifications{};

        [[nodiscard]] FabricConfig &configured()
        {
            if (!config.has_value())
            {
                throw std::logic_error("fabric publication node is not started");
            }
            return *config;
        }

        void begin_next(std::string_view data_id)
        {
            auto queue = queues.find(data_id);
            if (queue == queues.end() || queue->second.empty())
            {
                return;
            }
            auto machine = publishers.find(data_id);
            if (machine == publishers.end())
            {
                machine = publishers
                              .emplace(Str{data_id}, std::make_unique<PublisherStateMachine>(
                                                         configured(), Str{data_id}))
                              .first;
            }
            if (!publication_terminal(machine->second->state()) &&
                machine->second->state() != PublicationState::Idle)
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

    PublicationNodeState::PublicationNodeState() : impl_(std::make_unique<Impl>()) {}
    PublicationNodeState::~PublicationNodeState() = default;
    PublicationNodeState::PublicationNodeState(PublicationNodeState &&) noexcept = default;
    PublicationNodeState &
    PublicationNodeState::operator=(PublicationNodeState &&) noexcept = default;

    void PublicationNodeState::start(FabricConfig config, bool graph_notifications)
    {
        if (impl_->config.has_value())
        {
            throw std::logic_error("fabric publication node started twice");
        }
        impl_->config = checked_config(std::move(config));
        impl_->graph_notifications = graph_notifications;
    }

    void PublicationNodeState::stop() noexcept
    {
        impl_->publishers.clear();
        impl_->queues.clear();
        impl_->advertised.clear();
        impl_->diagnostics = {};
        impl_->config.reset();
        impl_->graph_notifications = false;
    }

    void PublicationNodeState::enqueue(PublicationRequestInput request)
    {
        require_data_id(request.data_id);
        auto &queue = impl_->queues[request.data_id];
        if (queue.size() >= FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID)
        {
            throw std::overflow_error("fabric publication queue is full");
        }
        const Str data_id = request.data_id;
        queue.push_back(std::move(request));
        impl_->begin_next(data_id);
    }

    std::vector<DataRevisionInput> PublicationNodeState::advance()
    {
        std::vector<DataRevisionInput> accepted;
        for (auto &[data_id, machine] : impl_->publishers)
        {
            for (std::size_t step = 0; step < 16; ++step)
            {
                const PublicationState before = machine->state();
                if (impl_->graph_notifications && before == PublicationState::LatestDurable)
                {
                    break;
                }
                PublicationState after{};
                try
                {
                    after = machine->advance();
                }
                catch (const std::exception &error)
                {
                    const bool transport = before == PublicationState::LatestDurable ||
                                           before == PublicationState::NotificationPending;
                    impl_->diagnostics.record(transport ? "transport" : "store",
                                              transport ? "notification" : "publication",
                                              error.what(), false, true);
                    throw;
                }
                if (publication_terminal(after) ||
                    (impl_->graph_notifications && after == PublicationState::LatestDurable) ||
                    (after == before && after == PublicationState::NotificationPending))
                {
                    break;
                }
            }
            if (impl_->graph_notifications && machine->state() == PublicationState::LatestDurable)
            {
                if (const auto revision = machine->candidate_revision(); revision.has_value())
                {
                    accepted.push_back(*revision);
                }
            }
            else if (!impl_->graph_notifications && machine->state() == PublicationState::Published)
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

    void PublicationNodeState::complete(NotificationDeliveryInput delivery)
    {
        require_data_id(delivery.data_id);
        if (!impl_->graph_notifications)
        {
            throw std::logic_error("fabric notification completion requires graph transport mode");
        }
        const auto machine = impl_->publishers.find(delivery.data_id);
        if (machine == impl_->publishers.end())
        {
            throw std::logic_error("fabric notification completion has no durable publication");
        }
        if (machine->second->state() != PublicationState::LatestDurable)
        {
            throw std::logic_error("fabric notification completion reached publication state " +
                                   std::to_string(static_cast<int>(machine->second->state())));
        }
        const auto candidate = machine->second->candidate_revision();
        if (!candidate.has_value() || candidate->revision != delivery.revision)
        {
            throw std::logic_error("fabric notification completion does not match the "
                                   "durable publication");
        }
        if (!delivery.delivered)
        {
            const Str message = delivery.message.empty()
                                    ? Str{"fabric revision notification delivery failed"}
                                    : std::move(delivery.message);
            impl_->diagnostics.record("transport", "notification", message, false, true);
            throw std::runtime_error(message);
        }
        machine->second->acknowledge_notification();
    }

    bool PublicationNodeState::work_pending() const noexcept
    {
        if (!impl_->config.has_value())
        {
            return false;
        }
        if (std::ranges::any_of(impl_->queues,
                                [](const auto &item) { return !item.second.empty(); }))
        {
            return true;
        }
        return std::ranges::any_of(impl_->publishers,
                                   [this](const auto &item)
                                   {
                                       const auto state = item.second->state();
                                       return !publication_terminal(state) &&
                                              (!impl_->graph_notifications ||
                                               state != PublicationState::LatestDurable);
                                   });
    }

    std::size_t PublicationNodeState::notification_candidate_limit() const
    {
        if (!impl_->config.has_value())
        {
            throw std::logic_error("fabric publication node is not started");
        }
        return impl_->config->notification_candidate_limit;
    }

    FabricNodeDiagnostics PublicationNodeState::diagnostics() const
    {
        std::size_t queued{};
        for (const auto &[data_id, queue] : impl_->queues)
        {
            static_cast<void>(data_id);
            queued += queue.size();
        }
        return FabricNodeDiagnostics{
            .metrics =
                {
                    {"publishers", std::to_string(impl_->publishers.size())},
                    {"publication.queued", std::to_string(queued)},
                    {"publication.queue_limit_per_data_id",
                     std::to_string(FABRIC_PUBLICATION_QUEUE_LIMIT_PER_DATA_ID)},
                },
            .events = {impl_->diagnostics.events.begin(), impl_->diagnostics.events.end()},
        };
    }

    SubscriptionSpec decode_subscription_key(std::string_view key)
    {
        Str data_id{key};
        require_data_id(data_id);
        return SubscriptionSpec{.key = data_id, .data_id = std::move(data_id)};
    }

    void FabricWiringPlan::add(SubscriptionSpec subscription)
    {
        const auto found = std::ranges::find(subscriptions, subscription.key,
                                             &SubscriptionSpec::key);
        if (found != subscriptions.end())
        {
            if (*found != subscription)
            {
                throw std::invalid_argument(
                    "fabric planned subscription key has conflicting policy");
            }
            return;
        }
        subscriptions.push_back(std::move(subscription));
    }

    [[nodiscard]] static std::shared_ptr<FabricWiringPlan>
    mutable_service_plan(Wiring &wiring, std::string_view path)
    {
        struct PlanRegistry
        {
            std::map<Str, std::shared_ptr<FabricWiringPlan>> paths{};
        };
        auto registry = std::static_pointer_cast<PlanRegistry>(
            wiring.acquire_extension_state(std::type_index(typeid(PlanRegistry)),
                                           [] { return std::make_shared<PlanRegistry>(); }));
        auto [found, inserted] = registry->paths.try_emplace(Str{path});
        if (inserted)
        {
            found->second = std::make_shared<FabricWiringPlan>();
        }
        return found->second;
    }

    FabricWiringPlanHandle service_plan(Wiring &wiring, std::string_view path)
    {
        return FabricWiringPlanHandle{mutable_service_plan(wiring, path)};
    }

    void plan_subscription(Wiring &wiring, SubscriptionSpec subscription,
                           std::string_view path)
    {
        mutable_service_plan(wiring, path)->add(std::move(subscription));
    }

} // namespace hgraph::fabric::detail
