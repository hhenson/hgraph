#include <hgraph/types/record_replay.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

namespace hgraph::record_replay
{
    namespace
    {
        inline constexpr std::string_view CONFIG_KEY{"__hgraph.record_replay.config__"};
        inline constexpr std::string_view COMPARE_KEY_PREFIX{"__hgraph.record_replay.compare__."};

        /** The single legacy-name choke point (RFC 0025 deprecation window):
            every entry that accepts a backend id normalises through here, so
            "InMemory" and "memory" can never select different overloads. */
        [[nodiscard]] std::string normalize_backend(std::string backend)
        {
            if (backend == "InMemory") { return std::string{MEMORY}; }
            if (backend == "InMemoryDense") { return std::string{TESTING}; }
            // Compat alias only (deprecation window): the id is defined and
            // served by hgraph-persistence; core merely translates the
            // released legacy spelling.
            if (backend == "DataFrame") { return std::string{"hgraph.persistence.frame"}; }
            return backend;
        }
        thread_local std::vector<ScopeState> g_scopes{};
        const ScopeState                     g_no_scope{};

        /** Backend id -> registered RECOVER seed resolver. Build-time
            registration state; reset() clears it and the registration
            installers replay it (RFC 0025 checkpoint 3 semantics). */
        std::vector<std::pair<std::string, SeedResolver>> g_seed_resolvers{};

        [[nodiscard]] SeedResolver find_seed_resolver(std::string_view backend) noexcept
        {
            for (const auto &[id, resolver] : g_seed_resolvers)
            {
                if (id == backend) { return resolver; }
            }
            return nullptr;
        }

        void ensure_config_type()
        {
            (void)TypeRegistry::instance().register_scalar<RecordReplayConfig>(
                "RecordReplayConfig");
        }

        void ensure_summary_type()
        {
            // Generation-checked (the per-tick cache validation pattern the
            // registry's atomic ``reset_generation`` exists for): the memory
            // compare publishes per tick, and the per-tick path must not
            // acquire the registry's counted mutex (single-threaded
            // evaluation ruling) — but a process-lifetime flag would survive
            // ``reset_all_registries()`` and strand the type unregistered.
            // Between resets this is one relaxed atomic load; set_config
            // also registers eagerly so evaluation normally sees zero
            // acquisitions even on the first publish after a reset.
            static std::atomic<std::uint64_t> registered_generation{
                std::numeric_limits<std::uint64_t>::max()};
            auto               &registry = TypeRegistry::instance();
            const std::uint64_t generation = registry.reset_generation();
            if (registered_generation.load(std::memory_order_acquire) == generation)
            {
                return;
            }
            (void)registry.register_scalar<ComparisonSummary>("__comparison_summary__");
            registered_generation.store(generation, std::memory_order_release);
        }
    }  // namespace

    void set_config(GlobalStateView state, RecordReplayConfig config)
    {
        if (!state.valid())
        {
            throw std::logic_error("record/replay configuration requires GlobalState");
        }
        if (config.backend.empty())
        {
            throw std::invalid_argument("record/replay backend must not be empty");
        }
        config.backend = normalize_backend(std::move(config.backend));
        ensure_config_type();
        ensure_summary_type();
        state.set(CONFIG_KEY, Value{std::move(config)});
    }

    RecordReplayConfig config(GlobalStateView state)
    {
        if (!state.valid())
        {
            return RecordReplayConfig{};
        }
        const ValueView value = state.get(CONFIG_KEY);
        return value.valid() ? value.checked_as<RecordReplayConfig>() : RecordReplayConfig{};
    }

    bool backend_is(GlobalStateView state, std::string_view backend)
    {
        return config(state).backend == backend;
    }

    std::string effective_backend(const OperatorCallContext &context)
    {
        // A call-site backend (the scalar remains spelled ``model`` through
        // the deprecation window) wins over the graph's configuration.
        // Reading it here rather than in each guard is what keeps the
        // overloads mutually exclusive - they all decide against the same
        // answer.
        if (const Str *local = context.scalar_as<Str>("model");
            local != nullptr && !local->empty())
        {
            return normalize_backend(*local);
        }
        // By value: ``config`` returns the configuration by value, so a view
        // into its ``backend`` would dangle the moment the temporary died.
        // (``set_config`` already normalised the stored value.)
        return config(context.global_state).backend;
    }

    bool effective_backend_is(const OperatorCallContext &context, std::string_view backend)
    {
        return effective_backend(context) == backend;
    }

    const ScopeState &current_scope() noexcept
    {
        return g_scopes.empty() ? g_no_scope : g_scopes.back();
    }

    scope::scope(Mode mode, std::string recordable_id)
    {
        g_scopes.push_back(ScopeState{mode, std::move(recordable_id)});
    }

    scope::~scope()
    {
        if (!g_scopes.empty())
        {
            g_scopes.pop_back();
        }
    }

    void publish_comparison_summary(GlobalStateView state, std::string_view fq_key,
                                    ComparisonSummary summary)
    {
        if (!state.valid())
        {
            throw std::logic_error("publishing a comparison summary requires GlobalState");
        }
        ensure_summary_type();
        state.set(std::string{COMPARE_KEY_PREFIX} + std::string{fq_key}, Value{summary});
    }

    std::optional<ComparisonSummary> comparison_summary(GlobalStateView state,
                                                        std::string_view fq_key)
    {
        if (!state.valid())
        {
            return std::nullopt;
        }
        const ValueView value = state.get(std::string{COMPARE_KEY_PREFIX} + std::string{fq_key});
        if (!value.valid())
        {
            return std::nullopt;
        }
        return value.checked_as<ComparisonSummary>();
    }

    Value recorded_seed_resolver(GlobalStateView state, std::string_view fq_key,
                                 const TSValueTypeMetaData *schema, DateTime start_time)
    {
        if (schema == nullptr)
        {
            return {};
        }
        // Dispatched by the effective backend (RFC 0025): core serves its
        // in-memory ids from the ``:memory:`` buffer (the dense harness
        // records plain-key buffers, so a recover under "testing" finds no
        // sparse recording and seeds empty — its recordings are test
        // fixtures, not recoveries); an extension id goes to its REGISTERED
        // resolver; anything else is a pointed error — never a silent read
        // of the wrong store.
        const RecordReplayConfig cfg = config(state);
        if (cfg.backend != MEMORY && cfg.backend != TESTING)
        {
            if (const SeedResolver resolver = find_seed_resolver(cfg.backend))
            {
                return resolver(state, fq_key, schema, start_time);
            }
            throw std::runtime_error("no recovery seed resolver for record/replay backend '" +
                                     cfg.backend + "'");
        }

        const ValueView buffer = state.get(":memory:" + std::string{fq_key});
        if (!buffer.valid())
        {
            return {};
        }

        TSOutput   accumulated{schema};
        const auto entries = buffer.as_list();
        for (std::size_t i = 0; i < entries.size(); ++i)
        {
            const auto     entry = entries.at(i).as_indexed_view();
            const DateTime when = entry.at(0).checked_as<DateTime>();
            if (when > start_time)
            {
                break;
            }
            apply_delta(accumulated.view(when), entry.at(1));
        }
        const auto view = accumulated.view(start_time);
        return view.valid() ? Value{view.value()} : Value{};
    }

    void register_seed_resolver(std::string_view backend, SeedResolver resolver)
    {
        if (backend.empty() || resolver == nullptr)
        {
            throw std::invalid_argument("seed resolver registration requires a backend id and fn");
        }
        for (auto &[id, registered] : g_seed_resolvers)
        {
            if (id == backend)
            {
                registered = resolver;
                return;
            }
        }
        g_seed_resolvers.emplace_back(std::string{backend}, resolver);
    }

    void reset() noexcept
    {
        g_scopes.clear();
        // Registered seed resolvers are registration content, replayed by
        // the installers on the next rebuild.
        g_seed_resolvers.clear();
    }

    bool has_recordable_id(const GraphView &graph) noexcept
    {
        return graph.trait(RECORDABLE_ID_TRAIT).valid();
    }

    namespace
    {
        [[nodiscard]] std::string fq_from_parent(const ValueView &parent,
                                                 std::string_view recordable_id)
        {
            if (!parent.valid())
            {
                if (recordable_id.empty())
                {
                    throw std::runtime_error(
                        "no recordable id provided and no parent recordable id trait found");
                }
                return std::string{recordable_id};
            }
            const Str &parent_id = parent.checked_as<Str>();
            if (recordable_id.empty())
            {
                return parent_id;
            }
            return parent_id + "." + std::string{recordable_id};
        }
    }  // namespace

    std::string fq_recordable_id(const TraitsView &traits, std::string_view recordable_id)
    {
        return fq_from_parent(traits.trait(RECORDABLE_ID_TRAIT), recordable_id);
    }

    std::string fq_recordable_id(const GraphView &graph, std::string_view recordable_id)
    {
        return fq_from_parent(graph.trait(RECORDABLE_ID_TRAIT), recordable_id);
    }
}  // namespace hgraph::record_replay
