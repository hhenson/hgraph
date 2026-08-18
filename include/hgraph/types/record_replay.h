#ifndef HGRAPH_TYPES_RECORD_REPLAY_H
#define HGRAPH_TYPES_RECORD_REPLAY_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace hgraph::record_replay
{
    /**
     * Record/replay wiring configuration and mode scope (design record:
     * *Record/replay, tables and const_fn*, P2/P3 — rulings 2026-07-04).
     *
     * Python drives this through mutable GlobalState magic keys read at
     * wiring time; here it is EXPLICIT wiring-time configuration: set the
     * config before wiring, backends select on it through ordinary
     * ``requires_`` predicates, and the mode rides a wiring-scope stack (the
     * mesh/context-scope machinery). Changing the model mid-wiring is
     * deliberately unsupported (the ruling) — the imperative Python setters
     * become bridge shims over this API.
     */

    /** The record/replay modes (a flag set, mirroring Python's
     * ``RecordReplayEnum``). */
    enum class Mode : unsigned
    {
        None = 0,
        Record = 1u << 0,
        Replay = 1u << 1,
        Compare = 1u << 2,
        ReplayOutput = 1u << 3,
        Reset = 1u << 4,
        Recover = 1u << 5,
    };

    [[nodiscard]] constexpr Mode operator|(Mode lhs, Mode rhs) noexcept
    {
        return static_cast<Mode>(static_cast<unsigned>(lhs) | static_cast<unsigned>(rhs));
    }

    [[nodiscard]] constexpr Mode operator&(Mode lhs, Mode rhs) noexcept
    {
        return static_cast<Mode>(static_cast<unsigned>(lhs) & static_cast<unsigned>(rhs));
    }

    /** True when every bit of ``flag`` is set in ``mode``. */
    [[nodiscard]] constexpr bool has_mode(Mode mode, Mode flag) noexcept
    {
        return (static_cast<unsigned>(mode) & static_cast<unsigned>(flag)) ==
                   static_cast<unsigned>(flag) &&
               flag != Mode::None;
    }

    /** The SPARSE, absolute-time in-memory backend
        (``:memory:<recordable_id>.<key>``; ``stdlib::sparse_record_impl``) —
        the default.  Appends across runs and tolerates arbitrary cross-cycle
        gaps (real-time alarms, components); see ``record_replay_table.rst``
        (*In-memory record/replay — sparse vs dense*). */
    inline constexpr std::string_view MEMORY = "memory";

    /** The DENSE cycle-aligned in-memory backend (the graph testing
        harness; ``stdlib::dense_record_impl``): a plain-key ``List`` indexed
        by evaluation cycle (``MIN_ST + i*MIN_TD``), read back with
        ``get_recorded_values`` / ``Run.recorded``. */
    inline constexpr std::string_view TESTING = "testing";

    /**
     * Backend selection is OPEN (RFC 0025): core recognises only its own
     * ``"memory"`` / ``"testing"`` identifiers above; an installed extension
     * defines its own namespaced identifier (for example
     * ``"hgraph.persistence.frame"``) and registers overloads of the same
     * operator markers.  Core defines no extension identifiers.
     *
     * The wiring-time configuration is exactly the cross-implementation
     * selection core needs; every implementation-specific option (bitemporal
     * column names, as-of overrides, flush thresholds, ...) belongs to the
     * implementation that reads it.
     */
    struct RecordReplayConfig
    {
        std::string backend{MEMORY};
    };

    /** Set the configuration in ``state`` before wiring.  Legacy model names
        (``"InMemory"`` / ``"InMemoryDense"`` / ``"DataFrame"``) are accepted
        and translated to backend identifiers during the RFC 0025 deprecation
        window. */
    HGRAPH_EXPORT void set_config(GlobalStateView state, RecordReplayConfig config);

    /** The configuration in ``state`` (the default when no entry is present). */
    [[nodiscard]] HGRAPH_EXPORT RecordReplayConfig config(GlobalStateView state);

    /** ``requires_``-friendly backend guard over a wiring state's backend. */
    [[nodiscard]] HGRAPH_EXPORT bool backend_is(GlobalStateView state, std::string_view backend);

    /**
     * The EFFECTIVE backend one call resolves against: a backend scalar
     * supplied at the call site if there is one (the wiring argument remains
     * spelled ``model`` through the deprecation window), otherwise the
     * graph's configured backend.  Legacy names are translated here too, so
     * every entry point normalises identically.
     *
     * ``requires_`` runs before the node exists, so it cannot read node state —
     * but ``OperatorCallContext::scalar`` exposes scalar wiring arguments by
     * name, which is enough to let a call select its own backend.
     *
     * Every record/replay guard resolves through this one function. That is
     * what keeps the overloads mutually exclusive: if one of them consulted a
     * local override and another did not, a call supplying one would match
     * both overloads or neither.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string effective_backend(const OperatorCallContext &context);

    /** ``requires_``-friendly guard over ``effective_backend``. */
    [[nodiscard]] HGRAPH_EXPORT bool effective_backend_is(const OperatorCallContext &context,
                                                          std::string_view           backend);

    /**
     * The mode scope: a wiring-time stack of ``(mode, recordable_id)``
     * (Python's ``RecordReplayContext``). Anything that consults the ambient
     * scope while wiring MUST fold what it consulted into its intern
     * identity (the ``MapCallConfig`` precedent) — identical calls under
     * different modes are distinct instances.
     */
    struct ScopeState
    {
        Mode        mode{Mode::None};
        std::string recordable_id{};
    };

    /** The innermost scope (``Mode::None`` + empty id when no scope is active). */
    [[nodiscard]] HGRAPH_EXPORT const ScopeState &current_scope() noexcept;

    /** RAII mode scope. */
    class HGRAPH_EXPORT scope
    {
      public:
        explicit scope(Mode mode, std::string recordable_id = {});
        scope(const scope &) = delete;
        scope &operator=(const scope &) = delete;
        scope(scope &&) = delete;
        scope &operator=(scope &&) = delete;
        ~scope();
    };

    /**
     * RECOVER seed resolution is dispatched by backend id (RFC 0025):
     * core serves its own in-memory ids; an extension registers a resolver
     * for each backend id it owns — from its keyed installer, so a registry
     * reset-and-rebuild replays it. Selecting a backend with no registered
     * resolver during RECOVER is a pointed error.
     */
    using SeedResolver = Value (*)(GlobalStateView state, std::string_view fq_key,
                                   const TSValueTypeMetaData *schema, DateTime start_time);

    HGRAPH_EXPORT void register_seed_resolver(std::string_view backend, SeedResolver resolver);

    /** Reset transient wiring scopes (mode stack, seed-resolver registry —
        the latter replayed by the registration installers). */
    HGRAPH_EXPORT void reset() noexcept;
}  // namespace hgraph::record_replay

namespace hgraph
{
    class GraphView;
    class TraitsView;

    namespace record_replay
    {
        /** The graph trait carrying a recordable id (a ``Str`` value). */
        inline constexpr std::string_view RECORDABLE_ID_TRAIT = "recordable_id";

        /** True when the graph (or a parent) carries a recordable id trait. */
        [[nodiscard]] HGRAPH_EXPORT bool has_recordable_id(const GraphView &graph) noexcept;

        /**
         * Resolve the fully-qualified recordable id (Python's
         * ``get_fq_recordable_id``): the parent chain's ``recordable_id``
         * trait joined to the local id with ``.``. With no parent trait the
         * local id must be non-empty (throws otherwise); with a parent trait
         * an empty local id resolves to the parent id alone.
         */
        [[nodiscard]] HGRAPH_EXPORT std::string fq_recordable_id(const GraphView &graph,
                                                                 std::string_view recordable_id);

        /** The node-injectable form (``TraitsView`` parameter on a hook). */
        [[nodiscard]] HGRAPH_EXPORT std::string fq_recordable_id(const TraitsView &traits,
                                                                 std::string_view  recordable_id);

        /**
         * The RECOVER seed read ``component<G>``'s recovering pass-through
         * calls under ``Mode::Recover``: the last recorded value at or
         * before the start time. Dispatched by the effective backend —
         * in-memory ids read the ``:memory:`` buffer; extension ids go to
         * their registered ``SeedResolver``.
         */
        [[nodiscard]] HGRAPH_EXPORT Value recorded_seed_resolver(GlobalStateView            state,
                                                                 std::string_view           fq_key,
                                                                 const TSValueTypeMetaData *schema,
                                                                 DateTime start_time);

        /**
         * Comparison-result summary: (rows compared, mismatches) for a
         * ``compare`` run under ``fq_key`` (typically
         * ``<component-id>.__compare__``).
         *
         * The summary is CORE-NEUTRAL (RFC 0025): every comparison
         * implementation — the in-memory compare and any durable extension —
         * publishes this small value under a private core-owned
         * ``GlobalState`` key, and the query reads only that.  It has no
         * dependency on ``Frame``, Arrow, a store, or a specific backend, and
         * it is total: ``std::nullopt`` when nothing was published under the
         * key.  Detailed, implementation-specific comparison rows (if any)
         * stay wherever the implementation keeps them.
         */
        struct ComparisonSummary
        {
            std::size_t compared{0};
            std::size_t mismatches{0};
        };

        HGRAPH_EXPORT void publish_comparison_summary(GlobalStateView  state,
                                                      std::string_view fq_key,
                                                      ComparisonSummary summary);

        [[nodiscard]] HGRAPH_EXPORT std::optional<ComparisonSummary>
        comparison_summary(GlobalStateView state, std::string_view fq_key);
    }  // namespace record_replay
}  // namespace hgraph

#endif  // HGRAPH_TYPES_RECORD_REPLAY_H
