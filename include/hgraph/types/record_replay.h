#ifndef HGRAPH_TYPES_RECORD_REPLAY_H
#define HGRAPH_TYPES_RECORD_REPLAY_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/util/date_time.h>

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

    /** The record/replay modes (a flag set, mirroring Python's ``RecordReplayEnum``). */
    enum class Mode : unsigned
    {
        None         = 0,
        Record       = 1u << 0,
        Replay       = 1u << 1,
        Compare      = 1u << 2,
        ReplayOutput = 1u << 3,
        Reset        = 1u << 4,
        Recover      = 1u << 5,
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
        return (static_cast<unsigned>(mode) & static_cast<unsigned>(flag)) == static_cast<unsigned>(flag) &&
               flag != Mode::None;
    }

    /** The default (in-memory GlobalState buffer) record/replay model. Under
        this model ``record`` binds to the SPARSE, absolute-time
        ``:memory:<recordable_id>.<key>`` backend (``stdlib::sparse_record_impl``)
        — the upstream semantics that append across runs and tolerate arbitrary
        cross-cycle gaps (real-time alarms, components). ``replay`` is a single
        backend serving both models; see
        ``record_replay_table.rst`` (*In-memory record/replay — sparse vs dense*). */
    inline constexpr std::string_view IN_MEMORY = "InMemory";

    /** The DENSE cycle-aligned in-memory model (the graph testing harness).
        Under this model ``record`` binds to ``stdlib::dense_record_impl``:
        a plain-key ``List`` indexed by evaluation
        cycle (``MIN_ST + i*MIN_TD``), read back with ``get_recorded_values`` /
        ``Run.recorded``. Distinct model so a single ``record`` operator selects
        sparse (default) vs dense purely from wiring-time configuration. */
    inline constexpr std::string_view IN_MEMORY_DENSE = "InMemoryDense";

    /** The Arrow data-frame record/replay model (frame-store backed). */
    inline constexpr std::string_view DATA_FRAME = "DataFrame";

    /**
     * Explicit wiring-time configuration. ``model`` selects the backend
     * (overloads guard on it via ``requires_``); the date / as-of keys name
     * the bitemporal table columns; ``as_of`` overrides the as-of time
     * (unset = the evaluation clock).
     */
    struct Config
    {
        std::string             model{IN_MEMORY};
        std::string             date_key{"__date_time__"};
        std::string             as_of_key{"__as_of__"};
        std::optional<DateTime> as_of{};
    };

    /** Set the configuration in ``state`` before wiring. */
    HGRAPH_EXPORT void set_config(GlobalStateView state, Config config);

    /** The configuration in ``state`` (the default when no entry is present). */
    [[nodiscard]] HGRAPH_EXPORT Config config(GlobalStateView state);

    /** ``requires_``-friendly backend guard over a wiring state's model. */
    [[nodiscard]] HGRAPH_EXPORT bool model_is(GlobalStateView state, std::string_view model);

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
        scope(const scope &)            = delete;
        scope &operator=(const scope &) = delete;
        scope(scope &&)                 = delete;
        scope &operator=(scope &&)      = delete;
        ~scope();
    };

    /**
     * The type-erased keyed content store (P6 — ruled): recorded frames,
     * comparison results and related content read/write through this ops
     * table, with implementations REGISTERED like operator backends. The
     * default registration is an in-memory map; file / Arrow-dataset stores
     * register over it. Ops-table shape (no virtuals): first param is the
     * store context.
     */
    struct FrameStoreOps
    {
        void *context{nullptr};
        void (*write)(void *context, std::string_view key, Frame frame){nullptr};
        /** Empty ``Frame`` when the key is absent. */
        Frame (*read)(void *context, std::string_view key){nullptr};
        bool (*contains)(void *context, std::string_view key){nullptr};
        void (*clear)(void *context){nullptr};
    };

    /** Register a store (replacing the active one). */
    HGRAPH_EXPORT void set_frame_store(FrameStoreOps ops);
    /** The active store's ops. */
    [[nodiscard]] HGRAPH_EXPORT const FrameStoreOps &frame_store();

    /** Convenience wrappers over the active store. */
    HGRAPH_EXPORT void store_write(std::string_view key, Frame frame);
    [[nodiscard]] HGRAPH_EXPORT Frame store_read(std::string_view key);
    [[nodiscard]] HGRAPH_EXPORT bool store_contains(std::string_view key);

    /** Reset transient scopes and the registered content store to defaults. */
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
                                                                 std::string_view recordable_id);

        /**
         * The ``replay_const`` read (Python's wiring-time recorded-state
         * read; the const-evaluable form of ``replay``): the LAST recorded
         * value under ``fq_key`` with value-time <= ``tm`` and as-of <=
         * ``as_of``, reconstructed at schema ``meta``. Returns an empty
         * ``Value`` when nothing qualifies. A plain function per the
         * const_fn ruling — wiring code calls it directly (wrap with
         * ``const_`` for a source); the bridge exposes it eagerly.
         */
        [[nodiscard]] HGRAPH_EXPORT Value replay_const_value(GlobalStateView state,
                                                             std::string_view fq_key,
                                                             const ValueTypeMetaData *meta,
                                                             DateTime tm    = MAX_DT,
                                                             DateTime as_of = MAX_DT);

        /**
         * The RECOVER seed resolver (``GraphBuilder::StartSeed::ResolveFn``
         * shape): the last recorded value at or before the start time,
         * honouring the configured as-of override. Registered on seeds by
         * ``component<G>`` under ``Mode::Recover``.
         */
        [[nodiscard]] HGRAPH_EXPORT Value recorded_seed_resolver(
            GlobalStateView state, std::string_view fq_key,
            const TSValueTypeMetaData *schema, DateTime start_time);

        /**
         * Comparison-result summary: (rows compared, mismatches) from the
         * ``compare`` sink's frame under ``fq_key`` (typically
         * ``<component-id>.__compare__``). Throws when no comparison was
         * recorded under the key.
         */
        struct ComparisonSummary
        {
            std::size_t compared{0};
            std::size_t mismatches{0};
        };

        [[nodiscard]] HGRAPH_EXPORT ComparisonSummary comparison_summary(GlobalStateView state,
                                                                         std::string_view fq_key);
    }  // namespace record_replay
}  // namespace hgraph

#endif  // HGRAPH_TYPES_RECORD_REPLAY_H
