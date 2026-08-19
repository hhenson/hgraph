#ifndef HGRAPH_PERSISTENCE_RECORDING_STORE_H
#define HGRAPH_PERSISTENCE_RECORDING_STORE_H

#include <hgraph/persistence/export.h>
#include <hgraph/persistence/frame_store.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <string>
#include <string_view>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/nanobind.h>
#endif

/**
 * The durable recording store surface (RFC 0025, checkpoint 4): the
 * GlobalState-scoped frame-store selection, the segmented-recording object
 * protocol (RFC 0019), and the durable const read. Extracted from core
 * ``types/record_replay.h`` — core sheds every store dependency; the
 * extension owns the ``__hgraph.record_replay.frame_store__`` key.
 *
 * There is NO process-global fallback store (checkpoint 4 decision): a
 * store lives in exactly one ``GlobalState`` scope, installed explicitly
 * (``set_frame_store``), by the Python activation path
 * (``install_fresh_frame_store``), or lazily at recording start
 * (``ensure_frame_store``). Store lifetime is therefore always the state
 * scope that selected it — nothing survives a run boundary by accident.
 */
namespace hgraph::persistence
{
    /** The backend identifier this extension registers. */
    inline constexpr std::string_view FRAME_BACKEND = "hgraph.persistence.frame";

    /** Install a configured store for one ``GlobalState`` scope. The state
        owns a copy of the erased handle and therefore shares its context
        across every graph run bound to that state. */
    HGRAPH_PERSISTENCE_EXPORT void set_frame_store(GlobalStateView state,
                                                   store::FrameStore frame_store);

    /** Remove the selected store from ``state``. */
    HGRAPH_PERSISTENCE_EXPORT void clear_frame_store(GlobalStateView state);

    /** The state-selected store, or an empty handle when none is installed. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT store::FrameStore frame_store(GlobalStateView state);

    /** Install a FRESH default (in-memory, immutable-key) store, replacing
        any previously selected native store — explicitly selecting the frame
        backend starts a new recording session, so immutable keys from an
        earlier run never leak into the next. A Python compatibility store is
        user-owned and left installed. */
    HGRAPH_PERSISTENCE_EXPORT void install_fresh_frame_store(GlobalStateView state);

    /** The state-selected store, installing the default store first when the
        state has none (the recording-start lazy path). */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT store::FrameStore ensure_frame_store(
        GlobalStateView state);

    /** Store operations over the state-selected store. Reads return an empty
        ``Frame`` / false when no store is installed; a write with no store
        installs the default store first. */
    HGRAPH_PERSISTENCE_EXPORT void store_write(GlobalStateView state, std::string_view key,
                                               Frame frame);
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT Frame store_read(GlobalStateView state,
                                                             std::string_view key);
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT bool store_contains(GlobalStateView state,
                                                                std::string_view key);

    /** RFC 0019's immutable segmented-recording object protocol. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT Frame segmented_recording_marker();
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT Frame segmented_recording_manifest(
        std::size_t segments, std::size_t rows);
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT bool is_segmented_recording(const Frame &frame) noexcept;
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::string segment_key(std::string_view key,
                                                                    std::size_t segment);
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::string completion_key(std::string_view key);

    /**
     * The durable ``replay_const`` read: the LAST recorded value under
     * ``fq_key`` with value-time <= ``tm`` and as-of <= ``as_of``,
     * reconstructed at schema ``meta`` from the state-selected store
     * (segment-aware). Returns an empty ``Value`` when nothing qualifies.
     */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT Value replay_const_value(
        GlobalStateView state, std::string_view fq_key, const ValueTypeMetaData *meta,
        DateTime tm = MAX_DT, DateTime as_of = MAX_DT);

    /** The RECOVER seed resolver this extension registers with core for its
        backend id (``record_replay::register_seed_resolver``). */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT Value frame_seed_resolver(
        GlobalStateView state, std::string_view fq_key, const TSValueTypeMetaData *schema,
        DateTime start_time);

    /** Register the durable backend: the frame-backed operator overloads and
        the recover seed resolver, as the keyed installer
        ``"hgraph.persistence"`` (replayed by every registry rebuild).
        Idempotent; called by the native module on import and by C++
        embedders that link the extension directly. */
    HGRAPH_PERSISTENCE_EXPORT void register_frame_backend();

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    /**
     * Associate the extension's Python recording-option enums with their
     * native scalars, from inside the keyed installer so a registry
     * reset-and-rebuild replays the association (RFC 0003, checkpoint 3).
     */
    HGRAPH_PERSISTENCE_EXPORT void register_recording_option_enums(
        nanobind::object record_as_of, nanobind::object record_removes);
#endif
}  // namespace hgraph::persistence

#endif  // HGRAPH_PERSISTENCE_RECORDING_STORE_H
