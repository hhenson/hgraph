#ifndef HGRAPH_PERSISTENCE_FRAME_STORE_H
#define HGRAPH_PERSISTENCE_FRAME_STORE_H

#include <hgraph/persistence/export.h>
#include <hgraph/persistence/store_location.h>
#include <hgraph/types/frame.h>

#include <memory>
#include <optional>
#include <string_view>

/**
 * Object-store frame persistence (RFC 0016).
 *
 * A configured content store: keyed frames written to memory, a local
 * filesystem, or S3, in Arrow IPC or Parquet. Record/replay is the first
 * consumer through the owning, type-erased ``persistence::store::FrameStore``;
 * the store itself knows nothing about record/replay.
 *
 * The frame is the unit. A frame carries its own description in its Arrow
 * schema metadata (RFC 0001), and both formats preserve it, so a stored frame
 * answers "what produced this?" without the store holding anything on the
 * side. There is no index object and no group construct.
 */
namespace hgraph::persistence::store
{
    /** Serialisation format. */
    enum class Format
    {
        /** Arrow IPC. Available wherever hgraph links libarrow. */
        ArrowIpc,
        /** Parquet. Requires libparquet — see HGRAPH_PERSISTENCE_WITH_PARQUET. */
        Parquet,
    };

    enum class Compression
    {
        Default,
        None,
        Snappy,
        Zstd,
    };

    struct FrameStoreConfig
    {
        Location    location{MemoryLocation{}};
        Format      format{Format::ArrowIpc};
        Compression compression{Compression::Default};
        /** Reject a write whose key already exists (RFC 0016 decision). */
        bool immutable{true};
    };

    /**
     * Passive operations table for one frame-store representation.
     *
     * The table has static lifetime. It owns no state: ``FrameStore`` supplies
     * the erased context on every call and keeps that context alive. Concrete
     * memory, filesystem, S3 and Python representations remain private to
     * their implementation translation units.
     */
    struct FrameStoreOps
    {
        void (*write)(void *context, std::string_view key, Frame frame,
                      std::optional<Compression> compression);
        /** Empty ``Frame`` when the key is absent. */
        Frame (*read)(void *context, std::string_view key);
        bool (*contains)(void *context, std::string_view key);
        void (*clear)(void *context);
    };

    /** True when this build links a Parquet implementation. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT bool parquet_available() noexcept;

    /**
     * Owning, type-erased handle to a configured store.
     *
     * The handle shares ownership of an erased context and dispatches only
     * through ``FrameStoreOps``. It has no virtual functions and exposes no
     * representation type. Copies refer to the same store, as required when
     * ``GlobalState`` is copied into and back out of a graph run.
     *
     * Default and moved-from handles use a canonical empty ops table. Queries
     * therefore remain safe without testing an ops pointer for null; a write
     * fails explicitly rather than silently discarding data.
     */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT FrameStore final
    {
      public:
        FrameStore() noexcept;

        /** Bind an owned erased context to a static, complete operations table. */
        FrameStore(std::shared_ptr<void> context, const FrameStoreOps &ops);

        FrameStore(const FrameStore &) = default;
        FrameStore &operator=(const FrameStore &) = default;
        FrameStore(FrameStore &&other) noexcept;
        FrameStore &operator=(FrameStore &&other) noexcept;
        ~FrameStore() = default;

        /** Write a frame. Rejects an existing key when the store is immutable. */
        void write(std::string_view key, Frame frame,
                   std::optional<Compression> compression = {}) const;
        /** An empty ``Frame`` when the key is absent. */
        [[nodiscard]] Frame read(std::string_view key) const;
        [[nodiscard]] bool  contains(std::string_view key) const;
        /** True only for native stores that implement immutable segment keys. */
        [[nodiscard]] bool supports_segmented_recordings() const noexcept;
        void               clear() const;

        /** True when this handle owns a concrete representation. */
        [[nodiscard]] explicit operator bool() const noexcept;

        /** Release the representation and rebind the canonical empty table. */
        void reset() noexcept;

      private:
        [[nodiscard]] static const FrameStoreOps &empty_ops() noexcept;

        std::shared_ptr<void> context_{};
        const FrameStoreOps  *ops_{&empty_ops()};
    };

    /**
     * Build a store from configuration.
     *
     * Throws when the configuration cannot be honoured — an unreachable
     * bucket, a Parquet format in a build without Parquet. It never silently
     * degrades to memory: environment selection is a deployment decision, and
     * a store that quietly fell back would turn a configuration error into
     * output that looks wrong much later.
     */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT FrameStore make_frame_store(FrameStoreConfig config);
}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_FRAME_STORE_H
