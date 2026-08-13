#ifndef HGRAPH_TYPES_FRAME_STORE_H
#define HGRAPH_TYPES_FRAME_STORE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/record_replay.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

/**
 * Object-store frame persistence (RFC 0016).
 *
 * A configured content store: keyed frames written to memory, a local
 * filesystem, or S3, in Arrow IPC or Parquet. Record/replay is the first
 * consumer through ``record_replay::FrameStoreOps``; the store itself knows
 * nothing about it.
 *
 * The frame is the unit. A frame carries its own description in its Arrow
 * schema metadata (RFC 0001), and both formats preserve it, so a stored frame
 * answers "what produced this?" without the store holding anything on the
 * side. There is no index object and no group construct.
 */
namespace hgraph::store
{
    /** Serialisation format. */
    enum class Format
    {
        /** Arrow IPC. Available wherever hgraph links libarrow. */
        ArrowIpc,
        /** Parquet. Requires libparquet — see HGRAPH_WITH_PARQUET. */
        Parquet,
    };

    enum class Compression
    {
        Default,
        None,
        Snappy,
        Zstd,
    };

    /** Frames live in process memory; nothing is persisted. */
    struct MemoryLocation
    {
    };

    /** Frames live under a directory. */
    struct LocalLocation
    {
        std::string root{};
    };

    /** How S3 credentials are obtained. */
    struct Credentials
    {
        /** The standard AWS chain: environment, profile, container, instance. */
        struct Ambient
        {
        };
        struct Explicit
        {
            std::string                access_key_id{};
            std::string                secret_access_key{};
            std::optional<std::string> session_token{};
        };
        /** A named profile from the shared credentials file. */
        struct Profile
        {
            std::string name{};
        };
        struct AssumeRole
        {
            std::string                role_arn{};
            std::optional<std::string> session_name{};
        };

        std::variant<Ambient, Explicit, Profile, AssumeRole> source{Ambient{}};
    };

    /** Frames live in an S3 bucket. */
    struct S3Location
    {
        std::string                bucket{};
        std::string                prefix{};
        /** Unset resolves the region through the ambient chain. */
        std::optional<std::string> region{};
        /** Set to point at an S3-compatible endpoint (MinIO, LocalStack). */
        std::optional<std::string> endpoint_override{};
        Credentials                credentials{};
    };

    using Location = std::variant<MemoryLocation, LocalLocation, S3Location>;

    struct FrameStoreConfig
    {
        Location    location{MemoryLocation{}};
        Format      format{Format::ArrowIpc};
        Compression compression{Compression::Default};
        /** Reject a write whose key already exists (RFC 0016 decision). */
        bool        immutable{true};
    };

    /**
     * Validate a store key (RFC 0016 decision: transparent paths, validated).
     *
     * A key is an object-path suffix and ``/`` nests, so a bucket browses as a
     * tree. Rejected: empty keys, absolute keys, trailing ``/``, empty
     * segments, ``.``/``..`` segments, backslashes, and control characters.
     *
     * Applied by EVERY backend, memory included, so a key that would fail
     * against S3 fails identically in a unit test.
     *
     * @returns the reason when invalid; ``std::nullopt`` when the key is valid.
     */
    [[nodiscard]] HGRAPH_EXPORT std::optional<std::string> validate_key(std::string_view key);

    /** Throws ``std::invalid_argument`` when ``validate_key`` rejects the key. */
    HGRAPH_EXPORT void require_valid_key(std::string_view key);

    /**
     * Shut the S3 layer down. Call before process exit when S3 has been used.
     *
     * Arrow requires a matching finalize for its process-global S3 init, and
     * it cannot be automated: running it from std::atexit or a static
     * destructor happens after Arrow's own statics are gone and terminates the
     * process. Safe to call when S3 was never used, and safe to call twice.
     */
    HGRAPH_EXPORT void finalize_s3() noexcept;

    /** True when this build links a Parquet implementation. */
    [[nodiscard]] HGRAPH_EXPORT bool parquet_available() noexcept;

    /**
     * A configured store, owning whatever the backend needs — an Arrow
     * filesystem, its credentials, the format choice.
     *
     * Held by ``shared_ptr`` and installed into ``GlobalState``, which owns it
     * for the run and releases it with the run. This follows the existing
     * time-zone-provider pattern rather than inventing a second lifetime rule.
     */
    class HGRAPH_EXPORT FrameStore
    {
      public:
        virtual ~FrameStore();

        /** Write a frame. Rejects an existing key when the store is immutable. */
        virtual void write(std::string_view key, Frame frame, std::optional<Compression> compression = {}) = 0;
        /** An empty ``Frame`` when the key is absent. */
        [[nodiscard]] virtual Frame read(std::string_view key) = 0;
        [[nodiscard]] virtual bool contains(std::string_view key) = 0;
        virtual void clear() = 0;

        [[nodiscard]] const FrameStoreConfig &config() const noexcept { return config_; }

      protected:
        explicit FrameStore(FrameStoreConfig config) : config_(std::move(config)) {}
        FrameStoreConfig config_;
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
    [[nodiscard]] HGRAPH_EXPORT std::shared_ptr<FrameStore> make_frame_store(FrameStoreConfig config);

    /** The ops table for a store, for the record/replay seam. */
    [[nodiscard]] HGRAPH_EXPORT record_replay::FrameStoreOps frame_store_ops(const std::shared_ptr<FrameStore> &store);
}  // namespace hgraph::store

namespace hgraph::record_replay
{
    /**
     * Install a configured store for the active run.
     *
     * ``GlobalState`` owns the store and releases it with the run, so two runs
     * configured with different destinations do not disturb each other. This
     * mirrors ``set_config``, which is already ``GlobalState``-scoped; the
     * process-global ``set_frame_store(FrameStoreOps)`` remains for the default
     * registration and for a caller owning its context for the process.
     */
    HGRAPH_EXPORT void set_frame_store(GlobalStateView state, std::shared_ptr<store::FrameStore> frame_store);

    /** Remove the store selected for this run. Subsequent operations use the
        process-lifetime fallback store. */
    HGRAPH_EXPORT void clear_frame_store(GlobalStateView state);

    /** The store installed for this run, or nullptr when none is. */
    [[nodiscard]] HGRAPH_EXPORT std::shared_ptr<store::FrameStore> frame_store(GlobalStateView state);
}  // namespace hgraph::record_replay

#endif  // HGRAPH_TYPES_FRAME_STORE_H
