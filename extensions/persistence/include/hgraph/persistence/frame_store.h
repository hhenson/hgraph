#ifndef HGRAPH_PERSISTENCE_FRAME_STORE_H
#define HGRAPH_PERSISTENCE_FRAME_STORE_H

#include <hgraph/persistence/export.h>
#include <hgraph/types/frame.h>

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
 * consumer through the owning, type-erased ``persistence::store::FrameStore``; the store
 * itself knows nothing about record/replay.
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
        /** Reserved configuration spelling. The Arrow backend rejects this
         * directly; set AWS_PROFILE and select Ambient instead. */
        struct Profile
        {
            std::string name{};
        };
        /** Assume a role using credentials resolved from the ambient chain. */
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
        std::string bucket{};
        std::string prefix{};
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
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::optional<std::string> validate_key(std::string_view key);

    /** Throws ``std::invalid_argument`` when ``validate_key`` rejects the key. */
    HGRAPH_PERSISTENCE_EXPORT void require_valid_key(std::string_view key);

    /**
     * Shut the S3 layer down. Call before process exit when S3 has been used.
     *
     * Arrow requires a matching finalize for its process-global S3 init, and
     * it cannot be automated: running it from std::atexit or a static
     * destructor happens after Arrow's own statics are gone and terminates the
     * process. Safe to call when S3 was never used, and safe to call twice.
     */
    HGRAPH_PERSISTENCE_EXPORT void finalize_s3() noexcept;

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
    class HGRAPH_PERSISTENCE_EXPORT FrameStore final
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
