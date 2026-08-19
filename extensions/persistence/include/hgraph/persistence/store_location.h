#ifndef HGRAPH_PERSISTENCE_STORE_LOCATION_H
#define HGRAPH_PERSISTENCE_STORE_LOCATION_H

#include <hgraph/persistence/export.h>

#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace hgraph::persistence::store
{
    /** Objects live in process memory; nothing is persisted. */
    struct MemoryLocation
    {
    };

    /** Objects live under a directory. */
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
        /**
         * Reserved configuration spelling. Set AWS_PROFILE and use Ambient
         * with the Arrow-backed implementation.
         */
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

    /** Objects live in an S3 bucket. */
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

    /**
     * Validate a transparent object key.
     *
     * A key is a relative, slash-separated object path. Empty and absolute
     * keys, empty segments, ``.``/``..`` segments, trailing slashes,
     * backslashes and control characters are rejected identically by every
     * backend.
     */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::optional<std::string> validate_key(
        std::string_view key);

    /** Throws ``std::invalid_argument`` when ``validate_key`` rejects the key. */
    HGRAPH_PERSISTENCE_EXPORT void require_valid_key(std::string_view key);

    /**
     * Shut the Arrow S3 layer down after every S3-backed store has been reset.
     * Safe when S3 was never used and safe to call twice.
     */
    HGRAPH_PERSISTENCE_EXPORT void finalize_s3() noexcept;
}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_STORE_LOCATION_H
