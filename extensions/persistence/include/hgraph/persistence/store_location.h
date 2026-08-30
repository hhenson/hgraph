#ifndef HGRAPH_PERSISTENCE_STORE_LOCATION_H
#define HGRAPH_PERSISTENCE_STORE_LOCATION_H

#include <hgraph/persistence/export.h>

#include <optional>
#include <cstdint>
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

    /** Encode an arbitrary string as one key segment, base64url without
        padding. Object keys are a restricted alphabet with '/' as the only
        separator, so a value that may contain either has to be encoded before
        it becomes part of a path. Generic: the caller owns the key layout,
        this owns the alphabet. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::string
    encode_key_segment(std::string_view value);

    /** Inverse of encode_key_segment. Throws std::invalid_argument on input
        that is not base64url. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::string
    decode_key_segment(std::string_view encoded);

    /** Render a positive integer zero-padded to `width`, so keys containing it
        sort lexicographically in numeric order -- which is what makes prefix
        listing return a range in sequence. Throws when the value is
        non-positive or does not fit. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::string
    encode_ordered_ordinal(std::int64_t value, std::size_t width);

    /** Inverse of encode_ordered_ordinal, rejecting a segment of the wrong
        width or with non-digit characters. */
    [[nodiscard]] HGRAPH_PERSISTENCE_EXPORT std::int64_t
    decode_ordered_ordinal(std::string_view encoded, std::size_t width);

    /**
     * Shut the Arrow S3 layer down after every S3-backed store has been reset.
     * Safe when S3 was never used and safe to call twice.
     */
    HGRAPH_PERSISTENCE_EXPORT void finalize_s3() noexcept;
}  // namespace hgraph::persistence::store

#endif  // HGRAPH_PERSISTENCE_STORE_LOCATION_H
