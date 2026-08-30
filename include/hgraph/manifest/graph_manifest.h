#ifndef HGRAPH_MANIFEST_GRAPH_MANIFEST_H
#define HGRAPH_MANIFEST_GRAPH_MANIFEST_H

/**
 * @file graph_manifest.h
 * The serializable graph manifest (RFC 0022, stage 1).
 *
 * ``capture`` walks a FINISHED ``GraphBuilder`` — after ``Wiring::finish``
 * and before ``make_executor`` — and produces the canonical descriptor of
 * the wired program: graph identity, the ordered node table (kind, all
 * endpoint schemas, behaviour flags, selector vectors, implementation
 * identity, canonical scalar-argument bytes), the ordered edge table with
 * source kinds and structural paths, and the push-source boundary.
 *
 * The DESCRIPTOR BYTES are the identity; ``ManifestId`` (SHA-256 with
 * format-version domain separation) is a lookup handle only. Validation
 * decodes both descriptors and reports path-addressed differences — never
 * just "hash mismatch".
 *
 * Stage 1 scope: static graphs whose scalar arguments carry canonically
 * encodable values. A node holding a live handle in its scalars (nested
 * graph contexts, Python callables) makes the graph non-manifestable and
 * ``capture`` raises a path-addressed error — the conservative refusal RFC
 * 0022 requires. Nested templates and extension identity follow in stage 2.
 */

#include <hgraph/hgraph_export.h>
#include <hgraph/util/sha256.h>

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph
{
    class GraphBuilder;
}

namespace hgraph::manifest
{
    /** The manifest format version encoded into every descriptor. */
    inline constexpr std::uint16_t k_manifest_format_version = 1;

    /** SHA-256 lookup handle over a canonical descriptor. */
    struct ManifestId
    {
        util::Sha256Digest digest{};

        friend bool operator==(const ManifestId &, const ManifestId &) = default;
    };

    /** A graph cannot be captured; ``path()`` addresses the refusing entry. */
    class HGRAPH_CLASS_EXPORT ManifestCaptureError : public std::runtime_error
    {
      public:
        ManifestCaptureError(std::string path, const std::string &message);

        [[nodiscard]] const std::string &path() const noexcept { return path_; }

      private:
        std::string path_;
    };

    /** One path-addressed difference between two manifests. */
    struct ManifestDifference
    {
        std::string path;
        std::string expected;
        std::string actual;
    };

    /** Path-addressed comparison outcome; empty differences = identical. */
    struct HGRAPH_CLASS_EXPORT ValidationResult
    {
        std::vector<ManifestDifference> differences{};

        [[nodiscard]] bool identical() const noexcept { return differences.empty(); }
    };

    /** Immutable description of a fully resolved wired program. */
    class HGRAPH_CLASS_EXPORT GraphManifest final
    {
      public:
        [[nodiscard]] std::uint16_t format_version() const noexcept { return format_version_; }
        [[nodiscard]] ManifestId id() const noexcept { return id_; }
        [[nodiscard]] std::span<const std::byte> canonical_descriptor() const noexcept
        {
            return descriptor_;
        }

      private:
        friend HGRAPH_EXPORT GraphManifest capture(const GraphBuilder &graph);
        friend HGRAPH_EXPORT GraphManifest decode_graph(std::span<const std::byte> bytes);

        GraphManifest(std::uint16_t format_version, std::vector<std::byte> descriptor);

        std::uint16_t format_version_{k_manifest_format_version};
        std::vector<std::byte> descriptor_{};
        ManifestId id_{};
    };

    /** Capture the manifest of a finished graph builder. */
    [[nodiscard]] HGRAPH_EXPORT GraphManifest capture(const GraphBuilder &graph);

    /** Compare two manifests, reporting path-addressed differences (bounded). */
    [[nodiscard]] HGRAPH_EXPORT ValidationResult validate(const GraphManifest &expected,
                                                          const GraphManifest &actual);

    /** Encode a manifest to its durable byte form (version-framed descriptor). */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::byte> encode(const GraphManifest &manifest);

    /** Decode a manifest, recomputing and verifying its id. */
    [[nodiscard]] HGRAPH_EXPORT GraphManifest decode_graph(std::span<const std::byte> bytes);
}  // namespace hgraph::manifest

#endif  // HGRAPH_MANIFEST_GRAPH_MANIFEST_H
