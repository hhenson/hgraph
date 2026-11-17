#ifndef HGRAPH_MANIFEST_SCHEMA_DESCRIPTOR_H
#define HGRAPH_MANIFEST_SCHEMA_DESCRIPTOR_H

/**
 * @file schema_descriptor.h
 * Canonical structural schema descriptors and scalar encoding (RFC 0022).
 *
 * A descriptor is the schema's durable identity: canonical bytes derived
 * from structure (kind, wire-affecting flags, children, nominal names for
 * named bundles and enums), never from interned pointers or diagnostic
 * labels. Two schemas the runtime treats as equivalent produce identical
 * descriptor bytes; anything the runtime distinguishes produces different
 * bytes (the conformance reference is ``time_series_schema_equivalent``).
 *
 * Descriptor synthesis is cold-path (wiring/attachment time) and computed
 * on demand — no cache, no lock, no registry-reset interaction.
 *
 * ``encode_manifest_scalar`` writes an immutable scalar Value in canonical
 * schema-directed binary form. A scalar whose type has no canonical
 * encoding (live handles, python-owned payloads, unregistered scalar
 * kinds) raises ``UnsupportedManifestValue`` carrying the reason — RFC
 * 0022's conservative refusal, surfaced with a path by the caller.
 */

#include <hgraph/hgraph_export.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/types/metadata/value_type_meta_data.h>

#include <cstdint>
#include <stdexcept>
#include <string>

namespace hgraph
{
    struct TSValueTypeMetaData;
    class ValueView;
}  // namespace hgraph

namespace hgraph::manifest
{
    /** A value/scalar cannot participate in a manifest; ``what()`` says why. */
    class HGRAPH_CLASS_EXPORT UnsupportedManifestValue : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /** Append the canonical descriptor for a value-layer schema. */
    HGRAPH_EXPORT void append_value_descriptor(CanonicalWriter &writer, const ValueTypeMetaData *meta);

    /**
     * The flags of a value schema as its descriptor writes them.
     *
     * ``ValueTypeFlags::Frame`` was released on the bit that
     * ``ValueTypeFlags::FixedEmpty`` has. The two are separate flags now, and
     * a descriptor still writes ``Frame`` on that bit, so the descriptor of a
     * typed frame is the one stored tables and manifests hold. Nothing is
     * lost: no schema has both traits, a frame being an atomic and
     * ``FixedEmpty`` a trait of a List, and the kind is written beside the
     * flags. For the same reason a typed frame's descriptor keeps the fixed
     * size of zero it was written with while it read as a fixed empty List.
     */
    [[nodiscard]] constexpr std::uint64_t descriptor_flags(ValueTypeFlags flags) noexcept
    {
        constexpr std::uint32_t frame_as_written = 1u << 14;
        auto bits = static_cast<std::uint32_t>(flags);
        if (has_flag(flags, ValueTypeFlags::Frame))
        {
            bits = (bits & ~static_cast<std::uint32_t>(ValueTypeFlags::Frame)) | frame_as_written;
        }
        return bits;
    }

    /** Append the canonical descriptor for a time-series schema. */
    HGRAPH_EXPORT void append_ts_descriptor(CanonicalWriter &writer, const TSValueTypeMetaData *meta);

    /** The canonical descriptor bytes for a value-layer schema. */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::byte> value_descriptor(const ValueTypeMetaData *meta);

    /** The canonical descriptor bytes for a time-series schema. */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::byte> ts_descriptor(const TSValueTypeMetaData *meta);

    /**
     * Append a scalar value in canonical schema-directed binary form.
     *
     * Supported: the core atomic scalars (bool/int/float/str and the
     * engine date/time family), enums, and tuples/bundles/lists/sets/maps
     * of supported values (set and map content is emitted in canonical
     * encoded-key order). Throws ``UnsupportedManifestValue`` otherwise.
     */
    HGRAPH_EXPORT void encode_manifest_scalar(CanonicalWriter &writer, const ValueView &value);

    /**
     * Whether a schema's values can be canonically encoded; when not,
     * ``reason`` (optional) receives a stable human-readable explanation.
     */
    [[nodiscard]] HGRAPH_EXPORT bool manifest_scalar_encodable(
        const ValueTypeMetaData *meta, std::string *reason = nullptr);
}  // namespace hgraph::manifest

#endif  // HGRAPH_MANIFEST_SCHEMA_DESCRIPTOR_H
