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

#include <hgraph/manifest/canonical.h>

#include <stdexcept>
#include <string>

namespace hgraph
{
    struct ValueTypeMetaData;
    struct TSValueTypeMetaData;
    class ValueView;
}  // namespace hgraph

namespace hgraph::manifest
{
    /** A value/scalar cannot participate in a manifest; ``what()`` says why. */
    class UnsupportedManifestValue : public std::runtime_error
    {
      public:
        using std::runtime_error::runtime_error;
    };

    /** Append the canonical descriptor for a value-layer schema. */
    void append_value_descriptor(CanonicalWriter &writer, const ValueTypeMetaData *meta);

    /** Append the canonical descriptor for a time-series schema. */
    void append_ts_descriptor(CanonicalWriter &writer, const TSValueTypeMetaData *meta);

    /** The canonical descriptor bytes for a value-layer schema. */
    [[nodiscard]] std::vector<std::byte> value_descriptor(const ValueTypeMetaData *meta);

    /** The canonical descriptor bytes for a time-series schema. */
    [[nodiscard]] std::vector<std::byte> ts_descriptor(const TSValueTypeMetaData *meta);

    /**
     * Append a scalar value in canonical schema-directed binary form.
     *
     * Supported: the core atomic scalars (bool/int/float/str and the
     * engine date/time family), enums, and tuples/bundles/lists/sets/maps
     * of supported values (set and map content is emitted in canonical
     * encoded-key order). Throws ``UnsupportedManifestValue`` otherwise.
     */
    void encode_manifest_scalar(CanonicalWriter &writer, const ValueView &value);

    /**
     * Whether a schema's values can be canonically encoded; when not,
     * ``reason`` (optional) receives a stable human-readable explanation.
     */
    [[nodiscard]] bool manifest_scalar_encodable(const ValueTypeMetaData *meta,
                                                 std::string *reason = nullptr);
}  // namespace hgraph::manifest

#endif  // HGRAPH_MANIFEST_SCHEMA_DESCRIPTOR_H
