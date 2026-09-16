#ifndef HGRAPH_TYPES_VALUE_BINARY_CODEC_H
#define HGRAPH_TYPES_VALUE_BINARY_CODEC_H

// A compact binary codec for values -- RFC 0017's field-wise path.
//
// The unit is a ``Value``, not a time series, because ``ts_delta.h`` already
// reduces a cycle's time-series delta to a canonical ``Value`` and back. This
// is the encoding that was missing underneath it.
//
// The converter mirrors ``JsonConverter`` deliberately: same synthesis, same
// interning, one converter per ``ValueTypeMetaData`` built recursively over
// the schema. What differs is the wire form -- canonical little-endian,
// packed, no interior padding, no field names, counts as LEB128 varints.
//
// JSON is not an alternative here. It costs a parse and a decimal render per
// value on a path that runs every engine cycle, which is the wrong order of
// magnitude for a distributed child.
//
// NOT implemented yet, and deliberately so: RFC 0017's ``trivial_layout`` fast
// path, which copies a whole StoragePlan image. It is an optimisation over
// this path and the RFC requires a conformance test proving the two produce
// byte-identical output, so the field-wise path has to exist first.

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;

    /** A read cursor over an encoded buffer. */
    struct HGRAPH_CLASS_EXPORT BinaryReader
    {
        std::string_view buffer{};
        std::size_t      offset{0};

        [[nodiscard]] std::size_t remaining() const noexcept { return buffer.size() - offset; }
        /** Consume ``count`` bytes, or throw when the buffer is short. */
        [[nodiscard]] const std::byte *take(std::size_t count);
    };

    /**
     * Interned per-schema binary converter -- the serializer-ops pattern, as
     * ``JsonConverter`` uses.
     */
    class HGRAPH_CLASS_EXPORT BinaryConverter
    {
      public:
        using WriteFn = void (*)(const BinaryConverter &, const ValueView &, std::string &);
        using ReadFn  = Value (*)(const BinaryConverter &, BinaryReader &);

        void write(const ValueView &view, std::string &out) const { write_(*this, view, out); }
        [[nodiscard]] Value read(BinaryReader &reader) const { return read_(*this, reader); }

        WriteFn                              write_{nullptr};
        ReadFn                               read_{nullptr};
        const ValueTypeMetaData             *meta{nullptr};
        ValueTypeRef                         binding{nullptr};
        /** Byte width of a trivially copyable atom; 0 when not one. */
        std::size_t                          atom_size{0};
        std::vector<const BinaryConverter *> children{};   ///< element / (key, value) / fields
    };

    /**
     * The interned converter for ``meta``; synthesizes on first use.
     *
     * Build-time machinery: may lock, exactly as ``json_converter`` does. A
     * per-tick caller resolves once and keeps the result.
     */
    [[nodiscard]] HGRAPH_EXPORT const BinaryConverter &binary_converter(const ValueTypeMetaData *meta);

    /** Clear the interned converters (registry reset). */
    HGRAPH_EXPORT void clear_binary_converters() noexcept;

    /** Encode one value; the schema is the reader's, not the stream's. */
    [[nodiscard]] HGRAPH_EXPORT std::string to_binary_string(const ValueView &view);
    HGRAPH_EXPORT void to_binary_string(const ValueView &view, std::string &out);

    /** Decode one value of ``meta`` from ``bytes``. */
    [[nodiscard]] HGRAPH_EXPORT Value from_binary_string(const ValueTypeMetaData *meta,
                                                         std::string_view bytes);

    // --- LEB128 -------------------------------------------------------------
    // Counts and lengths are varints because they are almost always small;
    // numeric and temporal atoms stay fixed-width, where a branchy decode
    // would be the wrong trade.

    HGRAPH_EXPORT void write_varint(std::uint64_t value, std::string &out);
    [[nodiscard]] HGRAPH_EXPORT std::uint64_t read_varint(BinaryReader &reader);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_BINARY_CODEC_H
