#ifndef HGRAPH_TYPES_VALUE_BINARY_SESSION_H
#define HGRAPH_TYPES_VALUE_BINARY_SESSION_H

// What one binary encoding shares across the values in it (RFC 0040).
//
// ``to_binary_string`` is stateless: the reader supplies the schema, and
// nothing a value says can outlive that value. An ``Any`` cannot be written
// that way, because its schema is only known when it is met, and naming it in
// full beside every value would cost more than the value. A session is the
// place for that knowledge: a schema table, written once, and one bound
// converter per entry. A value whose schema the reader already knows adds
// nothing to the table and pays nothing for it.
//
// A checkpoint image (RFC 0039) already carried exactly this pair for its own
// use. It now uses the session, so there is one implementation.

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/binary_codec.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace hgraph
{
    class SchemaTableWriter;
    class SchemaTableReader;

    /**
     * The writing half: schemas are added as values meet them.
     *
     * Build-time machinery on first sight of a schema -- binding a converter
     * locks -- and lock-free after it. A caller that encodes every cycle keeps
     * the session, not just the converter.
     */
    class HGRAPH_CLASS_EXPORT BinaryEncodeSession
    {
      public:
        explicit BinaryEncodeSession(BinaryProfile profile = BinaryProfile::Compact);
        ~BinaryEncodeSession();
        BinaryEncodeSession(const BinaryEncodeSession &) = delete;
        BinaryEncodeSession &operator=(const BinaryEncodeSession &) = delete;

        [[nodiscard]] BinaryProfile profile() const noexcept;
        /** The table itself, for a format that also names time-series schemas. */
        [[nodiscard]] SchemaTableWriter &schemas() noexcept;
        /** Table index of ``schema``, adding it and everything it is built from. */
        [[nodiscard]] std::size_t value_ref(const ValueTypeMetaData *schema);
        /** The converter for table entry ``index``, bound on first use. */
        [[nodiscard]] const BoundBinaryConverter &converter_at(std::size_t index);
        /** Encode ``view`` as entry ``index``; nested ``Any`` values extend the table. */
        void write(std::size_t index, const ValueView &view, std::string &out);
        /** Encode ``view`` under a converter the caller holds -- a schema the
            reader already knows, which therefore stays out of the table. */
        void write(const BoundBinaryConverter &converter, const ValueView &view, std::string &out);
        /** Append the tables. Call once every value has been written. */
        void write_tables(std::string &out) const;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    /** The reading half: the tables are read first, then the values. */
    class HGRAPH_CLASS_EXPORT BinaryDecodeSession
    {
      public:
        explicit BinaryDecodeSession(BinaryProfile profile = BinaryProfile::Compact);
        ~BinaryDecodeSession();
        BinaryDecodeSession(const BinaryDecodeSession &) = delete;
        BinaryDecodeSession &operator=(const BinaryDecodeSession &) = delete;

        [[nodiscard]] BinaryProfile profile() const noexcept;
        /** Read the tables, resolving and checking every entry. */
        void read_tables(BinaryReader &reader);
        [[nodiscard]] const SchemaTableReader &schemas() const noexcept;
        /** The converter for table entry ``index``; throws when it names none. */
        [[nodiscard]] const BoundBinaryConverter &converter_at(std::size_t index);
        /** Decode one value of entry ``index``; nested ``Any`` values resolve here. */
        [[nodiscard]] Value read(std::size_t index, BinaryReader &reader);
        /** Decode one value under a converter the caller holds. */
        [[nodiscard]] Value read(const BoundBinaryConverter &converter, BinaryReader &reader);

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    // --- a self-contained frame ------------------------------------------------
    //
    //   u8   profile
    //   u8   revision of that profile's encoding
    //   u32  payload length, little-endian
    //        payload
    //        session tables
    //
    // The root schema is the reader's, as with ``from_binary_string``, and is
    // not in the tables: they carry only the schemas the value introduced, so
    // a value with no ``Any`` in it pays two bytes for them. The payload comes
    // first so that it is written in place, with no copy once its tables are
    // known.
    //
    // The revision exists because a profile's bytes change as RFC 0040's stages
    // land while its name does not. Revision 0 of either profile is the RFC
    // 0017 field-wise encoding. A reader refuses a revision it does not know,
    // by number, rather than misreading it.

    [[nodiscard]] HGRAPH_EXPORT std::uint8_t binary_profile_revision(BinaryProfile profile) noexcept;

    HGRAPH_EXPORT void encode_binary_frame(const ValueView &view, BinaryProfile profile, std::string &out);
    [[nodiscard]] HGRAPH_EXPORT std::string encode_binary_frame(const ValueView &view,
                                                                 BinaryProfile profile = BinaryProfile::Compact);
    /** The profile a frame declares; throws when ``bytes`` is not a frame. */
    [[nodiscard]] HGRAPH_EXPORT BinaryProfile binary_frame_profile(std::string_view bytes);
    [[nodiscard]] HGRAPH_EXPORT Value decode_binary_frame(const ValueTypeMetaData *meta, std::string_view bytes,
                                                          BinaryDecodeLimits limits = {});
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_BINARY_SESSION_H
