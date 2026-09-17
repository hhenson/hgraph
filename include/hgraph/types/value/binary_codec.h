#ifndef HGRAPH_TYPES_VALUE_BINARY_CODEC_H
#define HGRAPH_TYPES_VALUE_BINARY_CODEC_H

// A compact binary codec for values -- RFC 0017's field-wise path.
//
// The unit is a ``Value``, not a time series, because ``ts_delta.h`` already
// reduces a cycle's time-series delta to a canonical ``Value`` and back. This
// is the encoding that was missing underneath it: everything a transport or a
// store needs above the byte level already existed.
//
// The wire form is canonical little-endian, packed, no interior padding, no
// field names -- the schema supplies them -- with counts and lengths as LEB128
// varints and numeric atoms fixed-width.
//
// Structurally this is the serializer-ops pattern ``JsonConverter`` also uses:
// one converter per ``ValueTypeMetaData``, synthesized recursively over the
// schema and interned. That is where to look for the shape; the two share no
// code and encode nothing alike.
//
// NOT implemented yet, and deliberately so: RFC 0017's ``trivial_layout`` fast
// path, which copies a whole StoragePlan image. It is an optimisation over
// this path and the RFC requires a conformance test proving the two produce
// byte-identical output, so the field-wise path has to exist first.

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;

    /** Decode-wide limits, including zero-byte values and nested collections. */
    struct BinaryDecodeLimits
    {
        std::uint64_t max_work{1'000'000};
        std::size_t max_depth{256};
    };

    /** A read cursor. Subreaders borrow the parent's budget and cannot outlive it. */
    struct HGRAPH_CLASS_EXPORT BinaryReader
    {
        explicit BinaryReader(std::string_view bytes = {}, std::size_t at = 0,
                              BinaryDecodeLimits limits = {}) noexcept
            : buffer(bytes), offset(at), local_{limits} {}

        std::string_view buffer{};
        std::size_t offset{0};

        [[nodiscard]] std::size_t remaining() const noexcept
        { return offset <= buffer.size() ? buffer.size() - offset : 0; }
        /** Consume bytes, or throw when the buffer is short. */
        [[nodiscard]] const std::byte *take(std::size_t count);
        /** Charge work before allocating or iterating an untrusted count. */
        void consume_work(std::uint64_t count);
        /** Consume a bounded payload; its decoder shares this reader's budget. */
        [[nodiscard]] BinaryReader subreader(std::size_t count);
        /** Scope one recursive decoding step; charges work and bounds depth. */
        [[nodiscard]] auto enter()
        {
            enter_value();
            return make_scope_exit([this]() noexcept { --budget().depth; });
        }

      private:
        struct Budget
        {
            BinaryDecodeLimits limits{};
            std::uint64_t work{0};
            std::size_t depth{0};
        };
        Budget local_{};
        Budget *shared_{nullptr};
        Budget &budget() noexcept { return shared_ ? *shared_ : local_; }
        void enter_value();
    };

    /**
     * Interned per-schema binary converter.
     */
    class HGRAPH_CLASS_EXPORT BinaryConverter
    {
      public:
        using WriteFn = void (*)(const BinaryConverter &, const ValueView &, std::string &);
        using ReadFn  = Value (*)(const BinaryConverter &, BinaryReader &);
        using HashFn = std::uint64_t (*)(const BinaryConverter &, const ValueView &);

        BinaryConverter() noexcept;
        BinaryConverter(const BinaryConverter &) = default;
        BinaryConverter &operator=(const BinaryConverter &) = default;
        BinaryConverter(BinaryConverter &&other) noexcept;
        BinaryConverter &operator=(BinaryConverter &&other) noexcept;
        void swap(BinaryConverter &other) noexcept;

        void write(const ValueView &view, std::string &out) const { write_(*this, view, out); }
        [[nodiscard]] Value read(BinaryReader &reader) const;

        WriteFn                              write_;
        ReadFn                               read_;
        HashFn                               hash_;
        const ValueTypeMetaData             *meta{nullptr};
        ValueTypeRef                         binding{nullptr};
        /** Byte width of a trivially copyable atom; 0 when not one. */
        std::size_t                          atom_size{0};
        bool                                 realization_bound{false};
        std::vector<const BinaryConverter *> children{};   ///< element / (key, value) / fields
        std::unordered_map<const ValueTypeMetaData *, const BinaryConverter *> write_alternatives{};
        std::unordered_map<std::string_view, const BinaryConverter *> read_alternatives{};
    };

    /**
     * The interned converter for ``meta``; synthesizes on first use.
     *
     * Build-time machinery: may lock. A per-tick caller resolves once and
     * keeps the result.
     */
    [[nodiscard]] HGRAPH_EXPORT const BinaryConverter &binary_converter(const ValueTypeMetaData *meta);

    /** Immutable, run-owned binary plan. Captures closed Bundle alternatives
     * and retains the realization that owns their storage bindings. Resolve
     * once at wiring/start and use its lock-free read/write methods per tick.
     */
    class HGRAPH_CLASS_EXPORT BoundBinaryConverter
    {
      public:
        BoundBinaryConverter() noexcept = default;
        [[nodiscard]] explicit operator bool() const noexcept { return impl_ != nullptr; }
        [[nodiscard]] ValueTypeRef binding() const noexcept;
        /** Stable process-independent hash for worker assignment. */
        [[nodiscard]] std::uint64_t portable_hash(const ValueView &view) const;
        void write(const ValueView &view, std::string &out) const;
        [[nodiscard]] Value read(BinaryReader &reader) const;

      private:
        struct Impl;
        explicit BoundBinaryConverter(std::shared_ptr<const Impl> impl) : impl_(std::move(impl)) {}
        std::shared_ptr<const Impl> impl_{};
        friend HGRAPH_EXPORT BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *meta);
    };

    [[nodiscard]] HGRAPH_EXPORT BoundBinaryConverter bind_binary_converter(const ValueTypeMetaData *meta);

    /** Clear the interned converters (registry reset). */
    HGRAPH_EXPORT void clear_binary_converters() noexcept;

    /** Encode one value; the schema is the reader's, not the stream's. */
    [[nodiscard]] HGRAPH_EXPORT std::string to_binary_string(const ValueView &view);
    HGRAPH_EXPORT void to_binary_string(const ValueView &view, std::string &out);

    /** Decode one value of ``meta`` from ``bytes``. */
    [[nodiscard]] HGRAPH_EXPORT Value from_binary_string(const ValueTypeMetaData *meta,
                                                         std::string_view bytes, BinaryDecodeLimits limits = {});

    // --- LEB128 -------------------------------------------------------------
    // Counts and lengths are varints because they are almost always small;
    // numeric and temporal atoms stay fixed-width, where a branchy decode
    // would be the wrong trade.

    HGRAPH_EXPORT void write_varint(std::uint64_t value, std::string &out);
    [[nodiscard]] HGRAPH_EXPORT std::uint64_t read_varint(BinaryReader &reader);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_BINARY_CODEC_H
