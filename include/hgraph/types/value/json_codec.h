#ifndef HGRAPH_TYPES_VALUE_JSON_CODEC_H
#define HGRAPH_TYPES_VALUE_JSON_CODEC_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>

#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    namespace json_detail
    {
        /** Minimal recursive-descent JSON reader; parsing is meta-directed. */
        struct Reader;
    }  // namespace json_detail

    /**
     * Interned per-schema JSON converter — the serializer-ops pattern from the
     * record/replay/table design record (design: *Record/replay, tables and
     * const_fn*, step 1). One converter per ``ValueTypeMetaData``, synthesized
     * recursively over the schema and interned (the C++ form of Python's
     * cached ``to_json_builder``/``from_json_builder`` closure pipelines).
     *
     * The wire format mirrors the Python implementation
     * (``ext/main/hgraph/_impl/_operators/_to_json.py``): atomics as JSON
     * scalars (dates ``"YYYY-MM-DD"``, datetimes ``"YYYY-MM-DD
     * HH:MM:SS.ffffff"``, times ``"HH:MM:SS.ffffff"``, timedeltas
     * ``"D:H:M:S.ffffff"``), bundles/tuples as objects/arrays, lists and sets
     * as arrays, maps as objects (non-string keys rendered then quoted).
     */
    class HGRAPH_EXPORT JsonConverter
    {
      public:
        using WriteFn = void (*)(const JsonConverter &, const ValueView &, std::string &);
        using ReadFn  = Value (*)(const JsonConverter &, json_detail::Reader &);

        void write(const ValueView &view, std::string &out) const { write_(*this, view, out); }
        [[nodiscard]] Value read(json_detail::Reader &reader) const;

        /** Which atomic wire form this converter uses (Atomic kinds only). */
        enum class AtomicTag : unsigned char
        {
            None,
            Bool,
            Int,
            Float,
            Str,
            Date,
            DateTime,
            TimeDelta,
            Time,
            CivilDateTime,
            Period,
            ZoneId,
            ZonedDateTime,
            InstantRange,
            CivilDateRange,
            InstantRangeSet,
            CivilDateRangeSet,
        };

        WriteFn                            write_{nullptr};
        ReadFn                             read_{nullptr};
        const ValueTypeMetaData           *meta{nullptr};
        ValueTypeRef binding{nullptr};   ///< construction binding for reads
        AtomicTag                          atomic_tag{AtomicTag::None};
        std::vector<const JsonConverter *> children{};         ///< element / (key, value) / fields
        std::vector<std::string_view>      names{};            ///< bundle field names
    };

    /**
     * The interned converter for ``meta``. Synthesizes (and caches) on first
     * use; throws ``std::logic_error`` for schemas with no JSON form.
     * Build-time machinery: may lock; never called on the per-tick path
     * directly (operators capture the converter at start/first use).
     */
    [[nodiscard]] HGRAPH_EXPORT const JsonConverter &json_converter(const ValueTypeMetaData *meta);

    /** Clear the interned converters (registry reset — see registry_reset.h). */
    HGRAPH_EXPORT void clear_json_converters() noexcept;

    /** Serialize any value view to a JSON string. */
    [[nodiscard]] HGRAPH_EXPORT std::string to_json_string(const ValueView &view);

    /** Parse ``text`` into an owned value of schema ``meta``. */
    [[nodiscard]] HGRAPH_EXPORT Value from_json_string(const ValueTypeMetaData *meta, std::string_view text);

    /** Parse with a pre-resolved converter (the node-State fast path). */
    [[nodiscard]] HGRAPH_EXPORT Value from_json_string(const JsonConverter &converter, std::string_view text);

    /**
     * Fragment cursor: lets TS-aware operators (the friendly JSON delta
     * forms) drive object/array structure themselves while delegating leaf
     * values to the meta-directed converters.
     */
    namespace json_fragment
    {
        struct Cursor
        {
            std::string_view text{};
            std::size_t      offset{0};
        };

        /** True (and advances) when the next non-whitespace char is ``token``. */
        HGRAPH_EXPORT bool consume(Cursor &cursor, char token);
        /** True (and advances) on a ``null`` keyword. */
        HGRAPH_EXPORT bool consume_null(Cursor &cursor);
        /** Peek the next non-whitespace char (0 at end). */
        HGRAPH_EXPORT char peek(Cursor &cursor);
        /** Parse a JSON string token. */
        HGRAPH_EXPORT std::string parse_string(Cursor &cursor);
        /** Parse one meta-directed value at the cursor. */
        HGRAPH_EXPORT Value parse_value(const JsonConverter &converter, Cursor &cursor);
        /** Throw a parse error at the cursor position. */
        [[noreturn]] HGRAPH_EXPORT void fail(Cursor &cursor, std::string_view message);
    }  // namespace json_fragment

    /**
     * Node-State payload carrying the converter resolved in ``start`` (the
     * lifecycle form of the builder pattern: compose once, read per tick).
     */
    struct JsonCodecState
    {
        const JsonConverter *converter{nullptr};
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_JSON_CODEC_H
