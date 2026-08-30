#ifndef HGRAPH_TYPES_VALUE_JSON_CODEC_H
#define HGRAPH_TYPES_VALUE_JSON_CODEC_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>

#include <string>
#include <string_view>
#include <memory>
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
     * (``release/0.5:hgraph/_impl/_operators/_to_json.py``): atomics as JSON
     * scalars (dates ``"YYYY-MM-DD"``, datetimes ``"YYYY-MM-DD
     * HH:MM:SS.ffffff"``, times ``"HH:MM:SS.ffffff"``, timedeltas
     * ``"D:H:M:S.ffffff"``), bundles/tuples as objects/arrays, lists and sets
     * as arrays, maps as objects (non-string keys rendered then quoted).
     */
    class HGRAPH_CLASS_EXPORT JsonConverter
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
        /** Pre-resolved concrete alternatives in a run-bound converter. */
        std::vector<const JsonConverter *> alternatives{};
        /** Opaque immutable parser configuration captured by a bound plan. */
        std::shared_ptr<const void> read_context{};
        bool realization_bound{false};
        bool polymorphic{false};
    };

    /**
     * The interned converter for ``meta``. Synthesizes (and caches) on first
     * use; throws ``std::logic_error`` for schemas with no JSON form.
     * Build-time/ad-hoc machinery: may lock. Evaluation paths use
     * ``bind_json_converter`` and retain the resulting owned plan.
     */
    [[nodiscard]] HGRAPH_EXPORT const JsonConverter &json_converter(const ValueTypeMetaData *meta);

    /**
     * A JSON conversion plan bound to the active graph type-realisation
     * snapshot. Construction may consult type registries and the snapshot;
     * read/write perform no schema lookup and take no type-system or
     * realisation lock. The plan owns its converter tree and retains the
     * realisation snapshot backing graph-local bindings; canonical metadata
     * remains registry-scoped. The plan is intended to live in run-local
     * node/service state.
     */
    class HGRAPH_CLASS_EXPORT BoundJsonConverter final
    {
      public:
        BoundJsonConverter() noexcept = default;
        BoundJsonConverter(const BoundJsonConverter &) noexcept = default;
        BoundJsonConverter &operator=(const BoundJsonConverter &) noexcept = default;
        BoundJsonConverter(BoundJsonConverter &&) noexcept = default;
        BoundJsonConverter &operator=(BoundJsonConverter &&) noexcept = default;
        ~BoundJsonConverter() = default;

        [[nodiscard]] explicit operator bool() const noexcept;
        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept;
        void write(const ValueView &view, std::string &out) const;
        [[nodiscard]] Value read(json_detail::Reader &reader) const;

      private:
        struct Impl;
        explicit BoundJsonConverter(std::shared_ptr<const Impl> impl) noexcept;
        [[nodiscard]] const JsonConverter &converter() const;

        std::shared_ptr<const Impl> impl_{};

        friend BoundJsonConverter bind_json_converter(const ValueTypeMetaData *meta);
    };

    /** Resolve and own a complete run-local conversion plan for ``meta``. */
    [[nodiscard]] HGRAPH_EXPORT BoundJsonConverter
    bind_json_converter(const ValueTypeMetaData *meta);

    /** Clear the interned converters (registry reset — see registry_reset.h). */
    HGRAPH_EXPORT void clear_json_converters() noexcept;

    /**
     * Register an additional Python-``strptime``-style format accepted by
     * schema-directed JSON temporal reads. Date/datetime formats are shared;
     * ``time_only`` selects the independent time-of-day list.
     *
     * Registration is process-wide, idempotent, and safe to perform while
     * other threads are reading JSON. ISO 8601 and the built-in compatibility
     * formats are always tried before registered extensions. A run-bound
     * converter snapshots the registered formats when it is bound, so a new
     * registration affects subsequently bound plans rather than changing an
     * active graph run.
     */
    HGRAPH_EXPORT void register_json_datetime_format(
        std::string format, bool time_only = false);

    /** Serialize any value view to a JSON string. */
    [[nodiscard]] HGRAPH_EXPORT std::string to_json_string(const ValueView &view);

    /** Parse ``text`` into an owned value of schema ``meta``. */
    [[nodiscard]] HGRAPH_EXPORT Value from_json_string(const ValueTypeMetaData *meta, std::string_view text);

    /** Parse with an interned converter. Ad-hoc use; realisation may lock. */
    [[nodiscard]] HGRAPH_EXPORT Value from_json_string(const JsonConverter &converter, std::string_view text);

    /** Parse with a run-bound converter (the lock-free evaluation path). */
    [[nodiscard]] HGRAPH_EXPORT Value from_json_string(
        const BoundJsonConverter &converter, std::string_view text);

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
        /** Parse one value through a run-bound conversion plan. */
        HGRAPH_EXPORT Value parse_value(const BoundJsonConverter &converter,
                                        Cursor &cursor);
        /** Throw a parse error at the cursor position. */
        [[noreturn]] HGRAPH_EXPORT void fail(Cursor &cursor, std::string_view message);
    }  // namespace json_fragment

    /**
     * Node-State payload carrying the converter resolved in ``start`` (the
     * lifecycle form of the builder pattern: compose once, read per tick).
     */
    struct JsonCodecState
    {
        BoundJsonConverter converter{};
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_JSON_CODEC_H
