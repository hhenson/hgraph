#ifndef HGRAPH_TYPES_FRAME_H
#define HGRAPH_TYPES_FRAME_H

#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string_view>

namespace arrow
{
    class Table;
}  // namespace arrow

namespace hgraph
{
    class Value;

    /**
     * The ``Frame`` scalar — a first-class table value backed by an **Apache
     * Arrow** table (design record: *Record/replay, tables and const_fn*,
     * P4 / Q-arrow-dep ruling 2026-07-04). Arrow is the interchange layer:
     * Polars, pandas and other Arrow-native frame libraries reach the data
     * zero-copy; user code names ``Frame``, never Arrow directly.
     *
     * A schema-less ``Frame`` is legal. When a schema qualifies a frame at a
     * boundary the rules are: an INPUT schema is a *minimum* requirement
     * (the arriving frame must contain at least those columns at those
     * types), an OUTPUT schema is *exact*. (The typed ``Frame<Schema>``
     * wiring marker lands with the schema-matching layer; the value kind
     * here is the substrate.)
     *
     * Value semantics: the handle is shared-immutable (Arrow tables are
     * immutable); copying a ``Frame`` copies the handle, not the data.
     * Equality is HANDLE IDENTITY (same table object, or both empty) — cheap
     * and honest for tick/delta suppression; content comparison is an
     * explicit operator when needed. The shared handle is an opaque
     * third-party resource (like ``Str``'s heap buffer), not runtime graph
     * structure — the no-shared-ptr rule governs the latter.
     */
    struct Frame
    {
        std::shared_ptr<arrow::Table> table{};

        [[nodiscard]] bool has_value() const noexcept { return table != nullptr; }
        /** True when the Arrow schema carries any reserved hgraph frame metadata. */
        [[nodiscard]] HGRAPH_EXPORT bool has_metadata() const noexcept;

        friend bool operator==(const Frame &lhs, const Frame &rhs) noexcept
        {
            return lhs.table == rhs.table;
        }
    };

    /** Reserved Arrow schema-metadata keys used by the field-wise codec. */
    inline constexpr std::string_view frame_metadata_prefix{"hgraph.metadata."};
    inline constexpr std::string_view frame_metadata_schema_key{"hgraph.metadata.schema"};
    inline constexpr std::string_view frame_metadata_version_key{"hgraph.metadata.version"};
    inline constexpr std::string_view frame_metadata_field_prefix{"hgraph.metadata.field."};

    /**
     * Encode a named Bundle value into the Arrow table's schema metadata.
     * Atomic fields use their plain string form; structured fields use JSON.
     * Existing non-hgraph Arrow metadata is preserved.
     */
    [[nodiscard]] HGRAPH_EXPORT Frame with_frame_metadata(Frame frame, Value metadata);
    /**
     * Decode metadata against a declared named Bundle schema. When no schema
     * is supplied, the optional encoded schema marker is used for reflective
     * loading; markerless metadata therefore requires an explicit schema.
     */
    [[nodiscard]] HGRAPH_EXPORT Value frame_metadata(
        const Frame &frame, const ValueTypeMetaData *metadata_schema = nullptr);
    /** True when the Arrow schema carries any reserved hgraph metadata entry. */
    [[nodiscard]] HGRAPH_EXPORT bool has_frame_metadata(const Frame &frame) noexcept;
    /** Compare only the reserved hgraph metadata entries on two Arrow schemas. */
    [[nodiscard]] HGRAPH_EXPORT bool frame_metadata_equal(
        const Frame &lhs, const Frame &rhs) noexcept;
    /** Explicitly discard hgraph metadata while preserving unrelated Arrow metadata. */
    [[nodiscard]] HGRAPH_EXPORT Frame without_frame_metadata(Frame frame);

    /** Render as ``frame[rows x cols]`` (diagnostics; not a data format). */
    std::ostream &operator<<(std::ostream &os, const Frame &value);

    /** Row count (0 for an empty frame) without exposing Arrow headers. */
    [[nodiscard]] HGRAPH_EXPORT std::int64_t frame_rows(const Frame &value) noexcept;

    namespace static_schema_detail
    {
        template <>
        struct scalar_name<Frame>
        {
            static constexpr std::string_view value{"frame"};
        };
    }  // namespace static_schema_detail
}  // namespace hgraph

/** ``std::hash`` for ``Frame`` (handle identity, matching equality). */
template <>
struct std::hash<hgraph::Frame>
{
    [[nodiscard]] std::size_t operator()(const hgraph::Frame &value) const noexcept
    {
        return std::hash<const void *>{}(static_cast<const void *>(value.table.get()));
    }
};

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/types/value/value_ops.h>

#include <nanobind/nanobind.h>

namespace hgraph
{
    /** Frame <-> pyarrow binds onto the type-erased ops (module installs the
        hooks; core dispatches uniformly - no kind-switch). */
    template <>
    struct python_conversion_traits<Frame>
    {
        using ToPythonHook   = nanobind::object (*)(const Frame &);
        using FromPythonHook = Frame (*)(nanobind::handle);

        [[nodiscard]] HGRAPH_EXPORT static ToPythonHook &to_python_hook() noexcept;
        [[nodiscard]] HGRAPH_EXPORT static FromPythonHook &from_python_hook() noexcept;
        HGRAPH_EXPORT static nanobind::object to_python(const Frame &value);
        HGRAPH_EXPORT static Frame from_python(nanobind::handle source);
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_TYPES_FRAME_H
