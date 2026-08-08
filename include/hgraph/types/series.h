#ifndef HGRAPH_TYPES_SERIES_H
#define HGRAPH_TYPES_SERIES_H

#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>

namespace arrow
{
    class Array;
}  // namespace arrow

namespace hgraph
{
    /**
     * The ``Series`` scalar — a first-class single-column value backed by an
     * **Apache Arrow** array (the column counterpart of ``Frame``; arrow is
     * the interchange layer, so pyarrow/polars/pandas reach the data
     * zero-copy). ``Series[T]`` on the wiring side documents the element
     * type; the arrow array carries the exact type at runtime.
     *
     * Value semantics mirror ``Frame``: the handle is shared-immutable
     * (arrow arrays are immutable), copying copies the handle, and equality
     * is HANDLE IDENTITY (cheap tick/delta suppression; content comparison
     * is an explicit operator). The shared handle is an opaque third-party
     * resource, not runtime graph structure.
     */
    struct Series
    {
        std::shared_ptr<arrow::Array> array{};

        [[nodiscard]] bool has_value() const noexcept { return array != nullptr; }

        friend bool operator==(const Series &lhs, const Series &rhs) noexcept
        {
            return lhs.array == rhs.array;
        }
    };

    /** Render as ``series[len]`` (diagnostics; not a data format). */
    std::ostream &operator<<(std::ostream &os, const Series &value);

    /** Element count (0 for an empty series) without exposing Arrow headers. */
    [[nodiscard]] std::int64_t series_length(const Series &value) noexcept;

    namespace static_schema_detail
    {
        template <>
        struct scalar_name<Series>
        {
            static constexpr std::string_view value{"series"};
        };
    }  // namespace static_schema_detail
}  // namespace hgraph

/** ``std::hash`` for ``Series`` (handle identity, matching equality). */
template <>
struct std::hash<hgraph::Series>
{
    [[nodiscard]] std::size_t operator()(const hgraph::Series &value) const noexcept
    {
        return std::hash<const void *>{}(static_cast<const void *>(value.array.get()));
    }
};

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/types/value/value_ops.h>

#include <nanobind/nanobind.h>

namespace hgraph
{
    /**
     * Series <-> pyarrow conversion binds onto the TYPE-ERASED ops (the
     * python_conversion_traits hook the scalar ops thunk already dispatches
     * through - no kind-switch in value_to_py). The arrow glue lives module-
     * side (pyarrow import + C Data Interface), so the module INSTALLS the
     * hooks at init; core dispatches through them uniformly.
     */
    template <>
    struct python_conversion_traits<Series>
    {
        using ToPythonHook   = nanobind::object (*)(const Series &);
        using FromPythonHook = Series (*)(nanobind::handle);

        [[nodiscard]] HGRAPH_EXPORT static ToPythonHook &to_python_hook() noexcept;
        [[nodiscard]] HGRAPH_EXPORT static FromPythonHook &from_python_hook() noexcept;
        HGRAPH_EXPORT static nanobind::object to_python(const Series &value);
        HGRAPH_EXPORT static Series from_python(nanobind::handle source);
    };
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_TYPES_SERIES_H
