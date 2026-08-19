#ifndef HGRAPH_PERSISTENCE_RECORDING_OPTIONS_H
#define HGRAPH_PERSISTENCE_RECORDING_OPTIONS_H

/**
 * @file recording_options.h
 * Durable recording option vocabularies (RFC 0019, RFC 0025 checkpoint 5).
 *
 * These select what a DURABLE recording stores.  Core's memory and testing
 * backends have no as-of column and no removal rows, so the vocabularies
 * belong to the extension that implements them; core's ``record`` operator
 * contract is unchanged and carries neither.
 *
 * Moved out of core at checkpoint 5.  The C++ names moved outright (RFC
 * 0025: pre-1.0 C++ constants are replaced rather than aliased); the Python
 * spellings keep deprecated aliases in ``hgraph`` for the deprecation
 * window.
 */

#include <hgraph/persistence/export.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <string_view>

namespace hgraph::persistence
{
    /**
     * Whether a recording carries an as-of column (RFC 0019).
     *
     * ``Inherit`` defers to the wiring-time table configuration, which
     * is what makes the configuration LOCAL with a global default rather than
     * a second override registry keyed on name.
     */
    enum class RecordAsOf : std::int64_t
    {
        Inherit,
        Track,   ///< an as-of column carrying the evaluation as-of
        Omit,    ///< no as-of column at all
    };

    /**
     * Whether a recording carries a removed flag per TSD level (RFC 0019).
     *
     * Omitting them means a removal records NOTHING - the stream simply stops
     * carrying that key, which is how most data streams are consumed. Tracking
     * them makes a removal an explicit row.
     */
    enum class RecordRemoves : std::int64_t
    {
        Inherit,
        Omit,
        Track,
    };
}  // namespace hgraph::persistence

namespace hgraph::static_schema_detail
{
    // The registered scalar names are unchanged by the move: they are the
    // schema identity a durable recording's wiring already resolves.
    template <>
    struct scalar_name<hgraph::persistence::RecordAsOf>
    {
        static constexpr std::string_view value{"RecordAsOf"};
    };

    template <>
    struct scalar_name<hgraph::persistence::RecordRemoves>
    {
        static constexpr std::string_view value{"RecordRemoves"};
    };
}  // namespace hgraph::static_schema_detail

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>

namespace hgraph
{
    /** Python conversion binds to the type AT DEFINITION (type-erasure rule).
        These cross as their integer member, which the Python ``Enum``
        spellings accept either way. */
#define HGRAPH_PERSISTENCE_PYTHON_ENUM_CONVERSION(EnumType)                                    \
    template <> struct python_conversion_traits<persistence::EnumType>                         \
    {                                                                                          \
        static nb::object to_python(const persistence::EnumType &value)                        \
        {                                                                                      \
            return nb::cast(static_cast<std::int64_t>(value));                                 \
        }                                                                                      \
        static persistence::EnumType from_python(nb::handle source)                            \
        {                                                                                      \
            if (nb::hasattr(source, "value")) { source = source.attr("value"); }               \
            return static_cast<persistence::EnumType>(nb::cast<std::int64_t>(source));         \
        }                                                                                      \
    }

    HGRAPH_PERSISTENCE_PYTHON_ENUM_CONVERSION(RecordAsOf);
    HGRAPH_PERSISTENCE_PYTHON_ENUM_CONVERSION(RecordRemoves);

#undef HGRAPH_PERSISTENCE_PYTHON_ENUM_CONVERSION
}  // namespace hgraph
#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES

#endif  // HGRAPH_PERSISTENCE_RECORDING_OPTIONS_H
