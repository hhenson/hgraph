#ifndef HGRAPH_FABRIC_TYPES_H
#define HGRAPH_FABRIC_TYPES_H

#include <hgraph/fabric/export.h>

#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::fabric
{
    using DataVersion = Int;
    using RevisionId  = Int;

    inline constexpr Int REVISION_FORMAT_VERSION{1};
    inline constexpr std::size_t MAX_DATA_ID_BYTES{4U * 1024U};
    inline constexpr std::size_t MAX_REVISION_DEPENDENCIES{65'535U};
    inline constexpr std::size_t MAX_METADATA_BYTES{16U * 1024U * 1024U};

    enum class SubscriptionMode : std::int64_t
    {
        Live,
        Replay,
        Snapshot,
    };

    [[nodiscard]] constexpr std::string_view enum_name(SubscriptionMode value) noexcept
    {
        switch (value)
        {
            case SubscriptionMode::Live:
                return "Live";
            case SubscriptionMode::Replay:
                return "Replay";
            case SubscriptionMode::Snapshot:
                return "Snapshot";
        }
        return "Unknown";
    }

    inline std::ostream &operator<<(std::ostream &stream, SubscriptionMode value)
    {
        return stream << enum_name(value);
    }

    using DataDependency =
        Bundle<"hgraph.fabric::DataDependency", Field<"data_id", Str>,
               Field<"version", Int>>;

    using DataRevision =
        Bundle<"hgraph.fabric::DataRevision", Field<"format_version", Int>,
               Field<"data_id", Str>, Field<"revision", Int>,
               Field<"output_version", Int>,
               Field<"dependencies", HomogeneousTuple<DataDependency>>,
               Field<"self_predecessor", Int>, Field<"as_of", DateTime>>;

    struct DataDependencyInput
    {
        Str         data_id{};
        DataVersion version{};

        friend bool operator==(const DataDependencyInput &,
                               const DataDependencyInput &) = default;
    };

    struct DataRevisionInput
    {
        Int                              format_version{REVISION_FORMAT_VERSION};
        Str                              data_id{};
        RevisionId                       revision{};
        DataVersion                      output_version{};
        std::vector<DataDependencyInput> dependencies{};
        std::optional<DataVersion>       self_predecessor{};
        DateTime                         as_of{MIN_DT};

        friend bool operator==(const DataRevisionInput &,
                               const DataRevisionInput &) = default;
    };

    /** Validate one logical data id. Data ids are non-empty UTF-8 strings,
        contain no control code points, and are bounded to 4 KiB. Slash is
        permitted because durable key encoding is a separate concern. */
    HGRAPH_FABRIC_EXPORT void require_data_id(std::string_view data_id);

    /** Canonical unsigned UTF-8 byte ordering used for dependency records. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT bool
    canonical_data_id_less(std::string_view lhs, std::string_view rhs) noexcept;

    /** Register the fabric enum and structural scalar schemas. Idempotent for
        one type-registry lifetime. */
    HGRAPH_FABRIC_EXPORT void register_fabric_types();
}  // namespace hgraph::fabric

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<fabric::SubscriptionMode>
    {
        static constexpr std::string_view value{"hgraph.fabric::SubscriptionMode"};
    };
}  // namespace hgraph::static_schema_detail

#if HGRAPH_ENABLE_PYTHON_USER_NODES
namespace hgraph
{
    template <>
    struct python_conversion_traits<fabric::SubscriptionMode>
    {
        static nb::object to_python(const fabric::SubscriptionMode &value)
        {
            return nb::cast(value);
        }

        static fabric::SubscriptionMode from_python(nb::handle source)
        {
            if (nb::hasattr(source, "value")) { source = source.attr("value"); }
            return static_cast<fabric::SubscriptionMode>(
                nb::cast<std::int64_t>(source));
        }
    };
}  // namespace hgraph
#endif

#endif  // HGRAPH_FABRIC_TYPES_H
