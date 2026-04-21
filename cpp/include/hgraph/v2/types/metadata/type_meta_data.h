//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_TYPE_META_DATA_H

#include <cstdint>
#include <string_view>

namespace hgraph::v2
{
    enum class MetaCategory : uint8_t
    {
        Value,
        TimeSeries,
    };

    /**
     * Shared base for interned schema descriptors.
     *
     * The v2 registry interns both value schemas and time-series schemas. This
     * small base keeps the common category and display name in one place.
     */
    struct TypeMetaData
    {
        constexpr explicit TypeMetaData(MetaCategory category_ = MetaCategory::Value,
                                        const char *display_name_ = nullptr) noexcept
            : category(category_)
            , display_name(display_name_)
        {
        }

        MetaCategory category{MetaCategory::Value};
        const char *display_name{nullptr};

        [[nodiscard]] constexpr bool is_value() const noexcept { return category == MetaCategory::Value; }
        [[nodiscard]] constexpr bool is_time_series() const noexcept { return category == MetaCategory::TimeSeries; }

        [[nodiscard]] constexpr std::string_view name() const noexcept
        {
            return display_name ? std::string_view{display_name} : std::string_view{};
        }
    };
}  // namespace hgraph::v2

#endif  // HGRAPH_CPP_ROOT_TYPE_META_DATA_H
