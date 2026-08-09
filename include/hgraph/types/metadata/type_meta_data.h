//
// Created by Howard Henson on 20/04/2026.
//

#ifndef HGRAPH_CPP_ROOT_TYPE_META_DATA_H
#define HGRAPH_CPP_ROOT_TYPE_META_DATA_H

#include <cstdint>
#include <string_view>

namespace hgraph
{
    /**
     * Discriminator for the two top-level schema families: ``Value`` (raw
     * payloads) and ``TimeSeries`` (TS wrappers).
     *
     * Stored on every ``TypeMetaData`` so generic code can downcast safely
     * to ``ValueTypeMetaData`` or ``TSValueTypeMetaData``.
     */
    enum class MetaCategory : uint8_t
    {
        /** ``ValueTypeMetaData`` — value-layer schemas. */
        Value,
        /** ``TSValueTypeMetaData`` — time-series-layer schemas. */
        TimeSeries,
    };

    /**
     * Shared base for interned schema descriptors.
     *
     * The runtime registry interns both value schemas and time-series
     * schemas. This small base keeps the common category and display name
     * in one place so generic code can hold a base pointer and dispatch by
     * category.
     */
    struct TypeMetaData
    {
        /** Construct with a category and an optional human-readable display name. */
        constexpr explicit TypeMetaData(MetaCategory category_ = MetaCategory::Value,
                                        const char *display_name_ = nullptr) noexcept
            : category(category_)
            , display_name(display_name_)
        {
        }

        /** Family discriminator; see ``MetaCategory``. */
        MetaCategory category{MetaCategory::Value};
        /** Optional human-readable name; pointer must out-live the metadata. */
        const char *display_name{nullptr};

        /** True when ``category == MetaCategory::Value``. */
        [[nodiscard]] constexpr bool is_value() const noexcept { return category == MetaCategory::Value; }
        /** True when ``category == MetaCategory::TimeSeries``. */
        [[nodiscard]] constexpr bool is_time_series() const noexcept { return category == MetaCategory::TimeSeries; }

        /** ``display_name`` as a ``string_view``; empty when unset. */
        [[nodiscard]] constexpr std::string_view name() const noexcept
        {
            return display_name ? std::string_view{display_name} : std::string_view{};
        }
    };
}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_TYPE_META_DATA_H
