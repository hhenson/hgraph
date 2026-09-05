#ifndef HGRAPH_TYPES_TYPE_CARRIER_H
#define HGRAPH_TYPES_TYPE_CARRIER_H

#include <cstddef>
#include <functional>
#include <optional>
#include <ostream>
#include <string_view>
#include <variant>

namespace hgraph
{
    struct TSValueTypeMetaData;
    struct ValueTypeMetaData;

    /** The three kinds of type variable a resolution map can bind. */
    enum class ResolutionKind
    {
        TimeSeries,
        Scalar,
        Size,
        Any
    };

    /**
     * One resolution binding as a value: a wiring-time *type argument*
     * (RFC 0033). A ``type[...]`` parameter receives a time-series schema,
     * a scalar schema or a fixed size — exactly the three ``ResolutionKind``
     * forms — so the carrier is a closed sum type: one pointer-or-size plus
     * the alternative index. It is registered as a standard scalar (like
     * ``WiredFn``) so it travels through ``Value`` and the resolved
     * operator call; it never enters a node's scalar layout from the C++
     * side (``TypeArg`` parameters are consumed at dispatch) and never
     * ticks.
     */
    struct TypeCarrier
    {
        using Binding = std::variant<const TSValueTypeMetaData *,   // ResolutionKind::TimeSeries
                                     const ValueTypeMetaData *,     // ResolutionKind::Scalar
                                     std::size_t>;                  // ResolutionKind::Size

        /** Never empty: the alternative *is* the form. Default-constructed is a null TS carrier. */
        Binding binding{static_cast<const TSValueTypeMetaData *>(nullptr)};

        [[nodiscard]] static TypeCarrier of_ts(const TSValueTypeMetaData *meta) noexcept
        {
            return TypeCarrier{Binding{std::in_place_index<0>, meta}};
        }
        [[nodiscard]] static TypeCarrier of_scalar(const ValueTypeMetaData *meta) noexcept
        {
            return TypeCarrier{Binding{std::in_place_index<1>, meta}};
        }
        [[nodiscard]] static TypeCarrier of_size(std::size_t size) noexcept
        {
            return TypeCarrier{Binding{std::in_place_index<2>, size}};
        }

        [[nodiscard]] ResolutionKind kind() const noexcept
        {
            switch (binding.index())
            {
                case 0: return ResolutionKind::TimeSeries;
                case 1: return ResolutionKind::Scalar;
                default: return ResolutionKind::Size;
            }
        }
        [[nodiscard]] const TSValueTypeMetaData *ts() const noexcept
        {
            const auto *meta = std::get_if<0>(&binding);
            return meta != nullptr ? *meta : nullptr;
        }
        [[nodiscard]] const ValueTypeMetaData *scalar() const noexcept
        {
            const auto *meta = std::get_if<1>(&binding);
            return meta != nullptr ? *meta : nullptr;
        }
        [[nodiscard]] std::optional<std::size_t> size() const noexcept
        {
            const auto *size = std::get_if<2>(&binding);
            return size != nullptr ? std::optional<std::size_t>{*size} : std::nullopt;
        }
        /** True when the carried form holds something: a non-null schema or a size. */
        [[nodiscard]] bool valid() const noexcept
        {
            return binding.index() == 2 || ts() != nullptr || scalar() != nullptr;
        }

        friend bool operator==(const TypeCarrier &, const TypeCarrier &) noexcept = default;
    };

    /** Renders ``type[<schema name>]`` / ``type[Size[n]]``; defined with the registry (type_resolution.cpp). */
    std::ostream &operator<<(std::ostream &out, const TypeCarrier &carrier);
}  // namespace hgraph

template <>
struct std::hash<hgraph::TypeCarrier>
{
    [[nodiscard]] std::size_t operator()(const hgraph::TypeCarrier &carrier) const noexcept
    {
        return std::visit(
            [&](const auto &value) -> std::size_t {
                using V = std::remove_cvref_t<decltype(value)>;
                if constexpr (std::is_same_v<V, std::size_t>) { return std::hash<std::size_t>{}(value) ^ 0x9e37u; }
                else { return std::hash<const void *>{}(value) ^ (carrier.binding.index() << 1); }
            },
            carrier.binding);
    }
};

#endif  // HGRAPH_TYPES_TYPE_CARRIER_H
