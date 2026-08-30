#include <hgraph/fabric/types.h>

#include <hgraph/fabric/planning.h>

#include <hgraph/types/metadata/type_registry.h>

#include <algorithm>
#include <stdexcept>
#include <string>

namespace hgraph::fabric
{
    namespace
    {
        [[nodiscard]] bool continuation(unsigned char value) noexcept
        {
            return (value & 0xc0U) == 0x80U;
        }

        [[nodiscard]] std::uint32_t decode_code_point(std::string_view text,
                                                       std::size_t &offset)
        {
            const auto first = static_cast<unsigned char>(text[offset]);
            if (first <= 0x7fU)
            {
                ++offset;
                return first;
            }

            std::size_t   width{};
            std::uint32_t code_point{};
            std::uint32_t minimum{};
            if ((first & 0xe0U) == 0xc0U)
            {
                width      = 2;
                code_point = first & 0x1fU;
                minimum    = 0x80U;
            }
            else if ((first & 0xf0U) == 0xe0U)
            {
                width      = 3;
                code_point = first & 0x0fU;
                minimum    = 0x800U;
            }
            else if ((first & 0xf8U) == 0xf0U)
            {
                width      = 4;
                code_point = first & 0x07U;
                minimum    = 0x10000U;
            }
            else
            {
                throw std::invalid_argument("fabric data id is not valid UTF-8");
            }
            if (offset + width > text.size())
            {
                throw std::invalid_argument("fabric data id is not valid UTF-8");
            }
            for (std::size_t index = 1; index < width; ++index)
            {
                const auto next = static_cast<unsigned char>(text[offset + index]);
                if (!continuation(next))
                {
                    throw std::invalid_argument("fabric data id is not valid UTF-8");
                }
                code_point = (code_point << 6U) | (next & 0x3fU);
            }
            offset += width;
            if (code_point < minimum || code_point > 0x10ffffU ||
                (code_point >= 0xd800U && code_point <= 0xdfffU))
            {
                throw std::invalid_argument("fabric data id is not valid UTF-8");
            }
            return code_point;
        }
    }  // namespace

    void require_data_id(std::string_view data_id)
    {
        if (data_id.empty())
        {
            throw std::invalid_argument("fabric data id must not be empty");
        }
        if (data_id.size() > MAX_DATA_ID_BYTES)
        {
            throw std::invalid_argument("fabric data id exceeds 4096 encoded bytes");
        }
        std::size_t offset{};
        while (offset < data_id.size())
        {
            const std::uint32_t code_point = decode_code_point(data_id, offset);
            if (code_point <= 0x1fU ||
                (code_point >= 0x7fU && code_point <= 0x9fU))
            {
                throw std::invalid_argument(
                    "fabric data id must not contain Unicode control code points");
            }
        }
    }

    bool canonical_data_id_less(std::string_view lhs,
                                std::string_view rhs) noexcept
    {
        return std::ranges::lexicographical_compare(
            lhs, rhs, {}, [](char value) {
                return static_cast<unsigned char>(value);
            }, [](char value) {
                return static_cast<unsigned char>(value);
            });
    }

    void register_fabric_types()
    {
        static_cast<void>(scalar_descriptor<DataDependency>::value_meta());
        static_cast<void>(scalar_descriptor<DataRevision>::value_meta());
        static_cast<void>(scalar_descriptor<FabricDiagnosticEvent>::value_meta());
        static_cast<void>(scalar_descriptor<PlannedPublisher>::value_meta());
        static_cast<void>(scalar_descriptor<ConsistencyForest>::value_meta());
        static_cast<void>(scalar_descriptor<DependencyPlan>::value_meta());
    }
}  // namespace hgraph::fabric
