#include "ir/hir.h"

#include <array>

namespace hgl::ir::hir
{
    std::string_view scalar_type_name(ScalarType type) noexcept {
        static constexpr std::array names{
            std::string_view{"bool"},
            std::string_view{"i64"},
            std::string_view{"f64"},
            std::string_view{"str"},
            std::string_view{"date"},
            std::string_view{"time"},
            std::string_view{"datetime"},
            std::string_view{"duration"},
            std::string_view{"civil_datetime"},
            std::string_view{"zoned_datetime"},
            std::string_view{"zoned_time"},
            std::string_view{"timezone"},
        };
        return names[static_cast<std::size_t>(type)];
    }
}  // namespace hgl::ir::hir
