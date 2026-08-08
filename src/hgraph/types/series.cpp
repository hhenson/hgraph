#include <hgraph/types/series.h>

#include <arrow/array.h>

#include <ostream>

namespace hgraph
{
    std::ostream &operator<<(std::ostream &os, const Series &value)
    {
        if (!value.has_value()) { return os << "series[empty]"; }
        return os << "series[" << value.array->length() << "]";
    }
    std::int64_t series_length(const Series &value) noexcept
    {
        return value.has_value() ? value.array->length() : 0;
    }
}  // namespace hgraph
