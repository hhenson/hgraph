#include <hgraph/types/primitive_types.h>

#include <cstdio>
#include <ostream>

namespace hgraph
{
    std::ostream &operator<<(std::ostream &os, const Bytes &value)
    {
        os << "b'";
        for (const unsigned char byte : value.data)
        {
            if (byte >= 0x20 && byte < 0x7F && byte != '\'' && byte != '\\')
            {
                os << static_cast<char>(byte);
            }
            else
            {
                char buffer[8];
                std::snprintf(buffer, sizeof buffer, "\\x%02x", byte);
                os << buffer;
            }
        }
        os << '\'';
        return os;
    }
}  // namespace hgraph
