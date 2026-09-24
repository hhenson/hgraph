#include <hgraph/types/value/unicode_printable.h>
#include <hgraph/types/value/value_ops.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace hgraph::value_ops_detail
{
    namespace
    {
        constexpr char hex_digits[] = "0123456789abcdef";

        void append_hex_escape(std::string &out, char kind, std::uint32_t value, int digits)
        {
            out.push_back('\\');
            out.push_back(kind);
            for (int shift = (digits - 1) * 4; shift >= 0; shift -= 4)
            {
                out.push_back(hex_digits[(value >> shift) & 0xF]);
            }
        }

        /** Decode one UTF-8 sequence at ``index``. Returns its length, or 0
            when the bytes there are not a well-formed sequence. */
        std::size_t decode_utf8(std::string_view text, std::size_t index, std::uint32_t &code_point)
        {
            const auto lead = static_cast<unsigned char>(text[index]);
            std::size_t length = 0;
            std::uint32_t value = 0;
            std::uint32_t minimum = 0;
            if (lead < 0x80) { code_point = lead; return 1; }
            if ((lead & 0xE0) == 0xC0) { length = 2; value = lead & 0x1F; minimum = 0x80; }
            else if ((lead & 0xF0) == 0xE0) { length = 3; value = lead & 0x0F; minimum = 0x800; }
            else if ((lead & 0xF8) == 0xF0) { length = 4; value = lead & 0x07; minimum = 0x10000; }
            else { return 0; }
            if (index + length > text.size()) { return 0; }
            for (std::size_t offset = 1; offset < length; ++offset)
            {
                const auto next = static_cast<unsigned char>(text[index + offset]);
                if ((next & 0xC0) != 0x80) { return 0; }
                value = (value << 6) | (next & 0x3F);
            }
            if (value < minimum || value > 0x10FFFF) { return 0; }
            code_point = value;
            return length;
        }
    }  // namespace

    std::string python_repr_string(std::string_view text)
    {
        const bool has_single = text.find('\'') != std::string_view::npos;
        const bool has_double = text.find('"') != std::string_view::npos;
        const char quote = (has_single && !has_double) ? '"' : '\'';
        std::string out;
        out.reserve(text.size() + 2);
        out.push_back(quote);
        for (std::size_t index = 0; index < text.size();)
        {
            std::uint32_t code_point = 0;
            const std::size_t length = decode_utf8(text, index, code_point);
            if (length == 0)
            {
                // Not UTF-8, so not a Python string: keep the byte visible.
                append_hex_escape(out, 'x', static_cast<unsigned char>(text[index]), 2);
                ++index;
                continue;
            }
            if (code_point == static_cast<std::uint32_t>(quote) || code_point == '\\')
            {
                out.push_back('\\');
                out.push_back(static_cast<char>(code_point));
            }
            else if (code_point == '\t') { out += "\\t"; }
            else if (code_point == '\n') { out += "\\n"; }
            else if (code_point == '\r') { out += "\\r"; }
            else if (code_point < 0x20 || code_point == 0x7F) { append_hex_escape(out, 'x', code_point, 2); }
            else if (code_point < 0x7F || unicode_detail::is_printable(code_point))
            {
                out.append(text.substr(index, length));
            }
            else if (code_point <= 0xFF) { append_hex_escape(out, 'x', code_point, 2); }
            else if (code_point <= 0xFFFF) { append_hex_escape(out, 'u', code_point, 4); }
            else { append_hex_escape(out, 'U', code_point, 8); }
            index += length;
        }
        out.push_back(quote);
        return out;
    }
}  // namespace hgraph::value_ops_detail
