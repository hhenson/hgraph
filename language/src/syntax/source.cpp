#include "syntax/source.h"

#include <algorithm>

namespace hgl::syntax
{
    SourceFile::SourceFile(std::string path, std::string text) : path_{std::move(path)}, text_{std::move(text)}
    {
        line_starts_.push_back(0);
        for (std::uint32_t i = 0; i < text_.size(); ++i)
        {
            if (text_[i] == '\n') { line_starts_.push_back(i + 1); }
        }
    }

    Location SourceFile::location(std::uint32_t offset) const noexcept
    {
        if (offset > text_.size()) { offset = static_cast<std::uint32_t>(text_.size()); }
        // First line start strictly greater than offset; the line is the one before it.
        auto it   = std::upper_bound(line_starts_.begin(), line_starts_.end(), offset);
        auto line = static_cast<std::uint32_t>(it - line_starts_.begin());  // one-based
        return {line, offset - line_starts_[line - 1] + 1};
    }

    std::string_view SourceFile::line_text(std::uint32_t line) const noexcept
    {
        if (line == 0 || line > line_starts_.size()) { return {}; }
        const std::uint32_t begin = line_starts_[line - 1];
        std::uint32_t       end   = line < line_starts_.size() ? line_starts_[line] : static_cast<std::uint32_t>(text_.size());
        while (end > begin && (text_[end - 1] == '\n' || text_[end - 1] == '\r')) { --end; }
        return std::string_view{text_}.substr(begin, end - begin);
    }
}  // namespace hgl::syntax
