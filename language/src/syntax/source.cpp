#include "syntax/source.h"

#include <algorithm>

namespace hgl::syntax
{
    namespace
    {
        std::vector<std::uint32_t> line_starts(std::string_view text) {
            std::vector<std::uint32_t> result{0};
            for (std::uint32_t i = 0; i < text.size(); ++i) {
                if (text[i] == '\n') { result.push_back(i + 1); }
            }
            return result;
        }
    }  // namespace

    SourceFile::SourceFile(std::string path, std::string text)
        : path_{std::move(path)}, text_{std::move(text)}, line_starts_{line_starts(text_)} {}

    SourceFile::SourceFile(std::string path, std::string text, std::vector<SourceOrigin> origins)
        : SourceFile{std::move(path), std::move(text)} {
        origins_.reserve(origins.size());
        for (SourceOrigin &origin : origins) {
            origins_.push_back(MappedOrigin{origin.range, std::move(origin.path), std::move(origin.text), {}});
            origins_.back().line_starts = line_starts(origins_.back().text);
        }
    }

    const SourceFile::MappedOrigin *SourceFile::origin_at(std::uint32_t offset) const noexcept {
        const auto found =
            std::upper_bound(origins_.begin(), origins_.end(), offset,
                             [](std::uint32_t value, const MappedOrigin &origin) { return value < origin.range.begin; });
        if (found == origins_.begin()) { return nullptr; }
        const MappedOrigin &candidate = *std::prev(found);
        // Retain the preceding origin for a zero-width diagnostic at a
        // part's EOF. At a boundary with no separator, upper_bound() has
        // already selected the following origin whose begin equals offset.
        if (offset <= candidate.range.end) { return &candidate; }
        return nullptr;
    }

    Location SourceFile::location_in(std::string_view text, const std::vector<std::uint32_t> &starts,
                                     std::uint32_t offset) noexcept {
        if (offset > text.size()) { offset = static_cast<std::uint32_t>(text.size()); }
        // First line start strictly greater than offset; the line is the one before it.
        auto it   = std::upper_bound(starts.begin(), starts.end(), offset);
        auto line = static_cast<std::uint32_t>(it - starts.begin());  // one-based
        return {line, offset - starts[line - 1] + 1};
    }

    Location SourceFile::location(std::uint32_t offset) const noexcept {
        if (const MappedOrigin *origin = origin_at(offset)) {
            return location_in(origin->text, origin->line_starts, offset - origin->range.begin);
        }
        return location_in(text_, line_starts_, offset);
    }

    std::string_view SourceFile::source_path(std::uint32_t offset) const noexcept {
        if (const MappedOrigin *origin = origin_at(offset)) { return origin->path; }
        return path_;
    }

    std::string_view SourceFile::line_text_in(std::string_view text, const std::vector<std::uint32_t> &starts,
                                              std::uint32_t line) noexcept {
        if (line == 0 || line > starts.size()) { return {}; }
        const std::uint32_t begin = starts[line - 1];
        std::uint32_t       end   = line < starts.size() ? starts[line] : static_cast<std::uint32_t>(text.size());
        while (end > begin && (text[end - 1] == '\n' || text[end - 1] == '\r')) { --end; }
        return text.substr(begin, end - begin);
    }

    std::string_view SourceFile::line_text(std::uint32_t line) const noexcept { return line_text_in(text_, line_starts_, line); }

    std::string_view SourceFile::line_text_at(std::uint32_t offset) const noexcept {
        if (const MappedOrigin *origin = origin_at(offset)) {
            const Location at = location_in(origin->text, origin->line_starts, offset - origin->range.begin);
            return line_text_in(origin->text, origin->line_starts, at.line);
        }
        const Location at = location_in(text_, line_starts_, offset);
        return line_text_in(text_, line_starts_, at.line);
    }
}  // namespace hgl::syntax
