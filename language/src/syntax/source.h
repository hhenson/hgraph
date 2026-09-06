#ifndef HGL_SYNTAX_SOURCE_H
#define HGL_SYNTAX_SOURCE_H

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace hgl::syntax
{
    /// Half-open byte range [begin, end) into one source file's text.
    struct SourceRange
    {
        std::uint32_t begin{0};
        std::uint32_t end{0};

        [[nodiscard]] constexpr bool empty() const noexcept { return begin >= end; }

        /// The smallest range covering both.
        [[nodiscard]] constexpr SourceRange join(SourceRange other) const noexcept
        {
            return {begin < other.begin ? begin : other.begin, end > other.end ? end : other.end};
        }

        friend constexpr bool operator==(SourceRange, SourceRange) noexcept = default;
    };

    struct SourceComment
    {
        SourceRange range{};
    };

    enum class SourceFragmentKind : std::uint8_t
    {
        Token,
        Whitespace,
        LineBreak,
        LineComment,
    };

    inline constexpr std::size_t no_token_index = std::numeric_limits<std::size_t>::max();

    /// One non-overlapping piece of the original source. Together the
    /// fragments cover the file byte-for-byte and in source order. Physical
    /// line breaks remain separate even when the grammar tokenizes a run of
    /// them as one newline.
    struct SourceFragment
    {
        SourceFragmentKind kind{SourceFragmentKind::Whitespace};
        SourceRange        range{};
        std::size_t        token_index{no_token_index};
    };

    /// One-based line and column of a byte offset (columns count bytes).
    struct Location
    {
        std::uint32_t line{1};
        std::uint32_t column{1};

        friend constexpr bool operator==(Location, Location) noexcept = default;
    };

    /// A source file: its path for diagnostics, its text, and the line table
    /// that turns byte offsets into locations.
    class SourceFile
    {
      public:
        SourceFile(std::string path, std::string text);

        [[nodiscard]] const std::string &path() const noexcept { return path_; }
        [[nodiscard]] std::string_view text() const noexcept { return text_; }
        [[nodiscard]] std::string_view slice(SourceRange range) const noexcept
        {
            return std::string_view{text_}.substr(range.begin, range.end - range.begin);
        }

        [[nodiscard]] Location location(std::uint32_t offset) const noexcept;
        /// The full text of the (one-based) line, without its terminator.
        [[nodiscard]] std::string_view line_text(std::uint32_t line) const noexcept;
        [[nodiscard]] std::uint32_t line_count() const noexcept
        {
            return static_cast<std::uint32_t>(line_starts_.size());
        }

      private:
        std::string                path_;
        std::string                text_;
        std::vector<std::uint32_t> line_starts_;
    };
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_SOURCE_H
