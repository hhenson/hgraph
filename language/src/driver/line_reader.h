#ifndef HGL_DRIVER_LINE_READER_H
#define HGL_DRIVER_LINE_READER_H

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hgl::driver
{
    /// The REPL's input line (developer guide, "Scripted, REPL, and AOT
    /// drivers"). On an interactive terminal, and when the tool was built
    /// with `HGL_ENABLE_LINE_EDITING`, this is an editable line with history
    /// (up/down), cursor movement, and tab completion over the words the
    /// session supplies; a history file keeps the lines across sessions.
    /// Otherwise (a pipe, a file, or the option off) it is a plain
    /// `std::getline`, so scripted use is unchanged.
    class LineReader
    {
      public:
        /// Words offered for completion: the session's declared names and the
        /// `:` commands, asked for on every completion request.
        using Completions = std::function<std::vector<std::string>()>;

        LineReader();
        ~LineReader();

        LineReader(const LineReader &)            = delete;
        LineReader &operator=(const LineReader &) = delete;

        void set_completions(Completions completions);

        /// True when input is edited interactively (history and completion
        /// are active).
        [[nodiscard]] bool interactive() const noexcept { return interactive_; }

        /// Read one line, without its newline. `prompt` is shown before the
        /// input in both modes. Returns nullopt at end of input.
        [[nodiscard]] std::optional<std::string> read(std::string_view prompt);

      private:
        friend struct LineReaderAccess;

        bool        interactive_{false};
        Completions completions_{};
    };
}  // namespace hgl::driver

#endif  // HGL_DRIVER_LINE_READER_H
