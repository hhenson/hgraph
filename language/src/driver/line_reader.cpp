#include "driver/line_reader.h"

#include <cstdlib>
#include <iostream>
#include <string>

#if defined(HGL_HAVE_ISOCLINE)
#include <isocline.h>
#if defined(_WIN32)
#include <io.h>
#define HGL_ISATTY _isatty
#define HGL_FILENO _fileno
#else
#include <unistd.h>
#define HGL_ISATTY isatty
#define HGL_FILENO fileno
#endif
#endif

namespace hgl::driver
{
    namespace
    {
#if defined(HGL_HAVE_ISOCLINE)
        LineReader *active_reader = nullptr;

        /// `$HGL_HISTORY`, else `~/.hgl_history`; empty disables the file.
        std::string history_path()
        {
            if (const char *explicit_path = std::getenv("HGL_HISTORY"); explicit_path != nullptr)
            {
                return explicit_path;
            }
            const char *home = std::getenv("HOME");
#if defined(_WIN32)
            if (home == nullptr) { home = std::getenv("USERPROFILE"); }
#endif
            if (home == nullptr) { return {}; }
            return std::string{home} + "/.hgl_history";
        }

        bool is_word_char(const char *s, long len)
        {
            if (len <= 0) { return false; }
            const char c = *s;
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':';
        }

        void complete_word(ic_completion_env_t *env, const char *word);

        void completer(ic_completion_env_t *env, const char *prefix)
        {
            ic_complete_word(env, prefix, &complete_word, &is_word_char);
        }
#endif
    }  // namespace

    struct LineReaderAccess
    {
        [[nodiscard]] static std::vector<std::string> completions(const LineReader &reader)
        {
            return reader.completions_ ? reader.completions_() : std::vector<std::string>{};
        }
    };

    namespace
    {
#if defined(HGL_HAVE_ISOCLINE)
        void complete_word(ic_completion_env_t *env, const char *word)
        {
            if (active_reader == nullptr) { return; }
            const std::string_view typed{word};
            for (const std::string &candidate : LineReaderAccess::completions(*active_reader))
            {
                if (candidate.starts_with(typed) && candidate.size() > typed.size())
                {
                    if (!ic_add_completion(env, candidate.c_str())) { return; }
                }
            }
        }
#endif
    }  // namespace

    LineReader::LineReader()
    {
#if defined(HGL_HAVE_ISOCLINE)
        interactive_ = HGL_ISATTY(HGL_FILENO(stdin)) != 0 && HGL_ISATTY(HGL_FILENO(stdout)) != 0 &&
                       std::getenv("HGL_NO_LINE_EDITING") == nullptr;
        if (interactive_)
        {
            active_reader = this;
            // The prompt text carries its own marker (`hgl> `, `...> `), so
            // isocline adds none. Bracket continuation is the REPL's own loop,
            // so a plain Enter submits the line.
            ic_set_prompt_marker("", "");
            ic_enable_multiline(false);
            ic_enable_history_duplicates(false);
            const std::string history = history_path();
            ic_set_history(history.empty() ? nullptr : history.c_str(), 500);
            ic_set_default_completer(&completer, nullptr);
        }
#endif
    }

    LineReader::~LineReader()
    {
#if defined(HGL_HAVE_ISOCLINE)
        if (active_reader == this) { active_reader = nullptr; }
#endif
    }

    void LineReader::set_completions(Completions completions) { completions_ = std::move(completions); }

    std::optional<std::string> LineReader::read(std::string_view prompt)
    {
#if defined(HGL_HAVE_ISOCLINE)
        if (interactive_)
        {
            const std::string text{prompt};
            char *line = ic_readline(text.c_str());
            if (line == nullptr) { return std::nullopt; }
            std::string result{line};
            ic_free(line);
            return result;
        }
#endif
        std::cout << prompt << std::flush;
        std::string line;
        if (!std::getline(std::cin, line)) { return std::nullopt; }
        return line;
    }
}  // namespace hgl::driver
