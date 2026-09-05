#include "driver/cpp_formatter.h"

#include "driver/process.h"
#include "hgl_native_compile_config.h"

#include <hgraph/util/scope.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace hgl::driver
{
    namespace
    {
        std::atomic<std::uint64_t> next_format{0};

        unsigned long process_id()
        {
#if defined(_WIN32)
            return static_cast<unsigned long>(GetCurrentProcessId());
#else
            return static_cast<unsigned long>(::getpid());
#endif
        }

        std::optional<std::filesystem::path> make_format_directory(std::string &error)
        {
            std::error_code ec;
            const std::filesystem::path root = std::filesystem::temp_directory_path(ec);
            if (ec)
            {
                error = "cannot locate the temporary directory for clang-format: " + ec.message();
                return std::nullopt;
            }
            for (unsigned attempt = 0; attempt != 1000; ++attempt)
            {
                const std::filesystem::path candidate = root / ("hgl-format-" + std::to_string(process_id()) + "-" +
                                                                std::to_string(next_format.fetch_add(1)));
                ec.clear();
                if (std::filesystem::create_directory(candidate, ec))
                {
                    return candidate;
                }
                if (ec && ec != std::errc::file_exists)
                {
                    error = "cannot create clang-format directory '" + candidate.string() + "': " + ec.message();
                    return std::nullopt;
                }
            }
            error = "cannot allocate a temporary clang-format directory";
            return std::nullopt;
        }

        bool write(const std::filesystem::path &path, std::string_view text, std::string &error)
        {
            std::ofstream out{path, std::ios::binary | std::ios::trunc};
            if (!out)
            {
                error = "cannot write temporary clang-format input '" + path.string() + "'";
                return false;
            }
            out << text;
            if (!out)
            {
                error = "cannot finish temporary clang-format input '" + path.string() + "'";
                return false;
            }
            return true;
        }

        std::optional<std::string> read(const std::filesystem::path &path)
        {
            std::ifstream in{path, std::ios::binary};
            if (!in)
            {
                return std::nullopt;
            }
            return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
        }

        std::string formatter()
        {
            if (const char *configured = std::getenv("HGL_CLANG_FORMAT"); configured != nullptr && *configured != '\0')
            {
                return configured;
            }
            const std::filesystem::path built_with{native_config::clang_format};
            std::error_code ec;
            if (!built_with.empty() && std::filesystem::is_regular_file(built_with, ec) && !ec)
            {
                return built_with.string();
            }
            return "clang-format";
        }
    }  // namespace

    bool format_cpp(codegen::EmittedModule &module, std::string &error)
    {
        const std::optional<std::filesystem::path> directory = make_format_directory(error);
        if (!directory)
        {
            return false;
        }
        auto cleanup = hgraph::make_scope_exit<true>(
            [&]
            {
                std::error_code ignored;
                std::filesystem::remove_all(*directory, ignored);
            });

        const std::filesystem::path header = *directory / "module.h";
        const std::filesystem::path source = *directory / "module.cpp";
        if (!write(header, module.header, error) || !write(source, module.source, error))
        {
            return false;
        }

        const std::string executable = formatter();
        const std::vector<std::string> command = {
            executable,
            "-i",
            "--style={BasedOnStyle: LLVM, BreakBeforeBraces: Allman, ColumnLimit: "
            "120, SortIncludes: Never, "
            "IndentWidth: 4, ContinuationIndentWidth: 4, NamespaceIndentation: All, "
            "AllowShortFunctionsOnASingleLine: Empty, "
            "AllowShortLambdasOnASingleLine: None, "
            "SpacesBeforeTrailingComments: 2, "
            "PenaltyReturnTypeOnItsOwnLine: 1000000}",
            header.string(),
            source.string(),
        };
        const ProcessResult result = run_process(command);
        if (result.status != 0)
        {
            error = "clang-format failed with '" + executable + "' (exit " + std::to_string(result.status) + ")";
            if (!result.output.empty())
            {
                error += ":\n" + result.output;
            }
            return false;
        }

        std::optional<std::string> formatted_header = read(header);
        std::optional<std::string> formatted_source = read(source);
        if (!formatted_header || !formatted_source)
        {
            error = "cannot read clang-format output";
            return false;
        }
        module.header = std::move(*formatted_header);
        module.source = std::move(*formatted_source);
        return true;
    }
}  // namespace hgl::driver
