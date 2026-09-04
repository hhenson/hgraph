#include "driver/native_module.h"

#include "hgl_native_compile_config.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
#include <dlfcn.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace hgl::driver
{
    namespace
    {
        constexpr std::string_view registration_symbol = "hgl_register_module_v1";

        std::vector<std::string> split(std::string_view text, char separator)
        {
            std::vector<std::string> values;
            std::size_t              begin = 0;
            while (begin <= text.size())
            {
                const std::size_t end = text.find(separator, begin);
                std::string value{
                    text.substr(begin, end == std::string_view::npos ? text.size() - begin : end - begin)};
                if (!value.empty() && std::find(values.begin(), values.end(), value) == values.end())
                {
                    values.push_back(std::move(value));
                }
                if (end == std::string_view::npos) { break; }
                begin = end + 1;
            }
            return values;
        }

        bool write_file(const std::filesystem::path &path, std::string_view contents, std::string &error)
        {
            std::ofstream out{path, std::ios::binary | std::ios::trunc};
            if (!out)
            {
                error = "cannot write native artifact '" + path.string() + "'";
                return false;
            }
            out << contents;
            if (!out)
            {
                error = "cannot finish native artifact '" + path.string() + "'";
                return false;
            }
            return true;
        }

        std::optional<std::filesystem::path> make_artifact_directory(std::string &error)
        {
            std::error_code ec;
            std::filesystem::path root;
            if (const char *configured = std::getenv("HGL_ARTIFACT_DIR"); configured != nullptr && *configured != '\0')
            {
                root = configured;
                std::filesystem::create_directories(root, ec);
                if (ec)
                {
                    error = "cannot create HGL artifact root '" + root.string() + "': " + ec.message();
                    return std::nullopt;
                }
            }
            else
            {
                root = std::filesystem::temp_directory_path(ec);
                if (ec)
                {
                    error = "cannot locate the temporary directory: " + ec.message();
                    return std::nullopt;
                }
            }

            static std::atomic<std::uint64_t> next{0};
#if defined(_WIN32)
            const auto process = static_cast<unsigned long>(GetCurrentProcessId());
#else
            const auto process = static_cast<unsigned long>(::getpid());
#endif
            for (unsigned attempt = 0; attempt != 1000; ++attempt)
            {
                const std::filesystem::path candidate =
                    root / ("hgl-" + std::to_string(process) + "-" + std::to_string(next.fetch_add(1)));
                ec.clear();
                if (std::filesystem::create_directory(candidate, ec)) { return candidate; }
                if (ec && ec != std::errc::file_exists)
                {
                    error = "cannot create HGL artifact directory '" + candidate.string() + "': " + ec.message();
                    return std::nullopt;
                }
            }
            error = "cannot allocate a unique HGL artifact directory under '" + root.string() + "'";
            return std::nullopt;
        }

#if !defined(_WIN32)
        struct ProcessResult
        {
            int         status{-1};
            std::string output{};
        };

        ProcessResult run_process(const std::vector<std::string> &arguments)
        {
            int output_pipe[2];
            if (::pipe(output_pipe) != 0)
            {
                return {-1, "cannot create compiler output pipe: " + std::string{std::strerror(errno)}};
            }
            const pid_t child = ::fork();
            if (child < 0)
            {
                const std::string message = "cannot start the native compiler: " + std::string{std::strerror(errno)};
                ::close(output_pipe[0]);
                ::close(output_pipe[1]);
                return {-1, message};
            }
            if (child == 0)
            {
                ::close(output_pipe[0]);
                (void)::dup2(output_pipe[1], STDOUT_FILENO);
                (void)::dup2(output_pipe[1], STDERR_FILENO);
                ::close(output_pipe[1]);
                std::vector<char *> argv;
                argv.reserve(arguments.size() + 1);
                for (const std::string &argument : arguments) { argv.push_back(const_cast<char *>(argument.c_str())); }
                argv.push_back(nullptr);
                ::execvp(argv.front(), argv.data());
                const std::string message =
                    "cannot execute '" + arguments.front() + "': " + std::string{std::strerror(errno)} + "\n";
                (void)::write(STDERR_FILENO, message.data(), message.size());
                _exit(127);
            }

            ::close(output_pipe[1]);
            std::string output;
            char        buffer[4096];
            while (true)
            {
                const ssize_t count = ::read(output_pipe[0], buffer, sizeof(buffer));
                if (count > 0) { output.append(buffer, static_cast<std::size_t>(count)); }
                else if (count == 0) { break; }
                else if (errno != EINTR)
                {
                    output += "cannot read compiler output: " + std::string{std::strerror(errno)};
                    break;
                }
            }
            ::close(output_pipe[0]);

            int status = 0;
            while (::waitpid(child, &status, 0) < 0)
            {
                if (errno != EINTR) { return {-1, output + "cannot wait for the native compiler"}; }
            }
            if (WIFEXITED(status)) { return {WEXITSTATUS(status), std::move(output)}; }
            if (WIFSIGNALED(status))
            {
                return {128 + WTERMSIG(status), output + "native compiler terminated by signal " +
                                                     std::to_string(WTERMSIG(status))};
            }
            return {-1, std::move(output)};
        }

        std::filesystem::path executable_path()
        {
#if defined(__APPLE__)
            std::uint32_t size = 1024;
            std::vector<char> buffer(size);
            if (::_NSGetExecutablePath(buffer.data(), &size) != 0)
            {
                buffer.resize(size);
                if (::_NSGetExecutablePath(buffer.data(), &size) != 0) { return {}; }
            }
            return std::filesystem::path{buffer.data()};
#elif defined(__linux__)
            std::vector<char> buffer(4096);
            const ssize_t count = ::readlink("/proc/self/exe", buffer.data(), buffer.size());
            return count > 0 ? std::filesystem::path{std::string{buffer.data(), static_cast<std::size_t>(count)}}
                             : std::filesystem::path{};
#else
            return {};
#endif
        }

        std::vector<void *> &resident_images()
        {
            // Deliberately process-lifetime: registry candidates hold code
            // pointers into every image registered here.
            static auto *images = new std::vector<void *>;
            return *images;
        }
#endif
    }  // namespace

    std::optional<NativeModule> compile_and_load_native_module(const codegen::EmittedModule &module,
                                                                std::string_view source_stem, std::string &error)
    {
#if defined(_WIN32)
        (void)module;
        (void)source_stem;
        error = "scripted native HGL modules are not yet supported on Windows";
        return std::nullopt;
#else
        const std::optional<std::filesystem::path> artifact_directory = make_artifact_directory(error);
        if (!artifact_directory) { return std::nullopt; }

        std::string stem{source_stem};
        if (stem.empty()) { stem = "module"; }
        const std::filesystem::path header_path = *artifact_directory / (stem + ".h");
        const std::filesystem::path source_path = *artifact_directory / (stem + ".cpp");
        const std::filesystem::path bootstrap_path = *artifact_directory / "hgl_module.cpp";
#if defined(__APPLE__)
        const std::filesystem::path image_path = *artifact_directory / (stem + ".bundle");
#else
        const std::filesystem::path image_path = *artifact_directory / (stem + ".so");
#endif

        std::ostringstream bootstrap;
        bootstrap << "#include \"" << stem << ".h\"\n\n"
                     "extern \"C\" __attribute__((visibility(\"default\"))) void "
                  << registration_symbol << "()\n{\n    " << module.namespace_name << "::register_operators();\n}\n";
        if (!write_file(header_path, module.header, error) || !write_file(source_path, module.source, error) ||
            !write_file(bootstrap_path, bootstrap.str(), error))
        {
            error += "; artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }

        std::vector<std::string> command = split(native_config::compiler_launcher, ';');
        const char *compiler_override = std::getenv("HGL_CXX");
        const std::string compiler = compiler_override != nullptr && *compiler_override != '\0'
                                         ? std::string{compiler_override}
                                         : std::string{native_config::compiler};
        command.push_back(compiler);
        command.emplace_back("-std=c++23");
        command.emplace_back("-fPIC");
        command.emplace_back("-fvisibility=hidden");
        for (const std::string &option : split(native_config::compile_options, ';')) { command.push_back(option); }
        for (const std::string &option : split(native_config::link_options, ';')) { command.push_back(option); }
        for (const std::string &definition : split(native_config::definitions, ';'))
        {
            command.push_back("-D" + definition);
        }
        const std::filesystem::path executable = executable_path();
        if (!executable.empty())
        {
            const std::filesystem::path installed_include = executable.parent_path().parent_path() / "include";
            std::error_code            include_error;
            if (std::filesystem::is_directory(installed_include, include_error))
            {
                command.push_back("-I" + installed_include.string());
            }
        }
        for (const std::string &include : split(native_config::include_directories, ';'))
        {
            command.push_back("-I" + include);
        }
        command.push_back("-I" + artifact_directory->string());
#if defined(__APPLE__)
        command.emplace_back("-bundle");
        command.emplace_back("-undefined");
        command.emplace_back("dynamic_lookup");
        if (!executable.empty())
        {
            command.emplace_back("-bundle_loader");
            command.push_back(executable.string());
        }
#else
        command.emplace_back("-shared");
#endif
        command.push_back(source_path.string());
        command.push_back(bootstrap_path.string());
        command.emplace_back("-o");
        command.push_back(image_path.string());

        const ProcessResult compiled = run_process(command);
        if (compiled.status != 0)
        {
            error = "native compilation failed with '" + compiler + "' (exit " + std::to_string(compiled.status) + ")";
            if (!compiled.output.empty())
            {
                error += ":\n" + compiled.output;
                if (error.back() != '\n') { error += '\n'; }
            }
            error += "artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }

        void *image = ::dlopen(image_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (image == nullptr)
        {
            const char *load_error = ::dlerror();
            error = "cannot load native artifact '" + image_path.string() + "': " +
                    (load_error != nullptr ? std::string{load_error} : std::string{"unknown loader error"}) +
                    "; artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }
        ::dlerror();
        void *symbol = ::dlsym(image, registration_symbol.data());
        if (const char *load_error = ::dlerror(); load_error != nullptr)
        {
            error = "native artifact has no registration entry point: " + std::string{load_error} +
                    "; artifacts retained in '" + artifact_directory->string() + "'";
            ::dlclose(image);
            return std::nullopt;
        }
        using Register = void (*)();
        resident_images().push_back(image);
        try
        {
            reinterpret_cast<Register>(symbol)();
        }
        catch (const std::exception &exception)
        {
            error = "native module registration failed: " + std::string{exception.what()} +
                    "; artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }
        catch (...)
        {
            error = "native module registration failed with an unknown exception; artifacts retained in '" +
                    artifact_directory->string() + "'";
            return std::nullopt;
        }
        std::error_code cleanup_error;
        std::filesystem::remove_all(*artifact_directory, cleanup_error);
        return NativeModule{*artifact_directory};
#endif
    }
}  // namespace hgl::driver
