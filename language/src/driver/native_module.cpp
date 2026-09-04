#include "driver/native_module.h"

#include "hgl_native_compile_config.h"

#include <hgraph/util/sha256.h>
#include <hgraph/version.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
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
        constexpr std::string_view cache_format = "hgl-native-cache-v1";

        std::atomic<std::uint64_t> next_directory{0};

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

        bool environment_flag(std::string_view name)
        {
            const std::string key{name};
            const char       *value = std::getenv(key.c_str());
            return value != nullptr && *value != '\0' && std::string_view{value} != "0";
        }

        void trace_cache(std::string_view message)
        {
            if (environment_flag("HGL_CACHE_TRACE")) { std::cerr << "hgl native cache " << message << '\n'; }
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

        std::optional<std::string> read_file(const std::filesystem::path &path)
        {
            std::ifstream in{path, std::ios::binary};
            if (!in) { return std::nullopt; }
            std::ostringstream contents;
            contents << in.rdbuf();
            if (in.bad()) { return std::nullopt; }
            return std::move(contents).str();
        }

        unsigned long process_id()
        {
#if defined(_WIN32)
            return static_cast<unsigned long>(GetCurrentProcessId());
#else
            return static_cast<unsigned long>(::getpid());
#endif
        }

        std::string unique_name(std::string_view prefix)
        {
            return std::string{prefix} + "-" + std::to_string(process_id()) + "-" +
                   std::to_string(next_directory.fetch_add(1));
        }

        std::optional<std::filesystem::path> make_unique_directory(const std::filesystem::path &root,
                                                                    std::string_view prefix, std::string &error)
        {
            std::error_code ec;
            std::filesystem::create_directories(root, ec);
            if (ec)
            {
                error = "cannot create directory '" + root.string() + "': " + ec.message();
                return std::nullopt;
            }
            for (unsigned attempt = 0; attempt != 1000; ++attempt)
            {
                const std::filesystem::path candidate = root / unique_name(prefix);
                ec.clear();
                if (std::filesystem::create_directory(candidate, ec)) { return candidate; }
                if (ec && ec != std::errc::file_exists)
                {
                    error = "cannot create directory '" + candidate.string() + "': " + ec.message();
                    return std::nullopt;
                }
            }
            error = "cannot allocate a unique directory under '" + root.string() + "'";
            return std::nullopt;
        }

        std::optional<std::filesystem::path> make_artifact_directory(std::string &error)
        {
            std::error_code ec;
            std::filesystem::path root;
            if (const char *configured = std::getenv("HGL_ARTIFACT_DIR"); configured != nullptr && *configured != '\0')
            {
                root = configured;
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
            return make_unique_directory(root, "hgl", error);
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

        void hash_text(hgraph::util::Sha256 &hasher, std::string_view text)
        {
            hasher.update(std::as_bytes(std::span{text.data(), text.size()}));
        }

        void hash_field(hgraph::util::Sha256 &hasher, std::string_view name, std::string_view value)
        {
            static constexpr std::string_view separator{"\0", 1};
            hash_text(hasher, name);
            hash_text(hasher, separator);
            hash_text(hasher, std::to_string(value.size()));
            hash_text(hasher, separator);
            hash_text(hasher, value);
            hash_text(hasher, separator);
        }

        std::string hex_digest(const hgraph::util::Sha256Digest &digest)
        {
            const std::array<char, 64> hex = hgraph::util::sha256_hex(digest);
            return {hex.data(), hex.size()};
        }

        std::optional<std::string> file_digest(const std::filesystem::path &path)
        {
            std::ifstream in{path, std::ios::binary};
            if (!in) { return std::nullopt; }
            hgraph::util::Sha256 hasher;
            std::array<char, 64 * 1024> buffer{};
            while (in)
            {
                in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                const std::streamsize count = in.gcount();
                if (count > 0)
                {
                    hasher.update(std::as_bytes(std::span{buffer.data(), static_cast<std::size_t>(count)}));
                }
            }
            if (!in.eof()) { return std::nullopt; }
            return hex_digest(hasher.finish());
        }

        std::optional<std::filesystem::path> canonical_regular_file(const std::filesystem::path &path)
        {
            if (path.empty()) { return std::nullopt; }
            std::error_code ec;
            std::filesystem::path absolute = path;
            if (absolute.is_relative())
            {
                absolute = std::filesystem::absolute(absolute, ec);
                if (ec) { return std::nullopt; }
            }
            const std::filesystem::path canonical = std::filesystem::weakly_canonical(absolute, ec);
            if (ec || !std::filesystem::is_regular_file(canonical, ec) || ec) { return std::nullopt; }
            return canonical;
        }

        std::optional<std::filesystem::path> resolve_executable(std::string_view command)
        {
            const std::filesystem::path requested{command};
            if (requested.has_parent_path())
            {
                const std::optional<std::filesystem::path> resolved = canonical_regular_file(requested);
                return resolved && ::access(resolved->c_str(), X_OK) == 0 ? resolved : std::nullopt;
            }

            const char *path_value = std::getenv("PATH");
            if (path_value == nullptr) { return std::nullopt; }
            const std::string_view path{path_value};
            std::size_t            begin = 0;
            while (begin <= path.size())
            {
                const std::size_t end = path.find(':', begin);
                const std::string_view directory =
                    path.substr(begin, end == std::string_view::npos ? path.size() - begin : end - begin);
                const std::filesystem::path candidate =
                    (directory.empty() ? std::filesystem::path{"."} : std::filesystem::path{directory}) / requested;
                if (const std::optional<std::filesystem::path> resolved = canonical_regular_file(candidate);
                    resolved && ::access(resolved->c_str(), X_OK) == 0)
                {
                    return resolved;
                }
                if (end == std::string_view::npos) { break; }
                begin = end + 1;
            }
            return std::nullopt;
        }

        std::string probe_compiler(const std::vector<std::string> &prefix, std::string_view argument)
        {
            std::vector<std::string> command = prefix;
            command.emplace_back(argument);
            const ProcessResult result = run_process(command);
            return std::to_string(result.status) + "\n" + result.output;
        }

        struct BuildContext
        {
            std::string              compiler{};
            std::vector<std::string> arguments{};
            std::string              compiler_version{};
            std::string              compiler_target{};
            std::filesystem::path    compiler_executable{};
            std::string              compiler_digest{};
            std::filesystem::path    executable{};
            std::string              executable_digest{};
            std::string              cache_unavailable_reason{};
        };

        void mark_cache_unavailable(BuildContext &context, std::string reason)
        {
            if (context.cache_unavailable_reason.empty()) { context.cache_unavailable_reason = std::move(reason); }
        }

        BuildContext build_context()
        {
            BuildContext context;
            const char  *compiler_override = std::getenv("HGL_CXX");
            context.compiler = compiler_override != nullptr && *compiler_override != '\0'
                                   ? std::string{compiler_override}
                                   : std::string{native_config::compiler};
            context.arguments = {context.compiler};
            context.compiler_version = probe_compiler(context.arguments, "--version");
            context.compiler_target = probe_compiler(context.arguments, "-dumpmachine");
            if (const std::optional<std::filesystem::path> compiler = resolve_executable(context.compiler))
            {
                context.compiler_executable = *compiler;
                if (const std::optional<std::string> digest = file_digest(*compiler))
                {
                    context.compiler_digest = *digest;
                }
                else
                {
                    mark_cache_unavailable(context, "compiler executable cannot be hashed");
                }
            }
            else
            {
                mark_cache_unavailable(context, "compiler executable cannot be identified");
            }

            context.arguments.emplace_back("-std=c++23");
            context.arguments.emplace_back("-fPIC");
            context.arguments.emplace_back("-fvisibility=hidden");
            for (const std::string &option : split(native_config::compile_options, ';'))
            {
                context.arguments.push_back(option);
            }
            for (const std::string &option : split(native_config::link_options, ';'))
            {
                context.arguments.push_back(option);
            }
            for (const std::string &definition : split(native_config::definitions, ';'))
            {
                context.arguments.push_back("-D" + definition);
            }

            if (const std::optional<std::filesystem::path> executable = canonical_regular_file(executable_path()))
            {
                context.executable = *executable;
                if (const std::optional<std::string> digest = file_digest(*executable))
                {
                    context.executable_digest = *digest;
                }
                else
                {
                    mark_cache_unavailable(context, "hosting hgl executable cannot be hashed");
                }
                const std::filesystem::path installed_include =
                    (context.executable.parent_path() / native_config::install_include_from_bindir).lexically_normal();
                std::error_code include_error;
                if (std::filesystem::is_directory(installed_include, include_error))
                {
                    context.arguments.push_back("-I" + installed_include.string());
                }
            }
            else
            {
                mark_cache_unavailable(context, "hosting hgl executable cannot be identified");
            }
            for (const std::string &include : split(native_config::include_directories, ';'))
            {
                context.arguments.push_back("-I" + include);
            }
#if defined(__APPLE__)
            context.arguments.emplace_back("-bundle");
            context.arguments.emplace_back("-undefined");
            context.arguments.emplace_back("dynamic_lookup");
            if (!context.executable.empty())
            {
                context.arguments.emplace_back("-bundle_loader");
                context.arguments.push_back(context.executable.string());
            }
#else
            context.arguments.emplace_back("-shared");
#endif
            return context;
        }

        std::string cache_key(const codegen::EmittedModule &module, std::string_view stem,
                              std::string_view bootstrap, const BuildContext &context)
        {
            hgraph::util::Sha256 hasher;
            hash_field(hasher, "format", cache_format);
            hash_field(hasher, "header-name", std::string{stem} + ".h");
            hash_field(hasher, "header", module.header);
            hash_field(hasher, "source", module.source);
            hash_field(hasher, "bootstrap", bootstrap);
            hash_field(hasher, "registration-symbol", registration_symbol);
            hash_field(hasher, "compiler-version", context.compiler_version);
            hash_field(hasher, "compiler-target", context.compiler_target);
            hash_field(hasher, "compiler-executable", context.compiler_executable.string());
            hash_field(hasher, "compiler-executable-digest", context.compiler_digest);
            for (std::size_t i = 0; i < context.arguments.size(); ++i)
            {
                hash_field(hasher, "argument-" + std::to_string(i), context.arguments[i]);
            }
            hash_field(hasher, "system", native_config::system_name);
            hash_field(hasher, "processor", native_config::system_processor);
            hash_field(hasher, "configuration", native_config::configuration);
            hash_field(hasher, "hgraph-version", hgraph::version_string);
            hash_field(hasher, "hgraph-commit", hgraph::git_commit_hash);
            hash_field(hasher, "hgl-executable", context.executable.string());
            hash_field(hasher, "hgl-executable-digest", context.executable_digest);
            static constexpr std::array<std::string_view, 13> compiler_environment{
                "PATH",          "SDKROOT",          "MACOSX_DEPLOYMENT_TARGET", "CPATH",
                "CPLUS_INCLUDE_PATH", "C_INCLUDE_PATH", "OBJC_INCLUDE_PATH",       "LIBRARY_PATH",
                "LD_LIBRARY_PATH",    "DYLD_LIBRARY_PATH", "GCC_EXEC_PREFIX",      "COMPILER_PATH",
                "SOURCE_DATE_EPOCH"};
            for (const std::string_view name : compiler_environment)
            {
                const std::string key{name};
                const char       *value = std::getenv(key.c_str());
                hash_field(hasher, "environment-" + key, value != nullptr ? std::string_view{value}
                                                                         : std::string_view{"<unset>"});
            }
            return hex_digest(hasher.finish());
        }

        std::optional<std::filesystem::path> cache_root(std::string &error)
        {
            if (environment_flag("HGL_DISABLE_CACHE")) { return std::nullopt; }

            std::filesystem::path root;
            if (const char *configured = std::getenv("HGL_CACHE_DIR"); configured != nullptr && *configured != '\0')
            {
                root = configured;
            }
            else if (const char *xdg = std::getenv("XDG_CACHE_HOME"); xdg != nullptr && *xdg != '\0')
            {
                root = std::filesystem::path{xdg} / "hgl" / "native";
            }
            else if (const char *home = std::getenv("HOME"); home != nullptr && *home != '\0')
            {
#if defined(__APPLE__)
                root = std::filesystem::path{home} / "Library" / "Caches" / "hgl" / "native";
#else
                root = std::filesystem::path{home} / ".cache" / "hgl" / "native";
#endif
            }
            else
            {
                error = "no per-user cache directory is available; set HGL_CACHE_DIR, XDG_CACHE_HOME, or HOME";
                return std::nullopt;
            }
            root /= "v1";
            std::error_code ec;
            std::filesystem::create_directories(root, ec);
            if (ec)
            {
                error = "cannot create HGL native cache '" + root.string() + "': " + ec.message();
                return std::nullopt;
            }
            return root;
        }

        std::string image_name()
        {
#if defined(__APPLE__)
            return "module.bundle";
#else
            return "module.so";
#endif
        }

        bool complete_cache_entry(const std::filesystem::path &entry, std::string_view key)
        {
            const std::filesystem::path image = entry / image_name();
            std::error_code            ec;
            if (!std::filesystem::is_regular_file(image, ec) || ec) { return false; }
            const std::optional<std::string> digest = file_digest(image);
            const std::optional<std::string> marker = read_file(entry / "complete");
            return digest && marker && *marker == std::string{key} + "\n" + *digest + "\n";
        }

        struct CachePublication
        {
            std::filesystem::path entry{};
            bool                  reused{false};
        };

        std::optional<CachePublication> publish_cache(const std::filesystem::path &root, std::string_view key,
                                                       const std::filesystem::path &artifact_directory,
                                                       std::string_view stem, const BuildContext &context,
                                                       std::string &warning)
        {
            const std::filesystem::path entry = root / key;
            if (complete_cache_entry(entry, key)) { return CachePublication{entry, true}; }

            const std::optional<std::filesystem::path> staging = make_unique_directory(root, ".staging", warning);
            if (!staging) { return std::nullopt; }
            const std::filesystem::path source_image = artifact_directory / (std::string{stem} +
#if defined(__APPLE__)
                                                                              ".bundle");
#else
                                                                              ".so");
#endif
            const std::array<std::pair<std::filesystem::path, std::filesystem::path>, 4> copies{
                std::pair{artifact_directory / (std::string{stem} + ".h"), *staging / (std::string{stem} + ".h")},
                std::pair{artifact_directory / (std::string{stem} + ".cpp"), *staging / (std::string{stem} + ".cpp")},
                std::pair{artifact_directory / "hgl_module.cpp", *staging / "hgl_module.cpp"},
                std::pair{source_image, *staging / image_name()}};
            std::error_code ec;
            for (const auto &[source, destination] : copies)
            {
                std::filesystem::copy_file(source, destination, std::filesystem::copy_options::overwrite_existing, ec);
                if (ec)
                {
                    warning = "cannot stage HGL native cache entry: " + ec.message();
                    std::filesystem::remove_all(*staging, ec);
                    return std::nullopt;
                }
            }

            std::ostringstream manifest;
            manifest << "format=" << cache_format << '\n'
                     << "key=" << key << '\n'
                     << "compiler=" << context.compiler << '\n'
                     << "target=" << native_config::system_name << '-' << native_config::system_processor << '\n'
                     << "configuration=" << native_config::configuration << '\n'
                     << "hgraph-version=" << hgraph::version_string << '\n'
                     << "hgraph-commit=" << hgraph::git_commit_hash << '\n';
            std::string write_error;
            const std::optional<std::string> digest = file_digest(*staging / image_name());
            if (!digest || !write_file(*staging / "manifest.txt", manifest.str(), write_error) ||
                !write_file(*staging / "complete", std::string{key} + "\n" + digest.value_or("") + "\n", write_error))
            {
                warning = digest ? write_error : "cannot hash staged HGL native image";
                std::filesystem::remove_all(*staging, ec);
                return std::nullopt;
            }

            for (unsigned attempt = 0; attempt != 3; ++attempt)
            {
                ec.clear();
                std::filesystem::rename(*staging, entry, ec);
                if (!ec) { return CachePublication{entry, false}; }
                if (complete_cache_entry(entry, key))
                {
                    std::filesystem::remove_all(*staging, ec);
                    return CachePublication{entry, true};
                }
                ec.clear();
                if (std::filesystem::exists(entry, ec) && !ec)
                {
                    const std::filesystem::path quarantine = root / unique_name(".incomplete");
                    std::filesystem::rename(entry, quarantine, ec);
                    if (!ec) { continue; }
                    if (complete_cache_entry(entry, key))
                    {
                        std::filesystem::remove_all(*staging, ec);
                        return CachePublication{entry, true};
                    }
                }
                break;
            }
            warning = "cannot publish HGL native cache entry '" + entry.string() + "': " + ec.message();
            std::filesystem::remove_all(*staging, ec);
            return std::nullopt;
        }

        bool load_native_image(const std::filesystem::path &image_path,
                               const std::filesystem::path &retained_directory, std::string &error)
        {
            void *image = ::dlopen(image_path.c_str(), RTLD_NOW | RTLD_LOCAL);
            if (image == nullptr)
            {
                const char *load_error = ::dlerror();
                error = "cannot load native artifact '" + image_path.string() + "': " +
                        (load_error != nullptr ? std::string{load_error} : std::string{"unknown loader error"}) +
                        "; artifacts retained in '" + retained_directory.string() + "'";
                return false;
            }
            ::dlerror();
            void *symbol = ::dlsym(image, registration_symbol.data());
            if (const char *load_error = ::dlerror(); load_error != nullptr)
            {
                error = "native artifact has no registration entry point: " + std::string{load_error} +
                        "; artifacts retained in '" + retained_directory.string() + "'";
                ::dlclose(image);
                return false;
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
                        "; artifacts retained in '" + retained_directory.string() + "'";
                return false;
            }
            catch (...)
            {
                error = "native module registration failed with an unknown exception; artifacts retained in '" +
                        retained_directory.string() + "'";
                return false;
            }
            return true;
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
        std::string stem{source_stem};
        if (stem.empty()) { stem = "module"; }
        std::ostringstream bootstrap;
        bootstrap << "#include \"" << stem << ".h\"\n\n"
                     "extern \"C\" __attribute__((visibility(\"default\"))) void "
                  << registration_symbol << "()\n{\n    " << module.namespace_name << "::register_operators();\n}\n";

        const BuildContext context = build_context();
        std::string        key;
        std::string        cache_error;
        std::optional<std::filesystem::path> root;
        if (environment_flag("HGL_DISABLE_CACHE"))
        {
            trace_cache("disabled");
        }
        else if (!context.cache_unavailable_reason.empty())
        {
            trace_cache("unavailable: " + context.cache_unavailable_reason);
        }
        else
        {
            root = cache_root(cache_error);
            if (!root && !cache_error.empty()) { trace_cache("unavailable: " + cache_error); }
        }
        if (root)
        {
            key = cache_key(module, stem, bootstrap.str(), context);
            const std::filesystem::path entry = *root / key;
            if (complete_cache_entry(entry, key))
            {
                trace_cache("hit " + key);
                if (!load_native_image(entry / image_name(), entry, error)) { return std::nullopt; }
                return NativeModule{entry, key, true};
            }
            trace_cache("miss " + key);
        }

        const std::optional<std::filesystem::path> artifact_directory = make_artifact_directory(error);
        if (!artifact_directory) { return std::nullopt; }
        const std::filesystem::path header_path = *artifact_directory / (stem + ".h");
        const std::filesystem::path source_path = *artifact_directory / (stem + ".cpp");
        const std::filesystem::path bootstrap_path = *artifact_directory / "hgl_module.cpp";
#if defined(__APPLE__)
        const std::filesystem::path image_path = *artifact_directory / (stem + ".bundle");
#else
        const std::filesystem::path image_path = *artifact_directory / (stem + ".so");
#endif
        if (!write_file(header_path, module.header, error) || !write_file(source_path, module.source, error) ||
            !write_file(bootstrap_path, bootstrap.str(), error))
        {
            error += "; artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }

        std::vector<std::string> command = context.arguments;
        command.push_back("-I" + artifact_directory->string());
        command.push_back(source_path.string());
        command.push_back(bootstrap_path.string());
        command.emplace_back("-o");
        command.push_back(image_path.string());
        const ProcessResult compiled = run_process(command);
        if (compiled.status != 0)
        {
            error = "native compilation failed with '" + context.compiler + "' (exit " +
                    std::to_string(compiled.status) + ")";
            if (!compiled.output.empty())
            {
                error += ":\n" + compiled.output;
                if (error.back() != '\n') { error += '\n'; }
            }
            error += "artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }

        std::filesystem::path load_path = image_path;
        std::filesystem::path result_directory = *artifact_directory;
        bool                  reused = false;
        if (root)
        {
            std::string warning;
            if (const std::optional<CachePublication> published =
                    publish_cache(*root, key, *artifact_directory, stem, context, warning))
            {
                result_directory = published->entry;
                load_path = published->entry / image_name();
                reused = published->reused;
                if (reused) { trace_cache("filled concurrently " + key); }
            }
            else
            {
                trace_cache("publish skipped: " + warning);
            }
        }
        if (!load_native_image(load_path, result_directory, error))
        {
            if (load_path != image_path)
            {
                error += "; build artifacts retained in '" + artifact_directory->string() + "'";
            }
            return std::nullopt;
        }
        std::error_code cleanup_error;
        std::filesystem::remove_all(*artifact_directory, cleanup_error);
        return NativeModule{result_directory, key, reused};
#endif
    }
}  // namespace hgl::driver
