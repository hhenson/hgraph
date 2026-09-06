#include "driver/native_module.h"

#include "driver/cpp_formatter.h"
#include "driver/process.h"
#include "hgl_native_compile_config.h"

#include <hgraph/util/scope.h>
#include <hgraph/util/sha256.h>
#include <hgraph/version.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
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
    #include <unistd.h>
#endif

namespace hgl::driver
{
    namespace
    {
        constexpr std::string_view query_symbol = HGL_NATIVE_MODULE_QUERY_SYMBOL_V1;
        constexpr std::string_view cache_format = "hgl-native-cache-v3";

        std::atomic<std::uint64_t> next_directory{0};

        std::vector<std::string> split(std::string_view text, char separator) {
            std::vector<std::string> values;
            std::size_t              begin = 0;
            while (begin <= text.size()) {
                const std::size_t end = text.find(separator, begin);
                std::string       value{text.substr(begin, end == std::string_view::npos ? text.size() - begin : end - begin)};
                if (!value.empty() && std::find(values.begin(), values.end(), value) == values.end()) {
                    values.push_back(std::move(value));
                }
                if (end == std::string_view::npos) { break; }
                begin = end + 1;
            }
            return values;
        }

        bool environment_flag(std::string_view name) {
            const std::string key{name};
            const char       *value = std::getenv(key.c_str());
            return value != nullptr && *value != '\0' && std::string_view{value} != "0";
        }

        void trace_cache(std::string_view message) {
            if (environment_flag("HGL_CACHE_TRACE")) { std::cerr << "hgl native cache " << message << '\n'; }
        }

        bool write_file(const std::filesystem::path &path, std::string_view contents, std::string &error) {
            std::ofstream out{path, std::ios::binary | std::ios::trunc};
            if (!out) {
                error = "cannot write native artifact '" + path.string() + "'";
                return false;
            }
            out << contents;
            if (!out) {
                error = "cannot finish native artifact '" + path.string() + "'";
                return false;
            }
            return true;
        }

        std::optional<std::string> read_file(const std::filesystem::path &path) {
            std::ifstream in{path, std::ios::binary};
            if (!in) { return std::nullopt; }
            std::ostringstream contents;
            contents << in.rdbuf();
            if (in.bad()) { return std::nullopt; }
            return std::move(contents).str();
        }

        unsigned long process_id() {
#if defined(_WIN32)
            return static_cast<unsigned long>(GetCurrentProcessId());
#else
            return static_cast<unsigned long>(::getpid());
#endif
        }

        std::string unique_name(std::string_view prefix) {
            return std::string{prefix} + "-" + std::to_string(process_id()) + "-" + std::to_string(next_directory.fetch_add(1));
        }

        std::optional<std::filesystem::path> make_unique_directory(const std::filesystem::path &root, std::string_view prefix,
                                                                   std::string &error) {
            std::error_code ec;
            std::filesystem::create_directories(root, ec);
            if (ec) {
                error = "cannot create directory '" + root.string() + "': " + ec.message();
                return std::nullopt;
            }
            for (unsigned attempt = 0; attempt != 1000; ++attempt) {
                const std::filesystem::path candidate = root / unique_name(prefix);
                ec.clear();
                if (std::filesystem::create_directory(candidate, ec)) { return candidate; }
                if (ec && ec != std::errc::file_exists) {
                    error = "cannot create directory '" + candidate.string() + "': " + ec.message();
                    return std::nullopt;
                }
            }
            error = "cannot allocate a unique directory under '" + root.string() + "'";
            return std::nullopt;
        }

        std::optional<std::filesystem::path> make_artifact_directory(std::string &error) {
            std::error_code       ec;
            std::filesystem::path root;
            if (const char *configured = std::getenv("HGL_ARTIFACT_DIR"); configured != nullptr && *configured != '\0') {
                root = configured;
            } else {
                root = std::filesystem::temp_directory_path(ec);
                if (ec) {
                    error = "cannot locate the temporary directory: " + ec.message();
                    return std::nullopt;
                }
            }
            return make_unique_directory(root, "hgl", error);
        }

        std::string native_error_message(const hgl_native_module_error_v1 &error) {
            std::size_t length = 0;
            while (length != HGL_NATIVE_MODULE_ERROR_CAPACITY && error.message[length] != '\0') { ++length; }
            return std::string{error.message, length};
        }

#if !defined(_WIN32)
        std::filesystem::path executable_path() {
    #if defined(__APPLE__)
            std::uint32_t     size = 1024;
            std::vector<char> buffer(size);
            if (::_NSGetExecutablePath(buffer.data(), &size) != 0) {
                buffer.resize(size);
                if (::_NSGetExecutablePath(buffer.data(), &size) != 0) { return {}; }
            }
            return std::filesystem::path{buffer.data()};
    #elif defined(__linux__)
            std::vector<char> buffer(4096);
            const ssize_t     count = ::readlink("/proc/self/exe", buffer.data(), buffer.size());
            return count > 0 ? std::filesystem::path{std::string{buffer.data(), static_cast<std::size_t>(count)}}
                             : std::filesystem::path{};
    #else
            return {};
    #endif
        }

        std::vector<void *> &resident_images() {
            // Deliberately process-lifetime: registry candidates hold code
            // pointers into every image registered here.
            static auto *images = new std::vector<void *>;
            return *images;
        }

        void hash_text(hgraph::util::Sha256 &hasher, std::string_view text) {
            hasher.update(std::as_bytes(std::span{text.data(), text.size()}));
        }

        void hash_field(hgraph::util::Sha256 &hasher, std::string_view name, std::string_view value) {
            static constexpr std::string_view separator{"\0", 1};
            hash_text(hasher, name);
            hash_text(hasher, separator);
            hash_text(hasher, std::to_string(value.size()));
            hash_text(hasher, separator);
            hash_text(hasher, value);
            hash_text(hasher, separator);
        }

        std::string hex_digest(const hgraph::util::Sha256Digest &digest) {
            const std::array<char, 64> hex = hgraph::util::sha256_hex(digest);
            return {hex.data(), hex.size()};
        }

        std::optional<std::string> file_digest(const std::filesystem::path &path) {
            std::ifstream in{path, std::ios::binary};
            if (!in) { return std::nullopt; }
            hgraph::util::Sha256        hasher;
            std::array<char, 64 * 1024> buffer{};
            while (in) {
                in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                const std::streamsize count = in.gcount();
                if (count > 0) { hasher.update(std::as_bytes(std::span{buffer.data(), static_cast<std::size_t>(count)})); }
            }
            if (!in.eof()) { return std::nullopt; }
            return hex_digest(hasher.finish());
        }

        std::optional<std::filesystem::path> canonical_regular_file(const std::filesystem::path &path) {
            if (path.empty()) { return std::nullopt; }
            std::error_code       ec;
            std::filesystem::path absolute = path;
            if (absolute.is_relative()) {
                absolute = std::filesystem::absolute(absolute, ec);
                if (ec) { return std::nullopt; }
            }
            const std::filesystem::path canonical = std::filesystem::weakly_canonical(absolute, ec);
            if (ec || !std::filesystem::is_regular_file(canonical, ec) || ec) { return std::nullopt; }
            return canonical;
        }

        std::optional<std::filesystem::path> resolve_executable(std::string_view command) {
            const std::filesystem::path requested{command};
            if (requested.has_parent_path()) {
                const std::optional<std::filesystem::path> resolved = canonical_regular_file(requested);
                return resolved && ::access(resolved->c_str(), X_OK) == 0 ? resolved : std::nullopt;
            }

            const char *path_value = std::getenv("PATH");
            if (path_value == nullptr) { return std::nullopt; }
            const std::string_view path{path_value};
            std::size_t            begin = 0;
            while (begin <= path.size()) {
                const std::size_t      end = path.find(':', begin);
                const std::string_view directory =
                    path.substr(begin, end == std::string_view::npos ? path.size() - begin : end - begin);
                const std::filesystem::path candidate =
                    (directory.empty() ? std::filesystem::path{"."} : std::filesystem::path{directory}) / requested;
                if (const std::optional<std::filesystem::path> resolved = canonical_regular_file(candidate);
                    resolved && ::access(resolved->c_str(), X_OK) == 0) {
                    return resolved;
                }
                if (end == std::string_view::npos) { break; }
                begin = end + 1;
            }
            return std::nullopt;
        }

        std::string probe_compiler(const std::vector<std::string> &prefix, std::string_view argument) {
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

        void mark_cache_unavailable(BuildContext &context, std::string reason) {
            if (context.cache_unavailable_reason.empty()) { context.cache_unavailable_reason = std::move(reason); }
        }

        BuildContext build_context() {
            BuildContext context;
            const char  *compiler_override = std::getenv("HGL_CXX");
            context.compiler  = compiler_override != nullptr && *compiler_override != '\0' ? std::string{compiler_override}
                                                                                           : std::string{native_config::compiler};
            context.arguments = {context.compiler};
            context.compiler_version = probe_compiler(context.arguments, "--version");
            context.compiler_target  = probe_compiler(context.arguments, "-dumpmachine");
            if (const std::optional<std::filesystem::path> compiler = resolve_executable(context.compiler)) {
                context.compiler_executable = *compiler;
                if (const std::optional<std::string> digest = file_digest(*compiler)) {
                    context.compiler_digest = *digest;
                } else {
                    mark_cache_unavailable(context, "compiler executable cannot be hashed");
                }
            } else {
                mark_cache_unavailable(context, "compiler executable cannot be identified");
            }

            context.arguments.emplace_back("-std=c++23");
            context.arguments.emplace_back("-fPIC");
            context.arguments.emplace_back("-fvisibility=hidden");
            for (const std::string &option : split(native_config::compile_options, ';')) { context.arguments.push_back(option); }
            for (const std::string &option : split(native_config::link_options, ';')) { context.arguments.push_back(option); }
            for (const std::string &definition : split(native_config::definitions, ';')) {
                context.arguments.push_back("-D" + definition);
            }

            if (const std::optional<std::filesystem::path> executable = canonical_regular_file(executable_path())) {
                context.executable = *executable;
                if (const std::optional<std::string> digest = file_digest(*executable)) {
                    context.executable_digest = *digest;
                } else {
                    mark_cache_unavailable(context, "hosting hgl executable cannot be hashed");
                }
                const std::filesystem::path installed_include =
                    (context.executable.parent_path() / native_config::install_include_from_bindir).lexically_normal();
                std::error_code include_error;
                if (std::filesystem::is_directory(installed_include, include_error)) {
                    context.arguments.push_back("-I" + installed_include.string());
                }
            } else {
                mark_cache_unavailable(context, "hosting hgl executable cannot be identified");
            }
            for (const std::string &include : split(native_config::include_directories, ';')) {
                context.arguments.push_back("-I" + include);
            }
    #if defined(__APPLE__)
            context.arguments.emplace_back("-bundle");
            context.arguments.emplace_back("-undefined");
            context.arguments.emplace_back("dynamic_lookup");
            if (!context.executable.empty()) {
                context.arguments.emplace_back("-bundle_loader");
                context.arguments.push_back(context.executable.string());
            }
    #else
            context.arguments.emplace_back("-shared");
    #endif
            return context;
        }

        std::string cache_key(const codegen::EmittedModule &module, std::string_view stem, std::string_view bootstrap,
                              const BuildContext &context) {
            hgraph::util::Sha256 hasher;
            hash_field(hasher, "format", cache_format);
            hash_field(hasher, "header-name", std::string{stem} + ".h");
            hash_field(hasher, "header", module.header);
            hash_field(hasher, "source", module.source);
            hash_field(hasher, "descriptor", module.descriptor);
            hash_field(hasher, "bootstrap", bootstrap);
            hash_field(hasher, "query-symbol", query_symbol);
            hash_field(hasher, "compiler-version", context.compiler_version);
            hash_field(hasher, "compiler-target", context.compiler_target);
            hash_field(hasher, "compiler-executable", context.compiler_executable.string());
            hash_field(hasher, "compiler-executable-digest", context.compiler_digest);
            for (std::size_t i = 0; i < context.arguments.size(); ++i) {
                hash_field(hasher, "argument-" + std::to_string(i), context.arguments[i]);
            }
            hash_field(hasher, "system", native_config::system_name);
            hash_field(hasher, "processor", native_config::system_processor);
            hash_field(hasher, "configuration", native_config::configuration);
            hash_field(hasher, "hgraph-version", hgraph::version_string);
            hash_field(hasher, "hgraph-commit", hgraph::git_commit_hash);
            hash_field(hasher, "hgl-executable", context.executable.string());
            hash_field(hasher, "hgl-executable-digest", context.executable_digest);
            static constexpr std::array<std::string_view, 13> compiler_environment{"PATH",
                                                                                   "SDKROOT",
                                                                                   "MACOSX_DEPLOYMENT_TARGET",
                                                                                   "CPATH",
                                                                                   "CPLUS_INCLUDE_PATH",
                                                                                   "C_INCLUDE_PATH",
                                                                                   "OBJC_INCLUDE_PATH",
                                                                                   "LIBRARY_PATH",
                                                                                   "LD_LIBRARY_PATH",
                                                                                   "DYLD_LIBRARY_PATH",
                                                                                   "GCC_EXEC_PREFIX",
                                                                                   "COMPILER_PATH",
                                                                                   "SOURCE_DATE_EPOCH"};
            for (const std::string_view name : compiler_environment) {
                const std::string key{name};
                const char       *value = std::getenv(key.c_str());
                hash_field(hasher, "environment-" + key, value != nullptr ? std::string_view{value} : std::string_view{"<unset>"});
            }
            return hex_digest(hasher.finish());
        }

        std::optional<std::filesystem::path> cache_root(std::string &error) {
            if (environment_flag("HGL_DISABLE_CACHE")) { return std::nullopt; }

            std::filesystem::path root;
            if (const char *configured = std::getenv("HGL_CACHE_DIR"); configured != nullptr && *configured != '\0') {
                root = configured;
            } else if (const char *xdg = std::getenv("XDG_CACHE_HOME"); xdg != nullptr && *xdg != '\0') {
                root = std::filesystem::path{xdg} / "hgl" / "native";
            } else if (const char *home = std::getenv("HOME"); home != nullptr && *home != '\0') {
    #if defined(__APPLE__)
                root = std::filesystem::path{home} / "Library" / "Caches" / "hgl" / "native";
    #else
                root = std::filesystem::path{home} / ".cache" / "hgl" / "native";
    #endif
            } else {
                error = "no per-user cache directory is available; set HGL_CACHE_DIR, XDG_CACHE_HOME, or HOME";
                return std::nullopt;
            }
            root /= "v3";
            std::error_code ec;
            std::filesystem::create_directories(root, ec);
            if (ec) {
                error = "cannot create HGL native cache '" + root.string() + "': " + ec.message();
                return std::nullopt;
            }
            return root;
        }

        std::string image_name() {
    #if defined(__APPLE__)
            return "module.bundle";
    #else
            return "module.so";
    #endif
        }

        bool complete_cache_entry(const std::filesystem::path &entry, std::string_view key, std::string_view stem,
                                  std::string_view expected_descriptor) {
            const std::filesystem::path image      = entry / image_name();
            const std::filesystem::path descriptor = entry / (std::string{stem} + ".hgl-module.json");
            std::error_code             ec;
            if (!std::filesystem::is_regular_file(image, ec) || ec) { return false; }
            const std::optional<std::string> digest            = file_digest(image);
            const std::optional<std::string> marker            = read_file(entry / "complete");
            const std::optional<std::string> cached_descriptor = read_file(descriptor);
            return digest && marker && cached_descriptor && *cached_descriptor == expected_descriptor &&
                   *marker == std::string{key} + "\n" + *digest + "\n";
        }

        struct CachePublication
        {
            std::filesystem::path entry{};
            bool                  reused{false};
        };

        std::optional<CachePublication> publish_cache(const std::filesystem::path &root, std::string_view key,
                                                      const std::filesystem::path &artifact_directory, std::string_view stem,
                                                      std::string_view expected_descriptor, const BuildContext &context,
                                                      std::string &warning) {
            const std::filesystem::path entry = root / key;
            if (complete_cache_entry(entry, key, stem, expected_descriptor)) { return CachePublication{entry, true}; }

            const std::optional<std::filesystem::path> staging = make_unique_directory(root, ".staging", warning);
            if (!staging) { return std::nullopt; }
            const std::filesystem::path source_image = artifact_directory / (std::string{stem} +
    #if defined(__APPLE__)
                                                                             ".bundle");
    #else
                                                                             ".so");
    #endif
            const std::array<std::pair<std::filesystem::path, std::filesystem::path>, 5> copies{
                std::pair{artifact_directory / (std::string{stem} + ".h"), *staging / (std::string{stem} + ".h")},
                std::pair{artifact_directory / (std::string{stem} + ".cpp"), *staging / (std::string{stem} + ".cpp")},
                std::pair{artifact_directory / (std::string{stem} + ".hgl-module.json"),
                          *staging / (std::string{stem} + ".hgl-module.json")},
                std::pair{artifact_directory / "hgl_module.cpp", *staging / "hgl_module.cpp"},
                std::pair{source_image, *staging / image_name()}};
            std::error_code ec;
            for (const auto &[source, destination] : copies) {
                std::filesystem::copy_file(source, destination, std::filesystem::copy_options::overwrite_existing, ec);
                if (ec) {
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
            std::string                      write_error;
            const std::optional<std::string> digest = file_digest(*staging / image_name());
            if (!digest || !write_file(*staging / "manifest.txt", manifest.str(), write_error) ||
                !write_file(*staging / "complete", std::string{key} + "\n" + digest.value_or("") + "\n", write_error)) {
                warning = digest ? write_error : "cannot hash staged HGL native image";
                std::filesystem::remove_all(*staging, ec);
                return std::nullopt;
            }

            for (unsigned attempt = 0; attempt != 3; ++attempt) {
                ec.clear();
                std::filesystem::rename(*staging, entry, ec);
                if (!ec) { return CachePublication{entry, false}; }
                if (complete_cache_entry(entry, key, stem, expected_descriptor)) {
                    std::filesystem::remove_all(*staging, ec);
                    return CachePublication{entry, true};
                }
                ec.clear();
                if (std::filesystem::exists(entry, ec) && !ec) {
                    const std::filesystem::path quarantine = root / unique_name(".incomplete");
                    std::filesystem::rename(entry, quarantine, ec);
                    if (!ec) { continue; }
                    if (complete_cache_entry(entry, key, stem, expected_descriptor)) {
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

        bool load_native_image(const std::filesystem::path &image_path, const std::filesystem::path &retained_directory,
                               std::string_view expected_identity, std::string_view expected_fingerprint,
                               const hgl_native_module_v1 *&module_abi, std::string &error) {
            void *image = ::dlopen(image_path.c_str(), RTLD_NOW | RTLD_LOCAL);
            if (image == nullptr) {
                const char *load_error = ::dlerror();
                error = "cannot load native artifact '" + image_path.string() +
                        "': " + (load_error != nullptr ? std::string{load_error} : std::string{"unknown loader error"}) +
                        "; artifacts retained in '" + retained_directory.string() + "'";
                return false;
            }
            ::dlerror();
            void *symbol = ::dlsym(image, query_symbol.data());
            if (const char *load_error = ::dlerror(); load_error != nullptr) {
                error = "native artifact has no HGL module query entry point: " + std::string{load_error} +
                        "; artifacts retained in '" + retained_directory.string() + "'";
                ::dlclose(image);
                return false;
            }
            const auto query = reinterpret_cast<hgl_query_native_module_fn_v1>(symbol);
            if (!hgraph::fallback_on_exception(
                    false,
                    [&] {
                        module_abi = query(HGL_NATIVE_MODULE_ABI_V1);
                        return true;
                    },
                    [&](std::string_view message) {
                        error = "native artifact module query failed: ";
                        error.append(message);
                    }) ||
                !validate_native_module_abi(module_abi, expected_identity, expected_fingerprint, error)) {
                error += "; artifacts retained in '" + retained_directory.string() + "'";
                ::dlclose(image);
                return false;
            }
            resident_images().push_back(image);
            return true;
        }
#endif
    }  // namespace

    bool validate_native_module_abi(const hgl_native_module_v1 *module, std::string_view expected_identity,
                                    std::string_view expected_fingerprint, std::string &error) {
        if (module == nullptr) {
            error = "native artifact does not support HGL native module ABI v1";
            return false;
        }
        if (module->abi_version != HGL_NATIVE_MODULE_ABI_V1 || module->struct_size < sizeof(*module)) {
            error = "native artifact returned an incompatible HGL native module ABI table";
            return false;
        }
        if (module->module_identity == nullptr || std::string_view{module->module_identity} != expected_identity) {
            error = "native artifact identity does not match descriptor module '" + std::string{expected_identity} + "'";
            return false;
        }
        if (module->descriptor_fingerprint == nullptr || std::string_view{module->descriptor_fingerprint} != expected_fingerprint) {
            error = "native artifact descriptor fingerprint does not match its descriptor";
            return false;
        }
        if (module->context == nullptr || module->init == nullptr || module->deinit == nullptr || module->is_active == nullptr) {
            error = "native artifact returned an incomplete HGL native module ABI table";
            return false;
        }
        return true;
    }

    static std::optional<NativeModule> compile_native_module(const codegen::EmittedModule &module, std::string_view source_stem,
                                                             std::string &error) {
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
                  << "#include <hgl/native_module_abi.h>\n"
                     "#include <hgraph/util/scope.h>\n\n"
                     "#include <cstddef>\n"
                     "#include <cstdint>\n"
                     "#include <string_view>\n\n"
                     "namespace\n"
                     "{\n"
                     "    struct ModuleState\n"
                     "    {\n"
                     "        hgraph::OperatorProviderHandle provider{};\n"
                     "    };\n\n"
                     "    ModuleState module_state{};\n\n"
                     "    void set_error(hgl_native_module_error_v1 *error, std::string_view message) noexcept\n"
                     "    {\n"
                     "        if (error == nullptr || error->struct_size < sizeof(*error))\n"
                     "        {\n"
                     "            return;\n"
                     "        }\n"
                     "        const std::size_t length =\n"
                     "            message.size() < HGL_NATIVE_MODULE_ERROR_CAPACITY - 1\n"
                     "                ? message.size()\n"
                     "                : HGL_NATIVE_MODULE_ERROR_CAPACITY - 1;\n"
                     "        for (std::size_t i = 0; i != length; ++i)\n"
                     "        {\n"
                     "            error->message[i] = message[i];\n"
                     "        }\n"
                     "        error->message[length] = '\\0';\n"
                     "    }\n\n"
                     "    std::int32_t init_module(void *context, hgl_native_module_error_v1 *error) noexcept\n"
                     "    {\n"
                     "        auto &state = *static_cast<ModuleState *>(context);\n"
                     "        if (state.provider.active())\n"
                     "        {\n"
                     "            return HGL_NATIVE_MODULE_OK;\n"
                     "        }\n"
                     "        return hgraph::fallback_on_exception(\n"
                     "            HGL_NATIVE_MODULE_ERROR,\n"
                     "            [&] {\n"
                     "                state.provider = "
                  << module.namespace_name
                  << "::register_operators();\n"
                     "                if (!state.provider.valid() || !state.provider.active())\n"
                     "                {\n"
                     "                    state.provider = {};\n"
                     "                    set_error(error, \"operator registration returned no active provider\");\n"
                     "                    return HGL_NATIVE_MODULE_ERROR;\n"
                     "                }\n"
                     "                return HGL_NATIVE_MODULE_OK;\n"
                     "            },\n"
                     "            [&](std::string_view message) { set_error(error, message); });\n"
                     "    }\n\n"
                     "    std::int32_t deinit_module(void *context, hgl_native_module_error_v1 *error) noexcept\n"
                     "    {\n"
                     "        auto &state = *static_cast<ModuleState *>(context);\n"
                     "        if (!state.provider.valid() || !state.provider.active())\n"
                     "        {\n"
                     "            state.provider = {};\n"
                     "            return HGL_NATIVE_MODULE_OK;\n"
                     "        }\n"
                     "        return hgraph::fallback_on_exception(\n"
                     "            HGL_NATIVE_MODULE_ERROR,\n"
                     "            [&] {\n"
                     "                if (!hgraph::OperatorRegistry::instance().remove_provider(state.provider))\n"
                     "                {\n"
                     "                    set_error(error, \"operator provider is stale\");\n"
                     "                    return HGL_NATIVE_MODULE_ERROR;\n"
                     "                }\n"
                     "                state.provider = {};\n"
                     "                return HGL_NATIVE_MODULE_OK;\n"
                     "            },\n"
                     "            [&](std::string_view message) { set_error(error, message); });\n"
                     "    }\n\n"
                     "    std::int32_t is_module_active(const void *context) noexcept\n"
                     "    {\n"
                     "        const auto &state = *static_cast<const ModuleState *>(context);\n"
                     "        return state.provider.active() ? 1 : 0;\n"
                     "    }\n\n"
                     "    const hgl_native_module_v1 module_abi{\n"
                     "        HGL_NATIVE_MODULE_ABI_V1,\n"
                     "        sizeof(hgl_native_module_v1),\n"
                     "        \""
                  << module.module_name
                  << "\",\n"
                     "        \""
                  << module.descriptor_fingerprint
                  << "\",\n"
                     "        &module_state,\n"
                     "        init_module,\n"
                     "        deinit_module,\n"
                     "        is_module_active,\n"
                     "    };\n"
                     "}  // namespace\n\n"
                     "extern \"C\" HGL_NATIVE_MODULE_EXPORT const hgl_native_module_v1 *\n"
                  << query_symbol << "(std::uint32_t requested_abi) noexcept\n"
                  << "{\n"
                     "    return requested_abi == HGL_NATIVE_MODULE_ABI_V1 ? &module_abi : nullptr;\n"
                     "}\n";

        codegen::EmittedModule formatted_bootstrap;
        formatted_bootstrap.source = bootstrap.str();
        std::string format_error;
        if (!format_cpp(formatted_bootstrap, format_error)) {
            error = "cannot format native module bootstrap: " + format_error;
            return std::nullopt;
        }
        const std::string &bootstrap_source = formatted_bootstrap.source;

        const BuildContext                   context = build_context();
        std::string                          key;
        std::string                          cache_error;
        std::optional<std::filesystem::path> root;
        if (environment_flag("HGL_DISABLE_CACHE")) {
            trace_cache("disabled");
        } else if (!context.cache_unavailable_reason.empty()) {
            trace_cache("unavailable: " + context.cache_unavailable_reason);
        } else {
            root = cache_root(cache_error);
            if (!root && !cache_error.empty()) { trace_cache("unavailable: " + cache_error); }
        }
        if (root) {
            key                               = cache_key(module, stem, bootstrap_source, context);
            const std::filesystem::path entry = *root / key;
            if (complete_cache_entry(entry, key, stem, module.descriptor)) {
                trace_cache("hit " + key);
                const hgl_native_module_v1 *module_abi = nullptr;
                if (!load_native_image(entry / image_name(), entry, module.module_name, module.descriptor_fingerprint, module_abi,
                                       error)) {
                    return std::nullopt;
                }
                return NativeModule{entry, key, true, module_abi};
            }
            trace_cache("miss " + key);
        }

        const std::optional<std::filesystem::path> artifact_directory = make_artifact_directory(error);
        if (!artifact_directory) { return std::nullopt; }
        const std::filesystem::path header_path     = *artifact_directory / (stem + ".h");
        const std::filesystem::path source_path     = *artifact_directory / (stem + ".cpp");
        const std::filesystem::path descriptor_path = *artifact_directory / (stem + ".hgl-module.json");
        const std::filesystem::path bootstrap_path  = *artifact_directory / "hgl_module.cpp";
    #if defined(__APPLE__)
        const std::filesystem::path image_path = *artifact_directory / (stem + ".bundle");
    #else
        const std::filesystem::path image_path = *artifact_directory / (stem + ".so");
    #endif
        if (!write_file(header_path, module.header, error) || !write_file(source_path, module.source, error) ||
            !write_file(descriptor_path, module.descriptor, error) || !write_file(bootstrap_path, bootstrap_source, error)) {
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
        if (compiled.status != 0) {
            error = "native compilation failed with '" + context.compiler + "' (exit " + std::to_string(compiled.status) + ")";
            if (!compiled.output.empty()) {
                error += ":\n" + compiled.output;
                if (error.back() != '\n') { error += '\n'; }
            }
            error += "artifacts retained in '" + artifact_directory->string() + "'";
            return std::nullopt;
        }

        std::filesystem::path load_path        = image_path;
        std::filesystem::path result_directory = *artifact_directory;
        bool                  reused           = false;
        if (root) {
            std::string warning;
            if (const std::optional<CachePublication> published =
                    publish_cache(*root, key, *artifact_directory, stem, module.descriptor, context, warning)) {
                result_directory = published->entry;
                load_path        = published->entry / image_name();
                reused           = published->reused;
                if (reused) { trace_cache("filled concurrently " + key); }
            } else {
                trace_cache("publish skipped: " + warning);
            }
        }
        const hgl_native_module_v1 *module_abi = nullptr;
        if (!load_native_image(load_path, result_directory, module.module_name, module.descriptor_fingerprint, module_abi, error)) {
            if (load_path != image_path) { error += "; build artifacts retained in '" + artifact_directory->string() + "'"; }
            return std::nullopt;
        }
        std::error_code cleanup_error;
        std::filesystem::remove_all(*artifact_directory, cleanup_error);
        return NativeModule{result_directory, key, reused, module_abi};
#endif
    }

    NativeModule::NativeModule(std::filesystem::path artifact_directory, std::string cache_key, bool cache_hit,
                               const hgl_native_module_v1 *module_abi) noexcept
        : artifact_directory(std::move(artifact_directory)), cache_key(std::move(cache_key)), cache_hit(cache_hit),
          module_abi_(module_abi) {}

    NativeModule::NativeModule(NativeModule &&other) noexcept
        : artifact_directory(std::move(other.artifact_directory)), cache_key(std::move(other.cache_key)),
          cache_hit(other.cache_hit), module_abi_(std::exchange(other.module_abi_, nullptr)),
          owns_activation_(std::exchange(other.owns_activation_, false)) {}

    NativeModule &NativeModule::operator=(NativeModule &&other) {
        if (this != &other) {
            const bool aliases_same_module = module_abi_ != nullptr && module_abi_ == other.module_abi_;
            if (!aliases_same_module) {
                std::string error;
                if (!deactivate(error)) { throw std::runtime_error(std::move(error)); }
            }
            artifact_directory = std::move(other.artifact_directory);
            cache_key          = std::move(other.cache_key);
            cache_hit          = other.cache_hit;
            module_abi_        = std::exchange(other.module_abi_, nullptr);
            if (aliases_same_module) {
                owns_activation_ = std::exchange(other.owns_activation_, false) || owns_activation_;
            } else {
                owns_activation_ = std::exchange(other.owns_activation_, false);
            }
        }
        return *this;
    }

    NativeModule::~NativeModule() {
        static_cast<void>(hgraph::fallback_on_exception(false, [this] {
            std::string ignored;
            return deactivate(ignored);
        }));
    }

    bool NativeModule::active() const noexcept {
        return module_abi_ != nullptr && module_abi_->is_active != nullptr &&
               hgraph::fallback_on_exception(false, [this] { return module_abi_->is_active(module_abi_->context) != 0; });
    }

    bool NativeModule::activate(std::string &error) {
        error.clear();
        if (active()) { return true; }
        if (module_abi_ == nullptr || module_abi_->init == nullptr) {
            error = "native module has no lifecycle ABI";
            return false;
        }
        hgl_native_module_error_v1 native_error{sizeof(hgl_native_module_error_v1), {}};
        std::int32_t               status = HGL_NATIVE_MODULE_ERROR;
        if (!hgraph::fallback_on_exception(
                false,
                [&] {
                    status = module_abi_->init(module_abi_->context, &native_error);
                    return true;
                },
                [&](std::string_view message) {
                    error = "native module initialization failed: ";
                    error.append(message);
                    error += "; artifacts retained in '" + artifact_directory.string() + "'";
                })) {
            return false;
        }
        if (status != HGL_NATIVE_MODULE_OK) {
            const std::string detail = native_error_message(native_error);
            error                    = "native module initialization failed";
            if (!detail.empty()) { error += ": " + detail; }
            error += "; artifacts retained in '" + artifact_directory.string() + "'";
            return false;
        }
        if (!active()) {
            error = "native module initialization returned an inactive module; artifacts retained in '" +
                    artifact_directory.string() + "'";
            return false;
        }
        owns_activation_ = true;
        return true;
    }

    bool NativeModule::deactivate(std::string &error) {
        error.clear();
        if (!owns_activation_) { return true; }
        if (!active()) {
            owns_activation_ = false;
            return true;
        }
        if (module_abi_ == nullptr || module_abi_->deinit == nullptr) {
            error = "native module has no lifecycle ABI";
            return false;
        }
        hgl_native_module_error_v1 native_error{sizeof(hgl_native_module_error_v1), {}};
        std::int32_t               status = HGL_NATIVE_MODULE_ERROR;
        if (!hgraph::fallback_on_exception(
                false,
                [&] {
                    status = module_abi_->deinit(module_abi_->context, &native_error);
                    return true;
                },
                [&](std::string_view message) {
                    error = "cannot deactivate native module: ";
                    error.append(message);
                })) {
            return false;
        }
        if (status != HGL_NATIVE_MODULE_OK) {
            const std::string detail = native_error_message(native_error);
            error                    = "cannot deactivate native module";
            if (!detail.empty()) { error += ": " + detail; }
            if (!active()) { owns_activation_ = false; }
            return false;
        }
        if (active()) {
            error = "native module deinitialization left the module active";
            return false;
        }
        owns_activation_ = false;
        return true;
    }

    std::optional<NativeModule> compile_and_load_native_module(const codegen::EmittedModule &module, std::string_view source_stem,
                                                               std::string &error) {
        std::optional<NativeModule> native = compile_native_module(module, source_stem, error);
        if (!native || !native->activate(error)) { return std::nullopt; }
        return native;
    }

    bool compile_and_replace_native_module(const codegen::EmittedModule &module, std::string_view source_stem,
                                           std::optional<NativeModule> &active, std::string &error) {
        std::optional<NativeModule> replacement = compile_native_module(module, source_stem, error);
        if (!replacement) { return false; }
        if (!active) {
            if (!replacement->activate(error)) { return false; }
            active = std::move(*replacement);
            return true;
        }

        if (!active->deactivate(error)) { return false; }
        if (replacement->activate(error)) {
            active = std::move(*replacement);
            return true;
        }

        const std::string replacement_error = error;
        std::string       restore_error;
        if (!active->activate(restore_error)) {
            error = replacement_error + "; previous module restore also failed: " + restore_error;
            return false;
        }
        error = replacement_error;
        return false;
    }
}  // namespace hgl::driver
