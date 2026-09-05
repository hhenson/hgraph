#include "driver/driver.h"

#include "codegen/cpp_emitter.h"
#include "driver/cpp_formatter.h"
#include "driver/line_reader.h"
#include "driver/native_module.h"
#include "hgraph_ir/lower.h"
#include "hgraph_ir/printer.h"
#include "ir/hir_printer.h"
#include "ir/lower.h"
#include "ir/type_check.h"
#include "semantics/resolve.h"
#include "syntax/ast_printer.h"
#include "syntax/diagnostic.h"
#include "syntax/lexer.h"
#include "syntax/parser.h"
#include "syntax/source.h"
#include "syntax/temporal.h"
#include "wiring/backend.h"
#include "wiring/operator_types.h"

#include <hgraph/version.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace hgl::driver
{
    namespace
    {
        constexpr int exit_ok          = 0;
        constexpr int exit_diagnostics = 1;
        constexpr int exit_usage       = 2;

        void print_help() {
            std::cout << "hgl - experimental hgraph language toolchain\n\n"
                         "Usage:\n"
                         "  hgl check <file> [--dump-tokens] [--dump-ast] [--dump-hir] [--dump-hgraph-ir]\n"
                         "  hgl test <file> [test-name]...\n"
                         "  hgl run <file> [--entry <name>] [--mode sim|realtime]\n"
                         "          [--start <datetime>] [--end <datetime|duration>]\n"
                         "          [--set <name>=<constant expression>]...\n"
                         "  hgl emit-cpp <file> [--out-dir <dir> | --include-dir <dir> --src-dir <dir>]\n"
                         "               [--python <file.py> --python-native <module>] [--print]\n"
                         "  hgl repl\n"
                         "  hgl --help\n"
                         "  hgl --version\n\n"
                         "Commands:\n"
                         "  check     parse, resolve and type-check a module and report diagnostics\n"
                         "  test      run the module's test declarations\n"
                         "  run       bind an entry to a mode, clock and parameters, then execute it\n"
                         "  emit-cpp  write the module as a C++ header/source pair of public hgraph\n"
                         "            authoring code, named after the file, in the module's namespace;\n"
                         "            --python also writes the Python wrapper module\n"
                         "  repl      accumulate declarations and evaluate expressions interactively\n"
                         "            (line editing, history and completion on a terminal)\n\n"
                         "Native runtime environment (Unix):\n"
                         "  HGL_CACHE_DIR       override the content-addressed native cache root\n"
                         "  HGL_DISABLE_CACHE   compile through a transient artifact when non-zero\n"
                         "  HGL_CACHE_TRACE     report cache hits, misses and publication fallbacks\n"
                         "  HGL_ARTIFACT_DIR    override the transient/failed artifact root\n"
                         "  HGL_CLANG_FORMAT    override the clang-format executable\n"
                         "  HGL_CXX             override the scripted native C++ compiler\n";
        }

        void print_version(std::string_view tool_version) {
            std::cout << "hgl " << tool_version << " (hgraph api " << hgraph::version_string << ")\n";
        }

        int usage_error(std::string_view message) {
            std::cerr << "hgl: " << message << "; use --help for the current interface\n";
            return exit_usage;
        }

        std::optional<std::string> read_file(const std::string &path) {
            std::ifstream in{path, std::ios::binary};
            if (!in) { return std::nullopt; }
            return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
        }

        void dump_tokens(const syntax::SourceFile &file, const syntax::LexResult &lexed) {
            for (const syntax::Token &token : lexed.tokens) {
                const syntax::Location at = file.location(token.range.begin);
                std::cout << at.line << ':' << at.column << ' ' << syntax::token_kind_name(token.kind);
                if (token.kind != syntax::TokenKind::Newline && token.kind != syntax::TokenKind::EndOfFile) {
                    std::cout << ' ' << token.text;
                }
                if (token.temporal_value) { std::cout << " = " << syntax::canonical_spelling(*token.temporal_value); }
                std::cout << '\n';
            }
        }

        /// One compilation unit through the frontend: parse, then resolve
        /// against the hgraph registry (developer guide, "Interim kernel
        /// table"). ``ok`` is false when either step reported.
        struct Unit
        {
            syntax::SourceFile               file;
            syntax::DiagnosticSink           diagnostics{};
            syntax::ast::Module              module{};
            semantics::ResolvedModule        resolved{};
            ir::hir::Module                  hir{};
            std::optional<hgraph_ir::Module> hgraph{};
            bool                             ok{false};

            Unit(std::string path, std::string text) : file{std::move(path), std::move(text)} {}
        };

        void frontend(Unit &unit) {
            unit.module = syntax::parse(unit.file, unit.diagnostics);
            if (unit.diagnostics.has_errors()) { return; }
            unit.resolved = semantics::resolve(unit.file, unit.module, wiring::has_operator, unit.diagnostics);
            if (unit.diagnostics.has_errors()) { return; }
            unit.hir = ir::lower_to_hir(unit.module, unit.resolved, unit.diagnostics);
            if (unit.diagnostics.has_errors()) { return; }
            if (ir::complete_hir(unit.hir, wiring::resolve_operator_types, unit.diagnostics)) {
                unit.hgraph = hgraph_ir::lower(unit.hir, unit.diagnostics);
            }
            unit.ok = !unit.diagnostics.has_errors();
        }

        std::optional<Unit> load(const std::string &path) {
            std::optional<std::string> text = read_file(path);
            if (!text) {
                std::cerr << "hgl: cannot read '" << path << "'\n";
                return std::nullopt;
            }
            Unit unit{path, std::move(*text)};
            frontend(unit);
            return unit;
        }

        bool needs_native_module(const Unit &unit) {
            return std::any_of(unit.resolved.functions.begin(), unit.resolved.functions.end(), [&](syntax::ast::DeclId id) {
                const auto &fn = std::get<syntax::ast::FunctionDecl>(unit.module.decl(id).node);
                return unit.resolved.kind(id) == semantics::FunctionKind::Runtime ||
                       fn.visibility == syntax::ast::FunctionVisibility::Impl;
            });
        }

        std::optional<codegen::EmittedModule> emit_native_module(Unit &unit, std::string_view language_version) {
            wiring::ensure_session();
            codegen::EmitOptions options;
            options.header_name  = "module.h";
            options.tool_version = std::string{language_version};
            std::optional<codegen::EmittedModule> emitted =
                codegen::emit_cpp(unit.file, unit.module, unit.resolved, options, unit.diagnostics);
            if (emitted) {
                std::string error;
                if (!format_cpp(*emitted, error)) {
                    unit.diagnostics.report(syntax::Category::Backend, syntax::SourceRange{0, 0}, std::move(error));
                    return std::nullopt;
                }
            }
            return emitted;
        }

        /// The scripted path for runtime functions: lower the whole resolved
        /// unit, compile a transient native image, load it into this process,
        /// and register its overloads before the direct harness wires tests or
        /// an entry point. Composition-only units retain the fast direct path.
        bool load_native_module(Unit &unit, std::string_view language_version, NativeModule &native_module) {
            if (!needs_native_module(unit)) { return true; }
            const std::optional<codegen::EmittedModule> emitted = emit_native_module(unit, language_version);
            if (!emitted) { return false; }
            std::string                 error;
            std::optional<NativeModule> loaded = compile_and_load_native_module(*emitted, "module", error);
            if (!loaded) {
                unit.diagnostics.report(syntax::Category::Backend, syntax::SourceRange{0, 0}, std::move(error));
                return false;
            }
            native_module = std::move(*loaded);
            return true;
        }

        int check(std::span<const std::string_view> arguments) {
            std::optional<std::string> path;
            bool                       want_tokens    = false;
            bool                       want_ast       = false;
            bool                       want_hir       = false;
            bool                       want_hgraph_ir = false;
            for (const std::string_view argument : arguments) {
                if (argument == "--dump-tokens") {
                    want_tokens = true;
                } else if (argument == "--dump-ast") {
                    want_ast = true;
                } else if (argument == "--dump-hir") {
                    want_hir = true;
                } else if (argument == "--dump-hgraph-ir") {
                    want_hgraph_ir = true;
                } else if (argument.starts_with("--")) {
                    return usage_error("unknown option '" + std::string{argument} + "'");
                } else if (path) {
                    return usage_error("check takes one file");
                } else {
                    path = std::string{argument};
                }
            }
            if (!path) { return usage_error("check needs a file"); }

            std::optional<std::string> text = read_file(*path);
            if (!text) {
                std::cerr << "hgl: cannot read '" << *path << "'\n";
                return exit_usage;
            }

            Unit unit{*path, std::move(*text)};
            if (want_tokens) {
                syntax::DiagnosticSink lex_diagnostics;
                dump_tokens(unit.file, syntax::lex(unit.file, lex_diagnostics));
                // The parser lexes again so token diagnostics are reported once.
            }
            frontend(unit);
            if (want_ast) { std::cout << syntax::print_ast(unit.module); }
            if (want_hir && !unit.hir.path.empty()) { std::cout << ir::print_hir(unit.hir); }
            if (want_hgraph_ir && unit.hgraph) { std::cout << hgraph_ir::print(*unit.hgraph); }
            if (unit.diagnostics.has_errors()) {
                std::cerr << unit.diagnostics.render(unit.file);
                return exit_diagnostics;
            }
            return exit_ok;
        }

        void print_test_results(const std::vector<wiring::TestResult> &results, std::ostream &out) {
            std::size_t failed = 0;
            for (const wiring::TestResult &result : results) {
                out << result.name << " ... " << (result.passed ? "ok" : "FAILED") << '\n';
                if (!result.passed) { ++failed; }
                if (!result.message.empty()) {
                    std::istringstream lines{result.message};
                    for (std::string line; std::getline(lines, line);) { out << "    " << line << '\n'; }
                }
            }
            out << results.size() << (results.size() == 1 ? " test" : " tests") << ", " << failed << " failed\n";
        }

        int test(std::span<const std::string_view> arguments, std::string_view language_version) {
            std::optional<std::string> path;
            wiring::TestOptions        options;
            for (const std::string_view argument : arguments) {
                if (argument.starts_with("--")) { return usage_error("unknown option '" + std::string{argument} + "'"); }
                if (!path) {
                    path = std::string{argument};
                } else {
                    options.names.emplace_back(argument);
                }
            }
            if (!path) { return usage_error("test needs a file"); }
            std::optional<Unit> unit = load(*path);
            if (!unit) { return exit_usage; }
            if (!unit->ok) {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            for (const std::string &name : options.names) {
                const bool known =
                    std::any_of(unit->resolved.tests.begin(), unit->resolved.tests.end(), [&](syntax::ast::DeclId id) {
                        return std::get<syntax::ast::TestDecl>(unit->module.decl(id).node).name.text == name;
                    });
                if (!known) { return usage_error("no test named '" + name + "'"); }
            }
            NativeModule native_module;
            if (!load_native_module(*unit, language_version, native_module)) {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            const std::vector<wiring::TestResult> results =
                wiring::run_tests(unit->file, unit->module, unit->resolved, options, unit->diagnostics);
            print_test_results(results, std::cout);
            if (unit->diagnostics.has_errors()) { std::cerr << unit->diagnostics.render(unit->file); }
            const bool failed = unit->diagnostics.has_errors() ||
                                std::any_of(results.begin(), results.end(), [](const wiring::TestResult &r) { return !r.passed; });
            return failed ? exit_diagnostics : exit_ok;
        }

        /// A `--start`/`--end` value: the HGL temporal literal, with or
        /// without its leading `@`.
        std::optional<syntax::TemporalValue> temporal_option(std::string_view text) {
            syntax::TemporalParseResult parsed = syntax::parse_temporal_literal(text);
            if (!parsed.value && !text.starts_with('@')) { parsed = syntax::parse_temporal_literal("@" + std::string{text}); }
            return parsed.value;
        }

        int run_command(std::span<const std::string_view> arguments, std::string_view language_version) {
            std::optional<std::string> path;
            wiring::RunOptions         options;
            for (std::size_t i = 0; i < arguments.size(); ++i) {
                const std::string_view argument = arguments[i];
                const auto             value    = [&]() -> std::optional<std::string_view> {
                    if (i + 1 >= arguments.size()) { return std::nullopt; }
                    return arguments[++i];
                };
                if (argument == "--entry") {
                    const auto entry = value();
                    if (!entry) { return usage_error("--entry needs a name"); }
                    options.entry = std::string{*entry};
                } else if (argument == "--mode") {
                    const auto mode = value();
                    if (mode == "sim") {
                        options.mode = wiring::RunMode::Simulation;
                    } else if (mode == "realtime") {
                        options.mode = wiring::RunMode::RealTime;
                    } else {
                        return usage_error("--mode is sim or realtime");
                    }
                } else if (argument == "--start") {
                    const auto text = value();
                    const auto when = text ? temporal_option(*text) : std::nullopt;
                    if (!when || when->kind != syntax::TemporalKind::DateTime) {
                        return usage_error("--start takes a datetime such as 2026-09-03T08:00:00Z");
                    }
                    options.start = hgraph::DateTime{std::chrono::microseconds{when->micros}};
                } else if (argument == "--end") {
                    const auto text = value();
                    const auto when = text ? temporal_option(*text) : std::nullopt;
                    if (when && when->kind == syntax::TemporalKind::DateTime) {
                        options.end = hgraph::DateTime{std::chrono::microseconds{when->micros}};
                    } else if (when && when->kind == syntax::TemporalKind::Duration) {
                        options.end_after = hgraph::TimeDelta{when->micros};
                    } else {
                        return usage_error("--end takes a datetime or a duration such as 1d");
                    }
                } else if (argument == "--set") {
                    const auto text = value();
                    const auto eq   = text ? text->find('=') : std::string_view::npos;
                    if (!text || eq == std::string_view::npos || eq == 0) {
                        return usage_error("--set takes name=<constant expression>");
                    }
                    options.settings.push_back(
                        wiring::Setting{std::string{text->substr(0, eq)}, std::string{text->substr(eq + 1)}});
                } else if (argument.starts_with("--")) {
                    return usage_error("unknown option '" + std::string{argument} + "'");
                } else if (path) {
                    return usage_error("run takes one file");
                } else {
                    path = std::string{argument};
                }
            }
            if (!path) { return usage_error("run needs a file"); }
            std::optional<Unit> unit = load(*path);
            if (!unit) { return exit_usage; }
            NativeModule native_module;
            if (unit->ok && load_native_module(*unit, language_version, native_module)) {
                (void)wiring::run_program(unit->file, unit->module, unit->resolved, options, unit->diagnostics, std::cout);
            }
            if (unit->diagnostics.has_errors()) {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            return exit_ok;
        }

        // ------------------------------------------------------------ emit-cpp
        // (developer guide, "C++ backend, first pass")

        bool write_file(const std::filesystem::path &path, const std::string &text) {
            std::error_code error;
            if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path(), error); }
            std::ofstream out{path, std::ios::binary | std::ios::trunc};
            if (!out) {
                std::cerr << "hgl: cannot write '" << path.string() << "'\n";
                return false;
            }
            out << text;
            return static_cast<bool>(out);
        }

        int emit_cpp(std::span<const std::string_view> arguments, std::string_view tool_version) {
            std::optional<std::string> path;
            std::optional<std::string> out_dir;
            std::optional<std::string> include_dir;
            std::optional<std::string> src_dir;
            std::optional<std::string> python_path;
            std::string                python_native;
            bool                       print = false;
            for (std::size_t i = 0; i < arguments.size(); ++i) {
                const std::string_view argument = arguments[i];
                const auto             value    = [&]() -> std::optional<std::string_view> {
                    if (i + 1 >= arguments.size()) { return std::nullopt; }
                    return arguments[++i];
                };
                if (argument == "--out-dir") {
                    const auto dir = value();
                    if (!dir) { return usage_error("--out-dir needs a directory"); }
                    out_dir = std::string{*dir};
                } else if (argument == "--include-dir") {
                    const auto dir = value();
                    if (!dir) { return usage_error("--include-dir needs a directory"); }
                    include_dir = std::string{*dir};
                } else if (argument == "--src-dir") {
                    const auto dir = value();
                    if (!dir) { return usage_error("--src-dir needs a directory"); }
                    src_dir = std::string{*dir};
                } else if (argument == "--python") {
                    const auto file = value();
                    if (!file) { return usage_error("--python needs a file"); }
                    python_path = std::string{*file};
                } else if (argument == "--python-native") {
                    const auto name = value();
                    if (!name) { return usage_error("--python-native needs a module name"); }
                    python_native = std::string{*name};
                } else if (argument == "--print") {
                    print = true;
                } else if (argument.starts_with("--")) {
                    return usage_error("unknown option '" + std::string{argument} + "'");
                } else if (path) {
                    return usage_error("emit-cpp takes one file");
                } else {
                    path = std::string{argument};
                }
            }
            if (!path) { return usage_error("emit-cpp needs a file"); }
            if (out_dir && (include_dir || src_dir)) { return usage_error("use --out-dir or --include-dir/--src-dir, not both"); }
            if (python_path && python_native.empty()) { return usage_error("--python needs --python-native <module>"); }
            if (!python_path && !python_native.empty()) { return usage_error("--python-native needs --python <file>"); }

            std::optional<Unit> unit = load(*path);
            if (!unit) { return exit_usage; }
            if (!unit->ok) {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }

            // The pair is named after the source: prices.hgl -> prices.h, prices.cpp.
            const std::filesystem::path source{*path};
            const std::string           stem        = source.stem().string();
            std::filesystem::path       header_path = source.parent_path() / (stem + ".h");
            std::filesystem::path       source_path = source.parent_path() / (stem + ".cpp");
            if (out_dir) {
                header_path = std::filesystem::path{*out_dir} / (stem + ".h");
                source_path = std::filesystem::path{*out_dir} / (stem + ".cpp");
            }
            if (include_dir) { header_path = std::filesystem::path{*include_dir} / (stem + ".h"); }
            if (src_dir) { source_path = std::filesystem::path{*src_dir} / (stem + ".cpp"); }

            codegen::EmitOptions options;
            options.header_name          = stem + ".h";
            options.tool_version         = std::string{tool_version};
            options.python_native_module = python_native;
            std::optional<codegen::EmittedModule> emitted =
                codegen::emit_cpp(unit->file, unit->module, unit->resolved, options, unit->diagnostics);
            if (!emitted) {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            std::string format_error;
            if (!format_cpp(*emitted, format_error)) {
                unit->diagnostics.report(syntax::Category::Backend, syntax::SourceRange{0, 0}, std::move(format_error));
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            if (print) {
                std::cout << "// ==== " << header_path.filename().string() << '\n' << emitted->header;
                std::cout << "// ==== " << source_path.filename().string() << '\n' << emitted->source;
                if (python_path) {
                    std::cout << "# ==== " << std::filesystem::path{*python_path}.filename().string() << '\n' << emitted->python;
                }
                return exit_ok;
            }
            if (!write_file(header_path, emitted->header) || !write_file(source_path, emitted->source)) { return exit_usage; }
            if (python_path && !write_file(std::filesystem::path{*python_path}, emitted->python)) { return exit_usage; }
            return exit_ok;
        }

        // ---------------------------------------------------------- the REPL
        // (developer guide, "Scripted, REPL, and AOT drivers", first pass)

        bool is_declaration(std::string_view input) {
            const auto       first = input.find_first_not_of(" \t");
            const auto       last  = input.find_first_of(" \t(<{", first);
            std::string_view word  = input.substr(first == std::string_view::npos ? 0 : first,
                                                  last == std::string_view::npos ? std::string_view::npos : last - first);
            return word == "fn" || word == "export" || word == "impl" || word == "operator" || word == "use" || word == "test";
        }

        bool is_binding(std::string_view input) {
            const auto first = input.find_first_not_of(" \t");
            return input.substr(first == std::string_view::npos ? 0 : first).starts_with("let ") ||
                   input.substr(first == std::string_view::npos ? 0 : first).starts_with("var ");
        }

        /// Open brackets minus closed ones, outside string literals.
        int bracket_depth(std::string_view text) {
            int  depth     = 0;
            bool in_string = false;
            for (std::size_t i = 0; i < text.size(); ++i) {
                const char c = text[i];
                if (in_string) {
                    if (c == '\\') {
                        ++i;
                    } else if (c == '"') {
                        in_string = false;
                    }
                    continue;
                }
                if (c == '"') {
                    in_string = true;
                } else if (c == '/' && i + 1 < text.size() && text[i + 1] == '/') {
                    break;
                } else if (c == '(' || c == '[' || c == '{') {
                    ++depth;
                } else if (c == ')' || c == ']' || c == '}') {
                    --depth;
                }
            }
            return depth;
        }

        std::string test_name_of(std::string_view input) {
            const auto at = input.find("test");
            if (at == std::string_view::npos) { return {}; }
            std::string_view rest  = input.substr(at + 4);
            const auto       begin = rest.find_first_not_of(" \t");
            if (begin == std::string_view::npos) { return {}; }
            const auto end = rest.find_first_of(" \t{", begin);
            return std::string{rest.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin)};
        }

        class Repl
        {
          public:
            explicit Repl(std::string_view language_version) : language_version_(language_version) {}

            int loop() {
                std::cout << "hgl repl " << hgraph::release_version_string << " (hgraph api " << hgraph::version_string
                          << "); :help for commands";
                if (reader_.interactive()) { std::cout << " (history in $HGL_HISTORY or ~/.hgl_history; tab completes)"; }
                std::cout << '\n';
                reader_.set_completions([this] { return completions(); });
                std::string pending;
                while (true) {
                    const std::optional<std::string> read = reader_.read(pending.empty() ? "hgl> " : "...> ");
                    if (!read) {
                        std::cout << '\n';
                        return exit_ok;
                    }
                    const std::string &line = *read;
                    pending += line;
                    pending += '\n';
                    if (bracket_depth(pending) > 0) { continue; }
                    std::string input = std::move(pending);
                    pending.clear();
                    const auto first = input.find_first_not_of(" \t\n");
                    if (first == std::string::npos) { continue; }
                    input.erase(0, first);
                    if (input.front() == ':') {
                        if (!command(input)) { return exit_ok; }
                        continue;
                    }
                    if (is_declaration(input)) {
                        declare(input);
                    } else {
                        evaluate(input);
                    }
                }
            }

          private:
            /// Completion words: the REPL commands, the language's declaration
            /// keywords, the kernel modules, and every name the session has
            /// declared or bound.
            std::vector<std::string> completions() const {
                std::vector<std::string> words{":help",    ":list", ":quit",  "fn",      "export",     "impl",
                                               "operator", "use",   "test",   "let",     "var",        "assert",
                                               "eval",     "const", "atomic", "rolling", "hgraph.std", "hgraph.analytics"};
                const auto               declared = [&](const std::string &text) {
                    // The declared name follows the first keyword(s): `fn name`,
                    // `export fn name`, `impl fn name`, `operator name`,
                    // `test name`, `let name`, `var name`, `use a.b::{x, y}`.
                    std::istringstream words_in{text};
                    std::string        word;
                    std::string        previous;
                    while (words_in >> word) {
                        if (previous == "fn" || previous == "operator" || previous == "test" || previous == "let" ||
                            previous == "var") {
                            const auto end = word.find_first_of("(<:{=");
                            words.push_back(word.substr(0, end));
                            return;
                        }
                        previous = word;
                    }
                };
                for (const std::string &declaration : declarations_) { declared(declaration); }
                for (const std::string &binding : bindings_) { declared(binding); }
                std::sort(words.begin(), words.end());
                words.erase(std::unique(words.begin(), words.end()), words.end());
                return words;
            }

            std::string session_text() const {
                std::string text = "module repl\n";
                for (const std::string &declaration : declarations_) { text += declaration; }
                return text;
            }

            std::string bindings_text() const {
                std::string text;
                for (const std::string &binding : bindings_) { text += "    " + binding; }
                return text;
            }

            bool command(std::string_view input) {
                const std::string_view word = input.substr(0, input.find_first_of(" \t\n"));
                if (word == ":quit" || word == ":q") { return false; }
                if (word == ":list") {
                    std::cout << session_text();
                    if (!bindings_.empty()) { std::cout << "// bindings\n" << bindings_text(); }
                    return true;
                }
                if (word == ":help") {
                    std::cout << "Declarations (fn, export fn, operator, use, test) join the session when\n"
                                 "the whole session still checks; a test runs as it is accepted.\n"
                                 "Any other input is evaluated as a statement or expression: let and var\n"
                                 "bindings persist, and a final expression prints its value.\n"
                                 "  :list   show the session\n"
                                 "  :quit   leave\n";
                    return true;
                }
                std::cout << "unknown command " << word << "; :help lists the commands\n";
                return true;
            }

            void declare(const std::string &input) {
                Unit unit{"<repl>", session_text() + input};
                frontend(unit);
                if (!unit.ok) {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                if (!replace_native_module(unit)) {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                declarations_.push_back(input);
                if (input.starts_with("test")) {
                    wiring::TestOptions options;
                    options.names.push_back(test_name_of(input));
                    print_test_results(wiring::run_tests(unit.file, unit.module, unit.resolved, options, unit.diagnostics),
                                       std::cout);
                    if (unit.diagnostics.has_errors()) { std::cout << unit.diagnostics.render(unit.file); }
                }
            }

            void evaluate(const std::string &input) {
                Unit unit{"<repl>", session_text() + "test __repl {\n" + bindings_text() + "    " + input + "}\n"};
                frontend(unit);
                if (!unit.ok) {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                if (needs_native_module(unit) && (!native_module_ || !native_module_->active()) && !replace_native_module(unit)) {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                wiring::TestOptions options;
                options.names.push_back("__repl");
                options.describe_tail = true;
                const std::vector<wiring::TestResult> results =
                    wiring::run_tests(unit.file, unit.module, unit.resolved, options, unit.diagnostics);
                if (unit.diagnostics.has_errors()) {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                for (const wiring::TestResult &result : results) {
                    if (!result.passed) {
                        std::cout << result.message << '\n';
                    } else if (!result.tail.empty()) {
                        std::cout << result.tail << '\n';
                    }
                }
                if (!results.empty() && results.front().passed && is_binding(input)) { bindings_.push_back(input); }
            }

            bool replace_native_module(Unit &unit) {
                if (!needs_native_module(unit)) { return true; }
                const std::optional<codegen::EmittedModule> emitted = emit_native_module(unit, language_version_);
                if (!emitted) { return false; }
                std::string error;
                if (!compile_and_replace_native_module(*emitted, "module", native_module_, error)) {
                    unit.diagnostics.report(syntax::Category::Backend, syntax::SourceRange{0, 0}, std::move(error));
                    return false;
                }
                return true;
            }

            std::vector<std::string>    declarations_;
            std::vector<std::string>    bindings_;
            std::string                 language_version_;
            std::optional<NativeModule> native_module_{};
            LineReader                  reader_{};
        };

        int repl(std::span<const std::string_view> arguments, std::string_view language_version) {
            if (!arguments.empty()) { return usage_error("repl takes no arguments"); }
            wiring::ensure_session();
            Repl session{language_version};
            return session.loop();
        }
    }  // namespace

    int run(std::span<const std::string_view> arguments, std::string_view tool_version) {
        if (arguments.empty()) {
            print_help();
            return exit_ok;
        }
        const std::string_view command = arguments[0];
        const auto             rest    = arguments.subspan(1);
        if (command == "--help" || command == "-h") {
            if (!rest.empty()) { return usage_error("--help takes no arguments"); }
            print_help();
            return exit_ok;
        }
        if (command == "--version") {
            if (!rest.empty()) { return usage_error("--version takes no arguments"); }
            print_version(tool_version);
            return exit_ok;
        }
        if (command == "check") { return check(rest); }
        if (command == "test") { return test(rest, tool_version); }
        if (command == "run") { return run_command(rest, tool_version); }
        if (command == "repl") { return repl(rest, tool_version); }
        if (command == "emit-cpp") { return emit_cpp(rest, tool_version); }
        if (command == "build") {
            return usage_error("'build' is not a command; build a package from emit-cpp output with the "
                               "hgl_add_module() CMake function (user guide, \"Building a package\")");
        }
        return usage_error("unknown command '" + std::string{command} + "'");
    }
}  // namespace hgl::driver
