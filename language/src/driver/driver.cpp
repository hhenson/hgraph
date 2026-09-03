#include "driver/driver.h"

#include "semantics/resolve.h"
#include "syntax/ast_printer.h"
#include "syntax/diagnostic.h"
#include "syntax/lexer.h"
#include "syntax/parser.h"
#include "syntax/source.h"
#include "syntax/temporal.h"
#include "wiring/backend.h"

#include <hgraph/version.h>

#include <algorithm>
#include <chrono>
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

        void print_help()
        {
            std::cout << "hgl - experimental hgraph language toolchain\n\n"
                         "Usage:\n"
                         "  hgl check <file> [--dump-tokens] [--dump-ast]\n"
                         "  hgl test <file> [test-name]...\n"
                         "  hgl run <file> [--entry <name>] [--mode sim|realtime]\n"
                         "          [--start <datetime>] [--end <datetime|duration>]\n"
                         "          [--set <name>=<constant expression>]...\n"
                         "  hgl repl\n"
                         "  hgl --help\n"
                         "  hgl --version\n\n"
                         "Commands:\n"
                         "  check    parse and resolve a module and report diagnostics\n"
                         "  test     run the module's test declarations\n"
                         "  run      bind an entry to a mode, clock and parameters, then execute it\n"
                         "  repl     accumulate declarations and evaluate expressions interactively\n\n"
                         "Planned (developer guide, \"Scripted, REPL, and AOT drivers\"):\n"
                         "  emit-cpp, build\n";
        }

        void print_version(std::string_view language_version)
        {
            std::cout << "hgl " << language_version << " (hgraph " << hgraph::version_string << ")\n";
        }

        int usage_error(std::string_view message)
        {
            std::cerr << "hgl: " << message << "; use --help for the current interface\n";
            return exit_usage;
        }

        std::optional<std::string> read_file(const std::string &path)
        {
            std::ifstream in{path, std::ios::binary};
            if (!in) { return std::nullopt; }
            return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
        }

        void dump_tokens(const syntax::SourceFile &file, const syntax::LexResult &lexed)
        {
            for (const syntax::Token &token : lexed.tokens)
            {
                const syntax::Location at = file.location(token.range.begin);
                std::cout << at.line << ':' << at.column << ' ' << syntax::token_kind_name(token.kind);
                if (token.kind != syntax::TokenKind::Newline && token.kind != syntax::TokenKind::EndOfFile)
                {
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
            syntax::SourceFile         file;
            syntax::DiagnosticSink     diagnostics{};
            syntax::ast::Module        module{};
            semantics::ResolvedModule  resolved{};
            bool                       ok{false};

            Unit(std::string path, std::string text) : file{std::move(path), std::move(text)} {}
        };

        void frontend(Unit &unit)
        {
            unit.module = syntax::parse(unit.file, unit.diagnostics);
            if (unit.diagnostics.has_errors()) { return; }
            unit.resolved = semantics::resolve(unit.file, unit.module, wiring::has_operator, unit.diagnostics);
            unit.ok       = !unit.diagnostics.has_errors();
        }

        std::optional<Unit> load(const std::string &path)
        {
            std::optional<std::string> text = read_file(path);
            if (!text)
            {
                std::cerr << "hgl: cannot read '" << path << "'\n";
                return std::nullopt;
            }
            Unit unit{path, std::move(*text)};
            frontend(unit);
            return unit;
        }

        int check(std::span<const std::string_view> arguments)
        {
            std::optional<std::string> path;
            bool                       want_tokens = false;
            bool                       want_ast    = false;
            for (const std::string_view argument : arguments)
            {
                if (argument == "--dump-tokens") { want_tokens = true; }
                else if (argument == "--dump-ast") { want_ast = true; }
                else if (argument.starts_with("--")) { return usage_error("unknown option '" + std::string{argument} + "'"); }
                else if (path) { return usage_error("check takes one file"); }
                else { path = std::string{argument}; }
            }
            if (!path) { return usage_error("check needs a file"); }

            std::optional<std::string> text = read_file(*path);
            if (!text)
            {
                std::cerr << "hgl: cannot read '" << *path << "'\n";
                return exit_usage;
            }

            Unit unit{*path, std::move(*text)};
            if (want_tokens)
            {
                syntax::DiagnosticSink lex_diagnostics;
                dump_tokens(unit.file, syntax::lex(unit.file, lex_diagnostics));
                // The parser lexes again so token diagnostics are reported once.
            }
            frontend(unit);
            if (want_ast) { std::cout << syntax::print_ast(unit.module); }
            if (unit.diagnostics.has_errors())
            {
                std::cerr << unit.diagnostics.render(unit.file);
                return exit_diagnostics;
            }
            return exit_ok;
        }

        void print_test_results(const std::vector<wiring::TestResult> &results, std::ostream &out)
        {
            std::size_t failed = 0;
            for (const wiring::TestResult &result : results)
            {
                out << result.name << " ... " << (result.passed ? "ok" : "FAILED") << '\n';
                if (!result.passed) { ++failed; }
                if (!result.message.empty())
                {
                    std::istringstream lines{result.message};
                    for (std::string line; std::getline(lines, line);) { out << "    " << line << '\n'; }
                }
            }
            out << results.size() << (results.size() == 1 ? " test" : " tests") << ", " << failed << " failed\n";
        }

        int test(std::span<const std::string_view> arguments)
        {
            std::optional<std::string> path;
            wiring::TestOptions        options;
            for (const std::string_view argument : arguments)
            {
                if (argument.starts_with("--")) { return usage_error("unknown option '" + std::string{argument} + "'"); }
                if (!path) { path = std::string{argument}; }
                else { options.names.emplace_back(argument); }
            }
            if (!path) { return usage_error("test needs a file"); }
            std::optional<Unit> unit = load(*path);
            if (!unit) { return exit_usage; }
            if (!unit->ok)
            {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            for (const std::string &name : options.names)
            {
                const bool known = std::any_of(unit->resolved.tests.begin(), unit->resolved.tests.end(),
                                               [&](syntax::ast::DeclId id) {
                                                   return std::get<syntax::ast::TestDecl>(unit->module.decl(id).node)
                                                              .name.text == name;
                                               });
                if (!known) { return usage_error("no test named '" + name + "'"); }
            }
            const std::vector<wiring::TestResult> results =
                wiring::run_tests(unit->file, unit->module, unit->resolved, options, unit->diagnostics);
            print_test_results(results, std::cout);
            if (unit->diagnostics.has_errors()) { std::cerr << unit->diagnostics.render(unit->file); }
            const bool failed = unit->diagnostics.has_errors() ||
                                std::any_of(results.begin(), results.end(),
                                            [](const wiring::TestResult &r) { return !r.passed; });
            return failed ? exit_diagnostics : exit_ok;
        }

        /// A `--start`/`--end` value: the HGL temporal literal, with or
        /// without its leading `@`.
        std::optional<syntax::TemporalValue> temporal_option(std::string_view text)
        {
            syntax::TemporalParseResult parsed = syntax::parse_temporal_literal(text);
            if (!parsed.value && !text.starts_with('@'))
            {
                parsed = syntax::parse_temporal_literal("@" + std::string{text});
            }
            return parsed.value;
        }

        int run_command(std::span<const std::string_view> arguments)
        {
            std::optional<std::string> path;
            wiring::RunOptions         options;
            for (std::size_t i = 0; i < arguments.size(); ++i)
            {
                const std::string_view argument = arguments[i];
                const auto             value    = [&]() -> std::optional<std::string_view> {
                    if (i + 1 >= arguments.size()) { return std::nullopt; }
                    return arguments[++i];
                };
                if (argument == "--entry")
                {
                    const auto entry = value();
                    if (!entry) { return usage_error("--entry needs a name"); }
                    options.entry = std::string{*entry};
                }
                else if (argument == "--mode")
                {
                    const auto mode = value();
                    if (mode == "sim") { options.mode = wiring::RunMode::Simulation; }
                    else if (mode == "realtime") { options.mode = wiring::RunMode::RealTime; }
                    else { return usage_error("--mode is sim or realtime"); }
                }
                else if (argument == "--start")
                {
                    const auto text = value();
                    const auto when = text ? temporal_option(*text) : std::nullopt;
                    if (!when || when->kind != syntax::TemporalKind::DateTime)
                    {
                        return usage_error("--start takes a datetime such as 2026-09-03T08:00:00Z");
                    }
                    options.start = hgraph::DateTime{std::chrono::microseconds{when->micros}};
                }
                else if (argument == "--end")
                {
                    const auto text = value();
                    const auto when = text ? temporal_option(*text) : std::nullopt;
                    if (when && when->kind == syntax::TemporalKind::DateTime)
                    {
                        options.end = hgraph::DateTime{std::chrono::microseconds{when->micros}};
                    }
                    else if (when && when->kind == syntax::TemporalKind::Duration)
                    {
                        options.end_after = hgraph::TimeDelta{when->micros};
                    }
                    else { return usage_error("--end takes a datetime or a duration such as 1d"); }
                }
                else if (argument == "--set")
                {
                    const auto text = value();
                    const auto eq   = text ? text->find('=') : std::string_view::npos;
                    if (!text || eq == std::string_view::npos || eq == 0)
                    {
                        return usage_error("--set takes name=<constant expression>");
                    }
                    options.settings.push_back(
                        wiring::Setting{std::string{text->substr(0, eq)}, std::string{text->substr(eq + 1)}});
                }
                else if (argument.starts_with("--")) { return usage_error("unknown option '" + std::string{argument} + "'"); }
                else if (path) { return usage_error("run takes one file"); }
                else { path = std::string{argument}; }
            }
            if (!path) { return usage_error("run needs a file"); }
            std::optional<Unit> unit = load(*path);
            if (!unit) { return exit_usage; }
            if (unit->ok)
            {
                (void)wiring::run_program(unit->file, unit->module, unit->resolved, options, unit->diagnostics,
                                          std::cout);
            }
            if (unit->diagnostics.has_errors())
            {
                std::cerr << unit->diagnostics.render(unit->file);
                return exit_diagnostics;
            }
            return exit_ok;
        }

        // ---------------------------------------------------------- the REPL
        // (developer guide, "Scripted, REPL, and AOT drivers", first pass)

        bool is_declaration(std::string_view input)
        {
            const auto      first = input.find_first_not_of(" \t");
            const auto      last  = input.find_first_of(" \t(<{", first);
            std::string_view word = input.substr(first == std::string_view::npos ? 0 : first,
                                                 last == std::string_view::npos ? std::string_view::npos : last - first);
            return word == "fn" || word == "export" || word == "impl" || word == "operator" || word == "use" ||
                   word == "test";
        }

        bool is_binding(std::string_view input)
        {
            const auto first = input.find_first_not_of(" \t");
            return input.substr(first == std::string_view::npos ? 0 : first).starts_with("let ") ||
                   input.substr(first == std::string_view::npos ? 0 : first).starts_with("var ");
        }

        /// Open brackets minus closed ones, outside string literals.
        int bracket_depth(std::string_view text)
        {
            int  depth     = 0;
            bool in_string = false;
            for (std::size_t i = 0; i < text.size(); ++i)
            {
                const char c = text[i];
                if (in_string)
                {
                    if (c == '\\') { ++i; }
                    else if (c == '"') { in_string = false; }
                    continue;
                }
                if (c == '"') { in_string = true; }
                else if (c == '/' && i + 1 < text.size() && text[i + 1] == '/') { break; }
                else if (c == '(' || c == '[' || c == '{') { ++depth; }
                else if (c == ')' || c == ']' || c == '}') { --depth; }
            }
            return depth;
        }

        std::string test_name_of(std::string_view input)
        {
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
            int loop()
            {
                std::cout << "hgl repl (hgraph " << hgraph::version_string << "); :help for commands\n";
                std::string pending;
                while (true)
                {
                    std::cout << (pending.empty() ? "hgl> " : "...> ") << std::flush;
                    std::string line;
                    if (!std::getline(std::cin, line))
                    {
                        std::cout << '\n';
                        return exit_ok;
                    }
                    pending += line;
                    pending += '\n';
                    if (bracket_depth(pending) > 0) { continue; }
                    std::string input = std::move(pending);
                    pending.clear();
                    const auto first = input.find_first_not_of(" \t\n");
                    if (first == std::string::npos) { continue; }
                    input.erase(0, first);
                    if (input.front() == ':')
                    {
                        if (!command(input)) { return exit_ok; }
                        continue;
                    }
                    if (is_declaration(input)) { declare(input); }
                    else { evaluate(input); }
                }
            }

          private:
            std::string session_text() const
            {
                std::string text = "module repl\n";
                for (const std::string &declaration : declarations_) { text += declaration; }
                return text;
            }

            std::string bindings_text() const
            {
                std::string text;
                for (const std::string &binding : bindings_) { text += "    " + binding; }
                return text;
            }

            bool command(std::string_view input)
            {
                const std::string_view word = input.substr(0, input.find_first_of(" \t\n"));
                if (word == ":quit" || word == ":q") { return false; }
                if (word == ":list")
                {
                    std::cout << session_text();
                    if (!bindings_.empty())
                    {
                        std::cout << "// bindings\n" << bindings_text();
                    }
                    return true;
                }
                if (word == ":help")
                {
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

            void declare(const std::string &input)
            {
                Unit unit{"<repl>", session_text() + input};
                frontend(unit);
                if (!unit.ok)
                {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                declarations_.push_back(input);
                if (input.starts_with("test"))
                {
                    wiring::TestOptions options;
                    options.names.push_back(test_name_of(input));
                    print_test_results(
                        wiring::run_tests(unit.file, unit.module, unit.resolved, options, unit.diagnostics),
                        std::cout);
                    if (unit.diagnostics.has_errors()) { std::cout << unit.diagnostics.render(unit.file); }
                }
            }

            void evaluate(const std::string &input)
            {
                Unit unit{"<repl>", session_text() + "test __repl {\n" + bindings_text() + "    " + input + "}\n"};
                frontend(unit);
                if (!unit.ok)
                {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                wiring::TestOptions options;
                options.names.push_back("__repl");
                options.describe_tail = true;
                const std::vector<wiring::TestResult> results =
                    wiring::run_tests(unit.file, unit.module, unit.resolved, options, unit.diagnostics);
                if (unit.diagnostics.has_errors())
                {
                    std::cout << unit.diagnostics.render(unit.file);
                    return;
                }
                for (const wiring::TestResult &result : results)
                {
                    if (!result.passed) { std::cout << result.message << '\n'; }
                    else if (!result.tail.empty()) { std::cout << result.tail << '\n'; }
                }
                if (!results.empty() && results.front().passed && is_binding(input)) { bindings_.push_back(input); }
            }

            std::vector<std::string> declarations_;
            std::vector<std::string> bindings_;
        };

        int repl(std::span<const std::string_view> arguments)
        {
            if (!arguments.empty()) { return usage_error("repl takes no arguments"); }
            wiring::ensure_session();
            Repl session;
            return session.loop();
        }
    }  // namespace

    int run(std::span<const std::string_view> arguments, std::string_view language_version)
    {
        if (arguments.empty())
        {
            print_help();
            return exit_ok;
        }
        const std::string_view command = arguments[0];
        const auto             rest    = arguments.subspan(1);
        if (command == "--help" || command == "-h")
        {
            if (!rest.empty()) { return usage_error("--help takes no arguments"); }
            print_help();
            return exit_ok;
        }
        if (command == "--version")
        {
            if (!rest.empty()) { return usage_error("--version takes no arguments"); }
            print_version(language_version);
            return exit_ok;
        }
        if (command == "check") { return check(rest); }
        if (command == "test") { return test(rest); }
        if (command == "run") { return run_command(rest); }
        if (command == "repl") { return repl(rest); }
        if (command == "emit-cpp" || command == "build")
        {
            return usage_error("'" + std::string{command} + "' is not implemented yet");
        }
        return usage_error("unknown command '" + std::string{command} + "'");
    }
}  // namespace hgl::driver
