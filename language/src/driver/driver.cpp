#include "driver/driver.h"

#include "syntax/ast_printer.h"
#include "syntax/diagnostic.h"
#include "syntax/lexer.h"
#include "syntax/parser.h"
#include "syntax/source.h"

#include <hgraph/version.h>

#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <sstream>
#include <string>

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
                         "  hgl --help\n"
                         "  hgl --version\n\n"
                         "Commands:\n"
                         "  check    parse a module and report diagnostics\n\n"
                         "Planned (developer guide, \"Scripted, REPL, and AOT drivers\"):\n"
                         "  test, run, emit-cpp, build, repl\n";
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

            const syntax::SourceFile file{*path, std::move(*text)};
            syntax::DiagnosticSink   diagnostics;
            if (want_tokens) { dump_tokens(file, syntax::lex(file, diagnostics)); }
            // The parser lexes again so token diagnostics are reported once.
            if (want_tokens) { diagnostics = syntax::DiagnosticSink{}; }
            const syntax::ast::Module module = syntax::parse(file, diagnostics);
            if (want_ast) { std::cout << syntax::print_ast(module); }
            if (diagnostics.has_errors())
            {
                std::cerr << diagnostics.render(file);
                return exit_diagnostics;
            }
            return exit_ok;
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
        if (command == "test" || command == "run" || command == "emit-cpp" || command == "build" || command == "repl")
        {
            return usage_error("'" + std::string{command} + "' is not implemented yet");
        }
        return usage_error("unknown command '" + std::string{command} + "'");
    }
}  // namespace hgl::driver
