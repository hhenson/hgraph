#include "syntax/lexer.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

using namespace hgl::syntax;

namespace
{
    struct Lexed
    {
        SourceFile     file;
        DiagnosticSink diagnostics;
        LexResult      result;

        explicit Lexed(std::string text) : file{"test.hgl", std::move(text)}, result{lex(file, diagnostics)} {}
    };

    std::vector<TokenKind> kinds(const Lexed &lexed)
    {
        std::vector<TokenKind> out;
        for (const Token &token : lexed.result.tokens) { out.push_back(token.kind); }
        return out;
    }

    std::vector<std::string> texts(const Lexed &lexed)
    {
        std::vector<std::string> out;
        for (const Token &token : lexed.result.tokens) { out.emplace_back(token.text); }
        return out;
    }
}  // namespace

TEST_CASE("empty input yields end of file", "[lexer]")
{
    Lexed lexed{""};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::EndOfFile});
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
}

TEST_CASE("keywords, identifiers and the placeholder", "[lexer]")
{
    Lexed lexed{"fn export abstract struct requires is null _ _x in tuple"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::KwFn, TokenKind::KwExport, TokenKind::KwAbstract, TokenKind::KwStruct,
                                                   TokenKind::KwRequires, TokenKind::KwIs, TokenKind::KwNull,
                                                   TokenKind::Placeholder, TokenKind::Identifier, TokenKind::Identifier,
                                                   TokenKind::Identifier, TokenKind::EndOfFile});
    REQUIRE(lexed.result.tokens[8].text == "_x");
    REQUIRE(lexed.result.tokens[9].text == "in");
}

TEST_CASE("type keywords are reserved", "[lexer]")
{
    Lexed lexed{"bool i64 f64 str date time datetime duration civil_datetime zoned_datetime zoned_time timezone"};
    const auto ks = kinds(lexed);
    REQUIRE(ks.size() == 13);
    for (std::size_t i = 0; i + 1 < ks.size(); ++i) { REQUIRE(is_scalar_type_keyword(ks[i])); }
}

TEST_CASE("one newline token per run of terminators including comments", "[lexer]")
{
    Lexed lexed{"a // first\n\n  // second\n\nb\n"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::Identifier, TokenKind::Newline, TokenKind::Identifier,
                                                   TokenKind::Newline, TokenKind::EndOfFile});
    REQUIRE(lexed.result.comments.size() == 2);
    REQUIRE(lexed.file.slice(lexed.result.comments[0].range) == "// first");
    REQUIRE(lexed.file.slice(lexed.result.comments[1].range) == "// second");
}

TEST_CASE("source fragments retain every byte without coalescing trivia", "[lexer][source-accurate]")
{
    Lexed lexed{"a \t// first\n\n  // second\nb"};
    REQUIRE_FALSE(lexed.diagnostics.has_errors());

    std::string   reconstructed;
    std::uint32_t next = 0;
    for (const SourceFragment &fragment : lexed.result.fragments)
    {
        REQUIRE(fragment.range.begin == next);
        REQUIRE_FALSE(fragment.range.empty());
        reconstructed += lexed.file.slice(fragment.range);
        next = fragment.range.end;
    }
    REQUIRE(next == lexed.file.text().size());
    REQUIRE(reconstructed == lexed.file.text());

    const std::vector<SourceFragmentKind> fragment_kinds{
        SourceFragmentKind::Token,       SourceFragmentKind::Whitespace,
        SourceFragmentKind::LineComment, SourceFragmentKind::LineBreak,
        SourceFragmentKind::LineBreak,   SourceFragmentKind::Whitespace,
        SourceFragmentKind::LineComment, SourceFragmentKind::LineBreak,
        SourceFragmentKind::Token,
    };
    std::vector<SourceFragmentKind> actual_kinds;
    for (const SourceFragment &fragment : lexed.result.fragments)
    {
        actual_kinds.push_back(fragment.kind);
    }
    REQUIRE(actual_kinds == fragment_kinds);
    REQUIRE(lexed.result.fragments[3].token_index == 1);
    REQUIRE(lexed.result.fragments[4].token_index == 1);
    REQUIRE(lexed.result.fragments[7].token_index == 1);
    REQUIRE(lexed.result.tokens[1].text == "\n\n  // second\n");
}

TEST_CASE("integer and float literals", "[lexer]")
{
    Lexed lexed{"42 3.5 1e5 2.5E-3 7."};
    const auto ks = kinds(lexed);
    REQUIRE(ks == std::vector<TokenKind>{TokenKind::IntLiteral, TokenKind::FloatLiteral, TokenKind::FloatLiteral,
                                         TokenKind::FloatLiteral, TokenKind::IntLiteral, TokenKind::Dot,
                                         TokenKind::EndOfFile});
    REQUIRE(lexed.result.tokens[0].int_value == 42);
    REQUIRE(lexed.result.tokens[1].float_value == 3.5);
    REQUIRE(lexed.result.tokens[2].float_value == 1e5);
    REQUIRE(lexed.result.tokens[3].float_value == 2.5e-3);
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
}

TEST_CASE("integer literal out of range is diagnosed", "[lexer]")
{
    Lexed lexed{"99999999999999999999"};
    REQUIRE(lexed.result.tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(lexed.diagnostics.size() == 1);
    REQUIRE(lexed.diagnostics.diagnostics()[0].message.find("out of range") != std::string::npos);
}

TEST_CASE("duration literals", "[lexer]")
{
    Lexed lexed{"5m 1h30m 2m30.5s 250ms"};
    const auto ks = kinds(lexed);
    REQUIRE(ks == std::vector<TokenKind>{TokenKind::TemporalLiteral, TokenKind::TemporalLiteral,
                                         TokenKind::TemporalLiteral, TokenKind::TemporalLiteral, TokenKind::EndOfFile});
    REQUIRE(texts(lexed)[1] == "1h30m");
    REQUIRE(texts(lexed)[2] == "2m30.5s");
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
    for (std::size_t i = 0; i < 4; ++i)
    {
        REQUIRE(lexed.result.tokens[i].temporal_value.has_value());
        REQUIRE(lexed.result.tokens[i].temporal_value->kind == TemporalKind::Duration);
    }
    REQUIRE(lexed.result.tokens[0].temporal_value->micros == 5LL * 60 * 1'000'000);
    REQUIRE(lexed.result.tokens[1].temporal_value->micros == 90LL * 60 * 1'000'000);
    REQUIRE(lexed.result.tokens[2].temporal_value->micros == 150'500'000LL);
    REQUIRE(lexed.result.tokens[3].temporal_value->micros == 250'000LL);
}

TEST_CASE("unknown duration unit is diagnosed", "[lexer]")
{
    Lexed lexed{"5min"};
    REQUIRE(lexed.result.tokens[0].kind == TokenKind::TemporalLiteral);
    REQUIRE_FALSE(lexed.result.tokens[0].temporal_value.has_value());
    REQUIRE(lexed.diagnostics.size() == 1);
    REQUIRE(lexed.diagnostics.diagnostics()[0].message == "'5min' has unknown duration unit 'min'; units are d h m s ms us");
}

TEST_CASE("date, datetime and zone literals", "[lexer]")
{
    Lexed lexed{"@2026-09-03 @2026-09-03T09:30Z @2026-09-03T09:30:15.5+01:00 "
                "@2026-09-03T10:30+01:00[Europe/London] @09:30 @09:30[Europe/London] @[Europe/London]"};
    const auto ks = kinds(lexed);
    REQUIRE(ks.size() == 8);
    for (std::size_t i = 0; i < 7; ++i)
    {
        INFO("token " << i << " " << lexed.result.tokens[i].text);
        REQUIRE(ks[i] == TokenKind::TemporalLiteral);
        REQUIRE(lexed.result.tokens[i].temporal_value.has_value());
    }
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
    REQUIRE(lexed.result.tokens[0].temporal_value->kind == TemporalKind::Date);
    REQUIRE(lexed.result.tokens[1].temporal_value->kind == TemporalKind::DateTime);
    REQUIRE(lexed.result.tokens[2].temporal_value->kind == TemporalKind::DateTime);
    REQUIRE(lexed.result.tokens[3].temporal_value->kind == TemporalKind::ZonedDateTime);
    REQUIRE(lexed.result.tokens[4].temporal_value->kind == TemporalKind::Time);
    REQUIRE(lexed.result.tokens[5].temporal_value->kind == TemporalKind::ZonedTime);
    REQUIRE(lexed.result.tokens[6].temporal_value->kind == TemporalKind::TimeZone);
    REQUIRE(texts(lexed)[3] == "@2026-09-03T10:30+01:00[Europe/London]");
}

TEST_CASE("a minus after a date or instant starts a new token", "[lexer]")
{
    Lexed lexed{"@2026-09-03-1d @2026-09-03T09:30Z-1d @2026-09-03T10:30-1d"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::TemporalLiteral, TokenKind::Minus,
                                                   TokenKind::TemporalLiteral, TokenKind::TemporalLiteral,
                                                   TokenKind::Minus, TokenKind::TemporalLiteral,
                                                   TokenKind::TemporalLiteral, TokenKind::Minus,
                                                   TokenKind::TemporalLiteral, TokenKind::EndOfFile});
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
    REQUIRE(lexed.result.tokens[6].temporal_value->kind == TemporalKind::CivilDateTime);
}

TEST_CASE("malformed temporal literals are diagnosed", "[lexer]")
{
    SECTION("bad date")
    {
        Lexed lexed{"@2026-13-03"};
        REQUIRE(lexed.result.tokens[0].kind == TokenKind::TemporalLiteral);
        REQUIRE_FALSE(lexed.result.tokens[0].temporal_value.has_value());
        REQUIRE(lexed.diagnostics.size() == 1);
    }
    SECTION("zoned literal without an offset")
    {
        Lexed lexed{"@2026-09-03T10:30[Europe/London]"};
        REQUIRE(lexed.result.tokens[0].kind == TokenKind::TemporalLiteral);
        REQUIRE(lexed.result.tokens[0].text == "@2026-09-03T10:30[Europe/London]");
        REQUIRE_FALSE(lexed.result.tokens[0].temporal_value.has_value());
        REQUIRE(lexed.diagnostics.size() == 1);
        REQUIRE_FALSE(lexed.diagnostics.diagnostics()[0].notes.empty());
    }
    SECTION("bare at")
    {
        Lexed lexed{"@x"};
        REQUIRE(lexed.result.tokens[0].kind == TokenKind::Error);
        REQUIRE(lexed.diagnostics.size() == 1);
    }
    SECTION("unterminated zone")
    {
        Lexed lexed{"@[Europe/London\n"};
        REQUIRE(lexed.result.tokens[0].kind == TokenKind::Error);
        REQUIRE(lexed.diagnostics.size() == 1);
        REQUIRE(lexed.diagnostics.diagnostics()[0].message == "unterminated zone annotation");
    }
}

TEST_CASE("string literals and escapes", "[lexer]")
{
    Lexed lexed{R"("plain" "a\"b\\c\n\t\r")"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::StringLiteral, TokenKind::StringLiteral, TokenKind::EndOfFile});
    REQUIRE(lexed.result.tokens[0].string_value == "plain");
    REQUIRE(lexed.result.tokens[1].string_value == "a\"b\\c\n\t\r");
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
}

TEST_CASE("string literal errors", "[lexer]")
{
    SECTION("unterminated")
    {
        Lexed lexed{"\"abc\nx"};
        REQUIRE(lexed.result.tokens[0].kind == TokenKind::StringLiteral);
        REQUIRE(lexed.diagnostics.size() == 1);
        REQUIRE(lexed.diagnostics.diagnostics()[0].message == "unterminated string literal");
        REQUIRE(lexed.result.tokens[1].kind == TokenKind::Newline);
    }
    SECTION("unknown escape")
    {
        Lexed lexed{R"("\q")"};
        REQUIRE(lexed.diagnostics.size() == 1);
        REQUIRE(lexed.diagnostics.diagnostics()[0].message == "unknown escape sequence '\\q'");
    }
}

TEST_CASE("punctuation and operators", "[lexer]")
{
    Lexed lexed{"( ) { } [ ] < > , : :: . -> => = += -= *= /= == != <= >= + - * / % ! && ||"};
    REQUIRE(kinds(lexed) ==
            std::vector<TokenKind>{TokenKind::LParen,     TokenKind::RParen,      TokenKind::LBrace,
                                   TokenKind::RBrace,     TokenKind::LBracket,    TokenKind::RBracket,
                                   TokenKind::Less,       TokenKind::Greater,     TokenKind::Comma,
                                   TokenKind::Colon,      TokenKind::ColonColon,  TokenKind::Dot,
                                   TokenKind::Arrow,      TokenKind::FatArrow,    TokenKind::Assign,
                                   TokenKind::PlusAssign, TokenKind::MinusAssign, TokenKind::StarAssign,
                                   TokenKind::SlashAssign, TokenKind::EqualEqual, TokenKind::NotEqual,
                                   TokenKind::LessEqual,  TokenKind::GreaterEqual, TokenKind::Plus,
                                   TokenKind::Minus,      TokenKind::Star,        TokenKind::Slash,
                                   TokenKind::Percent,    TokenKind::Bang,        TokenKind::AndAnd,
                                   TokenKind::OrOr,       TokenKind::EndOfFile});
    REQUIRE_FALSE(lexed.diagnostics.has_errors());
}

TEST_CASE("closing angle brackets are single tokens", "[lexer]")
{
    Lexed lexed{"list<tuple<f64, f64>>"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::Identifier, TokenKind::Less, TokenKind::Identifier,
                                                   TokenKind::Less, TokenKind::KwF64, TokenKind::Comma, TokenKind::KwF64,
                                                   TokenKind::Greater, TokenKind::Greater, TokenKind::EndOfFile});
}

TEST_CASE("unexpected characters are diagnosed once per code point", "[lexer]")
{
    Lexed lexed{"a $ b \xC3\xA9 c"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::Identifier, TokenKind::Error, TokenKind::Identifier,
                                                   TokenKind::Error, TokenKind::Identifier, TokenKind::EndOfFile});
    REQUIRE(lexed.diagnostics.size() == 2);
    REQUIRE(lexed.diagnostics.diagnostics()[0].message == "unexpected character '$'");
    REQUIRE(lexed.diagnostics.diagnostics()[1].message == "unexpected character '\xC3\xA9'");
}

TEST_CASE("semicolons are diagnosed as terminators", "[lexer]")
{
    Lexed lexed{"a; b"};
    REQUIRE(kinds(lexed) == std::vector<TokenKind>{TokenKind::Identifier, TokenKind::Error, TokenKind::Identifier,
                                                   TokenKind::EndOfFile});
    REQUIRE(lexed.diagnostics.size() == 1);
    REQUIRE(lexed.diagnostics.diagnostics()[0].message == "';' is not a statement terminator; use a newline");
}

TEST_CASE("token ranges index the source", "[lexer]")
{
    Lexed lexed{"let x = 10\n"};
    const Token &ten = lexed.result.tokens[3];
    REQUIRE(ten.kind == TokenKind::IntLiteral);
    REQUIRE(ten.range == SourceRange{8, 10});
    REQUIRE(lexed.file.location(ten.range.begin) == Location{1, 9});
}
