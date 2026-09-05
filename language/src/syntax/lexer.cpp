#include "syntax/lexer.h"

#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <string>

namespace hgl::syntax
{
    namespace
    {
        [[nodiscard]] constexpr bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }
        [[nodiscard]] constexpr bool is_letter(char c) noexcept
        {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        }
        [[nodiscard]] constexpr bool is_identifier_start(char c) noexcept { return is_letter(c) || c == '_'; }
        [[nodiscard]] constexpr bool is_identifier_part(char c) noexcept { return is_identifier_start(c) || is_digit(c); }

        class Lexer
        {
          public:
            Lexer(const SourceFile &file, DiagnosticSink &diagnostics)
                : src_{file.text()}, diagnostics_{diagnostics}
            {
            }

            LexResult run()
            {
                while (pos_ < src_.size()) { next(); }
                Token eof;
                eof.kind  = TokenKind::EndOfFile;
                eof.range = {end(), end()};
                result_.tokens.push_back(std::move(eof));
                return std::move(result_);
            }

          private:
            [[nodiscard]] std::uint32_t end() const noexcept { return static_cast<std::uint32_t>(src_.size()); }
            [[nodiscard]] char at(std::uint32_t i) const noexcept { return i < src_.size() ? src_[i] : '\0'; }
            [[nodiscard]] char peek(std::uint32_t ahead = 0) const noexcept { return at(pos_ + ahead); }

            void push(TokenKind kind, std::uint32_t begin, std::uint32_t finish)
            {
                Token token;
                token.kind  = kind;
                token.range = {begin, finish};
                push(std::move(token));
            }

            void push(Token token)
            {
                token.text = src_.substr(token.range.begin, token.range.end - token.range.begin);
                result_.fragments.push_back(SourceFragment{SourceFragmentKind::Token, token.range,
                                                           result_.tokens.size()});
                result_.tokens.push_back(std::move(token));
            }

            void error(std::uint32_t begin, std::uint32_t finish, std::string message)
            {
                diagnostics_.report(Category::Parse, {begin, finish}, std::move(message));
            }

            void next()
            {
                const char c = peek();
                if (c == ' ' || c == '\t' || c == '\r')
                {
                    whitespace();
                    return;
                }
                if (c == '\n')
                {
                    newline();
                    return;
                }
                if (c == '/' && peek(1) == '/')
                {
                    comment();
                    return;
                }
                if (is_identifier_start(c))
                {
                    identifier();
                    return;
                }
                if (is_digit(c))
                {
                    number();
                    return;
                }
                if (c == '"')
                {
                    string();
                    return;
                }
                if (c == '@')
                {
                    at_literal();
                    return;
                }
                punctuation();
            }

            void whitespace()
            {
                const std::uint32_t begin = pos_;
                while (peek() == ' ' || peek() == '\t' || peek() == '\r') { ++pos_; }
                result_.fragments.push_back(
                    SourceFragment{SourceFragmentKind::Whitespace, {begin, pos_}, no_token_index});
            }

            // One Newline token per run of terminators; the run may contain
            // blank lines, spaces, and comments.
            void newline()
            {
                const std::uint32_t begin = pos_;
                ++pos_;
                if (!result_.tokens.empty() && result_.tokens.back().kind == TokenKind::Newline)
                {
                    result_.tokens.back().range.end = pos_;
                    result_.tokens.back().text = src_.substr(result_.tokens.back().range.begin,
                                                             result_.tokens.back().range.end -
                                                                 result_.tokens.back().range.begin);
                    result_.fragments.push_back(SourceFragment{SourceFragmentKind::LineBreak,
                                                               {begin, pos_},
                                                               result_.tokens.size() - 1});
                    return;
                }
                Token token;
                token.kind  = TokenKind::Newline;
                token.range = {begin, pos_};
                token.text  = src_.substr(begin, pos_ - begin);
                result_.tokens.push_back(std::move(token));
                result_.fragments.push_back(SourceFragment{SourceFragmentKind::LineBreak,
                                                           {begin, pos_},
                                                           result_.tokens.size() - 1});
            }

            void comment()
            {
                const std::uint32_t begin = pos_;
                while (pos_ < src_.size() && src_[pos_] != '\n') { ++pos_; }
                result_.comments.push_back(SourceComment{{begin, pos_}});
                result_.fragments.push_back(
                    SourceFragment{SourceFragmentKind::LineComment, {begin, pos_}, no_token_index});
            }

            void identifier()
            {
                const std::uint32_t begin = pos_;
                while (is_identifier_part(peek())) { ++pos_; }
                const std::string_view text = src_.substr(begin, pos_ - begin);
                if (text == "_")
                {
                    push(TokenKind::Placeholder, begin, pos_);
                    return;
                }
                if (const auto keyword = keyword_kind(text)) { push(*keyword, begin, pos_); }
                else { push(TokenKind::Identifier, begin, pos_); }
            }

            // Numbers: `digits`, `digits.digits`, either with an exponent; a
            // letter directly after the numeric prefix makes the whole
            // digit-and-letter run a duration literal candidate (`5m`,
            // `1h30m`, `2m30.5s`), and an unknown unit is diagnosed there.
            void number()
            {
                const std::uint32_t begin = pos_;
                while (is_digit(peek())) { ++pos_; }
                bool is_float = false;
                if (peek() == '.' && is_digit(peek(1)))
                {
                    is_float = true;
                    ++pos_;
                    while (is_digit(peek())) { ++pos_; }
                }
                if ((peek() == 'e' || peek() == 'E') &&
                    (is_digit(peek(1)) || ((peek(1) == '+' || peek(1) == '-') && is_digit(peek(2)))))
                {
                    // An exponent is only an exponent when no unit follows it
                    // (`1e5` is a float; `1e5m` is an invalid duration run).
                    std::uint32_t probe = pos_ + 1;
                    if (src_[probe] == '+' || src_[probe] == '-') { ++probe; }
                    while (is_digit(at(probe))) { ++probe; }
                    if (!is_identifier_part(at(probe)))
                    {
                        is_float = true;
                        pos_     = probe;
                    }
                }
                if (is_identifier_part(peek()))
                {
                    duration(begin);
                    return;
                }

                Token token;
                token.range = {begin, pos_};
                const std::string_view text = src_.substr(begin, pos_ - begin);
                if (is_float)
                {
                    token.kind = TokenKind::FloatLiteral;
                    // std::from_chars for double is available on GCC 11+/MSVC.
                    double value = 0.0;
                    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
                    if (ec == std::errc::result_out_of_range)
                    {
                        error(begin, pos_, "'" + std::string{text} + "' is out of range for f64");
                    }
                    else { token.float_value = value; }
                }
                else
                {
                    token.kind = TokenKind::IntLiteral;
                    std::int64_t value = 0;
                    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
                    if (ec == std::errc::result_out_of_range)
                    {
                        error(begin, pos_, "'" + std::string{text} + "' is out of range for i64");
                    }
                    else { token.int_value = value; }
                }
                push(std::move(token));
            }

            // The digit/letter run of a duration: parts are `digits[.digits]unit`.
            void duration(std::uint32_t begin)
            {
                pos_ = begin;
                while (true)
                {
                    if (is_identifier_part(peek())) { ++pos_; }
                    else if (peek() == '.' && is_digit(peek(1))) { ++pos_; }
                    else { break; }
                }
                temporal(begin, pos_);
            }

            void temporal(std::uint32_t begin, std::uint32_t finish)
            {
                Token token;
                token.kind  = TokenKind::TemporalLiteral;
                token.range = {begin, finish};
                const std::string_view text = src_.substr(begin, finish - begin);
                TemporalParseResult    parsed = parse_temporal_literal(text);
                if (parsed.value.has_value()) { token.temporal_value = std::move(parsed.value); }
                else
                {
                    Diagnostic &diagnostic = diagnostics_.report(Category::Parse, {begin, finish}, std::move(parsed.error));
                    if (!parsed.hint.empty()) { diagnostic.notes.push_back(Note{std::move(parsed.hint), {begin, finish}}); }
                }
                push(std::move(token));
            }

            // `@` literals are scanned by shape (developer guide, "Literals"):
            // `@[zone]`, `@date[Ttime[offset][[zone]]]`, or `@time[[zone]]`.
            // Only the shape is consumed here; validation is the temporal
            // module's. A `-` after a date or `Z` starts a new token, so
            // `@2026-09-03-1d` is `date - duration`.
            void at_literal()
            {
                const std::uint32_t begin = pos_;
                std::uint32_t       p     = pos_ + 1;
                if (at(p) == '[')
                {
                    if (!scan_zone(p))
                    {
                        error(begin, p, "unterminated zone annotation");
                        pos_ = p;
                        push(TokenKind::Error, begin, p);
                        return;
                    }
                    pos_ = p;
                    temporal(begin, p);
                    return;
                }
                if (!is_digit(at(p)))
                {
                    error(begin, p, "'@' must be followed by a date, time, or zone annotation");
                    pos_ = p;
                    push(TokenKind::Error, begin, p);
                    return;
                }
                while (is_digit(at(p))) { ++p; }
                if (at(p) == '-')
                {
                    // calendar date: digits '-' digits '-' digits
                    ++p;
                    while (is_digit(at(p))) { ++p; }
                    if (at(p) == '-')
                    {
                        ++p;
                        while (is_digit(at(p))) { ++p; }
                    }
                    if (at(p) == 'T')
                    {
                        ++p;
                        scan_clock_time(p);
                        scan_offset(p);
                    }
                }
                else if (at(p) == ':')
                {
                    // clock time; rewind to scan the whole time
                    p = pos_ + 1;
                    scan_clock_time(p);
                }
                if (at(p) == '[' && !scan_zone(p))
                {
                    error(begin, p, "unterminated zone annotation");
                    pos_ = p;
                    push(TokenKind::Error, begin, p);
                    return;
                }
                pos_ = p;
                temporal(begin, p);
            }

            // digits ':' digits [':' digits ['.' digits]]
            void scan_clock_time(std::uint32_t &p) const noexcept
            {
                while (is_digit(at(p))) { ++p; }
                if (at(p) != ':') { return; }
                ++p;
                while (is_digit(at(p))) { ++p; }
                if (at(p) == ':' && is_digit(at(p + 1)))
                {
                    ++p;
                    while (is_digit(at(p))) { ++p; }
                    if (at(p) == '.' && is_digit(at(p + 1)))
                    {
                        ++p;
                        while (is_digit(at(p))) { ++p; }
                    }
                }
            }

            // 'Z' | sign digit digit [':' digit digit]. A sign needs two
            // digits directly after it, so `-1d` after a civil value is not
            // taken as an offset.
            void scan_offset(std::uint32_t &p) const noexcept
            {
                if (at(p) == 'Z')
                {
                    ++p;
                    return;
                }
                if ((at(p) == '+' || at(p) == '-') && is_digit(at(p + 1)) && is_digit(at(p + 2)))
                {
                    p += 3;
                    if (at(p) == ':' && is_digit(at(p + 1)) && is_digit(at(p + 2))) { p += 3; }
                }
            }

            // '[' ... ']' on one line; returns false when unterminated.
            [[nodiscard]] bool scan_zone(std::uint32_t &p) const noexcept
            {
                std::uint32_t q = p + 1;
                while (q < src_.size() && src_[q] != ']' && src_[q] != '\n') { ++q; }
                if (at(q) != ']')
                {
                    p = q;
                    return false;
                }
                p = q + 1;
                return true;
            }

            void string()
            {
                const std::uint32_t begin = pos_;
                ++pos_;
                std::string value;
                while (true)
                {
                    const char c = peek();
                    if (c == '\0' || c == '\n')
                    {
                        error(begin, pos_, "unterminated string literal");
                        break;
                    }
                    ++pos_;
                    if (c == '"') { break; }
                    if (c != '\\')
                    {
                        value.push_back(c);
                        continue;
                    }
                    const char escaped = peek();
                    switch (escaped)
                    {
                        case '"': value.push_back('"'); break;
                        case '\\': value.push_back('\\'); break;
                        case 'n': value.push_back('\n'); break;
                        case 'r': value.push_back('\r'); break;
                        case 't': value.push_back('\t'); break;
                        default:
                            error(pos_ - 1, pos_ + (escaped == '\0' || escaped == '\n' ? 0 : 1),
                                  std::string{"unknown escape sequence '\\"} + escaped + "'");
                            break;
                    }
                    if (escaped != '\0' && escaped != '\n') { ++pos_; }
                }
                Token token;
                token.kind         = TokenKind::StringLiteral;
                token.range        = {begin, pos_};
                token.string_value = std::move(value);
                push(std::move(token));
            }

            void punctuation()
            {
                const std::uint32_t begin = pos_;
                const char          c     = peek();
                const char          d     = peek(1);
                auto two = [&](TokenKind kind) {
                    pos_ += 2;
                    push(kind, begin, pos_);
                };
                auto one = [&](TokenKind kind) {
                    pos_ += 1;
                    push(kind, begin, pos_);
                };
                switch (c)
                {
                    case ':': return d == ':' ? two(TokenKind::ColonColon) : one(TokenKind::Colon);
                    case '-': return d == '>' ? two(TokenKind::Arrow) : d == '=' ? two(TokenKind::MinusAssign) : one(TokenKind::Minus);
                    case '=':
                        return d == '>' ? two(TokenKind::FatArrow) : d == '=' ? two(TokenKind::EqualEqual) : one(TokenKind::Assign);
                    case '+': return d == '=' ? two(TokenKind::PlusAssign) : one(TokenKind::Plus);
                    case '*': return d == '=' ? two(TokenKind::StarAssign) : one(TokenKind::Star);
                    case '/': return d == '=' ? two(TokenKind::SlashAssign) : one(TokenKind::Slash);
                    case '!': return d == '=' ? two(TokenKind::NotEqual) : one(TokenKind::Bang);
                    case '<': return d == '=' ? two(TokenKind::LessEqual) : one(TokenKind::Less);
                    case '>': return d == '=' ? two(TokenKind::GreaterEqual) : one(TokenKind::Greater);
                    case '&':
                        if (d == '&') { return two(TokenKind::AndAnd); }
                        break;
                    case '|':
                        if (d == '|') { return two(TokenKind::OrOr); }
                        break;
                    case '(': return one(TokenKind::LParen);
                    case ')': return one(TokenKind::RParen);
                    case '{': return one(TokenKind::LBrace);
                    case '}': return one(TokenKind::RBrace);
                    case '[': return one(TokenKind::LBracket);
                    case ']': return one(TokenKind::RBracket);
                    case ',': return one(TokenKind::Comma);
                    case '.': return one(TokenKind::Dot);
                    case '%': return one(TokenKind::Percent);
                    case ';':
                        error(begin, begin + 1, "';' is not a statement terminator; use a newline");
                        ++pos_;
                        push(TokenKind::Error, begin, pos_);
                        return;
                    default: break;
                }
                // Unknown byte (or a UTF-8 lead byte): consume the whole
                // code point so the diagnostic covers it once.
                std::uint32_t finish = pos_ + 1;
                if (static_cast<unsigned char>(c) >= 0x80)
                {
                    while (finish < src_.size() && (static_cast<unsigned char>(src_[finish]) & 0xC0) == 0x80) { ++finish; }
                }
                error(begin, finish, "unexpected character '" + std::string{src_.substr(begin, finish - begin)} + "'");
                pos_ = finish;
                push(TokenKind::Error, begin, finish);
            }

            std::string_view src_;
            DiagnosticSink  &diagnostics_;
            std::uint32_t    pos_{0};
            LexResult        result_{};
        };
    }  // namespace

    LexResult lex(const SourceFile &file, DiagnosticSink &diagnostics)
    {
        return Lexer{file, diagnostics}.run();
    }
}  // namespace hgl::syntax
