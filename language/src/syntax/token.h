#ifndef HGL_SYNTAX_TOKEN_H
#define HGL_SYNTAX_TOKEN_H

#include "syntax/source.h"
#include "syntax/temporal.h"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace hgl::syntax
{
    /// Token kinds. Keywords are the hard reserved words of the developer
    /// guide ("Lexical rules"); contextual type keywords (`atomic`, `tuple`,
    /// `list`, `set`, `map`, `rolling`, `unbounded`) lex as identifiers.
    enum class TokenKind : std::uint8_t
    {
        EndOfFile,
        Newline,  ///< one token per run of line terminators (comments included)

        Identifier,
        Placeholder,  ///< a lone `_`
        IntLiteral,
        FloatLiteral,
        StringLiteral,
        TemporalLiteral,

        // Hard reserved words.
        KwModule,
        KwUse,
        KwAs,
        KwExport,
        KwAbstract,
        KwImpl,
        KwOperator,
        KwFn,
        KwStruct,
        KwConst,
        KwRequires,
        KwIs,
        KwLet,
        KwVar,
        KwState,
        KwInject,
        KwReturn,
        KwIf,
        KwElse,
        KwStart,
        KwWhen,
        KwStop,
        KwFor,  ///< `in` is contextual after the pattern and lexes as an identifier
        KwTest,
        KwAssert,
        KwEval,
        KwTrue,
        KwFalse,
        KwNull,
        KwBool,
        KwI64,
        KwF64,
        KwStr,
        KwDate,
        KwTime,
        KwDateTime,
        KwDuration,
        KwCivilDateTime,
        KwZonedDateTime,
        KwZonedTime,
        KwTimeZone,

        // Punctuation and operators.
        LParen,
        RParen,
        LBrace,
        RBrace,
        LBracket,
        RBracket,
        Less,     ///< `<`: comparison or generic/type argument opener
        Greater,  ///< `>`: comparison or generic/type argument closer
        Comma,
        Colon,
        ColonColon,
        Dot,
        Arrow,       ///< `->`
        FatArrow,    ///< `=>`
        Assign,      ///< `=`
        PlusAssign,  ///< `+=`
        MinusAssign,
        StarAssign,
        SlashAssign,
        EqualEqual,
        NotEqual,
        LessEqual,
        GreaterEqual,
        Plus,
        Minus,
        Star,
        Slash,
        Percent,
        Bang,
        AndAnd,
        OrOr,

        Error,  ///< an unrecognised character run; a diagnostic was reported
    };

    [[nodiscard]] std::string_view token_kind_name(TokenKind kind) noexcept;

    /// Keyword lookup for an identifier spelling; nullopt when not reserved.
    [[nodiscard]] std::optional<TokenKind> keyword_kind(std::string_view spelling) noexcept;

    [[nodiscard]] constexpr bool is_keyword(TokenKind kind) noexcept
    {
        return kind >= TokenKind::KwModule && kind <= TokenKind::KwTimeZone;
    }

    /// The scalar type keywords (`bool` .. `timezone`).
    [[nodiscard]] constexpr bool is_scalar_type_keyword(TokenKind kind) noexcept
    {
        return kind >= TokenKind::KwBool && kind <= TokenKind::KwTimeZone;
    }

    struct Token
    {
        TokenKind   kind{TokenKind::EndOfFile};
        SourceRange range{};
        /// The token's spelling in the source (a string literal keeps its quotes).
        std::string_view text{};

        // Literal payloads, valid for the matching kind.
        std::int64_t                 int_value{0};
        double                       float_value{0.0};
        std::string                  string_value{};  ///< unescaped contents
        std::optional<TemporalValue> temporal_value{};  ///< empty when the literal was invalid

        [[nodiscard]] bool is(TokenKind k) const noexcept { return kind == k; }
    };
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_TOKEN_H
