#include "syntax/token.h"

#include <array>
#include <utility>

namespace hgl::syntax
{
    namespace
    {
        constexpr std::array<std::pair<std::string_view, TokenKind>, 42> keywords{{
            {"module", TokenKind::KwModule},
            {"use", TokenKind::KwUse},
            {"as", TokenKind::KwAs},
            {"export", TokenKind::KwExport},
            {"abstract", TokenKind::KwAbstract},
            {"impl", TokenKind::KwImpl},
            {"operator", TokenKind::KwOperator},
            {"fn", TokenKind::KwFn},
            {"struct", TokenKind::KwStruct},
            {"const", TokenKind::KwConst},
            {"requires", TokenKind::KwRequires},
            {"is", TokenKind::KwIs},
            {"let", TokenKind::KwLet},
            {"var", TokenKind::KwVar},
            {"state", TokenKind::KwState},
            {"inject", TokenKind::KwInject},
            {"return", TokenKind::KwReturn},
            {"if", TokenKind::KwIf},
            {"else", TokenKind::KwElse},
            {"start", TokenKind::KwStart},
            {"when", TokenKind::KwWhen},
            {"stop", TokenKind::KwStop},
            {"for", TokenKind::KwFor},
            {"test", TokenKind::KwTest},
            {"assert", TokenKind::KwAssert},
            {"eval", TokenKind::KwEval},
            {"true", TokenKind::KwTrue},
            {"false", TokenKind::KwFalse},
            {"null", TokenKind::KwNull},
            {"bool", TokenKind::KwBool},
            {"i64", TokenKind::KwI64},
            {"f64", TokenKind::KwF64},
            {"str", TokenKind::KwStr},
            {"date", TokenKind::KwDate},
            {"time", TokenKind::KwTime},
            {"datetime", TokenKind::KwDateTime},
            {"duration", TokenKind::KwDuration},
            {"civil_datetime", TokenKind::KwCivilDateTime},
            {"zoned_datetime", TokenKind::KwZonedDateTime},
            {"zoned_time", TokenKind::KwZonedTime},
            {"timezone", TokenKind::KwTimeZone},
            {"", TokenKind::Error},  // sentinel keeps the array size honest
        }};
    }  // namespace

    std::optional<TokenKind> keyword_kind(std::string_view spelling) noexcept
    {
        for (const auto &[text, kind] : keywords)
        {
            if (!text.empty() && text == spelling) { return kind; }
        }
        return std::nullopt;
    }

    std::string_view token_kind_name(TokenKind kind) noexcept
    {
        switch (kind)
        {
            case TokenKind::EndOfFile: return "end of file";
            case TokenKind::Newline: return "newline";
            case TokenKind::Identifier: return "identifier";
            case TokenKind::Placeholder: return "'_'";
            case TokenKind::IntLiteral: return "integer literal";
            case TokenKind::FloatLiteral: return "float literal";
            case TokenKind::StringLiteral: return "string literal";
            case TokenKind::TemporalLiteral: return "temporal literal";
            case TokenKind::LParen: return "'('";
            case TokenKind::RParen: return "')'";
            case TokenKind::LBrace: return "'{'";
            case TokenKind::RBrace: return "'}'";
            case TokenKind::LBracket: return "'['";
            case TokenKind::RBracket: return "']'";
            case TokenKind::Less: return "'<'";
            case TokenKind::Greater: return "'>'";
            case TokenKind::Comma: return "','";
            case TokenKind::Colon: return "':'";
            case TokenKind::ColonColon: return "'::'";
            case TokenKind::Dot: return "'.'";
            case TokenKind::Arrow: return "'->'";
            case TokenKind::FatArrow: return "'=>'";
            case TokenKind::Assign: return "'='";
            case TokenKind::PlusAssign: return "'+='";
            case TokenKind::MinusAssign: return "'-='";
            case TokenKind::StarAssign: return "'*='";
            case TokenKind::SlashAssign: return "'/='";
            case TokenKind::EqualEqual: return "'=='";
            case TokenKind::NotEqual: return "'!='";
            case TokenKind::LessEqual: return "'<='";
            case TokenKind::GreaterEqual: return "'>='";
            case TokenKind::Plus: return "'+'";
            case TokenKind::Minus: return "'-'";
            case TokenKind::Star: return "'*'";
            case TokenKind::Slash: return "'/'";
            case TokenKind::Percent: return "'%'";
            case TokenKind::Bang: return "'!'";
            case TokenKind::AndAnd: return "'&&'";
            case TokenKind::OrOr: return "'||'";
            case TokenKind::Error: return "invalid token";
            default: break;
        }
        for (const auto &[text, k] : keywords)
        {
            if (k == kind) { return text; }
        }
        return "token";
    }
}  // namespace hgl::syntax
