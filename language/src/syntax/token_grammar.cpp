#include "syntax/token_grammar.h"

#include <lexy/action/parse_as_tree.hpp>
#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <lexy/input/string_input.hpp>
#include <lexy/parse_tree.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace hgl::syntax
{
    namespace grammar
    {
        namespace dsl = lexy::dsl;

        enum class ContextToken : std::uint8_t {
            In = 0x80,
            Atomic,
            Tuple,
            List,
            Set,
            Map,
            Rolling,
            Unbounded,
            Delta,
            AppliedConstructor,
        };

        template <TokenKind Kind> inline constexpr auto token = dsl::lit_b<static_cast<std::uint8_t>(Kind)>;

        template <ContextToken Kind> inline constexpr auto contextual = dsl::lit_b<static_cast<std::uint8_t>(Kind)>;

        template <TokenKind... Kinds> inline constexpr auto token_choice = (token<Kinds> / ...);

        inline constexpr auto identifier      = token<TokenKind::Identifier>;
        inline constexpr auto newline         = token<TokenKind::Newline>;
        inline constexpr auto contextual_name = contextual<ContextToken::In> / contextual<ContextToken::Atomic> /
                                                contextual<ContextToken::Tuple> / contextual<ContextToken::List> /
                                                contextual<ContextToken::Set> / contextual<ContextToken::Map> /
                                                contextual<ContextToken::Rolling> / contextual<ContextToken::Unbounded> /
                                                contextual<ContextToken::Delta> / contextual<ContextToken::AppliedConstructor>;
        inline constexpr auto reserved_name =
            token_choice<TokenKind::KwModule, TokenKind::KwUse, TokenKind::KwAs, TokenKind::KwExport, TokenKind::KwAbstract,
                         TokenKind::KwImpl, TokenKind::KwOperator, TokenKind::KwFn, TokenKind::KwStruct, TokenKind::KwConst,
                         TokenKind::KwRequires, TokenKind::KwIs, TokenKind::KwLet, TokenKind::KwVar, TokenKind::KwState,
                         TokenKind::KwInject, TokenKind::KwReturn, TokenKind::KwIf, TokenKind::KwElse, TokenKind::KwStart,
                         TokenKind::KwWhen, TokenKind::KwStop, TokenKind::KwFor, TokenKind::KwTest, TokenKind::KwAssert,
                         TokenKind::KwEval, TokenKind::KwTrue, TokenKind::KwFalse, TokenKind::KwNull, TokenKind::KwBool,
                         TokenKind::KwI64, TokenKind::KwF64, TokenKind::KwStr, TokenKind::KwDate, TokenKind::KwTime,
                         TokenKind::KwDateTime, TokenKind::KwDuration, TokenKind::KwCivilDateTime, TokenKind::KwZonedDateTime,
                         TokenKind::KwZonedTime, TokenKind::KwTimeZone>;

        inline constexpr auto ordinary_name = identifier / contextual_name;
        inline constexpr auto raw_name      = ordinary_name / reserved_name;

        struct name
        { static constexpr auto rule = raw_name; };

        struct newlines
        { static constexpr auto rule = dsl::while_(newline); };

        struct line_end
        { static constexpr auto rule = newline >> dsl::while_(newline) | dsl::peek(dsl::eof); };

        struct comma_separator
        {
            static constexpr auto rule = dsl::peek(dsl::p<newlines> + token<TokenKind::Comma>) >>
                                         dsl::p<newlines> + token<TokenKind::Comma> + dsl::p<newlines>;
        };

        struct module_path
        { static constexpr auto rule = dsl::p<name> + dsl::while_(token<TokenKind::Dot> >> dsl::p<name>); };

        struct qualified_name
        { static constexpr auto rule = dsl::p<name> >> dsl::if_(token<TokenKind::ColonColon> >> dsl::p<name>); };

        inline constexpr auto scalar_type = token<TokenKind::KwBool> / token<TokenKind::KwI64> / token<TokenKind::KwF64> /
                                            token<TokenKind::KwStr> / token<TokenKind::KwDate> / token<TokenKind::KwTime> /
                                            token<TokenKind::KwDateTime> / token<TokenKind::KwDuration> /
                                            token<TokenKind::KwCivilDateTime> / token<TokenKind::KwZonedDateTime> /
                                            token<TokenKind::KwZonedTime> / token<TokenKind::KwTimeZone>;

        inline constexpr auto type_start = scalar_type / ordinary_name;

        struct expression;
        struct type;

        inline constexpr auto expression_start =
            ordinary_name / token<TokenKind::Placeholder> / token<TokenKind::IntLiteral> / token<TokenKind::FloatLiteral> /
            token<TokenKind::StringLiteral> / token<TokenKind::TemporalLiteral> / token<TokenKind::KwTrue> /
            token<TokenKind::KwFalse> / token<TokenKind::KwNull> / token<TokenKind::Minus> / token<TokenKind::Bang> /
            token<TokenKind::LParen> / token<TokenKind::LBracket> / token<TokenKind::KwFn> / token<TokenKind::KwIf> /
            token<TokenKind::KwEval> / token<TokenKind::LBrace>;

        struct size_expression;
        struct generic_argument;

        struct generic_arguments
        {
            static constexpr auto rule = token<TokenKind::Less> >>
                                         dsl::p<newlines> +
                                             dsl::list(dsl::peek(type_start / expression_start) >> dsl::recurse<generic_argument>,
                                                       dsl::trailing_sep(dsl::p<comma_separator>)) +
                                             dsl::p<newlines> + token<TokenKind::Greater>;
        };

        struct named_type
        { static constexpr auto rule = dsl::p<qualified_name> >> dsl::if_(dsl::p<generic_arguments>); };

        struct tuple_type
        {
            static constexpr auto rule = dsl::peek(contextual<ContextToken::Tuple> + token<TokenKind::Less>) >>
                                         contextual<ContextToken::Tuple> + token<TokenKind::Less> + dsl::p<newlines> +
                                             dsl::if_(dsl::list(dsl::peek(type_start) >> dsl::recurse<type>,
                                                                dsl::trailing_sep(dsl::p<comma_separator>))) +
                                             dsl::p<newlines> + token<TokenKind::Greater>;
        };

        struct list_type
        {
            static constexpr auto size =
                contextual<ContextToken::Unbounded> | dsl::peek(expression_start) >> dsl::recurse<size_expression>;
            static constexpr auto
                rule = dsl::peek(contextual<ContextToken::List> + token<TokenKind::Less>) >>
                       contextual<ContextToken::List> + token<TokenKind::Less> + dsl::p<newlines> + dsl::recurse<type> +
                           dsl::if_(dsl::p<comma_separator> >> size + dsl::p<newlines>) + token<TokenKind::Greater>;
        };

        struct set_type
        {
            static constexpr auto rule = dsl::peek(contextual<ContextToken::Set> + token<TokenKind::Less>) >>
                                         contextual<ContextToken::Set> + token<TokenKind::Less> + dsl::p<newlines> +
                                             dsl::recurse<type> + dsl::p<newlines> + token<TokenKind::Greater>;
        };

        struct map_type
        {
            static constexpr auto rule = dsl::peek(contextual<ContextToken::Map> + token<TokenKind::Less>) >>
                                         contextual<ContextToken::Map> + token<TokenKind::Less> + dsl::p<newlines> +
                                             dsl::recurse<type> + dsl::p<comma_separator> + dsl::recurse<type> + dsl::p<newlines> +
                                             token<TokenKind::Greater>;
        };

        struct rolling_type
        {
            static constexpr auto rule = dsl::peek(contextual<ContextToken::Rolling> + token<TokenKind::Less>) >>
                                         contextual<ContextToken::Rolling> + token<TokenKind::Less> + dsl::p<newlines> +
                                             dsl::recurse<type> + dsl::p<comma_separator> + dsl::recurse<size_expression> +
                                             dsl::if_(dsl::p<comma_separator> >> dsl::recurse<size_expression>) + dsl::p<newlines> +
                                             token<TokenKind::Greater>;
        };

        struct atomic_type
        {
            static constexpr auto rule = dsl::peek(contextual<ContextToken::Atomic> + token<TokenKind::Less>) >>
                                         contextual<ContextToken::Atomic> + token<TokenKind::Less> + dsl::p<newlines> +
                                             dsl::recurse<type> + dsl::p<newlines> + token<TokenKind::Greater>;
        };

        struct type
        {
            static constexpr auto rule = scalar_type | dsl::p<tuple_type> | dsl::p<list_type> | dsl::p<set_type> |
                                         dsl::p<map_type> | dsl::p<rolling_type> | dsl::p<atomic_type> | dsl::p<named_type>;
        };

        struct generic_argument
        {
            static constexpr auto rule = scalar_type | dsl::p<tuple_type> | dsl::p<list_type> | dsl::p<set_type> |
                                         dsl::p<map_type> | dsl::p<rolling_type> | dsl::p<atomic_type> |
                                         dsl::peek(ordinary_name + (token<TokenKind::Less> / token<TokenKind::ColonColon>)) >>
                                             dsl::p<named_type> |
                                         dsl::peek(expression_start) >> dsl::recurse<size_expression>;
        };

        template <TokenKind... Kinds> struct continued_operator
        {
            static constexpr auto direct = token_choice<Kinds...> >> dsl::p<newlines>;
            static constexpr auto continued =
                dsl::peek(newline + token_choice<Kinds...>) >> (newline + token_choice<Kinds...> + dsl::p<newlines>);
            static constexpr auto rule = direct | continued;
        };

        struct unary_expression;
        struct postfix_expression;
        struct primary_expression;

        struct product_expression
        {
            static constexpr auto rule =
                dsl::recurse<unary_expression> +
                dsl::while_(dsl::p<continued_operator<TokenKind::Star, TokenKind::Slash, TokenKind::Percent>> >>
                            dsl::recurse<unary_expression>);
        };

        struct sum_expression
        {
            static constexpr auto rule =
                dsl::p<product_expression> +
                dsl::while_(dsl::p<continued_operator<TokenKind::Plus, TokenKind::Minus>> >> dsl::p<product_expression>);
        };

        struct comparison_expression
        {
            static constexpr auto rule =
                dsl::p<sum_expression> +
                dsl::while_(
                    dsl::p<
                        continued_operator<TokenKind::Less, TokenKind::LessEqual, TokenKind::Greater, TokenKind::GreaterEqual>> >>
                    dsl::p<sum_expression>);
        };

        struct equality_expression
        {
            static constexpr auto rule = dsl::p<comparison_expression> +
                                         dsl::while_(dsl::p<continued_operator<TokenKind::EqualEqual, TokenKind::NotEqual>> >>
                                                     dsl::p<comparison_expression>);
        };

        struct and_expression
        {
            static constexpr auto rule = dsl::p<equality_expression> +
                                         dsl::while_(dsl::p<continued_operator<TokenKind::AndAnd>> >> dsl::p<equality_expression>);
        };

        struct expression
        {
            static constexpr auto rule =
                dsl::p<and_expression> + dsl::while_(dsl::p<continued_operator<TokenKind::OrOr>> >> dsl::p<and_expression>);
        };

        struct size_expression
        { static constexpr auto rule = dsl::p<sum_expression>; };

        struct argument
        {
            static constexpr auto named = dsl::peek(raw_name + token<TokenKind::Colon>) >>
                                          dsl::p<name> + token<TokenKind::Colon> + dsl::p<newlines> + dsl::recurse<expression>;
            static constexpr auto rule  = named | dsl::peek(expression_start) >> dsl::recurse<expression>;
        };

        struct arguments
        {
            static constexpr auto rule = token<TokenKind::LParen> >>
                                         dsl::p<newlines> +
                                             dsl::if_(dsl::list(dsl::p<argument>, dsl::trailing_sep(dsl::p<comma_separator>))) +
                                             dsl::p<newlines> + token<TokenKind::RParen>;
        };

        struct index_postfix
        {
            static constexpr auto rule = token<TokenKind::LBracket> >> dsl::p<newlines> + dsl::recurse<expression> +
                                                                           dsl::p<newlines> + token<TokenKind::RBracket>;
        };

        struct call_postfix
        { static constexpr auto rule = dsl::p<arguments>; };

        struct field_postfix
        { static constexpr auto rule = token<TokenKind::Dot> >> dsl::p<name>; };

        struct postfix
        { static constexpr auto rule = dsl::p<call_postfix> | dsl::p<index_postfix> | dsl::p<field_postfix>; };

        struct tuple_or_group;
        struct sequence_literal;
        struct anonymous_function;
        struct if_expression;
        struct eval_expression;
        struct block;

        struct explicit_construct
        {
            static constexpr auto
                normal = dsl::peek(contextual<ContextToken::AppliedConstructor>) >>
                         dsl::p<name> +
                             dsl::if_(token<TokenKind::ColonColon> >> dsl::p<name>) + dsl::p<generic_arguments> + dsl::p<arguments>;
            static constexpr auto delta = dsl::peek(contextual<ContextToken::Delta>) >>
                                          dsl::p<name> + token<TokenKind::Less> + dsl::p<newlines> + dsl::p<type> +
                                              dsl::p<newlines> + token<TokenKind::Greater> + dsl::p<arguments>;
            static constexpr auto rule  = normal | delta;
        };

        struct primary_expression
        {
            static constexpr auto literal = token<TokenKind::IntLiteral> / token<TokenKind::FloatLiteral> /
                                            token<TokenKind::StringLiteral> / token<TokenKind::TemporalLiteral> /
                                            token<TokenKind::KwTrue> / token<TokenKind::KwFalse> / token<TokenKind::KwNull> /
                                            token<TokenKind::Placeholder>;
            static constexpr auto rule    = dsl::p<explicit_construct> | literal |
                                            dsl::peek(token<TokenKind::LParen>) >> dsl::recurse<tuple_or_group> |
                                            dsl::peek(token<TokenKind::LBracket>) >> dsl::recurse<sequence_literal> |
                                            dsl::peek(token<TokenKind::KwFn>) >> dsl::recurse<anonymous_function> |
                                            dsl::peek(token<TokenKind::KwIf>) >> dsl::recurse<if_expression> |
                                            dsl::peek(token<TokenKind::KwEval>) >> dsl::recurse<eval_expression> |
                                            dsl::peek(token<TokenKind::LBrace>) >> dsl::recurse<block> | dsl::p<qualified_name>;
        };

        struct postfix_expression
        { static constexpr auto rule = dsl::p<primary_expression> + dsl::while_(dsl::p<postfix>); };

        struct unary_expression
        {
            static constexpr auto rule = token_choice<TokenKind::Minus, TokenKind::Bang> >> dsl::recurse<unary_expression> |
                                         dsl::peek(expression_start) >> dsl::p<postfix_expression>;
        };

        struct tuple_element
        { static constexpr auto rule = dsl::peek(expression_start) >> dsl::recurse<expression>; };

        struct tuple_or_group
        {
            static constexpr auto rule =
                token<TokenKind::LParen> >>
                dsl::p<newlines> +
                    dsl::if_(dsl::peek(expression_start) >>
                             dsl::p<tuple_element> +
                                  dsl::if_(dsl::p<comma_separator> >>
                                           dsl::if_(dsl::list(dsl::p<tuple_element>, dsl::trailing_sep(dsl::p<comma_separator>))))) +
                     dsl::p<newlines> + token<TokenKind::RParen>;
        };

        struct sequence_element
        {
            static constexpr auto timed =
                dsl::peek(token<TokenKind::TemporalLiteral> + token<TokenKind::Colon>) >>
                token<TokenKind::TemporalLiteral> + token<TokenKind::Colon> + dsl::p<newlines> + dsl::recurse<expression>;
            static constexpr auto rule = timed | dsl::peek(expression_start) >> dsl::recurse<expression>;
        };

        struct sequence_literal
        {
            static constexpr auto rule = token<TokenKind::LBracket> >>
                                         dsl::p<newlines> +
                                             dsl::if_(dsl::list(dsl::p<sequence_element>,
                                                                dsl::trailing_sep(dsl::p<comma_separator>))) +
                                             dsl::p<newlines> + token<TokenKind::RBracket>;
        };

        struct anonymous_parameter
        { static constexpr auto rule = dsl::p<name> >> dsl::if_(token<TokenKind::Colon> >> dsl::p<newlines> + dsl::p<type>); };

        struct anonymous_function
        {
            static constexpr auto rule = token<TokenKind::KwFn> >>
                                         token<TokenKind::LParen> + dsl::p<newlines> +
                                             dsl::if_(dsl::list(dsl::p<anonymous_parameter>,
                                                                dsl::trailing_sep(dsl::p<comma_separator>))) +
                                             dsl::p<newlines> + token<TokenKind::RParen> +
                                             dsl::if_(dsl::p<continued_operator<TokenKind::Arrow>> >> dsl::p<type>) +
                                             dsl::p<continued_operator<TokenKind::FatArrow>> + dsl::recurse<expression>;
        };

        struct else_arm
        {
            static constexpr auto rule =
                token<TokenKind::KwElse> >> (dsl::peek(token<TokenKind::KwIf>) >> dsl::recurse<if_expression> |
                                             dsl::peek(token<TokenKind::LBrace>) >> dsl::recurse<block>);
        };

        struct if_expression
        {
            static constexpr auto rule = token<TokenKind::KwIf> >> dsl::recurse<expression> + dsl::recurse<block> +
                                                                       dsl::if_(dsl::peek(newline + token<TokenKind::KwElse>) >>
                                                                                    newline + dsl::p<newlines> + dsl::p<else_arm> |
                                                                                dsl::p<else_arm>);
        };

        struct eval_expression
        {
            static constexpr auto next_argument =
                dsl::peek(dsl::p<comma_separator> + expression_start) >> dsl::p<comma_separator> + dsl::p<argument>;
            static constexpr auto trailing_comma = dsl::if_(dsl::p<comma_separator>);
            static constexpr auto rule           = token<TokenKind::KwEval> >> token<TokenKind::LParen> + dsl::p<newlines> +
                                                                                   dsl::recurse<expression> +
                                                                                   dsl::while_(next_argument) + trailing_comma
                                                                                   + dsl::p<newlines> + token<TokenKind::RParen>;
        };

        struct local_decl
        {
            static constexpr auto rule = token_choice<TokenKind::KwLet, TokenKind::KwVar> >>
                                         dsl::p<name> + dsl::if_(token<TokenKind::Colon> >> dsl::p<newlines> + dsl::p<type>) +
                                             token<TokenKind::Assign> + dsl::p<newlines> + dsl::recurse<expression>;
        };

        struct state_decl
        {
            static constexpr auto rule = token<TokenKind::KwState> >>
                                         dsl::p<name> + dsl::if_(token<TokenKind::Colon> >> dsl::p<newlines> + dsl::p<type>) +
                                             token<TokenKind::Assign> + dsl::p<newlines> + dsl::recurse<expression>;
        };

        struct inject_decl
        {
            static constexpr auto next_name = dsl::peek(dsl::p<newlines> + token<TokenKind::Comma> + dsl::p<newlines> + raw_name) >>
                                              dsl::p<newlines> + token<TokenKind::Comma> + dsl::p<newlines> + dsl::p<name>;
            static constexpr auto trailing_comma =
                dsl::peek(dsl::p<newlines> + token<TokenKind::Comma>) >> dsl::p<newlines> + token<TokenKind::Comma>;
            static constexpr auto rule = token<TokenKind::KwInject> >>
                                         dsl::p<newlines> + dsl::p<name> + dsl::while_(next_name) + dsl::if_(trailing_comma);
        };

        struct lifecycle_stmt
        { static constexpr auto rule = token_choice<TokenKind::KwStart, TokenKind::KwStop> >> dsl::recurse<block>; };

        struct when_stmt
        { static constexpr auto rule = token<TokenKind::KwWhen> >> dsl::recurse<expression> + dsl::recurse<block>; };

        struct for_stmt
        {
            static constexpr auto rule = token<TokenKind::KwFor> >>
                                         dsl::p<name> + dsl::if_(token<TokenKind::Comma> >> dsl::p<name>) +
                                             contextual<ContextToken::In> + dsl::recurse<expression> + dsl::recurse<block>;
        };

        struct return_stmt
        {
            static constexpr auto rule = token<TokenKind::KwReturn> >>
                                         dsl::if_(dsl::peek(expression_start) >> dsl::recurse<expression>);
        };

        struct assert_stmt
        { static constexpr auto rule = token<TokenKind::KwAssert> >> dsl::recurse<expression>; };

        struct assign_or_expression_stmt
        {
            static constexpr auto assignment = token_choice<TokenKind::Assign, TokenKind::PlusAssign, TokenKind::MinusAssign,
                                                            TokenKind::StarAssign, TokenKind::SlashAssign>;
            static constexpr auto rule       = dsl::peek(expression_start) >>
                                               dsl::recurse<expression> +
                                                   dsl::if_(assignment >> dsl::p<newlines> + dsl::recurse<expression>);
        };

        struct statement
        {
            static constexpr auto rule = dsl::p<local_decl> | dsl::p<state_decl> | dsl::p<inject_decl> | dsl::p<lifecycle_stmt> |
                                         dsl::p<when_stmt> | dsl::p<for_stmt> | dsl::p<return_stmt> | dsl::p<assert_stmt> |
                                         dsl::p<assign_or_expression_stmt>;
        };

        inline constexpr auto statement_start = token<TokenKind::KwLet> / token<TokenKind::KwVar> / token<TokenKind::KwState> /
                                                token<TokenKind::KwInject> / token<TokenKind::KwStart> / token<TokenKind::KwStop> /
                                                token<TokenKind::KwWhen> / token<TokenKind::KwFor> / token<TokenKind::KwReturn> /
                                                token<TokenKind::KwAssert> / expression_start;

        struct block_item
        {
            static constexpr auto item = dsl::peek(statement_start) >>
                                         dsl::p<statement> + (newline >> dsl::p<newlines> | dsl::peek(token<TokenKind::RBrace>));
            static constexpr auto recovery =
                dsl::recover(dsl::peek(newline + (statement_start / token<TokenKind::RBrace>)) >> newline);
            static constexpr auto rule = dsl::try_(item, recovery);
        };

        struct block
        {
            static constexpr auto rule = token<TokenKind::LBrace> >>
                                         dsl::p<newlines> + dsl::while_(dsl::p<block_item>) + token<TokenKind::RBrace>;
        };

        struct generic_parameter
        {
            static constexpr auto constant =
                token<TokenKind::KwConst> >> dsl::p<name> + token<TokenKind::Colon> + dsl::p<newlines> + dsl::p<type>;
            static constexpr auto rule = constant | dsl::p<name>;
        };

        struct generic_parameters
        {
            static constexpr auto rule = token<TokenKind::Less> >>
                                         dsl::p<newlines> +
                                             dsl::list(dsl::p<generic_parameter>, dsl::trailing_sep(dsl::p<comma_separator>)) +
                                             dsl::p<newlines> + token<TokenKind::Greater>;
        };

        struct parameter
        {
            static constexpr auto regular  = dsl::p<name> >>
                                             dsl::try_(token<TokenKind::Colon>) + dsl::p<newlines> + dsl::p<type> +
                                                 dsl::if_(token<TokenKind::Assign> >> dsl::p<newlines> + dsl::recurse<expression>);
            static constexpr auto constant = token<TokenKind::KwConst> >> regular;
            static constexpr auto rule     = constant | regular;
        };

        struct signature
        {
            static constexpr auto
                rule = token<TokenKind::LParen> >>
                       dsl::p<newlines> +
                           dsl::if_(dsl::list(dsl::p<parameter>, dsl::trailing_sep(dsl::p<comma_separator>))) + dsl::p<newlines> +
                           token<TokenKind::RParen> + dsl::if_(dsl::p<continued_operator<TokenKind::Arrow>> >> dsl::p<type>);
        };

        struct constraint;
        struct constraint_operand;

        struct constraint_set
        {
            static constexpr auto rule = token<TokenKind::LBrace> >>
                                         dsl::p<newlines> +
                                             dsl::list(dsl::peek(type_start / expression_start / token<TokenKind::LBrace>) >>
                                                           dsl::recurse<constraint_operand>,
                                                       dsl::trailing_sep(dsl::p<comma_separator>)) +
                                             dsl::p<newlines> + token<TokenKind::RBrace>;
        };

        struct constraint_call
        {
            static constexpr auto rule =
                dsl::peek(ordinary_name + dsl::if_(token<TokenKind::ColonColon> >> ordinary_name) + token<TokenKind::LParen>) >>
                dsl::p<qualified_name> + token<TokenKind::LParen> + dsl::p<newlines> +
                    dsl::if_(dsl::list(dsl::peek(type_start / expression_start / token<TokenKind::LBrace>) >>
                                           dsl::recurse<constraint_operand>,
                                       dsl::trailing_sep(dsl::p<comma_separator>))) +
                    dsl::p<newlines> + token<TokenKind::RParen>;
        };

        struct constraint_operand
        {
            static constexpr auto value_start = token<TokenKind::IntLiteral> / token<TokenKind::FloatLiteral> /
                                                token<TokenKind::StringLiteral> / token<TokenKind::TemporalLiteral> /
                                                token<TokenKind::KwTrue> / token<TokenKind::KwFalse> /
                                                token<TokenKind::KwNull> / token<TokenKind::Minus>;
            static constexpr auto rule =
                dsl::p<constraint_set> | scalar_type | dsl::p<tuple_type> | dsl::p<list_type> | dsl::p<set_type> |
                dsl::p<map_type> | dsl::p<rolling_type> | dsl::p<atomic_type> | dsl::p<constraint_call> |
                dsl::peek(ordinary_name + (token<TokenKind::Less> / token<TokenKind::ColonColon>)) >> dsl::p<named_type> |
                dsl::peek(value_start) >> dsl::recurse<size_expression> | dsl::p<name>;
        };

        struct constraint_term
        {
            static constexpr auto relation    = token<TokenKind::EqualEqual> >> dsl::p<constraint_operand> |
                                                contextual<ContextToken::In> >> dsl::p<constraint_operand> |
                                                token<TokenKind::KwIs> >> (token<TokenKind::KwStruct> | dsl::p<name>);
            static constexpr auto requirement = token<TokenKind::Arrow> >> dsl::p<newlines> + dsl::p<type>;
            static constexpr auto atom        = token<TokenKind::Bang> >> dsl::recurse<constraint_term> |
                                                token<TokenKind::LParen> >> dsl::p<newlines> + dsl::recurse<constraint> +
                                                                                dsl::p<newlines> + token<TokenKind::RParen> |
                                                dsl::p<constraint_operand>;
            static constexpr auto rule        = atom + dsl::if_(requirement | relation);
        };

        struct constraint_and
        {
            static constexpr auto rule =
                dsl::p<constraint_term> + dsl::while_(dsl::p<continued_operator<TokenKind::AndAnd>> >> dsl::p<constraint_term>);
        };

        struct constraint
        {
            static constexpr auto rule =
                dsl::p<constraint_and> + dsl::while_(dsl::p<continued_operator<TokenKind::OrOr>> >> dsl::p<constraint_and>);
        };

        struct requires_clause
        { static constexpr auto rule = token<TokenKind::KwRequires> >> dsl::p<newlines> + dsl::p<constraint>; };

        struct optional_requires_clause
        {
            static constexpr auto continued =
                dsl::peek(newline + token<TokenKind::KwRequires>) >> newline + dsl::p<newlines> + dsl::p<requires_clause>;
            static constexpr auto rule = dsl::if_(continued | dsl::p<requires_clause>);
        };

        struct function_decl
        {
            static constexpr auto visibility = token<TokenKind::KwExport> / token<TokenKind::KwImpl>;
            static constexpr auto body = dsl::p<continued_operator<TokenKind::FatArrow>> >> dsl::p<expression> | dsl::p<block>;
            static constexpr auto start =
                dsl::peek(token<TokenKind::KwExport> + token<TokenKind::KwFn>) | token<TokenKind::KwImpl> | token<TokenKind::KwFn>;
            static constexpr auto
                rule = dsl::peek(start) >>
                       dsl::opt(visibility) + token<TokenKind::KwFn> + dsl::p<name> + dsl::if_(dsl::p<generic_parameters>) +
                           dsl::p<signature> + dsl::p<optional_requires_clause> + (newline >> dsl::p<newlines> + body | body);
        };

        struct operator_decl
        {
            static constexpr auto body = dsl::p<continued_operator<TokenKind::FatArrow>> >> dsl::p<expression> | dsl::p<block>;
            static constexpr auto rule =
                token<TokenKind::KwOperator> >>
                dsl::p<name> +
                    dsl::if_(dsl::p<generic_parameters>) + dsl::p<signature> + dsl::p<optional_requires_clause> + dsl::if_(body);
        };

        struct struct_member
        {
            static constexpr auto rule =
                dsl::p<name> >>
                (token<TokenKind::Colon> >>
                     dsl::p<newlines> + dsl::p<type> + dsl::if_(token<TokenKind::Assign> >> dsl::p<newlines> + dsl::p<expression>) |
                 token<TokenKind::Assign> >> dsl::p<newlines> + dsl::p<expression>);
        };

        struct struct_body_item
        {
            static constexpr auto rule = dsl::peek(dsl::p<name>) >> dsl::p<struct_member> + (newline >> dsl::p<newlines> |
                                                                                             dsl::peek(token<TokenKind::RBrace>));
        };

        struct struct_decl
        {
            static constexpr auto modifiers = dsl::opt(token<TokenKind::KwExport>) + dsl::opt(token<TokenKind::KwAbstract>);
            static constexpr auto parents   = token<TokenKind::Colon> >>
                                              dsl::p<newlines> + dsl::list(dsl::peek(type_start) >> dsl::p<type>,
                                                                           dsl::trailing_sep(dsl::p<comma_separator>));
            static constexpr auto body      = token<TokenKind::LBrace> >>
                                              dsl::p<newlines> + dsl::while_(dsl::p<struct_body_item>) + token<TokenKind::RBrace>;
            static constexpr auto start =
                dsl::peek(token<TokenKind::KwExport> + dsl::opt(token<TokenKind::KwAbstract>) + token<TokenKind::KwStruct>) |
                dsl::peek(token<TokenKind::KwAbstract> + token<TokenKind::KwStruct>) | token<TokenKind::KwStruct>;
            static constexpr auto
                rule = dsl::peek(start) >>
                       modifiers + token<TokenKind::KwStruct> + dsl::p<name> +
                           dsl::if_(dsl::p<generic_parameters>) + dsl::if_(parents) + dsl::p<optional_requires_clause> +
                           (newline >> dsl::p<newlines> + body | body);
        };

        struct use_decl
        {
            static constexpr auto import_set = token<TokenKind::ColonColon> >>
                                               token<TokenKind::LBrace> + dsl::p<newlines> +
                                                   dsl::if_(dsl::list(dsl::p<name>, dsl::trailing_sep(dsl::p<comma_separator>))) +
                                                   dsl::p<newlines> + token<TokenKind::RBrace>;
            static constexpr auto alias      = token<TokenKind::KwAs> >> dsl::p<name>;
            static constexpr auto rule       = token<TokenKind::KwUse> >> dsl::p<module_path> + (import_set | alias);
        };

        struct test_decl
        { static constexpr auto rule = token<TokenKind::KwTest> >> dsl::p<name> + dsl::p<block>; };

        struct declaration
        {
            static constexpr auto rule =
                dsl::p<use_decl> | dsl::p<function_decl> | dsl::p<operator_decl> | dsl::p<struct_decl> | dsl::p<test_decl>;
        };

        inline constexpr auto declaration_start =
            token<TokenKind::KwUse> / token<TokenKind::KwFn> / token<TokenKind::KwOperator> / token<TokenKind::KwStruct> /
            token<TokenKind::KwTest> / token<TokenKind::KwExport> / token<TokenKind::KwImpl> / token<TokenKind::KwAbstract>;

        struct declaration_line
        {
            static constexpr auto line     = dsl::p<declaration> >> dsl::p<line_end>;
            static constexpr auto recovery = dsl::recover(dsl::peek(newline + declaration_start) >> newline);
            static constexpr auto rule     = dsl::try_(line, recovery);
        };

        struct invalid_declaration_line
        { static constexpr auto rule = dsl::peek_not(dsl::eof) >> dsl::until(newline).or_eof(); };

        struct module_decl
        { static constexpr auto rule = token<TokenKind::KwModule> >> dsl::p<module_path>; };

        struct module
        {
            static constexpr auto rule = dsl::p<newlines> + dsl::try_(dsl::p<module_decl> + dsl::p<line_end>) +
                                         dsl::while_(dsl::p<declaration_line> | dsl::p<invalid_declaration_line>) + dsl::eof;
        };
    }  // namespace grammar

    namespace
    {
        [[nodiscard]] bool looks_like_applied_constructor(std::span<const Token> tokens, std::size_t position) noexcept {
            if (tokens[position].kind != TokenKind::Identifier) { return false; }
            std::size_t cursor = position + 1;
            if (cursor < tokens.size() && tokens[cursor].kind == TokenKind::ColonColon) {
                cursor += 2;
                if (cursor > tokens.size() || tokens[cursor - 1].kind != TokenKind::Identifier) { return false; }
            }
            if (cursor >= tokens.size() || tokens[cursor].kind != TokenKind::Less) { return false; }

            int depth = 0;
            for (; cursor < tokens.size(); ++cursor) {
                if (tokens[cursor].kind == TokenKind::Less) {
                    ++depth;
                } else if (tokens[cursor].kind == TokenKind::Greater) {
                    --depth;
                    if (depth == 0) { return cursor + 1 < tokens.size() && tokens[cursor + 1].kind == TokenKind::LParen; }
                } else if (tokens[cursor].kind == TokenKind::EndOfFile) {
                    return false;
                }
            }
            return false;
        }

        [[nodiscard]] std::uint8_t encode(std::span<const Token> tokens, std::size_t position) noexcept {
            const Token &token = tokens[position];
            if (token.kind == TokenKind::Identifier && token.text == "delta" && position + 1 < tokens.size() &&
                tokens[position + 1].kind == TokenKind::Less) {
                return static_cast<std::uint8_t>(grammar::ContextToken::Delta);
            }
            if (looks_like_applied_constructor(tokens, position)) {
                return static_cast<std::uint8_t>(grammar::ContextToken::AppliedConstructor);
            }
            if (token.kind == TokenKind::Identifier) {
                if (token.text == "in") { return static_cast<std::uint8_t>(grammar::ContextToken::In); }
                if (token.text == "atomic") { return static_cast<std::uint8_t>(grammar::ContextToken::Atomic); }
                if (token.text == "tuple") { return static_cast<std::uint8_t>(grammar::ContextToken::Tuple); }
                if (token.text == "list") { return static_cast<std::uint8_t>(grammar::ContextToken::List); }
                if (token.text == "set") { return static_cast<std::uint8_t>(grammar::ContextToken::Set); }
                if (token.text == "map") { return static_cast<std::uint8_t>(grammar::ContextToken::Map); }
                if (token.text == "rolling") { return static_cast<std::uint8_t>(grammar::ContextToken::Rolling); }
                if (token.text == "unbounded") { return static_cast<std::uint8_t>(grammar::ContextToken::Unbounded); }
            }
            return static_cast<std::uint8_t>(token.kind);
        }

        [[nodiscard]] std::optional<TokenKind> decode_expected(std::uint8_t encoded) noexcept {
            if (encoded <= static_cast<std::uint8_t>(TokenKind::Error)) { return static_cast<TokenKind>(encoded); }
            if (encoded >= static_cast<std::uint8_t>(grammar::ContextToken::In) &&
                encoded <= static_cast<std::uint8_t>(grammar::ContextToken::AppliedConstructor)) {
                return TokenKind::Identifier;
            }
            return std::nullopt;
        }

        [[nodiscard]] SyntaxKind production_kind(std::string_view name) noexcept {
            if (const std::size_t grammar = name.find("grammar::"); grammar != std::string_view::npos) {
                name.remove_prefix(grammar + std::string_view{"grammar::"}.size());
            } else if (const std::size_t scope = name.rfind("::"); scope != std::string_view::npos) {
                name.remove_prefix(scope + 2);
            }
            if (const std::size_t arguments = name.find('<'); arguments != std::string_view::npos) {
                name = name.substr(0, arguments);
            }
            return syntax_kind_from_name(name);
        }

        [[nodiscard]] std::uint32_t source_offset(std::span<const Token> tokens, std::size_t token_index,
                                                  std::uint32_t source_size) noexcept {
            if (token_index >= tokens.size() || tokens[token_index].kind == TokenKind::EndOfFile) { return source_size; }
            return tokens[token_index].range.begin;
        }

        [[nodiscard]] SourceRange source_range(std::span<const Token> tokens, std::size_t begin_token, std::size_t end_token,
                                               std::uint32_t source_size) noexcept {
            const std::uint32_t begin = source_offset(tokens, begin_token, source_size);
            if (tokens.empty() || begin_token >= end_token) { return {begin, begin}; }
            const std::size_t last = std::min(end_token - 1, tokens.size() - 1);
            if (tokens[last].kind == TokenKind::EndOfFile) { return {begin, source_size}; }
            return {begin, tokens[last].range.end};
        }

        struct RawIssue
        {
            std::size_t              token_index{0};
            std::optional<TokenKind> expected{};
            SyntaxKind               context{SyntaxKind::Unknown};
        };

        template <typename ParseTree>
        void materialize_parse_tree(SyntaxTree &syntax, const ParseTree &parsed, const std::string &input_bytes,
                                    std::span<const Token> source_tokens) {
            if (parsed.empty()) { return; }

            const auto               *input_begin = reinterpret_cast<const unsigned char *>(input_bytes.data());
            std::vector<SyntaxNodeId> parents;
            for (const auto event : parsed.traverse()) {
                if (event.event == lexy::traverse_event::enter) {
                    const auto lexeme = event.node.covering_lexeme();
                    const auto begin  = static_cast<std::size_t>(lexeme.begin() - input_begin);
                    const auto end    = static_cast<std::size_t>(lexeme.end() - input_begin);
                    SyntaxNode node;
                    node.kind   = production_kind(event.node.kind().name());
                    node.range  = source_range(source_tokens, begin, end, syntax.source_size);
                    node.parent = parents.empty() ? no_syntax_node : parents.back();

                    const SyntaxNodeId id = static_cast<SyntaxNodeId>(syntax.nodes.size());
                    syntax.nodes.push_back(std::move(node));
                    if (parents.empty()) {
                        syntax.root            = id;
                        syntax.nodes[id].range = {0, syntax.source_size};
                    } else {
                        syntax.nodes[parents.back()].children.push_back(SyntaxChild{SyntaxChildKind::Node, id});
                    }
                    parents.push_back(id);
                } else if (event.event == lexy::traverse_event::exit) {
                    parents.pop_back();
                } else {
                    const auto lexeme     = event.node.lexeme();
                    const auto begin      = static_cast<std::size_t>(lexeme.begin() - input_begin);
                    const auto end        = static_cast<std::size_t>(lexeme.end() - input_begin);
                    const bool unexpected = event.node.kind() == lexy::error_token_kind;
                    for (std::size_t token_index = begin; token_index < end && token_index < source_tokens.size(); ++token_index) {
                        if (source_tokens[token_index].kind == TokenKind::EndOfFile) { continue; }
                        const SyntaxTokenId id = static_cast<SyntaxTokenId>(syntax.tokens.size());
                        syntax.tokens.push_back(SyntaxToken{source_tokens[token_index].kind, source_tokens[token_index].range,
                                                            token_index, unexpected});
                        if (!parents.empty()) {
                            syntax.nodes[parents.back()].children.push_back(SyntaxChild{SyntaxChildKind::Token, id});
                        }
                    }
                }
            }
        }

        [[nodiscard]] SyntaxParseResult parse_impl(std::span<const Token> tokens, std::span<const SourceFragment> fragments,
                                                   std::uint32_t source_size) {
            std::string input_bytes;
            input_bytes.reserve(tokens.size());
            for (std::size_t position = 0; position < tokens.size(); ++position) {
                if (tokens[position].kind != TokenKind::EndOfFile) {
                    input_bytes.push_back(static_cast<char>(encode(tokens, position)));
                }
            }

            const auto                            input = lexy::string_input<lexy::byte_encoding>{input_bytes};
            lexy::parse_tree_for<decltype(input)> parsed;
            std::vector<RawIssue>                 raw_issues;
            std::size_t                           first_error    = std::numeric_limits<std::size_t>::max();
            const auto                            error_callback = lexy::callback([&](const auto &context, const auto &error) {
                const auto *begin    = reinterpret_cast<const unsigned char *>(input_bytes.data());
                const auto  position = static_cast<std::size_t>(error.position() - begin);
                if (first_error == std::numeric_limits<std::size_t>::max()) { first_error = position; }

                RawIssue issue;
                issue.token_index = position;
                issue.context     = production_kind(context.production());
                if constexpr (requires { error.character(); }) {
                    issue.expected = decode_expected(static_cast<std::uint8_t>(error.character()));
                }
                raw_issues.push_back(issue);
            });
            const auto                            result = lexy::parse_as_tree<grammar::module>(parsed, input, error_callback);

            SyntaxParseResult output;
            output.grammar          = GrammarResult{!result.is_fatal_error(), result.is_recovered_error(), result.error_count(),
                                                    parsed.size(), first_error};
            output.tree.source_size = source_size;
            output.tree.fragments.assign(fragments.begin(), fragments.end());
            materialize_parse_tree(output.tree, parsed, input_bytes, tokens);

            for (const RawIssue &raw : raw_issues) {
                const std::uint32_t offset = source_offset(tokens, raw.token_index, source_size);
                SyntaxIssue         issue;
                issue.expected = raw.expected;
                issue.context  = raw.context;
                if (raw.expected.has_value()) {
                    issue.kind  = SyntaxIssueKind::Missing;
                    issue.range = {offset, offset};
                } else {
                    issue.kind  = SyntaxIssueKind::Unexpected;
                    issue.range = raw.token_index < tokens.size() ? tokens[raw.token_index].range : SourceRange{offset, offset};
                }
                output.tree.issues.push_back(issue);
            }
            for (const SyntaxNode &node : output.tree.nodes) {
                if (node.kind != SyntaxKind::InvalidDeclarationLine) { continue; }
                output.tree.issues.push_back(
                    SyntaxIssue{SyntaxIssueKind::Unexpected, node.range, std::nullopt, SyntaxKind::Module});
                output.grammar.recovered = true;
                ++output.grammar.error_count;
                for (std::size_t index = 0; index < tokens.size(); ++index) {
                    if (tokens[index].range.begin < node.range.begin) { continue; }
                    output.grammar.first_error_token = std::min(output.grammar.first_error_token, index);
                    break;
                }
            }
            std::stable_sort(
                output.tree.issues.begin(), output.tree.issues.end(),
                [](const SyntaxIssue &left, const SyntaxIssue &right) { return left.range.begin < right.range.begin; });
            return output;
        }
    }  // namespace

    GrammarResult parse_token_grammar(std::span<const Token> tokens) {
        const std::uint32_t source_size = tokens.empty() ? 0 : tokens.back().range.end;
        return parse_impl(tokens, {}, source_size).grammar;
    }

    SyntaxParseResult parse_source_syntax(const SourceFile &file, const LexResult &lexed) {
        return parse_impl(lexed.tokens, lexed.fragments, static_cast<std::uint32_t>(file.text().size()));
    }
}  // namespace hgl::syntax
