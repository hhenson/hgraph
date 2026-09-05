#ifndef HGL_SYNTAX_SYNTAX_TREE_H
#define HGL_SYNTAX_SYNTAX_TREE_H

#include "syntax/source.h"
#include "syntax/token.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgl::syntax
{
    /// Parser-library-independent production kinds. Helper productions remain
    /// visible because the source tree is also a debugging and tooling
    /// contract; semantic passes consume the typed AST/HIR projection instead.
    enum class SyntaxKind : std::uint8_t {
        Unknown,
        Newlines,
        LineEnd,
        CommaSeparator,
        ModulePath,
        QualifiedName,
        GenericArguments,
        GenericArgument,
        NamedType,
        TupleType,
        ListType,
        SetType,
        MapType,
        RollingType,
        AtomicType,
        Type,
        SizeExpression,
        ContinuedOperator,
        ProductExpression,
        SumExpression,
        ComparisonExpression,
        EqualityExpression,
        AndExpression,
        Expression,
        Argument,
        Arguments,
        IndexPostfix,
        CallPostfix,
        FieldPostfix,
        Postfix,
        ExplicitConstruct,
        PrimaryExpression,
        PostfixExpression,
        UnaryExpression,
        TupleElement,
        TupleOrGroup,
        SequenceElement,
        SequenceLiteral,
        AnonymousParameter,
        AnonymousFunction,
        ElseArm,
        IfExpression,
        EvalExpression,
        LocalDecl,
        StateDecl,
        InjectDecl,
        LifecycleStmt,
        WhenStmt,
        ForStmt,
        ReturnStmt,
        AssertStmt,
        AssignOrExpressionStmt,
        Statement,
        BlockItem,
        Block,
        GenericParameter,
        GenericParameters,
        Parameter,
        Signature,
        ConstraintSet,
        ConstraintCall,
        ConstraintOperand,
        ConstraintTerm,
        ConstraintAnd,
        Constraint,
        RequiresClause,
        OptionalRequiresClause,
        FunctionDecl,
        OperatorDecl,
        StructMember,
        StructBodyItem,
        StructDecl,
        UseDecl,
        TestDecl,
        Declaration,
        DeclarationLine,
        ModuleDecl,
        Module,
    };

    [[nodiscard]] std::string_view syntax_kind_name(SyntaxKind kind) noexcept;
    [[nodiscard]] SyntaxKind       syntax_kind_from_name(std::string_view name) noexcept;

    using SyntaxNodeId                           = std::uint32_t;
    using SyntaxTokenId                          = std::uint32_t;
    inline constexpr SyntaxNodeId no_syntax_node = std::numeric_limits<SyntaxNodeId>::max();

    enum class SyntaxChildKind : std::uint8_t {
        Node,
        Token,
    };

    struct SyntaxChild
    {
        SyntaxChildKind kind{SyntaxChildKind::Node};
        std::uint32_t   index{0};
    };

    struct SyntaxNode
    {
        SyntaxKind               kind{SyntaxKind::Unknown};
        SourceRange              range{};
        SyntaxNodeId             parent{no_syntax_node};
        std::vector<SyntaxChild> children{};
    };

    struct SyntaxToken
    {
        TokenKind   kind{TokenKind::Error};
        SourceRange range{};
        std::size_t source_token_index{no_token_index};
        bool        unexpected{false};
    };

    enum class SyntaxIssueKind : std::uint8_t {
        Missing,
        Unexpected,
    };

    /// Malformed syntax is data as well as a diagnostic. Missing ranges are
    /// zero-width and never manufacture source text; unexpected ranges point
    /// at the original token(s) retained by the lexical fragments.
    struct SyntaxIssue
    {
        SyntaxIssueKind          kind{SyntaxIssueKind::Unexpected};
        SourceRange              range{};
        std::optional<TokenKind> expected{};
        SyntaxKind               context{SyntaxKind::Unknown};
    };

    /// Source-accurate syntax owned entirely by HGL. No parser-library type is
    /// present in this contract.
    struct SyntaxTree
    {
        std::uint32_t               source_size{0};
        SyntaxNodeId                root{no_syntax_node};
        std::vector<SyntaxNode>     nodes{};
        std::vector<SyntaxToken>    tokens{};
        std::vector<SourceFragment> fragments{};
        std::vector<SyntaxIssue>    issues{};

        [[nodiscard]] bool        has_root() const noexcept { return root != no_syntax_node; }
        [[nodiscard]] bool        source_is_complete() const noexcept;
        [[nodiscard]] std::string reconstruct(const SourceFile &file) const;
    };
}  // namespace hgl::syntax

#endif  // HGL_SYNTAX_SYNTAX_TREE_H
