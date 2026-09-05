#ifndef HGL_SYNTAX_AST_H
#define HGL_SYNTAX_AST_H

#include "syntax/source.h"
#include "syntax/temporal.h"

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

/// The syntax tree of one compilation unit (developer guide, "AST
/// requirements"). Nodes live in per-kind arenas owned by `Module` and refer
/// to each other by index, so the tree is stable, copyable, and free of
/// ownership pointers. Every node keeps its half-open source range.
namespace hgl::syntax::ast
{
    using NodeId = std::uint32_t;
    inline constexpr NodeId no_node = std::numeric_limits<NodeId>::max();

    // Strongly named indices into the arenas; all are `NodeId`s.
    using TypeId  = NodeId;
    using ExprId  = NodeId;
    using StmtId  = NodeId;
    using BlockId = NodeId;
    using DeclId  = NodeId;
    using ConstraintId = NodeId;

    /// An identifier occurrence with its own range (for name diagnostics).
    struct Name
    {
        std::string_view text{};
        SourceRange      range{};

        [[nodiscard]] bool empty() const noexcept { return text.empty(); }
    };

    // ---------------------------------------------------------------- types

    enum class ScalarType : std::uint8_t
    {
        Bool,
        I64,
        F64,
        Str,
        Date,
        Time,
        DateTime,
        Duration,
        CivilDateTime,
        ZonedDateTime,
        ZonedTime,
        TimeZone,
    };

    [[nodiscard]] std::string_view scalar_type_name(ScalarType type) noexcept;

    enum class TypeKind : std::uint8_t
    {
        Scalar,   ///< `scalar`
        Named,    ///< `name`: a generic parameter or declared type
        Tuple,    ///< `children`
        List,     ///< `children[0]`, `size` (no_node = unsized) or `unbounded`
        Set,      ///< `children[0]`
        Map,      ///< `children[0]` key, `children[1]` value
        Rolling,  ///< `children[0]`, `size` max, `min_size` (no_node = omitted)
        Atomic,   ///< `children[0]`
    };

    /// One argument of an applied nominal type. A bare identifier is kept
    /// ambiguous until name resolution sees the referenced declaration's
    /// type/const parameter at this position.
    struct GenericArgument
    {
        SourceRange range{};
        TypeId      type{no_node};
        ExprId      value{no_node};
        Name        name{};
    };

    struct Type
    {
        TypeKind                     kind{TypeKind::Scalar};
        SourceRange                  range{};
        ScalarType                   scalar{ScalarType::Bool};
        Name                         qualifier{};
        Name                         name{};
        std::vector<GenericArgument> arguments{};
        std::vector<TypeId>          children{};
        ExprId                       size{no_node};
        ExprId                       min_size{no_node};
        bool                         unbounded{false};
        /// True when the type was written in a value-type position
        /// (`const`, generic, `set<>` element, map key, rolling element).
        bool value_position{false};
    };

    // ---------------------------------------------------------- expressions

    enum class UnaryOp : std::uint8_t
    {
        Negate,  ///< `-`
        Not,     ///< `!`
    };

    enum class BinaryOp : std::uint8_t
    {
        Mul,
        Div,
        Rem,
        Add,
        Sub,
        Less,
        LessEqual,
        Greater,
        GreaterEqual,
        Equal,
        NotEqual,
        And,
        Or,
    };

    [[nodiscard]] std::string_view unary_op_spelling(UnaryOp op) noexcept;
    [[nodiscard]] std::string_view binary_op_spelling(BinaryOp op) noexcept;

    struct IntLiteral
    {
        std::int64_t value{0};
    };
    struct FloatLiteral
    {
        double value{0.0};
    };
    struct StringLiteral
    {
        std::string value{};
    };
    struct BoolLiteral
    {
        bool value{false};
    };
    struct NullLiteral
    {
    };
    struct TemporalLiteral
    {
        TemporalValue value{};
    };
    /// A lone `_` (harness sequences only).
    struct Placeholder
    {
    };
    /// A plain identifier reference.
    struct NameRef
    {
        Name name{};
    };
    /// `alias::name`.
    struct QualifiedRef
    {
        Name qualifier{};
        Name name{};
    };
    struct Unary
    {
        UnaryOp op{UnaryOp::Negate};
        ExprId  operand{no_node};
    };
    struct Binary
    {
        BinaryOp op{BinaryOp::Add};
        ExprId   lhs{no_node};
        ExprId   rhs{no_node};
    };
    struct Argument
    {
        Name   name{};  ///< empty for a positional argument
        ExprId value{no_node};
    };
    struct Call
    {
        ExprId                callee{no_node};
        std::vector<Argument> arguments{};
    };
    struct Index
    {
        ExprId target{no_node};
        ExprId index{no_node};
    };
    struct Field
    {
        ExprId target{no_node};
        Name   field{};
    };
    struct SequenceElement
    {
        ExprId key{no_node};  ///< timed element: the duration or datetime literal
        ExprId value{no_node};
    };
    /// `[a, b]` or `[0s: a, 2m: b]`.
    struct SequenceLiteral
    {
        std::vector<SequenceElement> elements{};
    };
    /// `(a, b)` and `(a,)`.
    struct TupleLiteral
    {
        std::vector<ExprId> elements{};
    };
    struct AnonymousParameter
    {
        Name   name{};
        TypeId type{no_node};  ///< no_node = context-inferred
    };
    /// `fn(a, b: f64) -> T => expr`.
    struct AnonymousFn
    {
        std::vector<AnonymousParameter> parameters{};
        TypeId                          result{no_node};
        ExprId                          body{no_node};
    };
    /// `if c { } else { }` / `else if`; `otherwise` is a Block or If expr, or no_node.
    struct If
    {
        ExprId  condition{no_node};
        BlockId then_block{no_node};
        ExprId  otherwise{no_node};
    };
    /// A block used as an expression (the `else` arm, or a bare block).
    struct BlockExpr
    {
        BlockId block{no_node};
    };
    /// `eval(callee, args...)`.
    struct Eval
    {
        ExprId                callee{no_node};
        std::vector<Argument> arguments{};
    };

    /// A nominal struct constructor. The un-applied `Quote(...)` form stays
    /// an ordinary Call and becomes a constructor through name resolution;
    /// this node preserves the explicit `Box<f64>(...)` and `delta<S>(...)`
    /// type application without opening generic function-call syntax.
    struct Construct
    {
        TypeId                type{no_node};
        std::vector<Argument> arguments{};
        bool                  delta{false};
    };

    using ExprNode = std::variant<IntLiteral, FloatLiteral, StringLiteral, BoolLiteral, NullLiteral, TemporalLiteral, Placeholder,
                                  NameRef, QualifiedRef, Unary, Binary, Call, Index, Field, SequenceLiteral, TupleLiteral,
                                  AnonymousFn, If, BlockExpr, Eval, Construct>;

    struct Expr
    {
        SourceRange range{};
        ExprNode    node{};
    };

    // ----------------------------------------------------------- statements

    enum class AssignOp : std::uint8_t
    {
        Assign,
        Add,
        Sub,
        Mul,
        Div,
    };

    [[nodiscard]] std::string_view assign_op_spelling(AssignOp op) noexcept;

    /// `let` / `var`.
    struct LocalDecl
    {
        bool   mutable_{false};
        Name   name{};
        TypeId type{no_node};
        ExprId init{no_node};
    };
    struct StateDecl
    {
        Name   name{};
        TypeId type{no_node};
        ExprId init{no_node};
    };
    struct InjectDecl
    {
        std::vector<Name> names{};
    };
    struct LifecycleBlock
    {
        bool    is_stop{false};  ///< false = `start`
        BlockId block{no_node};
    };
    struct WhenStmt
    {
        ExprId  condition{no_node};
        BlockId block{no_node};
    };
    struct ForStmt
    {
        Name    first{};
        Name    second{};  ///< empty for a one-name pattern
        ExprId  iterable{no_node};
        BlockId block{no_node};
    };
    struct AssignStmt
    {
        AssignOp op{AssignOp::Assign};
        ExprId   place{no_node};
        ExprId   value{no_node};
    };
    struct ReturnStmt
    {
        ExprId value{no_node};  ///< no_node = bare `return`
    };
    struct AssertStmt
    {
        ExprId condition{no_node};
    };
    struct ExprStmt
    {
        ExprId expr{no_node};
    };

    using StmtNode = std::variant<LocalDecl, StateDecl, InjectDecl, LifecycleBlock, WhenStmt, ForStmt, AssignStmt,
                                  ReturnStmt, AssertStmt, ExprStmt>;

    struct Stmt
    {
        SourceRange range{};
        StmtNode    node{};
    };

    struct Block
    {
        SourceRange         range{};
        std::vector<StmtId> statements{};
        /// The tail expression: the block's last statement when it is an
        /// `ExprStmt`, else no_node. The statement stays in `statements`.
        ExprId tail{no_node};
    };

    // --------------------------------------------------------- declarations

    struct GenericParameter
    {
        Name   name{};
        bool   is_const{false};
        TypeId type{no_node};  ///< const generic: the declared value type
    };

    struct Parameter
    {
        Name   name{};
        bool   is_const{false};
        TypeId type{no_node};
        ExprId default_value{no_node};
    };

    struct Signature
    {
        std::vector<Parameter> parameters{};
        TypeId                 result{no_node};
    };

    // ---------------------------------------------------------- constraints

    enum class ConstraintLogicOp : std::uint8_t
    {
        And,
        Or,
    };

    enum class ConstraintRelationOp : std::uint8_t
    {
        Equal,
        In,
        Is,
    };

    /// An identifier whose type/value interpretation is determined by its
    /// declaration and its position in the constraint.
    struct ConstraintName
    {
        Name name{};
    };
    struct ConstraintType
    {
        TypeId type{no_node};
    };
    struct ConstraintValue
    {
        ExprId value{no_node};
    };
    struct ConstraintSet
    {
        std::vector<ConstraintId> elements{};
    };
    struct ConstraintCall
    {
        Name                      qualifier{};
        Name                      name{};
        std::vector<ConstraintId> arguments{};
    };
    struct OperatorRequirement
    {
        Name                      qualifier{};
        Name                      name{};
        std::vector<ConstraintId> arguments{};
        TypeId                    result{no_node};
    };
    struct ConstraintRelation
    {
        ConstraintRelationOp op{ConstraintRelationOp::Equal};
        ConstraintId         lhs{no_node};
        ConstraintId         rhs{no_node};
        Name                 category{};  ///< right side of `is`
    };
    struct ConstraintNot
    {
        ConstraintId operand{no_node};
    };
    struct ConstraintLogic
    {
        ConstraintLogicOp op{ConstraintLogicOp::And};
        ConstraintId      lhs{no_node};
        ConstraintId      rhs{no_node};
    };

    using ConstraintNode = std::variant<ConstraintName, ConstraintType, ConstraintValue, ConstraintSet, ConstraintCall,
                                        OperatorRequirement, ConstraintRelation, ConstraintNot, ConstraintLogic>;

    struct Constraint
    {
        SourceRange    range{};
        ConstraintNode node{};
    };

    struct ModuleDecl
    {
        std::vector<Name> path{};
    };

    struct UseDecl
    {
        std::vector<Name> path{};
        std::vector<Name> names{};  ///< selective import set
        Name              alias{};  ///< `as alias`
    };

    struct OperatorDecl
    {
        Name                          name{};
        std::vector<GenericParameter> generics{};
        Signature                     signature{};
        ConstraintId                  requirements{no_node};
    };

    enum class FunctionVisibility : std::uint8_t
    {
        Internal,
        Export,
        Impl,
    };

    struct FunctionDecl
    {
        FunctionVisibility            visibility{FunctionVisibility::Internal};
        Name                          name{};
        std::vector<GenericParameter> generics{};
        Signature                     signature{};
        ConstraintId                  requirements{no_node};
        ExprId                        concise_body{no_node};  ///< `=> expr`
        BlockId                       block_body{no_node};    ///< `{ ... }`
    };

    struct StructField
    {
        Name   name{};
        TypeId type{no_node};
        ExprId default_value{no_node};
    };

    struct InheritedDefault
    {
        Name   name{};
        ExprId value{no_node};
    };

    using StructMember = std::variant<StructField, InheritedDefault>;

    struct StructDecl
    {
        bool                          exported{false};
        bool                          abstract{false};
        Name                          name{};
        std::vector<GenericParameter> generics{};
        std::vector<TypeId>           parents{};
        ConstraintId                  requirements{no_node};
        std::vector<StructMember>     members{};
    };

    struct TestDecl
    {
        Name    name{};
        BlockId block{no_node};
    };

    using DeclNode = std::variant<ModuleDecl, UseDecl, StructDecl, OperatorDecl, FunctionDecl, TestDecl>;

    struct Decl
    {
        SourceRange range{};
        DeclNode    node{};
    };

    using Comment = SourceComment;

    /// One parsed source file. `declarations` are in source order; the
    /// module declaration is first when present.
    struct Module
    {
        std::vector<Type>    types{};
        std::vector<Expr>    exprs{};
        std::vector<Stmt>    stmts{};
        std::vector<Block>   blocks{};
        std::vector<Constraint> constraints{};
        std::vector<Decl>    decls{};
        std::vector<Comment> comments{};  ///< source trivia, in order

        std::vector<DeclId> declarations{};

        [[nodiscard]] const Type &type(TypeId id) const noexcept { return types[id]; }
        [[nodiscard]] const Expr &expr(ExprId id) const noexcept { return exprs[id]; }
        [[nodiscard]] const Stmt &stmt(StmtId id) const noexcept { return stmts[id]; }
        [[nodiscard]] const Block &block(BlockId id) const noexcept { return blocks[id]; }
        [[nodiscard]] const Constraint &constraint(ConstraintId id) const noexcept { return constraints[id]; }
        [[nodiscard]] const Decl &decl(DeclId id) const noexcept { return decls[id]; }

        TypeId add(Type node)
        {
            types.push_back(std::move(node));
            return static_cast<NodeId>(types.size() - 1);
        }
        ExprId add(Expr node)
        {
            exprs.push_back(std::move(node));
            return static_cast<NodeId>(exprs.size() - 1);
        }
        StmtId add(Stmt node)
        {
            stmts.push_back(std::move(node));
            return static_cast<NodeId>(stmts.size() - 1);
        }
        BlockId add(Block node)
        {
            blocks.push_back(std::move(node));
            return static_cast<NodeId>(blocks.size() - 1);
        }
        ConstraintId add(Constraint node)
        {
            constraints.push_back(std::move(node));
            return static_cast<NodeId>(constraints.size() - 1);
        }
        DeclId add(Decl node)
        {
            decls.push_back(std::move(node));
            return static_cast<NodeId>(decls.size() - 1);
        }
    };
}  // namespace hgl::syntax::ast

#endif  // HGL_SYNTAX_AST_H
