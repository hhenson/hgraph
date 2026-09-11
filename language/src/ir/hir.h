#ifndef HGL_IR_HIR_H
#define HGL_IR_HIR_H

#include "syntax/source.h"
#include "syntax/temporal.h"

#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

/// Backend-independent high-level IR. Unlike the syntax AST, every name in
/// this representation denotes a SymbolId. The representation deliberately
/// retains structured HGL control flow; graph/runtime lowering happens in the
/// next IR rather than in a backend.
namespace hgl::ir::hir
{
    template <typename Tag> struct Id
    {
        std::uint32_t value{std::numeric_limits<std::uint32_t>::max()};

        [[nodiscard]] constexpr bool valid() const noexcept { return value != std::numeric_limits<std::uint32_t>::max(); }
        friend constexpr bool        operator==(Id, Id) noexcept = default;
    };

    using DeclarationId = Id<struct DeclarationTag>;
    using SymbolId      = Id<struct SymbolTag>;
    using TypeId        = Id<struct TypeTag>;
    using ExprId        = Id<struct ExprTag>;
    using StmtId        = Id<struct StmtTag>;
    using BlockId       = Id<struct BlockTag>;
    using ConstraintId  = Id<struct ConstraintTag>;

    inline constexpr DeclarationId no_declaration{};
    inline constexpr SymbolId      no_symbol{};
    inline constexpr TypeId        no_type{};
    inline constexpr ExprId        no_expr{};
    inline constexpr StmtId        no_stmt{};
    inline constexpr BlockId       no_block{};
    inline constexpr ConstraintId  no_constraint{};

    enum class Completion : std::uint8_t {
        Resolved,  ///< names and source type patterns are complete
        Typed,     ///< expression types, phases, and substitutions are complete
    };

    enum class ScalarType : std::uint8_t {
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

    enum class SymbolKind : std::uint8_t {
        Module,
        Struct,
        Operator,
        Function,
        Test,
        TypeParameter,
        ConstParameter,
        SignalParameter,
        LocalLet,
        LocalVar,
        State,
        InjectedCapability,
        LoopValue,
        LambdaParameter,
        ImportedFunction,
        ImportedOperator,
        Intrinsic,
    };

    struct Symbol
    {
        SymbolKind  kind{SymbolKind::Module};
        std::string name{};
        /// Native registry key for an imported operator or intrinsic.
        std::string external_name{};
        /// Stable defining-module identity, independent of registry spelling.
        std::string         canonical_name{};
        DeclarationId       owner{};
        syntax::SourceRange range{};
        TypeId              type{};
        std::uint32_t       index{0};
    };

    enum class TypeKind : std::uint8_t {
        Void,
        Scalar,
        Symbol,
        Tuple,
        List,
        Set,
        Map,
        Rolling,
        Atomic,
        Reference,
        Signal,
        Iterator,
        Callable,
        Capability,
        HarnessSequence,
        Deferred,
    };

    enum class TypeArgumentKind : std::uint8_t {
        Type,
        Value,
    };

    struct TypeArgument
    {
        TypeArgumentKind    kind{TypeArgumentKind::Type};
        TypeId              type{};
        ExprId              value{};
        syntax::SourceRange range{};
        /// Retain the corresponding implementation generic rather than
        /// binding it. Used only by explicit materialization requests.
        bool retained{false};
    };

    struct Type
    {
        TypeKind            kind{TypeKind::Deferred};
        syntax::SourceRange range{};
        /// Declaration containing this source occurrence. Canonical types are
        /// context-neutral and leave this empty.
        DeclarationId             owner{};
        ScalarType                scalar{ScalarType::Bool};
        SymbolId                  symbol{};
        std::vector<TypeArgument> arguments{};
        std::vector<TypeId>       children{};
        ExprId                    size{};
        ExprId                    min_size{};
        bool                      unbounded{false};
        bool                      value_position{false};
        /// Structural representative, independent of source spelling and
        /// value/temporal use. Populated by type completion.
        TypeId canonical{};
    };

    enum class Phase : std::uint8_t {
        Unknown,
        Constant,
        Wiring,
        Runtime,
    };

    enum class ValueKind : std::uint8_t {
        Unknown,
        Void,
        Constant,
        Signal,
        RuntimeValue,
        Function,
        Operator,
        Type,
        Iterator,
    };

    /// Observable work performed by an HGL expression, statement, block, or
    /// function. These flags describe language semantics; they are not a
    /// backend execution plan.
    enum class Effect : std::uint32_t {
        None              = 0,
        WireGraph         = 1U << 0U,
        ReadRuntimeInput  = 1U << 1U,
        ReadState         = 1U << 2U,
        WriteLocal        = 1U << 3U,
        WriteState        = 1U << 4U,
        WriteOutput       = 1U << 5U,
        UseCapability     = 1U << 6U,
        IterateCollection = 1U << 7U,
        TestHarness       = 1U << 8U,
    };

    [[nodiscard]] constexpr Effect operator|(Effect lhs, Effect rhs) noexcept {
        return static_cast<Effect>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
    }

    constexpr Effect &operator|=(Effect &lhs, Effect rhs) noexcept {
        lhs = lhs | rhs;
        return lhs;
    }

    [[nodiscard]] constexpr bool has_effect(Effect effects, Effect test) noexcept {
        return (static_cast<std::uint32_t>(effects) & static_cast<std::uint32_t>(test)) != 0U;
    }

    struct NullValue
    { friend constexpr bool operator==(NullValue, NullValue) noexcept = default; };
    struct PlaceholderValue
    { friend constexpr bool operator==(PlaceholderValue, PlaceholderValue) noexcept = default; };
    using Constant = std::variant<NullValue, PlaceholderValue, bool, std::int64_t, double, std::string, syntax::TemporalValue>;

    struct Substitution
    {
        SymbolId                parameter{};
        std::string             name{};
        TypeId                  type{};
        ExprId                  value{};
        std::optional<Constant> constant{};
        /// The parameter remains a resolver variable in this candidate.
        bool retained{false};
    };

    enum class OperationKind : std::uint8_t {
        None,
        ExactFunction,
        NominalOperator,
        Intrinsic,
        Constructor,
        Capability,
        Index,
        Field,
        HarnessEval,
    };

    /// Semantic identity assigned to an expression operation. Operator
    /// implementations are named by stable symbols/strings and copied
    /// candidate/provider identities; no registry pointer or wiring object
    /// enters HIR.
    struct Operation
    {
        OperationKind             kind{OperationKind::None};
        SymbolId                  target{};
        SymbolId                  candidate{};
        std::string               identity{};
        std::string               candidate_label{};
        std::string               provider_key{};
        std::vector<Substitution> substitutions{};
        bool                      deferred{false};
    };

    enum class UnaryOp : std::uint8_t {
        Negate,
        Not,
    };

    enum class BinaryOp : std::uint8_t {
        Mul,
        Div,
        FloorDiv,
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

    [[nodiscard]] std::string_view binary_op_spelling(BinaryOp op) noexcept;
    /// Fixed system identities: symbol resolution never performs local-name lookup.
    [[nodiscard]] std::string_view system_operator_name(BinaryOp op) noexcept;
    [[nodiscard]] std::string_view system_operator_name(UnaryOp op) noexcept;

    struct Literal
    { Constant value{}; };
    struct SymbolRef
    { SymbolId symbol{}; };
    struct Unary
    {
        UnaryOp op{UnaryOp::Negate};
        ExprId  operand{};
    };
    struct Binary
    {
        BinaryOp op{BinaryOp::Add};
        ExprId   lhs{};
        ExprId   rhs{};
    };
    struct Argument
    {
        std::string         name{};
        ExprId              value{};
        syntax::SourceRange range{};
    };
    struct Call
    {
        ExprId                callee{};
        std::vector<Argument> arguments{};
    };
    struct Index
    {
        ExprId target{};
        ExprId index{};
    };
    struct Field
    {
        ExprId              target{};
        std::string         name{};
        syntax::SourceRange name_range{};
    };
    struct SequenceElement
    {
        ExprId key{};
        ExprId value{};
    };
    struct Sequence
    { std::vector<SequenceElement> elements{}; };
    struct Tuple
    { std::vector<ExprId> elements{}; };
    struct Lambda
    {
        std::vector<SymbolId> parameters{};
        TypeId                result{};
        ExprId                body{};
    };
    struct If
    {
        ExprId  condition{};
        BlockId then_block{};
        ExprId  otherwise{};
    };
    struct BlockExpr
    { BlockId block{}; };
    struct Eval
    {
        ExprId                callee{};
        std::vector<Argument> arguments{};
    };
    struct Construct
    {
        TypeId                type{};
        std::vector<Argument> arguments{};
        bool                  delta{false};
    };

    using ExprNode = std::variant<Literal, SymbolRef, Unary, Binary, Call, Index, Field, Sequence, Tuple, Lambda, If, BlockExpr,
                                  Eval, Construct>;

    struct Expr
    {
        syntax::SourceRange     range{};
        TypeId                  type{};
        Phase                   phase{Phase::Unknown};
        ValueKind               value_kind{ValueKind::Unknown};
        ExprNode                node{};
        DeclarationId           owner{};
        Effect                  effects{Effect::None};
        std::optional<Constant> constant{};
        Operation               operation{};
    };

    enum class AssignOp : std::uint8_t {
        Assign,
        Add,
        Sub,
        Mul,
        Div,
    };

    struct LocalDecl
    {
        SymbolId symbol{};
        TypeId   type{};
        ExprId   init{};
    };
    struct StateDecl
    {
        SymbolId symbol{};
        TypeId   type{};
        ExprId   init{};
    };
    struct InjectDecl
    { std::vector<SymbolId> symbols{}; };
    struct LifecycleBlock
    {
        bool    is_stop{false};
        BlockId block{};
    };
    struct WhenStmt
    {
        ExprId  condition{};
        BlockId block{};
    };
    struct ForStmt
    {
        std::vector<SymbolId> bindings{};
        ExprId                iterable{};
        BlockId               block{};
    };
    struct AssignStmt
    {
        AssignOp op{AssignOp::Assign};
        ExprId   place{};
        ExprId   value{};
    };
    struct ReturnStmt
    { ExprId value{}; };
    struct AssertStmt
    { ExprId condition{}; };
    struct ExprStmt
    { ExprId expr{}; };

    using StmtNode = std::variant<LocalDecl, StateDecl, InjectDecl, LifecycleBlock, WhenStmt, ForStmt, AssignStmt, ReturnStmt,
                                  AssertStmt, ExprStmt>;

    struct Stmt
    {
        syntax::SourceRange range{};
        StmtNode            node{};
        DeclarationId       owner{};
        Effect              effects{Effect::None};
    };

    struct Block
    {
        syntax::SourceRange range{};
        std::vector<StmtId> statements{};
        ExprId              tail{};
        DeclarationId       owner{};
        Effect              effects{Effect::None};
    };

    enum class ConstraintLogicOp : std::uint8_t {
        And,
        Or,
    };
    enum class ConstraintRelationOp : std::uint8_t {
        Equal,
        In,
        Is,
    };
    struct ConstraintSymbol
    { SymbolId symbol{}; };
    struct ConstraintType
    { TypeId type{}; };
    struct ConstraintValue
    { ExprId value{}; };
    struct ConstraintSet
    { std::vector<ConstraintId> elements{}; };
    struct ConstraintCall
    {
        SymbolId                  function{};
        std::vector<ConstraintId> arguments{};
    };
    struct ConstraintEach
    {
        SymbolId     binding{};
        ConstraintId source{};
        ConstraintId body{};
    };
    struct OperatorRequirement
    {
        SymbolId                  op{};
        std::vector<ConstraintId> arguments{};
        TypeId                    result{};
    };
    struct ConstraintRelation
    {
        ConstraintRelationOp op{ConstraintRelationOp::Equal};
        ConstraintId         lhs{};
        ConstraintId         rhs{};
        std::string          category{};
    };
    struct ConstraintNot
    { ConstraintId operand{}; };
    struct ConstraintLogic
    {
        ConstraintLogicOp op{ConstraintLogicOp::And};
        ConstraintId      lhs{};
        ConstraintId      rhs{};
    };
    using ConstraintNode = std::variant<ConstraintSymbol, ConstraintType, ConstraintValue, ConstraintSet, ConstraintCall,
                                        ConstraintEach, OperatorRequirement, ConstraintRelation, ConstraintNot, ConstraintLogic>;
    struct Constraint
    {
        syntax::SourceRange range{};
        ConstraintNode      node{};
    };

    struct GenericParameter
    {
        SymbolId symbol{};
        bool     is_const{false};
        TypeId   type{};
        bool     is_pack{false};
    };
    enum class ParameterPack : std::uint8_t {
        None,
        Positional,
        Keyword,
    };
    struct PackCardinality
    {
        std::uint32_t                minimum{0};
        std::optional<std::uint32_t> maximum{};

        friend bool operator==(const PackCardinality &, const PackCardinality &) = default;
    };
    struct Parameter
    {
        SymbolId        symbol{};
        bool            is_const{false};
        TypeId          type{};
        ExprId          default_value{};
        ParameterPack   pack{ParameterPack::None};
        PackCardinality cardinality{};
    };
    struct Signature
    {
        std::vector<Parameter> parameters{};
        TypeId                 result{};
    };

    enum class NativePhase : std::uint8_t {
        Wiring,
        Start,
        Evaluation,
        Stop,
    };

    enum class NativeParameterAccess : std::uint8_t {
        Value,
        InputView,
    };

    struct NativeParameter
    {
        std::string           name{};
        TypeId                type{};
        bool                  is_const{false};
        NativeParameterAccess access{NativeParameterAccess::Value};
    };

    /// An exact, descriptor-provided native callable. This is copied into HIR
    /// so later passes never depend on descriptor storage or a loaded module.
    struct NativeFunction
    {
        SymbolId                      symbol{};
        SymbolId                      family{};
        std::string                   module_identity{};
        std::string                   identity{};
        std::string                   candidate_identity{};
        std::string                   cpp_symbol{};
        std::vector<GenericParameter> generics{};
        std::vector<NativeParameter>  parameters{};
        TypeId                        result{};
        std::vector<NativePhase>      phases{};
        std::vector<std::string>      public_headers{};
        std::vector<std::string>      cmake_packages{};
        std::vector<std::string>      imported_targets{};
        std::vector<std::string>      runtime_images{};
        std::string                   descriptor_fingerprint{};
        bool                          source_defined{false};
        std::string                   cpp_parameters{};
        std::string                   cpp_body{};
        syntax::SourceRange           range{};
    };
    struct StructField
    {
        std::string         name{};
        TypeId              type{};
        ExprId              default_value{};
        DeclarationId       origin{};
        bool                optional{false};
        syntax::SourceRange range{};
    };

    enum class Visibility : std::uint8_t {
        Internal,
        Export,
        Implementation,
    };
    enum class FunctionKind : std::uint8_t {
        Composition,
        Runtime,
    };

    struct ModuleDecl
    {};
    struct UseDecl
    {
        std::string              module{};
        std::string              alias{};
        std::vector<std::string> names{};
    };
    struct CppIncludeDecl
    { std::string spelling{}; };
    struct StructDecl
    {
        bool                          exported{false};
        bool                          abstract{false};
        std::vector<GenericParameter> generics{};
        std::vector<TypeId>           parents{};
        ConstraintId                  requirements{};
        std::vector<StructField>      fields{};
    };
    struct OperatorProperty
    {
        std::string         name{};
        ExprId              value{};
        syntax::SourceRange range{};
    };
    struct OperatorProperties
    {
        std::vector<TypeId>           domain{};
        std::vector<OperatorProperty> entries{};
        syntax::SourceRange           range{};
    };
    struct OperatorDecl
    {
        std::vector<GenericParameter>   generics{};
        Signature                       signature{};
        ConstraintId                    requirements{};
        std::vector<OperatorProperties> properties{};
    };
    struct Materialization
    {
        SymbolId                  implementation{};
        std::vector<Substitution> substitutions{};
        syntax::SourceRange       range{};
    };
    struct Instantiation
    {
        SymbolId                     operator_contract{};
        std::vector<TypeArgument>    arguments{};
        std::vector<Materialization> materializations{};
        syntax::SourceRange          range{};
    };
    struct InstantiateDecl
    { std::vector<Instantiation> entries{}; };
    struct FunctionDecl
    {
        Visibility   visibility{Visibility::Internal};
        FunctionKind kind{FunctionKind::Composition};
        /// The nominal operator implemented by an `impl fn`.
        SymbolId                      operator_contract{};
        std::vector<GenericParameter> generics{};
        Signature                     signature{};
        ConstraintId                  requirements{};
        ExprId                        concise_body{};
        BlockId                       block_body{};
        Effect                        effects{Effect::None};
        std::vector<SymbolId>         capabilities{};
    };
    struct NativeSourceDecl
    {};
    struct TestDecl
    { BlockId block{}; };
    using DeclarationNode = std::variant<ModuleDecl, UseDecl, CppIncludeDecl, StructDecl, OperatorDecl, InstantiateDecl,
                                         FunctionDecl, NativeSourceDecl, TestDecl>;
    struct Declaration
    {
        DeclarationId       id{};
        SymbolId            symbol{};
        syntax::SourceRange range{};
        DeclarationNode     node{};
    };

    struct Module
    {
        std::string             path{};
        Completion              completion{Completion::Resolved};
        std::vector<Symbol>     symbols{};
        std::vector<Type>       types{};
        // Type completion may synthesize constants while retaining references
        // to source expressions. Stable addresses make that extension safe.
        std::deque<Expr>        exprs{};
        std::vector<Stmt>       stmts{};
        std::vector<Block>      blocks{};
        std::vector<Constraint> constraints{};
        /// Exact local `<...>` or `"..."` C++ include spellings in first-use order.
        std::vector<std::string>    cpp_includes{};
        std::vector<NativeFunction> native_functions{};
        std::vector<Declaration>    declarations{};
        std::vector<DeclarationId>  source_order{};

        [[nodiscard]] const Symbol      &symbol(SymbolId id) const noexcept { return symbols[id.value]; }
        [[nodiscard]] const Type        &type(TypeId id) const noexcept { return types[id.value]; }
        [[nodiscard]] const Expr        &expr(ExprId id) const noexcept { return exprs[id.value]; }
        [[nodiscard]] const Stmt        &stmt(StmtId id) const noexcept { return stmts[id.value]; }
        [[nodiscard]] const Block       &block(BlockId id) const noexcept { return blocks[id.value]; }
        [[nodiscard]] const Constraint  &constraint(ConstraintId id) const noexcept { return constraints[id.value]; }
        [[nodiscard]] const Declaration &declaration(DeclarationId id) const noexcept { return declarations[id.value]; }
    };
}  // namespace hgl::ir::hir

#endif  // HGL_IR_HIR_H
