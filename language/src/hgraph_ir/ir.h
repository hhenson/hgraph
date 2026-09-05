#ifndef HGL_HGRAPH_IR_IR_H
#define HGL_HGRAPH_IR_IR_H

#include "ir/hir.h"

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace hgl::hgraph_ir
{
    template <typename Tag> struct Id
    {
        static constexpr std::uint32_t invalid = static_cast<std::uint32_t>(-1);
        std::uint32_t                  value{invalid};

        [[nodiscard]] constexpr bool valid() const noexcept { return value != invalid; }
        friend constexpr bool        operator==(Id, Id) noexcept = default;
    };

    using TypeId       = Id<struct TypeTag>;
    using CallableId   = Id<struct CallableTag>;
    using ConstExprId  = Id<struct ConstExprTag>;
    using ConstraintId = Id<struct ConstraintTag>;
    using BindingId    = Id<struct BindingTag>;
    using ValueId      = Id<struct ValueTag>;
    using StatementId  = Id<struct StatementTag>;
    using BlockId      = Id<struct BlockTag>;

    enum class ConstExprKind : std::uint8_t {
        Literal,
        Parameter,
        Unary,
        Binary,
        Index,
        Field,
        Sequence,
        Tuple,
        Construct,
    };

    struct ConstElement
    {
        ConstExprId key{};
        ConstExprId value{};
    };

    struct ConstArgument
    {
        std::string name{};
        ConstExprId value{};
    };

    struct ConstExpr
    {
        ConstExprKind                    kind{ConstExprKind::Literal};
        std::optional<ir::hir::Constant> literal{};
        std::string                      parameter{};
        ir::hir::UnaryOp                 unary{ir::hir::UnaryOp::Negate};
        ir::hir::BinaryOp                binary{ir::hir::BinaryOp::Add};
        ConstExprId                      lhs{};
        ConstExprId                      rhs{};
        std::string                      member{};
        std::vector<ConstElement>        elements{};
        std::vector<ConstExprId>         items{};
        TypeId                           constructed_type{};
        std::vector<ConstArgument>       arguments{};
        bool                             delta{false};
        syntax::SourceRange              range{};
    };

    struct TypeArgument
    {
        std::optional<TypeId>      type{};
        std::optional<ConstExprId> value{};

        friend bool operator==(const TypeArgument &, const TypeArgument &) = default;
    };

    /// A self-contained canonical HGL type prepared for hgraph lowering.
    /// Source-only spelling, declaration ownership, and type-expression IDs
    /// have been removed; compile-time sizes use the graph-IR expression arena.
    struct Type
    {
        ir::hir::TypeKind         kind{ir::hir::TypeKind::Deferred};
        ir::hir::ScalarType       scalar{ir::hir::ScalarType::Bool};
        std::string               nominal_identity{};
        std::vector<TypeId>       children{};
        std::vector<TypeArgument> arguments{};
        ConstExprId               size{};
        ConstExprId               min_size{};
        bool                      unbounded{false};

        friend bool operator==(const Type &, const Type &) = default;
    };

    struct GenericParameter
    {
        std::string name{};
        bool        is_const{false};
        TypeId      type{};
        BindingId   binding{};
    };

    struct Parameter
    {
        std::string name{};
        bool        is_const{false};
        TypeId      type{};
        ConstExprId default_value{};
        BindingId   binding{};
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
    { std::string identity{}; };
    struct ConstraintType
    { TypeId type{}; };
    struct ConstraintValue
    { ConstExprId value{}; };
    struct ConstraintSet
    { std::vector<ConstraintId> elements{}; };
    struct ConstraintCall
    {
        std::string               function_identity{};
        std::vector<ConstraintId> arguments{};
    };
    struct OperatorRequirement
    {
        std::string               operator_identity{};
        std::string               operator_registry_name{};
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
                                        OperatorRequirement, ConstraintRelation, ConstraintNot, ConstraintLogic>;

    /// A resolved generic requirement. All references use hgraph-IR identities
    /// and arenas, so backends never need semantic symbols or expression IDs.
    struct Constraint
    {
        syntax::SourceRange range{};
        ConstraintNode      node{};
    };

    struct StructField
    {
        std::string         name{};
        TypeId              type{};
        ConstExprId         default_value{};
        std::string         origin_identity{};
        bool                optional{false};
        syntax::SourceRange range{};
    };

    /// A nominal value/schema contract with its complete inherited field set.
    /// Concrete specializations can be built without walking HIR declarations.
    struct StructContract
    {
        std::string                   identity{};
        bool                          exported{false};
        bool                          abstract{false};
        std::vector<GenericParameter> generics{};
        std::vector<TypeId>           parents{};
        ConstraintId                  requirements{};
        std::vector<StructField>      fields{};
        syntax::SourceRange           range{};
    };

    struct OperatorContract
    {
        std::string                   identity{};
        std::string                   registry_name{};
        bool                          imported{false};
        std::vector<GenericParameter> generics{};
        std::vector<Parameter>        parameters{};
        TypeId                        result{};
        ConstraintId                  requirements{};
        syntax::SourceRange           range{};
    };

    struct Capability
    {
        std::string name{};
        TypeId      type{};
        BindingId   binding{};
    };

    enum class BindingKind : std::uint8_t {
        TypeParameter,
        ConstParameter,
        SignalParameter,
        LocalLet,
        LocalVar,
        State,
        Capability,
        LoopValue,
        LambdaParameter,
    };

    /// One addressable value owned by a callable, lambda, or nominal contract.
    /// The hgraph IR deliberately keeps no HIR SymbolId references.
    struct Binding
    {
        std::string         name{};
        BindingKind         kind{BindingKind::LocalLet};
        TypeId              type{};
        std::string         owner_identity{};
        std::uint32_t       index{0};
        syntax::SourceRange range{};
    };

    enum class ReferenceKind : std::uint8_t {
        Binding,
        Callable,
        Operator,
        Struct,
        Intrinsic,
    };

    struct Reference
    {
        ReferenceKind kind{ReferenceKind::Binding};
        BindingId     binding{};
        CallableId    callable{};
        std::string   identity{};
        std::string   registry_name{};
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

    struct Substitution
    {
        BindingId                        parameter{};
        std::string                      parameter_identity{};
        TypeId                           type{};
        ConstExprId                      value{};
        std::optional<ir::hir::Constant> constant{};
    };

    /// Resolved semantic operation attached to a value. Canonical language
    /// identity and native registry spelling remain separate, while local
    /// callables are addressed by arena ID.
    struct Operation
    {
        OperationKind             kind{OperationKind::None};
        CallableId                callable{};
        CallableId                candidate{};
        BindingId                 capability{};
        std::string               identity{};
        std::string               registry_name{};
        std::string               candidate_identity{};
        std::string               candidate_label{};
        std::vector<Substitution> substitutions{};
        bool                      deferred{false};
    };

    struct Literal
    { ir::hir::Constant value{}; };
    struct Unary
    {
        ir::hir::UnaryOp op{ir::hir::UnaryOp::Negate};
        ValueId          operand{};
    };
    struct Binary
    {
        ir::hir::BinaryOp op{ir::hir::BinaryOp::Add};
        ValueId           lhs{};
        ValueId           rhs{};
    };
    struct Argument
    {
        std::string         name{};
        ValueId             value{};
        syntax::SourceRange range{};
    };
    struct Call
    {
        ValueId               callee{};
        std::vector<Argument> arguments{};
    };
    struct Index
    {
        ValueId target{};
        ValueId index{};
    };
    struct Field
    {
        ValueId             target{};
        std::string         name{};
        syntax::SourceRange name_range{};
    };
    struct SequenceElement
    {
        ValueId key{};
        ValueId value{};
    };
    struct Sequence
    { std::vector<SequenceElement> elements{}; };
    struct Tuple
    { std::vector<ValueId> elements{}; };
    struct Lambda
    {
        std::vector<BindingId> parameters{};
        TypeId                 result{};
        ValueId                body{};
    };
    struct Conditional
    {
        ValueId condition{};
        BlockId then_block{};
        ValueId otherwise{};
    };
    struct BlockValue
    { BlockId block{}; };
    struct HarnessEval
    {
        ValueId               callee{};
        std::vector<Argument> arguments{};
    };
    struct Construct
    {
        TypeId                type{};
        std::vector<Argument> arguments{};
        bool                  delta{false};
    };
    using ValueNode = std::variant<Literal, Reference, Unary, Binary, Call, Index, Field, Sequence, Tuple, Lambda, Conditional,
                                   BlockValue, HarnessEval, Construct>;

    struct Value
    {
        syntax::SourceRange              range{};
        TypeId                           type{};
        ir::hir::Phase                   phase{ir::hir::Phase::Unknown};
        ir::hir::ValueKind               value_kind{ir::hir::ValueKind::Unknown};
        ValueNode                        node{};
        ir::hir::Effect                  effects{ir::hir::Effect::None};
        std::optional<ir::hir::Constant> constant{};
        Operation                        operation{};
    };

    struct LocalBinding
    {
        BindingId binding{};
        TypeId    type{};
        ValueId   init{};
    };
    struct StateBinding
    {
        BindingId binding{};
        TypeId    type{};
        ValueId   init{};
    };
    struct Inject
    { std::vector<BindingId> bindings{}; };
    enum class LifecycleKind : std::uint8_t {
        Start,
        Stop,
    };
    struct Lifecycle
    {
        LifecycleKind kind{LifecycleKind::Start};
        BlockId       block{};
    };
    struct Activation
    {
        ValueId condition{};
        BlockId block{};
    };
    struct Traversal
    {
        std::vector<BindingId> bindings{};
        ValueId                iterable{};
        BlockId                block{};
    };
    enum class AssignOp : std::uint8_t {
        Assign,
        Add,
        Sub,
        Mul,
        Div,
    };
    struct Assignment
    {
        AssignOp op{AssignOp::Assign};
        ValueId  place{};
        ValueId  value{};
    };
    struct Return
    { ValueId value{}; };
    struct Assert
    { ValueId condition{}; };
    struct Evaluate
    { ValueId value{}; };
    using StatementNode =
        std::variant<LocalBinding, StateBinding, Inject, Lifecycle, Activation, Traversal, Assignment, Return, Assert, Evaluate>;

    struct Statement
    {
        syntax::SourceRange range{};
        StatementNode       node{};
        ir::hir::Effect     effects{ir::hir::Effect::None};
    };

    struct Block
    {
        syntax::SourceRange      range{};
        std::vector<StatementId> statements{};
        ValueId                  tail{};
        ir::hir::Effect          effects{ir::hir::Effect::None};
    };

    enum class CallableVisibility : std::uint8_t {
        Internal,
        Export,
        Implementation,
    };

    enum class CallableKind : std::uint8_t {
        Composition,
        RuntimeNode,
    };

    /// The execution-facing callable interface. Body/control-flow lowering is
    /// deliberately a following pass; this first checkpoint proves that
    /// backends can consume stable canonical types and nominal identities
    /// without reaching back into the syntax AST.
    struct Callable
    {
        std::string                   identity{};
        std::string                   operator_identity{};
        std::string                   operator_registry_name{};
        CallableVisibility            visibility{CallableVisibility::Internal};
        CallableKind                  kind{CallableKind::Composition};
        std::vector<GenericParameter> generics{};
        std::vector<Parameter>        parameters{};
        TypeId                        result{};
        ConstraintId                  requirements{};
        ir::hir::Effect               effects{ir::hir::Effect::None};
        std::vector<Capability>       capabilities{};
        ValueId                       concise_body{};
        BlockId                       block_body{};
        syntax::SourceRange           range{};
    };

    struct TestPlan
    {
        std::string         identity{};
        BlockId             body{};
        syntax::SourceRange range{};
    };

    enum class Completion : std::uint8_t {
        Interfaces,
        Bodies,
        Executable,
    };

    struct Module
    {
        std::string                   path{};
        Completion                    completion{Completion::Interfaces};
        std::vector<ConstExpr>        const_exprs{};
        std::vector<Type>             types{};
        std::vector<Constraint>       constraints{};
        std::vector<StructContract>   structures{};
        std::vector<OperatorContract> operators{};
        std::vector<Callable>         callables{};
        std::vector<Binding>          bindings{};
        std::vector<Value>            values{};
        std::vector<Statement>        statements{};
        std::vector<Block>            blocks{};
        std::vector<TestPlan>         tests{};
    };
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_IR_H
