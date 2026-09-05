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
    };

    struct Parameter
    {
        std::string name{};
        bool        is_const{false};
        TypeId      type{};
        ConstExprId default_value{};
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
        syntax::SourceRange           range{};
    };

    enum class Completion : std::uint8_t {
        Interfaces,
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
    };
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_IR_H
