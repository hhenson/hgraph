#ifndef HGL_HGRAPH_IR_IR_H
#define HGL_HGRAPH_IR_IR_H

#include "ir/hir.h"

#include <cstdint>
#include <optional>
#include <string>
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

    using TypeId      = Id<struct TypeTag>;
    using CallableId  = Id<struct CallableTag>;
    using ConstExprId = Id<struct ConstExprTag>;

    enum class ConstExprKind : std::uint8_t {
        Literal,
        Parameter,
        Unary,
        Binary,
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
        syntax::SourceRange              range{};
    };

    struct TypeArgument
    {
        std::optional<TypeId>      type{};
        std::optional<ConstExprId> value{};
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
    };

    struct GenericParameter
    {
        std::string name{};
        bool        is_const{false};
        TypeId      type{};
    };

    struct Parameter
    {
        std::string                      name{};
        bool                             is_const{false};
        TypeId                           type{};
        std::optional<ir::hir::Constant> default_value{};
    };

    struct OperatorContract
    {
        std::string                   identity{};
        std::string                   registry_name{};
        bool                          imported{false};
        std::vector<GenericParameter> generics{};
        std::vector<Parameter>        parameters{};
        TypeId                        result{};
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
        std::vector<OperatorContract> operators{};
        std::vector<Callable>         callables{};
    };
}  // namespace hgl::hgraph_ir

#endif  // HGL_HGRAPH_IR_IR_H
