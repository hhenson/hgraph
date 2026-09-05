#ifndef HGL_IR_CONSTRAINT_SOLVER_H
#define HGL_IR_CONSTRAINT_SOLVER_H

#include "ir/generic_substitution.h"
#include "ir/type_check.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgl::ir::detail
{
    struct RequiredOperation
    {
        hir::SymbolId op{};
        hir::TypeId   result{};
        std::string   identity{};
    };

    /// One requirement already guaranteed by the generic declaration whose
    /// body is being checked. The optional substitution maps an inherited
    /// operator contract into the implementation's symbols.
    struct ConstraintPremise
    {
        hir::ConstraintId    requirement{};
        GenericSubstitution *substitution{};
    };

    /// Evaluates normalized HIR constraints after ordinary signature
    /// unification. Native operator viability is delegated through the same
    /// resolver port used for calls; this layer never ranks overloads.
    class ConstraintSolver
    {
      public:
        ConstraintSolver(hir::Module &module, CanonicalTypes &types, const OperatorResolver &resolve_operator,
                         syntax::DiagnosticSink &diagnostics);

        /// Infer positive-conjunction equalities to a fixed point, then
        /// require the complete Boolean constraint to be true or follow from
        /// the declaration requirements supplied as premises.
        [[nodiscard]] bool solve(hir::ConstraintId requirement, GenericSubstitution &substitution, syntax::SourceRange use_range,
                                 std::string_view subject, bool report = true, std::span<const ConstraintPremise> premises = {});

        /// Symbolic facts used while checking a generic declaration body.
        [[nodiscard]] bool                             proves_numeric(hir::ConstraintId requirement, hir::TypeId type,
                                                                      GenericSubstitution *substitution = nullptr);
        [[nodiscard]] std::optional<RequiredOperation> required_operation(hir::ConstraintId requirement, std::string_view identity,
                                                                          const std::vector<hir::TypeId> &arguments,
                                                                          GenericSubstitution            *substitution = nullptr);
        [[nodiscard]] std::optional<hir::TypeId> field_type(hir::ConstraintId requirement, hir::TypeId subject,
                                                            std::string_view field, GenericSubstitution *substitution = nullptr);

      private:
        enum class Truth : std::uint8_t {
            False,
            True,
            Unresolved,
        };

        enum class OperandKind : std::uint8_t {
            Invalid,
            Type,
            Value,
            TypeSet,
            ValueSet,
            FieldSet,
            Boolean,
        };

        struct Operand
        {
            OperandKind              kind{OperandKind::Invalid};
            bool                     known{false};
            hir::SymbolId            variable{};
            hir::TypeId              type{};
            hir::ExprId              value{};
            std::vector<hir::TypeId> types{};
            std::vector<hir::ExprId> values{};
            std::vector<std::string> fields{};
            bool                     boolean{false};
        };

        struct EffectiveField
        {
            std::string name{};
            hir::TypeId type{};
        };

        [[nodiscard]] Operand operand(hir::ConstraintId id, GenericSubstitution &substitution);
        [[nodiscard]] Operand type_operand(hir::TypeId type, GenericSubstitution &substitution);
        [[nodiscard]] Operand value_operand(hir::ExprId value, GenericSubstitution &substitution);
        [[nodiscard]] Truth   evaluate(hir::ConstraintId id, GenericSubstitution &substitution,
                                       std::span<const ConstraintPremise> premises);
        [[nodiscard]] Truth   evaluate_relation(const hir::ConstraintRelation &relation, GenericSubstitution &substitution);
        [[nodiscard]] Truth   evaluate_operator(const hir::OperatorRequirement &requirement, GenericSubstitution &substitution,
                                                syntax::SourceRange range, std::span<const ConstraintPremise> premises);
        [[nodiscard]] bool    infer_equalities(hir::ConstraintId id, GenericSubstitution &substitution, bool &changed);

        [[nodiscard]] bool operand_equivalent(const Operand &lhs, const Operand &rhs) const;
        [[nodiscard]] bool relation_implies(const hir::ConstraintRelation &premise, GenericSubstitution &premise_substitution,
                                            const hir::ConstraintRelation &goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool atomic_equivalent(hir::ConstraintId premise, GenericSubstitution &premise_substitution,
                                             hir::ConstraintId goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool premise_implies(hir::ConstraintId premise, GenericSubstitution &premise_substitution,
                                           hir::ConstraintId goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool premises_prove(hir::ConstraintId goal, GenericSubstitution &goal_substitution,
                                          std::span<const ConstraintPremise> premises);

        [[nodiscard]] bool                             is_struct(hir::TypeId type) const noexcept;
        [[nodiscard]] std::vector<EffectiveField>      effective_fields(hir::TypeId type);
        void                                           append_fields(hir::TypeId type, std::vector<EffectiveField> &fields);
        [[nodiscard]] std::optional<std::string>       string_value(hir::ExprId value) const;
        [[nodiscard]] bool                             same_value(hir::ExprId lhs, hir::ExprId rhs) const;
        [[nodiscard]] bool                             same_symbolic_type(hir::TypeId lhs, hir::TypeId rhs) const noexcept;
        [[nodiscard]] std::string                      operator_identity(hir::SymbolId symbol) const;
        [[nodiscard]] std::string                      operator_registry_name(hir::SymbolId symbol) const;
        [[nodiscard]] static bool                      identity_matches(std::string_view lhs, std::string_view rhs) noexcept;
        [[nodiscard]] std::optional<RequiredOperation> find_required_operation(hir::ConstraintId               requirement,
                                                                               std::string_view                identity,
                                                                               const std::vector<hir::TypeId> &arguments,
                                                                               GenericSubstitution            &substitution);
        [[nodiscard]] std::optional<hir::TypeId> find_required_field(hir::ConstraintId requirement, hir::TypeId subject,
                                                                     std::string_view field, GenericSubstitution &substitution);

        void fail(std::string message);

        hir::Module            &module_;
        CanonicalTypes         &types_;
        const OperatorResolver &resolve_operator_;
        syntax::DiagnosticSink &diagnostics_;
        std::string             failure_detail_{};
        std::size_t             evaluation_depth_{0};
    };
}  // namespace hgl::ir::detail

#endif  // HGL_IR_CONSTRAINT_SOLVER_H
