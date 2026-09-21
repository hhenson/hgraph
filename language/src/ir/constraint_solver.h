#ifndef HGL_IR_CONSTRAINT_SOLVER_H
#define HGL_IR_CONSTRAINT_SOLVER_H

#include "ir/generic_substitution.h"
#include "ir/type_check.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
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
            Pack,
            Value,
            TypeSet,
            ValueSet,
            FieldSet,
            Boolean,
        };

        struct Operand
        {
            OperandKind                  kind{OperandKind::Invalid};
            bool                         known{false};
            hir::SymbolId                variable{};
            hir::TypeId                  type{};
            hir::ExprId                  value{};
            std::vector<hir::TypeId>     types{};
            std::vector<hir::ExprId>     values{};
            std::vector<std::string>     fields{};
            std::optional<hir::Constant> constant{};
            bool                         named{false};
            std::string                  symbolic{};
            bool                         boolean{false};
        };

        struct EffectiveField
        {
            std::string name{};
            hir::TypeId type{};
        };

        struct FieldNameHash
        {
            using is_transparent = void;
            [[nodiscard]] std::size_t operator()(std::string_view name) const noexcept {
                return std::hash<std::string_view>{}(name);
            }
        };

        /// The effective fields of one applied struct type, in declaration
        /// order, with an index by name so a lookup does not scan them.
        struct EffectiveFields
        {
            std::vector<EffectiveField>                                                  fields{};
            std::unordered_map<std::string, std::size_t, FieldNameHash, std::equal_to<>> index{};

            [[nodiscard]] const EffectiveField *find(std::string_view name) const noexcept {
                const auto found = index.find(name);
                return found == index.end() ? nullptr : &fields[found->second];
            }
        };

        [[nodiscard]] Operand operand(hir::ConstraintId id, GenericSubstitution &substitution);
        [[nodiscard]] Operand type_operand(hir::TypeId type, GenericSubstitution &substitution);
        [[nodiscard]] Operand value_operand(hir::ExprId value, GenericSubstitution &substitution);
        [[nodiscard]] Truth   evaluate(hir::ConstraintId id, GenericSubstitution &substitution,
                                       std::span<const ConstraintPremise> premises);
        [[nodiscard]] Truth   evaluate_relation(const hir::ConstraintRelation &relation, GenericSubstitution &substitution);
        [[nodiscard]] Truth   evaluate_operator(const hir::OperatorRequirement &requirement, GenericSubstitution &substitution,
                                                syntax::SourceRange range, std::span<const ConstraintPremise> premises);
        [[nodiscard]] Truth   evaluate_each(const hir::ConstraintEach &each, GenericSubstitution &substitution,
                                            std::span<const ConstraintPremise> premises);
        [[nodiscard]] bool    infer_equalities(hir::ConstraintId id, GenericSubstitution &substitution, bool &changed);

        [[nodiscard]] bool operand_equivalent(const Operand &lhs, const Operand &rhs) const;
        [[nodiscard]] bool relation_implies(const hir::ConstraintRelation &premise, GenericSubstitution &premise_substitution,
                                            const hir::ConstraintRelation &goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool atomic_equivalent(hir::ConstraintId premise, GenericSubstitution &premise_substitution,
                                             hir::ConstraintId goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool constraint_equivalent(hir::ConstraintId premise, GenericSubstitution &premise_substitution,
                                                 hir::ConstraintId goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool premise_implies(hir::ConstraintId premise, GenericSubstitution &premise_substitution,
                                           hir::ConstraintId goal, GenericSubstitution &goal_substitution);
        [[nodiscard]] bool premises_prove(hir::ConstraintId goal, GenericSubstitution &goal_substitution,
                                          std::span<const ConstraintPremise> premises);

        [[nodiscard]] bool                             is_struct(hir::TypeId type) const noexcept;
        /// The re-description of a struct another module exports, when
        /// `type` names one (ADR 0013): it has no declaration here.
        [[nodiscard]] const hir::ImportedStructDecl   *imported_struct(hir::TypeId type) const noexcept;
        [[nodiscard]] const hir::Parameter            *pack_parameter(hir::SymbolId symbol) const noexcept;
        [[nodiscard]] Operand                          pack_operand(hir::SymbolId symbol, GenericSubstitution &substitution);
        [[nodiscard]] const EffectiveFields           &effective_fields(hir::TypeId type);
        void                                           append_fields(hir::TypeId type, EffectiveFields &fields);
        [[nodiscard]] std::optional<std::string>       string_value(hir::ExprId value) const;
        [[nodiscard]] std::optional<std::string>       string_value(const Operand &value) const;
        [[nodiscard]] std::optional<std::int64_t>      integer_value(const Operand &value) const;
        [[nodiscard]] bool                             same_value(const Operand &lhs, const Operand &rhs) const;
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
        /// Effective fields by canonical struct type. Canonical types and the
        /// struct declarations are fixed while a module is checked, and every
        /// constructor argument asks for one field of the same type.
        std::unordered_map<std::uint32_t, EffectiveFields> effective_fields_{};
    };
}  // namespace hgl::ir::detail

#endif  // HGL_IR_CONSTRAINT_SOLVER_H
